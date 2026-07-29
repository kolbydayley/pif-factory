from __future__ import annotations

"""Run the reusable side-free candidate evaluator through managed app-server turns."""

import argparse
import asyncio
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from . import app_server_candidate_evaluation_bundle as evaluator
from . import app_server_capacity as capacity_module
from . import app_server_capacity_reserve as reserve_module
from . import codex_app_server
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .app_server_runtime_verifier import ContentHashCache
from .util import now_iso


CONFIG_VERSION = "pif_candidate_semantic_evaluation_run_config_v1"
LOCK_VERSION = "pif_candidate_semantic_evaluation_run_lock_v1"
PHASE_LOCK_VERSION = "pif_candidate_semantic_evaluation_phase_lock_v1"
TERMINAL_VERSION = "pif_candidate_semantic_evaluation_terminal_v1"
PROJECT_ROOT = evaluator.PROJECT_ROOT
PINNED_CODEX = (
    PROJECT_ROOT
    / "work"
    / "app-server-development-v2"
    / "pinned-runtime"
    / "codex-0.144.1"
    / "bin"
    / "codex"
).resolve()
MINIMUM_REMAINING_RESERVE_PERCENT = 20
QUOTA_POINTS_PER_MILLION_TOKENS = 17
SUPPORT_TOTAL_TOKEN_MAXIMUM = 70_000
ALIGNMENT_TOTAL_TOKEN_MAXIMUM = 180_000
ADJUDICATION_TOTAL_TOKEN_MAXIMUM = 180_000
TIMEOUT_SECONDS = 1200.0
USAGE_FIELDS = (
    "input_tokens",
    "cached_input_tokens",
    "output_tokens",
    "reasoning_output_tokens",
    "total_tokens",
)
HASH_CACHE = ContentHashCache()


class CandidateEvaluationRunError(RuntimeError):
    """The immutable candidate evaluation run cannot proceed safely."""


class CandidateSemanticQualityStop(CandidateEvaluationRunError):
    """Measured candidate semantics failed a frozen development gate."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CandidateEvaluationRunError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise CandidateEvaluationRunError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_private_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise CandidateEvaluationRunError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _record(path: Path) -> dict[str, Any]:
    return HASH_CACHE.record(path.expanduser().resolve())


def _verify_record(record: Mapping[str, Any]) -> None:
    if not HASH_CACHE.verify_record(record):
        raise CandidateEvaluationRunError("frozen direct artifact drifted")


def _direct_digest(records: Sequence[Mapping[str, Any]]) -> str:
    return hashlib.sha256(_canonical_json(list(records)).encode("utf-8")).hexdigest()


def _turn_paths(phase_root: Path, turn_name: str) -> dict[str, Path]:
    root = phase_root / "turns" / turn_name.replace("_", "-")
    return {
        "root": root,
        "input": root / "input.private.json",
        "base": root / "base-instructions.private.md",
        "prompt": root / "prompt.private.md",
        "schema": root / "schema.json",
        "capacity": root / "capacity.json",
        "sidecar": root / "sidecar.json",
        "output": root / "output.private.json",
    }


def _phase_paths(root: Path, phase: str) -> dict[str, Path]:
    phase_root = root / phase
    return {
        "root": phase_root,
        "audit": phase_root / "capacity-policy-audit.json",
        "policy": phase_root / "capacity-policy.json",
        "lock": phase_root / "runtime-lock.json",
        "launch": phase_root / "launch-receipt.json",
        "terminal": phase_root / "terminal.json",
    }


def _runtime_files() -> tuple[Path, ...]:
    return tuple(
        sorted(
            {
                Path(__file__).resolve(),
                Path(evaluator.__file__).resolve(),
                Path(capacity_module.__file__).resolve(),
                Path(reserve_module.__file__).resolve(),
                Path(codex_app_server.__file__).resolve(),
                PINNED_CODEX,
            },
            key=str,
        )
    )


def _validate_source_records(config: Mapping[str, Any]) -> None:
    for key in (
        "source",
        "candidate",
        "provenance",
        "shared_reference",
        "candidate_terminal",
        "candidate_gate",
        "evaluator_protocol_lock",
        "evaluator_protocol_receipt",
    ):
        record = config.get(key)
        if not isinstance(record, Mapping):
            raise CandidateEvaluationRunError(f"config has no {key} record")
        _verify_record(record)


def _validate_config(config_path: Path) -> dict[str, Any]:
    config = _load_json(config_path, "candidate evaluation config")
    root = config_path.parent.resolve()
    if (
        not isinstance(config, dict)
        or config.get("schema_version") != CONFIG_VERSION
        or config.get("output_root") != str(root)
        or not isinstance(config.get("evaluation_id"), str)
        or not str(config["evaluation_id"]).isascii()
        or not str(config["evaluation_id"])
        or config.get("support_model") != evaluator.SUPPORT_MODEL
        or config.get("support_effort") != evaluator.SUPPORT_EFFORT
        or config.get("alignment_model") != evaluator.ALIGNMENT_MODEL
        or config.get("alignment_effort") != evaluator.ALIGNMENT_EFFORT
        or config.get("adjudication_model") != evaluator.ADJUDICATION_MODEL
        or config.get("adjudication_effort") != evaluator.ADJUDICATION_EFFORT
        or config.get("retry_count_per_turn") != 0
        or config.get("managed_chatgpt_auth_only") is not True
        or config.get("official_persistent_codex_app_server_only") is not True
        or config.get("semantic_regex_or_keyword_rules_allowed") is not False
        or config.get("production_mutation_allowed") is not False
        or config.get("holdout_authorized") is not False
        or isinstance(config.get("production_amortized_total_token_ratio"), bool)
        or not isinstance(config.get("production_amortized_total_token_ratio"), (int, float))
        or not 0 <= float(config["production_amortized_total_token_ratio"]) <= 1
    ):
        raise CandidateEvaluationRunError("candidate evaluation config drifted")
    _validate_source_records(config)
    protocol_root = Path(str(config["evaluator_protocol_lock"]["path"])).parent
    evaluator.verify_protocol(protocol_root)
    terminal = _load_json(Path(str(config["candidate_terminal"]["path"])), "candidate terminal")
    gate = _load_json(Path(str(config["candidate_gate"]["path"])), "candidate gate")
    ratio = float(config["production_amortized_total_token_ratio"])
    if (
        terminal.get("accounting_complete") is not True
        or terminal.get("support_alignment_authorized") is not True
        or terminal.get("production_mutated") is not False
        or gate.get("passed") is not True
        or gate.get("support_alignment_authorized") is not True
        or gate.get("production_mutated") is not False
        or float(gate.get("production_amortized_total_token_ratio", -1)) != ratio
        or ratio > evaluator.TOKEN_RATIO_TARGET
    ):
        raise CandidateEvaluationRunError("candidate structural or cost predecessor drifted")
    return config


def create_config(
    *,
    output_dir: Path,
    evaluation_id: str,
    source_path: Path,
    candidate_path: Path,
    provenance_path: Path,
    shared_reference_path: Path,
    candidate_terminal_path: Path,
    candidate_gate_path: Path,
    production_amortized_total_token_ratio: float,
    protocol_root: Path = evaluator.DEFAULT_PROTOCOL_ROOT,
) -> Path:
    root = output_dir.expanduser().resolve()
    config_path = root / "evaluation-config.json"
    config = {
        "schema_version": CONFIG_VERSION,
        "evaluation_id": evaluation_id,
        "output_root": str(root),
        "source": _record(source_path),
        "candidate": _record(candidate_path),
        "provenance": _record(provenance_path),
        "shared_reference": _record(shared_reference_path),
        "candidate_terminal": _record(candidate_terminal_path),
        "candidate_gate": _record(candidate_gate_path),
        "evaluator_protocol_lock": _record(protocol_root / "runtime-lock.json"),
        "evaluator_protocol_receipt": _record(
            protocol_root / "runtime-lock-receipt.json"
        ),
        "support_model": evaluator.SUPPORT_MODEL,
        "support_effort": evaluator.SUPPORT_EFFORT,
        "alignment_model": evaluator.ALIGNMENT_MODEL,
        "alignment_effort": evaluator.ALIGNMENT_EFFORT,
        "adjudication_model": evaluator.ADJUDICATION_MODEL,
        "adjudication_effort": evaluator.ADJUDICATION_EFFORT,
        "production_amortized_total_token_ratio": round(
            production_amortized_total_token_ratio, 6
        ),
        "retry_count_per_turn": 0,
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "semantic_regex_or_keyword_rules_allowed": False,
        "production_mutation_allowed": False,
        "holdout_authorized": False,
    }
    _write_immutable(config_path, config)
    _validate_config(config_path)
    return config_path


def _capacity_policy(
    *,
    phase_root: Path,
    phase_id: str,
    turn_names: Sequence[str],
    maximum_total_tokens_per_turn: int,
) -> dict[str, Path]:
    paths = _phase_paths(phase_root.parent, phase_root.name)
    projected_tokens = len(turn_names) * maximum_total_tokens_per_turn
    projected_points = math.ceil(
        projected_tokens * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
    )
    _write_immutable(
        paths["audit"],
        {
            "schema_version": "pif_app_server_capacity_policy_audit_v20",
            "phase_id": phase_id,
            "measured_sidecar_count": 0,
            "maximum_total_tokens_per_turn": maximum_total_tokens_per_turn,
            "phase_total_token_bound": projected_tokens,
            "projected_phase_quota_points": projected_points,
            "minimum_remaining_reserve_percent": MINIMUM_REMAINING_RESERVE_PERCENT,
            "managed_chatgpt_auth_only": True,
            "production_mutation_performed": False,
        },
    )
    _write_immutable(
        paths["policy"],
        {
            "schema_version": reserve_module.RESERVE_CAPACITY_POLICY_VERSION,
            "phase_id": phase_id,
            "semantic_output_root": str(phase_root),
            "ordered_turn_names": list(turn_names),
            "minimum_remaining_reserve_percent": MINIMUM_REMAINING_RESERVE_PERCENT,
            "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
            "maximum_total_tokens_per_turn": maximum_total_tokens_per_turn,
            "phase_total_token_bound": projected_tokens,
            "projected_phase_quota_points": projected_points,
            "managed_chatgpt_auth_only": True,
            "official_persistent_codex_app_server_only": True,
            "retry_count_per_turn": 0,
            "production_mutation_allowed": False,
            "rate_limit_reached_type_must_be_null": True,
            "unknown_usage_hard_stop": True,
            "audit": _record(paths["audit"]),
        },
    )
    reserve_module.load_reserve_capacity_policy(paths["policy"])
    return paths


def _freeze_turn(
    *,
    phase_root: Path,
    turn_name: str,
    input_value: Mapping[str, Any],
    base_instructions: str,
    prompt: str,
    schema: Mapping[str, Any],
) -> dict[str, Path]:
    paths = _turn_paths(phase_root, turn_name)
    _write_immutable(paths["input"], input_value)
    _write_private_text(paths["base"], base_instructions)
    _write_private_text(paths["prompt"], prompt)
    _write_immutable(paths["schema"], schema)
    return paths


def _phase_lock(
    *,
    root: Path,
    phase: str,
    phase_id: str,
    turn_names: Sequence[str],
    maximum_total_tokens_per_turn: int,
    direct_records: Sequence[Mapping[str, Any]],
) -> Path:
    phase_paths = _capacity_policy(
        phase_root=root / phase,
        phase_id=phase_id,
        turn_names=turn_names,
        maximum_total_tokens_per_turn=maximum_total_tokens_per_turn,
    )
    records = [
        *direct_records,
        _record(root / "runtime-lock.json"),
        _record(phase_paths["audit"]),
        _record(phase_paths["policy"]),
    ]
    lock = {
        "schema_version": PHASE_LOCK_VERSION,
        "frozen_at": now_iso(),
        "phase": phase,
        "phase_id": phase_id,
        "turn_names": list(turn_names),
        "maximum_total_tokens_per_turn": maximum_total_tokens_per_turn,
        "direct_records": records,
        "direct_record_digest": _direct_digest(records),
        "retry_count_per_turn": 0,
        "production_mutation_allowed": False,
    }
    lock_path = phase_paths["lock"]
    if lock_path.exists():
        lock["frozen_at"] = _load_json(lock_path, "phase runtime lock")["frozen_at"]
    _write_immutable(lock_path, lock)
    return verify_phase_lock(lock_path)


def verify_phase_lock(path: Path) -> Path:
    lock_path = path.expanduser().resolve()
    lock = _load_json(lock_path, "phase runtime lock")
    records = lock.get("direct_records") or []
    if (
        lock.get("schema_version") != PHASE_LOCK_VERSION
        or lock.get("retry_count_per_turn") != 0
        or lock.get("production_mutation_allowed") is not False
        or not isinstance(records, list)
        or not records
        or lock.get("direct_record_digest") != _direct_digest(records)
    ):
        raise CandidateEvaluationRunError("phase runtime lock drifted")
    for record in records:
        _verify_record(record)
    policy = lock_path.parent / "capacity-policy.json"
    reserve_module.load_reserve_capacity_policy(policy)
    return lock_path


def freeze_run(config_path: Path) -> dict[str, Any]:
    config_path = config_path.expanduser().resolve()
    root = config_path.parent
    config = _validate_config(config_path)
    source = _load_json(Path(str(config["source"]["path"])), "source")
    candidate = _load_json(Path(str(config["candidate"]["path"])), "candidate")
    provenance = _load_json(Path(str(config["provenance"]["path"])), "provenance")
    shared_reference = _load_json(
        Path(str(config["shared_reference"]["path"])), "shared reference"
    )
    support_bundle = evaluator.build_support_bundle(
        evaluation_id=str(config["evaluation_id"]),
        source=source,
        candidate=candidate,
        provenance=provenance,
        shared_reference=shared_reference,
    )
    support_bundle_path = root / "support" / "support-bundle.private.json"
    _write_immutable(support_bundle_path, support_bundle)
    support_turn = _freeze_turn(
        phase_root=root / "support",
        turn_name="pointwise_support",
        input_value=support_bundle["support_value"],
        base_instructions=evaluator.support_instructions(),
        prompt=str(support_bundle["prompt"]),
        schema=support_bundle["schema"],
    )
    runtime_records = [_record(path) for path in _runtime_files()]
    source_records = [
        config[key]
        for key in (
            "source",
            "candidate",
            "provenance",
            "shared_reference",
            "candidate_terminal",
            "candidate_gate",
            "evaluator_protocol_lock",
            "evaluator_protocol_receipt",
        )
    ]
    request_records = [
        _record(support_bundle_path),
        *[_record(support_turn[key]) for key in ("input", "base", "prompt", "schema")],
    ]
    records = [*runtime_records, *source_records, _record(config_path), *request_records]
    lock_path = root / "runtime-lock.json"
    lock = {
        "schema_version": LOCK_VERSION,
        "frozen_at": now_iso(),
        "evaluation_id": config["evaluation_id"],
        "config": _record(config_path),
        "runtime_files": runtime_records,
        "source_records": source_records,
        "support_request_records": request_records,
        "direct_record_digest": _direct_digest(records),
        "retry_count_per_turn": 0,
        "production_mutation_allowed": False,
        "holdout_authorized": False,
    }
    if lock_path.exists():
        lock["frozen_at"] = _load_json(lock_path, "runtime lock")["frozen_at"]
    _write_immutable(lock_path, lock)
    verify_run_lock(lock_path)
    support_lock = _phase_lock(
        root=root,
        phase="support",
        phase_id=f"{config['evaluation_id']}_support",
        turn_names=("pointwise_support",),
        maximum_total_tokens_per_turn=SUPPORT_TOTAL_TOKEN_MAXIMUM,
        direct_records=request_records,
    )
    return {
        "root": root,
        "config": config,
        "runtime_lock": lock_path,
        "support_lock": support_lock,
        "support_bundle": support_bundle_path,
        "support_turn": support_turn,
    }


def verify_run_lock(path: Path) -> Path:
    lock_path = path.expanduser().resolve()
    lock = _load_json(lock_path, "candidate evaluation runtime lock")
    config_path = Path(str((lock.get("config") or {}).get("path") or "")).resolve()
    config = _validate_config(config_path)
    expected_runtime_paths = {str(path) for path in _runtime_files()}
    actual_runtime_paths = {
        str(Path(str(record["path"])).resolve())
        for record in lock.get("runtime_files") or []
    }
    expected_source = {
        str(Path(str(config[key]["path"])).resolve())
        for key in (
            "source",
            "candidate",
            "provenance",
            "shared_reference",
            "candidate_terminal",
            "candidate_gate",
            "evaluator_protocol_lock",
            "evaluator_protocol_receipt",
        )
    }
    actual_source = {
        str(Path(str(record["path"])).resolve())
        for record in lock.get("source_records") or []
    }
    records = [
        *(lock.get("runtime_files") or []),
        *(lock.get("source_records") or []),
        lock.get("config") or {},
        *(lock.get("support_request_records") or []),
    ]
    if (
        lock.get("schema_version") != LOCK_VERSION
        or lock.get("evaluation_id") != config["evaluation_id"]
        or lock.get("retry_count_per_turn") != 0
        or lock.get("production_mutation_allowed") is not False
        or lock.get("holdout_authorized") is not False
        or actual_runtime_paths != expected_runtime_paths
        or actual_source != expected_source
        or lock.get("direct_record_digest") != _direct_digest(records)
    ):
        raise CandidateEvaluationRunError("candidate evaluation runtime lock drifted")
    for record in records:
        _verify_record(record)
    return lock_path


def _inner_factory() -> codex_app_server.CodexAppServerClient:
    return codex_app_server.CodexAppServerClient(
        command=[str(PINNED_CODEX), "app-server", "--stdio", "--strict-config"]
    )


def _client_factory(policy_path: Path) -> ReserveCapacityGatedCodexAppServerClient:
    return ReserveCapacityGatedCodexAppServerClient(
        policy_path=policy_path,
        inner_factory=_inner_factory,
    )


def _usage(path: Path, *, model: str, effort: str, maximum: int) -> dict[str, int]:
    sidecar = _load_json(path, "semantic sidecar")
    capacity = _load_json(path.parent / "capacity.json", "capacity checkpoint")
    usage = sidecar.get("usage") if isinstance(sidecar, Mapping) else None
    if (
        sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("model") != model
        or sidecar.get("effort") != effort
        or not isinstance(usage, Mapping)
        or capacity.get("schema_version")
        != reserve_module.RESERVE_CAPACITY_CHECKPOINT_VERSION
        or capacity.get("managed_chatgpt_auth_verified") is not True
        or capacity.get("cleared_for_semantic_turn") is not True
        or capacity.get("rate_limit_reached_type") is not None
    ):
        raise CandidateEvaluationRunError("semantic sidecar is not completed and measured")
    normalized: dict[str, int] = {}
    for field in USAGE_FIELDS:
        value = usage.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise CandidateEvaluationRunError("semantic sidecar usage is incomplete")
        normalized[field] = value
    if normalized["total_tokens"] > maximum:
        raise CandidateEvaluationRunError("semantic turn exceeded its frozen token bound")
    return normalized


def _aggregate_usage(root: Path) -> dict[str, Any]:
    totals = {field: 0 for field in USAGE_FIELDS}
    measured = 0
    unknown = 0
    rows = []
    for sidecar_path in sorted(root.glob("*/turns/*/sidecar.json")):
        capacity_path = sidecar_path.parent / "capacity.json"
        try:
            sidecar = _load_json(sidecar_path, "sidecar")
            capacity = _load_json(capacity_path, "capacity checkpoint")
            usage = sidecar.get("usage") if isinstance(sidecar, Mapping) else None
            values = {}
            if (
                sidecar.get("state") != "completed"
                or sidecar.get("status") != "completed"
                or sidecar.get("usage_status") != "measured"
                or sidecar.get("usage_complete") is not True
                or not isinstance(usage, Mapping)
                or capacity.get("schema_version")
                != reserve_module.RESERVE_CAPACITY_CHECKPOINT_VERSION
                or capacity.get("managed_chatgpt_auth_verified") is not True
                or capacity.get("cleared_for_semantic_turn") is not True
                or capacity.get("rate_limit_reached_type") is not None
            ):
                raise CandidateEvaluationRunError("sidecar usage unknown")
            for field in USAGE_FIELDS:
                value = usage.get(field)
                if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                    raise CandidateEvaluationRunError("sidecar usage unknown")
                values[field] = value
        except CandidateEvaluationRunError:
            unknown += 1
            rows.append(
                {
                    "sidecar": _record(sidecar_path),
                    "capacity": (
                        _record(capacity_path)
                        if capacity_path.is_file()
                        else None
                    ),
                    "usage_status": "unknown",
                }
            )
            continue
        measured += 1
        for field in USAGE_FIELDS:
            totals[field] += values[field]
        rows.append(
            {
                "sidecar": _record(sidecar_path),
                "capacity": _record(capacity_path),
                "usage_status": "measured",
                "usage": values,
            }
        )
    return {
        "usage_status": "complete" if not unknown else "unknown",
        "accounting_complete": not unknown,
        "measured_turn_count": measured,
        "unknown_usage_turn_count": unknown,
        "usage": totals if not unknown else None,
        "turns": rows,
    }


def _assert_turn_adoptable_or_unstarted(paths: Mapping[str, Path]) -> bool:
    if paths["sidecar"].is_file():
        if not paths["output"].is_file():
            raise CandidateEvaluationRunError("completed sidecar has no output")
        return True
    if paths["capacity"].exists() or paths["output"].exists():
        raise CandidateEvaluationRunError(
            "ambiguous semantic attempt artifact exists without measured sidecar"
        )
    return False


async def _run_turn(
    *,
    client: Any,
    paths: Mapping[str, Path],
    model: str,
    effort: str,
    maximum_total_tokens: int,
    timeout_seconds: float,
) -> tuple[dict[str, Any], dict[str, int], bool]:
    adopted = _assert_turn_adoptable_or_unstarted(paths)
    if not adopted:
        await client.run_ephemeral_structured_turn(
            model=model,
            effort=effort,
            base_instructions=paths["base"].read_text(encoding="utf-8"),
            prompt=paths["prompt"].read_text(encoding="utf-8"),
            output_schema=_load_json(paths["schema"], "output schema"),
            cwd=PROJECT_ROOT,
            sidecar_path=paths["sidecar"],
            output_path=paths["output"],
            batch_size=1,
            thread_mode="new_thread",
            timeout_seconds=timeout_seconds,
            capacity_checkpoint_path=paths["capacity"],
        )
    usage = _usage(
        paths["sidecar"],
        model=model,
        effort=effort,
        maximum=maximum_total_tokens,
    )
    output = _load_json(paths["output"], "semantic output")
    return output, usage, adopted


def _phase_launch(
    *,
    root: Path,
    phase: str,
    semantic_attempt_count: int,
) -> None:
    paths = _phase_paths(root, phase)
    verify_run_lock(root / "runtime-lock.json")
    verify_phase_lock(paths["lock"])
    _write_stable_time(
        paths["launch"],
        {
            "schema_version": "pif_candidate_semantic_evaluation_phase_launch_v1",
            "launched_at": now_iso(),
            "phase": phase,
            "semantic_attempt_count": semantic_attempt_count,
            "semantic_retry_count": 0,
            "managed_chatgpt_auth_only": True,
            "runtime_lock": _record(paths["lock"]),
            "production_mutation_allowed": False,
            "holdout_authorized": False,
        },
        "launched_at",
    )


def _phase_terminal(
    *, root: Path, phase: str, state: str, records: Mapping[str, Any]
) -> None:
    paths = _phase_paths(root, phase)
    _write_stable_time(
        paths["terminal"],
        {
            "schema_version": "pif_candidate_semantic_evaluation_phase_terminal_v1",
            "terminal_at": now_iso(),
            "phase": phase,
            "state": state,
            "records": dict(records),
            "production_mutated": False,
            "holdout_authorized": False,
        },
        "terminal_at",
    )


def _failure_terminal(root: Path, exc: BaseException) -> dict[str, Any]:
    accounting = _aggregate_usage(root)
    semantic_quality = isinstance(exc, CandidateSemanticQualityStop)
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "development_quality_failed" if semantic_quality else "failed",
        "terminal_reason": (
            "candidate_semantic_quality_gate_not_passed"
            if semantic_quality
            else "infrastructure_or_judge_attempt_failed"
        ),
        "error_class": type(exc).__name__,
        "error_message_sha256": hashlib.sha256(str(exc).encode("utf-8")).hexdigest(),
        "error_message_size_bytes": len(str(exc).encode("utf-8")),
        "semantic_retry_count": 0,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "overall_goal_complete": False,
        "goal_status_required": "active",
        **accounting,
    }
    for name in (
        "runtime-lock.json",
        "support/support-audit.json",
        "alignment/alignment-score.json",
    ):
        path = root / name
        if path.is_file():
            terminal[name.replace("/", "_").replace("-", "_").removesuffix(".json")] = (
                _record(path)
            )
    _write_stable_time(root / "terminal.json", terminal, "terminal_at")
    return terminal


def _freeze_alignment_phase(
    *,
    root: Path,
    support_bundle: Mapping[str, Any],
    support_private_score: Mapping[str, Any],
    support_receipts: Mapping[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Path]]]:
    alignment_bundle = evaluator.build_alignment_bundle(
        support_bundle=support_bundle,
        support_private_score=support_private_score,
        support_receipts=support_receipts,
    )
    phase_root = root / "alignment"
    bundle_path = phase_root / "alignment-bundle.private.json"
    mapping_path = phase_root / "origin-map.private.json"
    receipts_path = phase_root / "support-receipts.private.json"
    _write_immutable(bundle_path, alignment_bundle)
    _write_immutable(mapping_path, alignment_bundle["mapping"])
    _write_immutable(receipts_path, alignment_bundle["receipts"])
    turn_paths = []
    turn_names = []
    for turn in alignment_bundle["turns"]:
        turn_name = f"neutral_alignment_{turn['permutation']}"
        turn_names.append(turn_name)
        turn_paths.append(
            _freeze_turn(
                phase_root=phase_root,
                turn_name=turn_name,
                input_value=turn["value"],
                base_instructions=evaluator.alignment_instructions(),
                prompt=str(turn["prompt"]),
                schema=turn["schema"],
            )
        )
    direct_records = [
        _record(root / "support" / "terminal.json"),
        _record(root / "support" / "support-audit.json"),
        _record(root / "support" / "support-score.private.json"),
        _record(root / "support" / "support-receipts.private.json"),
        _record(bundle_path),
        _record(mapping_path),
        _record(receipts_path),
        *[
            _record(paths[key])
            for paths in turn_paths
            for key in ("input", "base", "prompt", "schema")
        ],
    ]
    _phase_lock(
        root=root,
        phase="alignment",
        phase_id=f"{support_bundle['evaluation_id']}_alignment",
        turn_names=turn_names,
        maximum_total_tokens_per_turn=ALIGNMENT_TOTAL_TOKEN_MAXIMUM,
        direct_records=direct_records,
    )
    return alignment_bundle, turn_paths


def _freeze_adjudication_phase(
    *,
    root: Path,
    adjudication_bundle: Mapping[str, Any],
) -> dict[str, Path]:
    turn = adjudication_bundle.get("turn")
    if not isinstance(turn, Mapping):
        raise CandidateEvaluationRunError("required adjudication has no frozen turn")
    phase_root = root / "adjudication"
    bundle_path = phase_root / "adjudication-bundle.private.json"
    _write_immutable(bundle_path, adjudication_bundle)
    paths = _freeze_turn(
        phase_root=phase_root,
        turn_name=str(turn["name"]),
        input_value={
            "packet": adjudication_bundle["packet"],
            "alignment_input": turn["value"],
        },
        base_instructions=str(turn["base_instructions"]),
        prompt=str(turn["prompt"]),
        schema=turn["schema"],
    )
    direct_records = [
        _record(root / "alignment" / "terminal.json"),
        _record(root / "alignment" / "alignment-bundle.private.json"),
        _record(root / "alignment" / "observable-disagreements.private.json"),
        _record(bundle_path),
        *[_record(paths[key]) for key in ("input", "base", "prompt", "schema")],
    ]
    _phase_lock(
        root=root,
        phase="adjudication",
        phase_id=f"{_load_json(root / 'evaluation-config.json', 'config')['evaluation_id']}_adjudication",
        turn_names=(str(turn["name"]),),
        maximum_total_tokens_per_turn=ADJUDICATION_TOTAL_TOKEN_MAXIMUM,
        direct_records=direct_records,
    )
    return paths


async def run_evaluation(
    config_path: Path,
    *,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[Path], Any] = _client_factory,
) -> dict[str, Any]:
    config_path = config_path.expanduser().resolve()
    root = config_path.parent
    if (root / "terminal.json").is_file():
        return _load_json(root / "terminal.json", "terminal")
    frozen = freeze_run(config_path)
    config = frozen["config"]
    support_bundle = _load_json(frozen["support_bundle"], "support bundle")
    try:
        verify_run_lock(frozen["runtime_lock"])
        verify_phase_lock(frozen["support_lock"])
        _phase_launch(root=root, phase="support", semantic_attempt_count=1)
        async with client_factory(root / "support" / "capacity-policy.json") as client:
            support_output, support_usage, support_adopted = await _run_turn(
                client=client,
                paths=frozen["support_turn"],
                model=evaluator.SUPPORT_MODEL,
                effort=evaluator.SUPPORT_EFFORT,
                maximum_total_tokens=SUPPORT_TOTAL_TOKEN_MAXIMUM,
                timeout_seconds=timeout_seconds,
            )
        support_audit, support_private_score, support_receipts = (
            evaluator.score_support_output(
                output=support_output,
                support_bundle=support_bundle,
            )
        )
        support_audit_path = root / "support" / "support-audit.json"
        support_score_path = root / "support" / "support-score.private.json"
        support_receipts_path = root / "support" / "support-receipts.private.json"
        _write_immutable(support_audit_path, support_audit)
        _write_immutable(support_score_path, support_private_score)
        _write_immutable(support_receipts_path, support_receipts)
        _phase_terminal(
            root=root,
            phase="support",
            state="passed_alignment_authorized",
            records={
                "capacity": _record(frozen["support_turn"]["capacity"]),
                "sidecar": _record(frozen["support_turn"]["sidecar"]),
                "output": _record(frozen["support_turn"]["output"]),
                "audit": _record(support_audit_path),
                "score": _record(support_score_path),
                "receipts": _record(support_receipts_path),
                "usage": support_usage,
                "checkpoint_adopted": support_adopted,
            },
        )
        if not support_audit["passed"]:
            raise CandidateSemanticQualityStop("support gate produced no alignable case")

        alignment_bundle, alignment_turns = _freeze_alignment_phase(
            root=root,
            support_bundle=support_bundle,
            support_private_score=support_private_score,
            support_receipts=support_receipts,
        )
        _phase_launch(root=root, phase="alignment", semantic_attempt_count=2)
        alignment_outputs = []
        alignment_usages = []
        alignment_adopted = []
        async with client_factory(root / "alignment" / "capacity-policy.json") as client:
            for paths in alignment_turns:
                output, usage, adopted = await _run_turn(
                    client=client,
                    paths=paths,
                    model=evaluator.ALIGNMENT_MODEL,
                    effort=evaluator.ALIGNMENT_EFFORT,
                    maximum_total_tokens=ALIGNMENT_TOTAL_TOKEN_MAXIMUM,
                    timeout_seconds=timeout_seconds,
                )
                alignment_outputs.append(output)
                alignment_usages.append(usage)
                alignment_adopted.append(adopted)
        base_output, canary_output = alignment_outputs
        adjudication_bundle = evaluator.build_adjudication_bundle(
            base_output=base_output,
            canary_output=canary_output,
            alignment_bundle=alignment_bundle,
        )
        disagreements_path = root / "alignment" / "observable-disagreements.private.json"
        _write_immutable(
            disagreements_path,
            {
                "schema_version": evaluator.BUNDLE_VERSION,
                "adjudication_required": adjudication_bundle["adjudication_required"],
                "packet": adjudication_bundle["packet"],
            },
        )
        _phase_terminal(
            root=root,
            phase="alignment",
            state=(
                "completed_adjudication_required"
                if adjudication_bundle["adjudication_required"]
                else "completed_no_adjudication_required"
            ),
            records={
                "capacities": [_record(paths["capacity"]) for paths in alignment_turns],
                "outputs": [_record(paths["output"]) for paths in alignment_turns],
                "sidecars": [_record(paths["sidecar"]) for paths in alignment_turns],
                "usages": alignment_usages,
                "checkpoint_adopted": alignment_adopted,
                "observable_disagreements": _record(disagreements_path),
            },
        )

        adjudication_output = None
        if adjudication_bundle["adjudication_required"]:
            adjudication_paths = _freeze_adjudication_phase(
                root=root,
                adjudication_bundle=adjudication_bundle,
            )
            _phase_launch(root=root, phase="adjudication", semantic_attempt_count=1)
            async with client_factory(
                root / "adjudication" / "capacity-policy.json"
            ) as client:
                adjudication_output, adjudication_usage, adjudication_adopted = (
                    await _run_turn(
                        client=client,
                        paths=adjudication_paths,
                        model=evaluator.ADJUDICATION_MODEL,
                        effort=evaluator.ADJUDICATION_EFFORT,
                        maximum_total_tokens=ADJUDICATION_TOTAL_TOKEN_MAXIMUM,
                        timeout_seconds=timeout_seconds,
                    )
                )
            _phase_terminal(
                root=root,
                phase="adjudication",
                state="completed",
                records={
                    "capacity": _record(adjudication_paths["capacity"]),
                    "sidecar": _record(adjudication_paths["sidecar"]),
                    "output": _record(adjudication_paths["output"]),
                    "usage": adjudication_usage,
                    "checkpoint_adopted": adjudication_adopted,
                },
            )

        score = evaluator.score_alignment(
            base_output=base_output,
            canary_output=canary_output,
            alignment_bundle=alignment_bundle,
            support_bundle=support_bundle,
            support_private_score=support_private_score,
            production_amortized_total_token_ratio=float(
                config["production_amortized_total_token_ratio"]
            ),
            adjudication_bundle=(
                adjudication_bundle
                if adjudication_bundle["adjudication_required"]
                else None
            ),
            adjudication_output=adjudication_output,
        )
        score_path = root / "alignment" / "alignment-score.json"
        _write_immutable(score_path, score)
        if not score["passed"]:
            raise CandidateSemanticQualityStop("candidate semantic quality gate failed")
        accounting = _aggregate_usage(root)
        if not accounting["accounting_complete"]:
            raise CandidateEvaluationRunError("candidate evaluator accounting is incomplete")
        winner_path = root / "development-winner.json"
        _write_immutable(
            winner_path,
            {
                "schema_version": "pif_candidate_semantic_evaluation_winner_v1",
                "frozen_at": now_iso(),
                "evaluation_id": config["evaluation_id"],
                "candidate": config["candidate"],
                "candidate_terminal": config["candidate_terminal"],
                "candidate_gate": config["candidate_gate"],
                "semantic_score": _record(score_path),
                "production_amortized_total_token_ratio": config[
                    "production_amortized_total_token_ratio"
                ],
                "development_winner_frozen": True,
                "holdout_authorized": True,
                "production_mutated": False,
            },
        )
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": "development_winner_frozen",
            "terminal_reason": "candidate_semantic_quality_and_cost_gates_passed_holdout_authorized",
            "semantic_retry_count": 0,
            "development_winner_frozen": True,
            "holdout_authorized": True,
            "production_mutated": False,
            "overall_goal_complete": False,
            "goal_status_required": "active",
            "runtime_lock": _record(root / "runtime-lock.json"),
            "score": _record(score_path),
            "winner": _record(winner_path),
            **accounting,
        }
        _write_stable_time(root / "terminal.json", terminal, "terminal_at")
        return terminal
    except BaseException as exc:
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
        return _failure_terminal(root, exc)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "verify", "run"))
    parser.add_argument("--config", required=True, type=Path)
    args = parser.parse_args(argv)
    if args.command == "freeze":
        frozen = freeze_run(args.config)
        print(_canonical_json({"root": str(frozen["root"]), "semantic_turns": 0}))
        return 0
    if args.command == "verify":
        verified = verify_run_lock(args.config.resolve().parent / "runtime-lock.json")
        print(_canonical_json({"runtime_lock": str(verified), "verified": True}))
        return 0
    terminal = asyncio.run(run_evaluation(args.config))
    print(_canonical_json(terminal))
    return 0 if terminal.get("development_winner_frozen") is True else 2


if __name__ == "__main__":
    raise SystemExit(main())
