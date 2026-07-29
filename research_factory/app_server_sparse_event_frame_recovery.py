from __future__ import annotations

"""One immutable schema-compatibility recovery for the sparse event-frame lane."""

import argparse
import asyncio
import copy
import hashlib
import json
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from . import app_server_configured_experiment as configured
from . import app_server_flat_request_model_lane as flat
from . import app_server_sparse_event_frame_lane as v1
from .app_server_runtime_verifier import (
    ContentHashCache,
    RuntimeVerificationError,
    closure_receipt,
    normalize_record,
    verify_runtime_lock,
)
from .labels import ValidationError, _validate_schema
from .util import now_iso


CONFIG_VERSION = "pif_sparse_event_frame_recovery_config_v1"
LOCK_VERSION = "pif_sparse_event_frame_recovery_runtime_lock_v1"
TERMINAL_VERSION = "pif_sparse_event_frame_recovery_terminal_v1"
TURN_NAME = v1.TURN_NAME
MODEL = v1.MODEL
EFFORT = v1.EFFORT
MAX_TOTAL_TOKENS = v1.MAX_TOTAL_TOKENS
ARCHITECTURE_ID = v1.ARCHITECTURE_ID
PROJECT_ROOT = v1.PROJECT_ROOT
PINNED_CODEX = v1.PINNED_CODEX
USAGE_FIELDS = v1.USAGE_FIELDS
PREDECESSOR_ROOT = v1.DEFAULT_OUTPUT_ROOT
DEFAULT_OUTPUT_ROOT = (
    v1.PIPELINE_ROOT
    / "development-canary-sparse-event-frame-luna-high-schema-compat-recovery-v2-2026-07-18"
).resolve()
LINEAGE_KEYS = (
    "predecessor_terminal",
    "predecessor_runtime_lock",
    "predecessor_capacity",
    "predecessor_sidecar",
    "predecessor_config",
    "architecture_decision",
    "source_input",
    "source_prompt",
    "source_base",
    "projection_schema",
    "recovery_audit",
)
UNSUPPORTED_PROVIDER_KEYWORDS = frozenset({"minLength", "uniqueItems"})


class SparseEventFrameRecoveryError(RuntimeError):
    pass


class SparseEventFrameRecoveryOutputError(SparseEventFrameRecoveryError):
    pass


class SparseEventFrameRecoveryStop(SparseEventFrameRecoveryError):
    pass


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SparseEventFrameRecoveryError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    configured._write_immutable(path, value)  # noqa: SLF001


def _write_private_text(path: Path, value: str) -> None:
    configured._write_private_text(path, value)  # noqa: SLF001


def _record(path: Path, *, cache: ContentHashCache | None = None) -> dict[str, Any]:
    return (cache or ContentHashCache()).record(path)


def _verify_record(record: Mapping[str, Any], *, cache: ContentHashCache) -> None:
    if not cache.verify_record(record):
        raise SparseEventFrameRecoveryError("frozen direct-lineage artifact drifted")


def _turn_paths(root: Path) -> dict[str, Path]:
    turn_root = root / "turns" / TURN_NAME.replace("_", "-")
    return {
        "root": turn_root,
        "input": turn_root / "input.private.json",
        "prompt": turn_root / "prompt.private.md",
        "base": turn_root / "base-instructions.private.md",
        "schema": turn_root / "schema.json",
        "projection_schema": turn_root / "projection-schema.json",
        "capacity": turn_root / "capacity.json",
        "sidecar": turn_root / "sidecar.json",
        "output": turn_root / "output.private.json",
    }


def _predecessor_paths() -> dict[str, Path]:
    turn = v1._turn_paths(PREDECESSOR_ROOT)  # noqa: SLF001
    return {
        "predecessor_terminal": PREDECESSOR_ROOT / "terminal.json",
        "predecessor_runtime_lock": PREDECESSOR_ROOT / "runtime-lock.json",
        "predecessor_capacity": turn["capacity"],
        "predecessor_sidecar": turn["sidecar"],
        "predecessor_config": PREDECESSOR_ROOT / "experiment-config.json",
        "architecture_decision": PREDECESSOR_ROOT / "architecture-decision.json",
        "source_input": turn["input"],
        "source_prompt": turn["prompt"],
        "source_base": turn["base"],
        "projection_schema": turn["projection_schema"],
    }


def _runtime_files() -> tuple[Path, ...]:
    return tuple(
        sorted(
            {
                Path(__file__).resolve(),
                Path(v1.__file__).resolve(),
                Path(flat.__file__).resolve(),
                *configured._runtime_files(),  # noqa: SLF001
            },
            key=str,
        )
    )


def _drop_provider_unsupported_keywords(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _drop_provider_unsupported_keywords(item)
            for key, item in value.items()
            if key not in UNSUPPORTED_PROVIDER_KEYWORDS
        }
    if isinstance(value, list):
        return [_drop_provider_unsupported_keywords(item) for item in value]
    return copy.deepcopy(value)


def build_provider_compatible_schema(projection_schema: Mapping[str, Any]) -> dict[str, Any]:
    return _drop_provider_unsupported_keywords(v1.build_sparse_schema(projection_schema))


def _validate_sparse_semantic_containers(event: Mapping[str, Any]) -> None:
    for field in v1.OPTIONAL_TEXT_FIELDS:
        item = v1._one(event[field], field)  # noqa: SLF001
        if item is not None and not str(item).strip():
            raise SparseEventFrameRecoveryOutputError(f"{field} empty present value")
    for field in v1.ENTITY_FIELDS:
        values = [str(item) for item in event[field]]
        if any(not item.strip() for item in values) or len(values) != len(set(values)):
            raise SparseEventFrameRecoveryOutputError(f"{field} identity contract drifted")
    metric = v1._one(event["metric"], "metric")  # noqa: SLF001
    if metric is not None and not str(metric["raw_text"]).strip():
        raise SparseEventFrameRecoveryOutputError("metric raw text is empty")


def validate_and_project_output(
    output: Mapping[str, Any], frozen: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    turn = frozen["turn"]
    try:
        _validate_schema(_load_json(turn["schema"], "recovery schema"), output, path="$")
    except (ValidationError, TypeError, ValueError) as exc:
        raise SparseEventFrameRecoveryOutputError("sparse output schema failed") from exc
    direct = copy.deepcopy(dict(output))
    for segment in direct.get("segments") or []:
        projected = []
        for event in segment.get("events") or []:
            _validate_sparse_semantic_containers(event)
            try:
                projected.append(v1.project_sparse_event(event))
            except v1.SparseEventFrameOutputError as exc:
                raise SparseEventFrameRecoveryOutputError(str(exc)) from exc
        segment["events"] = projected
    try:
        normalized, provenance, diagnostics = flat.validate_and_project_output(
            direct,
            {
                "turn": {"schema": turn["projection_schema"]},
                "source": frozen["source"],
            },
        )
    except flat.FlatRequestOutputError as exc:
        raise SparseEventFrameRecoveryOutputError(str(exc)) from exc
    diagnostics = copy.deepcopy(diagnostics)
    diagnostics.update(
        {
            "representation": "cardinality_typed_sparse_event_frames",
            "provider_schema_compatibility_recovery": True,
            "deterministic_semantic_decision_made": False,
            "all_semantic_decisions_owned_by_llm": True,
        }
    )
    return normalized, provenance, diagnostics


def prepare_recovery(root: Path = DEFAULT_OUTPUT_ROOT) -> Path:
    root = root.expanduser().resolve()
    paths = _predecessor_paths()
    v1.verify_frozen(PREDECESSOR_ROOT)
    terminal = _load_json(paths["predecessor_terminal"], "predecessor terminal")
    sidecar = _load_json(paths["predecessor_sidecar"], "predecessor sidecar")
    capacity = _load_json(paths["predecessor_capacity"], "predecessor capacity")
    schema = _load_json(
        PREDECESSOR_ROOT / "turns/source-complete-sparse-event-frames/schema.json",
        "predecessor schema",
    )
    schema_keywords: set[str] = set()

    def collect(value: Any) -> None:
        if isinstance(value, dict):
            schema_keywords.update(value)
            for item in value.values():
                collect(item)
        elif isinstance(value, list):
            for item in value:
                collect(item)

    collect(schema)
    if (
        terminal.get("terminal_reason") != "infrastructure_or_extraction_attempt_failed"
        or terminal.get("usage_status") != "unknown"
        or terminal.get("accounting_complete") is not False
        or terminal.get("semantic_attempt_count") != 1
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("production_mutated") is not False
        or sidecar.get("state") != "failed"
        or sidecar.get("error_class") != "turn_failed"
        or sidecar.get("usage_status") != "unknown"
        or sidecar.get("usage_complete") is not False
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("model") != MODEL
        or sidecar.get("effort") != EFFORT
        or capacity.get("cleared_for_semantic_turn") is not True
        or capacity.get("managed_chatgpt_auth_verified") is not True
        or capacity.get("rate_limit_reached_type") is not None
        or not UNSUPPORTED_PROVIDER_KEYWORDS.issubset(schema_keywords)
    ):
        raise SparseEventFrameRecoveryError("predecessor infrastructure evidence drifted")
    cache = ContentHashCache()
    records = {key: _record(path, cache=cache) for key, path in paths.items()}
    audit_path = root / "schema-compatibility-recovery-audit.json"
    configured._write_stable_time(  # noqa: SLF001
        audit_path,
        {
            "schema_version": "pif_sparse_event_frame_schema_compatibility_audit_v1",
            "created_at": now_iso(),
            "state": "one_infrastructure_recovery_authorized",
            "predecessor_usage_status": "unknown",
            "predecessor_turn_state": "failed",
            "predecessor_error_class": "turn_failed",
            "predecessor_wall_elapsed_seconds": sidecar.get("wall_elapsed_seconds"),
            "predecessor_turn_error": copy.deepcopy(sidecar.get("turn_error")),
            "predecessor_attempt_count": 1,
            "predecessor_retry_count": 0,
            "recovery_attempt_count": 1,
            "recovery_retry_count": 0,
            "hypothesis": "The immediate no-output turn failure was caused by minLength or uniqueItems keywords introduced by the sparse schema but absent from accepted predecessor Structured Outputs schemas.",
            "nonsemantic_schema_delta": {
                "removed_keywords": sorted(UNSUPPORTED_PROVIDER_KEYWORDS),
                "deterministic_post_output_contracts_retained": [
                    "present optional strings are nonempty",
                    "entity strings are nonempty and unique",
                    "metric raw text is nonempty and literal in exact evidence",
                ],
            },
            "semantic_request_invariants": {
                "input_bytes_identical": True,
                "prompt_bytes_identical": True,
                "base_instructions_bytes_identical": True,
                "model": MODEL,
                "effort": EFFORT,
                "maximum_total_tokens": MAX_TOTAL_TOKENS,
                "retry_count": 0,
            },
            "on_any_failure": "freeze this recovery and reject the sparse-frame lane; no further infrastructure recovery",
            "production_mutated": False,
            "holdout_authorized": False,
            "predecessor_records": records,
        },
        "created_at",
    )
    config = {
        "schema_version": CONFIG_VERSION,
        "experiment_id": "sparse_event_frame_luna_high_schema_compat_recovery_v2_2026_07_18",
        "architecture_id": ARCHITECTURE_ID,
        "recovery_version": 2,
        "output_root": str(root),
        "model": MODEL,
        "effort": EFFORT,
        "declared_turn_count": 1,
        "retry_count": 0,
        "maximum_total_tokens": MAX_TOTAL_TOKENS,
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "semantic_regex_or_keyword_filtering": False,
        "production_mutation_allowed": False,
        "holdout_authorized": False,
        "minimum_remaining_reserve_percent": v1.MINIMUM_REMAINING_RESERVE_PERCENT,
        "quota_points_per_million_tokens": v1.QUOTA_POINTS_PER_MILLION_TOKENS,
        "production_amortized_context_tokens": v1.PRODUCTION_CONTEXT_TOKENS,
        "production_scale": v1.PRODUCTION_SCALE,
        "baseline_total_tokens": v1.BASELINE_TOTAL_TOKENS,
        **records,
        "recovery_audit": _record(audit_path, cache=cache),
    }
    config_path = root / "experiment-config.json"
    _write_immutable(config_path, config)
    return config_path


def _validate_config(config_path: Path) -> dict[str, Any]:
    value = _load_json(config_path, "recovery config")
    if (
        not isinstance(value, dict)
        or value.get("schema_version") != CONFIG_VERSION
        or value.get("architecture_id") != ARCHITECTURE_ID
        or value.get("recovery_version") != 2
        or value.get("model") != MODEL
        or value.get("effort") != EFFORT
        or value.get("declared_turn_count") != 1
        or value.get("retry_count") != 0
        or value.get("maximum_total_tokens") != MAX_TOTAL_TOKENS
        or value.get("managed_chatgpt_auth_only") is not True
        or value.get("official_persistent_codex_app_server_only") is not True
        or value.get("semantic_regex_or_keyword_filtering") is not False
        or value.get("production_mutation_allowed") is not False
        or value.get("holdout_authorized") is not False
        or value.get("minimum_remaining_reserve_percent")
        != v1.MINIMUM_REMAINING_RESERVE_PERCENT
        or value.get("quota_points_per_million_tokens")
        != v1.QUOTA_POINTS_PER_MILLION_TOKENS
        or value.get("production_amortized_context_tokens")
        != v1.PRODUCTION_CONTEXT_TOKENS
        or value.get("production_scale") != v1.PRODUCTION_SCALE
        or value.get("baseline_total_tokens") != v1.BASELINE_TOTAL_TOKENS
    ):
        raise SparseEventFrameRecoveryError("recovery config contract drifted")
    if Path(str(value.get("output_root") or "")).expanduser().resolve() != config_path.parent.resolve():
        raise SparseEventFrameRecoveryError("recovery output root drifted")
    for key in LINEAGE_KEYS:
        normalize_record(value.get(key) or {})
    return value


def freeze_recovery(config_path: Path) -> dict[str, Any]:
    config_path = config_path.expanduser().resolve()
    config = _validate_config(config_path)
    root = config_path.parent
    if (root / "runtime-lock.json").is_file():
        return verify_frozen(root)
    cache = ContentHashCache()
    for key in LINEAGE_KEYS:
        _verify_record(config[key], cache=cache)
    turn = _turn_paths(root)
    for destination, key in (
        ("input", "source_input"),
        ("prompt", "source_prompt"),
        ("base", "source_base"),
        ("projection_schema", "projection_schema"),
    ):
        source = Path(str(config[key]["path"]))
        if destination in {"input", "prompt", "base"}:
            _write_private_text(turn[destination], source.read_text(encoding="utf-8"))
        else:
            _write_immutable(turn[destination], _load_json(source, key))
        if _record(turn[destination], cache=cache)["sha256"] != config[key]["sha256"]:
            raise SparseEventFrameRecoveryError("semantic request bytes changed")
    projection_schema = _load_json(turn["projection_schema"], "projection schema")
    schema = build_provider_compatible_schema(projection_schema)
    _write_immutable(turn["schema"], schema)
    capacity = v1._capacity_policy(root, config)  # noqa: SLF001
    lock = {
        "schema_version": LOCK_VERSION,
        "frozen_at": now_iso(),
        "experiment_id": config["experiment_id"],
        "architecture_id": ARCHITECTURE_ID,
        "recovery_version": 2,
        "model": MODEL,
        "effort": EFFORT,
        "declared_turn_count": 1,
        "retry_count": 0,
        "maximum_total_tokens": MAX_TOTAL_TOKENS,
        "managed_chatgpt_auth_only": True,
        "semantic_regex_or_keyword_filtering": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "pinned_codex_cli": _record(PINNED_CODEX, cache=cache),
        "runtime_files": [_record(path, cache=cache) for path in _runtime_files()],
        "config": _record(config_path, cache=cache),
        "direct_lineage": [copy.deepcopy(config[key]) for key in LINEAGE_KEYS],
        "capacity_audit": _record(capacity["audit"], cache=cache),
        "capacity_policy": _record(capacity["policy"], cache=cache),
        "frozen_request": [
            _record(turn[name], cache=cache)
            for name in ("input", "prompt", "base", "schema", "projection_schema")
        ],
    }
    lock_path = root / "runtime-lock.json"
    configured._write_stable_time(lock_path, lock, "frozen_at")  # noqa: SLF001
    _write_immutable(
        root / "runtime-lock-closure.json",
        closure_receipt(lock_path, cache=ContentHashCache()),
    )
    return verify_frozen(root)


def verify_frozen(root: Path) -> dict[str, Any]:
    root = root.expanduser().resolve()
    config = _validate_config(root / "experiment-config.json")
    receipt = _load_json(root / "runtime-lock-closure.json", "runtime closure")
    try:
        result = verify_runtime_lock(
            root / "runtime-lock.json",
            cache=ContentHashCache(),
            expected_manifest_record=receipt.get("manifest"),
            expected_closure_digest=receipt.get("closure_digest"),
            required_fields={
                "schema_version": LOCK_VERSION,
                "experiment_id": config["experiment_id"],
                "architecture_id": ARCHITECTURE_ID,
                "recovery_version": 2,
                "model": MODEL,
                "effort": EFFORT,
                "declared_turn_count": 1,
                "retry_count": 0,
                "production_mutation_allowed": False,
            },
            required_record_paths=_runtime_files(),
        )
    except RuntimeVerificationError as exc:
        raise SparseEventFrameRecoveryError("runtime lock verification failed") from exc
    if result.manifest.get("pinned_codex_cli") != _record(PINNED_CODEX):
        raise SparseEventFrameRecoveryError("pinned Codex binary drifted")
    configured.reserve_module.load_reserve_capacity_policy(root / "capacity-policy.json")
    cache = ContentHashCache()
    for key in LINEAGE_KEYS:
        _verify_record(config[key], cache=cache)
    launch = root / "launch-receipt.json"
    if launch.exists():
        value = _load_json(launch, "launch receipt")
        for key in ("runtime_lock", "runtime_closure", "config"):
            _verify_record(value.get(key) or {}, cache=cache)
        if value.get("semantic_attempt_count") != 1 or value.get("retry_count") != 0:
            raise SparseEventFrameRecoveryError("launch receipt contract drifted")
    return {
        "root": root,
        "config": config,
        "runtime_lock": root / "runtime-lock.json",
        "runtime_closure": root / "runtime-lock-closure.json",
        "capacity_policy": root / "capacity-policy.json",
        "turn": _turn_paths(root),
        "source": _load_json(Path(config["source_input"]["path"]), "source input"),
    }


def _failure_terminal(
    root: Path, frozen: Mapping[str, Any], exc: BaseException
) -> dict[str, Any]:
    turn = frozen["turn"]
    measured: dict[str, int] | None = None
    unknown = 0
    if turn["sidecar"].exists():
        try:
            measured = v1._usage_from_sidecar(turn["sidecar"])  # noqa: SLF001
        except Exception:
            unknown = 1
    elif turn["capacity"].exists() or turn["output"].exists():
        unknown = 1
    semantic = isinstance(
        exc, (SparseEventFrameRecoveryOutputError, SparseEventFrameRecoveryStop)
    )
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "inactive_incomplete_recovery_required",
        "terminal_reason": (
            "sparse_event_frame_recovery_structural_or_cost_gate_not_passed"
            if semantic
            else "infrastructure_or_extraction_attempt_failed"
        ),
        "error_class": type(exc).__name__,
        "error_message_sha256": hashlib.sha256(message).hexdigest(),
        "error_message_bytes": len(message),
        "semantic_attempt_count": int(measured is not None) + unknown,
        "semantic_retry_count": 0,
        "usage_status": "unknown" if unknown else "complete",
        "accounting_complete": unknown == 0,
        "usage": measured or {field: 0 for field in USAGE_FIELDS},
        "unknown_usage_attempt_count": unknown,
        "predecessor_unknown_usage_attempt_count": 1,
        "architecture_strategy_rejected": True,
        "further_infrastructure_recovery_authorized": False,
        "support_alignment_authorized": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "overall_goal_complete": False,
        "goal_status_required": "active",
        "runtime_lock": _record(frozen["runtime_lock"]),
        "exact_next_action": "freeze and reject the sparse-frame lane; no repair, retry, or further infrastructure recovery",
    }
    configured._write_stable_time(root / "terminal.json", terminal, "terminal_at")  # noqa: SLF001
    return terminal


async def run_recovery(
    config_path: Path,
    *,
    timeout_seconds: float = 1200.0,
    client_factory: Callable[[Path], Any] = v1._client_factory,  # noqa: SLF001
) -> dict[str, Any]:
    root = config_path.expanduser().resolve().parent
    if (root / "terminal.json").is_file():
        return _load_json(root / "terminal.json", "terminal")
    frozen = freeze_recovery(config_path)
    try:
        verify_frozen(root)
        launch = root / "launch-receipt.json"
        if not launch.exists():
            configured._write_stable_time(  # noqa: SLF001
                launch,
                {
                    "schema_version": "pif_sparse_event_frame_recovery_launch_v1",
                    "launched_at": now_iso(),
                    "semantic_attempt_count": 1,
                    "retry_count": 0,
                    "managed_chatgpt_auth_only": True,
                    "holdout_authorized": False,
                    "production_mutation_allowed": False,
                    "runtime_lock": _record(frozen["runtime_lock"]),
                    "runtime_closure": _record(frozen["runtime_closure"]),
                    "config": _record(config_path),
                },
                "launched_at",
            )
        verify_frozen(root)
        turn = frozen["turn"]
        if not turn["sidecar"].exists() and (
            turn["capacity"].exists() or turn["output"].exists()
        ):
            raise SparseEventFrameRecoveryError("attempt artifact exists without sidecar")
        async with client_factory(frozen["capacity_policy"]) as client:
            if not turn["sidecar"].exists():
                await client.run_ephemeral_structured_turn(
                    model=MODEL,
                    effort=EFFORT,
                    base_instructions=turn["base"].read_text(encoding="utf-8"),
                    prompt=turn["prompt"].read_text(encoding="utf-8"),
                    output_schema=_load_json(turn["schema"], "recovery structured schema"),
                    cwd=PROJECT_ROOT,
                    sidecar_path=turn["sidecar"],
                    output_path=turn["output"],
                    batch_size=2,
                    thread_mode="new_thread",
                    timeout_seconds=timeout_seconds,
                    capacity_checkpoint_path=turn["capacity"],
                )
        usage = v1._usage_from_sidecar(turn["sidecar"])  # noqa: SLF001
        if usage["total_tokens"] > MAX_TOTAL_TOKENS:
            raise SparseEventFrameRecoveryStop("turn exceeded frozen token ceiling")
        output = _load_json(turn["output"], "recovery structured output")
        normalized, provenance, diagnostics = validate_and_project_output(output, frozen)
        _write_immutable(root / "normalized-output.private.json", normalized)
        _write_immutable(root / "evidence-provenance.private.json", provenance)
        _write_immutable(root / "diagnostics.private.json", diagnostics)
        gate = v1._gate(usage, diagnostics)  # noqa: SLF001
        gate["schema_version"] = "pif_sparse_event_frame_recovery_structural_gate_v1"
        gate["predecessor_unknown_usage_attempt_count"] = 1
        gate_path = root / "architecture-structural-gate.json"
        _write_immutable(gate_path, gate)
        if not gate["passed"]:
            raise SparseEventFrameRecoveryStop("structural or production-cost gate failed")
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": "sparse_event_frame_recovery_structural_cost_passed",
            "terminal_reason": "sparse_event_frame_recovery_structural_cost_passed_support_alignment_required",
            "semantic_attempt_count": 1,
            "semantic_retry_count": 0,
            "usage_status": "complete",
            "accounting_complete": True,
            "usage": usage,
            "predecessor_unknown_usage_attempt_count": 1,
            "production_amortized_total_token_ratio": gate[
                "production_amortized_total_token_ratio"
            ],
            "support_alignment_authorized": True,
            "development_winner_frozen": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "overall_goal_complete": False,
            "goal_status_required": "active",
            "further_infrastructure_recovery_authorized": False,
            "gate": _record(gate_path),
            "runtime_lock": _record(frozen["runtime_lock"]),
            "sidecar": _record(turn["sidecar"]),
            "normalized_output": _record(root / "normalized-output.private.json"),
            "evidence_provenance": _record(root / "evidence-provenance.private.json"),
            "exact_next_action": "run the frozen source-support and neutral alignment evaluator",
        }
        configured._write_stable_time(root / "terminal.json", terminal, "terminal_at")  # noqa: SLF001
        return terminal
    except BaseException as exc:
        if (root / "terminal.json").exists():
            return _load_json(root / "terminal.json", "terminal")
        return _failure_terminal(root, frozen, exc)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("prepare", "freeze", "verify", "run"))
    parser.add_argument("--root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--config", type=Path)
    args = parser.parse_args(argv)
    if args.command == "prepare":
        path = prepare_recovery(args.root)
        print(json.dumps({"config": str(path), "prepared": True}, sort_keys=True))
        return 0
    config = args.config or (args.root / "experiment-config.json")
    if args.command == "freeze":
        frozen = freeze_recovery(config)
        print(json.dumps({"root": str(frozen["root"]), "frozen": True}, sort_keys=True))
        return 0
    if args.command == "verify":
        frozen = verify_frozen(config.expanduser().resolve().parent)
        print(json.dumps({"root": str(frozen["root"]), "verified": True}, sort_keys=True))
        return 0
    terminal = asyncio.run(run_recovery(config))
    print(json.dumps(terminal, ensure_ascii=True, sort_keys=True, separators=(",", ":")))
    return 0 if terminal.get("support_alignment_authorized") else 2


if __name__ == "__main__":
    raise SystemExit(main())
