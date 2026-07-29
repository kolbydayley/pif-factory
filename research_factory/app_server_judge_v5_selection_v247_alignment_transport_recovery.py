from __future__ import annotations

"""Recover the byte-identical v246 alignment attempt after a pre-thread API mismatch."""

import argparse
import asyncio
import hashlib
import json
import math
import time
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_capacity as capacity
from . import app_server_capacity_reserve as reserve
from . import app_server_judge_v5 as judge
from . import app_server_judge_v5_selection_v244_source_indexed_graph as v244
from . import app_server_judge_v5_selection_v245_frozen_support as v245
from . import app_server_judge_v5_selection_v246_frozen_alignment as v246
from . import codex_app_server
from . import labels as labels_module
from . import util as util_module
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .util import now_iso


SCHEMA_VERSION = "pif_app_server_judge_v5_selection_v247_alignment_transport_recovery_v1"
RUNTIME_LOCK_VERSION = "pif_app_server_judge_v5_selection_v247_runtime_lock_v1"
TERMINAL_VERSION = "pif_app_server_judge_v5_selection_v247_terminal_v1"
WINNER_VERSION = "pif_app_server_judge_v5_selection_v247_winner_v1"
PHASE_ID = "judge_v5_4_selection_v247_alignment_transport_recovery"
TURN_NAMES = (
    "v247_frozen_alignment_base",
    "v247_frozen_alignment_balanced_canary",
)
PERMUTATIONS = v246.PERMUTATIONS
MODEL = v246.MODEL
EFFORT = v246.EFFORT
MAX_TOTAL_TOKENS_PER_TURN = v246.MAX_TOTAL_TOKENS_PER_TURN
MAX_PROMPT_BYTES = v246.MAX_PROMPT_BYTES
MAX_SCHEMA_BYTES = v246.MAX_SCHEMA_BYTES
TIMEOUT_SECONDS = v246.TIMEOUT_SECONDS
MIN_REMAINING_RESERVE_PERCENT = v246.MIN_REMAINING_RESERVE_PERCENT
QUOTA_POINTS_PER_MILLION_TOKENS = v246.QUOTA_POINTS_PER_MILLION_TOKENS
EXPECTED_ALIGNMENT_WITNESSES = v246.EXPECTED_ALIGNMENT_WITNESSES
OBSERVED_PRODUCTION_RATIO = v246.OBSERVED_PRODUCTION_RATIO
USAGE_FIELDS = v246.USAGE_FIELDS
PROJECT_ROOT = v246.PROJECT_ROOT
PIPELINE_ROOT = v246.PIPELINE_ROOT
V246_ROOT = v246.DEFAULT_OUTPUT_ROOT
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v247-alignment-transport-recovery"
).resolve()
PINNED_CODEX_0_144_1 = v246.PINNED_CODEX_0_144_1
V246_TYPE_ERROR_MESSAGE = (
    "run_ephemeral_structured_turn() got an unexpected keyword argument "
    "'output_validator'"
)
V246_TYPE_ERROR_SHA256 = hashlib.sha256(V246_TYPE_ERROR_MESSAGE.encode("utf-8")).hexdigest()


class V247AlignmentTransportRecoveryError(RuntimeError):
    """The additive v247 transport recovery cannot proceed or be adopted safely."""


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise V247AlignmentTransportRecoveryError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise V247AlignmentTransportRecoveryError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_private_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise V247AlignmentTransportRecoveryError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _record(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    data = resolved.read_bytes()
    return {
        "path": str(resolved),
        "sha256": hashlib.sha256(data).hexdigest(),
        "size_bytes": len(data),
    }


def _verify_record(record: Mapping[str, Any]) -> bool:
    try:
        return _record(Path(str(record["path"]))) == dict(record)
    except (KeyError, OSError, TypeError, ValueError):
        return False


def _v246_turn_paths(turn_name: str) -> dict[str, Path]:
    return v246._turn_paths(V246_ROOT, turn_name)


def _validate_v246_failure() -> dict[str, Any]:
    v246.verify_runtime_lock(V246_ROOT / "runtime-lock.json")
    terminal_path = V246_ROOT / "terminal.json"
    launch_path = V246_ROOT / "launch-receipt.json"
    terminal = _load_json(terminal_path, "v246 terminal")
    base_paths = _v246_turn_paths(v246.TURN_NAMES[0])
    canary_paths = _v246_turn_paths(v246.TURN_NAMES[1])
    capacity_path = base_paths["capacity"]
    checkpoint = _load_json(capacity_path, "v246 capacity checkpoint")
    if (
        terminal.get("state") != "inactive_incomplete_recovery_required"
        or terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("error_class") != "TypeError"
        or terminal.get("error_message_sha256") != V246_TYPE_ERROR_SHA256
        or terminal.get("error_message_bytes") != len(V246_TYPE_ERROR_MESSAGE.encode("utf-8"))
        or terminal.get("attempted_turn_count") != 1
        or terminal.get("measured_turn_count") != 0
        or terminal.get("unknown_usage_turn_count") != 1
        or terminal.get("usage_status") != "unknown"
        or terminal.get("accounting_complete") is not False
        or terminal.get("production_mutated") is not False
        or checkpoint.get("cleared_for_semantic_turn") is not True
        or checkpoint.get("managed_chatgpt_auth_verified") is not True
        or checkpoint.get("rate_limit_reached_type") is not None
        or checkpoint.get("thread_started") is not False
        or checkpoint.get("turn_started") is not False
        or checkpoint.get("sidecar_started") is not False
        or base_paths["sidecar"].exists()
        or base_paths["output"].exists()
        or base_paths["normalized"].exists()
        or canary_paths["capacity"].exists()
        or canary_paths["sidecar"].exists()
        or canary_paths["output"].exists()
        or not launch_path.is_file()
    ):
        raise V247AlignmentTransportRecoveryError("v246 pre-thread failure contract drifted")
    request_records = []
    for turn_name in v246.TURN_NAMES:
        paths = _v246_turn_paths(turn_name)
        request_records.extend(_record(paths[name]) for name in ("input", "prompt", "schema"))
    direct_paths = {
        "v246_terminal": terminal_path,
        "v246_runtime_lock": V246_ROOT / "runtime-lock.json",
        "v246_launch": launch_path,
        "v246_capacity": capacity_path,
        "v244_terminal": v244.DEFAULT_OUTPUT_ROOT / "terminal.json",
        "v244_gate": v244.DEFAULT_OUTPUT_ROOT / "architecture-structural-gate.json",
        "v244_normalized": next(v244.DEFAULT_OUTPUT_ROOT.glob("turns/*/normalized-output.private.json")),
        "v245_terminal": v245.DEFAULT_OUTPUT_ROOT / "terminal.json",
        "v245_support_receipts": v245.DEFAULT_OUTPUT_ROOT / "support-receipts.private.json",
        "v245_support_score": v245.DEFAULT_OUTPUT_ROOT / "support-score.private.json",
        "v245_support_audit": v245.DEFAULT_OUTPUT_ROOT / "support-audit.json",
    }
    return {
        "terminal": terminal,
        "checkpoint": checkpoint,
        "records": {name: _record(path) for name, path in direct_paths.items()},
        "request_records": request_records,
    }


def _turn_paths(root: Path, turn_name: str) -> dict[str, Path]:
    turn_root = root / "turns" / turn_name.replace("_", "-")
    return {
        "root": turn_root,
        "input": turn_root / "input.private.json",
        "prompt": turn_root / "prompt.private.md",
        "schema": turn_root / "schema.json",
        "capacity": turn_root / "capacity.json",
        "sidecar": turn_root / "sidecar.json",
        "output": turn_root / "output.private.json",
        "normalized": turn_root / "normalized.private.json",
    }


def build_recovery_bundle() -> dict[str, Any]:
    if not len(TURN_NAMES) == len(PERMUTATIONS) == len(v246.TURN_NAMES) == 2:
        raise V247AlignmentTransportRecoveryError("v247 turn plan drifted")
    turns = []
    for turn_name, permutation, predecessor_turn in zip(
        TURN_NAMES, PERMUTATIONS, v246.TURN_NAMES
    ):
        source_paths = _v246_turn_paths(predecessor_turn)
        value = _load_json(source_paths["input"], f"{predecessor_turn} input")
        prompt = source_paths["prompt"].read_text(encoding="utf-8")
        schema = _load_json(source_paths["schema"], f"{predecessor_turn} schema")
        prompt_bytes = len(prompt.encode("utf-8"))
        schema_bytes = len(
            json.dumps(schema, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode(
                "utf-8"
            )
        )
        if prompt_bytes > MAX_PROMPT_BYTES or schema_bytes > MAX_SCHEMA_BYTES:
            raise V247AlignmentTransportRecoveryError("v247 frozen request cap drifted")
        turns.append(
            {
                "turn_name": turn_name,
                "permutation": permutation,
                "predecessor_turn_name": predecessor_turn,
                "value": value,
                "prompt": prompt,
                "schema": schema,
                "prompt_bytes": prompt_bytes,
                "schema_bytes": schema_bytes,
                "source_records": {
                    name: _record(source_paths[name]) for name in ("input", "prompt", "schema")
                },
            }
        )
    base_ids = [
        row["witness_id"] for row in turns[0]["value"]["cases"][0]["witnesses"]
    ]
    canary_ids = [
        row["witness_id"] for row in turns[1]["value"]["cases"][0]["witnesses"]
    ]
    if canary_ids != list(reversed(base_ids)) or len(base_ids) != EXPECTED_ALIGNMENT_WITNESSES:
        raise V247AlignmentTransportRecoveryError("v247 permutation membership drifted")
    return {
        "turns": turns,
        "mapping": _load_json(V246_ROOT / "origin-map.private.json", "v246 origin map"),
        "instructions": (V246_ROOT / "alignment-instructions.private.md").read_text(
            encoding="utf-8"
        ),
    }


def _capacity_policy(root: Path) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAX_TOTAL_TOKENS_PER_TURN
    projected = math.ceil(bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000)
    audit = {
        "schema_version": "pif_app_server_capacity_policy_audit_v20",
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "measured_basis": {
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAX_TOTAL_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
            "projected_phase_quota_points": projected,
            "minimum_remaining_reserve_percent": MIN_REMAINING_RESERVE_PERCENT,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": "pif_app_server_capacity_policy_v20",
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "retry_count_per_turn": 0,
        "production_mutation_allowed": False,
        "rate_limit_reached_type_must_be_null": True,
        "unknown_usage_hard_stop": True,
        "ordered_turn_names": list(TURN_NAMES),
        "minimum_remaining_reserve_percent": MIN_REMAINING_RESERVE_PERCENT,
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "maximum_total_tokens_per_turn": MAX_TOTAL_TOKENS_PER_TURN,
        "phase_total_token_bound": bound,
        "projected_phase_quota_points": projected,
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    reserve.load_reserve_capacity_policy(policy_path)
    return {"audit": audit_path, "policy": policy_path}


def _runtime_files() -> tuple[Path, ...]:
    return tuple(
        sorted(
            {
                *v246._runtime_files(),
                Path(__file__).resolve(),
                Path(v246.__file__).resolve(),
                Path(v245.__file__).resolve(),
                Path(v244.__file__).resolve(),
                Path(judge.__file__).resolve(),
                Path(capacity.__file__).resolve(),
                Path(reserve.__file__).resolve(),
                Path(codex_app_server.__file__).resolve(),
                Path(labels_module.__file__).resolve(),
                Path(util_module.__file__).resolve(),
                codex_app_server.PROTOCOL_SCHEMA_PATH.resolve(),
            },
            key=str,
        )
    )


def _request_records(root: Path) -> list[dict[str, Any]]:
    records = []
    for turn_name in TURN_NAMES:
        paths = _turn_paths(root, turn_name)
        records.extend(_record(paths[name]) for name in ("input", "prompt", "schema"))
    return records


def verify_runtime_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path, "v247 runtime lock")
    root = path.parent.resolve()
    if (
        lock.get("schema_version") != RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or lock.get("model") != MODEL
        or lock.get("effort") != EFFORT
        or lock.get("declared_turn_count") != 2
        or lock.get("retry_count") != 0
        or lock.get("semantic_prompt_schema_or_model_changed") is not False
        or lock.get("unsupported_output_validator_kwarg_removed") is not True
        or lock.get("holdout_authorized_before_score") is not False
        or lock.get("production_mutation_allowed") is not False
        or {str(Path(row["path"]).resolve()) for row in lock.get("runtime_files") or []}
        != {str(value) for value in _runtime_files()}
        or {row["path"] for row in lock.get("frozen_requests") or []}
        != {row["path"] for row in _request_records(root)}
        or lock.get("pinned_codex_cli") != _record(PINNED_CODEX_0_144_1)
    ):
        raise V247AlignmentTransportRecoveryError("v247 runtime lock contract drifted")
    records = [
        lock.get("pinned_codex_cli"),
        *(lock.get("runtime_files") or []),
        *(lock.get("direct_lineage") or []),
        *(lock.get("source_requests") or []),
        lock.get("failure_audit"),
        lock.get("design"),
        lock.get("spec"),
        lock.get("capacity_audit"),
        lock.get("capacity_policy"),
        lock.get("mapping"),
        lock.get("instructions"),
        *(lock.get("frozen_requests") or []),
    ]
    if any(not _verify_record(row or {}) for row in records):
        raise V247AlignmentTransportRecoveryError("v247 runtime lock record drifted")
    predecessor = _validate_v246_failure()
    if {row["path"] for row in lock["direct_lineage"]} != {
        row["path"] for row in predecessor["records"].values()
    }:
        raise V247AlignmentTransportRecoveryError("v247 direct lineage set drifted")
    if {row["path"] for row in lock["source_requests"]} != {
        row["path"] for row in predecessor["request_records"]
    }:
        raise V247AlignmentTransportRecoveryError("v247 source request set drifted")
    reserve.load_reserve_capacity_policy(Path(lock["capacity_policy"]["path"]))
    return lock


def _load_frozen(root: Path) -> dict[str, Any]:
    turns = []
    for turn_name, permutation in zip(TURN_NAMES, PERMUTATIONS):
        paths = _turn_paths(root, turn_name)
        turns.append(
            {
                "turn_name": turn_name,
                "permutation": permutation,
                "value": _load_json(paths["input"], f"{turn_name} input"),
                "prompt": paths["prompt"].read_text(encoding="utf-8"),
                "schema": _load_json(paths["schema"], f"{turn_name} schema"),
                "paths": paths,
            }
        )
    spec_path = root / "attempt-spec.json"
    return {
        "root": root,
        "spec_path": spec_path,
        "spec": _load_json(spec_path, "v247 spec"),
        "runtime_lock": root / "runtime-lock.json",
        "capacity_policy": root / "capacity-policy.json",
        "bundle": {
            "mapping": _load_json(root / "origin-map.private.json", "v247 origin map"),
            "instructions": (root / "alignment-instructions.private.md").read_text(
                encoding="utf-8"
            ),
            "turns": turns,
        },
    }


def freeze_v247(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v247 terminal")}
    if any(root.iterdir()):
        if not (root / "runtime-lock.json").is_file():
            raise V247AlignmentTransportRecoveryError("unfinished v247 root is not replayable")
        verify_runtime_lock(root / "runtime-lock.json")
        return _load_frozen(root)
    predecessor = _validate_v246_failure()
    bundle = build_recovery_bundle()
    failure_audit_path = root / "v246-infrastructure-failure-audit.json"
    _write_stable_time(
        failure_audit_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "classification": "pre_thread_local_transport_interface_failure",
            "error_class": "TypeError",
            "error_message_sha256": V246_TYPE_ERROR_SHA256,
            "error_message_bytes": len(V246_TYPE_ERROR_MESSAGE.encode("utf-8")),
            "capacity_cleared": True,
            "thread_started": False,
            "turn_started": False,
            "sidecar_started": False,
            "semantic_output_created": False,
            "semantic_usage_status": "not_started_but_predecessor_itt_remains_unknown",
            "root_cause": "unsupported_output_validator_keyword_on_base_transport",
            "recovery_delta": "omit_transport_keyword_and_validate_returned_output_post_turn",
            "semantic_prompt_schema_model_or_scoring_changed": False,
            "v246_terminal": predecessor["records"]["v246_terminal"],
            "v246_capacity": predecessor["records"]["v246_capacity"],
            "production_mutated": False,
        },
        "created_at",
    )
    capacity_paths = _capacity_policy(root)
    design_path = root / "alignment-design.json"
    _write_stable_time(
        design_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "strategy": "byte_identical_v246_alignment_transport_recovery",
            "model": MODEL,
            "effort": EFFORT,
            "prompt_schema_model_scoring_changed": False,
            "unsupported_output_validator_kwarg_removed": True,
            "post_return_schema_and_protocol_validation_required": True,
            "retry_count": 0,
            "production_amortized_total_token_ratio": OBSERVED_PRODUCTION_RATIO,
            "holdout_authorized_before_score": False,
            "production_mutation_allowed": False,
        },
        "created_at",
    )
    mapping_path = root / "origin-map.private.json"
    instructions_path = root / "alignment-instructions.private.md"
    _write_immutable(mapping_path, bundle["mapping"])
    _write_private_text(instructions_path, bundle["instructions"])
    for turn in bundle["turns"]:
        paths = _turn_paths(root, turn["turn_name"])
        paths["root"].mkdir(parents=True, exist_ok=True)
        _write_immutable(paths["input"], turn["value"])
        _write_private_text(paths["prompt"], turn["prompt"])
        _write_immutable(paths["schema"], turn["schema"])
        for name in ("input", "prompt", "schema"):
            if _record(paths[name])["sha256"] != turn["source_records"][name]["sha256"]:
                raise V247AlignmentTransportRecoveryError("v247 request bytes differ from v246")
    spec_path = root / "attempt-spec.json"
    _write_stable_time(
        spec_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "phase_id": PHASE_ID,
            "state": "frozen_before_byte_identical_two_turn_alignment_recovery",
            "declared_turn_count": 2,
            "turn_names": list(TURN_NAMES),
            "permutations": list(PERMUTATIONS),
            "model": MODEL,
            "effort": EFFORT,
            "retry_count": 0,
            "alignment_witness_count": EXPECTED_ALIGNMENT_WITNESSES,
            "semantic_prompt_schema_or_model_changed": False,
            "transport_delta": "output_validator_removed_post_return_validation_retained",
            "holdout_authorized_before_score": False,
            "production_mutation_allowed": False,
        },
        "created_at",
    )
    lock_path = root / "runtime-lock.json"
    lock = {
        "schema_version": RUNTIME_LOCK_VERSION,
        "frozen_at": now_iso(),
        "phase_id": PHASE_ID,
        "model": MODEL,
        "effort": EFFORT,
        "declared_turn_count": 2,
        "retry_count": 0,
        "semantic_prompt_schema_or_model_changed": False,
        "unsupported_output_validator_kwarg_removed": True,
        "pinned_codex_cli": _record(PINNED_CODEX_0_144_1),
        "runtime_files": [_record(path) for path in _runtime_files()],
        "direct_lineage": list(predecessor["records"].values()),
        "source_requests": predecessor["request_records"],
        "failure_audit": _record(failure_audit_path),
        "design": _record(design_path),
        "spec": _record(spec_path),
        "capacity_audit": _record(capacity_paths["audit"]),
        "capacity_policy": _record(capacity_paths["policy"]),
        "mapping": _record(mapping_path),
        "instructions": _record(instructions_path),
        "frozen_requests": _request_records(root),
        "holdout_authorized_before_score": False,
        "production_mutation_allowed": False,
    }
    _write_stable_time(lock_path, lock, "frozen_at")
    verify_runtime_lock(lock_path)
    return _load_frozen(root)


def _inner_factory() -> codex_app_server.CodexAppServerClient:
    return codex_app_server.CodexAppServerClient(
        command=[str(PINNED_CODEX_0_144_1), "app-server", "--stdio", "--strict-config"]
    )


def _client_factory(policy_path: Path) -> ReserveCapacityGatedCodexAppServerClient:
    return ReserveCapacityGatedCodexAppServerClient(
        policy_path=policy_path, inner_factory=_inner_factory
    )


def _usage(sidecar: Mapping[str, Any]) -> dict[str, int]:
    return v246._usage(sidecar)


def _failure_terminal(root: Path, frozen: Mapping[str, Any], exc: BaseException) -> dict[str, Any]:
    attempted = measured = unknown = 0
    usage = {field: 0 for field in USAGE_FIELDS}
    sidecars = []
    for turn_name in TURN_NAMES:
        paths = _turn_paths(root, turn_name)
        if not paths["capacity"].exists() and not paths["sidecar"].exists():
            continue
        attempted += 1
        if paths["sidecar"].is_file():
            sidecars.append(_record(paths["sidecar"]))
            try:
                sidecar = _load_json(paths["sidecar"], f"{turn_name} sidecar")
                row_usage = _usage(sidecar)
                measured += 1
                for field in USAGE_FIELDS:
                    usage[field] += row_usage[field]
            except Exception:
                unknown += 1
        else:
            unknown += 1
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "inactive_incomplete_recovery_required",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "error_class": type(exc).__name__,
        "error_message_sha256": hashlib.sha256(message).hexdigest(),
        "error_message_bytes": len(message),
        "attempted_turn_count": attempted,
        "measured_turn_count": measured,
        "unknown_usage_turn_count": unknown,
        "semantic_retry_count": 0,
        "usage_status": "unknown" if unknown else "complete",
        "accounting_complete": unknown == 0,
        "usage": usage,
        "sidecars": sidecars,
        "development_quality_passed": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "overall_goal_complete": False,
        "goal_status_required": "active",
        "runtime_lock": _record(frozen["runtime_lock"]),
        "attempt_spec": _record(frozen["spec_path"]),
        "exact_next_action": "audit immutable v247 attempt; no retry",
    }
    _write_stable_time(root / "terminal.json", terminal, "terminal_at")
    return terminal


def _winner(score_path: Path) -> dict[str, Any]:
    return {
        "schema_version": WINNER_VERSION,
        "frozen_at": now_iso(),
        "system_id": "v244_source_indexed_event_graph_v247_frozen",
        "configuration": {
            "model": v244.MODEL,
            "reasoning_effort": v244.EFFORT,
            "max_events_per_segment": v244.MAX_EVENTS_PER_SEGMENT,
            "architecture_design": _record(v244.DEFAULT_OUTPUT_ROOT / "architecture-design.json"),
            "attempt_spec": _record(v244.DEFAULT_OUTPUT_ROOT / "attempt-spec.json"),
            "instructions": _record(
                next(v244.DEFAULT_OUTPUT_ROOT.glob("turns/*/base-instructions.private.md"))
            ),
            "schema": _record(next(v244.DEFAULT_OUTPUT_ROOT.glob("turns/*/schema.json"))),
            "runtime_lock": _record(v244.DEFAULT_OUTPUT_ROOT / "runtime-lock.json"),
        },
        "development_evidence": {
            "structural_gate": _record(
                v244.DEFAULT_OUTPUT_ROOT / "architecture-structural-gate.json"
            ),
            "support_terminal": _record(v245.DEFAULT_OUTPUT_ROOT / "terminal.json"),
            "alignment_score": _record(score_path),
        },
        "development_quality_passed": True,
        "production_amortized_total_token_ratio": OBSERVED_PRODUCTION_RATIO,
        "production_amortized_token_target_passed": True,
        "untouched_holdout_authorized": True,
        "overall_evaluation_complete": False,
        "production_mutated": False,
        "privacy": "hashes counts metrics and configuration no source or event text",
    }


async def run_v247(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[Path], Any] = _client_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if (root / "terminal.json").exists():
        return _load_json(root / "terminal.json", "v247 terminal")
    frozen = freeze_v247(output_dir=root)
    verify_runtime_lock(frozen["runtime_lock"])
    if (root / "launch-receipt.json").exists():
        return _failure_terminal(
            root,
            frozen,
            V247AlignmentTransportRecoveryError("launch exists; replay prohibited"),
        )
    _write_immutable(
        root / "launch-receipt.json",
        {
            "schema_version": SCHEMA_VERSION,
            "launched_at": now_iso(),
            "phase_id": PHASE_ID,
            "declared_turn_count": 2,
            "retry_count": 0,
            "model": MODEL,
            "effort": EFFORT,
            "runtime_lock": _record(frozen["runtime_lock"]),
            "semantic_prompt_schema_or_model_changed": False,
            "unsupported_output_validator_kwarg_removed": True,
            "managed_chatgpt_auth_only": True,
            "holdout_authorized_before_score": False,
            "production_mutation_allowed": False,
        },
    )
    started = time.monotonic()
    try:
        normalized = []
        usages = []
        async with client_factory(frozen["capacity_policy"]) as client:
            for turn in frozen["bundle"]["turns"]:
                paths = turn["paths"]
                result = await client.run_ephemeral_structured_turn(
                    model=MODEL,
                    effort=EFFORT,
                    base_instructions=frozen["bundle"]["instructions"],
                    prompt=turn["prompt"],
                    output_schema=turn["schema"],
                    cwd=PROJECT_ROOT,
                    sidecar_path=paths["sidecar"],
                    output_path=paths["output"],
                    batch_size=EXPECTED_ALIGNMENT_WITNESSES,
                    thread_mode="new_thread",
                    timeout_seconds=timeout_seconds,
                    capacity_checkpoint_path=paths["capacity"],
                )
                if result.status_ok is not True or not isinstance(result.output, Mapping):
                    raise V247AlignmentTransportRecoveryError(
                        f"alignment turn did not complete: {turn['turn_name']}"
                    )
                usages.append(_usage(_load_json(paths["sidecar"], f"{turn['turn_name']} sidecar")))
                errors = judge.validate_neutral_alignment_output(result.output, turn["value"])
                if errors:
                    raise V247AlignmentTransportRecoveryError(
                        "post-return neutral alignment validation failed: "
                        + "; ".join(sorted(set(errors)))
                    )
                projected = judge.normalize_neutral_alignment_output(
                    result.output, turn["value"]
                )
                _write_immutable(paths["normalized"], projected)
                normalized.append(projected)
        score = v246.score_alignment(
            base=normalized[0],
            canary=normalized[1],
            mapping=frozen["bundle"]["mapping"],
        )
        score_path = root / "alignment-score.json"
        _write_immutable(score_path, score)
        passed = bool(score["passed"])
        winner_path = root / "development-winner.json"
        winner_record = None
        if passed:
            winner = _winner(score_path)
            _write_stable_time(winner_path, winner, "frozen_at")
            winner_record = _record(winner_path)
        combined_usage = {
            field: sum(item[field] for item in usages) for field in USAGE_FIELDS
        }
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": "completed" if passed else "inactive_incomplete_recovery_required",
            "terminal_reason": (
                "v247_alignment_passed_development_winner_frozen_holdout_authorized"
                if passed
                else "v247_alignment_quality_or_permutation_gate_not_passed"
            ),
            "attempted_turn_count": 2,
            "measured_turn_count": 2,
            "unknown_usage_turn_count": 0,
            "semantic_retry_count": 0,
            "usage_status": "complete",
            "accounting_complete": True,
            "usage": combined_usage,
            "development_quality_passed": passed,
            "development_winner_frozen": passed,
            "holdout_authorized": passed,
            "production_mutated": False,
            "overall_goal_complete": False,
            "goal_status_required": "active",
            "production_amortized_total_token_ratio": OBSERVED_PRODUCTION_RATIO,
            "wall_seconds": round(time.monotonic() - started, 6),
            "score": _record(score_path),
            "development_winner": winner_record,
            "sidecars": [
                _record(turn["paths"]["sidecar"]) for turn in frozen["bundle"]["turns"]
            ],
            "runtime_lock": _record(frozen["runtime_lock"]),
            "attempt_spec": _record(frozen["spec_path"]),
            "exact_next_action": (
                "freeze the smallest balanced canary and untouched paired holdout"
                if passed
                else "reject v244 and select the next distinct architecture"
            ),
        }
        _write_stable_time(root / "terminal.json", terminal, "terminal_at")
        return terminal
    except BaseException as exc:
        if (root / "terminal.json").exists():
            return _load_json(root / "terminal.json", "v247 terminal")
        return _failure_terminal(root, frozen, exc)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Recover the frozen v246 alignment transport")
    parser.add_argument("action", choices=["freeze", "run"])
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    if args.action == "freeze":
        frozen = freeze_v247(output_dir=Path(args.output_dir))
        result = {
            "state": frozen["spec"]["state"],
            "root": str(frozen["root"]),
            "alignment_witness_count": frozen["spec"]["alignment_witness_count"],
        }
    else:
        terminal = asyncio.run(
            run_v247(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
        )
        result = {
            "state": terminal["state"],
            "terminal_reason": terminal["terminal_reason"],
            "usage_status": terminal.get("usage_status"),
            "total_tokens": (terminal.get("usage") or {}).get("total_tokens"),
            "development_quality_passed": terminal.get("development_quality_passed", False),
            "development_winner_frozen": terminal.get("development_winner_frozen", False),
            "holdout_authorized": terminal.get("holdout_authorized", False),
            "production_mutated": terminal.get("production_mutated", False),
        }
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
