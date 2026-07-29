from __future__ import annotations

"""Recover the pre-semantic v204 SQLite row-factory failure in a new version."""

import argparse
import asyncio
import hashlib
import json
import math
import sqlite3
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_evaluation as app_eval
from . import app_server_judge_v5_selection_v204_fresh_exhaustive_diagnostic as v204
from .app_server_capacity_reserve import load_reserve_capacity_policy
from .app_server_judge_v5_calibration_v25_diagnostic import (
    PINNED_CODEX_0_144_1,
    QUOTA_POINTS_PER_MILLION_TOKENS,
)
from .app_server_judge_v5_calibration_v26_diagnostic import (
    _load_json,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS
from .paths import db_path
from .util import now_iso


V205_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V205_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V205_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_v205_spec_v1"
V205_RUNTIME_LOCK_VERSION = "pif_app_server_judge_v5_4_selection_v205_runtime_lock_v1"
V205_LAUNCH_VERSION = "pif_app_server_judge_v5_4_selection_v205_launch_v1"
V205_FAILURE_VERSION = "pif_app_server_judge_v5_4_selection_v205_failure_v1"
V205_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_v205_terminal_v1"
PHASE_ID = "judge_v5_4_selection_v205_fresh_exhaustive_infrastructure_recovery"
DEFAULT_OUTPUT_ROOT = (
    v204.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v205-fresh-exhaustive-infrastructure-recovery"
).resolve()
TIMEOUT_SECONDS = v204.TIMEOUT_SECONDS
EXPECTED_V204_ERROR_CLASS = "TypeError"
EXPECTED_V204_ERROR_MESSAGE_BYTES = 49
EXPECTED_V204_ERROR_MESSAGE_SHA256 = (
    "696cc367b536e37efac4ce79ac5ff99dd8947bc17158b690ccfa18037ce3c22f"
)


class JudgeV5SelectionV205Error(RuntimeError):
    """The one allowed v205 infrastructure recovery cannot proceed safely."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v204_presemantic_failure() -> dict[str, Any]:
    root = v204.DEFAULT_OUTPUT_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "failure": root / "failure.json",
        "launch_receipt": root / "launch-receipt.json",
        "runtime_lock": root / "runtime-lock.json",
        "spec": root / "attempt-spec.json",
        "capacity_policy": root / "capacity-policy.json",
        "capacity_audit": root / "capacity-policy-audit.json",
    }
    values = {name: _load_json(path, f"v204 {name}") for name, path in paths.items()}
    terminal = values["terminal"]
    failure = values["failure"]
    zero_usage = {field: 0 for field in USAGE_FIELDS}
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason") != "infrastructure_or_extraction_attempt_failed"
        or terminal.get("terminal_classification") != "inactive_incomplete_recovery_required"
        or terminal.get("fresh_judge_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("semantic_retry_allowed") is not False
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("usage_status") != "not_started"
        or terminal.get("accounting_complete") is not False
        or terminal.get("usage") != zero_usage
        or terminal.get("failure") != _record(paths["failure"])
        or terminal.get("launch_receipt") != _record(paths["launch_receipt"])
        or terminal.get("runtime_lock") != _record(paths["runtime_lock"])
        or terminal.get("spec") != _record(paths["spec"])
        or failure.get("error_class") != EXPECTED_V204_ERROR_CLASS
        or failure.get("error_message_bytes") != EXPECTED_V204_ERROR_MESSAGE_BYTES
        or failure.get("error_message_sha256") != EXPECTED_V204_ERROR_MESSAGE_SHA256
        or failure.get("attempted_turn_count") != 0
        or failure.get("measured_turn_count") != 0
        or failure.get("unknown_usage_turn_count") != 0
        or failure.get("usage_status") != "not_started"
        or failure.get("accounting_complete") is not False
        or failure.get("usage") != zero_usage
        or failure.get("semantic_retry_allowed") is not False
        or failure.get("production_mutated") is not False
        or list(root.rglob("capacity.json"))
        or list((root / v204.ARM_DIRNAME / "sidecars").glob("*.json"))
        or list((root / v204.ARM_DIRNAME / "raw_outputs").glob("*.json"))
    ):
        raise JudgeV5SelectionV205Error("v204 pre-semantic failure contract drifted")
    if any(not _verify_record(_record(path)) for path in paths.values()):
        raise JudgeV5SelectionV205Error("v204 attempt artifact drifted")
    v203 = v204._validate_v203_authorization()
    v204.verify_runtime_lock(paths["runtime_lock"], predecessor=v203)
    load_reserve_capacity_policy(paths["capacity_policy"])
    return {
        "root": root,
        "paths": paths,
        "records": {name: _record(path) for name, path in paths.items()},
        "v203": v203,
        **values,
    }


def _build_capacity_policy(
    root: Path,
    predecessor: Mapping[str, Any],
    turn_names: Sequence[str],
) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    total_bound = len(turn_names) * v204.v203.MAXIMUM_TOTAL_TOKENS_PER_TURN
    projected = math.ceil(
        total_bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
    )
    audit = {
        "schema_version": V205_CAPACITY_AUDIT_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v204_terminal": predecessor["records"]["terminal"],
        "v204_failure": predecessor["records"]["failure"],
        "measured_basis": {
            "v204_semantic_turns_started": 0,
            "v204_new_usage_tokens": 0,
            "predecessor_known_total_tokens_lower_bound": predecessor["terminal"][
                "cumulative_known_usage_lower_bound"
            ]["total_tokens"],
            "predecessor_unknown_usage_turn_count": predecessor["terminal"][
                "cumulative_unknown_usage_turn_count"
            ],
            "predecessor_unknown_usage_upper_bound": predecessor["terminal"][
                "cumulative_conservative_unknown_usage_upper_bound"
            ],
            "declared_turn_count": len(turn_names),
            "maximum_total_tokens_per_turn": v204.v203.MAXIMUM_TOTAL_TOKENS_PER_TURN,
            "phase_total_token_bound": total_bound,
            "projected_phase_quota_points": projected,
            "minimum_remaining_reserve_percent": v204.MINIMUM_REMAINING_RESERVE_PERCENT,
            "maximum_live_used_percent_for_launch": 80 - projected,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V205_CAPACITY_POLICY_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "retry_count_per_turn": 0,
        "production_mutation_allowed": False,
        "rate_limit_reached_type_must_be_null": True,
        "unknown_usage_hard_stop": True,
        "ordered_turn_names": list(turn_names),
        "minimum_remaining_reserve_percent": v204.MINIMUM_REMAINING_RESERVE_PERCENT,
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "maximum_total_tokens_per_turn": v204.v203.MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "phase_total_token_bound": total_bound,
        "projected_phase_quota_points": projected,
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    load_reserve_capacity_policy(policy_path)
    return {"audit": audit_path, "policy": policy_path}


def _expected_runtime_paths() -> tuple[Path, ...]:
    return tuple(
        sorted(
            {Path(__file__).resolve(), *v204._expected_runtime_paths()},
            key=str,
        )
    )


def _freeze_runtime_lock(
    *,
    root: Path,
    predecessor: Mapping[str, Any],
    spec_path: Path,
    capacity_paths: Mapping[str, Path],
) -> Path:
    path = root / "runtime-lock.json"
    lock = {
        "schema_version": V205_RUNTIME_LOCK_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "pinned_codex_cli": _record(PINNED_CODEX_0_144_1),
        "runtime_files": [_record(item) for item in _expected_runtime_paths()],
        "v204_attempt": [
            predecessor["records"][name]
            for name in (
                "terminal",
                "failure",
                "launch_receipt",
                "runtime_lock",
                "spec",
                "capacity_policy",
                "capacity_audit",
            )
        ],
        "v203_inputs": [
            predecessor["v203"]["records"][name]
            for name in (
                "terminal",
                "spec",
                "design",
                "manifest",
                "selection_audit",
                "shared_reference_seed",
                "reference_noise",
            )
        ],
        "attempt_spec": _record(spec_path),
        "capacity_audit": _record(capacity_paths["audit"]),
        "capacity_policy": _record(capacity_paths["policy"]),
        "managed_chatgpt_auth_only": True,
        "production_mutation_allowed": False,
    }
    _write_stable_time(path, lock, "created_at")
    verify_runtime_lock(path, predecessor=predecessor)
    return path


def verify_runtime_lock(
    path: Path,
    *,
    predecessor: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    lock = _load_json(path, "v205 runtime lock")
    expected_paths = {str(item) for item in _expected_runtime_paths()}
    actual_paths = {
        str(Path(str(record.get("path") or "")).expanduser().resolve())
        for record in lock.get("runtime_files") or []
        if isinstance(record, Mapping)
    }
    if (
        lock.get("schema_version") != V205_RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or lock.get("managed_chatgpt_auth_only") is not True
        or lock.get("production_mutation_allowed") is not False
        or actual_paths != expected_paths
        or any(not _verify_record(record) for record in lock.get("runtime_files") or [])
        or any(not _verify_record(record) for record in lock.get("v204_attempt") or [])
        or any(not _verify_record(record) for record in lock.get("v203_inputs") or [])
        or not _verify_record(lock.get("pinned_codex_cli"))
        or not _verify_record(lock.get("attempt_spec"))
        or not _verify_record(lock.get("capacity_audit"))
        or not _verify_record(lock.get("capacity_policy"))
    ):
        raise JudgeV5SelectionV205Error("v205 runtime lock drifted")
    checked = predecessor or _validate_v204_presemantic_failure()
    expected_v204 = [
        checked["records"][name]
        for name in (
            "terminal",
            "failure",
            "launch_receipt",
            "runtime_lock",
            "spec",
            "capacity_policy",
            "capacity_audit",
        )
    ]
    if lock.get("v204_attempt") != expected_v204:
        raise JudgeV5SelectionV205Error("v205 v204 attempt binding drifted")
    load_reserve_capacity_policy(Path(lock["capacity_policy"]["path"]))
    return lock


def freeze_v205(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v205 terminal")}
    if (root / "launch-receipt.json").exists():
        raise JudgeV5SelectionV205Error("v205 launch already exists; replay is prohibited")
    predecessor = _validate_v204_presemantic_failure()
    semantic_predecessor = predecessor["v203"]
    turn_names = v204._ordered_batch_ids(
        semantic_predecessor["paths"]["manifest"], semantic_predecessor["manifest"]
    )
    spec_path = root / "attempt-spec.json"
    if not spec_path.exists():
        unexpected = [item for item in root.iterdir() if item.name != ".DS_Store"]
        if unexpected:
            raise JudgeV5SelectionV205Error("v205 root is nonempty without a frozen spec")
        capacity_paths = _build_capacity_policy(root, predecessor, turn_names)
        spec = {
            "schema_version": V205_SPEC_VERSION,
            "state": "frozen_before_semantic_attempt",
            "created_at": now_iso(),
            "phase_id": PHASE_ID,
            "recovery_scope": "sqlite_row_factory_only",
            "v204_root_cause": {
                "error_class": EXPECTED_V204_ERROR_CLASS,
                "error_message_sha256": EXPECTED_V204_ERROR_MESSAGE_SHA256,
                "error_message_bytes": EXPECTED_V204_ERROR_MESSAGE_BYTES,
                "semantic_turns_started": 0,
                "new_usage_tokens": 0,
            },
            "model": v204.v203.MODEL,
            "reasoning_effort": v204.v203.EFFORT,
            "batch_size": v204.v203.BATCH_SIZE,
            "thread_mode": v204.v203.THREAD_MODE,
            "concurrency": 1,
            "timeout_seconds_per_turn": timeout_seconds,
            "window_count": 4,
            "context_chars": 900,
            "max_events_per_segment": v204.v203.MAX_EVENTS_PER_SEGMENT,
            "turn_plan": turn_names,
            "semantic_model_calls_declared": v204.v203.TURN_COUNT,
            "retry_count_per_turn": 0,
            "persistent_app_server_process_count": 1,
            "managed_chatgpt_auth_only": True,
            "official_persistent_codex_app_server_only": True,
            "codex_exec_semantic_calls_allowed": False,
            "api_key_billing_allowed": False,
            "raw_session_token_access_allowed": False,
            "semantic_regex_or_keyword_pruning_allowed": False,
            "holdout_authorized": False,
            "production_mutation_allowed": False,
            "capacity_policy": _record(capacity_paths["policy"]),
            "capacity_audit": _record(capacity_paths["audit"]),
            "v204_attempt": predecessor["records"],
            "v203_manifest": semantic_predecessor["records"]["manifest"],
            "guideline": _record(v204.v203.DEFAULT_WINDOWED_GUIDELINES_PATH),
            "privacy": "private_inputs_outputs_and_mapping_sanitized_reports_only",
        }
        _write_stable_time(spec_path, spec, "created_at")
        lock_path = _freeze_runtime_lock(
            root=root,
            predecessor=predecessor,
            spec_path=spec_path,
            capacity_paths=capacity_paths,
        )
    else:
        spec = _load_json(spec_path, "v205 spec")
        capacity_paths = {
            "policy": root / "capacity-policy.json",
            "audit": root / "capacity-policy-audit.json",
        }
        lock_path = root / "runtime-lock.json"
        if (
            spec.get("schema_version") != V205_SPEC_VERSION
            or spec.get("recovery_scope") != "sqlite_row_factory_only"
            or spec.get("turn_plan") != turn_names
            or spec.get("retry_count_per_turn") != 0
            or spec.get("holdout_authorized") is not False
            or spec.get("production_mutation_allowed") is not False
            or spec.get("capacity_policy") != _record(capacity_paths["policy"])
            or spec.get("capacity_audit") != _record(capacity_paths["audit"])
        ):
            raise JudgeV5SelectionV205Error("v205 frozen spec drifted")
        verify_runtime_lock(lock_path, predecessor=predecessor)
    return {
        "root": root,
        "predecessor": predecessor,
        "semantic_predecessor": semantic_predecessor,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity_paths["policy"],
        "capacity_audit": capacity_paths["audit"],
        "runtime_lock": lock_path,
        "turn_names": turn_names,
    }


def _write_failure_terminal(
    *, root: Path, frozen: Mapping[str, Any], exc: BaseException
) -> dict[str, Any]:
    accounting = v204._sidecar_accounting(root)
    error = v204._error_receipt(exc)
    failure = {
        "schema_version": V205_FAILURE_VERSION,
        "failed_at": now_iso(),
        **error,
        **accounting,
        "semantic_retry_allowed": False,
        "fresh_judge_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "privacy": "error_class_hash_length_and_aggregate_usage_no_private_prompt_or_output",
    }
    failure_path = root / "failure.json"
    _write_stable_time(failure_path, failure, "failed_at")
    predecessor_terminal = frozen["predecessor"]["terminal"]
    known = dict(predecessor_terminal["cumulative_known_usage_lower_bound"])
    for field in USAGE_FIELDS:
        known[field] += int(accounting["usage"].get(field) or 0)
    unknown_turns = int(predecessor_terminal["cumulative_unknown_usage_turn_count"]) + int(
        accounting["unknown_usage_turn_count"]
    )
    unknown_upper = int(
        predecessor_terminal["cumulative_conservative_unknown_usage_upper_bound"]
    ) + int(accounting["unknown_usage_turn_count"]) * v204.v203.MAXIMUM_TOTAL_TOKENS_PER_TURN
    terminal = {
        "schema_version": V205_TERMINAL_VERSION,
        "state": "failed",
        "terminal_at": now_iso(),
        "terminal_reason": "infrastructure_or_extraction_attempt_failed",
        "terminal_classification": "inactive_incomplete_recovery_required",
        "overall_evaluation_complete": False,
        "development_winner_frozen": False,
        "fresh_judge_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_retry_count": 0,
        "semantic_retry_allowed": False,
        "usage_status": accounting["usage_status"],
        "accounting_complete": accounting["accounting_complete"],
        "usage": accounting["usage"],
        "cumulative_usage_status": "unknown",
        "cumulative_known_usage_lower_bound": known,
        "cumulative_unknown_usage_turn_count": unknown_turns,
        "cumulative_conservative_unknown_usage_upper_bound": unknown_upper,
        "failure": _record(failure_path),
        "v204_terminal": frozen["predecessor"]["records"]["terminal"],
        "spec": _record(frozen["spec_path"]),
        "runtime_lock": _record(frozen["runtime_lock"]),
        "launch_receipt": _record(root / "launch-receipt.json"),
    }
    _write_stable_time(root / "terminal.json", terminal, "terminal_at")
    return terminal


async def run_v205(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    database_path: Optional[Path] = None,
    timeout_seconds: float = TIMEOUT_SECONDS,
    inner_factory: Callable[[], Any] = v204._inner_factory,
    arm_runner: Callable[..., Any] = app_eval.run_app_server_core_arm,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if (root / "terminal.json").exists():
        return _load_json(root / "terminal.json", "v205 terminal")
    frozen = freeze_v205(output_dir=root, timeout_seconds=timeout_seconds)
    verify_runtime_lock(frozen["runtime_lock"], predecessor=frozen["predecessor"])
    launch_path = root / "launch-receipt.json"
    if launch_path.exists():
        raise JudgeV5SelectionV205Error("v205 launch receipt exists; semantic replay is prohibited")
    launch = {
        "schema_version": V205_LAUNCH_VERSION,
        "phase_id": PHASE_ID,
        "launched_at": now_iso(),
        "declared_turn_count": v204.v203.TURN_COUNT,
        "retry_count_per_turn": 0,
        "runtime_lock": _record(frozen["runtime_lock"]),
        "capacity_policy": _record(frozen["capacity_policy"]),
        "managed_chatgpt_auth_only": True,
        "semantic_thread_or_turn_started_before_receipt": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    _write_stable_time(launch_path, launch, "launched_at")

    source_db = (database_path or db_path()).expanduser().resolve()
    conn = sqlite3.connect(f"file:{source_db}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    arm_root = root / v204.ARM_DIRNAME

    def client_factory() -> v204.CoreArmReserveClient:
        return v204.CoreArmReserveClient(
            policy_path=frozen["capacity_policy"],
            arm_root=arm_root,
            inner_factory=inner_factory,
        )

    try:
        report = await arm_runner(
            conn,
            manifest_path=frozen["semantic_predecessor"]["paths"]["manifest"],
            output_dir=arm_root,
            batch_size=v204.v203.BATCH_SIZE,
            thread_mode=v204.v203.THREAD_MODE,
            model=v204.v203.MODEL,
            reasoning_effort=v204.v203.EFFORT,
            concurrency=1,
            timeout_seconds=timeout_seconds,
            window_count=4,
            context_chars=900,
            max_events_per_segment=v204.v203.MAX_EVENTS_PER_SEGMENT,
            guideline_path=v204.v203.DEFAULT_WINDOWED_GUIDELINES_PATH,
            client_factory=client_factory,
        )
        gate = v204.evaluate_structural_gate(
            report=report,
            manifest=frozen["semantic_predecessor"]["manifest"],
            arm_root=arm_root,
        )
        gate_path = root / "structural-gate.json"
        _write_immutable(gate_path, gate)
        usage = dict(report["usage"])
        predecessor_terminal = frozen["predecessor"]["terminal"]
        known = dict(predecessor_terminal["cumulative_known_usage_lower_bound"])
        for field in USAGE_FIELDS:
            known[field] += int(usage.get(field) or 0)
        passed = gate["passed"] is True
        terminal = {
            "schema_version": V205_TERMINAL_VERSION,
            "state": "completed" if passed else "failed",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v205_fresh_exhaustive_structural_gate_passed_fresh_judge_authorized"
                if passed
                else "v205_extraction_structural_quality_gate_not_passed"
            ),
            "terminal_classification": "active_development_recovery_required" if passed else "inactive_incomplete_recovery_required",
            "overall_evaluation_complete": False,
            "development_winner_frozen": False,
            "fresh_judge_authorized": passed,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_retry_count": 0,
            "semantic_retry_allowed": False,
            "usage_status": "complete",
            "accounting_complete": True,
            "usage": usage,
            "cumulative_usage_status": "unknown",
            "cumulative_known_usage_lower_bound": known,
            "cumulative_unknown_usage_turn_count": predecessor_terminal["cumulative_unknown_usage_turn_count"],
            "cumulative_conservative_unknown_usage_upper_bound": predecessor_terminal["cumulative_conservative_unknown_usage_upper_bound"],
            "production_amortized_total_token_ratio": gate["cost"]["production_amortized_total_token_ratio"],
            "production_amortized_token_target_passed": gate["cost"]["passed_lte_0_28"],
            "structural_gate": _record(gate_path),
            "arm_report": _record(arm_root / "report.json"),
            "v204_terminal": frozen["predecessor"]["records"]["terminal"],
            "spec": _record(frozen["spec_path"]),
            "runtime_lock": _record(frozen["runtime_lock"]),
            "launch_receipt": _record(launch_path),
            "required_next_artifact_path": str(
                root.parent
                / "development-selection-v5_4-v206-fresh-support-alignment-score"
                / "terminal.json"
            )
            if passed
            else None,
        }
        _write_stable_time(root / "terminal.json", terminal, "terminal_at")
        return terminal
    except BaseException as exc:
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
        return _write_failure_terminal(root=root, frozen=frozen, exc=exc)
    finally:
        conn.close()


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v205 fresh exhaustive recovery")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--database", default=str(db_path()))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v205(
            output_dir=Path(args.output_dir),
            database_path=Path(args.database),
            timeout_seconds=args.timeout_seconds,
        )
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "usage_status": terminal["usage_status"],
                "accounting_complete": terminal["accounting_complete"],
                "fresh_judge_authorized": terminal["fresh_judge_authorized"],
                "holdout_authorized": terminal["holdout_authorized"],
                "production_mutated": terminal["production_mutated"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
