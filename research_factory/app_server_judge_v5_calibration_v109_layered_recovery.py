from __future__ import annotations

"""Versioned pre-semantic recovery for the immutable v108 freeze defect."""

import argparse
import asyncio
import json
import math
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v108_layered_diagnostic as v108
from .app_server_judge_v5_calibration import pointwise_input_subset
from .app_server_judge_v5_calibration_v25_diagnostic import (
    QUOTA_POINTS_PER_MILLION_TOKENS,
)
from .app_server_judge_v5_calibration_v26_diagnostic import (
    MAX_TOKENS_PER_TURN,
    _freeze_turn_request,
    _load_json,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _record_matches,
    _verify_record,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS
from .util import now_iso


V109_SPEC_VERSION = "pif_app_server_judge_v5_4_v109_layered_recovery_spec_v1"
V109_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v109_terminal_v1"
V109_PREDECESSOR_FAILURE_VERSION = "pif_app_server_judge_v5_4_v108_presemantic_failure_v1"
V109_PREDECESSOR_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v108_presemantic_terminal_v1"
V109_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V109_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V109_PHASE_ID = "judge_v5_4_v109_layered_diagnostic_recovery"
DEFAULT_OUTPUT_ROOT = (
    v108.DEFAULT_OUTPUT_ROOT.parent
    / "judge-calibration-v5_4-v109-layered-diagnostic-recovery"
).resolve()
TIMEOUT_SECONDS = v108.TIMEOUT_SECONDS


class JudgeV5CalibrationV109Error(RuntimeError):
    """The v109 recovery cannot preserve its versioned pre-semantic contract."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        prior = _load_json(path, f"existing {path.name}")
        value[key] = prior.get(key)
    _write_immutable(path, value)


def finalize_v108_presemantic_failure(
    *, v108_root: Path = v108.DEFAULT_OUTPUT_ROOT
) -> dict[str, Any]:
    root = v108_root.expanduser().resolve()
    spec_path = root / "layered-diagnostic-spec.json"
    policy_path = root / "capacity-policy.json"
    audit_path = root / "capacity-policy-audit.json"
    if not spec_path.is_file() or not policy_path.is_file() or not audit_path.is_file():
        raise JudgeV5CalibrationV109Error("v108 frozen presemantic artifacts are incomplete")
    if (
        list(root.glob("turns/*/capacity.json"))
        or list(root.glob("turns/*/sidecar.json"))
        or list(root.glob("turns/*/output.private.json"))
    ):
        raise JudgeV5CalibrationV109Error("v108 unexpectedly contains semantic artifacts")
    terminal_path = root / "terminal.json"
    failure_path = root / "failure.json"
    failure = {
        "schema_version": V109_PREDECESSOR_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "error_class": "JudgeV5CalibrationV26DiagnosticError",
        "error_surface": "presemantic_capacity_audit_idempotence_validation",
        "semantic_attempt_started": False,
        "thread_started": False,
        "turn_started": False,
        "retry_allowed_in_this_version": False,
        "accounting_complete": True,
        "usage_status": "complete",
        "usage": {field: 0 for field in USAGE_FIELDS},
        "unknown_usage_turn_count": 0,
        "frozen_spec": _record(spec_path),
        "capacity_policy": _record(policy_path),
        "capacity_audit": _record(audit_path),
    }
    _write_stable_time(failure_path, failure, "terminal_at")
    terminal = {
        "schema_version": V109_PREDECESSOR_TERMINAL_VERSION,
        "state": "failed",
        "terminal_at": now_iso(),
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "diagnostic_passed": False,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_attempt_started": False,
        "semantic_retry_count": 0,
        "accounting_complete": True,
        "usage_status": "complete",
        "usage": {field: 0 for field in USAGE_FIELDS},
        "superseding_version_required": True,
    }
    _write_stable_time(terminal_path, terminal, "terminal_at")
    return terminal


def _validate_v108_terminal() -> dict[str, Any]:
    root = v108.DEFAULT_OUTPUT_ROOT
    terminal_path = root / "terminal.json"
    failure_path = root / "failure.json"
    terminal = _load_json(terminal_path, "v108 presemantic terminal")
    failure = _load_json(failure_path, "v108 presemantic failure")
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("semantic_attempt_started") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("usage", {}).get("total_tokens") != 0
        or terminal.get("production_mutated") is not False
        or failure.get("error_surface")
        != "presemantic_capacity_audit_idempotence_validation"
        or failure.get("semantic_attempt_started") is not False
        or failure.get("thread_started") is not False
        or failure.get("turn_started") is not False
        or failure.get("usage", {}).get("total_tokens") != 0
        or not _record_matches(terminal.get("failure"), failure_path)
    ):
        raise JudgeV5CalibrationV109Error("v108 presemantic terminal drifted")
    return {
        "terminal": terminal,
        "failure": failure,
        "terminal_record": _record(terminal_path),
        "failure_record": _record(failure_path),
    }


def _build_capacity_policy(
    *, root: Path, execution_root: Path, predecessor: Mapping[str, Any]
) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(v108.TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V109_CAPACITY_AUDIT_VERSION,
        "phase_id": V109_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": len(v108.TURN_NAMES),
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
        },
        "v108_presemantic_attempt_replayed": False,
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V109_CAPACITY_POLICY_VERSION,
        "phase_id": V109_PHASE_ID,
        "created_at": now_iso(),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "retry_count_per_turn": 0,
        "production_mutation_allowed": False,
        "rate_limit_reached_type_must_be_null": True,
        "unknown_usage_hard_stop": True,
        "ordered_turn_names": list(v108.TURN_NAMES),
        "minimum_remaining_reserve_percent": 20,
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
        "phase_total_token_bound": bound,
        "projected_phase_quota_points": math.ceil(
            bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
        ),
        "semantic_output_root": str(execution_root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v109(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    execution_root = root / "semantic-execution"
    root.mkdir(parents=True, exist_ok=True)
    execution_root.mkdir(parents=True, exist_ok=True)
    v108_terminal = _validate_v108_terminal()
    predecessor = v108._validate_predecessors()
    selection = v108._select_case_ids(predecessor)
    expected = v108._subset_truth(predecessor["values"]["v106_truth"], selection)
    pool = predecessor["values"]["v106_pool"]
    pointwise_full = predecessor["values"]["v106_pointwise_input"]
    shards = v108._case_shards(selection["selected"])
    selection_path = execution_root / "diagnostic-selection.json"
    truth_path = execution_root / "diagnostic-truth.private.json"
    _write_immutable(selection_path, selection)
    _write_immutable(truth_path, expected)
    pointwise_shards = []
    for turn_name, case_ids in zip(v108.POINTWISE_TURNS, shards, strict=True):
        value = pointwise_input_subset(pointwise_full, case_ids)
        prompt = v108.build_pointwise_checklist_prompt(value)
        schema = v108.pointwise_checklist_schema(value)
        paths = _freeze_turn_request(
            root=execution_root,
            turn_name=turn_name,
            input_value=value,
            prompt=prompt,
            schema=schema,
        )
        pointwise_shards.append(
            {
                "turn_name": turn_name,
                "case_ids": case_ids,
                "input": value,
                "prompt": prompt,
                "schema": schema,
                "paths": paths,
            }
        )
    predecessor_records = {
        **predecessor["records"],
        "v108_terminal": v108_terminal["terminal_record"],
        "v108_failure": v108_terminal["failure_record"],
    }
    capacity = _build_capacity_policy(
        root=root,
        execution_root=execution_root,
        predecessor=predecessor_records,
    )
    runtime_dir = Path(__file__).resolve().parent
    runtime_files = [
        Path(__file__),
        runtime_dir / "app_server_judge_v5_calibration_v108_layered_diagnostic.py",
        runtime_dir / "app_server_judge_v5_calibration_v107_recovery_receipt.py",
        runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py",
        runtime_dir / "app_server_judge_v5_calibration.py",
        runtime_dir / "app_server_judge_v5.py",
        runtime_dir / "app_server_capacity_reserve.py",
        runtime_dir / "codex_app_server.py",
    ]
    spec = {
        "schema_version": V109_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "supersedes_presemantic_attempt": "v108",
        "v108_semantic_attempt_replayed": False,
        "protocol_core": "v108_layered_diagnostic_unchanged",
        "execution_root": str(execution_root),
        "primary_model": v108.PRIMARY_MODEL,
        "adjudicator_model": v108.ADJUDICATOR_MODEL,
        "reasoning_effort": v108.EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "case_count": 18,
        "canary_case_count": 12,
        "minimum_turn_count": 8,
        "maximum_turn_count": 9,
        "turn_plan": list(v108.TURN_NAMES),
        "retry_count_per_turn": 0,
        "explicit_pointwise_15_field_checklist": True,
        "supported_equivalent_assignment_priority": True,
        "merge_split_atom_normalization": True,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor_records,
        "runtime_files": [_record(path) for path in runtime_files],
        "frozen_inputs": {
            "selection": _record(selection_path),
            "truth": _record(truth_path),
            "pointwise_shards": [
                {
                    "turn_name": shard["turn_name"],
                    "case_ids": shard["case_ids"],
                    "input": _record(shard["paths"]["input"]),
                    "prompt": _record(shard["paths"]["prompt"]),
                    "schema": _record(shard["paths"]["schema"]),
                }
                for shard in pointwise_shards
            ],
        },
        "privacy": "private_source_event_prompts_outputs_truth_sanitized_terminal_only",
    }
    spec_path = root / "layered-recovery-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v109 spec")
        spec["created_at"] = prior.get("created_at")
        if spec != prior:
            raise JudgeV5CalibrationV109Error("immutable v109 spec drifted")
    else:
        _write_immutable(spec_path, spec)
    if not all(_verify_record(record) for record in spec["runtime_files"]):
        raise JudgeV5CalibrationV109Error("v109 runtime record drifted")
    return {
        "root": execution_root,
        "wrapper_root": root,
        "spec": spec,
        "spec_path": spec_path,
        "selection": selection,
        "expected": expected,
        "pool": pool,
        "pointwise_full": pointwise_full,
        "shards": shards,
        "pointwise_shards": pointwise_shards,
        "capacity_policy": capacity["policy"],
    }


def _write_wrapper_terminal(
    *, root: Path, core_terminal: Mapping[str, Any], execution_root: Path
) -> dict[str, Any]:
    passed = core_terminal.get("diagnostic_passed") is True
    failed = core_terminal.get("state") == "failed"
    terminal = {
        "schema_version": V109_TERMINAL_VERSION,
        "state": "failed" if failed else ("completed" if passed else "inactive"),
        "terminal_at": now_iso(),
        "terminal_reason": (
            "infrastructure_or_judge_attempt_failed"
            if failed
            else (
                "v109_layered_diagnostic_passed_fresh_full_calibration_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            )
        ),
        "development_terminal_reason": (
            core_terminal.get("development_terminal_reason")
            or core_terminal.get("terminal_reason")
        ),
        "overall_evaluation_complete": False,
        "diagnostic_passed": passed,
        "fresh_full_calibration_authorized": passed and not failed,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_attempt_started": core_terminal.get("semantic_attempt_started", True),
        "semantic_retry_count": 0,
        "accounting_complete": core_terminal.get("accounting_complete"),
        "usage_status": core_terminal.get("usage_status"),
        "usage": core_terminal.get("usage"),
        "turn_count": core_terminal.get("turn_count"),
        "failed_quality_gates": core_terminal.get("failed_quality_gates", []),
        "metrics": core_terminal.get("metrics"),
        "protocol_core_terminal": _record(execution_root / "terminal.json"),
        "v108_presemantic_attempt_replayed": False,
    }
    terminal_path = root / "terminal.json"
    _write_stable_time(terminal_path, terminal, "terminal_at")
    return terminal


async def run_v109(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v109 terminal")
    frozen = freeze_v109(output_dir=root, timeout_seconds=timeout_seconds)
    original_freeze = v108.freeze_v108
    try:
        v108.freeze_v108 = lambda **_: frozen
        core_terminal = await v108.run_v108(
            output_dir=frozen["root"],
            timeout_seconds=timeout_seconds,
            client_factory=client_factory,
        )
    finally:
        v108.freeze_v108 = original_freeze
    return _write_wrapper_terminal(
        root=root,
        core_terminal=core_terminal,
        execution_root=frozen["root"],
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v109 layered diagnostic recovery")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v109(
            output_dir=Path(args.output_dir),
            timeout_seconds=args.timeout_seconds,
        )
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "diagnostic_passed": terminal.get("diagnostic_passed", False),
                "fresh_full_calibration_authorized": terminal.get(
                    "fresh_full_calibration_authorized", False
                ),
                "usage_status": terminal.get("usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
