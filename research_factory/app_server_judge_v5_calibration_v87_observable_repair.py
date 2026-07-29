from __future__ import annotations

"""One-call observable repair for the frozen v86 diagnostic."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from .app_server_judge_v5_calibration_v25_diagnostic import QUOTA_POINTS_PER_MILLION_TOKENS
from .app_server_judge_v5_calibration_v26_diagnostic import (
    MAX_TOKENS_PER_TURN,
    JudgeV5CalibrationV26DiagnosticAttemptFailed,
    _aggregate_usage,
    _client_factory,
    _freeze_turn_request,
    _get_or_run_turn,
    _load_json,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v75_exact_span_remaining_shard import project_exact_spans
from .app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic import (
    output_schema,
    validate_output,
)
from .app_server_judge_v5_calibration_v84_metric_target_reference_audit import (
    base_instructions,
    build_prompt,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V86_ROOT,
    _record_matches,
    _verify_record,
    _write_stable_created,
    score_v86,
)
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _record,
    _validate_usage,
)
from .util import now_iso


V87_INPUT_VERSION = "pif_app_server_judge_v5_4_v87_observable_repair_input_v1"
V87_SPEC_VERSION = "pif_app_server_judge_v5_4_v87_observable_repair_spec_v1"
V87_OUTPUT_VERSION = "pif_app_server_judge_v5_4_v87_observable_repair_output_v1"
V87_SCORE_VERSION = "pif_app_server_judge_v5_4_v87_observable_repair_score_v1"
V87_AUDIT_VERSION = "pif_app_server_judge_v5_4_v87_reconciliation_audit_v1"
V87_FAILURE_VERSION = "pif_app_server_judge_v5_4_v87_failure_v1"
V87_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v87_terminal_v1"
V87_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V87_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V87_PHASE_ID = "judge_v5_4_v87_observable_repair"
TURN_NAME = "observable_repair"
MODEL = "gpt-5.5"
EFFORT = "high"
TIMEOUT_SECONDS = 600.0
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V86_ROOT.parent / "judge-calibration-v5_4-v87-observable-repair"
).resolve()


class JudgeV5CalibrationV87Error(RuntimeError):
    """The v87 bounded repair cannot preserve its frozen contract."""


def _validate_v86(root: Path) -> dict[str, Any]:
    paths = {
        "terminal": root / "terminal.json",
        "spec": root / "fresh-enhanced-spec.json",
        "score": root / "fresh-enhanced-score.json",
        "input": root / "fresh-enhanced-input.private.json",
        "truth": root / "fresh-enhanced-truth.private.json",
        "output": root / "fresh-enhanced-output.private.json",
        "canary_output": root / "permutation-canary-output.private.json",
    }
    values = {name: _load_json(path, f"v86 {name}") for name, path in paths.items()}
    terminal = values["terminal"]
    spec = values["spec"]
    score = values["score"]
    trigger_ids = score.get("observable_repair_task_ids") or []
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("development_terminal_reason") != "v86_observable_repair_required"
        or terminal.get("diagnostic_passed") is not False
        or terminal.get("bounded_observable_repair_authorized") is not True
        or terminal.get("fresh_full_development_calibration_authorized") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("production_mutated") is not False
        or len(trigger_ids) != 1
        or score.get("bounded_observable_repair_authorized") is not True
        or score.get("metrics", {}).get("observable_repair_trigger_count") != 1
        or not _record_matches(terminal.get("score"), paths["score"])
        or not _record_matches(terminal.get("output"), paths["output"])
        or not _record_matches(terminal.get("canary_output"), paths["canary_output"])
        or not _record_matches(spec.get("frozen_inputs", {}).get("input"), paths["input"])
        or not _record_matches(spec.get("frozen_inputs", {}).get("truth"), paths["truth"])
        or not all(_verify_record(row) for row in spec.get("runtime_files") or [])
    ):
        raise JudgeV5CalibrationV87Error("v86 observable-repair contract drifted")
    return {
        "paths": paths,
        "values": values,
        "trigger_task_id": trigger_ids[0],
        "records": {name: _record(path) for name, path in paths.items()},
    }


def build_repair_input(v86: Mapping[str, Any]) -> dict[str, Any]:
    trigger_id = v86["trigger_task_id"]
    tasks = [
        deepcopy(row)
        for row in v86["values"]["input"].get("tasks") or []
        if row.get("task_id") == trigger_id
    ]
    if len(tasks) != 1:
        raise JudgeV5CalibrationV87Error("v87 trigger task coverage drifted")
    return {
        "schema_version": V87_INPUT_VERSION,
        "task_count": 1,
        "tasks": tasks,
        "repair_scope": "one_observable_abstention_only",
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
    }


def repair_instructions() -> str:
    return (
        base_instructions()
        + " Resolve this single bounded verification independently. Use abstain only when the exact source "
        "proposition genuinely cannot determine the requested field after accounting for explicit time "
        "expressions, horizons, durations, and qualitative temporal scope. Do not infer a horizon from an "
        "adjacent proposition."
    )


def reconcile_output(
    prior: Mapping[str, Any], repair: Mapping[str, Any], trigger_task_id: str
) -> dict[str, Any]:
    repair_rows = repair.get("decisions") or []
    if len(repair_rows) != 1 or repair_rows[0].get("task_id") != trigger_task_id:
        raise JudgeV5CalibrationV87Error("v87 repair output coverage drifted")
    decisions = []
    replaced = 0
    for row in prior.get("decisions") or []:
        if row.get("task_id") == trigger_task_id:
            decisions.append(deepcopy(repair_rows[0]))
            replaced += 1
        else:
            decisions.append(deepcopy(row))
    if replaced != 1 or len(decisions) != 15:
        raise JudgeV5CalibrationV87Error("v87 reconciliation coverage drifted")
    return {"schema_version": V87_OUTPUT_VERSION, "decisions": decisions}


def residual_mismatches(
    output: Mapping[str, Any], truth: Mapping[str, Any]
) -> list[dict[str, str]]:
    expected = {row["task_id"]: row for row in truth.get("tasks") or []}
    observed = {row["task_id"]: row for row in output.get("decisions") or []}
    if set(expected) != set(observed):
        raise JudgeV5CalibrationV87Error("v87 residual coverage drifted")
    return [
        {
            "task_id": task_id,
            "field": row["field"],
            "expected_status": row["expected_status"],
            "observed_status": observed[task_id]["field_status"],
        }
        for task_id, row in sorted(expected.items())
        if observed[task_id]["field_status"] != row["expected_status"]
    ]


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    audit = {
        "schema_version": V87_CAPACITY_AUDIT_VERSION,
        "phase_id": V87_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": 1,
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": MAX_TOKENS_PER_TURN,
        },
    }
    _write_stable_created(audit_path, audit, "v87 capacity audit")
    policy = {
        "schema_version": V87_CAPACITY_POLICY_VERSION,
        "phase_id": V87_PHASE_ID,
        "created_at": now_iso(),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "retry_count_per_turn": 0,
        "production_mutation_allowed": False,
        "rate_limit_reached_type_must_be_null": True,
        "unknown_usage_hard_stop": True,
        "ordered_turn_names": [TURN_NAME],
        "minimum_remaining_reserve_percent": 20,
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
        "phase_total_token_bound": MAX_TOKENS_PER_TURN,
        "projected_phase_quota_points": math.ceil(
            MAX_TOKENS_PER_TURN * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
        ),
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_created(policy_path, policy, "v87 capacity policy")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v87(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v86_root: Path = DEFAULT_V86_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessor = _validate_v86(v86_root.resolve())
    value = build_repair_input(predecessor)
    input_path = root / "observable-repair-input.private.json"
    _write_immutable(input_path, value)
    prompt = build_prompt(value)
    schema = output_schema(value)
    paths = _freeze_turn_request(
        root=root, turn_name=TURN_NAME, input_value=value, prompt=prompt, schema=schema
    )
    capacity = _build_capacity_policy(root, predecessor["records"])
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V87_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "single_stronger_owner_repair_of_one_observable_abstention",
        "turn_plan": [TURN_NAME],
        "task_count": 1,
        "trigger_task_id": predecessor["trigger_task_id"],
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "retry_count_per_turn": 0,
        "reconciliation_rule": "replace_exactly_the_single_trigger_task_and_preserve_all_other_v86_decisions",
        "promotion_rule": "full_v86_gate_must_pass_after_single_replacement",
        "residual_reference_audit_rule": "if_full_gate_fails_with_one_to_three_residuals_and_canary_evidence_checks_hold_authorize_one_blinded_control_calibrated_reference_audit",
        "fresh_full_development_calibration_authorized": False,
        "residual_reference_audit_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor["records"],
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v84_metric_target_reference_audit.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v75_exact_span_remaining_shard.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_inputs": {
            "input": _record(input_path),
            "turn": {
                "input": _record(paths["input"]),
                "prompt": _record(paths["prompt"]),
                "schema": _record(paths["schema"]),
            },
        },
        "privacy": "private_source_event_output_no_source_text_in_reports",
    }
    spec_path = root / "observable-repair-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v87 spec")
        candidate = deepcopy(spec)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV87Error("immutable v87 spec drifted")
        spec = prior
    else:
        _write_immutable(spec_path, spec)
    return {
        "root": root,
        "spec": spec,
        "value": value,
        "paths": paths,
        "prompt": prompt,
        "schema": schema,
        "capacity_policy": capacity["policy"],
        "predecessor": predecessor,
    }


def _real_attempts(root: Path) -> list[dict[str, Any]]:
    return [
        row
        for row in _attempt_records(root)
        if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))
    ]


def _write_failure(root: Path, turn_name: Optional[str], error_class: str) -> dict[str, Any]:
    attempts = _real_attempts(root)
    usage = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping):
            unknown += 1
            continue
        try:
            measured = _validate_usage(_load_json(Path(record["path"]), "v87 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V87_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": turn_name,
        "error_class": error_class,
        "retry_allowed_in_this_version": False,
        "attempts": attempts,
        "accounting_complete": complete,
        "usage_status": "complete" if complete else "unknown",
        "usage": usage if complete else None,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V87_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "repair_completed": False,
        "fresh_full_development_calibration_authorized": False,
        "residual_reference_audit_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_retry_count": 0,
        "accounting_complete": complete,
        "usage_status": failure["usage_status"],
        "usage": failure["usage"],
    }
    _write_immutable(root / "terminal.json", terminal)
    return terminal


async def run_v87(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v87 terminal")
    frozen = freeze_v87(output_dir=root, timeout_seconds=timeout_seconds)
    factory = client_factory or _client_factory
    current_turn = TURN_NAME
    try:
        async with factory(frozen["capacity_policy"]) as client:
            output, sidecar, adopted = await _get_or_run_turn(
                client=client,
                turn_name=TURN_NAME,
                paths=frozen["paths"],
                prompt=frozen["prompt"],
                schema=frozen["schema"],
                base_instructions=repair_instructions(),
                model=MODEL,
                effort=EFFORT,
                timeout_seconds=timeout_seconds,
                batch_size=1,
                policy_path=frozen["capacity_policy"],
                output_validator=lambda candidate: validate_output(
                    project_exact_spans(candidate, frozen["value"])[0], frozen["value"]
                ),
            )
        projected, operations = project_exact_spans(output, frozen["value"])
        if validate_output(projected, frozen["value"]):
            raise JudgeV5CalibrationV87Error("projected v87 output is invalid")
        repair_path = root / "observable-repair-output.private.json"
        _write_immutable(repair_path, projected)
        predecessor = frozen["predecessor"]["values"]
        reconciled = reconcile_output(
            predecessor["output"], projected, frozen["predecessor"]["trigger_task_id"]
        )
        reconciled_path = root / "reconciled-output.private.json"
        _write_immutable(reconciled_path, reconciled)
        score = score_v86(reconciled, predecessor["canary_output"], predecessor["truth"])
        score["schema_version"] = V87_SCORE_VERSION
        residuals = residual_mismatches(reconciled, predecessor["truth"])
        metrics = score["metrics"]
        reference_audit = (
            not score["passed"]
            and 1 <= len(residuals) <= 3
            and metrics["permutation_canary_exact_rate"] == 1.0
            and metrics["evidence_complete_rate"] == 1.0
            and metrics["canary_abstention_count"] == 0
        )
        score["residual_reference_audit_authorized"] = reference_audit
        score_path = root / "observable-repair-score.json"
        _write_immutable(score_path, score)
        audit = {
            "schema_version": V87_AUDIT_VERSION,
            "created_at": now_iso(),
            "replacement_count": 1,
            "trigger_task_id": frozen["predecessor"]["trigger_task_id"],
            "projection_operation_count": len(operations),
            "projection_operations": operations,
            "residual_mismatch_count": len(residuals),
            "residual_mismatches": residuals,
            "semantic_decisions_made_by_llm_only": True,
            "deterministic_scope": "exact_span_projection_single_identity_replacement_validation_and_scoring",
            "privacy": "opaque_task_ids_field_enums_statuses_and_span_hashes_only",
        }
        audit_path = root / "reconciliation-audit.json"
        _write_immutable(audit_path, audit)
        accounting = _aggregate_usage([sidecar])
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V87_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v87_repair_passed_full_development_calibration_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v87_repair_passed_full_development_calibration_authorized"
                if passed
                else "v87_repair_completed_residual_reference_audit_required"
            ),
            "overall_evaluation_complete": False,
            "repair_completed": True,
            "diagnostic_passed": passed,
            "fresh_full_development_calibration_authorized": passed,
            "residual_reference_audit_authorized": reference_audit,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(score_path),
            "repair_output": _record(repair_path),
            "reconciled_output": _record(reconciled_path),
            "reconciliation_audit": _record(audit_path),
            "attempts": _real_attempts(root),
            "completed_checkpoint_adoption": adopted,
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v87 one-call observable repair")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v87(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "repair_completed": terminal.get("repair_completed", False),
                "diagnostic_passed": terminal.get("diagnostic_passed", False),
                "residual_reference_audit_authorized": terminal.get(
                    "residual_reference_audit_authorized", False
                ),
                "usage_status": terminal["usage_status"],
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
