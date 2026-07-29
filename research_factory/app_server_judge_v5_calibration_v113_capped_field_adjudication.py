from __future__ import annotations

"""One capped side-free adjudication for the two observable v112 failures."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from .app_server_judge_v5_calibration_v25_diagnostic import (
    QUOTA_POINTS_PER_MILLION_TOKENS,
)
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
from .app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic import (
    output_schema,
    validate_output,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_calibration_v112_luna_minimal_root_diagnostic import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V112_ROOT,
    base_instructions_v112,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .util import now_iso, sha256_text


V113_INPUT_VERSION = "pif_app_server_judge_v5_4_v113_capped_field_input_v1"
V113_TRUTH_VERSION = "pif_app_server_judge_v5_4_v113_capped_field_truth_v1"
V113_SELECTION_VERSION = "pif_app_server_judge_v5_4_v113_selection_v1"
V113_SPEC_VERSION = "pif_app_server_judge_v5_4_v113_spec_v1"
V113_SCORE_VERSION = "pif_app_server_judge_v5_4_v113_score_v1"
V113_RECONCILIATION_VERSION = "pif_app_server_judge_v5_4_v113_reconciliation_v1"
V113_FAILURE_VERSION = "pif_app_server_judge_v5_4_v113_failure_v1"
V113_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v113_terminal_v1"
V113_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V113_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V113_PHASE_ID = "judge_v5_4_v113_capped_field_adjudication"

MODEL = "gpt-5.4"
EFFORT = "high"
TURN_NAME = "capped_side_free_field_adjudication"
TIMEOUT_SECONDS = 900.0
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V112_ROOT.parent / "judge-calibration-v5_4-v113-capped-field-adjudication"
).resolve()


class JudgeV5CalibrationV113Error(RuntimeError):
    """The v113 capped adjudication contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v112(v112_root: Path = DEFAULT_V112_ROOT) -> dict[str, Any]:
    root = v112_root.expanduser().resolve()
    paths = {
        "spec": root / "minimal-root-diagnostic-spec.json",
        "terminal": root / "terminal.json",
        "score": root / "minimal-root-score.json",
        "input": root / "minimal-root-input.private.json",
        "truth": root / "minimal-root-truth.private.json",
        "output": root / "minimal-root-output.private.json",
        "canary_input": root / "permutation-canary-input.private.json",
        "canary_output": root / "permutation-canary-output.private.json",
        "selection": root / "selection-audit.json",
    }
    values = {name: _load_json(path, f"v112 {name}") for name, path in paths.items()}
    terminal, score, spec = values["terminal"], values["score"], values["spec"]
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("development_terminal_reason")
        != "v112_minimal_root_field_diagnostic_quality_gate_not_passed"
        or terminal.get("diagnostic_passed") is not False
        or terminal.get("contested_field_reference_owner_authorized") is not False
        or terminal.get("fresh_full_calibration_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 90066
        or score.get("passed") is not False
        or score.get("failed_checks")
        != [
            "incorrect_control_sensitivity",
            "permutation_canary_exact_rate",
            "settled_control_exact_rate",
        ]
        or score.get("metrics", {}).get("settled_control_exact_count") != 7
        or score.get("metrics", {}).get("correct_control_exact_count") != 4
        or score.get("metrics", {}).get("incorrect_control_exact_count") != 3
        or score.get("metrics", {}).get("permutation_canary_exact_count") != 5
        or spec.get("model") != "gpt-5.6-luna"
        or spec.get("retry_count_per_turn") != 0
        or spec.get("selection_authorized") is not False
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5CalibrationV113Error("v112 predecessor contract drifted")
    for record in spec.get("runtime_files") or []:
        if not _verify_record(record):
            raise JudgeV5CalibrationV113Error("v112 runtime record drifted")
    turn_records = {}
    usage = {field: 0 for field in USAGE_FIELDS}
    for turn_name in spec["turn_plan"]:
        turn_root = root / "turns" / turn_name.replace("_", "-")
        turn_paths = {
            name: turn_root / filename
            for name, filename in {
                "capacity": "capacity.json",
                "input": "input.private.json",
                "prompt": "prompt.private.md",
                "schema": "schema.json",
                "sidecar": "sidecar.json",
                "output": "output.private.json",
            }.items()
        }
        if any(not path.is_file() for path in turn_paths.values()):
            raise JudgeV5CalibrationV113Error("v112 turn coverage is incomplete")
        measured = _validate_usage(_load_json(turn_paths["sidecar"], "v112 sidecar"))
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
        turn_records[turn_name] = {name: _record(path) for name, path in turn_paths.items()}
    if usage != terminal.get("usage"):
        raise JudgeV5CalibrationV113Error("v112 usage aggregate drifted")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "turn_records": turn_records,
        "usage": usage,
    }


def _task_id(source_task_id: str, role: str) -> str:
    return "capped_" + sha256_text(f"v113|{role}|{source_task_id}")[:24]


def build_v113_inputs(v112: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    values = v112["values"]
    source = {str(row["task_id"]): row for row in values["input"]["tasks"]}
    truth = {str(row["task_id"]): row for row in values["truth"]["tasks"]}
    owner = {str(row["task_id"]): row for row in values["output"]["decisions"]}
    repeated = {str(row["task_id"]): row for row in values["canary_output"]["decisions"]}
    failed_controls = [
        row
        for row in truth.values()
        if row["role"] == "settled_control"
        and owner[row["task_id"]]["field_status"] != row["current_status"]
    ]
    if len(failed_controls) != 1 or failed_controls[0]["field"] != "unsupported_inference":
        raise JudgeV5CalibrationV113Error("v112 failed control drifted")
    unstable = []
    for mapping in values["truth"]["canary_map"]:
        if (
            owner[mapping["owner_task_id"]]["field_status"]
            != repeated[mapping["canary_task_id"]]["field_status"]
        ):
            unstable.append((truth[mapping["owner_task_id"]], mapping))
    if len(unstable) != 1 or unstable[0][0]["field"] != "evidence":
        raise JudgeV5CalibrationV113Error("v112 observable disagreement drifted")
    stable_controls = []
    canary_by_owner = {row["owner_task_id"]: row for row in values["truth"]["canary_map"]}
    for row in truth.values():
        if row["role"] != "settled_control" or owner[row["task_id"]]["field_status"] != row["current_status"]:
            continue
        mapping = canary_by_owner.get(row["task_id"])
        if mapping and repeated[mapping["canary_task_id"]]["field_status"] == owner[row["task_id"]]["field_status"]:
            stable_controls.append(row)
    correct = sorted(
        [row for row in stable_controls if row["current_status"] == "correct"],
        key=lambda row: sha256_text(f"v113|correct|{row['task_id']}"),
    )[:2]
    incorrect = sorted(
        [row for row in stable_controls if row["current_status"] == "incorrect"],
        key=lambda row: sha256_text(f"v113|incorrect|{row['task_id']}"),
    )[:2]
    if len(correct) != 2 or len(incorrect) != 2:
        raise JudgeV5CalibrationV113Error("v112 stable control pool is too small")
    selected = [
        (row, "matched_control", row["current_status"])
        for row in correct + incorrect
    ]
    selected.append((failed_controls[0], "failed_settled_control", failed_controls[0]["current_status"]))
    selected.append((unstable[0][0], "observable_permutation_disagreement", None))
    model_tasks = []
    truth_rows = []
    for row, role, expected in selected:
        target_id = _task_id(row["task_id"], role)
        task = deepcopy(source[row["task_id"]])
        task["task_id"] = target_id
        model_tasks.append(task)
        mapping = canary_by_owner.get(row["task_id"])
        truth_rows.append(
            {
                "task_id": target_id,
                "source_task_id": row["task_id"],
                "role": role,
                "field": row["field"],
                "control_expected_status": expected,
                "v112_primary_status": owner[row["task_id"]]["field_status"],
                "v112_canary_status": (
                    repeated[mapping["canary_task_id"]]["field_status"] if mapping else None
                ),
            }
        )
    model_tasks.sort(key=lambda row: row["task_id"])
    truth_rows.sort(key=lambda row: row["task_id"])
    value = {
        "schema_version": V113_INPUT_VERSION,
        "task_count": 6,
        "tasks": model_tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
    }
    truth_value = {
        "schema_version": V113_TRUTH_VERSION,
        "task_count": 6,
        "tasks": truth_rows,
    }
    selection = {
        "schema_version": V113_SELECTION_VERSION,
        "created_at": now_iso(),
        "task_count": 6,
        "role_counts": {
            "matched_control": 4,
            "failed_settled_control": 1,
            "observable_permutation_disagreement": 1,
        },
        "control_status_counts": {"correct": 2, "incorrect": 2},
        "observable_failure_fields": ["evidence", "unsupported_inference"],
        "selection_uses_source_text": False,
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "majority_voting_used": False,
        "capped_adjudication_turn_count": 1,
        "privacy": "aggregate_counts_and_field_enums_only",
    }
    return value, truth_value, selection


def build_prompt_v113(value: Mapping[str, Any]) -> str:
    return (
        "Return one independent field decision for every opaque task_id. Judge only the requested "
        "field after mentally repairing every other field. Every source_evidence_span must be an exact "
        "substring of that task's source_excerpt. Do not compare tasks or emit whole-event verdicts.\n\n"
        + json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        + "\n"
    )


def score_v113(output: Mapping[str, Any], truth: Mapping[str, Any]) -> dict[str, Any]:
    expected = {str(row["task_id"]): row for row in truth["tasks"]}
    observed = {str(row["task_id"]): row for row in output["decisions"]}
    if set(expected) != set(observed):
        raise JudgeV5CalibrationV113Error("v113 score coverage drifted")
    controls = [row for row in expected.values() if row["role"] == "matched_control"]
    failed = [row for row in expected.values() if row["role"] == "failed_settled_control"]
    unstable = [row for row in expected.values() if row["role"] == "observable_permutation_disagreement"]
    if len(controls) != 4 or len(failed) != 1 or len(unstable) != 1:
        raise JudgeV5CalibrationV113Error("v113 role coverage drifted")
    control_exact = sum(
        observed[row["task_id"]]["field_status"] == row["control_expected_status"]
        for row in controls
    )
    failed_status = observed[failed[0]["task_id"]]["field_status"]
    unstable_status = observed[unstable[0]["task_id"]]["field_status"]
    abstentions = sum(row["field_status"] == "abstain" for row in observed.values())
    evidence_complete = sum(bool(row["source_evidence_spans"]) for row in observed.values())
    checks = {
        "matched_control_exact_rate": control_exact == 4,
        "failed_control_repaired": failed_status == failed[0]["control_expected_status"],
        "observable_disagreement_decided": unstable_status in {"correct", "incorrect"},
        "abstention_count": abstentions == 0,
        "evidence_complete_rate": evidence_complete == 6,
    }
    passed = all(checks.values())
    return {
        "schema_version": V113_SCORE_VERSION,
        "passed": passed,
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "metrics": {
            "task_count": 6,
            "matched_control_count": 4,
            "matched_control_exact_count": control_exact,
            "abstention_count": abstentions,
            "evidence_complete_count": evidence_complete,
        },
        "failed_control_adjudicated_status": failed_status if passed else None,
        "observable_disagreement_adjudicated_status": unstable_status if passed else None,
        "contested_field_reference_owner_authorized": passed,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "majority_voting_used": False,
    }


def build_reconciliation(score: Mapping[str, Any]) -> dict[str, Any]:
    if not score["passed"]:
        raise JudgeV5CalibrationV113Error("v113 failed score cannot reconcile v112")
    return {
        "schema_version": V113_RECONCILIATION_VERSION,
        "created_at": now_iso(),
        "state": "authorized",
        "v112_settled_control_exact_count_before": 7,
        "v112_settled_control_exact_count_after": 8,
        "v112_permutation_canary_exact_count_before": 5,
        "v112_permutation_canary_resolved_count_after": 6,
        "failed_control_field": "unsupported_inference",
        "failed_control_final_status": score["failed_control_adjudicated_status"],
        "observable_disagreement_field": "evidence",
        "observable_disagreement_final_status": score[
            "observable_disagreement_adjudicated_status"
        ],
        "basis": "one_capped_side_free_gpt54_adjudication_with_four_exact_controls",
        "majority_voting_used": False,
        "contested_field_reference_owner_authorized": True,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    audit = {
        "schema_version": V113_CAPACITY_AUDIT_VERSION,
        "phase_id": V113_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": 1,
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": MAX_TOKENS_PER_TURN,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V113_CAPACITY_POLICY_VERSION,
        "phase_id": V113_PHASE_ID,
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
    _write_stable_time(policy_path, policy, "created_at")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v113(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v113 terminal")}
    v112 = _validate_v112()
    value, truth, selection = build_v113_inputs(v112)
    input_path = root / "capped-adjudication-input.private.json"
    truth_path = root / "capped-adjudication-truth.private.json"
    selection_path = root / "selection-audit.json"
    _write_immutable(input_path, value)
    _write_immutable(truth_path, truth)
    _write_stable_time(selection_path, selection, "created_at")
    prompt = build_prompt_v113(value)
    schema = output_schema(value)
    paths = _freeze_turn_request(
        root=root, turn_name=TURN_NAME, input_value=value, prompt=prompt, schema=schema
    )
    predecessor = {**v112["records"], "turns": v112["turn_records"]}
    capacity = _build_capacity_policy(root, predecessor)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V113_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "one_capped_side_free_adjudication_for_two_observable_v112_failures",
        "task_count": 6,
        "observable_failure_count": 2,
        "matched_control_count": 4,
        "turn_plan": [TURN_NAME],
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "majority_voting_used": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "four_exact_controls_failed_control_repaired_other_disagreement_decided_all_evidence_no_abstention",
        "contested_field_reference_owner_authorized": False,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v112_luna_minimal_root_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_inputs": {
            "input": _record(input_path),
            "truth": _record(truth_path),
            "selection": _record(selection_path),
            "turn_input": _record(paths["input"]),
            "prompt": _record(paths["prompt"]),
            "schema": _record(paths["schema"]),
        },
        "privacy": "private_source_event_output_no_source_text_in_reports",
    }
    spec_path = root / "capped-adjudication-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "value": value,
        "truth": truth,
        "prompt": prompt,
        "schema": schema,
        "paths": paths,
        "capacity_policy": capacity["policy"],
    }


def _write_failure(root: Path, error_class: str) -> dict[str, Any]:
    attempts = [row for row in _attempt_records(root) if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))]
    usage = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping):
            unknown += 1
            continue
        try:
            measured = _validate_usage(_load_json(Path(record["path"]), "v113 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V113_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": TURN_NAME,
        "error_class": error_class,
        "retry_allowed_in_this_version": False,
        "accounting_complete": complete,
        "usage_status": "complete" if complete else "unknown",
        "usage": usage if complete else None,
        "known_usage_lower_bound": usage,
        "unknown_usage_turn_count": unknown,
        "attempts": attempts,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V113_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "contested_field_reference_owner_authorized": False,
        "fresh_full_calibration_authorized": False,
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


async def run_v113(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v113 terminal")
    frozen = freeze_v113(output_dir=root, timeout_seconds=timeout_seconds)
    try:
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            output, sidecar, _ = await _get_or_run_turn(
                client=client,
                turn_name=TURN_NAME,
                paths=frozen["paths"],
                prompt=frozen["prompt"],
                schema=frozen["schema"],
                base_instructions=base_instructions_v112(),
                model=MODEL,
                effort=EFFORT,
                timeout_seconds=timeout_seconds,
                batch_size=6,
                policy_path=frozen["capacity_policy"],
                output_validator=lambda candidate: validate_output(candidate, frozen["value"]),
            )
        output_path = root / "capped-adjudication-output.private.json"
        _write_immutable(output_path, output)
        score = score_v113(output, frozen["truth"])
        score_path = root / "capped-adjudication-score.json"
        _write_immutable(score_path, score)
        reconciliation_path = root / "reconciliation.json"
        if score["passed"]:
            _write_stable_time(reconciliation_path, build_reconciliation(score), "created_at")
        accounting = _aggregate_usage([sidecar])
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V113_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v113_capped_adjudication_passed_contested_field_owner_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v113_capped_adjudication_passed"
                if passed
                else "v113_capped_adjudication_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "contested_field_reference_owner_authorized": passed,
            "fresh_full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(score_path),
            "output": _record(output_path),
            "reconciliation": _record(reconciliation_path) if passed else None,
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.error_class)
    except Exception as exc:
        return _write_failure(root, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v113 capped field adjudication")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(run_v113(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds))
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "contested_field_reference_owner_authorized": terminal.get(
                    "contested_field_reference_owner_authorized", False
                ),
                "usage_status": terminal.get("usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
