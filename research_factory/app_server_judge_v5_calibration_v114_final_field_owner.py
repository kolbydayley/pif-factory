from __future__ import annotations

"""Final side-free owner for the two v112/v113 fixture-truth disputes."""

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
    base_instructions_v112,
)
from .app_server_judge_v5_calibration_v113_capped_field_adjudication import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V113_ROOT,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .util import now_iso, sha256_text


V114_INPUT_VERSION = "pif_app_server_judge_v5_4_v114_final_field_owner_input_v1"
V114_TRUTH_VERSION = "pif_app_server_judge_v5_4_v114_final_field_owner_truth_v1"
V114_SELECTION_VERSION = "pif_app_server_judge_v5_4_v114_selection_v1"
V114_SPEC_VERSION = "pif_app_server_judge_v5_4_v114_spec_v1"
V114_SCORE_VERSION = "pif_app_server_judge_v5_4_v114_score_v1"
V114_RESOLUTION_VERSION = "pif_app_server_judge_v5_4_v114_resolution_v1"
V114_FAILURE_VERSION = "pif_app_server_judge_v5_4_v114_failure_v1"
V114_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v114_terminal_v1"
V114_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V114_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V114_PHASE_ID = "judge_v5_4_v114_final_field_owner"

MODEL = "gpt-5.6-terra"
EFFORT = "high"
TURN_NAME = "final_side_free_field_owner"
TIMEOUT_SECONDS = 900.0
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V113_ROOT.parent / "judge-calibration-v5_4-v114-final-field-owner"
).resolve()


class JudgeV5CalibrationV114Error(RuntimeError):
    """The v114 final-owner contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v113(v113_root: Path = DEFAULT_V113_ROOT) -> dict[str, Any]:
    root = v113_root.expanduser().resolve()
    paths = {
        "spec": root / "capped-adjudication-spec.json",
        "terminal": root / "terminal.json",
        "score": root / "capped-adjudication-score.json",
        "input": root / "capped-adjudication-input.private.json",
        "truth": root / "capped-adjudication-truth.private.json",
        "output": root / "capped-adjudication-output.private.json",
        "selection": root / "selection-audit.json",
    }
    values = {name: _load_json(path, f"v113 {name}") for name, path in paths.items()}
    terminal, score, spec = values["terminal"], values["score"], values["spec"]
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("development_terminal_reason")
        != "v113_capped_adjudication_quality_gate_not_passed"
        or terminal.get("contested_field_reference_owner_authorized") is not False
        or terminal.get("fresh_full_calibration_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 24781
        or score.get("passed") is not False
        or score.get("failed_checks") != ["failed_control_repaired"]
        or score.get("metrics", {}).get("matched_control_exact_count") != 4
        or score.get("metrics", {}).get("evidence_complete_count") != 6
        or score.get("metrics", {}).get("abstention_count") != 0
        or spec.get("model") != "gpt-5.4"
        or spec.get("retry_count_per_turn") != 0
        or spec.get("selection_authorized") is not False
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5CalibrationV114Error("v113 predecessor contract drifted")
    if not all(_verify_record(record) for record in spec.get("runtime_files") or []):
        raise JudgeV5CalibrationV114Error("v113 runtime record drifted")
    turn_root = root / "turns" / TURN_NAME.replace("final_side_free_field_owner", "capped-side-free-field-adjudication")
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
        raise JudgeV5CalibrationV114Error("v113 turn coverage is incomplete")
    measured = _validate_usage(_load_json(turn_paths["sidecar"], "v113 sidecar"))
    if measured != terminal.get("usage"):
        raise JudgeV5CalibrationV114Error("v113 usage aggregate drifted")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "turn_records": {name: _record(path) for name, path in turn_paths.items()},
        "usage": measured,
    }


def _task_id(source_task_id: str) -> str:
    return "final_" + sha256_text(f"v114|{source_task_id}")[:24]


def build_v114_inputs(v113: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    source = {str(row["task_id"]): row for row in v113["values"]["input"]["tasks"]}
    truth = {str(row["task_id"]): row for row in v113["values"]["truth"]["tasks"]}
    roles = [row["role"] for row in truth.values()]
    if roles.count("matched_control") != 4 or roles.count("failed_settled_control") != 1 or roles.count("observable_permutation_disagreement") != 1:
        raise JudgeV5CalibrationV114Error("v113 role coverage drifted")
    model_tasks = []
    truth_rows = []
    for source_id, row in truth.items():
        target_id = _task_id(source_id)
        task = deepcopy(source[source_id])
        task["task_id"] = target_id
        model_tasks.append(task)
        truth_rows.append(
            {
                "task_id": target_id,
                "source_task_id": source_id,
                "role": row["role"],
                "field": row["field"],
                "control_expected_status": (
                    row["control_expected_status"]
                    if row["role"] == "matched_control"
                    else None
                ),
            }
        )
    model_tasks.sort(key=lambda row: row["task_id"])
    truth_rows.sort(key=lambda row: row["task_id"])
    value = {
        "schema_version": V114_INPUT_VERSION,
        "task_count": 6,
        "tasks": model_tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
    }
    truth_value = {
        "schema_version": V114_TRUTH_VERSION,
        "task_count": 6,
        "tasks": truth_rows,
    }
    selection = {
        "schema_version": V114_SELECTION_VERSION,
        "created_at": now_iso(),
        "task_count": 6,
        "role_counts": {
            "matched_control": 4,
            "fixture_truth_dispute": 1,
            "permutation_dispute": 1,
        },
        "control_status_counts": {"correct": 2, "incorrect": 2},
        "dispute_fields": ["evidence", "unsupported_inference"],
        "final_owner_accepts_either_nonabstain_dispute_status": True,
        "selection_uses_source_text": False,
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "majority_voting_used": False,
        "privacy": "aggregate_counts_and_field_enums_only",
    }
    return value, truth_value, selection


def build_prompt_v114(value: Mapping[str, Any]) -> str:
    return (
        "Return one independent final field decision for every opaque task_id. Judge only the requested "
        "field after mentally repairing all other fields. Every source_evidence_span must be an exact "
        "substring of that task's source_excerpt. Do not compare tasks or emit whole-event verdicts.\n\n"
        + json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        + "\n"
    )


def score_v114(output: Mapping[str, Any], truth: Mapping[str, Any]) -> dict[str, Any]:
    expected = {str(row["task_id"]): row for row in truth["tasks"]}
    observed = {str(row["task_id"]): row for row in output["decisions"]}
    if set(expected) != set(observed):
        raise JudgeV5CalibrationV114Error("v114 score coverage drifted")
    controls = [row for row in expected.values() if row["role"] == "matched_control"]
    fixture = [row for row in expected.values() if row["role"] == "failed_settled_control"]
    permutation = [row for row in expected.values() if row["role"] == "observable_permutation_disagreement"]
    if len(controls) != 4 or len(fixture) != 1 or len(permutation) != 1:
        raise JudgeV5CalibrationV114Error("v114 role coverage drifted")
    control_exact = sum(
        observed[row["task_id"]]["field_status"] == row["control_expected_status"]
        for row in controls
    )
    fixture_status = observed[fixture[0]["task_id"]]["field_status"]
    permutation_status = observed[permutation[0]["task_id"]]["field_status"]
    abstentions = sum(row["field_status"] == "abstain" for row in observed.values())
    evidence_complete = sum(bool(row["source_evidence_spans"]) for row in observed.values())
    checks = {
        "matched_control_exact_rate": control_exact == 4,
        "fixture_truth_dispute_decided": fixture_status in {"correct", "incorrect"},
        "permutation_dispute_decided": permutation_status in {"correct", "incorrect"},
        "abstention_count": abstentions == 0,
        "evidence_complete_rate": evidence_complete == 6,
    }
    passed = all(checks.values())
    return {
        "schema_version": V114_SCORE_VERSION,
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
        "fixture_truth_dispute_status": fixture_status if passed else None,
        "permutation_dispute_status": permutation_status if passed else None,
        "reference_patch_authorized": passed,
        "contested_field_reference_owner_authorized": passed,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "majority_voting_used": False,
    }


def build_resolution(score: Mapping[str, Any]) -> dict[str, Any]:
    if not score["passed"]:
        raise JudgeV5CalibrationV114Error("v114 failed score cannot resolve disputes")
    return {
        "schema_version": V114_RESOLUTION_VERSION,
        "created_at": now_iso(),
        "state": "authorized",
        "fixture_truth_dispute_field": "unsupported_inference",
        "fixture_truth_dispute_final_status": score["fixture_truth_dispute_status"],
        "permutation_dispute_field": "evidence",
        "permutation_dispute_final_status": score["permutation_dispute_status"],
        "reference_change_required": score["fixture_truth_dispute_status"] == "correct",
        "basis": "final_side_free_terra_owner_after_four_exact_controls",
        "final_owner_status_was_not_required_to_match_prior_truth_or_prior_models": True,
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
        "schema_version": V114_CAPACITY_AUDIT_VERSION,
        "phase_id": V114_PHASE_ID,
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
        "schema_version": V114_CAPACITY_POLICY_VERSION,
        "phase_id": V114_PHASE_ID,
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


def freeze_v114(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v114 terminal")}
    v113 = _validate_v113()
    value, truth, selection = build_v114_inputs(v113)
    input_path = root / "final-owner-input.private.json"
    truth_path = root / "final-owner-truth.private.json"
    selection_path = root / "selection-audit.json"
    _write_immutable(input_path, value)
    _write_immutable(truth_path, truth)
    _write_stable_time(selection_path, selection, "created_at")
    prompt = build_prompt_v114(value)
    schema = output_schema(value)
    paths = _freeze_turn_request(
        root=root, turn_name=TURN_NAME, input_value=value, prompt=prompt, schema=schema
    )
    predecessor = {**v113["records"], "turn": v113["turn_records"]}
    capacity = _build_capacity_policy(root, predecessor)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V114_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "final_side_free_owner_accepts_either_dispute_verdict_after_exact_controls",
        "task_count": 6,
        "dispute_count": 2,
        "matched_control_count": 4,
        "turn_plan": [TURN_NAME],
        "prior_labels_in_model_input": False,
        "prior_model_decisions_in_model_input": False,
        "majority_voting_used": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "four_exact_controls_both_disputes_nonabstain_all_evidence",
        "reference_patch_authorized": False,
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
            _record(runtime_dir / "app_server_judge_v5_calibration_v113_capped_field_adjudication.py"),
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
    spec_path = root / "final-owner-spec.json"
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
            measured = _validate_usage(_load_json(Path(record["path"]), "v114 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V114_FAILURE_VERSION,
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
        "schema_version": V114_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "reference_patch_authorized": False,
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


async def run_v114(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v114 terminal")
    frozen = freeze_v114(output_dir=root, timeout_seconds=timeout_seconds)
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
        output_path = root / "final-owner-output.private.json"
        _write_immutable(output_path, output)
        score = score_v114(output, frozen["truth"])
        score_path = root / "final-owner-score.json"
        _write_immutable(score_path, score)
        resolution_path = root / "resolution.json"
        if score["passed"]:
            _write_stable_time(resolution_path, build_resolution(score), "created_at")
        accounting = _aggregate_usage([sidecar])
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V114_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v114_final_field_owner_passed_contested_owner_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v114_final_field_owner_passed"
                if passed
                else "v114_final_field_owner_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "reference_patch_authorized": passed,
            "contested_field_reference_owner_authorized": passed,
            "fresh_full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(score_path),
            "output": _record(output_path),
            "resolution": _record(resolution_path) if passed else None,
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
    parser = argparse.ArgumentParser(description="Run v114 final field owner")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(run_v114(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds))
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "reference_patch_authorized": terminal.get("reference_patch_authorized", False),
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
