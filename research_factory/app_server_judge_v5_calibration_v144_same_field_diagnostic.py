from __future__ import annotations

"""Same-field recovery for the mixed-field failures observed in v143."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v142_corrected_field_reference_owner as v142
from . import app_server_judge_v5_calibration_v143_corrected_layered_diagnostic as v143
from .app_server_judge_v5 import CHECKLIST_FIELDS
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
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _validate_usage,
)
from .util import now_iso


V144_INPUT_VERSION = "pif_app_server_judge_v5_4_v144_same_field_input_v1"
V144_TAXONOMY_VERSION = "pif_app_server_judge_v5_4_v144_error_taxonomy_v1"
V144_SELECTION_VERSION = "pif_app_server_judge_v5_4_v144_selection_v1"
V144_SPEC_VERSION = "pif_app_server_judge_v5_4_v144_spec_v1"
V144_FAILURE_VERSION = "pif_app_server_judge_v5_4_v144_failure_v1"
V144_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v144_terminal_v1"
V144_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V144_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V144_PHASE_ID = "judge_v5_4_v144_same_field_diagnostic"

MODEL = "gpt-5.5"
EFFORT = "high"
PRIMARY_TURNS = tuple(f"same_field_{field}_primary" for field in CHECKLIST_FIELDS)
CANARY_TURNS = tuple(f"same_field_{field}_order_canary" for field in CHECKLIST_FIELDS)
TURN_NAMES = PRIMARY_TURNS + CANARY_TURNS
TIMEOUT_SECONDS = 900.0
DEFAULT_OUTPUT_ROOT = (
    v143.DEFAULT_OUTPUT_ROOT.parent
    / "judge-calibration-v5_4-v144-same-field-diagnostic"
).resolve()


class JudgeV5CalibrationV144Error(RuntimeError):
    """The v144 same-field recovery contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v143() -> dict[str, Any]:
    root = v143.DEFAULT_OUTPUT_ROOT
    paths = {
        "spec": root / "corrected-layered-diagnostic-spec.json",
        "terminal": root / "terminal.json",
        "score": root / "corrected-layered-score.json",
        "support": root / "support-output.private.json",
        "support_canary": root / "support-order-canary.private.json",
        "fields": root / "field-output.private.json",
        "field_canary": root / "field-order-canary.private.json",
        "truth": root / "corrected-layered-truth.private.json",
        "selection": root / "selection-audit.json",
    }
    values = {name: _load_json(path, f"v143 {name}") for name, path in paths.items()}
    terminal, score, spec = values["terminal"], values["score"], values["spec"]
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("development_terminal_reason")
        != "v143_corrected_support_field_quality_gate_not_passed"
        or terminal.get("corrected_alignment_diagnostic_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 344273
        or score.get("passed") is not False
        or score.get("failed_checks")
        != [
            "field_decision_accuracy",
            "field_order_canary_exact_rate",
            "pointwise_field_issue_f1",
            "structured_field_accuracy",
        ]
        or score.get("metrics", {}).get("support_sensitivity") != 1.0
        or score.get("metrics", {}).get("support_specificity") != 1.0
        or score.get("metrics", {}).get("support_canary_exact_count") != 12
        or score.get("metrics", {}).get("field_decision_accuracy") != 0.666667
        or score.get("metrics", {}).get("pointwise_field_issue_f1") != 0.615385
        or score.get("metrics", {}).get("field_canary_exact_count") != 25
        or spec.get("support_model") != "gpt-5.6-sol"
        or spec.get("field_model") != "gpt-5.5"
        or spec.get("turn_plan") != list(v143.TURN_NAMES)
        or spec.get("retry_count_per_turn") != 0
        or spec.get("selection_authorized") is not False
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5CalibrationV144Error("v143 predecessor contract drifted")
    if not all(_verify_record(record) for record in spec.get("runtime_files") or []):
        raise JudgeV5CalibrationV144Error("v143 runtime record drifted")
    for name, key in {
        "score": "score",
        "support": "support_output",
        "support_canary": "support_canary",
        "fields": "field_output",
        "field_canary": "field_canary",
    }.items():
        record = terminal.get(key)
        if record != _record(paths[name]) or not _verify_record(record):
            raise JudgeV5CalibrationV144Error(f"v143 {name} record drifted")
    frozen_turns = {row["turn_name"]: row for row in spec["frozen_inputs"]["turns"]}
    input_tasks = {}
    attempts = {}
    usage = {field: 0 for field in USAGE_FIELDS}
    for turn_name in spec["turn_plan"]:
        frozen = frozen_turns[turn_name]
        if frozen["role"] == "field_primary":
            value = _load_json(Path(frozen["input"]["path"]), "v143 field input")
            for task in value["tasks"]:
                input_tasks[str(task["task_id"])] = task
        turn_root = root / "turns" / turn_name.replace("_", "-")
        records = {}
        for name, filename in {
            "capacity": "capacity.json",
            "sidecar": "sidecar.json",
            "output": "output.private.json",
        }.items():
            path = turn_root / filename
            if not path.is_file():
                raise JudgeV5CalibrationV144Error("v143 turn coverage is incomplete")
            records[name] = _record(path)
        measured = _validate_usage(
            _load_json(Path(records["sidecar"]["path"]), "v143 sidecar")
        )
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
        attempts[turn_name] = records
    if len(input_tasks) != 30 or usage != terminal["usage"]:
        raise JudgeV5CalibrationV144Error("v143 task or usage aggregate drifted")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "attempts": attempts,
        "usage": usage,
        "input_tasks": input_tasks,
        "v142": v143._validate_v142(),
    }


def build_error_taxonomy_v144(predecessor: Mapping[str, Any]) -> dict[str, Any]:
    truth = {row["task_id"]: row for row in predecessor["values"]["truth"]["field_tasks"]}
    primary = {row["task_id"]: row for row in predecessor["values"]["fields"]["decisions"]}
    canary = {row["task_id"]: row for row in predecessor["values"]["field_canary"]["decisions"]}
    rows = []
    for field in CHECKLIST_FIELDS:
        selected = [row for row in truth.values() if row["field"] == field]
        rows.append(
            {
                "field": field,
                "decision_count": len(selected),
                "primary_correct_count": sum(
                    primary[row["task_id"]]["field_status"] == row["expected_status"]
                    for row in selected
                ),
                "order_exact_count": sum(
                    primary[row["task_id"]]["field_status"]
                    == canary[row["task_id"]]["field_status"]
                    for row in selected
                ),
                "primary_false_positive_count": sum(
                    row["expected_status"] == "correct"
                    and primary[row["task_id"]]["field_status"] == "incorrect"
                    for row in selected
                ),
                "primary_false_negative_count": sum(
                    row["expected_status"] == "incorrect"
                    and primary[row["task_id"]]["field_status"] != "incorrect"
                    for row in selected
                ),
            }
        )
    return {
        "schema_version": V144_TAXONOMY_VERSION,
        "source_turn_count": len(v143.TURN_NAMES),
        "source_field_decision_count": 30,
        "source_primary_correct_count": 20,
        "source_order_exact_count": 25,
        "source_support_gates_passed": True,
        "likely_causal_class": "mixed_field_batch_context_or_field_specific_semantic_miss",
        "next_falsification": "same_field_two_polarity_tasks_with_unmarked_reversed_order",
        "fields": rows,
        "source_text_included": False,
        "production_mutated": False,
    }


def _field_input(tasks: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": V144_INPUT_VERSION,
        "task_count": len(tasks),
        "tasks": [deepcopy(row) for row in tasks],
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "disagreement_reasons_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
    }


def build_v144_inputs(
    predecessor: Mapping[str, Any]
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    truth = {row["task_id"]: row for row in predecessor["values"]["truth"]["field_tasks"]}
    by_field: dict[str, list[dict[str, Any]]] = {field: [] for field in CHECKLIST_FIELDS}
    for task_id, task in predecessor["input_tasks"].items():
        by_field[truth[task_id]["field"]].append(deepcopy(task))
    rows = []
    for field, primary_turn, canary_turn in zip(
        CHECKLIST_FIELDS, PRIMARY_TURNS, CANARY_TURNS, strict=True
    ):
        tasks = sorted(by_field[field], key=lambda row: str(row["task_id"]))
        if len(tasks) != 2:
            raise JudgeV5CalibrationV144Error("v144 field polarity coverage drifted")
        rows.append(
            {
                "turn_name": primary_turn,
                "turn_role": "field_primary",
                "field": field,
                "value": _field_input(tasks),
            }
        )
        rows.append(
            {
                "turn_name": canary_turn,
                "turn_role": "field_order_canary",
                "field": field,
                "value": _field_input(list(reversed(tasks))),
            }
        )
    rows.sort(key=lambda row: TURN_NAMES.index(row["turn_name"]))
    selection = {
        "schema_version": V144_SELECTION_VERSION,
        "created_at": now_iso(),
        "reused_support_receipt_count": 12,
        "reused_field_task_count": 30,
        "field_enum_count": 15,
        "tasks_per_turn": 2,
        "primary_turn_count": 15,
        "order_canary_turn_count": 15,
        "canary_marker_in_model_input": False,
        "canary_membership_identical": True,
        "canary_order_reversed": True,
        "task_membership_changed_from_v143": False,
        "truth_changed_from_v143": False,
        "selection_uses_source_text": False,
        "selection_uses_only_observable_v143_failure_state_and_field_enums": True,
        "semantic_pruning_performed": False,
        "privacy": "opaque_ids_and_aggregate_counts_only",
    }
    return rows, selection


def _merge_outputs(outputs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    decisions = [deepcopy(row) for output in outputs for row in output["decisions"]]
    if len(decisions) != 30 or len({row["task_id"] for row in decisions}) != 30:
        raise JudgeV5CalibrationV144Error("v144 output coverage drifted")
    return {"schema_version": V144_INPUT_VERSION, "decisions": decisions}


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V144_CAPACITY_AUDIT_VERSION,
        "phase_id": V144_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V144_CAPACITY_POLICY_VERSION,
        "phase_id": V144_PHASE_ID,
        "created_at": now_iso(),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "retry_count_per_turn": 0,
        "production_mutation_allowed": False,
        "rate_limit_reached_type_must_be_null": True,
        "unknown_usage_hard_stop": True,
        "ordered_turn_names": list(TURN_NAMES),
        "minimum_remaining_reserve_percent": 20,
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
        "phase_total_token_bound": bound,
        "projected_phase_quota_points": math.ceil(
            bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
        ),
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v144(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v144 terminal")}
    predecessor = _validate_v143()
    rows, selection = build_v144_inputs(predecessor)
    selection_path = root / "selection-audit.json"
    taxonomy_path = root / "v143-field-error-taxonomy.json"
    _write_stable_time(selection_path, selection, "created_at")
    _write_immutable(taxonomy_path, build_error_taxonomy_v144(predecessor))
    turns = []
    for row in rows:
        value = row["value"]
        prompt, schema = v142.build_prompt_v142(value), output_schema(value)
        paths = _freeze_turn_request(
            root=root, turn_name=row["turn_name"], input_value=value, prompt=prompt, schema=schema
        )
        turns.append({**row, "prompt": prompt, "schema": schema, "paths": paths})
    predecessor_records = {
        **{f"v143_{name}": record for name, record in predecessor["records"].items()},
        "v143_attempts": predecessor["attempts"],
    }
    capacity = _build_capacity_policy(root, predecessor_records)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V144_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "same_field_two_polarity_tasks_with_unmarked_reversed_order",
        "reused_support_receipts": True,
        "reused_support_receipt_count": 12,
        "field_task_count": 30,
        "maximum_tasks_per_turn": 2,
        "turn_plan": list(TURN_NAMES),
        "canary_marker_in_model_input": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "original_v143_frozen_gates_with_reused_support_and_new_same_field_outputs",
        "corrected_alignment_diagnostic_authorized": False,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor_records,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v143_corrected_layered_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v142_corrected_field_reference_owner.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_inputs": {
            "truth": predecessor["records"]["truth"],
            "support": predecessor["records"]["support"],
            "support_canary": predecessor["records"]["support_canary"],
            "selection": _record(selection_path),
            "error_taxonomy": _record(taxonomy_path),
            "reference": predecessor["v142"]["records"]["reference"],
            "field_rubric": predecessor["v142"]["records"]["rubric"],
            "turns": [
                {
                    "turn_name": turn["turn_name"],
                    "role": turn["turn_role"],
                    "field": turn["field"],
                    "input": _record(turn["paths"]["input"]),
                    "prompt": _record(turn["paths"]["prompt"]),
                    "schema": _record(turn["paths"]["schema"]),
                }
                for turn in turns
            ],
        },
        "privacy": "private_source_event_output_no_source_text_in_reports",
    }
    spec_path = root / "same-field-diagnostic-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "v143": predecessor,
    }


def _write_failure(root: Path, turn_name: Optional[str], error_class: str) -> dict[str, Any]:
    attempts = [
        row
        for row in _attempt_records(root)
        if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))
    ]
    usage = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping):
            unknown += 1
            continue
        try:
            measured = _validate_usage(_load_json(Path(record["path"]), "v144 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V144_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": turn_name,
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
        "schema_version": V144_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "corrected_alignment_diagnostic_authorized": False,
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


async def run_v144(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v144 terminal")
    frozen = freeze_v144(output_dir=root, timeout_seconds=timeout_seconds)
    current_turn: Optional[str] = None
    try:
        primary_outputs, canary_outputs, sidecars = [], [], []
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            for turn in frozen["turns"]:
                current_turn = turn["turn_name"]
                output, sidecar, _ = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=turn["paths"],
                    prompt=turn["prompt"],
                    schema=turn["schema"],
                    base_instructions=v142.base_instructions_v142(),
                    model=MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=2,
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate, item=turn["value"]: validate_output(
                        candidate, item
                    ),
                )
                if turn["turn_role"] == "field_primary":
                    primary_outputs.append(output)
                else:
                    canary_outputs.append(output)
                sidecars.append(sidecar)
        fields = _merge_outputs(primary_outputs)
        field_canary = _merge_outputs(canary_outputs)
        score = v143.score_v143(
            support=frozen["v143"]["values"]["support"],
            support_canary=frozen["v143"]["values"]["support_canary"],
            fields=fields,
            field_canary=field_canary,
            truth=frozen["v143"]["values"]["truth"],
        )
        passed = bool(score["passed"])
        paths = {
            "fields": root / "same-field-output.private.json",
            "field_canary": root / "same-field-order-canary.private.json",
            "score": root / "same-field-layered-score.json",
        }
        _write_immutable(paths["fields"], fields)
        _write_immutable(paths["field_canary"], field_canary)
        _write_immutable(paths["score"], score)
        accounting = _aggregate_usage(sidecars)
        terminal = {
            "schema_version": V144_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v144_same_field_diagnostic_passed_alignment_diagnostic_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v144_same_field_support_field_gates_passed"
                if passed
                else "v144_same_field_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "reference_frozen": True,
            "reused_support_receipts": True,
            "corrected_alignment_diagnostic_authorized": passed,
            "fresh_full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(paths["score"]),
            "field_output": _record(paths["fields"]),
            "field_canary": _record(paths["field_canary"]),
            "support_output": frozen["v143"]["records"]["support"],
            "support_canary": frozen["v143"]["records"]["support_canary"],
            "reference": frozen["v143"]["v142"]["records"]["reference"],
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            "predecessor_v143_usage": frozen["v143"]["usage"],
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v144 same-field diagnostic")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v144(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "corrected_alignment_diagnostic_authorized": terminal.get(
                    "corrected_alignment_diagnostic_authorized", False
                ),
                "usage_status": terminal.get("usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
