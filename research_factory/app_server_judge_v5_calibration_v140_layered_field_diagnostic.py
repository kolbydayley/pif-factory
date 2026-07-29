from __future__ import annotations

"""Two-case layered support-first and field-specific Sol diagnostic."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v139_sol_low_pointwise_diagnostic as v139
from .app_server_judge_v5 import (
    CHECKLIST_FIELDS,
    build_pointwise_support_prompt,
    pointwise_support_base_instructions,
    pointwise_support_output_schema,
    validate_pointwise_support_output,
)
from .app_server_judge_v5_calibration import pointwise_input_subset
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
    _field_contracts,
    output_schema,
    validate_output,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_calibration_v99_refined_luna_diagnostic import (
    _general_requested_field_value,
)
from .app_server_judge_v5_calibration_v115_full_contested_field_owner import (
    base_instructions_v115,
    build_prompt_v115,
)
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _validate_usage,
)
from .app_server_judge_v5_fixture import compact_empty_event_fields
from .util import now_iso, sha256_text


V140_INPUT_VERSION = "pif_app_server_judge_v5_4_v140_field_input_v1"
V140_TRUTH_VERSION = "pif_app_server_judge_v5_4_v140_truth_v1"
V140_SELECTION_VERSION = "pif_app_server_judge_v5_4_v140_selection_v1"
V140_SPEC_VERSION = "pif_app_server_judge_v5_4_v140_spec_v1"
V140_SCORE_VERSION = "pif_app_server_judge_v5_4_v140_score_v1"
V140_OUTPUT_VERSION = "pif_app_server_judge_v5_4_v140_output_v1"
V140_FAILURE_VERSION = "pif_app_server_judge_v5_4_v140_failure_v1"
V140_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v140_terminal_v1"
V140_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V140_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V140_PHASE_ID = "judge_v5_4_v140_layered_field_diagnostic"

MODEL = "gpt-5.6-sol"
EFFORT = "high"
CASE_COUNT = 2
SUPPORT_TURNS = ("layered_support_primary", "layered_support_order_canary")
FIELD_PRIMARY_TURNS = tuple(f"layered_{field}_primary" for field in CHECKLIST_FIELDS)
FIELD_CANARY_TURNS = tuple(f"layered_{field}_order_canary" for field in CHECKLIST_FIELDS)
TURN_NAMES = SUPPORT_TURNS + FIELD_PRIMARY_TURNS + FIELD_CANARY_TURNS
TIMEOUT_SECONDS = 900.0
DEFAULT_OUTPUT_ROOT = (
    v139.DEFAULT_OUTPUT_ROOT.parent
    / "judge-calibration-v5_4-v140-layered-field-diagnostic"
).resolve()


class JudgeV5CalibrationV140Error(RuntimeError):
    """The v140 layered field diagnostic contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v139() -> dict[str, Any]:
    root = v139.DEFAULT_OUTPUT_ROOT
    paths = {
        "spec": root / "sol-low-pointwise-spec.json",
        "terminal": root / "terminal.json",
        "score": root / "sol-low-pointwise-score.json",
        "pointwise": root / "sol-low-pointwise-output.private.json",
        "canary": root / "sol-low-order-canary.private.json",
        "truth": root / "sol-low-pointwise-truth.private.json",
        "input": root / "pointwise-input.private.json",
        "selection": root / "selection-audit.json",
    }
    values = {name: _load_json(path, f"v139 {name}") for name, path in paths.items()}
    terminal, score, spec = values["terminal"], values["score"], values["spec"]
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("development_terminal_reason")
        != "v139_sol_low_pointwise_quality_gate_not_passed"
        or terminal.get("expanded_sol_low_pointwise_diagnostic_authorized") is not False
        or terminal.get("fresh_full_calibration_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 393290
        or score.get("passed") is not False
        or score.get("failed_checks")
        != [
            "order_bias",
            "pointwise_field_issue_f1",
            "structured_field_accuracy",
            "support_sensitivity",
        ]
        or score.get("metrics", {}).get("support_sensitivity") != 0.935484
        or score.get("metrics", {}).get("support_specificity") != 1.0
        or score.get("metrics", {}).get("structured_field_accuracy") != 0.764706
        or score.get("metrics", {}).get("pointwise_field_issue_f1") != 0.61244
        or score.get("metrics", {}).get("canary_case_exact_count") != 1
        or spec.get("model") != "gpt-5.6-sol"
        or spec.get("reasoning_effort") != "low"
        or spec.get("retry_count_per_turn") != 0
        or spec.get("selection_authorized") is not False
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5CalibrationV140Error("v139 predecessor contract drifted")
    if not all(_verify_record(record) for record in spec.get("runtime_files") or []):
        raise JudgeV5CalibrationV140Error("v139 runtime record drifted")
    for name, key in {
        "score": "score",
        "pointwise": "pointwise_output",
        "canary": "canary_output",
    }.items():
        record = terminal.get(key)
        if record != _record(paths[name]) or not _verify_record(record):
            raise JudgeV5CalibrationV140Error(f"v139 {name} record drifted")
    attempts = {}
    usage = {field: 0 for field in USAGE_FIELDS}
    for turn_name in spec["turn_plan"]:
        turn_root = root / "turns" / turn_name.replace("_", "-")
        records = {}
        for name, filename in {
            "capacity": "capacity.json",
            "sidecar": "sidecar.json",
            "output": "output.private.json",
        }.items():
            path = turn_root / filename
            if not path.is_file():
                raise JudgeV5CalibrationV140Error("v139 turn coverage is incomplete")
            records[name] = _record(path)
        measured = _validate_usage(
            _load_json(Path(records["sidecar"]["path"]), "v139 sidecar")
        )
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
        attempts[turn_name] = records
    if usage != terminal["usage"]:
        raise JudgeV5CalibrationV140Error("v139 usage aggregate drifted")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "attempts": attempts,
        "usage": usage,
        "v138": v139._validate_v138(),
    }


def _field_task_id(case_id: str, witness_id: str, field: str) -> str:
    return "field_" + sha256_text(f"v140|{case_id}|{witness_id}|{field}")[:24]


def _field_input(field: str, tasks: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": V140_INPUT_VERSION,
        "field_focus": field,
        "task_count": len(tasks),
        "tasks": deepcopy(list(tasks)),
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "disagreement_reasons_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
    }


def build_v140_inputs(
    predecessor: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, dict[str, Any]]]:
    reference = predecessor["v138"]["values"]["reference"]
    candidate_ids = list(predecessor["values"]["selection"]["selected"])
    candidate_ids.sort(key=lambda case_id: sha256_text(f"v140|candidate|{case_id}"))
    unsupported = [
        case_id
        for case_id in candidate_ids
        if "unsupported" in set(reference["cases"][case_id]["proposition"].values())
    ]
    if not unsupported:
        raise JudgeV5CalibrationV140Error("v140 unsupported case coverage drifted")
    first = unsupported[0]
    first_case = reference["cases"][first]
    second_candidates = [
        case_id
        for case_id in candidate_ids
        if case_id != first
        and set(reference["cases"][case_id]["proposition"].values()) == {"supported"}
        and reference["cases"][case_id]["shape"] != first_case["shape"]
        and len(first_case["proposition"])
        + len(reference["cases"][case_id]["proposition"])
        <= 6
    ]
    if not second_candidates:
        raise JudgeV5CalibrationV140Error("v140 supported control case coverage drifted")
    selected = [first, second_candidates[0]]
    selected_input = pointwise_input_subset(predecessor["values"]["input"], selected)
    witness_count = len(selected_input["units"])
    if witness_count > 6 or witness_count < 4:
        raise JudgeV5CalibrationV140Error("v140 witness count is outside the frozen bound")
    expected = deepcopy(reference)
    expected["schema_version"] = V140_TRUTH_VERSION
    expected["cases"] = {
        case_id: deepcopy(reference["cases"][case_id]) for case_id in selected
    }
    expected["diagnostic_source"] = "two_v139_cases_one_unsupported_one_supported_control"
    contracts = _field_contracts()
    field_inputs = {}
    truth_rows = []
    for field in CHECKLIST_FIELDS:
        tasks = []
        for unit in selected_input["units"]:
            event = compact_empty_event_fields(deepcopy(unit["structured_event"]))
            task_id = _field_task_id(unit["case_id"], unit["witness_id"], field)
            tasks.append(
                {
                    "task_id": task_id,
                    "field": field,
                    "field_contract": deepcopy(contracts[field]),
                    "requested_field_value": _general_requested_field_value(field, event),
                    "source_excerpt": unit["source_excerpt"],
                    "structured_event": event,
                }
            )
            truth_rows.append(
                {
                    "task_id": task_id,
                    "case_id": unit["case_id"],
                    "witness_id": unit["witness_id"],
                    "field": field,
                    "expected_status": (
                        "incorrect"
                        if field
                        in expected["cases"][unit["case_id"]]["field_issues"][
                            unit["witness_id"]
                        ]
                        else "correct"
                    ),
                }
            )
        tasks.sort(key=lambda task: sha256_text(f"v140|task-order|{field}|{task['task_id']}"))
        field_inputs[field] = _field_input(field, tasks)
    truth = {
        "schema_version": V140_TRUTH_VERSION,
        "case_count": CASE_COUNT,
        "witness_count": witness_count,
        "field_decision_count": witness_count * len(CHECKLIST_FIELDS),
        "cases": expected["cases"],
        "field_tasks": truth_rows,
    }
    selection = {
        "schema_version": V140_SELECTION_VERSION,
        "created_at": now_iso(),
        "candidate_case_count": len(candidate_ids),
        "selected_case_count": CASE_COUNT,
        "selected": selected,
        "selected_shapes": [reference["cases"][case_id]["shape"] for case_id in selected],
        "unsupported_case_count": 1,
        "supported_control_case_count": 1,
        "witness_count": witness_count,
        "field_count": len(CHECKLIST_FIELDS),
        "support_primary_turn_count": 1,
        "support_order_canary_turn_count": 1,
        "field_primary_turn_count": len(CHECKLIST_FIELDS),
        "field_order_canary_turn_count": len(CHECKLIST_FIELDS),
        "canary_marker_in_model_input": False,
        "selection_uses_source_text": False,
        "selection_uses_only_frozen_reference_labels_and_provenance": True,
        "model_outputs_used_for_selection": False,
        "privacy": "opaque_case_ids_and_aggregate_counts_only",
    }
    return selection, truth, selected_input, field_inputs


def _ratio(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 6) if denominator else 0.0


def _f1(tp: int, fp: int, fn: int) -> float:
    denominator = 2 * tp + fp + fn
    return round(2 * tp / denominator, 6) if denominator else 1.0


def score_v140(
    *,
    support: Mapping[str, Any],
    support_canary: Mapping[str, Any],
    fields: Mapping[str, Any],
    field_canary: Mapping[str, Any],
    truth: Mapping[str, Any],
) -> dict[str, Any]:
    support_rows = {str(row["witness_id"]): row for row in support["units"]}
    repeated_support = {
        str(row["witness_id"]): row for row in support_canary["units"]
    }
    expected_fields = {str(row["task_id"]): row for row in truth["field_tasks"]}
    observed_fields = {str(row["task_id"]): row for row in fields["decisions"]}
    repeated_fields = {
        str(row["task_id"]): row for row in field_canary["decisions"]
    }
    witness_ids = {
        witness_id
        for case in truth["cases"].values()
        for witness_id in case["proposition"]
    }
    if (
        set(support_rows) != witness_ids
        or set(repeated_support) != witness_ids
        or set(observed_fields) != set(expected_fields)
        or set(repeated_fields) != set(expected_fields)
    ):
        raise JudgeV5CalibrationV140Error("v140 score coverage drifted")
    tp = fn = tn = fp = 0
    support_abstentions = 0
    for case in truth["cases"].values():
        for witness_id, wanted in case["proposition"].items():
            got = support_rows[witness_id]["proposition_verdict"]
            support_abstentions += int(got == "abstain")
            if wanted == "supported" and got == "supported":
                tp += 1
            elif wanted == "supported":
                fn += 1
            elif got == "unsupported":
                tn += 1
            else:
                fp += 1
    support_canary_exact = sum(
        support_rows[witness_id]["proposition_verdict"]
        == repeated_support[witness_id]["proposition_verdict"]
        for witness_id in witness_ids
    )
    field_correct = 0
    field_tp = field_fp = field_fn = 0
    field_abstentions = 0
    predicted_issues: dict[str, set[str]] = {witness_id: set() for witness_id in witness_ids}
    for task_id, expected in expected_fields.items():
        status = observed_fields[task_id]["field_status"]
        field_abstentions += int(status == "abstain")
        field_correct += int(status == expected["expected_status"])
        wanted = expected["expected_status"] == "incorrect"
        got = status == "incorrect"
        if wanted and got:
            field_tp += 1
        elif wanted:
            field_fn += 1
        elif got:
            field_fp += 1
        if got:
            predicted_issues[expected["witness_id"]].add(expected["field"])
    field_canary_exact = sum(
        observed_fields[task_id]["field_status"]
        == repeated_fields[task_id]["field_status"]
        for task_id in expected_fields
    )
    structured_correct = 0
    for case in truth["cases"].values():
        for witness_id, expected_status in case["structured_fields"].items():
            got_status = "incorrect" if predicted_issues[witness_id] else "correct"
            structured_correct += int(got_status == expected_status)
    field_total = len(expected_fields)
    witness_total = len(witness_ids)
    metrics = {
        "case_count": CASE_COUNT,
        "witness_count": witness_total,
        "support_sensitivity": _ratio(tp, tp + fn),
        "support_specificity": _ratio(tn, tn + fp),
        "support_abstention_count": support_abstentions,
        "support_canary_decision_count": witness_total,
        "support_canary_exact_count": support_canary_exact,
        "field_decision_count": field_total,
        "field_decision_accuracy": _ratio(field_correct, field_total),
        "pointwise_field_issue_f1": _f1(field_tp, field_fp, field_fn),
        "structured_field_accuracy": _ratio(structured_correct, witness_total),
        "field_abstention_count": field_abstentions,
        "field_canary_decision_count": field_total,
        "field_canary_exact_count": field_canary_exact,
    }
    checks = {
        "support_sensitivity": metrics["support_sensitivity"] >= 0.95,
        "support_specificity": metrics["support_specificity"] >= 0.95,
        "support_abstention_count": support_abstentions == 0,
        "support_order_canary_exact_rate": support_canary_exact == witness_total,
        "field_decision_accuracy": metrics["field_decision_accuracy"] >= 0.95,
        "pointwise_field_issue_f1": metrics["pointwise_field_issue_f1"] >= 0.95,
        "structured_field_accuracy": metrics["structured_field_accuracy"] >= 0.95,
        "field_abstention_count": field_abstentions == 0,
        "field_order_canary_exact_rate": field_canary_exact == field_total,
        "schema_and_exact_evidence_validation": True,
    }
    passed = all(checks.values())
    return {
        "schema_version": V140_SCORE_VERSION,
        "passed": passed,
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "metrics": metrics,
        "expanded_layered_field_diagnostic_authorized": passed,
        "alignment_diagnostic_authorized": False,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V140_CAPACITY_AUDIT_VERSION,
        "phase_id": V140_PHASE_ID,
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
        "schema_version": V140_CAPACITY_POLICY_VERSION,
        "phase_id": V140_PHASE_ID,
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


def freeze_v140(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v140 terminal")}
    predecessor = _validate_v139()
    selection, truth, support_input, field_inputs = build_v140_inputs(predecessor)
    selection_path = root / "selection-audit.json"
    truth_path = root / "layered-field-truth.private.json"
    support_path = root / "support-input.private.json"
    _write_stable_time(selection_path, selection, "created_at")
    _write_immutable(truth_path, truth)
    _write_immutable(support_path, support_input)
    turns = []
    support_values = [support_input, deepcopy(support_input)]
    support_values[1]["units"] = list(reversed(support_values[1]["units"]))
    for turn_name, role, value in zip(
        SUPPORT_TURNS,
        ("support_primary", "support_order_canary"),
        support_values,
        strict=True,
    ):
        prompt = build_pointwise_support_prompt(value)
        schema = pointwise_support_output_schema(value)
        paths = _freeze_turn_request(
            root=root,
            turn_name=turn_name,
            input_value=value,
            prompt=prompt,
            schema=schema,
        )
        turns.append(
            {
                "turn_name": turn_name,
                "turn_role": role,
                "field": None,
                "value": value,
                "prompt": prompt,
                "schema": schema,
                "paths": paths,
            }
        )
    for field, turn_name in zip(CHECKLIST_FIELDS, FIELD_PRIMARY_TURNS, strict=True):
        value = field_inputs[field]
        prompt, schema = build_prompt_v115(value), output_schema(value)
        paths = _freeze_turn_request(
            root=root,
            turn_name=turn_name,
            input_value=value,
            prompt=prompt,
            schema=schema,
        )
        turns.append(
            {
                "turn_name": turn_name,
                "turn_role": "field_primary",
                "field": field,
                "value": value,
                "prompt": prompt,
                "schema": schema,
                "paths": paths,
            }
        )
    for field, turn_name in zip(CHECKLIST_FIELDS, FIELD_CANARY_TURNS, strict=True):
        value = _field_input(field, list(reversed(field_inputs[field]["tasks"])))
        prompt, schema = build_prompt_v115(value), output_schema(value)
        paths = _freeze_turn_request(
            root=root,
            turn_name=turn_name,
            input_value=value,
            prompt=prompt,
            schema=schema,
        )
        turns.append(
            {
                "turn_name": turn_name,
                "turn_role": "field_order_canary",
                "field": field,
                "value": value,
                "prompt": prompt,
                "schema": schema,
                "paths": paths,
            }
        )
    predecessor_records = {
        **{f"v139_{name}": record for name, record in predecessor["records"].items()},
        "v139_attempts": predecessor["attempts"],
        "v138_reference": predecessor["v138"]["records"]["reference"],
    }
    capacity = _build_capacity_policy(root, predecessor_records)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V140_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "two_case_support_first_then_fifteen_field_specific_sol_turns_with_unmarked_reversed_canaries",
        "case_count": CASE_COUNT,
        "witness_count": truth["witness_count"],
        "field_count": len(CHECKLIST_FIELDS),
        "turn_plan": list(TURN_NAMES),
        "support_turn_count": len(SUPPORT_TURNS),
        "field_primary_turn_count": len(FIELD_PRIMARY_TURNS),
        "field_order_canary_turn_count": len(FIELD_CANARY_TURNS),
        "maximum_tasks_per_turn": truth["witness_count"],
        "reference_truth_exposed_to_model": False,
        "canary_marker_in_model_input": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "all_support_field_abstention_exact_evidence_and_exact_order_gates",
        "expanded_layered_field_diagnostic_authorized": False,
        "alignment_diagnostic_authorized": False,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor_records,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v139_sol_low_pointwise_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v115_full_contested_field_owner.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5.py"),
            _record(runtime_dir / "app_server_judge_v5_fixture.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_inputs": {
            "selection": _record(selection_path),
            "truth": _record(truth_path),
            "support": _record(support_path),
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
    spec_path = root / "layered-field-diagnostic-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "truth": truth,
    }


def _merge_field_outputs(outputs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    decisions = [deepcopy(row) for output in outputs for row in output["decisions"]]
    task_ids = [str(row["task_id"]) for row in decisions]
    if len(task_ids) != len(set(task_ids)):
        raise JudgeV5CalibrationV140Error("v140 field outputs overlap")
    return {"schema_version": V140_OUTPUT_VERSION, "decisions": decisions}


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
            measured = _validate_usage(
                _load_json(Path(record["path"]), "v140 sidecar")
            )
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V140_FAILURE_VERSION,
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
        "schema_version": V140_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "expanded_layered_field_diagnostic_authorized": False,
        "alignment_diagnostic_authorized": False,
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


async def run_v140(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v140 terminal")
    frozen = freeze_v140(output_dir=root, timeout_seconds=timeout_seconds)
    current_turn: Optional[str] = None
    try:
        support = support_canary = None
        field_outputs, field_canary_outputs, sidecars = [], [], []
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            for turn in frozen["turns"]:
                current_turn = turn["turn_name"]
                is_support = turn["turn_role"].startswith("support_")
                output, sidecar, _ = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=turn["paths"],
                    prompt=turn["prompt"],
                    schema=turn["schema"],
                    base_instructions=(
                        pointwise_support_base_instructions()
                        if is_support
                        else base_instructions_v115()
                    ),
                    model=MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=(
                        len(turn["value"]["units"])
                        if is_support
                        else len(turn["value"]["tasks"])
                    ),
                    policy_path=frozen["capacity_policy"],
                    output_validator=(
                        (lambda candidate, item=turn["value"]: validate_pointwise_support_output(candidate, item))
                        if is_support
                        else (lambda candidate, item=turn["value"]: validate_output(candidate, item))
                    ),
                )
                sidecars.append(sidecar)
                if turn["turn_role"] == "support_primary":
                    support = output
                elif turn["turn_role"] == "support_order_canary":
                    support_canary = output
                elif turn["turn_role"] == "field_primary":
                    field_outputs.append(output)
                else:
                    field_canary_outputs.append(output)
        if support is None or support_canary is None:
            raise JudgeV5CalibrationV140Error("v140 support output coverage drifted")
        fields = _merge_field_outputs(field_outputs)
        field_canary = _merge_field_outputs(field_canary_outputs)
        score = score_v140(
            support=support,
            support_canary=support_canary,
            fields=fields,
            field_canary=field_canary,
            truth=frozen["truth"],
        )
        paths = {
            "support": root / "support-output.private.json",
            "support_canary": root / "support-order-canary.private.json",
            "fields": root / "field-output.private.json",
            "field_canary": root / "field-order-canary.private.json",
            "score": root / "layered-field-score.json",
        }
        _write_immutable(paths["support"], support)
        _write_immutable(paths["support_canary"], support_canary)
        _write_immutable(paths["fields"], fields)
        _write_immutable(paths["field_canary"], field_canary)
        _write_immutable(paths["score"], score)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V140_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v140_layered_field_diagnostic_passed_expansion_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v140_layered_support_field_quality_gates_passed"
                if passed
                else "v140_layered_support_field_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "expanded_layered_field_diagnostic_authorized": passed,
            "alignment_diagnostic_authorized": False,
            "fresh_full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(paths["score"]),
            "support_output": _record(paths["support"]),
            "support_canary": _record(paths["support_canary"]),
            "field_output": _record(paths["fields"]),
            "field_canary": _record(paths["field_canary"]),
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v140 layered field diagnostic")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v140(
            output_dir=Path(args.output_dir),
            timeout_seconds=args.timeout_seconds,
        )
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "expanded_layered_field_diagnostic_authorized": terminal.get(
                    "expanded_layered_field_diagnostic_authorized", False
                ),
                "usage_status": terminal.get("usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
