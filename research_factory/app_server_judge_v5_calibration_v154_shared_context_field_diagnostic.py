from __future__ import annotations

"""Validate shared-context field microtasks before the fresh full calibration."""

import argparse
import asyncio
import itertools
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v143_corrected_layered_diagnostic as v143
from . import app_server_judge_v5_calibration_v145_comprehensive_field_reference_owner as v145
from . import app_server_judge_v5_calibration_v150_fresh_alignment_diagnostic as v150
from . import app_server_judge_v5_calibration_v153_capped_alignment_adjudication as v153
from .app_server_judge_v5 import (
    CHECKLIST_FIELDS,
    build_pointwise_support_input,
    validate_app_server_output_schema_subset,
)
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
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_calibration_v99_refined_luna_diagnostic import (
    _general_requested_field_value,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .app_server_judge_v5_fixture import compact_empty_event_fields
from .util import now_iso, sha256_text


V154_INPUT_VERSION = "pif_app_server_judge_v5_4_v154_shared_context_field_input_v1"
V154_TRUTH_VERSION = "pif_app_server_judge_v5_4_v154_truth_v1"
V154_SELECTION_VERSION = "pif_app_server_judge_v5_4_v154_selection_v1"
V154_SPEC_VERSION = "pif_app_server_judge_v5_4_v154_spec_v1"
V154_SCORE_VERSION = "pif_app_server_judge_v5_4_v154_score_v1"
V154_PROTOCOL_VERSION = "pif_app_server_judge_v5_4_v154_field_adapter_protocol_v1"
V154_FAILURE_VERSION = "pif_app_server_judge_v5_4_v154_failure_v1"
V154_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v154_terminal_v1"
V154_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V154_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V154_PHASE_ID = "judge_v5_4_v154_shared_context_field_diagnostic"

SUPPORT_MODEL = "gpt-5.6-sol"
FIELD_MODEL = "gpt-5.5"
EFFORT = "high"
SUPPORT_TURNS = ("shared_field_support_primary", "shared_field_support_order_canary")
FIELD_PRIMARY_TURNS = ("shared_field_primary_00", "shared_field_primary_01")
FIELD_CANARY_TURN = "shared_field_order_canary"
TURN_NAMES = SUPPORT_TURNS + FIELD_PRIMARY_TURNS + (FIELD_CANARY_TURN,)
FIELD_NAMES = tuple(field for field in CHECKLIST_FIELDS if field != "unsupported_inference")
UNIT_COUNT = 8
UNITS_PER_PRIMARY_TURN = 4
CANARY_UNIT_COUNT = 4
TIMEOUT_SECONDS = 900.0
DEFAULT_OUTPUT_ROOT = (
    v153.DEFAULT_OUTPUT_ROOT.parent
    / "judge-calibration-v5_4-v154-shared-context-field-diagnostic"
).resolve()


class JudgeV5CalibrationV154Error(RuntimeError):
    """The immutable shared-context field diagnostic cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _ratio(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 6) if denominator else 0.0


def _f1(tp: int, fp: int, fn: int) -> float:
    denominator = 2 * tp + fp + fn
    return round(2 * tp / denominator, 6) if denominator else 1.0


def _validate_v153() -> dict[str, Any]:
    root = v153.DEFAULT_OUTPUT_ROOT
    paths = {
        "spec": root / "capped-alignment-adjudication-spec.json",
        "terminal": root / "terminal.json",
        "score": root / "reconciled-alignment-score.json",
        "protocol": root / "alignment-judge-protocol-v153.json",
        "audit": root / "reconciliation-audit.json",
    }
    values = {name: _load_json(path, f"v153 {name}") for name, path in paths.items()}
    terminal, score, spec = values["terminal"], values["score"], values["spec"]
    expected_usage = {
        "input_tokens": 26699,
        "cached_input_tokens": 1920,
        "output_tokens": 9235,
        "reasoning_output_tokens": 3624,
        "total_tokens": 35934,
    }
    if (
        terminal.get("state") != "completed"
        or terminal.get("terminal_reason")
        != "v153_alignment_protocol_frozen_full_development_calibration_authorized"
        or terminal.get("alignment_protocol_frozen") is not True
        or terminal.get("fresh_full_development_calibration_authorized") is not True
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage") != expected_usage
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("adjudication_call_count") != 1
        or terminal.get("majority_voting_used") is not False
        or terminal.get("failed_quality_gates") != []
        or score.get("passed") is not True
        or any(
            score.get("metrics", {}).get(key) != 1.0
            for key in (
                "alignment_f1",
                "equivalence_partition_exact_case_rate",
                "equivalent_sensitivity",
                "exact_case_rate",
                "mismatch_field_f1",
                "non_equivalent_sensitivity",
                "partial_sensitivity",
                "relation_accuracy",
                "unpaired_exact_case_rate",
            )
        )
        or score.get("metrics", {}).get("order_bias") != 0.0
        or score.get("metrics", {}).get("abstention_case_count") != 0
        or score.get("metrics", {}).get("permutation_canary_exact_count") != 12
        or terminal.get("protocol") != _record(paths["protocol"])
        or terminal.get("score") != _record(paths["score"])
        or terminal.get("reconciliation_audit") != _record(paths["audit"])
        or spec.get("turn_plan") != [v153.TURN_NAME]
        or spec.get("retry_count_per_turn") != 0
        or not all(_verify_record(record) for record in spec.get("runtime_files") or [])
    ):
        raise JudgeV5CalibrationV154Error("v153 authorization contract drifted")
    turn_root = root / "turns" / v153.TURN_NAME.replace("_", "-")
    attempts = {
        name: _record(turn_root / filename)
        for name, filename in {
            "capacity": "capacity.json",
            "sidecar": "sidecar.json",
            "output": "output.private.json",
        }.items()
    }
    if any(not _verify_record(record) for record in attempts.values()):
        raise JudgeV5CalibrationV154Error("v153 attempt record drifted")
    if _validate_usage(_load_json(Path(attempts["sidecar"]["path"]), "v153 sidecar")) != expected_usage:
        raise JudgeV5CalibrationV154Error("v153 usage drifted")
    source = v153._validate_v152()
    v106 = v150._validate_v106_sources()
    return {
        "root": root,
        "paths": paths,
        "records": {name: _record(path) for name, path in paths.items()},
        "values": values,
        "usage": expected_usage,
        "attempt": attempts,
        "source": source,
        "v106": v106,
    }


def _all_witness_rows(pool: Mapping[str, Any], reference: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for case in pool["cases"]:
        case_id = str(case["case_id"])
        for side in ("event_set_a", "event_set_b"):
            for witness in case[side]:
                witness_id = str(witness["witness_id"])
                issues = sorted(reference["cases"][case_id]["field_issues"][witness_id])
                rows.append(
                    {
                        "case_id": case_id,
                        "witness_id": witness_id,
                        "support_status": reference["cases"][case_id]["proposition"][witness_id],
                        "field_issues": issues,
                    }
                )
    if len(rows) != 182 or len({row["witness_id"] for row in rows}) != 182:
        raise JudgeV5CalibrationV154Error("v154 source witness coverage drifted")
    return rows


def select_v154_units(pool: Mapping[str, Any], reference: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = _all_witness_rows(pool, reference)
    unsupported = [
        row for row in rows if row["support_status"] == "unsupported" and row["field_issues"]
    ]
    best = None
    for combo in itertools.combinations(unsupported, 4):
        if len({row["case_id"] for row in combo}) != 4:
            continue
        coverage = len({field for row in combo for field in row["field_issues"]})
        rank = (
            -coverage,
            sha256_text("v154|unsupported|" + "|".join(sorted(row["witness_id"] for row in combo))),
        )
        if best is None or rank < best[0]:
            best = (rank, list(combo))
    if best is None:
        raise JudgeV5CalibrationV154Error("v154 unsupported selection unavailable")
    selected = best[1]
    used_cases = {row["case_id"] for row in selected}
    controls = [
        row
        for row in rows
        if row["support_status"] == "supported"
        and not row["field_issues"]
        and row["case_id"] not in used_cases
    ]
    controls.sort(key=lambda row: sha256_text(f"v154|control|{row['case_id']}|{row['witness_id']}"))
    chosen_controls = []
    for row in controls:
        if row["case_id"] in used_cases:
            continue
        chosen_controls.append(row)
        used_cases.add(row["case_id"])
        if len(chosen_controls) == 4:
            break
    selected.extend(chosen_controls)
    selected.sort(key=lambda row: sha256_text(f"v154|unit-order|{row['witness_id']}"))
    if (
        len(selected) != UNIT_COUNT
        or len({row["case_id"] for row in selected}) != UNIT_COUNT
        or sum(row["support_status"] == "supported" for row in selected) != 4
        or sum(bool(row["field_issues"]) for row in selected) != 4
        or len({field for row in selected for field in row["field_issues"]}) < 5
    ):
        raise JudgeV5CalibrationV154Error("v154 selected cohort drifted")
    return selected


def _support_value(units: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": v143.V143_SUPPORT_INPUT_VERSION,
        "unit_count": len(units),
        "units": [
            {
                "case_id": row["case_id"],
                "witness_id": row["witness_id"],
                "proposition": deepcopy(row["proposition"]),
                "source_excerpt": row["source_excerpt"],
            }
            for row in units
        ],
        "side_labels_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
    }


def _field_task_id(case_id: str, witness_id: str, field: str) -> str:
    return "shared_" + sha256_text(f"v154|{case_id}|{witness_id}|{field}")[:24]


def build_field_value(units: Sequence[Mapping[str, Any]], *, permutation: str) -> dict[str, Any]:
    contracts = [
        {"field": field, **deepcopy(v145.FIELD_RULES_V145[field])}
        for field in FIELD_NAMES
    ]
    unit_rows = []
    tasks = []
    for unit in units:
        event = compact_empty_event_fields(deepcopy(unit["structured_event"]))
        unit_rows.append(
            {
                "case_id": unit["case_id"],
                "witness_id": unit["witness_id"],
                "source_excerpt": unit["source_excerpt"],
                "structured_event": event,
            }
        )
        for field in FIELD_NAMES:
            tasks.append(
                {
                    "task_id": _field_task_id(unit["case_id"], unit["witness_id"], field),
                    "case_id": unit["case_id"],
                    "witness_id": unit["witness_id"],
                    "field": field,
                    "requested_field_value": _general_requested_field_value(field, event),
                }
            )
    if permutation == "reversed":
        unit_rows.reverse()
        tasks.reverse()
    value = {
        "schema_version": V154_INPUT_VERSION,
        "permutation": permutation,
        "unit_count": len(unit_rows),
        "task_count": len(tasks),
        "units": unit_rows,
        "field_contracts": contracts,
        "tasks": tasks,
        "prior_labels_present": False,
        "prior_model_decisions_present": False,
        "disagreement_reasons_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
        "shared_context_is_serialization_only": True,
        "unsupported_inference_projected_from_separate_support_pass": True,
    }
    if len(tasks) != len(units) * len(FIELD_NAMES):
        raise JudgeV5CalibrationV154Error("v154 field task coverage drifted")
    return value


def field_instructions_v154() -> str:
    return (
        "You are the neutral final owner for independent blinded source-to-field decisions. Each "
        "task asks about exactly one field under its supplied field_contract. Unit context is shared "
        "only to avoid repeating the same source and event. For every task, mentally correct every "
        "other event field first, then mark incorrect only when the requested field remains "
        "independently wrong. requested_field_value identifies value and presence; structural "
        "absence sentinels are not semantic values. Cite exact source substrings, with the first span "
        "directly licensing the decision. Abstain only when the source genuinely cannot determine "
        "the field. Do not compare tasks, vote, use confidence, regex, keywords, overlap, embeddings, "
        "prior labels, model identity, or system identity. unsupported_inference is excluded here "
        "and is projected only from the separate LLM proposition-support pass."
    )


def field_prompt_v154(value: Mapping[str, Any]) -> str:
    return (
        "Return one independent decision for every opaque task_id. Resolve each task from its shared "
        "unit source and event plus the matching field_contract. Every decision must contain at least "
        "one exact source span. Do not emit whole-event verdicts.\n\n"
        + json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        + "\n"
    )


def field_output_schema(value: Mapping[str, Any]) -> dict[str, Any]:
    task_ids = [row["task_id"] for row in value["tasks"]]
    decision = {
        "type": "object",
        "additionalProperties": False,
        "required": ["task_id", "field_status", "source_evidence_spans", "rationale"],
        "properties": {
            "task_id": {"type": "string", "enum": task_ids},
            "field_status": {"type": "string", "enum": ["correct", "incorrect", "abstain"]},
            "source_evidence_spans": {
                "type": "array",
                "minItems": 1,
                "maxItems": 2,
                "items": {"type": "string", "minLength": 1, "maxLength": 1000},
            },
            "rationale": {"type": "string", "minLength": 1, "maxLength": 260},
        },
    }
    schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["decisions"],
        "properties": {
            "decisions": {
                "type": "array",
                "minItems": len(task_ids),
                "maxItems": len(task_ids),
                "items": decision,
            }
        },
    }
    if validate_app_server_output_schema_subset(schema):
        raise JudgeV5CalibrationV154Error("v154 field schema is unsupported")
    return schema


def validate_field_output(output: Any, value: Mapping[str, Any]) -> list[str]:
    if not isinstance(output, Mapping) or set(output) != {"decisions"}:
        return ["invalid_field_root"]
    tasks = {str(row["task_id"]): row for row in value["tasks"]}
    sources = {
        (str(row["case_id"]), str(row["witness_id"])): str(row["source_excerpt"])
        for row in value["units"]
    }
    seen = set()
    errors = []
    for index, row in enumerate(output.get("decisions") or []):
        prefix = f"decision_{index}"
        if not isinstance(row, Mapping) or set(row) != {
            "task_id", "field_status", "source_evidence_spans", "rationale"
        }:
            errors.append(prefix + "_shape")
            continue
        task_id = str(row.get("task_id"))
        if task_id not in tasks or task_id in seen:
            errors.append(prefix + "_identity")
            continue
        seen.add(task_id)
        task = tasks[task_id]
        source = sources[(str(task["case_id"]), str(task["witness_id"]))]
        spans = row.get("source_evidence_spans")
        if row.get("field_status") not in {"correct", "incorrect", "abstain"}:
            errors.append(prefix + "_status")
        if (
            not isinstance(spans, list)
            or not 1 <= len(spans) <= 2
            or len(spans) != len(set(spans))
            or any(not isinstance(span, str) or not span or span not in source for span in spans)
        ):
            errors.append(prefix + "_evidence")
        if not isinstance(row.get("rationale"), str) or not row["rationale"]:
            errors.append(prefix + "_rationale")
    if seen != set(tasks):
        errors.append("field_task_coverage")
    return errors


def build_v154_inputs(source: Mapping[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    reference = source["source"]["v151"]["source"]["source"]["v148"]["values"]["reference"]
    pool = source["v106"]["pool"]
    selected = select_v154_units(pool, reference)
    pointwise = build_pointwise_support_input(pool)
    pointwise_units = {str(row["witness_id"]): row for row in pointwise["units"]}
    units = [deepcopy(pointwise_units[row["witness_id"]]) for row in selected]
    support_primary = _support_value(units)
    support_canary = _support_value(list(reversed(units)))
    primary_groups = [units[:4], units[4:]]
    canary_ids = {
        row["witness_id"]
        for row in sorted(
            selected,
            key=lambda row: (
                row["support_status"] != "unsupported",
                sha256_text(f"v154|canary|{row['witness_id']}"),
            ),
        )[:2]
    }
    canary_ids.update(
        row["witness_id"]
        for row in sorted(
            selected,
            key=lambda row: (
                row["support_status"] != "supported",
                sha256_text(f"v154|canary-control|{row['witness_id']}"),
            ),
        )[:2]
    )
    canary_units = [unit for unit in units if unit["witness_id"] in canary_ids]
    if len(canary_units) != CANARY_UNIT_COUNT:
        raise JudgeV5CalibrationV154Error("v154 canary selection drifted")
    rows = [
        {"turn_name": SUPPORT_TURNS[0], "turn_role": "support_primary", "value": support_primary},
        {"turn_name": SUPPORT_TURNS[1], "turn_role": "support_order_canary", "value": support_canary},
    ]
    rows.extend(
        {
            "turn_name": turn_name,
            "turn_role": "field_primary",
            "value": build_field_value(group, permutation="base"),
        }
        for turn_name, group in zip(FIELD_PRIMARY_TURNS, primary_groups, strict=True)
    )
    rows.append(
        {
            "turn_name": FIELD_CANARY_TURN,
            "turn_role": "field_order_canary",
            "value": build_field_value(canary_units, permutation="reversed"),
        }
    )
    truth = {
        "schema_version": V154_TRUTH_VERSION,
        "units": [
            {
                "case_id": row["case_id"],
                "witness_id": row["witness_id"],
                "expected_support_status": row["support_status"],
                "expected_field_issues": row["field_issues"],
            }
            for row in selected
        ],
        "canary_witness_ids": sorted(canary_ids),
    }
    selection = {
        "schema_version": V154_SELECTION_VERSION,
        "created_at": now_iso(),
        "unit_count": UNIT_COUNT,
        "distinct_case_count": UNIT_COUNT,
        "support_status_counts": {"supported": 4, "unsupported": 4},
        "structured_status_counts": {"correct": 4, "incorrect": 4},
        "distinct_issue_field_count": len({field for row in selected for field in row["field_issues"]}),
        "field_names_per_unit": len(CHECKLIST_FIELDS),
        "field_model_task_count": UNIT_COUNT * len(FIELD_NAMES),
        "unsupported_inference_projection_task_count": UNIT_COUNT,
        "primary_field_turn_count": 2,
        "units_per_primary_turn": UNITS_PER_PRIMARY_TURN,
        "canary_unit_count": CANARY_UNIT_COUNT,
        "selection_uses_source_text": False,
        "selection_uses_only_frozen_reference_labels_provenance_and_opaque_ids": True,
        "prior_model_outputs_used_for_selection": False,
        "semantic_pruning_performed": False,
        "empty_event_fields_omitted_only": True,
        "privacy": "private_source_event_output_aggregate_reports_only",
    }
    return rows, truth, selection


def _merge_field_outputs(outputs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    decisions = [deepcopy(row) for output in outputs for row in output["decisions"]]
    if len(decisions) != UNIT_COUNT * len(FIELD_NAMES) or len({row["task_id"] for row in decisions}) != len(decisions):
        raise JudgeV5CalibrationV154Error("v154 primary field output coverage drifted")
    return {"decisions": decisions}


def score_v154(
    *,
    support: Mapping[str, Any],
    support_canary: Mapping[str, Any],
    fields: Mapping[str, Any],
    field_canary: Mapping[str, Any],
    truth: Mapping[str, Any],
) -> dict[str, Any]:
    expected = {str(row["witness_id"]): row for row in truth["units"]}
    support_rows = {str(row["witness_id"]): row for row in support["units"]}
    support_repeat = {str(row["witness_id"]): row for row in support_canary["units"]}
    observed = {str(row["task_id"]): row for row in fields["decisions"]}
    repeated = {str(row["task_id"]): row for row in field_canary["decisions"]}
    task_truth = {
        _field_task_id(row["case_id"], witness_id, field): (
            "incorrect" if field in row["expected_field_issues"] else "correct"
        )
        for witness_id, row in expected.items()
        for field in FIELD_NAMES
    }
    if set(support_rows) != set(expected) or set(support_repeat) != set(expected) or set(observed) != set(task_truth):
        raise JudgeV5CalibrationV154Error("v154 score coverage drifted")
    canary_witnesses = set(truth["canary_witness_ids"])
    canary_task_ids = {
        task_id
        for task_id in task_truth
        if any(task_id == _field_task_id(expected[wid]["case_id"], wid, field) for wid in canary_witnesses for field in FIELD_NAMES)
    }
    if set(repeated) != canary_task_ids:
        raise JudgeV5CalibrationV154Error("v154 canary score coverage drifted")
    tp = fp = fn = field_exact = field_abstentions = 0
    predicted_issues = {witness_id: set() for witness_id in expected}
    for task_id, wanted in task_truth.items():
        actual = observed[task_id]["field_status"]
        field_exact += int(actual == wanted)
        field_abstentions += int(actual == "abstain")
        tp += int(wanted == "incorrect" and actual == "incorrect")
        fp += int(wanted == "correct" and actual == "incorrect")
        fn += int(wanted == "incorrect" and actual != "incorrect")
    for witness_id, row in expected.items():
        for field in FIELD_NAMES:
            task_id = _field_task_id(row["case_id"], witness_id, field)
            if observed[task_id]["field_status"] == "incorrect":
                predicted_issues[witness_id].add(field)
        support_status = support_rows[witness_id]["support_status"]
        if support_status == "unsupported":
            predicted_issues[witness_id].add("unsupported_inference")
        elif support_status == "abstain":
            field_abstentions += 1
    structured_exact = sum(
        predicted_issues[witness_id] == set(row["expected_field_issues"])
        for witness_id, row in expected.items()
    )
    support_tp = support_tn = support_fp = support_fn = support_abstentions = 0
    support_order_exact = 0
    for witness_id, row in expected.items():
        wanted = row["expected_support_status"]
        actual = support_rows[witness_id]["support_status"]
        repeated_status = support_repeat[witness_id]["support_status"]
        support_order_exact += int(actual == repeated_status)
        support_abstentions += int(actual == "abstain")
        support_tp += int(wanted == "supported" and actual == "supported")
        support_fn += int(wanted == "supported" and actual != "supported")
        support_tn += int(wanted == "unsupported" and actual == "unsupported")
        support_fp += int(wanted == "unsupported" and actual != "unsupported")
    canary_exact = sum(observed[task_id]["field_status"] == repeated[task_id]["field_status"] for task_id in canary_task_ids)
    evidence_complete = sum(bool(row["source_evidence_spans"]) for row in support["units"] + support_canary["units"] + fields["decisions"] + field_canary["decisions"])
    metrics = {
        "unit_count": UNIT_COUNT,
        "field_decision_count": len(task_truth) + UNIT_COUNT,
        "field_decision_accuracy": _ratio(field_exact + sum(support_rows[wid]["support_status"] == expected[wid]["expected_support_status"] for wid in expected), len(task_truth) + UNIT_COUNT),
        "pointwise_field_issue_f1": _f1(tp + support_tn, fp + support_fp, fn + support_fn),
        "structured_field_accuracy": _ratio(structured_exact, UNIT_COUNT),
        "field_abstention_count": field_abstentions,
        "field_canary_decision_count": len(canary_task_ids),
        "field_canary_exact_count": canary_exact,
        "support_sensitivity": _ratio(support_tp, support_tp + support_fn),
        "support_specificity": _ratio(support_tn, support_tn + support_fp),
        "support_abstention_count": support_abstentions,
        "support_canary_exact_count": support_order_exact,
        "support_canary_decision_count": UNIT_COUNT,
        "evidence_complete_count": evidence_complete,
        "evidence_expected_count": 2 * UNIT_COUNT + len(task_truth) + len(canary_task_ids),
    }
    checks = {
        "field_decision_accuracy": metrics["field_decision_accuracy"] >= 0.95,
        "pointwise_field_issue_f1": metrics["pointwise_field_issue_f1"] >= 0.95,
        "structured_field_accuracy": metrics["structured_field_accuracy"] >= 0.95,
        "field_abstention_count": metrics["field_abstention_count"] == 0,
        "field_order_canary_exact_rate": canary_exact == len(canary_task_ids),
        "support_sensitivity": metrics["support_sensitivity"] >= 0.95,
        "support_specificity": metrics["support_specificity"] >= 0.95,
        "support_abstention_count": metrics["support_abstention_count"] == 0,
        "support_order_canary_exact_rate": support_order_exact == UNIT_COUNT,
        "evidence_complete_rate": evidence_complete == metrics["evidence_expected_count"],
        "schema_and_exact_evidence_validation": True,
    }
    passed = all(checks.values())
    return {
        "schema_version": V154_SCORE_VERSION,
        "passed": passed,
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "metrics": metrics,
        "fresh_full_development_calibration_authorized": passed,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path, policy_path = root / "capacity-policy-audit.json", root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {"schema_version": V154_CAPACITY_AUDIT_VERSION, "phase_id": V154_PHASE_ID, "created_at": now_iso(), "production_mutation_performed": False, "predecessor": predecessor, "measured_basis": {"declared_turn_count": len(TURN_NAMES), "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN, "phase_total_token_bound": bound}}
    _write_stable_time(audit_path, audit, "created_at")
    policy = {"schema_version": V154_CAPACITY_POLICY_VERSION, "phase_id": V154_PHASE_ID, "created_at": now_iso(), "managed_chatgpt_auth_only": True, "official_persistent_codex_app_server_only": True, "retry_count_per_turn": 0, "production_mutation_allowed": False, "rate_limit_reached_type_must_be_null": True, "unknown_usage_hard_stop": True, "ordered_turn_names": list(TURN_NAMES), "minimum_remaining_reserve_percent": 20, "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS, "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN, "phase_total_token_bound": bound, "projected_phase_quota_points": math.ceil(bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000), "semantic_output_root": str(root), "audit": _record(audit_path)}
    _write_stable_time(policy_path, policy, "created_at")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v154(*, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS) -> dict[str, Any]:
    root = output_dir.expanduser().resolve(); root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists(): return {"root": root, "terminal": _load_json(root / "terminal.json", "v154 terminal")}
    source = _validate_v153(); rows, truth, selection = build_v154_inputs(source)
    truth_path, selection_path = root / "shared-field-truth.private.json", root / "selection-audit.json"
    _write_immutable(truth_path, truth); _write_stable_time(selection_path, selection, "created_at")
    turns = []
    for row in rows:
        support = row["turn_role"].startswith("support_")
        prompt = v143.support_prompt_v143(row["value"]) if support else field_prompt_v154(row["value"])
        schema = v143.support_output_schema(row["value"]) if support else field_output_schema(row["value"])
        paths = _freeze_turn_request(root=root, turn_name=row["turn_name"], input_value=row["value"], prompt=prompt, schema=schema)
        turns.append({**row, "prompt": prompt, "schema": schema, "paths": paths})
    predecessor = {**{f"v153_{name}": record for name, record in source["records"].items()}, "v153_attempt": source["attempt"], "v153_usage": source["usage"], "v106_pool": source["v106"]["records"]["pool"], "v149_field_protocol": source["source"]["v151"]["source"]["source"]["records"]["protocol"], "v148_reference": source["source"]["v151"]["source"]["source"]["v148"]["records"]["reference"]}
    capacity = _build_capacity_policy(root, predecessor); runtime_dir = Path(__file__).resolve().parent
    spec = {"schema_version": V154_SPEC_VERSION, "state": "frozen_before_model_calls", "created_at": now_iso(), "support_model": SUPPORT_MODEL, "field_model": FIELD_MODEL, "reasoning_effort": EFFORT, "timeout_seconds": timeout_seconds, "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth", "strategy": "corrected_reference_shared_context_independent_field_microtasks_serialized_four_witnesses_per_turn", "unit_count": UNIT_COUNT, "field_names_per_unit": len(CHECKLIST_FIELDS), "field_model_task_count": UNIT_COUNT * len(FIELD_NAMES), "turn_plan": list(TURN_NAMES), "retry_count_per_turn": 0, "fresh_full_development_calibration_authorized": False, "selection_authorized": False, "holdout_authorized": False, "production_mutation_allowed": False, "capacity_policy": _record(capacity["policy"]), "capacity_audit": _record(capacity["audit"]), "predecessor": predecessor, "runtime_files": [_record(Path(__file__)), _record(runtime_dir / "app_server_judge_v5_calibration_v153_capped_alignment_adjudication.py"), _record(runtime_dir / "app_server_judge_v5_calibration_v150_fresh_alignment_diagnostic.py"), _record(runtime_dir / "app_server_judge_v5_calibration_v143_corrected_layered_diagnostic.py"), _record(runtime_dir / "app_server_judge_v5_calibration_v145_comprehensive_field_reference_owner.py"), _record(runtime_dir / "app_server_judge_v5.py"), _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"), _record(runtime_dir / "app_server_capacity_reserve.py"), _record(runtime_dir / "codex_app_server.py")], "frozen_instructions": {"support_sha256": sha256_text(v143.support_base_instructions_v143()), "field_adapter_sha256": sha256_text(field_instructions_v154())}, "frozen_inputs": {"truth": _record(truth_path), "selection": _record(selection_path), "field_protocol": predecessor["v149_field_protocol"], "alignment_protocol": source["records"]["protocol"], "reference": predecessor["v148_reference"], "turns": [{"turn_name": turn["turn_name"], "role": turn["turn_role"], "input": _record(turn["paths"]["input"]), "prompt": _record(turn["paths"]["prompt"]), "schema": _record(turn["paths"]["schema"])} for turn in turns]}, "privacy": "private_source_event_output_no_source_text_in_reports"}
    spec_path = root / "shared-context-field-diagnostic-spec.json"; _write_stable_time(spec_path, spec, "created_at")
    return {"root": root, "spec": spec, "spec_path": spec_path, "capacity_policy": capacity["policy"], "turns": turns, "truth": truth, "selection": selection, "source": source}


def _write_failure(root: Path, turn_name: Optional[str], error_class: str) -> dict[str, Any]:
    attempts = [row for row in _attempt_records(root) if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))]; usage = {field: 0 for field in USAGE_FIELDS}; unknown = 0
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping): unknown += 1; continue
        try: measured = _validate_usage(_load_json(Path(record["path"]), "v154 sidecar"))
        except Exception: unknown += 1; continue
        for field in USAGE_FIELDS: usage[field] += measured[field]
    complete = unknown == 0; failure = {"schema_version": V154_FAILURE_VERSION, "terminal_at": now_iso(), "classification": "infrastructure_or_judge_attempt_failed", "failed_turn_name": turn_name, "error_class": error_class, "retry_allowed_in_this_version": False, "accounting_complete": complete, "usage_status": "complete" if complete else "unknown", "usage": usage if complete else None, "known_usage_lower_bound": usage, "unknown_usage_turn_count": unknown, "attempts": attempts}; failure_path = root / "failure.json"; _write_immutable(failure_path, failure)
    terminal = {"schema_version": V154_TERMINAL_VERSION, "state": "failed", "terminal_reason": "infrastructure_or_judge_attempt_failed", "overall_evaluation_complete": False, "failure": _record(failure_path), "field_adapter_frozen": False, "fresh_full_development_calibration_authorized": False, "selection_authorized": False, "holdout_authorized": False, "production_mutated": False, "semantic_retry_count": 0, "accounting_complete": complete, "usage_status": failure["usage_status"], "usage": failure["usage"]}; _write_immutable(root / "terminal.json", terminal); return terminal


async def run_v154(*, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS, client_factory: Optional[Callable[[Path], Any]] = None) -> dict[str, Any]:
    root = output_dir.expanduser().resolve(); terminal_path = root / "terminal.json"
    if terminal_path.exists(): return _load_json(terminal_path, "v154 terminal")
    frozen = freeze_v154(output_dir=root, timeout_seconds=timeout_seconds); current_turn: Optional[str] = None
    try:
        support = support_canary = None; primary_outputs = []; field_canary = None; sidecars = []
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            for turn in frozen["turns"]:
                current_turn = turn["turn_name"]; is_support = turn["turn_role"].startswith("support_")
                output, sidecar, _ = await _get_or_run_turn(client=client, turn_name=current_turn, paths=turn["paths"], prompt=turn["prompt"], schema=turn["schema"], base_instructions=v143.support_base_instructions_v143() if is_support else field_instructions_v154(), model=SUPPORT_MODEL if is_support else FIELD_MODEL, effort=EFFORT, timeout_seconds=timeout_seconds, batch_size=turn["value"].get("unit_count", turn["value"].get("task_count")), policy_path=frozen["capacity_policy"], output_validator=(lambda candidate, item=turn["value"]: v143.validate_support_output(candidate, item)) if is_support else (lambda candidate, item=turn["value"]: validate_field_output(candidate, item)))
                if turn["turn_role"] == "support_primary": support = output
                elif turn["turn_role"] == "support_order_canary": support_canary = output
                elif turn["turn_role"] == "field_primary": primary_outputs.append(output)
                else: field_canary = output
                sidecars.append(sidecar)
        if support is None or support_canary is None or field_canary is None: raise JudgeV5CalibrationV154Error("v154 output coverage drifted")
        fields = _merge_field_outputs(primary_outputs); score = score_v154(support=support, support_canary=support_canary, fields=fields, field_canary=field_canary, truth=frozen["truth"]); passed = bool(score["passed"])
        paths = {"support": root / "support-output.private.json", "support_canary": root / "support-order-canary.private.json", "fields": root / "field-output.private.json", "field_canary": root / "field-order-canary.private.json", "score": root / "shared-context-field-score.json", "protocol": root / "shared-context-field-adapter-v154.json"}
        for key, value in (("support", support), ("support_canary", support_canary), ("fields", fields), ("field_canary", field_canary), ("score", score)): _write_immutable(paths[key], value)
        if passed:
            protocol = {"schema_version": V154_PROTOCOL_VERSION, "frozen_at": now_iso(), "support_model": SUPPORT_MODEL, "field_model": FIELD_MODEL, "reasoning_effort": EFFORT, "support_instructions_sha256": sha256_text(v143.support_base_instructions_v143()), "field_adapter_instructions_sha256": sha256_text(field_instructions_v154()), "field_tasks_are_independent": True, "shared_context_is_serialization_only": True, "units_per_field_turn": UNITS_PER_PRIMARY_TURN, "unsupported_inference_projection_rule": {"supported": "correct", "unsupported": "incorrect"}, "field_protocol": frozen["spec"]["frozen_inputs"]["field_protocol"], "alignment_protocol": frozen["spec"]["frozen_inputs"]["alignment_protocol"], "reference": frozen["spec"]["frozen_inputs"]["reference"], "quality_gates_unchanged": True, "selection_authorized": False, "holdout_authorized": False, "production_mutation_allowed": False}; _write_immutable(paths["protocol"], protocol)
        accounting = _aggregate_usage(sidecars); terminal = {"schema_version": V154_TERMINAL_VERSION, "state": "completed" if passed else "inactive", "terminal_at": now_iso(), "terminal_reason": "v154_field_adapter_frozen_full_development_calibration_authorized" if passed else "inactive_incomplete_recovery_required", "development_terminal_reason": "v154_shared_context_field_diagnostic_passed" if passed else "v154_shared_context_field_diagnostic_quality_gate_not_passed", "overall_evaluation_complete": False, "field_adapter_frozen": passed, "fresh_full_development_calibration_authorized": passed, "selection_authorized": False, "holdout_authorized": False, "production_mutated": False, "semantic_attempt_started": True, "semantic_retry_count": 0, "failed_quality_gates": score["failed_checks"], "metrics": score["metrics"], "support_output": _record(paths["support"]), "support_canary_output": _record(paths["support_canary"]), "field_output": _record(paths["fields"]), "field_canary_output": _record(paths["field_canary"]), "score": _record(paths["score"]), "protocol": _record(paths["protocol"]) if passed else None, "reference": frozen["spec"]["frozen_inputs"]["reference"], "alignment_protocol": frozen["spec"]["frozen_inputs"]["alignment_protocol"], "predecessor_v153_usage": frozen["source"]["usage"], **accounting}; _write_immutable(terminal_path, terminal); return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc: return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc: return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v154 shared-context field diagnostic"); parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT)); parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS); args = parser.parse_args(argv)
    terminal = asyncio.run(run_v154(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)); print(json.dumps({"state": terminal["state"], "terminal_reason": terminal["terminal_reason"], "field_adapter_frozen": terminal.get("field_adapter_frozen", False), "fresh_full_development_calibration_authorized": terminal.get("fresh_full_development_calibration_authorized", False), "usage_status": terminal.get("usage_status")}, sort_keys=True)); return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__": raise SystemExit(main())
