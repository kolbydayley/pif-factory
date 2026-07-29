from __future__ import annotations

"""Fresh full development calibration for the frozen v149/v153 protocols."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v130_retained_alignment_owner as v130
from . import app_server_judge_v5_calibration_v143_corrected_layered_diagnostic as v143
from . import app_server_judge_v5_calibration_v145_comprehensive_field_reference_owner as v145
from . import app_server_judge_v5_calibration_v146_singleton_reference_owner as v146
from . import app_server_judge_v5_calibration_v149_fresh_corrected_field_diagnostic as v149
from . import app_server_judge_v5_calibration_v153_capped_alignment_adjudication as v153
from . import app_server_judge_v5_calibration_v154_shared_context_field_diagnostic as v154
from .app_server_judge_v5 import (
    CHECKLIST_FIELDS,
    POINTWISE_OUTPUT_VERSION,
    build_disagreement_adjudication_input,
    build_neutral_alignment_input,
    build_pointwise_support_input,
    neutral_alignment_output_schema,
    reconcile_neutral_alignment,
    validate_neutral_alignment_output,
)
from .app_server_judge_v5_calibration import calibration_case_shards, pointwise_input_subset
from .app_server_judge_v5_calibration_v25_diagnostic import QUOTA_POINTS_PER_MILLION_TOKENS
from .app_server_judge_v5_calibration_v26_diagnostic import (
    JudgeV5CalibrationV26DiagnosticAttemptFailed,
    _aggregate_usage,
    _client_factory,
    _freeze_turn_request,
    _get_or_run_turn,
    _load_json,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic import (
    output_schema as field_output_schema,
    validate_output as validate_field_output,
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


V155_TRUTH_VERSION = "pif_app_server_judge_v5_4_v155_full_truth_v1"
V155_SELECTION_VERSION = "pif_app_server_judge_v5_4_v155_selection_v1"
V155_SPEC_VERSION = "pif_app_server_judge_v5_4_v155_spec_v1"
V155_SCORE_VERSION = "pif_app_server_judge_v5_4_v155_score_v1"
V155_PROTOCOL_VERSION = "pif_app_server_judge_v5_4_v155_frozen_protocol_v1"
V155_FAILURE_VERSION = "pif_app_server_judge_v5_4_v155_failure_v1"
V155_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v155_terminal_v1"
V155_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V155_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V155_PHASE_ID = "judge_v5_4_v155_fresh_full_development"

SUPPORT_MODEL = "gpt-5.6-sol"
FIELD_MODEL = "gpt-5.5"
ALIGNMENT_MODEL = "gpt-5.6-luna"
ADJUDICATOR_MODEL = "gpt-5.5"
EFFORT = "high"
SUPPORT_PRIMARY_TURNS = tuple(f"full_support_shard_{index:02d}" for index in range(11))
SUPPORT_CANARY_TURNS = ("full_support_canary_00", "full_support_canary_01")
FIELD_PRIMARY_TURNS = tuple(f"full_field_singleton_{index:02d}" for index in range(28))
FIELD_REPEAT_TURNS = tuple(f"full_field_repeat_{index:02d}" for index in range(4))
ALIGNMENT_PRIMARY_TURNS = tuple(f"full_alignment_shard_{index:02d}" for index in range(11))
ALIGNMENT_CANARY_TURNS = ("full_alignment_canary_00", "full_alignment_canary_01")
ADJUDICATION_TURN = "full_alignment_disagreement_adjudication"
TURN_NAMES = (
    SUPPORT_PRIMARY_TURNS
    + SUPPORT_CANARY_TURNS
    + FIELD_PRIMARY_TURNS
    + FIELD_REPEAT_TURNS
    + ALIGNMENT_PRIMARY_TURNS
    + ALIGNMENT_CANARY_TURNS
    + (ADJUDICATION_TURN,)
)
REPEAT_FIELDS = ("evidence", "metric", "reported_actor", "target")
MAXIMUM_TOTAL_TOKENS_PER_TURN = 45_000
TIMEOUT_SECONDS = 1200.0
DEFAULT_OUTPUT_ROOT = (
    v154.DEFAULT_OUTPUT_ROOT.parent
    / "judge-calibration-v5_4-v155-fresh-full-development"
).resolve()


class JudgeV5CalibrationV155Error(RuntimeError):
    """The immutable v155 full-calibration contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _ratio(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 6) if denominator else 0.0


def _f1(tp: int, fp: int, fn: int) -> float:
    denominator = 2 * tp + fp + fn
    return round(2 * tp / denominator, 6) if denominator else 1.0


def _validate_v154() -> dict[str, Any]:
    root = v154.DEFAULT_OUTPUT_ROOT
    paths = {
        "spec": root / "shared-context-field-diagnostic-spec.json",
        "terminal": root / "terminal.json",
        "score": root / "shared-context-field-score.json",
        "truth": root / "shared-field-truth.private.json",
        "selection": root / "selection-audit.json",
    }
    values = {name: _load_json(path, f"v154 {name}") for name, path in paths.items()}
    terminal, score, spec = values["terminal"], values["score"], values["spec"]
    expected_usage = {
        "input_tokens": 127740,
        "cached_input_tokens": 5760,
        "output_tokens": 27869,
        "reasoning_output_tokens": 16666,
        "total_tokens": 155609,
    }
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("development_terminal_reason")
        != "v154_shared_context_field_diagnostic_quality_gate_not_passed"
        or terminal.get("field_adapter_frozen") is not False
        or terminal.get("fresh_full_development_calibration_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage") != expected_usage
        or terminal.get("semantic_retry_count") != 0
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
        or terminal.get("score") != _record(paths["score"])
        or spec.get("turn_plan") != list(v154.TURN_NAMES)
        or spec.get("retry_count_per_turn") != 0
        or not all(_verify_record(record) for record in spec.get("runtime_files") or [])
    ):
        raise JudgeV5CalibrationV155Error("v154 failure contract drifted")
    attempts = {}
    usage = {field: 0 for field in USAGE_FIELDS}
    for turn_name in v154.TURN_NAMES:
        turn_root = root / "turns" / turn_name.replace("_", "-")
        records = {
            name: _record(turn_root / filename)
            for name, filename in {
                "capacity": "capacity.json",
                "sidecar": "sidecar.json",
                "output": "output.private.json",
            }.items()
        }
        if any(not _verify_record(record) for record in records.values()):
            raise JudgeV5CalibrationV155Error("v154 attempt coverage drifted")
        measured = _validate_usage(_load_json(Path(records["sidecar"]["path"]), "v154 sidecar"))
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
        attempts[turn_name] = records
    if usage != expected_usage:
        raise JudgeV5CalibrationV155Error("v154 usage aggregate drifted")
    predecessor = v154._validate_v153()
    return {
        "root": root,
        "paths": paths,
        "records": {name: _record(path) for name, path in paths.items()},
        "values": values,
        "attempts": attempts,
        "usage": usage,
        "v153": predecessor,
    }


def _reference(source: Mapping[str, Any]) -> dict[str, Any]:
    return source["v153"]["source"]["v151"]["source"]["source"]["v148"]["values"]["reference"]


def _reference_record(source: Mapping[str, Any]) -> dict[str, Any]:
    return source["v153"]["source"]["v151"]["source"]["source"]["v148"]["records"]["reference"]


def _field_protocol_record(source: Mapping[str, Any]) -> dict[str, Any]:
    return source["v153"]["source"]["v151"]["source"]["source"]["records"]["protocol"]


def _witness_units(pool: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    pointwise = build_pointwise_support_input(pool)
    units = {str(row["witness_id"]): row for row in pointwise["units"]}
    if len(units) != 182:
        raise JudgeV5CalibrationV155Error("v155 pointwise witness coverage drifted")
    return units


def _field_task_id(case_id: str, witness_id: str, field: str, ordinal: int) -> str:
    return "fullfield_" + sha256_text(f"v155|{case_id}|{witness_id}|{field}|{ordinal}")[:24]


def _build_field_tasks(
    *, pool: Mapping[str, Any], reference: Mapping[str, Any]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    units = _witness_units(pool)
    candidates = []
    for case_id, case in reference["cases"].items():
        for witness_id, issues in case["field_issues"].items():
            for field in CHECKLIST_FIELDS:
                candidates.append(
                    {
                        "case_id": case_id,
                        "witness_id": witness_id,
                        "field": field,
                        "expected_status": "incorrect" if field in issues else "correct",
                    }
                )
    selected = []
    for field in CHECKLIST_FIELDS:
        values = [row for row in candidates if row["field"] == field and row["expected_status"] == "correct"]
        values.sort(key=lambda row: sha256_text(f"v155|field|correct|{field}|{row['case_id']}|{row['witness_id']}"))
        selected.append(values[0])
    for field in CHECKLIST_FIELDS:
        values = [row for row in candidates if row["field"] == field and row["expected_status"] == "incorrect"]
        values.sort(key=lambda row: sha256_text(f"v155|field|incorrect|{field}|{row['case_id']}|{row['witness_id']}"))
        if values:
            selected.append(values[0])
    actor_values = [row for row in candidates if row["field"] == "actor" and row["expected_status"] == "incorrect" and row not in selected]
    actor_values.sort(key=lambda row: sha256_text(f"v155|field|extra-actor|{row['case_id']}|{row['witness_id']}"))
    selected.append(actor_values[0])
    if (
        len(selected) != 30
        or len({(row["case_id"], row["witness_id"], row["field"]) for row in selected}) != 30
        or sum(row["expected_status"] == "correct" for row in selected) != 15
        or {row["field"] for row in selected} != set(CHECKLIST_FIELDS)
    ):
        raise JudgeV5CalibrationV155Error("v155 field selection drifted")
    selected.sort(key=lambda row: sha256_text(f"v155|field-order|{row['case_id']}|{row['witness_id']}|{row['field']}"))
    primary_rows = []
    truth_rows = []
    field_occurrences: dict[str, int] = {}
    for row in selected:
        field = row["field"]
        ordinal = field_occurrences.get(field, 0)
        field_occurrences[field] = ordinal + 1
        task_id = _field_task_id(row["case_id"], row["witness_id"], field, ordinal)
        truth_rows.append({**row, "task_id": task_id})
        if field == "unsupported_inference":
            continue
        unit = units[row["witness_id"]]
        event = compact_empty_event_fields(deepcopy(unit["structured_event"]))
        task = {
            "task_id": task_id,
            "field": field,
            "field_contract": {"field": field, **deepcopy(v145.FIELD_RULES_V145[field])},
            "requested_field_value": _general_requested_field_value(field, event),
            "source_excerpt": unit["source_excerpt"],
            "structured_event": event,
        }
        primary_rows.append(
            {
                "turn_role": "field_singleton",
                "task_id": task_id,
                "field": field,
                "value": v149._field_value(task),
            }
        )
    if len(primary_rows) != 28:
        raise JudgeV5CalibrationV155Error("v155 field model task count drifted")
    repeat_rows = []
    for field in REPEAT_FIELDS:
        candidates_for_field = [row for row in primary_rows if row["field"] == field]
        candidates_for_field.sort(key=lambda row: sha256_text(f"v155|repeat|{row['task_id']}"))
        repeat_rows.append(deepcopy(candidates_for_field[0]))
        repeat_rows[-1]["turn_role"] = "field_repeat"
    selection = {
        "schema_version": V155_SELECTION_VERSION,
        "created_at": now_iso(),
        "field_truth_task_count": 30,
        "field_model_primary_turn_count": 28,
        "field_repeat_turn_count": 4,
        "field_status_counts": {"correct": 15, "incorrect": 15},
        "field_enum_count": len(CHECKLIST_FIELDS),
        "unsupported_inference_projection_task_count": 2,
        "repeat_fields": list(REPEAT_FIELDS),
        "selection_uses_source_text": False,
        "selection_uses_only_frozen_reference_labels_provenance_and_opaque_ids": True,
        "prior_model_outputs_used_for_selection": False,
        "semantic_pruning_performed": False,
        "singleton_context_preserved": True,
    }
    return primary_rows, repeat_rows, {"rows": truth_rows, "selection": selection}


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


def _select_support_canary(pointwise: Mapping[str, Any], reference: Mapping[str, Any]) -> list[dict[str, Any]]:
    units = {str(row["witness_id"]): row for row in pointwise["units"]}
    chosen = []
    used_cases = set()
    for status in ("unsupported", "supported"):
        candidates = [
            (case_id, witness_id)
            for case_id, case in reference["cases"].items()
            for witness_id, verdict in case["proposition"].items()
            if verdict == status
        ]
        candidates.sort(key=lambda row: sha256_text(f"v155|support-canary|{status}|{row[0]}|{row[1]}"))
        count = 0
        for case_id, witness_id in candidates:
            if case_id in used_cases:
                continue
            chosen.append(deepcopy(units[witness_id]))
            used_cases.add(case_id)
            count += 1
            if count == 6:
                break
        if count != 6:
            raise JudgeV5CalibrationV155Error("v155 support canary selection drifted")
    chosen.sort(key=lambda row: sha256_text(f"v155|support-canary-order|{row['witness_id']}"))
    return chosen


def _correct_support_only_projection(case: Mapping[str, Any]) -> dict[str, Any]:
    supported = {witness_id for witness_id, verdict in case["proposition"].items() if verdict == "supported"}
    pairs = [
        deepcopy(pair)
        for pair in case["pairs"]
        if set(pair["witness_ids"]) <= supported
    ]
    groups = []
    for group in case["equivalence_groups"]:
        retained = sorted(set(group) & supported)
        if retained and retained not in groups:
            groups.append(retained)
    for witness_id in sorted(supported):
        if not any(witness_id in group for group in groups):
            groups.append([witness_id])
    groups.sort()
    paired = {witness_id for pair in pairs for witness_id in pair["witness_ids"]}
    return {
        "pairs": sorted(pairs, key=lambda row: tuple(sorted(row["witness_ids"]))),
        "equivalence_groups": groups,
        "unpaired_witness_ids": sorted(supported - paired),
    }


def build_v155_inputs(source: Mapping[str, Any]) -> dict[str, Any]:
    pool = source["v153"]["v106"]["pool"]
    reference = _reference(source)
    pointwise = build_pointwise_support_input(pool)
    case_shards = calibration_case_shards(pool)
    support_rows = []
    for turn_name, case_ids in zip(SUPPORT_PRIMARY_TURNS, case_shards, strict=True):
        subset = pointwise_input_subset(pointwise, case_ids)
        support_rows.append(
            {"turn_name": turn_name, "turn_role": "support_primary", "value": _support_value(subset["units"])}
        )
    support_canary_units = _select_support_canary(pointwise, reference)
    for turn_name, units in zip(SUPPORT_CANARY_TURNS, (support_canary_units[:6], support_canary_units[6:]), strict=True):
        support_rows.append(
            {"turn_name": turn_name, "turn_role": "support_canary", "value": _support_value(list(reversed(units)))}
        )
    field_primary, field_repeats, field_data = _build_field_tasks(pool=pool, reference=reference)
    for turn_name, row in zip(FIELD_PRIMARY_TURNS, field_primary, strict=True):
        row["turn_name"] = turn_name
    for turn_name, row in zip(FIELD_REPEAT_TURNS, field_repeats, strict=True):
        row["turn_name"] = turn_name
    truth = {
        "schema_version": V155_TRUTH_VERSION,
        "case_count": 66,
        "witness_count": 182,
        "support": [
            {"case_id": case_id, "witness_id": witness_id, "expected_status": verdict}
            for case_id, case in reference["cases"].items()
            for witness_id, verdict in case["proposition"].items()
        ],
        "support_canary_witness_ids": sorted(row["witness_id"] for row in support_canary_units),
        "field_tasks": field_data["rows"],
        "field_repeat_task_ids": sorted(row["task_id"] for row in field_repeats),
        "alignment_cases": [
            {
                "case_id": case_id,
                "shape": case["shape"],
                "expected": _correct_support_only_projection(case),
            }
            for case_id, case in sorted(reference["cases"].items())
        ],
        "alignment_canary_case_ids": sorted(reference["canary_case_ids"]),
    }
    if len(truth["support"]) != 182 or len(truth["alignment_cases"]) != 66:
        raise JudgeV5CalibrationV155Error("v155 truth coverage drifted")
    return {
        "pool": pool,
        "reference": reference,
        "pointwise": pointwise,
        "case_shards": case_shards,
        "static_turns": support_rows + field_primary + field_repeats,
        "truth": truth,
        "selection": field_data["selection"],
    }


def _support_receipts(output: Mapping[str, Any]) -> dict[str, Any]:
    rows = []
    for row in output["units"]:
        rows.append(
            {
                "case_id": row["case_id"],
                "witness_id": row["witness_id"],
                "proposition_verdict": row["support_status"],
                "proposition_evidence_spans": row["source_evidence_spans"],
                "proposition_rationale": row["rationale"],
                "structured_field_verdict": "abstain",
                "field_issue_fields": [],
                "field_evidence_spans": [],
                "field_rationale": "Structured-field decisions are owned by the separate frozen singleton pass.",
            }
        )
    rows.sort(key=lambda row: (row["case_id"], row["witness_id"]))
    return {
        "schema_version": POINTWISE_OUTPUT_VERSION,
        "units": rows,
        "side_free": True,
        "claim_support_and_field_correctness_separate": True,
    }


def _support_positive_alignment_input(
    pool: Mapping[str, Any], receipts: Mapping[str, Any], *, case_ids: Sequence[str], permutation: str
) -> dict[str, Any]:
    value = build_neutral_alignment_input(pool, receipts, case_ids=case_ids, permutation=permutation)
    for case in value["cases"]:
        case["witnesses"] = [
            witness
            for witness in case["witnesses"]
            if witness["support_receipt"]["proposition_verdict"] == "supported"
        ]
        if not case["witnesses"]:
            raise JudgeV5CalibrationV155Error("v155 support output collapsed an alignment case")
    value["supported_witnesses_only"] = True
    value["structured_field_receipts_withheld"] = True
    return value


def _merge_outputs(outputs: Sequence[Mapping[str, Any]], key: str) -> dict[str, Any]:
    rows = [deepcopy(row) for output in outputs for row in output[key]]
    return {key: rows}


def _score_alignment(alignment: Mapping[str, Any], truth: Mapping[str, Any]) -> dict[str, Any]:
    expected = {str(row["case_id"]): row["expected"] for row in truth["alignment_cases"]}
    observed = {str(row["case_id"]): row for row in alignment["cases"]}
    if set(observed) != set(expected):
        raise JudgeV5CalibrationV155Error("v155 alignment score coverage drifted")
    pair_tp = pair_fp = pair_fn = relation_exact = relation_count = 0
    eq_tp = eq_fn = eq_tn = eq_fp = field_tp = field_fp = field_fn = 0
    group_exact = unpaired_exact = abstentions = 0
    for case_id, wanted in expected.items():
        actual = v130._project_alignment(observed[case_id])
        wanted_map = {tuple(sorted(row["witness_ids"])): row for row in wanted["pairs"]}
        actual_map = {tuple(sorted(row["witness_ids"])): row for row in actual["pairs"]}
        pair_tp += len(set(wanted_map) & set(actual_map))
        pair_fp += len(set(actual_map) - set(wanted_map))
        pair_fn += len(set(wanted_map) - set(actual_map))
        for key in set(wanted_map) & set(actual_map):
            wanted_relation = wanted_map[key]["relation"]
            actual_relation = actual_map[key]["relation"]
            relation_count += 1
            relation_exact += int(wanted_relation == actual_relation)
            if wanted_relation == "equivalent":
                eq_tp += int(actual_relation == "equivalent")
                eq_fn += int(actual_relation != "equivalent")
            else:
                eq_tn += int(actual_relation != "equivalent")
                eq_fp += int(actual_relation == "equivalent")
            wanted_fields = set(wanted_map[key]["mismatch_fields"])
            actual_fields = set(actual_map[key]["mismatch_fields"])
            field_tp += len(wanted_fields & actual_fields)
            field_fp += len(actual_fields - wanted_fields)
            field_fn += len(wanted_fields - actual_fields)
        group_exact += int(actual["equivalence_groups"] == wanted["equivalence_groups"])
        unpaired_exact += int(actual["unpaired_witness_ids"] == wanted["unpaired_witness_ids"])
        abstentions += int(v130._case_has_abstention(observed[case_id]))
    return {
        "alignment_f1": _f1(pair_tp, pair_fp, pair_fn),
        "relation_accuracy": _ratio(relation_exact, relation_count),
        "equivalent_sensitivity": _ratio(eq_tp, eq_tp + eq_fn),
        "equivalent_specificity": _ratio(eq_tn, eq_tn + eq_fp),
        "mismatch_field_f1": _f1(field_tp, field_fp, field_fn),
        "equivalence_partition_exact_case_rate": _ratio(group_exact, 66),
        "unpaired_exact_case_rate": _ratio(unpaired_exact, 66),
        "alignment_abstention_case_count": abstentions,
        "matched_pair_count": relation_count,
    }


def score_v155(
    *,
    support: Mapping[str, Any],
    support_canary: Mapping[str, Any],
    fields: Mapping[str, Any],
    field_repeats: Mapping[str, Any],
    alignment: Mapping[str, Any],
    truth: Mapping[str, Any],
    raw_alignment_disagreement_count: int,
    final_canary_exact_count: int,
) -> dict[str, Any]:
    support_truth = {str(row["witness_id"]): row for row in truth["support"]}
    support_rows = {str(row["witness_id"]): row for row in support["units"]}
    support_repeat = {str(row["witness_id"]): row for row in support_canary["units"]}
    if set(support_rows) != set(support_truth) or set(support_repeat) != set(truth["support_canary_witness_ids"]):
        raise JudgeV5CalibrationV155Error("v155 support score coverage drifted")
    stp = stn = sfp = sfn = support_abstentions = 0
    for witness_id, wanted in support_truth.items():
        actual = support_rows[witness_id]["support_status"]
        support_abstentions += int(actual == "abstain")
        stp += int(wanted["expected_status"] == "supported" and actual == "supported")
        sfn += int(wanted["expected_status"] == "supported" and actual != "supported")
        stn += int(wanted["expected_status"] == "unsupported" and actual == "unsupported")
        sfp += int(wanted["expected_status"] == "unsupported" and actual != "unsupported")
    support_canary_exact = sum(
        support_rows[wid]["support_status"] == support_repeat[wid]["support_status"]
        for wid in support_repeat
    )
    field_truth = {str(row["task_id"]): row for row in truth["field_tasks"]}
    observed = {str(row["task_id"]): row for row in fields["decisions"]}
    repeated = {str(row["task_id"]): row for row in field_repeats["decisions"]}
    projection = {"supported": "correct", "unsupported": "incorrect", "abstain": "abstain"}
    for task_id, row in field_truth.items():
        if row["field"] == "unsupported_inference":
            receipt = support_rows[row["witness_id"]]
            observed[task_id] = {
                "task_id": task_id,
                "field_status": projection[receipt["support_status"]],
                "source_evidence_spans": receipt["source_evidence_spans"],
                "rationale": receipt["rationale"],
            }
    if set(observed) != set(field_truth) or set(repeated) != set(truth["field_repeat_task_ids"]):
        raise JudgeV5CalibrationV155Error("v155 field score coverage drifted")
    ftp = ffp = ffn = field_exact = field_abstentions = 0
    for task_id, wanted in field_truth.items():
        actual = observed[task_id]["field_status"]
        field_exact += int(actual == wanted["expected_status"])
        field_abstentions += int(actual == "abstain")
        ftp += int(wanted["expected_status"] == "incorrect" and actual == "incorrect")
        ffp += int(wanted["expected_status"] == "correct" and actual == "incorrect")
        ffn += int(wanted["expected_status"] == "incorrect" and actual != "incorrect")
    field_repeat_exact = sum(observed[task_id]["field_status"] == repeated[task_id]["field_status"] for task_id in repeated)
    evidence_complete = sum(bool(row["source_evidence_spans"]) for row in support["units"] + support_canary["units"] + list(observed.values()) + list(repeated.values()))
    alignment_metrics = _score_alignment(alignment, truth)
    metrics = {
        "case_count": 66,
        "witness_count": 182,
        "support_sensitivity": _ratio(stp, stp + sfn),
        "support_specificity": _ratio(stn, stn + sfp),
        "support_abstention_count": support_abstentions,
        "support_canary_exact_count": support_canary_exact,
        "support_canary_decision_count": len(support_repeat),
        "structured_field_accuracy": _ratio(field_exact, len(field_truth)),
        "field_diagnostic_f1": _f1(ftp, ffp, ffn),
        "field_abstention_count": field_abstentions,
        "field_repeat_exact_count": field_repeat_exact,
        "field_repeat_decision_count": len(repeated),
        "evidence_complete_count": evidence_complete,
        "evidence_expected_count": len(support_rows) + len(support_repeat) + len(observed) + len(repeated),
        "raw_alignment_disagreement_case_count": raw_alignment_disagreement_count,
        "permutation_canary_exact_count": final_canary_exact_count,
        "permutation_canary_case_count": len(truth["alignment_canary_case_ids"]),
        "order_bias": round(1 - final_canary_exact_count / len(truth["alignment_canary_case_ids"]), 6),
        **alignment_metrics,
    }
    checks = {
        "minimum_cases": metrics["case_count"] >= 60,
        "support_sensitivity": metrics["support_sensitivity"] >= 0.95,
        "support_specificity": metrics["support_specificity"] >= 0.95,
        "support_abstention_count": metrics["support_abstention_count"] == 0,
        "support_order_canary_exact_rate": support_canary_exact == len(support_repeat),
        "structured_field_accuracy": metrics["structured_field_accuracy"] >= 0.95,
        "field_diagnostic_f1": metrics["field_diagnostic_f1"] >= 0.95,
        "field_abstention_count": metrics["field_abstention_count"] == 0,
        "field_repeat_exact_rate": field_repeat_exact == len(repeated),
        "alignment_f1": metrics["alignment_f1"] >= 0.95,
        "relation_accuracy": metrics["relation_accuracy"] >= 0.95,
        "equivalent_sensitivity": metrics["equivalent_sensitivity"] >= 0.95,
        "equivalent_specificity": metrics["equivalent_specificity"] >= 0.95,
        "mismatch_field_f1": metrics["mismatch_field_f1"] >= 0.95,
        "equivalence_partition_exact_case_rate": metrics["equivalence_partition_exact_case_rate"] >= 0.95,
        "unpaired_exact_case_rate": metrics["unpaired_exact_case_rate"] >= 0.95,
        "order_bias": metrics["order_bias"] <= 0.05,
        "alignment_abstention_case_count": metrics["alignment_abstention_case_count"] == 0,
        "evidence_complete_rate": evidence_complete == metrics["evidence_expected_count"],
        "schema_and_exact_evidence_validation": True,
    }
    passed = all(checks.values())
    return {
        "schema_version": V155_SCORE_VERSION,
        "passed": passed,
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "metrics": metrics,
        "development_judge_frozen": passed,
        "selection_authorized": passed,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path, policy_path = root / "capacity-policy-audit.json", root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAXIMUM_TOTAL_TOKENS_PER_TURN
    audit = {
        "schema_version": V155_CAPACITY_AUDIT_VERSION,
        "phase_id": V155_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
            "measured_predecessor_maxima": {
                "field_singleton": 21779,
                "alignment_or_adjudication": 38416,
                "support": 23239,
            },
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V155_CAPACITY_POLICY_VERSION,
        "phase_id": V155_PHASE_ID,
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
        "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "phase_total_token_bound": bound,
        "projected_phase_quota_points": math.ceil(bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000),
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v155(*, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v155 terminal")}
    source = _validate_v154()
    data = build_v155_inputs(source)
    paths = {
        "pool": root / "shared-witness-pool.private.json",
        "pointwise": root / "pointwise-input-full.private.json",
        "truth": root / "full-calibration-truth.private.json",
        "selection": root / "field-selection-audit.json",
    }
    _write_immutable(paths["pool"], data["pool"])
    _write_immutable(paths["pointwise"], data["pointwise"])
    _write_immutable(paths["truth"], data["truth"])
    _write_stable_time(paths["selection"], data["selection"], "created_at")
    turns = []
    for row in data["static_turns"]:
        is_support = row["turn_role"].startswith("support_")
        prompt = v143.support_prompt_v143(row["value"]) if is_support else v149.field_prompt_v149(row["value"])
        schema = v143.support_output_schema(row["value"]) if is_support else field_output_schema(row["value"])
        request_paths = _freeze_turn_request(root=root, turn_name=row["turn_name"], input_value=row["value"], prompt=prompt, schema=schema)
        turns.append({**row, "prompt": prompt, "schema": schema, "paths": request_paths})
    predecessor = {
        **{f"v154_{name}": record for name, record in source["records"].items()},
        "v154_attempts": source["attempts"],
        "v154_usage": source["usage"],
        "v153_protocol": source["v153"]["records"]["protocol"],
        "v149_field_protocol": _field_protocol_record(source),
        "v148_reference": _reference_record(source),
        "v106_pool": source["v153"]["v106"]["records"]["pool"],
    }
    capacity = _build_capacity_policy(root, predecessor)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V155_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "support_model": SUPPORT_MODEL,
        "field_model": FIELD_MODEL,
        "alignment_model": ALIGNMENT_MODEL,
        "adjudicator_model": ADJUDICATOR_MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "fresh_full_support_plus_balanced_singleton_fields_plus_support_positive_alignment_and_one_capped_adjudication",
        "case_count": 66,
        "witness_count": 182,
        "field_truth_task_count": 30,
        "turn_plan": list(TURN_NAMES),
        "minimum_turn_count": len(TURN_NAMES) - 1,
        "maximum_turn_count": len(TURN_NAMES),
        "maximum_adjudication_call_count": 1,
        "retry_count_per_turn": 0,
        "all_semantic_turns_fresh": True,
        "prior_model_outputs_reused": False,
        "reference_truth_exposed_to_model": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v154_shared_context_field_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v153_capped_alignment_adjudication.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v149_fresh_corrected_field_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v146_singleton_reference_owner.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v143_corrected_layered_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v130_retained_alignment_owner.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration.py"),
            _record(runtime_dir / "app_server_judge_v5.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_instructions": {
            "support_sha256": sha256_text(v143.support_base_instructions_v143()),
            "field_sha256": sha256_text(v146.base_instructions_v146()),
            "alignment_sha256": sha256_text(v130.alignment_instructions_v130()),
            "adjudication_sha256": sha256_text(v153.adjudication_instructions_v153()),
        },
        "frozen_inputs": {
            **{name: _record(path) for name, path in paths.items()},
            "field_protocol": predecessor["v149_field_protocol"],
            "alignment_protocol": predecessor["v153_protocol"],
            "reference": predecessor["v148_reference"],
            "static_turns": [
                {"turn_name": turn["turn_name"], "role": turn["turn_role"], "input": _record(turn["paths"]["input"]), "prompt": _record(turn["paths"]["prompt"]), "schema": _record(turn["paths"]["schema"])}
                for turn in turns
            ],
        },
        "privacy": "private_source_event_output_truth_and_mapping_sanitized_terminal_only",
    }
    spec_path = root / "fresh-full-development-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {"root": root, "spec": spec, "spec_path": spec_path, "capacity_policy": capacity["policy"], "turns": turns, "data": data, "source": source}


def _write_failure(root: Path, turn_name: Optional[str], error_class: str) -> dict[str, Any]:
    attempts = [row for row in _attempt_records(root) if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))]
    usage = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping):
            unknown += 1
            continue
        try:
            measured = _validate_usage(_load_json(Path(record["path"]), "v155 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {"schema_version": V155_FAILURE_VERSION, "terminal_at": now_iso(), "classification": "infrastructure_or_judge_attempt_failed", "failed_turn_name": turn_name, "error_class": error_class, "retry_allowed_in_this_version": False, "accounting_complete": complete, "usage_status": "complete" if complete else "unknown", "usage": usage if complete else None, "known_usage_lower_bound": usage, "unknown_usage_turn_count": unknown, "attempts": attempts}
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {"schema_version": V155_TERMINAL_VERSION, "state": "failed", "terminal_reason": "infrastructure_or_judge_attempt_failed", "overall_evaluation_complete": False, "failure": _record(failure_path), "development_judge_frozen": False, "selection_authorized": False, "holdout_authorized": False, "production_mutated": False, "semantic_retry_count": 0, "accounting_complete": complete, "usage_status": failure["usage_status"], "usage": failure["usage"]}
    _write_immutable(root / "terminal.json", terminal)
    return terminal


async def run_v155(*, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS, client_factory: Optional[Callable[[Path], Any]] = None) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v155 terminal")
    frozen = freeze_v155(output_dir=root, timeout_seconds=timeout_seconds)
    current_turn: Optional[str] = None
    sidecars = []
    try:
        support_primary_outputs = []
        support_canary_outputs = []
        field_outputs = []
        field_repeat_outputs = []
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
                    base_instructions=v143.support_base_instructions_v143() if is_support else v146.base_instructions_v146(),
                    model=SUPPORT_MODEL if is_support else FIELD_MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=turn["value"].get("unit_count", turn["value"].get("task_count")),
                    policy_path=frozen["capacity_policy"],
                    output_validator=(lambda candidate, item=turn["value"]: v143.validate_support_output(candidate, item)) if is_support else (lambda candidate, item=turn["value"]: validate_field_output(candidate, item)),
                )
                sidecars.append(sidecar)
                if turn["turn_role"] == "support_primary": support_primary_outputs.append(output)
                elif turn["turn_role"] == "support_canary": support_canary_outputs.append(output)
                elif turn["turn_role"] == "field_singleton": field_outputs.append(output)
                else: field_repeat_outputs.append(output)
            support = _merge_outputs(support_primary_outputs, "units")
            support_canary = _merge_outputs(support_canary_outputs, "units")
            fields = _merge_outputs(field_outputs, "decisions")
            field_repeats = _merge_outputs(field_repeat_outputs, "decisions")
            if v143.validate_support_output(support, _support_value(frozen["data"]["pointwise"]["units"])):
                raise JudgeV5CalibrationV155Error("v155 aggregate support output is invalid")
            receipts = _support_receipts(support)
            _write_immutable(root / "support-output-full.private.json", support)
            _write_immutable(root / "support-canary-output.private.json", support_canary)
            _write_immutable(root / "support-receipts.private.json", receipts)
            _write_immutable(root / "field-output.private.json", fields)
            _write_immutable(root / "field-repeat-output.private.json", field_repeats)

            alignment_primary_outputs = []
            for turn_name, case_ids in zip(ALIGNMENT_PRIMARY_TURNS, frozen["data"]["case_shards"], strict=True):
                value = _support_positive_alignment_input(frozen["data"]["pool"], receipts, case_ids=case_ids, permutation="base")
                prompt = v130.alignment_prompt_v130(value)
                schema = neutral_alignment_output_schema(value)
                request_paths = _freeze_turn_request(root=root, turn_name=turn_name, input_value=value, prompt=prompt, schema=schema)
                current_turn = turn_name
                output, sidecar, _ = await _get_or_run_turn(client=client, turn_name=turn_name, paths=request_paths, prompt=prompt, schema=schema, base_instructions=v130.alignment_instructions_v130(), model=ALIGNMENT_MODEL, effort=EFFORT, timeout_seconds=timeout_seconds, batch_size=len(value["cases"]), policy_path=frozen["capacity_policy"], output_validator=lambda candidate, item=value: validate_neutral_alignment_output(candidate, item))
                alignment_primary_outputs.append(output)
                sidecars.append(sidecar)
            base_output = _merge_outputs(alignment_primary_outputs, "cases")
            base_input = _support_positive_alignment_input(frozen["data"]["pool"], receipts, case_ids=[case["case_id"] for case in frozen["data"]["pool"]["cases"]], permutation="base")
            if validate_neutral_alignment_output(base_output, base_input):
                raise JudgeV5CalibrationV155Error("v155 aggregate base alignment is invalid")
            _write_immutable(root / "alignment-base-output.private.json", base_output)

            canary_ids = frozen["data"]["truth"]["alignment_canary_case_ids"]
            canary_outputs = []
            canary_inputs = []
            for turn_name, case_ids in zip(ALIGNMENT_CANARY_TURNS, (canary_ids[:6], canary_ids[6:]), strict=True):
                value = _support_positive_alignment_input(frozen["data"]["pool"], receipts, case_ids=case_ids, permutation="balanced_canary")
                prompt = v130.alignment_prompt_v130(value)
                schema = neutral_alignment_output_schema(value)
                request_paths = _freeze_turn_request(root=root, turn_name=turn_name, input_value=value, prompt=prompt, schema=schema)
                current_turn = turn_name
                output, sidecar, _ = await _get_or_run_turn(client=client, turn_name=turn_name, paths=request_paths, prompt=prompt, schema=schema, base_instructions=v130.alignment_instructions_v130(), model=ALIGNMENT_MODEL, effort=EFFORT, timeout_seconds=timeout_seconds, batch_size=len(value["cases"]), policy_path=frozen["capacity_policy"], output_validator=lambda candidate, item=value: validate_neutral_alignment_output(candidate, item))
                canary_outputs.append(output)
                canary_inputs.append(value)
                sidecars.append(sidecar)
            canary_output = _merge_outputs(canary_outputs, "cases")
            canary_input = deepcopy(canary_inputs[0])
            canary_input["cases"] = [deepcopy(case) for value in canary_inputs for case in value["cases"]]
            if validate_neutral_alignment_output(canary_output, canary_input):
                raise JudgeV5CalibrationV155Error("v155 aggregate canary alignment is invalid")
            _write_immutable(root / "alignment-canary-output.private.json", canary_output)
            disagreements = build_disagreement_adjudication_input(base_input=base_input, base_output=base_output, canary_input=canary_input, canary_output=canary_output, support_receipts=receipts)
            _write_immutable(root / "alignment-observable-disagreements.private.json", disagreements)
            adjudication_output = None
            adjudication_alignment = None
            if disagreements["adjudication_required"]:
                packet = v153._balance_anonymous_candidates(deepcopy(disagreements))
                adjudication_alignment = v153.adjudication_alignment_input(base_input=base_input, adjudication_input=packet)
                prompt = v153.build_disagreement_adjudication_prompt(adjudication_input=packet, adjudication_alignment=adjudication_alignment)
                schema = neutral_alignment_output_schema(adjudication_alignment)
                request_paths = _freeze_turn_request(root=root, turn_name=ADJUDICATION_TURN, input_value={"packet": packet, "alignment_input": adjudication_alignment}, prompt=prompt, schema=schema)
                current_turn = ADJUDICATION_TURN
                adjudication_output, sidecar, _ = await _get_or_run_turn(client=client, turn_name=ADJUDICATION_TURN, paths=request_paths, prompt=prompt, schema=schema, base_instructions=v153.adjudication_instructions_v153(), model=ADJUDICATOR_MODEL, effort=EFFORT, timeout_seconds=timeout_seconds, batch_size=len(adjudication_alignment["cases"]), policy_path=frozen["capacity_policy"], output_validator=lambda candidate: validate_neutral_alignment_output(candidate, adjudication_alignment))
                sidecars.append(sidecar)
                _write_immutable(root / "alignment-adjudication-output.private.json", adjudication_output)
            reconciled = reconcile_neutral_alignment(base_output=base_output, base_input=base_input, canary_output=canary_output, canary_input=canary_input, support_receipts=receipts, adjudication_output=adjudication_output, adjudication_input=adjudication_alignment)
            _write_immutable(root / "alignment-reconciled.private.json", reconciled)

        final_canary_exact = len(frozen["data"]["truth"]["alignment_canary_case_ids"]) if not reconciled["unresolved_cases_abstained"] else 0
        score = score_v155(support=support, support_canary=support_canary, fields=fields, field_repeats=field_repeats, alignment=reconciled, truth=frozen["data"]["truth"], raw_alignment_disagreement_count=disagreements["disagreement_case_count"], final_canary_exact_count=final_canary_exact)
        score_path = root / "full-calibration-score.json"
        _write_immutable(score_path, score)
        passed = bool(score["passed"])
        protocol_path = root / "development-judge-protocol-v155.json"
        if passed:
            protocol = {"schema_version": V155_PROTOCOL_VERSION, "frozen_at": now_iso(), "support_model": SUPPORT_MODEL, "field_model": FIELD_MODEL, "alignment_model": ALIGNMENT_MODEL, "adjudicator_model": ADJUDICATOR_MODEL, "reasoning_effort": EFFORT, "support_instructions_sha256": sha256_text(v143.support_base_instructions_v143()), "field_instructions_sha256": sha256_text(v146.base_instructions_v146()), "alignment_instructions_sha256": sha256_text(v130.alignment_instructions_v130()), "adjudication_instructions_sha256": sha256_text(v153.adjudication_instructions_v153()), "field_protocol": frozen["spec"]["frozen_inputs"]["field_protocol"], "alignment_protocol": frozen["spec"]["frozen_inputs"]["alignment_protocol"], "reference": frozen["spec"]["frozen_inputs"]["reference"], "truth": frozen["spec"]["frozen_inputs"]["truth"], "adjudication_call_cap": 1, "retry_count_per_turn": 0, "quality_gates_unchanged": True, "selection_authorized": True, "holdout_authorized": False, "production_mutation_allowed": False}
            _write_immutable(protocol_path, protocol)
        accounting = _aggregate_usage(sidecars)
        terminal = {"schema_version": V155_TERMINAL_VERSION, "state": "completed" if passed else "inactive", "terminal_at": now_iso(), "terminal_reason": "v155_full_development_calibration_passed_selection_authorized" if passed else "inactive_incomplete_recovery_required", "development_terminal_reason": "v155_full_development_calibration_passed" if passed else "v155_full_development_calibration_quality_gate_not_passed", "overall_evaluation_complete": False, "development_judge_frozen": passed, "selection_authorized": passed, "holdout_authorized": False, "production_mutated": False, "semantic_attempt_started": True, "semantic_retry_count": 0, "failed_quality_gates": score["failed_checks"], "metrics": score["metrics"], "score": _record(score_path), "protocol": _record(protocol_path) if passed else None, "reference": frozen["spec"]["frozen_inputs"]["reference"], "truth": frozen["spec"]["frozen_inputs"]["truth"], "predecessor_v154_usage": frozen["source"]["usage"], **accounting}
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v155 fresh full development calibration")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(run_v155(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds))
    print(json.dumps({"state": terminal["state"], "terminal_reason": terminal["terminal_reason"], "development_judge_frozen": terminal.get("development_judge_frozen", False), "selection_authorized": terminal.get("selection_authorized", False), "usage_status": terminal.get("usage_status")}, sort_keys=True))
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
