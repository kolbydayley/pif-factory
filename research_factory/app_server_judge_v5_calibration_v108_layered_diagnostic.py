from __future__ import annotations

"""Small layered diagnostic for the two systematic v106 judge failures."""

import argparse
import asyncio
import hashlib
import json
import math
from collections import Counter
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from .app_server_judge_v5 import (
    CHECKLIST_FIELDS,
    adjudication_alignment_input,
    build_disagreement_adjudication_prompt,
    build_neutral_alignment_input,
    build_neutral_alignment_prompt,
    freeze_support_receipts,
    neutral_alignment_base_instructions,
    neutral_alignment_output_schema,
)
from .app_server_judge_v5_calibration import CALIBRATION_GATES, pointwise_input_subset
from .app_server_judge_v5_calibration_v25_diagnostic import (
    QUOTA_POINTS_PER_MILLION_TOKENS,
)
from .app_server_judge_v5_calibration_v26_diagnostic import (
    MAX_TOKENS_PER_TURN,
    JudgeV5CalibrationV26DiagnosticAttemptFailed,
    _aggregate_usage,
    _build_scoreable_adjudication_input,
    _client_factory,
    _find_scoreable_disagreements,
    _freeze_turn_request,
    _get_or_run_turn,
    _load_json,
    _merge_outputs,
    _reconcile_scoreable_alignment,
    _validate_scoreable_alignment_output,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _record_matches,
)
from .app_server_judge_v5_calibration_v106_full_development import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V106_ROOT,
)
from .app_server_judge_v5_calibration_v107_recovery_receipt import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V107_ROOT,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .app_server_judge_v5_fixture import load_fixture_truth_audit
from .app_server_llm_judge import (
    SUPPORT_VERDICTS,
    validate_app_server_output_schema_subset,
)
from .util import now_iso


V108_SPEC_VERSION = "pif_app_server_judge_v5_4_v108_layered_diagnostic_spec_v1"
V108_POINTWISE_VERSION = "pif_app_server_judge_v5_4_v108_pointwise_checklist_v1"
V108_SCORE_VERSION = "pif_app_server_judge_v5_4_v108_diagnostic_score_v1"
V108_FAILURE_VERSION = "pif_app_server_judge_v5_4_v108_failure_v1"
V108_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v108_terminal_v1"
V108_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V108_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V108_PHASE_ID = "judge_v5_4_v108_layered_diagnostic"

PRIMARY_MODEL = "gpt-5.5"
ADJUDICATOR_MODEL = "gpt-5.6-sol"
EFFORT = "high"
CASES_PER_SHARD = 6
POINTWISE_TURNS = tuple(f"pointwise_checklist_shard_{index:02d}" for index in range(3))
BASE_TURNS = tuple(f"neutral_alignment_base_shard_{index:02d}" for index in range(3))
CANARY_TURNS = tuple(f"neutral_alignment_canary_shard_{index:02d}" for index in range(2))
ADJUDICATION_TURN = "disagreement_adjudication"
TURN_NAMES = POINTWISE_TURNS + BASE_TURNS + CANARY_TURNS + (ADJUDICATION_TURN,)
TIMEOUT_SECONDS = 1200.0
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V107_ROOT.parent / "judge-calibration-v5_4-v108-layered-diagnostic"
).resolve()


class JudgeV5CalibrationV108Error(RuntimeError):
    """The v108 diagnostic cannot preserve its frozen evidence contract."""


def _validate_predecessors() -> dict[str, Any]:
    paths = {
        "v106_spec": DEFAULT_V106_ROOT / "full-calibration-spec.json",
        "v106_terminal": DEFAULT_V106_ROOT / "terminal.json",
        "v106_failure": DEFAULT_V106_ROOT / "failure.json",
        "v106_score": DEFAULT_V106_ROOT / "calibration-score.json",
        "v106_pool": DEFAULT_V106_ROOT / "shared-witness-pool.private.json",
        "v106_truth": DEFAULT_V106_ROOT / "calibration-truth.private.json",
        "v106_pointwise_input": DEFAULT_V106_ROOT / "pointwise-input-full.private.json",
        "v106_pointwise_output": DEFAULT_V106_ROOT / "pointwise-output-full.private.json",
        "v106_reconciled": DEFAULT_V106_ROOT / "reconciled-alignment.private.json",
        "v107_terminal": DEFAULT_V107_ROOT / "terminal.json",
        "v107_receipt": DEFAULT_V107_ROOT / "recovery-receipt.json",
        "v107_taxonomy": DEFAULT_V107_ROOT / "sanitized-error-taxonomy.json",
    }
    values = {name: _load_json(path, name) for name, path in paths.items()}
    t106, f106, s106, t107 = (
        values["v106_terminal"],
        values["v106_failure"],
        values["v106_score"],
        values["v107_terminal"],
    )
    if (
        t106.get("state") != "failed"
        or t106.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or t106.get("usage_status") != "complete"
        or t106.get("accounting_complete") is not True
        or t106.get("production_mutated") is not False
        or f106.get("failed_turn_name") != "disagreement_adjudication"
        or f106.get("error_class") != "KeyError"
        or s106.get("passed") is not False
        or t107.get("state") != "inactive"
        or t107.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or t107.get("fresh_small_diagnostic_required") is not True
        or t107.get("fresh_full_calibration_authorized") is not False
        or t107.get("selection_authorized") is not False
        or t107.get("holdout_authorized") is not False
        or t107.get("production_mutated") is not False
        or t107.get("usage", {}).get("total_tokens") != 0
        or not _record_matches(t107.get("error_taxonomy"), paths["v107_taxonomy"])
    ):
        raise JudgeV5CalibrationV108Error("v106/v107 predecessor contract drifted")
    return {
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
    }


def _select_case_ids(predecessor: Mapping[str, Any]) -> dict[str, list[str]]:
    truth = predecessor["values"]["v106_truth"]["cases"]
    pointwise = {
        str(row["witness_id"]): row
        for row in predecessor["values"]["v106_pointwise_output"]["units"]
    }
    alignment = {
        str(row["case_id"]): row
        for row in predecessor["values"]["v106_reconciled"]["cases"]
    }
    unsupported = sorted(
        case_id
        for case_id, value in truth.items()
        if value["shape"] == "one_sided_unsupported_residuals"
    )
    boundary_errors = []
    boundary_controls = []
    for case_id, value in truth.items():
        if value["shape"] != "merged_and_split_boundaries":
            continue
        observed = {
            tuple(sorted(pair["witness_ids"])): pair.get("relation")
            for pair in alignment[case_id].get("alignment_pairs") or []
        }
        has_error = any(
            observed.get(tuple(sorted(pair["witness_ids"]))) != pair["relation"]
            for pair in value["pairs"]
        )
        (boundary_errors if has_error else boundary_controls).append(case_id)
    boundary = sorted(boundary_errors) + sorted(boundary_controls)[:1]
    if len(unsupported) != 10 or len(boundary_errors) != 3 or len(boundary) != 4:
        raise JudgeV5CalibrationV108Error("v108 targeted case strata drifted")
    diverse = []
    for shape in (
        "single_event_pairs",
        "multi_event_set_alignment",
        "one_sided_supported_residuals",
        "same_side_equivalence_partition",
    ):
        candidates = []
        for case_id, value in truth.items():
            if value["shape"] != shape:
                continue
            errors = sum(
                pointwise[witness_id]["structured_field_verdict"]
                != value["structured_fields"][witness_id]
                for witness_id in value["proposition"]
            )
            candidates.append((-errors, case_id))
        if not candidates:
            raise JudgeV5CalibrationV108Error("v108 diverse control stratum drifted")
        diverse.append(sorted(candidates)[0][1])
    selected = unsupported + boundary + diverse
    canary = unsupported + sorted(boundary_errors)[:2]
    if len(selected) != 18 or len(set(selected)) != 18 or len(canary) != 12:
        raise JudgeV5CalibrationV108Error("v108 diagnostic selection drifted")
    return {
        "selected": selected,
        "canary": canary,
        "unsupported_residual": unsupported,
        "boundary": boundary,
        "diverse": diverse,
    }


def _subset_truth(truth: Mapping[str, Any], selection: Mapping[str, Sequence[str]]) -> dict[str, Any]:
    selected = list(selection["selected"])
    value = deepcopy(dict(truth))
    value["cases"] = {case_id: deepcopy(truth["cases"][case_id]) for case_id in selected}
    value["case_count"] = len(selected)
    value["witness_count"] = sum(
        len(case["proposition"]) for case in value["cases"].values()
    )
    value["canary_case_ids"] = list(selection["canary"])
    value["diagnostic_subset_of_reference_v8"] = True
    return value


def _case_shards(case_ids: Sequence[str]) -> list[list[str]]:
    shards = [list(case_ids[index : index + CASES_PER_SHARD]) for index in range(0, len(case_ids), CASES_PER_SHARD)]
    if len(shards) != 3 or any(len(shard) != 6 for shard in shards):
        raise JudgeV5CalibrationV108Error("v108 case shards drifted")
    return shards


def pointwise_checklist_schema(pointwise_input: Mapping[str, Any]) -> dict[str, Any]:
    units = pointwise_input.get("units")
    if not isinstance(units, list) or not units:
        raise JudgeV5CalibrationV108Error("v108 pointwise input is empty")
    case_ids = sorted({str(unit["case_id"]) for unit in units})
    witness_ids = sorted({str(unit["witness_id"]) for unit in units})
    spans = {
        "type": "array",
        "maxItems": 2,
        "items": {"type": "string", "minLength": 1, "maxLength": 1000},
    }
    check = {
        "type": "object",
        "additionalProperties": False,
        "required": ["field", "decision", "source_evidence_spans", "rationale"],
        "properties": {
            "field": {"type": "string", "enum": list(CHECKLIST_FIELDS)},
            "decision": {
                "type": "string",
                "enum": ["correct", "incorrect", "abstain"],
            },
            "source_evidence_spans": spans,
            "rationale": {"type": "string", "minLength": 1, "maxLength": 240},
        },
    }
    row = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "case_id",
            "witness_id",
            "proposition_verdict",
            "proposition_evidence_spans",
            "proposition_rationale",
            "field_checklist",
        ],
        "properties": {
            "case_id": {"type": "string", "enum": case_ids},
            "witness_id": {"type": "string", "enum": witness_ids},
            "proposition_verdict": {"type": "string", "enum": list(SUPPORT_VERDICTS)},
            "proposition_evidence_spans": spans,
            "proposition_rationale": {"type": "string", "minLength": 1, "maxLength": 600},
            "field_checklist": {
                "type": "array",
                "minItems": len(CHECKLIST_FIELDS),
                "maxItems": len(CHECKLIST_FIELDS),
                "items": check,
            },
        },
    }
    schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["units"],
        "properties": {
            "units": {
                "type": "array",
                "minItems": len(units),
                "maxItems": len(units),
                "items": row,
            }
        },
    }
    errors = validate_app_server_output_schema_subset(schema)
    if errors:
        raise JudgeV5CalibrationV108Error("v108 pointwise schema is unsupported: %s" % "; ".join(errors))
    return schema


def pointwise_checklist_instructions() -> str:
    return (
        "You are a side-free pointwise factual-support evaluator. Each unit contains one opaque "
        "witness, its complete structured event, and its source excerpt. First judge only whether "
        "every material proposition in proposition.claim_text is entailed by the source; harmless "
        "paraphrase and resolved coreference pass, but exact wording alone does not license an "
        "unsupported inference. Then complete all 15 field checks independently. A populated field "
        "is correct only when the source licenses that field's exact truth-conditional content; a "
        "contradiction or unsupported addition is incorrect. An empty or omitted event field is "
        "correct, not an error. Do not mark speaker merely because actor or attribution differs; do "
        "not mark attribution merely because speaker, reported_actor, or actor differs. Mark only "
        "independently wrong minimal root fields. unsupported_inference is incorrect exactly when "
        "claim_text adds an unsupported material assertion, correct when every material claim is "
        "source-supported, and abstain only when proposition support abstains. Cite exact source "
        "substrings. Do not infer origin or use keyword, regex, token overlap, embeddings, confidence, "
        "or majority voting."
    )


def build_pointwise_checklist_prompt(pointwise_input: Mapping[str, Any]) -> str:
    audit = load_fixture_truth_audit()
    rubric = [
        item
        for field in CHECKLIST_FIELDS
        for item in audit["mismatch_checklist"]
        if item["field"] == field
    ]
    packet = {
        "field_order": list(CHECKLIST_FIELDS),
        "rubric": rubric,
        "units": pointwise_input["units"],
    }
    return (
        "Return each case_id/witness_id exactly once and all 15 checks exactly once in field_order. "
        "A supported proposition requires exact source evidence. Each field decision requires a "
        "short independent rationale and zero to two exact source spans. Never collapse related "
        "categories into duplicate errors.\n\n# Side-free pointwise checklist units\n"
        + json.dumps(packet, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        + "\n"
    )


def validate_pointwise_checklist_output(output: Any, pointwise_input: Mapping[str, Any]) -> list[str]:
    if not isinstance(output, dict) or set(output) != {"units"} or not isinstance(output.get("units"), list):
        return ["invalid_pointwise_checklist_root"]
    expected = {
        (str(unit["case_id"]), str(unit["witness_id"])): unit
        for unit in pointwise_input.get("units") or []
    }
    errors = []
    seen = set()
    required = {
        "case_id",
        "witness_id",
        "proposition_verdict",
        "proposition_evidence_spans",
        "proposition_rationale",
        "field_checklist",
    }
    check_required = {"field", "decision", "source_evidence_spans", "rationale"}
    for index, row in enumerate(output["units"]):
        prefix = f"unit_{index}"
        if not isinstance(row, dict) or set(row) != required:
            errors.append(prefix + "_shape")
            continue
        key = (str(row.get("case_id")), str(row.get("witness_id")))
        if key not in expected or key in seen:
            errors.append(prefix + "_id")
            continue
        seen.add(key)
        source = expected[key]["source_excerpt"]
        proposition = row.get("proposition_verdict")
        prop_spans = row.get("proposition_evidence_spans")
        if proposition not in SUPPORT_VERDICTS:
            errors.append(prefix + "_proposition")
        if (
            not isinstance(prop_spans, list)
            or len(prop_spans) > 2
            or len(prop_spans) != len(set(prop_spans))
            or any(not isinstance(span, str) or not span or span not in source for span in prop_spans)
            or (proposition == "supported" and not prop_spans)
        ):
            errors.append(prefix + "_proposition_evidence")
        rationale = row.get("proposition_rationale")
        if not isinstance(rationale, str) or not rationale or len(rationale) > 600:
            errors.append(prefix + "_proposition_rationale")
        checklist = row.get("field_checklist")
        if (
            not isinstance(checklist, list)
            or [item.get("field") for item in checklist if isinstance(item, dict)] != list(CHECKLIST_FIELDS)
        ):
            errors.append(prefix + "_checklist_order")
            continue
        decisions = {}
        for check_index, item in enumerate(checklist):
            item_prefix = f"{prefix}_check_{check_index}"
            if not isinstance(item, dict) or set(item) != check_required:
                errors.append(item_prefix + "_shape")
                continue
            decision = item.get("decision")
            decisions[item["field"]] = decision
            spans = item.get("source_evidence_spans")
            if decision not in {"correct", "incorrect", "abstain"}:
                errors.append(item_prefix + "_decision")
            if (
                not isinstance(spans, list)
                or len(spans) > 2
                or len(spans) != len(set(spans))
                or any(not isinstance(span, str) or not span or span not in source for span in spans)
            ):
                errors.append(item_prefix + "_evidence")
            item_rationale = item.get("rationale")
            if not isinstance(item_rationale, str) or not item_rationale or len(item_rationale) > 240:
                errors.append(item_prefix + "_rationale")
        unsupported = decisions.get("unsupported_inference")
        expected_unsupported = {
            "supported": "correct",
            "unsupported": "incorrect",
            "abstain": "abstain",
        }.get(proposition)
        if unsupported != expected_unsupported:
            errors.append(prefix + "_unsupported_inference_consistency")
    if seen != set(expected):
        errors.append("pointwise_checklist_coverage")
    return errors


def project_pointwise_checklist(output: Mapping[str, Any]) -> dict[str, Any]:
    units = []
    for row in output["units"]:
        incorrect = [item for item in row["field_checklist"] if item["decision"] == "incorrect"]
        abstain = [item for item in row["field_checklist"] if item["decision"] == "abstain"]
        verdict = "incorrect" if incorrect else ("abstain" if abstain else "correct")
        evidence = []
        for item in incorrect or abstain:
            for span in item["source_evidence_spans"]:
                if span not in evidence:
                    evidence.append(span)
        units.append(
            {
                "case_id": row["case_id"],
                "witness_id": row["witness_id"],
                "proposition_verdict": row["proposition_verdict"],
                "proposition_evidence_spans": row["proposition_evidence_spans"],
                "proposition_rationale": row["proposition_rationale"],
                "structured_field_verdict": verdict,
                "field_issue_fields": [item["field"] for item in incorrect],
                "field_evidence_spans": evidence[:4],
                "field_rationale": "Explicit 15-field source-licensing checklist projection.",
            }
        )
    return {"units": units}


def alignment_instructions_v108() -> str:
    return neutral_alignment_base_instructions() + (
        " Freeze assignment before field comparison. First maximize full semantic equivalence among "
        "support-positive witnesses. In any case with three or more witnesses, reserve a supported "
        "duplicate counterpart for another supported witness before considering an unsupported "
        "residual; leave that residual unpaired when the supported-supported pair already covers its "
        "semantic core. Pair an unsupported residual only when no competing supported equivalent "
        "exists and it is the sole corresponding witness. For merge/split candidates, first normalize "
        "each witness into its source-supported atomic propositions. If the atom sets match and only "
        "grouping differs, relation is partial and the only different rows are event_boundary and "
        "evidence. Surface wording or mechanically inherited subject changes from regrouping are not "
        "independent speaker, attribution, actor, target, event_type, or stance differences."
    )


def build_alignment_prompt_v108(alignment_input: Mapping[str, Any]) -> str:
    return (
        "Apply assignment in this order: supported-equivalent pairs, then other one-to-one semantic "
        "correspondence, then residuals. Freeze that assignment before completing the 15-row checklist. "
        "Normalize merge/split atom sets before deciding relation.\n\n"
        + build_neutral_alignment_prompt(alignment_input)
    )


def _ratio(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 6) if denominator else 0.0


def _f1(tp: int, fp: int, fn: int) -> float:
    denominator = 2 * tp + fp + fn
    return round(2 * tp / denominator, 6) if denominator else 1.0


def score_v108(
    *,
    pointwise_output: Mapping[str, Any],
    reconciled_alignment: Mapping[str, Any],
    expected: Mapping[str, Any],
    observable_disagreements: Mapping[str, Any],
) -> dict[str, Any]:
    expected_cases = expected["cases"]
    pointwise_rows = {str(row["witness_id"]): row for row in pointwise_output["units"]}
    alignment_rows = {str(row["case_id"]): row for row in reconciled_alignment["cases"]}
    expected_witnesses = {
        witness_id for case in expected_cases.values() for witness_id in case["proposition"]
    }
    if set(pointwise_rows) != expected_witnesses or set(alignment_rows) != set(expected_cases):
        raise JudgeV5CalibrationV108Error("v108 score coverage drifted")
    support_tp = support_fn = support_tn = support_fp = 0
    structured_correct = structured_total = 0
    point_field_tp = point_field_fp = point_field_fn = 0
    pair_tp = pair_fp = pair_fn = 0
    relation_correct = relation_total = 0
    equivalent_tp = equivalent_fn = equivalent_tn = equivalent_fp = 0
    field_tp = field_fp = field_fn = 0
    partition_exact = unpaired_exact = abstentions = 0
    for truth in expected_cases.values():
        for witness_id, wanted_support in truth["proposition"].items():
            row = pointwise_rows[witness_id]
            observed_support = row["proposition_verdict"]
            abstentions += int(observed_support == "abstain")
            if wanted_support == "supported" and observed_support == "supported": support_tp += 1
            elif wanted_support == "supported": support_fn += 1
            elif observed_support == "unsupported": support_tn += 1
            else: support_fp += 1
            wanted_structured = truth["structured_fields"][witness_id]
            observed_structured = row["structured_field_verdict"]
            structured_total += 1
            structured_correct += int(wanted_structured == observed_structured)
            abstentions += int(observed_structured == "abstain")
            wanted_fields = set(truth["field_issues"][witness_id])
            observed_fields = set(row["field_issue_fields"])
            point_field_tp += len(wanted_fields & observed_fields)
            point_field_fp += len(observed_fields - wanted_fields)
            point_field_fn += len(wanted_fields - observed_fields)
    for case_id, truth in expected_cases.items():
        row = alignment_rows[case_id]
        abstentions += int(row.get("status") == "abstain")
        observed_pairs = {
            tuple(sorted(pair["witness_ids"])): pair for pair in row.get("alignment_pairs") or []
        }
        expected_pairs = {
            tuple(sorted(pair["witness_ids"])): pair for pair in truth["pairs"]
        }
        pair_tp += len(set(observed_pairs) & set(expected_pairs))
        pair_fp += len(set(observed_pairs) - set(expected_pairs))
        pair_fn += len(set(expected_pairs) - set(observed_pairs))
        for pair_ids, wanted in expected_pairs.items():
            observed = (observed_pairs.get(pair_ids) or {}).get("relation")
            abstentions += int(observed == "abstain")
            relation_total += 1
            relation_correct += int(observed == wanted["relation"])
            wanted_equivalent = wanted["relation"] == "equivalent"
            if wanted_equivalent and observed == "equivalent": equivalent_tp += 1
            elif wanted_equivalent: equivalent_fn += 1
            elif observed == "equivalent" or observed in {None, "abstain"}: equivalent_fp += 1
            else: equivalent_tn += 1
            wanted_fields = set(wanted["mismatch_fields"])
            observed_fields = set((observed_pairs.get(pair_ids) or {}).get("mismatch_fields") or [])
            field_tp += len(wanted_fields & observed_fields)
            field_fp += len(observed_fields - wanted_fields)
            field_fn += len(wanted_fields - observed_fields)
        observed_partition = {tuple(sorted(group)) for group in row.get("equivalence_groups") or []}
        expected_partition = {tuple(sorted(group)) for group in truth["equivalence_groups"]}
        partition_exact += int(observed_partition == expected_partition)
        unpaired_exact += int(sorted(row.get("unpaired_witness_ids") or []) == truth["unpaired_witness_ids"])
    raw_disagreements = int(observable_disagreements.get("disagreement_case_count") or 0)
    canary_count = len(expected["canary_case_ids"])
    metrics = {
        "case_count": len(expected_cases),
        "witness_count": structured_total,
        "support_sensitivity": _ratio(support_tp, support_tp + support_fn),
        "support_specificity": _ratio(support_tn, support_tn + support_fp),
        "structured_field_accuracy": _ratio(structured_correct, structured_total),
        "pointwise_field_issue_f1": _f1(point_field_tp, point_field_fp, point_field_fn),
        "alignment_f1": _f1(pair_tp, pair_fp, pair_fn),
        "equivalent_sensitivity": _ratio(equivalent_tp, equivalent_tp + equivalent_fn),
        "equivalent_specificity": _ratio(equivalent_tn, equivalent_tn + equivalent_fp),
        "field_diagnostic_f1": _f1(field_tp, field_fp, field_fn),
        "relation_accuracy": _ratio(relation_correct, relation_total),
        "equivalence_partition_exact_case_rate": _ratio(partition_exact, len(expected_cases)),
        "unpaired_exact_case_rate": _ratio(unpaired_exact, len(expected_cases)),
        "abstention_count": abstentions,
        "abstention_rate": _ratio(abstentions, structured_total + relation_total + len(expected_cases)),
        "order_bias": _ratio(raw_disagreements, canary_count),
        "canary_case_count": canary_count,
    }
    checks = {
        "case_count": metrics["case_count"] == 18,
        "canary_case_count": canary_count == 12,
        "support_sensitivity": metrics["support_sensitivity"] >= 0.95,
        "support_specificity": metrics["support_specificity"] >= 0.95,
        "structured_field_accuracy": metrics["structured_field_accuracy"] >= 0.95,
        "pointwise_field_issue_f1": metrics["pointwise_field_issue_f1"] >= 0.85,
        "alignment_f1": metrics["alignment_f1"] >= 0.95,
        "equivalent_sensitivity": metrics["equivalent_sensitivity"] >= 0.90,
        "equivalent_specificity": metrics["equivalent_specificity"] >= 0.90,
        "field_diagnostic_f1": metrics["field_diagnostic_f1"] >= 0.85,
        "relation_accuracy": metrics["relation_accuracy"] >= 0.90,
        "equivalence_partition_exact_case_rate": metrics["equivalence_partition_exact_case_rate"] >= 0.95,
        "unpaired_exact_case_rate": metrics["unpaired_exact_case_rate"] >= 0.95,
        "abstention_rate": metrics["abstention_rate"] == 0.0,
        "order_bias": metrics["order_bias"] <= 0.05,
    }
    return {
        "schema_version": V108_SCORE_VERSION,
        "passed": all(checks.values()),
        "metrics": metrics,
        "checks": checks,
        "failed_checks": sorted(key for key, passed in checks.items() if not passed),
        "thresholds": {
            **CALIBRATION_GATES,
            "diagnostic_case_count": 18,
            "pointwise_field_issue_f1_min": 0.85,
        },
    }


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    audit = {
        "schema_version": V108_CAPACITY_AUDIT_VERSION,
        "phase_id": V108_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
        },
    }
    _write_immutable(audit_path, audit)
    policy = {
        "schema_version": V108_CAPACITY_POLICY_VERSION,
        "phase_id": V108_PHASE_ID,
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
        "projected_phase_quota_points": math.ceil(bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000),
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_immutable(policy_path, policy)
    return {"audit": audit_path, "policy": policy_path}


def freeze_v108(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessor = _validate_predecessors()
    selection = _select_case_ids(predecessor)
    expected = _subset_truth(predecessor["values"]["v106_truth"], selection)
    pool = predecessor["values"]["v106_pool"]
    pointwise_full = predecessor["values"]["v106_pointwise_input"]
    shards = _case_shards(selection["selected"])
    files = {
        "selection": root / "diagnostic-selection.json",
        "truth": root / "diagnostic-truth.private.json",
    }
    _write_immutable(files["selection"], selection)
    _write_immutable(files["truth"], expected)
    pointwise_shards = []
    for turn_name, case_ids in zip(POINTWISE_TURNS, shards, strict=True):
        value = pointwise_input_subset(pointwise_full, case_ids)
        prompt, schema = build_pointwise_checklist_prompt(value), pointwise_checklist_schema(value)
        paths = _freeze_turn_request(root=root, turn_name=turn_name, input_value=value, prompt=prompt, schema=schema)
        pointwise_shards.append({"turn_name": turn_name, "case_ids": case_ids, "input": value, "prompt": prompt, "schema": schema, "paths": paths})
    capacity = _build_capacity_policy(root, predecessor["records"])
    runtime_dir = Path(__file__).resolve().parent
    runtime_files = [
        Path(__file__),
        runtime_dir / "app_server_judge_v5_calibration_v107_recovery_receipt.py",
        runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py",
        runtime_dir / "app_server_judge_v5_calibration.py",
        runtime_dir / "app_server_judge_v5.py",
        runtime_dir / "app_server_capacity_reserve.py",
        runtime_dir / "codex_app_server.py",
    ]
    spec = {
        "schema_version": V108_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "primary_model": PRIMARY_MODEL,
        "adjudicator_model": ADJUDICATOR_MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "case_count": 18,
        "canary_case_count": 12,
        "cases_per_shard": CASES_PER_SHARD,
        "minimum_turn_count": 8,
        "maximum_turn_count": 9,
        "turn_plan": list(TURN_NAMES),
        "retry_count_per_turn": 0,
        "explicit_pointwise_15_field_checklist": True,
        "supported_equivalent_assignment_priority": True,
        "merge_split_atom_normalization": True,
        "all_semantic_turns_fresh": True,
        "prior_model_outputs_reused": False,
        "reference_truth_exposed_to_model": False,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor["records"],
        "runtime_files": [_record(path) for path in runtime_files],
        "frozen_inputs": {
            "selection": _record(files["selection"]),
            "truth": _record(files["truth"]),
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
    spec_path = root / "layered-diagnostic-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v108 spec")
        spec["created_at"] = prior.get("created_at")
        if spec != prior:
            raise JudgeV5CalibrationV108Error("immutable v108 spec drifted")
    else:
        _write_immutable(spec_path, spec)
    return {
        "root": root,
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


def _write_failure(*, root: Path, turn_name: Optional[str], error_class: str) -> dict[str, Any]:
    attempts = [row for row in _attempt_records(root) if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))]
    usage = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping):
            unknown += 1
            continue
        try:
            measured = _validate_usage(_load_json(Path(record["path"]), "v108 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V108_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": turn_name,
        "error_class": error_class,
        "retry_allowed_in_this_version": False,
        "attempts": attempts,
        "accounting_complete": complete,
        "usage_status": "complete" if complete else "unknown",
        "usage": usage if complete else None,
        "known_usage_lower_bound": usage,
        "unknown_usage_turn_count": unknown,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V108_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "diagnostic_passed": False,
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


async def run_v108(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v108 terminal")
    frozen = freeze_v108(output_dir=root, timeout_seconds=timeout_seconds)
    policy_path = frozen["capacity_policy"]
    factory = client_factory or _client_factory
    sidecars = []
    adoptions = {}
    current_turn = None
    try:
        async with factory(policy_path) as client:
            explicit_outputs = []
            for shard in frozen["pointwise_shards"]:
                current_turn = shard["turn_name"]
                output, sidecar, adopted = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=shard["paths"],
                    prompt=shard["prompt"],
                    schema=shard["schema"],
                    base_instructions=pointwise_checklist_instructions(),
                    model=PRIMARY_MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(shard["input"]["units"]),
                    policy_path=policy_path,
                    output_validator=lambda value, item=shard: validate_pointwise_checklist_output(value, item["input"]),
                )
                explicit_outputs.append(output)
                sidecars.append(sidecar)
                adoptions[current_turn] = adopted
            explicit = _merge_outputs(explicit_outputs, "units")
            full_selected_input = pointwise_input_subset(frozen["pointwise_full"], frozen["selection"]["selected"])
            errors = validate_pointwise_checklist_output(explicit, full_selected_input)
            if errors:
                raise JudgeV5CalibrationV108Error("aggregate pointwise checklist invalid")
            explicit_path = root / "pointwise-checklist-full.private.json"
            _write_immutable(explicit_path, explicit)
            pointwise_output = project_pointwise_checklist(explicit)
            pointwise_path = root / "pointwise-output-full.private.json"
            _write_immutable(pointwise_path, pointwise_output)
            support_receipts = freeze_support_receipts(pointwise_output, full_selected_input)
            support_path = root / "support-receipts.private.json"
            _write_immutable(support_path, support_receipts)

            base_input = build_neutral_alignment_input(
                frozen["pool"], support_receipts, case_ids=frozen["selection"]["selected"]
            )
            base_outputs = []
            for turn_name, case_ids in zip(BASE_TURNS, frozen["shards"], strict=True):
                shard_input = build_neutral_alignment_input(frozen["pool"], support_receipts, case_ids=case_ids)
                prompt, schema = build_alignment_prompt_v108(shard_input), neutral_alignment_output_schema(shard_input)
                current_turn = turn_name
                paths = _freeze_turn_request(root=root, turn_name=current_turn, input_value=shard_input, prompt=prompt, schema=schema)
                output, sidecar, adopted = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=paths,
                    prompt=prompt,
                    schema=schema,
                    base_instructions=alignment_instructions_v108(),
                    model=PRIMARY_MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=6,
                    policy_path=policy_path,
                    output_validator=lambda value, item=shard_input: _validate_scoreable_alignment_output(value, item),
                )
                base_outputs.append(output)
                sidecars.append(sidecar)
                adoptions[current_turn] = adopted
            base_output = _merge_outputs(base_outputs, "cases")
            if _validate_scoreable_alignment_output(base_output, base_input):
                raise JudgeV5CalibrationV108Error("aggregate base alignment invalid")
            base_path = root / "base-alignment-full.private.json"
            _write_immutable(base_path, base_output)

            canary_input = build_neutral_alignment_input(
                frozen["pool"], support_receipts, case_ids=frozen["selection"]["canary"], permutation="balanced_canary"
            )
            canary_order = [row["case_id"] for row in canary_input["cases"]]
            canary_shards = [canary_order[index : index + 6] for index in range(0, 12, 6)]
            canary_outputs = []
            for turn_name, case_ids in zip(CANARY_TURNS, canary_shards, strict=True):
                shard_input = build_neutral_alignment_input(
                    frozen["pool"], support_receipts, case_ids=case_ids, permutation="balanced_canary"
                )
                prompt, schema = build_alignment_prompt_v108(shard_input), neutral_alignment_output_schema(shard_input)
                current_turn = turn_name
                paths = _freeze_turn_request(root=root, turn_name=current_turn, input_value=shard_input, prompt=prompt, schema=schema)
                output, sidecar, adopted = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=paths,
                    prompt=prompt,
                    schema=schema,
                    base_instructions=alignment_instructions_v108(),
                    model=PRIMARY_MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=6,
                    policy_path=policy_path,
                    output_validator=lambda value, item=shard_input: _validate_scoreable_alignment_output(value, item),
                )
                canary_outputs.append(output)
                sidecars.append(sidecar)
                adoptions[current_turn] = adopted
            canary_output = _merge_outputs(canary_outputs, "cases")
            canary_path = root / "canary-alignment-full.private.json"
            _write_immutable(canary_path, canary_output)
            disagreements = _find_scoreable_disagreements(
                base_output=base_output,
                base_input=base_input,
                canary_output=canary_output,
                canary_input=canary_input,
            )
            disagreements_path = root / "observable-disagreements.private.json"
            _write_immutable(disagreements_path, disagreements)
            adjudication_output = adjudication_input = None
            if disagreements["adjudication_required"]:
                packet = _build_scoreable_adjudication_input(
                    base_input=base_input,
                    base_output=base_output,
                    canary_input=canary_input,
                    canary_output=canary_output,
                )
                adjudication_input = adjudication_alignment_input(base_input=base_input, adjudication_input=packet)
                prompt = build_disagreement_adjudication_prompt(adjudication_input=packet, adjudication_alignment=adjudication_input)
                schema = neutral_alignment_output_schema(adjudication_input)
                current_turn = ADJUDICATION_TURN
                paths = _freeze_turn_request(
                    root=root,
                    turn_name=current_turn,
                    input_value={"adjudication_packet": packet, "alignment_input": adjudication_input},
                    prompt=prompt,
                    schema=schema,
                )
                adjudication_output, sidecar, adopted = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=paths,
                    prompt=prompt,
                    schema=schema,
                    base_instructions=alignment_instructions_v108(),
                    model=ADJUDICATOR_MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(adjudication_input["cases"]),
                    policy_path=policy_path,
                    output_validator=lambda value: _validate_scoreable_alignment_output(value, adjudication_input),
                )
                sidecars.append(sidecar)
                adoptions[current_turn] = adopted
        reconciled = _reconcile_scoreable_alignment(
            base_output=base_output,
            base_input=base_input,
            adjudication_output=adjudication_output,
            adjudication_input=adjudication_input,
        )
        reconciled_path = root / "reconciled-alignment.private.json"
        _write_immutable(reconciled_path, reconciled)
        score = score_v108(
            pointwise_output=pointwise_output,
            reconciled_alignment=reconciled,
            expected=frozen["expected"],
            observable_disagreements=disagreements,
        )
        score_path = root / "diagnostic-score.json"
        _write_immutable(score_path, score)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V108_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": "v108_layered_diagnostic_passed_fresh_full_calibration_authorized" if passed else "inactive_incomplete_recovery_required",
            "development_terminal_reason": "v108_layered_diagnostic_passed" if passed else "v108_layered_diagnostic_quality_gate_not_passed",
            "overall_evaluation_complete": False,
            "diagnostic_passed": passed,
            "fresh_full_calibration_authorized": passed,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(score_path),
            "support_receipts": _record(support_path),
            "observable_disagreements": _record(disagreements_path),
            "reconciled_alignment": _record(reconciled_path),
            "attempts": _attempt_records(root),
            "completed_checkpoint_adoptions": adoptions,
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            "checks": score["checks"],
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root=root, turn_name=exc.turn_name, error_class=exc.error_class)
    except Exception as exc:
        return _write_failure(root=root, turn_name=current_turn, error_class=type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v108 layered judge diagnostic")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(run_v108(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds))
    print(json.dumps({
        "state": terminal["state"],
        "terminal_reason": terminal["terminal_reason"],
        "diagnostic_passed": terminal.get("diagnostic_passed", False),
        "fresh_full_calibration_authorized": terminal.get("fresh_full_calibration_authorized", False),
        "usage_status": terminal["usage_status"],
    }, sort_keys=True))
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
