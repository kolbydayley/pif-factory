from __future__ import annotations

"""Audited 66-case calibration truth, sharding, and scoring for judge v5.4."""

from collections import Counter, defaultdict
from copy import deepcopy
from typing import Any, Mapping, Sequence

from .app_server_judge_v5 import CHECKLIST_FIELDS, _f1, _ratio
from .app_server_judge_v5_fixture import load_fixture_truth_audit
from .app_server_llm_judge import make_v2_calibration_pool


CALIBRATION_TRUTH_VERSION = "pif_app_server_judge_v5_calibration_truth_v1"
CALIBRATION_SCORE_VERSION = "pif_app_server_judge_v5_calibration_score_v1"
CALIBRATION_CASES_PER_SHARD = 6
CALIBRATION_GATES = {
    "minimum_cases": 60,
    "support_sensitivity_min": 0.95,
    "support_specificity_min": 0.95,
    "structured_field_accuracy_min": 0.95,
    "alignment_f1_min": 0.95,
    "equivalent_sensitivity_min": 0.9,
    "equivalent_specificity_min": 0.9,
    "field_diagnostic_f1_min": 0.85,
    "relation_accuracy_min": 0.9,
    "equivalence_partition_exact_case_rate_min": 0.95,
    "unpaired_exact_case_rate_min": 0.95,
    "abstention_rate_max": 0.0,
    "order_bias_max": 0.05,
    "canary_case_count": 12,
}
CANARY_BASE_CASE_IDS = (
    "radiology__identity_paraphrase",
    "climate__material_field_error",
    "hiring__merged_split_boundary",
    "database__reversed_multi_lens",
    "language_tutor__supported_one_sided",
    "logistics__unsupported_one_sided",
    "legal_search__identity_paraphrase",
    "manufacturing__material_field_error",
    "audio_production__merged_split_boundary",
    "cybersecurity__reversed_multi_lens",
    "synthetic_same_side_equivalence_00",
    "synthetic_same_side_equivalence_01",
)


class JudgeV5CalibrationError(ValueError):
    """The audited calibration truth, shards, or score are invalid."""


def _mapping_by_case(mapping: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    rows = mapping.get("cases")
    if not isinstance(rows, list):
        raise JudgeV5CalibrationError("calibration mapping is malformed")
    return {str(row["case_id"]): row for row in rows}


def _witness_at(mapping_case: Mapping[str, Any], position: str) -> str:
    side, index_text = position.split(":", 1)
    index = int(index_text)
    row = next(
        (
            item
            for item in mapping_case["witnesses"]
            if item["canonical_side"] == side and item["canonical_index"] == index
        ),
        None,
    )
    if not isinstance(row, Mapping):
        raise JudgeV5CalibrationError("audited witness position is missing")
    return str(row["witness_id"])


def make_v5_calibration_pool() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    pool, mapping, legacy = make_v2_calibration_pool()
    audit = load_fixture_truth_audit()
    mapping_by_id = _mapping_by_case(mapping)
    legacy_by_base = {
        str(case["base_case_id"]): (case_id, case)
        for case_id, case in legacy["cases"].items()
    }
    expected_cases: dict[str, Any] = {}
    for case_id, legacy_case in legacy["cases"].items():
        proposition = deepcopy(legacy_case["support"])
        structured_fields = {
            witness_id: "correct" if verdict == "supported" else "incorrect"
            for witness_id, verdict in proposition.items()
        }
        field_issues = {
            witness_id: [] if verdict == "supported" else ["unsupported_inference"]
            for witness_id, verdict in proposition.items()
        }
        pairs = []
        paired_ids: set[str] = set()
        for pair in legacy_case["pairs"]:
            witness_ids = sorted(
                (str(pair["left_witness_id"]), str(pair["right_witness_id"]))
            )
            paired_ids.update(witness_ids)
            pairs.append(
                {
                    "witness_ids": witness_ids,
                    "relation": pair["relation"],
                    "mismatch_fields": list(pair["mismatch_fields"]),
                }
            )
        expected_cases[case_id] = {
            "base_case_id": legacy_case["base_case_id"],
            "shape": legacy_case["shape"],
            "proposition": proposition,
            "structured_fields": structured_fields,
            "field_issues": field_issues,
            "pairs": pairs,
            "equivalence_groups": [
                sorted(group) for group in legacy_case["equivalence_groups"]
            ],
            "unpaired_witness_ids": sorted(set(proposition) - paired_ids),
        }

    for item in audit["material_field_error_reclassifications"]:
        case_id, _legacy_case = legacy_by_base[item["case_id"]]
        mapping_case = mapping_by_id[case_id]
        witness_id = _witness_at(mapping_case, "b:0")
        truth = expected_cases[case_id]
        truth["proposition"][witness_id] = item["proposition_source_support"]
        truth["structured_fields"][witness_id] = item[
            "structured_event_field_correctness"
        ]
        truth["field_issues"][witness_id] = list(item["changed_fields"])

    for item in audit["language_tutor_structured_field_reclassifications"]:
        case_id, _legacy_case = legacy_by_base[item["case_id"]]
        mapping_case = mapping_by_id[case_id]
        truth = expected_cases[case_id]
        for position in item["incorrect_witness_positions"]:
            witness_id = _witness_at(mapping_case, position)
            truth["structured_fields"][witness_id] = "incorrect"
            truth["field_issues"][witness_id] = sorted(
                set(truth["field_issues"][witness_id])
                | set(item["incorrect_fields"]),
                key=CHECKLIST_FIELDS.index,
            )

    case_ids_by_base = {
        truth["base_case_id"]: case_id for case_id, truth in expected_cases.items()
    }
    if not set(CANARY_BASE_CASE_IDS) <= set(case_ids_by_base):
        raise JudgeV5CalibrationError("calibration canary membership drifted")
    canary_case_ids = [case_ids_by_base[key] for key in CANARY_BASE_CASE_IDS]
    expected = {
        "schema_version": CALIBRATION_TRUTH_VERSION,
        "cases": expected_cases,
        "canary_case_ids": canary_case_ids,
        "case_count": len(expected_cases),
        "legacy_joint_support_labels_used": False,
        "proposition_and_structured_field_truth_separate": True,
        "fixture_truth_audit_version": audit["schema_version"],
    }
    validate_v5_calibration_truth(pool=pool, mapping=mapping, expected=expected)
    return pool, mapping, expected


def validate_v5_calibration_truth(
    *, pool: Mapping[str, Any], mapping: Mapping[str, Any], expected: Mapping[str, Any]
) -> None:
    cases = expected.get("cases")
    pool_cases = pool.get("cases")
    if (
        expected.get("schema_version") != CALIBRATION_TRUTH_VERSION
        or expected.get("case_count") != 66
        or not isinstance(cases, Mapping)
        or len(cases) != 66
        or not isinstance(pool_cases, list)
        or {case["case_id"] for case in pool_cases} != set(cases)
        or len((mapping.get("cases") or [])) != 66
    ):
        raise JudgeV5CalibrationError("calibration truth coverage drifted")
    for truth in cases.values():
        witness_ids = set(truth["proposition"])
        if (
            set(truth["structured_fields"]) != witness_ids
            or set(truth["field_issues"]) != witness_ids
            or any(
                verdict not in {"supported", "unsupported"}
                for verdict in truth["proposition"].values()
            )
            or any(
                verdict not in {"correct", "incorrect"}
                for verdict in truth["structured_fields"].values()
            )
            or any(
                field not in CHECKLIST_FIELDS
                for fields in truth["field_issues"].values()
                for field in fields
            )
        ):
            raise JudgeV5CalibrationError("calibration pointwise truth drifted")
        paired = set()
        for pair in truth["pairs"]:
            if (
                len(pair["witness_ids"]) != 2
                or len(set(pair["witness_ids"])) != 2
                or any(field not in CHECKLIST_FIELDS for field in pair["mismatch_fields"])
            ):
                raise JudgeV5CalibrationError("calibration pair truth drifted")
            paired.update(pair["witness_ids"])
        if paired | set(truth["unpaired_witness_ids"]) != witness_ids:
            raise JudgeV5CalibrationError("calibration residual truth drifted")
    canary = expected.get("canary_case_ids")
    if not isinstance(canary, list) or len(canary) != 12 or len(set(canary)) != 12:
        raise JudgeV5CalibrationError("calibration canary coverage drifted")


def calibration_case_shards(pool: Mapping[str, Any]) -> list[list[str]]:
    case_ids = [str(case["case_id"]) for case in pool.get("cases") or []]
    if len(case_ids) != 66 or len(set(case_ids)) != 66:
        raise JudgeV5CalibrationError("calibration pool coverage drifted")
    shards = [
        case_ids[index : index + CALIBRATION_CASES_PER_SHARD]
        for index in range(0, len(case_ids), CALIBRATION_CASES_PER_SHARD)
    ]
    if len(shards) != 11 or any(len(shard) != 6 for shard in shards):
        raise JudgeV5CalibrationError("calibration shard layout drifted")
    return shards


def pointwise_input_subset(
    pointwise_input: Mapping[str, Any], case_ids: Sequence[str]
) -> dict[str, Any]:
    selected = set(case_ids)
    value = deepcopy(dict(pointwise_input))
    value["units"] = [
        deepcopy(unit)
        for unit in pointwise_input["units"]
        if unit["case_id"] in selected
    ]
    if {unit["case_id"] for unit in value["units"]} != selected:
        raise JudgeV5CalibrationError("pointwise shard coverage drifted")
    return value


def score_v5_calibration(
    *,
    pointwise_output: Mapping[str, Any],
    reconciled_alignment: Mapping[str, Any],
    expected: Mapping[str, Any],
    observable_disagreements: Mapping[str, Any],
) -> dict[str, Any]:
    expected_cases = expected.get("cases")
    if (
        expected.get("schema_version") != CALIBRATION_TRUTH_VERSION
        or not isinstance(expected_cases, Mapping)
        or len(expected_cases) != 66
    ):
        raise JudgeV5CalibrationError("calibration truth is incomplete")
    pointwise_rows = {str(row["witness_id"]): row for row in pointwise_output["units"]}
    expected_witnesses = {
        witness_id
        for truth in expected_cases.values()
        for witness_id in truth["proposition"]
    }
    if set(pointwise_rows) != expected_witnesses:
        raise JudgeV5CalibrationError("calibration pointwise output coverage drifted")
    support_tp = support_fn = support_tn = support_fp = 0
    structured_correct = structured_total = 0
    pointwise_field_tp = pointwise_field_fp = pointwise_field_fn = 0
    abstentions = 0
    support_by_shape: dict[str, Counter[str]] = defaultdict(Counter)
    for truth in expected_cases.values():
        for witness_id, expected_verdict in truth["proposition"].items():
            row = pointwise_rows[witness_id]
            observed = row["proposition_verdict"]
            if observed == "abstain":
                abstentions += 1
            if expected_verdict == "supported" and observed == "supported":
                support_tp += 1
                support_by_shape[truth["shape"]]["tp"] += 1
            elif expected_verdict == "supported":
                support_fn += 1
                support_by_shape[truth["shape"]]["fn"] += 1
            elif observed == "unsupported":
                support_tn += 1
                support_by_shape[truth["shape"]]["tn"] += 1
            else:
                support_fp += 1
                support_by_shape[truth["shape"]]["fp"] += 1
            structured_total += 1
            structured_correct += int(
                row["structured_field_verdict"]
                == truth["structured_fields"][witness_id]
            )
            if row["structured_field_verdict"] == "abstain":
                abstentions += 1
            wanted = set(truth["field_issues"][witness_id])
            observed_issues = set(row["field_issue_fields"])
            pointwise_field_tp += len(wanted & observed_issues)
            pointwise_field_fp += len(observed_issues - wanted)
            pointwise_field_fn += len(wanted - observed_issues)

    alignment_rows = {
        str(row["case_id"]): row for row in reconciled_alignment.get("cases") or []
    }
    if set(alignment_rows) != set(expected_cases):
        raise JudgeV5CalibrationError("calibration alignment coverage drifted")
    alignment_tp = alignment_fp = alignment_fn = 0
    relation_correct = relation_total = 0
    field_tp = field_fp = field_fn = 0
    equivalent_tp = equivalent_fn = equivalent_tn = equivalent_fp = 0
    partition_exact = unpaired_exact = 0
    for case_id, truth in expected_cases.items():
        row = alignment_rows[case_id]
        if row.get("status") == "abstain":
            abstentions += 1
        observed_pairs = {
            tuple(sorted(pair["witness_ids"])): pair
            for pair in row.get("alignment_pairs") or []
        }
        expected_pairs = {
            tuple(sorted(pair["witness_ids"])): pair for pair in truth["pairs"]
        }
        alignment_tp += len(set(observed_pairs) & set(expected_pairs))
        alignment_fp += len(set(observed_pairs) - set(expected_pairs))
        alignment_fn += len(set(expected_pairs) - set(observed_pairs))
        for pair_ids, expected_pair in expected_pairs.items():
            observed_pair = observed_pairs.get(pair_ids)
            observed_relation = (
                observed_pair.get("relation") if observed_pair is not None else None
            )
            if observed_relation == "abstain":
                abstentions += 1
            relation_total += 1
            relation_correct += int(observed_relation == expected_pair["relation"])
            expected_equivalent = expected_pair["relation"] == "equivalent"
            if expected_equivalent and observed_relation == "equivalent":
                equivalent_tp += 1
            elif expected_equivalent:
                equivalent_fn += 1
            elif observed_relation == "equivalent" or observed_relation in {
                None,
                "abstain",
            }:
                equivalent_fp += 1
            else:
                equivalent_tn += 1
            wanted = set(expected_pair["mismatch_fields"])
            observed_fields = set(
                observed_pair.get("mismatch_fields") if observed_pair is not None else []
            )
            field_tp += len(wanted & observed_fields)
            field_fp += len(observed_fields - wanted)
            field_fn += len(wanted - observed_fields)
        observed_partition = {
            tuple(sorted(group)) for group in row.get("equivalence_groups") or []
        }
        expected_partition = {
            tuple(sorted(group)) for group in truth["equivalence_groups"]
        }
        partition_exact += int(observed_partition == expected_partition)
        unpaired_exact += int(
            sorted(row.get("unpaired_witness_ids") or [])
            == truth["unpaired_witness_ids"]
        )

    support_sensitivity = _ratio(support_tp, support_tp + support_fn)
    support_specificity = _ratio(support_tn, support_tn + support_fp)
    order_denominator = len(expected["canary_case_ids"])
    order_bias = _ratio(
        int(observable_disagreements.get("disagreement_case_count") or 0),
        order_denominator,
    )
    metrics = {
        "case_count": len(expected_cases),
        "witness_count": structured_total,
        "support_sensitivity": support_sensitivity,
        "support_specificity": support_specificity,
        "structured_field_accuracy": _ratio(structured_correct, structured_total),
        "pointwise_field_issue_f1": _f1(
            pointwise_field_tp, pointwise_field_fp, pointwise_field_fn
        ),
        "alignment_f1": _f1(alignment_tp, alignment_fp, alignment_fn),
        "equivalent_sensitivity": _ratio(equivalent_tp, equivalent_tp + equivalent_fn),
        "equivalent_specificity": _ratio(equivalent_tn, equivalent_tn + equivalent_fp),
        "field_diagnostic_f1": _f1(field_tp, field_fp, field_fn),
        "relation_accuracy": _ratio(relation_correct, relation_total),
        "equivalence_partition_exact_case_rate": _ratio(
            partition_exact, len(expected_cases)
        ),
        "unpaired_exact_case_rate": _ratio(unpaired_exact, len(expected_cases)),
        "abstention_count": abstentions,
        "abstention_rate": _ratio(
            abstentions, structured_total + relation_total + len(expected_cases)
        ),
        "order_bias": order_bias,
        "canary_case_count": order_denominator,
    }
    checks = {
        "minimum_cases": metrics["case_count"] >= CALIBRATION_GATES["minimum_cases"],
        "support_sensitivity": support_sensitivity
        >= CALIBRATION_GATES["support_sensitivity_min"],
        "support_specificity": support_specificity
        >= CALIBRATION_GATES["support_specificity_min"],
        "structured_field_accuracy": metrics["structured_field_accuracy"]
        >= CALIBRATION_GATES["structured_field_accuracy_min"],
        "alignment_f1": metrics["alignment_f1"]
        >= CALIBRATION_GATES["alignment_f1_min"],
        "equivalent_sensitivity": metrics["equivalent_sensitivity"]
        >= CALIBRATION_GATES["equivalent_sensitivity_min"],
        "equivalent_specificity": metrics["equivalent_specificity"]
        >= CALIBRATION_GATES["equivalent_specificity_min"],
        "field_diagnostic_f1": metrics["field_diagnostic_f1"]
        >= CALIBRATION_GATES["field_diagnostic_f1_min"],
        "relation_accuracy": metrics["relation_accuracy"]
        >= CALIBRATION_GATES["relation_accuracy_min"],
        "equivalence_partition_exact_case_rate": metrics[
            "equivalence_partition_exact_case_rate"
        ]
        >= CALIBRATION_GATES["equivalence_partition_exact_case_rate_min"],
        "unpaired_exact_case_rate": metrics["unpaired_exact_case_rate"]
        >= CALIBRATION_GATES["unpaired_exact_case_rate_min"],
        "abstention_rate": metrics["abstention_rate"]
        <= CALIBRATION_GATES["abstention_rate_max"],
        "order_bias": order_bias <= CALIBRATION_GATES["order_bias_max"],
        "canary_case_count": order_denominator
        == CALIBRATION_GATES["canary_case_count"],
    }
    return {
        "schema_version": CALIBRATION_SCORE_VERSION,
        "passed": all(checks.values()),
        "gates": CALIBRATION_GATES,
        "metrics": metrics,
        "checks": checks,
        "support_by_shape": {
            shape: dict(sorted(counts.items()))
            for shape, counts in sorted(support_by_shape.items())
        },
        "legacy_joint_support_labels_used": False,
        "proposition_and_structured_field_scores_separate": True,
    }
