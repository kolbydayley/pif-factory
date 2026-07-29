from __future__ import annotations

"""Zero-token recovery and error taxonomy for the immutable v106 attempt."""

import hashlib
import json
import math
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

from .app_server_judge_v5_calibration import CALIBRATION_GATES
from .app_server_judge_v5_calibration_v26_diagnostic import _load_json, _write_immutable
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _record_matches,
)
from .app_server_judge_v5_calibration_v106_full_development import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V106_ROOT,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _validate_usage
from .util import now_iso


V107_AUDIT_VERSION = "pif_app_server_judge_v5_4_v107_recovery_audit_v1"
V107_TAXONOMY_VERSION = "pif_app_server_judge_v5_4_v107_error_taxonomy_v1"
V107_RECEIPT_VERSION = "pif_app_server_judge_v5_4_v107_recovery_receipt_v1"
V107_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v107_terminal_v1"
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V106_ROOT.parent / "judge-calibration-v5_4-v107-recovery-receipt"
).resolve()


class JudgeV5CalibrationV107Error(RuntimeError):
    """The immutable v106 recovery evidence is incomplete or drifted."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_v106(v106_root: Path = DEFAULT_V106_ROOT) -> dict[str, Any]:
    root = v106_root.expanduser().resolve()
    paths = {
        "spec": root / "full-calibration-spec.json",
        "terminal": root / "terminal.json",
        "failure": root / "failure.json",
        "score": root / "calibration-score.json",
        "truth": root / "calibration-truth.private.json",
        "pointwise": root / "pointwise-output-full.private.json",
        "base": root / "base-alignment-full.private.json",
        "canary": root / "canary-alignment-full.private.json",
        "disagreements": root / "observable-disagreements.private.json",
        "reconciled": root / "reconciled-alignment.private.json",
    }
    values = {name: _load_json(path, f"v106 {name}") for name, path in paths.items()}
    terminal, failure, score, spec = (
        values["terminal"],
        values["failure"],
        values["score"],
        values["spec"],
    )
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage_status") != "complete"
        or terminal.get("calibration_passed") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or failure.get("failed_turn_name") != "disagreement_adjudication"
        or failure.get("error_class") != "KeyError"
        or failure.get("accounting_complete") is not True
        or failure.get("unknown_usage_turn_count") != 0
        or score.get("passed") is not False
        or spec.get("retry_count_per_turn") != 0
        or spec.get("selection_authorized") is not False
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5CalibrationV107Error("v106 terminal contract drifted")
    sidecars = sorted(root.glob("turns/*/sidecar.json"))
    outputs = sorted(root.glob("turns/*/output.private.json"))
    capacities = sorted(root.glob("turns/*/capacity.json"))
    if len(sidecars) != 25 or len(outputs) != 25 or len(capacities) != 25:
        raise JudgeV5CalibrationV107Error("v106 turn coverage is incomplete")
    usage = {field: 0 for field in USAGE_FIELDS}
    for path in sidecars:
        value = _load_json(path, "v106 sidecar")
        measured = _validate_usage(value)
        if value.get("status") != "completed" or value.get("usage_complete") is not True:
            raise JudgeV5CalibrationV107Error("v106 sidecar is not terminal and measured")
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    if usage != terminal.get("usage") or usage != failure.get("usage"):
        raise JudgeV5CalibrationV107Error("v106 usage aggregate drifted")
    if score.get("checks") is None or score.get("metrics") is None:
        raise JudgeV5CalibrationV107Error("v106 score is incomplete")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "sidecar_records": [_record(path) for path in sidecars],
        "output_records": [_record(path) for path in outputs],
        "capacity_records": [_record(path) for path in capacities],
        "usage": usage,
    }


def _required_successes(total: int, minimum: float) -> int:
    return math.ceil(total * minimum)


def _pair_f1_replacements(tp: int, fp: int, fn: int, minimum: float) -> int:
    for replacements in range(max(fp, fn) + 1):
        tp2 = tp + replacements
        fp2 = max(0, fp - replacements)
        fn2 = max(0, fn - replacements)
        denominator = 2 * tp2 + fp2 + fn2
        if denominator and (2 * tp2 / denominator) >= minimum:
            return replacements
    return max(fp, fn) + 1


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        prior = _load_json(path, f"existing {path.name}")
        value[key] = prior.get(key)
    _write_immutable(path, value)


def build_error_taxonomy(v106: Mapping[str, Any]) -> dict[str, Any]:
    truth = v106["values"]["truth"]["cases"]
    pointwise = {
        str(row["witness_id"]): row for row in v106["values"]["pointwise"]["units"]
    }
    alignment = {
        str(row["case_id"]): row for row in v106["values"]["reconciled"]["cases"]
    }
    support = Counter()
    structured = Counter()
    field_errors: Counter[tuple[str, str]] = Counter()
    structured_by_shape = Counter()
    pair_errors_by_shape: Counter[tuple[str, str]] = Counter()
    relation = Counter()
    relation_errors_by_shape = Counter()
    unpaired_errors_by_shape = Counter()
    partition_errors_by_shape = Counter()
    affected_cases: dict[str, set[str]] = defaultdict(set)
    alignment_tp = alignment_fp = alignment_fn = 0
    relation_correct = relation_total = 0
    equivalent_tp = equivalent_fn = equivalent_tn = equivalent_fp = 0
    unpaired_exact = partition_exact = 0
    for case_id, expected in truth.items():
        shape = expected["shape"]
        for witness_id, expected_support in expected["proposition"].items():
            row = pointwise[witness_id]
            observed_support = row["proposition_verdict"]
            support[(expected_support, observed_support)] += 1
            expected_structured = expected["structured_fields"][witness_id]
            observed_structured = row["structured_field_verdict"]
            structured[(expected_structured, observed_structured)] += 1
            if expected_structured != observed_structured:
                structured_by_shape[shape] += 1
                affected_cases[case_id].add("structured_field_accuracy")
            wanted = set(expected["field_issues"][witness_id])
            observed = set(row["field_issue_fields"])
            for field in wanted - observed:
                field_errors[(field, "missed")] += 1
            for field in observed - wanted:
                field_errors[(field, "extra")] += 1
        row = alignment[case_id]
        observed_pairs = {
            tuple(sorted(pair["witness_ids"])): pair
            for pair in row.get("alignment_pairs") or []
        }
        expected_pairs = {
            tuple(sorted(pair["witness_ids"])): pair for pair in expected["pairs"]
        }
        shared, extra, missed = (
            set(observed_pairs) & set(expected_pairs),
            set(observed_pairs) - set(expected_pairs),
            set(expected_pairs) - set(observed_pairs),
        )
        alignment_tp += len(shared)
        alignment_fp += len(extra)
        alignment_fn += len(missed)
        if extra:
            pair_errors_by_shape[(shape, "extra_pair")] += len(extra)
            affected_cases[case_id].add("alignment_f1")
        if missed:
            pair_errors_by_shape[(shape, "missed_pair")] += len(missed)
            affected_cases[case_id].add("alignment_f1")
        for pair_ids, expected_pair in expected_pairs.items():
            observed_relation = (observed_pairs.get(pair_ids) or {}).get("relation")
            wanted_relation = expected_pair["relation"]
            relation[(wanted_relation, str(observed_relation))] += 1
            relation_total += 1
            relation_correct += int(observed_relation == wanted_relation)
            if observed_relation != wanted_relation:
                relation_errors_by_shape[shape] += 1
                affected_cases[case_id].add("relation_accuracy")
            expected_equivalent = wanted_relation == "equivalent"
            if expected_equivalent and observed_relation == "equivalent":
                equivalent_tp += 1
            elif expected_equivalent:
                equivalent_fn += 1
            elif observed_relation == "equivalent" or observed_relation in {None, "abstain"}:
                equivalent_fp += 1
            else:
                equivalent_tn += 1
        observed_unpaired = sorted(row.get("unpaired_witness_ids") or [])
        if observed_unpaired == expected["unpaired_witness_ids"]:
            unpaired_exact += 1
        else:
            unpaired_errors_by_shape[shape] += 1
            affected_cases[case_id].add("unpaired_exact_case_rate")
        observed_partition = {
            tuple(sorted(group)) for group in row.get("equivalence_groups") or []
        }
        expected_partition = {
            tuple(sorted(group)) for group in expected["equivalence_groups"]
        }
        if observed_partition == expected_partition:
            partition_exact += 1
        else:
            partition_errors_by_shape[shape] += 1
            affected_cases[case_id].add("equivalence_partition_exact_case_rate")

    support_tp = support[("supported", "supported")]
    support_fn = sum(
        count for (wanted, observed), count in support.items()
        if wanted == "supported" and observed != "supported"
    )
    support_tn = support[("unsupported", "unsupported")]
    support_fp = sum(
        count for (wanted, observed), count in support.items()
        if wanted == "unsupported" and observed != "unsupported"
    )
    structured_correct = sum(
        count for (wanted, observed), count in structured.items() if wanted == observed
    )
    witness_count = len(pointwise)
    case_count = len(truth)
    raw_disagreements = int(
        v106["values"]["disagreements"].get("disagreement_case_count") or 0
    )
    false_checks = sorted(
        key for key, value in v106["values"]["score"]["checks"].items() if value is False
    )
    return {
        "schema_version": V107_TAXONOMY_VERSION,
        "created_at": now_iso(),
        "sanitized": True,
        "case_count": case_count,
        "witness_count": witness_count,
        "failed_gates": false_checks,
        "counts": {
            "support": {
                "true_positive": support_tp,
                "false_negative": support_fn,
                "true_negative": support_tn,
                "false_positive": support_fp,
            },
            "structured_field": {
                "correct": structured_correct,
                "incorrect": witness_count - structured_correct,
                "expected_correct_observed_incorrect": structured[("correct", "incorrect")],
                "expected_incorrect_observed_correct": structured[("incorrect", "correct")],
            },
            "alignment_pair": {
                "true_positive": alignment_tp,
                "false_positive": alignment_fp,
                "false_negative": alignment_fn,
            },
            "relation": {"correct": relation_correct, "total": relation_total},
            "equivalent_relation": {
                "true_positive": equivalent_tp,
                "false_negative": equivalent_fn,
                "true_negative": equivalent_tn,
                "false_positive": equivalent_fp,
            },
            "unpaired": {"exact_cases": unpaired_exact, "total_cases": case_count},
            "partition": {"exact_cases": partition_exact, "total_cases": case_count},
            "order": {
                "raw_disagreement_cases": raw_disagreements,
                "canary_cases": 12,
            },
        },
        "field_errors": [
            {"field": field, "direction": direction, "count": count}
            for (field, direction), count in sorted(field_errors.items())
        ],
        "errors_by_shape": {
            "structured": dict(sorted(structured_by_shape.items())),
            "pair": [
                {"shape": shape, "direction": direction, "count": count}
                for (shape, direction), count in sorted(pair_errors_by_shape.items())
            ],
            "relation": dict(sorted(relation_errors_by_shape.items())),
            "unpaired": dict(sorted(unpaired_errors_by_shape.items())),
            "partition": dict(sorted(partition_errors_by_shape.items())),
        },
        "relation_confusion": [
            {"expected": expected, "observed": observed, "count": count}
            for (expected, observed), count in sorted(relation.items())
        ],
        "affected_cases": [
            {"case_id": case_id, "failed_surfaces": sorted(surfaces)}
            for case_id, surfaces in sorted(affected_cases.items())
        ],
        "minimum_corrections": {
            "support_specificity": max(
                0,
                _required_successes(
                    support_tn + support_fp,
                    CALIBRATION_GATES["support_specificity_min"],
                )
                - support_tn,
            ),
            "structured_field_accuracy": max(
                0,
                _required_successes(
                    witness_count,
                    CALIBRATION_GATES["structured_field_accuracy_min"],
                )
                - structured_correct,
            ),
            "alignment_f1_pair_replacements": _pair_f1_replacements(
                alignment_tp,
                alignment_fp,
                alignment_fn,
                CALIBRATION_GATES["alignment_f1_min"],
            ),
            "equivalent_sensitivity": max(
                0,
                _required_successes(
                    equivalent_tp + equivalent_fn,
                    CALIBRATION_GATES["equivalent_sensitivity_min"],
                )
                - equivalent_tp,
            ),
            "relation_accuracy": max(
                0,
                _required_successes(
                    relation_total,
                    CALIBRATION_GATES["relation_accuracy_min"],
                )
                - relation_correct,
            ),
            "unpaired_exact_case_rate": max(
                0,
                _required_successes(
                    case_count,
                    CALIBRATION_GATES["unpaired_exact_case_rate_min"],
                )
                - unpaired_exact,
            ),
            "order_bias": max(0, raw_disagreements - math.floor(12 * 0.05)),
        },
        "likely_causal_classes": [
            {
                "class": "schema_or_rubric_projection_defect",
                "evidence": "pointwise_field_issue_overproduction_and_26_false_incorrect_verdicts",
                "affected_gates": ["structured_field_accuracy"],
            },
            {
                "class": "judge_instruction_ambiguity",
                "evidence": "unsupported_residual_assignment_dominates_pair_and_unpaired_errors",
                "affected_gates": [
                    "alignment_f1",
                    "equivalent_sensitivity",
                    "relation_accuracy",
                    "unpaired_exact_case_rate",
                ],
            },
            {
                "class": "judge_instruction_ambiguity",
                "evidence": "merge_split_partial_cases_projected_as_non_equivalent",
                "affected_gates": ["relation_accuracy"],
            },
            {
                "class": "genuine_semantic_miss",
                "evidence": "one_support_false_positive_and_four_support_false_negatives",
                "affected_gates": ["support_specificity"],
            },
            {
                "class": "comparison_stability_miss",
                "evidence": "one_of_twelve_balanced_canary_cases_disagreed_before_adjudication",
                "affected_gates": ["order_bias"],
            },
        ],
        "private_content_exposed": False,
    }


def freeze_v107(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, v106_root: Path = DEFAULT_V106_ROOT
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    v106 = _validate_v106(v106_root)
    taxonomy = build_error_taxonomy(v106)
    taxonomy_path = root / "sanitized-error-taxonomy.json"
    _write_stable_time(taxonomy_path, taxonomy, "created_at")
    failed = taxonomy["failed_gates"]
    audit = {
        "schema_version": V107_AUDIT_VERSION,
        "created_at": now_iso(),
        "v106_terminal_bug": "terminal_builder_requested_missing_score_key_failed_checks",
        "v106_model_turns_completed": 25,
        "v106_model_usage_complete": True,
        "v106_actual_quality_score_completed": True,
        "v106_actual_quality_passed": False,
        "v106_failed_quality_gates": failed,
        "v106_predecessor": v106["records"],
        "v106_sidecars": v106["sidecar_records"],
        "v106_outputs": v106["output_records"],
        "v106_capacity_checkpoints": v106["capacity_records"],
        "v106_usage": v106["usage"],
        "new_semantic_turn_count": 0,
        "new_usage": {field: 0 for field in USAGE_FIELDS},
        "production_mutated": False,
    }
    audit_path = root / "v106-recovery-audit.json"
    _write_stable_time(audit_path, audit, "created_at")
    receipt = {
        "schema_version": V107_RECEIPT_VERSION,
        "created_at": now_iso(),
        "v106_terminal": _record(v106["paths"]["terminal"]),
        "v106_score": _record(v106["paths"]["score"]),
        "recovery_audit": _record(audit_path),
        "error_taxonomy": _record(taxonomy_path),
        "classification": "development_quality_gate_not_passed_after_completed_accounted_attempt",
        "corrected_terminal_reason": "inactive_incomplete_recovery_required",
        "fresh_small_diagnostic_required": True,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    receipt_path = root / "recovery-receipt.json"
    _write_stable_time(receipt_path, receipt, "created_at")
    terminal = {
        "schema_version": V107_TERMINAL_VERSION,
        "state": "inactive",
        "terminal_at": now_iso(),
        "terminal_reason": "inactive_incomplete_recovery_required",
        "development_terminal_reason": "v106_quality_gate_not_passed_after_terminal_builder_recovery",
        "overall_evaluation_complete": False,
        "calibration_passed": False,
        "fresh_small_diagnostic_required": True,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_attempt_started": False,
        "semantic_retry_count": 0,
        "usage_status": "complete",
        "accounting_complete": True,
        "usage": {field: 0 for field in USAGE_FIELDS},
        "predecessor_v106_usage": v106["usage"],
        "failed_quality_gates": failed,
        "recovery_receipt": _record(receipt_path),
        "error_taxonomy": _record(taxonomy_path),
    }
    terminal_path = root / "terminal.json"
    _write_stable_time(terminal_path, terminal, "terminal_at")
    return {"terminal": terminal, "taxonomy": taxonomy, "root": root}


def main() -> int:
    result = freeze_v107()
    print(
        json.dumps(
            {
                "state": result["terminal"]["state"],
                "terminal_reason": result["terminal"]["terminal_reason"],
                "failed_quality_gates": result["terminal"]["failed_quality_gates"],
                "new_semantic_turn_count": 0,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
