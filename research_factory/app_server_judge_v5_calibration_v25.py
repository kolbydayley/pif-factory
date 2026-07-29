from __future__ import annotations

"""Presemantic v25 recovery plan after the scoreable v24 quality-gate failure."""

import argparse
import json
import math
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .app_server_judge_v5 import _f1
from .app_server_judge_v5_calibration import CALIBRATION_GATES
from .app_server_judge_v5_calibration_v24 import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V24_ROOT,
    DEFAULT_SOURCE_ROOT as DEFAULT_V23_ROOT,
)
from .app_server_judge_v5_diagnostic import (
    _record,
    _sha256_file,
    _write_immutable_json,
)
from .util import now_iso


V25_TAXONOMY_VERSION = "pif_app_server_judge_v5_4_v25_error_taxonomy_v1"
V25_CLEARANCE_VERSION = "pif_app_server_judge_v5_4_v25_clearance_analysis_v1"
V25_PROTOCOL_AUDIT_VERSION = "pif_app_server_judge_v5_4_v25_protocol_audit_v1"
V25_DIAGNOSTIC_SPEC_VERSION = "pif_app_server_judge_v5_4_v25_diagnostic_spec_v1"
V25_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v25_presemantic_terminal_v1"

DEFAULT_OUTPUT_ROOT = Path(
    "work/app-server-development-v2/unattended-pipeline-v5/"
    "judge-calibration-v5_4-v25-presemantic-recovery"
).resolve()

V24_FAILED_GATES = (
    "alignment_f1",
    "structured_field_accuracy",
    "support_specificity",
    "unpaired_exact_case_rate",
)

DIAGNOSTIC_CASE_IDS = (
    "jcase_4d5dff9aae0f62a79e8e1d50",
    "jcase_ddf03786960ff7583b95eb84",
    "jcase_f2be100994c934639476185d",
    "jcase_1153895f992c3ca36fecd004",
    "jcase_f5aac851a010eb910fac98c5",
    "jcase_d0510d8215b5a9cf8d731430",
    "jcase_c585111cba0f343d4047819f",
    "jcase_43f1212ec1d286cde0eaf89d",
    "jcase_a2a565c42526b84767bf4b1d",
    "jcase_486d2d05929d68d8a8a34262",
    "jcase_29baa535cf2425d79ef2e688",
    "jcase_b498a98ede5a7e127f358a5e",
    "jcase_27edbb38a94ae1eb60da4295",
    "jcase_065492a588e0906b0742a779",
    "jcase_1124b8b6032c8ed35dada2f0",
    "jcase_35112ff09a798d991065fe76",
    "jcase_1abfbea5d7812c82191c41c8",
    "jcase_616ca00172aad1a9ac7836db",
)


class JudgeV5CalibrationV25Error(RuntimeError):
    """The v25 presemantic recovery inputs or outputs are unsafe."""


def _load_json(path: Path, purpose: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise JudgeV5CalibrationV25Error(f"{purpose} is missing or invalid") from exc
    if not isinstance(value, dict):
        raise JudgeV5CalibrationV25Error(f"{purpose} is not an object")
    return value


def _verify_record(record: Mapping[str, Any], purpose: str) -> Path:
    path = Path(str(record.get("path") or "")).expanduser().resolve()
    if (
        not path.is_file()
        or record.get("sha256") != _sha256_file(path)
        or record.get("size_bytes") != path.stat().st_size
    ):
        raise JudgeV5CalibrationV25Error(f"{purpose} drifted")
    return path


def _counter(counter: Counter[Any]) -> dict[str, int]:
    return {str(key): int(value) for key, value in sorted(counter.items(), key=lambda x: str(x[0]))}


def _ceil_for_gate(metric_name: str, denominator: int) -> int:
    threshold = CALIBRATION_GATES[f"{metric_name}_min"]
    return math.ceil((threshold * denominator) - 1e-12)


def _classification_for_pointwise(
    *, category: str, expected: str, observed: str, observed_fields: Sequence[str]
) -> str:
    if category == "support" and expected == "unsupported" and observed == "supported":
        return "genuine_semantic_miss"
    if category == "support":
        return "judge_instruction_ambiguity"
    if observed_fields:
        return "judge_instruction_ambiguity"
    return "schema_or_normalization_defect"


def _classification_for_alignment(shape: str, direction: str) -> str:
    if "one_sided" in shape or direction == "unpaired_case_mismatch":
        return "judge_instruction_ambiguity"
    if direction in {"alignment_relation_error", "alignment_field_false_positive"}:
        return "judge_instruction_ambiguity"
    return "genuine_semantic_miss"


def _add_row(
    rows: list[dict[str, Any]],
    *,
    failed_gate: str,
    case_id: str,
    shape: str,
    direction: str,
    likely_causal_class: str,
    field: Optional[str] = None,
    witness_id: Optional[str] = None,
    witness_ids: Optional[Sequence[str]] = None,
    expected: Any = None,
    observed: Any = None,
    paired_status: str,
) -> None:
    rows.append(
        {
            "row_id": "v25_err_%04d" % (len(rows) + 1),
            "failed_gate": failed_gate,
            "case_id": case_id,
            "shape": shape,
            "direction": direction,
            "field": field,
            "witness_id": witness_id,
            "witness_ids": list(witness_ids or []),
            "paired_status": paired_status,
            "expected": expected,
            "observed": observed,
            "likely_causal_class": likely_causal_class,
            "causal_class_requires_llm_confirmation": True,
        }
    )


def _build_taxonomy(
    *,
    truth: Mapping[str, Any],
    pointwise_output: Mapping[str, Any],
    reconciled_alignment: Mapping[str, Any],
    score: Mapping[str, Any],
    source_records: Mapping[str, Any],
) -> dict[str, Any]:
    cases = truth.get("cases")
    pointwise_units = pointwise_output.get("units")
    alignment_cases = reconciled_alignment.get("cases")
    if (
        not isinstance(cases, Mapping)
        or not isinstance(pointwise_units, list)
        or not isinstance(alignment_cases, list)
    ):
        raise JudgeV5CalibrationV25Error("v25 taxonomy inputs are malformed")

    pointwise_rows = {str(row["witness_id"]): row for row in pointwise_units}
    alignment_rows = {str(row["case_id"]): row for row in alignment_cases}
    rows: list[dict[str, Any]] = []
    support_confusion: Counter[tuple[str, str]] = Counter()
    structured_confusion: Counter[tuple[str, str]] = Counter()
    alignment_counts = {"tp": 0, "fp": 0, "fn": 0}
    unpaired_exact = 0

    for case_id, raw_truth in cases.items():
        case = raw_truth
        shape = str(case["shape"])
        for witness_id, expected_support in case["proposition"].items():
            observed_row = pointwise_rows[str(witness_id)]
            observed_support = str(observed_row["proposition_verdict"])
            support_confusion[(str(expected_support), observed_support)] += 1
            if observed_support != expected_support:
                failed_gate = (
                    "support_specificity"
                    if expected_support == "unsupported"
                    else "support_sensitivity"
                )
                _add_row(
                    rows,
                    failed_gate=failed_gate,
                    case_id=str(case_id),
                    shape=shape,
                    direction=(
                        "support_false_positive"
                        if expected_support == "unsupported"
                        else "support_false_negative"
                    ),
                    field="proposition_support",
                    witness_id=str(witness_id),
                    paired_status="single_witness",
                    expected=expected_support,
                    observed=observed_support,
                    likely_causal_class=_classification_for_pointwise(
                        category="support",
                        expected=str(expected_support),
                        observed=observed_support,
                        observed_fields=observed_row.get("field_issue_fields") or [],
                    ),
                )
            expected_structured = case["structured_fields"][witness_id]
            observed_structured = str(observed_row["structured_field_verdict"])
            structured_confusion[(str(expected_structured), observed_structured)] += 1
            if observed_structured != expected_structured:
                observed_fields = sorted(observed_row.get("field_issue_fields") or [])
                for field in observed_fields or ["structured_event"]:
                    _add_row(
                        rows,
                        failed_gate="structured_field_accuracy",
                        case_id=str(case_id),
                        shape=shape,
                        direction=(
                            "structured_false_positive"
                            if expected_structured == "correct"
                            else "structured_false_negative"
                        ),
                        field=field,
                        witness_id=str(witness_id),
                        paired_status="single_witness",
                        expected={
                            "structured_field_verdict": expected_structured,
                            "field_issue_fields": list(case["field_issues"][witness_id]),
                        },
                        observed={
                            "structured_field_verdict": observed_structured,
                            "field_issue_fields": observed_fields,
                        },
                        likely_causal_class=_classification_for_pointwise(
                            category="structured",
                            expected=str(expected_structured),
                            observed=observed_structured,
                            observed_fields=observed_fields,
                        ),
                    )

    for case_id, raw_truth in cases.items():
        case = raw_truth
        shape = str(case["shape"])
        row = alignment_rows[str(case_id)]
        observed_pairs = {
            tuple(sorted(pair["witness_ids"])): pair
            for pair in row.get("alignment_pairs") or []
        }
        expected_pairs = {
            tuple(sorted(pair["witness_ids"])): pair for pair in case["pairs"]
        }
        alignment_counts["tp"] += len(set(observed_pairs) & set(expected_pairs))
        alignment_counts["fp"] += len(set(observed_pairs) - set(expected_pairs))
        alignment_counts["fn"] += len(set(expected_pairs) - set(observed_pairs))
        for pair_ids in sorted(set(observed_pairs) - set(expected_pairs)):
            observed_pair = observed_pairs[pair_ids]
            _add_row(
                rows,
                failed_gate="alignment_f1",
                case_id=str(case_id),
                shape=shape,
                direction="alignment_pair_false_positive",
                witness_ids=pair_ids,
                paired_status="paired_false_positive",
                expected=None,
                observed={
                    "relation": observed_pair.get("relation"),
                    "mismatch_fields": sorted(observed_pair.get("mismatch_fields") or []),
                },
                likely_causal_class=_classification_for_alignment(
                    shape, "alignment_pair_false_positive"
                ),
            )
        for pair_ids in sorted(set(expected_pairs) - set(observed_pairs)):
            expected_pair = expected_pairs[pair_ids]
            _add_row(
                rows,
                failed_gate="alignment_f1",
                case_id=str(case_id),
                shape=shape,
                direction="alignment_pair_false_negative",
                witness_ids=pair_ids,
                paired_status="paired_false_negative",
                expected={
                    "relation": expected_pair.get("relation"),
                    "mismatch_fields": sorted(expected_pair.get("mismatch_fields") or []),
                },
                observed=None,
                likely_causal_class=_classification_for_alignment(
                    shape, "alignment_pair_false_negative"
                ),
            )
        for pair_ids in sorted(set(observed_pairs) & set(expected_pairs)):
            observed_pair = observed_pairs[pair_ids]
            expected_pair = expected_pairs[pair_ids]
            if observed_pair.get("relation") != expected_pair.get("relation"):
                _add_row(
                    rows,
                    failed_gate="alignment_relation_accuracy",
                    case_id=str(case_id),
                    shape=shape,
                    direction="alignment_relation_error",
                    witness_ids=pair_ids,
                    paired_status="paired_expected",
                    expected=expected_pair.get("relation"),
                    observed=observed_pair.get("relation"),
                    likely_causal_class=_classification_for_alignment(
                        shape, "alignment_relation_error"
                    ),
                )
            expected_fields = set(expected_pair.get("mismatch_fields") or [])
            observed_fields = set(observed_pair.get("mismatch_fields") or [])
            for field in sorted(observed_fields - expected_fields):
                _add_row(
                    rows,
                    failed_gate="alignment_field_diagnostic",
                    case_id=str(case_id),
                    shape=shape,
                    direction="alignment_field_false_positive",
                    field=field,
                    witness_ids=pair_ids,
                    paired_status="paired_expected",
                    expected=None,
                    observed=field,
                    likely_causal_class=_classification_for_alignment(
                        shape, "alignment_field_false_positive"
                    ),
                )
            for field in sorted(expected_fields - observed_fields):
                _add_row(
                    rows,
                    failed_gate="alignment_field_diagnostic",
                    case_id=str(case_id),
                    shape=shape,
                    direction="alignment_field_false_negative",
                    field=field,
                    witness_ids=pair_ids,
                    paired_status="paired_expected",
                    expected=field,
                    observed=None,
                    likely_causal_class=_classification_for_alignment(
                        shape, "alignment_field_false_negative"
                    ),
                )
        expected_unpaired = sorted(case["unpaired_witness_ids"])
        observed_unpaired = sorted(row.get("unpaired_witness_ids") or [])
        if observed_unpaired == expected_unpaired:
            unpaired_exact += 1
        else:
            _add_row(
                rows,
                failed_gate="unpaired_exact_case_rate",
                case_id=str(case_id),
                shape=shape,
                direction="unpaired_case_mismatch",
                paired_status="unpaired_partition",
                expected={"unpaired_count": len(expected_unpaired)},
                observed={"unpaired_count": len(observed_unpaired)},
                likely_causal_class=_classification_for_alignment(
                    shape, "unpaired_case_mismatch"
                ),
            )

    by_gate = Counter(row["failed_gate"] for row in rows)
    by_direction = Counter(row["direction"] for row in rows)
    by_field = Counter(row["field"] for row in rows if row.get("field"))
    by_shape = Counter(row["shape"] for row in rows)
    by_status = Counter(row["paired_status"] for row in rows)
    by_causal = Counter(row["likely_causal_class"] for row in rows)
    return {
        "schema_version": V25_TAXONOMY_VERSION,
        "created_at": now_iso(),
        "privacy": {
            "sanitized": True,
            "contains_transcript_text": False,
            "contains_prompt_text": False,
            "contains_model_rationale_text": False,
            "identifier_policy": "opaque_case_and_witness_ids_only",
        },
        "source_records": dict(source_records),
        "failed_quality_gates": list(V24_FAILED_GATES),
        "v24_metrics": score["metrics"],
        "summary": {
            "case_count": len(cases),
            "witness_count": len(pointwise_units),
            "error_row_count": len(rows),
            "by_gate": _counter(by_gate),
            "by_direction": _counter(by_direction),
            "by_field": _counter(by_field),
            "by_shape": _counter(by_shape),
            "by_paired_status": _counter(by_status),
            "by_likely_causal_class": _counter(by_causal),
            "support_confusion": _counter(support_confusion),
            "structured_confusion": _counter(structured_confusion),
            "alignment_pair_counts": alignment_counts,
            "unpaired_exact_cases": unpaired_exact,
        },
        "error_rows": rows,
    }


def _minimal_f1_operations(tp: int, fp: int, fn: int, threshold: float) -> dict[str, Any]:
    def passes(value: int, false_pos: int, false_neg: int) -> bool:
        return _f1(value, false_pos, false_neg) >= threshold

    result: dict[str, Any] = {}
    for label, transform, limit in (
        ("convert_false_pair_to_missing_expected_pair", lambda n: (tp + n, fp - n, fn - n), min(fp, fn)),
        ("add_missing_expected_pair_only", lambda n: (tp + n, fp, fn - n), fn),
        ("remove_false_pair_only", lambda n: (tp, fp - n, fn), fp),
    ):
        value = None
        for count in range(0, limit + 1):
            next_tp, next_fp, next_fn = transform(count)
            if passes(next_tp, next_fp, next_fn):
                value = {
                    "minimum_operations": count,
                    "result_counts": {"tp": next_tp, "fp": next_fp, "fn": next_fn},
                    "result_f1": _f1(next_tp, next_fp, next_fn),
                }
                break
        result[label] = value
    return result


def _build_clearance_analysis(taxonomy: Mapping[str, Any]) -> dict[str, Any]:
    summary = taxonomy["summary"]
    support_confusion = {
        tuple(key.strip("()").replace("'", "").split(", ")): value
        for key, value in summary["support_confusion"].items()
    }
    structured_confusion = {
        tuple(key.strip("()").replace("'", "").split(", ")): value
        for key, value in summary["structured_confusion"].items()
    }
    support_tn = support_confusion.get(("unsupported", "unsupported"), 0)
    support_fp = support_confusion.get(("unsupported", "supported"), 0)
    structured_correct = sum(
        count for (expected, observed), count in structured_confusion.items() if expected == observed
    )
    witness_count = int(summary["witness_count"])
    alignment = summary["alignment_pair_counts"]
    unpaired_exact = int(summary["unpaired_exact_cases"])
    case_count = int(summary["case_count"])
    structured_required = _ceil_for_gate("structured_field_accuracy", witness_count)
    support_specificity_required = _ceil_for_gate(
        "support_specificity", support_tn + support_fp
    )
    unpaired_required = _ceil_for_gate("unpaired_exact_case_rate", case_count)
    alignment_threshold = CALIBRATION_GATES["alignment_f1_min"]
    alignment_ops = _minimal_f1_operations(
        alignment["tp"], alignment["fp"], alignment["fn"], alignment_threshold
    )
    return {
        "schema_version": V25_CLEARANCE_VERSION,
        "created_at": now_iso(),
        "failed_gate_clearance": {
            "structured_field_accuracy": {
                "current_correct": structured_correct,
                "denominator": witness_count,
                "required_correct": structured_required,
                "minimum_corrected_witness_decisions": max(
                    0, structured_required - structured_correct
                ),
                "dominant_error_kind": "structured_false_positive",
            },
            "support_specificity": {
                "current_true_negative": support_tn,
                "false_positive_count": support_fp,
                "denominator": support_tn + support_fp,
                "required_true_negative": support_specificity_required,
                "minimum_corrected_support_decisions": max(
                    0, support_specificity_required - support_tn
                ),
            },
            "alignment_f1": {
                "current_counts": dict(alignment),
                "threshold": alignment_threshold,
                "minimum_operation_models": alignment_ops,
                "observed_fp_fn_same_case_conversions_available": 4,
            },
            "unpaired_exact_case_rate": {
                "current_exact_cases": unpaired_exact,
                "denominator": case_count,
                "required_exact_cases": unpaired_required,
                "minimum_corrected_case_partitions": max(
                    0, unpaired_required - unpaired_exact
                ),
            },
        },
        "shared_root_cause": False,
        "root_cause_summary": [
            {
                "cause": "judge_instruction_ambiguity",
                "evidence": "dominant structured-field overflagging and residual pairing errors",
                "requires_intervention": "bounded LLM structured-field verification plus residual-pair review",
            },
            {
                "cause": "genuine_semantic_miss",
                "evidence": "one unsupported proposition was judged source-supported",
                "requires_intervention": "support specificity canary in diagnostic",
            },
        ],
        "smallest_joint_clearance_statement": (
            "At least 30 structured-field witness decisions, 1 support-specificity "
            "decision, and 2 unpaired case partitions must improve. The two residual "
            "partition fixes also clear alignment_f1 if at least one converts an observed "
            "false pair into its missing expected pair."
        ),
    }


def _build_protocol_audit() -> dict[str, Any]:
    return {
        "schema_version": V25_PROTOCOL_AUDIT_VERSION,
        "created_at": now_iso(),
        "llm_only_constraint": {
            "semantic_decisions_by_llm_only": True,
            "embeddings_used": False,
            "semantic_regex_or_keyword_pruning_used": False,
            "non_llm_semantic_matching_used": False,
            "fine_tuning_used": False,
            "deterministic_code_allowed_for": [
                "schema_validation",
                "enum_projection",
                "exact_evidence_and_offset_validation",
                "provenance_hashing",
                "lifecycle_and_retry_enforcement",
                "usage_accounting",
            ],
        },
        "existing_protocol_assessment": {
            "support_first": True,
            "neutral_alignment_after_support": True,
            "opaque_ids": True,
            "ab_ba_permutation_canary": True,
            "abstention_supported": True,
            "shared_augmented_reference": True,
            "primary_gap": (
                "The pointwise structured-field verdict overflags correct witnesses; "
                "alignment residual handling also pairs one-sided unsupported witnesses too often."
            ),
        },
        "v25_protocol_design": {
            "layer_1_support": (
                "Keep side-free proposition support and structured-event field correctness "
                "separate; support verdicts must not be inferred from structured field errors."
            ),
            "layer_2_alignment": (
                "Run neutral alignment on support-positive candidates first and keep "
                "support-negative witnesses in an explicit residual bucket unless a pair is "
                "LLM-justified by source-supported overlap."
            ),
            "layer_3_field_repair": (
                "For disputed structured fields, relation mismatches, and unpaired residuals, "
                "run one bounded side-free LLM verification pass over only the exact source "
                "unit, opaque IDs, field subset, and support receipt; output KEEP, PATCH, "
                "UNPAIR, PAIR, or ABSTAIN."
            ),
            "deterministic_projection_only": (
                "Code may apply a valid LLM patch to enum fields and recompute metrics, but "
                "may not create, delete, or relabel semantic facts without an LLM patch."
            ),
            "second_adjudicator_policy": (
                "Use a second independent adjudicator only if the diagnostic shows the first "
                "repair pass leaves a measured disagreement cluster whose projected full-run "
                "benefit is larger than the declared token ceiling cost."
            ),
        },
    }


def _build_diagnostic_spec(taxonomy: Mapping[str, Any]) -> dict[str, Any]:
    cases = {row["case_id"] for row in taxonomy["error_rows"]}
    selected = []
    for case_id in DIAGNOSTIC_CASE_IDS:
        rows = [row for row in taxonomy["error_rows"] if row["case_id"] == case_id]
        selected.append(
            {
                "case_id": case_id,
                "selection_kind": "failure_case" if case_id in cases else "clean_control",
                "covered_failed_gates": sorted(
                    {
                        row["failed_gate"]
                        for row in rows
                        if row["failed_gate"] in V24_FAILED_GATES
                    }
                ),
                "covered_directions": sorted({row["direction"] for row in rows}),
            }
        )
    if len(selected) != 18 or len({item["case_id"] for item in selected}) != 18:
        raise JudgeV5CalibrationV25Error("v25 diagnostic case selection drifted")
    return {
        "schema_version": V25_DIAGNOSTIC_SPEC_VERSION,
        "created_at": now_iso(),
        "semantic_attempt_authorized_by_this_artifact": False,
        "case_count": 18,
        "cases": selected,
        "privacy": {
            "contains_transcript_text": False,
            "contains_prompt_text": False,
            "contains_model_output_text": False,
        },
        "turn_plan": {
            "maximum_declared_semantic_turns": 10,
            "capacity_gate_before_every_turn": True,
            "managed_chatgpt_app_server_only": True,
            "api_key_billing_allowed": False,
            "raw_session_token_replay_allowed": False,
            "semantic_codex_exec_allowed": False,
            "retry_policy": "zero_retries_one_attempt_per_turn",
            "expected_total_token_ceiling": 420000,
            "unknown_usage_blocks_version": True,
        },
        "promotion_thresholds": {
            "support_sensitivity_min": CALIBRATION_GATES["support_sensitivity_min"],
            "support_specificity_min": CALIBRATION_GATES["support_specificity_min"],
            "structured_field_accuracy_min": CALIBRATION_GATES[
                "structured_field_accuracy_min"
            ],
            "alignment_f1_min": CALIBRATION_GATES["alignment_f1_min"],
            "equivalent_sensitivity_min": CALIBRATION_GATES[
                "equivalent_sensitivity_min"
            ],
            "equivalent_specificity_min": CALIBRATION_GATES[
                "equivalent_specificity_min"
            ],
            "field_diagnostic_f1_min": CALIBRATION_GATES["field_diagnostic_f1_min"],
            "relation_accuracy_min": CALIBRATION_GATES["relation_accuracy_min"],
            "equivalence_partition_exact_case_rate_min": CALIBRATION_GATES[
                "equivalence_partition_exact_case_rate_min"
            ],
            "unpaired_exact_case_rate_min": CALIBRATION_GATES[
                "unpaired_exact_case_rate_min"
            ],
            "abstention_rate_max": CALIBRATION_GATES["abstention_rate_max"],
            "order_bias_max": CALIBRATION_GATES["order_bias_max"],
        },
        "promotion_rule": (
            "Only a diagnostic passing every listed gate with complete usage may authorize "
            "one fresh full v25 development calibration. Any failed gate, transport error, "
            "unknown usage, retry need, or capacity stop leaves the lineage inactive and "
            "incomplete with no selection or holdout authorization."
        ),
    }


def build_v25_presemantic_recovery(
    *,
    v23_root: Path = DEFAULT_V23_ROOT,
    v24_root: Path = DEFAULT_V24_ROOT,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
) -> dict[str, Any]:
    source = v23_root.expanduser().resolve()
    prior = v24_root.expanduser().resolve()
    root = output_root.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v25 terminal")
    root.mkdir(parents=True, exist_ok=True)

    v23_terminal = _load_json(source / "terminal.json", "v23 terminal")
    v24_terminal = _load_json(prior / "terminal.json", "v24 terminal")
    if (
        v23_terminal.get("state") != "failed"
        or v23_terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or v24_terminal.get("state") != "completed"
        or v24_terminal.get("terminal_reason")
        != "judge_full_calibration_quality_gate_not_passed"
        or v24_terminal.get("calibration_passed") is not False
        or v24_terminal.get("selection_authorized") is not False
        or v24_terminal.get("production_mutated") is not False
        or tuple(v24_terminal.get("failed_quality_gates") or ()) != V24_FAILED_GATES
    ):
        raise JudgeV5CalibrationV25Error("v25 predecessor state is not the v24 quality failure")
    score_path = _verify_record(v24_terminal["score"], "v24 score")
    alignment_path = _verify_record(v24_terminal["reconciled_alignment"], "v24 alignment")
    _verify_record(v24_terminal["source_v23_terminal"], "v24 source terminal binding")
    source_records = {
        "v23_terminal": _record(source / "terminal.json"),
        "v23_failure": v24_terminal["source_v23_failure"],
        "v23_truth": _record(source / "calibration-truth.private.json"),
        "v23_pointwise_output": _record(source / "pointwise-output-full.private.json"),
        "v23_support_receipts": _record(source / "support-receipts.private.json"),
        "v23_shared_witness_pool": _record(source / "shared-witness-pool.private.json"),
        "v24_terminal": _record(prior / "terminal.json"),
        "v24_score": _record(score_path),
        "v24_reconciled_alignment": _record(alignment_path),
    }
    truth = _load_json(source / "calibration-truth.private.json", "v23 truth")
    pointwise_output = _load_json(
        source / "pointwise-output-full.private.json", "v23 pointwise output"
    )
    reconciled_alignment = _load_json(alignment_path, "v24 reconciled alignment")
    score = _load_json(score_path, "v24 score")
    taxonomy = _build_taxonomy(
        truth=truth,
        pointwise_output=pointwise_output,
        reconciled_alignment=reconciled_alignment,
        score=score,
        source_records=source_records,
    )
    taxonomy_path = root / "sanitized-error-taxonomy.json"
    _write_immutable_json(taxonomy_path, taxonomy)
    clearance = _build_clearance_analysis(taxonomy)
    clearance_path = root / "minimal-clearance-analysis.json"
    _write_immutable_json(clearance_path, clearance)
    protocol = _build_protocol_audit()
    protocol_path = root / "v25-protocol-audit.json"
    _write_immutable_json(protocol_path, protocol)
    diagnostic = _build_diagnostic_spec(taxonomy)
    diagnostic_path = root / "diagnostic-spec.json"
    _write_immutable_json(diagnostic_path, diagnostic)
    terminal = {
        "schema_version": V25_TERMINAL_VERSION,
        "state": "inactive",
        "terminal_at": now_iso(),
        "terminal_reason": "inactive_incomplete_recovery_required",
        "babysitter_status": "inactive_incomplete_recovery_required",
        "development_quality_gate_failure_is_overall_completion": False,
        "v24_quality_terminal_preserved_immutably": True,
        "semantic_attempt_started": False,
        "semantic_turn_count": 0,
        "new_semantic_usage": {
            "input_tokens": 0,
            "cached_input_tokens": 0,
            "output_tokens": 0,
            "reasoning_output_tokens": 0,
            "total_tokens": 0,
        },
        "predecessor_usage": v24_terminal["cumulative_usage"],
        "accounting_complete": True,
        "usage_status": "complete",
        "calibration_passed": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "failed_quality_gates": list(V24_FAILED_GATES),
        "next_required_artifact": "versioned_v25_diagnostic_semantic_attempt_after_capacity_policy",
        "source_records": source_records,
        "sanitized_error_taxonomy": _record(taxonomy_path),
        "minimal_clearance_analysis": _record(clearance_path),
        "v25_protocol_audit": _record(protocol_path),
        "diagnostic_spec": _record(diagnostic_path),
    }
    _write_immutable_json(terminal_path, terminal)
    return terminal


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Prepare v25 judge recovery artifacts")
    parser.add_argument("--v23-root", default=str(DEFAULT_V23_ROOT))
    parser.add_argument("--v24-root", default=str(DEFAULT_V24_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args(argv)
    terminal = build_v25_presemantic_recovery(
        v23_root=Path(args.v23_root),
        v24_root=Path(args.v24_root),
        output_root=Path(args.output_root),
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "semantic_attempt_started": terminal["semantic_attempt_started"],
                "selection_authorized": terminal["selection_authorized"],
                "diagnostic_spec": terminal["diagnostic_spec"]["path"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
