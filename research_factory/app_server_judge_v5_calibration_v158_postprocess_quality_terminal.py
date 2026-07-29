from __future__ import annotations

"""Score v157 after correcting its deterministic disagreement-count KeyError."""

import argparse
import itertools
import json
from collections import Counter
from copy import deepcopy
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v130_retained_alignment_owner as v130
from . import app_server_judge_v5_calibration_v155_fresh_full_development as v155
from . import app_server_judge_v5_calibration_v157_exact_span_canary_recovery as v157
from .app_server_judge_v5_calibration_v26_diagnostic import _load_json, _write_immutable
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _validate_usage
from .util import now_iso, sha256_text


V158_AUDIT_VERSION = "pif_app_server_judge_v5_4_v158_postprocess_audit_v1"
V158_TAXONOMY_VERSION = "pif_app_server_judge_v5_4_v158_residual_taxonomy_v1"
V158_DIAGNOSTIC_VERSION = "pif_app_server_judge_v5_4_v158_replacement_diagnostic_contract_v1"
V158_SCORE_VERSION = "pif_app_server_judge_v5_4_v158_score_v1"
V158_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v158_terminal_v1"
DEFAULT_OUTPUT_ROOT = (
    v157.DEFAULT_OUTPUT_ROOT.parent
    / "judge-calibration-v5_4-v158-postprocess-quality-terminal"
).resolve()


class JudgeV5CalibrationV158Error(RuntimeError):
    """The immutable v158 postprocess contract cannot be preserved."""


def _ratio(numerator: int, denominator: int) -> float:
    return numerator / denominator if denominator else 0.0


def _f1(tp: int, fp: int, fn: int) -> float:
    denominator = 2 * tp + fp + fn
    return 2 * tp / denominator if denominator else 1.0


def _validate_v157_failure() -> dict[str, Any]:
    root = v157.DEFAULT_OUTPUT_ROOT
    paths = {
        "spec": root / "exact-span-canary-recovery-spec.json",
        "terminal": root / "terminal.json",
        "failure": root / "failure.json",
        "reconciled": root / "alignment-reconciled.private.json",
        "disagreements": root / "alignment-observable-disagreements.private.json",
        "base": root / "alignment-base-projected.private.json",
        "canary": root / "alignment-canary-projected.private.json",
    }
    values = {name: _load_json(path, f"v157 {name}") for name, path in paths.items()}
    terminal, failure, spec = values["terminal"], values["failure"], values["spec"]
    expected_usage = {
        "input_tokens": 51310,
        "cached_input_tokens": 1920,
        "output_tokens": 20071,
        "reasoning_output_tokens": 6634,
        "total_tokens": 71381,
    }
    expected_predecessor = {
        "input_tokens": 1267388,
        "cached_input_tokens": 61440,
        "output_tokens": 168121,
        "reasoning_output_tokens": 57849,
        "total_tokens": 1435509,
    }
    expected_cumulative = {
        "input_tokens": 1318698,
        "cached_input_tokens": 63360,
        "output_tokens": 188192,
        "reasoning_output_tokens": 64483,
        "total_tokens": 1506890,
    }
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage_status") != "complete"
        or terminal.get("usage") != expected_usage
        or terminal.get("predecessor_cumulative_usage") != expected_predecessor
        or terminal.get("cumulative_known_usage_lower_bound") != expected_cumulative
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or failure.get("classification") != "infrastructure_or_judge_attempt_failed"
        or failure.get("failed_turn_name") != v157.ADJUDICATION_TURN
        or failure.get("error_class") != "KeyError"
        or failure.get("retry_allowed_in_this_version") is not False
        or failure.get("accounting_complete") is not True
        or failure.get("unknown_usage_turn_count") != 0
        or failure.get("usage") != expected_usage
        or failure.get("predecessor_cumulative_usage") != expected_predecessor
        or failure.get("cumulative_known_usage_lower_bound") != expected_cumulative
        or spec.get("turn_plan") != list(v157.TURN_NAMES)
        or spec.get("predecessor_semantic_turn_count_reused") != 57
        or spec.get("predecessor_turn_count_replayed") != 0
        or spec.get("retry_count_per_turn") != 0
        or terminal.get("failure") != _record(paths["failure"])
        or (root / "full-calibration-score.json").exists()
    ):
        raise JudgeV5CalibrationV158Error("v157 postprocess-failure contract drifted")
    if not all(_verify_record(record) for record in spec.get("runtime_files") or []):
        raise JudgeV5CalibrationV158Error("v157 runtime record drifted")

    attempts = failure.get("attempts") or []
    if (
        len(attempts) != 2
        or {row.get("turn_name") for row in attempts} != set(v157.TURN_NAMES)
    ):
        raise JudgeV5CalibrationV158Error("v157 attempt coverage drifted")
    usage = {field: 0 for field in USAGE_FIELDS}
    for attempt in attempts:
        for key in ("capacity", "sidecar", "output"):
            if not _verify_record(attempt[key]):
                raise JudgeV5CalibrationV158Error("v157 attempt record drifted")
        measured = _validate_usage(
            _load_json(Path(attempt["sidecar"]["path"]), "v157 measured sidecar")
        )
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
        turn_root = root / "turns" / str(attempt["turn_name"]).replace("_", "-")
        raw = _load_json(turn_root / "output.private.json", "v157 raw output")
        projected = _load_json(
            turn_root / "structurally-projected.private.json", "v157 projected output"
        )
        audit = _load_json(
            turn_root / "structural-projection-audit.json", "v157 projection audit"
        )
        if attempt["turn_name"] == v157.CANARY_TURN:
            alignment_input = _load_json(
                turn_root / "input.private.json", "v157 canary input"
            )
        else:
            request_input = _load_json(
                turn_root / "input.private.json", "v157 adjudication input"
            )
            alignment_input = request_input["alignment_input"]
        rebuilt, rebuilt_audit = v157.project_exact_spans_and_relation(
            raw, alignment_input
        )
        if projected != rebuilt or audit != rebuilt_audit:
            raise JudgeV5CalibrationV158Error("v157 structural projection drifted")
    if usage != expected_usage:
        raise JudgeV5CalibrationV158Error("v157 measured usage drifted")
    if (
        values["disagreements"].get("adjudication_required") is not True
        or len(values["disagreements"].get("cases") or []) != 2
    ):
        raise JudgeV5CalibrationV158Error("v157 disagreement packet drifted")
    source = v157._validate_v156_failure()
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "attempts": attempts,
        "usage": usage,
        "predecessor_usage": expected_predecessor,
        "cumulative_usage": expected_cumulative,
        "source": source,
    }


def _projected_alignment_metrics(
    observed: Mapping[str, Mapping[str, Any]],
    expected: Mapping[str, Mapping[str, Any]],
) -> dict[str, float]:
    pair_tp = pair_fp = pair_fn = relation_exact = relation_count = 0
    eq_tp = eq_fn = eq_tn = eq_fp = field_tp = field_fp = field_fn = 0
    group_exact = unpaired_exact = 0
    for case_id, wanted in expected.items():
        actual = observed[case_id]
        wanted_map = {
            tuple(sorted(row["witness_ids"])): row for row in wanted["pairs"]
        }
        actual_map = {
            tuple(sorted(row["witness_ids"])): row for row in actual["pairs"]
        }
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
        group_exact += int(
            actual["equivalence_groups"] == wanted["equivalence_groups"]
        )
        unpaired_exact += int(
            actual["unpaired_witness_ids"] == wanted["unpaired_witness_ids"]
        )
    return {
        "alignment_f1": _f1(pair_tp, pair_fp, pair_fn),
        "relation_accuracy": _ratio(relation_exact, relation_count),
        "equivalent_sensitivity": _ratio(eq_tp, eq_tp + eq_fn),
        "equivalent_specificity": _ratio(eq_tn, eq_tn + eq_fp),
        "mismatch_field_f1": _f1(field_tp, field_fp, field_fn),
        "equivalence_partition_exact_case_rate": _ratio(group_exact, 66),
        "unpaired_exact_case_rate": _ratio(unpaired_exact, 66),
    }


def score_and_taxonomy(source: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    v155_source = source["source"]["v155"]
    truth = v155_source["values"]["truth"]
    reconciled = source["values"]["reconciled"]
    disagreement_count = len(source["values"]["disagreements"]["cases"])
    final_canary_exact = (
        len(truth["alignment_canary_case_ids"])
        if not reconciled["unresolved_cases_abstained"]
        else 0
    )
    score = v155.score_v155(
        support=v155_source["values"]["support"],
        support_canary=v155_source["values"]["support_canary"],
        fields=v155_source["values"]["fields"],
        field_repeats=v155_source["values"]["field_repeats"],
        alignment=reconciled,
        truth=truth,
        raw_alignment_disagreement_count=disagreement_count,
        final_canary_exact_count=final_canary_exact,
    )
    score = {**score, "schema_version": V158_SCORE_VERSION}

    support_rows = {
        str(row["witness_id"]): row
        for row in v155_source["values"]["support"]["units"]
    }
    field_observed = {
        str(row["task_id"]): row
        for row in v155_source["values"]["fields"]["decisions"]
    }
    support_projection = {
        "supported": "correct",
        "unsupported": "incorrect",
        "abstain": "abstain",
    }
    for row in truth["field_tasks"]:
        if row["field"] == "unsupported_inference":
            field_observed[row["task_id"]] = {
                "field_status": support_projection[
                    support_rows[row["witness_id"]]["support_status"]
                ]
            }
    field_errors = []
    for row in truth["field_tasks"]:
        actual = field_observed[row["task_id"]]["field_status"]
        if actual != row["expected_status"]:
            field_errors.append(
                {
                    "task_id": row["task_id"],
                    "field": row["field"],
                    "expected_status": row["expected_status"],
                    "observed_status": actual,
                }
            )

    expected = {
        str(row["case_id"]): row["expected"] for row in truth["alignment_cases"]
    }
    observed = {
        str(row["case_id"]): v130._project_alignment(row)
        for row in reconciled["cases"]
    }
    alignment_errors = []
    for case_index, truth_row in enumerate(truth["alignment_cases"]):
        case_id = str(truth_row["case_id"])
        wanted = expected[case_id]
        actual = observed[case_id]
        wanted_pairs = {
            tuple(sorted(row["witness_ids"])): row for row in wanted["pairs"]
        }
        actual_pairs = {
            tuple(sorted(row["witness_ids"])): row for row in actual["pairs"]
        }
        categories = []
        if set(wanted_pairs) != set(actual_pairs):
            categories.append("pair_membership")
        for key in set(wanted_pairs) & set(actual_pairs):
            if wanted_pairs[key]["relation"] != actual_pairs[key]["relation"]:
                categories.append("relation")
            if set(wanted_pairs[key]["mismatch_fields"]) != set(
                actual_pairs[key]["mismatch_fields"]
            ):
                categories.append("mismatch_fields")
        if wanted["equivalence_groups"] != actual["equivalence_groups"]:
            categories.append("equivalence_partition")
        if wanted["unpaired_witness_ids"] != actual["unpaired_witness_ids"]:
            categories.append("unpaired")
        if categories:
            alignment_errors.append(
                {
                    "case_id": case_id,
                    "case_index": case_index,
                    "categories": sorted(set(categories)),
                }
            )

    alignment_error_ids = [row["case_id"] for row in alignment_errors]
    minimum_alignment_corrections: Optional[int] = None
    passing_subset_count = 0
    for size in range(len(alignment_error_ids) + 1):
        passing = 0
        for subset in itertools.combinations(alignment_error_ids, size):
            candidate = {
                case_id: expected[case_id] if case_id in subset else observed[case_id]
                for case_id in expected
            }
            metrics = _projected_alignment_metrics(candidate, expected)
            if all(value >= 0.95 for value in metrics.values()):
                passing += 1
        if passing:
            minimum_alignment_corrections = size
            passing_subset_count = passing
            break
    if minimum_alignment_corrections is None:
        raise JudgeV5CalibrationV158Error("alignment correction bound is unavailable")

    field_counts = Counter(
        (row["field"], row["expected_status"], row["observed_status"])
        for row in field_errors
    )
    alignment_counts = Counter(
        category for row in alignment_errors for category in row["categories"]
    )
    taxonomy = {
        "schema_version": V158_TAXONOMY_VERSION,
        "field_error_count": len(field_errors),
        "field_errors": field_errors,
        "field_error_counts": [
            {
                "field": key[0],
                "expected_status": key[1],
                "observed_status": key[2],
                "count": count,
            }
            for key, count in sorted(field_counts.items())
        ],
        "minimum_field_decision_corrections_to_clear_all_field_gates": 5,
        "alignment_error_case_count": len(alignment_errors),
        "alignment_errors": alignment_errors,
        "alignment_category_counts": dict(sorted(alignment_counts.items())),
        "minimum_alignment_case_corrections_to_clear_all_alignment_gates": minimum_alignment_corrections,
        "minimum_alignment_passing_subset_count": passing_subset_count,
        "source_text_included": False,
        "truth_labels_exposed_to_future_model": False,
        "production_mutated": False,
    }
    return score, taxonomy


def _select_controls(
    *, all_ids: Sequence[str], error_ids: set[str], prefix: str, count: int
) -> list[str]:
    candidates = sorted(
        (value for value in all_ids if value not in error_ids),
        key=lambda value: sha256_text(f"v158|{prefix}|{value}"),
    )
    if len(candidates) < count:
        raise JudgeV5CalibrationV158Error("diagnostic control coverage is insufficient")
    return candidates[:count]


def build_diagnostic_contract(
    source: Mapping[str, Any], taxonomy: Mapping[str, Any]
) -> dict[str, Any]:
    truth = source["source"]["v155"]["values"]["truth"]
    field_errors = [str(row["task_id"]) for row in taxonomy["field_errors"]]
    field_controls = _select_controls(
        all_ids=[str(row["task_id"]) for row in truth["field_tasks"]],
        error_ids=set(field_errors),
        prefix="field-control",
        count=6,
    )
    alignment_errors = [str(row["case_id"]) for row in taxonomy["alignment_errors"]]
    alignment_controls = _select_controls(
        all_ids=[str(row["case_id"]) for row in truth["alignment_cases"]],
        error_ids=set(alignment_errors),
        prefix="alignment-control",
        count=6,
    )
    return {
        "schema_version": V158_DIAGNOSTIC_VERSION,
        "created_at": now_iso(),
        "strategy": "blinded_replacement_model_diagnostic_before_any_full_replacement",
        "field_model": "gpt-5.6-sol",
        "field_effort": "high",
        "field_context_mode": "singleton",
        "field_error_task_ids": field_errors,
        "field_control_task_ids": field_controls,
        "field_turn_count": 12,
        "alignment_model": "gpt-5.5",
        "alignment_effort": "high",
        "alignment_error_case_ids": alignment_errors,
        "alignment_control_case_ids": alignment_controls,
        "alignment_case_count": 12,
        "alignment_primary_shard_count": 2,
        "alignment_balanced_canary_shard_count": 2,
        "maximum_turn_count": 16,
        "maximum_total_tokens_per_turn": 45000,
        "maximum_total_token_bound": 720000,
        "retry_count_per_turn": 0,
        "truth_labels_exposed_to_model": False,
        "side_labels_exposed_to_model": False,
        "system_identity_exposed_to_model": False,
        "field_pass_gates": {
            "overall_exact_count_minimum": 11,
            "residual_exact_count_minimum": 5,
            "control_exact_count_minimum": 6,
            "abstention_count_maximum": 0,
        },
        "alignment_pass_gates": {
            "replacement_metrics_clear_all_frozen_alignment_gates": True,
            "residual_exact_case_count_minimum": 5,
            "control_exact_case_count_minimum": 6,
            "permutation_exact_case_count_minimum": 12,
            "abstention_case_count_maximum": 0,
        },
        "promotion_rule": "freeze_models_then_run_fresh_full_replacement_field_and_alignment_calibration",
        "stop_rule": "any_failed_gate_blocks_full_replacement_and_requires_new_immutable_strategy",
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }


def freeze_v158(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v158 terminal")}
    source = _validate_v157_failure()
    score, taxonomy = score_and_taxonomy(source)
    diagnostic = build_diagnostic_contract(source, taxonomy)
    audit = {
        "schema_version": V158_AUDIT_VERSION,
        "created_at": now_iso(),
        "v157_error_class": "KeyError",
        "root_cause": "disagreement_packet_cases_array_was_read_through_nonexistent_disagreement_case_count_key",
        "correct_operation": "len(disagreement_packet.cases)",
        "semantic_model_output_changed": False,
        "reference_truth_changed": False,
        "quality_gates_changed": False,
        "new_semantic_turn_count": 0,
        "production_mutated": False,
        "predecessor": {
            **{f"v157_{name}": record for name, record in source["records"].items()},
            "v157_attempts": source["attempts"],
            "v157_usage": source["usage"],
            "cumulative_usage": source["cumulative_usage"],
        },
    }
    paths = {
        "audit": root / "postprocess-audit.json",
        "score": root / "full-calibration-score.json",
        "taxonomy": root / "residual-error-taxonomy.json",
        "diagnostic": root / "replacement-diagnostic-contract.json",
    }
    _write_immutable(paths["audit"], audit)
    _write_immutable(paths["score"], score)
    _write_immutable(paths["taxonomy"], taxonomy)
    if paths["diagnostic"].exists():
        diagnostic["created_at"] = _load_json(
            paths["diagnostic"], "existing v158 diagnostic"
        )["created_at"]
    _write_immutable(paths["diagnostic"], diagnostic)
    terminal = {
        "schema_version": V158_TERMINAL_VERSION,
        "state": "inactive",
        "terminal_at": now_iso(),
        "terminal_reason": "inactive_incomplete_recovery_required",
        "development_terminal_reason": "v158_full_development_calibration_quality_gate_not_passed",
        "overall_evaluation_complete": False,
        "development_judge_frozen": False,
        "bounded_replacement_model_diagnostic_authorized": True,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_attempt_started": False,
        "semantic_retry_count": 0,
        "usage_status": "complete",
        "accounting_complete": True,
        "usage": {field: 0 for field in USAGE_FIELDS},
        "predecessor_cumulative_usage": source["cumulative_usage"],
        "cumulative_calibration_usage": source["cumulative_usage"],
        "failed_quality_gates": score["failed_checks"],
        "metrics": score["metrics"],
        "audit": _record(paths["audit"]),
        "score": _record(paths["score"]),
        "taxonomy": _record(paths["taxonomy"]),
        "diagnostic_contract": _record(paths["diagnostic"]),
    }
    if terminal_path.exists():
        terminal["terminal_at"] = _load_json(
            terminal_path, "existing v158 terminal"
        )["terminal_at"]
    _write_immutable(terminal_path, terminal)
    return {
        "root": root,
        "terminal": terminal,
        "score": score,
        "taxonomy": taxonomy,
        "diagnostic": diagnostic,
        "source": source,
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Freeze v158 postprocess quality terminal")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args(argv)
    result = freeze_v158(output_dir=Path(args.output_dir))
    terminal = result["terminal"]
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "failed_quality_gates": terminal["failed_quality_gates"],
                "bounded_replacement_model_diagnostic_authorized": terminal[
                    "bounded_replacement_model_diagnostic_authorized"
                ],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
