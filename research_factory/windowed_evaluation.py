from __future__ import annotations

import json
import math
import random
import time
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from .efficient_backtest import (
    DEFAULT_WINDOWED_GUIDELINES_PATH,
    _codex_usage_from_jsonl,
    _collect_manifest_holdout_ids,
    _json_file_ok,
    _run_codex_smoke_command,
    _semantic_judge_event,
    normalize_windowed_core_payload,
    run_windowed_event_core_smoke,
    run_windowed_event_enrichment_smoke,
    windowed_event_core_schema,
)
from .util import now_iso, sha256_text, write_text_atomic


EVALUATION_DIR = Path(__file__).resolve().parent / "evaluation"
DEFAULT_ACCEPTANCE_SPEC_PATH = EVALUATION_DIR / "windowed_acceptance_v1.json"
DEFAULT_EVALUATOR_SPEC_PATH = EVALUATION_DIR / "windowed_acceptance_v2.json"
DEFAULT_JUDGE_FIXTURE_PATH = EVALUATION_DIR / "judge_calibration_v1.json"
DEFAULT_EXPANDED_JUDGE_FIXTURE_PATH = EVALUATION_DIR / "judge_calibration_v2.json"
JUDGE_CALIBRATION_SCHEMA_VERSION = "windowed_judge_calibration_run_v1"
EXPANDED_JUDGE_CALIBRATION_SCHEMA_VERSION = "windowed_judge_calibration_run_v2"
PAIRED_MANIFEST_SCHEMA_VERSION = "windowed_paired_evaluation_manifest_v1"
PAIRED_BASELINE_RUN_SCHEMA_VERSION = "windowed_paired_baseline_run_v1"
PAIRED_PHASE_ONE_SCHEMA_VERSION = "windowed_paired_phase_one_v1"
PAIRED_ENRICHMENT_RUN_SCHEMA_VERSION = "windowed_paired_enrichment_run_v1"
PAIRED_CORE_REPAIR_SCHEMA_VERSION = "windowed_paired_core_repair_v1"
PAIRED_ADJUDICATION_MANIFEST_VERSION = "windowed_paired_adjudication_v1"
PAIRED_ADJUDICATION_SCORE_VERSION = "windowed_paired_adjudication_score_v1"
CONSOLIDATED_ADJUDICATION_VERSION = "windowed_consolidated_human_adjudication_v1"
PAIRED_PROVENANCE_SCHEMA_VERSION = "windowed_paired_provenance_v1"
PAIRED_CONTEXT_COST_SCHEMA_VERSION = "windowed_paired_context_cost_v1"
NO_SIGNAL_POWER_MANIFEST_VERSION = "windowed_no_signal_power_manifest_v1"
NO_SIGNAL_POWER_REPORT_VERSION = "windowed_no_signal_power_report_v1"
NO_SIGNAL_HUMAN_AUDIT_VERSION = "windowed_no_signal_human_audit_v1"
NO_SIGNAL_AUDIT_MAPPING_VERSION = "windowed_no_signal_audit_mapping_v1"
PROSPECTIVE_SHADOW_MANIFEST_VERSION = "windowed_prospective_shadow_manifest_v1"
INTERRUPTED_CORE_RECOVERY_VERSION = "windowed_interrupted_core_recovery_v1"
CHECKPOINTED_CORE_RUN_VERSION = "windowed_checkpointed_core_run_v1"
HISTORICAL_CONTEXT_USAGE_VERSION = "historical_episode_context_rollout_usage_v1"
HISTORICAL_CONTEXT_USAGE_REPORT_VERSION = "historical_episode_context_usage_recovery_v1"
PAIRED_ACCEPTANCE_BUNDLE_VERSION = "windowed_paired_acceptance_evidence_v1"
PAIRED_FAIL_CLOSED_REPORT_VERSION = "windowed_paired_fail_closed_gate_v1"


def load_windowed_acceptance_spec(path: str | Path = DEFAULT_ACCEPTANCE_SPEC_PATH) -> dict[str, Any]:
    spec_path = Path(path).expanduser().resolve()
    payload = json.loads(spec_path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != "windowed_acceptance_v1":
        raise ValueError("unsupported windowed acceptance specification")
    return payload


def load_windowed_evaluator_spec(path: str | Path = DEFAULT_EVALUATOR_SPEC_PATH) -> dict[str, Any]:
    spec_path = Path(path).expanduser().resolve()
    payload = json.loads(spec_path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != "windowed_acceptance_v2":
        raise ValueError("unsupported fail-closed windowed evaluator specification")
    return payload


def _load_windowed_spec(path: str | Path) -> dict[str, Any]:
    payload = json.loads(Path(path).expanduser().resolve().read_text(encoding="utf-8"))
    if payload.get("schema_version") not in {"windowed_acceptance_v1", "windowed_acceptance_v2"}:
        raise ValueError("unsupported windowed specification")
    return payload


def load_judge_calibration_fixture(path: str | Path = DEFAULT_JUDGE_FIXTURE_PATH) -> dict[str, Any]:
    fixture_path = Path(path).expanduser().resolve()
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != "judge_calibration_v1":
        raise ValueError("unsupported judge calibration fixture")
    cases = payload.get("cases")
    if not isinstance(cases, list) or len(cases) < 20:
        raise ValueError("judge calibration fixture must contain at least 20 cases")
    case_ids = [case.get("case_id") for case in cases]
    if len(set(case_ids)) != len(case_ids) or any(not isinstance(case_id, str) for case_id in case_ids):
        raise ValueError("judge calibration case IDs must be unique strings")
    return payload


def materialize_expanded_judge_calibration_cases(fixture: dict[str, Any]) -> list[dict[str, Any]]:
    base_event = fixture["base_event"]
    cases = []
    for topic in fixture.get("topics") or []:
        topic_id = str(topic["topic_id"])
        source_excerpt = str(topic["source_excerpt"])
        primary = _deep_merge(base_event, topic["primary"])
        primary_paraphrase = _deep_merge(primary, topic["primary_paraphrase"])
        secondary = _deep_merge(base_event, topic["secondary"])
        secondary_paraphrase = _deep_merge(secondary, topic["secondary_paraphrase"])
        changed = _deep_merge(primary, topic["changed"])
        merged = _deep_merge(primary, topic["merged"])
        unsupported = _deep_merge(primary, topic["unsupported"])
        multi_primary = _deep_merge(primary, {"evidence": source_excerpt})
        multi_primary_paraphrase = _deep_merge(primary_paraphrase, {"evidence": source_excerpt})
        multi_secondary = _deep_merge(secondary, {"evidence": source_excerpt})
        multi_secondary_paraphrase = _deep_merge(secondary_paraphrase, {"evidence": source_excerpt})
        common = {"topic_id": topic_id, "source_excerpt": source_excerpt}
        cases.extend(
            [
                {
                    **common,
                    "case_id": f"{topic_id}__identity_paraphrase",
                    "shape": "single_event_pairs",
                    "set_a": [primary],
                    "set_b": [primary_paraphrase],
                    "expected_pairs": [
                        {
                            "pair_slot": "primary",
                            "a_id": 0,
                            "b_id": 0,
                            "relation": "equivalent",
                            "mismatch_fields": [],
                        }
                    ],
                    "support_a": [True],
                    "support_b": [True],
                },
                {
                    **common,
                    "case_id": f"{topic_id}__material_field_error",
                    "shape": "single_event_pairs",
                    "set_a": [primary],
                    "set_b": [changed],
                    "expected_pairs": [
                        {
                            "pair_slot": "primary_changed",
                            "a_id": 0,
                            "b_id": 0,
                            "relation": "non_equivalent",
                            "mismatch_fields": sorted(topic["changed_fields"]),
                        }
                    ],
                    "support_a": [True],
                    "support_b": ["unsupported_inference" not in topic["changed_fields"]],
                },
                {
                    **common,
                    "case_id": f"{topic_id}__merged_split_boundary",
                    "shape": "merged_and_split_boundaries",
                    "set_a": [merged],
                    "set_b": [primary, secondary],
                    "expected_pairs": [
                        {
                            "pair_slot": "merged_to_primary",
                            "a_id": 0,
                            "b_id": 0,
                            "relation": "partial",
                            "mismatch_fields": ["event_boundary", "evidence"],
                        }
                    ],
                    "support_a": [True],
                    "support_b": [True, True],
                },
                {
                    **common,
                    "case_id": f"{topic_id}__reversed_multi_lens",
                    "shape": "multi_event_set_alignment",
                    "set_a": [multi_primary, multi_secondary],
                    "set_b": [multi_secondary_paraphrase, multi_primary_paraphrase],
                    "expected_pairs": [
                        {
                            "pair_slot": "primary",
                            "a_id": 0,
                            "b_id": 1,
                            "relation": "equivalent",
                            "mismatch_fields": [],
                        },
                        {
                            "pair_slot": "secondary",
                            "a_id": 1,
                            "b_id": 0,
                            "relation": "equivalent",
                            "mismatch_fields": [],
                        },
                    ],
                    "support_a": [True, True],
                    "support_b": [True, True],
                },
                {
                    **common,
                    "case_id": f"{topic_id}__supported_one_sided",
                    "shape": "one_sided_supported_residuals",
                    "set_a": [primary],
                    "set_b": [primary_paraphrase, secondary],
                    "expected_pairs": [
                        {
                            "pair_slot": "primary",
                            "a_id": 0,
                            "b_id": 0,
                            "relation": "equivalent",
                            "mismatch_fields": [],
                        }
                    ],
                    "support_a": [True],
                    "support_b": [True, True],
                },
                {
                    **common,
                    "case_id": f"{topic_id}__unsupported_one_sided",
                    "shape": "one_sided_unsupported_residuals",
                    "set_a": [primary],
                    "set_b": [primary_paraphrase, unsupported],
                    "expected_pairs": [
                        {
                            "pair_slot": "primary",
                            "a_id": 0,
                            "b_id": 0,
                            "relation": "equivalent",
                            "mismatch_fields": [],
                        }
                    ],
                    "support_a": [True],
                    "support_b": [True, False],
                },
            ]
        )
    return cases


def load_expanded_judge_calibration_fixture(
    path: str | Path = DEFAULT_EXPANDED_JUDGE_FIXTURE_PATH,
) -> dict[str, Any]:
    fixture_path = Path(path).expanduser().resolve()
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != "judge_calibration_v2":
        raise ValueError("unsupported expanded judge calibration fixture")
    topics = payload.get("topics")
    templates = payload.get("templates")
    if not isinstance(topics, list) or len(topics) < 10:
        raise ValueError("expanded judge calibration requires at least ten topic domains")
    if not isinstance(templates, list) or len(templates) < 6:
        raise ValueError("expanded judge calibration requires all set-level shapes")
    topic_ids = [topic.get("topic_id") for topic in topics]
    if any(not isinstance(topic_id, str) for topic_id in topic_ids) or len(topic_ids) != len(set(topic_ids)):
        raise ValueError("expanded judge topic IDs must be unique strings")
    cases = materialize_expanded_judge_calibration_cases(payload)
    minimum = int(payload.get("minimum_expanded_cases") or 60)
    if len(cases) < minimum:
        raise ValueError(f"expanded judge fixture has {len(cases)} cases; requires {minimum}")
    case_ids = [case["case_id"] for case in cases]
    if len(case_ids) != len(set(case_ids)):
        raise ValueError("expanded judge case IDs must be unique")
    required_shapes = {
        "single_event_pairs",
        "multi_event_set_alignment",
        "one_sided_supported_residuals",
        "one_sided_unsupported_residuals",
        "merged_and_split_boundaries",
    }
    if not required_shapes <= {case["shape"] for case in cases}:
        raise ValueError("expanded judge fixture is missing required shapes")
    return {**payload, "expanded_cases": cases, "fixture_path": str(fixture_path)}


def build_expanded_judge_calibration_variants(
    fixture: dict[str, Any],
    *,
    seed: str = "judge-calibration-order-v2",
) -> dict[str, dict[str, Any]]:
    cases = fixture.get("expanded_cases") or materialize_expanded_judge_calibration_cases(fixture)
    variants = {}
    for variant_name, reverse in (("ab", False), ("ba", True)):
        rows = []
        expected = {}
        ordered_cases = sorted(
            cases,
            key=lambda case: sha256_text(f"{seed}|{variant_name}|case|{case['case_id']}"),
        )
        for case in ordered_cases:
            opaque_id = "jc2_" + sha256_text(
                f"{seed}|{variant_name}|{case['case_id']}"
            )[:16]
            set_a = json.loads(json.dumps(case["set_b"] if reverse else case["set_a"]))
            set_b = json.loads(json.dumps(case["set_a"] if reverse else case["set_b"]))
            support_a = list(case["support_b"] if reverse else case["support_a"])
            support_b = list(case["support_a"] if reverse else case["support_b"])
            raw_pairs = [
                {
                    **pair,
                    "a_id": pair["b_id"] if reverse else pair["a_id"],
                    "b_id": pair["a_id"] if reverse else pair["b_id"],
                }
                for pair in case["expected_pairs"]
            ]

            def permute(
                events: list[dict[str, Any]],
                supports: list[bool],
                side: str,
            ) -> tuple[list[dict[str, Any]], dict[int, int], dict[int, dict[str, Any]]]:
                order = sorted(
                    range(len(events)),
                    key=lambda index: sha256_text(
                        f"{seed}|{variant_name}|{case['case_id']}|{side}|{index}"
                    ),
                )
                old_to_new = {old_id: new_id for new_id, old_id in enumerate(order)}
                rendered = []
                expected_support = {}
                for new_id, old_id in enumerate(order):
                    rendered.append({"id": new_id, **events[old_id]})
                    canonical_side = "b" if reverse and side == "a" else "a" if reverse and side == "b" else side
                    expected_support[new_id] = {
                        "supported": bool(supports[old_id]),
                        "canonical_slot": f"{canonical_side}{old_id}",
                    }
                return rendered, old_to_new, expected_support

            rendered_a, map_a, expected_support_a = permute(set_a, support_a, "a")
            rendered_b, map_b, expected_support_b = permute(set_b, support_b, "b")
            expected_pairs = [
                {
                    **pair,
                    "a_id": map_a[pair["a_id"]],
                    "b_id": map_b[pair["b_id"]],
                }
                for pair in raw_pairs
            ]
            rows.append(
                {
                    "case_id": opaque_id,
                    "source_excerpt": case["source_excerpt"],
                    "event_set_a": rendered_a,
                    "event_set_b": rendered_b,
                }
            )
            expected[opaque_id] = {
                "base_case_id": case["case_id"],
                "shape": case["shape"],
                "pairs": expected_pairs,
                "support_a": expected_support_a,
                "support_b": expected_support_b,
            }
        variants[variant_name] = {"cases": rows, "expected": expected}
    return variants


def validate_expanded_judge_calibration_output(
    output: Any,
    *,
    expected: dict[str, dict[str, Any]],
    mismatch_fields: list[str],
) -> list[str]:
    if not isinstance(output, dict) or set(output) != {"cases"} or not isinstance(output.get("cases"), list):
        return ["invalid_root"]
    errors = []
    rows = output["cases"]
    if len(rows) != len(expected):
        errors.append("case_count_mismatch")
    seen_cases = set()
    allowed_fields = set(mismatch_fields)
    allowed_relations = {"equivalent", "partial", "non_equivalent"}
    for row_index, row in enumerate(rows):
        prefix = f"case_{row_index}"
        if not isinstance(row, dict) or set(row) != {"case_id", "pairs", "support_a", "support_b"}:
            errors.append(f"{prefix}_invalid_shape")
            continue
        case_id = row.get("case_id")
        if case_id not in expected or case_id in seen_cases:
            errors.append(f"{prefix}_invalid_or_duplicate_id")
            continue
        seen_cases.add(case_id)
        truth = expected[case_id]
        valid_a = set(truth["support_a"])
        valid_b = set(truth["support_b"])
        pairs = row.get("pairs")
        if not isinstance(pairs, list):
            errors.append(f"{prefix}_pairs_not_array")
        else:
            used_a = set()
            used_b = set()
            for pair_index, pair in enumerate(pairs):
                if not isinstance(pair, dict) or set(pair) != {
                    "a_id",
                    "b_id",
                    "relation",
                    "mismatch_fields",
                }:
                    errors.append(f"{prefix}_pair_{pair_index}_invalid_shape")
                    continue
                fields = pair.get("mismatch_fields")
                if (
                    pair.get("a_id") not in valid_a
                    or pair.get("b_id") not in valid_b
                    or pair["a_id"] in used_a
                    or pair["b_id"] in used_b
                    or pair.get("relation") not in allowed_relations
                    or not isinstance(fields, list)
                    or len(fields) != len(set(fields))
                    or any(field not in allowed_fields for field in fields)
                    or (pair.get("relation") == "equivalent" and fields)
                ):
                    errors.append(f"{prefix}_pair_{pair_index}_invalid")
                    continue
                used_a.add(pair["a_id"])
                used_b.add(pair["b_id"])
        for side in ("a", "b"):
            support = row.get(f"support_{side}")
            valid_ids = valid_a if side == "a" else valid_b
            if not isinstance(support, list):
                errors.append(f"{prefix}_support_{side}_not_array")
                continue
            seen_ids = set()
            for support_index, item in enumerate(support):
                if (
                    not isinstance(item, dict)
                    or set(item) != {"id", "supported"}
                    or item.get("id") not in valid_ids
                    or item["id"] in seen_ids
                    or not isinstance(item.get("supported"), bool)
                ):
                    errors.append(f"{prefix}_support_{side}_{support_index}_invalid")
                    continue
                seen_ids.add(item["id"])
            if seen_ids != valid_ids:
                errors.append(f"{prefix}_support_{side}_id_set_mismatch")
    if seen_cases != set(expected):
        errors.append("case_id_set_mismatch")
    return errors


def score_expanded_judge_calibration_variant(
    output: dict[str, Any],
    *,
    expected: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    predictions = {
        str(row["case_id"]): row
        for row in output.get("cases") or []
        if isinstance(row, dict) and row.get("case_id") in expected
    }
    alignment_tp = alignment_fp = alignment_fn = 0
    relation_correct = relation_total = 0
    equivalent_tp = equivalent_fp = equivalent_tn = equivalent_fn = 0
    field_tp = field_fp = field_fn = 0
    support_tp = support_fp = support_tn = support_fn = 0
    by_base_case = {}
    by_shape: dict[str, Counter[str]] = defaultdict(Counter)
    for case_id, truth in expected.items():
        prediction = predictions.get(case_id) or {
            "pairs": [],
            "support_a": [],
            "support_b": [],
        }
        predicted_pairs = {
            (int(pair["a_id"]), int(pair["b_id"])): pair
            for pair in prediction.get("pairs") or []
            if isinstance(pair, dict) and "a_id" in pair and "b_id" in pair
        }
        expected_pairs = {
            (int(pair["a_id"]), int(pair["b_id"])): pair for pair in truth["pairs"]
        }
        expected_keys = set(expected_pairs)
        predicted_keys = set(predicted_pairs)
        alignment_tp += len(expected_keys & predicted_keys)
        alignment_fp += len(predicted_keys - expected_keys)
        alignment_fn += len(expected_keys - predicted_keys)
        by_shape[truth["shape"]].update(
            {
                "alignment_tp": len(expected_keys & predicted_keys),
                "alignment_fp": len(predicted_keys - expected_keys),
                "alignment_fn": len(expected_keys - predicted_keys),
                "cases": 1,
            }
        )
        normalized_pairs = {}
        for key, expected_pair in expected_pairs.items():
            predicted_pair = predicted_pairs.get(key) or {}
            expected_relation = expected_pair["relation"]
            predicted_relation = predicted_pair.get("relation")
            relation_total += 1
            relation_correct += int(predicted_relation == expected_relation)
            expected_equivalent = expected_relation == "equivalent"
            predicted_equivalent = predicted_relation == "equivalent"
            if expected_equivalent and predicted_equivalent:
                equivalent_tp += 1
            elif expected_equivalent:
                equivalent_fn += 1
            elif predicted_equivalent:
                equivalent_fp += 1
            elif predicted_relation is not None:
                equivalent_tn += 1
            expected_fields = set(expected_pair["mismatch_fields"])
            predicted_fields = set(predicted_pair.get("mismatch_fields") or [])
            field_tp += len(expected_fields & predicted_fields)
            field_fp += len(predicted_fields - expected_fields)
            field_fn += len(expected_fields - predicted_fields)
            normalized_pairs[expected_pair["pair_slot"]] = {
                "relation": predicted_relation,
                "mismatch_fields": sorted(predicted_fields),
            }
        normalized_support = {}
        for side in ("a", "b"):
            predicted_support = {
                int(item["id"]): bool(item["supported"])
                for item in prediction.get(f"support_{side}") or []
                if isinstance(item, dict) and "id" in item and "supported" in item
            }
            for event_id, support_truth in truth[f"support_{side}"].items():
                expected_supported = bool(support_truth["supported"])
                predicted_supported = predicted_support.get(int(event_id))
                if expected_supported and predicted_supported is True:
                    support_tp += 1
                elif expected_supported:
                    support_fn += 1
                elif predicted_supported is True:
                    support_fp += 1
                elif predicted_supported is False:
                    support_tn += 1
                normalized_support[support_truth["canonical_slot"]] = predicted_supported
        by_base_case[truth["base_case_id"]] = {
            "pairs": normalized_pairs,
            "support": normalized_support,
            "extra_pairs": len(predicted_keys - expected_keys),
        }

    alignment_precision = _ratio(alignment_tp, alignment_tp + alignment_fp)
    alignment_recall = _ratio(alignment_tp, alignment_tp + alignment_fn)
    alignment_f1 = (
        round(
            2 * alignment_precision * alignment_recall / (alignment_precision + alignment_recall),
            6,
        )
        if alignment_precision is not None
        and alignment_recall is not None
        and alignment_precision + alignment_recall
        else 0.0
    )
    field_precision = _ratio(field_tp, field_tp + field_fp)
    field_recall = _ratio(field_tp, field_tp + field_fn)
    field_f1 = (
        round(2 * field_precision * field_recall / (field_precision + field_recall), 6)
        if field_precision is not None and field_recall is not None and field_precision + field_recall
        else 1.0
        if field_tp == field_fp == field_fn == 0
        else 0.0
    )
    return {
        "case_count": len(expected),
        "missing_case_ids": len(expected) - len(predictions),
        "alignment_precision": alignment_precision,
        "alignment_recall": alignment_recall,
        "alignment_f1": alignment_f1,
        "relation_accuracy": _ratio(relation_correct, relation_total),
        "equivalent_sensitivity": _ratio(equivalent_tp, equivalent_tp + equivalent_fn),
        "equivalent_specificity": _ratio(equivalent_tn, equivalent_tn + equivalent_fp),
        "field_diagnostic_precision": field_precision,
        "field_diagnostic_recall": field_recall,
        "field_diagnostic_f1": field_f1,
        "support_sensitivity": _ratio(support_tp, support_tp + support_fn),
        "support_specificity": _ratio(support_tn, support_tn + support_fp),
        "by_shape": {shape: dict(values) for shape, values in sorted(by_shape.items())},
        "by_base_case": by_base_case,
    }


def combine_expanded_judge_calibration_scores(
    scores: dict[str, dict[str, Any]],
    *,
    gates: dict[str, Any],
) -> dict[str, Any]:
    if set(scores) != {"ab", "ba"}:
        return {"passed": False, "reason": "missing_calibration_variant"}
    variants = [scores["ab"], scores["ba"]]
    comparable_ids = sorted(set(scores["ab"]["by_base_case"]) & set(scores["ba"]["by_base_case"]))
    order_changes = sum(
        scores["ab"]["by_base_case"][case_id] != scores["ba"]["by_base_case"][case_id]
        for case_id in comparable_ids
    )
    metrics = {
        "alignment_f1": min(item["alignment_f1"] or 0 for item in variants),
        "relation_accuracy": min(item["relation_accuracy"] or 0 for item in variants),
        "equivalent_sensitivity": min(item["equivalent_sensitivity"] or 0 for item in variants),
        "equivalent_specificity": min(item["equivalent_specificity"] or 0 for item in variants),
        "field_diagnostic_f1": min(item["field_diagnostic_f1"] or 0 for item in variants),
        "support_sensitivity": min(item["support_sensitivity"] or 0 for item in variants),
        "support_specificity": min(item["support_specificity"] or 0 for item in variants),
        "order_bias": _ratio(order_changes, len(comparable_ids)) or 0.0,
    }
    checks = {
        "complete_outputs": all(item["missing_case_ids"] == 0 for item in variants),
        "minimum_cases": all(item["case_count"] >= 60 for item in variants),
        "alignment_f1": metrics["alignment_f1"] >= float(gates["alignment_f1_min"]),
        "relation_accuracy": metrics["relation_accuracy"] >= float(gates["relation_accuracy_min"]),
        "equivalent_sensitivity": metrics["equivalent_sensitivity"]
        >= float(gates["equivalent_sensitivity_min"]),
        "equivalent_specificity": metrics["equivalent_specificity"]
        >= float(gates["equivalent_specificity_min"]),
        "field_diagnostic_f1": metrics["field_diagnostic_f1"]
        >= float(gates["field_diagnostic_f1_min"]),
        "support_sensitivity": metrics["support_sensitivity"]
        >= float(gates["support_sensitivity_min"]),
        "support_specificity": metrics["support_specificity"]
        >= float(gates["support_specificity_min"]),
        "order_bias": metrics["order_bias"] <= float(gates["order_bias_max"]),
    }
    return {
        "passed": all(checks.values()),
        "metrics": metrics,
        "checks": checks,
        "order_changes": order_changes,
        "comparable_cases": len(comparable_ids),
    }


def expanded_judge_calibration_output_schema(
    case_ids: list[str],
    mismatch_fields: list[str],
) -> dict[str, Any]:
    pair = {
        "type": "object",
        "additionalProperties": False,
        "required": ["a_id", "b_id", "relation", "mismatch_fields"],
        "properties": {
            "a_id": {"type": "integer", "minimum": 0},
            "b_id": {"type": "integer", "minimum": 0},
            "relation": {"type": "string", "enum": ["equivalent", "partial", "non_equivalent"]},
            "mismatch_fields": {
                "type": "array",
                "items": {"type": "string", "enum": mismatch_fields},
            },
        },
    }
    support = {
        "type": "object",
        "additionalProperties": False,
        "required": ["id", "supported"],
        "properties": {
            "id": {"type": "integer", "minimum": 0},
            "supported": {"type": "boolean"},
        },
    }
    case = {
        "type": "object",
        "additionalProperties": False,
        "required": ["case_id", "pairs", "support_a", "support_b"],
        "properties": {
            "case_id": {"type": "string", "enum": case_ids},
            "pairs": {"type": "array", "maxItems": 2, "items": pair},
            "support_a": {"type": "array", "maxItems": 2, "items": support},
            "support_b": {"type": "array", "maxItems": 2, "items": support},
        },
    }
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "additionalProperties": False,
        "required": ["cases"],
        "properties": {
            "cases": {
                "type": "array",
                "minItems": len(case_ids),
                "maxItems": len(case_ids),
                "items": case,
            }
        },
    }


def _expanded_judge_calibration_prompt(
    cases: list[dict[str, Any]],
    mismatch_fields: list[str],
) -> str:
    return "\n\n".join(
        [
            "You are being calibrated as a blinded set-level semantic extraction judge. Each case contains two "
            "anonymous event sets from one source excerpt. Align related events one-to-one even when they are partial "
            "or non-equivalent; omit wholly unrelated one-sided events. Equivalent requires agreement on every material "
            "field, allowing only harmless wording or aliases. Partial means a shared core event with a material merge, "
            "split, omission, or expansion. Non-equivalent includes wrong actor, attribution, stance, certainty, time, "
            "metric, negation, target, mechanism, event type, unsupported inference, or evidence. Distinct analytical "
            "lenses may legitimately share the same exact evidence.",
            "Independently mark source support for every event in both sets. Support requires an exact source evidence "
            "span and a claim that does not exceed that source. Return every opaque case_id exactly once, every event ID "
            "exactly once in its support array, and each aligned ID at most once. Use an empty mismatch_fields array for "
            f"equivalent pairs. Allowed diagnostics: {', '.join(mismatch_fields)}. Do not infer system identity or favor "
            "either side. Judge meaning directly; do not use keyword, regex, token-overlap, or phrase rules.",
            "# Blinded cases\n" + json.dumps({"cases": cases}, ensure_ascii=True, separators=(",", ":")),
        ]
    ) + "\n"


def run_expanded_judge_calibration(
    *,
    output_dir: str | Path,
    model: str = "gpt-5.5",
    reasoning_effort: str = "high",
    timeout_seconds: int = 900,
    fixture_path: str | Path = DEFAULT_EXPANDED_JUDGE_FIXTURE_PATH,
    evaluator_spec_path: str | Path = DEFAULT_EVALUATOR_SPEC_PATH,
    rerun: bool = False,
) -> dict[str, Any]:
    fixture = load_expanded_judge_calibration_fixture(fixture_path)
    evaluator = load_windowed_evaluator_spec(evaluator_spec_path)
    variants = build_expanded_judge_calibration_variants(fixture)
    root = Path(output_dir).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    scratch_dir = Path("/tmp/pif-efficiency-codex-smoke")
    scratch_dir.mkdir(parents=True, exist_ok=True)
    prepared = []
    for variant_name, variant in variants.items():
        case_ids = [item["case_id"] for item in variant["cases"]]
        schema_path = root / f"schema-{variant_name}.json"
        prompt_path = root / f"prompt-{variant_name}.private.md"
        output_path = root / f"output-{variant_name}.private.json"
        log_path = root / f"run-{variant_name}.private.jsonl"
        write_text_atomic(
            schema_path,
            json.dumps(
                expanded_judge_calibration_output_schema(case_ids, fixture["mismatch_fields"]),
                ensure_ascii=True,
                indent=2,
                sort_keys=True,
            )
            + "\n",
        )
        write_text_atomic(
            prompt_path,
            _expanded_judge_calibration_prompt(variant["cases"], fixture["mismatch_fields"]),
        )
        prepared.append((variant_name, variant, schema_path, prompt_path, output_path, log_path))

    def run_one(item: tuple[Any, ...]) -> dict[str, Any]:
        variant_name, variant, schema_path, prompt_path, output_path, log_path = item
        executed = bool(rerun or not _json_file_ok(output_path))
        exit_code = 0
        timed_out = False
        elapsed_seconds = 0.0
        if executed:
            output_path.write_text("", encoding="utf-8")
            command = [
                "codex",
                "exec",
                "-m",
                model,
                "-c",
                f'model_reasoning_effort="{reasoning_effort}"',
                "-C",
                str(scratch_dir),
                "--skip-git-repo-check",
                "--ignore-rules",
                "--ephemeral",
                "--sandbox",
                "read-only",
                "--output-schema",
                str(schema_path),
                "--output-last-message",
                str(output_path),
                "--json",
                "-",
            ]
            exit_code, timed_out, elapsed_seconds = _run_codex_smoke_command(
                command,
                prompt_path=prompt_path,
                log_path=log_path,
                timeout_seconds=timeout_seconds,
            )
        validation_errors = []
        score = None
        if _json_file_ok(output_path):
            payload = json.loads(output_path.read_text(encoding="utf-8"))
            validation_errors = validate_expanded_judge_calibration_output(
                payload,
                expected=variant["expected"],
                mismatch_fields=fixture["mismatch_fields"],
            )
            if not validation_errors:
                score = score_expanded_judge_calibration_variant(
                    payload,
                    expected=variant["expected"],
                )
        status_ok = bool(
            _json_file_ok(output_path)
            and not validation_errors
            and (not executed or (exit_code == 0 and not timed_out))
        )
        return {
            "variant": variant_name,
            "executed": executed,
            "status_ok": status_ok,
            "exit_code": exit_code,
            "timed_out": timed_out,
            "elapsed_seconds": elapsed_seconds,
            "validation_errors": validation_errors,
            "usage": _codex_usage_from_jsonl(log_path),
            "score": score,
        }

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = [
            future.result()
            for future in as_completed([executor.submit(run_one, item) for item in prepared])
        ]
    results.sort(key=lambda item: item["variant"])
    score_map = {item["variant"]: item["score"] for item in results if item.get("score")}
    combined = combine_expanded_judge_calibration_scores(
        score_map,
        gates=evaluator["judge_calibration"]["gates"],
    )
    report = {
        "schema_version": EXPANDED_JUDGE_CALIBRATION_SCHEMA_VERSION,
        "model": model,
        "reasoning_effort": reasoning_effort,
        "fixture_path": str(Path(fixture_path).expanduser().resolve()),
        "fixture_sha256": _sha256_file(fixture_path),
        "fixture_case_count": len(fixture["expanded_cases"]),
        "evaluator_spec_sha256": _sha256_file(evaluator_spec_path),
        "calibrated": bool(combined.get("passed")),
        "combined": combined,
        "usage": _sum_usage_dicts([item.get("usage") for item in results]),
        "accounting_complete": all(item.get("usage") is not None for item in results),
        "results": results,
        "privacy": "synthetic_multidomain_fixture_no_transcript_text",
    }
    report_path = root / "report.json"
    write_text_atomic(report_path, json.dumps(report, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
    return {**report, "report_path": str(report_path)}


def _density_stratum(event_count: int, strata: dict[str, dict[str, Any]]) -> str | None:
    for name, bounds in strata.items():
        minimum = int(bounds.get("golden_event_min", 0))
        maximum = bounds.get("golden_event_max")
        if event_count >= minimum and (maximum is None or event_count <= int(maximum)):
            return name
    return None


def _source_balanced_sample(
    rows: list[dict[str, Any]],
    *,
    count: int,
    seed: str,
) -> list[dict[str, Any]]:
    by_source: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_source[str(row["source_id"])].append(row)
    for source_id, candidates in by_source.items():
        candidates.sort(key=lambda row: sha256_text(f"{seed}|row|{source_id}|{row['segment_id']}"))
    source_order = sorted(by_source, key=lambda source_id: sha256_text(f"{seed}|source|{source_id}"))
    selected = []
    while len(selected) < count:
        added = False
        for source_id in source_order:
            if by_source[source_id]:
                selected.append(by_source[source_id].pop(0))
                added = True
                if len(selected) == count:
                    break
        if not added:
            break
    return selected


def _raise_source_coverage(
    selected_by_stratum: dict[str, list[dict[str, Any]]],
    candidates_by_stratum: dict[str, list[dict[str, Any]]],
    *,
    minimum_sources: int,
    seed: str,
) -> None:
    while True:
        selected = [row for rows in selected_by_stratum.values() for row in rows]
        source_counts = Counter(str(row["source_id"]) for row in selected)
        if len(source_counts) >= minimum_sources:
            return
        selected_ids = {str(row["segment_id"]) for row in selected}
        unseen_candidates = []
        for stratum, rows in candidates_by_stratum.items():
            for row in rows:
                if (
                    str(row["segment_id"]) not in selected_ids
                    and str(row["source_id"]) not in source_counts
                ):
                    unseen_candidates.append((stratum, row))
        unseen_candidates.sort(
            key=lambda item: sha256_text(
                f"{seed}|coverage|{item[0]}|{item[1]['source_id']}|{item[1]['segment_id']}"
            )
        )
        replaced = False
        for stratum, candidate in unseen_candidates:
            replaceable = [
                row
                for row in selected_by_stratum[stratum]
                if source_counts[str(row["source_id"])] > 1
            ]
            if not replaceable:
                continue
            replaceable.sort(
                key=lambda row: sha256_text(
                    f"{seed}|replace|{stratum}|{row['source_id']}|{row['segment_id']}"
                )
            )
            selected_by_stratum[stratum].remove(replaceable[0])
            selected_by_stratum[stratum].append(candidate)
            replaced = True
            break
        if not replaced:
            raise ValueError(f"could not reach required source coverage of {minimum_sources}")


def export_paired_evaluation_manifest(
    conn,
    *,
    output_path: str | Path,
    exclude_roots: list[str | Path] | None = None,
    acceptance_spec_path: str | Path = DEFAULT_ACCEPTANCE_SPEC_PATH,
    seed: str = "windowed-paired-holdout-v1",
    exclude_prior_episodes: bool = False,
) -> dict[str, Any]:
    acceptance = load_windowed_acceptance_spec(acceptance_spec_path)
    paired_spec = acceptance["paired_run"]
    strata = paired_spec["density_strata"]
    excluded_segments: set[str] = set()
    excluded_episodes: set[str] = set()
    scanned_manifests = 0
    for root in exclude_roots or []:
        root_path = Path(root).expanduser().resolve()
        paths = [root_path] if root_path.is_file() else sorted(root_path.rglob("*manifest*.json"))
        for path in paths:
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            segment_ids, episode_ids = _collect_manifest_holdout_ids(payload)
            excluded_segments.update(segment_ids)
            excluded_episodes.update(episode_ids)
            scanned_manifests += 1
    rows = conn.execute(
        """
        WITH ranked AS (
          SELECT l.id AS label_id,
                 l.segment_id,
                 l.output_json,
                 l.created_at,
                 sg.episode_id,
                 sg.source_id,
                 sg.segment_index,
                 so.name AS source_name,
                 ROW_NUMBER() OVER (
                   PARTITION BY l.segment_id
                   ORDER BY l.created_at DESC, l.id DESC
                 ) AS rank
          FROM labels l
          JOIN segments sg ON sg.id = l.segment_id
          JOIN sources so ON so.id = sg.source_id
          WHERE l.label_pack = 'ai_discourse_v3_1'
            AND l.model = 'gpt-5.5'
            AND l.status IN ('ready', 'completed')
        )
        SELECT * FROM ranked WHERE rank = 1
        """
    ).fetchall()
    from .worker import completed_episode_context_for_segment

    candidates_by_stratum: dict[str, list[dict[str, Any]]] = {name: [] for name in strata}
    missing_context = 0
    invalid_labels = 0
    for row in rows:
        if row["segment_id"] in excluded_segments:
            continue
        if exclude_prior_episodes and row["episode_id"] in excluded_episodes:
            continue
        try:
            label = json.loads(row["output_json"])
        except (TypeError, json.JSONDecodeError):
            invalid_labels += 1
            continue
        events = label.get("discourse_events") or []
        stratum = _density_stratum(len(events), strata)
        if stratum is None:
            continue
        context_run = completed_episode_context_for_segment(
            conn,
            row["segment_id"],
            label_pack="ai_discourse_v3_1",
            model="gpt-5.5",
        )
        context_path = Path(context_run["context_artifact_path"]).expanduser() if context_run else None
        if context_path is None or not context_path.exists():
            missing_context += 1
            continue
        family_counts = Counter(
            str(event.get("event_type") or "unknown")
            for event in events
            if isinstance(event, dict)
        )
        candidates_by_stratum[stratum].append(
            {
                "segment_id": row["segment_id"],
                "episode_id": row["episode_id"],
                "source_id": row["source_id"],
                "source_name": row["source_name"],
                "segment_index": int(row["segment_index"] or 0),
                "golden_label_id": row["label_id"],
                "golden_status": str(label.get("extraction_status") or "unknown"),
                "golden_event_count": len(events),
                "event_family_counts": dict(sorted(family_counts.items())),
                "density_stratum": stratum,
            }
        )
    selected_by_stratum = {}
    for stratum, bounds in strata.items():
        required = int(bounds["minimum_segments"])
        selected_by_stratum[stratum] = _source_balanced_sample(
            candidates_by_stratum[stratum],
            count=required,
            seed=f"{seed}|{stratum}",
        )
        if len(selected_by_stratum[stratum]) < required:
            raise ValueError(
                f"density stratum {stratum} has only {len(selected_by_stratum[stratum])} eligible segments; "
                f"requires {required}"
            )
    _raise_source_coverage(
        selected_by_stratum,
        candidates_by_stratum,
        minimum_sources=int(paired_spec["minimum_sources"]),
        seed=seed,
    )
    selected = [row for stratum in strata for row in selected_by_stratum[stratum]]
    selected.sort(key=lambda row: sha256_text(f"{seed}|evaluation-order|{row['segment_id']}"))
    if len(selected) < int(paired_spec["minimum_segments"]):
        raise ValueError("paired manifest does not meet minimum segment count")
    source_counts = Counter(str(row["source_id"]) for row in selected)
    if paired_spec.get("multiple_segments_per_source_required") and max(source_counts.values(), default=0) < 2:
        raise ValueError("paired manifest must include multiple segments from at least one source")
    chunks = []
    for index, row in enumerate(selected):
        chunks.append(
            {
                "chunk_id": row["segment_id"],
                "episode_id": row["episode_id"],
                "source_id": row["source_id"],
                "source_name": row["source_name"],
                "segment_ids": [row["segment_id"]],
                "segment_count": 1,
                "segment_index": row["segment_index"],
                "label_count": 1,
                "golden_label_id": row["golden_label_id"],
                "golden_status": row["golden_status"],
                "expected_status_counts": {row["golden_status"]: 1},
                "expected_coded_segments": int(row["golden_status"] == "coded"),
                "expected_discourse_events": row["golden_event_count"],
                "density_stratum": row["density_stratum"],
                "event_family_counts": row["event_family_counts"],
                "evaluation_order": index,
            }
        )
    output_file = Path(output_path).expanduser().resolve()
    output_file.parent.mkdir(parents=True, exist_ok=True)
    acceptance_text = Path(acceptance_spec_path).expanduser().resolve().read_text(encoding="utf-8")
    guideline_text = DEFAULT_WINDOWED_GUIDELINES_PATH.read_text(encoding="utf-8")
    manifest = {
        "schema_version": PAIRED_MANIFEST_SCHEMA_VERSION,
        "privacy": "private_analysis_only_no_transcript_text",
        "selection_status": "frozen_fresh_segment_holdout",
        "seed": seed,
        "acceptance_spec_path": str(Path(acceptance_spec_path).expanduser().resolve()),
        "acceptance_spec_sha256": sha256_text(acceptance_text),
        "candidate_guideline_sha256": sha256_text(guideline_text),
        "exclude_prior_episodes": exclude_prior_episodes,
        "scanned_prior_manifests": scanned_manifests,
        "excluded_prior_segment_count": len(excluded_segments),
        "excluded_prior_episode_count": len(excluded_episodes),
        "chunks": chunks,
    }
    write_text_atomic(output_file, json.dumps(manifest, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
    stratum_counts = Counter(row["density_stratum"] for row in selected)
    return {
        "ok": True,
        "path": str(output_file),
        "segments": len(selected),
        "sources": len(source_counts),
        "episodes": len({str(row["episode_id"]) for row in selected}),
        "sources_with_multiple_segments": sum(count >= 2 for count in source_counts.values()),
        "density_counts": dict(sorted(stratum_counts.items())),
        "candidate_pool_counts": {
            name: len(candidates_by_stratum[name]) for name in strata
        },
        "missing_episode_context": missing_context,
        "invalid_labels": invalid_labels,
        "scanned_prior_manifests": scanned_manifests,
        "excluded_prior_segments": len(excluded_segments),
        "excluded_prior_episodes": len(excluded_episodes),
        "privacy": "sanitized_no_prompt_or_transcript_text",
    }


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = json.loads(json.dumps(base, ensure_ascii=True))
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = json.loads(json.dumps(value, ensure_ascii=True))
    return merged


def build_judge_calibration_variants(
    fixture: dict[str, Any],
    *,
    seed: str = "judge-calibration-order-v1",
) -> dict[str, dict[str, Any]]:
    base_event = fixture["base_event"]
    variants = {}
    for variant_name, reverse in (("ab", False), ("ba", True)):
        rows = []
        expected = {}
        ordered_cases = sorted(
            fixture["cases"],
            key=lambda case: sha256_text(f"{seed}|{variant_name}|{case['case_id']}"),
        )
        for case in ordered_cases:
            opaque_id = "jc_" + sha256_text(f"{seed}|{variant_name}|{case['case_id']}")[:16]
            event_a = _deep_merge(base_event, case.get("a") or {})
            event_b = _deep_merge(base_event, case.get("b") or {})
            source_excerpt = case.get("source_excerpt")
            if not isinstance(source_excerpt, str) or not source_excerpt:
                evidence_values = []
                for event in (event_a, event_b):
                    evidence = event.get("evidence")
                    if isinstance(evidence, str) and evidence and evidence not in evidence_values:
                        evidence_values.append(evidence)
                source_excerpt = " ".join(evidence_values)
            if reverse:
                event_a, event_b = event_b, event_a
            rows.append(
                {
                    "case_id": opaque_id,
                    "source_excerpt": source_excerpt,
                    "event_a": event_a,
                    "event_b": event_b,
                }
            )
            expected[opaque_id] = {
                "base_case_id": case["case_id"],
                "expected_relation": case["expected_relation"],
                "expected_mismatch_fields": sorted(case.get("expected_mismatch_fields") or []),
            }
        variants[variant_name] = {"cases": rows, "expected": expected}
    return variants


def judge_calibration_output_schema(case_ids: list[str], mismatch_fields: list[str]) -> dict[str, Any]:
    item = {
        "type": "object",
        "additionalProperties": False,
        "required": ["case_id", "relation", "mismatch_fields"],
        "properties": {
            "case_id": {"type": "string", "enum": case_ids},
            "relation": {
                "type": "string",
                "enum": ["equivalent", "partial", "non_equivalent"],
            },
            "mismatch_fields": {
                "type": "array",
                "items": {"type": "string", "enum": mismatch_fields},
            },
        },
    }
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "additionalProperties": False,
        "required": ["cases"],
        "properties": {
            "cases": {
                "type": "array",
                "minItems": len(case_ids),
                "maxItems": len(case_ids),
                "items": item,
            }
        },
    }


def validate_judge_calibration_output(
    output: Any,
    *,
    case_ids: list[str],
    mismatch_fields: list[str],
) -> list[str]:
    errors: list[str] = []
    if not isinstance(output, dict) or set(output) != {"cases"}:
        return ["invalid_root"]
    rows = output.get("cases")
    if not isinstance(rows, list):
        return ["cases_not_array"]
    if len(rows) != len(case_ids):
        errors.append("case_count_mismatch")
    allowed_case_ids = set(case_ids)
    allowed_relations = {"equivalent", "partial", "non_equivalent"}
    allowed_fields = set(mismatch_fields)
    seen: set[str] = set()
    for index, row in enumerate(rows):
        prefix = f"case_{index}"
        if not isinstance(row, dict) or set(row) != {"case_id", "relation", "mismatch_fields"}:
            errors.append(f"{prefix}_invalid_shape")
            continue
        case_id = row.get("case_id")
        if case_id not in allowed_case_ids:
            errors.append(f"{prefix}_invalid_id")
        elif case_id in seen:
            errors.append(f"{prefix}_duplicate_id")
        else:
            seen.add(case_id)
        if row.get("relation") not in allowed_relations:
            errors.append(f"{prefix}_invalid_relation")
        fields = row.get("mismatch_fields")
        if not isinstance(fields, list):
            errors.append(f"{prefix}_fields_not_array")
        elif (
            any(not isinstance(field, str) or field not in allowed_fields for field in fields)
            or len(fields) != len(set(fields))
        ):
            errors.append(f"{prefix}_invalid_fields")
    if seen != allowed_case_ids:
        errors.append("case_id_set_mismatch")
    return errors


def _ratio(numerator: int | float, denominator: int | float) -> float | None:
    if not denominator:
        return None
    return round(float(numerator) / float(denominator), 6)


def score_judge_calibration_variant(
    output: dict[str, Any],
    *,
    expected: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    predictions = {}
    duplicate_case_ids = 0
    invalid_case_ids = 0
    for row in output.get("cases") or []:
        if not isinstance(row, dict) or row.get("case_id") not in expected:
            invalid_case_ids += 1
            continue
        case_id = row["case_id"]
        if case_id in predictions:
            duplicate_case_ids += 1
            continue
        predictions[case_id] = {
            "relation": row.get("relation"),
            "mismatch_fields": sorted(set(row.get("mismatch_fields") or [])),
        }
    relation_correct = 0
    equivalent_tp = 0
    equivalent_fn = 0
    equivalent_tn = 0
    equivalent_fp = 0
    field_tp = 0
    field_fp = 0
    field_fn = 0
    by_base_case = {}
    for case_id, truth in expected.items():
        prediction = predictions.get(case_id) or {"relation": None, "mismatch_fields": []}
        expected_relation = truth["expected_relation"]
        predicted_relation = prediction["relation"]
        relation_correct += int(predicted_relation == expected_relation)
        expected_positive = expected_relation == "equivalent"
        predicted_positive = predicted_relation == "equivalent"
        if expected_positive and predicted_positive:
            equivalent_tp += 1
        elif expected_positive:
            equivalent_fn += 1
        elif predicted_positive:
            equivalent_fp += 1
        else:
            equivalent_tn += 1
        expected_fields = set(truth["expected_mismatch_fields"])
        predicted_fields = set(prediction["mismatch_fields"])
        field_tp += len(expected_fields & predicted_fields)
        field_fp += len(predicted_fields - expected_fields)
        field_fn += len(expected_fields - predicted_fields)
        by_base_case[truth["base_case_id"]] = {
            "relation": predicted_relation,
            "mismatch_fields": sorted(predicted_fields),
        }
    field_precision = _ratio(field_tp, field_tp + field_fp)
    field_recall = _ratio(field_tp, field_tp + field_fn)
    field_f1 = (
        round(2 * field_precision * field_recall / (field_precision + field_recall), 6)
        if field_precision is not None and field_recall is not None and field_precision + field_recall
        else 0.0
    )
    return {
        "case_count": len(expected),
        "missing_case_ids": len(expected) - len(predictions),
        "invalid_case_ids": invalid_case_ids,
        "duplicate_case_ids": duplicate_case_ids,
        "relation_accuracy": _ratio(relation_correct, len(expected)),
        "equivalent_sensitivity": _ratio(equivalent_tp, equivalent_tp + equivalent_fn),
        "equivalent_specificity": _ratio(equivalent_tn, equivalent_tn + equivalent_fp),
        "field_diagnostic_precision": field_precision,
        "field_diagnostic_recall": field_recall,
        "field_diagnostic_f1": field_f1,
        "by_base_case": by_base_case,
    }


def combine_judge_calibration_scores(
    scores: dict[str, dict[str, Any]],
    *,
    gates: dict[str, Any],
) -> dict[str, Any]:
    variants = list(scores.values())
    base_case_ids = sorted(set.intersection(*(set(item["by_base_case"]) for item in variants)))
    order_relation_changes = sum(
        1
        for case_id in base_case_ids
        if len({item["by_base_case"][case_id]["relation"] for item in variants}) > 1
    )
    order_field_changes = sum(
        1
        for case_id in base_case_ids
        if len(
            {
                tuple(item["by_base_case"][case_id]["mismatch_fields"])
                for item in variants
            }
        )
        > 1
    )
    metrics = {
        "relation_accuracy": min(item["relation_accuracy"] or 0 for item in variants),
        "equivalent_sensitivity": min(item["equivalent_sensitivity"] or 0 for item in variants),
        "equivalent_specificity": min(item["equivalent_specificity"] or 0 for item in variants),
        "field_diagnostic_f1": min(item["field_diagnostic_f1"] or 0 for item in variants),
        "order_bias": _ratio(order_relation_changes, len(base_case_ids)) or 0.0,
        "order_field_bias": _ratio(order_field_changes, len(base_case_ids)) or 0.0,
    }
    checks = {
        "complete_outputs": all(
            item["missing_case_ids"] == 0
            and item["invalid_case_ids"] == 0
            and item["duplicate_case_ids"] == 0
            for item in variants
        ),
        "relation_accuracy": metrics["relation_accuracy"] >= gates["relation_accuracy_min"],
        "equivalent_sensitivity": metrics["equivalent_sensitivity"] >= gates["equivalent_sensitivity_min"],
        "equivalent_specificity": metrics["equivalent_specificity"] >= gates["equivalent_specificity_min"],
        "field_diagnostic_f1": metrics["field_diagnostic_f1"] >= gates["field_diagnostic_f1_min"],
        "order_bias": metrics["order_bias"] <= gates["order_bias_max"],
        "order_field_bias": metrics["order_field_bias"]
        <= gates.get("field_order_bias_max", gates["order_bias_max"]),
    }
    return {
        "passed": all(checks.values()),
        "metrics": metrics,
        "checks": checks,
        "order_relation_changes": order_relation_changes,
        "order_field_changes": order_field_changes,
        "comparable_cases": len(base_case_ids),
    }


def _judge_calibration_prompt(cases: list[dict[str, Any]], mismatch_fields: list[str]) -> str:
    return "\n\n".join(
        [
            "You are being calibrated as a blinded semantic equivalence judge. Each case contains two anonymous "
            "topic-general extraction events. Inspect every supplied field. Return equivalent only when they express "
            "the same complete research event and differ only by harmless wording or aliases. Return partial when "
            "they share a core event but one merges, splits, omits, or expands a material part. Return non_equivalent "
            "for a wrong actor, speaker, reported actor, attribution, stance, certainty, time, metric, negation, target, "
            "mechanism, event type, unsupported inference, or evidence. Use source_excerpt to verify that evidence is "
            "an exact source span and that claims are supported. Events sharing evidence can be legitimately "
            "distinct analytical lenses. Do not infer system identity or prefer A or B. Do not use keyword, regex, "
            "token-overlap, or phrase-match rules; judge meaning.",
            "Return every opaque case_id exactly once. mismatch_fields must contain only materially different fields "
            f"from this controlled list: {', '.join(mismatch_fields)}. Use an empty list for equivalent cases.",
            "# Blinded cases\n" + json.dumps({"cases": cases}, ensure_ascii=True, separators=(",", ":")),
        ]
    ) + "\n"


def run_judge_calibration(
    *,
    output_dir: str | Path,
    model: str,
    reasoning_effort: str = "low",
    timeout_seconds: int = 300,
    fixture_path: str | Path = DEFAULT_JUDGE_FIXTURE_PATH,
    acceptance_spec_path: str | Path = DEFAULT_ACCEPTANCE_SPEC_PATH,
    rerun: bool = False,
) -> dict[str, Any]:
    fixture = load_judge_calibration_fixture(fixture_path)
    acceptance = load_windowed_acceptance_spec(acceptance_spec_path)
    variants = build_judge_calibration_variants(fixture)
    root = Path(output_dir).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    scratch_dir = Path("/tmp/pif-efficiency-codex-smoke")
    scratch_dir.mkdir(parents=True, exist_ok=True)
    prepared = []
    for variant_name, variant in variants.items():
        case_ids = [item["case_id"] for item in variant["cases"]]
        schema_path = root / f"schema-{variant_name}.json"
        prompt_path = root / f"prompt-{variant_name}.md"
        output_path = root / f"output-{variant_name}.private.json"
        log_path = root / f"run-{variant_name}.private.jsonl"
        write_text_atomic(
            schema_path,
            json.dumps(
                judge_calibration_output_schema(case_ids, fixture["mismatch_fields"]),
                ensure_ascii=True,
                indent=2,
                sort_keys=True,
            )
            + "\n",
        )
        write_text_atomic(prompt_path, _judge_calibration_prompt(variant["cases"], fixture["mismatch_fields"]))
        prepared.append((variant_name, variant, schema_path, prompt_path, output_path, log_path))

    def run_one(item):
        variant_name, variant, schema_path, prompt_path, output_path, log_path = item
        exit_code = 0
        timed_out = False
        elapsed_seconds = 0.0
        executed = False
        if rerun or not _json_file_ok(output_path):
            executed = True
            command = [
                "codex",
                "exec",
                "-m",
                model,
                "-c",
                f'model_reasoning_effort="{reasoning_effort}"',
                "-C",
                str(scratch_dir),
                "--skip-git-repo-check",
                "--ignore-rules",
                "--ephemeral",
                "--sandbox",
                "read-only",
                "--output-schema",
                str(schema_path),
                "--output-last-message",
                str(output_path),
                "--json",
                "-",
            ]
            exit_code, timed_out, elapsed_seconds = _run_codex_smoke_command(
                command,
                prompt_path=prompt_path,
                log_path=log_path,
                timeout_seconds=timeout_seconds,
            )
        json_ok = _json_file_ok(output_path)
        score = None
        validation_errors = []
        if json_ok:
            payload = json.loads(output_path.read_text(encoding="utf-8"))
            validation_errors = validate_judge_calibration_output(
                payload,
                case_ids=[item["case_id"] for item in variant["cases"]],
                mismatch_fields=fixture["mismatch_fields"],
            )
        status_ok = bool(
            json_ok
            and not validation_errors
            and (not executed or (exit_code == 0 and not timed_out))
        )
        if status_ok:
            score = score_judge_calibration_variant(
                payload,
                expected=variant["expected"],
            )
        return {
            "variant": variant_name,
            "executed": executed,
            "exit_code": exit_code,
            "timed_out": timed_out,
            "elapsed_seconds": elapsed_seconds,
            "json_ok": json_ok,
            "validation_errors": validation_errors,
            "status_ok": status_ok,
            "usage": _codex_usage_from_jsonl(log_path),
            "score": score,
        }

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = [future.result() for future in as_completed([executor.submit(run_one, item) for item in prepared])]
    results.sort(key=lambda item: item["variant"])
    score_map = {item["variant"]: item["score"] for item in results if item["score"]}
    combined = (
        combine_judge_calibration_scores(
            score_map,
            gates=acceptance["judge_calibration_gates"],
        )
        if len(score_map) == 2
        else {"passed": False, "reason": "missing_calibration_variant"}
    )
    report = {
        "schema_version": JUDGE_CALIBRATION_SCHEMA_VERSION,
        "model": model,
        "reasoning_effort": reasoning_effort,
        "fixture_sha256": sha256_text(Path(fixture_path).expanduser().resolve().read_text(encoding="utf-8")),
        "acceptance_spec_sha256": sha256_text(
            Path(acceptance_spec_path).expanduser().resolve().read_text(encoding="utf-8")
        ),
        "calibrated": bool(combined.get("passed")),
        "combined": combined,
        "results": results,
        "privacy": "synthetic_fixture_no_transcript_text",
    }
    report_path = root / "report.json"
    write_text_atomic(report_path, json.dumps(report, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
    return {**report, "report_path": str(report_path)}


def _sum_usage(attempts: list[dict[str, Any]]) -> dict[str, int]:
    fields = (
        "input_tokens",
        "cached_input_tokens",
        "output_tokens",
        "reasoning_output_tokens",
        "total_tokens",
    )
    return {
        field: sum(int((attempt.get("usage") or {}).get(field) or 0) for attempt in attempts)
        for field in fields
    }


def run_paired_baseline(
    conn,
    *,
    manifest_path: str | Path,
    limit: int = 0,
    concurrency: int = 4,
    model: str = "gpt-5.5",
    reasoning_effort: str = "high",
    timeout_seconds: int = 900,
    retry_count: int = 1,
    acceptance_spec_path: str | Path = DEFAULT_ACCEPTANCE_SPEC_PATH,
    rerun: bool = False,
) -> dict[str, Any]:
    if limit < 0:
        raise ValueError("limit must be non-negative")
    if concurrency < 1:
        raise ValueError("concurrency must be at least 1")
    if retry_count < 0:
        raise ValueError("retry count must be non-negative")
    acceptance = load_windowed_acceptance_spec(acceptance_spec_path)
    baseline_spec = acceptance["baseline"]
    if model != baseline_spec["model"] or reasoning_effort != baseline_spec["reasoning_effort"]:
        raise ValueError("paired baseline model and reasoning must match the frozen acceptance specification")
    if concurrency != int(acceptance["paired_run"]["concurrency"]):
        raise ValueError("paired baseline concurrency must match the frozen acceptance specification")
    if retry_count != 1:
        raise ValueError("paired baseline retry count must remain one")
    started_at = now_iso()
    wall_started = time.monotonic()
    manifest_file = Path(manifest_path).expanduser().resolve()
    manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
    if manifest.get("schema_version") != PAIRED_MANIFEST_SCHEMA_VERSION:
        raise ValueError("paired baseline requires a frozen paired evaluation manifest")
    acceptance_sha256 = sha256_text(
        Path(acceptance_spec_path).expanduser().resolve().read_text(encoding="utf-8")
    )
    if manifest.get("acceptance_spec_sha256") != acceptance_sha256:
        raise ValueError("paired manifest acceptance specification has drifted")
    root = manifest_file.parent / "paired_baseline"
    prompt_dir = root / "prompts"
    output_dir = root / "outputs"
    log_dir = root / "logs"
    for path in (root, prompt_dir, output_dir, log_dir):
        path.mkdir(parents=True, exist_ok=True)
    from .labels import (
        _validate_pack_logic,
        _validate_schema,
        load_label_pack,
        render_prompt,
        repair_label_output_for_submission,
        validate_label_grounding,
    )
    from .worker import (
        adjacent_segment_context_for_segment,
        completed_episode_context_for_segment,
        segment_for_job,
    )

    pack = load_label_pack("ai_discourse_v3_1")
    schema_path = root / "schema.json"
    write_text_atomic(
        schema_path,
        json.dumps(pack.schema, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
    )
    chunks = list(manifest.get("chunks") or [])
    if limit:
        chunks = chunks[:limit]
    prepared = []
    preparation_failures = []
    for chunk in chunks:
        segment_id = str((chunk.get("segment_ids") or [chunk.get("chunk_id")])[0])
        output_path = output_dir / f"{segment_id}.private.json"
        if not rerun and _json_file_ok(output_path):
            continue
        try:
            segment_context = segment_for_job(conn, {"target_id": segment_id})
            context_run = completed_episode_context_for_segment(
                conn,
                segment_id,
                label_pack="ai_discourse_v3_1",
                model="gpt-5.5",
            )
            if not context_run:
                raise ValueError("missing completed episode context")
            artifact_path = Path(context_run["context_artifact_path"]).expanduser()
            if not artifact_path.exists():
                raise ValueError("missing episode context artifact")
            context = segment_context["context"]
            context["episode_context_artifact"] = json.loads(artifact_path.read_text(encoding="utf-8"))
            context["episode_context_run_id"] = context_run["id"]
            context["adjacent_segment_context"] = adjacent_segment_context_for_segment(conn, segment_id)
            context["episode_context_contract"] = (
                "This compact artifact came from a GPT-5.5 full-episode read. Use it for speaker/entity/concept "
                "context, Use adjacent_segment_context to resolve speaker continuity across segment boundaries, "
                "but emit evidence only from the current Segment Text section."
            )
            prompt = render_prompt(
                "ai_discourse_v3_1",
                {"text": segment_context["segment_text"]},
                context,
            )
        except Exception as exc:
            preparation_failures.append(
                {"segment_id": segment_id, "error_type": type(exc).__name__}
            )
            continue
        prompt_path = prompt_dir / f"{segment_id}.private.md"
        write_text_atomic(prompt_path, prompt)
        prepared.append(
            {
                "chunk": chunk,
                "segment_id": segment_id,
                "segment_text": segment_context["segment_text"],
                "prompt_path": prompt_path,
                "output_path": output_path,
            }
        )
    scratch_dir = Path("/tmp/pif-efficiency-codex-smoke")
    scratch_dir.mkdir(parents=True, exist_ok=True)

    def run_one(item: dict[str, Any]) -> dict[str, Any]:
        segment_id = item["segment_id"]
        attempts = []
        validation_ok = False
        output_event_count = None
        extraction_status = None
        for attempt_index in range(retry_count + 1):
            deterministic_repairs = 0
            output_path = item["output_path"]
            output_path.write_text("", encoding="utf-8")
            log_path = log_dir / f"{segment_id}.attempt-{attempt_index + 1}.private.jsonl"
            command = [
                "codex",
                "exec",
                "-m",
                model,
                "-c",
                f'model_reasoning_effort="{reasoning_effort}"',
                "-C",
                str(scratch_dir),
                "--skip-git-repo-check",
                "--ignore-rules",
                "--ephemeral",
                "--sandbox",
                "read-only",
                "--output-schema",
                str(schema_path),
                "--output-last-message",
                str(output_path),
                "--json",
                "-",
            ]
            exit_code, timed_out, elapsed_seconds = _run_codex_smoke_command(
                command,
                prompt_path=item["prompt_path"],
                log_path=log_path,
                timeout_seconds=timeout_seconds,
            )
            json_ok = _json_file_ok(output_path)
            error_type = None
            if exit_code == 0 and not timed_out and json_ok:
                try:
                    payload = json.loads(output_path.read_text(encoding="utf-8"))
                    deterministic_repairs = repair_label_output_for_submission(
                        "ai_discourse_v3_1", payload, segment_text=item["segment_text"]
                    )
                    _validate_schema(pack.schema, payload, path="$")
                    _validate_pack_logic(
                        "ai_discourse_v3_1", payload, segment_text=item["segment_text"]
                    )
                    validate_label_grounding(
                        "ai_discourse_v3_1", payload, segment_text=item["segment_text"]
                    )
                    write_text_atomic(
                        output_path,
                        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
                    )
                    validation_ok = True
                    output_event_count = len(payload.get("discourse_events") or [])
                    extraction_status = payload.get("extraction_status")
                except Exception as exc:
                    error_type = type(exc).__name__
            attempts.append(
                {
                    "attempt": attempt_index + 1,
                    "exit_code": exit_code,
                    "timed_out": timed_out,
                    "elapsed_seconds": elapsed_seconds,
                    "json_ok": json_ok,
                    "validation_ok": validation_ok,
                    "deterministic_repairs": deterministic_repairs,
                    "error_type": error_type,
                    "usage": _codex_usage_from_jsonl(log_path),
                }
            )
            if validation_ok:
                break
        return {
            "segment_id": segment_id,
            "output_path": str(item["output_path"].resolve()),
            "source_name": item["chunk"].get("source_name"),
            "density_stratum": item["chunk"].get("density_stratum"),
            "golden_event_count": item["chunk"].get("expected_discourse_events"),
            "attempts": attempts,
            "retry_attempts": max(0, len(attempts) - 1),
            "status_ok": validation_ok,
            "extraction_status": extraction_status,
            "output_event_count": output_event_count,
            "deterministic_repairs": sum(
                int(attempt.get("deterministic_repairs") or 0) for attempt in attempts
            ),
            "usage": _sum_usage(attempts),
            "usage_complete": all(attempt.get("usage") is not None for attempt in attempts),
        }

    if concurrency == 1:
        results = [run_one(item) for item in prepared]
    else:
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            results = [
                future.result()
                for future in as_completed([executor.submit(run_one, item) for item in prepared])
            ]
    results.sort(key=lambda item: item["segment_id"])
    wall_elapsed_seconds = round(time.monotonic() - wall_started, 3)
    all_attempts = [attempt for result in results for attempt in result["attempts"]]
    usage = _sum_usage(all_attempts)
    status_successes = sum(result["status_ok"] for result in results)
    accounting_complete = bool(
        len(results) + len(preparation_failures) == len(chunks)
        and all(result["usage_complete"] for result in results)
    )
    report = {
        "schema_version": PAIRED_BASELINE_RUN_SCHEMA_VERSION,
        "manifest_path": str(manifest_file),
        "acceptance_spec_sha256": acceptance_sha256,
        "model": model,
        "reasoning_effort": reasoning_effort,
        "concurrency": concurrency,
        "retry_count": retry_count,
        "measurement_boundary": "manifest_read_through_validation_and_accounting",
        "episode_context_policy": acceptance["cache_and_context_policy"]["episode_context"],
        "started_at": started_at,
        "finished_at": now_iso(),
        "wall_elapsed_seconds": wall_elapsed_seconds,
        "requested_segments": len(chunks),
        "executed_segments": len(results),
        "preparation_failures": preparation_failures,
        "terminal_failures": [
            *preparation_failures,
            *[
                {"segment_id": result["segment_id"], "error_type": "validation_failure"}
                for result in results
                if not result["status_ok"]
            ],
        ],
        "attempted_calls": len(all_attempts),
        "failed_attempts": sum(not attempt["validation_ok"] for attempt in all_attempts),
        "retry_attempts": sum(result["retry_attempts"] for result in results),
        "deterministic_repairs": sum(result["deterministic_repairs"] for result in results),
        "schema_status_success_rate": _ratio(status_successes, len(chunks)) or 0.0,
        "usage": usage,
        "usage_unknown_attempts": sum(attempt.get("usage") is None for attempt in all_attempts),
        "accounting_complete": accounting_complete,
        "ok": (
            len(results) == len(chunks)
            and not preparation_failures
            and all(result["status_ok"] for result in results)
            and accounting_complete
        ),
        "results": results,
        "privacy": "sanitized_report_private_prompts_and_outputs",
    }
    report_path = root / "run-report.json"
    write_text_atomic(report_path, json.dumps(report, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
    return {**report, "report_path": str(report_path), "output_dir": str(output_dir)}


def _sum_usage_dicts(items: list[dict[str, Any] | None]) -> dict[str, int]:
    fields = (
        "input_tokens",
        "cached_input_tokens",
        "output_tokens",
        "reasoning_output_tokens",
        "total_tokens",
    )
    return {
        field: sum(int((item or {}).get(field) or 0) for item in items)
        for field in fields
    }


def _paired_block_order(*, block_index: int, seed: str) -> str:
    baseline_first_at_zero = int(sha256_text(f"{seed}|block-order")[-1], 16) % 2 == 0
    baseline_first = baseline_first_at_zero if block_index % 2 == 0 else not baseline_first_at_zero
    return "baseline_then_candidate" if baseline_first else "candidate_then_baseline"


def run_paired_phase_one(
    conn,
    *,
    manifest_path: str | Path,
    output_dir: str | Path,
    limit: int = 0,
    timeout_seconds: int = 900,
    acceptance_spec_path: str | Path = DEFAULT_ACCEPTANCE_SPEC_PATH,
    rerun: bool = False,
) -> dict[str, Any]:
    if limit < 0:
        raise ValueError("limit must be non-negative")
    acceptance = load_windowed_acceptance_spec(acceptance_spec_path)
    paired_spec = acceptance["paired_run"]
    concurrency = int(paired_spec["concurrency"])
    retry_count = 1
    manifest_file = Path(manifest_path).expanduser().resolve()
    manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
    if manifest.get("schema_version") != PAIRED_MANIFEST_SCHEMA_VERSION:
        raise ValueError("paired phase one requires a frozen paired evaluation manifest")
    acceptance_sha256 = sha256_text(
        Path(acceptance_spec_path).expanduser().resolve().read_text(encoding="utf-8")
    )
    if manifest.get("acceptance_spec_sha256") != acceptance_sha256:
        raise ValueError("paired manifest acceptance specification has drifted")
    chunks = list(manifest.get("chunks") or [])
    if limit:
        chunks = chunks[:limit]
    root = Path(output_dir).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    started_at = now_iso()
    wall_started = time.monotonic()
    block_reports = []
    master_core_entries = []
    baseline_outputs = []
    seed = str(manifest.get("seed") or "windowed-paired-holdout-v1")
    blocks = [chunks[index : index + concurrency] for index in range(0, len(chunks), concurrency)]
    for block_index, block_chunks in enumerate(blocks):
        block_dir = root / "blocks" / f"block-{block_index:03d}"
        block_dir.mkdir(parents=True, exist_ok=True)
        block_manifest_path = block_dir / "manifest.json"
        block_manifest = {
            **{key: value for key, value in manifest.items() if key != "chunks"},
            "parent_manifest_path": str(manifest_file),
            "block_index": block_index,
            "chunks": block_chunks,
        }
        write_text_atomic(
            block_manifest_path,
            json.dumps(block_manifest, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        )
        order = _paired_block_order(block_index=block_index, seed=seed)

        def run_baseline_block() -> dict[str, Any]:
            return run_paired_baseline(
                conn,
                manifest_path=block_manifest_path,
                concurrency=concurrency,
                model=acceptance["baseline"]["model"],
                reasoning_effort=acceptance["baseline"]["reasoning_effort"],
                timeout_seconds=timeout_seconds,
                retry_count=retry_count,
                acceptance_spec_path=acceptance_spec_path,
                rerun=rerun,
            )

        def run_candidate_block() -> dict[str, Any]:
            return run_windowed_event_core_smoke(
                conn,
                manifest_path=block_manifest_path,
                limit=len(block_chunks),
                concurrency=concurrency,
                model=acceptance["candidate"]["core_model"],
                reasoning_effort=acceptance["candidate"]["core_reasoning_effort"],
                timeout_seconds=timeout_seconds,
                window_count=4,
                context_chars=900,
                guideline_path=DEFAULT_WINDOWED_GUIDELINES_PATH,
                max_total_events=int(acceptance["candidate"]["max_events"]),
                min_expected_events=0,
                max_expected_events=None,
                retry_count=retry_count,
                rerun=rerun,
            )

        if order == "baseline_then_candidate":
            baseline_report = run_baseline_block()
            candidate_report = run_candidate_block()
        else:
            candidate_report = run_candidate_block()
            baseline_report = run_baseline_block()
        candidate_manifest_path = Path(candidate_report["manifest_path"])
        candidate_manifest = json.loads(candidate_manifest_path.read_text(encoding="utf-8"))
        master_core_entries.extend(candidate_manifest.get("entries") or [])
        baseline_outputs.extend(
            {
                "segment_id": item["segment_id"],
                "output_path": item["output_path"],
                "status_ok": item["status_ok"],
            }
            for item in baseline_report.get("results") or []
        )
        baseline_outputs.extend(
            {
                "segment_id": item["segment_id"],
                "output_path": None,
                "status_ok": False,
                "terminal_stage": "preparation",
                "error_type": item.get("error_type"),
            }
            for item in baseline_report.get("preparation_failures") or []
        )
        block_reports.append(
            {
                "block_index": block_index,
                "segment_ids": [str(chunk["chunk_id"]) for chunk in block_chunks],
                "order": order,
                "baseline": {
                    "ok": baseline_report["ok"],
                    "wall_elapsed_seconds": baseline_report["wall_elapsed_seconds"],
                    "usage": baseline_report["usage"],
                    "attempted_calls": baseline_report["attempted_calls"],
                    "retry_attempts": baseline_report["retry_attempts"],
                    "failed_attempts": baseline_report["failed_attempts"],
                    "preparation_failures": baseline_report.get("preparation_failures") or [],
                    "accounting_complete": baseline_report["accounting_complete"],
                    "report_path": baseline_report["report_path"],
                },
                "candidate_core": {
                    "ok": candidate_report["ok"],
                    "wall_elapsed_seconds": candidate_report["wall_elapsed_seconds"],
                    "usage": candidate_report["usage"],
                    "attempted_calls": candidate_report["attempted_calls"],
                    "retry_attempts": candidate_report["retry_attempts"],
                    "accounting_complete": candidate_report["accounting_complete"],
                    "manifest_path": candidate_report["manifest_path"],
                    "results": candidate_report["results"],
                },
            }
        )
    master_core_manifest_path = root / "paired-core-manifest.json"
    master_core_manifest = {
        "schema_version": "ai_discourse_v3_1_windowed_event_core_v2",
        "source_manifest_path": str(manifest_file),
        "window_count": 4,
        "context_chars": 900,
        "guideline_artifact_sha256": manifest.get("candidate_guideline_sha256"),
        "max_total_events": int(acceptance["candidate"]["max_events"]),
        "structural_max_events": int(acceptance["candidate"]["max_events"]),
        "model": acceptance["candidate"]["core_model"],
        "reasoning_effort": acceptance["candidate"]["core_reasoning_effort"],
        "concurrency": concurrency,
        "retry_count": retry_count,
        "entries": master_core_entries,
        "privacy": "private_analysis_only",
    }
    write_text_atomic(
        master_core_manifest_path,
        json.dumps(master_core_manifest, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
    )
    baseline_usage = _sum_usage_dicts(
        [block["baseline"]["usage"] for block in block_reports]
    )
    core_usage = _sum_usage_dicts(
        [block["candidate_core"]["usage"] for block in block_reports]
    )
    baseline_wall = round(
        sum(float(block["baseline"]["wall_elapsed_seconds"]) for block in block_reports),
        3,
    )
    core_wall = round(
        sum(float(block["candidate_core"]["wall_elapsed_seconds"]) for block in block_reports),
        3,
    )
    core_results = [
        result
        for block in block_reports
        for result in block["candidate_core"]["results"]
    ]
    report = {
        "schema_version": PAIRED_PHASE_ONE_SCHEMA_VERSION,
        "manifest_path": str(manifest_file),
        "acceptance_spec_sha256": acceptance_sha256,
        "started_at": started_at,
        "finished_at": now_iso(),
        "overall_wall_elapsed_seconds": round(time.monotonic() - wall_started, 3),
        "measurement_boundary": "alternating_block_manifest_read_through_validation_and_accounting",
        "scheduler": {
            "block_size": concurrency,
            "worker_limit": concurrency,
            "retry_count": retry_count,
            "session_policy": acceptance["cache_and_context_policy"]["session_policy"],
            "block_order_counts": dict(Counter(block["order"] for block in block_reports)),
        },
        "requested_segments": len(chunks),
        "baseline": {
            "model": acceptance["baseline"]["model"],
            "reasoning_effort": acceptance["baseline"]["reasoning_effort"],
            "wall_elapsed_seconds": baseline_wall,
            "usage": baseline_usage,
            "attempted_calls": sum(block["baseline"]["attempted_calls"] for block in block_reports),
            "retry_attempts": sum(block["baseline"]["retry_attempts"] for block in block_reports),
            "failed_attempts": sum(block["baseline"]["failed_attempts"] for block in block_reports),
            "preparation_failures": [
                item
                for block in block_reports
                for item in block["baseline"].get("preparation_failures") or []
            ],
            "accounting_complete": all(block["baseline"]["accounting_complete"] for block in block_reports),
            "outputs": baseline_outputs,
            "ok": all(block["baseline"]["ok"] for block in block_reports),
        },
        "candidate_core": {
            "model": acceptance["candidate"]["core_model"],
            "reasoning_effort": acceptance["candidate"]["core_reasoning_effort"],
            "wall_elapsed_seconds": core_wall,
            "usage": core_usage,
            "attempted_calls": sum(block["candidate_core"]["attempted_calls"] for block in block_reports),
            "retry_attempts": sum(block["candidate_core"]["retry_attempts"] for block in block_reports),
            "accounting_complete": all(
                block["candidate_core"]["accounting_complete"] for block in block_reports
            ),
            "exact_evidence_exceptions": [
                {
                    "segment_id": result["segment_id"],
                    "invalid_evidence_events": (result.get("ownership") or {}).get("invalid_evidence_events", 0),
                }
                for result in core_results
                if (result.get("ownership") or {}).get("invalid_evidence_events", 0)
            ],
            "repair_trigger_counts": {
                "global_event_cap_hit": sum(
                    bool((result.get("ownership") or {}).get("event_cap_hit")) for result in core_results
                ),
                "nonexact_evidence": sum(
                    bool((result.get("ownership") or {}).get("invalid_evidence_events"))
                    for result in core_results
                ),
            },
            "manifest_path": str(master_core_manifest_path),
            "ok": all(block["candidate_core"]["ok"] for block in block_reports),
        },
        "blocks": block_reports,
        "sample_shape_eligible": len(chunks) >= int(paired_spec["minimum_segments"]),
        "privacy": "sanitized_report_private_prompts_and_outputs",
    }
    report["ok"] = bool(
        block_reports
        and report["baseline"]["ok"]
        and report["candidate_core"]["ok"]
        and report["baseline"]["accounting_complete"]
        and report["candidate_core"]["accounting_complete"]
    )
    report_path = root / "phase-one-report.json"
    write_text_atomic(report_path, json.dumps(report, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
    return {**report, "report_path": str(report_path)}


def _run_paired_enrichment_batch_worker(task: dict[str, Any]) -> dict[str, Any]:
    from . import db

    batch_conn = db.connect(Path(task["database_file"]))
    try:
        result = run_windowed_event_enrichment_smoke(
            batch_conn,
            core_manifest_path=task["batch_manifest_path"],
            limit=int(task["count"]),
            model=task["model"],
            reasoning_effort=task["reasoning_effort"],
            timeout_seconds=int(task["timeout_seconds"]),
            output_namespace=task["path_name"],
            retry_count=1,
            rerun=bool(task["rerun"]),
        )
    finally:
        batch_conn.close()
    return {"batch_index": int(task["batch_index"]), **result}


def run_paired_core_repairs(
    conn,
    *,
    phase_one_report_path: str | Path,
    timeout_seconds: int = 600,
    acceptance_spec_path: str | Path = DEFAULT_EVALUATOR_SPEC_PATH,
    rerun: bool = False,
) -> dict[str, Any]:
    acceptance = load_windowed_evaluator_spec(acceptance_spec_path)
    repair_spec = acceptance["failure_repair"]
    phase_one_file = Path(phase_one_report_path).expanduser().resolve()
    phase_one = json.loads(phase_one_file.read_text(encoding="utf-8"))
    if phase_one.get("schema_version") != PAIRED_PHASE_ONE_SCHEMA_VERSION:
        raise ValueError("paired core repair requires a phase-one report")
    if phase_one.get("acceptance_spec_sha256") != acceptance["parent_extraction_spec_sha256"]:
        raise ValueError("phase-one extraction specification does not match evaluator parent hash")
    core_manifest_file = Path(phase_one["candidate_core"]["manifest_path"])
    core_manifest = json.loads(core_manifest_file.read_text(encoding="utf-8"))
    root = phase_one_file.parent / "observable-core-repair"
    prompt_dir = root / "prompts"
    output_dir = root / "outputs"
    log_dir = root / "logs"
    for path in (root, prompt_dir, output_dir, log_dir):
        path.mkdir(parents=True, exist_ok=True)
    schema_path = root / "schema.json"
    max_events = int(acceptance["candidate"]["max_events"])
    write_text_atomic(
        schema_path,
        json.dumps(windowed_event_core_schema(max_events=max_events), ensure_ascii=True, indent=2, sort_keys=True)
        + "\n",
    )
    core_results = {
        result["segment_id"]: result
        for block in phase_one.get("blocks") or []
        for result in (block.get("candidate_core") or {}).get("results") or []
    }
    prepared = []
    triggered = []
    for entry in core_manifest.get("entries") or []:
        segment_id = str(entry["segment_id"])
        result = core_results.get(segment_id) or {}
        ownership = result.get("ownership") or {}
        core_output_path = Path(entry["output_path"])
        if not _json_file_ok(core_output_path):
            continue
        core_payload = json.loads(core_output_path.read_text(encoding="utf-8"))
        triggers = []
        if ownership.get("event_cap_hit"):
            triggers.append("global_event_cap_hit")
        if ownership.get("invalid_evidence_events"):
            triggers.append("nonexact_evidence")
        if not triggers:
            continue
        segment_text, read_error = _paired_segment_text(conn, segment_id)
        if read_error:
            raise ValueError(f"could not read repair segment: {read_error}")
        output_path = output_dir / f"{segment_id}.private.json"
        prompt_path = prompt_dir / f"{segment_id}.private.md"
        original_prompt_path = Path(entry["prompt_path"])
        diagnostic = {
            "triggers": sorted(set(triggers)),
            "event_cap": max_events,
            "invalid_evidence_ids": ownership.get("invalid_evidence_ids") or [],
        }
        instruction = "\n\n".join(
            [
                original_prompt_path.read_text(encoding="utf-8"),
                "# Observable-failure repair",
                "The previous LLM output failed one or more observable structural or grounding checks. Re-read every "
                "window and return a complete replacement global events array. Make every semantic extraction "
                "decision yourself. Do not use keyword, regex, token-overlap, or phrase rules. Evidence must be an "
                "exact contiguous span from one extract_text and window_id must identify the owner range containing "
                "the evidence start. Remove an event if no exact support exists. Resolve only the listed failure "
                "diagnostics; do not invent events and do not treat the event cap as a target.",
                "# Failure diagnostics\n" + json.dumps(diagnostic, ensure_ascii=True, separators=(",", ":")),
                "# Previous output\n" + json.dumps(core_payload, ensure_ascii=True, separators=(",", ":")),
            ]
        ) + "\n"
        write_text_atomic(prompt_path, instruction)
        prepared.append(
            {
                "entry": entry,
                "segment_id": segment_id,
                "triggers": sorted(set(triggers)),
                "before_ownership": ownership,
                "segment_text": segment_text,
                "prompt_path": prompt_path,
                "output_path": output_path,
            }
        )
        triggered.append(segment_id)
    scratch_dir = Path("/tmp/pif-efficiency-codex-smoke")
    scratch_dir.mkdir(parents=True, exist_ok=True)
    started_at = now_iso()
    wall_started = time.monotonic()

    def run_one(item: dict[str, Any]) -> dict[str, Any]:
        attempts = []
        normalized_payload = None
        after_ownership = None
        status_ok = False
        for attempt_index in range(int(repair_spec["max_headless_calls_per_segment"])):
            item["output_path"].write_text("", encoding="utf-8")
            log_path = log_dir / f"{item['segment_id']}.attempt-{attempt_index + 1}.private.jsonl"
            command = [
                "codex",
                "exec",
                "-m",
                repair_spec["model"],
                "-c",
                f'model_reasoning_effort="{repair_spec["reasoning_effort"]}"',
                "-C",
                str(scratch_dir),
                "--skip-git-repo-check",
                "--ignore-rules",
                "--ephemeral",
                "--sandbox",
                "read-only",
                "--output-schema",
                str(schema_path),
                "--output-last-message",
                str(item["output_path"]),
                "--json",
                "-",
            ]
            exit_code, timed_out, elapsed_seconds = _run_codex_smoke_command(
                command,
                prompt_path=item["prompt_path"],
                log_path=log_path,
                timeout_seconds=timeout_seconds,
            )
            json_ok = _json_file_ok(item["output_path"])
            normalization_error = None
            if exit_code == 0 and not timed_out and json_ok:
                try:
                    payload = json.loads(item["output_path"].read_text(encoding="utf-8"))
                    if payload.get("segment_id") != item["segment_id"]:
                        raise ValueError("segment_id_mismatch")
                    normalized_payload, after_ownership = normalize_windowed_core_payload(
                        payload,
                        segment_text=item["segment_text"],
                        boundaries=item["entry"]["boundaries"],
                        max_events=max_events,
                    )
                    write_text_atomic(
                        item["output_path"],
                        json.dumps(normalized_payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
                    )
                    status_ok = bool(
                        after_ownership["invalid_evidence_events"] == 0
                        and not after_ownership["event_cap_hit"]
                    )
                except Exception as exc:
                    normalization_error = type(exc).__name__
            attempts.append(
                {
                    "attempt": attempt_index + 1,
                    "exit_code": exit_code,
                    "timed_out": timed_out,
                    "elapsed_seconds": elapsed_seconds,
                    "json_ok": json_ok,
                    "normalization_error": normalization_error,
                    "usage": _codex_usage_from_jsonl(log_path),
                }
            )
            if status_ok:
                break
        before = item["before_ownership"]
        unresolved = []
        if after_ownership:
            if after_ownership.get("event_cap_hit"):
                unresolved.append("global_event_cap_hit")
            if after_ownership.get("invalid_evidence_events"):
                unresolved.append("nonexact_evidence")
        return {
            "segment_id": item["segment_id"],
            "triggers": item["triggers"],
            "unresolved_triggers": sorted(set(unresolved)),
            "status_ok": status_ok,
            "output_path": str(item["output_path"].resolve()),
            "before": {
                "events": before.get("output_events"),
                "invalid_evidence_events": before.get("invalid_evidence_events"),
                "event_cap_hit": bool(before.get("event_cap_hit")),
            },
            "after": {
                "events": (after_ownership or {}).get("output_events"),
                "invalid_evidence_events": (after_ownership or {}).get("invalid_evidence_events"),
                "event_cap_hit": bool((after_ownership or {}).get("event_cap_hit")),
            },
            "attempts": attempts,
            "retry_attempts": max(0, len(attempts) - 1),
            "usage": _sum_usage(attempts),
            "usage_complete": all(attempt.get("usage") is not None for attempt in attempts),
        }

    concurrency = int(acceptance["paired_run"]["concurrency"])
    if prepared:
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            results = [
                future.result()
                for future in as_completed([executor.submit(run_one, item) for item in prepared])
            ]
    else:
        results = []
    results.sort(key=lambda item: item["segment_id"])
    result_by_segment = {item["segment_id"]: item for item in results}
    effective_entries = json.loads(json.dumps(core_manifest.get("entries") or [], ensure_ascii=True))
    for entry in effective_entries:
        repair = result_by_segment.get(str(entry["segment_id"]))
        if repair and repair["status_ok"]:
            entry["original_output_path"] = entry["output_path"]
            entry["output_path"] = repair["output_path"]
    effective_manifest_path = root / "effective-core-manifest.json"
    write_text_atomic(
        effective_manifest_path,
        json.dumps(
            {
                **{key: value for key, value in core_manifest.items() if key != "entries"},
                "repair_model": repair_spec["model"],
                "repair_reasoning_effort": repair_spec["reasoning_effort"],
                "entries": effective_entries,
            },
            ensure_ascii=True,
            indent=2,
            sort_keys=True,
        )
        + "\n",
    )
    all_attempts = [attempt for result in results for attempt in result["attempts"]]
    trigger_counts = Counter(trigger for item in results for trigger in item["triggers"])
    unresolved_counts = Counter(trigger for item in results for trigger in item["unresolved_triggers"])
    initial_exact_failures = len(phase_one["candidate_core"].get("exact_evidence_exceptions") or [])
    repaired_exact_failures = sum(
        item["after"]["invalid_evidence_events"] or 0
        for item in results
        if item["status_ok"]
    ) + sum(
        1
        for item in results
        if not item["status_ok"] and "nonexact_evidence" in item["triggers"]
    )
    report = {
        "schema_version": PAIRED_CORE_REPAIR_SCHEMA_VERSION,
        "phase_one_report_path": str(phase_one_file),
        "model": repair_spec["model"],
        "reasoning_effort": repair_spec["reasoning_effort"],
        "max_calls_per_segment": int(repair_spec["max_headless_calls_per_segment"]),
        "generic_confidence_routing": False,
        "fallback": None,
        "started_at": started_at,
        "finished_at": now_iso(),
        "wall_elapsed_seconds": round(time.monotonic() - wall_started, 3),
        "triggered_segments": len(triggered),
        "trigger_counts": dict(sorted(trigger_counts.items())),
        "unresolved_trigger_counts": dict(sorted(unresolved_counts.items())),
        "initial_exact_evidence_exceptions": initial_exact_failures,
        "effective_exact_evidence_exceptions": repaired_exact_failures,
        "attempted_calls": len(all_attempts),
        "retry_attempts": sum(item["retry_attempts"] for item in results),
        "usage": _sum_usage(all_attempts),
        "accounting_complete": all(item["usage_complete"] for item in results),
        "results": results,
        "effective_core_manifest_path": str(effective_manifest_path),
        "quality_gain_status": "pending_blinded_adjudication",
        "privacy": "sanitized_report_private_prompts_and_outputs",
    }
    report["all_observable_triggers_cleared"] = bool(
        all(item["status_ok"] and not item["unresolved_triggers"] for item in results)
    )
    report["effective_core_ok"] = bool(
        repaired_exact_failures == 0 and report["all_observable_triggers_cleared"]
    )
    report["ok"] = bool(
        report["all_observable_triggers_cleared"]
        and report["accounting_complete"]
        and report["attempted_calls"] <= len(triggered)
        and report["retry_attempts"] == 0
    )
    report_path = root / "run-report.json"
    write_text_atomic(report_path, json.dumps(report, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
    return {**report, "report_path": str(report_path)}


def _paired_segment_text(conn, segment_id: str) -> tuple[str, str | None]:
    from .worker import segment_for_job

    try:
        return segment_for_job(conn, {"target_id": segment_id})["segment_text"], None
    except Exception as exc:
        return "", type(exc).__name__


def run_paired_enrichment_path(
    conn,
    *,
    phase_one_report_path: str | Path,
    path_name: str,
    core_repair_report_path: str | Path | None = None,
    batch_size: int = 7,
    timeout_seconds: int = 600,
    acceptance_spec_path: str | Path = DEFAULT_EVALUATOR_SPEC_PATH,
    rerun: bool = False,
) -> dict[str, Any]:
    if batch_size < 1:
        raise ValueError("batch size must be at least 1")
    acceptance = _load_windowed_spec(acceptance_spec_path)
    enrichment_spec = next(
        (
            item
            for item in acceptance["candidate"]["enrichment_paths"]
            if item["name"] == path_name
        ),
        None,
    )
    if enrichment_spec is None:
        raise ValueError("unknown frozen enrichment path")
    if enrichment_spec.get("fallback") is not None:
        raise ValueError("paired enrichment path must not blend fallback models")
    phase_one_file = Path(phase_one_report_path).expanduser().resolve()
    phase_one = json.loads(phase_one_file.read_text(encoding="utf-8"))
    if phase_one.get("schema_version") != PAIRED_PHASE_ONE_SCHEMA_VERSION:
        raise ValueError("paired enrichment requires a phase-one report")
    core_repair = None
    if core_repair_report_path is not None:
        core_repair_file = Path(core_repair_report_path).expanduser().resolve()
        core_repair = json.loads(core_repair_file.read_text(encoding="utf-8"))
        if core_repair.get("schema_version") != PAIRED_CORE_REPAIR_SCHEMA_VERSION:
            raise ValueError("invalid paired core repair report")
        if Path(core_repair["phase_one_report_path"]).resolve() != phase_one_file:
            raise ValueError("core repair report belongs to a different phase-one run")
        core_manifest_file = Path(core_repair["effective_core_manifest_path"])
    else:
        core_manifest_file = Path(phase_one["candidate_core"]["manifest_path"])
    core_manifest = json.loads(core_manifest_file.read_text(encoding="utf-8"))
    core_variant = "repaired" if core_repair is not None else "unrepaired"
    root = phase_one_file.parent / f"enrichment-{path_name}-{core_variant}"
    root.mkdir(parents=True, exist_ok=True)
    requested_entries = list(core_manifest.get("entries") or [])
    primary_core_status = {
        str(result["segment_id"]): bool(result.get("status_ok"))
        for block in phase_one.get("blocks") or []
        for result in (block.get("candidate_core") or {}).get("results") or []
    }
    effective_core_status = dict(primary_core_status)
    if core_repair is not None:
        for item in core_repair.get("results") or []:
            effective_core_status[str(item["segment_id"])] = bool(
                item.get("status_ok") and not item.get("unresolved_triggers")
            )
    entries = [
        entry
        for entry in requested_entries
        if effective_core_status.get(str(entry["segment_id"]), False)
        and _json_file_ok(Path(entry["output_path"]))
    ]
    terminal_core_ids = sorted(
        str(entry["segment_id"])
        for entry in requested_entries
        if entry not in entries
    )
    batches = [entries[index : index + batch_size] for index in range(0, len(entries), batch_size)]
    prepared = []
    for batch_index, batch_entries in enumerate(batches):
        batch_dir = root / "batches" / f"batch-{batch_index:03d}"
        batch_dir.mkdir(parents=True, exist_ok=True)
        batch_manifest_path = batch_dir / "core-manifest.json"
        write_text_atomic(
            batch_manifest_path,
            json.dumps(
                {**{key: value for key, value in core_manifest.items() if key != "entries"}, "entries": batch_entries},
                ensure_ascii=True,
                indent=2,
                sort_keys=True,
            )
            + "\n",
        )
        prepared.append((batch_index, batch_manifest_path, len(batch_entries)))
    started_at = now_iso()
    wall_started = time.monotonic()
    database_row = conn.execute("PRAGMA database_list").fetchone()
    database_file = Path(database_row["file"] if hasattr(database_row, "keys") else database_row[2])

    concurrency = int(acceptance["paired_run"]["concurrency"])
    tasks = [
        {
            "batch_index": batch_index,
            "batch_manifest_path": str(batch_manifest_path),
            "count": count,
            "database_file": str(database_file),
            "model": enrichment_spec["model"],
            "reasoning_effort": enrichment_spec["reasoning_effort"],
            "timeout_seconds": timeout_seconds,
            "path_name": f"{path_name}-{core_variant}",
            "rerun": rerun,
        }
        for batch_index, batch_manifest_path, count in prepared
    ]
    if tasks:
        with ProcessPoolExecutor(max_workers=concurrency) as executor:
            batch_results = [
                future.result()
                for future in as_completed(
                    [executor.submit(_run_paired_enrichment_batch_worker, task) for task in tasks]
                )
            ]
    else:
        batch_results = []
    batch_results.sort(key=lambda item: item["batch_index"])
    usage = _sum_usage_dicts([item.get("usage") for item in batch_results])
    hydrated = [item for batch in batch_results for item in batch.get("hydrated") or []]
    hydrated_outputs = [
        {"segment_id": item["segment_id"], "output_path": item.get("output_path"), "ok": item["ok"]}
        for item in hydrated
    ] + [
        {
            "segment_id": segment_id,
            "output_path": None,
            "ok": False,
            "terminal_stage": "candidate_core",
        }
        for segment_id in terminal_core_ids
    ]
    manifest = json.loads(Path(phase_one["manifest_path"]).read_text(encoding="utf-8"))
    chunk_by_segment = {
        str((chunk.get("segment_ids") or [chunk["chunk_id"]])[0]): chunk
        for chunk in manifest.get("chunks") or []
    }
    no_signal_segments = 0
    no_signal_false_positives = 0
    for item in hydrated:
        chunk = chunk_by_segment.get(str(item["segment_id"])) or {}
        if chunk.get("density_stratum") != "no_signal" or not item.get("ok"):
            continue
        no_signal_segments += 1
        output_path = Path(item["output_path"])
        output = json.loads(output_path.read_text(encoding="utf-8"))
        no_signal_false_positives += int(bool(output.get("discourse_events")))
    core_usage = phase_one["candidate_core"]["usage"]
    repair_usage = (core_repair or {}).get("usage") or {}
    baseline_usage = phase_one["baseline"]["usage"]
    candidate_usage = _sum_usage_dicts([core_usage, repair_usage, usage])
    enrichment_wall = round(time.monotonic() - wall_started, 3)
    candidate_wall = round(
        float(phase_one["candidate_core"]["wall_elapsed_seconds"]) + enrichment_wall,
        3,
    )
    if core_repair:
        candidate_wall = round(candidate_wall + float(core_repair["wall_elapsed_seconds"]), 3)
    baseline_wall = float(phase_one["baseline"]["wall_elapsed_seconds"])
    repair_trigger_counts = Counter(
        trigger
        for batch in batch_results
        for trigger in batch.get("repair_triggers") or []
    )
    report = {
        "schema_version": PAIRED_ENRICHMENT_RUN_SCHEMA_VERSION,
        "phase_one_report_path": str(phase_one_file),
        "core_repair_report_path": (
            str(Path(core_repair_report_path).expanduser().resolve())
            if core_repair_report_path is not None
            else None
        ),
        "path_name": path_name,
        "core_variant": core_variant,
        "model": enrichment_spec["model"],
        "reasoning_effort": enrichment_spec["reasoning_effort"],
        "fallback": None,
        "batch_size": batch_size,
        "concurrency": concurrency,
        "retry_count": 1,
        "started_at": started_at,
        "finished_at": now_iso(),
        "wall_elapsed_seconds": enrichment_wall,
        "requested_segments": len(requested_entries),
        "eligible_core_segments": len(entries),
        "terminal_core_segments": terminal_core_ids,
        "segments": len(requested_entries),
        "hydrated_segments": sum(item.get("ok", False) for item in hydrated),
        "schema_status_success_rate": (
            _ratio(sum(item.get("ok", False) for item in hydrated), len(requested_entries)) or 0.0
        ),
        "usage": usage,
        "accounting_complete": all(item.get("usage_complete", False) for item in batch_results),
        "batch_results": batch_results,
        "hydrated_outputs": hydrated_outputs,
        "repair_trigger_counts": dict(sorted(repair_trigger_counts.items())),
        "no_signal": {
            "segments": no_signal_segments,
            "false_positives": no_signal_false_positives,
            "false_positive_rate": _ratio(no_signal_false_positives, no_signal_segments),
        },
        "paired_efficiency": {
            "baseline_usage": baseline_usage,
            "candidate_usage": candidate_usage,
            "total_token_ratio": _ratio(candidate_usage["total_tokens"], baseline_usage["total_tokens"]),
            "baseline_wall_elapsed_seconds": baseline_wall,
            "candidate_wall_elapsed_seconds": candidate_wall,
            "equal_concurrency_wall_ratio": _ratio(candidate_wall, baseline_wall),
            "wall_comparability": "core_alternated_with_baseline_but_enrichment_measured_posthoc",
        },
        "sample_shape_eligible": bool(
            phase_one.get("sample_shape_eligible", phase_one.get("acceptance_eligible", False))
        ),
        "privacy": "sanitized_report_private_prompts_and_outputs",
    }
    report["run_complete"] = bool(
        len(hydrated) == len(entries)
        and all(item.get("ok") for item in batch_results)
        and report["accounting_complete"]
    )
    report["ok"] = report["run_complete"]
    report_path = root / "run-report.json"
    write_text_atomic(report_path, json.dumps(report, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
    return {**report, "report_path": str(report_path)}


def _full_event_for_adjudication(event: dict[str, Any], *, event_id: int) -> dict[str, Any]:
    return _semantic_judge_event(event, event_id=event_id, include_full_fields=True)


def _exact_full_event_pairs(
    golden_events: list[dict[str, Any]],
    system_events: list[dict[str, Any]],
) -> tuple[list[tuple[int, int]], list[int], list[int]]:
    system_by_identity: dict[str, list[int]] = defaultdict(list)
    for system_id, event in enumerate(system_events):
        canonical = _full_event_for_adjudication(event, event_id=system_id)
        canonical.pop("id", None)
        identity = json.dumps(canonical, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        system_by_identity[identity].append(system_id)
    pairs = []
    used_system = set()
    used_golden = set()
    for golden_id, event in enumerate(golden_events):
        canonical = _full_event_for_adjudication(event, event_id=golden_id)
        canonical.pop("id", None)
        identity = json.dumps(canonical, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        candidates = system_by_identity.get(identity) or []
        if not candidates:
            continue
        system_id = candidates.pop(0)
        pairs.append((golden_id, system_id))
        used_golden.add(golden_id)
        used_system.add(system_id)
    return (
        pairs,
        [index for index in range(len(golden_events)) if index not in used_golden],
        [index for index in range(len(system_events)) if index not in used_system],
    )


def export_blinded_paired_adjudication(
    conn,
    *,
    phase_one_report_path: str | Path,
    enrichment_report_path: str | Path,
    output_dir: str | Path,
    shared_human_output_dir: str | Path | None = None,
    seed: str = "windowed-paired-adjudication-v1",
) -> dict[str, Any]:
    phase_one_file = Path(phase_one_report_path).expanduser().resolve()
    enrichment_file = Path(enrichment_report_path).expanduser().resolve()
    phase_one = json.loads(phase_one_file.read_text(encoding="utf-8"))
    enrichment = json.loads(enrichment_file.read_text(encoding="utf-8"))
    if phase_one.get("schema_version") != PAIRED_PHASE_ONE_SCHEMA_VERSION:
        raise ValueError("invalid paired phase-one report")
    if enrichment.get("schema_version") != PAIRED_ENRICHMENT_RUN_SCHEMA_VERSION:
        raise ValueError("invalid paired enrichment report")
    if Path(enrichment["phase_one_report_path"]).resolve() != phase_one_file:
        raise ValueError("enrichment report belongs to a different phase-one run")
    manifest = json.loads(Path(phase_one["manifest_path"]).read_text(encoding="utf-8"))
    chunks = {
        str((chunk.get("segment_ids") or [chunk["chunk_id"]])[0]): chunk
        for chunk in manifest.get("chunks") or []
    }
    requested_segment_ids = [
        str(segment_id)
        for block in phase_one.get("blocks") or []
        for segment_id in block.get("segment_ids") or []
    ]
    baseline_items = {
        str(item["segment_id"]): item for item in phase_one["baseline"].get("outputs") or []
    }
    candidate_items = {
        str(item["segment_id"]): item for item in enrichment.get("hydrated_outputs") or []
    }
    core_status = {
        str(result["segment_id"]): bool(result.get("status_ok"))
        for block in phase_one.get("blocks") or []
        for result in (block.get("candidate_core") or {}).get("results") or []
    }
    repaired_status = {}
    if enrichment.get("core_repair_report_path"):
        repair = json.loads(Path(enrichment["core_repair_report_path"]).read_text(encoding="utf-8"))
        repaired_status = {
            str(item["segment_id"]): bool(item.get("status_ok") and not item.get("unresolved_triggers"))
            for item in repair.get("results") or []
        }
    root = Path(output_dir).expanduser().resolve()
    shared_root = (
        Path(shared_human_output_dir).expanduser().resolve()
        if shared_human_output_dir is not None
        else root
    )
    prompt_dir = shared_root / "prompts"
    output_path_dir = shared_root / "human_outputs"
    for path in (root, shared_root, prompt_dir, output_path_dir):
        path.mkdir(parents=True, exist_ok=True)
    fixture = load_judge_calibration_fixture()
    mismatch_fields = fixture["mismatch_fields"]
    schema_path = shared_root / "human-output-schema.json"
    pair_schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["a_id", "b_id", "relation", "mismatch_fields"],
        "properties": {
            "a_id": {"type": "integer", "minimum": 0},
            "b_id": {"type": "integer", "minimum": 0},
            "relation": {"type": "string", "enum": ["equivalent", "partial", "non_equivalent"]},
            "mismatch_fields": {
                "type": "array",
                "items": {"type": "string", "enum": mismatch_fields},
            },
        },
    }
    support_schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["id", "supported"],
        "properties": {
            "id": {"type": "integer", "minimum": 0},
            "supported": {"type": "boolean"},
        },
    }
    human_schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["reviewer_kind", "reviewer_id", "reviewed_at", "pairs", "support_a", "support_b"],
        "properties": {
            "reviewer_kind": {"type": "string", "enum": ["human"]},
            "reviewer_id": {"type": "string", "minLength": 1},
            "reviewed_at": {"type": "string", "minLength": 1},
            "pairs": {"type": "array", "items": pair_schema},
            "support_a": {"type": "array", "items": support_schema},
            "support_b": {"type": "array", "items": support_schema},
        },
    }
    write_text_atomic(
        schema_path,
        json.dumps(human_schema, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
    )
    comparisons = []
    missing_segments = []
    for segment_id in requested_segment_ids:
        chunk = chunks.get(segment_id)
        if chunk is None:
            missing_segments.append(segment_id)
            continue
        golden_row = conn.execute(
            "SELECT output_json FROM labels WHERE id = ? AND segment_id = ?",
            (chunk["golden_label_id"], segment_id),
        ).fetchone()
        if not golden_row:
            missing_segments.append(segment_id)
            continue
        golden = json.loads(golden_row["output_json"])
        source_excerpt, read_error = _paired_segment_text(conn, segment_id)
        if read_error:
            missing_segments.append(segment_id)
            continue
        golden_events = golden.get("discourse_events") or []
        baseline_item = baseline_items.get(segment_id) or {}
        candidate_item = candidate_items.get(segment_id) or {}
        baseline_terminal = not bool(baseline_item.get("status_ok"))
        repaired_core_ok = repaired_status.get(segment_id, core_status.get(segment_id, False))
        candidate_terminal = not bool(
            candidate_item.get("ok") and candidate_item.get("output_path") and repaired_core_ok
        )
        system_inputs = [
            (
                "baseline",
                None
                if baseline_terminal
                else json.loads(Path(baseline_item["output_path"]).read_text(encoding="utf-8")),
                baseline_terminal,
            ),
            (
                "candidate",
                None
                if candidate_terminal
                else json.loads(Path(candidate_item["output_path"]).read_text(encoding="utf-8")),
                candidate_terminal,
            ),
        ]
        for system_name, output, terminal_failure in system_inputs:
            system_events = [] if terminal_failure else output.get("discourse_events") or []
            exact_pairs, unresolved_golden, unresolved_system = _exact_full_event_pairs(
                golden_events, system_events
            )
            golden_blinded = [
                _full_event_for_adjudication(golden_events[index], event_id=index)
                for index in unresolved_golden
            ]
            system_blinded = [
                _full_event_for_adjudication(system_events[index], event_id=index)
                for index in unresolved_system
            ]
            golden_all = [
                _full_event_for_adjudication(event, event_id=index)
                for index, event in enumerate(golden_events)
            ]
            system_all = [
                _full_event_for_adjudication(event, event_id=index)
                for index, event in enumerate(system_events)
            ]
            comparison_fingerprint = sha256_text(
                json.dumps(
                    {
                        "segment_id": segment_id,
                        "source_excerpt_sha256": sha256_text(source_excerpt),
                        "golden_events": golden_blinded,
                        "system_events": system_blinded,
                        "terminal_failure": terminal_failure,
                    },
                    ensure_ascii=True,
                    sort_keys=True,
                    separators=(",", ":"),
                )
            )
            comparison_id = "pa_" + comparison_fingerprint[:18]
            reverse = int(sha256_text(f"orientation|{comparison_fingerprint}")[-1], 16) % 2 == 1
            set_a, set_b = (
                (system_blinded, golden_blinded) if reverse else (golden_blinded, system_blinded)
            )
            output_path = None
            prompt_path = None
            requires_human = bool((set_a or set_b) and not terminal_failure)
            if requires_human:
                output_path = output_path_dir / f"{comparison_id}.private.json"
                prompt_path = prompt_dir / f"{comparison_id}.private.md"
                prompt = "\n\n".join(
                    [
                        "You are the blinded human auditor for two anonymous full-field event sets extracted "
                        "from the same source excerpt. Align related events one-to-one. Mark equivalent only when "
                        "every material semantic field agrees, allowing harmless paraphrase or aliases. Mark partial "
                        "for a shared core event with a material omission, expansion, merge, or split. Mark "
                        "non_equivalent for wrong actor, attribution, stance, certainty, time, metric, negation, "
                        "target, mechanism, event family, unsupported inference, or nonexact evidence. The same "
                        "evidence may support legitimately distinct analytical lenses. Omit wholly unrelated pairs.",
                        "Return each a_id and b_id at most once. Independently mark source support for every event "
                        "in event_set_a and event_set_b, including one-sided residuals. A supported one-sided event "
                        "is not automatically a false positive. Equivalent pairs require an empty mismatch_fields "
                        f"array. Use only these diagnostic fields: {', '.join(mismatch_fields)}. Set reviewer_kind "
                        "to human and provide your reviewer ID and review time; model-generated or unlabeled review "
                        "files are invalid.",
                        "# Blinded packet\n"
                        + json.dumps(
                            {
                                "comparison_id": comparison_id,
                                "source_excerpt": source_excerpt,
                                "event_set_a": set_a,
                                "event_set_b": set_b,
                            },
                            ensure_ascii=True,
                            separators=(",", ":"),
                        ),
                    ]
                ) + "\n"
                write_text_atomic(prompt_path, prompt)
            comparisons.append(
                {
                    "comparison_id": comparison_id,
                    "comparison_fingerprint": comparison_fingerprint,
                    "segment_id": segment_id,
                    "system_name": system_name,
                    "system_is_a": reverse,
                    "terminal_failure": terminal_failure,
                    "golden_count": len(golden_events),
                    "system_count": len(system_events),
                    "golden_event_families": {
                        str(index): str(event.get("event_type") or "unknown")
                        for index, event in enumerate(golden_events)
                    },
                    "system_event_families": {
                        str(index): str(event.get("event_type") or "unknown")
                        for index, event in enumerate(system_events)
                    },
                    "exact_pairs": [[gold_id, system_id] for gold_id, system_id in exact_pairs],
                    "unresolved_golden_ids": unresolved_golden,
                    "unresolved_system_ids": unresolved_system,
                    "requires_human": requires_human,
                    "prompt_path": str(prompt_path.resolve()) if prompt_path else None,
                    "output_path": str(output_path.resolve()) if output_path else None,
                    "source_name": chunk.get("source_name"),
                    "source_id": chunk.get("source_id"),
                    "density_stratum": chunk.get("density_stratum"),
                    "event_families": sorted((chunk.get("event_family_counts") or {}).keys()),
                    "private_source_excerpt": source_excerpt,
                    "private_golden_events": golden_all,
                    "private_system_events": system_all,
                }
            )
    mapping = {
        "schema_version": PAIRED_ADJUDICATION_MANIFEST_VERSION,
        "phase_one_report_path": str(phase_one_file),
        "enrichment_report_path": str(enrichment_file),
        "path_name": enrichment.get("path_name"),
        "core_variant": enrichment.get("core_variant", "unrepaired"),
        "seed": seed,
        "schema_path": str(schema_path.resolve()),
        "shared_human_output_dir": str(shared_root),
        "comparisons": comparisons,
        "missing_segments": missing_segments,
        "privacy": "private_mapping_prompts_contain_source_text",
    }
    mapping_path = root / "mapping.private.json"
    write_text_atomic(mapping_path, json.dumps(mapping, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
    return {
        "ok": bool(comparisons) and not missing_segments,
        "mapping_path": str(mapping_path),
        "segments": len({item["segment_id"] for item in comparisons}),
        "comparisons": len(comparisons),
        "human_required": sum(item["requires_human"] for item in comparisons),
        "terminal_failures": sum(item["terminal_failure"] for item in comparisons),
        "exact_identity_pairs": sum(len(item["exact_pairs"]) for item in comparisons),
        "missing_segments": len(missing_segments),
        "privacy": "sanitized_summary_private_adjudication_artifacts",
    }


def export_consolidated_human_adjudication(
    *,
    mapping_paths: list[str | Path],
    output_dir: str | Path,
    seed: str = "windowed-consolidated-human-v1",
) -> dict[str, Any]:
    if len(mapping_paths) < 2:
        raise ValueError("consolidated human adjudication requires at least two mappings")
    loaded_mappings = []
    for value in mapping_paths:
        path = Path(value).expanduser().resolve()
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("schema_version") != PAIRED_ADJUDICATION_MANIFEST_VERSION:
            raise ValueError(f"invalid paired adjudication mapping: {path}")
        loaded_mappings.append((path, payload))
    root = Path(output_dir).expanduser().resolve()
    prompt_dir = root / "prompts"
    human_output_dir = root / "human_outputs"
    for path in (root, prompt_dir, human_output_dir):
        path.mkdir(parents=True, exist_ok=True)
    mismatch_fields = load_judge_calibration_fixture()["mismatch_fields"]
    schema_path = root / "human-output-schema.json"
    write_text_atomic(
        schema_path,
        json.dumps(
            {
                "type": "object",
                "additionalProperties": False,
                "required": [
                    "reviewer_kind",
                    "reviewer_id",
                    "reviewed_at",
                    "segment_id",
                    "support",
                    "comparisons",
                ],
                "properties": {
                    "reviewer_kind": {"type": "string", "enum": ["human"]},
                    "reviewer_id": {"type": "string", "minLength": 1},
                    "reviewed_at": {"type": "string", "minLength": 1},
                    "segment_id": {"type": "string", "minLength": 1},
                    "support": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["set_id", "event_id", "supported"],
                            "properties": {
                                "set_id": {"type": "string"},
                                "event_id": {"type": "integer", "minimum": 0},
                                "supported": {"type": "boolean"},
                            },
                        },
                    },
                    "comparisons": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["set_id", "pairs"],
                            "properties": {
                                "set_id": {"type": "string"},
                                "pairs": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "additionalProperties": False,
                                        "required": [
                                            "anchor_id",
                                            "event_id",
                                            "relation",
                                            "mismatch_fields",
                                        ],
                                        "properties": {
                                            "anchor_id": {"type": "integer", "minimum": 0},
                                            "event_id": {"type": "integer", "minimum": 0},
                                            "relation": {
                                                "type": "string",
                                                "enum": ["equivalent", "partial", "non_equivalent"],
                                            },
                                            "mismatch_fields": {
                                                "type": "array",
                                                "items": {"type": "string", "enum": mismatch_fields},
                                            },
                                        },
                                    },
                                },
                            },
                        },
                    },
                },
            },
            ensure_ascii=True,
            indent=2,
            sort_keys=True,
        )
        + "\n",
    )
    by_segment: dict[str, dict[str, Any]] = {}
    total_original_comparisons = 0
    for mapping_path, mapping in loaded_mappings:
        for comparison in mapping.get("comparisons") or []:
            if not comparison.get("requires_human"):
                continue
            total_original_comparisons += 1
            segment_id = str(comparison["segment_id"])
            source_excerpt = comparison.get("private_source_excerpt")
            golden_events = comparison.get("private_golden_events")
            system_events = comparison.get("private_system_events")
            if (
                not isinstance(source_excerpt, str)
                or not isinstance(golden_events, list)
                or not isinstance(system_events, list)
            ):
                raise ValueError("paired mapping lacks private consolidated audit payload")
            segment = by_segment.setdefault(
                segment_id,
                {
                    "segment_id": segment_id,
                    "source_excerpt": source_excerpt,
                    "golden_events": golden_events,
                    "system_sets": {},
                },
            )
            if (
                segment["source_excerpt"] != source_excerpt
                or segment["golden_events"] != golden_events
            ):
                raise ValueError(f"inconsistent anchor payload for segment {segment_id}")
            system_hash = sha256_text(
                json.dumps(system_events, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
            )
            system_set = segment["system_sets"].setdefault(
                system_hash,
                {"events": system_events, "references": []},
            )
            system_set["references"].append(
                {
                    "mapping_path": str(mapping_path),
                    "comparison_id": comparison["comparison_id"],
                    "system_name": comparison["system_name"],
                    "system_is_a": bool(comparison["system_is_a"]),
                    "unresolved_golden_ids": comparison["unresolved_golden_ids"],
                    "unresolved_system_ids": comparison["unresolved_system_ids"],
                    "output_path": comparison["output_path"],
                }
            )

    segment_mappings = []
    total_unique_system_sets = 0
    for segment_id, segment in sorted(by_segment.items()):
        anchor_set_id = "set_" + sha256_text(f"{seed}|{segment_id}|anchor")[:10]
        rendered_anchor = {
            "set_id": anchor_set_id,
            "events": segment["golden_events"],
        }
        rendered_system_sets = []
        private_system_sets = {}
        used_set_ids = {anchor_set_id}
        for system_hash, values in sorted(segment["system_sets"].items()):
            set_id = "set_" + sha256_text(f"{seed}|{segment_id}|{system_hash}")[:10]
            if set_id in used_set_ids:
                raise ValueError("anonymous set ID collision")
            used_set_ids.add(set_id)
            rendered_system_sets.append({"set_id": set_id, "events": values["events"]})
            private_system_sets[set_id] = values
        rendered_system_sets.sort(
            key=lambda item: sha256_text(f"{seed}|presentation|{segment_id}|{item['set_id']}")
        )
        prompt_path = prompt_dir / f"{segment_id}.private.md"
        human_output_path = human_output_dir / f"{segment_id}.private.json"
        prompt = "\n\n".join(
            [
                "You are the blinded human source auditor for several anonymous event sets extracted from one source "
                "excerpt. The anchor is an anonymous comparison set, not guaranteed source truth. For every other set, "
                "align related events one-to-one against anchor events. Equivalent requires every material semantic "
                "field to agree, allowing harmless wording or aliases. Partial means a shared core event with a material "
                "merge, split, omission, or expansion. Non-equivalent includes wrong actor, attribution, stance, "
                "certainty, time, metric, negation, target, mechanism, event type, unsupported inference, or evidence. "
                "Omit unrelated one-sided pairs. Distinct analytical lenses may share evidence.",
                "Independently mark source support for every event in every set, including the anchor and one-sided "
                "events. Support requires exact evidence and a claim that does not exceed the excerpt. Use each event ID "
                "at most once per comparison. Set reviewer_kind to human and provide reviewer ID and review time. "
                f"Allowed mismatch fields: {', '.join(mismatch_fields)}.",
                "# Blinded consolidated packet\n"
                + json.dumps(
                    {
                        "segment_id": segment_id,
                        "source_excerpt": segment["source_excerpt"],
                        "anchor_set": rendered_anchor,
                        "comparison_sets": rendered_system_sets,
                    },
                    ensure_ascii=True,
                    separators=(",", ":"),
                ),
            ]
        ) + "\n"
        write_text_atomic(prompt_path, prompt)
        segment_mappings.append(
            {
                "segment_id": segment_id,
                "anchor_set_id": anchor_set_id,
                "anchor_event_ids": [int(event["id"]) for event in segment["golden_events"]],
                "system_sets": {
                    set_id: {
                        "event_ids": [int(event["id"]) for event in values["events"]],
                        "references": values["references"],
                    }
                    for set_id, values in private_system_sets.items()
                },
                "prompt_path": str(prompt_path),
                "human_output_path": str(human_output_path),
            }
        )
        total_unique_system_sets += len(private_system_sets)
    mapping = {
        "schema_version": CONSOLIDATED_ADJUDICATION_VERSION,
        "source_mapping_paths": [str(path) for path, _payload in loaded_mappings],
        "seed": seed,
        "mismatch_fields": mismatch_fields,
        "schema_path": str(schema_path),
        "segments": segment_mappings,
        "privacy": "private_mapping_prompts_contain_source_text_and_blinded_event_sets",
    }
    mapping_path = root / "mapping.private.json"
    write_text_atomic(mapping_path, json.dumps(mapping, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
    return {
        "ok": bool(segment_mappings),
        "mapping_path": str(mapping_path),
        "source_mappings": len(loaded_mappings),
        "original_human_comparisons": total_original_comparisons,
        "consolidated_segment_packets": len(segment_mappings),
        "unique_system_sets": total_unique_system_sets,
        "source_reads_saved": total_original_comparisons - len(segment_mappings),
        "privacy": "sanitized_counts_private_packets_contain_source_text",
    }


def materialize_consolidated_human_outputs(
    *,
    mapping_path: str | Path,
) -> dict[str, Any]:
    mapping_file = Path(mapping_path).expanduser().resolve()
    mapping = json.loads(mapping_file.read_text(encoding="utf-8"))
    if mapping.get("schema_version") != CONSOLIDATED_ADJUDICATION_VERSION:
        raise ValueError("invalid consolidated human adjudication mapping")
    allowed_fields = set(mapping["mismatch_fields"])
    pending = []
    generated_by_path: dict[str, dict[str, Any]] = {}
    reviewed_segments = 0
    for segment in mapping.get("segments") or []:
        output_path = Path(segment["human_output_path"])
        if not _json_file_ok(output_path):
            pending.append(segment["segment_id"])
            continue
        payload = json.loads(output_path.read_text(encoding="utf-8"))
        if (
            not isinstance(payload, dict)
            or payload.get("reviewer_kind") != "human"
            or not str(payload.get("reviewer_id") or "").strip()
            or not str(payload.get("reviewed_at") or "").strip()
            or payload.get("segment_id") != segment["segment_id"]
            or not isinstance(payload.get("support"), list)
            or not isinstance(payload.get("comparisons"), list)
        ):
            raise ValueError(f"invalid consolidated human output for {segment['segment_id']}")
        valid_ids = {
            segment["anchor_set_id"]: set(segment["anchor_event_ids"]),
            **{
                set_id: set(values["event_ids"])
                for set_id, values in segment["system_sets"].items()
            },
        }
        support = {}
        for item in payload["support"]:
            if (
                not isinstance(item, dict)
                or set(item) != {"set_id", "event_id", "supported"}
                or item.get("set_id") not in valid_ids
                or item.get("event_id") not in valid_ids[item["set_id"]]
                or (item["set_id"], item["event_id"]) in support
                or not isinstance(item.get("supported"), bool)
            ):
                raise ValueError(f"invalid consolidated support row for {segment['segment_id']}")
            support[(item["set_id"], item["event_id"])] = bool(item["supported"])
        expected_support_keys = {
            (set_id, event_id) for set_id, event_ids in valid_ids.items() for event_id in event_ids
        }
        if set(support) != expected_support_keys:
            raise ValueError(f"incomplete consolidated support for {segment['segment_id']}")
        comparisons = {}
        for item in payload["comparisons"]:
            if (
                not isinstance(item, dict)
                or set(item) != {"set_id", "pairs"}
                or item.get("set_id") not in segment["system_sets"]
                or item["set_id"] in comparisons
                or not isinstance(item.get("pairs"), list)
            ):
                raise ValueError(f"invalid consolidated comparison for {segment['segment_id']}")
            used_anchor = set()
            used_system = set()
            pairs = []
            for pair in item["pairs"]:
                fields = pair.get("mismatch_fields") if isinstance(pair, dict) else None
                if (
                    not isinstance(pair, dict)
                    or set(pair) != {"anchor_id", "event_id", "relation", "mismatch_fields"}
                    or pair.get("anchor_id") not in valid_ids[segment["anchor_set_id"]]
                    or pair.get("event_id") not in valid_ids[item["set_id"]]
                    or pair["anchor_id"] in used_anchor
                    or pair["event_id"] in used_system
                    or pair.get("relation") not in {"equivalent", "partial", "non_equivalent"}
                    or not isinstance(fields, list)
                    or any(field not in allowed_fields for field in fields)
                    or len(fields) != len(set(fields))
                    or (pair["relation"] == "equivalent" and fields)
                ):
                    raise ValueError(f"invalid consolidated pair for {segment['segment_id']}")
                used_anchor.add(pair["anchor_id"])
                used_system.add(pair["event_id"])
                pairs.append(pair)
            comparisons[item["set_id"]] = pairs
        if set(comparisons) != set(segment["system_sets"]):
            raise ValueError(f"incomplete consolidated comparisons for {segment['segment_id']}")

        for set_id, values in segment["system_sets"].items():
            for reference in values["references"]:
                allowed_golden = set(reference["unresolved_golden_ids"])
                allowed_system = set(reference["unresolved_system_ids"])
                translated_pairs = []
                for pair in comparisons[set_id]:
                    if pair["anchor_id"] not in allowed_golden or pair["event_id"] not in allowed_system:
                        continue
                    translated_pairs.append(
                        {
                            "a_id": pair["event_id"] if reference["system_is_a"] else pair["anchor_id"],
                            "b_id": pair["anchor_id"] if reference["system_is_a"] else pair["event_id"],
                            "relation": pair["relation"],
                            "mismatch_fields": pair["mismatch_fields"],
                        }
                    )
                support_anchor = [
                    {"id": event_id, "supported": support[(segment["anchor_set_id"], event_id)]}
                    for event_id in sorted(allowed_golden)
                ]
                support_system = [
                    {"id": event_id, "supported": support[(set_id, event_id)]}
                    for event_id in sorted(allowed_system)
                ]
                materialized = {
                    "reviewer_kind": "human",
                    "reviewer_id": str(payload["reviewer_id"]),
                    "reviewed_at": str(payload["reviewed_at"]),
                    "pairs": translated_pairs,
                    "support_a": support_system if reference["system_is_a"] else support_anchor,
                    "support_b": support_anchor if reference["system_is_a"] else support_system,
                }
                destination = str(Path(reference["output_path"]).expanduser().resolve())
                prior = generated_by_path.get(destination)
                if prior is not None and prior != materialized:
                    raise ValueError("consolidated audit produced conflicting shared outputs")
                generated_by_path[destination] = materialized
        reviewed_segments += 1
    for destination, payload in generated_by_path.items():
        write_text_atomic(
            Path(destination),
            json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        )
    return {
        "ok": not pending,
        "reviewed_segments": reviewed_segments,
        "pending_segments": pending,
        "materialized_comparison_outputs": len(generated_by_path),
        "privacy": "sanitized_counts_private_human_outputs_materialized",
    }


def _event_f1(*, golden: int, system: int, equivalent: int) -> float:
    if golden == 0 and system == 0:
        return 1.0
    if golden == 0 or system == 0 or equivalent == 0:
        return 0.0
    precision = equivalent / system
    recall = equivalent / golden
    return 2 * precision * recall / (precision + recall)


def paired_bootstrap_full_field(
    rows: list[dict[str, Any]],
    *,
    iterations: int = 10000,
    confidence: float = 0.95,
    seed: str = "windowed-paired-bootstrap-v1",
) -> dict[str, Any]:
    if not rows:
        raise ValueError("paired bootstrap requires at least one row")
    if iterations < 1 or not 0 < confidence < 1:
        raise ValueError("invalid paired bootstrap configuration")

    def row_f1(row: dict[str, Any], system: str) -> float:
        if row.get(f"{system}_terminal_failure"):
            return 0.0
        explicit = row.get(f"{system}_f1")
        if isinstance(explicit, (int, float)):
            return float(explicit)
        return _event_f1(
            golden=int(row["golden_events"]),
            system=int(row[f"{system}_events"]),
            equivalent=int(row[f"{system}_equivalent"]),
        )

    by_source: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_density_source: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for row in rows:
        density = str(row.get("density_stratum") or "all")
        source = str(row.get("source_id") or row.get("source_name") or row.get("segment_id"))
        by_source[source].append(row)
        by_density_source[density][source].append(row)
    density_weights = {
        density: sum(len(cluster_rows) for cluster_rows in clusters.values()) / len(rows)
        for density, clusters in by_density_source.items()
    }

    def stratified_cluster_statistic(source_multiplicity: Counter[str] | None, system: str) -> float:
        total = 0.0
        for density, clusters in by_density_source.items():
            density_rows = []
            for source, cluster_rows in clusters.items():
                repeats = source_multiplicity[source] if source_multiplicity is not None else 1
                for _repeat in range(repeats):
                    density_rows.extend(cluster_rows)
            if not density_rows:
                raise ValueError(f"cluster bootstrap replicate omitted density stratum {density}")
            density_mean = sum(row_f1(row, system) for row in density_rows) / len(density_rows)
            total += density_weights[density] * density_mean
        return total

    baseline_f1 = stratified_cluster_statistic(None, "baseline")
    candidate_f1 = stratified_cluster_statistic(None, "candidate")
    rng = random.Random(int(sha256_text(seed)[:16], 16))
    source_ids = sorted(by_source)
    deltas = []
    for _iteration in range(iterations):
        for _draw_attempt in range(100):
            source_multiplicity = Counter(
                source_ids[rng.randrange(len(source_ids))] for _index in range(len(source_ids))
            )
            if all(
                any(source_multiplicity[source] for source in clusters)
                for clusters in by_density_source.values()
            ):
                break
        else:
            raise ValueError("could not draw source clusters covering every density stratum")
        deltas.append(
            stratified_cluster_statistic(source_multiplicity, "candidate")
            - stratified_cluster_statistic(source_multiplicity, "baseline")
        )
    deltas.sort()

    def quantile(probability: float) -> float:
        position = probability * (len(deltas) - 1)
        lower = int(position)
        upper = min(lower + 1, len(deltas) - 1)
        fraction = position - lower
        return deltas[lower] * (1 - fraction) + deltas[upper] * fraction

    alpha = (1 - confidence) / 2
    return {
        "segments": len(rows),
        "baseline_f1": round(baseline_f1, 6),
        "candidate_f1": round(candidate_f1, 6),
        "candidate_minus_baseline": round(candidate_f1 - baseline_f1, 6),
        "confidence": confidence,
        "iterations": iterations,
        "method": "paired_source_clustered_density_stratified",
        "density_strata": sorted(by_density_source),
        "source_clusters": len(by_source),
        "source_clusters_by_density": {
            density: len(clusters) for density, clusters in sorted(by_density_source.items())
        },
        "ci_lower": round(quantile(alpha), 6),
        "ci_upper": round(quantile(1 - alpha), 6),
        "seed": seed,
    }


def score_blinded_paired_adjudication(
    *,
    mapping_path: str | Path,
    output_path: str | Path | None = None,
    acceptance_spec_path: str | Path = DEFAULT_EVALUATOR_SPEC_PATH,
) -> dict[str, Any]:
    mapping_file = Path(mapping_path).expanduser().resolve()
    mapping = json.loads(mapping_file.read_text(encoding="utf-8"))
    if mapping.get("schema_version") != PAIRED_ADJUDICATION_MANIFEST_VERSION:
        raise ValueError("invalid paired adjudication mapping")
    acceptance = load_windowed_evaluator_spec(acceptance_spec_path)
    allowed_mismatch_fields = set(load_judge_calibration_fixture()["mismatch_fields"])
    by_segment: dict[str, dict[str, Any]] = {}
    pending = []
    invalid = []
    human_review_provenance = []
    for comparison in mapping.get("comparisons") or []:
        row = by_segment.setdefault(
            comparison["segment_id"],
            {
                "segment_id": comparison["segment_id"],
                "source_id": comparison.get("source_id"),
                "source_name": comparison.get("source_name"),
                "density_stratum": comparison.get("density_stratum"),
            },
        )
        system = comparison["system_name"]
        terminal_failure = bool(comparison.get("terminal_failure"))
        row[f"{system}_terminal_failure"] = terminal_failure
        golden_families = {
            int(key): str(value) for key, value in comparison["golden_event_families"].items()
        }
        system_families = {
            int(key): str(value) for key, value in comparison["system_event_families"].items()
        }
        exact_pairs = [tuple(pair) for pair in comparison.get("exact_pairs") or []]
        if terminal_failure:
            golden_family_counts = Counter(golden_families.values())
            row[f"{system}_events"] = 0
            row[f"{system}_equivalent"] = 0
            row[f"{system}_effective_golden_events"] = comparison["golden_count"]
            row[f"{system}_f1"] = 0.0
            row[f"{system}_family_scores"] = {
                family: {
                    "golden_events": count,
                    "system_events": 0,
                    "matched_golden": 0,
                    "matched_system": 0,
                }
                for family, count in golden_family_counts.items()
            }
            continue

        allowed_golden = set(comparison["unresolved_golden_ids"])
        allowed_system = set(comparison["unresolved_system_ids"])
        equivalent_pairs = list(exact_pairs)
        paired_golden = set()
        paired_system = set()
        support_golden = set(allowed_golden)
        support_system = set(allowed_system)
        if comparison.get("requires_human"):
            human_path = Path(comparison["output_path"])
            if not _json_file_ok(human_path):
                pending.append(comparison["comparison_id"])
                continue
            payload = json.loads(human_path.read_text(encoding="utf-8"))
            if (
                not isinstance(payload, dict)
                or payload.get("reviewer_kind") != "human"
                or not str(payload.get("reviewer_id") or "").strip()
                or not str(payload.get("reviewed_at") or "").strip()
            ):
                invalid.append(comparison["comparison_id"])
                continue
            pairs = payload.get("pairs")
            support_a = payload.get("support_a")
            support_b = payload.get("support_b")
            if not all(isinstance(value, list) for value in (pairs, support_a, support_b)):
                invalid.append(comparison["comparison_id"])
                continue

            def parse_support(items: list[dict[str, Any]], expected_ids: set[int]) -> set[int] | None:
                seen = set()
                supported = set()
                for item in items:
                    if (
                        not isinstance(item, dict)
                        or set(item) != {"id", "supported"}
                        or item.get("id") not in expected_ids
                        or item["id"] in seen
                        or not isinstance(item.get("supported"), bool)
                    ):
                        return None
                    seen.add(item["id"])
                    if item["supported"]:
                        supported.add(item["id"])
                return supported if seen == expected_ids else None

            expected_a = allowed_system if comparison["system_is_a"] else allowed_golden
            expected_b = allowed_golden if comparison["system_is_a"] else allowed_system
            supported_a = parse_support(support_a, expected_a)
            supported_b = parse_support(support_b, expected_b)
            if supported_a is None or supported_b is None:
                invalid.append(comparison["comparison_id"])
                continue
            support_golden, support_system = (
                (supported_b, supported_a)
                if comparison["system_is_a"]
                else (supported_a, supported_b)
            )
            valid = True
            for pair in pairs:
                if not isinstance(pair, dict):
                    valid = False
                    break
                a_id = pair.get("a_id")
                b_id = pair.get("b_id")
                golden_id, system_id = (
                    (b_id, a_id) if comparison["system_is_a"] else (a_id, b_id)
                )
                fields = pair.get("mismatch_fields")
                relation = pair.get("relation")
                if (
                    golden_id not in allowed_golden
                    or system_id not in allowed_system
                    or golden_id in paired_golden
                    or system_id in paired_system
                    or relation not in {"equivalent", "partial", "non_equivalent"}
                    or not isinstance(fields, list)
                    or any(field not in allowed_mismatch_fields for field in fields)
                    or (relation == "equivalent" and fields)
                    or (relation == "equivalent" and (golden_id not in support_golden or system_id not in support_system))
                ):
                    valid = False
                    break
                paired_golden.add(golden_id)
                paired_system.add(system_id)
                if relation == "equivalent":
                    equivalent_pairs.append((golden_id, system_id))
            if not valid:
                invalid.append(comparison["comparison_id"])
                continue
            human_review_provenance.append(
                {
                    "comparison_id": comparison["comparison_id"],
                    "reviewer_id": str(payload["reviewer_id"]),
                    "reviewed_at": str(payload["reviewed_at"]),
                }
            )

        exact_golden = {gold_id for gold_id, _system_id in exact_pairs}
        novel_supported_system = support_system - paired_system
        effective_golden_ids = exact_golden | support_golden
        equivalent_count = len(equivalent_pairs)
        effective_golden_count = len(effective_golden_ids)
        system_count = int(comparison["system_count"]) - len(novel_supported_system)
        row[f"{system}_events"] = system_count
        row[f"{system}_raw_events"] = int(comparison["system_count"])
        row[f"{system}_equivalent"] = equivalent_count
        row[f"{system}_effective_golden_events"] = effective_golden_count
        row[f"{system}_source_supported_unscored"] = len(novel_supported_system)
        row[f"{system}_f1"] = round(
            _event_f1(
                golden=effective_golden_count,
                system=system_count,
                equivalent=equivalent_count,
            ),
            6,
        )
        golden_counts = Counter(golden_families[event_id] for event_id in effective_golden_ids)
        system_counts = Counter(system_families.values())
        supported_unscored = Counter(system_families[system_id] for system_id in novel_supported_system)
        for family, count in supported_unscored.items():
            system_counts[family] -= count
        matched_golden = Counter(golden_families[gold_id] for gold_id, _system_id in equivalent_pairs)
        matched_system = Counter(system_families[system_id] for _gold_id, system_id in equivalent_pairs)
        families = (
            set(golden_counts)
            | set(system_counts)
            | set(matched_golden)
            | set(matched_system)
            | set(supported_unscored)
        )
        row[f"{system}_family_scores"] = {
            family: {
                "golden_events": golden_counts[family],
                "system_events": system_counts[family],
                "matched_golden": matched_golden[family],
                "matched_system": matched_system[family],
                "source_supported_unscored": supported_unscored[family],
            }
            for family in sorted(families)
        }
    complete_rows = [
        row
        for row in by_segment.values()
        if all(
            key in row
            for key in (
                "baseline_events",
                "baseline_f1",
                "candidate_events",
                "candidate_f1",
            )
        )
    ]
    bootstrap = None
    if not pending and not invalid and len(complete_rows) == len(by_segment):
        bootstrap_spec = acceptance["bootstrap"]
        bootstrap = paired_bootstrap_full_field(
            complete_rows,
            iterations=int(bootstrap_spec["iterations"]),
            confidence=float(bootstrap_spec["confidence"]),
            seed=str(bootstrap_spec["seed"]),
        )

    def macro(field: str) -> dict[str, Any]:
        groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in complete_rows:
            values = row[field] if isinstance(row[field], list) else [row[field]]
            for value in values:
                groups[str(value)].append(row)
        result = {}
        for value, rows in sorted(groups.items()):
            baseline_mean = sum(row["baseline_f1"] for row in rows) / len(rows)
            candidate_mean = sum(row["candidate_f1"] for row in rows) / len(rows)
            result[value] = {
                "segments": len(rows),
                "baseline_f1": round(baseline_mean, 6),
                "candidate_f1": round(candidate_mean, 6),
                "candidate_minus_baseline": round(candidate_mean - baseline_mean, 6),
            }
        return result

    def event_family_scores() -> dict[str, Any]:
        totals: dict[str, dict[str, Counter[str]]] = {
            "baseline": defaultdict(Counter),
            "candidate": defaultdict(Counter),
        }
        segment_counts: dict[str, Counter[str]] = {
            "baseline": Counter(),
            "candidate": Counter(),
        }
        for row in complete_rows:
            for system in ("baseline", "candidate"):
                for family, values in row[f"{system}_family_scores"].items():
                    totals[system][family].update(values)
                    if values.get("golden_events", 0) or values.get("system_events", 0):
                        segment_counts[system][family] += 1
        families = set(totals["baseline"]) | set(totals["candidate"])
        result = {}
        for family in sorted(families):
            item = {}
            for system in ("baseline", "candidate"):
                values = totals[system][family]
                precision = _ratio(values["matched_system"], values["system_events"])
                recall = _ratio(values["matched_golden"], values["golden_events"])
                f1 = (
                    2 * precision * recall / (precision + recall)
                    if precision is not None and recall is not None and precision + recall
                    else 1.0
                    if values["golden_events"] == 0 and values["system_events"] == 0
                    else 0.0
                )
                item[system] = {
                    **dict(values),
                    "segments": segment_counts[system][family],
                    "precision": precision,
                    "recall": recall,
                    "f1": round(f1, 6),
                }
            item["candidate_minus_baseline"] = round(
                item["candidate"]["f1"] - item["baseline"]["f1"], 6
            )
            result[family] = item
        return result

    report = {
        "schema_version": PAIRED_ADJUDICATION_SCORE_VERSION,
        "mapping_path": str(mapping_file),
        "path_name": mapping.get("path_name"),
        "core_variant": mapping.get("core_variant", "unrepaired"),
        "complete": not pending and not invalid and len(complete_rows) * 2 == len(mapping.get("comparisons") or []),
        "intent_to_treat": True,
        "adjudication_method": "exact_identity_plus_blinded_human_source_audit",
        "llm_judge_used": False,
        "human_review_provenance": human_review_provenance,
        "human_reviews_validated": len(human_review_provenance),
        "pending_comparisons": pending,
        "invalid_comparisons": invalid,
        "scored_segments": len(complete_rows),
        "bootstrap": bootstrap,
        "macro_by_source": macro("source_name"),
        "macro_by_density": macro("density_stratum"),
        "event_level_by_family": event_family_scores(),
        "rows": complete_rows,
        "privacy": "sanitized_scores_no_source_text",
    }
    destination = (
        Path(output_path).expanduser().resolve()
        if output_path is not None
        else mapping_file.parent / "score-report.json"
    )
    write_text_atomic(destination, json.dumps(report, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
    return {**report, "report_path": str(destination)}


def _sha256_file(path: str | Path) -> str:
    return sha256_text(Path(path).expanduser().resolve().read_text(encoding="utf-8"))


def _normalize_rollout_usage(value: Any) -> dict[str, int] | None:
    if not isinstance(value, dict):
        return None
    required = ("input_tokens", "output_tokens", "total_tokens")
    if any(isinstance(value.get(field), bool) or not isinstance(value.get(field), int) for field in required):
        return None
    usage = {
        "input_tokens": int(value["input_tokens"]),
        "cached_input_tokens": int(value.get("cached_input_tokens") or 0),
        "output_tokens": int(value["output_tokens"]),
        "reasoning_output_tokens": int(value.get("reasoning_output_tokens") or 0),
        "total_tokens": int(value["total_tokens"]),
    }
    if any(count < 0 for count in usage.values()):
        return None
    if usage["cached_input_tokens"] > usage["input_tokens"]:
        return None
    if usage["reasoning_output_tokens"] > usage["output_tokens"]:
        return None
    if usage["total_tokens"] != usage["input_tokens"] + usage["output_tokens"]:
        return None
    return usage


def _rollout_completion_matches(
    path: Path,
    *,
    target_output_hashes: set[str],
    target_prompt_hashes: set[str],
    target_run_ids: set[str],
) -> list[dict[str, Any]]:
    marker = "You are the GPT-5.5 full-episode context reader for ai_discourse_v3_1."
    marker_seen = False
    session_id = None
    source = None
    cwd = None
    model = None
    latest_usage = None
    referenced_run_ids = set()
    matched_prompt_sha256s = set()
    matches = []
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            if marker in raw_line:
                marker_seen = True
            for run_id in target_run_ids:
                if run_id in raw_line:
                    referenced_run_ids.add(run_id)
            if '"type"' not in raw_line:
                continue
            try:
                event = json.loads(raw_line)
            except json.JSONDecodeError:
                continue
            event_type = event.get("type")
            payload = event.get("payload") or {}
            if event_type == "session_meta":
                session_id = payload.get("session_id") or payload.get("id")
                source = payload.get("source")
                cwd = payload.get("cwd")
                continue
            if event_type == "turn_context":
                model = payload.get("model") or model
                cwd = payload.get("cwd") or cwd
                continue
            if (
                event_type == "response_item"
                and payload.get("type") == "message"
                and payload.get("role") == "user"
            ):
                input_text = "".join(
                    str(item.get("text") or "")
                    for item in payload.get("content") or []
                    if item.get("type") == "input_text"
                )
                prompt_sha256 = sha256_text(input_text)
                if prompt_sha256 in target_prompt_hashes:
                    matched_prompt_sha256s.add(prompt_sha256)
                continue
            if event_type != "event_msg":
                continue
            if payload.get("type") == "token_count":
                latest_usage = _normalize_rollout_usage(
                    ((payload.get("info") or {}).get("total_token_usage"))
                )
                continue
            if payload.get("type") != "task_complete":
                continue
            last_message = payload.get("last_agent_message")
            if not isinstance(last_message, str):
                continue
            task_complete_message_sha256 = sha256_text(last_message)
            if (
                task_complete_message_sha256 not in target_output_hashes
                and not matched_prompt_sha256s
                and not referenced_run_ids
            ):
                continue
            matches.append(
                {
                    "task_complete_message_sha256": task_complete_message_sha256,
                    "matched_prompt_sha256s": sorted(matched_prompt_sha256s),
                    "referenced_run_ids": sorted(referenced_run_ids),
                    "usage": latest_usage,
                    "session_id": session_id,
                    "source": source,
                    "cwd": cwd,
                    "model": model,
                    "task_completed_at": event.get("timestamp"),
                    "duration_ms": payload.get("duration_ms"),
                    "time_to_first_token_ms": payload.get("time_to_first_token_ms"),
                }
            )
    if not marker_seen:
        return []
    return matches


def recover_historical_episode_context_usage(
    conn,
    *,
    context_cost_report_path: str | Path,
    rollout_roots: list[str | Path],
    output_path: str | Path,
    expected_usage: dict[str, int] | None = None,
) -> dict[str, Any]:
    context_cost_file = Path(context_cost_report_path).expanduser().resolve()
    context_cost = json.loads(context_cost_file.read_text(encoding="utf-8"))
    historical_runs = context_cost.get("historical_context_runs") or []
    if context_cost.get("schema_version") != PAIRED_CONTEXT_COST_SCHEMA_VERSION:
        raise ValueError("invalid paired context-cost report")
    if not historical_runs:
        raise ValueError("context-cost report has no historical context runs")
    run_ids = [str(item["run_id"]) for item in historical_runs]
    if len(set(run_ids)) != len(run_ids):
        raise ValueError("context-cost report repeats an episode-context run")

    rows_by_id = {}
    output_hash_to_run_id = {}
    prompt_hash_to_run_id = {}
    source_items = {str(item["run_id"]): item for item in historical_runs}
    created_values = []
    completed_values = []
    artifact_errors = []
    for run_id in run_ids:
        row = conn.execute(
            "SELECT * FROM episode_context_runs WHERE id = ? AND status = 'completed'",
            (run_id,),
        ).fetchone()
        if not row:
            artifact_errors.append({"run_id": run_id, "error": "completed_run_missing"})
            continue
        output_file = Path(row["output_path"]).expanduser().resolve()
        if not output_file.exists():
            artifact_errors.append({"run_id": run_id, "error": "output_missing"})
            continue
        output_sha256 = _sha256_file(output_file)
        expected_output_sha256 = source_items[run_id].get("output_sha256")
        if expected_output_sha256 and output_sha256 != expected_output_sha256:
            artifact_errors.append({"run_id": run_id, "error": "output_hash_changed"})
            continue
        if output_sha256 in output_hash_to_run_id:
            raise ValueError("two context runs have identical final-output hashes")
        prompt_sha256 = str(source_items[run_id]["prompt_sha256"])
        if prompt_sha256 in prompt_hash_to_run_id:
            raise ValueError("two context runs have identical prompt hashes")
        rows_by_id[run_id] = row
        output_hash_to_run_id[output_sha256] = run_id
        prompt_hash_to_run_id[prompt_sha256] = run_id
        created_values.append(datetime.fromisoformat(str(row["created_at"])))
        completed_values.append(datetime.fromisoformat(str(row["completed_at"])))

    if not created_values or not completed_values:
        raise ValueError("no valid historical context artifacts were found")
    existing_sidecars = {}
    for run_id, row in rows_by_id.items():
        sidecar_path = _context_usage_sidecar_path(row)
        if not sidecar_path.exists():
            continue
        try:
            sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            artifact_errors.append({"run_id": run_id, "error": "usage_sidecar_unreadable"})
            continue
        usage = _normalize_rollout_usage(sidecar.get("usage"))
        expected_output_sha256 = source_items[run_id]["output_sha256"]
        if (
            sidecar.get("schema_version") != HISTORICAL_CONTEXT_USAGE_VERSION
            or sidecar.get("state") != "recovered_completed"
            or not sidecar.get("accounting_complete")
            or sidecar.get("output_sha256") != expected_output_sha256
            or usage is None
        ):
            artifact_errors.append({"run_id": run_id, "error": "usage_sidecar_invalid"})
            continue
        existing_sidecars[run_id] = {
            "payload": sidecar,
            "path": sidecar_path,
            "usage": usage,
        }
    pending_run_ids = set(run_ids) - set(existing_sidecars)
    first_date = (min(created_values) - timedelta(days=1)).date()
    last_date = (max(completed_values) + timedelta(days=1)).date()
    date_prefixes = set()
    cursor = first_date
    while cursor <= last_date:
        date_prefixes.add(f"rollout-{cursor.isoformat()}T")
        cursor += timedelta(days=1)

    candidate_paths = set()
    if pending_run_ids:
        for root_value in rollout_roots:
            root = Path(root_value).expanduser().resolve()
            if root.is_file():
                if root.suffix == ".jsonl":
                    candidate_paths.add(root)
                continue
            if not root.exists():
                continue
            for path in root.rglob("*.jsonl"):
                if any(path.name.startswith(prefix) for prefix in date_prefixes):
                    candidate_paths.add(path.resolve())

    matches_by_run_id: dict[str, dict[str, Any]] = {}
    duplicate_matches = []
    context_rollouts_scanned = 0
    for rollout_path in sorted(candidate_paths):
        matches = _rollout_completion_matches(
            rollout_path,
            target_output_hashes={
                output_hash
                for output_hash, run_id in output_hash_to_run_id.items()
                if run_id in pending_run_ids
            },
            target_prompt_hashes={
                prompt_hash
                for prompt_hash, run_id in prompt_hash_to_run_id.items()
                if run_id in pending_run_ids
            },
            target_run_ids=pending_run_ids,
        )
        if matches:
            context_rollouts_scanned += 1
        for match in matches:
            task_message_hash = match["task_complete_message_sha256"]
            run_id = output_hash_to_run_id.get(task_message_hash)
            match_method = "exact_terminal_output_sha256" if run_id is not None else None
            if run_id is None:
                prompt_run_ids = {
                    prompt_hash_to_run_id[prompt_hash]
                    for prompt_hash in match.get("matched_prompt_sha256s") or []
                }
                if len(prompt_run_ids) == 1:
                    run_id = prompt_run_ids.pop()
                    match_method = "exact_input_prompt_sha256"
            if run_id is None:
                in_window = []
                task_completed_at = match.get("task_completed_at")
                try:
                    completed_at = datetime.fromisoformat(str(task_completed_at).replace("Z", "+00:00"))
                except ValueError:
                    completed_at = None
                if completed_at is not None:
                    for referenced_run_id in match.get("referenced_run_ids") or []:
                        row = rows_by_id.get(referenced_run_id)
                        if row is None:
                            continue
                        started_bound = datetime.fromisoformat(str(row["created_at"])) - timedelta(minutes=10)
                        finished_bound = datetime.fromisoformat(str(row["completed_at"])) + timedelta(minutes=10)
                        if started_bound <= completed_at <= finished_bound:
                            in_window.append(referenced_run_id)
                if len(in_window) == 1:
                    run_id = in_window[0]
                    match_method = "exact_run_id_reference_and_execution_time_window"
            if run_id is None:
                continue
            enriched = {
                **match,
                "match_method": match_method,
                "output_sha256": source_items[run_id]["output_sha256"],
                "rollout_path": str(rollout_path),
                "rollout_filename": rollout_path.name,
                "rollout_sha256": _sha256_file(rollout_path),
            }
            prior = matches_by_run_id.get(run_id)
            if prior is None:
                matches_by_run_id[run_id] = enriched
                continue
            same_session = prior.get("session_id") == enriched.get("session_id")
            same_usage = prior.get("usage") == enriched.get("usage")
            if not (same_session and same_usage):
                duplicate_matches.append(
                    {
                        "run_id": run_id,
                        "session_ids": [prior.get("session_id"), enriched.get("session_id")],
                    }
                )

    invalid_matches = []
    sidecars = []
    usage_items = []
    for run_id in run_ids:
        row = rows_by_id.get(run_id)
        source_item = source_items[run_id]
        existing = existing_sidecars.get(run_id)
        if existing is not None:
            sidecar = existing["payload"]
            sidecar_path = existing["path"]
            sidecars.append(
                {
                    "run_id": run_id,
                    "sidecar_path": str(sidecar_path),
                    "sidecar_sha256": _sha256_file(sidecar_path),
                    "rollout_session_id": sidecar.get("rollout_session_id"),
                    "rollout_sha256": sidecar.get("rollout_sha256"),
                }
            )
            usage_items.append(
                {
                    "usage": existing["usage"],
                    "evaluation_selected_segments": int(
                        source_item["evaluation_selected_segments"]
                    ),
                    "production_reuse_segments": int(source_item["production_reuse_segments"]),
                }
            )
            continue
        match = matches_by_run_id.get(run_id)
        if row is None or match is None:
            continue
        usage = match.get("usage")
        if usage is None:
            invalid_matches.append({"run_id": run_id, "error": "terminal_usage_missing_or_invalid"})
            continue
        if match.get("source") != "exec":
            invalid_matches.append({"run_id": run_id, "error": "rollout_source_not_exec"})
            continue
        if match.get("model") != "gpt-5.5":
            invalid_matches.append({"run_id": run_id, "error": "rollout_model_mismatch"})
            continue
        started = datetime.fromisoformat(str(row["created_at"]))
        completed = datetime.fromisoformat(str(row["completed_at"]))
        sidecar_path = _context_usage_sidecar_path(row)
        sidecar = {
            "schema_version": HISTORICAL_CONTEXT_USAGE_VERSION,
            "state": "recovered_completed",
            "episode_context_run_id": run_id,
            "episode_id": row["episode_id"],
            "model": match["model"],
            "usage": usage,
            "usage_complete": True,
            "accounting_complete": True,
            "status_ok": True,
            "usage_source": "codex_rollout_terminal_total_token_usage",
            "output_sha256": match["output_sha256"],
            "task_complete_message_sha256": match["task_complete_message_sha256"],
            "match_method": match["match_method"],
            "rollout_session_id": match.get("session_id"),
            "rollout_filename": match["rollout_filename"],
            "rollout_sha256": match["rollout_sha256"],
            "task_completed_at": match.get("task_completed_at"),
            "duration_ms": match.get("duration_ms"),
            "time_to_first_token_ms": match.get("time_to_first_token_ms"),
            "historical_orchestration_elapsed_seconds": round((completed - started).total_seconds(), 3),
            "recovery_reran_model": False,
            "recovered_at": now_iso(),
            "privacy": "usage_hashes_and_session_identity_no_prompt_transcript_response_or_credentials",
        }
        if sidecar_path.exists():
            existing = json.loads(sidecar_path.read_text(encoding="utf-8"))
            if (
                existing.get("schema_version") != HISTORICAL_CONTEXT_USAGE_VERSION
                or existing.get("output_sha256") != sidecar["output_sha256"]
                or existing.get("usage") != sidecar["usage"]
            ):
                raise ValueError(f"conflicting context usage sidecar: {sidecar_path}")
            sidecar = existing
        else:
            _write_context_usage_sidecar(sidecar_path, sidecar)
        sidecars.append(
            {
                "run_id": run_id,
                "sidecar_path": str(sidecar_path),
                "sidecar_sha256": _sha256_file(sidecar_path),
                "rollout_session_id": match.get("session_id"),
                "rollout_sha256": match["rollout_sha256"],
            }
        )
        usage_items.append(
            {
                "usage": usage,
                "evaluation_selected_segments": int(source_item["evaluation_selected_segments"]),
                "production_reuse_segments": int(source_item["production_reuse_segments"]),
            }
        )

    aggregate_usage = _sum_usage_dicts([item["usage"] for item in usage_items])
    usage_fields = tuple(aggregate_usage)
    production_amortized_usage = {
        field: math.ceil(
            sum(
                int(item["usage"][field])
                * item["evaluation_selected_segments"]
                / item["production_reuse_segments"]
                for item in usage_items
            )
        )
        for field in usage_fields
    }
    expected_usage = expected_usage or {}
    expected_mismatches = {
        field: {"expected": expected, "observed": aggregate_usage.get(field)}
        for field, expected in expected_usage.items()
        if aggregate_usage.get(field) != expected
    }
    recovered_run_ids = {item["run_id"] for item in sidecars}
    missing_run_ids = sorted(set(run_ids) - recovered_run_ids)
    complete = bool(
        len(sidecars) == len(run_ids)
        and not artifact_errors
        and not missing_run_ids
        and not invalid_matches
        and not duplicate_matches
        and not expected_mismatches
    )
    report = {
        "schema_version": HISTORICAL_CONTEXT_USAGE_REPORT_VERSION,
        "generated_at": now_iso(),
        "context_cost_report_path": str(context_cost_file),
        "context_cost_report_sha256": _sha256_file(context_cost_file),
        "requested_runs": len(run_ids),
        "recovered_runs": len(sidecars),
        "candidate_rollouts_scanned": len(candidate_paths),
        "matching_context_rollouts_scanned": context_rollouts_scanned,
        "usage": aggregate_usage,
        "production_amortized_usage": production_amortized_usage,
        "expected_usage": expected_usage,
        "expected_usage_mismatches": expected_mismatches,
        "artifact_errors": artifact_errors,
        "missing_run_ids": missing_run_ids,
        "invalid_matches": invalid_matches,
        "duplicate_matches": duplicate_matches,
        "sidecars": sidecars,
        "recovery_reran_model": False,
        "ok": complete,
        "privacy": "usage_hashes_and_session_identity_no_prompt_transcript_response_or_credentials",
    }
    destination = Path(output_path).expanduser().resolve()
    write_text_atomic(destination, json.dumps(report, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
    return {**report, "report_path": str(destination)}


def build_paired_provenance_and_context_cost(
    conn,
    *,
    phase_one_report_path: str | Path,
    output_dir: str | Path,
    evaluator_spec_path: str | Path = DEFAULT_EVALUATOR_SPEC_PATH,
) -> dict[str, Any]:
    phase_one_file = Path(phase_one_report_path).expanduser().resolve()
    phase_one = json.loads(phase_one_file.read_text(encoding="utf-8"))
    evaluator = load_windowed_evaluator_spec(evaluator_spec_path)
    if phase_one.get("schema_version") != PAIRED_PHASE_ONE_SCHEMA_VERSION:
        raise ValueError("invalid paired phase-one report")
    if phase_one.get("acceptance_spec_sha256") != evaluator["parent_extraction_spec_sha256"]:
        raise ValueError("phase-one extraction hash does not match evaluator parent")
    manifest_file = Path(phase_one["manifest_path"])
    manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
    chunk_by_segment = {
        str((chunk.get("segment_ids") or [chunk["chunk_id"]])[0]): chunk
        for chunk in manifest.get("chunks") or []
    }
    requested_segment_ids = [
        str(segment_id)
        for block in phase_one.get("blocks") or []
        for segment_id in block.get("segment_ids") or []
    ]
    root = Path(output_dir).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    project_root = Path(__file__).resolve().parents[1]
    label_pack_root = project_root / "label_packs" / "ai_discourse_v3_1"
    evaluator_files = [Path(__file__).resolve(), Path(__file__).resolve().parent / "efficient_backtest.py"]
    baseline_static = {
        "prompt": _sha256_file(label_pack_root / "prompt.md"),
        "schema": _sha256_file(label_pack_root / "schema.json"),
        "codebook": _sha256_file(label_pack_root / "codebook.md"),
    }
    baseline_prompt_hashes = {}
    candidate_prompt_hashes = {}
    baseline_run_report_hashes = {}
    declared_baseline_preparation_failures = {}
    missing_artifacts = []
    for block in phase_one.get("blocks") or []:
        baseline_report = Path(block["baseline"]["report_path"])
        baseline_payload = json.loads(baseline_report.read_text(encoding="utf-8"))
        baseline_run_report_hashes[str(block["block_index"])] = _sha256_file(baseline_report)
        for failure in baseline_payload.get("preparation_failures") or []:
            declared_baseline_preparation_failures[str(failure["segment_id"])] = {
                "block_index": block["block_index"],
                "error_type": failure.get("error_type"),
            }
        baseline_prompt_dir = baseline_report.parent / "prompts"
        core_manifest = json.loads(Path(block["candidate_core"]["manifest_path"]).read_text(encoding="utf-8"))
        core_entries = {str(entry["segment_id"]): entry for entry in core_manifest.get("entries") or []}
        for segment_id in block.get("segment_ids") or []:
            segment_id = str(segment_id)
            baseline_prompt_path = baseline_prompt_dir / f"{segment_id}.private.md"
            core_entry = core_entries.get(segment_id)
            baseline_prompt_expected = segment_id not in declared_baseline_preparation_failures
            if (
                (baseline_prompt_expected and not baseline_prompt_path.exists())
                or not core_entry
                or not Path(core_entry["prompt_path"]).exists()
            ):
                missing_artifacts.append(segment_id)
                continue
            if baseline_prompt_path.exists():
                baseline_prompt_hashes[segment_id] = _sha256_file(baseline_prompt_path)
            candidate_prompt_hashes[segment_id] = _sha256_file(core_entry["prompt_path"])
    from .worker import completed_episode_context_for_segment

    try:
        import tiktoken

        context_encoding = tiktoken.get_encoding("o200k_base")
    except (ImportError, KeyError):
        context_encoding = None

    context_by_episode = {}
    selected_segments_by_episode = Counter(
        str(chunk_by_segment[segment_id]["episode_id"]) for segment_id in requested_segment_ids
    )
    golden_output_hashes = {}
    missing_context = []
    missing_golden = []
    for segment_id in requested_segment_ids:
        chunk = chunk_by_segment[segment_id]
        golden_row = conn.execute(
            "SELECT output_json FROM labels WHERE id = ? AND segment_id = ?",
            (chunk["golden_label_id"], segment_id),
        ).fetchone()
        if golden_row:
            golden_output_hashes[segment_id] = sha256_text(str(golden_row["output_json"]))
        else:
            missing_golden.append(segment_id)
        context_run = completed_episode_context_for_segment(
            conn,
            segment_id,
            label_pack="ai_discourse_v3_1",
            model="gpt-5.5",
        )
        if not context_run:
            missing_context.append(segment_id)
            continue
        artifact_path = Path(context_run["context_artifact_path"])
        prompt_path = Path(context_run["prompt_path"])
        output_path = Path(context_run["output_path"])
        if not all(path.exists() for path in (artifact_path, prompt_path, output_path)):
            missing_context.append(segment_id)
            continue
        episode_id = str(context_run["episode_id"])
        if episode_id not in context_by_episode:
            started = datetime.fromisoformat(str(context_run["created_at"]))
            completed = datetime.fromisoformat(str(context_run["completed_at"]))
            prompt_text = prompt_path.read_text(encoding="utf-8")
            output_text = output_path.read_text(encoding="utf-8")
            output_sha256 = _sha256_file(output_path)
            usage_sidecar_path = _context_usage_sidecar_path(context_run)
            usage_sidecar = None
            usage_sidecar_error = None
            if usage_sidecar_path.exists():
                try:
                    sidecar_payload = json.loads(usage_sidecar_path.read_text(encoding="utf-8"))
                    normalized_usage = _normalize_rollout_usage(sidecar_payload.get("usage"))
                    if sidecar_payload.get("state") not in {"completed", "recovered_completed"}:
                        usage_sidecar_error = "sidecar_not_completed"
                    elif not sidecar_payload.get("accounting_complete"):
                        usage_sidecar_error = "sidecar_accounting_incomplete"
                    elif normalized_usage is None:
                        usage_sidecar_error = "sidecar_usage_invalid"
                    elif sidecar_payload.get("output_sha256") not in {None, output_sha256}:
                        usage_sidecar_error = "sidecar_output_hash_mismatch"
                    else:
                        usage_sidecar = {
                            "schema_version": sidecar_payload.get("schema_version"),
                            "sha256": _sha256_file(usage_sidecar_path),
                            "usage_source": sidecar_payload.get("usage_source"),
                            "rollout_session_id": sidecar_payload.get("rollout_session_id"),
                            "usage": normalized_usage,
                        }
                except (OSError, json.JSONDecodeError):
                    usage_sidecar_error = "sidecar_unreadable"
            production_segment_count = int(
                conn.execute(
                    "SELECT COUNT(*) AS count FROM segments WHERE episode_id = ?",
                    (episode_id,),
                ).fetchone()["count"]
            )
            context_by_episode[episode_id] = {
                "run_id": context_run["id"],
                "artifact_sha256": _sha256_file(artifact_path),
                "prompt_sha256": _sha256_file(prompt_path),
                "output_sha256": output_sha256,
                "prompt_chars": prompt_path.stat().st_size,
                "output_chars": output_path.stat().st_size,
                "estimated_prompt_tokens_o200k": (
                    len(context_encoding.encode(prompt_text)) if context_encoding is not None else None
                ),
                "estimated_output_tokens_o200k": (
                    len(context_encoding.encode(output_text)) if context_encoding is not None else None
                ),
                "evaluation_selected_segments": selected_segments_by_episode[episode_id],
                "production_reuse_segments": production_segment_count,
                "historical_elapsed_seconds": round((completed - started).total_seconds(), 3),
                "usage_sidecar_path": str(usage_sidecar_path),
                "usage_sidecar_sha256": usage_sidecar.get("sha256") if usage_sidecar else None,
                "usage_sidecar_schema_version": (
                    usage_sidecar.get("schema_version") if usage_sidecar else None
                ),
                "usage_source": usage_sidecar.get("usage_source") if usage_sidecar else None,
                "rollout_session_id": (
                    usage_sidecar.get("rollout_session_id") if usage_sidecar else None
                ),
                "usage_sidecar_error": usage_sidecar_error,
                "exact_token_usage": usage_sidecar.get("usage") if usage_sidecar else None,
            }
    guideline_path = DEFAULT_WINDOWED_GUIDELINES_PATH.resolve()
    actual_guideline_sha256 = _sha256_file(guideline_path)
    window_parameters = {
        "window_count": 4,
        "context_chars": 900,
        "max_events": int(evaluator["candidate"]["max_events"]),
        "representation": evaluator["candidate"]["representation"],
    }
    batch_parameters = {
        "concurrency": int(evaluator["paired_run"]["concurrency"]),
        "retry_policy": evaluator["paired_run"]["retry_policy"],
        "enrichment_paths": evaluator["candidate"]["enrichment_paths"],
    }
    over_cap = [
        {
            "segment_id": segment_id,
            "golden_events": int(chunk_by_segment[segment_id]["expected_discourse_events"]),
            "candidate_cap": int(evaluator["candidate"]["max_events"]),
            "maximum_event_recall": round(
                int(evaluator["candidate"]["max_events"])
                / int(chunk_by_segment[segment_id]["expected_discourse_events"]),
                6,
            ),
        }
        for segment_id in requested_segment_ids
        if int(chunk_by_segment[segment_id]["expected_discourse_events"])
        > int(evaluator["candidate"]["max_events"])
    ]
    total_golden = sum(int(chunk_by_segment[sid]["expected_discourse_events"]) for sid in requested_segment_ids)
    capped_maximum = sum(
        min(
            int(chunk_by_segment[sid]["expected_discourse_events"]),
            int(evaluator["candidate"]["max_events"]),
        )
        for sid in requested_segment_ids
    )
    provenance = {
        "schema_version": PAIRED_PROVENANCE_SCHEMA_VERSION,
        "phase_one_report_path": str(phase_one_file),
        "manifest_sha256": _sha256_file(manifest_file),
        "extraction_spec_sha256": phase_one["acceptance_spec_sha256"],
        "evaluator_spec_sha256": _sha256_file(evaluator_spec_path),
        "evaluator_code": {str(path): _sha256_file(path) for path in evaluator_files},
        "baseline_static": baseline_static,
        "baseline_run_report_hashes": baseline_run_report_hashes,
        "declared_baseline_preparation_failures": declared_baseline_preparation_failures,
        "baseline_prompt_hashes": baseline_prompt_hashes,
        "candidate_core_prompt_hashes": candidate_prompt_hashes,
        "context_artifacts": {
            episode_id: {
                "run_id": item["run_id"],
                "artifact_sha256": item["artifact_sha256"],
                "prompt_sha256": item["prompt_sha256"],
                "output_sha256": item["output_sha256"],
                "usage_sidecar_sha256": item["usage_sidecar_sha256"],
                "usage_sidecar_schema_version": item["usage_sidecar_schema_version"],
                "usage_source": item["usage_source"],
                "rollout_session_id": item["rollout_session_id"],
            }
            for episode_id, item in sorted(context_by_episode.items())
        },
        "golden_output_hashes": golden_output_hashes,
        "candidate_guideline": {
            "path": str(guideline_path),
            "actual_sha256": actual_guideline_sha256,
            "manifest_sha256": manifest.get("candidate_guideline_sha256"),
            "verified": actual_guideline_sha256 == manifest.get("candidate_guideline_sha256"),
        },
        "window_parameters": window_parameters,
        "window_parameters_sha256": sha256_text(
            json.dumps(window_parameters, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        ),
        "batch_parameters": batch_parameters,
        "batch_parameters_sha256": sha256_text(
            json.dumps(batch_parameters, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        ),
        "structural_recall_ceiling": {
            "segments_above_cap": over_cap,
            "segment_count": len(over_cap),
            "aggregate_golden_events": total_golden,
            "aggregate_maximum_events_under_cap": capped_maximum,
            "aggregate_maximum_event_recall": _ratio(capped_maximum, total_golden),
        },
        "missing_artifacts": sorted(set(missing_artifacts)),
        "missing_context": sorted(set(missing_context)),
        "missing_golden": sorted(set(missing_golden)),
        "privacy": "hashes_and_counts_only",
    }
    provenance["complete"] = bool(
        not provenance["missing_artifacts"]
        and not provenance["missing_context"]
        and not provenance["missing_golden"]
        and len(baseline_prompt_hashes) + len(declared_baseline_preparation_failures)
        == len(requested_segment_ids)
        and len(candidate_prompt_hashes) == len(requested_segment_ids)
        and provenance["candidate_guideline"]["verified"]
    )
    provenance_path = root / "provenance-report.json"
    write_text_atomic(
        provenance_path,
        json.dumps(provenance, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
    )
    exact_usage_items = [
        item["exact_token_usage"]
        for item in context_by_episode.values()
        if item.get("exact_token_usage") is not None
    ]
    expected_unique_episodes = len(selected_segments_by_episode)
    exact_usage_available = bool(
        len(context_by_episode) == expected_unique_episodes
        and len(exact_usage_items) == expected_unique_episodes
        and all(item["production_reuse_segments"] > 0 for item in context_by_episode.values())
    )
    exact_context_usage = _sum_usage_dicts(exact_usage_items) if exact_usage_available else None
    exact_context_usage_production_amortized = (
        {
            field: math.ceil(
                sum(
                    int(item["exact_token_usage"][field])
                    * item["evaluation_selected_segments"]
                    / item["production_reuse_segments"]
                    for item in context_by_episode.values()
                )
            )
            for field in (
                "input_tokens",
                "cached_input_tokens",
                "output_tokens",
                "reasoning_output_tokens",
                "total_tokens",
            )
        }
        if exact_usage_available
        else None
    )
    missing_usage_sidecars = [
        {
            "episode_id": episode_id,
            "run_id": item["run_id"],
            "error": item.get("usage_sidecar_error") or "usage_sidecar_missing",
        }
        for episode_id, item in sorted(context_by_episode.items())
        if item.get("exact_token_usage") is None
    ]
    if len(context_by_episode) != expected_unique_episodes:
        fail_closed_reason = "one or more requested episodes have no completed context artifact"
    elif missing_usage_sidecars:
        fail_closed_reason = "one or more historical episode-context runs lack validated exact usage"
    else:
        fail_closed_reason = None
    context_cost = {
        "schema_version": PAIRED_CONTEXT_COST_SCHEMA_VERSION,
        "phase_one_report_path": str(phase_one_file),
        "shared_context_policy": "same_context_cost_added_to_both_systems_before_ratio",
        "unique_episodes": len(context_by_episode),
        "expected_unique_episodes": expected_unique_episodes,
        "segments": len(requested_segment_ids),
        "historical_context_runs": list(context_by_episode.values()),
        "historical_wall_seconds_sum": round(
            sum(item["historical_elapsed_seconds"] for item in context_by_episode.values()), 3
        ),
        "estimated_context_prompt_tokens_o200k": (
            sum(item["estimated_prompt_tokens_o200k"] for item in context_by_episode.values())
            if context_encoding is not None
            else None
        ),
        "estimated_context_output_tokens_o200k": (
            sum(item["estimated_output_tokens_o200k"] for item in context_by_episode.values())
            if context_encoding is not None
            else None
        ),
        "estimated_context_total_tokens_o200k": (
            sum(
                item["estimated_prompt_tokens_o200k"] + item["estimated_output_tokens_o200k"]
                for item in context_by_episode.values()
            )
            if context_encoding is not None
            else None
        ),
        "estimated_context_total_tokens_o200k_production_amortized": (
            math.ceil(
                sum(
                    (item["estimated_prompt_tokens_o200k"] + item["estimated_output_tokens_o200k"])
                    * item["evaluation_selected_segments"]
                    / item["production_reuse_segments"]
                    for item in context_by_episode.values()
                )
            )
            if context_encoding is not None
            and all(item["production_reuse_segments"] > 0 for item in context_by_episode.values())
            else None
        ),
        "historical_wall_seconds_production_amortized": round(
            sum(
                item["historical_elapsed_seconds"]
                * item["evaluation_selected_segments"]
                / item["production_reuse_segments"]
                for item in context_by_episode.values()
            ),
            3,
        ),
        "production_amortization_policy": (
            "allocate each one-time episode-context run across every prepared segment in that episode; "
            "apply the identical allocation to baseline and candidate"
        ),
        "estimated_context_tokenizer": "o200k_base" if context_encoding is not None else None,
        "estimated_context_usage_scope": "persisted_user_prompt_plus_final_output_only_excludes_system_reasoning_retries",
        "exact_context_usage": exact_context_usage,
        "exact_context_tokens": (
            exact_context_usage["total_tokens"] if exact_context_usage is not None else None
        ),
        "exact_context_usage_production_amortized": exact_context_usage_production_amortized,
        "exact_context_tokens_production_amortized": (
            exact_context_usage_production_amortized["total_tokens"]
            if exact_context_usage_production_amortized is not None
            else None
        ),
        "exact_usage_available": exact_usage_available,
        "usage_sidecar_coverage": {
            "required": expected_unique_episodes,
            "validated": len(exact_usage_items),
            "missing_or_invalid": missing_usage_sidecars,
        },
        "marginal_extractor_cost_evaluable": True,
        "end_to_end_cost_evaluable": exact_usage_available,
        "fail_closed_reason": fail_closed_reason,
        "privacy": "aggregate_cost_and_hash_metadata_only",
    }
    context_cost_path = root / "context-cost-report.json"
    write_text_atomic(
        context_cost_path,
        json.dumps(context_cost, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
    )
    return {
        "ok": provenance["complete"],
        "provenance_report_path": str(provenance_path),
        "context_cost_report_path": str(context_cost_path),
        "provenance_complete": provenance["complete"],
        "end_to_end_cost_evaluable": exact_usage_available,
        "structural_segments_above_cap": len(over_cap),
        "privacy": "sanitized_hashes_and_counts_only",
    }


def export_no_signal_power_manifest(
    conn,
    *,
    output_path: str | Path,
    exclude_roots: list[str | Path] | None = None,
    count: int = 60,
    seed: str = "windowed-no-signal-power-v1",
) -> dict[str, Any]:
    if count < 1:
        raise ValueError("no-signal sample count must be positive")
    excluded_segments: set[str] = set()
    excluded_episodes: set[str] = set()
    scanned_manifests = 0
    for root in exclude_roots or []:
        root_path = Path(root).expanduser().resolve()
        paths = [root_path] if root_path.is_file() else sorted(root_path.rglob("*manifest*.json"))
        for path in paths:
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            segment_ids, episode_ids = _collect_manifest_holdout_ids(payload)
            excluded_segments.update(segment_ids)
            excluded_episodes.update(episode_ids)
            scanned_manifests += 1
    rows = conn.execute(
        """
        WITH ranked AS (
          SELECT l.id AS label_id,
                 l.segment_id,
                 l.output_json,
                 sg.episode_id,
                 sg.source_id,
                 sg.segment_index,
                 so.name AS source_name,
                 ROW_NUMBER() OVER (
                   PARTITION BY l.segment_id
                   ORDER BY l.created_at DESC, l.id DESC
                 ) AS rank
          FROM labels l
          JOIN segments sg ON sg.id = l.segment_id
          JOIN sources so ON so.id = sg.source_id
          WHERE l.label_pack = 'ai_discourse_v3_1'
            AND l.model = 'gpt-5.5'
            AND l.status IN ('ready', 'completed')
            AND COALESCE(json_array_length(json_extract(l.output_json, '$.discourse_events')), 0) = 0
        )
        SELECT * FROM ranked WHERE rank = 1
        """
    ).fetchall()
    from .worker import completed_episode_context_for_segment

    candidates = []
    missing_context = 0
    for row in rows:
        if row["segment_id"] in excluded_segments or row["episode_id"] in excluded_episodes:
            continue
        context_run = completed_episode_context_for_segment(
            conn,
            row["segment_id"],
            label_pack="ai_discourse_v3_1",
            model="gpt-5.5",
        )
        if not context_run or not Path(context_run["context_artifact_path"]).exists():
            missing_context += 1
            continue
        label = json.loads(row["output_json"])
        candidates.append(
            {
                "segment_id": row["segment_id"],
                "episode_id": row["episode_id"],
                "source_id": row["source_id"],
                "source_name": row["source_name"],
                "segment_index": int(row["segment_index"] or 0),
                "golden_label_id": row["label_id"],
                "golden_status": str(label.get("extraction_status") or "unknown"),
                "density_stratum": "no_signal",
            }
        )
    selected = _source_balanced_sample(candidates, count=count, seed=seed)
    if len(selected) < count:
        raise ValueError(f"only {len(selected)} eligible no-signal segments remain; requires {count}")
    chunks = [
        {
            "chunk_id": row["segment_id"],
            "episode_id": row["episode_id"],
            "source_id": row["source_id"],
            "source_name": row["source_name"],
            "segment_ids": [row["segment_id"]],
            "segment_count": 1,
            "segment_index": row["segment_index"],
            "label_count": 1,
            "golden_label_id": row["golden_label_id"],
            "golden_status": row["golden_status"],
            "expected_coded_segments": 0,
            "expected_discourse_events": 0,
            "density_stratum": "no_signal",
            "cleanliness_status": "requires_explicit_human_audit",
        }
        for row in selected
    ]
    output_file = Path(output_path).expanduser().resolve()
    output_file.parent.mkdir(parents=True, exist_ok=True)
    manifest = {
        "schema_version": NO_SIGNAL_POWER_MANIFEST_VERSION,
        "seed": seed,
        "selection_status": "retrospective_zero_event_candidates_pending_human_cleanliness_audit",
        "selection_classification": "retrospective_human_cleanliness_power_set_not_prospective_shadow",
        "required_clean_cases": count,
        "exclude_listed_prior_episodes": bool(excluded_episodes),
        "chunks": chunks,
        "privacy": "private_analysis_only_no_transcript_text",
    }


def export_prospective_shadow_manifest(
    conn,
    *,
    output_path: str | Path,
    frozen_after: str,
    exclude_roots: list[str | Path] | None = None,
    minimum_segments: int = 60,
    minimum_unseen_sources: int = 5,
    max_segments_per_episode: int = 2,
    seed: str = "windowed-prospective-shadow-v1",
) -> dict[str, Any]:
    if minimum_segments < 1 or minimum_unseen_sources < 0 or max_segments_per_episode < 1:
        raise ValueError("invalid prospective shadow selection limits")
    try:
        frozen_timestamp = datetime.fromisoformat(frozen_after.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("frozen_after must be an ISO timestamp") from exc
    excluded_segments: set[str] = set()
    excluded_episodes: set[str] = set()
    excluded_sources: set[str] = set()
    scanned_manifests = 0
    for root in exclude_roots or []:
        root_path = Path(root).expanduser().resolve()
        paths = [root_path] if root_path.is_file() else sorted(root_path.rglob("*manifest*.json"))
        for path in paths:
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            segment_ids, episode_ids = _collect_manifest_holdout_ids(payload)
            excluded_segments.update(segment_ids)
            excluded_episodes.update(episode_ids)
            excluded_sources.update(
                str(chunk["source_id"])
                for chunk in payload.get("chunks") or []
                if chunk.get("source_id")
            )
            scanned_manifests += 1
    if excluded_episodes:
        placeholders = ",".join("?" for _item in excluded_episodes)
        excluded_sources.update(
            str(row["source_id"])
            for row in conn.execute(
                f"SELECT DISTINCT source_id FROM episodes WHERE id IN ({placeholders})",
                tuple(sorted(excluded_episodes)),
            ).fetchall()
        )
    rows = conn.execute(
        """
        SELECT sg.id AS segment_id,
               sg.episode_id,
               sg.source_id,
               sg.segment_index,
               e.published_at,
               so.name AS source_name
        FROM segments sg
        JOIN episodes e ON e.id = sg.episode_id
        JOIN sources so ON so.id = sg.source_id
        WHERE e.published_at > ?
          AND EXISTS (
            SELECT 1
            FROM episode_context_runs context
            WHERE context.episode_id = sg.episode_id
              AND context.label_pack = 'ai_discourse_v3_1'
              AND context.model = 'gpt-5.5'
              AND context.status = 'completed'
              AND context.context_artifact_path IS NOT NULL
          )
        ORDER BY e.published_at, sg.episode_id, sg.segment_index
        """,
        (frozen_timestamp.isoformat(),),
    ).fetchall()
    candidates = [
        {
            **dict(row),
            "unseen_show": str(row["source_id"]) not in excluded_sources,
        }
        for row in rows
        if row["segment_id"] not in excluded_segments and row["episode_id"] not in excluded_episodes
    ]
    candidates.sort(
        key=lambda row: (
            not row["unseen_show"],
            sha256_text(f"{seed}|{row['source_id']}|{row['episode_id']}|{row['segment_id']}"),
        )
    )
    by_source: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in candidates:
        by_source[str(row["source_id"])].append(row)
    source_order = sorted(
        by_source,
        key=lambda source_id: (
            source_id in excluded_sources,
            sha256_text(f"{seed}|source|{source_id}"),
        ),
    )
    selected = []
    episode_counts: Counter[str] = Counter()
    while len(selected) < minimum_segments:
        added = False
        for source_id in source_order:
            while by_source[source_id]:
                candidate = by_source[source_id].pop(0)
                if episode_counts[str(candidate["episode_id"])] >= max_segments_per_episode:
                    continue
                selected.append(candidate)
                episode_counts[str(candidate["episode_id"])] += 1
                added = True
                break
            if len(selected) >= minimum_segments:
                break
        if not added:
            break
    chunks = [
        {
            "chunk_id": row["segment_id"],
            "segment_ids": [row["segment_id"]],
            "segment_count": 1,
            "episode_id": row["episode_id"],
            "source_id": row["source_id"],
            "source_name": row["source_name"],
            "segment_index": int(row["segment_index"] or 0),
            "published_at": row["published_at"],
            "unseen_show": bool(row["unseen_show"]),
            "expected_discourse_events": 0,
            "expected_event_count_status": "unknown_shadow_not_golden",
        }
        for row in selected
    ]
    unseen_sources = len({row["source_id"] for row in selected if row["unseen_show"]})
    complete_shape = bool(
        len(chunks) >= minimum_segments and unseen_sources >= minimum_unseen_sources
    )
    status = (
        "ready"
        if complete_shape
        else "blocked_no_post_freeze_context_ready_segments"
        if not candidates
        else "blocked_insufficient_post_freeze_shadow_coverage"
    )
    manifest = {
        "schema_version": PROSPECTIVE_SHADOW_MANIFEST_VERSION,
        "selection_status": status,
        "frozen_after": frozen_timestamp.isoformat(),
        "seed": seed,
        "minimum_segments": minimum_segments,
        "minimum_unseen_sources": minimum_unseen_sources,
        "max_segments_per_episode": max_segments_per_episode,
        "scanned_manifests": scanned_manifests,
        "excluded_segments": len(excluded_segments),
        "excluded_episodes": len(excluded_episodes),
        "excluded_sources": len(excluded_sources),
        "chunks": chunks,
        "privacy": "private_analysis_only_no_transcript_text",
    }
    output_file = Path(output_path).expanduser().resolve()
    write_text_atomic(output_file, json.dumps(manifest, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
    return {
        "ok": complete_shape,
        "path": str(output_file),
        "selection_status": status,
        "candidate_segments": len(candidates),
        "selected_segments": len(chunks),
        "episodes": len({row["episode_id"] for row in selected}),
        "sources": len({row["source_id"] for row in selected}),
        "unseen_sources": unseen_sources,
        "privacy": "sanitized_counts_only",
    }
    write_text_atomic(output_file, json.dumps(manifest, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
    return {
        "ok": True,
        "path": str(output_file),
        "segments": len(chunks),
        "sources": len({row["source_id"] for row in selected}),
        "episodes": len({row["episode_id"] for row in selected}),
        "human_cleanliness_audit_required": len(chunks),
        "candidate_pool": len(candidates),
        "missing_context": missing_context,
        "excluded_segments": len(excluded_segments),
        "excluded_episodes_observed": len(excluded_episodes),
        "scanned_manifests": scanned_manifests,
        "privacy": "sanitized_counts_only",
    }


def export_no_signal_human_audit(
    conn,
    *,
    manifest_path: str | Path,
    output_dir: str | Path,
) -> dict[str, Any]:
    manifest_file = Path(manifest_path).expanduser().resolve()
    manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
    if manifest.get("schema_version") != NO_SIGNAL_POWER_MANIFEST_VERSION:
        raise ValueError("invalid no-signal power manifest")
    chunks = list(manifest.get("chunks") or [])
    if not chunks:
        raise ValueError("no-signal human audit requires at least one case")
    root = Path(output_dir).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    packet_path = root / "review-packet.private.json"
    output_path = root / "human-review.private.json"
    schema_path = root / "human-review-schema.json"
    cases = []
    for chunk in chunks:
        segment_id = str((chunk.get("segment_ids") or [chunk["chunk_id"]])[0])
        source_excerpt, read_error = _paired_segment_text(conn, segment_id)
        if read_error:
            raise ValueError(f"could not read no-signal audit segment {segment_id}: {read_error}")
        cases.append(
            {
                "segment_id": segment_id,
                "source_excerpt": source_excerpt,
            }
        )
    schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["schema_version", "reviewer_kind", "reviewer_id", "reviewed_at", "cases"],
        "properties": {
            "schema_version": {"type": "string", "const": NO_SIGNAL_HUMAN_AUDIT_VERSION},
            "reviewer_kind": {"type": "string", "enum": ["human"]},
            "reviewer_id": {"type": "string", "minLength": 1},
            "reviewed_at": {"type": "string", "minLength": 1},
            "cases": {
                "type": "array",
                "minItems": len(cases),
                "maxItems": len(cases),
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["segment_id", "clean_no_signal"],
                    "properties": {
                        "segment_id": {"type": "string"},
                        "clean_no_signal": {"type": "boolean"},
                    },
                },
            },
        },
    }
    packet = {
        "schema_version": NO_SIGNAL_AUDIT_MAPPING_VERSION,
        "instruction": (
            "A case is clean_no_signal only when the source excerpt contains no independently useful, "
            "source-supported AI discourse event under the frozen codebook. Review every excerpt directly."
        ),
        "cases": cases,
        "privacy": "private_source_excerpts_for_explicit_human_audit",
    }
    mapping = {
        "schema_version": NO_SIGNAL_AUDIT_MAPPING_VERSION,
        "manifest_path": str(manifest_file),
        "packet_path": str(packet_path),
        "schema_path": str(schema_path),
        "human_output_path": str(output_path),
        "segment_ids": [item["segment_id"] for item in cases],
        "privacy": "private_mapping_no_model_adjudication",
    }
    write_text_atomic(packet_path, json.dumps(packet, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
    write_text_atomic(schema_path, json.dumps(schema, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
    mapping_path = root / "mapping.private.json"
    write_text_atomic(mapping_path, json.dumps(mapping, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
    return {
        "ok": True,
        "mapping_path": str(mapping_path),
        "packet_path": str(packet_path),
        "schema_path": str(schema_path),
        "human_output_path": str(output_path),
        "cases": len(cases),
        "human_review_required": len(cases),
        "privacy": "sanitized_counts_private_packet_contains_source_text",
    }


def one_sided_binomial_upper_bound(
    *,
    successes: int,
    trials: int,
    confidence: float = 0.95,
) -> float | None:
    if trials < 0 or successes < 0 or successes > trials or not 0 < confidence < 1:
        raise ValueError("invalid binomial inputs")
    if trials == 0:
        return None
    if successes == trials:
        return 1.0
    alpha = 1 - confidence

    def cumulative(probability: float) -> float:
        return sum(
            math.comb(trials, index)
            * (probability**index)
            * ((1 - probability) ** (trials - index))
            for index in range(successes + 1)
        )

    low = successes / trials
    high = 1.0
    for _iteration in range(80):
        midpoint = (low + high) / 2
        if cumulative(midpoint) > alpha:
            low = midpoint
        else:
            high = midpoint
    return round(high, 6)


def build_no_signal_power_report(
    *,
    trials: int,
    false_positives: int,
    human_confirmed_clean: int,
    output_path: str | Path,
    evaluator_spec_path: str | Path = DEFAULT_EVALUATOR_SPEC_PATH,
    human_provenance_verified: bool = False,
    candidate_result_provenance_verified: bool = False,
    candidate_schema_successes: int | None = None,
) -> dict[str, Any]:
    evaluator = load_windowed_evaluator_spec(evaluator_spec_path)
    minimum = int(evaluator["no_signal_power"]["minimum_clean_cases"])
    point_rate = _ratio(false_positives, trials)
    upper = one_sided_binomial_upper_bound(
        successes=false_positives,
        trials=trials,
        confidence=0.95,
    )
    gate = float(evaluator["gates"]["max_no_signal_false_positive_rate"])
    report = {
        "schema_version": NO_SIGNAL_POWER_REPORT_VERSION,
        "trials": trials,
        "false_positives": false_positives,
        "human_confirmed_clean": human_confirmed_clean,
        "human_provenance_verified": human_provenance_verified,
        "candidate_result_provenance_verified": candidate_result_provenance_verified,
        "candidate_schema_successes": candidate_schema_successes,
        "point_false_positive_rate": point_rate,
        "one_sided_95_percent_upper_bound": upper,
        "minimum_clean_cases": minimum,
        "gate_max": gate,
        "checks": {
            "sample_size": trials >= minimum,
            "human_cleanliness": human_confirmed_clean == trials,
            "human_provenance": human_provenance_verified,
            "candidate_result_provenance": candidate_result_provenance_verified,
            "candidate_schema_status": candidate_schema_successes == trials,
            "point_rate": point_rate is not None and point_rate <= gate,
            "upper_bound": upper is not None and upper <= gate,
        },
        "privacy": "aggregate_counts_only",
    }
    report["passed"] = all(report["checks"].values())
    destination = Path(output_path).expanduser().resolve()
    write_text_atomic(destination, json.dumps(report, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
    return {**report, "report_path": str(destination)}


def score_no_signal_human_audit(
    *,
    mapping_path: str | Path,
    candidate_results_path: str | Path,
    output_path: str | Path,
    evaluator_spec_path: str | Path = DEFAULT_EVALUATOR_SPEC_PATH,
) -> dict[str, Any]:
    mapping_file = Path(mapping_path).expanduser().resolve()
    mapping = json.loads(mapping_file.read_text(encoding="utf-8"))
    if mapping.get("schema_version") != NO_SIGNAL_AUDIT_MAPPING_VERSION:
        raise ValueError("invalid no-signal audit mapping")
    expected_ids = [str(segment_id) for segment_id in mapping.get("segment_ids") or []]
    if len(expected_ids) != len(set(expected_ids)) or not expected_ids:
        raise ValueError("no-signal audit mapping has invalid segment IDs")
    human_path = Path(mapping["human_output_path"]).expanduser().resolve()
    if not _json_file_ok(human_path):
        raise ValueError("completed human no-signal audit is missing")
    human = json.loads(human_path.read_text(encoding="utf-8"))
    if (
        human.get("schema_version") != NO_SIGNAL_HUMAN_AUDIT_VERSION
        or human.get("reviewer_kind") != "human"
        or not str(human.get("reviewer_id") or "").strip()
        or not str(human.get("reviewed_at") or "").strip()
        or not isinstance(human.get("cases"), list)
    ):
        raise ValueError("no-signal audit lacks explicit human provenance")
    human_by_segment = {}
    for item in human["cases"]:
        if (
            not isinstance(item, dict)
            or set(item) != {"segment_id", "clean_no_signal"}
            or str(item.get("segment_id")) in human_by_segment
            or not isinstance(item.get("clean_no_signal"), bool)
        ):
            raise ValueError("invalid no-signal human audit cases")
        human_by_segment[str(item["segment_id"])] = bool(item["clean_no_signal"])
    if set(human_by_segment) != set(expected_ids):
        raise ValueError("no-signal human audit case set mismatch")

    candidate_file = Path(candidate_results_path).expanduser().resolve()
    candidate = json.loads(candidate_file.read_text(encoding="utf-8"))
    output_rows = candidate.get("hydrated_outputs")
    candidate_stage = "hydrated"
    if not isinstance(output_rows, list):
        output_rows = candidate.get("results")
        candidate_stage = "shared_core"
        if not isinstance(output_rows, list) or not candidate.get("manifest_path"):
            raise ValueError("candidate no-signal report must contain hydrated_outputs or core results")
        core_manifest = json.loads(Path(candidate["manifest_path"]).read_text(encoding="utf-8"))
        output_by_segment = {
            str(entry["segment_id"]): entry.get("output_path")
            for entry in core_manifest.get("entries") or []
        }
        output_rows = [
            {
                "segment_id": item["segment_id"],
                "ok": bool(item.get("status_ok")),
                "output_path": output_by_segment.get(str(item["segment_id"])),
            }
            for item in output_rows
        ]
    candidate_by_segment = {}
    for item in output_rows:
        segment_id = str(item.get("segment_id"))
        if segment_id in candidate_by_segment:
            raise ValueError("duplicate candidate no-signal segment")
        candidate_by_segment[segment_id] = item
    if set(candidate_by_segment) != set(expected_ids):
        raise ValueError("candidate no-signal result set mismatch")

    clean_ids = [segment_id for segment_id in expected_ids if human_by_segment[segment_id]]
    candidate_schema_successes = 0
    false_positives = 0
    invalid_candidate_outputs = []
    for segment_id in clean_ids:
        item = candidate_by_segment[segment_id]
        output_file_value = item.get("output_path")
        if not item.get("ok") or not output_file_value or not _json_file_ok(Path(output_file_value)):
            invalid_candidate_outputs.append(segment_id)
            continue
        output = json.loads(Path(output_file_value).read_text(encoding="utf-8"))
        candidate_schema_successes += 1
        false_positives += int(bool(output.get("discourse_events") or output.get("events")))
    report = build_no_signal_power_report(
        trials=len(clean_ids),
        false_positives=false_positives,
        human_confirmed_clean=len(clean_ids),
        output_path=output_path,
        evaluator_spec_path=evaluator_spec_path,
        human_provenance_verified=True,
        candidate_result_provenance_verified=True,
        candidate_schema_successes=candidate_schema_successes,
    )
    report.update(
        {
            "mapping_path": str(mapping_file),
            "candidate_results_path": str(candidate_file),
            "candidate_path_name": candidate.get("path_name") or candidate_stage,
            "candidate_core_variant": candidate.get("core_variant"),
            "reviewer_id": str(human["reviewer_id"]),
            "reviewed_at": str(human["reviewed_at"]),
            "audited_cases": len(expected_ids),
            "excluded_not_clean": len(expected_ids) - len(clean_ids),
            "invalid_candidate_outputs": invalid_candidate_outputs,
        }
    )
    report["passed"] = bool(report["passed"] and not invalid_candidate_outputs)
    write_text_atomic(
        Path(output_path).expanduser().resolve(),
        json.dumps({key: value for key, value in report.items() if key != "report_path"}, ensure_ascii=True, indent=2, sort_keys=True)
        + "\n",
    )
    return report


def recover_interrupted_windowed_core_run(
    conn,
    *,
    source_manifest_path: str | Path,
    core_manifest_path: str | Path,
    output_path: str | Path,
    parent_exit_code: int,
) -> dict[str, Any]:
    source_manifest_file = Path(source_manifest_path).expanduser().resolve()
    core_manifest_file = Path(core_manifest_path).expanduser().resolve()
    source_manifest = json.loads(source_manifest_file.read_text(encoding="utf-8"))
    core_manifest = json.loads(core_manifest_file.read_text(encoding="utf-8"))
    requested_ids = [
        str((chunk.get("segment_ids") or [chunk["chunk_id"]])[0])
        for chunk in source_manifest.get("chunks") or []
    ]
    entries = {str(entry["segment_id"]): entry for entry in core_manifest.get("entries") or []}
    if set(entries) != set(requested_ids):
        raise ValueError("interrupted core manifest does not cover the requested segment set")
    raw_dir = core_manifest_file.parent / "interrupted_raw_outputs"
    raw_dir.mkdir(parents=True, exist_ok=True)
    results = []
    for segment_id in requested_ids:
        entry = entries[segment_id]
        log_path = Path(entry["log_path"])
        output_file = Path(entry["output_path"])
        log_exists = log_path.exists() and log_path.stat().st_size > 0
        usage = _codex_usage_from_jsonl(log_path) if log_exists else None
        json_ok = _json_file_ok(output_file)
        ownership = None
        normalization_error = None
        raw_sha256 = None
        if json_ok:
            raw_text = output_file.read_text(encoding="utf-8")
            raw_sha256 = sha256_text(raw_text)
            write_text_atomic(raw_dir / f"{segment_id}.private.json", raw_text)
            try:
                payload = json.loads(raw_text)
                segment_text, read_error = _paired_segment_text(conn, segment_id)
                if read_error:
                    raise ValueError(read_error)
                normalized, ownership = normalize_windowed_core_payload(
                    payload,
                    segment_text=segment_text,
                    boundaries=entry["boundaries"],
                    max_events=int(core_manifest.get("structural_max_events") or 25),
                )
                write_text_atomic(
                    output_file,
                    json.dumps(normalized, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
                )
            except Exception as exc:
                normalization_error = type(exc).__name__
        status_ok = bool(
            json_ok
            and normalization_error is None
            and ownership is not None
            and int(ownership.get("invalid_evidence_events") or 0) == 0
            and not ownership.get("event_cap_hit")
        )
        state = (
            "completed_before_parent_termination"
            if usage is not None
            else "interrupted_call"
            if log_exists
            else "not_started_before_parent_termination"
        )
        results.append(
            {
                "segment_id": segment_id,
                "state": state,
                "status_ok": status_ok,
                "json_ok": json_ok,
                "normalization_error": normalization_error,
                "ownership": ownership,
                "output_path": str(output_file),
                "log_path": str(log_path),
                "raw_output_sha256": raw_sha256,
                "usage": usage,
                "usage_complete": usage is not None if log_exists else True,
            }
        )
    usage = _sum_usage_dicts([item.get("usage") for item in results])
    completed = sum(item["state"] == "completed_before_parent_termination" for item in results)
    interrupted = sum(item["state"] == "interrupted_call" for item in results)
    not_started = sum(item["state"] == "not_started_before_parent_termination" for item in results)
    validated = sum(item["status_ok"] for item in results)
    apparent_false_positives = sum(
        bool((item.get("ownership") or {}).get("output_events"))
        for item in results
        if item["status_ok"]
    )
    report = {
        "schema_version": INTERRUPTED_CORE_RECOVERY_VERSION,
        "source_manifest_path": str(source_manifest_file),
        "manifest_path": str(core_manifest_file),
        "parent_exit_code": parent_exit_code,
        "intent_to_treat": True,
        "requested_segments": len(requested_ids),
        "completed_calls": completed,
        "interrupted_calls": interrupted,
        "not_started_calls": not_started,
        "validated_segments": validated,
        "terminal_failures": len(requested_ids) - validated,
        "schema_status_success_rate": _ratio(validated, len(requested_ids)) or 0.0,
        "usage": usage,
        "usage_unknown_calls": interrupted,
        "accounting_complete": False,
        "apparent_no_signal": {
            "status": "pending_explicit_human_cleanliness_audit",
            "validated_segments": validated,
            "segments_with_events": apparent_false_positives,
            "unadjudicated_rate": _ratio(apparent_false_positives, validated),
        },
        "results": results,
        "privacy": "sanitized_recovery_report_private_raw_outputs_preserved",
    }
    destination = Path(output_path).expanduser().resolve()
    write_text_atomic(destination, json.dumps(report, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
    return {**report, "report_path": str(destination)}


def run_checkpointed_windowed_core(
    conn,
    *,
    manifest_path: str | Path,
    output_dir: str | Path,
    timeout_seconds: int = 600,
    block_size: int = 4,
    evaluator_spec_path: str | Path = DEFAULT_EVALUATOR_SPEC_PATH,
    guideline_path: str | Path = DEFAULT_WINDOWED_GUIDELINES_PATH,
) -> dict[str, Any]:
    if block_size < 1:
        raise ValueError("checkpointed core block size must be positive")
    evaluator = load_windowed_evaluator_spec(evaluator_spec_path)
    expected_concurrency = int(evaluator["paired_run"]["concurrency"])
    if block_size != expected_concurrency:
        raise ValueError("checkpointed core block size must equal frozen concurrency")
    manifest_file = Path(manifest_path).expanduser().resolve()
    manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
    chunks = list(manifest.get("chunks") or [])
    if not chunks:
        raise ValueError("checkpointed core run requires a non-empty manifest")
    root = Path(output_dir).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    guideline_file = Path(guideline_path).expanduser().resolve()
    run_spec = {
        "schema_version": CHECKPOINTED_CORE_RUN_VERSION,
        "source_manifest_path": str(manifest_file),
        "source_manifest_sha256": _sha256_file(manifest_file),
        "evaluator_spec_sha256": _sha256_file(evaluator_spec_path),
        "guideline_sha256": _sha256_file(guideline_file),
        "model": evaluator["candidate"]["core_model"],
        "reasoning_effort": evaluator["candidate"]["core_reasoning_effort"],
        "block_size": block_size,
        "concurrency": expected_concurrency,
        "retry_count": 1,
        "window_count": 4,
        "context_chars": 900,
        "max_total_events": int(evaluator["candidate"]["max_events"]),
    }
    run_spec_sha256 = sha256_text(
        json.dumps(run_spec, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    )
    run_spec_path = root / "run-spec.json"
    if run_spec_path.exists():
        existing = json.loads(run_spec_path.read_text(encoding="utf-8"))
        if existing != {**run_spec, "run_spec_sha256": run_spec_sha256}:
            raise ValueError("checkpointed core run specification has drifted")
    else:
        write_text_atomic(
            run_spec_path,
            json.dumps(
                {**run_spec, "run_spec_sha256": run_spec_sha256},
                ensure_ascii=True,
                indent=2,
                sort_keys=True,
            )
            + "\n",
        )
    blocks = [chunks[index : index + block_size] for index in range(0, len(chunks), block_size)]
    block_summaries = []
    all_results = []
    interrupted_block = None
    wall_started = time.monotonic()
    started_at = now_iso()

    def persist_partial() -> None:
        partial = {
            "schema_version": CHECKPOINTED_CORE_RUN_VERSION,
            "run_spec_sha256": run_spec_sha256,
            "source_manifest_path": str(manifest_file),
            "requested_segments": len(chunks),
            "completed_blocks": len(block_summaries),
            "total_blocks": len(blocks),
            "interrupted_block": interrupted_block,
            "blocks": block_summaries,
            "updated_at": now_iso(),
            "privacy": "sanitized_checkpoint_no_prompt_or_transcript_text",
        }
        write_text_atomic(
            root / "run-report.partial.json",
            json.dumps(partial, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        )

    for block_index, block_chunks in enumerate(blocks):
        block_dir = root / "blocks" / f"block-{block_index:03d}"
        block_report_path = block_dir / "run-report.json"
        block_manifest_path = block_dir / "manifest.json"
        expected_segment_ids = [
            str((chunk.get("segment_ids") or [chunk["chunk_id"]])[0]) for chunk in block_chunks
        ]
        if block_report_path.exists():
            report = json.loads(block_report_path.read_text(encoding="utf-8"))
            reported_ids = [str(item["segment_id"]) for item in report.get("results") or []]
            if set(reported_ids) != set(expected_segment_ids):
                raise ValueError(f"checkpointed block {block_index} result set mismatch")
        else:
            interrupted_artifacts = bool(
                block_dir.exists()
                and any(
                    path.exists()
                    for path in (
                        block_dir / "windowed_core_manifest.json",
                        block_dir / "windowed_core_outputs",
                        block_dir / "windowed_core_prompts",
                    )
                )
            )
            if interrupted_artifacts:
                interrupted_block = {
                    "block_index": block_index,
                    "segment_ids": expected_segment_ids,
                    "recovery_command": (
                        "python3 -m research_factory.cli efficiency-windowed-core-recover "
                        f"--source-manifest {block_manifest_path} "
                        f"--core-manifest {block_dir / 'windowed_core_manifest.json'} "
                        f"--output {block_report_path} --parent-exit-code <exit-code>"
                    ),
                }
                persist_partial()
                break
            block_dir.mkdir(parents=True, exist_ok=True)
            block_manifest = {
                **{key: value for key, value in manifest.items() if key != "chunks"},
                "parent_manifest_path": str(manifest_file),
                "parent_manifest_sha256": run_spec["source_manifest_sha256"],
                "checkpointed_block_index": block_index,
                "chunks": block_chunks,
            }
            write_text_atomic(
                block_manifest_path,
                json.dumps(block_manifest, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
            )
            report = run_windowed_event_core_smoke(
                conn,
                manifest_path=block_manifest_path,
                limit=len(block_chunks),
                concurrency=expected_concurrency,
                model=run_spec["model"],
                reasoning_effort=run_spec["reasoning_effort"],
                timeout_seconds=timeout_seconds,
                window_count=run_spec["window_count"],
                context_chars=run_spec["context_chars"],
                guideline_path=guideline_file,
                max_total_events=run_spec["max_total_events"],
                min_expected_events=0,
                max_expected_events=None,
                retry_count=run_spec["retry_count"],
                rerun=True,
            )
            report = {
                **report,
                "checkpointed_block_index": block_index,
                "run_spec_sha256": run_spec_sha256,
                "requested_segments": len(block_chunks),
            }
            write_text_atomic(
                block_report_path,
                json.dumps(report, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
            )
        validated = sum(bool(item.get("status_ok")) for item in report.get("results") or [])
        output_paths = {}
        if report.get("manifest_path") and Path(report["manifest_path"]).exists():
            core_payload = json.loads(Path(report["manifest_path"]).read_text(encoding="utf-8"))
            output_paths = {
                str(entry["segment_id"]): entry.get("output_path")
                for entry in core_payload.get("entries") or []
            }
        all_results.extend(
            {
                **item,
                "output_path": item.get("output_path")
                or output_paths.get(str(item["segment_id"])),
                "checkpointed_block_index": block_index,
            }
            for item in report.get("results") or []
        )
        block_summaries.append(
            {
                "block_index": block_index,
                "segment_ids": expected_segment_ids,
                "report_path": str(block_report_path),
                "core_manifest_path": report.get("manifest_path"),
                "requested_segments": len(block_chunks),
                "validated_segments": validated,
                "terminal_failures": len(block_chunks) - validated,
                "attempted_calls": int(
                    report.get("attempted_calls")
                    or report.get("completed_calls", 0) + report.get("interrupted_calls", 0)
                ),
                "retry_attempts": int(report.get("retry_attempts") or 0),
                "usage": report.get("usage") or {},
                "accounting_complete": bool(report.get("accounting_complete")),
            }
        )
        persist_partial()

    completed_all_blocks = len(block_summaries) == len(blocks) and interrupted_block is None
    requested_completed = sum(item["requested_segments"] for item in block_summaries)
    validated = sum(item["validated_segments"] for item in block_summaries)
    usage = _sum_usage_dicts([item["usage"] for item in block_summaries])
    report = {
        "schema_version": CHECKPOINTED_CORE_RUN_VERSION,
        "run_spec_path": str(run_spec_path),
        "run_spec_sha256": run_spec_sha256,
        "source_manifest_path": str(manifest_file),
        "started_at": started_at,
        "finished_at": now_iso(),
        "wall_elapsed_seconds_this_invocation": round(time.monotonic() - wall_started, 3),
        "requested_segments": len(chunks),
        "accounted_segments": requested_completed,
        "validated_segments": validated,
        "terminal_failures": requested_completed - validated,
        "schema_status_success_rate": _ratio(validated, len(chunks)) or 0.0,
        "completed_blocks": len(block_summaries),
        "total_blocks": len(blocks),
        "interrupted_block": interrupted_block,
        "attempted_calls": sum(item["attempted_calls"] for item in block_summaries),
        "retry_attempts": sum(item["retry_attempts"] for item in block_summaries),
        "usage": usage,
        "accounting_complete": bool(
            completed_all_blocks and all(item["accounting_complete"] for item in block_summaries)
        ),
        "run_complete": completed_all_blocks,
        "ok": bool(
            completed_all_blocks and all(item["accounting_complete"] for item in block_summaries)
        ),
        "blocks": block_summaries,
        "results": sorted(all_results, key=lambda item: str(item["segment_id"])),
        "privacy": "sanitized_checkpoint_no_prompt_or_transcript_text",
    }
    destination = root / "run-report.json"
    write_text_atomic(destination, json.dumps(report, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
    return {**report, "report_path": str(destination)}


def _context_usage_sidecar_path(context_run: Any) -> Path:
    prompt_path = Path(context_run["prompt_path"]).expanduser().resolve()
    return prompt_path.parent.parent / "context_usage" / f"{context_run['id']}.json"


def _write_context_usage_sidecar(path: Path, payload: dict[str, Any]) -> None:
    write_text_atomic(
        path,
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
    )


def _context_output_validation_error(
    output_path: Path,
    *,
    expected_episode_id: str,
) -> tuple[bool, str | None]:
    try:
        output = json.loads(output_path.read_text(encoding="utf-8"))
        from .worker import validate_episode_context_output

        validate_episode_context_output(
            output,
            expected_episode_id=expected_episode_id,
        )
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"
    return True, None


def _sum_context_usage_profiles(
    attempts: list[dict[str, Any]],
) -> dict[str, int] | None:
    profiles = [
        attempt.get("usage_profile")
        for attempt in attempts
        if isinstance(attempt.get("usage_profile"), dict)
    ]
    if len(profiles) != len(attempts):
        return None
    fields = (
        "usage_event_count",
        "cumulative_billed_total_tokens",
        "cumulative_input_tokens",
        "cumulative_cached_input_tokens",
        "cumulative_output_tokens",
        "last_turn_unique_input_tokens",
        "last_turn_unique_total_tokens",
    )
    return {
        field: sum(int(profile.get(field) or 0) for profile in profiles)
        for field in fields
    }


def _finalize_instrumented_context_failure(
    conn,
    *,
    job_id: int,
    context_run_id: str,
    worker_id: str,
    error: str,
) -> None:
    ts = now_iso()
    conn.execute(
        """
        UPDATE episode_context_runs
        SET status = 'failed',
            error = ?,
            updated_at = ?
        WHERE id = ?
          AND status = 'claimed'
        """,
        (error, ts, context_run_id),
    )
    conn.execute(
        """
        UPDATE jobs
        SET status = 'failed',
            error = ?,
            lease_owner = NULL,
            leased_until = NULL,
            updated_at = ?
        WHERE id = ?
          AND status = 'claimed'
          AND lease_owner = ?
        """,
        (error, ts, job_id, worker_id),
    )
    conn.commit()


def release_instrumented_context_job_for_corrective_retry(
    conn,
    *,
    job_id: int,
) -> dict[str, Any]:
    job = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    if not job or job["job_type"] != "episode_context":
        raise ValueError("corrective release requires an episode_context job")
    if job["status"] != "claimed":
        raise ValueError("corrective release requires a claimed job")
    context_run = conn.execute(
        "SELECT * FROM episode_context_runs WHERE job_id = ?",
        (job_id,),
    ).fetchone()
    if not context_run:
        raise ValueError("corrective release requires an episode context run")
    output_path = Path(context_run["output_path"]).expanduser().resolve()
    valid, validation_error = _context_output_validation_error(
        output_path,
        expected_episode_id=str(job["target_id"]),
    )
    if valid or not validation_error:
        raise ValueError("corrective release requires invalid model output")
    sidecar_path = _context_usage_sidecar_path(context_run)
    prior = json.loads(sidecar_path.read_text(encoding="utf-8"))
    if prior.get("state") != "failed":
        raise ValueError("corrective release requires a failed usage sidecar")
    prior["validation_error"] = validation_error
    prior["corrective_retry_authorized"] = True
    _write_context_usage_sidecar(sidecar_path, prior)
    payload = json.loads(job["payload_json"] or "{}")
    payload["corrective_retry_pending"] = True
    payload["corrective_validation_error"] = validation_error
    ts = now_iso()
    conn.execute(
        """
        UPDATE episode_context_runs
        SET status = 'failed',
            error = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (validation_error, ts, context_run["id"]),
    )
    conn.execute(
        """
        UPDATE jobs
        SET status = 'pending',
            payload_json = ?,
            error = ?,
            lease_owner = NULL,
            leased_until = NULL,
            updated_at = ?
        WHERE id = ?
        """,
        (
            json.dumps(payload, ensure_ascii=True, sort_keys=True),
            f"corrective_retry_pending:{validation_error}",
            ts,
            job_id,
        ),
    )
    conn.commit()
    return {
        "job_id": job_id,
        "episode_context_run_id": str(context_run["id"]),
        "job_status": "pending",
        "validation_error": validation_error,
        "sidecar_path": str(sidecar_path),
    }


def execute_instrumented_episode_context_job(
    conn,
    *,
    job_id: int,
    worker_id: str,
    timeout_seconds: int = 1800,
    evaluator_spec_path: str | Path = DEFAULT_EVALUATOR_SPEC_PATH,
) -> dict[str, Any]:
    evaluator = load_windowed_evaluator_spec(evaluator_spec_path)
    context_spec = evaluator["context_generation"]
    if int(context_spec["retry_count"]) != 0:
        raise ValueError("frozen evaluator context retry count must remain zero")
    job = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    if not job or job["job_type"] != "episode_context":
        raise ValueError("instrumented context requires an episode_context job")
    if job["status"] != "claimed":
        raise ValueError(f"episode context job {job_id} is {job['status']}, not claimed")
    if job["lease_owner"] and job["lease_owner"] != worker_id:
        raise ValueError(f"episode context job {job_id} belongs to another worker")
    context_run = conn.execute(
        "SELECT * FROM episode_context_runs WHERE job_id = ? AND status = 'claimed'",
        (job_id,),
    ).fetchone()
    if not context_run:
        raise ValueError("claimed episode context run not found")
    prompt_path = Path(context_run["prompt_path"]).expanduser().resolve()
    output_path = Path(context_run["output_path"]).expanduser().resolve()
    if not prompt_path.exists():
        raise ValueError("episode context prompt artifact is missing")
    sidecar_path = _context_usage_sidecar_path(context_run)
    payload = json.loads(job["payload_json"] or "{}")
    prior_attempts: list[dict[str, Any]] = []
    corrective_error = payload.get("corrective_validation_error")
    corrective_retry_pending = bool(payload.get("corrective_retry_pending"))
    if sidecar_path.exists():
        prior = json.loads(sidecar_path.read_text(encoding="utf-8"))
        if (
            prior.get("state") == "failed"
            and corrective_retry_pending
            and corrective_error
        ):
            prior_attempts = list(prior.get("attempts") or [])
            if not prior_attempts:
                prior_attempts = [
                    {
                        "attempt": 1,
                        "exit_code": prior.get("exit_code"),
                        "timed_out": bool(prior.get("timed_out")),
                        "elapsed_seconds": prior.get("elapsed_seconds"),
                        "call_wall_seconds": prior.get("call_wall_seconds"),
                        "json_ok": bool(prior.get("json_ok")),
                        "validation_ok": False,
                        "validation_error": str(corrective_error),
                        "usage": prior.get("usage"),
                        "usage_profile": prior.get("usage_profile"),
                        "provider_pressure_signals": prior.get(
                            "provider_pressure_signals", []
                        ),
                        "lease_expired_before_submission": bool(
                            prior.get("lease_expired_before_submission")
                        ),
                    }
                ]
        elif prior.get("state") in {"started", "interrupted"}:
            raise ValueError(
                f"instrumented context attempt requires recovery: {sidecar_path}"
            )
        elif prior.get("state") in {"completed", "recovered_completed"}:
            return {**prior, "sidecar_path": str(sidecar_path), "cached_report": True}
        elif not prior_attempts:
            raise ValueError(
                f"instrumented context sidecar already exists: {sidecar_path}"
            )
    log_path = sidecar_path.parent / f"{context_run['id']}.private.jsonl"
    started_at = now_iso()
    initial = {
        "schema_version": "instrumented_episode_context_usage_v1",
        "state": "started",
        "job_id": job_id,
        "episode_context_run_id": context_run["id"],
        "episode_id": context_run["episode_id"],
        "model": context_spec["model"],
        "reasoning_effort": context_spec["reasoning_effort"],
        "corrective_retry_cap": 1,
        "retry_count": len(prior_attempts),
        "prompt_path": str(prompt_path),
        "output_path": str(output_path),
        "log_path": str(log_path),
        "started_at": started_at,
        "evaluator_spec_sha256": _sha256_file(evaluator_spec_path),
        "privacy": "usage_and_artifact_paths_no_prompt_or_transcript_text",
    }
    _write_context_usage_sidecar(sidecar_path, initial)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    scratch_dir = Path("/tmp/pif-efficiency-codex-smoke")
    scratch_dir.mkdir(parents=True, exist_ok=True)
    from .headless_codex import (
        _provider_pressure_signals,
        _usage_profile_from_jsonl,
    )

    current_attempts: list[dict[str, Any]] = []
    submission = None
    submission_error = None
    validation_error = str(corrective_error) if corrective_error else None
    first_attempt_number = 2 if prior_attempts else 1
    for attempt_number in range(first_attempt_number, 3):
        attempt_prompt_path = prompt_path
        if validation_error:
            attempt_prompt_path = (
                prompt_path.parent
                / f"{context_run['id']}.corrective-{attempt_number}.md"
            )
            write_text_atomic(
                attempt_prompt_path,
                prompt_path.read_text(encoding="utf-8")
                + "\n\n# Corrective Retry\n"
                + "The previous output was rejected with this exact validation "
                + f"error:\n{validation_error}\n"
                + "Return a corrected JSON object satisfying the original output "
                + "contract. Do not discuss the error.\n",
            )
        attempt_log_path = (
            log_path
            if attempt_number == 1
            else sidecar_path.parent
            / f"{context_run['id']}.attempt-{attempt_number}.private.jsonl"
        )
        output_path.write_text("", encoding="utf-8")
        command = [
            "codex",
            "exec",
            "-m",
            context_spec["model"],
            "-c",
            f'model_reasoning_effort="{context_spec["reasoning_effort"]}"',
            "-C",
            str(scratch_dir),
            "--skip-git-repo-check",
            "--ignore-rules",
            "--ephemeral",
            "--sandbox",
            "read-only",
            "--output-last-message",
            str(output_path),
            "--json",
            "-",
        ]
        exit_code, timed_out, elapsed_seconds = _run_codex_smoke_command(
            command,
            prompt_path=attempt_prompt_path,
            log_path=attempt_log_path,
            timeout_seconds=timeout_seconds,
        )
        usage = _codex_usage_from_jsonl(attempt_log_path)
        usage_profile = _usage_profile_from_jsonl(attempt_log_path)
        pressure = _provider_pressure_signals(attempt_log_path)
        json_ok = _json_file_ok(output_path)
        validation_ok = False
        validation_error = None
        if exit_code == 0 and not timed_out:
            validation_ok, validation_error = _context_output_validation_error(
                output_path,
                expected_episode_id=str(context_run["episode_id"]),
            )
        current_job = conn.execute(
            "SELECT leased_until FROM jobs WHERE id = ?",
            (job_id,),
        ).fetchone()
        lease_expired = False
        if current_job and current_job["leased_until"]:
            leased_until = datetime.fromisoformat(
                str(current_job["leased_until"]).replace("Z", "+00:00")
            )
            now = (
                datetime.now(leased_until.tzinfo)
                if leased_until.tzinfo
                else datetime.now()
            )
            lease_expired = leased_until <= now
        current_attempts.append(
            {
                "attempt": attempt_number,
                "prompt_path": str(attempt_prompt_path),
                "log_path": str(attempt_log_path),
                "exit_code": exit_code,
                "timed_out": timed_out,
                "elapsed_seconds": elapsed_seconds,
                "call_wall_seconds": elapsed_seconds,
                "json_ok": json_ok,
                "validation_ok": validation_ok,
                "validation_error": validation_error,
                "usage": usage,
                "usage_profile": usage_profile,
                "provider_pressure_signals": pressure,
                "lease_expired_before_submission": lease_expired,
            }
        )
        if validation_ok:
            try:
                from .worker import submit_episode_context_output

                submission = submit_episode_context_output(
                    conn,
                    job_id=job_id,
                    output_json_path=output_path,
                    worker_id=worker_id,
                    allow_expired=False,
                )
            except Exception as exc:
                submission_error = type(exc).__name__
                validation_error = f"{type(exc).__name__}: {exc}"
            break
        if (
            validation_error is None
            or attempt_number >= 2
            or timed_out
            or exit_code != 0
        ):
            break

    all_attempts = [*prior_attempts, *current_attempts]
    completed = bool(submission is not None)
    terminal_error = (
        validation_error
        or submission_error
        or (
            "codex_exec_timeout"
            if any(attempt["timed_out"] for attempt in current_attempts)
            else "codex_exec_failed"
        )
    )
    if not completed:
        _finalize_instrumented_context_failure(
            conn,
            job_id=job_id,
            context_run_id=str(context_run["id"]),
            worker_id=worker_id,
            error=str(terminal_error),
        )
    usage = (
        _sum_usage_dicts(current_attempts)
        if all(isinstance(attempt.get("usage"), dict) for attempt in current_attempts)
        else None
    )
    usage_profile = _sum_context_usage_profiles(current_attempts)
    provider_pressure_signals = sorted(
        {
            signal
            for attempt in current_attempts
            for signal in attempt.get("provider_pressure_signals", [])
        }
    )
    lease_expired_before_submission = any(
        bool(attempt.get("lease_expired_before_submission"))
        for attempt in current_attempts
    )
    report = {
        **initial,
        "state": "completed" if completed else "failed",
        "finished_at": now_iso(),
        "exit_code": current_attempts[-1]["exit_code"],
        "timed_out": any(attempt["timed_out"] for attempt in current_attempts),
        "elapsed_seconds": sum(
            float(attempt["elapsed_seconds"]) for attempt in current_attempts
        ),
        "call_wall_seconds": sum(
            float(attempt["call_wall_seconds"]) for attempt in current_attempts
        ),
        "json_ok": bool(current_attempts[-1]["json_ok"]),
        "validation_error": validation_error,
        "validation_error_kind": (
            str(validation_error).split(":", 1)[0]
            if validation_error
            else None
        ),
        "usage": usage,
        "usage_profile": usage_profile,
        "usage_complete": usage is not None,
        "accounting_complete": usage is not None,
        "job_attempt_number": int(job["attempts"] or 0),
        "job_retry_count": max(int(job["attempts"] or 0) - 1, 0),
        "provider_calls_this_invocation": len(current_attempts),
        "provider_retry_count": max(0, len(all_attempts) - 1),
        "retry_count": max(0, len(all_attempts) - 1),
        "attempts": all_attempts,
        "lease_expired_before_submission": lease_expired_before_submission,
        "provider_pressure_signals": provider_pressure_signals,
        "submission": submission,
        "submission_error": submission_error,
        "status_ok": completed and usage is not None,
    }
    _write_context_usage_sidecar(sidecar_path, report)
    return {**report, "sidecar_path": str(sidecar_path)}


def recover_instrumented_episode_context_job(
    conn,
    *,
    sidecar_path: str | Path,
    worker_id: str,
) -> dict[str, Any]:
    sidecar_file = Path(sidecar_path).expanduser().resolve()
    report = json.loads(sidecar_file.read_text(encoding="utf-8"))
    if report.get("schema_version") != "instrumented_episode_context_usage_v1":
        raise ValueError("invalid instrumented context sidecar")
    if report.get("state") not in {"started", "interrupted", "failed"}:
        raise ValueError("instrumented context sidecar is not recoverable")
    output_path = Path(report["output_path"])
    log_path = Path(report["log_path"])
    usage = _codex_usage_from_jsonl(log_path) if log_path.exists() else None
    json_ok = _json_file_ok(output_path)
    job = conn.execute("SELECT * FROM jobs WHERE id = ?", (int(report["job_id"]),)).fetchone()
    submission = None
    submission_error = None
    if job and job["status"] == "completed":
        context_run = conn.execute(
            "SELECT * FROM episode_context_runs WHERE id = ?",
            (report["episode_context_run_id"],),
        ).fetchone()
        if context_run and context_run["status"] == "completed":
            submission = {
                "job_id": str(job["id"]),
                "episode_context_run_id": context_run["id"],
                "context_artifact_path": context_run["context_artifact_path"],
            }
    elif job and job["status"] == "claimed" and json_ok:
        try:
            from .worker import submit_episode_context_output

            submission = submit_episode_context_output(
                conn,
                job_id=int(report["job_id"]),
                output_json_path=output_path,
                worker_id=worker_id,
                allow_expired=True,
            )
        except Exception as exc:
            submission_error = type(exc).__name__
    recovered = bool(submission is not None)
    recovered_report = {
        **report,
        "state": "recovered_completed" if recovered else "recovered_failed",
        "recovered_at": now_iso(),
        "json_ok": json_ok,
        "usage": usage,
        "usage_complete": usage is not None,
        "accounting_complete": usage is not None,
        "submission": submission,
        "submission_error": submission_error,
        "status_ok": recovered and usage is not None,
        "recovery_reran_model": False,
    }
    _write_context_usage_sidecar(sidecar_file, recovered_report)
    return {**recovered_report, "sidecar_path": str(sidecar_file)}


def _gate_check(
    *,
    state: str,
    observed: Any,
    requirement: Any,
    detail: str | None = None,
) -> dict[str, Any]:
    if state not in {"passed", "failed", "inconclusive", "pending", "not_applicable"}:
        raise ValueError(f"invalid gate state: {state}")
    item = {
        "state": state,
        "passed": state in {"passed", "not_applicable"},
        "observed": observed,
        "requirement": requirement,
    }
    if detail:
        item["detail"] = detail
    return item


def _combined_gate_state(checks: dict[str, dict[str, Any]]) -> str:
    states = {item["state"] for item in checks.values()}
    if "failed" in states:
        return "failed"
    if "inconclusive" in states:
        return "inconclusive"
    if "pending" in states:
        return "pending"
    return "passed"


def build_fail_closed_acceptance_report(
    *,
    evidence_bundle_path: str | Path,
    output_path: str | Path | None = None,
    evaluator_spec_path: str | Path = DEFAULT_EVALUATOR_SPEC_PATH,
) -> dict[str, Any]:
    bundle_file = Path(evidence_bundle_path).expanduser().resolve()
    bundle = json.loads(bundle_file.read_text(encoding="utf-8"))
    if bundle.get("schema_version") != PAIRED_ACCEPTANCE_BUNDLE_VERSION:
        raise ValueError("invalid paired acceptance evidence bundle")
    evaluator_path = Path(evaluator_spec_path).expanduser().resolve()
    evaluator = load_windowed_evaluator_spec(evaluator_path)
    artifact_errors: dict[str, str] = {}

    def load_artifact(
        label: str,
        path_value: Any,
        *,
        schema_version: str | None = None,
    ) -> dict[str, Any] | None:
        if not isinstance(path_value, str) or not path_value.strip():
            artifact_errors[label] = "missing_path"
            return None
        path = Path(path_value).expanduser()
        if not path.is_absolute():
            path = bundle_file.parent / path
        path = path.resolve()
        if not path.exists():
            artifact_errors[label] = "missing_file"
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            artifact_errors[label] = "invalid_json"
            return None
        if schema_version is not None and payload.get("schema_version") != schema_version:
            artifact_errors[label] = "schema_version_mismatch"
            return None
        payload["_artifact_path"] = str(path)
        return payload

    phase_one = load_artifact(
        "phase_one",
        bundle.get("phase_one_report_path"),
        schema_version=PAIRED_PHASE_ONE_SCHEMA_VERSION,
    )
    provenance = load_artifact(
        "provenance",
        bundle.get("provenance_report_path"),
        schema_version=PAIRED_PROVENANCE_SCHEMA_VERSION,
    )
    context_cost = load_artifact(
        "context_cost",
        bundle.get("context_cost_report_path"),
        schema_version=PAIRED_CONTEXT_COST_SCHEMA_VERSION,
    )
    no_signal = load_artifact(
        "no_signal_power",
        bundle.get("no_signal_power_report_path"),
        schema_version=NO_SIGNAL_POWER_REPORT_VERSION,
    )
    shadow = load_artifact("prospective_shadow", bundle.get("prospective_shadow_report_path"))
    expanded_judge = load_artifact(
        "expanded_judge_calibration",
        bundle.get("expanded_judge_calibration_report_path"),
        schema_version=EXPANDED_JUDGE_CALIBRATION_SCHEMA_VERSION,
    )

    global_checks: dict[str, dict[str, Any]] = {}
    requested_ids: list[str] = []
    manifest: dict[str, Any] | None = None
    manifest_rows: list[dict[str, Any]] = []
    if phase_one is None:
        global_checks["phase_one_available"] = _gate_check(
            state="pending",
            observed=None,
            requirement="valid frozen phase-one report",
        )
    else:
        requested_ids = [
            str(segment_id)
            for block in phase_one.get("blocks") or []
            for segment_id in block.get("segment_ids") or []
        ]
        try:
            manifest_path = Path(phase_one["manifest_path"]).expanduser().resolve()
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (KeyError, OSError, json.JSONDecodeError):
            artifact_errors["paired_manifest"] = "unreadable"
        if manifest is not None:
            by_segment = {
                str((chunk.get("segment_ids") or [chunk.get("chunk_id")])[0]): chunk
                for chunk in manifest.get("chunks") or []
            }
            manifest_rows = [by_segment[segment_id] for segment_id in requested_ids if segment_id in by_segment]
        global_checks["phase_one_available"] = _gate_check(
            state="passed",
            observed=len(requested_ids),
            requirement="valid frozen phase-one report",
        )

    paired_spec = evaluator["paired_run"]
    source_counts = Counter(str(row.get("source_id")) for row in manifest_rows)
    density_counts = Counter(str(row.get("density_stratum")) for row in manifest_rows)
    sample_observed = {
        "segments": len(requested_ids),
        "unique_segments": len(set(requested_ids)),
        "manifest_rows": len(manifest_rows),
        "sources": len(source_counts),
        "density_counts": dict(sorted(density_counts.items())),
        "max_segments_per_source": max(source_counts.values(), default=0),
    }
    sample_passed = bool(
        phase_one is not None
        and len(requested_ids) >= int(paired_spec["minimum_segments"])
        and len(set(requested_ids)) == len(requested_ids)
        and len(manifest_rows) == len(requested_ids)
        and len(source_counts) >= int(paired_spec["minimum_sources"])
        and max(source_counts.values(), default=0) >= 2
        and all(
            density_counts[name] >= int(bounds["minimum_segments"])
            for name, bounds in paired_spec["density_strata"].items()
        )
    )
    global_checks["paired_sample_shape"] = _gate_check(
        state="passed" if sample_passed else ("failed" if phase_one is not None else "pending"),
        observed=sample_observed,
        requirement={
            "minimum_segments": paired_spec["minimum_segments"],
            "minimum_sources": paired_spec["minimum_sources"],
            "density_strata": paired_spec["density_strata"],
            "multiple_segments_per_source": True,
        },
    )
    retrospective = bool(manifest is not None and not manifest.get("exclude_prior_episodes", False))
    global_checks["retrospective_classification"] = _gate_check(
        state="passed" if retrospective else ("passed" if manifest is not None else "pending"),
        observed="retrospective_validation" if retrospective else "prospective_or_unknown",
        requirement="exclude_prior_episodes=false must be reported as retrospective validation",
    )

    expected_extraction_hash = evaluator["parent_extraction_spec_sha256"]
    evaluator_code_verified = bool(
        provenance
        and provenance.get("evaluator_code")
        and all(
            Path(path).exists() and _sha256_file(path) == expected_hash
            for path, expected_hash in provenance.get("evaluator_code", {}).items()
        )
    )
    project_root = Path(__file__).resolve().parents[1]
    label_pack_root = project_root / "label_packs" / "ai_discourse_v3_1"
    expected_baseline_static = {
        "prompt": _sha256_file(label_pack_root / "prompt.md"),
        "schema": _sha256_file(label_pack_root / "schema.json"),
        "codebook": _sha256_file(label_pack_root / "codebook.md"),
    }
    baseline_static_verified = bool(
        provenance and provenance.get("baseline_static") == expected_baseline_static
    )
    guideline_metadata = (provenance or {}).get("candidate_guideline") or {}
    guideline_verified_live = bool(
        guideline_metadata.get("verified")
        and guideline_metadata.get("path")
        and Path(guideline_metadata["path"]).exists()
        and _sha256_file(guideline_metadata["path"]) == guideline_metadata.get("actual_sha256")
    )
    provenance_complete = bool(
        provenance
        and provenance.get("complete")
        and evaluator_code_verified
        and baseline_static_verified
        and provenance.get("extraction_spec_sha256") == expected_extraction_hash
        and provenance.get("evaluator_spec_sha256") == _sha256_file(evaluator_path)
        and guideline_verified_live
        and not provenance.get("missing_artifacts")
        and not provenance.get("missing_context")
        and not provenance.get("missing_golden")
        and len(provenance.get("golden_output_hashes") or {}) == len(requested_ids)
        and len(provenance.get("baseline_prompt_hashes") or {})
        + len(provenance.get("declared_baseline_preparation_failures") or {})
        == len(requested_ids)
        and len(provenance.get("candidate_core_prompt_hashes") or {}) == len(requested_ids)
    )
    global_checks["provenance_hashes"] = _gate_check(
        state="passed" if provenance_complete else ("failed" if provenance is not None else "pending"),
        observed={
            "complete": bool(provenance and provenance.get("complete")),
            "evaluator_code_verified_live": evaluator_code_verified,
            "baseline_static_verified_live": baseline_static_verified,
            "guideline_verified_live": guideline_verified_live,
            "hashed_golden_outputs": len((provenance or {}).get("golden_output_hashes") or {}),
            "hashed_baseline_prompts": len((provenance or {}).get("baseline_prompt_hashes") or {}),
            "declared_baseline_preparation_failures": len(
                (provenance or {}).get("declared_baseline_preparation_failures") or {}
            ),
        },
        requirement="all declared evaluator, prompt, schema, codebook, context, golden, guideline, window, and batch hashes",
    )
    context_measurement_complete = bool(
        context_cost
        and context_cost.get("end_to_end_cost_evaluable")
        and context_cost.get("exact_usage_available")
        and (
            context_cost.get("exact_context_tokens_production_amortized") is not None
            or context_cost.get("exact_context_tokens") is not None
        )
    )
    global_checks["episode_context_cost_measurement"] = _gate_check(
        state=(
            "passed"
            if context_measurement_complete
            else "failed"
            if context_cost is not None
            else "pending"
        ),
        observed={
            "unique_episodes": (context_cost or {}).get("unique_episodes"),
            "exact_usage_available": (context_cost or {}).get("exact_usage_available"),
            "end_to_end_cost_evaluable": (context_cost or {}).get("end_to_end_cost_evaluable"),
            "estimated_context_total_tokens_o200k": (context_cost or {}).get(
                "estimated_context_total_tokens_o200k"
            ),
            "estimated_context_total_tokens_o200k_production_amortized": (
                context_cost or {}
            ).get("estimated_context_total_tokens_o200k_production_amortized"),
            "fail_closed_reason": (context_cost or {}).get("fail_closed_reason"),
        },
        requirement="exact or production-valid amortized episode-context tokens and wall time added symmetrically to both systems",
    )

    ceiling = (provenance or {}).get("structural_recall_ceiling") or {}
    ceiling_disclosed = bool(
        provenance is not None
        and isinstance(ceiling.get("segments_above_cap"), list)
        and ceiling.get("segment_count") == len(ceiling.get("segments_above_cap") or [])
    )
    global_checks["structural_recall_ceiling_disclosed"] = _gate_check(
        state="passed" if ceiling_disclosed else ("failed" if provenance is not None else "pending"),
        observed=ceiling,
        requirement="enumerate every holdout segment above the 25-event structural cap",
    )

    no_signal_passed = bool(
        no_signal
        and no_signal.get("passed")
        and int(no_signal.get("trials") or 0) >= int(evaluator["no_signal_power"]["minimum_clean_cases"])
        and int(no_signal.get("human_confirmed_clean") or 0) == int(no_signal.get("trials") or 0)
        and float(no_signal.get("one_sided_95_percent_upper_bound") or 1.0)
        <= float(evaluator["gates"]["max_no_signal_false_positive_rate"])
    )
    global_checks["no_signal_false_positive_power"] = _gate_check(
        state="passed" if no_signal_passed else ("failed" if no_signal is not None else "pending"),
        observed={
            "trials": (no_signal or {}).get("trials"),
            "false_positives": (no_signal or {}).get("false_positives"),
            "human_confirmed_clean": (no_signal or {}).get("human_confirmed_clean"),
            "one_sided_95_percent_upper_bound": (no_signal or {}).get(
                "one_sided_95_percent_upper_bound"
            ),
        },
        requirement={
            "minimum_clean_cases": evaluator["no_signal_power"]["minimum_clean_cases"],
            "maximum_upper_bound": evaluator["gates"]["max_no_signal_false_positive_rate"],
        },
    )

    shadow_paths = (shadow or {}).get("paths") or {}
    shadow_passed = bool(
        shadow
        and shadow.get("complete")
        and int(shadow.get("new_episode_segments") or 0) > 0
        and all(name in shadow_paths for name in ("spark_only", "mini_only"))
        and all(
            all(key in shadow_paths[name] for key in ("segments", "quota_failures", "fallback_frequency", "tail_latency_seconds"))
            for name in ("spark_only", "mini_only")
        )
    )
    global_checks["prospective_shadow"] = _gate_check(
        state="passed" if shadow_passed else ("failed" if shadow is not None else "pending"),
        observed={
            "complete": bool(shadow and shadow.get("complete")),
            "new_episode_segments": (shadow or {}).get("new_episode_segments"),
            "unseen_show_segments": (shadow or {}).get("unseen_show_segments"),
            "paths": sorted(shadow_paths),
        },
        requirement="sustained new-episode shadow with separate Spark and Mini quota, fallback, tail-latency, retry, and cache accounting",
    )
    expanded_judge_admissible = bool(
        expanded_judge
        and expanded_judge.get("calibrated")
        and int(expanded_judge.get("fixture_case_count") or 0)
        >= int(evaluator["judge_calibration"]["minimum_cases"])
        and expanded_judge.get("fixture_sha256") == _sha256_file(DEFAULT_EXPANDED_JUDGE_FIXTURE_PATH)
    )
    global_checks["llm_judge_admissibility"] = _gate_check(
        state="passed" if expanded_judge_admissible else "not_applicable",
        observed={
            "calibrated": bool(expanded_judge and expanded_judge.get("calibrated")),
            "fixture_case_count": (expanded_judge or {}).get("fixture_case_count"),
            "combined": (expanded_judge or {}).get("combined"),
        },
        requirement="required only if an LLM judge is used; failed calibration forces explicit human adjudication",
        detail="No LLM judge is used by the current score mappings.",
    )

    baseline_status: dict[str, bool] = {}
    core_status: dict[str, bool] = {}
    baseline_preparation_failures: list[dict[str, Any]] = []
    if phase_one is not None:
        baseline_status = {
            str(item["segment_id"]): bool(item.get("status_ok"))
            for item in (phase_one.get("baseline") or {}).get("outputs") or []
        }
        core_status = {
            str(result["segment_id"]): bool(result.get("status_ok"))
            for block in phase_one.get("blocks") or []
            for result in (block.get("candidate_core") or {}).get("results") or []
        }
        for block in phase_one.get("blocks") or []:
            report_path = (block.get("baseline") or {}).get("report_path")
            if not report_path:
                continue
            try:
                block_baseline = json.loads(Path(report_path).read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                artifact_errors[f"baseline_block_{block.get('block_index')}"] = "unreadable_report"
                continue
            baseline_preparation_failures.extend(block_baseline.get("preparation_failures") or [])

    allowed_triggers = set(evaluator["failure_repair"]["observable_triggers"])
    core_trigger_counts = {
        name: int(count or 0)
        for name, count in ((phase_one or {}).get("candidate_core") or {}).get(
            "repair_trigger_counts", {}
        ).items()
        if name in allowed_triggers
    }

    def variant_report(path_name: str, variant_name: str, evidence: dict[str, Any]) -> dict[str, Any]:
        label = f"{path_name}.{variant_name}"
        enrichment = load_artifact(
            f"{label}.enrichment",
            evidence.get("enrichment_report_path"),
            schema_version=PAIRED_ENRICHMENT_RUN_SCHEMA_VERSION,
        )
        score = load_artifact(
            f"{label}.score",
            evidence.get("score_report_path"),
            schema_version=PAIRED_ADJUDICATION_SCORE_VERSION,
        )
        repair = None
        if variant_name == "repaired":
            repair = load_artifact(
                f"{label}.repair",
                evidence.get("repair_report_path") or bundle.get("repair_report_path"),
                schema_version=PAIRED_CORE_REPAIR_SCHEMA_VERSION,
            )
        checks: dict[str, dict[str, Any]] = {}
        artifacts_ready = bool(enrichment is not None and score is not None and (repair is not None or variant_name == "unrepaired"))
        checks["artifacts"] = _gate_check(
            state="passed" if artifacts_ready else "pending",
            observed={"enrichment": enrichment is not None, "score": score is not None, "repair": repair is not None},
            requirement="enrichment and score reports, plus repair report for repaired variants",
        )

        candidate_status = {
            str(item["segment_id"]): bool(item.get("ok"))
            for item in (enrichment or {}).get("hydrated_outputs") or []
        }
        baseline_valid = sum(baseline_status.get(segment_id, False) for segment_id in requested_ids)
        candidate_valid = sum(candidate_status.get(segment_id, False) for segment_id in requested_ids)
        minimum_status = float(evaluator["gates"]["min_schema_status_success"])
        baseline_rate = _ratio(baseline_valid, len(requested_ids))
        candidate_rate = _ratio(candidate_valid, len(requested_ids))
        status_available = bool(phase_one is not None and enrichment is not None and requested_ids)
        checks["intent_to_treat_schema_status"] = _gate_check(
            state=(
                "passed"
                if status_available
                and baseline_rate is not None
                and candidate_rate is not None
                and baseline_rate >= minimum_status
                and candidate_rate >= minimum_status
                else "failed"
                if status_available
                else "pending"
            ),
            observed={
                "denominator": len(requested_ids),
                "baseline_validated": baseline_valid,
                "baseline_terminal_failures": [
                    segment_id for segment_id in requested_ids if not baseline_status.get(segment_id, False)
                ],
                "baseline_rate": baseline_rate,
                "candidate_validated": candidate_valid,
                "candidate_terminal_failures": [
                    segment_id for segment_id in requested_ids if not candidate_status.get(segment_id, False)
                ],
                "candidate_rate": candidate_rate,
            },
            requirement={"minimum_each": minimum_status, "denominator": "all frozen requested segments"},
        )

        score_rows = {
            str(row.get("segment_id")): row for row in (score or {}).get("rows") or []
        }
        itt_preserved = bool(
            score
            and score.get("complete")
            and score.get("intent_to_treat")
            and int(score.get("scored_segments") or 0) == len(requested_ids)
            and set(score_rows) == set(requested_ids)
            and all(
                bool(score_rows[segment_id].get("baseline_terminal_failure"))
                == (not baseline_status.get(segment_id, False))
                and bool(score_rows[segment_id].get("candidate_terminal_failure"))
                == (not candidate_status.get(segment_id, False))
                and (
                    baseline_status.get(segment_id, False)
                    or float(score_rows[segment_id].get("baseline_f1", -1)) == 0.0
                )
                and (
                    candidate_status.get(segment_id, False)
                    or float(score_rows[segment_id].get("candidate_f1", -1)) == 0.0
                )
                for segment_id in requested_ids
            )
        )
        checks["intent_to_treat_quality"] = _gate_check(
            state="passed" if itt_preserved else ("failed" if score is not None else "pending"),
            observed={
                "score_complete": bool(score and score.get("complete")),
                "scored_segments": (score or {}).get("scored_segments"),
                "requested_segments": len(requested_ids),
            },
            requirement="all terminal failures retained with F1 zero and no dropped requested segments",
        )

        bootstrap = (score or {}).get("bootstrap") or {}
        margin = -float(evaluator["gates"]["max_paired_f1_drop"])
        lower = bootstrap.get("ci_lower")
        upper = bootstrap.get("ci_upper")
        if not score or not score.get("complete") or lower is None or upper is None:
            quality_state = "pending"
        elif bootstrap.get("method") != evaluator["bootstrap"]["method"]:
            quality_state = "failed"
        elif float(lower) >= margin:
            quality_state = "passed"
        elif float(upper) < margin:
            quality_state = "failed"
        else:
            quality_state = "inconclusive"
        checks["paired_non_inferiority"] = _gate_check(
            state=quality_state,
            observed=bootstrap,
            requirement={
                "method": evaluator["bootstrap"]["method"],
                "ci_lower_minimum": margin,
                "crossing_result": "inconclusive",
            },
        )

        subgroup_minimum = int(evaluator["gates"]["minimum_subgroup_size"])
        macro_floor = -float(evaluator["gates"]["max_macro_f1_drop"])
        macro_results = {}
        macro_states = []
        for macro_name, report_key in (
            ("source", "macro_by_source"),
            ("density", "macro_by_density"),
            ("event_family", "event_level_by_family"),
        ):
            groups = (score or {}).get(report_key) or {}
            evaluated = {}
            underpowered = []
            for group_name, values in groups.items():
                if macro_name == "event_family":
                    size = max(
                        int((values.get("baseline") or {}).get("segments") or 0),
                        int((values.get("candidate") or {}).get("segments") or 0),
                    )
                else:
                    size = int(values.get("segments") or 0)
                if size < subgroup_minimum:
                    underpowered.append(group_name)
                    continue
                delta = float(values.get("candidate_minus_baseline") or 0.0)
                evaluated[group_name] = {"size": size, "delta": delta, "passed": delta >= macro_floor}
            if not score or not score.get("complete"):
                state = "pending"
            elif not evaluated:
                state = "inconclusive"
            elif all(item["passed"] for item in evaluated.values()):
                state = "passed"
            else:
                state = "failed"
            macro_states.append(state)
            macro_results[macro_name] = {
                "state": state,
                "evaluated": evaluated,
                "underpowered_groups": sorted(underpowered),
            }
        macro_state = (
            "failed"
            if "failed" in macro_states
            else "inconclusive"
            if "inconclusive" in macro_states
            else "pending"
            if "pending" in macro_states
            else "passed"
        )
        checks["macro_regressions"] = _gate_check(
            state=macro_state,
            observed=macro_results,
            requirement={"minimum_group_size": subgroup_minimum, "minimum_delta": macro_floor},
        )

        human_safe = bool(
            score
            and score.get("complete")
            and score.get("llm_judge_used") is False
            and score.get("adjudication_method") == "exact_identity_plus_blinded_human_source_audit"
            and not score.get("pending_comparisons")
            and not score.get("invalid_comparisons")
        )
        checks["adjudication_safety"] = _gate_check(
            state="passed" if human_safe else ("failed" if score is not None else "pending"),
            observed={
                "method": (score or {}).get("adjudication_method"),
                "llm_judge_used": (score or {}).get("llm_judge_used"),
                "human_reviews_validated": (score or {}).get("human_reviews_validated"),
            },
            requirement="exact identity plus explicit blinded human provenance, or a separately passed expanded >=60-case LLM judge fixture",
        )

        core_invalid = sum(
            int(((result.get("ownership") or {}).get("invalid_evidence_events") or 0))
            for block in (phase_one or {}).get("blocks") or []
            for result in (block.get("candidate_core") or {}).get("results") or []
        )
        effective_invalid = (
            int((repair or {}).get("effective_exact_evidence_exceptions") or 0)
            if variant_name == "repaired" and repair is not None
            else core_invalid
        )
        enrichment_nonexact = int(
            ((enrichment or {}).get("repair_trigger_counts") or {}).get("nonexact_evidence") or 0
        )
        exact_passed = bool(enrichment is not None and effective_invalid == 0 and enrichment_nonexact == 0)
        checks["exact_evidence"] = _gate_check(
            state="passed" if exact_passed else ("failed" if enrichment is not None else "pending"),
            observed={
                "core_exceptions": effective_invalid,
                "enrichment_nonexact_batches": enrichment_nonexact,
                "rate": 1.0 if exact_passed else None,
            },
            requirement=evaluator["gates"]["exact_evidence_rate_min"],
        )

        enrichment_triggers = {
            name: int(count or 0)
            for name, count in ((enrichment or {}).get("repair_trigger_counts") or {}).items()
            if name in allowed_triggers
        }
        if variant_name == "repaired":
            repair_calls_ok = bool(
                repair
                and int(repair.get("max_calls_per_segment") or 0)
                <= int(evaluator["failure_repair"]["max_headless_calls_per_segment"])
                and int(repair.get("attempted_calls") or 0)
                <= int(repair.get("triggered_segments") or 0)
                and int(repair.get("retry_attempts") or 0) == 0
                and repair.get("all_observable_triggers_cleared")
            )
            unresolved_core = sum(
                int(count or 0) for count in (repair or {}).get("unresolved_trigger_counts", {}).values()
            )
        else:
            repair_calls_ok = True
            unresolved_core = sum(core_trigger_counts.values())
        unresolved_enrichment = sum(enrichment_triggers.values())
        triggers_clear = bool(
            enrichment is not None
            and repair_calls_ok
            and unresolved_core == 0
            and unresolved_enrichment == 0
        )
        checks["observable_failure_repair"] = _gate_check(
            state="passed" if triggers_clear else ("failed" if enrichment is not None else "pending"),
            observed={
                "initial_core_triggers": core_trigger_counts,
                "unresolved_core_triggers": unresolved_core,
                "enrichment_triggers": enrichment_triggers,
                "repair_call_contract_ok": repair_calls_ok,
            },
            requirement="all real triggers clear; at most one repair call per segment; zero repair retries",
        )

        baseline_usage = (phase_one or {}).get("baseline", {}).get("usage") or {}
        core_usage = (phase_one or {}).get("candidate_core", {}).get("usage") or {}
        repair_usage = (repair or {}).get("usage") or {}
        enrichment_usage = (enrichment or {}).get("usage") or {}
        candidate_usage = _sum_usage_dicts([core_usage, repair_usage, enrichment_usage])
        baseline_tokens = int(baseline_usage.get("total_tokens") or 0)
        candidate_tokens = int(candidate_usage.get("total_tokens") or 0)
        baseline_wall = float((phase_one or {}).get("baseline", {}).get("wall_elapsed_seconds") or 0.0)
        candidate_wall = float((phase_one or {}).get("candidate_core", {}).get("wall_elapsed_seconds") or 0.0)
        candidate_wall += float((enrichment or {}).get("wall_elapsed_seconds") or 0.0)
        candidate_wall += float((repair or {}).get("wall_elapsed_seconds") or 0.0)
        marginal_token_ratio = _ratio(candidate_tokens, baseline_tokens)
        marginal_wall_ratio = _ratio(candidate_wall, baseline_wall)
        token_gate = float(evaluator["gates"]["max_total_token_ratio"])
        wall_gate = float(evaluator["gates"]["max_equal_concurrency_wall_ratio"])
        marginal_available = bool(enrichment is not None and baseline_tokens > 0 and baseline_wall > 0)
        checks["marginal_tokens"] = _gate_check(
            state=(
                "passed"
                if marginal_available and marginal_token_ratio is not None and marginal_token_ratio <= token_gate
                else "failed"
                if marginal_available
                else "pending"
            ),
            observed={"baseline": baseline_tokens, "candidate": candidate_tokens, "ratio": marginal_token_ratio},
            requirement={"maximum_ratio": token_gate, "includes_failed_calls_and_retries": True},
        )
        wall_comparable = bool(
            enrichment
            and ((enrichment.get("paired_efficiency") or {}).get("wall_comparability"))
            == "fully_paired_same_quota_window"
        )
        checks["marginal_equal_concurrency_wall"] = _gate_check(
            state=(
                "passed"
                if marginal_available
                and wall_comparable
                and marginal_wall_ratio is not None
                and marginal_wall_ratio <= wall_gate
                else "failed"
                if marginal_available
                else "pending"
            ),
            observed={
                "baseline_seconds": baseline_wall,
                "candidate_seconds": candidate_wall,
                "ratio": marginal_wall_ratio,
                "wall_comparability": (enrichment or {}).get("paired_efficiency", {}).get(
                    "wall_comparability"
                ),
            },
            requirement={"maximum_ratio": wall_gate, "same_worker_limit": evaluator["paired_run"]["concurrency"]},
        )

        exact_context_tokens = (context_cost or {}).get(
            "exact_context_tokens_production_amortized",
            (context_cost or {}).get("exact_context_tokens"),
        )
        if isinstance(exact_context_tokens, dict):
            exact_context_tokens = exact_context_tokens.get("total_tokens")
        context_wall = (context_cost or {}).get(
            "historical_wall_seconds_production_amortized",
            (context_cost or {}).get("historical_wall_seconds_sum"),
        )
        end_to_end_available = bool(
            context_cost
            and context_cost.get("end_to_end_cost_evaluable")
            and isinstance(exact_context_tokens, (int, float))
            and isinstance(context_wall, (int, float))
            and marginal_available
        )
        end_token_ratio = (
            _ratio(candidate_tokens + int(exact_context_tokens), baseline_tokens + int(exact_context_tokens))
            if end_to_end_available
            else None
        )
        estimated_context_tokens = (context_cost or {}).get("estimated_context_total_tokens_o200k")
        estimated_amortized_context_tokens = (context_cost or {}).get(
            "estimated_context_total_tokens_o200k_production_amortized"
        )
        estimated_end_token_ratio = (
            _ratio(
                candidate_tokens + int(estimated_context_tokens),
                baseline_tokens + int(estimated_context_tokens),
            )
            if marginal_available and isinstance(estimated_context_tokens, (int, float))
            else None
        )
        estimated_amortized_end_token_ratio = (
            _ratio(
                candidate_tokens + int(estimated_amortized_context_tokens),
                baseline_tokens + int(estimated_amortized_context_tokens),
            )
            if marginal_available and isinstance(estimated_amortized_context_tokens, (int, float))
            else None
        )
        end_wall_ratio = (
            _ratio(candidate_wall + float(context_wall), baseline_wall + float(context_wall))
            if end_to_end_available
            else None
        )
        unavailable_state = "failed" if context_cost is not None else "pending"
        checks["end_to_end_tokens_with_context"] = _gate_check(
            state=(
                "passed"
                if end_to_end_available and end_token_ratio is not None and end_token_ratio <= token_gate
                else "failed"
                if end_to_end_available
                else unavailable_state
            ),
            observed={
                "shared_context_tokens": exact_context_tokens,
                "ratio": end_token_ratio,
                "estimated_shared_context_tokens_o200k": estimated_context_tokens,
                "estimated_ratio_o200k_prompt_plus_output_only": estimated_end_token_ratio,
                "estimated_production_amortized_context_tokens_o200k": (
                    estimated_amortized_context_tokens
                ),
                "estimated_production_amortized_ratio_o200k": (
                    estimated_amortized_end_token_ratio
                ),
            },
            requirement={"maximum_ratio": token_gate, "shared_context_added_to_both": True},
            detail=(context_cost or {}).get("fail_closed_reason"),
        )
        checks["end_to_end_wall_with_context"] = _gate_check(
            state=(
                "passed"
                if end_to_end_available
                and wall_comparable
                and end_wall_ratio is not None
                and end_wall_ratio <= wall_gate
                else "failed"
                if end_to_end_available
                else unavailable_state
            ),
            observed={"shared_context_seconds": context_wall, "ratio": end_wall_ratio},
            requirement={"maximum_ratio": wall_gate, "shared_context_added_to_both": True},
            detail=(context_cost or {}).get("fail_closed_reason"),
        )

        accounting_complete = bool(
            phase_one
            and (phase_one.get("baseline") or {}).get("accounting_complete")
            and (phase_one.get("candidate_core") or {}).get("accounting_complete")
            and enrichment
            and enrichment.get("accounting_complete")
            and (repair is None or repair.get("accounting_complete"))
            and int((phase_one.get("baseline") or {}).get("attempted_calls") or 0)
            == len(requested_ids)
            - len(baseline_preparation_failures)
            + int((phase_one.get("baseline") or {}).get("retry_attempts") or 0)
        )
        checks["whole_pipeline_accounting"] = _gate_check(
            state="passed" if accounting_complete else ("failed" if enrichment is not None else "pending"),
            observed={
                "baseline_attempted_calls": (phase_one or {}).get("baseline", {}).get("attempted_calls"),
                "baseline_retries": (phase_one or {}).get("baseline", {}).get("retry_attempts"),
                "baseline_preparation_failures": baseline_preparation_failures,
                "candidate_core_attempted_calls": (phase_one or {}).get("candidate_core", {}).get("attempted_calls"),
                "candidate_core_retries": (phase_one or {}).get("candidate_core", {}).get("retry_attempts"),
                "repair_attempted_calls": (repair or {}).get("attempted_calls"),
                "enrichment_batches": len((enrichment or {}).get("batch_results") or []),
            },
            requirement="all failed calls, retries, quota waits, validator outcomes, and fallback calls accounted",
        )

        return {
            "path_name": path_name,
            "core_variant": variant_name,
            "state": _combined_gate_state(checks),
            "checks": checks,
            "quality_f1": bootstrap.get("candidate_f1"),
            "marginal_total_token_ratio": marginal_token_ratio,
            "marginal_equal_concurrency_wall_ratio": marginal_wall_ratio,
            "end_to_end_total_token_ratio": end_token_ratio,
            "estimated_end_to_end_total_token_ratio_o200k": estimated_end_token_ratio,
            "estimated_production_amortized_end_to_end_total_token_ratio_o200k": (
                estimated_amortized_end_token_ratio
            ),
            "end_to_end_equal_concurrency_wall_ratio": end_wall_ratio,
        }

    path_reports: dict[str, Any] = {}
    path_selection_checks: dict[str, dict[str, Any]] = {}
    bundle_paths = bundle.get("paths") or {}
    for path_name in ("spark_only", "mini_only"):
        path_evidence = bundle_paths.get(path_name) or {}
        variants = {}
        for variant_name in ("unrepaired", "repaired"):
            variant_evidence = path_evidence.get(variant_name)
            if not isinstance(variant_evidence, dict):
                variants[variant_name] = {
                    "path_name": path_name,
                    "core_variant": variant_name,
                    "state": "pending",
                    "checks": {
                        "artifacts": _gate_check(
                            state="pending",
                            observed=None,
                            requirement=f"{path_name} {variant_name} evidence",
                        )
                    },
                }
            else:
                variants[variant_name] = variant_report(path_name, variant_name, variant_evidence)
        promotable = [name for name, item in variants.items() if item["state"] == "passed"]
        selected = "unrepaired" if "unrepaired" in promotable else "repaired" if "repaired" in promotable else None
        path_reports[path_name] = {
            "variants": variants,
            "promotable_variants": promotable,
            "selected_variant": selected,
        }
        variant_states = {item["state"] for item in variants.values()}
        selection_state = (
            "passed"
            if selected is not None
            else "failed"
            if variant_states == {"failed"}
            else "inconclusive"
            if "inconclusive" in variant_states
            else "pending"
        )
        path_selection_checks[path_name] = _gate_check(
            state=selection_state,
            observed={"selected_variant": selected, "variant_states": sorted(variant_states)},
            requirement=f"at least one separately scored {path_name} variant passes every per-path gate",
        )

    global_checks["separate_spark_and_mini_paths"] = _gate_check(
        state=_combined_gate_state(path_selection_checks),
        observed={name: item["observed"] for name, item in path_selection_checks.items()},
        requirement="Spark-only and Mini-only/fallback paths independently pass; mixed outputs are forbidden",
    )
    global_state = _combined_gate_state(global_checks)
    all_states = [global_state] + [item["state"] for item in path_selection_checks.values()]
    if "failed" in all_states:
        decision = "failed"
    elif "inconclusive" in all_states:
        decision = "inconclusive"
    elif "pending" in all_states:
        decision = "pending"
    else:
        decision = "passed"
    report = {
        "schema_version": PAIRED_FAIL_CLOSED_REPORT_VERSION,
        "generated_at": now_iso(),
        "evidence_bundle_path": str(bundle_file),
        "evidence_bundle_sha256": _sha256_file(bundle_file),
        "evaluator_spec_path": str(evaluator_path),
        "evaluator_spec_sha256": _sha256_file(evaluator_path),
        "fail_closed": True,
        "intent_to_treat": True,
        "decision": decision,
        "acceptance_eligible": decision == "passed",
        "promotion_allowed": False,
        "global_checks": global_checks,
        "path_selection_checks": path_selection_checks,
        "paths": path_reports,
        "artifact_errors": artifact_errors,
        "retrospective_validation": retrospective,
        "structural_recall_ceiling": ceiling,
        "privacy": "sanitized_gate_metadata_no_source_text",
    }
    destination = (
        Path(output_path).expanduser().resolve()
        if output_path is not None
        else bundle_file.parent / "fail-closed-gate-report.json"
    )
    write_text_atomic(destination, json.dumps(report, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
    return {**report, "report_path": str(destination)}
