from __future__ import annotations

from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v107_recovery_receipt import (
    _validate_v106,
    build_error_taxonomy,
    freeze_v107,
)


def test_v107_validates_all_v106_turns_and_usage():
    v106 = _validate_v106()

    assert len(v106["sidecar_records"]) == 25
    assert len(v106["output_records"]) == 25
    assert len(v106["capacity_records"]) == 25
    assert v106["usage"] == {
        "input_tokens": 644671,
        "cached_input_tokens": 46080,
        "output_tokens": 252709,
        "reasoning_output_tokens": 107660,
        "total_tokens": 897380,
    }


def test_v107_taxonomy_exposes_only_sanitized_counts_and_minimum_corrections():
    taxonomy = build_error_taxonomy(_validate_v106())

    assert taxonomy["failed_gates"] == [
        "alignment_f1",
        "equivalent_sensitivity",
        "order_bias",
        "relation_accuracy",
        "structured_field_accuracy",
        "support_specificity",
        "unpaired_exact_case_rate",
    ]
    assert taxonomy["counts"]["support"] == {
        "true_positive": 164,
        "false_negative": 4,
        "true_negative": 13,
        "false_positive": 1,
    }
    assert taxonomy["counts"]["structured_field"] == {
        "correct": 155,
        "incorrect": 27,
        "expected_correct_observed_incorrect": 26,
        "expected_incorrect_observed_correct": 1,
    }
    assert taxonomy["counts"]["alignment_pair"] == {
        "true_positive": 68,
        "false_positive": 8,
        "false_negative": 7,
    }
    assert taxonomy["minimum_corrections"] == {
        "support_specificity": 1,
        "structured_field_accuracy": 18,
        "alignment_f1_pair_replacements": 4,
        "equivalent_sensitivity": 2,
        "relation_accuracy": 4,
        "unpaired_exact_case_rate": 5,
        "order_bias": 1,
    }
    assert taxonomy["private_content_exposed"] is False


def test_v107_freeze_is_zero_token_inactive_and_idempotent(tmp_path: Path):
    root = tmp_path / "v107"
    first = freeze_v107(output_dir=root)
    second = freeze_v107(output_dir=root)

    assert first["terminal"] == second["terminal"]
    assert first["terminal"]["state"] == "inactive"
    assert first["terminal"]["terminal_reason"] == "inactive_incomplete_recovery_required"
    assert first["terminal"]["fresh_small_diagnostic_required"] is True
    assert first["terminal"]["fresh_full_calibration_authorized"] is False
    assert first["terminal"]["selection_authorized"] is False
    assert first["terminal"]["holdout_authorized"] is False
    assert first["terminal"]["production_mutated"] is False
    assert first["terminal"]["semantic_attempt_started"] is False
    assert first["terminal"]["usage"]["total_tokens"] == 0
