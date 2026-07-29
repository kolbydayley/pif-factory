from __future__ import annotations

from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v111_sol_reference_recovery import (
    _validate_v110,
    build_sanitized_taxonomy,
    freeze_v111,
    normalize_v110_alignment,
)
from research_factory import app_server_judge_v5_calibration_v110_sol_reference_audit as v110


def test_v111_validates_all_completed_v110_turns_and_usage():
    evidence = _validate_v110()

    assert len(evidence["turn_records"]) == 5
    assert evidence["usage"] == {
        "input_tokens": 130008,
        "cached_input_tokens": 0,
        "output_tokens": 69669,
        "reasoning_output_tokens": 19495,
        "total_tokens": 199677,
    }


def test_v111_normalizes_each_original_alignment_shard_before_scoring():
    evidence = _validate_v110()
    normalized = normalize_v110_alignment(evidence)
    score = v110.score_owner_audit(
        predecessor=evidence["predecessor"],
        sol_pointwise=evidence["values"]["pointwise_output"],
        sol_alignment=normalized,
        alignment_selection=evidence["values"]["selection"],
    )

    assert len(normalized["cases"]) == 12
    assert score["pointwise_control_exact_rate"] == 0.714286
    assert score["alignment_control_exact_rate"] == 0.875
    assert score["checks"] == {
        "pointwise_control_exact_rate": False,
        "alignment_control_exact_rate": True,
    }
    assert score["reference_patch_authorized"] is False
    assert score["pointwise_consensus_patch_count"] == 13
    assert score["alignment_consensus_patch_count"] == 2


def test_v111_taxonomy_is_sanitized_and_separates_support_from_fields():
    evidence = _validate_v110()
    taxonomy = build_sanitized_taxonomy(
        predecessor=evidence["predecessor"],
        sol_pointwise=evidence["values"]["pointwise_output"],
    )

    assert taxonomy["proposition_support_consensus_count"] == 53
    assert taxonomy["cohorts"]["control"]["counts"] == {
        "total": 14,
        "sol_truth_exact": 10,
        "sol_gpt55_exact": 10,
    }
    assert taxonomy["cohorts"]["candidate"]["counts"] == {
        "total": 39,
        "sol_truth_exact": 2,
        "sol_gpt55_exact": 13,
    }
    assert taxonomy["private_content_exposed"] is False


def test_v111_freeze_is_idempotent_zero_token_and_keeps_all_gates_closed(tmp_path: Path):
    root = tmp_path / "v111"
    first = freeze_v111(output_dir=root)
    second = freeze_v111(output_dir=root)

    assert first["terminal"] == second["terminal"]
    terminal = first["terminal"]
    assert terminal["state"] == "inactive"
    assert terminal["terminal_reason"] == "inactive_incomplete_recovery_required"
    assert terminal["reference_frozen"] is False
    assert terminal["fresh_full_calibration_authorized"] is False
    assert terminal["selection_authorized"] is False
    assert terminal["holdout_authorized"] is False
    assert terminal["production_mutated"] is False
    assert terminal["semantic_attempt_started"] is False
    assert terminal["usage"]["total_tokens"] == 0
    assert terminal["predecessor_v110_usage"]["total_tokens"] == 199677
