from __future__ import annotations

from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v105_reconciliation_receipt import (
    _validate_predecessors,
    build_reconciliation_audit,
    freeze_v105,
)


def test_v105_preserves_raw_and_adjudicated_stability_metrics():
    audit = build_reconciliation_audit(_validate_predecessors())

    assert audit["protocol_regression_primary_exact_count"] == 6
    assert audit["raw_permutation_canary_exact_count"] == 1
    assert audit["raw_permutation_canary_count"] == 2
    assert audit["observable_disagreement_count"] == 1
    assert audit["side_free_adjudication_call_count"] == 1
    assert audit["side_free_control_exact_count"] == 3
    assert audit["adjudicated_permutation_exact_count"] == 2
    assert audit["adjudicated_permutation_count"] == 2
    assert audit["reference_change_applied"] is False
    assert audit["majority_voting_used"] is False


def test_v105_is_zero_token_immutable_and_authorizes_only_full_calibration(
    tmp_path: Path,
):
    root = tmp_path / "v105"
    first = freeze_v105(output_dir=root)
    second = freeze_v105(output_dir=root)

    assert first == second
    assert first["fresh_full_development_calibration_authorized"] is True
    assert first["selection_authorized"] is False
    assert first["holdout_authorized"] is False
    assert first["production_mutated"] is False
    assert first["semantic_attempt_started"] is False
    assert first["usage"]["total_tokens"] == 0
