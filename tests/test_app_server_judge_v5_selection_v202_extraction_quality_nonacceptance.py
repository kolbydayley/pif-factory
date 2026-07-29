from __future__ import annotations

import json
from pathlib import Path

from research_factory.app_server_judge_v5_selection_v202_extraction_quality_nonacceptance import (
    _validate_v201_nonacceptance,
    freeze_v202,
)


def test_v202_validates_measured_quality_failure_and_token_pass():
    predecessor = _validate_v201_nonacceptance()
    assert predecessor["report"]["production_amortized_token_target_passed"] is True
    assert predecessor["report"]["production_amortized_total_token_ratio"] == 0.236727
    assert predecessor["report"]["semantic_gate"]["semantic_passed"] is False
    assert predecessor["report"]["semantic_gate"]["bootstrap"][
        "candidate_minus_baseline"
    ] == -0.263555


def test_v202_binds_conflict_perfect_upper_bound_that_still_fails():
    predecessor = _validate_v201_nonacceptance()
    oracle = predecessor["audit"]["conflict_perfect_oracle_gate"]
    assert oracle["semantic_passed"] is False
    assert oracle["bootstrap"]["candidate_minus_baseline"] == -0.172646
    assert oracle["bootstrap"]["ci_upper"] == -0.122663
    assert predecessor["terminal"]["conflict_recovery_could_change_verdict"] is False


def test_v202_freezes_zero_token_nonacceptance_and_keeps_holdout_closed(
    tmp_path: Path,
):
    root = tmp_path / "v202"
    first = freeze_v202(output_dir=root)
    second = freeze_v202(output_dir=root)
    assert first == second
    assert first["state"] == "waiting_for_extraction_quality_strategy_authorization"
    assert first["terminal_classification"] == "external_policy_authorization_required"
    assert first["overall_evaluation_complete"] is False
    assert first["evaluation_accepted"] is False
    assert first["development_quality_passed"] is False
    assert first["production_amortized_token_target_passed"] is True
    assert first["production_amortized_total_token_ratio"] == 0.236727
    assert first["conflict_recovery_could_change_verdict"] is False
    assert first["extraction_rerun_authorized"] is False
    assert first["holdout_authorized"] is False
    assert first["production_mutated"] is False
    assert first["usage"]["total_tokens"] == 0
    assert first["cumulative_unknown_usage_turn_count"] == 2
    report = json.loads((root / "development-nonacceptance-report.json").read_text())
    assert report["blocker_class"] == "measured_development_extraction_quality_shortfall"
    assert report["safe_local_judge_only_experiment_remaining"] is False
    assert report["evaluation_acceptance_receipt_emitted"] is False
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
