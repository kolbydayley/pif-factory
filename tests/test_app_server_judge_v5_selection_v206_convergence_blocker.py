from __future__ import annotations

import json
from pathlib import Path

from research_factory.app_server_judge_v5_selection_v206_convergence_blocker import (
    _cost_envelope,
    _select_canary,
    _validate_historical_frontier,
    _validate_v205_quality_failure,
    freeze_v206,
)


def test_v206_binds_v205_as_measured_quality_failure_not_transport():
    predecessor = _validate_v205_quality_failure()
    assert predecessor["terminal"]["usage_status"] == "complete"
    assert predecessor["terminal"]["accounting_complete"] is True
    assert predecessor["terminal"]["usage"]["total_tokens"] == 192_062
    assert predecessor["terminal"]["production_amortized_total_token_ratio"] == 0.131219
    assert predecessor["gate"]["dense_median_candidate_to_reference_event_count_ratio"] == 0.445
    assert predecessor["report"]["candidate_events"] == 110
    assert predecessor["report"]["golden_events"] == 255
    assert len(predecessor["sidecar_records"]) == 4
    assert len(predecessor["capacity_records"]) == 4


def test_v206_binds_historical_frontier_without_importing_heavy_lineage():
    historical = _validate_historical_frontier()
    assert len(historical["records"]) == 4
    assert historical["values"]["v201_report"]["viable_systems"] == []
    assert (
        historical["values"]["v192_oracle"]["purpose"]
        == "development_ceiling_only_not_a_selectable_system"
    )


def test_v206_canary_is_small_balanced_and_cost_bounded():
    predecessor = _validate_v205_quality_failure()
    selected = _select_canary(predecessor)
    assert len(selected) == 8
    assert len({row["episode_id"] for row in selected}) == 4
    assert len({row["source_id"] for row in selected}) == 4
    assert sum(row["density_stratum"] == "dense" for row in selected) == 4
    assert sum(row["density_stratum"] == "no_signal" for row in selected) == 4
    cost = _cost_envelope()
    assert cost["remaining_development_extraction_token_envelope"] == 399_346
    assert cost["combined_development_extraction_token_bound"] == 588_062
    assert cost["production_amortized_total_token_ratio_bound"] == 0.278753
    assert cost["passed_lte_0_28"] is True


def test_v206_freezes_truthful_nonacceptance_and_prepared_next_experiment(
    tmp_path: Path,
):
    root = tmp_path / "v206"
    first = freeze_v206(output_dir=root)
    second = freeze_v206(output_dir=root)
    assert first == second
    assert first["state"] == "waiting_for_next_semantic_strategy_authorization"
    assert first["terminal_classification"] == "inactive_incomplete_recovery_required"
    assert first["overall_evaluation_complete"] is False
    assert first["development_winner_frozen"] is False
    assert first["viable_system_count"] == 0
    assert first["can_more_cases_change_v205_verdict"] is False
    assert first["materially_new_extraction_strategy_prepared"] is True
    assert first["next_semantic_attempt_started"] is False
    assert first["holdout_authorized"] is False
    assert first["production_mutated"] is False
    assert first["usage"]["total_tokens"] == 0
    report = json.loads((root / "convergence-report.json").read_text())
    assert report["viable_systems_meeting_joint_gates"] == []
    assert report["can_more_cases_change_v205_verdict"] is False
    assert report["can_a_materially_new_extraction_strategy_change_joint_feasibility"] is True
    experiment = json.loads((root / "next-experiment.json").read_text())
    assert experiment["state"] == "prepared_not_launched"
    assert experiment["canary_case_count"] == 8
    assert experiment["retry_count_per_turn"] == 0
    assert experiment["holdout_authorized"] is False
    assert experiment["cost_envelope"]["passed_lte_0_28"] is True
    spec = json.loads((root / "convergence-spec.json").read_text())
    assert spec["semantic_model_calls_started"] == 0
    assert len(spec["v205_attempt"]["sidecars"]) == 4
    assert len(spec["historical_frontier"]) == 4
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
