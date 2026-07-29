from __future__ import annotations

import json
from pathlib import Path

from research_factory.app_server_judge_v5_selection_v186_convergence_checkpoint import (
    _completed_alignment_sets,
    _optimistic_remaining_bound,
    _sanitized_semantic_score,
    _score_subset,
    _selection_sources,
    _validate_v185_operator_stop,
    _build_consensus,
    freeze_v186,
)


def test_v186_preserves_five_measured_v185_turns_and_four_never_started():
    stop = _validate_v185_operator_stop()
    assert len(stop["completed"]) == 5
    assert len(stop["never_started"]) == 4
    assert stop["usage"]["total_tokens"] == 405354
    assert stop["cumulative_known_lower_bound"]["total_tokens"] == 8087855
    assert stop["cumulative_unknown_usage_turn_count"] == 1
    assert stop["cumulative_unknown_usage_upper_bound"] == 120000


def test_v186_completed_evidence_has_stable_leader_but_no_viable_arm():
    stop = _validate_v185_operator_stop()
    sources = _selection_sources()
    sets = _completed_alignment_sets(stop)
    leaders = []
    for cases in sets.values():
        consensus, case_ids = _build_consensus(aligned_cases=cases, sources=sources)
        score = _score_subset(consensus=consensus, selected_case_ids=case_ids, sources=sources)
        sanitized = _sanitized_semantic_score(score)
        leaders.append(
            max(
                sanitized["systems"],
                key=lambda name: sanitized["systems"][name]["bootstrap"]["candidate_f1"],
            )
        )
        assert not any(row["semantic_passed"] for row in sanitized["systems"].values())
    assert leaders == ["batch_3_same_thread", "batch_3_same_thread"]


def test_v186_impossible_remaining_bound_cannot_clear_every_semantic_gate():
    stop = _validate_v185_operator_stop()
    sources = _selection_sources()
    cases = _completed_alignment_sets(stop)["all_completed"]
    consensus, case_ids = _build_consensus(aligned_cases=cases, sources=sources)
    score = _score_subset(consensus=consensus, selected_case_ids=case_ids, sources=sources)
    bound = _optimistic_remaining_bound(
        completed_score=score, completed_case_ids=case_ids, sources=sources
    )
    assert bound["remaining_case_count"] == 13
    assert bound["any_system_can_become_semantically_viable"] is False
    assert all(
        row["could_pass_all_frozen_semantic_noninferiority_checks"] is False
        for row in bound["systems"].values()
    )


def test_v186_freeze_is_zero_token_nonacceptance_and_idempotent(tmp_path: Path):
    root = tmp_path / "v186"
    first = freeze_v186(output_dir=root)
    second = freeze_v186(output_dir=root)
    assert first == second
    assert first["state"] == "inactive"
    assert first["terminal_classification"] == "inactive_incomplete_recovery_required"
    assert first["viable_systems"] == []
    assert first["quality_leader"] == "batch_3_same_thread"
    assert first["all_arms_cost_passed"] is True
    assert first["more_development_cases_can_change_viability"] is False
    assert first["remaining_alignment_calls_authorized"] is False
    assert first["holdout_authorized"] is False
    assert first["production_mutated"] is False
    spec = json.loads((root / "convergence-checkpoint-spec.json").read_text())
    assert spec["semantic_model_calls_declared"] == 0
    assert spec["semantic_model_calls_started"] == 0
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
