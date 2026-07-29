from __future__ import annotations

import json
from pathlib import Path

from research_factory.app_server_judge_v5_selection_v192_residual_repair_design import (
    BASE_ARMS,
    DIAGNOSTIC_CASE_COUNT,
    MAXIMUM_TOTAL_TOKENS_PER_TURN,
    _all_composite_score,
    _dynamic_oracle_audit,
    _selected_cases,
    _validate_v191_nonacceptance,
    freeze_v192,
)
from research_factory.app_server_judge_v5_selection_v186_convergence_checkpoint import (
    _selection_sources,
)


def test_v192_preserves_v191_and_finds_nonzero_residual_ceiling():
    predecessor = _validate_v191_nonacceptance()
    assert predecessor["terminal"]["semantic_quality_passed"] is False
    assert predecessor["terminal"]["production_amortized_token_target_passed"] is True
    augmented, score, _combinations = _all_composite_score()
    oracle = _dynamic_oracle_audit(augmented=augmented, score=score)
    assert oracle["bootstrap"]["ci_lower"] >= -0.03
    assert oracle["worst_source_delta"] >= -0.05
    assert oracle["remaining_headroom_tokens"] > 100000
    assert oracle["production_semantic_routing_by_reference_forbidden"] is True


def test_v192_case_selection_is_balanced_and_uses_existing_batch8_outputs():
    sources = _selection_sources()
    _augmented, score, _combinations = _all_composite_score()
    cases = _selected_cases(sources=sources, score=score)
    assert len(cases) == DIAGNOSTIC_CASE_COUNT
    assert len({row["case_id"] for row in cases}) == DIAGNOSTIC_CASE_COUNT
    assert {row["source_id"] for row in cases} == {
        "corecursive",
        "latent-space",
        "odd-lots",
        "practical-ai",
    }
    assert sum(row["density_stratum"] == "no_signal" for row in cases) == 1
    assert sum(row["candidate_event_count"] for row in cases) == 47
    assert BASE_ARMS == ("batch_8_new_thread", "batch_8_same_thread")


def test_v192_freeze_is_presemantic_and_within_repair_budget(tmp_path: Path):
    root = tmp_path / "v192"
    first = freeze_v192(output_dir=root)
    second = freeze_v192(output_dir=root)
    assert first == second
    assert first["semantic_attempt_authorized"] is True
    assert first["authorized_turn_count"] == 1
    assert first["holdout_authorized"] is False
    assert first["production_mutated"] is False
    design = json.loads((root / "residual-repair-design.json").read_text())
    assert design["declared_turn_count"] == 1
    assert design["retry_count_per_turn"] == 0
    assert design["repair_budget"]["declared_bound_fits_production_headroom"] is True
    assert design["maximum_total_tokens_per_turn"] == MAXIMUM_TOTAL_TOKENS_PER_TURN
    assert design["prompt_bytes"] < 240000
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("capacity.json"))
