from __future__ import annotations

import json
from pathlib import Path

from research_factory.app_server_judge_v5_selection_v186_convergence_checkpoint import (
    _selection_sources,
)
from research_factory.app_server_judge_v5_selection_v188_composite_postprocess import (
    _candidate_combinations,
    _validate_v187_success,
    _with_composite_systems,
    freeze_v188,
)


def test_v188_validates_both_measured_v187_turns_without_replay():
    success = _validate_v187_success()
    assert success["terminal"]["usage"]["total_tokens"] == 144663
    assert len(success["turn_records"]) == 2
    assert sum(row["usage"]["total_tokens"] for row in success["turn_records"]) == 144663
    assert success["terminal"]["semantic_retry_count"] == 0


def test_v188_enumerates_only_structurally_cost_and_cap_safe_composites():
    sources = _selection_sources()
    combinations, _contract = _candidate_combinations(sources=sources)
    assert len(combinations) == 26
    eligible = [row for row in combinations if row["cost_and_cap_eligible"]]
    assert eligible
    assert all(row["cost"]["passed_lte_0_28"] for row in eligible)
    assert all(row["max_exact_union_event_count"] <= 32 for row in eligible)
    augmented = _with_composite_systems(sources=sources, combinations=combinations)
    for row in eligible:
        assert row["system_id"] in augmented["membership"]["systems"]
        assert len(augmented["membership"]["system_cases"][row["system_id"]]) == 32


def test_v188_freeze_is_zero_token_and_keeps_holdout_closed(tmp_path: Path):
    root = tmp_path / "v188"
    first = freeze_v188(output_dir=root)
    second = freeze_v188(output_dir=root)
    assert first == second
    assert first["overall_evaluation_complete"] is False
    assert first["development_winner_frozen"] is False
    assert first["holdout_authorized"] is False
    assert first["production_mutated"] is False
    assert first["attempt_usage"]["total_tokens"] == 0
    spec = json.loads((root / "composite-postprocess-spec.json").read_text())
    assert spec["semantic_model_calls_started"] == 0
    assert spec["extraction_model_calls_started"] == 0
    assert spec["semantic_regex_or_keyword_rules_used"] is False
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("capacity.json"))
