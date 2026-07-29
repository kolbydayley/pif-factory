from __future__ import annotations

from pathlib import Path

from research_factory.app_server_judge_v5_selection_v200_reference_conflict_abstention import (
    _validate_v199_timeout,
    build_abstention_reconciliation,
    freeze_v200,
)


def test_v200_preserves_v199_timeout_as_unknown_usage():
    predecessor = _validate_v199_timeout()
    assert predecessor["failure"]["error_class"] == "AppServerTurnTimeout"
    assert predecessor["terminal"]["usage_status"] == "unknown"
    assert predecessor["terminal"]["cumulative_unknown_usage_turn_count"] == 2
    assert predecessor["attempt"]["output"] is None


def test_v200_abstains_the_two_cases_containing_three_unresolved_repairs():
    predecessor = _validate_v199_timeout()
    reconciliation, audit = build_abstention_reconciliation(predecessor)
    assert audit["unresolved_repair_placement_count"] == 3
    assert audit["abstained_original_case_count"] == 2
    assert audit["post_reconciliation_reference_conflict_count"] == 0
    assert audit["new_semantic_model_call_count"] == 0
    assert len(reconciliation["abstained_original_case_ids"]) == 2


def test_v200_freeze_is_idempotent_zero_token_and_preholdout(tmp_path: Path):
    root = tmp_path / "v200"
    first = freeze_v200(output_dir=root)
    second = freeze_v200(output_dir=root)
    assert first["terminal"] == second["terminal"]
    assert first["spec"]["semantic_model_calls_started"] == 0
    assert first["spec"]["v199_semantic_attempt_replayed"] is False
    assert first["terminal"]["usage"]["total_tokens"] == 0
    assert first["terminal"]["cumulative_unknown_usage_turn_count"] == 2
    assert first["terminal"]["scoring_authorized"] is True
    assert first["terminal"]["holdout_authorized"] is False
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
