from __future__ import annotations

from pathlib import Path

from research_factory.app_server_judge_v5_selection_v178_alignment_scale_postprocess_recovery import (
    _validate_v177_failure,
    freeze_v178,
    score_v178,
)


def test_v178_preserves_both_completed_v177_turns_and_keyerror_terminal():
    predecessor = _validate_v177_failure()
    assert predecessor["values"]["failure"]["error_class"] == "KeyError"
    assert predecessor["values"]["failure"]["failed_turn_name"] == (
        "selection_alignment_scale_canary"
    )
    assert predecessor["usage"]["total_tokens"] == 173535
    assert len(predecessor["attempts"]) == 2


def test_v178_corrected_normalized_projection_passes_scale_gate():
    predecessor = _validate_v177_failure()
    score = score_v178(predecessor["normalized_base"], predecessor["normalized_canary"])
    assert score["passed"] is True
    assert score["base_canary_projection_exact"] is True
    assert score["alignment_pair_count"] == 19
    assert score["abstained_pair_count"] == 0
    assert score["normalized_checklist_field_used"] == "checklist_decisions"


def test_v178_freeze_is_zero_token_idempotent_and_keeps_downstream_closed(tmp_path: Path):
    root = tmp_path / "v178"
    first = freeze_v178(output_dir=root)
    second = freeze_v178(output_dir=root)
    assert first["terminal"] == second["terminal"]
    assert first["terminal"]["full_alignment_authorized"] is True
    assert first["terminal"]["semantic_turn_count"] == 0
    assert first["terminal"]["usage"]["total_tokens"] == 0
    assert first["terminal"]["cumulative_evaluation_usage"]["total_tokens"] == 6842317
    assert first["terminal"]["selection_winner_frozen"] is False
    assert first["terminal"]["holdout_authorized"] is False
    assert first["terminal"]["production_mutated"] is False
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
