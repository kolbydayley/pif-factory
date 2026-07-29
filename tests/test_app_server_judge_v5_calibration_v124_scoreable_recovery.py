from __future__ import annotations

from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v123_capped_field_repair import (
    _validate_v122,
)
from research_factory.app_server_judge_v5_calibration_v124_scoreable_recovery import (
    _validate_v123,
    freeze_v124,
    recover_v123,
    score_recovered_v123,
)


def test_v124_removes_only_one_exact_duplicate_span_and_preserves_semantics():
    v123 = _validate_v123()
    recovered, audit = recover_v123(v123)
    assert audit["removed_exact_duplicate_span_count"] == 1
    assert audit["changed_row_count"] == 1
    assert audit["field_status_change_count"] == 0
    assert audit["rationale_change_count"] == 0
    assert audit["new_semantic_turn_count"] == 0
    assert len(recovered["decisions"]) == 9


def test_v124_classifies_the_recovered_output_as_quality_disagreement():
    v123 = _validate_v123()
    v122 = _validate_v122()
    recovered, _ = recover_v123(v123)
    score, full_score, _ = score_recovered_v123(
        v123=v123, recovered=recovered, v122=v122
    )
    assert score["passed"] is False
    assert score["metrics"]["matched_control_exact_count"] == 3
    assert score["metrics"]["repair_gate_exact_count"] == 0
    assert score["metrics"]["evidence_complete_count"] == 9
    assert full_score["passed"] is False
    assert full_score["metrics"]["observable_repair_trigger_count"] == 5


def test_v124_freeze_is_idempotent_zero_token_and_keeps_later_gates_closed(
    tmp_path: Path,
):
    root = tmp_path / "v124"
    first = freeze_v124(output_dir=root)
    second = freeze_v124(output_dir=root)
    assert first == second
    assert first["state"] == "inactive"
    assert first["quality_result_scoreable"] is True
    assert first["quality_gate_passed"] is False
    assert first["final_owner_authorized"] is True
    assert first["retained_field_reference_patch_authorized"] is False
    assert first["fresh_diagnostic_authorized"] is False
    assert first["selection_authorized"] is False
    assert first["holdout_authorized"] is False
    assert first["production_mutated"] is False
    assert first["usage"]["total_tokens"] == 0
    assert first["predecessor_v123_usage"]["total_tokens"] == 26894
