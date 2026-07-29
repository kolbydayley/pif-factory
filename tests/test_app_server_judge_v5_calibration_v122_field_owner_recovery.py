from __future__ import annotations

from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v122_field_owner_recovery import (
    _validate_v121,
    freeze_v122,
    recover_v121,
)


def test_v122_recovers_115_unique_task_decisions_and_real_quality_result():
    recovered, audit = recover_v121(_validate_v121())
    assert len(recovered["primary"]["decisions"]) == 115
    assert len({row["task_id"] for row in recovered["primary"]["decisions"]}) == 115
    assert recovered["score"]["passed"] is False
    assert recovered["score"]["capped_repair_authorized"] is True
    assert recovered["score"]["metrics"]["observable_repair_trigger_count"] == 5
    assert audit["root_cause"] == "generic_case_id_merge_helper_applied_to_task_id_decisions"
    assert audit["new_semantic_turn_count"] == 0
    assert audit["new_usage"]["total_tokens"] == 0


def test_v122_freeze_is_idempotent_zero_token_and_keeps_later_gates_closed(
    tmp_path: Path,
):
    root = tmp_path / "v122"
    first = freeze_v122(output_dir=root)
    second = freeze_v122(output_dir=root)
    assert first == second
    assert first["state"] == "inactive"
    assert first["capped_repair_authorized"] is True
    assert first["retained_field_reference_patch_authorized"] is False
    assert first["fresh_diagnostic_authorized"] is False
    assert first["selection_authorized"] is False
    assert first["holdout_authorized"] is False
    assert first["production_mutated"] is False
    assert first["semantic_attempt_started"] is False
    assert first["usage"]["total_tokens"] == 0
    assert first["predecessor_v121_usage"]["total_tokens"] == 178899
