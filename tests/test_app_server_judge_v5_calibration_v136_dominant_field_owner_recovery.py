from __future__ import annotations

from pathlib import Path

import pytest

from research_factory.app_server_judge_v5_calibration_v136_dominant_field_owner_recovery import (
    JudgeV5CalibrationV136Error,
    _merge_by_task_id,
    _validate_v135,
    recover_v136,
)


def test_v136_validates_all_six_measured_v135_outputs():
    predecessor = _validate_v135()

    assert len(predecessor["turns"]) == 6
    assert sum(row["role"] == "primary" for row in predecessor["turns"]) == 3
    assert sum(row["role"] == "order_canary" for row in predecessor["turns"]) == 3
    assert predecessor["usage"]["total_tokens"] == 133874
    assert all(row["output"]["size_bytes"] > 0 for row in predecessor["turns"])


def test_v136_task_id_merge_fails_closed_on_overlap():
    output = {
        "decisions": [
            {
                "task_id": "same",
                "field_status": "correct",
                "source_evidence_spans": ["evidence"],
                "rationale": "expected",
            }
        ]
    }
    with pytest.raises(JudgeV5CalibrationV136Error, match="overlap"):
        _merge_by_task_id([output, output])


def test_v136_recovery_is_zero_model_idempotent_and_authorizes_only_expansion(
    tmp_path: Path,
):
    root = tmp_path / "v136"
    first = recover_v136(output_dir=root)
    second = recover_v136(output_dir=root)

    assert first == second
    assert first["state"] == "completed"
    assert first["expanded_field_owner_diagnostic_authorized"] is True
    assert first["reference_patch_authorized"] is False
    assert first["fresh_full_calibration_authorized"] is False
    assert first["selection_authorized"] is False
    assert first["holdout_authorized"] is False
    assert first["production_mutated"] is False
    assert first["semantic_attempt_started"] is False
    assert first["semantic_retry_count"] == 0
    assert first["usage_status"] == "no_new_semantic_usage"
    assert first["usage"]["total_tokens"] == 0
    assert first["reused_predecessor_usage"]["total_tokens"] == 133874
    assert first["metrics"]["matched_control_exact_count"] == 6
    assert first["metrics"]["order_canary_exact_count"] == 12
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("turns"))


def test_v136_preserves_v135_immutably(tmp_path: Path):
    before = _validate_v135()["records"]
    recover_v136(output_dir=tmp_path / "v136")
    after = _validate_v135()["records"]

    assert before == after
