from __future__ import annotations

import json
from pathlib import Path

import pytest

from research_factory.app_server_judge_v5_calibration_v26_diagnostic import _load_json
from research_factory.app_server_judge_v5_calibration_v90_reference_v5_freeze import (
    DEFAULT_OUTPUT_ROOT as V90_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v94_systematic_field_audit import (
    DEFAULT_OUTPUT_ROOT as V94_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v95_reference_v6_freeze import (
    JudgeV5CalibrationV95Error,
    build_v95_reference,
    freeze_v95,
)


def _sources():
    return (
        _load_json(V90_ROOT / "calibration-truth-v5.private.json", "v90 truth"),
        _load_json(V94_ROOT / "reference-patch-proposal.json", "v94 proposal"),
    )


def test_v95_applies_only_authorized_field_changes_and_normalizes_statuses():
    base, patch = _sources()
    reference, audit = build_v95_reference(base, patch)
    assert reference["reference_version"] == "fixture_reference_v6_v95_systematic_field_reconciled"
    assert len(reference["cases"]) == 66
    assert sum(len(case["field_issues"]) for case in reference["cases"].values()) == 182
    assert audit["operation_count"] == 25
    assert audit["reference_change_count"] == 14
    assert audit["field_change_counts"] == {"certainty": 12, "target": 2}
    assert audit["normalization_operation_count"] == 2
    assert audit["proposition_truth_changed"] is False
    for case_id, case in reference["cases"].items():
        assert case["proposition"] == base["cases"][case_id]["proposition"]
        for witness_id, fields in case["field_issues"].items():
            assert case["structured_fields"][witness_id] == (
                "incorrect" if fields else "correct"
            )


def test_v95_rejects_patch_count_or_prior_status_drift():
    base, patch = _sources()
    changed = json.loads(json.dumps(patch))
    changed["changes"] = changed["changes"][:-1]
    with pytest.raises(JudgeV5CalibrationV95Error, match="coverage"):
        build_v95_reference(base, changed)

    changed = json.loads(json.dumps(patch))
    changed["changes"][0]["prior_status"] = "correct"
    with pytest.raises(JudgeV5CalibrationV95Error, match="status"):
        build_v95_reference(base, changed)


def test_v95_freeze_is_zero_token_immutable_and_keeps_downstream_closed(tmp_path: Path):
    root = tmp_path / "v95"
    first = freeze_v95(output_dir=root)
    second = freeze_v95(output_dir=root)
    assert first == second
    assert first["state"] == "completed"
    assert first["reference_frozen"] is True
    assert first["fresh_diagnostic_authorized"] is True
    assert first["full_calibration_authorized"] is False
    assert first["selection_authorized"] is False
    assert first["holdout_authorized"] is False
    assert first["production_mutated"] is False
    assert first["semantic_attempt_started"] is False
    assert first["usage"]["total_tokens"] == 0
    receipt = _load_json(root / "reference-receipt.json", "receipt")
    assert receipt["reference_change_count"] == 14
    assert receipt["normalization_operation_count"] == 2
    assert receipt["new_semantic_turn_count"] == 0
