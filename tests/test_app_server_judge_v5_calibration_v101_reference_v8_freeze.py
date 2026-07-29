from __future__ import annotations

import json
from pathlib import Path

import pytest

from research_factory.app_server_judge_v5_calibration_v26_diagnostic import _load_json
from research_factory.app_server_judge_v5_calibration_v98_reference_v7_freeze import (
    DEFAULT_OUTPUT_ROOT as V98_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v100_stance_inference_audit import (
    DEFAULT_OUTPUT_ROOT as V100_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v101_reference_v8_freeze import (
    JudgeV5CalibrationV101Error,
    build_v101_reference,
    freeze_v101,
)


def _sources():
    return (
        _load_json(V98_ROOT / "calibration-truth-v7.private.json", "v98 truth"),
        _load_json(V100_ROOT / "reference-patch-proposal.json", "v100 proposal"),
    )


def test_v101_applies_one_authorized_change_and_preserves_proposition_truth():
    base, patch = _sources()
    reference, audit = build_v101_reference(base, patch)

    assert reference["reference_version"] == (
        "fixture_reference_v8_v101_stance_inference_reconciled"
    )
    assert audit["operation_count"] == 2
    assert audit["reference_change_count"] == 1
    assert audit["field_change_counts"] == {"unsupported_inference": 1}
    assert sum(row["reference_changed"] for row in audit["operations"]) == 1
    for case_id, case in reference["cases"].items():
        assert case["proposition"] == base["cases"][case_id]["proposition"]
        for witness_id, fields in case["field_issues"].items():
            assert case["structured_fields"][witness_id] == (
                "incorrect" if fields else "correct"
            )


def test_v101_rejects_patch_coverage_drift():
    base, patch = _sources()
    changed = json.loads(json.dumps(patch))
    changed["changes"] = changed["changes"][:-1]

    with pytest.raises(JudgeV5CalibrationV101Error, match="coverage"):
        build_v101_reference(base, changed)


def test_v101_freeze_is_zero_token_immutable_and_keeps_downstream_closed(
    tmp_path: Path,
):
    root = tmp_path / "v101"
    first = freeze_v101(output_dir=root)
    second = freeze_v101(output_dir=root)

    assert first == second
    assert first["reference_frozen"] is True
    assert first["fresh_diagnostic_authorized"] is True
    assert first["full_calibration_authorized"] is False
    assert first["selection_authorized"] is False
    assert first["holdout_authorized"] is False
    assert first["production_mutated"] is False
    assert first["semantic_attempt_started"] is False
    assert first["usage"]["total_tokens"] == 0
