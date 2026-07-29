from __future__ import annotations

import json
from pathlib import Path

import pytest

from research_factory.app_server_judge_v5_calibration_v26_diagnostic import _load_json
from research_factory.app_server_judge_v5_calibration_v95_reference_v6_freeze import (
    DEFAULT_OUTPUT_ROOT as V95_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v97_residual_field_audit import (
    DEFAULT_OUTPUT_ROOT as V97_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v98_reference_v7_freeze import (
    JudgeV5CalibrationV98Error,
    build_v98_reference,
    freeze_v98,
)


def _sources():
    return (
        _load_json(V95_ROOT / "calibration-truth-v6.private.json", "v95 truth"),
        _load_json(V97_ROOT / "reference-patch-proposal.json", "v97 proposal"),
    )


def test_v98_applies_two_authorized_changes_and_preserves_proposition_truth():
    base, patch = _sources()
    reference, audit = build_v98_reference(base, patch)
    assert reference["reference_version"] == "fixture_reference_v7_v98_residual_field_reconciled"
    assert audit["operation_count"] == 4
    assert audit["reference_change_count"] == 2
    assert audit["field_change_counts"] == {"metric": 1, "speaker": 1}
    assert audit["normalization_operation_count"] == 1
    for case_id, case in reference["cases"].items():
        assert case["proposition"] == base["cases"][case_id]["proposition"]
        for witness_id, fields in case["field_issues"].items():
            assert case["structured_fields"][witness_id] == (
                "incorrect" if fields else "correct"
            )


def test_v98_rejects_patch_drift():
    base, patch = _sources()
    changed = json.loads(json.dumps(patch))
    changed["changes"] = changed["changes"][:-1]
    with pytest.raises(JudgeV5CalibrationV98Error, match="coverage"):
        build_v98_reference(base, changed)


def test_v98_freeze_is_zero_token_immutable_and_keeps_downstream_closed(tmp_path: Path):
    root = tmp_path / "v98"
    first = freeze_v98(output_dir=root)
    second = freeze_v98(output_dir=root)
    assert first == second
    assert first["reference_frozen"] is True
    assert first["fresh_diagnostic_authorized"] is True
    assert first["full_calibration_authorized"] is False
    assert first["selection_authorized"] is False
    assert first["holdout_authorized"] is False
    assert first["production_mutated"] is False
    assert first["usage"]["total_tokens"] == 0
