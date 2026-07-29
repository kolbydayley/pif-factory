from __future__ import annotations

import json
import tempfile
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v85_reference_v4_freeze import (
    DEFAULT_OUTPUT_ROOT as V85_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v88_residual_reference_audit import (
    DEFAULT_OUTPUT_ROOT as V88_ROOT,
    score_v88,
)
from research_factory.app_server_judge_v5_calibration_v89_control_truth_audit import (
    DEFAULT_OUTPUT_ROOT as V89_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v90_reference_v5_freeze import (
    build_v90_reference,
    derive_v88_dispute_patch,
    freeze_v90,
    reconcile_v88_truth,
)


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_v90_authorized_truth_reconciliation_makes_v88_pass() -> None:
    truth, operations = reconcile_v88_truth(
        _load(V88_ROOT / "residual-reference-truth.private.json"),
        _load(V89_ROOT / "control-truth-patch-proposal.json"),
    )
    score = score_v88(
        _load(V88_ROOT / "residual-reference-output.private.json"),
        _load(V88_ROOT / "permutation-canary-output.private.json"),
        truth,
    )
    assert len(operations) == 2
    assert score["passed"] is True
    assert score["metrics"]["matched_control_exact_rate"] == 1.0
    assert score["metrics"]["permutation_canary_exact_rate"] == 1.0
    assert score["reference_patch_authorized"] is True


def test_v90_combined_patch_has_five_operations_four_changes() -> None:
    truth, _ = reconcile_v88_truth(
        _load(V88_ROOT / "residual-reference-truth.private.json"),
        _load(V89_ROOT / "control-truth-patch-proposal.json"),
    )
    disputes = derive_v88_dispute_patch(
        truth, _load(V88_ROOT / "residual-reference-output.private.json")
    )
    changes = _load(V89_ROOT / "control-truth-patch-proposal.json")["changes"] + disputes
    reference, audit = build_v90_reference(
        _load(V85_ROOT / "calibration-truth-v4.private.json"), changes
    )
    assert audit["operation_count"] == 5
    assert audit["reference_change_count"] == 4
    assert audit["field_change_counts"] == {"certainty": 2, "target": 2}
    assert len(reference["cases"]) == 66
    assert sum(len(case["field_issues"]) for case in reference["cases"].values()) == 182


def test_v90_freeze_is_zero_token_immutable_and_nonpromoting() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "v90"
        terminal = freeze_v90(output_dir=root)
        assert freeze_v90(output_dir=root) == terminal
        assert terminal["state"] == "completed"
        assert terminal["reference_frozen"] is True
        assert terminal["fresh_diagnostic_authorized"] is True
        assert terminal["full_calibration_authorized"] is False
        assert terminal["selection_authorized"] is False
        assert terminal["holdout_authorized"] is False
        assert terminal["production_mutated"] is False
        assert terminal["semantic_attempt_started"] is False
        assert terminal["usage"]["total_tokens"] == 0
        assert (root / "calibration-truth-v5.private.json").is_file()
        assert (root / "reference-receipt.json").is_file()
        assert not (root / "turns").exists()
