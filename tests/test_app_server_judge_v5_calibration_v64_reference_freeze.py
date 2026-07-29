from __future__ import annotations

import json
import tempfile
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v53_reference import (
    DEFAULT_OUTPUT_ROOT as V53_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v63_reference_reaudit import (
    DEFAULT_OUTPUT_ROOT as V63_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v64_reference_freeze import (
    build_v64_reference,
    freeze_v64_reference,
)


def test_v64_changes_exactly_five_structured_truth_rows() -> None:
    base = json.loads((V53_ROOT / "scoped-calibration-truth.private.json").read_text())
    proposal = json.loads((V63_ROOT / "reference-patch-proposal.json").read_text())
    patched, audit = build_v64_reference(base, proposal)
    assert audit["operation_count"] == 5
    assert audit["proposition_truth_changed"] is False
    assert audit["alignment_truth_changed"] is False
    changed = {
        (operation["case_id"], operation["witness_id"])
        for operation in audit["operations"]
    }
    actual = set()
    for case_id, case in base["cases"].items():
        for witness_id, fields in case["field_issues"].items():
            if fields != patched["cases"][case_id]["field_issues"][witness_id]:
                actual.add((case_id, witness_id))
    assert actual == changed
    assert len(changed) == 5


def test_v64_freeze_is_zero_token_and_idempotent() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "v64"
        terminal = freeze_v64_reference(output_dir=root)
        again = freeze_v64_reference(output_dir=root)
        assert again == terminal
        assert terminal["reference_frozen"] is True
        assert terminal["fresh_diagnostic_authorized"] is True
        assert terminal["full_calibration_authorized"] is False
        assert terminal["new_semantic_turn_count"] == 0
        assert terminal["new_usage"]["total_tokens"] == 0
        receipt = json.loads((root / "reference-receipt.json").read_text())
        assert receipt["case_count"] == 66
        assert receipt["witness_count"] == 182
        assert receipt["production_mutated"] is False
