from __future__ import annotations

import json
import tempfile
from pathlib import Path

from research_factory.app_server_judge_v5 import CHECKLIST_FIELDS
from research_factory.app_server_judge_v5_calibration_v62_root_status import (
    DEFAULT_OUTPUT_ROOT as V62_ROOT,
    validate_v62_output,
)
from research_factory.app_server_judge_v5_calibration_v63_reference_reaudit import (
    build_reference_proposal,
    build_v63_input,
    freeze_v63,
)


def _input() -> tuple[dict, dict]:
    value = json.loads((V62_ROOT / "root-status-input.private.json").read_text())
    output = json.loads((V62_ROOT / "root-status-output.private.json").read_text())
    truth = json.loads((V62_ROOT / "diagnostic-truth.private.json").read_text())
    return build_v63_input(value, output, truth)


def test_v63_is_six_side_free_residuals_without_prior_labels() -> None:
    value, old_truth = _input()
    assert len(value["units"]) == 6
    assert len(old_truth["units"]) == 6
    assert value["prior_labels_present"] is False
    assert value["candidate_outputs_present"] is False
    assert value["system_identity_present"] is False
    rendered = json.dumps(value)
    assert "old_root_fields" not in rendered
    assert "prior_value_differences" not in rendered


def test_v63_proposal_detects_field_changes() -> None:
    value, old_truth = _input()
    old = {
        (row["case_id"], row["witness_id"]): set(row["old_root_fields"])
        for row in old_truth["units"]
    }
    output = {"units": []}
    for index, unit in enumerate(value["units"]):
        fields = set(old[(unit["case_id"], unit["witness_id"])])
        if index == 0:
            fields.add("target")
        output["units"].append(
            {
                "case_id": unit["case_id"],
                "witness_id": unit["witness_id"],
                "structured_field_verdict": "incorrect" if fields else "correct",
                "checklist": [
                    {
                        "field": field,
                        "value_relation": "different" if field in fields else "same",
                        "independent_root_status": "root" if field in fields else "not_different",
                        "dependency_fields": [],
                        "source_evidence_spans": [],
                        "rationale": "Synthetic reference decision.",
                    }
                    for field in CHECKLIST_FIELDS
                ],
            }
        )
    assert validate_v62_output(output, value) == []
    proposal = build_reference_proposal(output, old_truth)
    assert proposal["reference_patch_proposed"] is True
    assert proposal["change_count"] == 1
    assert proposal["reference_freeze_authorized"] is True


def test_v63_freeze_is_one_side_free_presemantic_turn() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "v63"
        frozen = freeze_v63(output_dir=root)
        again = freeze_v63(output_dir=root)
        assert again["spec"] == frozen["spec"]
        assert frozen["spec"]["model"] == "gpt-5.6-sol"
        assert frozen["spec"]["prior_labels_in_model_input"] is False
        assert frozen["spec"]["candidate_outputs_in_model_input"] is False
        assert frozen["spec"]["reference_change_allowed_in_this_version"] is False
        assert frozen["spec"]["retry_count_per_turn"] == 0
        assert not list(root.glob("turns/*/capacity.json"))
        assert not list(root.glob("turns/*/sidecar.json"))
