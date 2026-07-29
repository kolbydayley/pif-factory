from __future__ import annotations

import json
import tempfile
from pathlib import Path

from research_factory.app_server_judge_v5 import CHECKLIST_FIELDS
from research_factory.app_server_judge_v5_calibration_v69_stable_recovery import (
    DEFAULT_OUTPUT_ROOT as V69_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v70_blind_reference_audit import (
    EFFORT,
    MODEL,
    TURN_NAME,
    _validate_v69,
    build_reference_proposal,
    build_v70_cohort,
    freeze_v70,
)


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _cohort() -> tuple[dict, dict, dict]:
    truth = _load(V69_ROOT / "diagnostic-truth.private.json")
    value, roles = build_v70_cohort(
        _load(V69_ROOT / "layered-input.private.json"),
        _load(V69_ROOT / "layered-output.private.json"),
        truth,
    )
    return value, truth, roles


def _perfect_output(value: dict, truth: dict) -> dict:
    expected = {
        (case_id, witness_id): set(fields)
        for case_id, case in truth["cases"].items()
        for witness_id, fields in case["field_issues"].items()
    }
    return {
        "units": [
            {
                "case_id": unit["case_id"],
                "witness_id": unit["witness_id"],
                "structured_field_verdict": (
                    "incorrect" if expected[(unit["case_id"], unit["witness_id"])] else "correct"
                ),
                "checklist": [
                    {
                        "field": field,
                        "root_status": (
                            "root"
                            if field in expected[(unit["case_id"], unit["witness_id"])]
                            else "not_root"
                        ),
                        "source_evidence_spans": [],
                        "rationale": "Independent blind reference fixture decision.",
                    }
                    for field in CHECKLIST_FIELDS
                ],
            }
            for unit in value["units"]
        ]
    }


def test_v70_predecessor_and_blind_cohort_are_exact() -> None:
    assert _validate_v69(V69_ROOT)["v69_terminal"]["sha256"]
    value, _, roles = _cohort()
    assert len(value["units"]) == 6
    assert value["prior_labels_present"] is False
    assert value["candidate_outputs_present"] is False
    assert value["reference_proposals_present"] is False
    assert roles["selection_uses_source_text"] is False
    assert roles["role_counts"] == {
        "reference_disagreement": 4,
        "empty_control": 1,
        "nonempty_control": 1,
    }


def test_v70_reference_proposal_requires_both_controls_and_no_abstention() -> None:
    value, truth, roles = _cohort()
    output = _perfect_output(value, truth)
    proposal = build_reference_proposal(output, truth, roles)
    assert proposal["reference_audit_valid"] is True
    assert proposal["reference_confirmed_without_patch"] is True
    assert proposal["reference_patch_proposal_authorized"] is False
    output["units"][0]["checklist"][0]["root_status"] = (
        "not_root"
        if output["units"][0]["checklist"][0]["root_status"] == "root"
        else "root"
    )
    changed = build_reference_proposal(output, truth, roles)
    assert changed["reference_audit_valid"] is True
    assert changed["reference_patch_proposal_authorized"] is True
    control = next(
        unit
        for unit in output["units"]
        if next(
            row["role"]
            for row in roles["units"]
            if (row["case_id"], row["witness_id"])
            == (unit["case_id"], unit["witness_id"])
        )
        != "reference_disagreement"
    )
    control["checklist"][0]["root_status"] = "abstain"
    assert build_reference_proposal(output, truth, roles)["reference_audit_valid"] is False


def test_v70_freeze_is_one_presemantic_blind_luna_turn() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "v70"
        frozen = freeze_v70(output_dir=root)
        again = freeze_v70(output_dir=root)
        assert again["spec"] == frozen["spec"]
        assert frozen["spec"]["model"] == MODEL
        assert frozen["spec"]["reasoning_effort"] == EFFORT
        assert frozen["spec"]["turn_plan"] == [TURN_NAME]
        assert frozen["spec"]["prior_labels_in_model_input"] is False
        assert frozen["spec"]["candidate_outputs_in_model_input"] is False
        assert frozen["spec"]["reference_proposals_in_model_input"] is False
        assert frozen["spec"]["retry_count_per_turn"] == 0
        assert frozen["spec"]["holdout_authorized"] is False
        assert not (root / "terminal.json").exists()
        assert not (root / "turns/blind-reference-audit/capacity.json").exists()
        assert not (root / "turns/blind-reference-audit/sidecar.json").exists()
