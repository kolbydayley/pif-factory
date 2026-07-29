from __future__ import annotations

import json
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v172_speaker_truth_owner import (
    CANARY_TURN,
    CONTROL_TRUTH,
    NEGATIVE_CONTROL_ID,
    POSITIVE_CONTROL_ID,
    PRIMARY_TURN,
    TARGET_TASK_ID,
    build_v172_input,
    freeze_v172,
    score_v172,
)


def _output(target: str, *, reverse: bool = False):
    rows = [
        {"task_id": TARGET_TASK_ID, "field_status": target},
        {"task_id": NEGATIVE_CONTROL_ID, "field_status": CONTROL_TRUTH[NEGATIVE_CONTROL_ID]},
        {"task_id": POSITIVE_CONTROL_ID, "field_status": CONTROL_TRUTH[POSITIVE_CONTROL_ID]},
    ]
    if reverse:
        rows.reverse()
    for row in rows:
        row["source_evidence_spans"] = [{"text": "x", "start": 0, "end": 1}]
        row["rationale"] = "fixture"
    return {"decisions": rows}


def test_v172_input_is_target_blind_and_has_positive_negative_controls():
    value = build_v172_input()
    assert value["task_count"] == 3
    assert {row["task_id"] for row in value["tasks"]} == {
        TARGET_TASK_ID,
        NEGATIVE_CONTROL_ID,
        POSITIVE_CONTROL_ID,
    }
    assert all(row["field"] == "speaker" for row in value["tasks"])
    assert value["prior_labels_present"] is False
    assert value["prior_model_decisions_present"] is False
    assert value["system_identity_present"] is False


def test_v172_freeze_has_identical_membership_reversed_order_and_no_semantic_artifacts(tmp_path: Path):
    root = tmp_path / "v172"
    first = freeze_v172(output_dir=root)
    second = freeze_v172(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["turn_plan"] == [PRIMARY_TURN, CANARY_TURN]
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["target_current_truth_exposed_to_model"] is False
    primary = first["turns"][0]["value"]["tasks"]
    canary = first["turns"][1]["value"]["tasks"]
    assert [row["task_id"] for row in canary] == list(
        reversed([row["task_id"] for row in primary])
    )
    policy = json.loads(first["capacity_policy"].read_text())
    assert policy["maximum_total_tokens_per_turn"] == 45000
    assert policy["phase_total_token_bound"] == 90000
    assert policy["projected_phase_quota_points"] == 2
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()


def test_v172_score_accepts_either_stable_target_status_but_not_order_disagreement():
    for status in ("correct", "incorrect"):
        score = score_v172([_output(status), _output(status, reverse=True)])
        assert score["passed"] is True
        assert score["metrics"]["target_adjudicated_status"] == status
        assert score["metrics"]["control_exact_count"] == 4
    failed = score_v172([_output("correct"), _output("incorrect", reverse=True)])
    assert failed["passed"] is False
    assert "target_order_agreement" in failed["failed_checks"]


def test_v172_score_fails_closed_on_control_error():
    second = _output("incorrect", reverse=True)
    for row in second["decisions"]:
        if row["task_id"] == POSITIVE_CONTROL_ID:
            row["field_status"] = "incorrect"
    score = score_v172([_output("incorrect"), second])
    assert score["passed"] is False
    assert "controls_exact" in score["failed_checks"]
