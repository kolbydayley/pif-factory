from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v149_fresh_corrected_field_diagnostic import (
    FIELD_TURNS,
    REQUIRED_PAIRED_FIELDS,
    REPEAT_TURNS,
    SUPPORT_TURNS,
    TURN_NAMES,
    _select_field_tasks,
    _validate_v148,
    build_v149_inputs,
    freeze_v149,
    score_v149,
)


def _support_output(value, truth):
    expected = {(row["case_id"], row["witness_id"]): row["expected_status"] for row in truth["support_controls"]}
    for row in truth["tasks"]:
        if row["task_id"] in truth["support_target_task_ids"]:
            expected[(row["case_id"], row["witness_id"])] = "supported" if row["expected_status"] == "correct" else "unsupported"
    return {"units": [{"case_id": row["case_id"], "witness_id": row["witness_id"], "support_status": expected[(row["case_id"], row["witness_id"])], "source_evidence_spans": ["evidence"], "rationale": "expected"} for row in value["units"]]}


def _field_outputs(rows, truth):
    expected = {row["task_id"]: row for row in truth["tasks"]}
    return {row["turn_name"]: {"decisions": [{"task_id": row["task_id"], "field_status": expected[row["task_id"]]["expected_status"], "source_evidence_spans": ["evidence"], "rationale": "expected"}]} for row in rows}


def test_v149_selects_eighteen_balanced_tasks_covering_all_fields():
    source = _validate_v148()
    selected = _select_field_tasks(source)

    assert len(selected) == 18
    assert len({row["field"] for row in selected}) == 15
    assert sum(row["expected_status"] == "correct" for row in selected) == 9
    assert len({row["case_id"] for row in selected}) >= 12
    for field in REQUIRED_PAIRED_FIELDS:
        rows = [row for row in selected if row["field"] == field]
        assert {row["expected_status"] for row in rows} == {"correct", "incorrect"}


def test_v149_builds_support_projection_singletons_and_targeted_repeats():
    rows, truth, selection, records = build_v149_inputs(_validate_v148())

    assert len(rows) == len(TURN_NAMES) == 22
    assert truth["task_count"] == 18
    assert truth["status_counts"] == {"correct": 9, "incorrect": 9}
    assert len(truth["support_target_task_ids"]) == 2
    assert len(truth["repeat_task_ids"]) == 4
    assert selection["field_singleton_turn_count"] == 16
    assert selection["unsupported_inference_is_projected_from_llm_support"] is True
    assert selection["selection_uses_source_text"] is False
    assert selection["majority_voting_used"] is False
    assert len(records) == 2


def test_v149_score_passes_only_all_frozen_quality_gates():
    rows, truth, _, _ = build_v149_inputs(_validate_v148())
    support_rows = [row for row in rows if row["turn_name"] in SUPPORT_TURNS]
    field_rows = [row for row in rows if row["turn_name"] in FIELD_TURNS]
    repeat_rows = [row for row in rows if row["turn_name"] in REPEAT_TURNS]
    support = _support_output(support_rows[0]["value"], truth)
    canary = _support_output(support_rows[1]["value"], truth)
    fields = _field_outputs(field_rows, truth)
    repeats = _field_outputs(repeat_rows, truth)
    passed = score_v149(support=support, support_canary=canary, field_outputs=fields, repeat_outputs=repeats, truth=truth)

    assert passed["passed"] is True
    assert passed["metrics"]["field_decision_accuracy"] == 1.0
    assert passed["metrics"]["pointwise_field_issue_f1"] == 1.0
    assert passed["metrics"]["support_sensitivity"] == 1.0
    assert passed["metrics"]["support_specificity"] == 1.0

    broken = deepcopy(repeats)
    first = broken[REPEAT_TURNS[0]]["decisions"][0]
    first["field_status"] = "incorrect" if first["field_status"] == "correct" else "correct"
    assert score_v149(support=support, support_canary=canary, field_outputs=fields, repeat_outputs=broken, truth=truth)["passed"] is False


def test_v149_freeze_is_idempotent_twenty_two_turn_bounded_and_private(tmp_path: Path):
    root = tmp_path / "v149"
    first = freeze_v149(output_dir=root)
    second = freeze_v149(output_dir=root)

    assert first["spec"] == second["spec"]
    assert first["spec"]["support_model"] == "gpt-5.6-sol"
    assert first["spec"]["field_model"] == "gpt-5.5"
    assert first["spec"]["turn_plan"] == list(TURN_NAMES)
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    policy = json.loads(Path(first["capacity_policy"]).read_text())
    assert policy["phase_total_token_bound"] == 1540000
    assert policy["minimum_remaining_reserve_percent"] == 20
    for row in first["spec"]["frozen_inputs"]["turns"]:
        prompt = Path(row["prompt"]["path"]).read_text()
        assert "expected_status" not in prompt
        assert "source_v143_task_id" not in prompt
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()


def test_v149_freeze_does_not_mutate_v148(tmp_path: Path):
    before = _validate_v148()["records"]
    freeze_v149(output_dir=tmp_path / "v149")
    after = _validate_v148()["records"]
    assert before == after
