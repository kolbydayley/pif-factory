from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v146_singleton_reference_owner import (
    CONTROL_TURNS,
    OWNER_TURNS,
    REPEAT_TURNS,
    TURN_NAMES,
    _patch_truth_and_reference,
    _validate_v145,
    build_v146_inputs,
    freeze_v146,
    score_v146,
)


def _passing_outputs(truth):
    outputs = {}
    expected = {row["task_id"]: row for row in truth["tasks"]}
    primary_ids = [row["task_id"] for row in truth["tasks"]]
    for turn_name, task_id in zip(CONTROL_TURNS + OWNER_TURNS, primary_ids, strict=True):
        row = expected[task_id]
        status = row.get("control_expected_status", row.get("current_status"))
        outputs[turn_name] = {
            "decisions": [
                {
                    "task_id": task_id,
                    "field_status": status,
                    "source_evidence_spans": ["evidence"],
                    "rationale": "expected",
                }
            ]
        }
    for turn_name, mapping in zip(REPEAT_TURNS, truth["repeat_map"], strict=True):
        task_id = mapping["owner_task_id"]
        outputs[turn_name] = deepcopy(
            outputs[OWNER_TURNS[primary_ids[5:].index(task_id)]]
        )
    return outputs


def test_v146_builds_singleton_controls_owners_and_targeted_repeats():
    predecessor = _validate_v145()
    rows, truth, selection, records = build_v146_inputs(predecessor)

    assert len(rows) == len(TURN_NAMES) == 24
    assert truth["task_count"] == 19
    assert truth["control_count"] == 5
    assert truth["owner_count"] == 14
    assert truth["repeat_count"] == 5
    assert len(records) == 10
    assert selection["maximum_tasks_per_turn"] == 1
    assert selection["repeat_membership_is_exactly_v145_unstable_owners"] is True
    assert selection["repeat_input_is_byte_identical_before_freeze"] is True
    assert selection["selection_uses_source_text"] is False
    assert selection["majority_voting_used"] is False
    assert all(row["value"]["task_count"] == 1 for row in rows)
    assert all("role" not in row["value"]["tasks"][0] for row in rows)


def test_v146_score_requires_controls_nonabstention_repeatability_and_evidence():
    _, truth, _, _ = build_v146_inputs(_validate_v145())
    outputs = _passing_outputs(truth)
    passed = score_v146(outputs=outputs, truth=truth)

    assert passed["passed"] is True
    assert passed["metrics"]["control_exact_count"] == 5
    assert passed["metrics"]["repeat_exact_count"] == 5
    assert passed["metrics"]["owner_abstention_count"] == 0

    broken = deepcopy(outputs)
    broken[REPEAT_TURNS[0]]["decisions"][0]["field_status"] = (
        "incorrect"
        if broken[REPEAT_TURNS[0]]["decisions"][0]["field_status"] == "correct"
        else "correct"
    )
    assert score_v146(outputs=broken, truth=truth)["passed"] is False


def test_v146_patch_updates_field_truth_and_reference_only_from_singleton_owners():
    predecessor = _validate_v145()
    _, truth, _, _ = build_v146_inputs(predecessor)
    outputs = _passing_outputs(truth)
    owner_output = {
        "decisions": [
            outputs[turn]["decisions"][0] for turn in OWNER_TURNS
        ]
    }
    patched_truth, reference = _patch_truth_and_reference(
        current_truth=predecessor["v144"]["v143"]["values"]["truth"],
        current_reference=predecessor["v144"]["v143"]["v142"]["values"]["reference"],
        owner_truth=truth,
        owner_output=owner_output,
    )

    assert patched_truth["v146_owner_task_count"] == 14
    assert reference["reference_version"] == (
        "fixture_reference_v13_singleton_reference_owner_frozen"
    )
    assert reference["v146_owner_task_count"] == 14


def test_v146_freeze_is_idempotent_private_and_twenty_four_turn_bounded(tmp_path: Path):
    root = tmp_path / "v146"
    first = freeze_v146(output_dir=root)
    second = freeze_v146(output_dir=root)

    assert first["spec"] == second["spec"]
    assert first["spec"]["model"] == "gpt-5.5"
    assert first["spec"]["reasoning_effort"] == "high"
    assert first["spec"]["turn_plan"] == list(TURN_NAMES)
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["fresh_full_calibration_authorized"] is False
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert first["spec"]["production_mutation_allowed"] is False
    policy = json.loads(Path(first["capacity_policy"]).read_text())
    assert policy["phase_total_token_bound"] == 1680000
    assert policy["minimum_remaining_reserve_percent"] == 20
    assert policy["retry_count_per_turn"] == 0
    for row in first["spec"]["frozen_inputs"]["turns"]:
        prompt = Path(row["prompt"]["path"]).read_text()
        assert "control_expected_status" not in prompt
        assert "current_status" not in prompt
        assert "v145_primary_status" not in prompt
        assert '"disagreement_reasons_present":false' in prompt
        assert "source_v145_task_id" not in prompt
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()


def test_v146_freeze_does_not_mutate_v145(tmp_path: Path):
    before = _validate_v145()["records"]
    freeze_v146(output_dir=tmp_path / "v146")
    after = _validate_v145()["records"]
    assert before == after
