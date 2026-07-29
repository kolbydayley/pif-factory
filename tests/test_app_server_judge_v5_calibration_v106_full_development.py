from __future__ import annotations

import json
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v106_full_development import (
    ADJUDICATION_TURN,
    ADJUDICATOR_MODEL,
    BASE_TURNS,
    CANARY_TURNS,
    POINTWISE_TURNS,
    PRIMARY_MODEL,
    TURN_NAMES,
    _calibration_inputs,
    _validate_predecessors,
    freeze_v106,
    pointwise_base_instructions_v106,
)


def test_v106_projects_reference_v8_over_all_cases_and_witnesses():
    predecessor = _validate_predecessors()
    data = _calibration_inputs(predecessor["values"]["v101_truth"])

    assert len(data["pool"]["cases"]) == 66
    assert len(data["mapping"]["cases"]) == 66
    assert len(data["pointwise"]["units"]) == 182
    assert data["expected"]["case_count"] == 66
    assert len(data["expected"]["canary_case_ids"]) == 12
    for case in data["expected"]["cases"].values():
        for witness_id, fields in case["field_issues"].items():
            assert case["structured_fields"][witness_id] == (
                "incorrect" if fields else "correct"
            )


def test_v106_turn_plan_is_sharded_and_caps_one_independent_adjudication():
    assert len(POINTWISE_TURNS) == 11
    assert len(BASE_TURNS) == 11
    assert len(CANARY_TURNS) == 2
    assert TURN_NAMES[-1] == ADJUDICATION_TURN
    assert len(TURN_NAMES) == len(set(TURN_NAMES)) == 25
    assert PRIMARY_MODEL == "gpt-5.5"
    assert ADJUDICATOR_MODEL == "gpt-5.6-sol"


def test_v106_pointwise_instructions_fix_unsupported_label_inversion():
    instructions = pointwise_base_instructions_v106()

    assert "not a stored boolean event field" in instructions
    assert "only when claim_text adds at least one unsupported material assertion" in instructions
    assert "Do not invert that rule" in instructions


def test_v106_predecessor_usage_is_complete_and_measured():
    predecessor = _validate_predecessors()

    assert predecessor["development_usage"] == {
        "input_tokens": 218569,
        "cached_input_tokens": 17280,
        "output_tokens": 8386,
        "reasoning_output_tokens": 5995,
        "total_tokens": 226955,
    }


def test_v106_freeze_is_immutable_presemantic_and_keeps_downstream_closed(
    tmp_path: Path,
):
    root = tmp_path / "v106"
    first = freeze_v106(output_dir=root)
    second = freeze_v106(output_dir=root)

    assert first["spec"] == second["spec"]
    assert first["spec"]["primary_model"] == PRIMARY_MODEL
    assert first["spec"]["adjudicator_model"] == ADJUDICATOR_MODEL
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["minimum_turn_count"] == 24
    assert first["spec"]["maximum_turn_count"] == 25
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert first["spec"]["production_mutation_allowed"] is False
    assert len(first["pointwise_shards"]) == 11
    policy = json.loads(Path(first["capacity_policy"]).read_text())
    assert policy["ordered_turn_names"] == list(TURN_NAMES)
    assert policy["minimum_remaining_reserve_percent"] == 20
    assert not list(root.glob("turns/*/capacity.json"))
    assert not (root / "terminal.json").exists()
