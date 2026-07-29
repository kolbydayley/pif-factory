from __future__ import annotations

import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v102_fresh_gpt55_diagnostic import (
    _payload_hash,
    _prior_gpt55_sources,
)
from research_factory.app_server_judge_v5_calibration_v103_unsupported_inference_protocol import (
    MODEL,
    TURN_NAMES,
    _validate_predecessors,
    build_v103_inputs,
    field_rubric_v103,
    freeze_v103,
    score_v103,
)


def _built():
    predecessor = _validate_predecessors()
    values = predecessor["values"]
    prior_inputs = [
        values[f"{name}_gpt55_input"] for name in _prior_gpt55_sources()
    ] + [values["v102_input"]]
    built = build_v103_inputs(
        pointwise=values["v23_pointwise"],
        reference=values["v101_truth"],
        v102_input=values["v102_input"],
        v102_truth=values["v102_truth"],
        prior_gpt55_inputs=prior_inputs,
        rubric=field_rubric_v103(),
    )
    return built, prior_inputs


def _decision(task_id: str, status: str) -> dict:
    return {
        "task_id": task_id,
        "field_status": status,
        "source_evidence_spans": ["evidence"],
        "rationale": "test",
    }


def test_v103_rubric_defines_noninverted_derived_check_semantics():
    contract = field_rubric_v103()["field_contracts"]["unsupported_inference"]

    assert contract["output_semantics"]["correct"] == (
        "the event contains no unsupported material inference"
    )
    assert contract["output_semantics"]["incorrect"] == (
        "the event contains at least one unsupported material inference"
    )
    assert "not a stored boolean event field" in contract["decision_rule"]


def test_v103_has_one_regression_and_five_unseen_balanced_cases():
    (value, truth, canary, selection), prior_inputs = _built()
    task_by_id = {row["task_id"]: row for row in value["tasks"]}
    prior_hashes = {
        _payload_hash(task)
        for prior in prior_inputs
        for task in prior.get("tasks") or []
    }
    fresh = [row for row in truth["tasks"] if row["cohort_role"] == "fresh_to_gpt55"]

    assert value["task_count"] == truth["task_count"] == 6
    assert canary["task_count"] == 2
    assert Counter(row["expected_status"] for row in truth["tasks"]) == {
        "correct": 3,
        "incorrect": 3,
    }
    assert Counter(row["cohort_role"] for row in truth["tasks"]) == {
        "v102_protocol_regression": 1,
        "fresh_to_gpt55": 5,
    }
    assert all(_payload_hash(task_by_id[row["task_id"]]) not in prior_hashes for row in fresh)
    assert selection["distinct_case_count"] == 6


def test_v103_model_input_is_blinded_and_uses_derived_value_kind():
    (value, truth, _canary, _selection), _prior = _built()
    rendered = json.dumps(value, sort_keys=True)

    assert "expected_status" not in rendered
    assert "cohort_role" not in rendered
    assert all(
        task["requested_field_value"]["kind"]
        == "derived_support_check_not_stored_event_field"
        for task in value["tasks"]
    )
    assert len({row["case_id"] for row in truth["tasks"]}) == 6


def test_v103_score_requires_every_primary_canary_and_evidence_receipt():
    (_value, truth, _canary, _selection), _prior = _built()
    expected = {row["task_id"]: row["expected_status"] for row in truth["tasks"]}
    primary = {
        "decisions": [_decision(task_id, status) for task_id, status in expected.items()]
    }
    canary = {
        "decisions": [
            _decision(row["canary_task_id"], expected[row["primary_task_id"]])
            for row in truth["canary_map"]
        ]
    }

    passed = score_v103(primary, canary, truth)
    assert passed["passed"] is True
    assert passed["fresh_full_development_calibration_authorized"] is True

    failed = deepcopy(primary)
    failed["decisions"][0]["source_evidence_spans"] = []
    score = score_v103(failed, canary, truth)
    assert score["passed"] is False
    assert score["fresh_full_development_calibration_authorized"] is False


def test_v103_freeze_is_three_turns_immutable_and_presemantic(tmp_path: Path):
    root = tmp_path / "v103"
    first = freeze_v103(output_dir=root)
    second = freeze_v103(output_dir=root)

    assert first["spec"] == second["spec"]
    assert first["spec"]["model"] == MODEL
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["fresh_full_development_calibration_authorized"] is False
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert first["spec"]["production_mutation_allowed"] is False
    assert len(first["turns"]) == len(TURN_NAMES) == 3
    assert not (root / "terminal.json").exists()
