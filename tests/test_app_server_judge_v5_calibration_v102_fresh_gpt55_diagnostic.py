from __future__ import annotations

import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

import pytest

from research_factory.app_server_judge_v5_calibration_v102_fresh_gpt55_diagnostic import (
    MODEL,
    TARGETED_FIELDS,
    TURN_NAMES,
    JudgeV5CalibrationV102Error,
    _combined_rubric,
    _payload_hash,
    _prior_gpt55_sources,
    _validate_predecessors,
    build_v102_inputs,
    freeze_v102,
    primary_shards,
    score_v102,
)


def _built():
    predecessor = _validate_predecessors()
    values = predecessor["values"]
    rubric = _combined_rubric(
        v94_rubric=values["v94_rubric"],
        v97_rubric=values["v97_rubric"],
        v100_rubric=values["v100_rubric"],
    )
    prior_inputs = [
        values[f"{name}_gpt55_input"] for name in _prior_gpt55_sources()
    ]
    built = build_v102_inputs(
        pointwise=values["v23_pointwise"],
        reference=values["v101_truth"],
        v99_input=values["v99_input"],
        v99_truth=values["v99_truth"],
        v99_output=values["v99_output"],
        v99_score=values["v99_score"],
        prior_gpt55_inputs=prior_inputs,
        rubric=rubric,
    )
    return built, rubric, prior_inputs


def _decision(task_id: str, status: str) -> dict:
    return {
        "task_id": task_id,
        "field_status": status,
        "source_evidence_spans": ["evidence"],
        "rationale": "test",
    }


def test_v102_covers_all_fields_with_balanced_truth_and_disclosed_cohorts():
    (value, truth, canary, selection), rubric, _prior = _built()

    assert value["task_count"] == truth["task_count"] == 15
    assert canary["task_count"] == 4
    assert Counter(row["field"] for row in truth["tasks"]) == Counter(
        rubric["field_contracts"].keys()
    )
    assert Counter(row["expected_status"] for row in truth["tasks"]) == {
        "incorrect": 7,
        "correct": 8,
    }
    assert Counter(row["cohort_role"] for row in truth["tasks"]) == {
        "v99_regression": 4,
        "sparse_prior_exposed_regression": 1,
        "fresh_to_gpt55": 10,
    }
    assert selection["targeted_regression_fields"] == list(TARGETED_FIELDS)
    assert selection["selected_distinct_witness_count"] == 15
    assert selection["selected_distinct_case_count"] == 15


def test_v102_fresh_payloads_are_unseen_and_sparse_metric_is_disclosed():
    (value, truth, _canary, selection), _rubric, prior_inputs = _built()
    prior_hashes = {
        _payload_hash(task)
        for prior in prior_inputs
        for task in prior.get("tasks") or []
    }
    task_by_id = {row["task_id"]: row for row in value["tasks"]}
    fresh = [row for row in truth["tasks"] if row["cohort_role"] == "fresh_to_gpt55"]
    sparse = [
        row
        for row in truth["tasks"]
        if row["cohort_role"] == "sparse_prior_exposed_regression"
    ]

    assert all(_payload_hash(task_by_id[row["task_id"]]) not in prior_hashes for row in fresh)
    assert len(sparse) == 1
    assert sparse[0]["field"] == "metric"
    assert _payload_hash(task_by_id[sparse[0]["task_id"]]) in prior_hashes
    assert selection["exact_payload_hash_scope"] == (
        "prior_exposure_and_duplicate_identity_only"
    )


def test_v102_inputs_are_blinded_and_canaries_cover_all_v99_regressions():
    (value, truth, canary, _selection), _rubric, _prior = _built()
    rendered = json.dumps(value, sort_keys=True)

    assert "expected_status" not in rendered
    assert "cohort_role" not in rendered
    assert all("requested_field_value" in task for task in value["tasks"])
    regression_ids = {
        row["task_id"] for row in truth["tasks"] if row["cohort_role"] == "v99_regression"
    }
    assert {row["primary_task_id"] for row in truth["canary_map"]} == regression_ids
    assert len({row["task_id"] for row in canary["tasks"]}) == 4


def test_v102_shards_cover_every_primary_task_once():
    (value, _truth, canary, _selection), _rubric, _prior = _built()
    shards = primary_shards(value)
    ids = [task["task_id"] for shard in shards for task in shard["tasks"]]

    assert len(shards) + 1 == len(TURN_NAMES) == 6
    assert ids == [task["task_id"] for task in value["tasks"]]
    assert len(ids) == len(set(ids)) == 15
    assert len(canary["tasks"]) == 4


def test_v102_score_requires_perfect_primary_and_canary_results():
    (_value, truth, _canary, _selection), _rubric, _prior = _built()
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

    passed = score_v102(primary, canary, truth)
    assert passed["passed"] is True
    assert passed["fresh_full_development_calibration_authorized"] is True

    failed_primary = deepcopy(primary)
    failed_primary["decisions"][0]["field_status"] = (
        "incorrect"
        if failed_primary["decisions"][0]["field_status"] == "correct"
        else "correct"
    )
    failed = score_v102(failed_primary, canary, truth)
    assert failed["passed"] is False
    assert failed["fresh_full_development_calibration_authorized"] is False


def test_v102_rejects_regression_trigger_drift():
    predecessor = _validate_predecessors()
    values = predecessor["values"]
    rubric = _combined_rubric(
        v94_rubric=values["v94_rubric"],
        v97_rubric=values["v97_rubric"],
        v100_rubric=values["v100_rubric"],
    )
    changed = deepcopy(values["v99_score"])
    changed["observable_repair_task_ids"] = changed["observable_repair_task_ids"][:-1]

    with pytest.raises(JudgeV5CalibrationV102Error, match="regression coverage"):
        build_v102_inputs(
            pointwise=values["v23_pointwise"],
            reference=values["v101_truth"],
            v99_input=values["v99_input"],
            v99_truth=values["v99_truth"],
            v99_output=values["v99_output"],
            v99_score=changed,
            prior_gpt55_inputs=[
                values[f"{name}_gpt55_input"] for name in _prior_gpt55_sources()
            ],
            rubric=rubric,
        )


def test_v102_freeze_is_six_turns_immutable_and_presemantic(tmp_path: Path):
    root = tmp_path / "v102"
    first = freeze_v102(output_dir=root)
    second = freeze_v102(output_dir=root)

    assert first["spec"] == second["spec"]
    assert first["spec"]["model"] == MODEL
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["fresh_full_development_calibration_authorized"] is False
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert first["spec"]["production_mutation_allowed"] is False
    assert len(first["turns"]) == 6
    assert not (root / "terminal.json").exists()
