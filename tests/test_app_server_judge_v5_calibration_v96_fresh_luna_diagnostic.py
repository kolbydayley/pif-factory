from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v26_diagnostic import _load_json
from research_factory.app_server_judge_v5_calibration_v75_exact_span_remaining_shard import (
    DEFAULT_OUTPUT_ROOT as V75_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic import V23_ROOT
from research_factory.app_server_judge_v5_calibration_v88_residual_reference_audit import (
    DEFAULT_OUTPUT_ROOT as V88_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v91_fresh_luna_diagnostic import (
    DEFAULT_OUTPUT_ROOT as V91_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v94_systematic_field_audit import (
    DEFAULT_OUTPUT_ROOT as V94_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v95_reference_v6_freeze import (
    DEFAULT_OUTPUT_ROOT as V95_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v96_fresh_luna_diagnostic import (
    FIELD_POLARITY_SLOTS,
    TURN_NAMES,
    build_v96_inputs,
    freeze_v96,
    primary_shards,
    score_v96,
)


def _built():
    pointwise = _load_json(V23_ROOT / "pointwise-input-full.private.json", "pointwise")
    reference = _load_json(V95_ROOT / "calibration-truth-v6.private.json", "reference")
    rubric = _load_json(V94_ROOT / "field-rubric.json", "rubric")
    truths = [
        _load_json(V75_ROOT / "selected-truth.private.json", "v75 truth"),
        _load_json(V88_ROOT / "residual-reference-truth.private.json", "v88 truth"),
        _load_json(V91_ROOT / "fresh-luna-truth.private.json", "v91 truth"),
    ]
    return build_v96_inputs(
        pointwise=pointwise,
        reference=reference,
        rubric=rubric,
        prior_luna_truths=truths,
    ), truths


def _decision(task_id: str, status: str) -> dict:
    return {
        "task_id": task_id,
        "field_status": status,
        "source_evidence_spans": ["evidence"],
        "rationale": "test",
    }


def test_v96_selection_is_balanced_distinct_and_fresh_to_prior_luna_inputs():
    (value, truth, canary, selection), prior_truths = _built()
    assert value["task_count"] == truth["task_count"] == 15
    assert canary["task_count"] == 4
    assert selection["expected_incorrect_count"] == 7
    assert selection["expected_correct_count"] == 8
    assert len({(row["case_id"], row["witness_id"]) for row in truth["tasks"]}) == 15
    excluded = {
        (row["case_id"], row["witness_id"])
        for prior in prior_truths
        for row in prior["tasks"]
    }
    assert not excluded & {
        (row["case_id"], row["witness_id"]) for row in truth["tasks"]
    }
    assert Counter((row["field"], row["expected_status"]) for row in truth["tasks"]) == Counter(
        FIELD_POLARITY_SLOTS
    )


def test_v96_model_input_is_blinded_compacted_and_uses_v94_semantics():
    (value, _truth, _canary, selection), _ = _built()
    rendered = json.dumps(value, sort_keys=True)
    assert "expected_status" not in rendered
    assert "prior_status" not in rendered
    assert selection["selection_uses_source_text"] is False
    assert selection["semantic_pruning_performed"] is False
    certainty = next(row for row in value["tasks"] if row["field"] == "certainty")
    assert "Medium is a valid neutral mapping" in certainty["field_contract"]["decision_rule"]
    assert all(
        child not in (None, "", [], {})
        for task in value["tasks"]
        for child in task["structured_event"].values()
    )


def test_v96_shards_cover_all_tasks_once():
    (value, _truth, canary, _selection), _ = _built()
    shards = primary_shards(value)
    assert len(shards) + 1 == len(TURN_NAMES) == 6
    ids = [task["task_id"] for shard in shards for task in shard["tasks"]]
    assert ids == [task["task_id"] for task in value["tasks"]]
    assert len(ids) == len(set(ids)) == 15
    assert len({row["task_id"] for row in canary["tasks"]}) == 4


def test_v96_score_requires_perfect_primary_and_canary_results():
    (_value, truth, _canary_input, _selection), _ = _built()
    expected = {row["task_id"]: row["expected_status"] for row in truth["tasks"]}
    primary = {"decisions": [_decision(task_id, status) for task_id, status in expected.items()]}
    canary = {
        "decisions": [
            _decision(row["canary_task_id"], expected[row["primary_task_id"]])
            for row in truth["canary_map"]
        ]
    }
    score = score_v96(primary, canary, truth)
    assert score["passed"] is True
    assert score["fresh_full_development_calibration_authorized"] is True

    primary["decisions"][0]["field_status"] = (
        "incorrect" if primary["decisions"][0]["field_status"] == "correct" else "correct"
    )
    failed = score_v96(primary, canary, truth)
    assert failed["passed"] is False
    assert failed["fresh_full_development_calibration_authorized"] is False


def test_v96_freeze_is_six_turns_immutable_and_presemantic(tmp_path: Path):
    root = tmp_path / "v96"
    first = freeze_v96(output_dir=root)
    second = freeze_v96(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["model"] == "gpt-5.6-luna"
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["production_mutation_allowed"] is False
    assert len(first["turns"]) == 6
    assert not (root / "terminal.json").exists()
