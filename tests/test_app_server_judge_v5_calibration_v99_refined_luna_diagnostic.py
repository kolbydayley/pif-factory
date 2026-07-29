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
from research_factory.app_server_judge_v5_calibration_v96_fresh_luna_diagnostic import (
    DEFAULT_OUTPUT_ROOT as V96_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v97_residual_field_audit import (
    DEFAULT_OUTPUT_ROOT as V97_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v98_reference_v7_freeze import (
    DEFAULT_OUTPUT_ROOT as V98_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v99_refined_luna_diagnostic import (
    TARGETED_FIELDS,
    TURN_NAMES,
    build_v99_inputs,
    freeze_v99,
    primary_shards,
    score_v99,
)


def _built():
    truths = [
        _load_json(V75_ROOT / "selected-truth.private.json", "v75"),
        _load_json(V88_ROOT / "residual-reference-truth.private.json", "v88"),
        _load_json(V91_ROOT / "fresh-luna-truth.private.json", "v91"),
        _load_json(V96_ROOT / "fresh-luna-v6-truth.private.json", "v96"),
    ]
    value = build_v99_inputs(
        pointwise=_load_json(V23_ROOT / "pointwise-input-full.private.json", "pointwise"),
        reference=_load_json(V98_ROOT / "calibration-truth-v7.private.json", "reference"),
        v94_rubric=_load_json(V94_ROOT / "field-rubric.json", "v94 rubric"),
        v97_rubric=_load_json(V97_ROOT / "field-rubric.json", "v97 rubric"),
        v96_input=_load_json(V96_ROOT / "fresh-luna-v6-input.private.json", "v96 input"),
        v96_truth=truths[-1],
        v96_output=_load_json(V96_ROOT / "fresh-luna-v6-output.private.json", "v96 output"),
        prior_luna_truths=truths,
    )
    return value, truths


def _decision(task_id: str, status: str) -> dict:
    return {
        "task_id": task_id,
        "field_status": status,
        "source_evidence_spans": ["evidence"],
        "rationale": "test",
    }


def test_v99_has_four_targeted_regressions_and_eleven_fresh_cases():
    (value, truth, canary, selection), truths = _built()
    assert value["task_count"] == truth["task_count"] == 15
    assert canary["task_count"] == 4
    roles = Counter(row["cohort_role"] for row in truth["tasks"])
    assert roles == {"v96_targeted_regression": 4, "fresh_to_luna": 11}
    assert Counter(
        row["field"] for row in truth["tasks"] if row["cohort_role"] == "v96_targeted_regression"
    ) == Counter(TARGETED_FIELDS)
    assert Counter(row["expected_status"] for row in truth["tasks"]) == {
        "incorrect": 7,
        "correct": 8,
    }
    assert selection["canary_includes_prompt_fix_regressions"] == ["evidence", "reported_actor"]
    fresh = {
        (row["case_id"], row["witness_id"])
        for row in truth["tasks"]
        if row["cohort_role"] == "fresh_to_luna"
    }
    excluded = {
        (row["case_id"], row["witness_id"])
        for prior in truths
        for row in prior["tasks"]
    }
    assert not fresh & excluded


def test_v99_input_explicitly_represents_absent_fields_and_is_blinded():
    (value, _truth, _canary, selection), _ = _built()
    assert all("requested_field_value" in task for task in value["tasks"])
    assert any(task["requested_field_value"]["presence"] == "absent" for task in value["tasks"])
    assert selection["requested_field_presence_explicit"] is True
    rendered = json.dumps(value, sort_keys=True)
    assert "expected_status" not in rendered
    assert "cohort_role" not in rendered


def test_v99_shards_and_canaries_cover_once():
    (value, _truth, canary, _selection), _ = _built()
    shards = primary_shards(value)
    assert len(shards) + 1 == len(TURN_NAMES) == 6
    ids = [task["task_id"] for shard in shards for task in shard["tasks"]]
    assert ids == [task["task_id"] for task in value["tasks"]]
    assert len(ids) == len(set(ids)) == 15
    assert len({task["task_id"] for task in canary["tasks"]}) == 4


def test_v99_score_requires_perfect_results():
    (_value, truth, _canary_input, _selection), _ = _built()
    expected = {row["task_id"]: row["expected_status"] for row in truth["tasks"]}
    primary = {"decisions": [_decision(task_id, status) for task_id, status in expected.items()]}
    canary = {
        "decisions": [
            _decision(row["canary_task_id"], expected[row["primary_task_id"]])
            for row in truth["canary_map"]
        ]
    }
    score = score_v99(primary, canary, truth)
    assert score["passed"] is True
    assert score["fresh_full_development_calibration_authorized"] is True


def test_v99_freeze_is_six_turns_immutable_and_presemantic(tmp_path: Path):
    root = tmp_path / "v99"
    first = freeze_v99(output_dir=root)
    second = freeze_v99(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["model"] == "gpt-5.6-luna"
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["production_mutation_allowed"] is False
    assert len(first["turns"]) == 6
    assert not (root / "terminal.json").exists()
