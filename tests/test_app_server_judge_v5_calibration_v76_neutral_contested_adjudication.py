from __future__ import annotations

import json
import tempfile
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v75_exact_span_remaining_shard import (
    DEFAULT_OUTPUT_ROOT as V75_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v76_neutral_contested_adjudication import (
    EFFORT,
    MODEL,
    TURN_NAMES,
    _validate_v75,
    build_shards,
    build_v76_inputs,
    freeze_v76,
    merge_outputs,
    score_v76,
    validate_output,
)


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _inputs() -> tuple[dict, dict, dict]:
    return build_v76_inputs(
        _load(V75_ROOT / "direct-field-input.private.json"),
        _load(V75_ROOT / "selected-truth.private.json"),
        _load(V75_ROOT / "direct-field-output.private.json"),
    )


def _passing_output(shard: dict, truth: dict) -> dict:
    expected = {row["task_id"]: row for row in truth["tasks"]}
    decisions = []
    for task in shard["tasks"]:
        row = expected[task["task_id"]]
        if row["role"] == "matched_canary":
            status = row["expected_status"]
        elif row["role"] == "primary_abstention":
            status = row["expected_status"]
        else:
            status = row["primary_status"]
        decisions.append(
            {
                "task_id": task["task_id"],
                "field_status": status,
                "source_evidence_spans": [task["source_excerpt"][: min(20, len(task["source_excerpt"]))]],
                "rationale": "Neutral source-only adjudication.",
            }
        )
    return {"decisions": decisions}


def test_v76_requires_the_complete_scoreable_v75_quality_failure() -> None:
    assert _validate_v75(V75_ROOT)["v75_terminal"]["sha256"]


def test_v76_selection_is_seven_contested_plus_three_canaries_and_blind() -> None:
    value, truth, audit = _inputs()
    assert value["task_count"] == truth["task_count"] == 10
    assert len({row["task_id"] for row in value["tasks"]}) == 10
    role_counts = {}
    for row in truth["tasks"]:
        role_counts[row["role"]] = role_counts.get(row["role"], 0) + 1
    assert role_counts == {
        "matched_canary": 3,
        "primary_abstention": 1,
        "primary_control_disagreement": 1,
        "primary_truth_disagreement": 5,
    }
    assert audit["selection_uses_source_text"] is False
    assert value["prior_labels_present"] is False
    assert value["prior_model_decisions_present"] is False
    assert all(set(row) == {"task_id", "field", "field_contract", "source_excerpt", "structured_event"} for row in value["tasks"])


def test_v76_shards_cover_every_task_once_and_passing_projection_is_strict() -> None:
    value, truth, _ = _inputs()
    shards = build_shards(value)
    assert len(shards) == len(TURN_NAMES) == 5
    ids = [row["task_id"] for shard in shards for row in shard["tasks"]]
    assert len(ids) == len(set(ids)) == 10
    outputs = []
    for shard in shards:
        output = _passing_output(shard, truth)
        assert validate_output(output, shard) == []
        outputs.append(output)
    score = score_v76(merge_outputs(outputs), truth)
    assert score["passed"] is True
    assert score["metrics"]["canary_exact_rate"] == 1.0
    assert score["metrics"]["primary_proposal_agreement_rate"] == 1.0
    assert score["metrics"]["adjudicator_abstention_count"] == 0
    assert score["reference_freeze_authorized"] is True
    assert score["fresh_12_authorized"] is False


def test_v76_one_canary_miss_or_proposal_disagreement_fails() -> None:
    value, truth, _ = _inputs()
    outputs = [_passing_output(shard, truth) for shard in build_shards(value)]
    canary = next(row for row in truth["tasks"] if row["role"] == "matched_canary")
    decision = next(
        row
        for output in outputs
        for row in output["decisions"]
        if row["task_id"] == canary["task_id"]
    )
    decision["field_status"] = (
        "incorrect" if decision["field_status"] == "correct" else "correct"
    )
    score = score_v76(merge_outputs(outputs), truth)
    assert score["passed"] is False
    assert score["proposed_reference_changes"] == []
    assert score["reference_freeze_authorized"] is False


def test_v76_freeze_is_presemantic_sol_only_and_immutable() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "v76"
        frozen = freeze_v76(output_dir=root)
        again = freeze_v76(output_dir=root)
        assert again["spec"] == frozen["spec"]
        assert frozen["spec"]["model"] == MODEL
        assert frozen["spec"]["reasoning_effort"] == EFFORT
        assert frozen["spec"]["turn_plan"] == list(TURN_NAMES)
        assert frozen["spec"]["contested_task_count"] == 7
        assert frozen["spec"]["canary_count"] == 3
        assert frozen["spec"]["prior_labels_in_model_input"] is False
        assert frozen["spec"]["prior_model_decisions_in_model_input"] is False
        assert frozen["spec"]["retry_count_per_turn"] == 0
        assert frozen["spec"]["reference_freeze_authorized"] is False
        assert frozen["spec"]["holdout_authorized"] is False
        assert frozen["spec"]["production_mutation_allowed"] is False
        assert not (root / "terminal.json").exists()
        for turn_name in TURN_NAMES:
            turn_root = root / "turns" / turn_name.replace("_", "-")
            assert not (turn_root / "capacity.json").exists()
            assert not (turn_root / "sidecar.json").exists()
            assert not (turn_root / "output.private.json").exists()
