from __future__ import annotations

import json
import tempfile
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v72_sharded_field_microtasks import (
    DEFAULT_OUTPUT_ROOT as V72_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v73_direct_field_audit import (
    EFFORT,
    MODEL,
    TASKS_PER_SHARD,
    TURN_NAMES,
    _validate_v72,
    build_prompt,
    build_v73_inputs,
    build_v73_shards,
    freeze_v73,
    merge_outputs,
    output_schema,
    score_v73,
    validate_output,
)


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _inputs() -> tuple[dict, dict, dict]:
    return build_v73_inputs(
        _load(V72_ROOT / "field-microtask-input.private.json"),
        _load(V72_ROOT / "diagnostic-truth.private.json"),
        _load(V72_ROOT / "assembled-checklist.private.json"),
        _load(V72_ROOT / "cohort-roles.json"),
    )


def _perfect_output(shard: dict, truth: dict) -> dict:
    expected = {row["task_id"]: row["expected_status"] for row in truth["tasks"]}
    return {
        "decisions": [
            {
                "task_id": task["task_id"],
                "field_status": expected[task["task_id"]],
                "source_evidence_spans": [task["source_excerpt"][: min(20, len(task["source_excerpt"]))]],
                "rationale": "Direct source-to-field decision.",
            }
            for task in shard["tasks"]
        ]
    }


def test_v73_requires_complete_measured_v72_quality_failure() -> None:
    records = _validate_v72(V72_ROOT)
    assert records["v72_terminal"]["sha256"]


def test_v73_selection_is_all_disagreements_plus_balanced_matched_controls() -> None:
    value, truth, taxonomy = _inputs()
    assert value["task_count"] == truth["task_count"] == 15
    assert len({row["task_id"] for row in value["tasks"]}) == 15
    assert sum(row["role"] == "disagreement" for row in truth["tasks"]) == 11
    controls = [row for row in truth["tasks"] if row["role"] == "matched_control"]
    assert len(controls) == 4
    assert sorted(row["control_polarity"] for row in controls) == [
        "correct",
        "correct",
        "incorrect",
        "incorrect",
    ]
    assert taxonomy["false_positive_count"] == 2
    assert taxonomy["false_negative_count"] == 9
    assert taxonomy["selection_uses_source_text"] is False
    assert all(set(row) == {"task_id", "field", "field_contract", "source_excerpt", "structured_event"} for row in value["tasks"])


def test_v73_five_shards_cover_each_blind_task_once_and_fit_caps() -> None:
    value, _, _ = _inputs()
    shards = build_v73_shards(value)
    assert len(shards) == len(TURN_NAMES) == 5
    assert all(shard["task_count"] == TASKS_PER_SHARD for shard in shards)
    task_ids = [row["task_id"] for shard in shards for row in shard["tasks"]]
    assert len(task_ids) == len(set(task_ids)) == 15
    for shard in shards:
        assert len(build_prompt(shard).encode("utf-8")) <= 96 * 1024
        assert len(json.dumps(output_schema(shard), sort_keys=True).encode("utf-8")) <= 64 * 1024
        assert shard["prior_labels_present"] is False
        assert shard["prior_model_decisions_present"] is False


def test_v73_perfect_controls_no_abstention_and_exact_evidence_pass() -> None:
    value, truth, _ = _inputs()
    outputs = []
    for shard in build_v73_shards(value):
        output = _perfect_output(shard, truth)
        assert validate_output(output, shard) == []
        outputs.append(output)
    score = score_v73(merge_outputs(outputs), truth)
    assert score["passed"] is True
    assert score["metrics"]["control_exact_rate"] == 1.0
    assert score["metrics"]["abstention_count"] == 0
    assert score["metrics"]["evidence_complete_rate"] == 1.0
    assert score["proposals_authorized_for_neutral_verification"] is True
    assert score["reference_patch_authorized"] is False


def test_v73_control_miss_or_abstention_fails_and_suppresses_proposals() -> None:
    value, truth, _ = _inputs()
    outputs = [_perfect_output(shard, truth) for shard in build_v73_shards(value)]
    control = next(row for row in truth["tasks"] if row["role"] == "matched_control")
    decision = next(
        row
        for output in outputs
        for row in output["decisions"]
        if row["task_id"] == control["task_id"]
    )
    decision["field_status"] = "abstain"
    score = score_v73(merge_outputs(outputs), truth)
    assert score["passed"] is False
    assert score["proposed_changes"] == []
    assert score["proposals_authorized_for_neutral_verification"] is False


def test_v73_freeze_is_presemantic_luna_only_and_immutable() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "v73"
        frozen = freeze_v73(output_dir=root)
        again = freeze_v73(output_dir=root)
        assert again["spec"] == frozen["spec"]
        assert frozen["spec"]["model"] == MODEL
        assert frozen["spec"]["reasoning_effort"] == EFFORT
        assert frozen["spec"]["turn_plan"] == list(TURN_NAMES)
        assert frozen["spec"]["retry_count_per_turn"] == 0
        assert frozen["spec"]["prior_labels_in_model_input"] is False
        assert frozen["spec"]["prior_model_decisions_in_model_input"] is False
        assert frozen["spec"]["reference_patch_authorized"] is False
        assert frozen["spec"]["holdout_authorized"] is False
        assert frozen["spec"]["production_mutation_allowed"] is False
        assert not (root / "terminal.json").exists()
        for turn_name in TURN_NAMES:
            turn_root = root / "turns" / turn_name.replace("_", "-")
            assert not (turn_root / "capacity.json").exists()
            assert not (turn_root / "sidecar.json").exists()
            assert not (turn_root / "output.private.json").exists()
