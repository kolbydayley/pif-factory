from __future__ import annotations

import json
import tempfile
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v75_exact_span_remaining_shard import (
    DEFAULT_OUTPUT_ROOT as V75_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v77_reconcile_or_abstain_design import (
    DEFAULT_OUTPUT_ROOT as V77_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic import (
    ADJUDICATOR_MODEL,
    FIELD_POLARITY_SLOTS,
    PRIMARY_MODEL,
    TURN_NAMES,
    V23_ROOT,
    V24_ROOT,
    _validate_sources,
    build_shards,
    build_v78_inputs,
    freeze_v78,
    merge_outputs,
    reconcile_outputs,
    score_v78,
    validate_output,
)


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _inputs() -> tuple[dict, dict, dict]:
    return build_v78_inputs(
        _load(V23_ROOT / "pointwise-input-full.private.json"),
        _load(V23_ROOT / "calibration-truth.private.json"),
        _load(V75_ROOT / "selected-truth.private.json"),
    )


def _perfect_output(shard: dict, truth: dict) -> dict:
    expected = {row["task_id"]: row["expected_status"] for row in truth["tasks"]}
    return {
        "decisions": [
            {
                "task_id": task["task_id"],
                "field_status": expected[task["task_id"]],
                "source_evidence_spans": [task["source_excerpt"][: min(20, len(task["source_excerpt"]))]],
                "rationale": "Fresh independent direct field decision.",
            }
            for task in shard["tasks"]
        ]
    }


def test_v78_binds_v77_design_and_original_182_witness_pool() -> None:
    records = _validate_sources(v77_root=V77_ROOT, v23_root=V23_ROOT, v24_root=V24_ROOT)
    assert records["v77_terminal"]["sha256"]
    assert records["v23_pointwise"]["sha256"]
    assert records["v23_truth"]["sha256"]
    assert records["v24_terminal"]["sha256"]


def test_v78_selection_is_15_distinct_unseen_witnesses_without_source_semantics() -> None:
    value, truth, audit = _inputs()
    prior = _load(V75_ROOT / "selected-truth.private.json")
    prior_ids = {(row["case_id"], row["witness_id"]) for row in prior["tasks"]}
    selected_ids = {(row["case_id"], row["witness_id"]) for row in truth["tasks"]}
    assert value["task_count"] == truth["task_count"] == 15
    assert len(selected_ids) == 15
    assert not (selected_ids & prior_ids)
    assert sorted((row["field"], row["expected_status"]) for row in truth["tasks"]) == sorted(FIELD_POLARITY_SLOTS)
    assert audit["selected_distinct_witness_count"] == 15
    assert audit["expected_correct_count"] == 8
    assert audit["expected_incorrect_count"] == 7
    assert audit["selection_uses_source_text"] is False
    assert value["prior_labels_present"] is False
    assert value["prior_model_decisions_present"] is False


def test_v78_perfect_independent_outputs_reconcile_and_pass_every_gate() -> None:
    value, truth, _ = _inputs()
    shards = build_shards(value)
    outputs = []
    for shard in shards:
        output = _perfect_output(shard, truth)
        assert validate_output(output, shard) == []
        outputs.append(output)
    primary = merge_outputs(outputs)
    adjudicator = merge_outputs(outputs)
    reconciled = reconcile_outputs(primary, adjudicator)
    score = score_v78(reconciled, truth, primary, adjudicator)
    assert score["passed"] is True
    assert score["metrics"]["exact_rate"] == 1.0
    assert score["metrics"]["incorrect_sensitivity"] == 1.0
    assert score["metrics"]["correct_specificity"] == 1.0
    assert score["metrics"]["independent_agreement_rate"] == 1.0
    assert score["metrics"]["reconciled_abstention_count"] == 0
    assert score["fresh_full_development_calibration_authorized"] is True
    assert score["holdout_authorized"] is False


def test_v78_one_model_disagreement_becomes_abstain_and_fails() -> None:
    value, truth, _ = _inputs()
    outputs = [_perfect_output(shard, truth) for shard in build_shards(value)]
    primary = merge_outputs(outputs)
    adjudicator = merge_outputs(outputs)
    decision = adjudicator["decisions"][0]
    decision["field_status"] = (
        "incorrect" if decision["field_status"] == "correct" else "correct"
    )
    reconciled = reconcile_outputs(primary, adjudicator)
    score = score_v78(reconciled, truth, primary, adjudicator)
    assert score["passed"] is False
    assert score["metrics"]["reconciled_abstention_count"] == 1
    assert score["fresh_full_development_calibration_authorized"] is False


def test_v78_freeze_is_ten_presemantic_turns_and_immutable() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "v78"
        frozen = freeze_v78(output_dir=root)
        again = freeze_v78(output_dir=root)
        assert again["spec"] == frozen["spec"]
        assert frozen["spec"]["primary_model"] == PRIMARY_MODEL
        assert frozen["spec"]["adjudicator_model"] == ADJUDICATOR_MODEL
        assert frozen["spec"]["turn_plan"] == list(TURN_NAMES)
        assert frozen["spec"]["task_count"] == 15
        assert frozen["spec"]["distinct_witness_count"] == 15
        assert frozen["spec"]["prior_labels_in_model_input"] is False
        assert frozen["spec"]["prior_model_decisions_in_model_input"] is False
        assert frozen["spec"]["retry_count_per_turn"] == 0
        assert frozen["spec"]["selection_authorized"] is False
        assert frozen["spec"]["holdout_authorized"] is False
        assert frozen["spec"]["production_mutation_allowed"] is False
        assert not (root / "terminal.json").exists()
        for turn_name in TURN_NAMES:
            turn_root = root / "turns" / turn_name.replace("_", "-")
            assert not (turn_root / "capacity.json").exists()
            assert not (turn_root / "sidecar.json").exists()
            assert not (turn_root / "output.private.json").exists()
