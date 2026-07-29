from __future__ import annotations

import json
import tempfile
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v75_exact_span_remaining_shard import (
    DEFAULT_OUTPUT_ROOT as V75_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic import (
    DEFAULT_OUTPUT_ROOT as V78_ROOT,
    FIELD_POLARITY_SLOTS,
    V23_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v82_reference_freeze import (
    DEFAULT_OUTPUT_ROOT as V82_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v83_fresh_alignment_diagnostic import (
    CANARY_TURN,
    MODEL,
    TURN_NAMES,
    build_v83_inputs,
    freeze_v83,
    score_v83,
)


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _inputs() -> tuple[dict, dict, dict, dict]:
    return build_v83_inputs(
        pointwise=_load(V23_ROOT / "pointwise-input-full.private.json"),
        reference=_load(V82_ROOT / "calibration-truth-v3.private.json"),
        v75_truth=_load(V75_ROOT / "selected-truth.private.json"),
        v78_truth=_load(V78_ROOT / "fresh-truth.private.json"),
    )


def _perfect_outputs(truth: dict) -> tuple[dict, dict]:
    primary = []
    statuses = {}
    for row in truth["tasks"]:
        statuses[row["task_id"]] = row["expected_status"]
        primary.append(
            {
                "task_id": row["task_id"],
                "field_status": row["expected_status"],
                "source_evidence_spans": ["evidence"],
                "rationale": "test",
            }
        )
    canary = [
        {
            "task_id": row["canary_task_id"],
            "field_status": statuses[row["primary_task_id"]],
            "source_evidence_spans": ["evidence"],
            "rationale": "test",
        }
        for row in truth["canary_map"]
    ]
    return {"decisions": primary}, {"decisions": canary}


def test_v83_selection_is_15_distinct_witnesses_unseen_by_v75_and_v78() -> None:
    value, truth, canary, audit = _inputs()
    prior = {
        (row["case_id"], row["witness_id"])
        for source in (
            _load(V75_ROOT / "selected-truth.private.json"),
            _load(V78_ROOT / "fresh-truth.private.json"),
        )
        for row in source["tasks"]
    }
    selected = {(row["case_id"], row["witness_id"]) for row in truth["tasks"]}
    assert value["task_count"] == truth["task_count"] == 15
    assert len(selected) == 15
    assert not (selected & prior)
    assert sorted((row["field"], row["expected_status"]) for row in truth["tasks"]) == sorted(
        FIELD_POLARITY_SLOTS
    )
    assert audit["selected_distinct_witness_count"] == 15
    assert audit["expected_correct_count"] == 8
    assert audit["expected_incorrect_count"] == 7
    assert audit["selection_uses_source_text"] is False
    assert audit["permutation_canary_count"] == canary["task_count"] == 4
    assert value["prior_labels_present"] is False
    assert value["prior_model_decisions_present"] is False


def test_v83_perfect_primary_and_canary_authorize_full_development_only() -> None:
    _, truth, _, _ = _inputs()
    primary, canary = _perfect_outputs(truth)
    score = score_v83(primary, canary, truth)
    assert score["passed"] is True
    assert score["metrics"]["exact_rate"] == 1.0
    assert score["metrics"]["incorrect_sensitivity"] == 1.0
    assert score["metrics"]["correct_specificity"] == 1.0
    assert score["metrics"]["evidence_complete_rate"] == 1.0
    assert score["metrics"]["permutation_canary_exact_rate"] == 1.0
    assert score["bounded_observable_repair_authorized"] is False
    assert score["fresh_full_development_calibration_authorized"] is True
    assert score["selection_authorized"] is False
    assert score["holdout_authorized"] is False


def test_v83_observable_canary_disagreement_authorizes_bounded_repair() -> None:
    _, truth, _, _ = _inputs()
    primary, canary = _perfect_outputs(truth)
    canary["decisions"][0]["field_status"] = (
        "incorrect" if canary["decisions"][0]["field_status"] == "correct" else "correct"
    )
    score = score_v83(primary, canary, truth)
    assert score["passed"] is False
    assert score["metrics"]["observable_repair_trigger_count"] == 1
    assert score["bounded_observable_repair_authorized"] is True
    assert score["fresh_full_development_calibration_authorized"] is False


def test_v83_unobservable_semantic_miss_does_not_trigger_repair() -> None:
    _, truth, _, _ = _inputs()
    primary, canary = _perfect_outputs(truth)
    canary_primary_ids = {row["primary_task_id"] for row in truth["canary_map"]}
    target = next(row for row in truth["tasks"] if row["task_id"] not in canary_primary_ids)
    decision = next(row for row in primary["decisions"] if row["task_id"] == target["task_id"])
    decision["field_status"] = (
        "incorrect" if decision["field_status"] == "correct" else "correct"
    )
    score = score_v83(primary, canary, truth)
    assert score["passed"] is False
    assert score["metrics"]["observable_repair_trigger_count"] == 0
    assert score["bounded_observable_repair_authorized"] is False


def test_v83_freeze_is_six_presemantic_turns_and_immutable() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "v83"
        frozen = freeze_v83(output_dir=root)
        again = freeze_v83(output_dir=root)
        assert again["spec"] == frozen["spec"]
        assert frozen["spec"]["model"] == MODEL
        assert frozen["spec"]["turn_plan"] == list(TURN_NAMES)
        assert frozen["spec"]["turn_plan"][-1] == CANARY_TURN
        assert frozen["spec"]["task_count"] == 15
        assert frozen["spec"]["distinct_witness_count"] == 15
        assert frozen["spec"]["retry_count_per_turn"] == 0
        assert frozen["spec"]["fresh_full_development_calibration_authorized"] is False
        assert frozen["spec"]["holdout_authorized"] is False
        assert frozen["spec"]["production_mutation_allowed"] is False
        assert not (root / "terminal.json").exists()
        assert len(frozen["turns"]) == 6
        for turn in frozen["turns"]:
            turn_root = turn["paths"]["root"]
            assert not (turn_root / "capacity.json").exists()
            assert not (turn_root / "sidecar.json").exists()
            assert not (turn_root / "output.private.json").exists()
