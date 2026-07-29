from __future__ import annotations

import json
import tempfile
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v75_exact_span_remaining_shard import (
    DEFAULT_OUTPUT_ROOT as V75_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic import (
    DEFAULT_OUTPUT_ROOT as V78_ROOT,
    V23_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v83_fresh_alignment_diagnostic import (
    DEFAULT_OUTPUT_ROOT as V83_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v84_metric_target_reference_audit import (
    DEFAULT_OUTPUT_ROOT as V84_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    DEFAULT_OUTPUT_ROOT as V86_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v88_residual_reference_audit import (
    DEFAULT_OUTPUT_ROOT as V88_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v90_reference_v5_freeze import (
    DEFAULT_OUTPUT_ROOT as V90_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v91_fresh_luna_diagnostic import (
    CANARY_TURN,
    MODEL,
    TURN_NAMES,
    V91_FIELD_POLARITY_SLOTS,
    build_v91_inputs,
    freeze_v91,
    score_v91,
)


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _prior_truths() -> list[dict]:
    return [
        _load(V75_ROOT / "selected-truth.private.json"),
        _load(V78_ROOT / "fresh-truth.private.json"),
        _load(V83_ROOT / "fresh-alignment-truth.private.json"),
        _load(V84_ROOT / "metric-target-truth.private.json"),
        _load(V86_ROOT / "fresh-enhanced-truth.private.json"),
        _load(V88_ROOT / "residual-reference-truth.private.json"),
    ]


def _inputs() -> tuple[dict, dict, dict, dict]:
    return build_v91_inputs(
        pointwise=_load(V23_ROOT / "pointwise-input-full.private.json"),
        reference=_load(V90_ROOT / "calibration-truth-v5.private.json"),
        prior_truths=_prior_truths(),
    )


def _perfect_outputs(truth: dict) -> tuple[dict, dict]:
    statuses = {row["task_id"]: row["expected_status"] for row in truth["tasks"]}
    primary = {
        "decisions": [
            {
                "task_id": row["task_id"],
                "field_status": row["expected_status"],
                "source_evidence_spans": ["evidence"],
                "rationale": "test",
            }
            for row in truth["tasks"]
        ]
    }
    canary = {
        "decisions": [
            {
                "task_id": row["canary_task_id"],
                "field_status": statuses[row["primary_task_id"]],
                "source_evidence_spans": ["evidence"],
                "rationale": "test",
            }
            for row in truth["canary_map"]
        ]
    }
    return primary, canary


def test_v91_selection_is_fresh_balanced_and_distinct() -> None:
    value, truth, canary, audit = _inputs()
    prior = {
        (row["case_id"], row["witness_id"])
        for source in _prior_truths()
        for row in source["tasks"]
    }
    selected = {(row["case_id"], row["witness_id"]) for row in truth["tasks"]}
    assert value["task_count"] == truth["task_count"] == 15
    assert len(selected) == 15
    assert not (selected & prior)
    assert sorted((row["field"], row["expected_status"]) for row in truth["tasks"]) == sorted(
        V91_FIELD_POLARITY_SLOTS
    )
    assert audit["eligible_fresh_witness_count"] == 128
    assert audit["expected_correct_count"] == 8
    assert audit["expected_incorrect_count"] == 7
    assert audit["selection_uses_source_text"] is False
    assert canary["task_count"] == 4


def test_v91_perfect_result_authorizes_full_development_only() -> None:
    _, truth, _, _ = _inputs()
    primary, canary = _perfect_outputs(truth)
    score = score_v91(primary, canary, truth)
    assert score["passed"] is True
    assert score["metrics"]["exact_rate"] == 1.0
    assert score["metrics"]["incorrect_sensitivity"] == 1.0
    assert score["metrics"]["correct_specificity"] == 1.0
    assert score["metrics"]["permutation_canary_exact_rate"] == 1.0
    assert score["fresh_full_development_calibration_authorized"] is True
    assert score["selection_authorized"] is False
    assert score["holdout_authorized"] is False


def test_v91_freeze_is_six_presemantic_turns_and_immutable() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "v91"
        frozen = freeze_v91(output_dir=root)
        assert freeze_v91(output_dir=root)["spec"] == frozen["spec"]
        assert frozen["spec"]["model"] == MODEL
        assert frozen["spec"]["turn_plan"] == list(TURN_NAMES)
        assert frozen["spec"]["turn_plan"][-1] == CANARY_TURN
        assert frozen["spec"]["retry_count_per_turn"] == 0
        assert frozen["spec"]["fresh_full_development_calibration_authorized"] is False
        assert frozen["spec"]["holdout_authorized"] is False
        assert frozen["spec"]["production_mutation_allowed"] is False
        assert not (root / "terminal.json").exists()
        for turn in frozen["turns"]:
            turn_root = turn["paths"]["root"]
            assert not (turn_root / "capacity.json").exists()
            assert not (turn_root / "sidecar.json").exists()
            assert not (turn_root / "output.private.json").exists()
