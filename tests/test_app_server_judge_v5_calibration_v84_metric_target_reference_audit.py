from __future__ import annotations

import json
import tempfile
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic import (
    DEFAULT_OUTPUT_ROOT as V78_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v82_reference_freeze import (
    DEFAULT_OUTPUT_ROOT as V82_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v83_fresh_alignment_diagnostic import (
    DEFAULT_OUTPUT_ROOT as V83_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v84_metric_target_reference_audit import (
    CANARY_TURN,
    MODEL,
    TURN_NAMES,
    base_instructions,
    build_v84_inputs,
    freeze_v84,
    score_v84,
)


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _inputs() -> tuple[dict, dict, dict, dict]:
    return build_v84_inputs(
        v83_input=_load(V83_ROOT / "fresh-alignment-input.private.json"),
        v83_truth=_load(V83_ROOT / "fresh-alignment-truth.private.json"),
        v83_output=_load(V83_ROOT / "fresh-alignment-output.private.json"),
        v78_input=_load(V78_ROOT / "fresh-input.private.json"),
        v78_truth=_load(V78_ROOT / "fresh-truth.private.json"),
        reference=_load(V82_ROOT / "calibration-truth-v3.private.json"),
    )


def _perfect_outputs(truth: dict) -> tuple[dict, dict]:
    owner = []
    statuses = {}
    for row in truth["tasks"]:
        status = row["control_expected_status"] or row["terra_status"]
        statuses[row["task_id"]] = status
        owner.append(
            {
                "task_id": row["task_id"],
                "field_status": status,
                "source_evidence_spans": ["evidence"],
                "rationale": "test",
            }
        )
    canary = [
        {
            "task_id": row["canary_task_id"],
            "field_status": statuses[row["owner_task_id"]],
            "source_evidence_spans": ["evidence"],
            "rationale": "test",
        }
        for row in truth["canary_map"]
    ]
    return {"decisions": owner}, {"decisions": canary}


def test_v84_selects_three_disputes_and_field_matched_controls() -> None:
    value, truth, canary, audit = _inputs()
    assert value["task_count"] == truth["task_count"] == 9
    assert audit["role_counts"] == {"reference_dispute": 3, "matched_control": 6}
    assert audit["field_control_counts"] == {"actor": 2, "metric": 2, "target": 2}
    assert audit["control_status_counts"] == {"correct": 3, "incorrect": 3}
    disputes = [row for row in truth["tasks"] if row["role"] == "reference_dispute"]
    assert sorted(row["field"] for row in disputes) == ["metric", "metric", "target"]
    assert canary["task_count"] == audit["permutation_canary_count"] == 3
    assert audit["selection_uses_source_text"] is False
    assert audit["prior_labels_in_model_input"] is False
    assert audit["prior_model_decisions_in_model_input"] is False
    assert audit["majority_voting_used"] is False


def test_v84_rubric_covers_qualitative_metric_direction_and_target_paraphrase() -> None:
    instructions = base_instructions()
    assert "qualitative direction" in instructions
    assert "even without a numeral" in instructions
    assert "harmless paraphrase or coreference" in instructions


def test_v84_perfect_controls_and_canary_authorize_reference_freeze_only() -> None:
    _, truth, _, _ = _inputs()
    owner, canary = _perfect_outputs(truth)
    score = score_v84(owner, canary, truth)
    assert score["passed"] is True
    assert score["reference_freeze_authorized"] is True
    assert len(score["reference_patch_proposal"]) == 3
    assert score["metrics"]["matched_control_exact_rate"] == 1.0
    assert score["metrics"]["permutation_canary_exact_rate"] == 1.0
    assert score["majority_voting_used"] is False
    assert score["fresh_diagnostic_authorized"] is False
    assert score["full_calibration_authorized"] is False
    assert score["selection_authorized"] is False
    assert score["holdout_authorized"] is False


def test_v84_control_or_canary_failure_suppresses_reference_patch() -> None:
    _, truth, _, _ = _inputs()
    owner, canary = _perfect_outputs(truth)
    control = next(row for row in truth["tasks"] if row["role"] == "matched_control")
    decision = next(row for row in owner["decisions"] if row["task_id"] == control["task_id"])
    decision["field_status"] = (
        "incorrect" if decision["field_status"] == "correct" else "correct"
    )
    score = score_v84(owner, canary, truth)
    assert score["passed"] is False
    assert score["reference_patch_proposal"] == []
    owner, canary = _perfect_outputs(truth)
    canary["decisions"][0]["field_status"] = (
        "incorrect" if canary["decisions"][0]["field_status"] == "correct" else "correct"
    )
    score = score_v84(owner, canary, truth)
    assert score["passed"] is False
    assert score["reference_patch_proposal"] == []


def test_v84_freeze_is_four_presemantic_turns_and_immutable() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "v84"
        frozen = freeze_v84(output_dir=root)
        again = freeze_v84(output_dir=root)
        assert again["spec"] == frozen["spec"]
        assert frozen["spec"]["model"] == MODEL
        assert frozen["spec"]["turn_plan"] == list(TURN_NAMES)
        assert frozen["spec"]["turn_plan"][-1] == CANARY_TURN
        assert frozen["spec"]["task_count"] == 9
        assert frozen["spec"]["retry_count_per_turn"] == 0
        assert frozen["spec"]["reference_freeze_authorized"] is False
        assert frozen["spec"]["holdout_authorized"] is False
        assert frozen["spec"]["production_mutation_allowed"] is False
        assert not (root / "terminal.json").exists()
        assert len(frozen["turns"]) == 4
        for turn in frozen["turns"]:
            turn_root = turn["paths"]["root"]
            assert not (turn_root / "capacity.json").exists()
            assert not (turn_root / "sidecar.json").exists()
            assert not (turn_root / "output.private.json").exists()
