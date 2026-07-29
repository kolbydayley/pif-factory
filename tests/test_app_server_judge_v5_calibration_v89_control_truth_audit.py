from __future__ import annotations

import json
import tempfile
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v84_metric_target_reference_audit import (
    DEFAULT_OUTPUT_ROOT as V84_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v88_residual_reference_audit import (
    DEFAULT_OUTPUT_ROOT as V88_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v89_control_truth_audit import (
    CANARY_TURN,
    MODEL,
    TURN_NAMES,
    _validate_predecessors,
    build_v89_inputs,
    freeze_v89,
    score_v89,
)


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _inputs() -> tuple[dict, dict, dict, dict]:
    return build_v89_inputs(
        v88_input=_load(V88_ROOT / "residual-reference-input.private.json"),
        v88_truth=_load(V88_ROOT / "residual-reference-truth.private.json"),
        v88_output=_load(V88_ROOT / "residual-reference-output.private.json"),
        v84_input=_load(V84_ROOT / "metric-target-input.private.json"),
        v84_truth=_load(V84_ROOT / "metric-target-truth.private.json"),
        v84_output=_load(V84_ROOT / "metric-target-output.private.json"),
    )


def _perfect_outputs(truth: dict, challenge_status: str = "correct") -> tuple[dict, dict]:
    statuses = {
        row["task_id"]: (
            row["control_expected_status"]
            if row["role"] == "settled_control"
            else challenge_status
        )
        for row in truth["tasks"]
    }
    owner = {
        "decisions": [
            {
                "task_id": task_id,
                "field_status": status,
                "source_evidence_spans": ["evidence"],
                "rationale": "test",
            }
            for task_id, status in statuses.items()
        ]
    }
    canary = {
        "decisions": [
            {
                "task_id": row["canary_task_id"],
                "field_status": statuses[row["owner_task_id"]],
                "source_evidence_spans": ["evidence"],
                "rationale": "test",
            }
            for row in truth["canary_map"]
        ]
    }
    return owner, canary


def test_v89_predecessors_are_complete_and_nonpromoting() -> None:
    predecessor = _validate_predecessors(v88_root=V88_ROOT, v84_root=V84_ROOT)
    assert predecessor["values"]["v88_terminal"]["reference_patch_authorized"] is False
    assert predecessor["values"]["v88_score"]["metrics"]["matched_control_exact_count"] == 4
    assert predecessor["values"]["v84_terminal"]["reference_freeze_authorized"] is True


def test_v89_inputs_are_blinded_with_two_challenges_four_controls() -> None:
    value, truth, canary, selection = _inputs()
    assert value["task_count"] == truth["task_count"] == 6
    assert value["prior_labels_present"] is False
    assert value["prior_model_decisions_present"] is False
    assert selection["role_counts"] == {"truth_challenge": 2, "settled_control": 4}
    assert selection["permutation_canary_count"] == 2
    assert canary["task_count"] == 2
    challenges = [row for row in truth["tasks"] if row["role"] == "truth_challenge"]
    assert sorted(row["field"] for row in challenges) == ["certainty", "target"]


def test_v89_perfect_controls_and_canaries_authorize_reconciliation_only() -> None:
    _, truth, _, _ = _inputs()
    owner, canary = _perfect_outputs(truth)
    score = score_v89(owner, canary, truth)
    assert score["passed"] is True
    assert score["metrics"]["settled_control_exact_rate"] == 1.0
    assert score["metrics"]["permutation_canary_exact_rate"] == 1.0
    assert score["metrics"]["evidence_complete_rate"] == 1.0
    assert score["v88_reconciliation_authorized"] is True
    assert score["reference_freeze_authorized"] is False
    assert score["holdout_authorized"] is False


def test_v89_control_failure_blocks_reconciliation() -> None:
    _, truth, _, _ = _inputs()
    owner, canary = _perfect_outputs(truth)
    control = next(row for row in truth["tasks"] if row["role"] == "settled_control")
    decision = next(row for row in owner["decisions"] if row["task_id"] == control["task_id"])
    decision["field_status"] = "incorrect" if decision["field_status"] == "correct" else "correct"
    score = score_v89(owner, canary, truth)
    assert score["passed"] is False
    assert score["v88_reconciliation_authorized"] is False


def test_v89_freeze_is_three_presemantic_turns_and_immutable() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "v89"
        frozen = freeze_v89(output_dir=root)
        assert freeze_v89(output_dir=root)["spec"] == frozen["spec"]
        assert frozen["spec"]["model"] == MODEL
        assert frozen["spec"]["turn_plan"] == list(TURN_NAMES)
        assert frozen["spec"]["turn_plan"][-1] == CANARY_TURN
        assert frozen["spec"]["retry_count_per_turn"] == 0
        assert frozen["spec"]["v88_reconciliation_authorized"] is False
        assert frozen["spec"]["holdout_authorized"] is False
        assert frozen["spec"]["production_mutation_allowed"] is False
        assert not (root / "terminal.json").exists()
        for turn in frozen["turns"]:
            turn_root = turn["paths"]["root"]
            assert not (turn_root / "capacity.json").exists()
            assert not (turn_root / "sidecar.json").exists()
            assert not (turn_root / "output.private.json").exists()
