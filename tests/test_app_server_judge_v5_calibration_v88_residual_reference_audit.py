from __future__ import annotations

import json
import tempfile
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic import V23_ROOT
from research_factory.app_server_judge_v5_calibration_v85_reference_v4_freeze import (
    DEFAULT_OUTPUT_ROOT as V85_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    DEFAULT_OUTPUT_ROOT as V86_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v87_observable_repair import (
    DEFAULT_OUTPUT_ROOT as V87_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v88_residual_reference_audit import (
    CANARY_TURN,
    MODEL,
    TURN_NAMES,
    _validate_predecessors,
    build_v88_inputs,
    freeze_v88,
    score_v88,
)


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _inputs() -> tuple[dict, dict, dict, dict]:
    return build_v88_inputs(
        pointwise=_load(V23_ROOT / "pointwise-input-full.private.json"),
        reference=_load(V85_ROOT / "calibration-truth-v4.private.json"),
        v86_input=_load(V86_ROOT / "fresh-enhanced-input.private.json"),
        v86_truth=_load(V86_ROOT / "fresh-enhanced-truth.private.json"),
        residuals=_load(V87_ROOT / "reconciliation-audit.json")["residual_mismatches"],
    )


def _perfect_outputs(truth: dict, dispute_status: str = "correct") -> tuple[dict, dict]:
    status = {}
    for row in truth["tasks"]:
        status[row["task_id"]] = (
            row["control_expected_status"]
            if row["role"] == "matched_control"
            else dispute_status
        )
    owner = {
        "decisions": [
            {
                "task_id": task_id,
                "field_status": field_status,
                "source_evidence_spans": ["evidence"],
                "rationale": "test",
            }
            for task_id, field_status in status.items()
        ]
    }
    canary = {
        "decisions": [
            {
                "task_id": row["canary_task_id"],
                "field_status": status[row["owner_task_id"]],
                "source_evidence_spans": ["evidence"],
                "rationale": "test",
            }
            for row in truth["canary_map"]
        ]
    }
    return owner, canary


def test_v88_predecessor_authorizes_exactly_three_residuals() -> None:
    predecessor = _validate_predecessors(
        v87_root=V87_ROOT,
        v86_root=V86_ROOT,
        v85_root=V85_ROOT,
        v23_root=V23_ROOT,
    )
    assert predecessor["values"]["v87_audit"]["residual_mismatch_count"] == 3
    assert predecessor["values"]["v87_terminal"]["residual_reference_audit_authorized"] is True


def test_v88_inputs_are_blinded_balanced_and_distinct() -> None:
    value, truth, canary, selection = _inputs()
    assert value["task_count"] == truth["task_count"] == 9
    assert value["prior_labels_present"] is False
    assert value["prior_model_decisions_present"] is False
    assert len({row["task_id"] for row in value["tasks"]}) == 9
    assert selection["role_counts"] == {"reference_dispute": 3, "matched_control": 6}
    assert selection["field_counts"] == {"certainty": 3, "target": 3, "temporal_horizon": 3}
    assert selection["control_status_counts"] == {"correct": 3, "incorrect": 3}
    assert selection["control_selection_uses_source_text"] is False
    assert canary["task_count"] == 3


def test_v88_perfect_controls_and_canary_authorize_patch_only() -> None:
    _, truth, _, _ = _inputs()
    owner, canary = _perfect_outputs(truth)
    score = score_v88(owner, canary, truth)
    assert score["passed"] is True
    assert score["metrics"]["matched_control_exact_rate"] == 1.0
    assert score["metrics"]["permutation_canary_exact_rate"] == 1.0
    assert score["metrics"]["evidence_complete_rate"] == 1.0
    assert score["reference_patch_authorized"] is True
    assert score["fresh_diagnostic_authorized"] is False
    assert score["holdout_authorized"] is False


def test_v88_control_failure_blocks_patch() -> None:
    _, truth, _, _ = _inputs()
    owner, canary = _perfect_outputs(truth)
    control = next(row for row in truth["tasks"] if row["role"] == "matched_control")
    decision = next(row for row in owner["decisions"] if row["task_id"] == control["task_id"])
    decision["field_status"] = "incorrect" if decision["field_status"] == "correct" else "correct"
    score = score_v88(owner, canary, truth)
    assert score["passed"] is False
    assert score["reference_patch_authorized"] is False


def test_v88_freeze_is_four_presemantic_turns_and_immutable() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "v88"
        frozen = freeze_v88(output_dir=root)
        assert freeze_v88(output_dir=root)["spec"] == frozen["spec"]
        assert frozen["spec"]["model"] == MODEL
        assert frozen["spec"]["turn_plan"] == list(TURN_NAMES)
        assert frozen["spec"]["turn_plan"][-1] == CANARY_TURN
        assert frozen["spec"]["retry_count_per_turn"] == 0
        assert frozen["spec"]["reference_patch_authorized"] is False
        assert frozen["spec"]["holdout_authorized"] is False
        assert frozen["spec"]["production_mutation_allowed"] is False
        assert not (root / "terminal.json").exists()
        for turn in frozen["turns"]:
            turn_root = turn["paths"]["root"]
            assert not (turn_root / "capacity.json").exists()
            assert not (turn_root / "sidecar.json").exists()
            assert not (turn_root / "output.private.json").exists()
