from __future__ import annotations

import json
import tempfile
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic import (
    DEFAULT_OUTPUT_ROOT as V78_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v79_reference_owner import (
    DEFAULT_OUTPUT_ROOT as V79_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v80_alignment_reference_owner import (
    CANARY_TURN,
    MODEL,
    TURN_NAMES,
    base_instructions,
    build_v80_inputs,
    freeze_v80,
    score_v80,
)


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _inputs() -> tuple[dict, dict, dict, dict]:
    return build_v80_inputs(
        v79_input=_load(V79_ROOT / "reference-owner-input.private.json"),
        v79_truth=_load(V79_ROOT / "reference-owner-truth.private.json"),
        v79_output=_load(V79_ROOT / "reference-owner-output.private.json"),
        v78_input=_load(V78_ROOT / "fresh-input.private.json"),
        v78_truth=_load(V78_ROOT / "fresh-truth.private.json"),
        v78_primary=_load(V78_ROOT / "primary-output.private.json"),
        v78_adjudicator=_load(V78_ROOT / "adjudicator-output.private.json"),
        v78_reconciled=_load(V78_ROOT / "reconciled-output.private.json"),
    )


def _perfect_outputs(truth: dict) -> tuple[dict, dict]:
    owner = []
    owner_status = {}
    for row in truth["tasks"]:
        status = row["control_expected_status"] or row["v79_owner_status"] or row["prior_status"]
        owner_status[row["task_id"]] = status
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
            "field_status": owner_status[row["owner_task_id"]],
            "source_evidence_spans": ["evidence"],
            "rationale": "test",
        }
        for row in truth["canary_map"]
    ]
    return {"decisions": owner}, {"decisions": canary}


def test_v80_reclassifies_only_failed_control_and_rebalances_controls() -> None:
    value, truth, canary, audit = _inputs()
    assert value["task_count"] == truth["task_count"] == 16
    assert len({row["task_id"] for row in value["tasks"]}) == 16
    assert audit["role_counts"] == {
        "matched_control": 6,
        "proposed_change": 5,
        "unresolved": 4,
        "truth_challenge": 1,
    }
    assert audit["control_status_counts"] == {"correct": 3, "incorrect": 3}
    assert audit["permutation_canary_count"] == canary["task_count"] == 4
    assert audit["selection_uses_source_text"] is False
    assert audit["majority_voting_used"] is False
    challenges = [row for row in truth["tasks"] if row["role"] == "truth_challenge"]
    assert len(challenges) == 1
    assert challenges[0]["field"] == "stance"
    assert challenges[0]["prior_status"] == "incorrect"
    assert challenges[0]["v79_owner_status"] == "correct"
    assert value["prior_labels_present"] is False
    assert value["prior_model_decisions_present"] is False


def test_v80_prompt_requires_event_proposition_alignment() -> None:
    instructions = base_instructions()
    assert "align the structured event to the exact source proposition" in instructions
    assert "adjacent proposition" in instructions
    assert "Never transfer" in instructions


def test_v80_perfect_controls_and_canary_authorize_reference_freeze_only() -> None:
    _, truth, _, _ = _inputs()
    owner, canary = _perfect_outputs(truth)
    score = score_v80(owner, canary, truth)
    assert score["passed"] is True
    assert score["reference_freeze_authorized"] is True
    assert len(score["reference_patch_proposal"]) == 10
    assert score["metrics"]["matched_control_exact_rate"] == 1.0
    assert score["metrics"]["permutation_canary_exact_rate"] == 1.0
    assert score["majority_voting_used"] is False
    assert score["fresh_primary_repair_diagnostic_authorized"] is False
    assert score["full_calibration_authorized"] is False
    assert score["selection_authorized"] is False
    assert score["holdout_authorized"] is False


def test_v80_control_or_canary_failure_suppresses_proposal() -> None:
    _, truth, _, _ = _inputs()
    owner, canary = _perfect_outputs(truth)
    control = next(row for row in truth["tasks"] if row["role"] == "matched_control")
    decision = next(row for row in owner["decisions"] if row["task_id"] == control["task_id"])
    decision["field_status"] = (
        "incorrect" if decision["field_status"] == "correct" else "correct"
    )
    score = score_v80(owner, canary, truth)
    assert score["passed"] is False
    assert score["reference_patch_proposal"] == []
    owner, canary = _perfect_outputs(truth)
    canary["decisions"][0]["field_status"] = (
        "incorrect" if canary["decisions"][0]["field_status"] == "correct" else "correct"
    )
    score = score_v80(owner, canary, truth)
    assert score["passed"] is False
    assert score["reference_patch_proposal"] == []


def test_v80_freeze_is_five_presemantic_turns_and_immutable() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "v80"
        frozen = freeze_v80(output_dir=root)
        again = freeze_v80(output_dir=root)
        assert again["spec"] == frozen["spec"]
        assert frozen["spec"]["model"] == MODEL
        assert frozen["spec"]["turn_plan"] == list(TURN_NAMES)
        assert frozen["spec"]["turn_plan"][-1] == CANARY_TURN
        assert frozen["spec"]["task_count"] == 16
        assert frozen["spec"]["prior_labels_in_model_input"] is False
        assert frozen["spec"]["prior_model_decisions_in_model_input"] is False
        assert frozen["spec"]["majority_voting_used"] is False
        assert frozen["spec"]["retry_count_per_turn"] == 0
        assert frozen["spec"]["reference_freeze_authorized"] is False
        assert frozen["spec"]["holdout_authorized"] is False
        assert frozen["spec"]["production_mutation_allowed"] is False
        assert not (root / "terminal.json").exists()
        assert len(frozen["turns"]) == 5
        assert max(len(turn["prompt"].encode("utf-8")) for turn in frozen["turns"]) < 12_000
        assert max(len(json.dumps(turn["schema"]).encode("utf-8")) for turn in frozen["turns"]) < 10_000
        for turn in frozen["turns"]:
            turn_root = turn["paths"]["root"]
            assert not (turn_root / "capacity.json").exists()
            assert not (turn_root / "sidecar.json").exists()
            assert not (turn_root / "output.private.json").exists()
