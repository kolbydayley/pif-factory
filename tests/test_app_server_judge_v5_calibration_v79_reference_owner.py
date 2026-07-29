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
    DEFAULT_OUTPUT_ROOT as V78_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v79_reference_owner import (
    CANARY_TURN,
    MODEL,
    TURN_NAMES,
    build_v79_inputs,
    freeze_v79,
    score_v79,
)


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _inputs() -> tuple[dict, dict, dict, dict]:
    return build_v79_inputs(
        v75_input=_load(V75_ROOT / "direct-field-input.private.json"),
        v77_delta=_load(V77_ROOT / "reconciled-delta.private.json"),
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
        status = row["control_expected_status"] or (
            row["proposed_status"]
            if row["proposed_status"] in {"correct", "incorrect"}
            else row["prior_status"]
        )
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


def test_v79_selection_is_blind_balanced_and_not_a_vote() -> None:
    value, truth, canary, audit = _inputs()
    assert value["task_count"] == truth["task_count"] == 15
    assert len({row["task_id"] for row in value["tasks"]}) == 15
    assert audit["role_counts"] == {
        "matched_control": 6,
        "proposed_change": 5,
        "unresolved": 4,
    }
    assert audit["control_status_counts"] == {"correct": 3, "incorrect": 3}
    assert audit["permutation_canary_count"] == canary["task_count"] == 3
    assert audit["selection_uses_source_text"] is False
    assert audit["majority_voting_used"] is False
    assert value["prior_labels_present"] is False
    assert value["prior_model_decisions_present"] is False
    assert canary["prior_labels_present"] is False
    assert canary["prior_model_decisions_present"] is False


def test_v79_perfect_controls_and_canary_authorize_reference_freeze_only() -> None:
    _, truth, _, _ = _inputs()
    owner, canary = _perfect_outputs(truth)
    score = score_v79(owner, canary, truth)
    assert score["passed"] is True
    assert score["reference_freeze_authorized"] is True
    assert len(score["reference_patch_proposal"]) == 9
    assert score["metrics"]["matched_control_exact_rate"] == 1.0
    assert score["metrics"]["permutation_canary_exact_rate"] == 1.0
    assert score["majority_voting_used"] is False
    assert score["fresh_primary_repair_diagnostic_authorized"] is False
    assert score["full_calibration_authorized"] is False
    assert score["selection_authorized"] is False
    assert score["holdout_authorized"] is False


def test_v79_control_error_suppresses_every_reference_change() -> None:
    _, truth, _, _ = _inputs()
    owner, canary = _perfect_outputs(truth)
    control = next(row for row in truth["tasks"] if row["role"] == "matched_control")
    decision = next(row for row in owner["decisions"] if row["task_id"] == control["task_id"])
    decision["field_status"] = (
        "incorrect" if decision["field_status"] == "correct" else "correct"
    )
    score = score_v79(owner, canary, truth)
    assert score["passed"] is False
    assert score["reference_patch_proposal"] == []
    assert score["reference_freeze_authorized"] is False


def test_v79_permutation_disagreement_is_not_voted_away() -> None:
    _, truth, _, _ = _inputs()
    owner, canary = _perfect_outputs(truth)
    canary["decisions"][0]["field_status"] = (
        "incorrect" if canary["decisions"][0]["field_status"] == "correct" else "correct"
    )
    score = score_v79(owner, canary, truth)
    assert score["passed"] is False
    assert score["checks"]["permutation_canary_exact_rate"] is False
    assert score["reference_patch_proposal"] == []


def test_v79_freeze_is_six_presemantic_turns_and_immutable() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "v79"
        frozen = freeze_v79(output_dir=root)
        again = freeze_v79(output_dir=root)
        assert again["spec"] == frozen["spec"]
        assert frozen["spec"]["model"] == MODEL
        assert frozen["spec"]["turn_plan"] == list(TURN_NAMES)
        assert frozen["spec"]["turn_plan"][-1] == CANARY_TURN
        assert frozen["spec"]["task_count"] == 15
        assert frozen["spec"]["prior_labels_in_model_input"] is False
        assert frozen["spec"]["prior_model_decisions_in_model_input"] is False
        assert frozen["spec"]["majority_voting_used"] is False
        assert frozen["spec"]["retry_count_per_turn"] == 0
        assert frozen["spec"]["reference_freeze_authorized"] is False
        assert frozen["spec"]["holdout_authorized"] is False
        assert frozen["spec"]["production_mutation_allowed"] is False
        assert not (root / "terminal.json").exists()
        assert len(frozen["turns"]) == 6
        assert max(len(turn["prompt"].encode("utf-8")) for turn in frozen["turns"]) < 10_000
        assert max(len(json.dumps(turn["schema"]).encode("utf-8")) for turn in frozen["turns"]) < 10_000
        for turn in frozen["turns"]:
            turn_root = turn["paths"]["root"]
            assert not (turn_root / "capacity.json").exists()
            assert not (turn_root / "sidecar.json").exists()
            assert not (turn_root / "output.private.json").exists()
