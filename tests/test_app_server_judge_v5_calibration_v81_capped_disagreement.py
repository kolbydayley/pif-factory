from __future__ import annotations

import json
import tempfile
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v80_alignment_reference_owner import (
    DEFAULT_OUTPUT_ROOT as V80_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v81_capped_disagreement import (
    MODEL,
    TURN_NAME,
    build_reconciliation,
    build_v81_inputs,
    freeze_v81,
    score_v81,
)


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _inputs() -> tuple[dict, dict, dict]:
    return build_v81_inputs(
        v80_input=_load(V80_ROOT / "alignment-owner-input.private.json"),
        v80_truth=_load(V80_ROOT / "alignment-owner-truth.private.json"),
        v80_output=_load(V80_ROOT / "alignment-owner-output.private.json"),
        v80_canary=_load(V80_ROOT / "permutation-canary-output.private.json"),
    )


def _perfect_output(truth: dict) -> dict:
    decisions = []
    for row in truth["tasks"]:
        status = row["control_expected_status"] or row["v80_canary_status"]
        decisions.append(
            {
                "task_id": row["task_id"],
                "field_status": status,
                "source_evidence_spans": ["evidence"],
                "rationale": "test",
            }
        )
    return {"decisions": decisions}


def test_v81_selects_one_disagreement_and_four_balanced_controls() -> None:
    value, truth, audit = _inputs()
    assert value["task_count"] == truth["task_count"] == 5
    assert audit["role_counts"] == {"observable_disagreement": 1, "matched_control": 4}
    assert audit["control_status_counts"] == {"correct": 2, "incorrect": 2}
    assert audit["selection_uses_source_text"] is False
    assert audit["prior_labels_in_model_input"] is False
    assert audit["prior_model_decisions_in_model_input"] is False
    assert audit["majority_voting_used"] is False
    assert audit["capped_adjudication_turn_count"] == 1
    disputed = [row for row in truth["tasks"] if row["role"] == "observable_disagreement"]
    assert len(disputed) == 1
    assert disputed[0]["field"] == "event_boundary"
    assert disputed[0]["v80_owner_status"] != disputed[0]["v80_canary_status"]


def test_v81_perfect_controls_authorize_reference_freeze_only() -> None:
    _, truth, _ = _inputs()
    output = _perfect_output(truth)
    score = score_v81(output, truth)
    assert score["passed"] is True
    assert score["reference_freeze_authorized"] is True
    assert score["adjudicated_status"] in {"correct", "incorrect"}
    assert score["metrics"]["matched_control_exact_rate"] == 1.0
    assert score["majority_voting_used"] is False
    assert score["full_calibration_authorized"] is False
    assert score["selection_authorized"] is False
    assert score["holdout_authorized"] is False
    reconciliation = build_reconciliation(
        v80_truth=_load(V80_ROOT / "alignment-owner-truth.private.json"),
        v80_output=_load(V80_ROOT / "alignment-owner-output.private.json"),
        v81_truth=truth,
        v81_score=score,
    )
    assert reconciliation["row_count"] == 10
    assert reconciliation["reference_freeze_authorized"] is True
    assert reconciliation["majority_voting_used"] is False
    assert sum(row["basis"] == "capped_side_free_adjudication" for row in reconciliation["rows"]) == 1


def test_v81_control_failure_or_abstention_suppresses_resolution() -> None:
    _, truth, _ = _inputs()
    output = _perfect_output(truth)
    control = next(row for row in truth["tasks"] if row["role"] == "matched_control")
    decision = next(row for row in output["decisions"] if row["task_id"] == control["task_id"])
    decision["field_status"] = (
        "incorrect" if decision["field_status"] == "correct" else "correct"
    )
    score = score_v81(output, truth)
    assert score["passed"] is False
    assert score["adjudicated_status"] is None
    output = _perfect_output(truth)
    disputed = next(row for row in truth["tasks"] if row["role"] == "observable_disagreement")
    decision = next(row for row in output["decisions"] if row["task_id"] == disputed["task_id"])
    decision["field_status"] = "abstain"
    score = score_v81(output, truth)
    assert score["passed"] is False
    assert score["adjudicated_status"] is None


def test_v81_freeze_is_one_presemantic_turn_and_immutable() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "v81"
        frozen = freeze_v81(output_dir=root)
        again = freeze_v81(output_dir=root)
        assert again["spec"] == frozen["spec"]
        assert frozen["spec"]["model"] == MODEL
        assert frozen["spec"]["turn_plan"] == [TURN_NAME]
        assert frozen["spec"]["task_count"] == 5
        assert frozen["spec"]["prior_labels_in_model_input"] is False
        assert frozen["spec"]["prior_model_decisions_in_model_input"] is False
        assert frozen["spec"]["majority_voting_used"] is False
        assert frozen["spec"]["retry_count_per_turn"] == 0
        assert frozen["spec"]["reference_freeze_authorized"] is False
        assert frozen["spec"]["holdout_authorized"] is False
        assert frozen["spec"]["production_mutation_allowed"] is False
        assert not (root / "terminal.json").exists()
        turn_root = frozen["paths"]["root"]
        assert not (turn_root / "capacity.json").exists()
        assert not (turn_root / "sidecar.json").exists()
        assert not (turn_root / "output.private.json").exists()
