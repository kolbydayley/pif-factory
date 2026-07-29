from __future__ import annotations

import json
import tempfile
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v91_fresh_luna_diagnostic import (
    DEFAULT_OUTPUT_ROOT as V91_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v92_speaker_repair import (
    MODEL,
    TURN_NAME,
    _validate_v91,
    build_repair_input,
    freeze_v92,
    reconcile_output,
    residual_mismatches,
)


def test_v92_predecessor_has_one_speaker_trigger() -> None:
    predecessor = _validate_v91(V91_ROOT)
    assert predecessor["trigger_task_id"] == "fresh_d417d4737822f52761005b97"
    value = build_repair_input(predecessor)
    assert value["task_count"] == 1
    assert value["tasks"][0]["field"] == "speaker"
    assert value["prior_labels_present"] is False
    assert value["prior_model_decisions_present"] is False


def test_v92_reconciliation_replaces_only_trigger() -> None:
    predecessor = _validate_v91(V91_ROOT)
    trigger = predecessor["trigger_task_id"]
    replacement = {
        "decisions": [
            {
                "task_id": trigger,
                "field_status": "incorrect",
                "source_evidence_spans": ["evidence"],
                "rationale": "test",
            }
        ]
    }
    prior = predecessor["values"]["output"]
    reconciled = reconcile_output(prior, replacement, trigger)
    before = {row["task_id"]: row for row in prior["decisions"]}
    after = {row["task_id"]: row for row in reconciled["decisions"]}
    assert after[trigger]["field_status"] == "incorrect"
    assert all(after[key] == value for key, value in before.items() if key != trigger)
    residuals = residual_mismatches(reconciled, predecessor["values"]["truth"])
    assert sorted(row["field"] for row in residuals) == ["certainty", "target"]


def test_v92_freeze_is_one_presemantic_turn_and_immutable() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "v92"
        frozen = freeze_v92(output_dir=root)
        assert freeze_v92(output_dir=root)["spec"] == frozen["spec"]
        assert frozen["spec"]["model"] == MODEL
        assert frozen["spec"]["turn_plan"] == [TURN_NAME]
        assert frozen["spec"]["retry_count_per_turn"] == 0
        assert frozen["spec"]["fresh_full_development_calibration_authorized"] is False
        assert frozen["spec"]["holdout_authorized"] is False
        assert frozen["spec"]["production_mutation_allowed"] is False
        assert not (root / "terminal.json").exists()
        assert not (frozen["paths"]["root"] / "capacity.json").exists()
        assert not (frozen["paths"]["root"] / "sidecar.json").exists()
        assert not (frozen["paths"]["root"] / "output.private.json").exists()
