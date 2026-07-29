from __future__ import annotations

import json
import tempfile
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v87_observable_repair import (
    MODEL,
    TURN_NAME,
    _validate_v86,
    build_repair_input,
    freeze_v87,
    reconcile_output,
    residual_mismatches,
)


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_v87_predecessor_has_exactly_one_observable_trigger() -> None:
    predecessor = _validate_v86(
        Path(
            "work/app-server-development-v2/unattended-pipeline-v5/"
            "judge-calibration-v5_4-v86-fresh-enhanced-diagnostic"
        ).resolve()
    )
    assert predecessor["trigger_task_id"] == "enh_d3742ac90da0db650c0a4d35"
    assert predecessor["values"]["score"]["metrics"]["observable_repair_trigger_count"] == 1


def test_v87_repair_input_is_one_task_and_unanchored() -> None:
    predecessor = _validate_v86(
        Path(
            "work/app-server-development-v2/unattended-pipeline-v5/"
            "judge-calibration-v5_4-v86-fresh-enhanced-diagnostic"
        ).resolve()
    )
    value = build_repair_input(predecessor)
    assert value["task_count"] == 1
    assert value["tasks"][0]["task_id"] == predecessor["trigger_task_id"]
    assert value["tasks"][0]["field"] == "temporal_horizon"
    assert value["prior_labels_present"] is False
    assert value["prior_model_decisions_present"] is False
    assert "field_status" not in value["tasks"][0]


def test_v87_reconciliation_replaces_only_trigger() -> None:
    predecessor = _validate_v86(
        Path(
            "work/app-server-development-v2/unattended-pipeline-v5/"
            "judge-calibration-v5_4-v86-fresh-enhanced-diagnostic"
        ).resolve()
    )
    prior = predecessor["values"]["output"]
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
    reconciled = reconcile_output(prior, replacement, trigger)
    before = {row["task_id"]: row for row in prior["decisions"]}
    after = {row["task_id"]: row for row in reconciled["decisions"]}
    assert after[trigger]["field_status"] == "incorrect"
    assert all(after[key] == value for key, value in before.items() if key != trigger)
    assert len(residual_mismatches(reconciled, predecessor["values"]["truth"])) == 2


def test_v87_freeze_is_single_presemantic_turn_and_immutable() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "v87"
        frozen = freeze_v87(output_dir=root)
        assert freeze_v87(output_dir=root)["spec"] == frozen["spec"]
        assert frozen["spec"]["model"] == MODEL
        assert frozen["spec"]["turn_plan"] == [TURN_NAME]
        assert frozen["spec"]["task_count"] == 1
        assert frozen["spec"]["retry_count_per_turn"] == 0
        assert frozen["spec"]["fresh_full_development_calibration_authorized"] is False
        assert frozen["spec"]["residual_reference_audit_authorized"] is False
        assert frozen["spec"]["holdout_authorized"] is False
        assert frozen["spec"]["production_mutation_allowed"] is False
        turn_root = frozen["paths"]["root"]
        assert not (root / "terminal.json").exists()
        assert not (turn_root / "capacity.json").exists()
        assert not (turn_root / "sidecar.json").exists()
        assert not (turn_root / "output.private.json").exists()


def test_v87_fails_closed_when_v86_record_drifts() -> None:
    with tempfile.TemporaryDirectory() as temp:
        source = Path(
            "work/app-server-development-v2/unattended-pipeline-v5/"
            "judge-calibration-v5_4-v86-fresh-enhanced-diagnostic"
        ).resolve()
        root = Path(temp) / "v86"
        root.mkdir()
        for path in source.iterdir():
            if path.is_file():
                (root / path.name).write_bytes(path.read_bytes())
        terminal = _load(root / "terminal.json")
        terminal["bounded_observable_repair_authorized"] = False
        (root / "terminal.json").write_text(json.dumps(terminal), encoding="utf-8")
        try:
            _validate_v86(root)
        except Exception as exc:
            assert "contract drifted" in str(exc)
        else:
            raise AssertionError("v87 accepted a drifted predecessor")
