from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v148_support_projection_repair import (
    TURN_NAMES,
    _project_owner_output,
    _validate_v147,
    build_v148_inputs,
    freeze_v148,
    score_v148,
)


def _outputs(value, truth, target_status="supported"):
    statuses = {
        (row["case_id"], row["witness_id"]): row["expected_status"]
        for row in truth["controls"]
    }
    statuses[(truth["target"]["case_id"], truth["target"]["witness_id"])] = target_status
    units = [
        {
            "case_id": row["case_id"],
            "witness_id": row["witness_id"],
            "support_status": statuses[(row["case_id"], row["witness_id"])],
            "source_evidence_spans": ["evidence"],
            "rationale": "expected",
        }
        for row in value["units"]
    ]
    return {"units": units}


def test_v148_builds_two_balanced_controls_one_target_and_reversed_canary():
    source = _validate_v147()
    primary, canary, truth, selection, records = build_v148_inputs(source)

    assert truth["unit_count"] == 3
    assert truth["control_count"] == 2
    assert truth["target_count"] == 1
    assert {row["expected_status"] for row in truth["controls"]} == {
        "supported",
        "unsupported",
    }
    assert len(records) == 2
    assert [row["witness_id"] for row in canary["units"]] == list(
        reversed([row["witness_id"] for row in primary["units"]])
    )
    assert selection["canary_marker_in_model_input"] is False
    assert selection["selection_uses_source_text"] is False
    assert selection["projection_is_deterministic_from_llm_support_status"] is True
    assert selection["majority_voting_used"] is False


def test_v148_score_requires_both_controls_target_order_and_evidence():
    primary_input, canary_input, truth, _, _ = build_v148_inputs(_validate_v147())
    primary = _outputs(primary_input, truth)
    canary = _outputs(canary_input, truth)
    passed = score_v148(primary=primary, canary=canary, truth=truth)

    assert passed["passed"] is True
    assert passed["metrics"]["control_exact_count"] == 2
    assert passed["metrics"]["order_canary_exact_count"] == 3
    assert passed["metrics"]["target_abstention_count"] == 0

    broken = deepcopy(canary)
    target = truth["target"]
    row = next(
        item
        for item in broken["units"]
        if item["case_id"] == target["case_id"]
        and item["witness_id"] == target["witness_id"]
    )
    row["support_status"] = "unsupported"
    assert score_v148(primary=primary, canary=broken, truth=truth)["passed"] is False


def test_v148_projects_only_two_ui_fields_from_llm_support_receipts():
    source = _validate_v147()
    primary_input, _, truth, _, _ = build_v148_inputs(source)
    primary = _outputs(primary_input, truth)
    result = _project_owner_output(source=source, truth=truth, fresh_support=primary)

    assert result["unsupported_inference_projection_count"] == 2
    assert len(result["decisions"]) == 14
    projected = {
        truth["inherited_projection"]["owner_task_id"],
        truth["target"]["owner_task_id"],
    }
    rows = {row["task_id"]: row for row in result["decisions"]}
    assert all(rows[task_id]["field_status"] in {"correct", "incorrect"} for task_id in projected)


def test_v148_freeze_is_idempotent_two_turn_bounded_and_private(tmp_path: Path):
    root = tmp_path / "v148"
    first = freeze_v148(output_dir=root)
    second = freeze_v148(output_dir=root)

    assert first["spec"] == second["spec"]
    assert first["spec"]["model"] == "gpt-5.6-sol"
    assert first["spec"]["reasoning_effort"] == "high"
    assert first["spec"]["turn_plan"] == list(TURN_NAMES)
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert first["spec"]["production_mutation_allowed"] is False
    policy = json.loads(Path(first["capacity_policy"]).read_text())
    assert policy["phase_total_token_bound"] == 140000
    assert policy["minimum_remaining_reserve_percent"] == 20
    for row in first["spec"]["frozen_inputs"]["turns"]:
        prompt = Path(row["prompt"]["path"]).read_text()
        assert "expected_status" not in prompt
        assert "owner_task_id" not in prompt
        assert "pointwise_support_receipt_available" not in prompt
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()


def test_v148_freeze_does_not_mutate_v147_or_v146(tmp_path: Path):
    before = _validate_v147()
    before_records = (before["records"], before["v146"]["records"])
    freeze_v148(output_dir=tmp_path / "v148")
    after = _validate_v147()
    assert before_records == (after["records"], after["v146"]["records"])
