from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

from research_factory.app_server_judge_v5 import CHECKLIST_FIELDS
from research_factory.app_server_judge_v5_calibration_v126_singleton_field_owner import (
    _validate_v125,
)
from research_factory.app_server_judge_v5_calibration_v145_comprehensive_field_reference_owner import (
    FIELDS,
    TURN_NAMES,
    _patch_truth_and_reference,
    _validate_v144,
    build_v145_inputs,
    field_rubric_v145,
    freeze_v145,
    score_v145,
)


def _outputs(truth):
    decisions = []
    for row in truth["tasks"]:
        status = row.get("control_expected_status", row.get("current_status"))
        decisions.append(
            {
                "task_id": row["task_id"],
                "field_status": status,
                "source_evidence_spans": ["evidence"],
                "rationale": "expected",
            }
        )
    value = {"decisions": decisions}
    return value, deepcopy(value)


def test_v145_selects_all_observable_v144_triggers_with_one_control_per_field():
    rows, truth, selection = build_v145_inputs(_validate_v144(), _validate_v125())
    by_name = {row["turn_name"]: row for row in rows}

    assert len(rows) == len(TURN_NAMES) == 22
    assert truth["task_count"] == 25
    assert truth["control_count"] == 11
    assert truth["owner_count"] == 14
    assert selection["trigger_field_counts"] == {
        "actor": 1,
        "attribution": 1,
        "certainty": 2,
        "event_type": 1,
        "evidence": 1,
        "metric": 1,
        "negation": 2,
        "reported_actor": 1,
        "target": 1,
        "temporal_horizon": 1,
        "unsupported_inference": 2,
    }
    assert selection["maximum_tasks_per_turn"] == 3
    assert selection["canary_marker_in_model_input"] is False
    assert selection["majority_voting_used"] is False
    assert selection["selection_uses_source_text"] is False
    event_type_control = next(
        row
        for row in truth["tasks"]
        if row["role"] == "control" and row["field"] == "event_type"
    )
    assert event_type_control["control_source_lineage"] == (
        "v143_v144_unanimous_order_stable_control"
    )
    for field in FIELDS:
        primary = by_name[f"sol_owner_{field}_primary"]["value"]
        canary = by_name[f"sol_owner_{field}_order_canary"]["value"]
        primary_ids = [row["task_id"] for row in primary["tasks"]]
        canary_ids = [row["task_id"] for row in canary["tasks"]]
        assert canary_ids == list(reversed(primary_ids))
        assert all(row["field"] == field for row in primary["tasks"])
        assert "permutation_canary" not in canary
        assert all("role" not in row for row in primary["tasks"])


def test_v145_rubric_defines_all_fifteen_independent_fields_without_gate_changes():
    rubric = field_rubric_v145()

    assert set(rubric["fields"]) == set(CHECKLIST_FIELDS)
    assert rubric["rubric_changes_quality_gates"] is False
    assert rubric["rubric_changes_source_evidence_contract"] is False
    assert rubric["semantic_pruning_performed"] is False
    assert "direct speaker" in rubric["fields"]["reported_actor"]["decision_rule"]
    assert "bare factual assertion" in rubric["fields"]["stance"]["decision_rule"]
    assert "not_applicable" in rubric["fields"]["metric"]["decision_rule"]
    assert "bare assertion" in rubric["fields"]["certainty"]["decision_rule"]
    assert "adjacent proposition" in rubric["fields"]["negation"]["decision_rule"]
    assert "claim_text" in rubric["fields"]["unsupported_inference"]["decision_rule"]


def test_v145_owner_gate_requires_controls_order_evidence_and_no_abstention():
    _, truth, _ = build_v145_inputs(_validate_v144(), _validate_v125())
    primary, canary = _outputs(truth)
    passed = score_v145(primary=primary, canary=canary, truth=truth)

    assert passed["passed"] is True
    assert passed["metrics"]["control_exact_count"] == 11
    assert passed["metrics"]["order_canary_exact_count"] == 25
    assert passed["metrics"]["owner_abstention_count"] == 0

    broken = deepcopy(canary)
    broken["decisions"][0]["field_status"] = (
        "incorrect"
        if broken["decisions"][0]["field_status"] == "correct"
        else "correct"
    )
    assert score_v145(primary=primary, canary=broken, truth=truth)["passed"] is False


def test_v145_reference_patch_is_independent_of_old_v144_rescore():
    predecessor = _validate_v144()
    _, truth, _ = build_v145_inputs(predecessor, _validate_v125())
    primary, _ = _outputs(truth)
    patched_truth, reference = _patch_truth_and_reference(
        current_truth=predecessor["v143"]["values"]["truth"],
        current_reference=predecessor["v143"]["v142"]["values"]["reference"],
        owner_truth=truth,
        primary=primary,
    )

    assert patched_truth["v145_owner_task_count"] == 14
    assert patched_truth["v145_reference_change_count"] == 0
    assert reference["reference_version"] == (
        "fixture_reference_v13_comprehensive_field_owner_frozen"
    )
    assert reference["v145_reference_change_count"] == 0


def test_v145_freeze_is_idempotent_presemantic_and_twenty_two_turn_bounded(tmp_path: Path):
    root = tmp_path / "v145"
    first = freeze_v145(output_dir=root)
    second = freeze_v145(output_dir=root)

    assert first["spec"] == second["spec"]
    assert first["spec"]["model"] == "gpt-5.6-sol"
    assert first["spec"]["reasoning_effort"] == "high"
    assert first["spec"]["turn_plan"] == list(TURN_NAMES)
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["old_v144_rescore_is_audit_only"] is True
    assert first["spec"]["fresh_full_calibration_authorized"] is False
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert first["spec"]["production_mutation_allowed"] is False

    policy = json.loads(Path(first["capacity_policy"]).read_text())
    assert policy["phase_total_token_bound"] == 1540000
    assert policy["minimum_remaining_reserve_percent"] == 20
    assert policy["retry_count_per_turn"] == 0
    for frozen in first["spec"]["frozen_inputs"]["turns"]:
        prompt = Path(frozen["prompt"]["path"]).read_text()
        assert "control_expected_status" not in prompt
        assert "current_status" not in prompt
        assert "v144_primary_status" not in prompt
        assert "permutation_canary" not in prompt
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()


def test_v145_freeze_does_not_mutate_v144(tmp_path: Path):
    before = _validate_v144()["records"]
    freeze_v145(output_dir=tmp_path / "v145")
    after = _validate_v144()["records"]
    assert before == after
