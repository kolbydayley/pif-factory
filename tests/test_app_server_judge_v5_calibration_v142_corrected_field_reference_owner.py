from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v142_corrected_field_reference_owner import (
    FIELDS,
    TURN_NAMES,
    _patch_truth_and_reference,
    _validate_v141,
    build_v142_inputs,
    field_rubric_v142,
    freeze_v142,
    score_v142,
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


def test_v142_groups_identical_membership_by_field_with_unmarked_reversal():
    rows, truth, selection = build_v142_inputs(_validate_v141())
    by_name = {row["turn_name"]: row for row in rows}

    assert len(rows) == len(TURN_NAMES) == 10
    assert truth["task_count"] == 14
    assert truth["control_count"] == 5
    assert truth["owner_count"] == 9
    assert selection["field_task_counts"] == {
        "event_boundary": 2,
        "evidence": 2,
        "metric": 4,
        "reported_actor": 3,
        "stance": 3,
    }
    assert selection["maximum_tasks_per_turn"] == 4
    assert selection["canary_marker_in_model_input"] is False
    assert selection["majority_voting_used"] is False
    for field in FIELDS:
        primary = by_name[f"gpt55_{field}_primary"]["value"]
        canary = by_name[f"gpt55_{field}_order_canary"]["value"]
        primary_ids = [row["task_id"] for row in primary["tasks"]]
        canary_ids = [row["task_id"] for row in canary["tasks"]]
        assert canary_ids == list(reversed(primary_ids))
        assert all(row["field"] == field for row in primary["tasks"])
        assert "permutation_canary" not in canary
        assert all("role" not in row for row in primary["tasks"])


def test_v142_rubric_repairs_independent_field_semantics_without_gate_changes():
    rubric = field_rubric_v142()

    assert rubric["rubric_changes_quality_gates"] is False
    assert rubric["rubric_changes_source_evidence_contract"] is False
    assert rubric["semantic_pruning_performed"] is False
    assert "not_applicable" in rubric["fields"]["metric"]["decision_rule"]
    assert "direct speaker" in rubric["fields"]["reported_actor"]["decision_rule"]
    assert "bare factual assertion" in rubric["fields"]["stance"]["decision_rule"]
    assert "merge or split" in rubric["fields"]["event_boundary"]["decision_rule"]
    assert "need not be minimal" in rubric["fields"]["evidence"]["decision_rule"]


def test_v142_owner_gate_passes_only_with_controls_order_and_no_abstention():
    _, truth, _ = build_v142_inputs(_validate_v141())
    primary, canary = _outputs(truth)

    passed = score_v142(primary=primary, canary=canary, truth=truth)
    assert passed["passed"] is True
    assert passed["metrics"]["control_exact_count"] == 5
    assert passed["metrics"]["order_canary_exact_count"] == 14
    assert passed["metrics"]["owner_abstention_count"] == 0

    broken_order = deepcopy(canary)
    broken_order["decisions"][0]["field_status"] = (
        "incorrect"
        if broken_order["decisions"][0]["field_status"] == "correct"
        else "correct"
    )
    assert score_v142(primary=primary, canary=broken_order, truth=truth)["passed"] is False

    owner_id = next(row["task_id"] for row in truth["tasks"] if row["role"] == "owner")
    abstaining = deepcopy(primary)
    next(row for row in abstaining["decisions"] if row["task_id"] == owner_id)[
        "field_status"
    ] = "abstain"
    assert score_v142(primary=abstaining, canary=canary, truth=truth)["passed"] is False


def test_v142_reference_patch_is_owned_independently_of_old_v140_rescore():
    predecessor = _validate_v141()
    _, truth, _ = build_v142_inputs(predecessor)
    primary, _ = _outputs(truth)

    patched_truth, reference = _patch_truth_and_reference(
        current_truth=predecessor["v140"]["values"]["truth"],
        current_reference=predecessor["current_reference"],
        owner_truth=truth,
        primary=primary,
    )

    assert patched_truth["v142_owner_task_count"] == 9
    assert patched_truth["v142_reference_change_count"] == 0
    assert reference["reference_version"] == (
        "fixture_reference_v12_corrected_field_owner_frozen"
    )
    assert reference["v142_owner_task_count"] == 9
    assert reference["v142_reference_change_count"] == 0


def test_v142_freeze_is_idempotent_presemantic_and_ten_turn_bounded(tmp_path: Path):
    root = tmp_path / "v142"
    first = freeze_v142(output_dir=root)
    second = freeze_v142(output_dir=root)

    assert first["spec"] == second["spec"]
    assert first["spec"]["model"] == "gpt-5.5"
    assert first["spec"]["reasoning_effort"] == "high"
    assert first["spec"]["turn_plan"] == list(TURN_NAMES)
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["old_v140_rescore_is_audit_only"] is True
    assert first["spec"]["fresh_full_calibration_authorized"] is False
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert first["spec"]["production_mutation_allowed"] is False

    policy = json.loads(Path(first["capacity_policy"]).read_text())
    assert policy["phase_total_token_bound"] == 700000
    assert policy["minimum_remaining_reserve_percent"] == 20
    assert policy["retry_count_per_turn"] == 0
    for frozen in first["spec"]["frozen_inputs"]["turns"]:
        prompt = Path(frozen["prompt"]["path"]).read_text()
        assert "control_expected_status" not in prompt
        assert "current_status" not in prompt
        assert "v140_status" not in prompt
        assert "permutation_canary" not in prompt
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()


def test_v142_freeze_does_not_mutate_v141(tmp_path: Path):
    before = _validate_v141()["records"]
    freeze_v142(output_dir=tmp_path / "v142")
    after = _validate_v141()["records"]
    assert before == after
