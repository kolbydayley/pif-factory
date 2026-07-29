from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

from research_factory.app_server_judge_v5 import CHECKLIST_FIELDS
from research_factory import app_server_judge_v5_calibration_v143_corrected_layered_diagnostic as v143
from research_factory.app_server_judge_v5_calibration_v144_same_field_diagnostic import (
    TURN_NAMES,
    _validate_v143,
    build_error_taxonomy_v144,
    build_v144_inputs,
    freeze_v144,
)


def _expected_fields(truth):
    return {
        "decisions": [
            {
                "task_id": row["task_id"],
                "field_status": row["expected_status"],
                "source_evidence_spans": ["evidence"],
                "rationale": "expected",
            }
            for row in truth["field_tasks"]
        ]
    }


def test_v144_taxonomy_is_sanitized_and_matches_v143_failure_counts():
    taxonomy = build_error_taxonomy_v144(_validate_v143())

    assert taxonomy["source_field_decision_count"] == 30
    assert taxonomy["source_primary_correct_count"] == 20
    assert taxonomy["source_order_exact_count"] == 25
    assert taxonomy["source_support_gates_passed"] is True
    assert taxonomy["source_text_included"] is False
    assert taxonomy["production_mutated"] is False
    assert len(taxonomy["fields"]) == 15
    assert sum(row["primary_correct_count"] for row in taxonomy["fields"]) == 20
    assert sum(row["order_exact_count"] for row in taxonomy["fields"]) == 25


def test_v144_reuses_exact_tasks_in_same_field_two_polarity_turns():
    predecessor = _validate_v143()
    rows, selection = build_v144_inputs(predecessor)
    by_name = {row["turn_name"]: row for row in rows}
    source_ids = set(predecessor["input_tasks"])

    assert len(rows) == len(TURN_NAMES) == 30
    assert selection["reused_field_task_count"] == 30
    assert selection["truth_changed_from_v143"] is False
    assert selection["task_membership_changed_from_v143"] is False
    assert selection["selection_uses_source_text"] is False
    assert selection["semantic_pruning_performed"] is False
    seen = set()
    for field in CHECKLIST_FIELDS:
        primary = by_name[f"same_field_{field}_primary"]["value"]
        canary = by_name[f"same_field_{field}_order_canary"]["value"]
        primary_ids = [row["task_id"] for row in primary["tasks"]]
        canary_ids = [row["task_id"] for row in canary["tasks"]]
        assert primary["task_count"] == canary["task_count"] == 2
        assert canary_ids == list(reversed(primary_ids))
        assert all(row["field"] == field for row in primary["tasks"])
        seen.update(primary_ids)
    assert seen == source_ids


def test_v144_same_field_outputs_can_pass_original_v143_frozen_gates():
    predecessor = _validate_v143()
    fields = _expected_fields(predecessor["values"]["truth"])
    score = v143.score_v143(
        support=predecessor["values"]["support"],
        support_canary=predecessor["values"]["support_canary"],
        fields=fields,
        field_canary=deepcopy(fields),
        truth=predecessor["values"]["truth"],
    )

    assert score["passed"] is True
    assert score["corrected_alignment_diagnostic_authorized"] is True
    assert score["metrics"]["support_sensitivity"] == 1.0
    assert score["metrics"]["support_specificity"] == 1.0
    assert score["metrics"]["field_decision_accuracy"] == 1.0
    assert score["metrics"]["pointwise_field_issue_f1"] == 1.0
    assert score["metrics"]["field_canary_exact_count"] == 30


def test_v144_freeze_is_idempotent_presemantic_and_thirty_turn_bounded(tmp_path: Path):
    root = tmp_path / "v144"
    first = freeze_v144(output_dir=root)
    second = freeze_v144(output_dir=root)

    assert first["spec"] == second["spec"]
    assert first["spec"]["model"] == "gpt-5.5"
    assert first["spec"]["reasoning_effort"] == "high"
    assert first["spec"]["turn_plan"] == list(TURN_NAMES)
    assert len(TURN_NAMES) == 30
    assert first["spec"]["maximum_tasks_per_turn"] == 2
    assert first["spec"]["reused_support_receipts"] is True
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["canary_marker_in_model_input"] is False
    assert first["spec"]["corrected_alignment_diagnostic_authorized"] is False
    assert first["spec"]["fresh_full_calibration_authorized"] is False
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert first["spec"]["production_mutation_allowed"] is False

    policy = json.loads(Path(first["capacity_policy"]).read_text())
    assert policy["phase_total_token_bound"] == 2100000
    assert policy["minimum_remaining_reserve_percent"] == 20
    assert policy["retry_count_per_turn"] == 0
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()


def test_v144_freeze_does_not_mutate_v143(tmp_path: Path):
    before = _validate_v143()["records"]
    freeze_v144(output_dir=tmp_path / "v144")
    after = _validate_v143()["records"]
    assert before == after
