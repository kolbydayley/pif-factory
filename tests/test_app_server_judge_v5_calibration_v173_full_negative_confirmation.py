from __future__ import annotations

import json
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v173_full_negative_confirmation import (
    TURN_NAMES,
    _validate_v172_reference,
    build_final_field_output,
    build_v173_selection,
    freeze_v173,
)


def test_v173_selects_every_observed_negative_without_truth():
    source = _validate_v172_reference()
    selected = build_v173_selection(source)
    observed = {
        row["task_id"]
        for row in source["v170"]["values"]["fields"]["decisions"]
        if row["field_status"] == "incorrect"
    }
    assert len(selected) == 15
    assert set(selected) == observed


def test_v173_freeze_is_idempotent_fresh_and_capacity_bounded(tmp_path: Path):
    root = tmp_path / "v173"
    first = freeze_v173(output_dir=root)
    second = freeze_v173(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["turn_plan"] == list(TURN_NAMES)
    assert len(TURN_NAMES) == 15
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["v171_diagnostic_outputs_reused"] is False
    assert first["spec"]["all_confirmation_turns_fresh"] is True
    assert first["spec"]["selection_uses_truth_labels"] is False
    policy = json.loads(first["capacity_policy"].read_text())
    audit = json.loads((root / "capacity-policy-audit.json").read_text())
    assert policy["maximum_total_tokens_per_turn"] == 28000
    assert policy["phase_total_token_bound"] == 420000
    assert policy["projected_phase_quota_points"] == 8
    assert policy["minimum_remaining_reserve_percent"] == 20
    assert audit["measured_basis"]["v171_gpt54_maximum_total_tokens"] == 22100
    assert len(audit["v171_measured_sidecars"]) == 16
    assert first["spec"]["frozen_inputs"]["support_canary"]["sha256"]
    assert first["spec"]["frozen_inputs"]["final_alignment_canary"]["sha256"]
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()


def test_v173_final_output_replaces_only_every_observed_negative():
    source = _validate_v172_reference()
    primary = source["v170"]["values"]["fields"]
    negatives = [row for row in primary["decisions"] if row["field_status"] == "incorrect"]
    confirmations = []
    for row in negatives:
        replacement = dict(row)
        replacement["field_status"] = "correct"
        confirmations.append({"decisions": [replacement]})
    final = build_final_field_output(primary=primary, confirmations=confirmations)
    assert len(final["decisions"]) == len(primary["decisions"])
    assert all(row["field_status"] == "correct" for row in final["decisions"])


def test_v173_keeps_selection_holdout_and_production_closed_before_result(tmp_path: Path):
    frozen = freeze_v173(output_dir=tmp_path / "v173")
    audit = json.loads(
        (frozen["root"] / "full-negative-confirmation-selection-audit.json").read_text()
    )
    assert audit["selection_uses_source_text"] is False
    assert audit["selection_uses_truth_labels"] is False
    assert audit["every_observed_incorrect_primary_field_decision_selected"] is True
    assert frozen["spec"]["development_judge_frozen"] is False
    assert frozen["spec"]["selection_authorized"] is False
    assert frozen["spec"]["holdout_authorized"] is False
    assert frozen["spec"]["production_mutation_allowed"] is False
