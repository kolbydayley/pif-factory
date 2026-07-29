from __future__ import annotations

import json
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v171_negative_field_confirmation_diagnostic import (
    TURN_NAMES,
    _validate_v170_quality_terminal,
    build_v171_selection,
    freeze_v171,
    score_v171,
)


def test_v171_selects_all_known_false_positives_and_balanced_negative_controls():
    source = _validate_v170_quality_terminal()
    selection = build_v171_selection(source)
    assert selection["negative_task_count"] == 15
    assert len(selection["primary"]) == 12
    assert len(selection["repeats"]) == 4
    targets = [row for row in selection["primary"] if row["expected_status"] == "correct"]
    controls = [row for row in selection["primary"] if row["expected_status"] == "incorrect"]
    assert {row["field"] for row in targets} == {"certainty", "speaker"}
    assert len(controls) == 10
    assert len({row["field"] for row in controls}) == 10
    assert {row["field"] for row in selection["repeats"]} == {"certainty", "speaker"}


def test_v171_freeze_is_idempotent_presemantic_and_capacity_bounded(tmp_path: Path):
    root = tmp_path / "v171"
    first = freeze_v171(output_dir=root)
    second = freeze_v171(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["turn_plan"] == list(TURN_NAMES)
    assert len(TURN_NAMES) == 16
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["promotion_requires_primary_exact_count"] == 12
    assert first["spec"]["promotion_requires_repeat_exact_count"] == 4
    assert first["spec"]["full_confirmation_authorized"] is False
    policy = json.loads(first["capacity_policy"].read_text())
    assert policy["maximum_total_tokens_per_turn"] == 28000
    assert policy["phase_total_token_bound"] == 448000
    assert policy["projected_phase_quota_points"] == 8
    assert policy["minimum_remaining_reserve_percent"] == 20
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()


def test_v171_selection_is_source_free_and_downstream_stays_closed(tmp_path: Path):
    frozen = freeze_v171(output_dir=tmp_path / "v171")
    audit = json.loads(
        (frozen["root"] / "negative-field-confirmation-selection-audit.json").read_text()
    )
    assert audit["selection_uses_source_text"] is False
    assert audit["selection_uses_only_frozen_truth_observed_status_and_opaque_ids"] is True
    assert audit["production_rule_routes_every_observed_incorrect_field_decision"] is True
    assert frozen["spec"]["selection_authorized"] is False
    assert frozen["spec"]["holdout_authorized"] is False
    assert frozen["spec"]["production_mutation_allowed"] is False


def test_v171_score_requires_all_primary_repeat_and_evidence():
    frozen = freeze_v171(output_dir=Path("/tmp") / "pif-v171-score-fixture")
    outputs = []
    for turn in frozen["turns"]:
        outputs.append(
            {
                "decisions": [
                    {
                        "task_id": turn["task_id"],
                        "field_status": turn["expected_status"],
                        "source_evidence_spans": [{"text": "x", "start": 0, "end": 1}],
                        "rationale": "fixture",
                    }
                ]
            }
        )
    score = score_v171(turns=frozen["turns"], outputs=outputs)
    assert score["passed"] is True
    outputs[0]["decisions"][0]["field_status"] = (
        "incorrect" if frozen["turns"][0]["expected_status"] == "correct" else "correct"
    )
    assert score_v171(turns=frozen["turns"], outputs=outputs)["passed"] is False
