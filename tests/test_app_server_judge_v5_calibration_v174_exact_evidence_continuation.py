from __future__ import annotations

import json
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v174_exact_evidence_continuation import (
    FAILED_TURN_NAME,
    REUSED_TURN_NAMES,
    TURN_NAMES,
    _validate_v173_failure,
    exact_single_evidence_schema,
    freeze_v174,
)


def test_v174_accepts_only_the_six_valid_v173_prefix_outputs():
    predecessor = _validate_v173_failure()
    assert len(predecessor["attempts"]) == 7
    assert len(predecessor["reused_outputs"]) == 6
    assert len(predecessor["reused_records"]) == 6
    assert REUSED_TURN_NAMES == tuple(f"full_negative_confirmation_{i:02d}" for i in range(6))
    assert FAILED_TURN_NAME == "full_negative_confirmation_06"
    assert predecessor["values"]["failure"]["unknown_usage_turn_count"] == 0


def test_v174_schema_requires_exactly_one_evidence_span():
    predecessor = _validate_v173_failure()
    value = predecessor["turn_values"][FAILED_TURN_NAME]["input"]
    schema = exact_single_evidence_schema(value)
    evidence = schema["properties"]["decisions"]["items"]["properties"][
        "source_evidence_spans"
    ]
    assert evidence["minItems"] == 1
    assert evidence["maxItems"] == 1


def test_v174_freeze_is_idempotent_fresh_and_capacity_bounded(tmp_path: Path):
    root = tmp_path / "v174"
    first = freeze_v174(output_dir=root)
    second = freeze_v174(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["turn_plan"] == list(TURN_NAMES)
    assert len(TURN_NAMES) == 9
    assert first["spec"]["reused_valid_turn_count"] == 6
    assert first["spec"]["fresh_turn_count"] == 9
    assert first["spec"]["v173_failed_output_reused"] is False
    assert first["spec"]["semantic_rubric_changed"] is False
    policy = json.loads(first["capacity_policy"].read_text())
    audit = json.loads((root / "capacity-policy-audit.json").read_text())
    assert policy["maximum_total_tokens_per_turn"] == 28000
    assert policy["phase_total_token_bound"] == 252000
    assert policy["projected_phase_quota_points"] == 5
    assert policy["minimum_remaining_reserve_percent"] == 20
    assert audit["measured_basis"]["v173_measured_turn_count"] == 7
    assert audit["measured_basis"]["v173_maximum_total_tokens"] == 22266
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()


def test_v174_keeps_downstream_gates_closed_before_result(tmp_path: Path):
    frozen = freeze_v174(output_dir=tmp_path / "v174")
    audit = json.loads(
        (frozen["root"] / "exact-evidence-continuation-audit.json").read_text()
    )
    assert audit["v173_failed_output_reused"] is False
    assert audit["semantic_rubric_changed"] is False
    assert audit["selection_uses_truth_labels"] is False
    assert frozen["spec"]["development_judge_frozen"] is False
    assert frozen["spec"]["selection_authorized"] is False
    assert frozen["spec"]["holdout_authorized"] is False
    assert frozen["spec"]["production_mutation_allowed"] is False
