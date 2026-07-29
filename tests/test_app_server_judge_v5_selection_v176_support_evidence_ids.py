from __future__ import annotations

import json
from pathlib import Path

from research_factory.app_server_judge_v5_selection_v176_support_evidence_ids import (
    _validate_v175_failure,
    build_case_input,
    build_source_units,
    freeze_v176,
    plan_case_turns,
    project_support_output_v176,
    support_schema_v176,
    validate_support_output_v176,
)
from research_factory.app_server_judge_v5_selection_v175_support import build_exact_claim_dedup


def test_v176_preserves_the_measured_v175_failure():
    predecessor = _validate_v175_failure()
    assert predecessor["values"]["failure"]["error_class"] == "ReserveCapacityError"
    assert predecessor["values"]["failure"]["usage"]["total_tokens"] == 45993
    assert predecessor["validator_error_count"] == 12
    assert len(predecessor["attempts"]) == 1


def test_v176_fixed_units_and_projection_are_exact():
    source = "0123456789" * 120
    units = build_source_units(source)
    assert units[0]["start"] == 0
    assert units[0]["text"] == source[:450]
    assert units[1]["start"] == 400
    assert all(row["text"] == source[row["start"] : row["end"]] for row in units)
    value = {
        "schema_version": "test",
        "case_id": "case",
        "source_units": units,
        "witnesses": [{"witness_id": "wit", "proposition": {"claim_text": "claim"}}],
    }
    output = {"units": [{
        "case_id": "case", "witness_id": "wit", "support_status": "supported",
        "source_evidence_unit_ids": [units[0]["evidence_id"]], "rationale": "test",
    }]}
    assert validate_support_output_v176(output, value) == []
    projected = project_support_output_v176(output, value)
    assert projected["units"][0]["source_evidence_spans"] == [source[:450]]


def test_v176_one_case_turns_cover_every_representative():
    predecessor = _validate_v175_failure()
    reps, _, _ = build_exact_claim_dedup(predecessor["sources"]["pointwise"])
    turns = plan_case_turns(reps)
    assert len(turns) == 28
    assert sum(map(len, turns)) == 1566
    assert max(map(len, turns)) == 76
    for turn in turns:
        value = build_case_input(turn)
        schema = support_schema_v176(value)
        assert schema["properties"]["units"]["minItems"] == len(turn)


def test_v176_freeze_is_idempotent_and_presemantic(tmp_path: Path):
    root = tmp_path / "v176"
    first = freeze_v176(output_dir=root)
    second = freeze_v176(output_dir=root)
    assert first["spec"] == second["spec"]
    assert len(first["spec"]["turn_plan"]) == 28
    assert first["spec"]["v175_output_reused"] is False
    assert first["spec"]["semantic_evidence_selection_owned_by_llm"] is True
    assert first["spec"]["deterministic_evidence_projection_only"] is True
    policy = json.loads(first["capacity_policy"].read_text())
    assert policy["maximum_total_tokens_per_turn"] == 45000
    assert policy["phase_total_token_bound"] == 1260000
    assert policy["projected_phase_quota_points"] == 22
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()
    assert first["spec"]["alignment_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert first["spec"]["production_mutation_allowed"] is False
