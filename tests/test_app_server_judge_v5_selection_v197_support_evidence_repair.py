from __future__ import annotations

import json
from pathlib import Path

from research_factory.app_server_judge_v5_selection_v197_support_evidence_repair import (
    TURN_NAME,
    _validate_v196_failed_attempt,
    build_repair_input,
    freeze_v197,
    merge_repaired_support_output,
    project_repair_output_v197,
    repair_schema_v197,
    validate_repair_output_v197,
)


def _synthetic_repair(value):
    rows = []
    for case in value["cases"]:
        evidence_id = case["source_units"][0]["evidence_id"]
        for witness in case["witnesses"]:
            rows.append(
                {
                    "case_id": case["case_id"],
                    "witness_id": witness["witness_id"],
                    "support_status": "supported",
                    "source_evidence_unit_ids": [evidence_id],
                    "rationale": "Synthetic exact-evidence projection test.",
                }
            )
    return {"units": rows}


def test_v197_preserves_v196_and_selects_only_nonexact_rows():
    predecessor = _validate_v196_failed_attempt()
    assert predecessor["terminal"]["usage"]["total_tokens"] == 30884
    assert predecessor["invalid_indices"] == [5, 25, 26, 27, 28, 29]
    value = build_repair_input(predecessor)
    assert len(value["cases"]) == 2
    assert sorted(len(case["witnesses"]) for case in value["cases"]) == [1, 5]
    assert value["side_free"] is True
    assert value["system_identity_present"] is False
    assert value["prior_support_decisions_present"] is False
    for case in value["cases"]:
        for unit in case["source_units"]:
            assert unit["text"] == next(
                row["source_excerpt"]
                for row in predecessor["value"]["units"]
                if row["case_id"] == case["case_id"]
            )[unit["start"] : unit["end"]]


def test_v197_exact_id_projection_repairs_merged_schema():
    predecessor = _validate_v196_failed_attempt()
    value = build_repair_input(predecessor)
    output = _synthetic_repair(value)
    assert validate_repair_output_v197(output, value) == []
    schema = repair_schema_v197(value)
    assert schema["properties"]["units"]["minItems"] == 6
    projected = project_repair_output_v197(output, value)
    merged = merge_repaired_support_output(predecessor, projected)
    assert len(merged["units"]) == 30


def test_v197_rejects_cross_case_evidence_id():
    predecessor = _validate_v196_failed_attempt()
    value = build_repair_input(predecessor)
    output = _synthetic_repair(value)
    output["units"][0]["source_evidence_unit_ids"] = [
        value["cases"][1]["source_units"][0]["evidence_id"]
    ]
    assert "repair_0_decision_or_evidence_ids" in validate_repair_output_v197(
        output, value
    )


def test_v197_freeze_is_idempotent_and_presemantic(tmp_path: Path):
    root = tmp_path / "v197"
    first = freeze_v197(output_dir=root)
    second = freeze_v197(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["turn_plan"] == [TURN_NAME]
    assert first["spec"]["preserved_valid_support_decision_count"] == 24
    assert first["spec"]["fresh_repair_support_decision_count"] == 6
    assert first["spec"]["v196_turn_replayed"] is False
    assert first["spec"]["v196_failure_preserved"] is True
    assert first["spec"]["support_rubric_changed"] is False
    assert first["spec"]["alignment_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    policy = json.loads(first["capacity_policy"].read_text())
    assert policy["phase_total_token_bound"] == 30000
    assert policy["projected_phase_quota_points"] == 1
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()
