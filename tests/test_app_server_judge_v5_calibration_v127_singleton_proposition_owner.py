from __future__ import annotations

import json
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v121_retained_field_owner import (
    _validate_v106,
    _validate_v120,
)
from research_factory.app_server_judge_v5_calibration_v127_singleton_proposition_owner import (
    _validate_v126,
    build_reference_candidate_v127,
    build_v127_inputs,
    freeze_v127,
    proposition_output_schema,
    score_v127,
    validate_proposition_output,
)


def _built():
    predecessor = _validate_v126()
    return predecessor, build_v127_inputs(predecessor, _validate_v120(), _validate_v106())


def _passing_outputs(rows):
    result = {}
    for row in rows:
        truth = row["truth"]
        result[row["turn_name"]] = {
            "decisions": [
                {
                    "task_id": truth["task_id"],
                    "proposition_status": truth.get("control_expected_status") or "supported",
                    "source_evidence_spans": [row["value"]["tasks"][0]["source_excerpt"]],
                    "rationale": "synthetic singleton support decision",
                }
            ]
        }
    return result


def test_v127_selects_three_nonunanimous_propositions_and_balanced_controls():
    _, (rows, truth, selection) = _built()
    assert len(rows) == 6
    assert truth["singleton_control_count"] == 3
    assert truth["singleton_owner_count"] == 3
    assert selection["cohort_case_count"] == 18
    assert selection["cohort_witness_count"] == 52
    assert selection["nonunanimous_proposition_count"] == 3
    assert selection["dispute_role_counts"] == {
        "consensus_reference_dispute": 1,
        "model_disagreement": 2,
    }
    assert selection["control_status_counts"] == {"supported": 2, "unsupported": 1}
    assert selection["proposition_support_separate_from_structured_field_correctness"] is True
    assert selection["prior_labels_in_model_input"] is False
    assert selection["prior_model_decisions_in_model_input"] is False
    assert all(set(row["value"]["tasks"][0]) == {"task_id", "source_excerpt", "proposition_text"} for row in rows)


def test_v127_schema_and_validator_require_exact_nonempty_evidence():
    _, (rows, _, _) = _built()
    value = rows[0]["value"]
    schema = proposition_output_schema(value)
    assert schema["properties"]["decisions"]["minItems"] == 1
    output = _passing_outputs([rows[0]])[rows[0]["turn_name"]]
    assert validate_proposition_output(output, value) == []
    output["decisions"][0]["source_evidence_spans"] = ["not exact"]
    assert validate_proposition_output(output, value) == ["decision_0_evidence"]


def test_v127_score_and_reference_projection_are_fail_closed():
    predecessor, (rows, truth, _) = _built()
    outputs = _passing_outputs(rows)
    score = score_v127(outputs, truth)
    assert score["passed"] is True
    assert score["metrics"]["singleton_control_exact_count"] == 3
    candidate = build_reference_candidate_v127(
        current_reference=predecessor["values"]["reference"],
        truth=truth,
        outputs=outputs,
    )
    assert len(candidate["cases"]) == 66
    assert candidate["retained_proposition_reference_patch_authorized"] is True
    assert candidate["proposition_reference_frozen"] is True
    assert candidate["alignment_reference_frozen"] is False

    owner = next(row for row in truth["tasks"] if row["role"] == "singleton_owner")
    for output in outputs.values():
        if output["decisions"][0]["task_id"] == owner["task_id"]:
            output["decisions"][0]["proposition_status"] = "abstain"
    assert score_v127(outputs, truth)["passed"] is False


def test_v127_freeze_is_idempotent_presemantic_and_keeps_later_gates_closed(
    tmp_path: Path,
):
    root = tmp_path / "v127"
    first = freeze_v127(output_dir=root)
    second = freeze_v127(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["model"] == "gpt-5.6-sol"
    assert len(first["spec"]["turn_plan"]) == 6
    assert first["spec"]["maximum_tasks_per_turn"] == 1
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["retained_proposition_reference_patch_authorized"] is False
    assert first["spec"]["proposition_reference_frozen"] is False
    assert first["spec"]["alignment_reference_frozen"] is False
    assert first["spec"]["fresh_diagnostic_authorized"] is False
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert first["spec"]["production_mutation_allowed"] is False
    assert not list(root.glob("turns/*/capacity.json"))
    assert not list(root.glob("turns/*/sidecar.json"))
    assert not (root / "terminal.json").exists()
    policy = json.loads(Path(first["capacity_policy"]).read_text())
    assert policy["phase_total_token_bound"] == 420000
    assert policy["minimum_remaining_reserve_percent"] == 20
    assert policy["retry_count_per_turn"] == 0
