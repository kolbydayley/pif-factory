from __future__ import annotations

from copy import deepcopy
from pathlib import Path

from research_factory.app_server_judge_v5 import CHECKLIST_FIELDS
from research_factory.app_server_judge_v5_calibration_v108_layered_diagnostic import (
    ADJUDICATION_TURN,
    BASE_TURNS,
    CANARY_TURNS,
    POINTWISE_TURNS,
    TURN_NAMES,
    _select_case_ids,
    _validate_predecessors,
    freeze_v108,
    pointwise_checklist_instructions,
    pointwise_checklist_schema,
    project_pointwise_checklist,
    score_v108,
    validate_pointwise_checklist_output,
)


def _perfect_explicit(pointwise_input):
    units = []
    for unit in pointwise_input["units"]:
        units.append(
            {
                "case_id": unit["case_id"],
                "witness_id": unit["witness_id"],
                "proposition_verdict": "supported",
                "proposition_evidence_spans": [unit["source_excerpt"][:20]],
                "proposition_rationale": "Supported by the cited source span.",
                "field_checklist": [
                    {
                        "field": field,
                        "decision": "correct",
                        "source_evidence_spans": [],
                        "rationale": "No independent error for this field.",
                    }
                    for field in CHECKLIST_FIELDS
                ],
            }
        )
    return {"units": units}


def test_v108_selection_targets_observed_errors_and_balanced_controls():
    predecessor = _validate_predecessors()
    selection = _select_case_ids(predecessor)
    truth = predecessor["values"]["v106_truth"]["cases"]

    assert len(selection["selected"]) == len(set(selection["selected"])) == 18
    assert len(selection["unsupported_residual"]) == 10
    assert len(selection["boundary"]) == 4
    assert len(selection["diverse"]) == 4
    assert len(selection["canary"]) == 12
    assert {truth[case_id]["shape"] for case_id in selection["diverse"]} == {
        "single_event_pairs",
        "multi_event_set_alignment",
        "one_sided_supported_residuals",
        "same_side_equivalence_partition",
    }


def test_v108_explicit_checklist_schema_and_projection_are_deterministic():
    predecessor = _validate_predecessors()
    selection = _select_case_ids(predecessor)
    case_ids = selection["selected"][:6]
    from research_factory.app_server_judge_v5_calibration import pointwise_input_subset

    pointwise_input = pointwise_input_subset(
        predecessor["values"]["v106_pointwise_input"], case_ids
    )
    schema = pointwise_checklist_schema(pointwise_input)
    output = _perfect_explicit(pointwise_input)

    assert schema["properties"]["units"]["items"]["properties"]["field_checklist"]["minItems"] == 15
    assert validate_pointwise_checklist_output(output, pointwise_input) == []
    projected = project_pointwise_checklist(output)
    assert all(row["structured_field_verdict"] == "correct" for row in projected["units"])
    assert all(row["field_issue_fields"] == [] for row in projected["units"])


def test_v108_validator_rejects_unsupported_inference_inversion():
    predecessor = _validate_predecessors()
    selection = _select_case_ids(predecessor)
    from research_factory.app_server_judge_v5_calibration import pointwise_input_subset

    pointwise_input = pointwise_input_subset(
        predecessor["values"]["v106_pointwise_input"], selection["selected"][:6]
    )
    output = _perfect_explicit(pointwise_input)
    target = next(
        item for item in output["units"][0]["field_checklist"]
        if item["field"] == "unsupported_inference"
    )
    target["decision"] = "incorrect"

    assert "unit_0_unsupported_inference_consistency" in validate_pointwise_checklist_output(output, pointwise_input)


def test_v108_protocol_instructions_bind_independent_fields_and_assignment():
    instructions = pointwise_checklist_instructions()

    assert "all 15 field checks independently" in instructions
    assert "Do not mark speaker merely because actor or attribution differs" in instructions
    assert "unsupported_inference is incorrect exactly when" in instructions


def test_v108_perfect_projection_clears_every_declared_gate():
    predecessor = _validate_predecessors()
    selection = _select_case_ids(predecessor)
    truth = deepcopy(predecessor["values"]["v106_truth"])
    truth["cases"] = {
        case_id: truth["cases"][case_id] for case_id in selection["selected"]
    }
    truth["canary_case_ids"] = selection["canary"]
    pointwise = {"units": []}
    alignment = {"cases": []}
    for case_id, case in truth["cases"].items():
        for witness_id, verdict in case["proposition"].items():
            pointwise["units"].append(
                {
                    "witness_id": witness_id,
                    "proposition_verdict": verdict,
                    "structured_field_verdict": case["structured_fields"][witness_id],
                    "field_issue_fields": case["field_issues"][witness_id],
                }
            )
        alignment["cases"].append(
            {
                "case_id": case_id,
                "status": "completed",
                "alignment_pairs": deepcopy(case["pairs"]),
                "equivalence_groups": deepcopy(case["equivalence_groups"]),
                "unpaired_witness_ids": deepcopy(case["unpaired_witness_ids"]),
            }
        )
    score = score_v108(
        pointwise_output=pointwise,
        reconciled_alignment=alignment,
        expected=truth,
        observable_disagreements={"disagreement_case_count": 0},
    )

    assert score["passed"] is True
    assert score["failed_checks"] == []
    assert all(score["checks"].values())


def test_v108_freeze_is_presemantic_sharded_and_downstream_closed(tmp_path: Path):
    root = tmp_path / "v108"
    first = freeze_v108(output_dir=root)
    second = freeze_v108(output_dir=root)

    assert first["spec"] == second["spec"]
    assert len(POINTWISE_TURNS) == 3
    assert len(BASE_TURNS) == 3
    assert len(CANARY_TURNS) == 2
    assert TURN_NAMES[-1] == ADJUDICATION_TURN
    assert len(TURN_NAMES) == 9
    assert first["spec"]["minimum_turn_count"] == 8
    assert first["spec"]["maximum_turn_count"] == 9
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["fresh_full_calibration_authorized"] is False
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert first["spec"]["production_mutation_allowed"] is False
    assert not list(root.glob("turns/*/capacity.json"))
    assert not (root / "terminal.json").exists()
