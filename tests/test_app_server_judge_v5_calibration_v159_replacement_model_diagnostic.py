from __future__ import annotations

from copy import deepcopy
from pathlib import Path

from research_factory import app_server_judge_v5_calibration_v158_postprocess_quality_terminal as v158
from research_factory.app_server_judge_v5_calibration_v159_replacement_model_diagnostic import (
    TURN_NAMES,
    _validate_v158,
    build_v159_inputs,
    freeze_v159,
    score_v159,
)


def test_v159_preserves_v158_quality_terminal_and_contract():
    source = _validate_v158()
    assert source["values"]["terminal"]["state"] == "inactive"
    assert source["values"]["score"]["passed"] is False
    assert source["values"]["taxonomy"]["field_error_count"] == 6
    assert source["values"]["taxonomy"]["alignment_error_case_count"] == 6
    assert source["values"]["diagnostic"]["maximum_turn_count"] == 16
    assert source["cumulative_usage"]["total_tokens"] == 1506890


def test_v159_inputs_cover_residuals_controls_and_no_truth_labels():
    source = _validate_v158()
    data = build_v159_inputs(source)
    diagnostic = source["values"]["diagnostic"]
    assert len(data["field_turns"]) == 12
    assert len(data["alignment_turns"]) == 4
    assert set(data["selected_field_ids"]) == set(
        diagnostic["field_error_task_ids"] + diagnostic["field_control_task_ids"]
    )
    assert set(data["selected_case_ids"]) == set(
        diagnostic["alignment_error_case_ids"]
        + diagnostic["alignment_control_case_ids"]
    )
    for row in data["field_turns"] + data["alignment_turns"]:
        rendered = str(row["value"])
        assert "expected_status" not in rendered
        assert "truth" not in rendered.lower()


def test_v159_perfect_diagnostic_clears_every_gate():
    source = _validate_v158()
    data = build_v159_inputs(source)
    truth = data["truth"]
    field_truth = {row["task_id"]: row for row in truth["field_tasks"]}
    field_output = {
        "decisions": [
            {
                "task_id": task_id,
                "field_status": field_truth[task_id]["expected_status"],
                "source_evidence_spans": ["fixture"],
                "rationale": "fixture",
            }
            for task_id in data["selected_field_ids"]
        ]
    }

    expected = data["alignment_diagnostic_expected"]

    def raw_case(case_id: str) -> dict:
        value = expected[case_id]
        return {
            "case_id": case_id,
            "equivalence_groups": [
                {"witness_ids": group, "rationale": "fixture"}
                for group in value["equivalence_groups"]
            ],
            "alignment_pairs": [
                {
                    "witness_id_1": pair["witness_ids"][0],
                    "witness_id_2": pair["witness_ids"][1],
                    "relation": pair["relation"],
                    "checklist": [
                        {
                            "field": field,
                            "decision": "different"
                            if field in pair["mismatch_fields"]
                            else "same",
                            "source_evidence_spans": [],
                            "witness_evidence_ids": pair["witness_ids"],
                            "rationale": "fixture",
                        }
                        for field in v158.v155.CHECKLIST_FIELDS
                    ],
                    "rationale": "fixture",
                }
                for pair in value["pairs"]
            ],
            "unpaired_witness_ids": value["unpaired_witness_ids"],
        }

    alignment = {"cases": [raw_case(case_id) for case_id in data["selected_case_ids"]]}
    score = score_v159(
        field_output=field_output,
        alignment_primary=alignment,
        alignment_canary=deepcopy(alignment),
        data=data,
        source=source,
    )
    assert score["passed"] is True
    assert score["failed_checks"] == []


def test_v159_freeze_is_idempotent_bounded_and_presemantic(tmp_path: Path):
    root = tmp_path / "v159"
    first = freeze_v159(output_dir=root)
    second = freeze_v159(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["turn_plan"] == list(TURN_NAMES)
    assert len(TURN_NAMES) == 16
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["truth_labels_exposed_to_model"] is False
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()


def test_v159_freeze_does_not_mutate_v158(tmp_path: Path):
    root = v158.DEFAULT_OUTPUT_ROOT
    paths = [root / "terminal.json", root / "replacement-diagnostic-contract.json"]
    before = {str(path): path.read_bytes() for path in paths}
    freeze_v159(output_dir=tmp_path / "v159")
    after = {str(path): path.read_bytes() for path in paths}
    assert before == after
