from __future__ import annotations

from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v154_shared_context_field_diagnostic import (
    FIELD_NAMES,
    TURN_NAMES,
    _field_task_id,
    _validate_v153,
    build_v154_inputs,
    freeze_v154,
    score_v154,
    validate_field_output,
)


def _evidence(source: str) -> str:
    return source[: min(24, len(source))]


def test_v154_preserves_passing_v153_and_measured_usage():
    source = _validate_v153()
    assert source["usage"]["total_tokens"] == 35934
    assert source["values"]["score"]["passed"] is True
    assert source["values"]["terminal"]["production_mutated"] is False


def test_v154_selection_is_balanced_label_only_and_covers_all_fields():
    source = _validate_v153()
    rows, truth, selection = build_v154_inputs(source)

    assert len(rows) == 5
    assert selection["unit_count"] == 8
    assert selection["distinct_case_count"] == 8
    assert selection["support_status_counts"] == {"supported": 4, "unsupported": 4}
    assert selection["structured_status_counts"] == {"correct": 4, "incorrect": 4}
    assert selection["distinct_issue_field_count"] >= 5
    assert selection["field_names_per_unit"] == 15
    assert selection["selection_uses_source_text"] is False
    assert selection["prior_model_outputs_used_for_selection"] is False
    assert len(truth["units"]) == 8


def test_v154_synthetic_perfect_outputs_clear_every_gate():
    source = _validate_v153()
    rows, truth, _selection = build_v154_inputs(source)
    support_values = {row["turn_name"]: row["value"] for row in rows if row["turn_role"].startswith("support_")}
    field_rows = [row for row in rows if row["turn_role"] == "field_primary"]
    canary_row = next(row for row in rows if row["turn_role"] == "field_order_canary")
    expected = {row["witness_id"]: row for row in truth["units"]}

    def support_output(value):
        return {"units": [{"case_id": unit["case_id"], "witness_id": unit["witness_id"], "support_status": expected[unit["witness_id"]]["expected_support_status"], "source_evidence_spans": [_evidence(unit["source_excerpt"])], "rationale": "Synthetic exact fixture decision."} for unit in value["units"]]}

    def field_output(value):
        units = {(row["case_id"], row["witness_id"]): row for row in value["units"]}
        decisions = []
        for task in value["tasks"]:
            wanted = "incorrect" if task["field"] in expected[task["witness_id"]]["expected_field_issues"] else "correct"
            decisions.append({"task_id": task["task_id"], "field_status": wanted, "source_evidence_spans": [_evidence(units[(task["case_id"], task["witness_id"])]["source_excerpt"])], "rationale": "Synthetic exact fixture decision."})
        output = {"decisions": decisions}
        assert validate_field_output(output, value) == []
        return output

    support = support_output(support_values[TURN_NAMES[0]])
    support_canary = support_output(support_values[TURN_NAMES[1]])
    fields = {"decisions": [decision for row in field_rows for decision in field_output(row["value"])["decisions"]]}
    field_canary = field_output(canary_row["value"])
    score = score_v154(support=support, support_canary=support_canary, fields=fields, field_canary=field_canary, truth=truth)
    assert score["passed"] is True
    assert score["failed_checks"] == []
    assert score["metrics"]["field_decision_accuracy"] == 1.0
    assert score["metrics"]["structured_field_accuracy"] == 1.0


def test_v154_freeze_is_idempotent_five_turns_and_presemantic(tmp_path: Path):
    root = tmp_path / "v154"
    first = freeze_v154(output_dir=root)
    second = freeze_v154(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["turn_plan"] == list(TURN_NAMES)
    assert first["spec"]["support_model"] == "gpt-5.6-sol"
    assert first["spec"]["field_model"] == "gpt-5.5"
    assert first["spec"]["field_model_task_count"] == 8 * len(FIELD_NAMES)
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()


def test_v154_freeze_does_not_mutate_v153(tmp_path: Path):
    before = _validate_v153()["records"]
    freeze_v154(output_dir=tmp_path / "v154")
    after = _validate_v153()["records"]
    assert before == after
