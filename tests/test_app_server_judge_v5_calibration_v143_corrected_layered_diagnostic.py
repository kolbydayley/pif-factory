from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

from research_factory.app_server_judge_v5 import CHECKLIST_FIELDS
from research_factory.app_server_judge_v5_calibration_v143_corrected_layered_diagnostic import (
    FIELD_TASK_COUNT,
    SUPPORT_COUNT,
    TURN_NAMES,
    _validate_source,
    _validate_v142,
    build_v143_inputs,
    freeze_v143,
    score_v143,
    support_output_schema,
    validate_support_output,
)


def _expected_outputs(truth):
    support = {
        "units": [
            {
                "case_id": row["case_id"],
                "witness_id": row["witness_id"],
                "support_status": row["expected_status"],
                "source_evidence_spans": ["evidence"],
                "rationale": "expected",
            }
            for row in truth["support"]
        ]
    }
    fields = {
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
    return support, deepcopy(support), fields, deepcopy(fields)


def test_v143_selects_balanced_fresh_support_and_every_field_polarity():
    support, support_canary, shards, truth, selection = build_v143_inputs(
        _validate_v142(), _validate_source()
    )

    assert selection["excluded_recent_case_count"] == 30
    assert selection["candidate_case_count"] == 36
    assert selection["support_unit_count"] == SUPPORT_COUNT == 12
    assert selection["support_status_counts"] == {"supported": 6, "unsupported": 6}
    assert selection["support_distinct_case_count"] == 12
    assert selection["field_task_count"] == FIELD_TASK_COUNT == 30
    assert selection["field_status_counts"] == {"correct": 15, "incorrect": 15}
    assert selection["field_enum_count"] == 15
    assert selection["selection_uses_source_text"] is False
    assert selection["model_outputs_used_for_selection"] is False
    assert selection["semantic_pruning_performed"] is False
    assert [row["witness_id"] for row in support_canary["units"]] == list(
        reversed([row["witness_id"] for row in support["units"]])
    )
    assert len(shards) == 6
    assert all(shard["task_count"] == 5 for shard in shards)
    status_by_field = {
        field: {
            row["expected_status"]
            for row in truth["field_tasks"]
            if row["field"] == field
        }
        for field in CHECKLIST_FIELDS
    }
    assert status_by_field == {
        field: {"correct", "incorrect"} for field in CHECKLIST_FIELDS
    }


def test_v143_support_schema_and_exact_span_validator():
    support, _, _, _, _ = build_v143_inputs(_validate_v142(), _validate_source())
    schema = support_output_schema(support)
    rows = []
    for unit in support["units"]:
        span = unit["source_excerpt"][: min(1000, len(unit["source_excerpt"]))]
        rows.append(
            {
                "case_id": unit["case_id"],
                "witness_id": unit["witness_id"],
                "support_status": "supported",
                "source_evidence_spans": [span],
                "rationale": "bounded",
            }
        )
    output = {"units": rows}

    assert schema["properties"]["units"]["minItems"] == 12
    assert validate_support_output(output, support) == []
    broken = deepcopy(output)
    broken["units"][0]["source_evidence_spans"] = ["not an exact source span"]
    assert "support_0_evidence_not_exact" in validate_support_output(broken, support)


def test_v143_score_requires_all_frozen_support_field_and_order_gates():
    _, _, _, truth, _ = build_v143_inputs(_validate_v142(), _validate_source())
    support, support_canary, fields, field_canary = _expected_outputs(truth)

    passed = score_v143(
        support=support,
        support_canary=support_canary,
        fields=fields,
        field_canary=field_canary,
        truth=truth,
    )
    assert passed["passed"] is True
    assert passed["corrected_alignment_diagnostic_authorized"] is True
    assert passed["metrics"]["support_sensitivity"] == 1.0
    assert passed["metrics"]["support_specificity"] == 1.0
    assert passed["metrics"]["field_decision_accuracy"] == 1.0
    assert passed["metrics"]["pointwise_field_issue_f1"] == 1.0
    assert passed["metrics"]["support_canary_exact_count"] == 12
    assert passed["metrics"]["field_canary_exact_count"] == 30

    broken = deepcopy(field_canary)
    broken["decisions"][0]["field_status"] = (
        "incorrect"
        if broken["decisions"][0]["field_status"] == "correct"
        else "correct"
    )
    failed = score_v143(
        support=support,
        support_canary=support_canary,
        fields=fields,
        field_canary=broken,
        truth=truth,
    )
    assert failed["passed"] is False
    assert failed["checks"]["field_order_canary_exact_rate"] is False


def test_v143_freeze_is_idempotent_presemantic_and_fourteen_turn_bounded(tmp_path: Path):
    root = tmp_path / "v143"
    first = freeze_v143(output_dir=root)
    second = freeze_v143(output_dir=root)

    assert first["spec"] == second["spec"]
    assert first["spec"]["support_model"] == "gpt-5.6-sol"
    assert first["spec"]["field_model"] == "gpt-5.5"
    assert first["spec"]["reasoning_effort"] == "high"
    assert first["spec"]["turn_plan"] == list(TURN_NAMES)
    assert len(TURN_NAMES) == 14
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["canary_marker_in_model_input"] is False
    assert first["spec"]["corrected_alignment_diagnostic_authorized"] is False
    assert first["spec"]["fresh_full_calibration_authorized"] is False
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert first["spec"]["production_mutation_allowed"] is False

    policy = json.loads(Path(first["capacity_policy"]).read_text())
    assert policy["phase_total_token_bound"] == 980000
    assert policy["minimum_remaining_reserve_percent"] == 20
    assert policy["retry_count_per_turn"] == 0
    frozen_turns = first["spec"]["frozen_inputs"]["turns"]
    assert len(frozen_turns) == 14
    for item in frozen_turns:
        prompt = Path(item["prompt"]["path"]).read_text()
        assert "expected_status" not in prompt
        assert "permutation_canary" not in prompt
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()


def test_v143_freeze_does_not_mutate_v142(tmp_path: Path):
    before = _validate_v142()["records"]
    freeze_v143(output_dir=tmp_path / "v143")
    after = _validate_v142()["records"]
    assert before == after
