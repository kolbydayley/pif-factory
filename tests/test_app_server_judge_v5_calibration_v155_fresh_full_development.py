from __future__ import annotations

from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v155_fresh_full_development import (
    FIELD_PRIMARY_TURNS,
    FIELD_REPEAT_TURNS,
    SUPPORT_CANARY_TURNS,
    SUPPORT_PRIMARY_TURNS,
    TURN_NAMES,
    _correct_support_only_projection,
    _field_task_id,
    _validate_v154,
    build_v155_inputs,
    freeze_v155,
    score_v155,
)


def test_v155_preserves_v154_quality_failure_and_complete_usage():
    source = _validate_v154()
    assert source["usage"]["total_tokens"] == 155609
    assert len(source["attempts"]) == 5
    assert source["values"]["score"]["passed"] is False
    assert source["values"]["terminal"]["production_mutated"] is False


def test_v155_inputs_cover_full_support_balanced_singletons_and_full_alignment():
    source = _validate_v154()
    data = build_v155_inputs(source)
    truth = data["truth"]
    roles = [row["turn_role"] for row in data["static_turns"]]
    field_counts = {"correct": 0, "incorrect": 0}
    for row in truth["field_tasks"]:
        field_counts[row["expected_status"]] += 1

    assert len(truth["support"]) == 182
    assert len(truth["alignment_cases"]) == 66
    assert len(truth["alignment_canary_case_ids"]) == 12
    assert len(truth["field_tasks"]) == 30
    assert {row["field"] for row in truth["field_tasks"]} == set(__import__("research_factory.app_server_judge_v5", fromlist=["CHECKLIST_FIELDS"]).CHECKLIST_FIELDS)
    assert field_counts == {"correct": 15, "incorrect": 15}
    assert roles.count("support_primary") == len(SUPPORT_PRIMARY_TURNS) == 11
    assert roles.count("support_canary") == len(SUPPORT_CANARY_TURNS) == 2
    assert roles.count("field_singleton") == len(FIELD_PRIMARY_TURNS) == 28
    assert roles.count("field_repeat") == len(FIELD_REPEAT_TURNS) == 4


def test_v155_corrected_projection_never_retains_hidden_unsupported_ids():
    source = _validate_v154()
    reference = source["v153"]["source"]["v151"]["source"]["source"]["v148"]["values"]["reference"]
    for case in reference["cases"].values():
        projection = _correct_support_only_projection(case)
        expected_ids = {witness_id for witness_id, verdict in case["proposition"].items() if verdict == "supported"}
        projected_ids = {witness_id for group in projection["equivalence_groups"] for witness_id in group}
        assert projected_ids == expected_ids
        assert set(projection["unpaired_witness_ids"]) <= expected_ids


def test_v155_synthetic_perfect_outputs_clear_every_frozen_gate():
    source = _validate_v154()
    truth = build_v155_inputs(source)["truth"]
    evidence = "fixture"
    support = {"units": [{"case_id": row["case_id"], "witness_id": row["witness_id"], "support_status": row["expected_status"], "source_evidence_spans": [evidence], "rationale": "Synthetic exact decision."} for row in truth["support"]]}
    support_map = {row["witness_id"]: row for row in support["units"]}
    support_canary = {"units": [dict(support_map[wid]) for wid in truth["support_canary_witness_ids"]]}
    decisions = []
    for row in truth["field_tasks"]:
        if row["field"] == "unsupported_inference":
            continue
        decisions.append({"task_id": row["task_id"], "field_status": row["expected_status"], "source_evidence_spans": [evidence], "rationale": "Synthetic exact decision."})
    observed = {row["task_id"]: row for row in decisions}
    repeats = {"decisions": [dict(observed[task_id]) for task_id in truth["field_repeat_task_ids"]]}
    alignment = {"cases": [{"case_id": row["case_id"], "alignment_pairs": row["expected"]["pairs"], "equivalence_groups": row["expected"]["equivalence_groups"], "unpaired_witness_ids": row["expected"]["unpaired_witness_ids"]} for row in truth["alignment_cases"]], "unresolved_cases_abstained": []}
    score = score_v155(support=support, support_canary=support_canary, fields={"decisions": decisions}, field_repeats=repeats, alignment=alignment, truth=truth, raw_alignment_disagreement_count=0, final_canary_exact_count=12)
    assert score["passed"] is True
    assert score["failed_checks"] == []
    assert score["metrics"]["support_sensitivity"] == 1.0
    assert score["metrics"]["structured_field_accuracy"] == 1.0
    assert score["metrics"]["alignment_f1"] == 1.0


def test_v155_freeze_is_idempotent_bounded_and_presemantic(tmp_path: Path):
    root = tmp_path / "v155"
    first = freeze_v155(output_dir=root)
    second = freeze_v155(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["turn_plan"] == list(TURN_NAMES)
    assert len(TURN_NAMES) == 59
    assert first["spec"]["minimum_turn_count"] == 58
    assert first["spec"]["maximum_turn_count"] == 59
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()


def test_v155_freeze_does_not_mutate_v154(tmp_path: Path):
    before = _validate_v154()["records"]
    freeze_v155(output_dir=tmp_path / "v155")
    after = _validate_v154()["records"]
    assert before == after
