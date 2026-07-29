from __future__ import annotations

from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v152_support_only_truth_repair import (
    _validate_v151,
    build_v152_plan,
    repair_support_only_truth,
    run_v152,
)


def test_v152_preserves_complete_v151_attempt_and_usage():
    source = _validate_v151()

    assert source["usage"]["total_tokens"] == 75098
    assert len(source["attempts"]) == 2
    assert source["values"]["terminal"]["production_mutated"] is False
    assert source["values"]["terminal"]["fresh_full_development_calibration_authorized"] is False


def test_v152_truth_patch_removes_only_three_hidden_unsupported_witnesses():
    source = _validate_v151()
    truth, audit = repair_support_only_truth(source)
    reference = source["source"]["source"]["v148"]["values"]["reference"]

    assert audit["patched_case_count"] == 3
    assert audit["removed_hidden_group_id_count"] == 3
    assert audit["removed_hidden_unpaired_id_count"] == 3
    assert audit["semantic_model_decisions_changed"] is False
    assert truth["unpaired_case_count"] == 3
    for row in truth["cases"]:
        supported = {
            witness_id
            for witness_id, verdict in reference["cases"][row["case_id"]]["proposition"].items()
            if verdict == "supported"
        }
        expected_ids = {
            witness_id
            for group in row["expected"]["equivalence_groups"]
            for witness_id in group
        }
        assert expected_ids == supported
        assert set(row["expected"]["unpaired_witness_ids"]) <= supported


def test_v152_plan_is_one_capped_side_free_call_for_four_observable_disagreements():
    source = _validate_v151()
    truth, _audit = repair_support_only_truth(source)
    plan = build_v152_plan(
        primary=source["values"]["primary"],
        canary=source["values"]["canary"],
        truth=truth,
    )

    assert plan["observable_disagreement_case_count"] == 4
    assert plan["relation_bucket_counts"] == {"non_equivalent": 2, "partial": 2}
    assert plan["shape_counts"] == {
        "merged_and_split_boundaries": 2,
        "single_event_pairs": 2,
    }
    assert plan["call_cap"] == 1
    assert plan["retry_count_per_turn"] == 0
    assert plan["majority_voting_used"] is False


def test_v152_is_idempotent_zero_token_and_keeps_downstream_closed(tmp_path: Path):
    root = tmp_path / "v152"
    first = run_v152(output_dir=root)
    second = run_v152(output_dir=root)

    assert first == second
    assert first["state"] == "inactive"
    assert first["terminal_reason"] == "inactive_incomplete_recovery_required"
    assert first["reference_truth_repaired"] is True
    assert first["capped_side_free_alignment_adjudication_authorized"] is True
    assert first["semantic_attempt_started"] is False
    assert first["usage_status"] == "complete"
    assert first["usage"]["total_tokens"] == 0
    assert first["selection_authorized"] is False
    assert first["holdout_authorized"] is False
    assert first["production_mutated"] is False


def test_v152_does_not_mutate_v151(tmp_path: Path):
    before = _validate_v151()["records"]
    run_v152(output_dir=tmp_path / "v152")
    after = _validate_v151()["records"]
    assert before == after
