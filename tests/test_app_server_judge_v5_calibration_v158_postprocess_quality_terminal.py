from __future__ import annotations

from pathlib import Path

from research_factory import app_server_judge_v5_calibration_v157_exact_span_canary_recovery as v157
from research_factory.app_server_judge_v5_calibration_v158_postprocess_quality_terminal import (
    _validate_v157_failure,
    build_diagnostic_contract,
    freeze_v158,
    score_and_taxonomy,
)


def test_v158_preserves_v157_complete_usage_and_keyerror_signature():
    source = _validate_v157_failure()
    assert source["usage"]["total_tokens"] == 71381
    assert source["cumulative_usage"]["total_tokens"] == 1506890
    assert len(source["attempts"]) == 2
    assert len(source["values"]["disagreements"]["cases"]) == 2


def test_v158_scores_truthfully_and_quantifies_minimum_corrections():
    source = _validate_v157_failure()
    score, taxonomy = score_and_taxonomy(source)
    assert score["passed"] is False
    assert score["failed_checks"] == [
        "equivalence_partition_exact_case_rate",
        "equivalent_specificity",
        "field_diagnostic_f1",
        "mismatch_field_f1",
        "structured_field_accuracy",
    ]
    assert taxonomy["field_error_count"] == 6
    assert taxonomy["minimum_field_decision_corrections_to_clear_all_field_gates"] == 5
    assert taxonomy["alignment_error_case_count"] == 6
    assert taxonomy[
        "minimum_alignment_case_corrections_to_clear_all_alignment_gates"
    ] == 2


def test_v158_diagnostic_is_blinded_bounded_and_balanced():
    source = _validate_v157_failure()
    _score, taxonomy = score_and_taxonomy(source)
    diagnostic = build_diagnostic_contract(source, taxonomy)
    assert len(diagnostic["field_error_task_ids"]) == 6
    assert len(diagnostic["field_control_task_ids"]) == 6
    assert not set(diagnostic["field_error_task_ids"]) & set(
        diagnostic["field_control_task_ids"]
    )
    assert len(diagnostic["alignment_error_case_ids"]) == 6
    assert len(diagnostic["alignment_control_case_ids"]) == 6
    assert not set(diagnostic["alignment_error_case_ids"]) & set(
        diagnostic["alignment_control_case_ids"]
    )
    assert diagnostic["maximum_turn_count"] == 16
    assert diagnostic["maximum_total_token_bound"] == 720000
    assert diagnostic["truth_labels_exposed_to_model"] is False
    assert diagnostic["selection_authorized"] is False
    assert diagnostic["holdout_authorized"] is False


def test_v158_freeze_is_zero_token_idempotent_quality_terminal(tmp_path: Path):
    root = tmp_path / "v158"
    first = freeze_v158(output_dir=root)
    second = freeze_v158(output_dir=root)
    assert first["terminal"] == second["terminal"]
    terminal = first["terminal"]
    assert terminal["state"] == "inactive"
    assert terminal["terminal_reason"] == "inactive_incomplete_recovery_required"
    assert terminal["development_judge_frozen"] is False
    assert terminal["bounded_replacement_model_diagnostic_authorized"] is True
    assert terminal["selection_authorized"] is False
    assert terminal["holdout_authorized"] is False
    assert terminal["semantic_attempt_started"] is False
    assert terminal["usage"]["total_tokens"] == 0
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))


def test_v158_freeze_does_not_mutate_v157(tmp_path: Path):
    root = v157.DEFAULT_OUTPUT_ROOT
    paths = [root / "terminal.json", root / "failure.json"]
    before = {str(path): path.read_bytes() for path in paths}
    freeze_v158(output_dir=tmp_path / "v158")
    after = {str(path): path.read_bytes() for path in paths}
    assert before == after
