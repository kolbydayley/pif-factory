from __future__ import annotations

from copy import deepcopy
from pathlib import Path

from research_factory import app_server_judge_v5_calibration_v165_alignment_truth_owner as v165
from research_factory.app_server_judge_v5_calibration_v166_capped_owner_reconciliation import (
    TURN_NAME,
    _validate_v165,
    build_v166_input,
    freeze_v166,
    score_v166,
)


def test_v166_validates_immutable_v165_failure_and_usage():
    source = _validate_v165()
    terminal = source["values"]["terminal"]
    assert terminal["state"] == "inactive"
    assert terminal["usage"]["total_tokens"] == 82055
    assert source["cumulative_usage"]["total_tokens"] == 3186051
    assert len(source["sidecars"]) == 2


def test_v166_selects_only_two_observable_boundary_disagreements():
    source = _validate_v165()
    value = build_v166_input(source)
    assert value["packet"]["adjudication_required"] is True
    assert value["packet"]["call_cap"] == 1
    assert value["packet"]["anonymous_candidate_swap_count"] == 1
    assert len(value["disagreement_ids"]) == 2
    assert source["data"]["target_owner_case_id"] not in value["disagreement_ids"]
    assert all(
        value["expected"][key]["pairs"][0]["relation"] == "partial"
        for key in value["disagreement_ids"]
    )
    assert value["alignment_input"]["side_labels_present"] is False
    assert value["alignment_input"]["system_identity_present"] is False


def test_v166_exact_adjudication_repairs_v165_and_authorizes_reference():
    source = _validate_v165()
    value = build_v166_input(source)
    adjudicated = deepcopy(value["expected"])
    score = score_v166(adjudicated=adjudicated, source=source, value=value)
    assert score["passed"] is True
    assert score["failed_checks"] == []
    assert score["metrics"]["adjudicated_control_exact_count"] == 2
    repaired = score["metrics"]["repaired_v165_metrics"]
    assert repaired["primary_control_exact_count"] == 5
    assert repaired["canary_control_exact_count"] == 5
    assert repaired["permutation_exact_count"] == 6


def test_v166_wrong_or_abstaining_adjudication_fails_closed():
    source = _validate_v165()
    value = build_v166_input(source)
    adjudicated = deepcopy(value["expected"])
    key = value["disagreement_ids"][0]
    adjudicated[key]["pairs"][0]["relation"] = "abstain"
    score = score_v166(adjudicated=adjudicated, source=source, value=value)
    assert score["passed"] is False
    assert "adjudicated_controls_exact" in score["failed_checks"]
    assert "adjudication_no_abstention" in score["failed_checks"]


def test_v166_freeze_is_idempotent_bounded_and_presemantic(tmp_path: Path):
    root = tmp_path / "v166"
    first = freeze_v166(output_dir=root)
    second = freeze_v166(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["turn_plan"] == [TURN_NAME]
    assert first["spec"]["maximum_turn_count"] == 1
    assert first["spec"]["adjudication_call_cap"] == 1
    assert first["spec"]["majority_voting_used"] is False
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()


def test_v166_freeze_does_not_mutate_v165(tmp_path: Path):
    paths = [v165.DEFAULT_OUTPUT_ROOT / "terminal.json", v165.DEFAULT_OUTPUT_ROOT / "alignment-truth-owner-score.json"]
    before = {str(path): path.read_bytes() for path in paths}
    freeze_v166(output_dir=tmp_path / "v166")
    after = {str(path): path.read_bytes() for path in paths}
    assert before == after
