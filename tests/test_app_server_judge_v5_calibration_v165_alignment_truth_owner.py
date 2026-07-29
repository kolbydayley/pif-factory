from __future__ import annotations

from copy import deepcopy
from pathlib import Path

from research_factory import app_server_judge_v5_calibration_v164_equivalent_pair_verifier_diagnostic as v164
from research_factory.app_server_judge_v5_calibration_v165_alignment_truth_owner import (
    TURN_NAMES,
    _patch_projection,
    _validate_v164,
    build_v165_inputs,
    freeze_v165,
    score_v165,
)


def _owner_outputs(data: dict, *, target_equivalent: bool = True) -> dict:
    output = {
        row["owner_case_id"]: deepcopy(row["expected"])
        for row in data["rows"]
    }
    if target_equivalent:
        target = data["target_owner_case_id"]
        output[target] = _patch_projection(output[target], data["target_witness_ids"])
    return output


def test_v165_validates_immutable_v164_quality_terminal_and_usage():
    source = _validate_v164()
    assert source["values"]["terminal"]["state"] == "inactive"
    assert source["values"]["terminal"]["usage"]["total_tokens"] == 193876
    assert source["cumulative_usage"]["total_tokens"] == 3103996
    assert len(source["sidecars"]) == 6


def test_v165_has_one_disputed_target_and_five_balanced_controls():
    source = _validate_v164()
    data = build_v165_inputs(source)
    assert len(data["rows"]) == 6
    assert sum(row["role"] == "disputed_reference_target" for row in data["rows"]) == 1
    controls = [row for row in data["rows"] if row["role"] == "settled_control"]
    assert sum(row["expected"]["pairs"][0]["relation"] == "partial" for row in controls) == 2
    assert sum(row["expected"]["pairs"][0]["relation"] == "equivalent" for row in controls) == 3
    assert len(data["turns"]) == 2
    assert {row["case"]["case_id"] for row in data["rows"]} == {
        row["case_id"] for row in data["turns"][0]["value"]["cases"]
    } == {row["case_id"] for row in data["turns"][1]["value"]["cases"]}
    for turn in data["turns"]:
        assert "expected" not in str(turn["value"].keys())


def test_v165_equivalent_owner_with_exact_controls_repairs_all_v164_gates():
    source = _validate_v164()
    data = build_v165_inputs(source)
    output = _owner_outputs(data)
    score = score_v165(
        primary=output,
        canary=deepcopy(output),
        data=data,
        source=source,
    )
    assert score["passed"] is True
    assert score["failed_checks"] == []
    assert score["metrics"]["primary_control_exact_count"] == 5
    assert score["metrics"]["permutation_exact_count"] == 6
    assert score["metrics"]["retrospective_v164_metrics"]["equivalent_specificity"] == 1.0


def test_v165_retained_old_truth_or_control_error_fails_closed():
    source = _validate_v164()
    data = build_v165_inputs(source)
    retained = _owner_outputs(data, target_equivalent=False)
    score = score_v165(primary=retained, canary=deepcopy(retained), data=data, source=source)
    assert score["passed"] is False
    assert "target_reference_defect_confirmed" in score["failed_checks"]
    wrong = _owner_outputs(data)
    control = next(row for row in data["rows"] if row["role"] == "settled_control")
    wrong[control["owner_case_id"]]["pairs"][0]["relation"] = "abstain"
    score = score_v165(primary=wrong, canary=deepcopy(wrong), data=data, source=source)
    assert score["passed"] is False
    assert "primary_controls_exact" in score["failed_checks"]


def test_v165_permutation_drift_fails_closed():
    source = _validate_v164()
    data = build_v165_inputs(source)
    primary = _owner_outputs(data)
    canary = deepcopy(primary)
    target = data["target_owner_case_id"]
    canary[target] = deepcopy(next(row["expected"] for row in data["rows"] if row["owner_case_id"] == target))
    score = score_v165(primary=primary, canary=canary, data=data, source=source)
    assert score["passed"] is False
    assert "permutation_exact" in score["failed_checks"]
    assert "target_owner_consistent" in score["failed_checks"]


def test_v165_freeze_is_idempotent_bounded_and_presemantic(tmp_path: Path):
    root = tmp_path / "v165"
    first = freeze_v165(output_dir=root)
    second = freeze_v165(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["turn_plan"] == list(TURN_NAMES)
    assert first["spec"]["maximum_turn_count"] == 2
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()


def test_v165_freeze_does_not_mutate_v164(tmp_path: Path):
    paths = [v164.DEFAULT_OUTPUT_ROOT / "terminal.json", v164.DEFAULT_OUTPUT_ROOT / "equivalent-pair-verifier-diagnostic-score.json"]
    before = {str(path): path.read_bytes() for path in paths}
    freeze_v165(output_dir=tmp_path / "v165")
    after = {str(path): path.read_bytes() for path in paths}
    assert before == after
