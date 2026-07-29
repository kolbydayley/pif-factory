from __future__ import annotations

from copy import deepcopy
from pathlib import Path

from research_factory import app_server_judge_v5_calibration_v163_fresh_corrected_field_diagnostic as v163
from research_factory.app_server_judge_v5_calibration_v164_equivalent_pair_verifier_diagnostic import (
    TURN_NAMES,
    _validate_v163,
    build_v164_inputs,
    freeze_v164,
    score_v164,
)


def _perfect(data: dict) -> dict:
    return {row["pair_case_id"]: deepcopy(row["expected"]) for row in data["pairs"]}


def test_v164_preserves_frozen_v163_field_protocol_and_usage():
    source = _validate_v163()
    assert source["values"]["terminal"]["field_protocol_frozen"] is True
    assert source["values"]["terminal"]["usage"]["total_tokens"] == 233682
    assert source["values"]["terminal"]["cumulative_calibration_usage"]["total_tokens"] == 2910120
    assert len(source["sidecars"]) == 11


def test_v164_verifies_all_and_only_primary_equivalent_pairs():
    source = _validate_v163()
    data = build_v164_inputs(source)
    assert len(data["pairs"]) == 15
    assert sum(row["expected"]["pairs"][0]["relation"] == "equivalent" for row in data["pairs"]) == 14
    assert sum(row["expected"]["pairs"][0]["relation"] == "non_equivalent" for row in data["pairs"]) == 1
    assert len(data["turns"]) == 6
    assert all(len(row["value"]["cases"]) == 5 for row in data["turns"])


def test_v164_pair_inputs_are_blinded_and_canary_membership_matches():
    source = _validate_v163()
    data = build_v164_inputs(source)
    primary = {case for row in data["turns"] if row["turn_role"] == "pair_primary" for case in row["pair_case_ids"]}
    canary = {case for row in data["turns"] if row["turn_role"] == "pair_canary" for case in row["pair_case_ids"]}
    assert primary == canary

    def keys(value):
        if isinstance(value, dict):
            return set(value) | set().union(*(keys(item) for item in value.values()))
        if isinstance(value, list):
            return set().union(*(keys(item) for item in value))
        return set()

    for row in data["turns"]:
        assert not ({"expected", "expected_relation", "truth", "reference_label"} & keys(row["value"]))


def test_v164_perfect_verifier_clears_every_gate():
    source = _validate_v163()
    data = build_v164_inputs(source)
    perfect = _perfect(data)
    score = score_v164(primary_output=perfect, canary_output=deepcopy(perfect), data=data, source=source)
    assert score["passed"] is True
    assert score["failed_checks"] == []
    assert score["metrics"]["false_equivalent_pair_exact_count"] == 1
    assert score["metrics"]["true_equivalent_pair_exact_count"] == 14
    assert score["fresh_full_replacement_calibration_authorized"] is True


def test_v164_missing_false_equivalence_or_canary_drift_fails_closed():
    source = _validate_v163()
    data = build_v164_inputs(source)
    perfect = _perfect(data)
    false_id = next(key for key, value in perfect.items() if value["pairs"][0]["relation"] == "non_equivalent")
    wrong = deepcopy(perfect)
    wrong[false_id]["pairs"][0]["relation"] = "equivalent"
    wrong[false_id]["pairs"][0]["mismatch_fields"] = []
    wrong[false_id]["equivalence_groups"] = [wrong[false_id]["pairs"][0]["witness_ids"]]
    score = score_v164(primary_output=wrong, canary_output=deepcopy(wrong), data=data, source=source)
    assert score["passed"] is False
    assert "false_equivalent_detection_rate" in score["failed_checks"]
    score = score_v164(primary_output=perfect, canary_output=wrong, data=data, source=source)
    assert score["passed"] is False
    assert "permutation_exact_rate" in score["failed_checks"]


def test_v164_freeze_is_idempotent_bounded_and_presemantic(tmp_path: Path):
    root = tmp_path / "v164"
    first = freeze_v164(output_dir=root)
    second = freeze_v164(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["turn_plan"] == list(TURN_NAMES)
    assert first["spec"]["maximum_turn_count"] == 6
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()


def test_v164_freeze_does_not_mutate_v163(tmp_path: Path):
    root = v163.DEFAULT_OUTPUT_ROOT
    paths = [root / "terminal.json", root / "field-protocol-v163.json"]
    before = {str(path): path.read_bytes() for path in paths}
    freeze_v164(output_dir=tmp_path / "v164")
    after = {str(path): path.read_bytes() for path in paths}
    assert before == after
