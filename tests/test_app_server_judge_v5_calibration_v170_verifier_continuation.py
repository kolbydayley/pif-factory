from __future__ import annotations

import json
from pathlib import Path

from research_factory import app_server_judge_v5_calibration_v169_capacity_bound_continuation as v169
from research_factory.app_server_judge_v5_calibration_v170_verifier_continuation import (
    TURN_NAMES,
    _validate_v169_failure,
    freeze_v170,
)


def test_v170_adopts_complete_v169_alignment_prefix_without_replay():
    source = _validate_v169_failure()
    assert source["values"]["failure"]["error_class"] == "ReserveCapacityError"
    assert source["values"]["failure"]["failed_turn_name"] == v169.ALIGNMENT_CANARY_TURN
    assert source["values"]["failure"]["usage_status"] == "complete"
    assert source["usage"]["total_tokens"] == 352312
    assert source["cumulative_usage"]["total_tokens"] == 4513998
    assert len(source["base_pairs"]) == 56
    assert len(source["canary_pairs"]) == 10


def test_v170_freeze_is_idempotent_and_bounds_only_comparable_verifier_turns(tmp_path: Path):
    root = tmp_path / "v170"
    first = freeze_v170(output_dir=root)
    second = freeze_v170(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["turn_plan"] == list(TURN_NAMES)
    assert len(TURN_NAMES) == 15
    assert first["spec"]["minimum_turn_count"] == 15
    assert first["spec"]["maximum_turn_count"] == 15
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["v168_completed_prefix_turn_count"] == 40
    assert first["spec"]["v169_completed_prefix_turn_count"] == 9
    assert first["spec"]["predecessor_turns_replayed"] is False
    policy = json.loads(first["capacity_policy"].read_text())
    assert policy["maximum_total_tokens_per_turn"] == 40000
    assert policy["phase_total_token_bound"] == 600000
    assert policy["projected_phase_quota_points"] == 11
    assert policy["minimum_remaining_reserve_percent"] == 20
    audit = json.loads((root / "capacity-policy-audit.json").read_text())
    assert audit["measured_basis"]["v169_failed_canary_total_tokens"] == 50994
    assert audit["measured_basis"]["v169_failed_canary_is_not_in_remaining_workload"] is True
    assert audit["measured_basis"]["prior_comparable_sol_verifier_maximum_total_tokens"] == 33290
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()


def test_v170_freezes_every_equivalent_pair_once_and_keeps_downstream_closed(tmp_path: Path):
    frozen = freeze_v170(output_dir=tmp_path / "v170")
    selection = json.loads(
        (frozen["root"] / "equivalent-pair-verifier-selection-audit.json").read_text()
    )
    assert selection["base_primary_equivalent_pair_count"] == 56
    assert selection["canary_primary_equivalent_pair_count"] == 10
    assert sum(selection["base_cases_per_turn"]) == 56
    assert sum(selection["canary_cases_per_turn"]) == 10
    assert max(selection["base_cases_per_turn"] + selection["canary_cases_per_turn"]) == 5
    assert selection["truth_labels_used_for_selection"] is False
    assert frozen["spec"]["selection_authorized"] is False
    assert frozen["spec"]["holdout_authorized"] is False
    assert frozen["spec"]["production_mutation_allowed"] is False


def test_v170_freeze_does_not_mutate_v169(tmp_path: Path):
    paths = [
        v169.DEFAULT_OUTPUT_ROOT / "terminal.json",
        v169.DEFAULT_OUTPUT_ROOT / "failure.json",
        v169.DEFAULT_OUTPUT_ROOT / "turns/continuation-alignment-canary/sidecar.json",
        v169.DEFAULT_OUTPUT_ROOT / "turns/continuation-alignment-canary/output.private.json",
    ]
    before = {str(path): path.read_bytes() for path in paths}
    freeze_v170(output_dir=tmp_path / "v170")
    after = {str(path): path.read_bytes() for path in paths}
    assert before == after
