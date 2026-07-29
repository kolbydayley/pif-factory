from __future__ import annotations

import json
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v110_sol_reference_audit import (
    ALIGNMENT_TURNS,
    POINTWISE_TURNS,
    TURN_NAMES,
    _select_alignment_audit_ids,
    _validate_predecessor,
    freeze_v110,
)


def test_v110_selects_all_four_alignment_disagreements_and_eight_controls():
    predecessor = _validate_predecessor()
    selection = _select_alignment_audit_ids(predecessor)

    assert len(selection["candidates"]) == 4
    assert len(selection["controls"]) == 8
    assert len(selection["audit"]) == len(set(selection["audit"])) == 12


def test_v110_freeze_is_side_free_idempotent_and_presemantic(tmp_path: Path):
    root = tmp_path / "v110"
    first = freeze_v110(output_dir=root)
    second = freeze_v110(output_dir=root)

    assert first["spec"] == second["spec"]
    assert len(POINTWISE_TURNS) == 3
    assert len(ALIGNMENT_TURNS) == 2
    assert len(TURN_NAMES) == 5
    assert first["spec"]["model"] == "gpt-5.6-sol"
    assert first["spec"]["owner_is_side_free"] is True
    assert first["spec"]["gpt55_outputs_exposed_to_owner"] is False
    assert first["spec"]["fixture_truth_exposed_to_owner"] is False
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["reference_patch_authorized"] is False
    assert first["spec"]["fresh_full_calibration_authorized"] is False
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert first["spec"]["production_mutation_allowed"] is False
    assert not list(root.glob("turns/*/capacity.json"))
    assert not list(root.glob("turns/*/sidecar.json"))
    assert not (root / "terminal.json").exists()


def test_v110_capacity_policy_preserves_reserve_and_five_turn_bound(tmp_path: Path):
    root = tmp_path / "v110"
    frozen = freeze_v110(output_dir=root)
    policy = json.loads(Path(frozen["capacity_policy"]).read_text())

    assert policy["minimum_remaining_reserve_percent"] == 20
    assert policy["retry_count_per_turn"] == 0
    assert policy["phase_total_token_bound"] == 350000
    assert policy["projected_phase_quota_points"] == 6
    assert policy["managed_chatgpt_auth_only"] is True
    assert policy["official_persistent_codex_app_server_only"] is True
