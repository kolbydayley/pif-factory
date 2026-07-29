from __future__ import annotations

import json
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v120_retained_case_diagnostic import (
    _validate_v119,
    build_v120_selection,
    freeze_v120,
)


def test_v120_selects_18_balanced_retained_cases_with_zero_audited_overlap():
    v119 = _validate_v119()
    selection, expected = build_v120_selection(v119)
    assert selection["retained_candidate_count"] == 48
    assert selection["selected_case_count"] == 18
    assert selection["canary_case_count"] == 12
    assert selection["audited_v118_case_overlap_count"] == 0
    assert len(selection["selected"]) == len(set(selection["selected"])) == 18
    assert set(selection["canary"]).issubset(selection["selected"])
    assert len(selection["selected_shape_counts"]) == 5
    assert max(selection["selected_shape_counts"].values()) - min(
        selection["selected_shape_counts"].values()
    ) <= 1
    assert set(expected["cases"]) == set(selection["selected"])
    assert expected["canary_case_ids"] == selection["canary"]
    assert selection["selection_uses_source_text"] is False
    assert selection["model_outputs_used_for_selection"] is False


def test_v120_freeze_is_idempotent_presemantic_and_keeps_later_gates_closed(
    tmp_path: Path,
):
    root = tmp_path / "v120"
    first = freeze_v120(output_dir=root)
    second = freeze_v120(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["pointwise_model"] == "gpt-5.6-luna"
    assert first["spec"]["alignment_model"] == "gpt-5.6-terra"
    assert first["spec"]["adjudicator_model"] == "gpt-5.4"
    assert first["spec"]["case_count"] == 18
    assert first["spec"]["canary_case_count"] == 12
    assert first["spec"]["minimum_turn_count"] == 8
    assert first["spec"]["maximum_turn_count"] == 9
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["fresh_full_calibration_authorized"] is False
    assert first["spec"]["selection_authorized"] is False
    assert first["spec"]["holdout_authorized"] is False
    assert first["spec"]["production_mutation_allowed"] is False
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not (root / "terminal.json").exists()


def test_v120_capacity_policy_bounds_nine_turns_and_preserves_reserve(tmp_path: Path):
    frozen = freeze_v120(output_dir=tmp_path / "v120")
    policy = json.loads(Path(frozen["capacity_policy"]).read_text())
    assert len(policy["ordered_turn_names"]) == 9
    assert policy["phase_total_token_bound"] == 630000
    assert policy["minimum_remaining_reserve_percent"] == 20
    assert policy["retry_count_per_turn"] == 0


def test_v120_pointwise_shards_cover_every_selected_case_once(tmp_path: Path):
    frozen = freeze_v120(output_dir=tmp_path / "v120")
    covered = [case_id for shard in frozen["shards"] for case_id in shard]
    assert covered == frozen["selection"]["selected"]
    assert len(frozen["pointwise_shards"]) == 3
    assert all(len(shard["case_ids"]) == 6 for shard in frozen["pointwise_shards"])
