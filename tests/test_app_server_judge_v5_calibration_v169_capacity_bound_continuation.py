from __future__ import annotations

import json
from pathlib import Path

from research_factory import app_server_judge_v5_calibration_v168_capacity_recovery as v168
from research_factory.app_server_judge_v5_calibration_v169_capacity_bound_continuation import (
    TURN_NAMES,
    _validate_v168_failure,
    freeze_v169,
)


def test_v169_adopts_complete_v168_prefix_without_replay():
    source = _validate_v168_failure()
    assert source["values"]["failure"]["error_class"] == "ReserveCapacityError"
    assert source["values"]["failure"]["failed_turn_name"] == "recovery_alignment_shard_02"
    assert source["values"]["failure"]["usage_status"] == "complete"
    assert source["usage"]["total_tokens"] == 945212
    assert source["cumulative_usage"]["total_tokens"] == 4161686
    assert len(source["alignment_prefix"]) == 3


def test_v169_freeze_is_idempotent_and_has_evidenced_capacity_bound(tmp_path: Path):
    root = tmp_path / "v169"
    first = freeze_v169(output_dir=root)
    second = freeze_v169(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["turn_plan"] == list(TURN_NAMES)
    assert len(TURN_NAMES) == 24
    assert first["spec"]["minimum_turn_count"] == 24
    assert first["spec"]["maximum_turn_count"] == 24
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["v168_completed_prefix_turn_count"] == 40
    assert first["spec"]["v168_completed_prefix_outputs_reused"] is True
    assert first["spec"]["v168_completed_turns_replayed"] is False
    policy = json.loads(first["capacity_policy"].read_text())
    assert policy["maximum_total_tokens_per_turn"] == 42000
    assert policy["phase_total_token_bound"] == 1008000
    assert policy["projected_phase_quota_points"] == 18
    assert policy["minimum_remaining_reserve_percent"] == 20
    audit = json.loads((root / "capacity-policy-audit.json").read_text())
    assert audit["measured_basis"]["v168_measured_maximum_total_tokens"] == 39678
    assert audit["measured_basis"]["v168_overrun_tokens"] == 178
    assert audit["continuation_rule"]["selection_based_on_output_content"] is False
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()


def test_v169_freeze_excludes_every_completed_v168_turn(tmp_path: Path):
    frozen = freeze_v169(output_dir=tmp_path / "v169")
    planned = set(frozen["spec"]["turn_plan"])
    completed = set(
        v168.SUPPORT_PRIMARY_TURNS
        + (v168.SUPPORT_CANARY_TURN,)
        + v168.FIELD_PRIMARY_TURNS
        + (v168.FIELD_REPEAT_TURN,)
        + v168.ALIGNMENT_PRIMARY_TURNS[:3]
    )
    assert planned.isdisjoint(completed)
    assert [row["shard_index"] for row in frozen["turns"]] == list(range(3, 11))
    assert frozen["spec"]["prior_output_reuse_is_complete_prefix_and_content_blind"] is True
    assert frozen["spec"]["selection_authorized"] is False
    assert frozen["spec"]["holdout_authorized"] is False
    assert frozen["spec"]["production_mutation_allowed"] is False


def test_v169_freeze_does_not_mutate_v168(tmp_path: Path):
    paths = [
        v168.DEFAULT_OUTPUT_ROOT / "terminal.json",
        v168.DEFAULT_OUTPUT_ROOT / "failure.json",
        v168.DEFAULT_OUTPUT_ROOT / "turns/recovery-alignment-shard-02/sidecar.json",
        v168.DEFAULT_OUTPUT_ROOT / "turns/recovery-alignment-shard-02/output.private.json",
    ]
    before = {str(path): path.read_bytes() for path in paths}
    freeze_v169(output_dir=tmp_path / "v169")
    after = {str(path): path.read_bytes() for path in paths}
    assert before == after
