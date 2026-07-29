from __future__ import annotations

import json
from pathlib import Path

from research_factory.app_server_judge_v5_selection_v194_metric_patch_repair import (
    TURN_NAME,
    _metric_repair_packets,
    _validate_v193_metric_failure,
    freeze_v194,
    validate_metric_patch_output,
)


def test_v194_targets_only_the_five_observable_metric_failures():
    predecessor = _validate_v193_metric_failure()
    packets = _metric_repair_packets(predecessor)
    assert len(packets) == 5
    assert len({row["repair_id"] for row in packets}) == 5
    assert all(row["evidence"] for row in packets)
    assert predecessor["gate"]["exactness_pruned_event_count"] == 0
    assert predecessor["gate"]["no_signal_false_positive_event_count"] == 0


def test_v194_patch_validator_is_literal_and_fail_closed():
    packets = _metric_repair_packets(_validate_v193_metric_failure())
    clear = {
        "patches": [
            {
                "repair_id": row["repair_id"],
                "action": "clear",
                "metric_value": "",
                "metric_unit": "",
                "metric_comparator": "",
                "metric_direction": "not_applicable",
                "metric_raw_text": "",
            }
            for row in packets
        ]
    }
    assert validate_metric_patch_output(clear, packets) == []
    malformed = json.loads(json.dumps(clear))
    malformed["patches"][0]["metric_unit"] = "unsupported-unit"
    assert validate_metric_patch_output(malformed, packets)


def test_v194_freeze_is_one_metric_only_turn_and_presemantic(tmp_path: Path):
    root = tmp_path / "v194"
    first = freeze_v194(output_dir=root)
    second = freeze_v194(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["turn_plan"] == [TURN_NAME]
    assert first["spec"]["repair_packet_count"] == 5
    assert first["spec"]["metric_fields_only"] is True
    assert first["spec"]["event_claim_or_evidence_changes_allowed"] is False
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["extraction_replay_allowed"] is False
    policy = json.loads(first["capacity_policy"].read_text())
    assert policy["phase_total_token_bound"] == 20000
    assert policy["projected_phase_quota_points"] == 1
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
