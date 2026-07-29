from __future__ import annotations

import json
from pathlib import Path

from research_factory.app_server_judge_v5_selection_v193_residual_repair_diagnostic import (
    TURN_NAME,
    _phase_one_gate,
    _validate_v192_authorization,
    freeze_v193,
    validate_repair_envelope,
)


def test_v193_validates_v192_one_turn_authorization():
    predecessor = _validate_v192_authorization()
    assert predecessor["terminal"]["semantic_attempt_authorized"] is True
    assert predecessor["design"]["declared_turn_count"] == 1
    assert predecessor["design"]["retry_count_per_turn"] == 0
    assert predecessor["design"]["repair_budget"]["declared_bound_fits_production_headroom"] is True


def test_v193_freeze_is_one_turn_and_presemantic(tmp_path: Path):
    root = tmp_path / "v193"
    first = freeze_v193(output_dir=root)
    second = freeze_v193(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["turn_plan"] == [TURN_NAME]
    assert first["spec"]["retry_count_per_turn"] == 0
    assert first["spec"]["extraction_replay_allowed"] is False
    assert first["spec"]["batch_5_replay_allowed"] is False
    policy = json.loads(first["capacity_policy"].read_text())
    assert policy["phase_total_token_bound"] == 100000
    assert policy["projected_phase_quota_points"] == 2
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))


def test_v193_envelope_and_token_gate_are_fail_closed():
    predecessor = _validate_v192_authorization()
    case_ids = [row["case_id"] for row in predecessor["private_input"]["cases"]]
    output = {
        "episode_id": "v193_residual_repair_diagnostic",
        "segments": [
            {
                "segment_id": case_id,
                "status": "no_signal",
                "segment_source_context": {
                    "kind": "substantive_dialogue",
                    "confidence": 1.0,
                    "rationale": "No retained event in this synthetic validator fixture.",
                },
                "no_signal_reason": "No retained event in this synthetic validator fixture.",
                "events": [],
            }
            for case_id in case_ids
        ],
    }
    assert validate_repair_envelope(output, predecessor["schema"]) == []
    diagnostics = [
        {
            "status_ok": True,
            "exactness_pruned_events": 0,
            "metric_grounding_error_events": 0,
            "event_cap_hit": False,
        }
        for _case_id in case_ids
    ]
    usage = {
        "input_tokens": 100001,
        "cached_input_tokens": 0,
        "output_tokens": 0,
        "reasoning_output_tokens": 0,
        "total_tokens": 100001,
    }
    gate = _phase_one_gate(
        normalized=output,
        diagnostics=diagnostics,
        usage=usage,
        predecessor=predecessor,
    )
    assert gate["passed"] is False
    assert gate["checks"]["declared_turn_token_bound"] is False
    assert gate["support_judge_authorized"] is False
