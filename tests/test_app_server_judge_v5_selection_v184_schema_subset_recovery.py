from __future__ import annotations

import json
from pathlib import Path

from research_factory.app_server_judge_v5_selection_v184_schema_subset_recovery import (
    _validate_v183_schema_failure,
    freeze_v184,
    repair_schema_v184,
)
from research_factory.app_server_llm_judge import validate_app_server_output_schema_subset


def test_v184_preserves_v183_as_unknown_usage_schema_failure():
    predecessor = _validate_v183_schema_failure()
    assert predecessor["values"]["terminal"]["state"] == "failed"
    assert predecessor["values"]["terminal"]["usage_status"] == "unknown"
    assert predecessor["predecessor_unknown_usage_turn_count"] == 1
    assert predecessor["predecessor_unknown_usage_upper_bound"] == 120000
    assert len(predecessor["unsupported_schema_paths"]) == 2


def test_v184_changes_only_unsupported_schema_keywords():
    predecessor = _validate_v183_schema_failure()
    corrected = repair_schema_v184(predecessor["value"])
    assert validate_app_server_output_schema_subset(corrected) == []
    rendered = json.dumps(corrected, sort_keys=True)
    assert '"uniqueItems"' not in rendered
    assert predecessor["prompt"] == predecessor["paths"]["prompt"].read_text()


def test_v184_freeze_is_new_one_turn_version_and_presemantic(tmp_path: Path):
    root = tmp_path / "v184"
    first = freeze_v184(output_dir=root)
    second = freeze_v184(output_dir=root)
    assert first["spec"] == second["spec"]
    assert first["spec"]["v183_attempt_replayed_in_place"] is False
    assert first["spec"]["v183_usage_status"] == "unknown"
    assert first["spec"]["v183_unknown_usage_turn_count"] == 1
    assert first["spec"]["schema_change_only"] is True
    assert first["spec"]["prompt_identical_to_v183"] is True
    assert first["spec"]["input_identical_to_v183"] is True
    assert first["spec"]["semantic_validator_identical_to_v183"] is True
    assert len(first["spec"]["removed_schema_keywords"]) == 2
    policy = json.loads(first["capacity_policy"].read_text())
    assert policy["phase_total_token_bound"] == 120000
    assert policy["projected_phase_quota_points"] == 3
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
    assert not (root / "terminal.json").exists()
