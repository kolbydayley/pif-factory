from __future__ import annotations

import json
from pathlib import Path

import pytest

from research_factory.app_server_llm_judge import (
    validate_app_server_output_schema_subset,
)
from research_factory.app_server_judge_v5_selection_v209_adaptive_router_schema_recovery import (
    EXPECTED_MESSAGE_SHA256,
    EXPECTED_UNSUPPORTED_SCHEMA_PATHS,
    JudgeV5SelectionV209Error,
    _validate_v208_schema_failure,
    freeze_v209,
    repair_schema_v209,
    validate_router_output_v209,
    verify_runtime_lock,
)


@pytest.fixture(scope="module")
def frozen_v209(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, dict]:
    root = tmp_path_factory.mktemp("v209") / "attempt"
    return root, freeze_v209(output_dir=root)


def test_v209_binds_exact_v208_unknown_usage_schema_failure():
    predecessor = _validate_v208_schema_failure()
    turn_error = predecessor["sidecar"]["turn_error"]
    assert predecessor["terminal"]["usage_status"] == "unknown"
    assert predecessor["terminal"]["cumulative_unknown_usage_turn_count"] == 3
    assert predecessor["failure"]["unknown_usage_turn_count"] == 1
    assert predecessor["capacity"]["cleared_for_semantic_turn"] is True
    assert predecessor["capacity"]["primary_used_percent"] == 8
    assert turn_error["message_sha256"] == EXPECTED_MESSAGE_SHA256
    assert validate_app_server_output_schema_subset(predecessor["schema"]) == (
        EXPECTED_UNSUPPORTED_SCHEMA_PATHS
    )


def test_v209_schema_repair_removes_only_unsupported_keywords():
    predecessor = _validate_v208_schema_failure()
    corrected = repair_schema_v209(predecessor["schema"])
    assert validate_app_server_output_schema_subset(corrected) == []
    assert "$schema" not in corrected
    selected = corrected["properties"]["cases"]["items"]["properties"][
        "selected_package_ids"
    ]
    assert "uniqueItems" not in selected
    original = json.loads(json.dumps(predecessor["schema"]))
    original.pop("$schema")
    original["properties"]["cases"]["items"]["properties"][
        "selected_package_ids"
    ].pop("uniqueItems")
    assert corrected == original


def test_v209_deterministic_validator_retains_uniqueness_contract():
    predecessor = _validate_v208_schema_failure()
    corrected = repair_schema_v209(predecessor["schema"])
    v207 = predecessor["predecessor"]
    package_id = sorted(v207["design"]["package_mapping_private"].values())[0]
    output = {
        "cases": [
            {
                "case_id": case_id,
                "base_coverage": "complete",
                "route_reason": "base_sufficient",
                "selected_package_ids": [package_id, package_id],
            }
            for case_id in v207["design"]["canary_case_ids"]
        ]
    }
    assert validate_router_output_v209(output, v207, corrected) == [
        "duplicate_package_id"
    ]


def test_v209_freezes_one_new_attempt_without_semantic_artifacts(frozen_v209):
    root, value = frozen_v209
    lock = verify_runtime_lock(value["runtime_lock"])
    spec = json.loads((root / "attempt-spec.json").read_text())
    audit = json.loads((root / "schema-recovery-audit.json").read_text())
    assert len(lock["runtime_files"]) == 15
    assert spec["recovery_scope"] == (
        "remove_unsupported_structured_output_schema_keywords_only"
    )
    assert spec["v208_turn_replayed"] is False
    assert spec["v208_unknown_usage_preserved"] is True
    assert spec["prompt_input_and_semantic_contract_unchanged"] is True
    assert spec["retry_count_per_turn"] == 0
    assert spec["extraction_model_calls_authorized"] == 0
    assert spec["holdout_authorized"] is False
    assert audit["unsupported_paths_after"] == []
    assert audit["semantic_contract_changed"] is False
    assert not (root / "launch-receipt.json").exists()
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))


def test_v209_runtime_lock_rejects_missing_runtime_file(frozen_v209):
    root, value = frozen_v209
    lock = json.loads(value["runtime_lock"].read_text())
    lock["runtime_files"] = lock["runtime_files"][1:]
    mutated = root / "runtime-lock-mutated.json"
    mutated.write_text(json.dumps(lock) + "\n", encoding="utf-8")
    with pytest.raises(JudgeV5SelectionV209Error, match="runtime lock drifted"):
        verify_runtime_lock(mutated)
