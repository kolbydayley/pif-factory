from __future__ import annotations

import json
from pathlib import Path

import pytest

from research_factory import (
    app_server_judge_v5_selection_v214_truth_conditional_selector as v214,
)
from research_factory.app_server_llm_judge import (
    validate_app_server_output_schema_subset,
)


@pytest.fixture(scope="module")
def frozen_v214(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, dict]:
    root = tmp_path_factory.mktemp("v214") / "attempt"
    frozen = v214.freeze_v214(output_dir=root)
    return root, frozen


def test_v214_binds_measured_v213_failure_without_replay():
    checkpoint = v214._validate_v213_checkpoint()
    assert checkpoint["terminal"]["state"] == "inactive"
    assert checkpoint["terminal"]["usage_status"] == "complete"
    assert checkpoint["terminal"]["usage"]["total_tokens"] == 40_773
    assert checkpoint["sidecar"]["auth_type"] == "chatgpt"
    assert checkpoint["sidecar"]["usage_complete"] is True
    assert checkpoint["capacity"]["cleared_for_semantic_turn"] is True
    decisions = [
        decision
        for case in checkpoint["output"]["cases"]
        for decision in case["decisions"]
    ]
    assert len(decisions) == 79
    assert {row["verdict"] for row in decisions} == {"unsupported"}
    assert checkpoint["gate"]["maximum_selected_event_count"] == 0
    assert checkpoint["terminal"]["holdout_authorized"] is False
    assert checkpoint["terminal"]["production_mutated"] is False


def test_v214_projection_preserves_semantic_coverage_and_removes_only_frozen_fields():
    checkpoint = v214._validate_v213_checkpoint()
    request = v214._build_request(checkpoint["predecessor"])
    packet = request["packet"]
    original = checkpoint["predecessor"]["input"]
    assert set(packet["event_field_legend"].values()) == set(
        v214.TRUTH_CONDITIONAL_FIELDS
    )
    assert set(original["event_field_legend"].values()) == set(
        v214.TRUTH_CONDITIONAL_FIELDS
    ) | set(v214.OMITTED_NONSELECTION_FIELDS)
    assert len(packet["cases"]) == 4
    assert sum(len(row["candidate_events"]) for row in packet["cases"]) == 79
    assert request["projection"]["complete_source_preserved"] is True
    assert request["projection"]["case_order_preserved"] is True
    assert request["projection"]["event_order_and_coverage_preserved"] is True
    assert request["projection"]["semantic_rewrite_performed"] is False
    assert request["projection"]["semantic_filtering_performed"] is False
    assert request["prompt_bytes"] <= v214.MAX_PROMPT_BYTES
    assert request["instruction_bytes"] <= v214.MAX_INSTRUCTIONS_BYTES
    assert request["schema_bytes"] <= v214.MAX_SCHEMA_BYTES


def test_v214_prompt_has_explicit_truth_conditional_contract():
    instructions = v214.selector_instructions()
    normalized = " ".join(instructions.split())
    assert "complete source text" in normalized
    assert "every listed field and no omitted" in normalized
    assert "classification labels" in normalized
    assert "Supported paraphrase and coreference pass" in normalized
    assert "Omitted metadata is retained unchanged" in normalized
    assert "Do not rewrite, add, merge, or infer" in normalized


def test_v214_schema_is_supported_and_model_blind():
    checkpoint = v214._validate_v213_checkpoint()
    request = v214._build_request(checkpoint["predecessor"])
    schema = request["schema"]
    rendered = json.dumps(request["packet"])
    assert validate_app_server_output_schema_subset(schema) == []
    assert "$schema" not in schema
    assert "uniqueItems" not in json.dumps(schema)
    assert "density_stratum" not in rendered
    assert "source_id" not in rendered
    assert "affordable_oracle_f1" not in rendered
    assert "package_id" not in request["prompt"]


def test_v214_freezes_exact_runtime_without_semantic_artifacts(frozen_v214):
    root, frozen = frozen_v214
    lock = v214.verify_runtime_lock(frozen["runtime_lock"])
    spec = json.loads((root / "attempt-spec.json").read_text())
    defect = json.loads((root / "materiality-defect-audit.json").read_text())
    assert len(lock["runtime_files"]) == 20
    assert len(lock["v213_attempt"]) == 15
    assert len(lock["v211_semantic_sources"]) == 9
    assert spec["turn_plan"] == [v214.TURN_NAME]
    assert spec["retry_count_per_turn"] == 0
    assert spec["maximum_total_tokens_per_turn"] == 40_000
    assert spec["promotion_total_token_gate"] == 35_000
    assert spec["extraction_model_calls_authorized"] == 0
    assert spec["extraction_replay_allowed"] is False
    assert spec["holdout_authorized"] is False
    assert defect["candidate_event_count"] == 79
    assert defect["verdict_counts"] == {"unsupported": 79}
    assert defect["semantic_retry_of_v213"] is False
    assert not (root / "launch-receipt.json").exists()
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))


def test_v214_freeze_to_run_handoff_is_idempotent(frozen_v214):
    root, first = frozen_v214
    second = v214.freeze_v214(output_dir=root)
    assert second["runtime_lock"] == first["runtime_lock"]
    assert second["request"]["packet"] == first["request"]["packet"]
    assert not (root / "launch-receipt.json").exists()


def test_v214_runtime_lock_rejects_missing_runtime_file(frozen_v214):
    root, frozen = frozen_v214
    lock = json.loads(frozen["runtime_lock"].read_text())
    lock["runtime_files"] = lock["runtime_files"][1:]
    mutated = root / "runtime-lock-mutated.json"
    mutated.write_text(json.dumps(lock) + "\n", encoding="utf-8")
    with pytest.raises(v214.JudgeV5SelectionV214Error, match="runtime lock drifted"):
        v214.verify_runtime_lock(mutated)


def test_v214_started_attempt_cannot_be_silently_resumed(tmp_path: Path):
    root = tmp_path / "attempt"
    frozen = v214.freeze_v214(output_dir=root)
    receipt = v214._freeze_launch_receipt(frozen)
    assert receipt.is_file()
    with pytest.raises(
        v214.JudgeV5SelectionV214Error,
        match="cannot be silently resumed",
    ):
        v214.freeze_v214(output_dir=root)
    with pytest.raises(v214.JudgeV5SelectionV214Error, match="already exists"):
        v214._freeze_launch_receipt(frozen)
