from __future__ import annotations

import json
from pathlib import Path

import pytest

from research_factory import (
    app_server_judge_v5_selection_v216_spark_support_canary as v216,
)
from research_factory.app_server_llm_judge import (
    validate_app_server_output_schema_subset,
)


@pytest.fixture(scope="module")
def frozen_v216(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, dict]:
    root = tmp_path_factory.mktemp("v216") / "attempt"
    frozen = v216.freeze_v216(output_dir=root)
    return root, frozen


def _all_supported_output(packet: dict) -> dict:
    return {
        "cases": [
            {
                "case_id": case["case_id"],
                "decisions": [
                    {"event_id": event["event_id"], "verdict": "s"}
                    for event in case["candidate_events"]
                ],
            }
            for case in packet["cases"]
        ]
    }


def test_v216_binds_zero_token_v215_nonacceptance():
    checkpoint = v216._validate_v215_checkpoint()
    assert checkpoint["terminal"]["state"] == "inactive"
    assert checkpoint["terminal"]["new_semantic_model_calls"] == 0
    assert checkpoint["terminal"]["usage"]["total_tokens"] == 0
    assert checkpoint["terminal"]["cumulative_known_usage_lower_bound"][
        "total_tokens"
    ] == 9_011_691
    assert checkpoint["terminal"]["holdout_authorized"] is False
    assert checkpoint["terminal"]["production_mutated"] is False


def test_v216_model_choice_uses_matched_measured_evidence():
    evidence = v216._validate_model_evidence()
    assert evidence["spark"]["model"] == "gpt-5.3-codex-spark"
    assert evidence["mini"]["model"] == "gpt-5.4-mini"
    assert evidence["spark"]["turn_count"] == 3
    assert evidence["spark"]["total_tokens"] == 100_319
    assert evidence["mini"]["total_tokens"] == 123_333
    assert evidence["spark"]["metrics"]["support_sensitivity"] == 0.942857
    assert evidence["spark"]["metrics"] == evidence["mini"]["metrics"] | {
        "pointwise_field_issue_f1": 0.666667
    }


def test_v216_dictionary_packet_is_lossless_and_complete():
    checkpoint = v216._validate_v215_checkpoint()
    request = v216._build_request(checkpoint["predecessor"])
    audit = request["projection"]
    assert audit["losslessly_reversible"] is True
    assert audit["complete_source_preserved"] is True
    assert audit["case_order_preserved"] is True
    assert audit["event_order_and_coverage_preserved"] is True
    assert audit["semantic_rewrite_performed"] is False
    assert audit["semantic_filtering_performed"] is False
    assert audit["semantic_deduplication_performed"] is False
    assert audit["case_count"] == 4
    assert audit["candidate_event_count"] == 79
    assert request["prompt_bytes"] <= v216.MAX_PROMPT_BYTES
    assert request["instruction_bytes"] <= v216.MAX_INSTRUCTIONS_BYTES
    assert request["schema_bytes"] <= v216.MAX_SCHEMA_BYTES


def test_v216_support_schema_and_projection_are_structural_only():
    checkpoint = v216._validate_v215_checkpoint()
    request = v216._build_request(checkpoint["predecessor"])
    output = _all_supported_output(request["packet"])
    assert validate_app_server_output_schema_subset(request["schema"]) == []
    assert v216.validate_support_output(
        output, request["packet"], request["schema"]
    ) == []
    projected = v216._project_support_output(output)
    assert all(
        decision["verdict"] == "keep"
        and decision["canonical_event_id"] == decision["event_id"]
        for case in projected["cases"]
        for decision in case["decisions"]
    )
    assert v216.v212.validate_selector_output(
        projected, request["scoring_predecessor"]
    ) == []


def test_v216_prompt_is_model_blind_and_support_only():
    checkpoint = v216._validate_v215_checkpoint()
    request = v216._build_request(checkpoint["predecessor"])
    rendered = json.dumps(request["packet"])
    normalized = " ".join(request["instructions"].split())
    assert "density_stratum" not in rendered
    assert "source_id" not in rendered
    assert "affordable_oracle_f1" not in rendered
    assert "package_id" not in request["prompt"]
    assert "Judge each event independently" in normalized
    assert "Do not compare, merge, or deduplicate" in normalized
    assert "complete source context" in normalized


def test_v216_freezes_exact_runtime_without_semantic_artifacts(frozen_v216):
    root, frozen = frozen_v216
    lock = v216.verify_runtime_lock(frozen["runtime_lock"])
    spec = json.loads((root / "attempt-spec.json").read_text())
    assert len(lock["runtime_files"]) == 22
    assert len(lock["v215_receipt"]) == 7
    assert len(lock["model_evidence"]) == 10
    assert spec["model"] == "gpt-5.3-codex-spark"
    assert spec["reasoning_effort"] == "low"
    assert spec["retry_count_per_turn"] == 0
    assert spec["maximum_total_tokens_per_turn"] == 38_000
    assert spec["promotion_total_token_gate"] == 35_000
    assert spec["semantic_deduplication_allowed"] is False
    assert spec["extraction_model_calls_authorized"] == 0
    assert spec["holdout_authorized"] is False
    assert not (root / "launch-receipt.json").exists()
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))


def test_v216_freeze_to_run_handoff_is_idempotent(frozen_v216):
    root, first = frozen_v216
    second = v216.freeze_v216(output_dir=root)
    assert second["runtime_lock"] == first["runtime_lock"]
    assert second["request"]["packet"] == first["request"]["packet"]
    assert not (root / "launch-receipt.json").exists()


def test_v216_runtime_lock_rejects_missing_runtime_file(frozen_v216):
    root, frozen = frozen_v216
    lock = json.loads(frozen["runtime_lock"].read_text())
    lock["runtime_files"] = lock["runtime_files"][1:]
    mutated = root / "runtime-lock-mutated.json"
    mutated.write_text(json.dumps(lock) + "\n", encoding="utf-8")
    with pytest.raises(v216.JudgeV5SelectionV216Error, match="runtime lock drifted"):
        v216.verify_runtime_lock(mutated)


def test_v216_started_attempt_cannot_be_silently_resumed(tmp_path: Path):
    root = tmp_path / "attempt"
    frozen = v216.freeze_v216(output_dir=root)
    receipt = v216._freeze_launch_receipt(frozen)
    assert receipt.is_file()
    with pytest.raises(
        v216.JudgeV5SelectionV216Error,
        match="cannot be silently resumed",
    ):
        v216.freeze_v216(output_dir=root)
    with pytest.raises(v216.JudgeV5SelectionV216Error, match="already exists"):
        v216._freeze_launch_receipt(frozen)
