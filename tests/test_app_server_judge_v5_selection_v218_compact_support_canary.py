from __future__ import annotations

import json
from pathlib import Path

import pytest

from research_factory import (
    app_server_judge_v5_selection_v218_compact_support_canary as v218,
)
from research_factory.app_server_llm_judge import (
    validate_app_server_output_schema_subset,
)


@pytest.fixture(scope="module")
def frozen_v218(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, dict]:
    root = tmp_path_factory.mktemp("v218") / "attempt"
    frozen = v218.freeze_v218(output_dir=root)
    return root, frozen


def _all_supported_output(packet: dict) -> dict:
    return {"v": [["s"] * len(case[1]) for case in packet["c"]]}


def test_v218_binds_v217_quality_pass_cost_failure():
    checkpoint = v218._validate_v217_checkpoint()
    assert checkpoint["terminal"]["state"] == "inactive"
    assert checkpoint["terminal"]["semantic_quality_gate_passed"] is True
    assert checkpoint["terminal"]["selector_cost_gate_passed"] is False
    assert checkpoint["terminal"]["new_semantic_model_calls"] == 0
    assert checkpoint["terminal"]["cumulative_known_usage_lower_bound"][
        "total_tokens"
    ] == 9_053_190
    assert checkpoint["terminal"]["holdout_authorized"] is False
    assert checkpoint["terminal"]["production_mutated"] is False


def test_v218_vector_packet_is_lossless_and_smaller():
    checkpoint = v218._validate_v217_checkpoint()
    request = v218._build_request(checkpoint)
    audit = request["projection"]
    assert audit["losslessly_reversible"] is True
    assert audit["complete_source_preserved"] is True
    assert audit["case_order_preserved"] is True
    assert audit["event_order_and_coverage_preserved"] is True
    assert audit["identity_projection_is_structural_only"] is True
    assert audit["semantic_rewrite_performed"] is False
    assert audit["semantic_filtering_performed"] is False
    assert audit["semantic_deduplication_performed"] is False
    assert audit["case_count"] == 4
    assert audit["candidate_event_count"] == 79
    assert audit["case_event_counts"] == [23, 20, 11, 25]
    assert request["prompt_bytes"] < 45_000
    assert request["prompt_bytes"] < 53_483 * 0.85
    assert request["instruction_bytes"] <= v218.MAX_INSTRUCTIONS_BYTES
    assert request["schema_bytes"] <= v218.MAX_SCHEMA_BYTES


def test_v218_compact_marker_projection_is_reversible():
    checkpoint = v218._validate_v217_checkpoint()
    original = checkpoint["scoring_predecessor"]["input"]
    request = v218._build_request(checkpoint)
    for old, new in zip(original["cases"], request["packet"]["c"], strict=True):
        expanded = v218._expand_markers(str(new[0]), len(new[1]))
        assert expanded == old["annotated_source"]
        assert v218.v216.v211.strip_selector_markers(expanded) == (
            v218.v216.v211.strip_selector_markers(old["annotated_source"])
        )


def test_v218_support_schema_and_identity_projection_are_structural():
    checkpoint = v218._validate_v217_checkpoint()
    request = v218._build_request(checkpoint)
    output = _all_supported_output(request["packet"])
    assert validate_app_server_output_schema_subset(request["schema"]) == []
    assert v218.validate_support_output(
        output,
        request["packet"],
        request["schema"],
    ) == []
    projected = v218._project_support_output(output, request["identity"])
    assert all(
        decision["verdict"] == "keep"
        and decision["canonical_event_id"] == decision["event_id"]
        for case in projected["cases"]
        for decision in case["decisions"]
    )
    assert v218.v216.v212.validate_selector_output(
        projected,
        request["scoring_predecessor"],
    ) == []


def test_v218_prompt_is_model_blind_and_support_only():
    checkpoint = v218._validate_v217_checkpoint()
    request = v218._build_request(checkpoint)
    rendered = json.dumps(request["packet"])
    normalized = " ".join(request["instructions"].split())
    assert "density_stratum" not in rendered
    assert "source_id" not in rendered
    assert "affordable_oracle_f1" not in rendered
    assert "package_id" not in request["prompt"]
    assert "Judge rows independently" in normalized
    assert "Do not compare, merge, or deduplicate" in normalized
    assert "complete source text" in normalized


def test_v218_freezes_exact_runtime_without_semantic_artifacts(frozen_v218):
    root, frozen = frozen_v218
    lock = v218.verify_runtime_lock(frozen["runtime_lock"])
    spec = json.loads((root / "attempt-spec.json").read_text())
    assert len(lock["runtime_files"]) == 24
    assert len(lock["v217_receipt"]) == 7
    assert spec["model"] == "gpt-5.6-sol"
    assert spec["reasoning_effort"] == "low"
    assert spec["retry_count_per_turn"] == 0
    assert spec["maximum_total_tokens_per_turn"] == 50_000
    assert spec["promotion_total_token_gate"] == 35_000
    assert spec["semantic_deduplication_allowed"] is False
    assert spec["extraction_model_calls_authorized"] == 0
    assert spec["holdout_authorized"] is False
    assert not (root / "launch-receipt.json").exists()
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))


def test_v218_freeze_to_run_handoff_is_idempotent(frozen_v218):
    root, first = frozen_v218
    second = v218.freeze_v218(output_dir=root)
    assert second["runtime_lock"] == first["runtime_lock"]
    assert second["request"]["packet"] == first["request"]["packet"]
    assert not (root / "launch-receipt.json").exists()


def test_v218_runtime_lock_rejects_missing_runtime_file(frozen_v218):
    root, frozen = frozen_v218
    lock = json.loads(frozen["runtime_lock"].read_text())
    lock["runtime_files"] = lock["runtime_files"][1:]
    mutated = root / "runtime-lock-mutated.json"
    mutated.write_text(json.dumps(lock) + "\n", encoding="utf-8")
    with pytest.raises(v218.JudgeV5SelectionV218Error, match="runtime lock drifted"):
        v218.verify_runtime_lock(mutated)


def test_v218_started_attempt_cannot_be_silently_resumed(tmp_path: Path):
    root = tmp_path / "attempt"
    frozen = v218.freeze_v218(output_dir=root)
    receipt = v218._freeze_launch_receipt(frozen)
    assert receipt.is_file()
    with pytest.raises(
        v218.JudgeV5SelectionV218Error,
        match="cannot be silently resumed",
    ):
        v218.freeze_v218(output_dir=root)
    with pytest.raises(v218.JudgeV5SelectionV218Error, match="already exists"):
        v218._freeze_launch_receipt(frozen)
