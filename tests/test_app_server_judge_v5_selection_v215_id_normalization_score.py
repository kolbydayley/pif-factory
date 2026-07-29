from __future__ import annotations

import json
from pathlib import Path

import pytest

from research_factory import (
    app_server_judge_v5_selection_v215_id_normalization_score as v215,
)


@pytest.fixture(scope="module")
def frozen_v215(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, dict]:
    root = tmp_path_factory.mktemp("v215") / "receipt"
    terminal = v215.freeze_v215(output_dir=root)
    return root, terminal


def test_v215_binds_completed_v214_as_infrastructure_failure_with_measured_usage():
    checkpoint = v215._validate_v214_checkpoint()
    assert checkpoint["terminal"]["state"] == "failed"
    assert checkpoint["terminal"]["usage_status"] == "complete"
    assert checkpoint["terminal"]["usage"]["total_tokens"] == 39_476
    assert checkpoint["failure"]["error_class"] == (
        "JudgeV5CalibrationV26DiagnosticError"
    )
    assert checkpoint["sidecar"]["status"] == "completed"
    assert checkpoint["sidecar"]["auth_type"] == "chatgpt"
    assert checkpoint["terminal"]["holdout_authorized"] is False
    assert checkpoint["terminal"]["production_mutated"] is False


def test_v215_normalization_changes_only_keep_canonical_ids():
    checkpoint = v215._validate_v214_checkpoint()
    normalized, audit = v215._normalize_keep_links(
        checkpoint["output"], checkpoint["request"]["scoring_predecessor"]
    )
    assert audit["normalized_keep_link_count"] == 56
    assert audit["semantic_verdicts_changed"] is False
    assert audit["event_payload_changed"] is False
    assert audit["new_semantic_model_calls"] == 0
    before = [
        (case["case_id"], decision["event_id"], decision["verdict"])
        for case in checkpoint["output"]["cases"]
        for decision in case["decisions"]
    ]
    after = [
        (case["case_id"], decision["event_id"], decision["verdict"])
        for case in normalized["cases"]
        for decision in case["decisions"]
    ]
    assert before == after
    assert all(
        decision["canonical_event_id"] == decision["event_id"]
        for case in normalized["cases"]
        for decision in case["decisions"]
        if decision["verdict"] == "keep"
    )


def test_v215_freezes_truthful_zero_token_nonacceptance(frozen_v215):
    root, terminal = frozen_v215
    gate = json.loads((root / "normalized-selector-gate.json").read_text())
    audit = json.loads((root / "id-normalization-audit.json").read_text())
    assert terminal["terminal_reason"] == (
        "v215_normalized_selector_quality_or_cost_gate_not_passed"
    )
    assert terminal["new_semantic_model_calls"] == 0
    assert terminal["usage"]["total_tokens"] == 0
    assert terminal["cumulative_known_usage_lower_bound"]["total_tokens"] == 9_011_691
    assert terminal["full_development_router_authorized"] is False
    assert terminal["holdout_authorized"] is False
    assert terminal["production_mutated"] is False
    assert audit["semantic_verdicts_changed"] is False
    assert gate["candidate_mean_f1"] == 0.785931
    assert gate["mean_f1_regret_to_oracle"] == 0.04661
    assert gate["maximum_dense_case_regret_to_oracle"] == 0.250602
    assert gate["dense_improvement_count"] == 4
    assert set(gate["failed_checks"]) == {
        "actual_selector_total_tokens_lte_35000",
        "maximum_dense_case_regret_lte_0_15",
    }


def test_v215_runtime_lock_has_exact_coverage(frozen_v215):
    root, _terminal = frozen_v215
    lock = v215.verify_runtime_lock(root / "runtime-lock.json")
    assert len(lock["runtime_files"]) == 9
    assert len(lock["v214_attempt"]) == 16
    assert lock["semantic_model_calls_authorized"] == 0
    assert lock["production_mutation_allowed"] is False


def test_v215_runtime_lock_rejects_missing_runtime_file(frozen_v215):
    root, _terminal = frozen_v215
    lock = json.loads((root / "runtime-lock.json").read_text())
    lock["runtime_files"] = lock["runtime_files"][1:]
    mutated = root / "runtime-lock-mutated.json"
    mutated.write_text(json.dumps(lock) + "\n", encoding="utf-8")
    with pytest.raises(v215.JudgeV5SelectionV215Error, match="runtime lock drifted"):
        v215.verify_runtime_lock(mutated)


def test_v215_is_immutable(frozen_v215):
    root, terminal = frozen_v215
    assert v215.freeze_v215(output_dir=root) == terminal
