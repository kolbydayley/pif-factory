from __future__ import annotations

import json
from pathlib import Path

import pytest

from research_factory import (
    app_server_judge_v5_selection_v217_spark_support_score as v217,
)


@pytest.fixture(scope="module")
def scored_v217(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, dict]:
    root = tmp_path_factory.mktemp("v217") / "attempt"
    terminal = v217.freeze_v217(output_dir=root)
    return root, terminal


def test_v217_binds_completed_v216_failure_without_replay():
    checkpoint = v217._validate_v216_checkpoint()
    assert checkpoint["terminal"]["state"] == "failed"
    assert checkpoint["failure"]["error_class"] == "ReserveCapacityError"
    assert checkpoint["sidecar"]["state"] == "completed"
    assert checkpoint["sidecar"]["usage_complete"] is True
    assert checkpoint["sidecar"]["usage"]["total_tokens"] == 41_499
    assert checkpoint["terminal"]["holdout_authorized"] is False
    assert checkpoint["terminal"]["production_mutated"] is False


def test_v217_recovers_schema_valid_complete_support_output():
    checkpoint = v217._validate_v216_checkpoint()
    output = checkpoint["output"]
    assert len(output["cases"]) == 4
    assert sum(len(case["decisions"]) for case in output["cases"]) == 79
    assert v217.v216.validate_support_output(
        output,
        checkpoint["request"]["packet"],
        checkpoint["request"]["schema"],
    ) == []
    assert v217.v216.v212.validate_selector_output(
        checkpoint["projected"],
        checkpoint["request"]["scoring_predecessor"],
    ) == []


def test_v217_freezes_quality_pass_and_cost_failure(scored_v217):
    root, terminal = scored_v217
    gate = json.loads((root / "spark-support-score-gate.json").read_text())
    audit = json.loads((root / "score-recovery-audit.json").read_text())
    assert terminal["state"] == "inactive"
    assert terminal["semantic_quality_gate_passed"] is True
    assert terminal["selector_cost_gate_passed"] is False
    assert terminal["new_semantic_model_calls"] == 0
    assert terminal["cumulative_known_usage_lower_bound"]["total_tokens"] == 9_053_190
    assert terminal["holdout_authorized"] is False
    assert terminal["production_mutated"] is False
    assert gate["failed_checks"] == ["actual_selector_total_tokens_lte_35000"]
    assert gate["candidate_mean_f1"] == 0.803978
    assert gate["mean_f1_regret_to_oracle"] == 0.028563
    assert gate["maximum_dense_case_regret_to_oracle"] == 0.124286
    assert gate["dense_improvement_count"] == 4
    assert gate["normalized_nonexact_evidence_events"] == 0
    assert audit["v216_semantic_turn_replayed"] is False
    assert audit["v216_semantic_verdicts_changed"] is False


def test_v217_runtime_lock_is_exact(scored_v217):
    root, _terminal = scored_v217
    lock = v217.verify_runtime_lock(root / "runtime-lock.json")
    assert len(lock["runtime_files"]) == 23
    assert len(lock["v216_attempt"]) == 16
    assert lock["semantic_model_calls_authorized"] == 0
    assert lock["production_mutation_allowed"] is False


def test_v217_is_idempotent_after_terminal(scored_v217):
    root, terminal = scored_v217
    assert v217.freeze_v217(output_dir=root) == terminal


def test_v217_runtime_lock_rejects_missing_v216_artifact(scored_v217):
    root, _terminal = scored_v217
    lock = json.loads((root / "runtime-lock.json").read_text())
    lock["v216_attempt"] = lock["v216_attempt"][1:]
    mutated = root / "runtime-lock-mutated.json"
    mutated.write_text(json.dumps(lock) + "\n", encoding="utf-8")
    with pytest.raises(v217.JudgeV5SelectionV217Error, match="drifted"):
        v217.verify_runtime_lock(mutated)
