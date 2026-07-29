from __future__ import annotations

import json
from pathlib import Path

import pytest

from research_factory import app_server_candidate_evaluation_unpaired_recovery as recovery
from research_factory import app_server_judge_v5 as judge


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_unpaired_projection_is_exact_complement_only() -> None:
    source = recovery._validate_predecessor(  # noqa: SLF001
        recovery.DEFAULT_PREDECESSOR_ROOT
    )
    assert source["audit"]["added_exact_complement_id_count"] == 32
    assert source["audit"]["witness_pairs_changed"] is False
    assert source["audit"]["relations_changed"] is False
    assert source["audit"]["equivalence_groups_changed"] is False
    assert (
        judge.validate_neutral_alignment_output(
            source["projected"],
            source["adjudication_bundle"]["turn"]["value"],
        )
        == []
    )


def test_zero_turn_recovery_freezes_real_quality_failure(tmp_path: Path) -> None:
    root = tmp_path / "recovery"
    terminal = recovery.freeze_and_score(output_dir=root)
    assert terminal["terminal_reason"] == "candidate_semantic_quality_gate_not_passed"
    assert terminal["semantic_turn_count"] == 0
    assert terminal["measured_turn_count"] == 4
    assert terminal["usage"]["total_tokens"] == 295_670
    assert terminal["development_winner_frozen"] is False
    assert terminal["holdout_authorized"] is False
    assert terminal["production_mutated"] is False
    score = _load(root / "alignment-score.json")
    assert score["metrics"]["development_strict_full_field_macro_f1"] == 0.857143
    assert score["metrics"]["production_amortized_total_token_ratio"] == 0.275013
    assert "strict_full_field_macro_f1_gte_0_97" in score["failed_checks"]
    assert recovery.verify_lock(root / "runtime-lock.json") == root / "runtime-lock.json"


def test_runtime_lock_rejects_projection_audit_mutation(tmp_path: Path) -> None:
    root = tmp_path / "recovery"
    recovery.freeze_and_score(output_dir=root)
    audit = root / "unpaired-complement-projection-audit.json"
    audit.write_text(audit.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    with pytest.raises(recovery.CandidateUnpairedRecoveryError):
        recovery.verify_lock(root / "runtime-lock.json")
