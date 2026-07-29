from __future__ import annotations

from pathlib import Path

from research_factory import app_server_adoption_alignment_measured_score as v5


def test_v4_is_measured_and_has_only_exact_span_join_errors() -> None:
    predecessor = v5.validate_v4_predecessor()

    assert predecessor["sidecar"]["usage"]["total_tokens"] == 78_781
    assert predecessor["terminal"]["accounting_complete"] is True
    assert predecessor["terminal"]["second_turn_attempted_count"] == 1


def test_exact_span_projection_changes_no_semantic_decision() -> None:
    predecessor = v5.validate_v4_predecessor()
    projected, audit = v5.project_exact_span_components(
        predecessor["output"], predecessor["frozen"]["second"]["value"]
    )

    assert audit["projected_entry_count"] == 4
    assert audit["semantic_payload_changed"] is False
    assert v5.v4.v3.v2.semantic.judge.validate_neutral_alignment_output(
        projected, predecessor["frozen"]["second"]["value"]
    ) == []


def test_measured_score_fails_unchanged_quality_gate(tmp_path: Path) -> None:
    terminal = v5.freeze_and_score(output_dir=tmp_path / "v5")
    score = v5._load_json(tmp_path / "v5" / "alignment-score.json", "score")

    assert score["passed"] is False
    assert score["metrics"]["development_strict_full_field_macro_f1"] == 0.886364
    assert score["metrics"]["production_amortized_total_token_ratio"] == 0.259107
    assert score["permutation_disagreement_case_ids"]
    assert terminal["terminal_reason"] == "development_semantic_quality_gate_not_passed"
    assert terminal["development_quality_passed"] is False
    assert terminal["holdout_authorized"] is False
    assert terminal["production_mutated"] is False
    assert terminal["model_calls_performed"] == 0


def test_score_runtime_lock_is_immutable(tmp_path: Path) -> None:
    root = tmp_path / "v5"
    v5.freeze_and_score(output_dir=root)
    lock = v5.verify_runtime_lock(root / "runtime-lock.json")

    assert lock["model_calls_performed"] == 0
    assert lock["semantic_decisions_changed"] is False
    assert lock["frozen_quality_threshold"] == 0.97
