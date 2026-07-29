from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from research_factory import app_server_candidate_evaluation_bundle as evaluator
from research_factory import app_server_candidate_evaluation_identity_adoption as adoption


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )


def _record(path: Path) -> dict[str, object]:
    return adoption.ContentHashCache().record(path)


def _fixture(tmp_path: Path, *, candidate_identity: bool = True) -> Path:
    root = tmp_path / "identity-adoption"
    lineage = tmp_path / "lineage"
    target_candidate = lineage / "target-candidate.json"
    evaluated_candidate = lineage / "evaluated-candidate.json"
    candidate = {"episode_id": "episode", "segments": [{"segment_id": "s1", "events": []}]}
    _write_json(target_candidate, candidate)
    _write_json(
        evaluated_candidate,
        candidate if candidate_identity else {"episode_id": "episode", "segments": []},
    )

    source = lineage / "source.json"
    shared_reference = lineage / "reference.json"
    protocol_lock = lineage / "evaluator-lock.json"
    protocol_receipt = lineage / "evaluator-receipt.json"
    projection_audit = lineage / "projection-audit.json"
    for path, value in (
        (source, {"source": "opaque"}),
        (shared_reference, {"reference": "opaque"}),
        (protocol_lock, {"lock": "frozen"}),
        (protocol_receipt, {"receipt": "frozen"}),
        (projection_audit, {"semantic_projection_changed": False}),
    ):
        _write_json(path, value)

    ratio = 0.2
    target_gate = lineage / "target-gate.json"
    target_terminal = lineage / "target-terminal.json"
    evaluated_score = lineage / "evaluated-score.json"
    evaluated_terminal = lineage / "evaluated-terminal.json"
    _write_json(
        target_gate,
        {"passed": True, "production_amortized_total_token_ratio": ratio},
    )
    _write_json(
        target_terminal,
        {
            "support_alignment_authorized": True,
            "production_mutated": False,
        },
    )
    _write_json(
        evaluated_score,
        {
            "schema_version": evaluator.SCORE_VERSION,
            "metrics": {
                "development_strict_full_field_macro_f1": 0.85,
                "production_amortized_total_token_ratio": 0.27,
            },
            "checks": {
                "strict_full_field_macro_f1_gte_0_97": False,
                "production_amortized_total_token_ratio_lte_0_28": True,
            },
            "failed_checks": ["strict_full_field_macro_f1_gte_0_97"],
            "passed": False,
            "development_winner_frozen": False,
            "holdout_authorized": False,
            "production_mutated": False,
        },
    )
    _write_json(
        evaluated_terminal,
        {
            "terminal_reason": "candidate_semantic_quality_gate_not_passed",
            "accounting_complete": True,
            "unknown_usage_turn_count": 0,
            "production_mutated": False,
            "usage": {
                "input_tokens": 10,
                "cached_input_tokens": 2,
                "output_tokens": 3,
                "reasoning_output_tokens": 1,
                "total_tokens": 14,
            },
        },
    )

    evaluated_config = lineage / "evaluated-config.json"
    _write_json(
        evaluated_config,
        {
            "candidate": _record(evaluated_candidate),
            "source": _record(source),
            "shared_reference": _record(shared_reference),
            "evaluator_protocol_lock": _record(protocol_lock),
            "evaluator_protocol_receipt": _record(protocol_receipt),
        },
    )
    root.mkdir(parents=True)
    config_path = root / "adoption-config.json"
    _write_json(
        config_path,
        {
            "schema_version": adoption.CONFIG_VERSION,
            "adoption_id": "test-identity-adoption",
            "output_root": str(root.resolve()),
            "semantic_turn_count": 0,
            "production_mutation_allowed": False,
            "holdout_authorized": False,
            "production_amortized_total_token_ratio": ratio,
            "target_candidate": _record(target_candidate),
            "evaluated_candidate": _record(evaluated_candidate),
            "target_candidate_terminal": _record(target_terminal),
            "target_candidate_gate": _record(target_gate),
            "evaluated_config": _record(evaluated_config),
            "evaluated_score": _record(evaluated_score),
            "evaluated_terminal": _record(evaluated_terminal),
            "evaluated_projection_audit": _record(projection_audit),
            "source": _record(source),
            "shared_reference": _record(shared_reference),
            "evaluator_protocol_lock": _record(protocol_lock),
            "evaluator_protocol_receipt": _record(protocol_receipt),
        },
    )
    return config_path


def test_identical_candidate_adopts_only_cost_without_semantic_turn(tmp_path: Path) -> None:
    config_path = _fixture(tmp_path)
    terminal = adoption.run_adoption(config_path)
    assert terminal["semantic_turn_count"] == 0
    assert terminal["terminal_reason"] == "candidate_semantic_quality_gate_not_passed"
    assert terminal["development_winner_frozen"] is False
    assert terminal["holdout_authorized"] is False
    assert terminal["production_mutated"] is False
    score = json.loads(
        (config_path.parent / "adopted-alignment-score.json").read_text(encoding="utf-8")
    )
    assert score["metrics"]["production_amortized_total_token_ratio"] == 0.2
    assert score["metrics"]["development_strict_full_field_macro_f1"] == 0.85
    assert score["checks"]["strict_full_field_macro_f1_gte_0_97"] is False
    assert adoption.verify_adoption(config_path.parent)["config"]["semantic_turn_count"] == 0


def test_candidate_content_mismatch_is_rejected(tmp_path: Path) -> None:
    config_path = _fixture(tmp_path, candidate_identity=False)
    with pytest.raises(adoption.CandidateIdentityAdoptionError, match="identity"):
        adoption.freeze_adoption(config_path)


def test_target_gate_semantics_are_rejected_before_freeze(tmp_path: Path) -> None:
    config_path = _fixture(tmp_path)
    config = json.loads(config_path.read_text(encoding="utf-8"))
    gate_path = Path(config["target_candidate_gate"]["path"])
    _write_json(
        gate_path,
        {"passed": False, "production_amortized_total_token_ratio": 0.2},
    )
    config["target_candidate_gate"] = _record(gate_path)
    _write_json(config_path, config)
    with pytest.raises(adoption.CandidateIdentityAdoptionError, match="target candidate gate"):
        adoption.freeze_adoption(config_path)


def test_frozen_lineage_mutation_is_rejected(tmp_path: Path) -> None:
    config_path = _fixture(tmp_path)
    adoption.freeze_adoption(config_path)
    config = json.loads(config_path.read_text(encoding="utf-8"))
    source_path = Path(config["source"]["path"])
    source_path.write_text(source_path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    with pytest.raises(adoption.CandidateIdentityAdoptionError):
        adoption.verify_adoption(config_path.parent)


def test_cost_adoption_preserves_semantic_projection() -> None:
    source = {
        "schema_version": evaluator.SCORE_VERSION,
        "metrics": {
            "development_strict_full_field_macro_f1": 0.85,
            "production_amortized_total_token_ratio": 0.27,
        },
        "checks": {
            "strict_full_field_macro_f1_gte_0_97": False,
            "production_amortized_total_token_ratio_lte_0_28": True,
        },
        "failed_checks": ["strict_full_field_macro_f1_gte_0_97"],
        "passed": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }
    adopted = adoption._adopt_score(copy.deepcopy(source), 0.2)  # noqa: SLF001
    assert adoption._semantic_projection(source) == adoption._semantic_projection(adopted)  # noqa: SLF001
