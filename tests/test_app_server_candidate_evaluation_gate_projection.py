import copy
import json
from pathlib import Path

import pytest

from research_factory import app_server_candidate_evaluation_gate_projection as gate


def _score() -> dict:
    return {
        "schema_version": "source",
        "checks": {
            "permutation_projection_exact": False,
            "observable_disagreement_adjudication_complete": True,
            "adjudication_call_cap_lte_1": True,
            "unresolved_alignment_cases_0": True,
            "primary_abstention_count_0": True,
            "strict_full_field_macro_f1_gte_0_97": True,
            "no_material_source_macro_regression": True,
            "production_amortized_total_token_ratio_lte_0_28": True,
        },
        "adjudication": {
            "required": True,
            "call_count": 1,
            "call_cap": 1,
            "unresolved_case_ids": [],
            "majority_voting_used": False,
        },
        "permutation_disagreement_case_ids": ["opaque_case"],
        "metrics": {
            "development_strict_full_field_macro_f1": 0.98,
            "production_amortized_total_token_ratio": 0.2,
        },
        "case_metrics": [],
        "mismatch_field_counts": {},
        "observable_disagreements": {},
        "abstention_case_ids": [],
        "alignment_consistency_issues": [],
        "failed_checks": ["permutation_projection_exact"],
        "passed": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def test_completed_adjudication_resolves_acceptance_gate_without_semantic_change() -> None:
    source = _score()
    before = copy.deepcopy(source)
    projected, audit = gate.project_resolved_permutation_gate(source)
    assert source == before
    assert projected["checks"]["permutation_projection_exact"] is False
    assert projected["checks"]["permutation_resolution_complete"] is True
    assert projected["passed"] is True
    assert projected["metrics"] == source["metrics"]
    assert audit["resolved_by_adjudication"] is True


def test_raw_exact_permutation_needs_no_adjudication() -> None:
    source = _score()
    source["checks"]["permutation_projection_exact"] = True
    source["adjudication"].update(required=False, call_count=0)
    source["permutation_disagreement_case_ids"] = []
    projected, audit = gate.project_resolved_permutation_gate(source)
    assert projected["checks"]["permutation_resolution_complete"] is True
    assert audit["resolved_by_adjudication"] is False


@pytest.mark.parametrize(
    ("change", "expected"),
    [
        ({"call_count": 0}, False),
        ({"unresolved_case_ids": ["opaque_case"]}, False),
        ({"majority_voting_used": True}, False),
    ],
)
def test_incomplete_or_forbidden_adjudication_fails_resolution(change, expected) -> None:
    source = _score()
    source["adjudication"].update(change)
    if change.get("unresolved_case_ids"):
        source["checks"]["unresolved_alignment_cases_0"] = False
    projected, _audit = gate.project_resolved_permutation_gate(source)
    assert projected["checks"]["permutation_resolution_complete"] is expected
    assert "permutation_resolution_complete" in projected["failed_checks"]


def test_quality_gate_is_not_relaxed() -> None:
    source = _score()
    source["checks"]["strict_full_field_macro_f1_gte_0_97"] = False
    source["checks"]["no_material_source_macro_regression"] = False
    source["metrics"]["development_strict_full_field_macro_f1"] = 0.857143
    projected, audit = gate.project_resolved_permutation_gate(source)
    assert projected["checks"]["permutation_resolution_complete"] is True
    assert projected["passed"] is False
    assert projected["failed_checks"] == [
        "no_material_source_macro_regression",
        "strict_full_field_macro_f1_gte_0_97",
    ]
    assert audit["quality_threshold_changed"] is False
    assert audit["token_threshold_changed"] is False


def test_freeze_is_immutable_and_verifiable(tmp_path: Path) -> None:
    source_path = tmp_path / "source.json"
    source_path.write_text(json.dumps(_score()), encoding="utf-8")
    root = tmp_path / "projection"
    first = gate.freeze_projection(source_score=source_path, output_dir=root)
    second = gate.freeze_projection(source_score=source_path, output_dir=root)
    assert first == second
    assert gate.verify_frozen(root)["development_quality_passed"] is True
    source_path.write_text(json.dumps({"drift": True}), encoding="utf-8")
    with pytest.raises(gate.CandidateGateProjectionError):
        gate.verify_frozen(root)
