from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from research_factory import app_server_adoption_semantic_evaluation as semantic


def _contains_key(value: object, key: str) -> bool:
    if isinstance(value, dict):
        return key in value or any(_contains_key(child, key) for child in value.values())
    if isinstance(value, list):
        return any(_contains_key(child, key) for child in value)
    return False


def test_projection_preserves_semantics_and_records_overridden_diagnostics() -> None:
    candidate, provenance, diagnostics = semantic._project_candidate()

    assert [len(row["events"]) for row in candidate["segments"]] == [24, 1]
    assert len(provenance["events"]) == 25
    assert diagnostics["exact_evidence_rate"] == 1.0
    assert diagnostics["metric_substring_error_count"] == 0
    assert diagnostics["metric_applicability_mismatch_count"] == 2
    assert diagnostics["exact_identity_duplicate_count"] == 0
    assert diagnostics["semantic_fields_changed"] is False
    assert diagnostics["dense_event_count_used_as_semantic_gate"] is False


def test_support_pool_is_side_free_and_has_exact_membership() -> None:
    lineage = semantic._validate_lineage()
    bundle = semantic.build_support_bundle(lineage)

    assert len(bundle["units"]) == 52
    assert len(bundle["origins"]) == 52
    assert sum(row["origin"] == "reference" for row in bundle["origins"]) == 27
    assert sum(row["origin"] == "candidate" for row in bundle["origins"]) == 25
    assert not _contains_key(bundle["support_value"], "origin")


def test_direct_successor_predecessor_check_never_calls_full_tree_verifier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        semantic.runtime_verifier,
        "verify_runtime_lock",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("full-tree verify called")),
    )

    rows = semantic._predecessor_manifests()
    semantic._verify_predecessor_manifests(rows)

    assert len(rows) == 3
    assert all(len(row["closure_digest"]) == 64 for row in rows)


def _normalized_alignment(represented: int) -> tuple[dict, dict]:
    reference_ids = [f"r{index:02d}" for index in range(27)]
    candidate_ids = [f"c{index:02d}" for index in range(24)]
    groups = [
        [reference_ids[index], candidate_ids[index]]
        for index in range(represented)
    ]
    groups.extend([reference_id] for reference_id in reference_ids[represented:])
    groups.extend([candidate_id] for candidate_id in candidate_ids[represented:])
    checklist = {field: "same" for field in semantic.v277.v246.v130.CHECKLIST_FIELDS}
    pairs = [
        {
            "witness_ids": [reference_ids[index], candidate_ids[index]],
            "relation": "equivalent",
            "mismatch_fields": [],
            "checklist_decisions": checklist,
        }
        for index in range(represented)
    ]
    case = {
        "case_id": "jcase_test",
        "equivalence_groups": groups,
        "alignment_pairs": pairs,
        "unpaired_witness_ids": [
            *reference_ids[represented:],
            *candidate_ids[represented:],
        ],
    }
    normalized = {
        "schema_version": "test",
        "origin_neutral": True,
        "mismatch_fields_projected_from_checklists": True,
        "cases": [case],
    }
    mapping = {
        "rows": [
            {
                "witness_id": witness_id,
                "origin": "reference",
                "support_status": "supported",
            }
            for witness_id in reference_ids
        ]
        + [
            {
                "witness_id": witness_id,
                "origin": "candidate",
                "support_status": "supported",
            }
            for witness_id in candidate_ids
        ],
        "no_signal_rows": [
            {
                "witness_id": "c_no_signal",
                "origin": "candidate",
                "support_status": "supported",
            }
        ],
    }
    return normalized, mapping


def test_twenty_four_candidates_can_pass_unchanged_semantic_gate() -> None:
    normalized, mapping = _normalized_alignment(represented=24)

    score = semantic.score_alignment(
        base=normalized,
        canary=copy.deepcopy(normalized),
        mapping=mapping,
    )

    assert score["metrics"]["development_strict_full_field_macro_f1"] == 0.970588
    assert score["passed"] is True


def test_semantic_gate_still_fails_below_point_nine_seven() -> None:
    normalized, mapping = _normalized_alignment(represented=23)

    score = semantic.score_alignment(
        base=normalized,
        canary=copy.deepcopy(normalized),
        mapping=mapping,
    )

    assert score["metrics"]["development_strict_full_field_macro_f1"] < 0.97
    assert score["passed"] is False
    assert "strict_full_field_macro_f1_gte_0_97" in score["failed_checks"]


def test_override_rejects_threshold_drift(tmp_path: Path) -> None:
    lineage = semantic._validate_lineage()
    records = {
        name: lineage["records"][name]
        for name in (
            "adoption_terminal",
            "adoption_output",
            "adoption_sidecar",
            "adoption_authorization",
            "adoption_audit",
            "post_adoption_blocker",
        )
    }
    receipt = {
        "schema_version": semantic.OVERRIDE_VERSION,
        "authority": "direct_user_instruction",
        "scope": "evaluate_existing_adoption_output_support_then_neutral_alignment",
        "extraction_replay_authorized": False,
        "semantic_repair_authorized": False,
        "dense_event_count_proxy_stop_overridden": True,
        "metric_applicability_mismatches_are_diagnostics": True,
        "frozen_quality_threshold": semantic.QUALITY_THRESHOLD,
        "prompt_model_rubric_changed": False,
        "holdout_authorized_before_alignment": False,
        "production_mutation_allowed": False,
        "records": records,
    }
    path = tmp_path / "operator-override-audit-v1.json"
    path.write_text(json.dumps(receipt), encoding="utf-8")

    assert semantic._verify_override(tmp_path, lineage)["frozen_quality_threshold"] == 0.97

    receipt["frozen_quality_threshold"] = 0.96
    path.write_text(json.dumps(receipt), encoding="utf-8")
    with pytest.raises(semantic.AdoptionSemanticEvaluationError):
        semantic._verify_override(tmp_path, lineage)
