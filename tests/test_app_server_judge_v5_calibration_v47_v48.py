from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v47_reference import (
    DEFAULT_V45_ROOT,
    DEFAULT_V46_ROOT,
    apply_reference_consistency_patch,
    audit_reference_consistency,
    freeze_v47_reference_consistency,
)
from research_factory.app_server_judge_v5_calibration_v48_diagnostic import (
    ANONYMOUS_SET_LABELS,
    DEFAULT_OUTPUT_ROOT as DEFAULT_V48_ROOT,
    MAX_PROMPT_BYTES,
    MAX_SCHEMA_BYTES,
    TARGET_CASE_IDS,
    annotate_anonymous_comparison_sets,
    bipartite_pair_errors,
    bipartite_alignment_instructions,
    freeze_v48_bipartite_diagnostic,
)
from research_factory.app_server_judge_v5_calibration_v50_reference_restore import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V50_ROOT,
    audit_reference_drift,
    freeze_v50_canonical_reference,
)


def _contradictory_truth() -> dict:
    return {
        "schema_version": "test",
        "cases": {
            "case_1": {
                "proposition": {"w1": "unsupported", "w2": "supported"},
                "equivalence_groups": [["w1", "w2"]],
                "pairs": [
                    {
                        "witness_ids": ["w1", "w2"],
                        "relation": "equivalent",
                        "mismatch_fields": [],
                    }
                ],
            }
        },
    }


def test_v47_projects_frozen_support_without_semantic_rules() -> None:
    truth = _contradictory_truth()
    issues = audit_reference_consistency(truth)
    assert [item["issue_type"] for item in issues] == [
        "support_alignment_contradiction"
    ]
    patched, operations = apply_reference_consistency_patch(truth, issues)
    assert audit_reference_consistency(patched) == []
    pair = patched["cases"]["case_1"]["pairs"][0]
    assert pair["relation"] == "non_equivalent"
    assert pair["mismatch_fields"] == ["unsupported_inference"]
    assert patched["cases"]["case_1"]["equivalence_groups"] == [["w1"], ["w2"]]
    assert operations[0]["operation"] == "project_frozen_support_verdict_difference"


def test_v48_anonymous_membership_swaps_without_system_identity() -> None:
    pool = {
        "cases": [
            {
                "case_id": "case_1",
                "event_set_a": [{"witness_id": "w1"}],
                "event_set_b": [{"witness_id": "w2"}],
            }
        ]
    }
    alignment = {
        "cases": [
            {
                "case_id": "case_1",
                "witnesses": [{"witness_id": "w1"}, {"witness_id": "w2"}],
            }
        ]
    }
    base = annotate_anonymous_comparison_sets(alignment, pool, permutation="base")
    canary = annotate_anonymous_comparison_sets(
        alignment, pool, permutation="balanced_canary"
    )
    base_by_id = {
        item["witness_id"]: item["comparison_set"]
        for item in base["cases"][0]["witnesses"]
    }
    canary_by_id = {
        item["witness_id"]: item["comparison_set"]
        for item in canary["cases"][0]["witnesses"]
    }
    assert set(base_by_id.values()) == set(ANONYMOUS_SET_LABELS)
    assert base_by_id["w1"] == canary_by_id["w2"]
    assert base_by_id["w2"] == canary_by_id["w1"]
    assert base["comparison_sets_reveal_system_identity"] is False


def test_v48_rejects_same_set_alignment_pair() -> None:
    alignment_input = {
        "cases": [
            {
                "case_id": "case_1",
                "witnesses": [
                    {"witness_id": "w1", "comparison_set": "set_kappa"},
                    {"witness_id": "w2", "comparison_set": "set_kappa"},
                ],
            }
        ]
    }
    output = {
        "cases": [
            {
                "case_id": "case_1",
                "equivalence_groups": [],
                "alignment_pairs": [
                    {"witness_id_1": "w1", "witness_id_2": "w2", "checklist": []}
                ],
                "unpaired_witness_ids": [],
            }
        ]
    }
    errors = bipartite_pair_errors(output, alignment_input)
    assert "case_1_alignment_pair_not_cross_set" in errors


def test_v48_instructions_preserve_origin_neutrality() -> None:
    instructions = bipartite_alignment_instructions()
    assert "arbitrary" in instructions
    assert "exactly one witness from each comparison set" in instructions
    assert "baseline" not in instructions.lower()
    assert "candidate" not in instructions.lower()


def test_v47_and_v48_freeze_without_semantic_turns(tmp_path: Path) -> None:
    v47_root = tmp_path / "v47"
    v47_terminal = freeze_v47_reference_consistency(
        output_dir=v47_root,
        v45_root=DEFAULT_V45_ROOT,
        v46_root=DEFAULT_V46_ROOT,
    )
    assert v47_terminal["targeted_diagnostic_authorized"] is True
    assert v47_terminal["semantic_attempt_started"] is False
    v48_root = tmp_path / "v48"
    frozen = freeze_v48_bipartite_diagnostic(
        output_dir=v48_root,
        v46_root=DEFAULT_V46_ROOT,
        v47_root=v47_root,
    )
    frozen_again = freeze_v48_bipartite_diagnostic(
        output_dir=v48_root,
        v46_root=DEFAULT_V46_ROOT,
        v47_root=v47_root,
    )
    assert frozen_again["spec"] == frozen["spec"]
    assert frozen["spec"]["case_ids"] == list(TARGET_CASE_IDS)
    assert len(frozen["spec"]["frozen_inputs"]["turn_requests"]) == 4
    assert not list((v48_root / "turns").glob("*/sidecar.json"))
    for turn in frozen["spec"]["frozen_inputs"]["turn_requests"]:
        prompt = Path(turn["prompt"]["path"])
        schema = Path(turn["schema"]["path"])
        assert prompt.stat().st_size <= MAX_PROMPT_BYTES
        assert schema.stat().st_size <= MAX_SCHEMA_BYTES
    policy = json.loads((v48_root / "capacity-policy.json").read_text())
    assert policy["retry_count_per_turn"] == 0
    assert policy["managed_chatgpt_auth_only"] is True
    assert policy["minimum_remaining_reserve_percent"] == 20
    v49_root = tmp_path / "v49"
    recovered = freeze_v48_bipartite_diagnostic(
        output_dir=v49_root,
        v46_root=DEFAULT_V46_ROOT,
        v47_root=v47_root,
        presemantic_predecessor_root=DEFAULT_V48_ROOT,
    )
    assert "presemantic_recovery" in recovered["spec"]["predecessors"]
    assert not list((v49_root / "turns").glob("*/capacity.json"))


def test_v50_drift_audit_is_structural_and_sanitized() -> None:
    source = {
        "cases": {
            "case_1": {
                "base_case_id": "fixture_1",
                "proposition": {"wit_1": "unsupported", "wit_2": "supported"},
                "structured_fields": {"wit_1": "incorrect", "wit_2": "correct"},
                "field_issues": {"wit_1": ["unsupported_inference"], "wit_2": []},
                "pairs": [],
                "equivalence_groups": [["wit_1"], ["wit_2"]],
                "unpaired_witness_ids": ["wit_1", "wit_2"],
            }
        }
    }
    canonical = deepcopy(source)
    canonical["cases"]["case_1"]["proposition"]["wit_1"] = "supported"
    canonical["cases"]["case_1"]["structured_fields"]["wit_1"] = "correct"
    canonical["cases"]["case_1"]["field_issues"]["wit_1"] = []
    canonical["cases"]["case_1"]["pairs"] = [
        {"witness_ids": ["wit_1", "wit_2"], "relation": "equivalent", "mismatch_fields": []}
    ]
    canonical["cases"]["case_1"]["equivalence_groups"] = [["wit_1", "wit_2"]]
    canonical["cases"]["case_1"]["unpaired_witness_ids"] = []
    audit = audit_reference_drift(source, canonical)
    assert audit["affected_case_count"] == 1
    assert audit["category_counts"]["proposition"] == 1
    assert audit["category_counts"]["pair_restored"] == 1
    assert "source_text" not in json.dumps(audit)


def test_v50_freezes_canonical_reference_without_semantic_turn(tmp_path: Path) -> None:
    root = tmp_path / "v50"
    terminal = freeze_v50_canonical_reference(output_dir=root)
    terminal_again = freeze_v50_canonical_reference(output_dir=root)
    assert terminal_again == terminal
    assert terminal["reference_frozen"] is True
    assert terminal["fresh_diagnostic_required"] is True
    assert terminal["semantic_attempt_started"] is False
    assert terminal["usage"]["total_tokens"] == 0
    assert terminal["holdout_authorized"] is False
    assert not list(root.glob("turns/*/sidecar.json"))
    audit = json.loads((root / "reference-restoration-audit.json").read_text())
    assert audit["canonical_pool_reproduces_v46_exactly"] is True
    assert audit["canonical_pool_reproduces_v49_exactly"] is True
    assert audit["v45_reference_drift"]["affected_case_count"] == 10
    assert audit["v47_reference_drift"]["affected_case_count"] == 10
    rescore = json.loads((root / "v49-canonical-rescore.json").read_text())
    assert rescore["promotion_authorized"] is False
    assert rescore["passed"] is False
