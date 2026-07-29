from __future__ import annotations

import json
from pathlib import Path

import pytest

from research_factory import app_server_candidate_shared_reference_repair as repair
from research_factory import app_server_llm_judge as judge


def _frozen_pool_and_mapping():
    contract = repair.load_contract()
    return repair.build_shared_pool(contract)


def _system_by_witness(mapping):
    return {
        witness["witness_id"]: witness["provenance"]["system_id"]
        for case in mapping["cases"]
        for witness in case["witnesses"]
    }


def _maximal_consensus(pool, mapping, *, no_signal_supported=True):
    systems = _system_by_witness(mapping)
    cases = []
    for case in pool["cases"]:
        ids = {
            side: [row["witness_id"] for row in case[f"event_set_{side}"]]
            for side in ("a", "b")
        }
        support_results = []
        for witness_id in [*ids["a"], *ids["b"]]:
            supported = no_signal_supported or len(ids["a"]) > 0
            support_results.append(
                {
                    "witness_id": witness_id,
                    "verdict": "supported" if supported else "unsupported",
                    "evidence_spans": [
                        next(
                            row["event"]["evidence"]
                            for side in ("a", "b")
                            for row in case[f"event_set_{side}"]
                            if row["witness_id"] == witness_id
                        )
                    ],
                }
            )
        baseline = sorted(
            witness_id
            for witness_id in [*ids["a"], *ids["b"]]
            if systems[witness_id] == repair.SYSTEM_BASELINE
        )
        candidate = sorted(
            witness_id
            for witness_id in [*ids["a"], *ids["b"]]
            if systems[witness_id] == repair.SYSTEM_CANDIDATE
        )
        groups = [sorted(pair) for pair in zip(baseline, candidate)]
        groups.extend([item] for item in baseline[len(candidate) :])
        groups.extend([item] for item in candidate[len(baseline) :])
        cases.append(
            {
                "case_id": case["case_id"],
                "status": "agreed",
                "support_results": support_results,
                "equivalence_groups": groups,
                "partition_abstained_witness_ids": [],
                "alignment_results": [],
                "unaligned_a_witness_ids": ids["a"],
                "unaligned_b_witness_ids": ids["b"],
                "alignment_abstained_witness_ids": [],
            }
        )
    return {
        "schema_version": judge.JUDGE_CONSENSUS_VERSION,
        "pool_sha256": "test",
        "cases": cases,
        "abstentions": {},
        "selection_admissible": True,
        "abstention_gate_applied": False,
    }


def _accounting(total_tokens=100_000):
    return {
        "usage_status": "complete",
        "accounting_complete": True,
        "measured_model_call_count": 2,
        "usage": {
            "input_tokens": total_tokens - 1,
            "cached_input_tokens": 0,
            "output_tokens": 1,
            "reasoning_output_tokens": 1,
            "total_tokens": total_tokens,
        },
        "turns": [],
    }


def test_contract_builds_exact_full_52_witness_pool():
    pool, mapping = _frozen_pool_and_mapping()
    errors = judge.validate_shared_witness_pool(pool)
    variants = judge.build_judge_variants(pool)
    assert errors == []
    assert len(pool["cases"]) == 2
    assert sum(
        len(case["event_set_a"]) + len(case["event_set_b"])
        for case in pool["cases"]
    ) == 52
    assert all(
        witness["event"]["submitted_evidence_exact"] is True
        for case in pool["cases"]
        for side in ("a", "b")
        for witness in case[f"event_set_{side}"]
    )
    assert [case["case_id"] for case in variants["ab"]["cases"]] == [
        case["case_id"] for case in variants["ba"]["cases"]
    ]
    assert set(_system_by_witness(mapping).values()) == {
        repair.SYSTEM_BASELINE,
        repair.SYSTEM_CANDIDATE,
    }


def test_side_free_adjudication_pools_all_witnesses_without_origin_side():
    pool, _mapping = _frozen_pool_and_mapping()
    case_id = pool["cases"][0]["case_id"]
    variant = repair._build_adjudication_variant(pool, [case_id])
    assert len(variant["cases"]) == 1
    assert variant["cases"][0]["event_set_b"] == []
    assert len(variant["cases"][0]["event_set_a"]) == sum(
        len(pool["cases"][0][f"event_set_{side}"]) for side in ("a", "b")
    )
    assert judge.validate_app_server_output_schema_subset(
        judge.semantic_judge_output_schema(variant)
    ) == []


def test_shared_union_scores_both_systems_symmetrically_and_counts_no_signal():
    pool, mapping = _frozen_pool_and_mapping()
    consensus = _maximal_consensus(pool, mapping)
    score = repair.score_shared_reference(
        pool=pool,
        mapping=mapping,
        consensus=consensus,
        production_token_ratio=0.192218,
        accounting=_accounting(),
        adjudication_call_count=0,
    )
    assert score["reference_system_ids"] == [
        repair.SYSTEM_BASELINE,
        repair.SYSTEM_CANDIDATE,
    ]
    assert score["candidate_strict_full_field_macro_f1"] == 0.970588
    assert score["baseline_strict_full_field_macro_f1"] == 0.5
    assert score["passed"] is True
    no_signal = next(
        row for row in score["macro"]["cases"] if row["density_stratum"] == "no_signal"
    )
    assert no_signal["systems"][repair.SYSTEM_BASELINE]["strict_f1"] == 0.0
    assert no_signal["systems"][repair.SYSTEM_CANDIDATE]["strict_f1"] == 1.0


def test_unsupported_candidate_only_no_signal_event_fails_quality():
    pool, mapping = _frozen_pool_and_mapping()
    consensus = _maximal_consensus(pool, mapping, no_signal_supported=False)
    score = repair.score_shared_reference(
        pool=pool,
        mapping=mapping,
        consensus=consensus,
        production_token_ratio=0.192218,
        accounting=_accounting(),
        adjudication_call_count=0,
    )
    assert score["passed"] is False
    assert "candidate_strict_full_field_macro_f1_gte_0_97" in score["failed_checks"]


def test_runtime_lock_rejects_direct_request_mutation(tmp_path: Path):
    frozen = repair.freeze_run(tmp_path / "repair")
    repair.verify_runtime_lock(frozen["runtime_lock"])
    prompt = tmp_path / "repair" / "judge" / "prompt-ab.private.md"
    prompt.write_text(prompt.read_text(encoding="utf-8") + "drift", encoding="utf-8")
    with pytest.raises(repair.SharedReferenceRepairError):
        repair.verify_runtime_lock(frozen["runtime_lock"])


def test_supervisor_receipt_contract_fields(tmp_path: Path):
    frozen = repair.freeze_run(tmp_path / "repair")
    receipt = repair._receipt(
        root=frozen["root"],
        state="waiting",
        terminal_reason="test_wait",
        accounting=_accounting(0),
        score=None,
    )
    assert receipt["schema_version"] == "pif_semantic_plan_step_receipt_v1"
    assert receipt["thread_id"] == repair.THREAD_ID
    assert receipt["plan_epoch"] == 1
    assert receipt["step_id"] == repair.STEP_ID
    assert receipt["state"] == "waiting"
