from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from research_factory import app_server_candidate_true_full_output_repair as repair
from research_factory import app_server_llm_judge as judge


def _pool_and_mapping():
    contract = repair.load_contract()
    return repair.build_shared_pool(contract)


def _systems(mapping):
    return {
        witness["witness_id"]: witness["provenance"]["system_id"]
        for case in mapping["cases"]
        for witness in case["witnesses"]
    }


def _consensus(pool, mapping, *, no_signal_supported: bool = True):
    systems = _systems(mapping)
    cases = []
    for case in pool["cases"]:
        witnesses = [
            witness
            for side in ("a", "b")
            for witness in case[f"event_set_{side}"]
        ]
        baseline = sorted(
            row["witness_id"]
            for row in witnesses
            if systems[row["witness_id"]] == repair.SYSTEM_BASELINE
        )
        candidate = sorted(
            row["witness_id"]
            for row in witnesses
            if systems[row["witness_id"]] == repair.SYSTEM_CANDIDATE
        )
        support = []
        for witness in witnesses:
            supported = no_signal_supported or bool(baseline)
            support.append(
                {
                    "witness_id": witness["witness_id"],
                    "verdict": "supported" if supported else "unsupported",
                    "evidence_spans": [witness["event"]["evidence"]] if supported else [],
                }
            )
        groups = [sorted(pair) for pair in zip(baseline, candidate)]
        groups.extend([item] for item in baseline[len(candidate) :])
        groups.extend([item] for item in candidate[len(baseline) :])
        cases.append(
            {
                "case_id": case["case_id"],
                "status": "agreed",
                "support_results": support,
                "equivalence_groups": groups,
                "partition_abstained_witness_ids": [],
                "alignment_results": [],
                "unaligned_a_witness_ids": baseline,
                "unaligned_b_witness_ids": candidate,
                "alignment_abstained_witness_ids": [],
            }
        )
    return {
        "schema_version": judge.JUDGE_CONSENSUS_VERSION,
        "pool_sha256": "test-only",
        "cases": cases,
        "abstentions": {},
        "selection_admissible": True,
        "abstention_gate_applied": False,
    }


def _accounting(total_tokens: int = 100_000, calls: int = 2):
    return {
        "usage_status": "complete",
        "accounting_complete": True,
        "measured_model_call_count": calls,
        "usage": {
            "input_tokens": total_tokens - 1,
            "cached_input_tokens": 0,
            "output_tokens": 1,
            "reasoning_output_tokens": 1,
            "total_tokens": total_tokens,
        },
        "turns": [],
    }


def test_contract_builds_true_full_sixty_submission_pool() -> None:
    contract = repair.load_contract()
    pool, mapping = repair.build_shared_pool(contract)
    variants = judge.build_judge_variants(pool)
    assert contract["bundle_path"] == repair.DEFAULT_BUNDLE_PATH
    assert contract["production_token_ratio"] == 0.192218
    assert judge.validate_shared_witness_pool(pool) == []
    assert [case["case_provenance"]["density_stratum"] for case in mapping["cases"]] == [
        "dense",
        "no_signal",
    ]
    assert sum(
        len(case["event_set_a"]) + len(case["event_set_b"])
        for case in pool["cases"]
    ) == 60
    assert sum(len(case["event_set_a"]) for case in pool["cases"]) == 27
    assert sum(len(case["event_set_b"]) for case in pool["cases"]) == 33
    assert all(
        witness["event"]["submitted_evidence_exact"] is True
        for case in pool["cases"]
        for side in ("a", "b")
        for witness in case[f"event_set_{side}"]
    )
    assert [row["case_id"] for row in variants["ab"]["cases"]] == [
        row["case_id"] for row in variants["ba"]["cases"]
    ]
    rendered_pool = json.dumps(pool, sort_keys=True)
    assert repair.SYSTEM_BASELINE not in rendered_pool
    assert repair.SYSTEM_CANDIDATE not in rendered_pool
    assert set(_systems(mapping).values()) == {
        repair.SYSTEM_BASELINE,
        repair.SYSTEM_CANDIDATE,
    }


def test_symmetric_shared_union_scores_candidate_macro_and_empty_baseline_case() -> None:
    pool, mapping = _pool_and_mapping()
    consensus = _consensus(pool, mapping)
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
    assert score["candidate_strict_full_field_macro_f1"] == 1.0
    assert score["baseline_strict_full_field_macro_f1"] == 0.457627
    assert score["passed"] is True
    no_signal = next(
        row for row in score["macro"]["cases"] if row["density_stratum"] == "no_signal"
    )
    assert no_signal["systems"][repair.SYSTEM_BASELINE]["strict_precision"] == 0.0
    assert no_signal["systems"][repair.SYSTEM_BASELINE]["strict_recall"] == 0.0
    assert no_signal["systems"][repair.SYSTEM_BASELINE]["strict_f1"] == 0.0
    assert no_signal["systems"][repair.SYSTEM_CANDIDATE]["strict_f1"] == 1.0


def test_only_quality_result_is_rejected_and_cap_failure_waits() -> None:
    pool, mapping = _pool_and_mapping()
    quality = repair.score_shared_reference(
        pool=pool,
        mapping=mapping,
        consensus=_consensus(pool, mapping, no_signal_supported=False),
        production_token_ratio=0.192218,
        accounting=_accounting(),
        adjudication_call_count=0,
    )
    assert quality["passed"] is False
    assert "candidate_strict_full_field_macro_f1_gte_0_97" in quality["failed_checks"]
    with pytest.raises(repair.OperationalWaitingError):
        repair.score_shared_reference(
            pool=pool,
            mapping=mapping,
            consensus=_consensus(pool, mapping),
            production_token_ratio=0.192218,
            accounting=_accounting(total_tokens=400_001),
            adjudication_call_count=0,
        )


def test_adjudication_preflight_requires_measured_remaining_reserve() -> None:
    assert repair._preflight_adjudication(  # noqa: SLF001
        _accounting(total_tokens=250_000), token_reserve=100_000
    ) == 150_000
    with pytest.raises(repair.OperationalWaitingError):
        repair._preflight_adjudication(  # noqa: SLF001
            _accounting(total_tokens=350_001), token_reserve=50_000
        )
    with pytest.raises(repair.OperationalWaitingError):
        repair._preflight_adjudication(  # noqa: SLF001
            _accounting(total_tokens=10_000, calls=3), token_reserve=50_000
        )


def test_consensus_metadata_is_recomputed_after_neutral_adjudication() -> None:
    pool, mapping = _pool_and_mapping()
    consensus = _consensus(pool, mapping)
    consensus["abstentions"] = {
        "support": {"numerator": 999, "denominator": 1, "rate": 999.0}
    }
    normalized = repair._normalized_consensus_metadata(consensus)  # noqa: SLF001
    assert normalized["abstentions"]["support"] == {
        "numerator": 0,
        "denominator": 60,
        "rate": 0.0,
    }
    assert normalized["abstentions"]["equivalence_partition"]["numerator"] == 0
    assert normalized["selection_admissible"] is True


def test_freeze_uses_advisory_lock_and_excludes_capacity_records(tmp_path: Path) -> None:
    root = tmp_path / "epoch2"
    frozen = repair.freeze_run(root)
    lock = repair.verify_runtime_lock(frozen["runtime_lock"])
    assert lock["capacity_checkpoint_records_excluded"] is True
    assert not any(
        Path(row["path"]).name.startswith("capacity")
        for key in ("runtime_files", "source_records", "request_records")
        for row in lock[key]
    )
    with repair._advisory_process_lock(root):  # noqa: SLF001
        with pytest.raises(repair.OperationalWaitingError):
            repair.freeze_run(root)


def test_operational_exception_writes_hash_verified_waiting_receipt(tmp_path: Path) -> None:
    root = tmp_path / "epoch2"
    calls = 0

    async def no_live_runner(**_kwargs):
        nonlocal calls
        calls += 1
        raise RuntimeError("synthetic transport stop")

    receipt = asyncio.run(repair.run(output_dir=root, judge_runner=no_live_runner))
    assert calls == 1
    assert receipt["state"] == "waiting"
    assert receipt["development_winner_frozen"] is False
    assert receipt["holdout_authorized"] is False
    assert receipt["production_mutated"] is False
    resumed = asyncio.run(repair.run(output_dir=root, judge_runner=no_live_runner))
    assert resumed == receipt
    assert calls == 1
    lock = root / "runtime-lock.json"
    lock.write_text(lock.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    with pytest.raises(repair.TrueFullOutputRepairError):
        repair.verify_receipt(root)
