from __future__ import annotations

import fcntl
import copy
import asyncio
import json
from collections import Counter
from pathlib import Path

import pytest

from research_factory import app_server_candidate_recurrent_fold_direct_reference as direct
from research_factory import app_server_candidate_ordered_recurrent_full_schema_fold_canary as canary
from research_factory import app_server_judge_v5_selection_v249_explicit_applicability as v249
from research_factory import app_server_llm_judge as judge


def _write(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _record(path: Path) -> dict:
    return direct._record(path)  # noqa: SLF001


def _event(claim: str, evidence: str) -> dict:
    return {
        "event_type": "claim",
        "claim_type": "descriptive",
        "claim_text": claim,
        "speaker_name": "Speaker",
        "speaker_role": "guest",
        "stance": "supportive",
        "certainty": "high",
        "target_concept": "tooling",
        "evidence": evidence,
    }


def _contract_tree(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[dict, Path]:
    source = tmp_path / "source.private.json"
    baseline = tmp_path / "baseline.private.json"
    canary_root = tmp_path / "real-layout-canary"
    normalized = canary_root / "combined-normalized-output.private.json"
    provenance = canary_root / "combined-evidence-provenance.private.json"
    diagnostics_path = canary_root / "combined-diagnostics.private.json"
    fold_path = canary_root / "fold-receipt.json"
    structural_gate_path = canary_root / "structural-gate.json"
    canary_lock = canary_root / "runtime-lock.json"
    canary_receipt = canary_root / "plan-step-receipt.json"
    canary_terminal = canary_root / "terminal.json"
    directive = tmp_path / "directive.json"
    plan = tmp_path / "plan.json"
    output_root = tmp_path / "evaluation"

    turns = canary.prepare_turns(canary.load_contract())
    frozen_raw = json.loads(
        next(v249.DEFAULT_OUTPUT_ROOT.glob("turns/*/output.private.json")).read_text(
            encoding="utf-8"
        )
    )
    raw_by_id = {str(row["segment_id"]): row for row in frozen_raw["segments"]}
    projected = []
    record_paths: list[Path] = []
    for turn_index, turn in enumerate(turns):
        turn_root = canary_root / "turns" / turn["turn_name"]
        raw_output = {
            "episode_id": turn["episode_id"],
            "segments": [copy.deepcopy(raw_by_id[turn["segment_id"]])],
        }
        if turn_index == 0:
            duplicate = copy.deepcopy(raw_output["segments"][0]["events"][0])
            raw_output["segments"][0]["events"].append(duplicate)
            start_id = duplicate["evidence_start_unit_id"]
            receipt = next(
                row
                for row in raw_output["segments"][0]["unit_receipts"]
                if row["unit_id"] == start_id
            )
            receipt["eligible_event_count"] += 1
        artifacts = {
            "input.private.json": turn["private_input"],
            "schema.json": turn["schema"],
            "projection-schema.json": turn["direct_schema"],
            "output.private.json": raw_output,
        }
        replayed = canary.project_turn_output(raw_output, turn)
        artifacts.update(
            {
                "normalized-output.private.json": replayed[0],
                "evidence-provenance.private.json": replayed[1],
                "diagnostics.private.json": {"segments": replayed[2]},
                "applicability-receipt.json": replayed[3],
            }
        )
        for basename, value in artifacts.items():
            path = turn_root / basename
            _write(path, value)
            record_paths.append(path)
        projected.append(replayed)
    folded, combined_provenance, combined_diagnostics, fold_receipt = (
        canary.fold_validated_outputs(projected[0], projected[1])
    )
    _write(normalized, folded)
    _write(provenance, combined_provenance)
    _write(diagnostics_path, {"segments": combined_diagnostics})
    _write(fold_path, fold_receipt)
    production_total = (
        canary.PRODUCTION_AMORTIZED_CONTEXT_TOKENS
        + 72_891 * canary.PRODUCTION_SCALE
    )
    production_ratio = round(
        production_total / canary.BASELINE_END_TO_END_TOKENS, 6
    )
    _write(
        structural_gate_path,
        {
            "schema_version": canary.SCHEMA_VERSION,
            "passed": True,
            "combined_extraction_total_tokens": 72_891,
            "production_amortized_total_tokens": production_total,
            "production_amortized_total_token_ratio": production_ratio,
        },
    )
    _write(canary_lock, {"schema_version": canary.LOCK_VERSION, "immutable": True})
    record_paths.extend(
        [
            normalized,
            provenance,
            diagnostics_path,
            fold_path,
            structural_gate_path,
            canary_lock,
        ]
    )
    source_segments = [
        copy.deepcopy(segment)
        for turn in turns
        for segment in turn["private_input"]["segments"]
    ]
    _write(
        source,
        {
            "schema_version": "test-v249-source-v1",
            "episode_id": turns[0]["episode_id"],
            "segments": source_segments,
        },
    )
    folded_by_id = {row["segment_id"]: row for row in folded["segments"]}
    _write(
        baseline,
        {
            "references": [
                {
                    "segment_id": segment["segment_id"],
                    "golden_output": {
                        "discourse_events": copy.deepcopy(
                            folded_by_id[segment["segment_id"]]["events"][:1]
                        )
                    },
                }
                for segment in source_segments
            ]
        },
    )
    canary_records = [_record(path) for path in record_paths]
    terminal_value = {
        "state": "passed",
        "schema_version": direct.RECEIPT_VERSION,
        "plan_epoch": 3,
        "step_id": canary.STEP_ID,
        "semantic_model_call_count": 2,
        "usage_status": "complete",
        "accounting_complete": True,
        "usage": {
            "input_tokens": 70_000,
            "cached_input_tokens": 0,
            "output_tokens": 2_891,
            "reasoning_output_tokens": 1_000,
            "total_tokens": 72_891,
        },
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "records": canary_records,
        "records_sha256": direct._record_digest(canary_records),  # noqa: SLF001
    }
    _write(canary_terminal, terminal_value)
    _write(canary_receipt, terminal_value)
    frozen = [
        {"role": "baseline_complete_output", **_record(baseline)},
        {"role": "blind_source_packet", **_record(source)},
        {"role": "recurrent_canary_terminal", **_record(canary_terminal)},
        {"role": "recurrent_canary_normalized_output", **_record(normalized)},
        {"role": "canary_plan_step_receipt", **_record(canary_receipt)},
    ]
    directive_value = {
        "schema_version": "adaptable-test-directive-v1",
        "plan_epoch": direct.PLAN_EPOCH,
        "step_id": direct.STEP_ID,
        "expected_receipt_path": str(output_root / "plan-step-receipt.json"),
        "production_amortized_total_token_ratio": production_ratio,
        "execution_contract": {
            "extraction_model_call_cap": 0,
            "semantic_judge_call_cap": 3,
            "semantic_retry_cap": 0,
            "adjudication_call_cap": 1,
            "adjudication_max_total_tokens": 100_000,
            "production_cost_projection": {
                "baseline_end_to_end_tokens": canary.BASELINE_END_TO_END_TOKENS,
                "production_amortized_context_tokens": canary.PRODUCTION_AMORTIZED_CONTEXT_TOKENS,
                "production_scale": canary.PRODUCTION_SCALE,
            },
            "judge_model": "gpt-5.5",
            "judge_reasoning_effort": "high",
            "judge_transport": "official_codex_app_server_stdio_managed_chatgpt_auth",
            "variants": ["ab", "ba"],
            "all_events_direct_to_full_event_judge": True,
            "pointwise_support_results_allowed": False,
            "support_filtering_allowed": False,
            "alignment_filtering_allowed": False,
            "deterministic_semantic_pruning_allowed": False,
        },
        "acceptance_contract": {
            "candidate_strict_full_field_macro_f1_min": 0.97,
            "candidate_must_be_noninferior_to_baseline": True,
            "production_amortized_total_token_ratio_max": 0.28,
            "quality_failure_is_rejected": True,
            "operational_failure_is_waiting": True,
        },
        "frozen_inputs": frozen,
    }
    _write(directive, directive_value)
    _write(
        plan,
        {
            "schema_version": "pif_evaluation_semantic_plan_v1",
            "plan_epoch": direct.PLAN_EPOCH,
            "state": "executable",
            "step": {
                "step_id": direct.STEP_ID,
                "state": "executable",
                "max_model_calls": 3,
                "max_total_tokens": 400_000,
                "directive_path": str(directive),
                "directive_sha256": direct._sha256_file(directive),  # noqa: SLF001
                "expected_receipt_path": str(output_root / "plan-step-receipt.json"),
            },
        },
    )
    monkeypatch.setattr(direct, "PLAN_PATH", plan.resolve())
    contract = direct.load_contract()
    return contract, output_root


def _systems(mapping: dict) -> dict[str, str]:
    return {
        witness["witness_id"]: witness["provenance"]["system_id"]
        for case in mapping["cases"]
        for witness in case["witnesses"]
    }


def _consensus(pool: dict, mapping: dict, *, reject_quiet: bool = False) -> dict:
    systems = _systems(mapping)
    cases = []
    for case in pool["cases"]:
        witnesses = [
            witness
            for side in ("a", "b")
            for witness in case[f"event_set_{side}"]
        ]
        baseline = [
            row["witness_id"]
            for row in witnesses
            if systems[row["witness_id"]] == direct.SYSTEM_BASELINE
        ]
        candidate = [
            row["witness_id"]
            for row in witnesses
            if systems[row["witness_id"]] == direct.SYSTEM_CANDIDATE
        ]
        support = [
            {
                "witness_id": row["witness_id"],
                "verdict": (
                    "unsupported"
                    if reject_quiet
                    and systems[row["witness_id"]] == direct.SYSTEM_CANDIDATE
                    else "supported"
                ),
                "evidence_spans": (
                    []
                    if reject_quiet
                    and systems[row["witness_id"]] == direct.SYSTEM_CANDIDATE
                    else [row["event"]["evidence"]]
                ),
            }
            for row in witnesses
        ]
        groups = [sorted(pair) for pair in zip(baseline, candidate)]
        groups.extend([value] for value in baseline[len(candidate) :])
        groups.extend([value] for value in candidate[len(baseline) :])
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
        "pool_sha256": "test",
        "cases": cases,
        "abstentions": {},
        "selection_admissible": True,
        "abstention_gate_applied": False,
    }


def _accounting(total_tokens: int = 100_000, calls: int = 2) -> dict:
    return {
        "usage_status": "complete",
        "accounting_complete": True,
        "measured_model_call_count": calls,
        "usage": {
            "input_tokens": total_tokens - 1,
            "cached_input_tokens": 0,
            "output_tokens": 1,
            "reasoning_output_tokens": 0,
            "total_tokens": total_tokens,
        },
        "turns": [],
    }


def _judge_outputs(pool: dict, mapping: dict) -> dict[str, dict]:
    desired = {
        row["case_id"]: row for row in _consensus(pool, mapping)["cases"]
    }
    variants = judge.build_judge_variants(pool)
    outputs = {}
    for name in ("ab", "ba"):
        cases = []
        for case in variants[name]["cases"]:
            truth = desired[case["case_id"]]
            cases.append(
                {
                    "case_id": case["case_id"],
                    "support_results": [
                        {
                            **row,
                            "rationale": "Synthetic full-event support.",
                        }
                        for row in truth["support_results"]
                    ],
                    "equivalence_groups": [
                        {
                            "witness_ids": group,
                            "rationale": "Synthetic full-event equivalence.",
                        }
                        for group in truth["equivalence_groups"]
                    ],
                    "alignments": [],
                    "unaligned_left_witness_ids": [
                        row["witness_id"] for row in case["event_set_a"]
                    ],
                    "unaligned_right_witness_ids": [
                        row["witness_id"] for row in case["event_set_b"]
                    ],
                }
            )
        outputs[name] = {"cases": cases}
        assert judge.validate_judge_output(outputs[name], variants[name]) == []
    return outputs


def test_all_event_lineage_uses_exact_multisets_and_no_prefilter(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    contract, _ = _contract_tree(tmp_path, monkeypatch)
    assert contract["canary_accounting"]["semantic_model_call_count"] == 2
    assert isinstance(
        json.loads(Path(contract["terminal_record"]["path"]).read_text())["records"],
        list,
    )
    pool, mapping = direct.build_complete_container(contract)
    fold = json.loads(
        Path(contract["terminal_artifacts"]["fold_receipt"]["path"]).read_text()
    )
    candidate_count = sum(len(case["event_set_b"]) for case in pool["cases"])
    assert candidate_count == fold["input_event_count"] == fold["output_event_count"]
    originals = [
        witness["provenance"]["original_event"]
        for case in mapping["cases"]
        for witness in case["witnesses"]
        if witness["provenance"]["system_id"] == direct.SYSTEM_CANDIDATE
    ]
    raw_hashes = [
        witness["provenance"]["raw_event_sha256"]
        for case in mapping["cases"]
        for witness in case["witnesses"]
        if witness["provenance"]["system_id"] == direct.SYSTEM_CANDIDATE
    ]
    assert len(originals) == candidate_count
    assert Counter(raw_hashes).most_common(1)[0][1] >= 2
    raw = json.loads(Path(contract["normalized_record"]["path"]).read_text())
    assert Counter(
        direct._event_sha256(event)  # noqa: SLF001
        for segment in raw["segments"]
        for event in segment["events"]
    ) == Counter(direct._event_sha256(event) for event in originals)  # noqa: SLF001


def test_replayed_projection_and_fold_bytes_are_mandatory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    contract, _ = _contract_tree(tmp_path, monkeypatch)
    normalized_path = Path(contract["normalized_record"]["path"])
    value = json.loads(normalized_path.read_text())
    value["segments"][0]["events"][0]["claim_text"] = "tampered after freeze"
    _write(normalized_path, value)
    with pytest.raises(
        direct.RecurrentFoldDirectReferenceError,
        match="replayed combined folded output bytes differ",
    ):
        direct.build_complete_container(contract)


def test_ab_ba_preserve_case_order_opaque_ids_and_full_payloads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pool, _ = direct.build_complete_container(_contract_tree(tmp_path, monkeypatch)[0])
    variants = judge.build_judge_variants(pool)
    assert [row["case_id"] for row in variants["ab"]["cases"]] == [
        row["case_id"] for row in variants["ba"]["cases"]
    ]
    for ab, ba in zip(variants["ab"]["cases"], variants["ba"]["cases"]):
        assert ab["event_set_a"] == ba["event_set_b"]
        assert ab["event_set_b"] == ba["event_set_a"]


def test_symmetric_union_pass_and_quality_rejection_are_distinct_from_waiting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pool, mapping = direct.build_complete_container(_contract_tree(tmp_path, monkeypatch)[0])
    passed = direct.score_shared_reference(
        pool=pool,
        mapping=mapping,
        consensus=_consensus(pool, mapping),
        production_token_ratio=0.27,
        accounting=_accounting(),
        adjudication_call_count=0,
    )
    assert passed["passed"] is True
    assert passed["candidate_strict_full_field_macro_f1"] == 1.0
    assert passed["reference_system_ids"] == [
        direct.SYSTEM_BASELINE,
        direct.SYSTEM_CANDIDATE,
    ]
    rejected = direct.score_shared_reference(
        pool=pool,
        mapping=mapping,
        consensus=_consensus(pool, mapping, reject_quiet=True),
        production_token_ratio=0.27,
        accounting=_accounting(),
        adjudication_call_count=0,
    )
    assert rejected["passed"] is False
    assert "candidate_strict_full_field_macro_f1_gte_0_97" in rejected["failed_checks"]
    with pytest.raises(direct.OperationalWaitingError):
        direct.score_shared_reference(
            pool=pool,
            mapping=mapping,
            consensus=_consensus(pool, mapping),
            production_token_ratio=0.27,
            accounting=_accounting(total_tokens=400_001),
            adjudication_call_count=0,
        )


def test_adjudication_preflight_uses_measured_remaining_tokens() -> None:
    assert direct._preflight_adjudication(  # noqa: SLF001
        _accounting(total_tokens=250_000), token_max=100_000
    ) == 150_000
    with pytest.raises(direct.OperationalWaitingError):
        direct._preflight_adjudication(  # noqa: SLF001
            _accounting(total_tokens=350_001), token_max=50_000
        )
    with pytest.raises(direct.OperationalWaitingError):
        direct._preflight_adjudication(  # noqa: SLF001
            _accounting(total_tokens=10_000, calls=3), token_max=50_000
        )
    assert direct._enforce_adjudication_usage(  # noqa: SLF001
        _accounting(total_tokens=99_999, calls=1),
        measured_ab_ba_total=250_000,
        token_max=100_000,
    ) == 99_999
    with pytest.raises(direct.OperationalWaitingError):
        direct._enforce_adjudication_usage(  # noqa: SLF001
            _accounting(total_tokens=100_001, calls=1),
            measured_ab_ba_total=250_000,
            token_max=100_000,
        )
    with pytest.raises(direct.OperationalWaitingError):
        direct._enforce_adjudication_usage(  # noqa: SLF001
            _accounting(total_tokens=90_000, calls=1),
            measured_ab_ba_total=320_001,
            token_max=100_000,
        )


def test_process_lock_runtime_hashes_receipt_integrity_and_no_false_winner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, root = _contract_tree(tmp_path, monkeypatch)
    frozen = direct.freeze_run(root)
    lock = direct.verify_runtime_lock(frozen["runtime_lock"])
    assert lock["capacity_checkpoint_records_excluded"] is True
    assert not any(
        Path(row["path"]).name.startswith("capacity")
        for key in ("runtime_files", "source_records", "request_records")
        for row in lock[key]
    )
    lock_path = root / direct.PROCESS_LOCK_NAME
    with lock_path.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(direct.OperationalWaitingError):
            direct.freeze_run(root)
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    receipt = direct._receipt(  # noqa: SLF001
        root=root,
        state="waiting",
        terminal_reason="synthetic_operational_wait",
        accounting=direct._zero_accounting(),  # noqa: SLF001
        score=None,
    )
    direct._write_immutable_json(root / "plan-step-receipt.json", receipt)  # noqa: SLF001
    direct._write_immutable_json(root / "terminal.json", receipt)  # noqa: SLF001
    verified = direct.verify_receipt(root)
    assert verified["winner_frozen"] is False
    assert verified["development_winner_frozen"] is False
    assert verified["holdout"] is False
    assert verified["production"] is False
    (root / "attempt-spec.json").write_text("{}\n", encoding="utf-8")
    with pytest.raises(direct.RecurrentFoldDirectReferenceError):
        direct.verify_receipt(root)


def _write_sidecar(path: Path, total_tokens: int) -> None:
    _write(
        path,
        {
            "state": "completed",
            "status": "completed",
            "usage_status": "measured",
            "usage_complete": True,
            "auth_type": "chatgpt",
            "model": direct.MODEL,
            "effort": direct.EFFORT,
            "usage": {
                "input_tokens": total_tokens - 1_000,
                "cached_input_tokens": 0,
                "output_tokens": 1_000,
                "reasoning_output_tokens": 100,
                "total_tokens": total_tokens,
            },
        },
    )


def test_quality_receipt_recomputes_score_accounting_and_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    contract, root = _contract_tree(tmp_path, monkeypatch)
    direct.freeze_run(root)
    pool = json.loads((root / "shared-witness-pool.private.json").read_text())
    mapping = json.loads((root / "private-witness-mapping.private.json").read_text())
    outputs = _judge_outputs(pool, mapping)
    base_consensus = judge.combine_judge_consensus(pool, outputs)
    consensus = direct._normalized_consensus_metadata(base_consensus)  # noqa: SLF001
    judge_root = root / "judge"
    _write(judge_root / "output-ab.private.json", outputs["ab"])
    _write(judge_root / "output-ba.private.json", outputs["ba"])
    _write(judge_root / "consensus.private.json", base_consensus)
    _write(root / "consensus.private.json", consensus)
    _write_sidecar(judge_root / "sidecars" / "ab.json", 50_000)
    _write_sidecar(judge_root / "sidecars" / "ba.json", 50_000)
    accounting = direct._aggregate_usage(  # noqa: SLF001
        [judge_root / "sidecars" / "ab.json", judge_root / "sidecars" / "ba.json"]
    )
    _write(
        judge_root / "report.json",
        {
            "schema_version": judge.JUDGE_RUN_VERSION,
            "state": "completed",
            "model": direct.MODEL,
            "reasoning_effort": direct.EFFORT,
            "variant_count": 2,
            "accounting_complete": True,
            "usage_status": "complete",
            "usage": accounting["usage"],
            "consensus_sha256": direct._sha256_file(  # noqa: SLF001
                judge_root / "consensus.private.json"
            ),
            "selection_admissible": True,
        },
    )
    score = direct.score_shared_reference(
        pool=pool,
        mapping=mapping,
        consensus=consensus,
        production_token_ratio=contract["production_token_ratio"],
        accounting=accounting,
        adjudication_call_count=0,
    )
    _write(root / "shared-reference-score.json", score)
    receipt = direct._receipt(  # noqa: SLF001
        root=root,
        state="passed",
        terminal_reason="recurrent_fold_direct_reference_quality_passed",
        accounting=accounting,
        score=score,
    )
    _write(root / "plan-step-receipt.json", receipt)
    _write(root / "terminal.json", receipt)
    assert direct.verify_receipt(root)["state"] == "passed"

    receipt["state"] = "rejected"
    _write(root / "plan-step-receipt.json", receipt)
    _write(root / "terminal.json", receipt)
    with pytest.raises(
        direct.RecurrentFoldDirectReferenceError,
        match="exact passed/rejected quality projection",
    ):
        direct.verify_receipt(root)

    substituted_consensus = direct._normalized_consensus_metadata(  # noqa: SLF001
        _consensus(pool, mapping, reject_quiet=True)
    )
    substituted_score = direct.score_shared_reference(
        pool=pool,
        mapping=mapping,
        consensus=substituted_consensus,
        production_token_ratio=contract["production_token_ratio"],
        accounting=accounting,
        adjudication_call_count=0,
    )
    assert substituted_score["passed"] is False
    _write(root / "consensus.private.json", substituted_consensus)
    _write(root / "shared-reference-score.json", substituted_score)
    substituted_receipt = direct._receipt(  # noqa: SLF001
        root=root,
        state="rejected",
        terminal_reason="synthetic_substituted_quality_consensus",
        accounting=accounting,
        score=substituted_score,
    )
    _write(root / "plan-step-receipt.json", substituted_receipt)
    _write(root / "terminal.json", substituted_receipt)
    with pytest.raises(
        direct.RecurrentFoldDirectReferenceError,
        match="differs from bound AB/BA/adjudication reconstruction",
    ):
        direct.verify_receipt(root)

    _write(root / "consensus.private.json", consensus)
    ratio_score = copy.deepcopy(score)
    ratio_score["production_amortized_total_token_ratio"] = 0.01
    _write(root / "shared-reference-score.json", ratio_score)
    ratio_receipt = direct._receipt(  # noqa: SLF001
        root=root,
        state="passed",
        terminal_reason="synthetic_substituted_ratio",
        accounting=accounting,
        score=ratio_score,
    )
    _write(root / "plan-step-receipt.json", ratio_receipt)
    _write(root / "terminal.json", ratio_receipt)
    with pytest.raises(
        direct.RecurrentFoldDirectReferenceError,
        match="score does not recompute exactly",
    ):
        direct.verify_receipt(root)


def test_integrity_error_is_not_downgraded_to_waiting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, root = _contract_tree(tmp_path, monkeypatch)

    async def corrupt_runner(**_kwargs):
        raise direct.RecurrentFoldDirectReferenceError("synthetic integrity defect")

    with pytest.raises(direct.RecurrentFoldDirectReferenceError, match="integrity defect"):
        asyncio.run(direct.run(output_dir=root, judge_runner=corrupt_runner))
    assert not (root / "plan-step-receipt.json").exists()
