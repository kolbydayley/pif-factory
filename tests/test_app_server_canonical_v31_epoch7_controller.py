from __future__ import annotations

import asyncio
import copy
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from research_factory import app_server_canonical_v31_epoch7_controller as controller
from research_factory import app_server_canonical_v31_development_matrix_runtime as runtime
from tests import test_app_server_canonical_v31_development_matrix as matrix_fixtures
from tests import test_app_server_canonical_v31_development_matrix_runtime as runtime_fixtures


_REAL_EXTRACTION_RUNTIME = runtime.run_canonical_v31_development_matrix_runtime


@pytest.fixture(autouse=True)
def _official_live_fixture_client(monkeypatch: pytest.MonkeyPatch):
    factory = runtime_fixtures.LiveFixtureFactory()
    monkeypatch.setattr(controller.matrix.adapter, "_client_factory", factory)
    return factory


def _write(path: Path, value: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    )
    return path


def _checklist(**decisions: str) -> list[dict[str, str]]:
    return [
        {
            "field": field,
            "decision": decisions.get(field, "same"),
            "rationale": f"fixture decision for {field}",
        }
        for field in controller._TRUTH_CONDITIONAL_CHECKLIST_FIELDS
    ]


def _support_rows(
    source_by_witness: dict[str, str],
    *,
    verdicts: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    verdicts = verdicts or {}
    rows: list[dict[str, Any]] = []
    for witness_id, source in source_by_witness.items():
        verdict = verdicts.get(witness_id, "supported")
        if verdict == "supported":
            entailment = "entailed"
            evidence = [{"text": source, "start": 0, "end": len(source)}]
        elif verdict == "unsupported":
            entailment = "not_established"
            evidence = []
        else:
            entailment = "abstain"
            evidence = []
        rows.append(
            {
                "witness_id": witness_id,
                "verdict": verdict,
                "material_claim_entailment": entailment,
                "evidence_spans": evidence,
                "rationale": f"fixture support decision for {witness_id}",
            }
        )
    return rows


def _fixture(tmp_path: Path) -> dict[str, Any]:
    project = tmp_path / "project"
    data = matrix_fixtures._fixture(project / "fixture")
    episodes_path = _write(project / "inputs" / "episodes.json", data["episodes"])
    now = datetime.now(timezone.utc).replace(microsecond=0)
    controller_root = project / "work" / "epoch7-controller"
    extraction_root = project / "work" / "epoch7-extraction"
    planned = controller.freeze_epoch7_plan(
        controller_root=controller_root,
        extraction_root=extraction_root,
        manifest_path=Path(data["manifest_record"]["path"]),
        episodes_path=episodes_path,
        capacity_policy_path=Path(data["capacity_record"]["path"]),
        operator_authorization_id="kolby-epoch7-test-authorization",
        issued_at=(now - timedelta(minutes=1)).isoformat(),
        expires_at=(now + timedelta(hours=1)).isoformat(),
        project_root=project,
    )
    return {
        "project": project,
        "data": data,
        "episodes_path": episodes_path,
        "controller_root": controller_root,
        "extraction_root": extraction_root,
        "authorization_id": "kolby-epoch7-test-authorization",
        "planned": planned,
        "client_factory": controller.matrix.adapter._client_factory,
    }


async def _fake_extraction_receipt(bundle: dict[str, Any]) -> dict[str, Any]:
    loaded = controller.load_epoch7_controller(
        bundle["controller_root"], project_root=bundle["project"]
    )
    path = Path(loaded["contract"]["expected_extraction_receipt_path"])
    if path.is_file():
        return controller._load_object(path, label="fixture extraction receipt")
    result = await _REAL_EXTRACTION_RUNTIME(
        output_root=loaded["extraction_root"],
        semantic_plan_path=loaded["records"]["semantic_plan"],
        thread_id=controller.supervisor.TARGET_THREAD_ID,
        project_root=loaded["project_root"],
        manifest_path=loaded["records"]["manifest"],
        manifest_sha256=loaded["contract"]["manifest"]["sha256"],
        episodes=loaded["episodes"],
        capacity_policy_path=loaded["records"]["capacity_policy"],
        capacity_policy_sha256=loaded["contract"]["capacity_policy"][
            "sha256"
        ],
        future_plan_binding=loaded["future_binding"],
        client_factory=bundle["client_factory"],
    )
    assert result["receipt"]["state"] == "passed"
    return result["receipt"]


def _prepare_quality_handoff(
    bundle: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> dict[str, Any]:
    async def fake_runtime(**kwargs: Any) -> dict[str, Any]:
        await _fake_extraction_receipt(bundle)
        loaded = controller.load_epoch7_controller(
            bundle["controller_root"], project_root=bundle["project"]
        )
        return {
            "receipt": controller._load_object(
                Path(loaded["contract"]["expected_extraction_receipt_path"]),
                label="fixture extraction receipt",
            ),
            "semantic_calls_started_by_this_invocation": loaded["limits"][
                "exact_model_call_cap"
            ],
        }

    monkeypatch.setattr(
        controller.extraction_runtime,
        "run_canonical_v31_development_matrix_runtime",
        fake_runtime,
    )
    receipt = asyncio.run(
        controller.execute_epoch7(
            controller_root=bundle["controller_root"],
            operator_authorization_id=bundle["authorization_id"],
            project_root=bundle["project"],
        )
    )
    assert receipt["state"] == "passed"
    return controller.verify_epoch7(
        bundle["controller_root"], project_root=bundle["project"]
    )


def _quality_evidence_fixture(
    bundle: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    *,
    sources: dict[str, str] | None = None,
    pairs: list[dict[str, str]] | None = None,
    alignment_rows: list[dict[str, Any]] | None = None,
    scoring_manifest: dict[str, Any] | None = None,
) -> dict[str, Any]:
    handoff = _prepare_quality_handoff(bundle, monkeypatch)
    quality_root = bundle["project"] / "work" / "epoch7-quality-evidence"
    sources = sources or {
        "witness-a": "Exact source evidence A.",
        "witness-b": "Exact source evidence B.",
    }
    scoring_manifest = scoring_manifest or _quality_scoring_manifest(list(sources))
    scoring_record = controller._write_immutable_json(
        quality_root / controller.QUALITY_SCORING_MANIFEST_FILENAME,
        scoring_manifest,
    )
    witness_ids = list(sources)
    support_ab = _support_rows(sources)
    support_ba = copy.deepcopy(support_ab)
    support_consensus = controller.reconcile_balanced_support_decisions(
        support_ab,
        support_ba,
        expected_witness_ids=witness_ids,
        source_by_witness=sources,
    )
    support_payload = {
        "schema_version": controller.QUALITY_SUPPORT_CONSENSUS_VERSION,
        "expected_witness_ids": witness_ids,
        "source_by_witness_sha256": controller._sha256_bytes(
            controller._canonical_json(sources).encode("ascii")
        ),
        "decisions": support_consensus,
    }
    support_record = controller._write_immutable_json(
        quality_root / "support-consensus.json", support_payload
    )
    support_sha = controller._sha256_bytes(
        controller._canonical_json(support_payload).encode("ascii")
    )
    support_verdicts = {
        row["witness_id"]: row["verdict"] for row in support_consensus
    }
    pairs = pairs or [
        {
            "pair_id": "pair-1",
            "left_witness_id": "witness-a",
            "right_witness_id": "witness-b",
        }
    ]
    alignment_ab = alignment_rows or [
        {
            **pair,
            "relation": "equivalent",
            "checklist": _checklist(),
            "rationale": "The fixture events are equivalent.",
        }
        for pair in pairs
    ]
    alignment_ba = copy.deepcopy(alignment_ab)
    alignment_consensus = controller.reconcile_balanced_alignment_decisions(
        alignment_ab,
        alignment_ba,
        expected_pairs=pairs,
        frozen_support_verdicts=support_verdicts,
    )
    alignment_payload = {
        "schema_version": controller.QUALITY_ALIGNMENT_CONSENSUS_VERSION,
        "support_consensus_sha256": support_sha,
        "expected_pairs": pairs,
        "decisions": alignment_consensus,
    }
    alignment_record = controller._write_immutable_json(
        quality_root / "alignment-consensus.json", alignment_payload
    )

    decision_payloads = [
        {
            "schema_version": controller.QUALITY_TURN_DECISIONS_VERSION,
            "stage": "support_first",
            "orientation": orientation,
            "case_order_sha256": controller._sha256_bytes(
                controller._canonical_json(witness_ids).encode("ascii")
            ),
            "expected_witness_ids": witness_ids,
            "source_by_witness": sources,
            "decisions": decisions,
        }
        for orientation, decisions in (("ab", support_ab), ("ba", support_ba))
    ]
    decision_payloads.extend(
        {
            "schema_version": controller.QUALITY_TURN_DECISIONS_VERSION,
            "stage": "alignment",
            "orientation": orientation,
            "case_order_sha256": controller._sha256_bytes(
                controller._canonical_json(pairs).encode("ascii")
            ),
            "expected_pairs": pairs,
            "frozen_support_verdicts": support_verdicts,
            "support_consensus_sha256": support_sha,
            "decisions": decisions,
        }
        for orientation, decisions in (("ab", alignment_ab), ("ba", alignment_ba))
    )

    turns: list[dict[str, Any]] = []
    usages: list[dict[str, int]] = []
    walls: list[float] = []
    protocol_sha = controller._record(
        controller.matrix.adapter.codex_app_server.PROTOCOL_SCHEMA_PATH
    )["sha256"]
    for index, ((stage, orientation), parsed) in enumerate(
        zip(controller._QUALITY_TURN_ORDER, decision_payloads)
    ):
        turn_root = quality_root / "turns" / f"{stage}-{orientation}"
        prompt = f"quality prompt {stage} {orientation}".encode("ascii")
        base = b"quality base instructions"
        schema = (
            controller.pointwise_support_decision_schema()
            if stage == "support_first"
            else controller.alignment_decision_schema()
        )
        raw_output = controller._canonical_json(parsed).encode("ascii")
        paths: dict[str, Path] = {
            "prompt": turn_root / "prompt.private.md",
            "base_instructions": turn_root / "base-instructions.private.md",
            "raw_output": turn_root / "output.private.json",
        }
        for role, raw in (
            ("prompt", prompt),
            ("base_instructions", base),
            ("raw_output", raw_output),
        ):
            paths[role].parent.mkdir(parents=True, exist_ok=True)
            paths[role].write_bytes(raw)
        json_payloads = {
            "capacity_request": {"stage": stage, "orientation": orientation},
            "capacity_measurement": {
                "managed_chatgpt_auth_verified": True,
                "rate_limit_reached_type": None,
            },
            "capacity_admission": {
                "cleared_for_semantic_turn": True,
                "managed_chatgpt_auth_verified": True,
                "rate_limit_reached_type": None,
            },
            "request": {
                "schema_version": controller.QUALITY_TURN_REQUEST_VERSION,
                "stage": stage,
                "orientation": orientation,
                "quality_handoff": handoff["handoff_record"],
                "quality_runtime_evidence_contract_sha256": handoff["handoff"][
                    "quality_runtime_evidence_contract_sha256"
                ],
                "quality_scoring_manifest": scoring_record,
                "decision_case_order_sha256": parsed["case_order_sha256"],
                "frozen_support_consensus_sha256": (
                    support_record["sha256"] if stage == "alignment" else None
                ),
                "prompt_sha256": controller._sha256_bytes(prompt),
                "base_instructions_sha256": controller._sha256_bytes(base),
                "output_schema_sha256": controller._sha256_bytes(
                    controller._canonical_json(schema).encode("ascii")
                ),
                "managed_chatgpt_auth_only": True,
                "official_persistent_codex_app_server_only": True,
                "semantic_retry_count": 0,
                "deterministic_semantic_matching": False,
                "semantic_pruning": False,
                "holdout_authorized": False,
                "production_mutation_allowed": False,
            },
            "output_schema": schema,
            "parsed_decisions": parsed,
        }
        for role, payload in json_payloads.items():
            paths[role] = turn_root / f"{role}.json"
            controller._write_immutable_json(paths[role], payload)
        usage = {
            "input_tokens": 1000 + index,
            "cached_input_tokens": 100,
            "output_tokens": 200,
            "reasoning_output_tokens": 50,
            "total_tokens": 1200 + index,
        }
        wall = 10.0 + index
        thread_id = f"quality-thread-{index}"
        turn_id = f"quality-turn-{index}"
        sidecar = {
            "schema_version": "pif_codex_app_server_turn_v2",
            "state": "completed",
            "status": "completed",
            "client_version": (
                controller.matrix.adapter.codex_app_server.APP_SERVER_CLIENT_VERSION
            ),
            "cli_version": (
                controller.matrix.adapter.codex_app_server.PINNED_CODEX_CLI_VERSION
            ),
            "protocol_schema_sha256": protocol_sha,
            "transport": "stdio",
            "auth_type": "chatgpt",
            "plan_type": "pro",
            "thread_mode": "new_thread",
            "model": "gpt-5.5",
            "effort": "high",
            "thread_id": thread_id,
            "turn_id": turn_id,
            "prompt_sha256": controller._sha256_bytes(prompt),
            "prompt_bytes": len(prompt),
            "base_instructions_sha256": controller._sha256_bytes(base),
            "base_instructions_bytes": len(base),
            "output_schema_sha256": controller._sha256_bytes(
                controller._canonical_json(schema).encode("ascii")
            ),
            "output_schema_bytes": len(
                controller._canonical_json(schema).encode("ascii")
            ),
            "output_path": str(paths["raw_output"].resolve()),
            "output_sha256": controller._sha256_bytes(raw_output),
            "usage_status": "measured",
            "usage_complete": True,
            "usage": usage,
            "thread_total_usage": usage,
            "wall_elapsed_seconds": wall,
            "synthetic_debug_errors": False,
            "recovery_reran_model": False,
            "app_server_user_agent": "fixture-managed-app-server",
        }
        paths["sidecar"] = turn_root / "sidecar.json"
        controller._write_immutable_json(paths["sidecar"], sidecar)
        terminal = {
            "state": "completed",
            "thread_id": thread_id,
            "turn_id": turn_id,
            "semantic_model_call_count": 1,
            "semantic_retry_count": 0,
            "production_mutated": False,
        }
        paths["terminal"] = turn_root / "terminal.json"
        controller._write_immutable_json(paths["terminal"], terminal)
        artifacts = {
            role: controller._record(paths[role], allowed_root=quality_root)
            for role in controller._QUALITY_TURN_ARTIFACT_ROLES
        }
        turns.append(
            {
                "stage": stage,
                "orientation": orientation,
                "model": "gpt-5.5",
                "effort": "high",
                "thread_id": thread_id,
                "turn_id": turn_id,
                "artifacts": artifacts,
                "usage": usage,
                "wall_elapsed_seconds": wall,
            }
        )
        usages.append(usage)
        walls.append(wall)

    receipt = {
        "schema_version": controller.QUALITY_EVIDENCE_RECEIPT_VERSION,
        "state": "completed_four_turn_evidence_quality_not_scored",
        "thread_id": controller.supervisor.TARGET_THREAD_ID,
        "plan_epoch": controller.PLAN_EPOCH,
        "quality_handoff": handoff["handoff_record"],
        "quality_runtime_evidence_contract_sha256": handoff["handoff"][
            "quality_runtime_evidence_contract_sha256"
        ],
        "quality_scoring_manifest": scoring_record,
        "turn_order": [
            {"stage": stage, "orientation": orientation}
            for stage, orientation in controller._QUALITY_TURN_ORDER
        ],
        "turns": turns,
        "support_consensus": support_record,
        "alignment_consensus": alignment_record,
        "aggregate_usage": controller._sum_usage(usages),
        "aggregate_wall_elapsed_seconds": sum(walls),
        "semantic_model_call_count": 4,
        "semantic_retry_count": 0,
        "deterministic_score_recomputation_required": True,
        "quality_gate_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }
    receipt["receipt_sha256"] = controller._receipt_checksum(receipt)
    receipt_path = quality_root / controller.QUALITY_EVIDENCE_RECEIPT_FILENAME
    controller._write_immutable_json(receipt_path, receipt)
    return {
        "quality_root": quality_root,
        "receipt_path": receipt_path,
        "receipt": receipt,
        "support_payload": support_payload,
        "alignment_payload": alignment_payload,
        "scoring_manifest": scoring_manifest,
        "sources": sources,
        "pairs": pairs,
    }


def _quality_scoring_manifest(
    witness_ids: list[str],
    *,
    system_ids: list[str] | None = None,
) -> dict[str, Any]:
    systems = system_ids or ["baseline", "candidate"]
    assert len(witness_ids) == len(systems)
    fields = list(controller._TRUTH_CONDITIONAL_CHECKLIST_FIELDS)
    return {
        "schema_version": controller.QUALITY_SCORING_MANIFEST_VERSION,
        "state": "frozen_before_quality_turns",
        "case_order": ["case-1"],
        "system_order": list(dict.fromkeys(systems)),
        "witnesses": [
            {
                "witness_id": witness_id,
                "case_id": "case-1",
                "system_id": system_id,
                "submitted_evidence_exact": True,
            }
            for witness_id, system_id in zip(witness_ids, systems)
        ],
        "one_shared_augmented_reference": True,
        "reference_system_ids": None,
        "semantic_fields": fields,
        "semantic_fields_sha256": controller._sha256_bytes(
            controller._canonical_json(fields).encode("ascii")
        ),
        "deterministic_semantic_matching": False,
        "semantic_pruning": False,
    }


def _quality_cost_fixture(
    bundle: dict[str, Any],
    quality_root: Path,
    *,
    baseline_tokens: int = 1_000_000,
    context_tokens: int = 1_000,
    baseline_segments: int = 60,
) -> dict[str, Any]:
    project = bundle["project"]
    sidecar_path = project / "runs" / "context_usage" / "context-fixture.json"
    controller._write_immutable_json(
        sidecar_path,
        {
            "schema_version": "fixture-context-sidecar-v1",
            "usage": {
                "input_tokens": 200,
                "cached_input_tokens": 0,
                "output_tokens": 0,
                "reasoning_output_tokens": 0,
                "total_tokens": 200,
            },
        },
    )
    baseline_usage = {
        "input_tokens": baseline_tokens,
        "cached_input_tokens": 0,
        "output_tokens": 0,
        "reasoning_output_tokens": 0,
        "total_tokens": baseline_tokens,
    }
    context_usage = {
        "input_tokens": 200,
        "cached_input_tokens": 0,
        "output_tokens": 0,
        "reasoning_output_tokens": 0,
        "total_tokens": 200,
    }
    production_context_usage = {
        "input_tokens": context_tokens,
        "cached_input_tokens": 0,
        "output_tokens": 0,
        "reasoning_output_tokens": 0,
        "total_tokens": context_tokens,
    }
    phase_path = project / "historical" / "phase-one-report.json"
    controller._write_immutable_json(
        phase_path,
        {
            "schema_version": "windowed_paired_phase_one_v1",
            "requested_segments": baseline_segments,
            "baseline": {
                "ok": True,
                "accounting_complete": True,
                "failed_attempts": 0,
                "retry_attempts": 0,
                "usage": baseline_usage,
            },
        },
    )
    context_cost_path = project / "historical" / "context-cost-report.json"
    controller._write_immutable_json(
        context_cost_path,
        {
            "schema_version": "windowed_paired_context_cost_v1",
            "exact_usage_available": True,
            "end_to_end_cost_evaluable": True,
            "fail_closed_reason": None,
            "exact_context_usage": context_usage,
            "exact_context_usage_production_amortized": production_context_usage,
            "exact_context_tokens": context_usage["total_tokens"],
            "exact_context_tokens_production_amortized": context_tokens,
            "unique_episodes": 1,
            "expected_unique_episodes": 1,
            "segments": baseline_segments,
            "usage_sidecar_coverage": {
                "required": 1,
                "validated": 1,
                "missing_or_invalid": [],
            },
            "phase_one_report_path": str(phase_path.resolve()),
        },
    )
    recovery_path = project / "historical" / "context-usage-recovery-report.json"
    controller._write_immutable_json(
        recovery_path,
        {
            "schema_version": "historical_episode_context_usage_recovery_v1",
            "ok": True,
            "recovery_reran_model": False,
            "requested_runs": 1,
            "recovered_runs": 1,
            "missing_run_ids": [],
            "duplicate_matches": [],
            "invalid_matches": [],
            "artifact_errors": [],
            "expected_usage_mismatches": {},
            "usage": context_usage,
            "expected_usage": {
                "input_tokens": context_usage["input_tokens"],
                "output_tokens": context_usage["output_tokens"],
                "total_tokens": context_usage["total_tokens"],
            },
            "production_amortized_usage": production_context_usage,
            "context_cost_report_path": str(context_cost_path.resolve()),
            "context_cost_report_sha256": controller._record(context_cost_path)[
                "sha256"
            ],
            "sidecars": [
                {
                    "run_id": "context-fixture",
                    "sidecar_path": str(sidecar_path.resolve()),
                    "sidecar_sha256": controller._record(sidecar_path)["sha256"],
                    "rollout_session_id": "fixture-rollout-session",
                    "rollout_sha256": "a" * 64,
                }
            ],
        },
    )
    return controller.freeze_quality_cost_authority(
        quality_root,
        context_usage_recovery_path=recovery_path,
        project_root=project,
    )


def _terminal_quality_fixture(
    bundle: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    *,
    semantic_pass: bool = True,
    baseline_tokens: int = 1_000_000,
    context_tokens: int = 1_000,
) -> dict[str, Any]:
    _prepare_quality_handoff(bundle, monkeypatch)
    loaded = controller.load_epoch7_controller(
        bundle["controller_root"], project_root=bundle["project"]
    )
    arm_ids = [
        str(row["variant_id"])
        for row in loaded["precommit"]["precommit"]["arms"]
    ]
    systems = ["baseline", *arm_ids]
    case_order = loaded["preflight"]["receipt"]["opaque_case_order"]
    witness_rows: list[dict[str, Any]] = []
    sources: dict[str, str] = {}
    witness_systems: dict[str, str] = {}
    case_witnesses: dict[str, list[str]] = {case_id: [] for case_id in case_order}
    for case_index, case_id in enumerate(case_order):
        for system_index, system_id in enumerate(systems):
            witness_id = f"witness-case-{case_index}-system-{system_index}"
            witness_systems[witness_id] = system_id
            sources[witness_id] = (
                f"Exact source evidence for case {case_index} system {system_index}."
            )
            case_witnesses[case_id].append(witness_id)
            witness_rows.append(
                {
                    "witness_id": witness_id,
                    "case_id": case_id,
                    "system_id": system_id,
                    "submitted_evidence_exact": True,
                }
            )
    pairs: list[dict[str, str]] = []
    alignment_rows: list[dict[str, Any]] = []
    pair_index = 0
    for case_id in case_order:
        witnesses = case_witnesses[case_id]
        for left_index, left in enumerate(witnesses):
            for right in witnesses[left_index + 1 :]:
                pair_index += 1
                pair = {
                    "pair_id": f"pair-{pair_index}",
                    "left_witness_id": left,
                    "right_witness_id": right,
                }
                pairs.append(pair)
                baseline_pair = "baseline" in {
                    witness_systems[left],
                    witness_systems[right],
                }
                if semantic_pass or not baseline_pair:
                    alignment_rows.append(
                        {
                            **pair,
                            "relation": "equivalent",
                            "checklist": _checklist(),
                            "rationale": "The fixture events are equivalent.",
                        }
                    )
                else:
                    alignment_rows.append(
                        {
                            **pair,
                            "relation": "not_equivalent",
                            "checklist": _checklist(
                                **{
                                    field: "different"
                                    for field in controller._TRUTH_CONDITIONAL_CHECKLIST_FIELDS
                                }
                            ),
                            "rationale": "The fixture systems differ on every field.",
                        }
                    )
    fields = list(controller._TRUTH_CONDITIONAL_CHECKLIST_FIELDS)
    manifest = {
        "schema_version": controller.QUALITY_SCORING_MANIFEST_VERSION,
        "state": "frozen_before_quality_turns",
        "case_order": case_order,
        "system_order": systems,
        "witnesses": witness_rows,
        "one_shared_augmented_reference": True,
        "reference_system_ids": None,
        "semantic_fields": fields,
        "semantic_fields_sha256": controller._sha256_bytes(
            controller._canonical_json(fields).encode("ascii")
        ),
        "deterministic_semantic_matching": False,
        "semantic_pruning": False,
    }
    evidence = _quality_evidence_fixture(
        bundle,
        monkeypatch,
        sources=sources,
        pairs=pairs,
        alignment_rows=alignment_rows,
        scoring_manifest=manifest,
    )
    cost = _quality_cost_fixture(
        bundle,
        evidence["quality_root"],
        baseline_tokens=baseline_tokens,
        context_tokens=context_tokens,
    )
    return {**evidence, "manifest": manifest, "cost": cost, "arm_ids": arm_ids}


def test_plan_freezes_exact_zero_call_authority_and_runtime_contract(
    tmp_path: Path,
) -> None:
    bundle = _fixture(tmp_path)
    loaded = controller.load_epoch7_controller(
        bundle["controller_root"], project_root=bundle["project"]
    )
    assert bundle["planned"]["semantic_model_call_count"] == 0
    assert loaded["contract"]["authorized_by"] == "kolby"
    assert loaded["contract"]["caller_capacity_admission_allowed"] is False
    assert loaded["contract"]["caller_client_factory_allowed"] is False
    assert loaded["directive"]["operator_authorization_id"] == bundle[
        "authorization_id"
    ]
    assert loaded["directive"]["quality_evaluation_authorized"] is False
    assert loaded["directive"]["holdout_authorized"] is False
    evidence = loaded["contract"]["quality_runtime_evidence_contract"]
    assert evidence["exact_semantic_turn_count"] == 4
    assert evidence["semantic_turn_order"] == [
        {"stage": "support_first", "orientation": "ab"},
        {"stage": "support_first", "orientation": "ba"},
        {"stage": "alignment", "orientation": "ab"},
        {"stage": "alignment", "orientation": "ba"},
    ]
    assert evidence["reported_scalar_metrics_never_sufficient_authority"] is True
    assert evidence["deterministic_score_recomputation_from_raw_decisions_required"] is True
    assert evidence["quality_scoring_manifest_schema_version"] == (
        controller.QUALITY_SCORING_MANIFEST_VERSION
    )
    assert evidence["recomputed_quality_score_schema_version"] == (
        controller.QUALITY_RECOMPUTED_SCORE_VERSION
    )
    assert evidence["every_support_positive_unordered_pair_within_case_required"] is True
    assert evidence["shared_reference_system_ids"] is None
    assert evidence["production_cost_formula"] == (
        controller.QUALITY_PRODUCTION_COST_FORMULA
    )
    assert evidence["quality_cost_authority_schema_version"] == (
        controller.QUALITY_COST_AUTHORITY_VERSION
    )
    assert evidence["quality_terminal_receipt_schema_version"] == (
        controller.QUALITY_TERMINAL_RECEIPT_VERSION
    )
    assert evidence["judge_usage_in_production_cost_formula"] is False
    assert evidence["truth_conditional_checklist_fields"] == [
        "actor",
        "attribution",
        "causal_mechanism",
        "certainty",
        "event_boundary",
        "event_type",
        "evidence",
        "metric",
        "negation",
        "reported_actor",
        "speaker",
        "stance",
        "target",
        "temporal_horizon",
        "unsupported_inference",
    ]
    assert evidence["truth_conditional_checklist_decisions"] == [
        "same",
        "different",
        "abstain",
    ]
    assert evidence["free_form_mismatch_field_generation_allowed"] is False
    assert evidence["majority_voting_allowed"] is False
    assert evidence["confidence_routing_allowed"] is False
    assert evidence["support_stage_contract"]["decision_schema"] == (
        controller.pointwise_support_decision_schema()
    )
    assert evidence["alignment_decision_schema"] == (
        controller.alignment_decision_schema()
    )
    assert evidence["support_stage_contract"][
        "balanced_permutation_reconciliation"
    ] == "exact_verdict_entailment_and_evidence_agreement_else_abstain"
    assert not bundle["extraction_root"].exists()
    assert controller.status_epoch7(
        bundle["controller_root"], project_root=bundle["project"]
    )["state"] == "ready"


def test_truth_conditional_schema_and_validator_require_all_15_rows() -> None:
    schema = controller.truth_conditional_checklist_schema()
    assert schema["minItems"] == 15
    assert schema["maxItems"] == 15
    assert schema["items"]["properties"]["decision"]["enum"] == [
        "same",
        "different",
        "abstain",
    ]
    rows = _checklist(causal_mechanism="different")
    assert controller.validate_truth_conditional_checklist(rows) == rows
    with pytest.raises(controller.Epoch7ControllerError, match="row count"):
        controller.validate_truth_conditional_checklist(rows[:-1])
    with pytest.raises(controller.Epoch7ControllerError, match="support-positive"):
        controller.validate_truth_conditional_checklist(
            rows,
            left_support_verdict="unsupported",
        )


def test_pointwise_support_requires_exact_evidence_and_material_entailment() -> None:
    sources = {
        "witness-a": "The recommendation reduced latency.",
        "witness-b": "The source does not establish the claimed dependency.",
    }
    rows = _support_rows(sources, verdicts={"witness-b": "unsupported"})
    assert controller.validate_pointwise_support_decisions(
        rows,
        expected_witness_ids=list(sources),
        source_by_witness=sources,
    ) == rows
    schema = controller.pointwise_support_decision_schema()
    assert schema["items"]["additionalProperties"] is False
    assert schema["items"]["properties"]["verdict"]["enum"] == [
        "supported",
        "unsupported",
        "abstain",
    ]

    nonexact = copy.deepcopy(rows)
    nonexact[0]["evidence_spans"][0]["text"] = "The recommendation"
    with pytest.raises(controller.Epoch7ControllerError, match="not exact"):
        controller.validate_pointwise_support_decisions(
            nonexact,
            expected_witness_ids=list(sources),
            source_by_witness=sources,
        )
    wrong_entailment = copy.deepcopy(rows)
    wrong_entailment[0]["material_claim_entailment"] = "not_established"
    with pytest.raises(controller.Epoch7ControllerError, match="entailment contract"):
        controller.validate_pointwise_support_decisions(
            wrong_entailment,
            expected_witness_ids=list(sources),
            source_by_witness=sources,
        )


def test_balanced_support_disagreement_abstains_without_evidence_projection() -> None:
    sources = {"witness-a": "Exact source evidence."}
    ab = _support_rows(sources)
    ba = _support_rows(sources, verdicts={"witness-a": "unsupported"})
    reconciled = controller.reconcile_balanced_support_decisions(
        ab,
        ba,
        expected_witness_ids=list(sources),
        source_by_witness=sources,
    )
    assert reconciled == [
        {
            "witness_id": "witness-a",
            "verdict": "abstain",
            "material_claim_entailment": "abstain",
            "evidence_spans": [],
            "reconciliation": "observable_permutation_disagreement_abstained",
        }
    ]


def test_alignment_container_rejects_free_form_fields_and_requires_support() -> None:
    pairs = [
        {
            "pair_id": "pair-1",
            "left_witness_id": "witness-a",
            "right_witness_id": "witness-b",
        }
    ]
    support = {"witness-a": "supported", "witness-b": "supported"}
    rows = [
        {
            **pairs[0],
            "relation": "not_equivalent",
            "checklist": _checklist(causal_mechanism="different"),
            "rationale": "The causal mechanism changes the material claim.",
        }
    ]
    assert controller.validate_alignment_decisions(
        rows,
        expected_pairs=pairs,
        frozen_support_verdicts=support,
    ) == rows
    free_form = copy.deepcopy(rows)
    free_form[0]["mismatch_fields"] = ["causal_mechanism"]
    with pytest.raises(controller.Epoch7ControllerError, match="malformed"):
        controller.validate_alignment_decisions(
            free_form,
            expected_pairs=pairs,
            frozen_support_verdicts=support,
        )
    with pytest.raises(controller.Epoch7ControllerError, match="contract drifted"):
        controller.validate_alignment_decisions(
            rows,
            expected_pairs=pairs,
            frozen_support_verdicts={
                "witness-a": "supported",
                "witness-b": "unsupported",
            },
        )


def test_balanced_alignment_disagreement_abstains_without_voting() -> None:
    pairs = [
        {
            "pair_id": "pair-1",
            "left_witness_id": "witness-a",
            "right_witness_id": "witness-b",
        }
    ]
    support = {"witness-a": "supported", "witness-b": "supported"}
    ab = [
        {
            **pairs[0],
            "relation": "not_equivalent",
            "checklist": _checklist(target="different"),
            "rationale": "The targets differ.",
        }
    ]
    ba = [
        {
            **pairs[0],
            "relation": "equivalent",
            "checklist": _checklist(),
            "rationale": "The claims are equivalent.",
        }
    ]
    reconciled = controller.reconcile_balanced_alignment_decisions(
        ab,
        ba,
        expected_pairs=pairs,
        frozen_support_verdicts=support,
    )
    assert reconciled[0]["relation"] == "abstain"
    assert reconciled[0]["reconciliation"] == (
        "observable_permutation_disagreement_abstained"
    )
    checklist = {row["field"]: row for row in reconciled[0]["checklist"]}
    assert checklist["target"]["decision"] == "abstain"


@pytest.mark.parametrize("field", controller._TRUTH_CONDITIONAL_CHECKLIST_FIELDS)
def test_each_truth_conditional_field_has_an_order_invariant_minimal_pair(
    field: str,
) -> None:
    pairs = [
        {
            "pair_id": "pair-minimal",
            "left_witness_id": "witness-left",
            "right_witness_id": "witness-right",
        }
    ]
    support = {"witness-left": "supported", "witness-right": "supported"}
    decision = [
        {
            **pairs[0],
            "relation": "not_equivalent",
            "checklist": _checklist(**{field: "different"}),
            "rationale": f"Only {field} differs in this fixture.",
        }
    ]
    forward = controller.reconcile_balanced_alignment_decisions(
        decision,
        copy.deepcopy(decision),
        expected_pairs=pairs,
        frozen_support_verdicts=support,
    )
    reverse = controller.reconcile_balanced_alignment_decisions(
        copy.deepcopy(decision),
        decision,
        expected_pairs=pairs,
        frozen_support_verdicts=support,
    )
    assert forward == reverse
    assert forward[0]["relation"] == "not_equivalent"
    rows = {row["field"]: row for row in forward[0]["checklist"]}
    assert rows[field]["decision"] == "different"
    assert sum(row["decision"] == "different" for row in rows.values()) == 1


def test_support_and_alignment_case_order_drift_is_rejected() -> None:
    sources = {"witness-a": "A", "witness-b": "B"}
    support_rows = list(reversed(_support_rows(sources)))
    with pytest.raises(controller.Epoch7ControllerError, match="decision 0 drifted"):
        controller.validate_pointwise_support_decisions(
            support_rows,
            expected_witness_ids=list(sources),
            source_by_witness=sources,
        )

    pairs = [
        {
            "pair_id": "pair-1",
            "left_witness_id": "witness-a",
            "right_witness_id": "witness-b",
        },
        {
            "pair_id": "pair-2",
            "left_witness_id": "witness-c",
            "right_witness_id": "witness-d",
        },
    ]
    support = {
        "witness-a": "supported",
        "witness-b": "supported",
        "witness-c": "supported",
        "witness-d": "supported",
    }
    rows = [
        {
            **pair,
            "relation": "equivalent",
            "checklist": _checklist(),
            "rationale": "Fixture equivalent pair.",
        }
        for pair in reversed(pairs)
    ]
    with pytest.raises(controller.Epoch7ControllerError, match="decision 0 drifted"):
        controller.validate_alignment_decisions(
            rows,
            expected_pairs=pairs,
            frozen_support_verdicts=support,
        )


def test_minimal_pair_and_balanced_order_invariance_are_deterministic() -> None:
    ab = _checklist(causal_mechanism="different")
    ba = _checklist(causal_mechanism="different")
    reconciled = controller.reconcile_balanced_checklists(ab, ba)
    by_field = {row["field"]: row for row in reconciled}
    assert by_field["causal_mechanism"] == {
        "field": "causal_mechanism",
        "decision": "different",
        "reconciliation": "balanced_permutations_agreed",
    }
    assert sum(row["decision"] == "different" for row in reconciled) == 1
    assert all(row["decision"] != "abstain" for row in reconciled)


def test_observable_permutation_disagreement_abstains_without_voting() -> None:
    ab = _checklist(target="different")
    ba = _checklist(target="same")
    reconciled = controller.reconcile_balanced_checklists(ab, ba)
    by_field = {row["field"]: row for row in reconciled}
    assert by_field["target"] == {
        "field": "target",
        "decision": "abstain",
        "reconciliation": "observable_permutation_disagreement_abstained",
    }
    assert sum(row["decision"] == "abstain" for row in reconciled) == 1
    reordered = copy.deepcopy(ab)
    reordered[0], reordered[1] = reordered[1], reordered[0]
    with pytest.raises(controller.Epoch7ControllerError, match="row 0 drifted"):
        controller.reconcile_balanced_checklists(reordered, ba)


def test_execute_requires_exact_authorization_before_runtime(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle = _fixture(tmp_path)
    calls = 0

    async def forbidden(**_kwargs: Any) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        raise AssertionError("runtime must not be called")

    monkeypatch.setattr(
        controller.extraction_runtime,
        "run_canonical_v31_development_matrix_runtime",
        forbidden,
    )
    with pytest.raises(controller.Epoch7ControllerError, match="does not match"):
        asyncio.run(
            controller.execute_epoch7(
                controller_root=bundle["controller_root"],
                operator_authorization_id="kolby-wrong-authorization",
                project_root=bundle["project"],
            )
        )
    assert calls == 0
    assert not (bundle["controller_root"] / controller.EXECUTION_RECEIPT_FILENAME).exists()


def test_second_controller_writer_fails_before_runtime_or_waiting_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle = _fixture(tmp_path)
    calls = 0

    async def forbidden(**_kwargs: Any) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        raise AssertionError("concurrent controller must not reach the runtime")

    monkeypatch.setattr(
        controller.extraction_runtime,
        "run_canonical_v31_development_matrix_runtime",
        forbidden,
    )
    with controller._ControllerWriterLock(bundle["controller_root"]):
        with pytest.raises(
            controller.Epoch7ControllerLockUnavailable,
            match="another epoch-7 controller",
        ):
            asyncio.run(
                controller.execute_epoch7(
                    controller_root=bundle["controller_root"],
                    operator_authorization_id=bundle["authorization_id"],
                    project_root=bundle["project"],
                )
            )
    assert calls == 0
    assert not (
        bundle["controller_root"] / controller.EXECUTION_RECEIPT_FILENAME
    ).exists()


def test_execute_delegates_once_without_capacity_or_client_injection_and_hands_off(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle = _fixture(tmp_path)
    calls: list[dict[str, Any]] = []

    async def fake_runtime(**kwargs: Any) -> dict[str, Any]:
        calls.append(copy.deepcopy(kwargs))
        assert "capacity_probe" not in kwargs
        assert "client_factory" not in kwargs
        assert "arm_runner" not in kwargs
        await _fake_extraction_receipt(bundle)
        loaded = controller.load_epoch7_controller(
            bundle["controller_root"], project_root=bundle["project"]
        )
        return {
            "receipt": controller._load_object(
                Path(loaded["contract"]["expected_extraction_receipt_path"]),
                label="fixture extraction receipt",
            ),
            "semantic_calls_started_by_this_invocation": loaded["limits"][
                "exact_model_call_cap"
            ],
        }

    monkeypatch.setattr(
        controller.extraction_runtime,
        "run_canonical_v31_development_matrix_runtime",
        fake_runtime,
    )
    receipt = asyncio.run(
        controller.execute_epoch7(
            controller_root=bundle["controller_root"],
            operator_authorization_id=bundle["authorization_id"],
            project_root=bundle["project"],
        )
    )
    assert receipt["state"] == "passed"
    assert len(calls) == 1
    second = asyncio.run(
        controller.execute_epoch7(
            controller_root=bundle["controller_root"],
            operator_authorization_id=bundle["authorization_id"],
            project_root=bundle["project"],
        )
    )
    assert second == receipt
    assert len(calls) == 1
    handoff = controller.verify_epoch7(
        bundle["controller_root"], project_root=bundle["project"]
    )
    assert handoff["handoff"]["state"] == (
        "ready_for_separately_checksum_bound_quality_runtime"
    )
    assert handoff["handoff"]["quality_runtime_authorized"] is False
    assert handoff["handoff"]["quality_model_call_count"] == 0
    assert handoff["handoff"]["holdout_authorized"] is False
    assert handoff["handoff"]["quality_runtime_evidence_contract"] == (
        controller.quality_runtime_evidence_contract()["contract"]
    )
    assert handoff["handoff"]["quality_evaluator_contract_sha256"] == (
        controller.quality_runtime_evidence_contract()[
            "quality_evaluator_contract_sha256"
        ]
    )
    verified = controller.verify_quality_handoff(
        bundle["controller_root"], project_root=bundle["project"]
    )
    assert verified["handoff_record"] == handoff["handoff_record"]
    assert verified["extraction_receipt_record"] == receipt["extraction_receipt"]

    handoff_path = bundle["controller_root"] / controller.QUALITY_HANDOFF_FILENAME
    drifted = json.loads(handoff_path.read_text())
    drifted["quality_runtime_authorized"] = True
    handoff_path.write_text(json.dumps(drifted, sort_keys=True, separators=(",", ":")))
    with pytest.raises(controller.Epoch7ControllerError, match="handoff drifted"):
        controller.verify_quality_handoff(
            bundle["controller_root"], project_root=bundle["project"]
        )


def test_rehashed_extraction_report_forgery_cannot_mint_handoff(
    tmp_path: Path,
) -> None:
    bundle = _fixture(tmp_path)
    receipt = asyncio.run(_fake_extraction_receipt(bundle))
    loaded = controller.load_epoch7_controller(
        bundle["controller_root"], project_root=bundle["project"]
    )
    report_path = Path(receipt["arms"][0]["report"]["path"])
    report = json.loads(report_path.read_text())
    report["managed_chatgpt_auth_verified"] = False
    report_path.unlink()
    report_record = controller._write_immutable_json(report_path, report)
    forged = copy.deepcopy(receipt)
    forged["arms"][0]["report"] = report_record
    receipt_path = Path(loaded["contract"]["expected_extraction_receipt_path"])
    receipt_path.unlink()
    controller._write_immutable_json(receipt_path, forged)

    with pytest.raises(
        controller.Epoch7ControllerError,
        match="artifact reconstruction failed",
    ):
        controller._verify_extraction_receipt(loaded)


def test_rehashed_capacity_admission_forgery_is_rejected(
    tmp_path: Path,
) -> None:
    bundle = _fixture(tmp_path)
    receipt = asyncio.run(_fake_extraction_receipt(bundle))
    loaded = controller.load_epoch7_controller(
        bundle["controller_root"], project_root=bundle["project"]
    )
    first_arm = receipt["arms"][0]
    admission_path = Path(first_arm["capacity_admission"]["path"])
    admission = json.loads(admission_path.read_text())
    admission["managed_chatgpt_auth_only"] = False
    admission_path.unlink()
    admission_record = controller._write_immutable_json(admission_path, admission)

    envelope_path = Path(first_arm["arm_envelope"]["path"])
    envelope = json.loads(envelope_path.read_text())
    envelope["capacity_admission_record"] = admission_record
    envelope["capacity_admission_sha256"] = controller._sha256_bytes(
        controller._canonical_json(admission).encode("ascii")
    )
    envelope_path.unlink()
    envelope_record = controller._write_immutable_json(envelope_path, envelope)

    forged = copy.deepcopy(receipt)
    forged["arms"][0]["capacity_admission"] = admission_record
    forged["arms"][0]["arm_envelope"] = envelope_record
    forged["arms"][0]["arm_envelope_sha256"] = controller._sha256_bytes(
        controller._canonical_json(envelope).encode("ascii")
    )
    receipt_path = Path(loaded["contract"]["expected_extraction_receipt_path"])
    receipt_path.unlink()
    controller._write_immutable_json(receipt_path, forged)

    with pytest.raises(
        controller.Epoch7ControllerError,
        match="artifact reconstruction failed",
    ):
        controller._verify_extraction_receipt(loaded)


def test_canonical_output_and_full_validation_tamper_are_rejected(
    tmp_path: Path,
) -> None:
    bundle = _fixture(tmp_path)
    receipt = asyncio.run(_fake_extraction_receipt(bundle))
    loaded = controller.load_epoch7_controller(
        bundle["controller_root"], project_root=bundle["project"]
    )
    envelope_path = Path(receipt["arms"][0]["arm_envelope"]["path"])
    envelope = json.loads(envelope_path.read_text())
    labels_record = envelope["artifact_validation"]["artifacts"][0][
        "result_records"
    ]["canonical_labels"]
    labels_path = Path(labels_record["path"])
    labels_path.write_bytes(labels_path.read_bytes() + b" ")
    with pytest.raises(
        controller.Epoch7ControllerError,
        match="artifact reconstruction failed",
    ):
        controller._verify_extraction_receipt(loaded)

    labels_path.write_bytes(labels_path.read_bytes()[:-1])
    forged = copy.deepcopy(receipt)
    forged["full_output_validation"]["arm_count"] = 5
    receipt_path = Path(loaded["contract"]["expected_extraction_receipt_path"])
    receipt_path.unlink()
    controller._write_immutable_json(receipt_path, forged)
    with pytest.raises(
        controller.Epoch7ControllerError,
        match="artifact reconstruction failed",
    ):
        controller._verify_extraction_receipt(loaded)


def test_quality_evidence_receipt_rebuilds_four_turn_consensus_without_authority(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle = _fixture(tmp_path)
    evidence = _quality_evidence_fixture(bundle, monkeypatch)
    verified = controller.verify_quality_runtime_evidence_receipt(
        bundle["controller_root"],
        evidence["receipt_path"],
        project_root=bundle["project"],
    )
    assert verified["support_consensus"] == evidence["support_payload"]
    assert verified["alignment_consensus"] == evidence["alignment_payload"]
    assert verified["receipt"]["semantic_model_call_count"] == 4
    assert verified["receipt"]["semantic_retry_count"] == 0
    assert verified["quality_gate_authorized"] is False
    assert verified["holdout_authorized"] is False
    assert verified["production_mutated"] is False


def test_scalar_only_quality_receipt_cannot_mint_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle = _fixture(tmp_path)
    handoff = _prepare_quality_handoff(bundle, monkeypatch)
    quality_root = bundle["project"] / "work" / "scalar-only-quality"
    receipt_path = quality_root / controller.QUALITY_EVIDENCE_RECEIPT_FILENAME
    scalar = {
        "schema_version": controller.QUALITY_EVIDENCE_RECEIPT_VERSION,
        "state": "passed",
        "quality_handoff": handoff["handoff_record"],
        "strict_full_field_macro": 1.0,
        "production_amortized_total_token_ratio": 0.01,
    }
    controller._write_immutable_json(receipt_path, scalar)
    with pytest.raises(controller.Epoch7ControllerError, match="shape drifted"):
        controller.verify_quality_runtime_evidence_receipt(
            bundle["controller_root"],
            receipt_path,
            project_root=bundle["project"],
        )


def test_quality_turn_artifact_tamper_and_thread_reuse_are_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle = _fixture(tmp_path)
    evidence = _quality_evidence_fixture(bundle, monkeypatch)
    receipt = evidence["receipt"]
    raw_path = Path(receipt["turns"][0]["artifacts"]["raw_output"]["path"])
    raw_path.write_text("{}", encoding="ascii")
    with pytest.raises(controller.Epoch7ControllerError, match="record drifted"):
        controller.verify_quality_runtime_evidence_receipt(
            bundle["controller_root"],
            evidence["receipt_path"],
            project_root=bundle["project"],
        )

    bundle = _fixture(tmp_path / "scoring-remap")
    evidence = _quality_evidence_fixture(bundle, monkeypatch)
    receipt = copy.deepcopy(evidence["receipt"])
    request_path = Path(receipt["turns"][0]["artifacts"]["request"]["path"])
    request = json.loads(request_path.read_text())
    request["quality_scoring_manifest"]["sha256"] = "f" * 64
    request_path.unlink()
    controller._write_immutable_json(request_path, request)
    receipt["turns"][0]["artifacts"]["request"] = controller._record(
        request_path, allowed_root=evidence["quality_root"]
    )
    receipt["receipt_sha256"] = controller._receipt_checksum(receipt)
    evidence["receipt_path"].unlink()
    controller._write_immutable_json(evidence["receipt_path"], receipt)
    with pytest.raises(controller.Epoch7ControllerError, match="request lineage drifted"):
        controller.verify_quality_runtime_evidence_receipt(
            bundle["controller_root"],
            evidence["receipt_path"],
            project_root=bundle["project"],
        )

    bundle = _fixture(tmp_path / "thread-reuse")
    evidence = _quality_evidence_fixture(bundle, monkeypatch)
    receipt = copy.deepcopy(evidence["receipt"])
    receipt["turns"][1]["thread_id"] = receipt["turns"][0]["thread_id"]
    receipt["receipt_sha256"] = controller._receipt_checksum(receipt)
    evidence["receipt_path"].unlink()
    controller._write_immutable_json(evidence["receipt_path"], receipt)
    with pytest.raises(
        controller.Epoch7ControllerError,
        match="sidecar contract drifted|identity was reused",
    ):
        controller.verify_quality_runtime_evidence_receipt(
            bundle["controller_root"],
            evidence["receipt_path"],
            project_root=bundle["project"],
        )


def test_quality_consensus_is_recomputed_from_raw_ab_ba_decisions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle = _fixture(tmp_path)
    evidence = _quality_evidence_fixture(bundle, monkeypatch)
    support_path = Path(evidence["receipt"]["support_consensus"]["path"])
    forged = copy.deepcopy(evidence["support_payload"])
    forged["decisions"][0]["verdict"] = "unsupported"
    support_path.write_text(controller._canonical_json(forged), encoding="ascii")
    receipt = copy.deepcopy(evidence["receipt"])
    receipt["support_consensus"] = controller._record(
        support_path, allowed_root=evidence["quality_root"]
    )
    receipt["receipt_sha256"] = controller._receipt_checksum(receipt)
    evidence["receipt_path"].unlink()
    controller._write_immutable_json(evidence["receipt_path"], receipt)
    with pytest.raises(
        controller.Epoch7ControllerError,
        match="request lineage drifted|consensus drifted",
    ):
        controller.verify_quality_runtime_evidence_receipt(
            bundle["controller_root"],
            evidence["receipt_path"],
            project_root=bundle["project"],
        )


def test_quality_score_is_recomputed_from_shared_reference_consensus(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle = _fixture(tmp_path)
    evidence = _quality_evidence_fixture(bundle, monkeypatch)
    manifest = _quality_scoring_manifest(["witness-a", "witness-b"])
    score = controller.recompute_shared_reference_quality_score(
        scoring_manifest=manifest,
        support_consensus=evidence["support_payload"],
        alignment_consensus=evidence["alignment_payload"],
    )
    assert score["systems"]["baseline"]["strict_full_field_macro"] == 1.0
    assert score["systems"]["candidate"]["strict_full_field_macro"] == 1.0
    assert score["reported_scalar_metrics_used"] is False
    assert score["deterministic_semantic_matching"] is False
    assert score["holdout_authorized"] is False


def test_one_field_minimal_pair_recomputes_strict_macro_without_scalar_input(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle = _fixture(tmp_path)
    evidence = _quality_evidence_fixture(bundle, monkeypatch)
    alignment = copy.deepcopy(evidence["alignment_payload"])
    decision = alignment["decisions"][0]
    decision["relation"] = "not_equivalent"
    for row in decision["checklist"]:
        if row["field"] == "target":
            row["decision"] = "different"
    manifest = _quality_scoring_manifest(["witness-a", "witness-b"])
    score = controller.recompute_shared_reference_quality_score(
        scoring_manifest=manifest,
        support_consensus=evidence["support_payload"],
        alignment_consensus=alignment,
    )
    candidate = score["systems"]["candidate"]
    assert candidate["fields"]["target"]["f1"] == 0.666667
    assert candidate["strict_full_field_macro"] == 0.977778


def test_unsupported_submission_is_a_precision_and_recall_failure() -> None:
    witness_ids = ["witness-a", "witness-b"]
    support = {
        "schema_version": controller.QUALITY_SUPPORT_CONSENSUS_VERSION,
        "expected_witness_ids": witness_ids,
        "source_by_witness_sha256": "a" * 64,
        "decisions": [
            {
                "witness_id": "witness-a",
                "verdict": "supported",
                "material_claim_entailment": "entailed",
                "evidence_spans": [{"text": "A", "start": 0, "end": 1}],
                "reconciliation": "balanced_permutations_agreed",
            },
            {
                "witness_id": "witness-b",
                "verdict": "unsupported",
                "material_claim_entailment": "not_established",
                "evidence_spans": [],
                "reconciliation": "balanced_permutations_agreed",
            },
        ],
    }
    alignment = {
        "schema_version": controller.QUALITY_ALIGNMENT_CONSENSUS_VERSION,
        "support_consensus_sha256": controller._sha256_bytes(
            controller._canonical_json(support).encode("ascii")
        ),
        "expected_pairs": [],
        "decisions": [],
    }
    score = controller.recompute_shared_reference_quality_score(
        scoring_manifest=_quality_scoring_manifest(witness_ids),
        support_consensus=support,
        alignment_consensus=alignment,
    )
    assert score["systems"]["baseline"]["strict_full_field_macro"] == 1.0
    assert score["systems"]["candidate"]["strict_full_field_macro"] == 0.0


def test_quality_score_rejects_incomplete_pair_coverage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle = _fixture(tmp_path)
    evidence = _quality_evidence_fixture(bundle, monkeypatch)
    alignment = copy.deepcopy(evidence["alignment_payload"])
    alignment["expected_pairs"] = []
    alignment["decisions"] = []
    with pytest.raises(controller.Epoch7ControllerError, match="coverage is incomplete"):
        controller.recompute_shared_reference_quality_score(
            scoring_manifest=_quality_scoring_manifest(
                ["witness-a", "witness-b"]
            ),
            support_consensus=evidence["support_payload"],
            alignment_consensus=alignment,
        )


def test_quality_terminal_recomputes_six_arm_quality_and_exact_cost_authority(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle = _fixture(tmp_path)
    quality = _terminal_quality_fixture(bundle, monkeypatch)
    verified = controller.freeze_quality_terminal_receipt(
        bundle["controller_root"],
        quality["quality_root"],
        project_root=bundle["project"],
    )
    receipt = verified["receipt"]
    assert receipt["state"] == (
        "passed_development_quality_checkpoint_selection_not_frozen"
    )
    assert receipt["passing_arm_ids"] == quality["arm_ids"]
    assert receipt["rejected_arm_ids"] == []
    assert receipt["quality_gate_passed"] is True
    assert receipt["baseline_strict_full_field_macro"] == 1.0
    assert receipt["quality_judge_usage_in_production_cost_formula"] is False
    assert receipt["production_cost_formula"] == (
        controller.QUALITY_PRODUCTION_COST_FORMULA
    )
    assert all(row["strict_full_field_macro"] == 1.0 for row in receipt["arm_results"])
    assert all(row["submitted_exact_evidence_rate"] == 1.0 for row in receipt["arm_results"])
    assert all(row["passed"] for row in receipt["arm_results"])
    assert receipt["quality_selection_authorized"] is False
    assert receipt["winner_frozen"] is False
    assert receipt["holdout_authorized"] is False
    assert receipt["production_mutated"] is False
    assert controller.verify_quality_terminal_receipt(
        bundle["controller_root"],
        quality["quality_root"],
        project_root=bundle["project"],
    )["receipt"] == receipt


def test_quality_terminal_rejects_real_subthreshold_semantic_score(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle = _fixture(tmp_path)
    quality = _terminal_quality_fixture(
        bundle,
        monkeypatch,
        semantic_pass=False,
    )
    receipt = controller.freeze_quality_terminal_receipt(
        bundle["controller_root"],
        quality["quality_root"],
        project_root=bundle["project"],
    )["receipt"]
    assert receipt["state"] == "rejected_development_quality_or_cost_gate"
    assert receipt["passing_arm_ids"] == []
    assert receipt["rejected_arm_ids"] == quality["arm_ids"]
    assert all(
        row["checks"]["strict_full_field_macro_gte_0_97"] is False
        for row in receipt["arm_results"]
    )
    assert receipt["quality_gate_passed"] is False
    assert receipt["winner_frozen"] is False
    assert receipt["holdout_authorized"] is False


def test_quality_terminal_rejects_exact_over_threshold_production_cost(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle = _fixture(tmp_path)
    quality = _terminal_quality_fixture(
        bundle,
        monkeypatch,
        baseline_tokens=1,
        context_tokens=0,
    )
    receipt = controller.freeze_quality_terminal_receipt(
        bundle["controller_root"],
        quality["quality_root"],
        project_root=bundle["project"],
    )["receipt"]
    assert receipt["state"] == "rejected_development_quality_or_cost_gate"
    assert receipt["passing_arm_ids"] == []
    assert all(
        row["checks"][
            "production_amortized_total_token_ratio_lte_0_28"
        ]
        is False
        for row in receipt["arm_results"]
    )
    assert all(row["strict_full_field_macro"] == 1.0 for row in receipt["arm_results"])


def test_quality_cost_and_terminal_tamper_cannot_mint_authority(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle = _fixture(tmp_path)
    quality = _terminal_quality_fixture(bundle, monkeypatch)
    controller.freeze_quality_terminal_receipt(
        bundle["controller_root"],
        quality["quality_root"],
        project_root=bundle["project"],
    )
    authority_path = (
        quality["quality_root"] / controller.QUALITY_COST_AUTHORITY_FILENAME
    )
    authority = json.loads(authority_path.read_text())
    phase_path = Path(authority["baseline_phase_one_report"]["path"])
    phase = json.loads(phase_path.read_text())
    phase["baseline"]["usage"]["input_tokens"] += 1
    phase["baseline"]["usage"]["total_tokens"] += 1
    phase_path.write_text(
        json.dumps(phase, sort_keys=True, separators=(",", ":")),
        encoding="ascii",
    )
    with pytest.raises(controller.Epoch7ControllerError, match="authority drifted"):
        controller.verify_quality_terminal_receipt(
            bundle["controller_root"],
            quality["quality_root"],
            project_root=bundle["project"],
        )


def test_capacity_wait_freezes_nonreplayable_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle = _fixture(tmp_path)
    calls = 0

    async def denied(**kwargs: Any) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        Path(kwargs["output_root"]).mkdir(parents=True, exist_ok=True)
        controller._write_immutable_json(
            Path(kwargs["output_root"]) / "capacity-denial.json",
            {"cleared": False, "semantic_dispatch": False},
        )
        raise runtime.CanonicalV31CapacityUnavailable("reserve admission denied")

    monkeypatch.setattr(
        controller.extraction_runtime,
        "run_canonical_v31_development_matrix_runtime",
        denied,
    )
    first = asyncio.run(
        controller.execute_epoch7(
            controller_root=bundle["controller_root"],
            operator_authorization_id=bundle["authorization_id"],
            project_root=bundle["project"],
        )
    )
    assert first["state"] == "waiting"
    assert first["replay_authorized"] is False
    assert first["quality_runtime_authorized"] is False
    assert first["blocker_class"] == "CanonicalV31CapacityUnavailable"
    second = asyncio.run(
        controller.execute_epoch7(
            controller_root=bundle["controller_root"],
            operator_authorization_id=bundle["authorization_id"],
            project_root=bundle["project"],
        )
    )
    assert second == first
    assert calls == 1


def test_cancelled_runtime_freezes_waiting_before_propagating(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle = _fixture(tmp_path)
    calls = 0

    async def cancelled(**kwargs: Any) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        Path(kwargs["output_root"]).mkdir(parents=True, exist_ok=True)
        controller._write_immutable_json(
            Path(kwargs["output_root"]) / "interrupted-before-terminal.json",
            {"semantic_usage_status": "unknown", "replay_authorized": False},
        )
        raise asyncio.CancelledError

    monkeypatch.setattr(
        controller.extraction_runtime,
        "run_canonical_v31_development_matrix_runtime",
        cancelled,
    )
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(
            controller.execute_epoch7(
                controller_root=bundle["controller_root"],
                operator_authorization_id=bundle["authorization_id"],
                project_root=bundle["project"],
            )
        )
    status = controller.status_epoch7(
        bundle["controller_root"], project_root=bundle["project"]
    )
    assert status["state"] == "waiting"
    second = asyncio.run(
        controller.execute_epoch7(
            controller_root=bundle["controller_root"],
            operator_authorization_id=bundle["authorization_id"],
            project_root=bundle["project"],
        )
    )
    assert second["state"] == "waiting"
    assert second["blocker_class"] == "CancelledError"
    assert calls == 1


def test_plan_or_runtime_artifact_tamper_is_rejected(tmp_path: Path) -> None:
    bundle = _fixture(tmp_path)
    directive = bundle["controller_root"] / controller.DIRECTIVE_FILENAME
    directive.write_bytes(directive.read_bytes() + b" ")
    with pytest.raises(controller.Epoch7ControllerError, match="record drifted"):
        controller.load_epoch7_controller(
            bundle["controller_root"], project_root=bundle["project"]
        )


def test_foreign_controller_artifact_and_symlink_root_are_rejected(
    tmp_path: Path,
) -> None:
    bundle = _fixture(tmp_path / "foreign")
    (bundle["controller_root"] / "foreign.json").write_text("{}")
    with pytest.raises(controller.Epoch7ControllerError, match="artifact set"):
        controller.load_epoch7_controller(
            bundle["controller_root"], project_root=bundle["project"]
        )

    project = tmp_path / "symlink" / "project"
    data = matrix_fixtures._fixture(project / "fixture")
    episodes = _write(project / "inputs" / "episodes.json", data["episodes"])
    real_root = project / "work" / "real-controller"
    real_root.mkdir(parents=True)
    linked_root = project / "work" / "linked-controller"
    linked_root.symlink_to(real_root, target_is_directory=True)
    now = datetime.now(timezone.utc).replace(microsecond=0)
    with pytest.raises(controller.Epoch7ControllerError, match="symlink"):
        controller.freeze_epoch7_plan(
            controller_root=linked_root,
            extraction_root=project / "work" / "extraction",
            manifest_path=Path(data["manifest_record"]["path"]),
            episodes_path=episodes,
            capacity_policy_path=Path(data["capacity_record"]["path"]),
            operator_authorization_id="kolby-symlink-authorization",
            issued_at=(now - timedelta(minutes=1)).isoformat(),
            expires_at=(now + timedelta(hours=1)).isoformat(),
            project_root=project,
        )


def test_cli_has_exact_four_phases_and_no_injectable_live_authority(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    project = tmp_path / "project"
    data = matrix_fixtures._fixture(project / "fixture")
    episodes = _write(project / "inputs" / "episodes.json", data["episodes"])
    controller_root = project / "work" / "controller"
    extraction_root = project / "work" / "extraction"
    now = datetime.now(timezone.utc).replace(microsecond=0)
    monkeypatch.setattr(controller, "PROJECT_ROOT", project)
    code = controller.main(
        [
            "plan",
            "--controller-root",
            str(controller_root),
            "--extraction-root",
            str(extraction_root),
            "--manifest",
            data["manifest_record"]["path"],
            "--episodes",
            str(episodes),
            "--capacity-policy",
            data["capacity_record"]["path"],
            "--operator-authorization-id",
            "kolby-cli-epoch7-authorization",
            "--issued-at",
            (now - timedelta(minutes=1)).isoformat(),
            "--expires-at",
            (now + timedelta(hours=1)).isoformat(),
        ]
    )
    assert code == 0
    assert json.loads(capsys.readouterr().out)["ok"] is True
    assert controller.main(["status", "--controller-root", str(controller_root)]) == 0
    assert json.loads(capsys.readouterr().out)["result"]["state"] == "ready"
    execute_help = controller._parser()._subparsers._group_actions[0].choices[
        "execute"
    ].format_help()
    assert "--operator-authorization-id" in execute_help
    assert "capacity" not in execute_help
    assert "client" not in execute_help
    assert "api-key" not in execute_help
    assert "session-token" not in execute_help
