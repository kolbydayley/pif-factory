from __future__ import annotations

import asyncio
import copy
import json
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping

import pytest

from research_factory import app_server_canonical_v31_epoch7_controller as controller
from research_factory import app_server_canonical_v31_quality_runtime as quality
from research_factory import codex_app_server
from tests import test_app_server_canonical_v31_development_matrix_runtime as runtime_fixtures
from tests import test_app_server_canonical_v31_epoch7_controller as controller_fixtures


def _rate_limits(*, used_percent: int = 0, reached: str | None = None) -> dict[str, Any]:
    reset_at = int(datetime.now(timezone.utc).timestamp()) + 3_600
    snapshot = {
        "limitId": "codex",
        "primary": {
            "usedPercent": used_percent,
            "resetsAt": reset_at,
            "windowDurationMins": 300,
        },
        "secondary": {
            "usedPercent": used_percent,
            "resetsAt": reset_at,
            "windowDurationMins": 10_080,
        },
        "rateLimitReachedType": reached,
    }
    return {
        "rateLimits": copy.deepcopy(snapshot),
        "rateLimitsByLimitId": {"codex": snapshot},
        "rateLimitResetCredits": {"availableCount": 0},
    }


class FakeQualityClient:
    def __init__(self) -> None:
        self.account_summary = {"type": "chatgpt", "plan_type": "pro"}
        self.calls: list[tuple[str, str]] = []
        self.entered = 0

    async def __aenter__(self) -> "FakeQualityClient":
        self.entered += 1
        return self

    async def __aexit__(self, *_args: Any) -> None:
        return None

    async def run_ephemeral_structured_turn(self, **kwargs: Any) -> Any:
        prompt = kwargs["prompt"]
        payload = json.loads(prompt.split("\n", 1)[1])
        stage = payload["stage"]
        orientation = payload["orientation"]
        self.calls.append((stage, orientation))
        if stage == "support_first":
            source_by_id = {
                witness["witness_id"]: case["source_excerpt"]
                for case in payload["cases"]
                for witness in case["witnesses"]
            }
            event_by_id = {
                witness["witness_id"]: witness["event"]
                for case in payload["cases"]
                for witness in case["witnesses"]
            }
            output = []
            for witness_id in payload["expected_witness_ids"]:
                event = event_by_id[witness_id]
                source = source_by_id[witness_id]
                start = int(event["evidence_start"])
                end = int(event["evidence_end"])
                output.append(
                    {
                        "witness_id": witness_id,
                        "verdict": "supported",
                        "material_claim_entailment": "entailed",
                        "evidence_spans": [
                            {"text": source[start:end], "start": start, "end": end}
                        ],
                        "rationale": "The exact source materially entails this event.",
                    }
                )
        else:
            output = []
            for pair in payload["pairs"]:
                output.append(
                    {
                        "pair_id": pair["pair_id"],
                        "left_witness_id": pair["canonical_left_witness_id"],
                        "right_witness_id": pair["canonical_right_witness_id"],
                        "relation": "equivalent",
                        "checklist": [
                            {
                                "field": field,
                                "decision": "same",
                                "rationale": f"Fixture agreement for {field}.",
                            }
                            for field in controller._TRUTH_CONDITIONAL_CHECKLIST_FIELDS
                        ],
                        "rationale": "The blinded full events are equivalent.",
                    }
                )
        raw = quality._canonical_json(output).encode("ascii")
        output_path = Path(kwargs["output_path"])
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(raw)
        sidecar_path = Path(kwargs["sidecar_path"])
        index = len(self.calls)
        usage = {
            "input_tokens": 900,
            "cached_input_tokens": 100,
            "output_tokens": 200,
            "reasoning_output_tokens": 50,
            "total_tokens": 1_100,
        }
        schema_bytes = quality._canonical_json(kwargs["output_schema"]).encode("ascii")
        base_bytes = kwargs["base_instructions"].encode("utf-8")
        prompt_bytes = prompt.encode("utf-8")
        sidecar = {
            "schema_version": "pif_codex_app_server_turn_v2",
            "state": "completed",
            "status": "completed",
            "client_version": codex_app_server.APP_SERVER_CLIENT_VERSION,
            "cli_version": codex_app_server.PINNED_CODEX_CLI_VERSION,
            "protocol_schema_sha256": quality._record(
                codex_app_server.PROTOCOL_SCHEMA_PATH
            )["sha256"],
            "transport": "stdio",
            "auth_type": "chatgpt",
            "plan_type": "pro",
            "thread_mode": "new_thread",
            "model": kwargs["model"],
            "effort": kwargs["effort"],
            "thread_id": f"quality-thread-{index}",
            "turn_id": f"quality-turn-{index}",
            "prompt_sha256": quality._sha256_bytes(prompt_bytes),
            "prompt_bytes": len(prompt_bytes),
            "base_instructions_sha256": quality._sha256_bytes(base_bytes),
            "base_instructions_bytes": len(base_bytes),
            "instruction_sources_sha256": quality._sha256_bytes(b"fixture-sources"),
            "instruction_sources_count": 1,
            "output_schema_sha256": quality._sha256_bytes(schema_bytes),
            "output_schema_bytes": len(schema_bytes),
            "output_path": str(output_path.resolve()),
            "output_sha256": quality._sha256_bytes(raw),
            "usage_status": "measured",
            "usage_complete": True,
            "usage": usage,
            "thread_total_usage": usage,
            "wall_elapsed_seconds": 2.0,
            "synthetic_debug_errors": False,
            "recovery_reran_model": False,
            "app_server_user_agent": "fixture-managed-app-server",
        }
        controller._write_immutable_json(sidecar_path, sidecar)
        return SimpleNamespace(status_ok=True, usage=SimpleNamespace(**usage))


class FakeQualityFactory:
    def __init__(self) -> None:
        self.clients: list[FakeQualityClient] = []

    def __call__(self) -> FakeQualityClient:
        client = FakeQualityClient()
        self.clients.append(client)
        return client


@pytest.fixture(scope="module")
def controller_bundle(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    root = tmp_path_factory.mktemp("canonical-quality-runtime")
    patcher = pytest.MonkeyPatch()
    extraction_client = runtime_fixtures.LiveFixtureFactory()
    patcher.setattr(controller.matrix.adapter, "_client_factory", extraction_client)
    bundle = controller_fixtures._fixture(root)
    controller_fixtures._prepare_quality_handoff(bundle, patcher)
    original_output_loader = quality._load_extraction_outputs

    def coded_quality_outputs(loaded: Mapping[str, Any]) -> dict[str, Any]:
        # Revalidate the real all-no-signal fixture artifacts, then provide a
        # test-only scored event per arm so the controller's system-coverage
        # invariant can exercise the four-turn path.
        original_output_loader(loaded)
        case_order = loaded["preflight"]["manifest_info"]["opaque_case_order"]
        manifest_info = loaded["preflight"]["manifest_info"]
        template_event = next(
            event
            for case_id in case_order
            for event in manifest_info["references_by_case"][case_id]["label"]
            ["discourse_events"]
        )
        rows = []
        for case_id in case_order:
            reference = manifest_info["references_by_case"][case_id]
            label = copy.deepcopy(reference["label"])
            if not label["discourse_events"]:
                source = manifest_info["source_by_segment"][
                    reference["segment_id"]
                ]["segment_text"]
                event = copy.deepcopy(template_event)
                event["evidence_start"] = 0
                event["evidence_end"] = min(len(source), 120)
                event["evidence"] = source[: event["evidence_end"]]
                label["discourse_events"] = [event]
            rows.append({"opaque_case_id": case_id, "label": label})
        return {
            str(arm["variant_id"]): copy.deepcopy(rows)
            for arm in loaded["precommit"]["precommit"]["arms"]
        }

    patcher.setattr(quality, "_load_extraction_outputs", coded_quality_outputs)
    cost_root = bundle["project"] / "work" / "quality-cost-bootstrap"
    controller_fixtures._quality_cost_fixture(bundle, cost_root)
    bundle["context_recovery"] = (
        bundle["project"]
        / "historical"
        / "context-usage-recovery-report.json"
    )
    bundle["patcher"] = patcher
    yield bundle
    patcher.undo()


def _freeze(
    bundle: Mapping[str, Any], name: str, *, lifetime_minutes: int = 30
) -> dict[str, Any]:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    root = bundle["project"] / "work" / name
    return quality.freeze_quality_runtime_plan(
        controller_root=bundle["controller_root"],
        quality_root=root,
        context_usage_recovery_path=bundle["context_recovery"],
        operator_authorization_id=f"kolby-quality-{name}",
        issued_at=(now - timedelta(minutes=1)).isoformat(),
        expires_at=(now + timedelta(minutes=lifetime_minutes)).isoformat(),
        project_root=bundle["project"],
    )


def test_plan_freezes_verified_zero_call_contract(controller_bundle: Mapping[str, Any]) -> None:
    loaded = _freeze(controller_bundle, "quality-zero-call")
    verified = quality.verify_quality_runtime(
        loaded["quality_root"], project_root=controller_bundle["project"]
    )
    status = quality.status_quality_runtime(
        loaded["quality_root"], project_root=controller_bundle["project"]
    )
    assert verified["state"] == "verified_zero_call_ready"
    assert verified["semantic_model_call_count"] == 0
    assert status["state"] == "ready"
    assert not (loaded["quality_root"] / "turns").exists()


def test_witness_pool_contains_baseline_and_all_six_arms_without_origin_leak(
    controller_bundle: Mapping[str, Any],
) -> None:
    loaded = _freeze(controller_bundle, "quality-witness-contract")
    scoring = loaded["scoring_manifest"]
    packet = loaded["source_packet"]
    expected_systems = [
        quality.BASELINE_SYSTEM_ID,
        *[
            str(row["variant_id"])
            for row in loaded["handoff"]["extraction_receipt"]["arms"]
        ],
    ]
    assert scoring["system_order"] == expected_systems
    rebuilt = quality.build_quality_source_and_scoring(
        loaded["controller_root"], project_root=controller_bundle["project"]
    )
    outputs = quality._load_extraction_outputs(rebuilt["loaded"])
    expected_counts = {
        quality.BASELINE_SYSTEM_ID: sum(
            len(
                rebuilt["loaded"]["preflight"]["manifest_info"]
                ["references_by_case"][case_id]["label"]["discourse_events"]
            )
            for case_id in scoring["case_order"]
        ),
        **{
            variant_id: sum(
                len(row["label"]["discourse_events"]) for row in rows
            )
            for variant_id, rows in outputs.items()
        },
    }
    observed_counts = {
        system_id: sum(
            row["system_id"] == system_id for row in scoring["witnesses"]
        )
        for system_id in expected_systems
    }
    assert observed_counts == expected_counts
    event_by_witness = {
        witness["witness_id"]: witness["event"]
        for case in packet["cases"]
        for witness in case["witnesses"]
    }
    observed_multiset = Counter(
        (
            row["case_id"],
            row["system_id"],
            quality._canonical_json(event_by_witness[row["witness_id"]]),
        )
        for row in scoring["witnesses"]
    )
    expected_multiset = Counter()
    manifest_info = rebuilt["loaded"]["preflight"]["manifest_info"]
    for case_id in scoring["case_order"]:
        for event in manifest_info["references_by_case"][case_id]["label"][
            "discourse_events"
        ]:
            expected_multiset[
                (case_id, quality.BASELINE_SYSTEM_ID, quality._canonical_json(event))
            ] += 1
    for system_id, rows in outputs.items():
        for row in rows:
            for event in row["label"]["discourse_events"]:
                expected_multiset[
                    (
                        row["opaque_case_id"],
                        system_id,
                        quality._canonical_json(event),
                    )
                ] += 1
    assert observed_multiset == expected_multiset
    assert len(scoring["witnesses"]) == len(packet["expected_witness_ids"])
    assert all(row["submitted_evidence_exact"] for row in scoring["witnesses"])
    assert packet["system_origin_exposed_to_model"] is False
    assert all(
        "system_id" not in witness
        for case in packet["cases"]
        for witness in case["witnesses"]
    )


def test_live_execution_rejects_injected_client_before_call(
    controller_bundle: Mapping[str, Any],
) -> None:
    loaded = _freeze(controller_bundle, "quality-live-injection")
    factory = FakeQualityFactory()
    with pytest.raises(quality.CanonicalV31QualityRuntimeError, match="injected"):
        asyncio.run(
            quality.execute_quality_runtime(
                quality_root=loaded["quality_root"],
                operator_authorization_id=loaded["authorization_id"],
                project_root=controller_bundle["project"],
                client_factory=factory,
            )
        )
    assert factory.clients == []


def test_offline_four_turn_runtime_is_ordered_unique_and_controller_verified(
    controller_bundle: Mapping[str, Any],
) -> None:
    loaded = _freeze(controller_bundle, "quality-four-turn")
    factory = FakeQualityFactory()
    now = datetime.now(timezone.utc)
    result = asyncio.run(
        quality.execute_quality_runtime(
            quality_root=loaded["quality_root"],
            operator_authorization_id=loaded["authorization_id"],
            project_root=controller_bundle["project"],
            offline_test_mode=True,
            client_factory=factory,
            capacity_provider=lambda _context: _rate_limits(),
            now=lambda: now,
        )
    )
    assert len(factory.clients) == 1
    assert factory.clients[0].calls == list(quality._TURN_ORDER)
    assert result["state"] == "offline_fixture_completed_non_promotable"
    assert result["receipt"]["semantic_model_call_count"] == 4
    assert result["receipt"]["promotable"] is False
    assert result["receipt"]["aggregate_usage"]["total_tokens"] == 4_400
    assert len({row["thread_id"] for row in result["receipt"]["turns"]}) == 4
    assert len({row["turn_id"] for row in result["receipt"]["turns"]}) == 4
    support_ab, support_ba, alignment_ab, alignment_ba = [
        controller._load_object(
            quality._turn_paths(loaded["quality_root"], stage, orientation)[
                "parsed_decisions"
            ],
            label="fixture decisions",
        )
        for stage, orientation in quality._TURN_ORDER
    ]
    assert support_ab["expected_witness_ids"] == support_ba["expected_witness_ids"]
    assert alignment_ab["expected_pairs"] == alignment_ba["expected_pairs"]
    assert not (
        loaded["quality_root"] / controller.QUALITY_EVIDENCE_RECEIPT_FILENAME
    ).exists()
    assert not (
        loaded["quality_root"] / controller.QUALITY_TERMINAL_RECEIPT_FILENAME
    ).exists()
    assert loaded["cost"]["authority"][
        "judge_usage_in_production_cost_formula"
    ] is False
    assert result["receipt"]["production_mutated"] is False
    assert result["receipt"]["holdout_authorized"] is False


def test_capacity_denial_is_zero_call_waiting_and_never_replayed(
    controller_bundle: Mapping[str, Any],
) -> None:
    loaded = _freeze(controller_bundle, "quality-capacity-wait")
    factory = FakeQualityFactory()
    now = datetime.now(timezone.utc)
    first = asyncio.run(
        quality.execute_quality_runtime(
            quality_root=loaded["quality_root"],
            operator_authorization_id=loaded["authorization_id"],
            project_root=controller_bundle["project"],
            offline_test_mode=True,
            client_factory=factory,
            capacity_provider=lambda _context: _rate_limits(used_percent=100),
            now=lambda: now,
        )
    )
    assert first["state"] == "waiting"
    assert first["receipt"]["semantic_model_call_count"] == 0
    assert factory.clients[0].calls == []
    second_factory = FakeQualityFactory()
    second = asyncio.run(
        quality.execute_quality_runtime(
            quality_root=loaded["quality_root"],
            operator_authorization_id=loaded["authorization_id"],
            project_root=controller_bundle["project"],
            offline_test_mode=True,
            client_factory=second_factory,
            capacity_provider=lambda _context: _rate_limits(),
            now=lambda: now,
        )
    )
    assert second["state"] == "waiting"
    assert second_factory.clients == []


def test_preexisting_partial_dispatch_terminalizes_waiting_without_client(
    controller_bundle: Mapping[str, Any],
) -> None:
    loaded = _freeze(controller_bundle, "quality-partial")
    prepared = quality._prepare_turn(
        loaded,
        stage="support_first",
        orientation="ab",
        support_consensus_record=None,
        support_verdicts=None,
    )
    context = quality._capacity_context(
        loaded,
        stage="support_first",
        orientation="ab",
        sequence=0,
        used_calls=0,
        used_tokens=0,
    )
    request, measurement, admission = quality._capacity_records(
        loaded,
        response=_rate_limits(),
        context=context,
        measured_at=datetime.now(timezone.utc),
    )
    controller._write_immutable_json(
        prepared["paths"]["capacity_request"], request
    )
    controller._write_immutable_json(
        prepared["paths"]["capacity_measurement"], measurement
    )
    controller._write_immutable_json(
        prepared["paths"]["capacity_admission"], admission
    )
    controller._write_immutable_json(
        prepared["paths"]["dispatch"],
        quality._dispatch_payload(
            loaded,
            stage="support_first",
            orientation="ab",
            offline_test_mode=True,
        ),
    )
    factory = FakeQualityFactory()
    result = asyncio.run(
        quality.execute_quality_runtime(
            quality_root=loaded["quality_root"],
            operator_authorization_id=loaded["authorization_id"],
            project_root=controller_bundle["project"],
            offline_test_mode=True,
            client_factory=factory,
            capacity_provider=lambda _context: _rate_limits(),
            now=lambda: datetime.now(timezone.utc),
        )
    )
    assert result["state"] == "waiting"
    assert result["receipt"]["terminal_reason"] == (
        "partial_or_interrupted_quality_attempt_no_replay"
    )
    assert result["receipt"]["semantic_model_call_count"] == 1
    assert result["receipt"]["usage_status"] == "unknown_partial"
    assert result["receipt"]["known_completed_attempt_count"] == 0
    assert result["receipt"]["unknown_attempt_count"] == 1
    assert result["receipt"]["offline_test_mode"] is True
    assert result["receipt"]["promotable"] is False
    assert factory.clients == []


def test_completed_offline_turns_rebuild_only_nonpromotable_receipt_without_calls(
    controller_bundle: Mapping[str, Any],
) -> None:
    loaded = _freeze(controller_bundle, "quality-offline-recovery")
    first_factory = FakeQualityFactory()
    now = datetime.now(timezone.utc)
    first = asyncio.run(
        quality.execute_quality_runtime(
            quality_root=loaded["quality_root"],
            operator_authorization_id=loaded["authorization_id"],
            project_root=controller_bundle["project"],
            offline_test_mode=True,
            client_factory=first_factory,
            capacity_provider=lambda _context: _rate_limits(),
            now=lambda: now,
        )
    )
    assert first["state"] == "offline_fixture_completed_non_promotable"
    (loaded["quality_root"] / quality.OFFLINE_RECEIPT_FILENAME).unlink()

    recovery_factory = FakeQualityFactory()
    recovered = asyncio.run(
        quality.execute_quality_runtime(
            quality_root=loaded["quality_root"],
            operator_authorization_id=loaded["authorization_id"],
            project_root=controller_bundle["project"],
            offline_test_mode=True,
            client_factory=recovery_factory,
            capacity_provider=lambda _context: _rate_limits(),
            now=lambda: now,
        )
    )
    assert recovered["state"] == "offline_fixture_completed_non_promotable"
    assert recovered["receipt"]["aggregate_usage"]["total_tokens"] == 4_400
    assert recovery_factory.clients == []
    assert not (
        loaded["quality_root"] / controller.QUALITY_EVIDENCE_RECEIPT_FILENAME
    ).exists()
    assert not (
        loaded["quality_root"] / controller.QUALITY_TERMINAL_RECEIPT_FILENAME
    ).exists()


def test_capacity_denial_after_one_completed_turn_is_measured_waiting_no_replay(
    controller_bundle: Mapping[str, Any],
) -> None:
    loaded = _freeze(controller_bundle, "quality-second-capacity-wait")
    responses = iter((_rate_limits(), _rate_limits(used_percent=100)))
    factory = FakeQualityFactory()
    now = datetime.now(timezone.utc)
    first = asyncio.run(
        quality.execute_quality_runtime(
            quality_root=loaded["quality_root"],
            operator_authorization_id=loaded["authorization_id"],
            project_root=controller_bundle["project"],
            offline_test_mode=True,
            client_factory=factory,
            capacity_provider=lambda _context: next(responses),
            now=lambda: now,
        )
    )
    assert first["state"] == "waiting"
    assert factory.clients[0].calls == [("support_first", "ab")]
    assert first["receipt"]["semantic_model_call_count"] == 1
    assert first["receipt"]["usage_status"] == "measured_complete"
    assert first["receipt"]["usage_complete"] is True
    assert first["receipt"]["known_completed_attempt_count"] == 1
    assert first["receipt"]["unknown_attempt_count"] == 0
    assert first["receipt"]["measured_usage"]["total_tokens"] == 1_100

    recovery_factory = FakeQualityFactory()
    second = asyncio.run(
        quality.execute_quality_runtime(
            quality_root=loaded["quality_root"],
            operator_authorization_id=loaded["authorization_id"],
            project_root=controller_bundle["project"],
            offline_test_mode=True,
            client_factory=recovery_factory,
            capacity_provider=lambda _context: _rate_limits(),
            now=lambda: now,
        )
    )
    assert second["state"] == "waiting"
    assert recovery_factory.clients == []


def test_wrong_operator_authorization_and_raw_auth_fail_before_client(
    controller_bundle: Mapping[str, Any],
) -> None:
    loaded = _freeze(controller_bundle, "quality-authority-rejection")
    factory = FakeQualityFactory()
    with pytest.raises(
        quality.CanonicalV31QualityRuntimeError, match="authorization ID"
    ):
        asyncio.run(
            quality.execute_quality_runtime(
                quality_root=loaded["quality_root"],
                operator_authorization_id="kolby-wrong-authority",
                project_root=controller_bundle["project"],
                offline_test_mode=True,
                client_factory=factory,
                capacity_provider=lambda _context: _rate_limits(),
                now=lambda: datetime.now(timezone.utc),
            )
        )
    assert factory.clients == []
    with pytest.raises(
        quality.CanonicalV31QualityRuntimeError, match="API keys"
    ):
        asyncio.run(
            quality.execute_quality_runtime(
                quality_root=loaded["quality_root"],
                operator_authorization_id=loaded["authorization_id"],
                project_root=controller_bundle["project"],
                environ={"OPENAI_API_KEY": "forbidden"},
            )
        )


@pytest.mark.parametrize(
    ("filename", "field"),
    [
        (quality.DIRECTIVE_FILENAME, "model"),
        (quality.PLAN_FILENAME, "step_id"),
        (quality.RUNTIME_LOCK_FILENAME, "state"),
        (quality.SOURCE_PACKET_FILENAME, "state"),
        (controller.QUALITY_SCORING_MANIFEST_FILENAME, "state"),
    ],
)
def test_static_artifact_tamper_is_rejected(
    controller_bundle: Mapping[str, Any], filename: str, field: str
) -> None:
    name = f"quality-tamper-{filename.replace('.', '-')}"
    loaded = _freeze(controller_bundle, name)
    path = loaded["quality_root"] / filename
    value = json.loads(path.read_text())
    value[field] = "tampered"
    path.write_text(json.dumps(value, sort_keys=True, separators=(",", ":")))
    quality._cached_record.cache_clear()
    with pytest.raises(quality.CanonicalV31QualityRuntimeError):
        quality.load_quality_runtime_plan(
            loaded["quality_root"], project_root=controller_bundle["project"]
        )


def test_cli_status_and_verify_are_zero_call(
    controller_bundle: Mapping[str, Any],
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = _freeze(controller_bundle, "quality-cli")
    monkeypatch.setattr(quality, "PROJECT_ROOT", controller_bundle["project"])
    assert quality.main(["status", "--quality-root", str(loaded["quality_root"])]) == 0
    status = json.loads(capsys.readouterr().out)
    assert status["result"]["state"] == "ready"
    assert quality.main(["verify", "--quality-root", str(loaded["quality_root"])]) == 0
    verified = json.loads(capsys.readouterr().out)
    assert verified["result"]["semantic_model_call_count"] == 0
