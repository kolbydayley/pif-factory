from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import app_server_canonical_v31_literal_pointer_canary_runtime as runtime
from research_factory import app_server_canonical_v31_epoch7_input_authority_runtime as capacity
from research_factory import app_server_one_turn_canary_runtime as one_turn


AUTHORIZATION_ID = "kolby-epoch14-literal-pointer-test"


@pytest.fixture
def prepared_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(
        runtime,
        "SPEC",
        replace(runtime.SPEC, validate_predecessor=lambda _full: None),
    )
    root = tmp_path / "epoch14"
    status = runtime.prepare_canary(root)
    assert status["state"] == "verified_zero_call_runtime"
    return root


def test_directive_plan_and_predecessor_taxonomy_are_exact() -> None:
    runtime._validate_directive(
        json.loads(runtime.DIRECTIVE_PATH.read_text(encoding="utf-8"))
    )
    assert runtime._metric_literal_taxonomy() == runtime.EXPECTED_TAXONOMY
    runtime._validate_predecessor(False)


def test_prepare_freezes_compact_pointer_request(prepared_root: Path) -> None:
    status = runtime.verify_runtime(prepared_root)
    request = json.loads(
        (prepared_root / "prepared-turn/request.private.json").read_text(
            encoding="utf-8"
        )
    )
    contract = json.loads(
        (prepared_root / "runtime-contract.json").read_text(encoding="utf-8")
    )
    assert status["semantic_model_call_count"] == 0
    assert status["literal_token_count"] == runtime.EXPECTED_LITERAL_TOKEN_COUNT
    assert status["prompt_bytes"] == runtime.EXPECTED_PROMPT_BYTES
    assert request["effective_batch_size"] == 6
    assert contract["exact_turn_count"] == 1
    assert contract["maximum_total_tokens_per_turn"] == 90_000
    assert contract["semantic_retry_count"] == 0


def test_authorization_round_trips_without_another_approval(prepared_root: Path) -> None:
    authorization = runtime.authorize_canary(
        root=prepared_root,
        operator_authorization_id=AUTHORIZATION_ID,
    )
    assert authorization["authorized_by"] == "kolby"
    assert authorization["semantic_model_call_cap"] == 1
    assert authorization["authorization_statement"] == runtime.AUTHORIZATION_STATEMENT
    assert runtime.verify_authorization(
        prepared_root,
        expected_authorization_id=AUTHORIZATION_ID,
        require_current=True,
    ) == authorization


def test_partial_attempt_terminalizes_waiting_without_client_or_replay(
    prepared_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    authorization = runtime.authorize_canary(
        root=prepared_root,
        operator_authorization_id=AUTHORIZATION_ID,
    )
    paths = one_turn._paths(prepared_root)
    paths["attempt"].parent.mkdir(parents=True, exist_ok=True)
    one_turn._write_json(
        paths["attempt"],
        one_turn._attempt_payload(runtime.SPEC, prepared_root, authorization),
    )

    def forbidden_client():
        raise AssertionError("client must not be created during no-replay recovery")

    monkeypatch.setattr(runtime.adapter, "_client_factory", forbidden_client)
    receipt = asyncio.run(
        runtime.execute_canary(
            root=prepared_root,
            operator_authorization_id=AUTHORIZATION_ID,
        )
    )
    assert receipt["state"] == "waiting"
    assert receipt["new_semantic_model_call_count"] == 0
    assert receipt["new_unknown_usage_turn_count"] == 0
    assert not paths["dispatch"].exists()
    assert runtime.verify_receipt(prepared_root) == receipt


def test_runtime_lock_rejects_pointer_prompt_tamper(prepared_root: Path) -> None:
    prompt = prepared_root / "prepared-turn/prompt.private.md"
    prompt.write_text(prompt.read_text(encoding="utf-8") + "tamper", encoding="utf-8")
    with pytest.raises(RuntimeError, match="drifted"):
        runtime.verify_runtime(prepared_root)


def test_execute_surface_has_no_injected_client_or_capacity_parameter() -> None:
    parameters = runtime.execute_canary.__annotations__
    assert "client_factory" not in parameters
    assert "capacity_provider" not in parameters
    assert "capacity_admission" not in parameters


def test_completed_turn_finalizes_and_verifies_without_model_replay(
    prepared_root: Path,
) -> None:
    authorization = runtime.authorize_canary(
        root=prepared_root,
        operator_authorization_id=AUTHORIZATION_ID,
    )
    paths = one_turn._paths(prepared_root)
    request = one_turn._load(paths["request"], "request")
    contract = one_turn._load(paths["contract"], "contract")
    paths["attempt"].parent.mkdir(parents=True, exist_ok=True)
    one_turn._write_json(
        paths["attempt"],
        one_turn._attempt_payload(runtime.SPEC, prepared_root, authorization),
    )
    response = json.loads(
        (
            runtime.PREDECESSOR_ROOT
            / "turn/capacity/initial/provider-response.private.json"
        ).read_text(encoding="utf-8")
    )
    for boundary, role in (
        ("initial_before_thread", "initial_capacity"),
        ("preturn_before_turn", "preturn_capacity"),
    ):
        capacity_request = one_turn._capacity_request(
            runtime.SPEC,
            prepared_root,
            contract,
            authorization,
            boundary=boundary,
        )
        measured_at = datetime.now(timezone.utc)
        measurement = capacity._capacity_measurement(  # noqa: SLF001
            response,
            request=capacity_request,
            contract=contract,
            authorization=authorization,
            measured_at=measured_at,
        )
        capacity._publish_capacity_bundle(  # noqa: SLF001
            paths[role],
            request=capacity_request,
            response=response,
            measurement=measurement,
        )
    sources = runtime.adapter.expected_instruction_source_contract()
    thread = SimpleNamespace(
        thread_id="thread-epoch14-completed-fixture",
        model=runtime.adapter.MODEL,
        cwd=str(runtime.PROJECT_ROOT.resolve()),
        ephemeral=True,
        base_instructions_sha256=request["base_instructions_sha256"],
        base_instructions_bytes=len(request["base_instructions"].encode("utf-8")),
        instruction_sources_sha256=sources["effective_instruction_sources_sha256"],
        instruction_sources_count=sources["effective_instruction_sources_count"],
    )
    one_turn._write_json(
        paths["thread"],
        one_turn._thread_payload(
            runtime.SPEC, thread, root=prepared_root, request=request
        ),
    )
    one_turn._write_json(
        paths["dispatch"],
        {
            "schema_version": one_turn.DISPATCH_VERSION,
            "state": "semantic_turn_dispatch_committed",
            "turn_name": runtime.TURN_NAME,
            "request": one_turn._record(paths["request"]),
            "runtime_lock": one_turn._record(paths["lock"]),
            "operator_authorization": one_turn._record(paths["authorization"]),
            "thread": one_turn._record(paths["thread"]),
            "initial_capacity": one_turn._capacity_records(paths, "initial_capacity"),
            "preturn_capacity": one_turn._capacity_records(paths, "preturn_capacity"),
            "semantic_retry_count": 0,
        },
    )
    output = {"episode_id": request["episode_id"], "segments": []}
    for source in request["private_input"]["segments"]:
        output["segments"].append(
            {
                "segment_id": source["segment_id"],
                "extraction_status": "no_signal",
                "segment_source_context": {
                    "kind": "substantive_dialogue",
                    "confidence": 0.9,
                    "rationale": "No eligible independent proposition was selected.",
                },
                "discourse_events": [],
                "concept_candidates": [],
                "rejected_candidates": [],
                "no_signal_reason": "No eligible independent proposition was selected.",
                "overall_confidence": 0.9,
                "needs_review": False,
                "review_reason": None,
                "unit_receipts": [
                    {
                        "unit_id": unit["unit_id"],
                        "reviewed": True,
                        "grounded_event_count": 0,
                        "grounded_concept_candidate_count": 0,
                        "unresolved_count": 0,
                    }
                    for unit in source["units"]
                ],
                "coverage_audit": {
                    "all_source_units_reviewed": True,
                    "unresolved_count": 0,
                },
            }
        )
    paths["output"].write_text(
        json.dumps(output, ensure_ascii=True, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    usage = {
        "input_tokens": 100,
        "cached_input_tokens": 0,
        "output_tokens": 50,
        "reasoning_output_tokens": 10,
        "total_tokens": 150,
    }
    sidecar = {
        "schema_version": runtime.adapter.base.codex_app_server.TURN_SIDECAR_SCHEMA_VERSION,
        "state": "completed",
        "status": "completed",
        "started_at": "2026-07-19T20:00:00+00:00",
        "finished_at": "2026-07-19T20:00:01+00:00",
        "client_version": runtime.adapter.base.codex_app_server.APP_SERVER_CLIENT_VERSION,
        "cli_version": runtime.adapter.base.codex_app_server.PINNED_CODEX_CLI_VERSION,
        "protocol_schema_sha256": runtime.adapter.base._sha256_file(  # noqa: SLF001
            runtime.adapter.base.codex_app_server.PROTOCOL_SCHEMA_PATH
        ),
        "transport": "stdio",
        "app_server_user_agent": "fixture",
        "max_message_bytes": 64 * 1024,
        "synthetic_debug_errors": False,
        "auth_type": "chatgpt",
        "plan_type": "pro",
        "thread_id": thread.thread_id,
        "turn_id": "turn-epoch14-completed-fixture",
        "model": runtime.adapter.MODEL,
        "effort": runtime.adapter.EFFORT,
        "thread_mode": "new_thread",
        "batch_size": request["effective_batch_size"],
        "prompt_sha256": request["prompt_sha256"],
        "prompt_bytes": len(request["prompt"].encode("utf-8")),
        "base_instructions_sha256": request["base_instructions_sha256"],
        "base_instructions_bytes": len(request["base_instructions"].encode("utf-8")),
        "instruction_sources_sha256": sources["effective_instruction_sources_sha256"],
        "instruction_sources_count": sources["effective_instruction_sources_count"],
        "output_schema_sha256": request["output_schema_sha256"],
        "output_schema_bytes": len(
            one_turn._canonical_json(request["output_schema"]).encode("utf-8")
        ),
        "error_class": None,
        "usage_status": "measured",
        "usage_complete": True,
        "usage": usage,
        "thread_total_usage": usage,
        "wall_elapsed_seconds": 1.0,
        "recovery_reran_model": False,
        "stderr_sha256": "0" * 64,
        "stderr_bytes": 0,
        "output_sha256": runtime.adapter.base._output_message_hash(paths["output"]),  # noqa: SLF001
        "output_path": str(paths["output"].resolve()),
    }
    one_turn._write_json(paths["sidecar"], sidecar)
    receipt = one_turn.write_terminal(runtime.SPEC, prepared_root)
    assert receipt["state"] == "passed"
    assert receipt["new_semantic_model_call_count"] == 1
    assert receipt["new_measured_usage"] == usage
    assert runtime.verify_receipt(prepared_root) == receipt
