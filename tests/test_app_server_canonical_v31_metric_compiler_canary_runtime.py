from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import app_server_canonical_v31_epoch7_input_authority_runtime as capacity
from research_factory import app_server_canonical_v31_metric_compiler_canary_runtime as runtime
from research_factory import app_server_one_turn_canary_runtime as one_turn


AUTHORIZATION_ID = "kolby-epoch15-metric-compiler-test"


@pytest.fixture
def prepared_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(
        runtime,
        "SPEC",
        replace(runtime.SPEC, validate_predecessor=lambda _full: None),
    )
    root = tmp_path / "epoch15"
    status = runtime.prepare_canary(root)
    assert status["state"] == "verified_zero_call_runtime"
    return root


def _compiler_output(request: dict) -> dict:
    rows = []
    for case in request["private_input"]["metric_cases"]:
        row = {
            "metric_case_id": case["metric_case_id"],
            "direction": "not_applicable",
        }
        for field in runtime.adapter.METRIC_LITERAL_FIELDS:
            row[f"{field}_start_token_index"] = None
            row[f"{field}_end_token_index"] = None
        rows.append(row)
    return {"metric_repairs": rows}


def _usage(*, input_tokens: int, output_tokens: int, reasoning: int) -> dict:
    return {
        "input_tokens": input_tokens,
        "cached_input_tokens": 0,
        "output_tokens": output_tokens,
        "reasoning_output_tokens": reasoning,
        "total_tokens": input_tokens + output_tokens,
    }


def _freeze_completed_fixture(
    root: Path,
    *,
    compiler_total_usage: dict | None = None,
) -> dict:
    authorization = runtime.authorize_canary(
        root=root,
        operator_authorization_id=AUTHORIZATION_ID,
    )
    paths = one_turn._paths(root)
    request = one_turn._load(paths["request"], "request")
    contract = one_turn._load(paths["contract"], "contract")
    paths["attempt"].parent.mkdir(parents=True, exist_ok=True)
    one_turn._write_json(
        paths["attempt"],
        one_turn._attempt_payload(runtime.SPEC, root, authorization),
    )
    response = json.loads(
        (
            runtime.SOURCE_ROOT
            / "turn/capacity/initial/provider-response.private.json"
        ).read_text(encoding="utf-8")
    )
    for boundary, role in (
        ("initial_before_thread", "initial_capacity"),
        ("preturn_before_turn", "preturn_capacity"),
    ):
        capacity_request = one_turn._capacity_request(
            runtime.SPEC,
            root,
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
        thread_id="thread-epoch15-completed-fixture",
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
        one_turn._thread_payload(runtime.SPEC, thread, root=root, request=request),
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
    output = _compiler_output(request)
    paths["output"].write_text(
        json.dumps(output, ensure_ascii=True, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    last_usage = _usage(input_tokens=400, output_tokens=100, reasoning=25)
    total_usage = compiler_total_usage or _usage(
        input_tokens=7_000,
        output_tokens=1_000,
        reasoning=100,
    )
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
        "turn_id": "turn-epoch15-completed-fixture",
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
        "usage": last_usage,
        "thread_total_usage": total_usage,
        "wall_elapsed_seconds": 1.0,
        "recovery_reran_model": False,
        "stderr_sha256": "0" * 64,
        "stderr_bytes": 0,
        "output_sha256": runtime.adapter.base._output_message_hash(paths["output"]),  # noqa: SLF001
        "output_path": str(paths["output"].resolve()),
    }
    one_turn._write_json(paths["sidecar"], sidecar)
    return one_turn.write_terminal(runtime.SPEC, root)


def test_directive_plan_and_predecessors_are_exact() -> None:
    runtime._validate_directive(
        json.loads(runtime.DIRECTIVE_PATH.read_text(encoding="utf-8"))
    )
    runtime._validate_predecessor(False)


def test_prepare_freezes_exact_compiler_contract(prepared_root: Path) -> None:
    status = runtime.verify_runtime(prepared_root)
    request = one_turn._load(
        prepared_root / "prepared-turn/request.private.json", "request"
    )
    contract = one_turn._load(prepared_root / "runtime-contract.json", "contract")
    assert status["semantic_model_call_count"] == 0
    assert status["prompt_bytes"] == runtime.EXPECTED_PROMPT_BYTES
    assert status["literal_token_count"] == runtime.EXPECTED_LITERAL_TOKEN_COUNT
    assert status["compiler_literal_token_count"] == runtime.EXPECTED_LITERAL_TOKEN_COUNT
    assert status["canonical_schema_bytes"] == runtime.EXPECTED_SCHEMA_BYTES
    assert request["effective_batch_size"] == runtime.EXPECTED_DISPUTED_CASE_COUNT
    assert sum(
        len(case["literal_tokens"])
        for case in request["private_input"]["metric_cases"]
    ) == runtime.EXPECTED_LITERAL_TOKEN_COUNT
    assert contract["maximum_total_tokens_per_turn"] == 12_000
    assert contract["semantic_retry_count"] == 0


def test_authorization_round_trips_without_another_approval(
    prepared_root: Path,
) -> None:
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
    assert receipt["combined_extraction_and_compiler_total_tokens"] is None
    assert not paths["dispatch"].exists()
    assert runtime.verify_receipt(prepared_root) == receipt


def test_runtime_lock_rejects_compiler_prompt_tamper(prepared_root: Path) -> None:
    prompt = prepared_root / "prepared-turn/prompt.private.md"
    prompt.write_text(prompt.read_text(encoding="utf-8") + "tamper", encoding="utf-8")
    with pytest.raises(RuntimeError, match="drifted"):
        runtime.verify_runtime(prepared_root)


def test_completed_turn_uses_cumulative_usage_and_combined_cost(
    prepared_root: Path,
) -> None:
    receipt = _freeze_completed_fixture(prepared_root)
    assert receipt["state"] == "passed"
    assert receipt["new_semantic_model_call_count"] == 1
    assert receipt["new_measured_usage"]["total_tokens"] == 8_000
    assert receipt["combined_extraction_and_compiler_total_tokens"] == 69_072
    assert receipt["production_amortized_ratio"] == runtime._production_ratio(69_072)
    assert all(receipt["production_cost_gate"].values())
    assert runtime.verify_receipt(prepared_root) == receipt


def test_compiler_cost_overrun_rejects_and_recomputes_combined_ratio(
    prepared_root: Path,
) -> None:
    receipt = _freeze_completed_fixture(
        prepared_root,
        compiler_total_usage=_usage(
            input_tokens=11_000,
            output_tokens=1_001,
            reasoning=100,
        ),
    )
    assert receipt["state"] == "rejected"
    assert receipt["failed_checks"] == ["measured_total_token_acceptance_ceiling"]
    assert receipt["combined_extraction_and_compiler_total_tokens"] == 73_073
    assert receipt["production_cost_gate"] == {
        "compiler_usage_complete": True,
        "compiler_total_tokens_at_most_12000": False,
        "combined_total_tokens_at_most_73072": False,
        "production_amortized_ratio_below_0_28": True,
    }
    assert runtime.verify_receipt(prepared_root) == receipt


def test_execute_surface_has_no_injected_client_or_capacity_parameter() -> None:
    parameters = runtime.execute_canary.__annotations__
    assert "client_factory" not in parameters
    assert "capacity_provider" not in parameters
    assert "capacity_admission" not in parameters
