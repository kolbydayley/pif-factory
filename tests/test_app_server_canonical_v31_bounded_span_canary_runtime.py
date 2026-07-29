from __future__ import annotations

import copy
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from research_factory import app_server_canonical_v31_bounded_span_canary_runtime as runtime


AUTHORIZATION_ID = "kolby-epoch13-bounded-span-canary-test"


@pytest.fixture
def prepared_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(runtime, "_validate_predecessor", lambda *, full_verify: {})
    root = tmp_path / "epoch13"
    status = runtime.prepare_canary(root)
    assert status["state"] == "verified_zero_call_runtime"
    return root


def test_prepare_freezes_exact_one_turn_bounded_span_contract(prepared_root: Path) -> None:
    status = runtime.verify_runtime(prepared_root)
    contract = json.loads((prepared_root / runtime.CONTRACT_FILENAME).read_text())
    request = json.loads((prepared_root / "prepared-turn/request.private.json").read_text())
    assert status["semantic_model_call_count"] == 0
    assert contract["exact_turn_count"] == 1
    assert contract["maximum_total_tokens_per_turn"] == 90_000
    assert contract["semantic_retry_count"] == 0
    assert request["candidate_system_id"] == runtime.adapter.CANDIDATE_SYSTEM_ID
    assert request["effective_batch_size"] == 6
    assert status["source_unit_count"] == 60
    assert status["evidence_span_count"] == 114


def test_authorization_round_trips_without_additional_approval(
    prepared_root: Path,
) -> None:
    now = datetime(2026, 7, 19, 16, 0, tzinfo=timezone.utc)
    authorization = runtime.authorize_canary(
        root=prepared_root,
        operator_authorization_id=AUTHORIZATION_ID,
        now=now,
    )
    assert authorization["authorized_by"] == "kolby"
    assert authorization["authorization_statement"] == runtime.AUTHORIZATION_STATEMENT
    assert authorization["semantic_model_call_cap"] == 1
    verified = runtime.verify_authorization(
        prepared_root,
        expected_authorization_id=AUTHORIZATION_ID,
        require_current=True,
        now=now,
    )
    assert verified == authorization


def test_partial_predispatch_attempt_terminalizes_waiting_without_model_replay(
    prepared_root: Path,
) -> None:
    now = datetime(2026, 7, 19, 16, 0, tzinfo=timezone.utc)
    authorization = runtime.authorize_canary(
        root=prepared_root,
        operator_authorization_id=AUTHORIZATION_ID,
        now=now,
    )
    paths = runtime._paths(prepared_root)
    paths["attempt"].parent.mkdir(parents=True, exist_ok=True)
    runtime._write_json(
        paths["attempt"], runtime._attempt_payload(prepared_root, authorization)
    )
    receipt = runtime._write_terminal(prepared_root)
    assert receipt["state"] == "waiting"
    assert receipt["terminal_reason"] == (
        "epoch13_capacity_or_predispatch_waiting_no_semantic_call"
    )
    assert receipt["new_semantic_model_call_count"] == 0
    assert receipt["new_measured_usage"]["total_tokens"] == 0
    assert not paths["dispatch"].exists()
    assert runtime.verify_receipt(prepared_root) == receipt


def test_runtime_lock_rejects_prepared_prompt_tamper(
    prepared_root: Path,
) -> None:
    prompt = prepared_root / "prepared-turn/prompt.private.md"
    prompt.write_text(prompt.read_text() + "tamper", encoding="utf-8")
    with pytest.raises(
        runtime.BoundedSpanCanaryError,
        match="record drifted|runtime contract drifted|runtime lock drifted",
    ):
        runtime.verify_runtime(prepared_root)


def test_plan_and_directive_are_checksum_bound() -> None:
    directive, plan = runtime._verify_directive_and_plan()
    assert directive["canary_contract"]["new_model_call_cap"] == 1
    assert directive["promotion_contract"]["holdout_authorized"] is False
    assert plan["step"]["directive_sha256"] == runtime.DIRECTIVE_SHA256
    assert plan["step"]["accepted_receipt_states"] == [
        "passed",
        "rejected",
        "waiting",
    ]


def test_live_execute_surface_has_no_injected_client_parameter() -> None:
    parameters = runtime.execute_canary.__annotations__
    assert "client_factory" not in parameters
    assert runtime.TRUSTED_CLIENT_FACTORY is runtime.adapter._client_factory
