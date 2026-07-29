from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from research_factory import app_server_canonical_v31_epoch14_terminal_recovery as recovery
from research_factory import app_server_canonical_v31_literal_pointer_canary_runtime as epoch14
from research_factory import app_server_one_turn_canary_runtime as one_turn


def test_completed_epoch14_attempt_has_exact_recoverable_accounting_and_rejection() -> None:
    audit = recovery.audit_completed_attempt()
    assert audit["turn_usage"]["total_tokens"] == 78_693
    assert audit["thread_total_usage"] == {
        "input_tokens": 406_927,
        "cached_input_tokens": 351_232,
        "output_tokens": 36_605,
        "reasoning_output_tokens": 4_315,
        "total_tokens": 443_532,
    }
    assert audit["projection_error"] == recovery.EXPECTED_PROJECTION_ERROR
    assert audit["thread_id"] == recovery.EXPECTED_THREAD_ID
    assert audit["turn_id"] == recovery.EXPECTED_TURN_ID


def test_recovery_payload_accounts_for_original_call_without_new_call() -> None:
    audit = recovery.audit_completed_attempt()
    payload = recovery._recovery_payload(
        audit, created_at="2026-07-19T20:30:00+00:00"
    )
    rejection = recovery._rejection_payload(audit)
    assert payload["recovery_semantic_model_call_count"] == 0
    assert payload["adopted_original_semantic_model_call_count"] == 1
    assert payload["thread_total_usage"]["total_tokens"] == 443_532
    assert rejection["failed_checks"] == [
        "measured_total_token_acceptance_ceiling",
        "semantic_output_validity",
    ]


def test_recovery_rejects_sidecar_that_erases_cumulative_usage_difference(
    tmp_path: Path,
) -> None:
    root = recovery.ROOT
    paths = one_turn._paths(root)
    request = one_turn._load(paths["request"], "request")
    thread_payload = one_turn._load(paths["thread"], "thread")
    thread = one_turn._thread_from_payload(epoch14.SPEC, thread_payload, request)
    sidecar = json.loads(paths["sidecar"].read_text(encoding="utf-8"))
    sidecar["thread_total_usage"] = copy.deepcopy(sidecar["usage"])
    tampered = tmp_path / "sidecar.json"
    tampered.write_text(json.dumps(sidecar), encoding="utf-8")
    with pytest.raises(
        recovery.Epoch14TerminalRecoveryError,
        match="unexpectedly passed",
    ):
        recovery._validate_completed_accounting(
            sidecar,
            request=request,
            thread=thread,
            sidecar_path=tampered,
            output_path=paths["output"],
        )
