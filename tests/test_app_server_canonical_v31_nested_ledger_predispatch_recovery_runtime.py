from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from research_factory import (
    app_server_canonical_v31_nested_ledger_predispatch_recovery_runtime as runtime,
)


def test_request_is_byte_identical_to_epoch33_semantic_contract() -> None:
    request = runtime._build_request()  # noqa: SLF001
    assert runtime._request_shape(request) == runtime.EXPECTED_SHAPE  # noqa: SLF001
    assert runtime._request_sha256(request) == runtime.EXPECTED_REQUEST_SHA256  # noqa: SLF001
    assert request == runtime.epoch33._build_request()  # noqa: SLF001
    assert request["model"] == "gpt-5.6-sol"
    assert request["effort"] == "medium"
    assert request["retry_count"] == 0


def test_epoch33_zero_call_waiting_and_capacity_are_directly_bound() -> None:
    runtime._validate_predecessor(full_verify=False)  # noqa: SLF001
    accounting = runtime._intention_to_treat_accounting()  # noqa: SLF001
    assert accounting["predecessor_semantic_model_call_count"] == 10
    assert accounting["predecessor_unknown_usage_turn_count"] == 2
    assert accounting["predecessor_measured_total_tokens"] == 347709
    assert accounting["maximum_semantic_model_call_count_after_epoch34_dispatch"] == 11


def test_directive_allows_only_one_exact_predispatch_recovery() -> None:
    directive = json.loads(runtime.DIRECTIVE_PATH.read_text(encoding="utf-8"))
    runtime._validate_directive(directive)  # noqa: SLF001
    recovery = directive["recovery_contract"]
    assert recovery["exact_epoch33_semantic_request_reused"] is True
    assert recovery["semantic_request_change_allowed"] is False
    assert recovery["epoch33_semantic_model_call_count"] == 0
    assert recovery["epoch33_capacity_available"] is True
    assert recovery["semantic_model_call_cap"] == 1
    assert recovery["semantic_retry_cap"] == 0


def test_plan_hash_and_managed_auth_spec_are_exact() -> None:
    plan = json.loads(runtime.PLAN_PATH.read_text(encoding="utf-8"))
    digest = hashlib.sha256(runtime.DIRECTIVE_PATH.read_bytes()).hexdigest()
    assert plan["step"]["directive_sha256"] == digest
    assert runtime.SPEC.directive_sha256 == digest
    assert runtime.SPEC.model == "gpt-5.6-sol"
    assert runtime.SPEC.effort == "medium"
    assert runtime.SPEC.maximum_total_tokens == 36000
    assert runtime.SPEC.minimum_remaining_reserve_percent == 20
    assert runtime.SPEC.adapter.RETRY_COUNT == 0


def test_receipt_metadata_preserves_zero_call_predecessor_accounting() -> None:
    metadata = runtime._receipt_metadata(  # noqa: SLF001
        {
            "state": "passed",
            "semantic_model_call_count": 1,
            "unknown_usage_turn_count": 0,
            "usage": {
                "input_tokens": 15000,
                "cached_input_tokens": 0,
                "output_tokens": 11000,
                "reasoning_output_tokens": 2500,
                "total_tokens": 26000,
            },
        }
    )
    assert metadata["epoch33_zero_call_waiting_preserved"] is True
    assert metadata["full_six_case_cost_projection_pass"] is True
    assert metadata["structural_and_cost_pass"] is True
    assert metadata["aggregate_architecture_semantic_model_call_count"] == 11
    assert metadata["aggregate_architecture_unknown_usage_turn_count"] == 2
    assert metadata["aggregate_architecture_measured_total_tokens"] == 373709


def test_zero_call_runtime_freeze_rejects_request_tamper(tmp_path: Path) -> None:
    root = tmp_path / "epoch34"
    status = runtime.prepare_canary(root)
    assert status["state"] == "verified_zero_call_runtime"
    assert status["semantic_model_call_count"] == 0
    request_path = root / "prepared-turn/request.private.json"
    request = json.loads(request_path.read_text(encoding="utf-8"))
    request["effort"] = "high"
    request_path.write_text(json.dumps(request, sort_keys=True) + "\n", encoding="utf-8")
    with pytest.raises(
        (runtime.NestedLedgerRecoveryError, runtime.adapter.CanonicalV31EpisodeBatchError)
    ):
        runtime.verify_runtime(root)
