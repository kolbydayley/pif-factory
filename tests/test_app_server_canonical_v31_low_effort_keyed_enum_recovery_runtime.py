from __future__ import annotations

import hashlib
import json
from functools import lru_cache
from pathlib import Path

import pytest

from research_factory import (
    app_server_canonical_v31_low_effort_keyed_enum_recovery_runtime as runtime,
)


@lru_cache(maxsize=1)
def _request() -> dict:
    return runtime._build_request()  # noqa: SLF001


def test_request_is_exact_low_effort_fresh_identity_recovery() -> None:
    request = _request()
    assert tuple(request["segment_ids"]) == runtime.SELECTED_SEGMENT_IDS
    assert runtime._request_shape(request) == runtime.EXPECTED_SHAPE  # noqa: SLF001
    assert runtime._request_sha256(request) == runtime.EXPECTED_REQUEST_SHA256  # noqa: SLF001
    assert request["model"] == "gpt-5.6-sol"
    assert request["effort"] == "low"
    assert request["retry_count"] == 0
    assert request["candidate_system_id"] == runtime.adapter.CANDIDATE_SYSTEM_ID
    integrity = request["semantic_integrity"]
    assert integrity["low_effort_timeout_recovery_only"] is True
    assert integrity["model_authors_closed_keyed_canonical_enum_object"] is True
    assert integrity["all_canonical_enum_fields_are_position_specific"] is True
    assert integrity["deterministic_keyed_enum_order_projection_only"] is True


def test_epoch43_interruption_is_bound_as_unknown_usage_and_never_replayed() -> None:
    runtime._validate_predecessor(full_verify=False)  # noqa: SLF001
    records = runtime._predecessor_records()  # noqa: SLF001
    predecessor = records["epoch43_interrupted_keyed_enum_attempt"]
    assert predecessor["receipt"]["sha256"] == runtime.EPOCH43_HASHES["receipt"]
    assert predecessor["sidecar"]["sha256"] == runtime.EPOCH43_HASHES["sidecar"]
    assert "output" not in predecessor
    assert runtime.PREDECESSOR_MODEL_CALLS == 22
    assert runtime.PREDECESSOR_UNKNOWN_USAGE == 4
    assert runtime.PREDECESSOR_MEASURED_TOKENS == 896_728


def test_directive_authorizes_only_one_low_effort_timeout_recovery() -> None:
    directive = json.loads(runtime.DIRECTIVE_PATH.read_text(encoding="utf-8"))
    runtime._validate_directive(directive)  # noqa: SLF001
    predecessor = directive["predecessor_evidence"]
    assert predecessor["epoch43_state"] == "waiting"
    assert predecessor["epoch43_output_absent"] is True
    assert predecessor["semantic_replay_authorized"] is False
    contract = directive["recovery_contract"]
    assert contract["candidate_reasoning_effort"] == "low"
    assert contract["fresh_request_identity_required"] is True
    assert contract["prompt_base_schema_and_source_semantics_unchanged"] is True
    assert contract["semantic_model_call_cap"] == 1
    assert contract["semantic_retry_cap"] == 0
    assert contract["deterministic_semantic_pruning_allowed"] is False
    assert contract["predecessor_semantic_replay_allowed"] is False


def test_plan_hash_and_managed_auth_spec_are_exact() -> None:
    plan = json.loads(runtime.PLAN_PATH.read_text(encoding="utf-8"))
    digest = hashlib.sha256(runtime.DIRECTIVE_PATH.read_bytes()).hexdigest()
    assert plan["step"]["directive_sha256"] == digest
    assert runtime.SPEC.directive_sha256 == digest
    assert runtime.SPEC.model == "gpt-5.6-sol"
    assert runtime.SPEC.effort == "low"
    assert runtime.SPEC.maximum_total_tokens == 40_000
    assert runtime.SPEC.minimum_remaining_reserve_percent == 20
    assert runtime.SPEC.adapter.RETRY_COUNT == 0


def test_receipt_metadata_passes_only_structural_and_cost_gate() -> None:
    metadata = runtime._receipt_metadata(  # noqa: SLF001
        {
            "state": "passed",
            "semantic_model_call_count": 1,
            "unknown_usage_turn_count": 0,
            "usage": {
                "input_tokens": 15_000,
                "cached_input_tokens": 1_000,
                "output_tokens": 10_000,
                "reasoning_output_tokens": 2_000,
                "total_tokens": 25_000,
            },
        }
    )
    assert metadata["full_six_case_total_token_projection_by_case_count"] <= 73_926
    assert metadata["structural_and_cost_pass"] is True
    assert metadata["fresh_full_event_ab_ba_quality_required"] is True
    assert metadata["low_effort_timeout_recovery_only"] is True
    assert metadata["aggregate_architecture_semantic_model_call_count"] == 23
    assert metadata["aggregate_architecture_unknown_usage_turn_count"] == 4
    assert metadata["aggregate_architecture_measured_total_tokens"] == 921_728


def test_waiting_metadata_preserves_second_unknown_usage_without_quality() -> None:
    metadata = runtime._receipt_metadata(  # noqa: SLF001
        {
            "state": "waiting",
            "semantic_model_call_count": 1,
            "unknown_usage_turn_count": 1,
            "usage": {
                "input_tokens": 0,
                "cached_input_tokens": 0,
                "output_tokens": 0,
                "reasoning_output_tokens": 0,
                "total_tokens": 0,
            },
        }
    )
    assert metadata["full_six_case_total_token_projection_by_case_count"] is None
    assert metadata["structural_and_cost_pass"] is False
    assert metadata["fresh_full_event_ab_ba_quality_required"] is False
    assert metadata["aggregate_architecture_semantic_model_call_count"] == 23
    assert metadata["aggregate_architecture_unknown_usage_turn_count"] == 5
    assert metadata["aggregate_architecture_measured_total_tokens"] == 896_728


def test_zero_call_runtime_freeze_rejects_request_tamper(tmp_path: Path) -> None:
    root = tmp_path / "epoch44"
    status = runtime.prepare_recovery(root)
    assert status["state"] == "verified_zero_call_runtime"
    assert status["semantic_model_call_count"] == 0
    request_path = root / "prepared-turn/request.private.json"
    request = json.loads(request_path.read_text(encoding="utf-8"))
    request["effort"] = "medium"
    request_path.write_text(json.dumps(request, sort_keys=True) + "\n", encoding="utf-8")
    with pytest.raises(
        (runtime.LowEffortRecoveryError, runtime.adapter.CanonicalV31EpisodeBatchError)
    ):
        runtime.verify_runtime(root)
