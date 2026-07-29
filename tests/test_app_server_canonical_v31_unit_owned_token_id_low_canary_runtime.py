from __future__ import annotations

import hashlib
import json

from research_factory import (
    app_server_canonical_v31_unit_owned_token_id_low_canary_runtime as runtime,
)


def test_request_is_exact_low_effort_system_variant() -> None:
    request = runtime._build_request()  # noqa: SLF001
    parent = runtime.adapter._parent_request(request)  # noqa: SLF001
    assert tuple(request["segment_ids"]) == runtime.SELECTED_SEGMENT_IDS
    assert runtime._request_shape(request) == runtime.EXPECTED_SHAPE  # noqa: SLF001
    assert runtime._request_sha256(request) == runtime.EXPECTED_REQUEST_SHA256  # noqa: SLF001
    assert request["model"] == "gpt-5.6-sol"
    assert request["effort"] == "low"
    assert request["retry_count"] == 0
    assert request["prompt"] == parent["prompt"]
    assert request["base_instructions"] == parent["base_instructions"]
    assert request["output_schema"] == parent["output_schema"]


def test_predecessor_reconciles_two_unknown_usage_attempts_without_replay() -> None:
    runtime._validate_predecessor(full_verify=False)  # noqa: SLF001
    accounting = runtime._intention_to_treat_accounting()  # noqa: SLF001
    assert accounting["predecessor_semantic_model_call_count"] == 3
    assert accounting["predecessor_unknown_usage_turn_count"] == 2
    assert accounting["predecessor_measured_total_tokens"] == 29079
    assert accounting["predecessor_replay_allowed"] is False
    assert set(runtime._predecessor_records()) == {  # noqa: SLF001
        "epoch25_interrupted_unknown_usage_attempt",
        "epoch24_measured_structural_rejection",
        "epoch23_interrupted_unknown_usage_attempt",
    }


def test_directive_ranks_three_distinct_architectures_and_binds_low_effort() -> None:
    directive = json.loads(runtime.DIRECTIVE_PATH.read_text())
    runtime._validate_directive(directive)  # noqa: SLF001
    assert [row["rank"] for row in directive["architecture_ranking"]] == [1, 2, 3]
    assert sum(row["selected"] for row in directive["architecture_ranking"]) == 1
    assert directive["canary_contract"]["candidate_reasoning_effort"] == "low"
    assert directive["canary_contract"]["epoch25_replay_allowed"] is False
    assert directive["promotion_contract"]["quality_threshold"] == 0.97


def test_plan_and_directive_hash_are_exact() -> None:
    plan = json.loads(runtime.PLAN_PATH.read_text())
    directive_sha = hashlib.sha256(runtime.DIRECTIVE_PATH.read_bytes()).hexdigest()
    assert plan["step"]["max_model_calls"] == 1
    assert plan["step"]["max_total_tokens"] == 30000
    assert plan["step"]["directive_sha256"] == directive_sha
    assert runtime.SPEC.directive_sha256 == directive_sha


def test_receipt_accounting_adds_current_attempt_to_intention_to_treat() -> None:
    metadata = runtime._receipt_metadata(  # noqa: SLF001
        {
            "state": "passed",
            "semantic_model_call_count": 1,
            "unknown_usage_turn_count": 0,
            "usage": {
                "input_tokens": 14000,
                "cached_input_tokens": 0,
                "output_tokens": 14000,
                "reasoning_output_tokens": 2000,
                "total_tokens": 28000,
            },
        }
    )
    assert metadata["aggregate_architecture_semantic_model_call_count"] == 4
    assert metadata["aggregate_architecture_unknown_usage_turn_count"] == 2
    assert metadata["aggregate_architecture_measured_total_tokens"] == 57079
    assert metadata["predecessor_accounting_reconciled_additively"] is True


def test_waiting_receipt_accounting_preserves_current_unknown_usage() -> None:
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
    assert metadata["aggregate_architecture_semantic_model_call_count"] == 4
    assert metadata["aggregate_architecture_unknown_usage_turn_count"] == 3
    assert metadata["full_six_case_total_token_projection_by_case_count"] is None


def test_spec_is_managed_auth_zero_retry_and_nonpromoting() -> None:
    assert runtime.SPEC.model == "gpt-5.6-sol"
    assert runtime.SPEC.effort == "low"
    assert runtime.SPEC.maximum_total_tokens == 30000
    assert runtime.SPEC.minimum_remaining_reserve_percent == 10
    assert runtime.SPEC.adapter.RETRY_COUNT == 0
    directive = json.loads(runtime.DIRECTIVE_PATH.read_text())
    assert directive["transport_contract"]["managed_chatgpt_pro_auth_required"] is True
    assert directive["promotion_contract"]["holdout_authorized"] is False
    assert directive["promotion_contract"]["production_mutation_allowed"] is False
