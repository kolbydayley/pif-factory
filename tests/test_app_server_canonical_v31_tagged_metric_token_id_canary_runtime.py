from __future__ import annotations

import hashlib
import json
from functools import lru_cache

from research_factory import app_server_canonical_v31_tagged_metric_token_id_canary_runtime as runtime


@lru_cache(maxsize=1)
def _request() -> dict:
    return runtime._build_request()  # noqa: SLF001


def test_request_is_exact_medium_effort_tagged_metric_canary() -> None:
    request = _request()
    assert tuple(request["segment_ids"]) == runtime.SELECTED_SEGMENT_IDS
    assert runtime._request_shape(request) == runtime.EXPECTED_SHAPE  # noqa: SLF001
    assert runtime._request_sha256(request) == runtime.EXPECTED_REQUEST_SHA256  # noqa: SLF001
    assert request["model"] == "gpt-5.6-sol"
    assert request["effort"] == "medium"
    assert request["retry_count"] == 0
    assert request["semantic_integrity"]["tagged_metric_protocol_version"] == (
        runtime.adapter.TAGGED_METRIC_PROTOCOL_VERSION
    )


def test_predecessor_accounting_is_intention_to_treat_and_no_replay() -> None:
    runtime._validate_predecessor(full_verify=False)  # noqa: SLF001
    accounting = runtime._intention_to_treat_accounting()  # noqa: SLF001
    assert accounting["predecessor_semantic_model_call_count"] == 4
    assert accounting["predecessor_measured_call_count"] == 2
    assert accounting["predecessor_unknown_usage_turn_count"] == 2
    assert accounting["predecessor_measured_total_tokens"] == 49384
    assert accounting["predecessor_replay_allowed"] is False


def test_directive_ranks_three_architectures_and_forbids_semantic_repair() -> None:
    directive = json.loads(runtime.DIRECTIVE_PATH.read_text())
    runtime._validate_directive(directive)  # noqa: SLF001
    assert [row["rank"] for row in directive["architecture_ranking"]] == [1, 2, 3]
    assert sum(row["selected"] for row in directive["architecture_ranking"]) == 1
    assert directive["canary_contract"][
        "explicit_metric_bundle_cardinality_is_model_authored"
    ] is True
    assert directive["canary_contract"]["deterministic_semantic_repair_allowed"] is False
    assert directive["canary_contract"]["epoch26_replay_allowed"] is False


def test_plan_and_directive_hash_are_exact() -> None:
    plan = json.loads(runtime.PLAN_PATH.read_text())
    directive_sha = hashlib.sha256(runtime.DIRECTIVE_PATH.read_bytes()).hexdigest()
    assert plan["step"]["max_model_calls"] == 1
    assert plan["step"]["max_total_tokens"] == 32000
    assert plan["step"]["directive_sha256"] == directive_sha
    assert runtime.SPEC.directive_sha256 == directive_sha


def test_receipt_metadata_reconciles_all_predecessor_usage() -> None:
    metadata = runtime._receipt_metadata(  # noqa: SLF001
        {
            "state": "passed",
            "semantic_model_call_count": 1,
            "unknown_usage_turn_count": 0,
            "usage": {
                "input_tokens": 14500,
                "cached_input_tokens": 0,
                "output_tokens": 10000,
                "reasoning_output_tokens": 1000,
                "total_tokens": 24500,
            },
        }
    )
    assert metadata["aggregate_architecture_semantic_model_call_count"] == 5
    assert metadata["aggregate_architecture_unknown_usage_turn_count"] == 2
    assert metadata["aggregate_architecture_measured_total_tokens"] == 73884
    assert metadata["predecessor_accounting_reconciled_additively"] is True


def test_spec_is_managed_auth_zero_retry_and_nonpromoting() -> None:
    assert runtime.SPEC.model == "gpt-5.6-sol"
    assert runtime.SPEC.effort == "medium"
    assert runtime.SPEC.maximum_total_tokens == 32000
    assert runtime.SPEC.minimum_remaining_reserve_percent == 10
    assert runtime.SPEC.adapter.RETRY_COUNT == 0
    directive = json.loads(runtime.DIRECTIVE_PATH.read_text())
    assert directive["transport_contract"]["managed_chatgpt_pro_auth_required"] is True
    assert directive["promotion_contract"]["holdout_authorized"] is False
    assert directive["promotion_contract"]["production_mutation_allowed"] is False
