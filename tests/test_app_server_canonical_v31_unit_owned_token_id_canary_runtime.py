from __future__ import annotations

import hashlib
import json

from research_factory import app_server_canonical_v31_unit_owned_token_id_canary_runtime as runtime


def test_request_is_exact_two_case_token_id_canary() -> None:
    request = runtime._build_request()  # noqa: SLF001
    assert tuple(request["segment_ids"]) == runtime.SELECTED_SEGMENT_IDS
    assert runtime._request_shape(request) == runtime.EXPECTED_SHAPE  # noqa: SLF001
    assert request["model"] == "gpt-5.6-sol"
    assert request["effort"] == "high"
    assert request["retry_count"] == 0


def test_epoch24_diagnostic_is_recomputed_without_semantic_text() -> None:
    assert runtime._epoch24_diagnostic() == {  # noqa: SLF001
        "event_count": 30,
        "concept_count": 13,
        "event_owner_container_mismatch_count": 0,
        "concept_owner_container_mismatch_count": 1,
        "nonnull_metric_range_count": 9,
        "invalid_numeric_metric_range_count": 1,
        "measured_total_tokens": 29079,
        "case_count_scaled_full_run_token_projection": 67630,
    }


def test_directive_ranks_three_architectures_and_binds_no_replay() -> None:
    directive = json.loads(runtime.DIRECTIVE_PATH.read_text())
    runtime._validate_directive(directive)  # noqa: SLF001
    assert [row["rank"] for row in directive["architecture_ranking"]] == [1, 2, 3]
    assert sum(row["selected"] for row in directive["architecture_ranking"]) == 1
    assert directive["canary_contract"]["epoch24_replay_allowed"] is False
    assert directive["promotion_contract"]["quality_threshold"] == 0.97


def test_plan_and_directive_hash_are_exact() -> None:
    plan = json.loads(runtime.PLAN_PATH.read_text())
    directive_sha = hashlib.sha256(runtime.DIRECTIVE_PATH.read_bytes()).hexdigest()
    assert plan["step"]["max_model_calls"] == 1
    assert plan["step"]["max_total_tokens"] == 32000
    assert plan["step"]["directive_sha256"] == directive_sha
    assert runtime.SPEC.directive_sha256 == directive_sha


def test_predecessor_binds_measured_rejection_and_unknown_usage_attempt() -> None:
    runtime._validate_predecessor(full_verify=False)  # noqa: SLF001
    records = runtime._predecessor_records()  # noqa: SLF001
    assert set(records) == {
        "epoch24_measured_structural_rejection",
        "epoch23_interrupted_unknown_usage_attempt",
    }


def test_cost_hypothesis_has_positive_frozen_margin() -> None:
    diagnostic = runtime._epoch24_diagnostic()  # noqa: SLF001
    assert diagnostic["case_count_scaled_full_run_token_projection"] == 67630
    assert runtime.FULL_PRODUCTION_TOKEN_CEILING - 67630 == 6296


def test_spec_is_managed_auth_zero_retry_and_nonpromoting() -> None:
    assert runtime.SPEC.maximum_total_tokens == 32000
    assert runtime.SPEC.minimum_remaining_reserve_percent == 10
    assert runtime.SPEC.adapter.RETRY_COUNT == 0
    directive = json.loads(runtime.DIRECTIVE_PATH.read_text())
    assert directive["promotion_contract"]["holdout_authorized"] is False
    assert directive["promotion_contract"]["production_mutation_allowed"] is False
