from __future__ import annotations

import hashlib
import json

from research_factory import app_server_canonical_v31_unit_owned_positional_canary_runtime as runtime


def test_request_is_exact_two_case_falsification_canary() -> None:
    request = runtime._build_request()  # noqa: SLF001
    assert tuple(request["segment_ids"]) == runtime.SELECTED_SEGMENT_IDS
    assert runtime._request_shape(request) == runtime.EXPECTED_SHAPE  # noqa: SLF001
    assert request["model"] == "gpt-5.6-sol"
    assert request["effort"] == "high"
    assert request["retry_count"] == 0


def test_directive_ranks_three_distinct_architectures() -> None:
    directive = json.loads(runtime.DIRECTIVE_PATH.read_text())
    runtime._validate_directive(directive)  # noqa: SLF001
    ranking = directive["architecture_ranking"]
    assert [row["rank"] for row in ranking] == [1, 2, 3]
    assert sum(row["selected"] for row in ranking) == 1
    assert directive["promotion_contract"]["quality_threshold"] == 0.97


def test_plan_and_directive_hash_are_exact() -> None:
    plan = json.loads(runtime.PLAN_PATH.read_text())
    assert plan["step"]["max_model_calls"] == 1
    assert plan["step"]["max_total_tokens"] == 32000
    assert plan["step"]["directive_sha256"] == hashlib.sha256(
        runtime.DIRECTIVE_PATH.read_bytes()
    ).hexdigest()
    assert runtime.SPEC.directive_sha256 == plan["step"]["directive_sha256"]


def test_predecessor_quality_rejection_and_source_are_bound() -> None:
    runtime._validate_predecessor(full_verify=False)  # noqa: SLF001
    records = runtime._predecessor_records()  # noqa: SLF001
    assert set(records) == {"epoch19_source", "epoch22_quality_rejection"}


def test_spec_is_managed_auth_zero_retry_and_nonpromoting() -> None:
    assert runtime.SPEC.maximum_total_tokens == 32000
    assert runtime.SPEC.minimum_remaining_reserve_percent == 10
    assert runtime.SPEC.adapter.RETRY_COUNT == 0
    assert runtime.SPEC.pass_next_action == (
        "freeze_two_case_full_event_ab_ba_quality_and_cost_projection"
    )
