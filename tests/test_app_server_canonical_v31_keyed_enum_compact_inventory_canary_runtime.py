from __future__ import annotations

import hashlib
import json
from functools import lru_cache
from pathlib import Path

import pytest

from research_factory import (
    app_server_canonical_v31_keyed_enum_compact_inventory_canary_runtime as runtime,
)


@lru_cache(maxsize=1)
def _request() -> dict:
    return runtime._build_request()  # noqa: SLF001


def test_request_is_exact_keyed_enum_medium_effort_canary() -> None:
    request = _request()
    assert tuple(request["segment_ids"]) == runtime.SELECTED_SEGMENT_IDS
    assert runtime._request_shape(request) == runtime.EXPECTED_SHAPE  # noqa: SLF001
    assert runtime._request_sha256(request) == runtime.EXPECTED_REQUEST_SHA256  # noqa: SLF001
    assert request["model"] == "gpt-5.6-sol"
    assert request["effort"] == "medium"
    assert request["retry_count"] == 0
    integrity = request["semantic_integrity"]
    assert (
        integrity["keyed_enum_protocol_version"]
        == runtime.adapter.KEYED_ENUM_PROTOCOL_VERSION
    )
    assert integrity["model_authors_closed_keyed_canonical_enum_object"] is True
    assert integrity["all_canonical_enum_fields_are_position_specific"] is True
    assert integrity["deterministic_keyed_enum_order_projection_only"] is True


def test_epoch42_rejection_is_directly_bound_and_never_replayed() -> None:
    runtime._validate_predecessor(full_verify=False)  # noqa: SLF001
    records = runtime._predecessor_records()  # noqa: SLF001
    predecessor = records["epoch42_rejected_compact_inventory_attempt"]
    assert predecessor["receipt"]["sha256"] == runtime.EPOCH42_HASHES["receipt"]
    assert predecessor["sidecar"]["sha256"] == runtime.EPOCH42_HASHES["sidecar"]
    assert predecessor["output"]["sha256"] == runtime.EPOCH42_HASHES["output"]
    assert predecessor["rejection"]["sha256"] == runtime.EPOCH42_HASHES["rejection"]
    assert runtime.PREDECESSOR_MODEL_CALLS == 21
    assert runtime.PREDECESSOR_UNKNOWN_USAGE == 3
    assert runtime.PREDECESSOR_MEASURED_TOKENS == 896_728


def test_directive_predeclares_schema_architecture_and_nonsemantic_boundary() -> None:
    directive = json.loads(runtime.DIRECTIVE_PATH.read_text(encoding="utf-8"))
    runtime._validate_directive(directive)  # noqa: SLF001
    assert [row["rank"] for row in directive["architecture_ranking"]] == [1, 2, 3]
    assert directive["architecture_ranking"][0]["selected"] is True
    contract = directive["canary_contract"]
    assert (
        contract["keyed_enum_protocol_version"]
        == runtime.adapter.KEYED_ENUM_PROTOCOL_VERSION
    )
    assert contract["model_authors_closed_keyed_canonical_enum_object"] is True
    assert contract["all_canonical_enum_fields_are_position_specific"] is True
    assert contract["deterministic_keyed_enum_order_projection_only"] is True
    assert contract["deterministic_semantic_relabeling_allowed"] is False
    assert contract["deterministic_semantic_pruning_allowed"] is False
    assert contract["predecessor_semantic_replay_allowed"] is False


def test_plan_hash_and_managed_auth_spec_are_exact() -> None:
    plan = json.loads(runtime.PLAN_PATH.read_text(encoding="utf-8"))
    digest = hashlib.sha256(runtime.DIRECTIVE_PATH.read_bytes()).hexdigest()
    assert plan["step"]["directive_sha256"] == digest
    assert runtime.SPEC.directive_sha256 == digest
    assert runtime.SPEC.model == "gpt-5.6-sol"
    assert runtime.SPEC.effort == "medium"
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
    assert metadata["aggregate_architecture_semantic_model_call_count"] == 22
    assert metadata["aggregate_architecture_unknown_usage_turn_count"] == 3
    assert metadata["aggregate_architecture_measured_total_tokens"] == 921_728


def test_cost_failure_becomes_rejected_without_semantic_proxy() -> None:
    metadata = runtime._receipt_metadata(  # noqa: SLF001
        {
            "state": "passed",
            "semantic_model_call_count": 1,
            "unknown_usage_turn_count": 0,
            "usage": {
                "input_tokens": 20_000,
                "cached_input_tokens": 0,
                "output_tokens": 20_000,
                "reasoning_output_tokens": 5_000,
                "total_tokens": 40_000,
            },
        }
    )
    assert metadata["state"] == "rejected"
    assert metadata["full_six_case_cost_projection_pass"] is False
    assert metadata["structural_and_cost_pass"] is False
    assert metadata["event_count_is_diagnostic_only"] is True


def test_zero_call_runtime_freeze_rejects_request_tamper(tmp_path: Path) -> None:
    root = tmp_path / "epoch43"
    status = runtime.prepare_canary(root)
    assert status["state"] == "verified_zero_call_runtime"
    assert status["semantic_model_call_count"] == 0
    request_path = root / "prepared-turn/request.private.json"
    request = json.loads(request_path.read_text(encoding="utf-8"))
    request["effort"] = "high"
    request_path.write_text(json.dumps(request, sort_keys=True) + "\n", encoding="utf-8")
    with pytest.raises(
        (runtime.KeyedEnumCanaryError, runtime.adapter.CanonicalV31EpisodeBatchError)
    ):
        runtime.verify_runtime(root)
