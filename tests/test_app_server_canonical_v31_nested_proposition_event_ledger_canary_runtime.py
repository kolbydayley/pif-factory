from __future__ import annotations

import hashlib
import json
from functools import lru_cache
from pathlib import Path

import pytest

from research_factory import (
    app_server_canonical_v31_nested_proposition_event_ledger_canary_runtime as runtime,
)


@lru_cache(maxsize=1)
def _request() -> dict:
    return runtime._build_request()  # noqa: SLF001


def test_request_is_exact_nested_ledger_medium_effort_canary() -> None:
    request = _request()
    assert tuple(request["segment_ids"]) == runtime.SELECTED_SEGMENT_IDS
    assert runtime._request_shape(request) == runtime.EXPECTED_SHAPE  # noqa: SLF001
    assert runtime._request_sha256(request) == runtime.EXPECTED_REQUEST_SHA256  # noqa: SLF001
    assert request["model"] == "gpt-5.6-sol"
    assert request["effort"] == "medium"
    assert request["retry_count"] == 0
    assert request["semantic_integrity"]["ledger_protocol_version"] == (
        runtime.adapter.LEDGER_PROTOCOL_VERSION
    )


def test_epoch32_failure_is_directly_bound_and_never_replayed() -> None:
    runtime._validate_predecessor(full_verify=False)  # noqa: SLF001
    accounting = runtime._intention_to_treat_accounting()  # noqa: SLF001
    assert accounting == {
        "predecessor_semantic_model_call_count": 10,
        "predecessor_unknown_usage_turn_count": 2,
        "predecessor_measured_total_tokens": 347709,
        "maximum_semantic_model_call_count_after_epoch33_dispatch": 11,
        "predecessor_replay_allowed": False,
    }
    assert runtime._epoch32_output_shape() == {  # noqa: SLF001
        "inventory_item_count": 40,
        "event_count": 40,
        "concept_count": 22,
        "pointer_mismatch_count": 2,
    }


def test_directive_predeclares_distinct_architecture_and_stop_rule() -> None:
    directive = json.loads(runtime.DIRECTIVE_PATH.read_text(encoding="utf-8"))
    runtime._validate_directive(directive)  # noqa: SLF001
    assert [row["rank"] for row in directive["architecture_ranking"]] == [1, 2, 3]
    assert sum(row["selected"] for row in directive["architecture_ranking"]) == 1
    contract = directive["canary_contract"]
    assert contract["model_authors_one_nested_full_canonical_event_per_proposition"] is True
    assert contract["no_duplicate_inventory_event_evidence_transport"] is True
    assert contract["deterministic_ledger_unwrap_only"] is True
    assert contract["deterministic_semantic_repair_allowed"] is False
    assert contract["epoch32_extraction_replay_allowed"] is False


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


def test_receipt_metadata_reconciles_usage_and_cost_projection() -> None:
    metadata = runtime._receipt_metadata(  # noqa: SLF001
        {
            "state": "passed",
            "semantic_model_call_count": 1,
            "unknown_usage_turn_count": 0,
            "usage": {
                "input_tokens": 15000,
                "cached_input_tokens": 1000,
                "output_tokens": 11000,
                "reasoning_output_tokens": 2500,
                "total_tokens": 26000,
            },
        }
    )
    assert metadata["full_six_case_total_token_projection_by_case_count"] <= 73926
    assert metadata["full_six_case_cost_projection_pass"] is True
    assert metadata["structural_and_cost_pass"] is True
    assert metadata["aggregate_architecture_semantic_model_call_count"] == 11
    assert metadata["aggregate_architecture_unknown_usage_turn_count"] == 2
    assert metadata["aggregate_architecture_measured_total_tokens"] == 373709


def test_zero_call_runtime_freeze_rejects_request_tamper(tmp_path: Path) -> None:
    root = tmp_path / "epoch33"
    status = runtime.prepare_canary(root)
    assert status["state"] == "verified_zero_call_runtime"
    assert status["semantic_model_call_count"] == 0
    request_path = root / "prepared-turn/request.private.json"
    request = json.loads(request_path.read_text(encoding="utf-8"))
    request["effort"] = "high"
    request_path.write_text(json.dumps(request, sort_keys=True) + "\n", encoding="utf-8")
    with pytest.raises(
        (runtime.NestedLedgerCanaryError, runtime.adapter.CanonicalV31EpisodeBatchError)
    ):
        runtime.verify_runtime(root)
