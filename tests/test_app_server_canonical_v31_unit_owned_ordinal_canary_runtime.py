from __future__ import annotations

import hashlib
import json
from pathlib import Path

from research_factory import app_server_canonical_v31_unit_owned_ordinal_canary_runtime as runtime


def test_request_is_exact_provider_compatible_two_case_recovery() -> None:
    request = runtime._build_request()  # noqa: SLF001
    assert tuple(request["segment_ids"]) == runtime.SELECTED_SEGMENT_IDS
    assert runtime._request_shape(request) == runtime.EXPECTED_SHAPE  # noqa: SLF001
    assert request["model"] == "gpt-5.6-sol"
    assert request["effort"] == "high"
    assert request["retry_count"] == 0
    assert "positional_protocol_version" not in request["semantic_integrity"]
    runtime._validate_provider_schema(request["output_schema"])  # noqa: SLF001


def test_provider_schema_rejects_tuple_and_boolean_items() -> None:
    for schema in (
        {"type": "array", "prefixItems": [{"type": "string"}], "items": False},
        {"type": "array", "items": False},
    ):
        try:
            runtime._validate_provider_schema(schema)  # noqa: SLF001
        except runtime.UnitOwnedOrdinalCanaryError:
            pass
        else:
            raise AssertionError("provider-incompatible schema was accepted")


def test_directive_binds_interrupted_epoch23_without_replay() -> None:
    directive = json.loads(runtime.DIRECTIVE_PATH.read_text())
    runtime._validate_directive(directive)  # noqa: SLF001
    recovery = directive["recovery_contract"]
    assert recovery["semantic_delta_from_epoch23"] == "none"
    assert recovery["epoch23_replay_allowed"] is False
    assert recovery["semantic_model_call_cap"] == 1
    assert recovery["semantic_retry_cap"] == 0
    assert directive["frozen_predecessor_evidence"] == (
        runtime._frozen_predecessor_evidence()  # noqa: SLF001
    )


def test_plan_and_directive_hash_are_exact() -> None:
    plan = json.loads(runtime.PLAN_PATH.read_text())
    directive_sha = hashlib.sha256(runtime.DIRECTIVE_PATH.read_bytes()).hexdigest()
    assert plan["step"]["max_model_calls"] == 1
    assert plan["step"]["max_total_tokens"] == 32000
    assert plan["step"]["directive_sha256"] == directive_sha
    assert runtime.SPEC.directive_sha256 == directive_sha


def test_predecessor_is_one_unknown_usage_call_and_no_output() -> None:
    runtime._validate_predecessor(full_verify=False)  # noqa: SLF001
    receipt = runtime._load(  # noqa: SLF001
        runtime.PREDECESSOR_ROOT / "plan-step-receipt.json", "epoch-23 receipt"
    )
    assert receipt["state"] == "waiting"
    assert receipt["new_semantic_model_call_count"] == 1
    assert receipt["new_unknown_usage_turn_count"] == 1
    assert receipt["semantic_retry_count"] == 0
    assert not (runtime.PREDECESSOR_ROOT / "turn/output.private.json").exists()


def test_spec_is_managed_auth_zero_retry_and_nonpromoting() -> None:
    assert runtime.SPEC.maximum_total_tokens == 32000
    assert runtime.SPEC.minimum_remaining_reserve_percent == 10
    assert runtime.SPEC.adapter.RETRY_COUNT == 0
    assert runtime.SPEC.pass_next_action == (
        "freeze_two_case_full_event_ab_ba_quality_and_cost_projection"
    )
    directive = json.loads(runtime.DIRECTIVE_PATH.read_text())
    assert directive["promotion_contract"]["holdout_authorized"] is False
    assert directive["promotion_contract"]["production_mutation_allowed"] is False


def test_runtime_module_set_is_additive_and_does_not_edit_predecessor() -> None:
    paths = {Path(path).name for path in runtime._runtime_module_paths()}  # noqa: SLF001
    assert Path(runtime.__file__).name in paths
    assert Path(runtime.adapter.__file__).name in paths
    assert Path(runtime.epoch23.__file__).name in paths
