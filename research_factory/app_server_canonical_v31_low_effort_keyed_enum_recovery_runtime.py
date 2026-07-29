from __future__ import annotations

"""Epoch-44 low-effort recovery for the interrupted keyed-enum canary."""

import argparse
import asyncio
import copy
import json
import math
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import (
    app_server_canonical_v31_low_effort_keyed_enum_compact_inventory_episode_batch as adapter,
)
from . import app_server_one_turn_canary_runtime as one_turn


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_ROOT = (
    PROJECT_ROOT / "work/app-server-development-v2/unattended-pipeline-v5"
).resolve()
DEFAULT_ROOT = (
    PIPELINE_ROOT / "canonical-v31-epoch44-low-effort-keyed-enum-recovery-v1"
).resolve()
EPOCH27_ROOT = (
    PIPELINE_ROOT / "canonical-v31-epoch27-tagged-metric-token-id-canary-v1"
).resolve()
EPOCH43_ROOT = (
    PIPELINE_ROOT / "canonical-v31-epoch43-keyed-enum-compact-inventory-canary-v1"
).resolve()
EPOCH43_DIRECTIVE_PATH = (
    PROJECT_ROOT
    / "automation/pif-evaluation-epoch43-keyed-enum-compact-inventory-canary-v43.json"
).resolve()
EPOCH43_PLAN_PATH = (
    PROJECT_ROOT / "automation/pif-evaluation-semantic-plan-v43.json"
).resolve()
DIRECTIVE_PATH = (
    PROJECT_ROOT
    / "automation/pif-evaluation-epoch44-low-effort-keyed-enum-recovery-v44.json"
).resolve()
PLAN_PATH = (PROJECT_ROOT / "automation/pif-evaluation-semantic-plan-v44.json").resolve()
DIRECTIVE_SHA256 = "31c085c69e247ccc806da288da08986642d257606f9843d7b08c97ce1a9b58ee"
THREAD_ID = "019f4cf1-c46e-7db3-acd2-bf03c4459a10"
PLAN_EPOCH = 44
STEP_ID = "canonical_v31_epoch44_low_effort_keyed_enum_recovery_v44"
TURN_NAME = "epoch44_low_effort_keyed_enum_timeout_recovery"
AUTHORIZATION_STATEMENT = (
    "You have my full permission to continue. No need to seek out my approval anymore."
)
AUTHORIZATION_ID_DEFAULT = "kolby-epoch44-low-effort-keyed-enum-recovery-20260720"
SELECTED_SEGMENT_IDS = (
    "seg_80fe585badf8f3d3597d0f96",
    "seg_7c027e01ed7e813b528c5c7f",
)
MAXIMUM_TOTAL_TOKENS = 40_000
MAXIMUM_WALL_SECONDS = 1_200
MINIMUM_REMAINING_RESERVE_PERCENT = 20
CAPACITY_SAFETY_MARGIN_PERCENT = 1
QUOTA_POINTS_PER_MILLION_TOKENS = 17
AUTHORIZATION_WINDOW_SECONDS = 14_400
EXPECTED_SHAPE = {
    "prompt_bytes": 14_277,
    "base_bytes": 25_177,
    "schema_bytes": 6_600,
    "request_bytes": 316_144,
    "source_unit_count": 20,
    "evidence_span_count": 38,
    "literal_token_count": 2_020,
}
EXPECTED_REQUEST_SHA256 = "a34a28cd78297a5c951ebddb71575842e30ade1e84f8e7e4958502c6c0d59ac4"
EXPECTED_PROMPT_SHA256 = "36af4c581e70487d72887f225ed18430d4b5c35b02370fed0a0257f34bc3a1c1"
EXPECTED_BASE_SHA256 = "a7e9aa5e2575cdb8196c31db4eb07a3de497d0dae5d4986731c58a180069f727"
EXPECTED_SCHEMA_SHA256 = "37ae90f1a9e585d1ed0cd2ea5e8dcece0e60c8e5575e5d1d3beb9976831b7e44"
CANARY_VISIBLE_REQUEST_BYTES = 46_054
FULL_VISIBLE_REQUEST_BYTES = 73_991
FULL_CASE_SCALE = 3
FULL_PRODUCTION_TOKEN_CEILING = 73_926
EPOCH27_REQUEST_SHA256 = "f94df0a9f66f3de82a67f6442e37e5f6f0e563794e6722e5a6e56cc1e02513b6"
EPOCH43_HASHES = {
    "authorization": "99162bf9040649555c3181655637a8bd0686933a5613738d16b94979ebac62bb",
    "receipt": "b27184b28501b2d2d289d371319ee1c4f56c862ea9ba61434fb961f21a0ce6c0",
    "terminal": "b27184b28501b2d2d289d371319ee1c4f56c862ea9ba61434fb961f21a0ce6c0",
    "runtime_lock": "6add3d1b0b090806c91edb134759acf44d41427b232f6f632a8cdf17e7d51652",
    "runtime_contract": "1d1af48f0f186ad568d6d982f3d8a99e05d8a679898bd43264e0a02222e2ab62",
    "request": "0ed1711039859fa32e28e9840f4a464b7f4d240ed5c2c17e29adc8e91b0ba29c",
    "attempt": "7343147952c60ae268be5ed77f890e3bddd980c9cb7134d6331629a9b68c84c1",
    "dispatch": "0d022ccdc0963dcb87d77d57fe2a6bdf9d9e7526a2e6f56f507bc15dd755f144",
    "thread_start": "43964517dc30cdb56384e0d456fd1e9174b482fd0722b6ef96dda1a02495719a",
    "sidecar": "ef8dccc361f4a083f015a418c67f69b96f03c8fddd8c1e9ab8e31d08cde7feb6",
    "initial_capacity_request": "4039a7b8d5db4dd7d0add3fb4e5e58036903a1889299c06e2d4c92040e959232",
    "initial_capacity_measurement": "a9f782a7467d0e917e466b27df92982c1572bc73a4eed1c128d09adc2502cc75",
    "initial_capacity_provider_response": "7915f105547df4b2c11c74c7f38275d72dca8a2abb078f8a149a34ca9eafcf9c",
    "preturn_capacity_request": "9844ee066ca8a344ad2ff9a05234182670c306b3fb37c235ef85e719722ce8ef",
    "preturn_capacity_measurement": "088142c8db362eafcbc9fa10d7cb8daf9bd67fda34c60afc34470dc653156080",
    "preturn_capacity_provider_response": "7915f105547df4b2c11c74c7f38275d72dca8a2abb078f8a149a34ca9eafcf9c",
    "directive": "520be2e5e4b2c4402a7c7dc0de1f7d69352a8a77ef0196bfa72cabef85445808",
    "plan": "20db0364b95c1e7ec48bd73c01eec0bb96cd298935d2400d6700f9beddcbe6a6",
}
PREDECESSOR_MODEL_CALLS = 22
PREDECESSOR_UNKNOWN_USAGE = 4
PREDECESSOR_MEASURED_TOKENS = 896_728

LowEffortRecoveryError = one_turn.OneTurnCanaryError
LowEffortRecoveryWaiting = one_turn.OneTurnCanaryWaiting


def _load(path: Path, label: str) -> dict[str, Any]:
    return one_turn._load(path, label)  # noqa: SLF001


def _record(path: Path) -> dict[str, Any]:
    return one_turn._record(path)  # noqa: SLF001


def _request_shape(request: Mapping[str, Any]) -> dict[str, int]:
    return {
        "prompt_bytes": len(request["prompt"].encode("utf-8")),
        "base_bytes": len(request["base_instructions"].encode("utf-8")),
        "schema_bytes": len(
            one_turn._canonical_json(request["output_schema"]).encode("utf-8")  # noqa: SLF001
        ),
        "request_bytes": len(
            one_turn._canonical_json(request).encode("utf-8")  # noqa: SLF001
        ),
        "source_unit_count": sum(
            len(segment["units"])
            for segment in request["private_input"]["segments"]
        ),
        "evidence_span_count": sum(
            len(segment["evidence_spans"])
            for segment in request["private_input"]["segments"]
        ),
        "literal_token_count": sum(
            len(unit["literal_tokens"])
            for segment in request["private_input"]["segments"]
            for unit in segment["units"]
        ),
    }


def _request_sha256(request: Mapping[str, Any]) -> str:
    return one_turn._sha256_bytes(  # noqa: SLF001
        one_turn._canonical_json(request).encode("utf-8")  # noqa: SLF001
    )


def _source_request() -> dict[str, Any]:
    path = EPOCH27_ROOT / "prepared-turn/request.private.json"
    if _record(path)["sha256"] != EPOCH27_REQUEST_SHA256:
        raise LowEffortRecoveryError("epoch-27 frozen source request drifted")
    return _load(path, "epoch-27 frozen source request")


def _build_request_uncached() -> dict[str, Any]:
    values = adapter.prepare_episode_batches(
        adapter._episode_from_request(_source_request()),  # noqa: SLF001
        batch_size=3,
        thread_mode="new_thread",
    )
    if len(values) != 1:
        raise LowEffortRecoveryError("epoch-44 request count drifted")
    request = values[0]
    integrity = request.get("semantic_integrity", {})
    required_true = (
        "model_authors_ordered_source_unit_proposition_inventory",
        "one_inventory_item_per_canonical_event",
        "inventory_and_event_evidence_pointer_identity_required",
        "model_authors_evidence_relative_metric_token_ordinals",
        "model_authors_every_normalized_table_value",
        "model_authors_every_full_canonical_event_value",
        "model_authors_closed_keyed_canonical_enum_object",
        "all_canonical_enum_fields_are_position_specific",
        "deterministic_inventory_removal_only",
        "deterministic_evidence_relative_metric_ordinal_projection_only",
        "deterministic_keyed_enum_order_projection_only",
        "low_effort_timeout_recovery_only",
    )
    if (
        tuple(request.get("segment_ids", ())) != SELECTED_SEGMENT_IDS
        or request.get("effective_batch_size") != 2
        or request.get("batch_size_ceiling") != 3
        or request.get("candidate_system_id") != adapter.CANDIDATE_SYSTEM_ID
        or request.get("model") != "gpt-5.6-sol"
        or request.get("effort") != "low"
        or request.get("retry_count") != 0
        or integrity.get("inventory_protocol_version")
        != adapter.INVENTORY_PROTOCOL_VERSION
        or integrity.get("keyed_enum_protocol_version")
        != adapter.KEYED_ENUM_PROTOCOL_VERSION
        or any(integrity.get(name) is not True for name in required_true)
        or request["prompt_sha256"] != EXPECTED_PROMPT_SHA256
        or request["base_instructions_sha256"] != EXPECTED_BASE_SHA256
        or request["output_schema_sha256"] != EXPECTED_SCHEMA_SHA256
        or _request_shape(request) != EXPECTED_SHAPE
        or _request_sha256(request) != EXPECTED_REQUEST_SHA256
    ):
        raise LowEffortRecoveryError("epoch-44 frozen request contract drifted")
    adapter.validate_prepared_request(request)
    return request


@lru_cache(maxsize=1)
def _frozen_request_json() -> str:
    return one_turn._canonical_json(_build_request_uncached())  # noqa: SLF001


def _build_request() -> dict[str, Any]:
    return json.loads(_frozen_request_json())


_PREDECESSOR_FILES: tuple[tuple[str, Path], ...] = (
    ("authorization", EPOCH43_ROOT / "operator-authorization.json"),
    ("receipt", EPOCH43_ROOT / "plan-step-receipt.json"),
    ("terminal", EPOCH43_ROOT / "terminal.json"),
    ("runtime_lock", EPOCH43_ROOT / "runtime-lock.json"),
    ("runtime_contract", EPOCH43_ROOT / "runtime-contract.json"),
    ("request", EPOCH43_ROOT / "prepared-turn/request.private.json"),
    ("attempt", EPOCH43_ROOT / "turn/semantic-attempt.json"),
    ("dispatch", EPOCH43_ROOT / "turn/semantic-dispatch.json"),
    ("thread_start", EPOCH43_ROOT / "turn/thread-start.json"),
    ("sidecar", EPOCH43_ROOT / "turn/sidecar.json"),
    ("initial_capacity_request", EPOCH43_ROOT / "turn/capacity/initial/request.json"),
    (
        "initial_capacity_measurement",
        EPOCH43_ROOT / "turn/capacity/initial/measurement.json",
    ),
    (
        "initial_capacity_provider_response",
        EPOCH43_ROOT / "turn/capacity/initial/provider-response.private.json",
    ),
    ("preturn_capacity_request", EPOCH43_ROOT / "turn/capacity/preturn/request.json"),
    (
        "preturn_capacity_measurement",
        EPOCH43_ROOT / "turn/capacity/preturn/measurement.json",
    ),
    (
        "preturn_capacity_provider_response",
        EPOCH43_ROOT / "turn/capacity/preturn/provider-response.private.json",
    ),
    ("directive", EPOCH43_DIRECTIVE_PATH),
    ("plan", EPOCH43_PLAN_PATH),
    ("epoch27_request", EPOCH27_ROOT / "prepared-turn/request.private.json"),
)


def _predecessor_records() -> Mapping[str, Any]:
    return {
        "epoch43_interrupted_keyed_enum_attempt": {
            role: _record(path)
            for role, path in _PREDECESSOR_FILES
            if role != "epoch27_request"
        },
        "epoch27_frozen_source_request": _record(
            EPOCH27_ROOT / "prepared-turn/request.private.json"
        ),
    }


def _validate_predecessor(full_verify: bool) -> None:
    del full_verify
    expected = {
        path: EPOCH43_HASHES[role]
        for role, path in _PREDECESSOR_FILES
        if role != "epoch27_request"
    }
    expected[EPOCH27_ROOT / "prepared-turn/request.private.json"] = EPOCH27_REQUEST_SHA256
    if any(_record(path)["sha256"] != digest for path, digest in expected.items()):
        raise LowEffortRecoveryError("epoch-44 predecessor record drifted")
    receipt = _load(EPOCH43_ROOT / "plan-step-receipt.json", "epoch-43 receipt")
    terminal = _load(EPOCH43_ROOT / "terminal.json", "epoch-43 terminal")
    sidecar = _load(EPOCH43_ROOT / "turn/sidecar.json", "epoch-43 sidecar")
    initial_capacity = _load(
        EPOCH43_ROOT / "turn/capacity/initial/measurement.json",
        "epoch-43 initial capacity",
    )
    preturn_capacity = _load(
        EPOCH43_ROOT / "turn/capacity/preturn/measurement.json",
        "epoch-43 preturn capacity",
    )
    if (
        receipt != terminal
        or receipt.get("state") != "waiting"
        or receipt.get("terminal_reason")
        != "epoch43_interrupted_semantic_attempt_preserved_no_replay"
        or receipt.get("new_semantic_model_call_count") != 1
        or receipt.get("new_unknown_usage_turn_count") != 1
        or receipt.get("semantic_retry_count") != 0
        or receipt.get("new_measured_usage", {}).get("total_tokens") != 0
        or receipt.get("new_wall_elapsed_seconds") != 1200.011
        or receipt.get("aggregate_architecture_semantic_model_call_count") != 22
        or receipt.get("aggregate_architecture_unknown_usage_turn_count") != 4
        or receipt.get("aggregate_architecture_measured_total_tokens") != 896_728
        or receipt.get("diagnostic", {}).get("diagnostic_path")
        != "semantic_output_absent_or_accounting_incomplete"
        or receipt.get("diagnostic", {}).get("error_class") != "turn_timeout"
        or receipt.get("structural_and_cost_pass") is not False
        or receipt.get("quality_measured_by_this_step") is not False
        or receipt.get("winner_frozen") is not False
        or receipt.get("holdout_authorized") is not False
        or receipt.get("production_mutated") is not False
        or (EPOCH43_ROOT / "turn/output.private.json").exists()
        or sidecar.get("state") != "interrupted"
        or sidecar.get("status") != "timeout"
        or sidecar.get("error_class") != "turn_timeout"
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("plan_type") != "pro"
        or sidecar.get("model") != "gpt-5.6-sol"
        or sidecar.get("effort") != "medium"
        or sidecar.get("usage_status") != "unknown"
        or sidecar.get("usage_complete") is not False
        or sidecar.get("usage") is not None
        or sidecar.get("thread_total_usage") is not None
        or sidecar.get("output_sha256") is not None
        or sidecar.get("wall_elapsed_seconds") != 1200.011
        or sidecar.get("recovery_reran_model") is not False
        or any(
            capacity.get("state") != "cleared_before_semantic_boundary"
            or capacity.get("capacity_available") is not True
            or capacity.get("managed_chatgpt_auth_only") is not True
            or capacity.get("rate_limit_reached_type") is not None
            for capacity in (initial_capacity, preturn_capacity)
        )
    ):
        raise LowEffortRecoveryError("epoch-43 interrupted predecessor contract drifted")


def _runtime_module_paths() -> Sequence[Path]:
    names = (
        "app_server_canonical_v31_low_effort_keyed_enum_compact_inventory_episode_batch.py",
        "app_server_canonical_v31_keyed_enum_compact_inventory_evidence_ordinal_episode_batch.py",
        "app_server_canonical_v31_compact_inventory_evidence_ordinal_episode_batch.py",
        "app_server_canonical_v31_compact_exhaustive_event_table_episode_batch.py",
        "app_server_canonical_v31_tagged_metric_token_id_episode_batch.py",
        "app_server_canonical_v31_unit_owned_token_id_episode_batch.py",
        "app_server_canonical_v31_unit_owned_ordinal_episode_batch.py",
        "app_server_canonical_v31_unit_owned_positional_episode_batch.py",
        "app_server_canonical_v31_single_message_compact_pointer_episode_batch.py",
        "app_server_canonical_v31_compact_unit_pointer_episode_batch.py",
        "app_server_canonical_v31_unit_local_pointer_episode_batch.py",
        "app_server_canonical_v31_bounded_span_episode_batch.py",
        "app_server_canonical_v31_literal_pointer_episode_batch.py",
        "app_server_canonical_v31_episode_batch.py",
    )
    return (Path(__file__), *(PROJECT_ROOT / "research_factory" / name for name in names))


def _validate_directive(value: Mapping[str, Any]) -> None:
    expected = json.loads(DIRECTIVE_PATH.read_text(encoding="utf-8"))
    predecessor = expected.get("predecessor_evidence", {})
    recovery = expected.get("recovery_contract", {})
    required_true = (
        "all_canonical_enum_fields_are_position_specific",
        "deterministic_evidence_relative_metric_ordinal_projection_only",
        "deterministic_inventory_removal_only",
        "deterministic_keyed_enum_order_projection_only",
        "event_count_is_diagnostic_only",
        "fresh_request_identity_required",
        "inventory_and_event_evidence_pointer_identity_required",
        "low_effort_timeout_recovery_only",
        "model_authors_closed_keyed_canonical_enum_object",
        "model_authors_evidence_relative_metric_token_ordinals",
        "model_authors_every_full_canonical_event_value",
        "model_authors_every_normalized_table_value",
        "model_authors_ordered_source_unit_proposition_inventory",
        "one_inventory_item_per_canonical_event",
        "prompt_base_schema_and_source_semantics_unchanged",
    )
    if dict(value) != expected:
        raise LowEffortRecoveryError("epoch-44 directive contract drifted")
    if (
        expected.get("schema_version")
        != "pif_evaluation_epoch44_low_effort_keyed_enum_recovery_directive_v1"
        or expected.get("thread_id") != THREAD_ID
        or expected.get("plan_epoch") != PLAN_EPOCH
        or expected.get("step_id") != STEP_ID
        or expected.get("authority") != "direct_user_instruction"
        or expected.get("authorized_by") != "kolby"
        or expected.get("authorization_statement") != AUTHORIZATION_STATEMENT
        or expected.get("state")
        != "authorized_for_exactly_one_low_effort_keyed_enum_timeout_recovery"
        or expected.get("expected_receipt_path")
        != str(DEFAULT_ROOT / "plan-step-receipt.json")
        or predecessor.get("epoch43_state") != "waiting"
        or predecessor.get("epoch43_terminal_reason")
        != "epoch43_interrupted_semantic_attempt_preserved_no_replay"
        or predecessor.get("epoch43_new_semantic_model_call_count") != 1
        or predecessor.get("epoch43_new_unknown_usage_turn_count") != 1
        or predecessor.get("epoch43_measured_total_tokens") != 0
        or predecessor.get("epoch43_wall_elapsed_seconds") != 1200.011
        or predecessor.get("epoch43_output_absent") is not True
        or predecessor.get("semantic_replay_authorized") is not False
        or recovery.get("selected_segment_ids") != list(SELECTED_SEGMENT_IDS)
        or recovery.get("candidate_model") != adapter.MODEL
        or recovery.get("candidate_reasoning_effort") != adapter.EFFORT
        or recovery.get("semantic_model_call_cap") != 1
        or recovery.get("semantic_retry_cap") != 0
        or recovery.get("measured_total_token_ceiling") != MAXIMUM_TOTAL_TOKENS
        or recovery.get("full_six_case_total_token_ceiling")
        != FULL_PRODUCTION_TOKEN_CEILING
        or recovery.get("expected_request_shape") != EXPECTED_SHAPE
        or recovery.get("expected_request_sha256") != EXPECTED_REQUEST_SHA256
        or recovery.get("inventory_protocol_version")
        != adapter.INVENTORY_PROTOCOL_VERSION
        or recovery.get("keyed_enum_protocol_version")
        != adapter.KEYED_ENUM_PROTOCOL_VERSION
        or any(recovery.get(name) is not True for name in required_true)
        or recovery.get("table_duplicate_rejection_allowed") is not False
        or recovery.get("predecessor_semantic_replay_allowed") is not False
        or any(
            recovery.get(name) is not False
            for name in (
                "deterministic_semantic_deduplication_allowed",
                "deterministic_semantic_pruning_allowed",
                "deterministic_semantic_relabeling_allowed",
                "deterministic_support_filtering_allowed",
            )
        )
        or expected.get("intention_to_treat_accounting")
        != {
            "maximum_semantic_model_call_count_after_epoch44_dispatch": 23,
            "predecessor_measured_total_tokens": PREDECESSOR_MEASURED_TOKENS,
            "predecessor_replay_allowed": False,
            "predecessor_semantic_model_call_count": PREDECESSOR_MODEL_CALLS,
            "predecessor_unknown_usage_turn_count": PREDECESSOR_UNKNOWN_USAGE,
        }
        or expected.get("promotion_contract", {}).get("quality_threshold") != 0.97
        or expected.get("promotion_contract", {}).get("winner_frozen") is not False
        or expected.get("promotion_contract", {}).get("holdout_authorized") is not False
        or expected.get("promotion_contract", {}).get("production_mutation_allowed")
        is not False
        or expected.get("transport_contract", {}).get("transport")
        != "official_codex_app_server_stdio_managed_chatgpt_auth"
        or expected.get("transport_contract", {}).get("managed_chatgpt_pro_auth_required")
        is not True
    ):
        raise LowEffortRecoveryError("epoch-44 directive invariants drifted")


def _receipt_metadata(outcome: Mapping[str, Any]) -> Mapping[str, Any]:
    usage = outcome["usage"]
    total = int(usage["total_tokens"])
    fidelity_path = DEFAULT_ROOT / "turn/semantic-fidelity.json"
    fidelity: Mapping[str, Any] = {}
    if fidelity_path.is_file():
        fidelity = _load(fidelity_path, "epoch-44 semantic fidelity")
    measurable = outcome["state"] != "waiting"
    input_projection = (
        math.ceil(
            int(usage["input_tokens"])
            * FULL_VISIBLE_REQUEST_BYTES
            / CANARY_VISIBLE_REQUEST_BYTES
        )
        if measurable
        else None
    )
    output_projection = int(usage["output_tokens"]) * FULL_CASE_SCALE if measurable else None
    full_projection = (
        input_projection + output_projection
        if input_projection is not None and output_projection is not None
        else None
    )
    cost_pass = (
        full_projection <= FULL_PRODUCTION_TOKEN_CEILING
        if full_projection is not None
        else False
    )
    structural_and_cost_pass = outcome["state"] == "passed" and cost_pass
    current_calls = int(outcome["semantic_model_call_count"])
    current_unknown = int(outcome["unknown_usage_turn_count"])
    metadata: dict[str, Any] = {
        "diagnostic_segment_ids": list(SELECTED_SEGMENT_IDS),
        "diagnostic_source_unit_count": EXPECTED_SHAPE["source_unit_count"],
        "inventory_item_count": fidelity.get("inventory_item_count"),
        "inventory_event_count": fidelity.get("inventory_event_count"),
        "inventory_evidence_identity_count": fidelity.get(
            "inventory_evidence_identity_count"
        ),
        "inventory_metric_ordinal_pair_count": fidelity.get(
            "inventory_metric_ordinal_pair_count"
        ),
        "inventory_manifest_sha256": fidelity.get("inventory_manifest_sha256"),
        "compact_event_count": fidelity.get("compact_event_count"),
        "compact_concept_count": fidelity.get("compact_concept_count"),
        "compact_table_duplicate_count": fidelity.get("compact_table_duplicate_count"),
        "model_authors_closed_keyed_canonical_enum_object": fidelity.get(
            "model_authors_closed_keyed_canonical_enum_object"
        ),
        "all_canonical_enum_fields_are_position_specific": fidelity.get(
            "all_canonical_enum_fields_are_position_specific"
        ),
        "deterministic_keyed_enum_order_projection_only": True,
        "low_effort_timeout_recovery_only": True,
        "diagnostic_event_count": fidelity.get("emitted_event_count"),
        "diagnostic_concept_count": fidelity.get("emitted_concept_count"),
        "event_count_is_diagnostic_only": True,
        "table_duplicate_count_is_diagnostic_only": True,
        "measured_canary_total_tokens": total,
        "full_six_case_input_token_projection": input_projection,
        "full_six_case_output_token_projection_by_case_count": output_projection,
        "full_six_case_total_token_projection_by_case_count": full_projection,
        "full_six_case_cost_projection_pass": cost_pass,
        "structural_and_cost_pass": structural_and_cost_pass,
        "production_token_ceiling_for_future_full_run": FULL_PRODUCTION_TOKEN_CEILING,
        "quality_measured_by_this_step": False,
        "fresh_full_event_ab_ba_quality_required": structural_and_cost_pass,
        "aggregate_architecture_semantic_model_call_count": (
            PREDECESSOR_MODEL_CALLS + current_calls
        ),
        "aggregate_architecture_unknown_usage_turn_count": (
            PREDECESSOR_UNKNOWN_USAGE + current_unknown
        ),
        "aggregate_architecture_measured_total_tokens": (
            PREDECESSOR_MEASURED_TOKENS + total
        ),
        "predecessor_accounting_reconciled_additively": True,
        "epoch43_replayed": False,
        "prior_semantic_output_reuse_count": 0,
        "deterministic_semantic_pruning": False,
        "deterministic_support_filtering": False,
        "deterministic_deduplication": False,
        "deterministic_relabeling": False,
    }
    if outcome["state"] == "passed" and not cost_pass:
        metadata.update(
            {
                "state": "rejected",
                "terminal_reason": "epoch44_full_six_case_cost_projection_rejected",
                "next_authorized_action": (
                    "reject_low_effort_keyed_enum_architecture_without_field_patch"
                ),
            }
        )
    return metadata


SPEC = one_turn.OneTurnCanarySpec(
    project_root=PROJECT_ROOT,
    default_root=DEFAULT_ROOT,
    directive_path=DIRECTIVE_PATH,
    plan_path=PLAN_PATH,
    directive_sha256=DIRECTIVE_SHA256,
    thread_id=THREAD_ID,
    plan_epoch=PLAN_EPOCH,
    step_id=STEP_ID,
    turn_name=TURN_NAME,
    stage="low_effort_keyed_enum_timeout_recovery_full_canonical_two_segment_canary",
    architecture_class="one_turn_low_effort_compact_inventory_closed_keyed_enum_transport_v1",
    authorization_statement=AUTHORIZATION_STATEMENT,
    authorization_state="authorized_for_exactly_one_low_effort_keyed_enum_timeout_recovery",
    model=adapter.MODEL,
    effort=adapter.EFFORT,
    maximum_total_tokens=MAXIMUM_TOTAL_TOKENS,
    maximum_wall_seconds=MAXIMUM_WALL_SECONDS,
    minimum_remaining_reserve_percent=MINIMUM_REMAINING_RESERVE_PERCENT,
    capacity_safety_margin_percent=CAPACITY_SAFETY_MARGIN_PERCENT,
    quota_points_per_million_tokens=QUOTA_POINTS_PER_MILLION_TOKENS,
    authorization_window_seconds=AUTHORIZATION_WINDOW_SECONDS,
    adapter=adapter,
    validate_directive=_validate_directive,
    validate_predecessor=_validate_predecessor,
    build_request=_build_request,
    predecessor_records=_predecessor_records,
    runtime_module_paths=_runtime_module_paths,
    receipt_metadata=_receipt_metadata,
    pass_next_action="run_fresh_full_event_ab_ba_quality_only_if_structural_and_cost_pass_true",
    reject_next_action="reject_low_effort_keyed_enum_architecture_without_field_patch",
    waiting_next_action="no_replay_distinct_smaller_recovery_design_only",
)


def _runtime_status(status: Mapping[str, Any], root: Path) -> dict[str, Any]:
    request = _load(root.resolve() / "prepared-turn/request.private.json", "epoch-44 request")
    shape = _request_shape(request)
    if shape != EXPECTED_SHAPE or _request_sha256(request) != EXPECTED_REQUEST_SHA256:
        raise LowEffortRecoveryError("epoch-44 runtime request drifted")
    return {**copy.deepcopy(dict(status)), **shape}


def prepare_recovery(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    return _runtime_status(one_turn.prepare(SPEC, root), root)


def verify_runtime(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    return _runtime_status(one_turn.verify_runtime(SPEC, root), root)


def authorize_recovery(
    *, root: Path = DEFAULT_ROOT, operator_authorization_id: str, now: Any = None
) -> dict[str, Any]:
    return one_turn.authorize(
        SPEC, root=root, operator_authorization_id=operator_authorization_id, now=now
    )


async def execute_recovery(
    *, root: Path = DEFAULT_ROOT, operator_authorization_id: str
) -> dict[str, Any]:
    return await one_turn.execute(
        SPEC, root=root, operator_authorization_id=operator_authorization_id
    )


def verify_receipt(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    return one_turn.verify_receipt(SPEC, root)


def status_recovery(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    return one_turn.status(SPEC, root)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("prepare")
    authorize_parser = sub.add_parser("authorize")
    authorize_parser.add_argument("--operator-authorization-id", required=True)
    execute_parser = sub.add_parser("execute")
    execute_parser.add_argument("--operator-authorization-id", required=True)
    sub.add_parser("verify-runtime")
    sub.add_parser("verify-receipt")
    sub.add_parser("status")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "prepare":
        value = prepare_recovery(args.root)
    elif args.command == "authorize":
        value = authorize_recovery(
            root=args.root, operator_authorization_id=args.operator_authorization_id
        )
    elif args.command == "execute":
        value = asyncio.run(
            execute_recovery(
                root=args.root,
                operator_authorization_id=args.operator_authorization_id,
            )
        )
    elif args.command == "verify-runtime":
        value = verify_runtime(args.root)
    elif args.command == "verify-receipt":
        value = verify_receipt(args.root)
    else:
        value = status_recovery(args.root)
    print(json.dumps(value, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
