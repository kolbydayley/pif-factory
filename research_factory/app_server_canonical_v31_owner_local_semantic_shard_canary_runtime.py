from __future__ import annotations

"""Epoch-41 owner-local disjoint semantic-shard extraction canary."""

import argparse
import asyncio
import copy
import json
import math
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_canonical_v31_owner_local_semantic_shard_episode_batch as adapter
from . import app_server_one_turn_canary_runtime as one_turn


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_ROOT = (
    PROJECT_ROOT / "work/app-server-development-v2/unattended-pipeline-v5"
).resolve()
DEFAULT_ROOT = (
    PIPELINE_ROOT / "canonical-v31-epoch41-owner-local-semantic-shard-canary-v1"
).resolve()
EPOCH27_ROOT = (
    PIPELINE_ROOT / "canonical-v31-epoch27-tagged-metric-token-id-canary-v1"
).resolve()
EPOCH40_ROOT = (
    PIPELINE_ROOT / "canonical-v31-epoch40-compact-exhaustive-event-table-canary-v1"
).resolve()
DIRECTIVE_PATH = (
    PROJECT_ROOT
    / "automation/pif-evaluation-epoch41-owner-local-semantic-shard-canary-v41.json"
).resolve()
PLAN_PATH = (
    PROJECT_ROOT / "automation/pif-evaluation-semantic-plan-v41.json"
).resolve()
DIRECTIVE_SHA256 = "cb340fa93b29ad18c24a5fc9badb6250cf27ba8636c7cc38503d03b4b82a3068"
THREAD_ID = "019f4cf1-c46e-7db3-acd2-bf03c4459a10"
PLAN_EPOCH = 41
STEP_ID = "canonical_v31_epoch41_owner_local_semantic_shard_canary_v41"
TURN_NAME = "epoch41_owner_local_semantic_shard_two_segment_canary"
AUTHORIZATION_STATEMENT = (
    "You have my full permission to continue. No need to seek out my approval anymore."
)
AUTHORIZATION_ID_DEFAULT = "kolby-epoch41-owner-local-semantic-shard-20260720"
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
    "base_bytes": 25_908,
    "schema_bytes": 13_731,
    "request_bytes": 323_925,
    "source_unit_count": 20,
    "evidence_span_count": 38,
    "literal_token_count": 2_020,
}
EXPECTED_REQUEST_SHA256 = "8f515d4e1cd4730485681d84d8cd42daafe1b8b7d32d3fd76628a575b440e760"
EXPECTED_PROMPT_SHA256 = "36af4c581e70487d72887f225ed18430d4b5c35b02370fed0a0257f34bc3a1c1"
EXPECTED_BASE_SHA256 = "5eda87f3625db27341634a40a2cc0fc943a208b424827a4f65ff042dc0cdb6e5"
EXPECTED_SCHEMA_SHA256 = "6bc99bc9e1e53e73a5dc03e0fb25c7f1f2316ae6c1c58fb1dd87f756bc6dd74b"
CANARY_VISIBLE_REQUEST_BYTES = 53_916
FULL_VISIBLE_REQUEST_BYTES = 81_853
FULL_CASE_SCALE = 3
FULL_PRODUCTION_TOKEN_CEILING = 73_926
EPOCH27_REQUEST_SHA256 = "f94df0a9f66f3de82a67f6442e37e5f6f0e563794e6722e5a6e56cc1e02513b6"
EPOCH40_HASHES = {
    "authorization": "86019f3fa50c1b36b2ee027ea6cd995c3fbe8d1322c953414de55d971d44ee05",
    "receipt": "d09fffcd7b14f52ade4beed223caa99621ce429b15c9e3cb291d09df002d2562",
    "terminal": "d09fffcd7b14f52ade4beed223caa99621ce429b15c9e3cb291d09df002d2562",
    "runtime_lock": "c92f0720b97a6a19200b99cc5f6351ec7658f9e31c2d0012ef3f05454c03c349",
    "runtime_contract": "95b368e404aed032589820589b5e350fe6cc5e57e43619075adb2bd35177e570",
    "request": "dbf44ff2b1f9ddb4b71c797028122bdab24097fd9e422f49a74c05ce8d1bfde4",
    "attempt": "58065bbbb28f435dccad29f3dbbd524da664adce8036eb6d30515195c68d8978",
    "dispatch": "0285756dc56d9a882dd9c7076569e5325e7657c695c90116a405645157826bec",
    "thread_start": "3987308ccffc3e48f68fef46527282a4a560b5df248498ce0e62e1f8a78435ab",
    "sidecar": "6c131cb6d963a499a8ff822fc7a43a03abaadd0bdb58add4a6811b2040fa1759",
    "initial_capacity_request": "487d878fe418d2355724a55567ee730f612e72bb1b82f0a91c9967ba9c318c86",
    "initial_capacity_measurement": "b99fda9a90ef2e8b1e7eb2f1dd35baaded46de1fcb9032846f90aab17ef65a05",
    "initial_capacity_provider_response": "e42b4d8f6c6fd826e15ae2cb54c0cb36c269403aa6f19ae9d04e5abf9d3d0cf7",
    "preturn_capacity_request": "441d265e6e1bc300c2440a5631b078bae13266fa3ed6761ee32bddb2bbd8e71a",
    "preturn_capacity_measurement": "70dc9abb878c295d6a757c363b81f696905b42cde6d54171996c1a97a0e4f5a2",
    "preturn_capacity_provider_response": "e42b4d8f6c6fd826e15ae2cb54c0cb36c269403aa6f19ae9d04e5abf9d3d0cf7",
}
PREDECESSOR_MODEL_CALLS = 19
PREDECESSOR_UNKNOWN_USAGE = 3
PREDECESSOR_MEASURED_TOKENS = 846_632

SemanticShardCanaryError = one_turn.OneTurnCanaryError
SemanticShardCanaryWaiting = one_turn.OneTurnCanaryWaiting


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
        raise SemanticShardCanaryError("epoch-27 frozen source request drifted")
    return _load(path, "epoch-27 frozen source request")


def _build_request_uncached() -> dict[str, Any]:
    values = adapter.prepare_episode_batches(
        adapter._episode_from_request(_source_request()),  # noqa: SLF001
        batch_size=3,
        thread_mode="new_thread",
    )
    if len(values) != 1:
        raise SemanticShardCanaryError("epoch-41 request count drifted")
    request = values[0]
    integrity = request.get("semantic_integrity", {})
    if (
        tuple(request.get("segment_ids", ())) != SELECTED_SEGMENT_IDS
        or request.get("effective_batch_size") != 2
        or request.get("batch_size_ceiling") != 3
        or request.get("candidate_system_id") != adapter.CANDIDATE_SYSTEM_ID
        or request.get("model") != "gpt-5.6-sol"
        or request.get("effort") != "medium"
        or request.get("retry_count") != 0
        or integrity.get("shard_protocol_version")
        != adapter.SHARD_PROTOCOL_VERSION
        or integrity.get("model_assigns_each_event_to_exactly_one_semantic_shard")
        is not True
        or integrity.get("semantic_shard_names") != list(adapter.SHARD_NAMES)
        or integrity.get("model_authors_every_normalized_table_value") is not True
        or integrity.get("model_authors_every_full_canonical_event_value") is not True
        or integrity.get("deterministic_shard_concatenation_only") is not True
        or request["prompt_sha256"] != EXPECTED_PROMPT_SHA256
        or request["base_instructions_sha256"] != EXPECTED_BASE_SHA256
        or request["output_schema_sha256"] != EXPECTED_SCHEMA_SHA256
        or _request_shape(request) != EXPECTED_SHAPE
        or _request_sha256(request) != EXPECTED_REQUEST_SHA256
    ):
        raise SemanticShardCanaryError("epoch-41 frozen request contract drifted")
    adapter.validate_prepared_request(request)
    return request


@lru_cache(maxsize=1)
def _frozen_request_json() -> str:
    return one_turn._canonical_json(_build_request_uncached())  # noqa: SLF001


def _build_request() -> dict[str, Any]:
    return json.loads(_frozen_request_json())


_PREDECESSOR_FILES: tuple[tuple[str, Path], ...] = (
    ("authorization", EPOCH40_ROOT / "operator-authorization.json"),
    ("receipt", EPOCH40_ROOT / "plan-step-receipt.json"),
    ("terminal", EPOCH40_ROOT / "terminal.json"),
    ("runtime_lock", EPOCH40_ROOT / "runtime-lock.json"),
    ("runtime_contract", EPOCH40_ROOT / "runtime-contract.json"),
    ("request", EPOCH40_ROOT / "prepared-turn/request.private.json"),
    ("attempt", EPOCH40_ROOT / "turn/semantic-attempt.json"),
    ("dispatch", EPOCH40_ROOT / "turn/semantic-dispatch.json"),
    ("thread_start", EPOCH40_ROOT / "turn/thread-start.json"),
    ("sidecar", EPOCH40_ROOT / "turn/sidecar.json"),
    (
        "initial_capacity_request",
        EPOCH40_ROOT / "turn/capacity/initial/request.json",
    ),
    (
        "initial_capacity_measurement",
        EPOCH40_ROOT / "turn/capacity/initial/measurement.json",
    ),
    (
        "initial_capacity_provider_response",
        EPOCH40_ROOT / "turn/capacity/initial/provider-response.private.json",
    ),
    (
        "preturn_capacity_request",
        EPOCH40_ROOT / "turn/capacity/preturn/request.json",
    ),
    (
        "preturn_capacity_measurement",
        EPOCH40_ROOT / "turn/capacity/preturn/measurement.json",
    ),
    (
        "preturn_capacity_provider_response",
        EPOCH40_ROOT / "turn/capacity/preturn/provider-response.private.json",
    ),
    ("epoch27_request", EPOCH27_ROOT / "prepared-turn/request.private.json"),
)


def _predecessor_records() -> Mapping[str, Any]:
    return {
        "epoch40_interrupted_unknown_usage_attempt": {
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
        path: EPOCH40_HASHES[role]
        for role, path in _PREDECESSOR_FILES
        if role != "epoch27_request"
    }
    expected[EPOCH27_ROOT / "prepared-turn/request.private.json"] = EPOCH27_REQUEST_SHA256
    if any(_record(path)["sha256"] != digest for path, digest in expected.items()):
        raise SemanticShardCanaryError("epoch-41 predecessor record drifted")
    receipt = _load(EPOCH40_ROOT / "plan-step-receipt.json", "epoch-40 receipt")
    terminal = _load(EPOCH40_ROOT / "terminal.json", "epoch-40 terminal")
    sidecar = _load(EPOCH40_ROOT / "turn/sidecar.json", "epoch-40 sidecar")
    attempt = _load(EPOCH40_ROOT / "turn/semantic-attempt.json", "epoch-40 attempt")
    dispatch = _load(EPOCH40_ROOT / "turn/semantic-dispatch.json", "epoch-40 dispatch")
    thread = _load(EPOCH40_ROOT / "turn/thread-start.json", "epoch-40 thread")
    initial_capacity = _load(
        EPOCH40_ROOT / "turn/capacity/initial/measurement.json",
        "epoch-40 initial capacity",
    )
    preturn_capacity = _load(
        EPOCH40_ROOT / "turn/capacity/preturn/measurement.json",
        "epoch-40 preturn capacity",
    )
    if (
        receipt != terminal
        or receipt.get("state") != "waiting"
        or receipt.get("terminal_reason")
        != "epoch40_interrupted_semantic_attempt_preserved_no_replay"
        or receipt.get("new_semantic_model_call_count") != 1
        or receipt.get("new_unknown_usage_turn_count") != 1
        or receipt.get("semantic_retry_count") != 0
        or receipt.get("new_measured_usage", {}).get("total_tokens") != 0
        or receipt.get("new_wall_elapsed_seconds") != 1200.007
        or receipt.get("aggregate_architecture_semantic_model_call_count")
        != PREDECESSOR_MODEL_CALLS
        or receipt.get("aggregate_architecture_unknown_usage_turn_count")
        != PREDECESSOR_UNKNOWN_USAGE
        or receipt.get("aggregate_architecture_measured_total_tokens")
        != PREDECESSOR_MEASURED_TOKENS
        or receipt.get("winner_frozen") is not False
        or receipt.get("holdout_authorized") is not False
        or receipt.get("production_mutated") is not False
        or attempt.get("state") != "declared_before_capacity_thread_or_turn"
        or attempt.get("semantic_model_call_cap") != 1
        or attempt.get("semantic_retry_count") != 0
        or dispatch.get("state") != "semantic_turn_dispatch_committed"
        or dispatch.get("semantic_retry_count") != 0
        or thread.get("state") != "ephemeral_thread_started_before_semantic_turn"
        or thread.get("thread_id") != sidecar.get("thread_id")
        or sidecar.get("schema_version") != "pif_codex_app_server_turn_v2"
        or sidecar.get("state") != "interrupted"
        or sidecar.get("status") != "timeout"
        or sidecar.get("error_class") != "turn_timeout"
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("plan_type") != "pro"
        or sidecar.get("usage_status") != "unknown"
        or sidecar.get("usage_complete") is not False
        or sidecar.get("usage") is not None
        or sidecar.get("thread_total_usage") is not None
        or sidecar.get("output_sha256") is not None
        or sidecar.get("recovery_reran_model") is not False
        or (EPOCH40_ROOT / "turn/output.private.json").exists()
        or any(
            capacity.get("state") != "cleared_before_semantic_boundary"
            or capacity.get("capacity_available") is not True
            or capacity.get("managed_chatgpt_auth_only") is not True
            or capacity.get("rate_limit_reached_type") is not None
            for capacity in (initial_capacity, preturn_capacity)
        )
    ):
        raise SemanticShardCanaryError("epoch-40 interrupted predecessor contract drifted")


def _runtime_module_paths() -> Sequence[Path]:
    names = (
        "app_server_canonical_v31_owner_local_semantic_shard_episode_batch.py",
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
    canary = expected.get("canary_contract", {})
    ranking = expected.get("architecture_ranking", [])
    evidence = expected.get("architecture_evidence", {})
    if dict(value) != expected:
        raise SemanticShardCanaryError("epoch-41 directive contract drifted")
    if (
        expected.get("schema_version")
        != "pif_evaluation_epoch41_owner_local_semantic_shard_canary_directive_v1"
        or expected.get("thread_id") != THREAD_ID
        or expected.get("plan_epoch") != PLAN_EPOCH
        or expected.get("step_id") != STEP_ID
        or expected.get("authority") != "direct_user_instruction"
        or expected.get("authorized_by") != "kolby"
        or expected.get("authorization_statement") != AUTHORIZATION_STATEMENT
        or expected.get("state")
        != "authorized_for_exactly_one_owner_local_semantic_shard_two_segment_canary"
        or expected.get("expected_receipt_path")
        != str(DEFAULT_ROOT / "plan-step-receipt.json")
        or len(ranking) != 3
        or [row.get("rank") for row in ranking] != [1, 2, 3]
        or sum(row.get("selected") is True for row in ranking) != 1
        or ranking[0].get("architecture")
        != "one_turn_disjoint_owner_local_full_canonical_semantic_shards"
        or ranking[0].get("selected") is not True
        or evidence.get("epoch40_state") != "waiting"
        or evidence.get("epoch40_terminal_reason")
        != "epoch40_interrupted_semantic_attempt_preserved_no_replay"
        or evidence.get("epoch40_reasoning_effort") != "high"
        or evidence.get("epoch40_wall_elapsed_seconds") != 1200.007
        or evidence.get("epoch40_unknown_usage_turn_count") != 1
        or evidence.get("epoch40_output_absent") is not True
        or evidence.get("semantic_field_repair_authorized") is not False
        or canary.get("selected_segment_ids") != list(SELECTED_SEGMENT_IDS)
        or canary.get("candidate_model") != adapter.MODEL
        or canary.get("candidate_reasoning_effort") != adapter.EFFORT
        or canary.get("semantic_model_call_cap") != 1
        or canary.get("semantic_retry_cap") != 0
        or canary.get("measured_total_token_ceiling") != MAXIMUM_TOTAL_TOKENS
        or canary.get("full_six_case_total_token_ceiling")
        != FULL_PRODUCTION_TOKEN_CEILING
        or canary.get("expected_request_shape") != EXPECTED_SHAPE
        or canary.get("expected_request_sha256") != EXPECTED_REQUEST_SHA256
        or canary.get("semantic_shard_names") != list(adapter.SHARD_NAMES)
        or canary.get("model_assigns_each_event_to_exactly_one_semantic_shard")
        is not True
        or canary.get("model_authors_every_normalized_table_value") is not True
        or canary.get("model_authors_every_full_canonical_event_value") is not True
        or canary.get("deterministic_shard_concatenation_only") is not True
        or canary.get("cross_shard_duplicate_rejection_allowed") is not False
        or canary.get("event_count_is_diagnostic_only") is not True
        or canary.get("predecessor_semantic_replay_allowed") is not False
        or any(
            canary.get(name) is not False
            for name in (
                "deterministic_semantic_deduplication_allowed",
                "deterministic_semantic_pruning_allowed",
                "deterministic_semantic_relabeling_allowed",
                "deterministic_support_filtering_allowed",
            )
        )
        or expected.get("intention_to_treat_accounting")
        != {
            "maximum_semantic_model_call_count_after_epoch41_dispatch": 20,
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
        raise SemanticShardCanaryError("epoch-41 directive invariants drifted")


def _receipt_metadata(outcome: Mapping[str, Any]) -> Mapping[str, Any]:
    usage = outcome["usage"]
    total = int(usage["total_tokens"])
    fidelity_path = DEFAULT_ROOT / "turn/semantic-fidelity.json"
    fidelity: Mapping[str, Any] = {}
    if fidelity_path.is_file():
        fidelity = _load(fidelity_path, "epoch-41 semantic fidelity")
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
        "semantic_shard_names": fidelity.get("semantic_shard_names"),
        "semantic_shard_event_counts": fidelity.get("semantic_shard_event_counts"),
        "semantic_shard_event_count": fidelity.get("semantic_shard_event_count"),
        "semantic_shard_manifest_sha256": fidelity.get("semantic_shard_manifest_sha256"),
        "cross_shard_duplicate_count": fidelity.get("cross_shard_duplicate_count"),
        "compact_event_count": fidelity.get("compact_event_count"),
        "compact_concept_count": fidelity.get("compact_concept_count"),
        "diagnostic_event_count": fidelity.get("emitted_event_count"),
        "diagnostic_concept_count": fidelity.get("emitted_concept_count"),
        "event_count_is_diagnostic_only": True,
        "cross_shard_duplicate_count_is_diagnostic_only": True,
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
        "epoch40_replayed": False,
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
                "terminal_reason": "epoch41_full_six_case_cost_projection_rejected",
                "next_authorized_action": (
                    "reject_owner_local_semantic_shard_architecture_without_field_patch"
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
    stage="owner_local_semantic_shard_full_canonical_two_segment_canary",
    architecture_class="one_turn_owner_local_disjoint_semantic_shards_v1",
    authorization_statement=AUTHORIZATION_STATEMENT,
    authorization_state="authorized_for_exactly_one_owner_local_semantic_shard_two_segment_canary",
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
    reject_next_action="reject_owner_local_semantic_shard_architecture_without_field_patch",
    waiting_next_action="no_replay_bounded_recovery_only",
)


def _runtime_status(status: Mapping[str, Any], root: Path) -> dict[str, Any]:
    request = _load(root.resolve() / "prepared-turn/request.private.json", "epoch-41 request")
    shape = _request_shape(request)
    if shape != EXPECTED_SHAPE or _request_sha256(request) != EXPECTED_REQUEST_SHA256:
        raise SemanticShardCanaryError("epoch-41 runtime request drifted")
    return {**copy.deepcopy(dict(status)), **shape}


def prepare_canary(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    return _runtime_status(one_turn.prepare(SPEC, root), root)


def verify_runtime(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    return _runtime_status(one_turn.verify_runtime(SPEC, root), root)


def authorize_canary(
    *, root: Path = DEFAULT_ROOT, operator_authorization_id: str, now: Any = None
) -> dict[str, Any]:
    return one_turn.authorize(
        SPEC, root=root, operator_authorization_id=operator_authorization_id, now=now
    )


async def execute_canary(
    *, root: Path = DEFAULT_ROOT, operator_authorization_id: str
) -> dict[str, Any]:
    return await one_turn.execute(
        SPEC, root=root, operator_authorization_id=operator_authorization_id
    )


def verify_receipt(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    return one_turn.verify_receipt(SPEC, root)


def status_canary(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
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
        value = prepare_canary(args.root)
    elif args.command == "authorize":
        value = authorize_canary(
            root=args.root, operator_authorization_id=args.operator_authorization_id
        )
    elif args.command == "execute":
        value = asyncio.run(
            execute_canary(
                root=args.root,
                operator_authorization_id=args.operator_authorization_id,
            )
        )
    elif args.command == "verify-runtime":
        value = verify_runtime(args.root)
    elif args.command == "verify-receipt":
        value = verify_receipt(args.root)
    else:
        value = status_canary(args.root)
    print(json.dumps(value, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
