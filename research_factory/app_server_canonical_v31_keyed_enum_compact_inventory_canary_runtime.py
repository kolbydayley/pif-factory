from __future__ import annotations

"""Epoch-43 closed-keyed-enum compact inventory two-segment canary."""

import argparse
import asyncio
import copy
import json
import math
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import (
    app_server_canonical_v31_keyed_enum_compact_inventory_evidence_ordinal_episode_batch as adapter,
)
from . import app_server_one_turn_canary_runtime as one_turn


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_ROOT = (
    PROJECT_ROOT / "work/app-server-development-v2/unattended-pipeline-v5"
).resolve()
DEFAULT_ROOT = (
    PIPELINE_ROOT / "canonical-v31-epoch43-keyed-enum-compact-inventory-canary-v1"
).resolve()
EPOCH27_ROOT = (
    PIPELINE_ROOT / "canonical-v31-epoch27-tagged-metric-token-id-canary-v1"
).resolve()
EPOCH42_ROOT = (
    PIPELINE_ROOT
    / "canonical-v31-epoch42-compact-inventory-evidence-ordinal-canary-v1"
).resolve()
DIRECTIVE_PATH = (
    PROJECT_ROOT
    / "automation/pif-evaluation-epoch43-keyed-enum-compact-inventory-canary-v43.json"
).resolve()
PLAN_PATH = (PROJECT_ROOT / "automation/pif-evaluation-semantic-plan-v43.json").resolve()
DIRECTIVE_SHA256 = "520be2e5e4b2c4402a7c7dc0de1f7d69352a8a77ef0196bfa72cabef85445808"
THREAD_ID = "019f4cf1-c46e-7db3-acd2-bf03c4459a10"
PLAN_EPOCH = 43
STEP_ID = "canonical_v31_epoch43_keyed_enum_compact_inventory_canary_v43"
TURN_NAME = "epoch43_keyed_enum_compact_inventory_two_segment_canary"
AUTHORIZATION_STATEMENT = (
    "You have my full permission to continue. No need to seek out my approval anymore."
)
AUTHORIZATION_ID_DEFAULT = "kolby-epoch43-keyed-enum-compact-inventory-20260720"
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
    "request_bytes": 316_134,
    "source_unit_count": 20,
    "evidence_span_count": 38,
    "literal_token_count": 2_020,
}
EXPECTED_REQUEST_SHA256 = "b567a3d0ef2801e367bbaf753f24f004a39cda805634b635d6b5ca89996a4ec1"
EXPECTED_PROMPT_SHA256 = "36af4c581e70487d72887f225ed18430d4b5c35b02370fed0a0257f34bc3a1c1"
EXPECTED_BASE_SHA256 = "a7e9aa5e2575cdb8196c31db4eb07a3de497d0dae5d4986731c58a180069f727"
EXPECTED_SCHEMA_SHA256 = "37ae90f1a9e585d1ed0cd2ea5e8dcece0e60c8e5575e5d1d3beb9976831b7e44"
CANARY_VISIBLE_REQUEST_BYTES = 46_054
FULL_VISIBLE_REQUEST_BYTES = 73_991
FULL_CASE_SCALE = 3
FULL_PRODUCTION_TOKEN_CEILING = 73_926
EPOCH27_REQUEST_SHA256 = "f94df0a9f66f3de82a67f6442e37e5f6f0e563794e6722e5a6e56cc1e02513b6"
EPOCH42_HASHES = {
    "authorization": "a977b00178e8f5b8b99cbd09804562f936cb6f50f07c0b9f43f5fe8b92a1d17d",
    "receipt": "092c3c12ac364c111293b58fd12e54edf26f7c36ffe10fe935eafc8a2c97e9d7",
    "terminal": "092c3c12ac364c111293b58fd12e54edf26f7c36ffe10fe935eafc8a2c97e9d7",
    "runtime_lock": "f96c3880dc577b53a05c46ab5b0bfe061141c4a23ef20dbe4aee676ce7363755",
    "runtime_contract": "9f017b686fd2480807f25296761ec42213cd032ab7be7d79a95a4e5c818c106c",
    "request": "f0f5f3639a8b405eeb5e7d51f1ea7cba393fc97f644694e8dc97035e9d6cfc10",
    "attempt": "0c79b40c85e802523172559e656485a73313c3f801a0cd532014bcd556195713",
    "dispatch": "b69b23fd139eb25beb51323f08657a845c931eb0797f43d6e4eff9ec07d01d7e",
    "thread_start": "a85884b91f2ce94a555c1ba042ac6ed60a38f258910ab4cec677a0ec1fd9f67e",
    "sidecar": "e5a9f5a7985e01d4b694afc801227a2ee8acdaecd79df5226d8be028632ccd98",
    "output": "4e8d38ac50bc86fba74821978cc16ad7f523d593adc66cb3a16461bb7df3dccc",
    "rejection": "18d097b8ffbc9a75e0cb97e05045c14c00e10e7ecf53a4deafc63c163dd4077b",
    "initial_capacity_request": "590eeea7f7ee2c96f0e2c9ad4d52585a60252c5a9e149dda6794649f19bbf886",
    "initial_capacity_measurement": "6df6fd80c3667947d1903582db2ff1dd627a9a8020502f4ef8ccf99a3963d941",
    "initial_capacity_provider_response": "894d18a6da7f24806d51c7224998dbc7c0467ed11474f76261543971a58671a3",
    "preturn_capacity_request": "bbd587e116d9671d1724274ae8248ce382d3468d7cef936488ad83261c8e4066",
    "preturn_capacity_measurement": "c2396006c3e291f36ae14cdfd0670437156c5f13724b8c25b4b546c5bfe082bb",
    "preturn_capacity_provider_response": "894d18a6da7f24806d51c7224998dbc7c0467ed11474f76261543971a58671a3",
}
PREDECESSOR_MODEL_CALLS = 21
PREDECESSOR_UNKNOWN_USAGE = 3
PREDECESSOR_MEASURED_TOKENS = 896_728

KeyedEnumCanaryError = one_turn.OneTurnCanaryError
KeyedEnumCanaryWaiting = one_turn.OneTurnCanaryWaiting


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
        raise KeyedEnumCanaryError("epoch-27 frozen source request drifted")
    return _load(path, "epoch-27 frozen source request")


def _build_request_uncached() -> dict[str, Any]:
    values = adapter.prepare_episode_batches(
        adapter._episode_from_request(_source_request()),  # noqa: SLF001
        batch_size=3,
        thread_mode="new_thread",
    )
    if len(values) != 1:
        raise KeyedEnumCanaryError("epoch-43 request count drifted")
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
    )
    if (
        tuple(request.get("segment_ids", ())) != SELECTED_SEGMENT_IDS
        or request.get("effective_batch_size") != 2
        or request.get("batch_size_ceiling") != 3
        or request.get("candidate_system_id") != adapter.CANDIDATE_SYSTEM_ID
        or request.get("model") != "gpt-5.6-sol"
        or request.get("effort") != "medium"
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
        raise KeyedEnumCanaryError("epoch-43 frozen request contract drifted")
    adapter.validate_prepared_request(request)
    return request


@lru_cache(maxsize=1)
def _frozen_request_json() -> str:
    return one_turn._canonical_json(_build_request_uncached())  # noqa: SLF001


def _build_request() -> dict[str, Any]:
    return json.loads(_frozen_request_json())


_PREDECESSOR_FILES: tuple[tuple[str, Path], ...] = (
    ("authorization", EPOCH42_ROOT / "operator-authorization.json"),
    ("receipt", EPOCH42_ROOT / "plan-step-receipt.json"),
    ("terminal", EPOCH42_ROOT / "terminal.json"),
    ("runtime_lock", EPOCH42_ROOT / "runtime-lock.json"),
    ("runtime_contract", EPOCH42_ROOT / "runtime-contract.json"),
    ("request", EPOCH42_ROOT / "prepared-turn/request.private.json"),
    ("attempt", EPOCH42_ROOT / "turn/semantic-attempt.json"),
    ("dispatch", EPOCH42_ROOT / "turn/semantic-dispatch.json"),
    ("thread_start", EPOCH42_ROOT / "turn/thread-start.json"),
    ("sidecar", EPOCH42_ROOT / "turn/sidecar.json"),
    ("output", EPOCH42_ROOT / "turn/output.private.json"),
    ("rejection", EPOCH42_ROOT / "semantic-rejection.json"),
    ("initial_capacity_request", EPOCH42_ROOT / "turn/capacity/initial/request.json"),
    (
        "initial_capacity_measurement",
        EPOCH42_ROOT / "turn/capacity/initial/measurement.json",
    ),
    (
        "initial_capacity_provider_response",
        EPOCH42_ROOT / "turn/capacity/initial/provider-response.private.json",
    ),
    ("preturn_capacity_request", EPOCH42_ROOT / "turn/capacity/preturn/request.json"),
    (
        "preturn_capacity_measurement",
        EPOCH42_ROOT / "turn/capacity/preturn/measurement.json",
    ),
    (
        "preturn_capacity_provider_response",
        EPOCH42_ROOT / "turn/capacity/preturn/provider-response.private.json",
    ),
    ("epoch27_request", EPOCH27_ROOT / "prepared-turn/request.private.json"),
)


def _predecessor_records() -> Mapping[str, Any]:
    return {
        "epoch42_rejected_compact_inventory_attempt": {
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
        path: EPOCH42_HASHES[role]
        for role, path in _PREDECESSOR_FILES
        if role != "epoch27_request"
    }
    expected[EPOCH27_ROOT / "prepared-turn/request.private.json"] = EPOCH27_REQUEST_SHA256
    if any(_record(path)["sha256"] != digest for path, digest in expected.items()):
        raise KeyedEnumCanaryError("epoch-43 predecessor record drifted")
    receipt = _load(EPOCH42_ROOT / "plan-step-receipt.json", "epoch-42 receipt")
    terminal = _load(EPOCH42_ROOT / "terminal.json", "epoch-42 terminal")
    sidecar = _load(EPOCH42_ROOT / "turn/sidecar.json", "epoch-42 sidecar")
    rejection = _load(EPOCH42_ROOT / "semantic-rejection.json", "epoch-42 rejection")
    initial_capacity = _load(
        EPOCH42_ROOT / "turn/capacity/initial/measurement.json",
        "epoch-42 initial capacity",
    )
    preturn_capacity = _load(
        EPOCH42_ROOT / "turn/capacity/preturn/measurement.json",
        "epoch-42 preturn capacity",
    )
    diagnostic = (
        "$.segments[1].discourse_events[6].event_type must be one of "
        "['term_usage', 'frame_usage', 'stance_position', 'forecast', "
        "'causal_mechanism', 'capability_claim', 'product_signal', 'market_signal', "
        "'risk_signal', 'counterclaim', 'uncertainty', 'adoption_signal', "
        "'actor_mention', 'entity_reference']"
    )
    if (
        receipt != terminal
        or receipt.get("state") != "rejected"
        or receipt.get("terminal_reason")
        != "epoch42_one_turn_canary_semantic_output_rejected"
        or receipt.get("new_semantic_model_call_count") != 1
        or receipt.get("new_unknown_usage_turn_count") != 0
        or receipt.get("semantic_retry_count") != 0
        or receipt.get("new_measured_usage", {}).get("total_tokens") != 25_283
        or receipt.get("aggregate_architecture_semantic_model_call_count") != 21
        or receipt.get("aggregate_architecture_unknown_usage_turn_count") != 3
        or receipt.get("aggregate_architecture_measured_total_tokens") != 896_728
        or receipt.get("failed_checks") != ["semantic_output_validity"]
        or receipt.get("diagnostic", {}).get("diagnostic_path") != diagnostic
        or receipt.get("full_six_case_total_token_projection_by_case_count") != 54_968
        or receipt.get("full_six_case_cost_projection_pass") is not True
        or receipt.get("structural_and_cost_pass") is not False
        or receipt.get("quality_measured_by_this_step") is not False
        or receipt.get("winner_frozen") is not False
        or receipt.get("holdout_authorized") is not False
        or receipt.get("production_mutated") is not False
        or sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("plan_type") != "pro"
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or sidecar.get("usage") != receipt.get("new_measured_usage")
        or sidecar.get("thread_total_usage") != sidecar.get("usage")
        or sidecar.get("recovery_reran_model") is not False
        or rejection.get("error_class") != "CanonicalV31OutputError"
        or rejection.get("diagnostic_path") != diagnostic
        or any(
            capacity.get("state") != "cleared_before_semantic_boundary"
            or capacity.get("capacity_available") is not True
            or capacity.get("managed_chatgpt_auth_only") is not True
            or capacity.get("rate_limit_reached_type") is not None
            for capacity in (initial_capacity, preturn_capacity)
        )
    ):
        raise KeyedEnumCanaryError("epoch-42 rejected predecessor contract drifted")


def _runtime_module_paths() -> Sequence[Path]:
    names = (
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
    canary = expected.get("canary_contract", {})
    ranking = expected.get("architecture_ranking", [])
    evidence = expected.get("architecture_evidence", {})
    required_true = (
        "all_canonical_enum_fields_are_position_specific",
        "deterministic_evidence_relative_metric_ordinal_projection_only",
        "deterministic_inventory_removal_only",
        "deterministic_keyed_enum_order_projection_only",
        "inventory_and_event_evidence_pointer_identity_required",
        "model_authors_closed_keyed_canonical_enum_object",
        "model_authors_evidence_relative_metric_token_ordinals",
        "model_authors_every_full_canonical_event_value",
        "model_authors_every_normalized_table_value",
        "model_authors_ordered_source_unit_proposition_inventory",
        "one_inventory_item_per_canonical_event",
    )
    if dict(value) != expected:
        raise KeyedEnumCanaryError("epoch-43 directive contract drifted")
    if (
        expected.get("schema_version")
        != "pif_evaluation_epoch43_keyed_enum_compact_inventory_canary_directive_v1"
        or expected.get("thread_id") != THREAD_ID
        or expected.get("plan_epoch") != PLAN_EPOCH
        or expected.get("step_id") != STEP_ID
        or expected.get("authority") != "direct_user_instruction"
        or expected.get("authorized_by") != "kolby"
        or expected.get("authorization_statement") != AUTHORIZATION_STATEMENT
        or expected.get("state")
        != "authorized_for_exactly_one_keyed_enum_compact_inventory_two_segment_canary"
        or expected.get("expected_receipt_path")
        != str(DEFAULT_ROOT / "plan-step-receipt.json")
        or len(ranking) != 3
        or [row.get("rank") for row in ranking] != [1, 2, 3]
        or sum(row.get("selected") is True for row in ranking) != 1
        or ranking[0].get("architecture")
        != "one_turn_compact_inventory_closed_keyed_canonical_enum_transport"
        or ranking[0].get("selected") is not True
        or evidence.get("epoch42_state") != "rejected"
        or evidence.get("epoch42_measured_total_tokens") != 25_283
        or evidence.get("epoch42_raw_inventory_item_count_before_canonical_validation")
        != 28
        or evidence.get("epoch42_raw_event_count_before_canonical_validation") != 28
        or evidence.get("epoch42_homogeneous_enum_vector_allowed_cross_field_value")
        is not True
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
        or canary.get("inventory_protocol_version")
        != adapter.INVENTORY_PROTOCOL_VERSION
        or canary.get("keyed_enum_protocol_version")
        != adapter.KEYED_ENUM_PROTOCOL_VERSION
        or any(canary.get(name) is not True for name in required_true)
        or canary.get("table_duplicate_rejection_allowed") is not False
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
            "maximum_semantic_model_call_count_after_epoch43_dispatch": 22,
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
        raise KeyedEnumCanaryError("epoch-43 directive invariants drifted")


def _receipt_metadata(outcome: Mapping[str, Any]) -> Mapping[str, Any]:
    usage = outcome["usage"]
    total = int(usage["total_tokens"])
    fidelity_path = DEFAULT_ROOT / "turn/semantic-fidelity.json"
    fidelity: Mapping[str, Any] = {}
    if fidelity_path.is_file():
        fidelity = _load(fidelity_path, "epoch-43 semantic fidelity")
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
        "epoch42_replayed": False,
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
                "terminal_reason": "epoch43_full_six_case_cost_projection_rejected",
                "next_authorized_action": (
                    "reject_keyed_enum_compact_inventory_architecture_without_field_patch"
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
    stage="keyed_enum_compact_inventory_full_canonical_two_segment_canary",
    architecture_class="one_turn_compact_inventory_closed_keyed_canonical_enum_transport_v1",
    authorization_statement=AUTHORIZATION_STATEMENT,
    authorization_state="authorized_for_exactly_one_keyed_enum_compact_inventory_two_segment_canary",
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
    reject_next_action="reject_keyed_enum_compact_inventory_architecture_without_field_patch",
    waiting_next_action="no_replay_bounded_recovery_only",
)


def _runtime_status(status: Mapping[str, Any], root: Path) -> dict[str, Any]:
    request = _load(root.resolve() / "prepared-turn/request.private.json", "epoch-43 request")
    shape = _request_shape(request)
    if shape != EXPECTED_SHAPE or _request_sha256(request) != EXPECTED_REQUEST_SHA256:
        raise KeyedEnumCanaryError("epoch-43 runtime request drifted")
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
