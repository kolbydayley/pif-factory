from __future__ import annotations

"""Compact inventory adapter with closed keyed canonical enum transport."""

import copy
import hashlib
import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import (
    app_server_canonical_v31_compact_inventory_evidence_ordinal_episode_batch as parent,
)


base = parent.base
PROJECT_ROOT = parent.PROJECT_ROOT
MODEL = parent.MODEL
EFFORT = parent.EFFORT
PINNED_CODEX = parent.PINNED_CODEX
CANONICAL_LABEL_PACK = parent.CANONICAL_LABEL_PACK
CANONICAL_LABEL_SCHEMA_SHA256 = parent.CANONICAL_LABEL_SCHEMA_SHA256
SUPPORTED_BATCH_SIZES = parent.SUPPORTED_BATCH_SIZES
SUPPORTED_THREAD_MODES = parent.SUPPORTED_THREAD_MODES
RETRY_COUNT = parent.RETRY_COUNT
USAGE_FIELDS = parent.USAGE_FIELDS
CanonicalV31EpisodeBatchError = parent.CanonicalV31EpisodeBatchError
CanonicalV31OutputError = parent.CanonicalV31OutputError
CanonicalV31TelemetryError = parent.CanonicalV31TelemetryError
_client_factory = parent._client_factory
_verify_started_thread = parent._verify_started_thread
expected_instruction_source_contract = parent.expected_instruction_source_contract
verified_context_control_overlay = parent.verified_context_control_overlay

ADAPTER_SCHEMA_VERSION = (
    "pif_canonical_v31_keyed_enum_compact_inventory_evidence_ordinal_episode_batch_v1"
)
PROJECTION_SCHEMA_VERSION = (
    "pif_canonical_v31_keyed_enum_compact_inventory_evidence_ordinal_projection_v1"
)
PROVENANCE_SCHEMA_VERSION = (
    "pif_canonical_v31_keyed_enum_compact_inventory_evidence_ordinal_provenance_v1"
)
FIDELITY_SCHEMA_VERSION = (
    "pif_canonical_v31_keyed_enum_compact_inventory_evidence_ordinal_fidelity_v1"
)
CANDIDATE_SYSTEM_ID = (
    "pif_keyed_enum_compact_inventory_evidence_ordinal_ai_discourse_v3_1_v1"
)
CANONICAL_SCHEMA_STRATEGY = (
    "canonical_v31_compact_inventory_closed_keyed_enum_evidence_ordinal_v1"
)
KEYED_ENUM_PROTOCOL_VERSION = "pif_closed_keyed_canonical_enum_object_v1"
INVENTORY_PROTOCOL_VERSION = parent.INVENTORY_PROTOCOL_VERSION
EVENT_ENUM_NAMES = parent.EVENT_ENUM_NAMES
_PARENT_REQUEST_CACHE: dict[tuple[str, int, str, tuple[str, ...]], str] = {}


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def semantic_integrity_contract() -> dict[str, Any]:
    value = copy.deepcopy(parent.semantic_integrity_contract())
    value.update(
        {
            "keyed_enum_protocol_version": KEYED_ENUM_PROTOCOL_VERSION,
            "model_authors_closed_keyed_canonical_enum_object": True,
            "all_canonical_enum_fields_are_position_specific": True,
            "deterministic_keyed_enum_order_projection_only": True,
            "deterministic_semantic_defaults": {},
            "deterministic_semantic_pruning": False,
            "deterministic_support_filtering": False,
            "deterministic_deduplication": False,
            "deterministic_relabeling": False,
        }
    )
    return value


def _episode_from_request(request: Mapping[str, Any]) -> dict[str, Any]:
    return parent._episode_from_request(request)  # noqa: SLF001


@lru_cache(maxsize=8)
def _parent_request_json(
    episode_json: str,
    batch_size: int,
    thread_mode: str,
    segment_ids: tuple[str, ...],
) -> str:
    values = parent.prepare_episode_batches(
        json.loads(episode_json), batch_size=batch_size, thread_mode=thread_mode
    )
    matches = [value for value in values if tuple(value["segment_ids"]) == segment_ids]
    if len(matches) != 1:
        raise CanonicalV31EpisodeBatchError(
            "keyed-enum parent request cannot be reconstructed"
        )
    return _canonical_json(matches[0])


def _parent_cache_key(
    request: Mapping[str, Any],
) -> tuple[str, int, str, tuple[str, ...]]:
    return (
        _canonical_json(_episode_from_request(request)),
        int(request["batch_size_ceiling"]),
        str(request["thread_mode"]),
        tuple(request["segment_ids"]),
    )


def _parent_request(request: Mapping[str, Any]) -> dict[str, Any]:
    key = _parent_cache_key(request)
    encoded = _PARENT_REQUEST_CACHE.get(key)
    if encoded is None:
        encoded = _parent_request_json(*key)
        if len(_PARENT_REQUEST_CACHE) >= 8:
            _PARENT_REQUEST_CACHE.pop(next(iter(_PARENT_REQUEST_CACHE)))
        _PARENT_REQUEST_CACHE[key] = encoded
    return json.loads(encoded)


def _keyed_enum_schema(request: Mapping[str, Any]) -> dict[str, Any]:
    schema = copy.deepcopy(request["output_schema"])
    event = schema["properties"]["1"]["items"]["properties"]["2"]["items"][
        "properties"
    ]["1"]["items"]
    enum_vector = event["properties"]["0"]
    choices = enum_vector.get("items", {}).get("anyOf")
    if (
        enum_vector.get("type") != "array"
        or enum_vector.get("minItems") != len(EVENT_ENUM_NAMES)
        or enum_vector.get("maxItems") != len(EVENT_ENUM_NAMES)
        or not isinstance(choices, list)
        or len(choices) != len(EVENT_ENUM_NAMES)
    ):
        raise CanonicalV31EpisodeBatchError("keyed-enum parent schema drifted")
    event["properties"]["0"] = {
        "type": "object",
        "additionalProperties": False,
        "required": [str(index) for index in range(len(EVENT_ENUM_NAMES))],
        "properties": {
            str(index): copy.deepcopy(choice) for index, choice in enumerate(choices)
        },
    }
    schema["$id"] = ADAPTER_SCHEMA_VERSION
    return schema


def _keyed_enum_instructions(base_instructions: str) -> str:
    old = (
        "Field 0 is the five-value enum vector "
        + _canonical_json(EVENT_ENUM_NAMES)
        + "."
    )
    new = (
        "Field 0 is a closed keyed enum object with numeric keys 0..4 corresponding "
        "in order to "
        + _canonical_json(EVENT_ENUM_NAMES)
        + "; each key accepts only that named canonical field's enum."
    )
    if base_instructions.count(old) != 1:
        raise CanonicalV31EpisodeBatchError("keyed-enum instruction boundary drifted")
    value = base_instructions.replace(old, new, 1)
    if old in value or new not in value:
        raise CanonicalV31EpisodeBatchError("keyed-enum instruction replacement failed")
    return value


def _keyed_request(request: Mapping[str, Any]) -> dict[str, Any]:
    value = copy.deepcopy(dict(request))
    value["schema_version"] = ADAPTER_SCHEMA_VERSION
    value["candidate_system_id"] = CANDIDATE_SYSTEM_ID
    value["canonical_schema_strategy"] = CANONICAL_SCHEMA_STRATEGY
    value["batch_id"] = "cv31keci_" + _sha256_text(
        _canonical_json(
            {
                "parent_batch_id": request["batch_id"],
                "candidate_system_id": CANDIDATE_SYSTEM_ID,
                "keyed_enum_protocol_version": KEYED_ENUM_PROTOCOL_VERSION,
            }
        )
    )[:24]
    value["semantic_integrity"] = semantic_integrity_contract()
    value["private_input"]["schema_version"] = ADAPTER_SCHEMA_VERSION
    value["base_instructions"] = _keyed_enum_instructions(value["base_instructions"])
    value["base_instructions_sha256"] = _sha256_text(value["base_instructions"])
    value["output_schema"] = _keyed_enum_schema(request)
    value["output_schema_sha256"] = _sha256_text(
        _canonical_json(value["output_schema"])
    )
    _PARENT_REQUEST_CACHE[_parent_cache_key(value)] = _canonical_json(request)
    return value


def prepare_episode_batches(
    episode: Mapping[str, Any], *, batch_size: int, thread_mode: str
) -> list[dict[str, Any]]:
    values = [
        _keyed_request(request)
        for request in parent.prepare_episode_batches(
            episode, batch_size=batch_size, thread_mode=thread_mode
        )
    ]
    for value in values:
        validate_prepared_request(value)
    return values


def validate_prepared_request(request: Mapping[str, Any]) -> dict[str, Any]:
    expected = _keyed_request(_parent_request(request))
    if dict(request) != expected:
        raise CanonicalV31EpisodeBatchError("keyed-enum request drifted")
    return copy.deepcopy(dict(request))


def _parent_wire_output(output: Mapping[str, Any]) -> dict[str, Any]:
    converted = copy.deepcopy(dict(output))
    segments = converted.get("1")
    if not isinstance(segments, list):
        raise CanonicalV31OutputError("keyed-enum output segment collection is malformed")
    for segment in segments:
        units = segment.get("2") if isinstance(segment, Mapping) else None
        if not isinstance(units, list):
            raise CanonicalV31OutputError("keyed-enum output unit collection is malformed")
        for unit in units:
            events = unit.get("1") if isinstance(unit, Mapping) else None
            if not isinstance(events, list):
                raise CanonicalV31OutputError("keyed-enum output event collection is malformed")
            for event in events:
                keyed = event.get("0") if isinstance(event, Mapping) else None
                required = {str(index) for index in range(len(EVENT_ENUM_NAMES))}
                if not isinstance(keyed, Mapping) or set(keyed) != required:
                    raise CanonicalV31OutputError("keyed-enum canonical field object drifted")
                event["0"] = [copy.deepcopy(keyed[str(index)]) for index in range(5)]
    return converted


def validate_and_project_output(
    request: Mapping[str, Any], output: Mapping[str, Any]
) -> dict[str, Any]:
    validated = validate_prepared_request(request)
    projected = parent.validate_and_project_output(
        _parent_request(validated), _parent_wire_output(output)
    )
    provenance = copy.deepcopy(projected["provenance"])
    provenance.update(
        {
            "schema_version": PROVENANCE_SCHEMA_VERSION,
            "candidate_system_id": CANDIDATE_SYSTEM_ID,
            "keyed_enum_protocol_version": KEYED_ENUM_PROTOCOL_VERSION,
        }
    )
    fidelity = copy.deepcopy(projected["fidelity"])
    fidelity.update(
        {
            "schema_version": FIDELITY_SCHEMA_VERSION,
            "candidate_system_id": CANDIDATE_SYSTEM_ID,
            "keyed_enum_protocol_version": KEYED_ENUM_PROTOCOL_VERSION,
            "model_authors_closed_keyed_canonical_enum_object": True,
            "all_canonical_enum_fields_are_position_specific": True,
            "deterministic_keyed_enum_order_projection_only": True,
            "deterministic_semantic_defaults": {},
            "deterministic_semantic_pruning": False,
            "deterministic_support_filtering": False,
            "deterministic_deduplication": False,
            "deterministic_relabeling": False,
        }
    )
    return {
        "schema_version": PROJECTION_SCHEMA_VERSION,
        "labels": projected["labels"],
        "provenance": provenance,
        "fidelity": fidelity,
    }


def encode_parent_output_for_test(
    request: Mapping[str, Any], output: Mapping[str, Any]
) -> dict[str, Any]:
    converted = parent.encode_parent_output_for_test(_parent_request(request), output)
    for segment in converted["1"]:
        for unit in segment["2"]:
            for event in unit["1"]:
                event["0"] = {
                    str(index): copy.deepcopy(value)
                    for index, value in enumerate(event["0"])
                }
    return converted


def validate_turn_sidecar(
    request: Mapping[str, Any],
    sidecar_path: Path,
    *,
    output_path: Path,
    expected_thread: Any,
) -> dict[str, Any]:
    validated = validate_prepared_request(request)
    sidecar = base._load_json(sidecar_path, "keyed-enum sidecar")  # noqa: SLF001
    thread_preflight = _verify_started_thread(expected_thread, validated)
    if (
        not isinstance(sidecar, Mapping)
        or sidecar.get("schema_version") != base.codex_app_server.TURN_SIDECAR_SCHEMA_VERSION
        or not base._is_iso_timestamp(sidecar.get("started_at"))  # noqa: SLF001
        or not base._is_iso_timestamp(sidecar.get("finished_at"))  # noqa: SLF001
        or sidecar.get("client_version") != base.codex_app_server.APP_SERVER_CLIENT_VERSION
        or sidecar.get("cli_version") != base.codex_app_server.PINNED_CODEX_CLI_VERSION
        or sidecar.get("protocol_schema_sha256")
        != base._sha256_file(base.codex_app_server.PROTOCOL_SCHEMA_PATH)  # noqa: SLF001
        or sidecar.get("transport") != "stdio"
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("plan_type") != "pro"
        or sidecar.get("thread_id") != thread_preflight["thread_id"]
        or not isinstance(sidecar.get("turn_id"), str)
        or not sidecar.get("turn_id")
        or sidecar.get("model") != MODEL
        or sidecar.get("effort") != EFFORT
        or sidecar.get("thread_mode") != validated["thread_mode"]
        or sidecar.get("batch_size") != validated["effective_batch_size"]
        or sidecar.get("prompt_sha256") != validated["prompt_sha256"]
        or sidecar.get("prompt_bytes") != len(validated["prompt"].encode("utf-8"))
        or sidecar.get("base_instructions_sha256") != validated["base_instructions_sha256"]
        or sidecar.get("base_instructions_bytes")
        != len(validated["base_instructions"].encode("utf-8"))
        or sidecar.get("instruction_sources_sha256")
        != thread_preflight["instruction_sources_sha256"]
        or sidecar.get("instruction_sources_count")
        != thread_preflight["instruction_sources_count"]
        or sidecar.get("output_schema_sha256") != validated["output_schema_sha256"]
        or sidecar.get("output_schema_bytes")
        != len(_canonical_json(validated["output_schema"]).encode("utf-8"))
        or sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("error_class") is not None
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or sidecar.get("recovery_reran_model") is not False
        or sidecar.get("synthetic_debug_errors") is not False
    ):
        raise CanonicalV31TelemetryError("managed-auth keyed-enum sidecar contract failed")
    usage = base._usage_values(sidecar.get("usage"), "usage")  # noqa: SLF001
    total = base._usage_values(  # noqa: SLF001
        sidecar.get("thread_total_usage"), "thread_total_usage"
    )
    if any(total[field] < usage[field] for field in USAGE_FIELDS):
        raise CanonicalV31TelemetryError("thread total usage is below last usage")
    wall = base._nonnegative_number(  # noqa: SLF001
        sidecar.get("wall_elapsed_seconds"), "wall_elapsed_seconds"
    )
    if (
        not output_path.is_file()
        or sidecar.get("output_sha256")
        != base._output_message_hash(output_path)  # noqa: SLF001
        or Path(str(sidecar.get("output_path") or "")).expanduser().resolve()
        != output_path.expanduser().resolve()
    ):
        raise CanonicalV31TelemetryError("keyed-enum output is absent or changed")
    return {
        "sidecar": copy.deepcopy(dict(sidecar)),
        "thread_id": sidecar["thread_id"],
        "turn_id": sidecar["turn_id"],
        "usage": usage,
        "thread_total_usage": total,
        "wall_elapsed_seconds": wall,
        "cached_input_tokens": total["cached_input_tokens"],
        "reasoning_output_tokens": total["reasoning_output_tokens"],
    }


def build_six_arm_matrix_binding() -> dict[str, Any]:
    return {
        "schema_version": "pif_canonical_v31_keyed_enum_compact_inventory_binding_v1",
        "parent_binding": parent.build_six_arm_matrix_binding(),
        "parent_adapter_module_sha256": hashlib.sha256(
            Path(parent.__file__).read_bytes()
        ).hexdigest(),
        "adapter_module_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "candidate_system_id": CANDIDATE_SYSTEM_ID,
        "canonical_schema_strategy": CANONICAL_SCHEMA_STRATEGY,
        "inventory_protocol_version": INVENTORY_PROTOCOL_VERSION,
        "keyed_enum_protocol_version": KEYED_ENUM_PROTOCOL_VERSION,
        "model": MODEL,
        "effort": EFFORT,
        "deterministic_inventory_removal_only": True,
        "deterministic_evidence_relative_metric_ordinal_projection_only": True,
        "deterministic_keyed_enum_order_projection_only": True,
        "deterministic_semantic_defaults": {},
        "deterministic_semantic_pruning": False,
        "deterministic_support_filtering": False,
        "deterministic_deduplication": False,
        "deterministic_relabeling": False,
    }


__all__: Sequence[str] = (
    "ADAPTER_SCHEMA_VERSION",
    "CANDIDATE_SYSTEM_ID",
    "INVENTORY_PROTOCOL_VERSION",
    "KEYED_ENUM_PROTOCOL_VERSION",
    "CanonicalV31EpisodeBatchError",
    "CanonicalV31OutputError",
    "CanonicalV31TelemetryError",
    "build_six_arm_matrix_binding",
    "encode_parent_output_for_test",
    "prepare_episode_batches",
    "validate_and_project_output",
    "validate_prepared_request",
    "validate_turn_sidecar",
)
