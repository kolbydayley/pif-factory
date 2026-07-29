from __future__ import annotations

"""Full-canonical extraction through disjoint owner-local semantic shards."""

import copy
import hashlib
import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_canonical_v31_compact_exhaustive_event_table_episode_batch as parent


base = parent.base
PROJECT_ROOT = parent.PROJECT_ROOT
MODEL = parent.MODEL
EFFORT = "medium"
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

ADAPTER_SCHEMA_VERSION = "pif_canonical_v31_owner_local_semantic_shard_episode_batch_v1"
RAW_OUTPUT_SCHEMA_VERSION = "pif_canonical_v31_owner_local_semantic_shard_raw_output_v1"
PROJECTION_SCHEMA_VERSION = "pif_canonical_v31_owner_local_semantic_shard_projection_v1"
PROVENANCE_SCHEMA_VERSION = "pif_canonical_v31_owner_local_semantic_shard_provenance_v1"
FIDELITY_SCHEMA_VERSION = "pif_canonical_v31_owner_local_semantic_shard_fidelity_v1"
CANDIDATE_SYSTEM_ID = "pif_owner_local_semantic_shard_ai_discourse_v3_1_v1"
CANONICAL_SCHEMA_STRATEGY = "canonical_v31_owner_local_disjoint_semantic_shards_v1"
SHARD_PROTOCOL_VERSION = "pif_owner_local_disjoint_semantic_shards_v1"
SHARD_NAMES = (
    "assertions_capabilities_product_market_adoption_entity_actor",
    "causal_mechanisms_consequences_risks",
    "comparisons_counterclaims_stance_uncertainty",
    "recommendations_constraints_tradeoffs_alternatives_forecasts",
)

_PARENT_REQUEST_CACHE: dict[tuple[str, int, str, tuple[str, ...]], str] = {}
_SHARD_REQUEST_CACHE: dict[tuple[str, int, str, tuple[str, ...]], str] = {}


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def semantic_integrity_contract() -> dict[str, Any]:
    value = copy.deepcopy(parent.semantic_integrity_contract())
    value.update(
        {
            "reasoning_effort": EFFORT,
            "shard_protocol_version": SHARD_PROTOCOL_VERSION,
            "semantic_shard_names": list(SHARD_NAMES),
            "model_assigns_each_event_to_exactly_one_semantic_shard": True,
            "model_authors_every_normalized_table_value": True,
            "model_authors_every_full_canonical_event_value": True,
            "deterministic_shard_concatenation_only": True,
            "event_count_is_diagnostic_only": True,
            "table_duplicate_count_is_diagnostic_only": True,
            "cross_shard_duplicate_count_is_diagnostic_only": True,
            "deterministic_semantic_defaults": {},
            "deterministic_semantic_pruning": False,
            "deterministic_support_filtering": False,
            "deterministic_deduplication": False,
            "deterministic_relabeling": False,
        }
    )
    return value


def _shard_instructions() -> str:
    return (
        "# Owner-local disjoint semantic shards\n"
        "For every source unit, perform four independent owner-local semantic passes and "
        "place each complete compact canonical event in exactly one unit-field-0 shard. "
        "The four closed shard arrays, in key order 0 through 3, are: "
        f"{_canonical_json(SHARD_NAMES)}. Use the lane whose semantic purpose most directly "
        "owns the proposition. Do not repeat an event across lanes. Preserve independent "
        "adjacent propositions even when they share an actor, topic, evidence span, or "
        "conclusion. Do not merge recommendations, counterclaims, consequences, comparisons, "
        "constraints, or tradeoffs merely because they are related. Do not invent unsupported "
        "propositions and do not aim for a target count. Unit field 1 remains the independently "
        "authored compact concept array. All normalized table values and all event and concept "
        "semantics remain model authored. Deterministic code concatenates the four arrays in "
        "key order only; it never prunes, support-filters, deduplicates, relabels, defaults, "
        "repairs, or reassigns semantics.\n\n"
    )


def _episode_from_request(request: Mapping[str, Any]) -> dict[str, Any]:
    episode = copy.deepcopy(dict(request["episode_context"]))
    episode["segments"] = [
        {
            key: copy.deepcopy(segment[key])
            for key in (
                "segment_id",
                "segment_text",
                "segment_quality",
                "density_stratum",
                "boundaries",
            )
        }
        for segment in request["private_input"]["segments"]
    ]
    return episode


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
        raise CanonicalV31EpisodeBatchError("semantic-shard parent request cannot be reconstructed")
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


def _output_schema(request: Mapping[str, Any]) -> dict[str, Any]:
    schema = copy.deepcopy(request["output_schema"])
    row = schema["properties"]["1"]["items"]["properties"]["2"]["items"]
    if row.get("required") != ["0", "1"] or set(row.get("properties", {})) != {"0", "1"}:
        raise CanonicalV31EpisodeBatchError("semantic-shard parent unit schema drifted")
    events = copy.deepcopy(row["properties"]["0"])
    if events.get("type") != "array" or not isinstance(events.get("items"), Mapping):
        raise CanonicalV31EpisodeBatchError("semantic-shard parent event schema drifted")
    row["properties"]["0"] = {
        "type": "object",
        "additionalProperties": False,
        "required": [str(index) for index in range(len(SHARD_NAMES))],
        "properties": {
            str(index): copy.deepcopy(events) for index in range(len(SHARD_NAMES))
        },
    }
    schema["$id"] = RAW_OUTPUT_SCHEMA_VERSION
    return schema


def _shard_request(request: Mapping[str, Any]) -> dict[str, Any]:
    value = copy.deepcopy(dict(request))
    value["schema_version"] = ADAPTER_SCHEMA_VERSION
    value["candidate_system_id"] = CANDIDATE_SYSTEM_ID
    value["canonical_schema_strategy"] = CANONICAL_SCHEMA_STRATEGY
    value["effort"] = EFFORT
    value["batch_id"] = "cv31olss_" + _sha256_text(
        _canonical_json(
            {
                "parent_batch_id": request["batch_id"],
                "candidate_system_id": CANDIDATE_SYSTEM_ID,
                "effort": EFFORT,
                "shard_protocol_version": SHARD_PROTOCOL_VERSION,
                "semantic_shard_names": list(SHARD_NAMES),
            }
        )
    )[:24]
    value["semantic_integrity"] = semantic_integrity_contract()
    value["private_input"]["schema_version"] = ADAPTER_SCHEMA_VERSION
    value["base_instructions"] = _shard_instructions() + value["base_instructions"]
    value["base_instructions_sha256"] = _sha256_text(value["base_instructions"])
    value["output_schema"] = _output_schema(request)
    value["output_schema_sha256"] = _sha256_text(_canonical_json(value["output_schema"]))
    cache_key = _parent_cache_key(value)
    _PARENT_REQUEST_CACHE[cache_key] = _canonical_json(request)
    _SHARD_REQUEST_CACHE[cache_key] = _canonical_json(value)
    return value


def prepare_episode_batches(
    episode: Mapping[str, Any], *, batch_size: int, thread_mode: str
) -> list[dict[str, Any]]:
    values = [
        _shard_request(request)
        for request in parent.prepare_episode_batches(
            episode, batch_size=batch_size, thread_mode=thread_mode
        )
    ]
    for value in values:
        validate_prepared_request(value)
    return values


def validate_prepared_request(request: Mapping[str, Any]) -> dict[str, Any]:
    cache_key = _parent_cache_key(request)
    encoded = _SHARD_REQUEST_CACHE.get(cache_key)
    if encoded is None:
        encoded = _canonical_json(_shard_request(_parent_request(request)))
    if _canonical_json(request) != encoded:
        raise CanonicalV31EpisodeBatchError("owner-local semantic-shard request drifted")
    return copy.deepcopy(dict(request))


def _parent_wire_output(
    request: Mapping[str, Any], output: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    if not isinstance(output, Mapping) or set(output) != {"0", "1"}:
        raise CanonicalV31OutputError("semantic-shard output shape drifted")
    converted = copy.deepcopy(dict(output))
    segments = converted["1"]
    if not isinstance(segments, list) or len(segments) != len(request["private_input"]["segments"]):
        raise CanonicalV31OutputError("semantic-shard segment count drifted")
    lane_counts = [0 for _ in SHARD_NAMES]
    manifest: list[dict[str, Any]] = []
    for segment_index, segment in enumerate(segments):
        unit_rows = segment.get("2") if isinstance(segment, Mapping) else None
        if not isinstance(unit_rows, list):
            raise CanonicalV31OutputError("semantic-shard unit rows drifted")
        for unit_index, row in enumerate(unit_rows):
            if not isinstance(row, Mapping) or set(row) != {"0", "1"}:
                raise CanonicalV31OutputError("semantic-shard unit row is malformed")
            shards = row["0"]
            concepts = row["1"]
            expected_keys = {str(index) for index in range(len(SHARD_NAMES))}
            if not isinstance(shards, Mapping) or set(shards) != expected_keys:
                raise CanonicalV31OutputError("semantic-shard lane object is malformed")
            if not isinstance(concepts, list):
                raise CanonicalV31OutputError("semantic-shard concept collection is malformed")
            events: list[dict[str, Any]] = []
            for lane_index, lane_name in enumerate(SHARD_NAMES):
                lane = shards[str(lane_index)]
                if not isinstance(lane, list):
                    raise CanonicalV31OutputError("semantic-shard lane collection is malformed")
                lane_counts[lane_index] += len(lane)
                for lane_event_index, event in enumerate(lane):
                    if not isinstance(event, Mapping):
                        raise CanonicalV31OutputError("semantic-shard event row is malformed")
                    event_copy = copy.deepcopy(dict(event))
                    events.append(event_copy)
                    manifest.append(
                        {
                            "segment_index": segment_index,
                            "unit_index": unit_index,
                            "lane_index": lane_index,
                            "lane_name": lane_name,
                            "lane_event_index": lane_event_index,
                            "event_sha256": _sha256_text(_canonical_json(event_copy)),
                        }
                    )
            row["0"] = events
            row["1"] = copy.deepcopy(concepts)
    event_hashes = [item["event_sha256"] for item in manifest]
    return converted, {
        "semantic_shard_names": list(SHARD_NAMES),
        "semantic_shard_event_counts": lane_counts,
        "semantic_shard_event_count": len(manifest),
        "cross_shard_duplicate_count": len(event_hashes) - len(set(event_hashes)),
        "semantic_shard_manifest_sha256": _sha256_text(_canonical_json(manifest)),
    }


def validate_and_project_output(
    request: Mapping[str, Any], output: Mapping[str, Any]
) -> dict[str, Any]:
    validated = validate_prepared_request(request)
    parent_output, diagnostics = _parent_wire_output(validated, output)
    projected = parent.validate_and_project_output(_parent_request(validated), parent_output)
    provenance = copy.deepcopy(projected["provenance"])
    provenance.update(
        {
            "schema_version": PROVENANCE_SCHEMA_VERSION,
            "candidate_system_id": CANDIDATE_SYSTEM_ID,
            "reasoning_effort": EFFORT,
            "shard_protocol_version": SHARD_PROTOCOL_VERSION,
            "semantic_shard_manifest_sha256": diagnostics["semantic_shard_manifest_sha256"],
        }
    )
    fidelity = copy.deepcopy(projected["fidelity"])
    fidelity.update(
        {
            "schema_version": FIDELITY_SCHEMA_VERSION,
            "candidate_system_id": CANDIDATE_SYSTEM_ID,
            "reasoning_effort": EFFORT,
            "shard_protocol_version": SHARD_PROTOCOL_VERSION,
            **diagnostics,
            "deterministic_shard_concatenation_only": True,
            "event_count_is_diagnostic_only": True,
            "cross_shard_duplicate_count_is_diagnostic_only": True,
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
    request: Mapping[str, Any], parent_output: Mapping[str, Any]
) -> dict[str, Any]:
    validate_prepared_request(request)
    value = copy.deepcopy(dict(parent_output))
    for segment in value["1"]:
        for row in segment["2"]:
            lanes = {str(index): [] for index in range(len(SHARD_NAMES))}
            events = row["0"]
            for event_index, event in enumerate(events):
                lane_index = min(
                    event_index * len(SHARD_NAMES) // max(len(events), 1),
                    len(SHARD_NAMES) - 1,
                )
                lanes[str(lane_index)].append(copy.deepcopy(event))
            row["0"] = lanes
    return value


def validate_turn_sidecar(
    request: Mapping[str, Any],
    sidecar_path: Path,
    *,
    output_path: Path,
    expected_thread: Any,
) -> dict[str, Any]:
    validated = validate_prepared_request(request)
    sidecar = base._load_json(sidecar_path, "semantic-shard sidecar")  # noqa: SLF001
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
        raise CanonicalV31TelemetryError("managed-auth semantic-shard sidecar contract failed")
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
        or sidecar.get("output_sha256") != base._output_message_hash(output_path)  # noqa: SLF001
        or Path(str(sidecar.get("output_path") or "")).expanduser().resolve()
        != output_path.expanduser().resolve()
    ):
        raise CanonicalV31TelemetryError("sidecar-bound semantic-shard output is absent or changed")
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
        "schema_version": "pif_canonical_v31_owner_local_semantic_shard_binding_v1",
        "parent_binding": parent.build_six_arm_matrix_binding(),
        "parent_adapter_module_sha256": hashlib.sha256(Path(parent.__file__).read_bytes()).hexdigest(),
        "adapter_module_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "candidate_system_id": CANDIDATE_SYSTEM_ID,
        "canonical_schema_strategy": CANONICAL_SCHEMA_STRATEGY,
        "shard_protocol_version": SHARD_PROTOCOL_VERSION,
        "semantic_shard_names": list(SHARD_NAMES),
        "model": MODEL,
        "effort": EFFORT,
        "deterministic_shard_concatenation_only": True,
        "deterministic_semantic_defaults": {},
        "deterministic_semantic_pruning": False,
        "deterministic_support_filtering": False,
        "deterministic_deduplication": False,
        "deterministic_relabeling": False,
    }


__all__: Sequence[str] = (
    "ADAPTER_SCHEMA_VERSION",
    "CANDIDATE_SYSTEM_ID",
    "CanonicalV31EpisodeBatchError",
    "CanonicalV31OutputError",
    "CanonicalV31TelemetryError",
    "SHARD_NAMES",
    "SHARD_PROTOCOL_VERSION",
    "build_six_arm_matrix_binding",
    "encode_parent_output_for_test",
    "prepare_episode_batches",
    "validate_and_project_output",
    "validate_prepared_request",
    "validate_turn_sidecar",
)
