from __future__ import annotations

"""Full-canonical extraction as a nested proposition-to-event ledger."""

import copy
import hashlib
import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_canonical_v31_tagged_metric_token_id_episode_batch as parent


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

ADAPTER_SCHEMA_VERSION = "pif_canonical_v31_nested_proposition_event_ledger_episode_batch_v1"
RAW_OUTPUT_SCHEMA_VERSION = "pif_canonical_v31_nested_proposition_event_ledger_raw_output_v1"
PROJECTION_SCHEMA_VERSION = "pif_canonical_v31_nested_proposition_event_ledger_projection_v1"
PROVENANCE_SCHEMA_VERSION = "pif_canonical_v31_nested_proposition_event_ledger_provenance_v1"
FIDELITY_SCHEMA_VERSION = "pif_canonical_v31_nested_proposition_event_ledger_fidelity_v1"
CANDIDATE_SYSTEM_ID = "pif_nested_proposition_event_ledger_ai_discourse_v3_1_v1"
CANONICAL_SCHEMA_STRATEGY = "canonical_v31_nested_proposition_event_ledger_v1"
LEDGER_PROTOCOL_VERSION = "pif_source_unit_nested_proposition_event_ledger_v1"
_PARENT_REQUEST_CACHE: dict[tuple[str, int, str, tuple[str, ...]], str] = {}


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def semantic_integrity_contract() -> dict[str, Any]:
    value = copy.deepcopy(parent.semantic_integrity_contract())
    value.update(
        {
            "reasoning_effort": EFFORT,
            "ledger_protocol_version": LEDGER_PROTOCOL_VERSION,
            "model_authors_one_nested_full_canonical_event_per_proposition": True,
            "no_duplicate_inventory_event_evidence_transport": True,
            "ledger_summary_is_private_fidelity_evidence": True,
            "deterministic_ledger_unwrap_only": True,
            "deterministic_semantic_defaults": {},
            "deterministic_semantic_pruning": False,
            "deterministic_support_filtering": False,
            "deterministic_deduplication": False,
            "deterministic_relabeling": False,
        }
    )
    return value


def _ledger_instructions() -> str:
    return (
        "# Nested proposition-event ledger\n"
        "For each source unit, author an ordered ledger in unit field 0. Each ledger "
        "item contains a concise proposition label in item field 0 and exactly one full "
        "canonical event in item field 1. Inventory every independent grounded eligible "
        "proposition supported by the source unit exactly once. Keep adjacent "
        "recommendations, counterclaims, consequences, comparisons, and tradeoffs "
        "separate when each is independently asserted; do not merge them merely because "
        "they share an actor or topic. Do not invent unsupported propositions. Because "
        "the event is nested in its proposition item, author evidence only in the event's "
        "existing evidence field; there is no duplicate inventory evidence field. Unit "
        "field 1 remains the independently authored concept array. The proposition label "
        "is private planning output retained only for fidelity audit. Do not aim for a "
        "target count and do not alter event semantics to satisfy structure. Deterministic "
        "code unwraps the ledger into the frozen canonical transport and never prunes, "
        "support-filters, deduplicates, relabels, or repairs semantics.\n\n"
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
        raise CanonicalV31EpisodeBatchError("ledger parent request cannot be reconstructed")
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
    row = schema["properties"]["s"]["items"]["properties"]["2"]["items"]
    if row.get("required") != ["0", "1"] or set(row.get("properties", {})) != {"0", "1"}:
        raise CanonicalV31EpisodeBatchError("ledger parent unit schema drifted")
    events = copy.deepcopy(row["properties"]["0"])
    concepts = copy.deepcopy(row["properties"]["1"])
    ledger_item = {
        "type": "object",
        "additionalProperties": False,
        "required": ["0", "1"],
        "properties": {
            "0": {"type": "string", "minLength": 8, "maxLength": 96},
            "1": copy.deepcopy(events["items"]),
        },
    }
    row["required"] = ["0", "1"]
    row["properties"] = {
        "0": {"type": "array", "items": ledger_item},
        "1": concepts,
    }
    schema["$id"] = RAW_OUTPUT_SCHEMA_VERSION
    return schema


def _ledger_request(request: Mapping[str, Any]) -> dict[str, Any]:
    value = copy.deepcopy(dict(request))
    value["schema_version"] = ADAPTER_SCHEMA_VERSION
    value["candidate_system_id"] = CANDIDATE_SYSTEM_ID
    value["canonical_schema_strategy"] = CANONICAL_SCHEMA_STRATEGY
    value["effort"] = EFFORT
    value["batch_id"] = "cv31pel_" + _sha256_text(
        _canonical_json(
            {
                "parent_batch_id": request["batch_id"],
                "candidate_system_id": CANDIDATE_SYSTEM_ID,
                "effort": EFFORT,
                "ledger_protocol_version": LEDGER_PROTOCOL_VERSION,
            }
        )
    )[:24]
    value["semantic_integrity"] = semantic_integrity_contract()
    value["private_input"]["schema_version"] = ADAPTER_SCHEMA_VERSION
    value["base_instructions"] = _ledger_instructions() + value["base_instructions"]
    value["base_instructions_sha256"] = _sha256_text(value["base_instructions"])
    value["output_schema"] = _output_schema(request)
    value["output_schema_sha256"] = _sha256_text(_canonical_json(value["output_schema"]))
    _PARENT_REQUEST_CACHE[_parent_cache_key(value)] = _canonical_json(request)
    return value


def prepare_episode_batches(
    episode: Mapping[str, Any], *, batch_size: int, thread_mode: str
) -> list[dict[str, Any]]:
    values = [
        _ledger_request(request)
        for request in parent.prepare_episode_batches(
            episode, batch_size=batch_size, thread_mode=thread_mode
        )
    ]
    for value in values:
        validate_prepared_request(value)
    return values


def validate_prepared_request(request: Mapping[str, Any]) -> dict[str, Any]:
    expected = _ledger_request(_parent_request(request))
    if dict(request) != expected:
        raise CanonicalV31EpisodeBatchError("nested proposition-event ledger request drifted")
    return copy.deepcopy(dict(request))


def _parent_wire_output(
    request: Mapping[str, Any], output: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    if not isinstance(output, Mapping) or set(output) != {"s"} or not isinstance(output["s"], list):
        raise CanonicalV31OutputError("nested ledger output shape drifted")
    if len(output["s"]) != len(request["private_input"]["segments"]):
        raise CanonicalV31OutputError("nested ledger segment count drifted")
    converted = copy.deepcopy(dict(output))
    manifest: list[dict[str, Any]] = []
    event_count = 0
    for segment_index, segment in enumerate(converted["s"]):
        unit_rows = segment.get("2") if isinstance(segment, Mapping) else None
        if not isinstance(unit_rows, list):
            raise CanonicalV31OutputError("nested ledger unit rows drifted")
        for unit_index, row in enumerate(unit_rows):
            if not isinstance(row, Mapping) or set(row) != {"0", "1"}:
                raise CanonicalV31OutputError("nested ledger unit row is malformed")
            ledger = row["0"]
            concepts = row["1"]
            if not isinstance(ledger, list) or not isinstance(concepts, list):
                raise CanonicalV31OutputError("nested ledger unit arrays are malformed")
            events: list[dict[str, Any]] = []
            for item_index, item in enumerate(ledger):
                if (
                    not isinstance(item, Mapping)
                    or set(item) != {"0", "1"}
                    or not isinstance(item["0"], str)
                    or not 8 <= len(item["0"]) <= 96
                    or not isinstance(item["1"], Mapping)
                ):
                    raise CanonicalV31OutputError("nested ledger item is malformed")
                events.append(copy.deepcopy(dict(item["1"])))
                manifest.append(
                    {
                        "segment_index": segment_index,
                        "unit_index": unit_index,
                        "item_index": item_index,
                        "summary_sha256": _sha256_text(item["0"]),
                    }
                )
                event_count += 1
            segment["2"][unit_index] = {
                "0": events,
                "1": copy.deepcopy(concepts),
            }
    return converted, {
        "ledger_item_count": event_count,
        "ledger_event_count": event_count,
        "ledger_manifest_sha256": _sha256_text(_canonical_json(manifest)),
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
            "ledger_protocol_version": LEDGER_PROTOCOL_VERSION,
            "ledger_manifest_sha256": diagnostics["ledger_manifest_sha256"],
        }
    )
    fidelity = copy.deepcopy(projected["fidelity"])
    fidelity.update(
        {
            "schema_version": FIDELITY_SCHEMA_VERSION,
            "candidate_system_id": CANDIDATE_SYSTEM_ID,
            "reasoning_effort": EFFORT,
            "ledger_protocol_version": LEDGER_PROTOCOL_VERSION,
            **diagnostics,
            "deterministic_ledger_unwrap_only": True,
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
    for segment_index, segment in enumerate(value["s"]):
        for unit_index, row in enumerate(segment["2"]):
            events = row["0"]
            concepts = row["1"]
            ledger = [
                {
                    "0": f"Fixture proposition {segment_index}-{unit_index}-{index}",
                    "1": copy.deepcopy(event),
                }
                for index, event in enumerate(events)
            ]
            segment["2"][unit_index] = {"0": ledger, "1": concepts}
    return value


def validate_turn_sidecar(
    request: Mapping[str, Any],
    sidecar_path: Path,
    *,
    output_path: Path,
    expected_thread: Any,
) -> dict[str, Any]:
    validated = validate_prepared_request(request)
    sidecar = base._load_json(sidecar_path, "nested ledger sidecar")  # noqa: SLF001
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
        raise CanonicalV31TelemetryError("managed-auth nested ledger sidecar contract failed")
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
        raise CanonicalV31TelemetryError("sidecar-bound nested ledger output is absent or changed")
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
        "schema_version": "pif_canonical_v31_nested_proposition_event_ledger_binding_v1",
        "parent_binding": parent.build_six_arm_matrix_binding(),
        "parent_adapter_module_sha256": hashlib.sha256(Path(parent.__file__).read_bytes()).hexdigest(),
        "adapter_module_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "candidate_system_id": CANDIDATE_SYSTEM_ID,
        "canonical_schema_strategy": CANONICAL_SCHEMA_STRATEGY,
        "ledger_protocol_version": LEDGER_PROTOCOL_VERSION,
        "model": MODEL,
        "effort": EFFORT,
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
    "LEDGER_PROTOCOL_VERSION",
    "build_six_arm_matrix_binding",
    "encode_parent_output_for_test",
    "prepare_episode_batches",
    "validate_and_project_output",
    "validate_prepared_request",
    "validate_turn_sidecar",
)
