from __future__ import annotations

"""Inventory-first compact canonical events with evidence-relative metric ordinals."""

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

ADAPTER_SCHEMA_VERSION = "pif_canonical_v31_compact_inventory_evidence_ordinal_episode_batch_v1"
RAW_OUTPUT_SCHEMA_VERSION = "pif_canonical_v31_compact_inventory_evidence_ordinal_raw_output_v1"
PROJECTION_SCHEMA_VERSION = "pif_canonical_v31_compact_inventory_evidence_ordinal_projection_v1"
PROVENANCE_SCHEMA_VERSION = "pif_canonical_v31_compact_inventory_evidence_ordinal_provenance_v1"
FIDELITY_SCHEMA_VERSION = "pif_canonical_v31_compact_inventory_evidence_ordinal_fidelity_v1"
CANDIDATE_SYSTEM_ID = "pif_compact_inventory_evidence_ordinal_ai_discourse_v3_1_v1"
CANONICAL_SCHEMA_STRATEGY = "canonical_v31_compact_inventory_evidence_relative_metric_ordinal_v1"
INVENTORY_PROTOCOL_VERSION = "pif_compact_inventory_evidence_relative_metric_ordinal_v1"

EVENT_ENUM_FIELDS = (0, 13, 15, 16, 17)
EVENT_TEXT_FIELDS = (1, 14, 18, 19, 21, 25)
EVENT_TABLE_FIELDS = (2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 22, 23)
EVENT_INLINE_FIELDS = (20, 24, 26)
CONCEPT_TEXT_FIELDS = (0, 2)
CONCEPT_NUMBER_FIELDS = (3, 4)
CONCEPT_SURFACE_TABLE_INDEX = EVENT_TABLE_FIELDS.index(7)

EVENT_ENUM_NAMES = (
    "event_type",
    "stance",
    "claim_type",
    "certainty",
    "temporal_horizon",
)
EVENT_TEXT_NAMES = (
    "event_subtype",
    "claim_text",
    "causal_mechanism",
    "counterclaim",
    "signal_reason",
    "audit_notes",
)
EVENT_TABLE_NAMES = (
    "actor",
    "speaker_context",
    "reported_actor",
    "source_context",
    "target",
    "surface_terms",
    "frames",
    "model_names",
    "product_names",
    "organizations",
    "people",
    "exclusion_flags",
    "quality_flags",
)
CONCEPT_TEXT_NAMES = ("candidate", "rationale")
CONCEPT_NUMBER_NAMES = ("usefulness_score", "confidence")

_PARENT_REQUEST_CACHE: dict[tuple[str, int, str, tuple[str, ...]], str] = {}
_INVENTORY_REQUEST_CACHE: dict[tuple[str, int, str, tuple[str, ...]], str] = {}


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def semantic_integrity_contract() -> dict[str, Any]:
    value = copy.deepcopy(parent.semantic_integrity_contract())
    value.update(
        {
            "reasoning_effort": EFFORT,
            "inventory_protocol_version": INVENTORY_PROTOCOL_VERSION,
            "model_authors_ordered_source_unit_proposition_inventory": True,
            "one_inventory_item_per_canonical_event": True,
            "inventory_and_event_evidence_pointer_identity_required": True,
            "model_authors_evidence_relative_metric_token_ordinals": True,
            "model_authors_every_normalized_table_value": True,
            "model_authors_every_full_canonical_event_value": True,
            "deterministic_inventory_removal_only": True,
            "deterministic_evidence_relative_metric_ordinal_projection_only": True,
            "event_count_is_diagnostic_only": True,
            "table_duplicate_count_is_diagnostic_only": True,
            "deterministic_semantic_defaults": {},
            "deterministic_semantic_pruning": False,
            "deterministic_support_filtering": False,
            "deterministic_deduplication": False,
            "deterministic_relabeling": False,
        }
    )
    return value


def _inventory_instructions() -> str:
    return (
        "# Compact inventory-first evidence-relative canonical realization\n"
        "For each source unit, first author the complete ordered proposition inventory in "
        "unit field 0, then author compact full-canonical events in unit field 1 and concepts "
        "in unit field 2. Inventory every independent grounded eligible proposition exactly "
        "once. Keep adjacent recommendations, counterclaims, consequences, comparisons, "
        "constraints, and tradeoffs separate when each is independently asserted. For every "
        "inventory item, emit exactly one event in the same array position and use the same "
        "evidence-span integer in inventory field 1 and event field 5. Do not invent unsupported "
        "propositions, merge related propositions, or aim for a target count.\n"
        "The root is a closed object with field 0 holding normalized authored-value tables "
        "and field 1 holding segment rows in input order. Table fields 0..12 are, in order: "
        f"{_canonical_json(EVENT_TABLE_NAMES)}. Reuse an existing table index when the exact "
        "authored value repeats; table duplication is diagnostic only and never changes event "
        "acceptance. Each event is a closed object with fields 0..5. Field 0 is the five-value "
        f"enum vector {_canonical_json(EVENT_ENUM_NAMES)}. Field 1 is the six-value text vector "
        f"{_canonical_json(EVENT_TEXT_NAMES)}. Field 2 is the thirteen-integer table-reference "
        f"vector {_canonical_json(EVENT_TABLE_NAMES)}. Field 3 is the complete tagged metric "
        "bundle, field 4 is confidence, and field 5 is the evidence-span integer. Inside metric "
        "field 3, every non-null start/end pair is a zero-based inclusive token ordinal inside "
        "the event's selected evidence span, not a copied string ID. Each concept "
        "is a closed object with fields 0..3: field 0 is the two-value text vector "
        f"{_canonical_json(CONCEPT_TEXT_NAMES)}, field 1 references the shared surface_terms "
        f"table, field 2 is the two-value numeric vector {_canonical_json(CONCEPT_NUMBER_NAMES)}, "
        "and field 3 is the evidence-span integer. Every table value, vector value, metric, "
        "confidence, and evidence selection is model authored. The inventory summary is private "
        "fidelity evidence only. Deterministic code validates inventory/event cardinality and "
        "evidence identity, maps evidence-relative metric ordinals to exact frozen literal-token "
        "IDs, removes only the inventory layer, and expands table references. It never prunes, "
        "support-filters, deduplicates, relabels, defaults, repairs, or otherwise changes "
        "semantics. Return only the completed structured object.\n\n"
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
        raise CanonicalV31EpisodeBatchError("compact-table parent request cannot be reconstructed")
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


def _fixed_array(item: Mapping[str, Any], size: int) -> dict[str, Any]:
    return {
        "type": "array",
        "items": copy.deepcopy(dict(item)),
        "minItems": size,
        "maxItems": size,
    }


def _union_schema(schemas: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    encoded: dict[str, dict[str, Any]] = {}
    for schema in schemas:
        value = copy.deepcopy(dict(schema))
        encoded[_canonical_json(value)] = value
    values = list(encoded.values())
    if len(values) == 1:
        return values[0]
    return {"anyOf": values}


def _evidence_relative_metric_schema(value: Mapping[str, Any]) -> dict[str, Any]:
    schema = copy.deepcopy(dict(value))
    try:
        metric = schema["properties"]["0"]["items"]
        properties = metric["properties"]
    except (KeyError, TypeError) as exc:
        raise CanonicalV31EpisodeBatchError("inventory metric schema drifted") from exc
    if metric.get("required") != ["0", "1", "2", "3", "4"]:
        raise CanonicalV31EpisodeBatchError("inventory metric layout drifted")
    for key in ("1", "2", "3", "4"):
        pair = properties.get(key)
        if not isinstance(pair, Mapping) or set(pair.get("properties", {})) != {"0", "1"}:
            raise CanonicalV31EpisodeBatchError("inventory metric token pair drifted")
        pair["properties"] = {
            "0": {"type": "integer", "minimum": 0},
            "1": {"type": "integer", "minimum": 0},
        }
    return schema


def _output_schema(request: Mapping[str, Any]) -> dict[str, Any]:
    parent_schema = request["output_schema"]
    segment_schema = copy.deepcopy(parent_schema["properties"]["s"]["items"])
    unit_schema = segment_schema["properties"]["2"]["items"]
    if unit_schema.get("required") != ["0", "1"] or set(unit_schema.get("properties", {})) != {
        "0",
        "1",
    }:
        raise CanonicalV31EpisodeBatchError("compact-table parent unit schema drifted")
    event_schema = copy.deepcopy(unit_schema["properties"]["0"]["items"])
    concept_schema = copy.deepcopy(unit_schema["properties"]["1"]["items"])
    event_properties = event_schema.get("properties")
    concept_properties = concept_schema.get("properties")
    if (
        event_schema.get("required") != [str(index) for index in range(27)]
        or not isinstance(event_properties, Mapping)
        or concept_schema.get("required") != [str(index) for index in range(6)]
        or not isinstance(concept_properties, Mapping)
    ):
        raise CanonicalV31EpisodeBatchError("compact-table parent canonical layout drifted")

    tables = {
        str(table_index): {
            "type": "array",
            "items": copy.deepcopy(event_properties[str(field_index)]),
        }
        for table_index, field_index in enumerate(EVENT_TABLE_FIELDS)
    }
    compact_event = {
        "type": "object",
        "additionalProperties": False,
        "required": ["0", "1", "2", "3", "4", "5"],
        "properties": {
            "0": _fixed_array(
                _union_schema([event_properties[str(index)] for index in EVENT_ENUM_FIELDS]),
                len(EVENT_ENUM_FIELDS),
            ),
            "1": _fixed_array(
                _union_schema([event_properties[str(index)] for index in EVENT_TEXT_FIELDS]),
                len(EVENT_TEXT_FIELDS),
            ),
            "2": _fixed_array({"type": "integer", "minimum": 0}, len(EVENT_TABLE_FIELDS)),
            "3": _evidence_relative_metric_schema(
                event_properties[str(EVENT_INLINE_FIELDS[0])]
            ),
            "4": copy.deepcopy(event_properties[str(EVENT_INLINE_FIELDS[1])]),
            "5": copy.deepcopy(event_properties[str(EVENT_INLINE_FIELDS[2])]),
        },
    }
    compact_concept = {
        "type": "object",
        "additionalProperties": False,
        "required": ["0", "1", "2", "3"],
        "properties": {
            "0": _fixed_array(
                _union_schema([concept_properties[str(index)] for index in CONCEPT_TEXT_FIELDS]),
                len(CONCEPT_TEXT_FIELDS),
            ),
            "1": {"type": "integer", "minimum": 0},
            "2": _fixed_array(
                _union_schema([concept_properties[str(index)] for index in CONCEPT_NUMBER_FIELDS]),
                len(CONCEPT_NUMBER_FIELDS),
            ),
            "3": copy.deepcopy(concept_properties["5"]),
        },
    }
    inventory_item = {
        "type": "object",
        "additionalProperties": False,
        "required": ["0", "1"],
        "properties": {
            "0": {"type": "string", "minLength": 12, "maxLength": 240},
            "1": copy.deepcopy(compact_event["properties"]["5"]),
        },
    }
    unit_schema["required"] = ["0", "1", "2"]
    unit_schema["properties"] = {
        "0": {"type": "array", "items": inventory_item},
        "1": {"type": "array", "items": compact_event},
        "2": {"type": "array", "items": compact_concept},
    }
    segments_schema = copy.deepcopy(parent_schema["properties"]["s"])
    segments_schema["items"] = segment_schema
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": RAW_OUTPUT_SCHEMA_VERSION,
        "type": "object",
        "additionalProperties": False,
        "required": ["0", "1"],
        "properties": {
            "0": {
                "type": "object",
                "additionalProperties": False,
                "required": list(tables),
                "properties": tables,
            },
            "1": segments_schema,
        },
    }


def _ordinal_instruction(request: Mapping[str, Any]) -> str:
    start_marker = "# Unit-owned ordinal full-schema contract\n"
    end_marker = "# Single-message structured-output contract\n"
    instructions = str(request["base_instructions"])
    if instructions.count(start_marker) != 1 or instructions.count(end_marker) != 1:
        raise CanonicalV31EpisodeBatchError("compact-table ordinal instruction markers drifted")
    start = instructions.index(start_marker)
    end = instructions.index(end_marker, start)
    return instructions[start:end]


def _inventory_request(request: Mapping[str, Any]) -> dict[str, Any]:
    value = copy.deepcopy(dict(request))
    old_instruction = _ordinal_instruction(request)
    if value["base_instructions"].count(old_instruction) != 1:
        raise CanonicalV31EpisodeBatchError("inventory ordinal instruction boundary drifted")
    value["schema_version"] = ADAPTER_SCHEMA_VERSION
    value["candidate_system_id"] = CANDIDATE_SYSTEM_ID
    value["canonical_schema_strategy"] = CANONICAL_SCHEMA_STRATEGY
    value["effort"] = EFFORT
    value["batch_id"] = "cv31cieo_" + _sha256_text(
        _canonical_json(
            {
                "parent_batch_id": request["batch_id"],
                "candidate_system_id": CANDIDATE_SYSTEM_ID,
                "effort": EFFORT,
                "inventory_protocol_version": INVENTORY_PROTOCOL_VERSION,
            }
        )
    )[:24]
    value["semantic_integrity"] = semantic_integrity_contract()
    value["private_input"]["schema_version"] = ADAPTER_SCHEMA_VERSION
    value["base_instructions"] = value["base_instructions"].replace(
        old_instruction, _inventory_instructions(), 1
    )
    value["base_instructions_sha256"] = _sha256_text(value["base_instructions"])
    value["output_schema"] = _output_schema(request)
    value["output_schema_sha256"] = _sha256_text(_canonical_json(value["output_schema"]))
    cache_key = _parent_cache_key(value)
    _PARENT_REQUEST_CACHE[cache_key] = _canonical_json(request)
    _INVENTORY_REQUEST_CACHE[cache_key] = _canonical_json(value)
    return value


def prepare_episode_batches(
    episode: Mapping[str, Any], *, batch_size: int, thread_mode: str
) -> list[dict[str, Any]]:
    values = [
        _inventory_request(request)
        for request in parent.prepare_episode_batches(
            episode, batch_size=batch_size, thread_mode=thread_mode
        )
    ]
    for value in values:
        validate_prepared_request(value)
    return values


def validate_prepared_request(request: Mapping[str, Any]) -> dict[str, Any]:
    cache_key = _parent_cache_key(request)
    encoded = _INVENTORY_REQUEST_CACHE.get(cache_key)
    if encoded is None:
        encoded = _canonical_json(_inventory_request(_parent_request(request)))
    if _canonical_json(request) != encoded:
        raise CanonicalV31EpisodeBatchError("compact inventory ordinal request drifted")
    return copy.deepcopy(dict(request))


def _table_value(tables: Sequence[Sequence[Any]], table_index: int, reference: Any) -> Any:
    if isinstance(reference, bool) or not isinstance(reference, int) or reference < 0:
        raise CanonicalV31OutputError("compact-table reference is malformed")
    try:
        return copy.deepcopy(tables[table_index][reference])
    except IndexError as exc:
        raise CanonicalV31OutputError("compact-table reference is out of range") from exc


def _evidence_tokens(source: Mapping[str, Any], pointer: Any) -> list[Mapping[str, Any]]:
    spans = source.get("evidence_spans")
    units = source.get("units")
    if (
        isinstance(pointer, bool)
        or not isinstance(pointer, int)
        or not isinstance(spans, list)
        or not 0 <= pointer < len(spans)
        or not isinstance(units, list)
    ):
        raise CanonicalV31OutputError("inventory event evidence pointer is invalid")
    span = spans[pointer]
    tokens = sorted(
        (
            token
            for unit in units
            for token in unit.get("literal_tokens", [])
            if token["start_char"] >= span["start_char"]
            and token["end_char"] <= span["end_char"]
        ),
        key=lambda token: (token["start_char"], token["end_char"], token["literal_token_id"]),
    )
    if not tokens:
        raise CanonicalV31OutputError("inventory evidence span has no literal tokens")
    return tokens


def _metric_token_ids(value: Any, tokens: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != {"0"} or not isinstance(value["0"], list):
        raise CanonicalV31OutputError("inventory compact metric bundle is malformed")
    converted = copy.deepcopy(dict(value))
    for metric in converted["0"]:
        if not isinstance(metric, Mapping) or set(metric) != {"0", "1", "2", "3", "4"}:
            raise CanonicalV31OutputError("inventory compact metric row is malformed")
        for key in ("1", "2", "3", "4"):
            pair = metric[key]
            if pair is None:
                continue
            if not isinstance(pair, Mapping) or set(pair) != {"0", "1"}:
                raise CanonicalV31OutputError("inventory metric ordinal pair is malformed")
            start = pair["0"]
            end = pair["1"]
            if (
                isinstance(start, bool)
                or not isinstance(start, int)
                or isinstance(end, bool)
                or not isinstance(end, int)
                or start < 0
                or end < start
                or end >= len(tokens)
            ):
                raise CanonicalV31OutputError("inventory metric ordinal range is invalid")
            metric[key] = {
                "0": tokens[start]["literal_token_id"],
                "1": tokens[end]["literal_token_id"],
            }
    return converted


def _expand_event(
    tables: Sequence[Sequence[Any]], value: Any, source: Mapping[str, Any]
) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != {"0", "1", "2", "3", "4", "5"}:
        raise CanonicalV31OutputError("compact event row is malformed")
    enums, texts, references = value["0"], value["1"], value["2"]
    if (
        not isinstance(enums, list)
        or len(enums) != len(EVENT_ENUM_FIELDS)
        or not isinstance(texts, list)
        or len(texts) != len(EVENT_TEXT_FIELDS)
        or not isinstance(references, list)
        or len(references) != len(EVENT_TABLE_FIELDS)
    ):
        raise CanonicalV31OutputError("compact event vector cardinality drifted")
    event: dict[str, Any] = {}
    for field, item in zip(EVENT_ENUM_FIELDS, enums):
        event[str(field)] = copy.deepcopy(item)
    for field, item in zip(EVENT_TEXT_FIELDS, texts):
        event[str(field)] = copy.deepcopy(item)
    for table_index, (field, reference) in enumerate(zip(EVENT_TABLE_FIELDS, references)):
        event[str(field)] = _table_value(tables, table_index, reference)
    event[str(EVENT_INLINE_FIELDS[0])] = _metric_token_ids(
        value["3"], _evidence_tokens(source, value["5"])
    )
    event[str(EVENT_INLINE_FIELDS[1])] = copy.deepcopy(value["4"])
    event[str(EVENT_INLINE_FIELDS[2])] = copy.deepcopy(value["5"])
    if set(event) != {str(index) for index in range(27)}:
        raise CanonicalV31OutputError("compact event expansion lost canonical fields")
    return event


def _expand_concept(tables: Sequence[Sequence[Any]], value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != {"0", "1", "2", "3"}:
        raise CanonicalV31OutputError("compact concept row is malformed")
    texts, numbers = value["0"], value["2"]
    if (
        not isinstance(texts, list)
        or len(texts) != len(CONCEPT_TEXT_FIELDS)
        or not isinstance(numbers, list)
        or len(numbers) != len(CONCEPT_NUMBER_FIELDS)
    ):
        raise CanonicalV31OutputError("compact concept vector cardinality drifted")
    return {
        "0": copy.deepcopy(texts[0]),
        "1": _table_value(tables, CONCEPT_SURFACE_TABLE_INDEX, value["1"]),
        "2": copy.deepcopy(texts[1]),
        "3": copy.deepcopy(numbers[0]),
        "4": copy.deepcopy(numbers[1]),
        "5": copy.deepcopy(value["3"]),
    }


def _parent_wire_output(
    request: Mapping[str, Any], output: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    if not isinstance(output, Mapping) or set(output) != {"0", "1"}:
        raise CanonicalV31OutputError("compact-table output shape drifted")
    table_object, segments = output["0"], output["1"]
    if (
        not isinstance(table_object, Mapping)
        or set(table_object) != {str(index) for index in range(len(EVENT_TABLE_FIELDS))}
        or not isinstance(segments, list)
        or len(segments) != len(request["private_input"]["segments"])
    ):
        raise CanonicalV31OutputError("compact-table root cardinality drifted")
    tables = [table_object[str(index)] for index in range(len(EVENT_TABLE_FIELDS))]
    if any(not isinstance(table, list) for table in tables):
        raise CanonicalV31OutputError("compact-table authored tables are malformed")
    converted = {"s": copy.deepcopy(segments)}
    event_count = 0
    concept_count = 0
    reference_count = 0
    inventory_manifest: list[dict[str, Any]] = []
    inventory_count = 0
    metric_ordinal_pair_count = 0
    for segment_index, (segment, source) in enumerate(
        zip(converted["s"], request["private_input"]["segments"])
    ):
        unit_rows = segment.get("2") if isinstance(segment, Mapping) else None
        if not isinstance(unit_rows, list):
            raise CanonicalV31OutputError("compact-table unit rows drifted")
        for unit_index, unit_row in enumerate(unit_rows):
            if not isinstance(unit_row, Mapping) or set(unit_row) != {"0", "1", "2"}:
                raise CanonicalV31OutputError("compact inventory unit row is malformed")
            inventory = unit_row["0"]
            raw_events = unit_row["1"]
            raw_concepts = unit_row["2"]
            if not all(isinstance(value, list) for value in (inventory, raw_events, raw_concepts)):
                raise CanonicalV31OutputError("compact inventory unit collections are malformed")
            if len(inventory) != len(raw_events):
                raise CanonicalV31OutputError("inventory/event cardinality drifted")
            for item_index, (item, event) in enumerate(zip(inventory, raw_events)):
                if (
                    not isinstance(item, Mapping)
                    or set(item) != {"0", "1"}
                    or not isinstance(item["0"], str)
                    or not 12 <= len(item["0"]) <= 240
                    or not isinstance(event, Mapping)
                    or event.get("5") != item["1"]
                ):
                    raise CanonicalV31OutputError("inventory/event evidence identity drifted")
                inventory_manifest.append(
                    {
                        "segment_index": segment_index,
                        "unit_index": unit_index,
                        "item_index": item_index,
                        "summary_sha256": _sha256_text(item["0"]),
                        "evidence_pointer": copy.deepcopy(item["1"]),
                    }
                )
                metric = event.get("3")
                if isinstance(metric, Mapping) and isinstance(metric.get("0"), list):
                    metric_ordinal_pair_count += sum(
                        pair is not None
                        for row in metric["0"]
                        if isinstance(row, Mapping)
                        for pair in (row.get("1"), row.get("2"), row.get("3"), row.get("4"))
                    )
                inventory_count += 1
            events = [_expand_event(tables, event, source) for event in raw_events]
            concepts = [_expand_concept(tables, concept) for concept in raw_concepts]
            event_count += len(events)
            concept_count += len(concepts)
            reference_count += len(events) * len(EVENT_TABLE_FIELDS) + len(concepts)
            unit_row["0"] = events
            unit_row["1"] = concepts
            del unit_row["2"]
    duplicate_count = sum(
        len(table) - len({_canonical_json(value) for value in table}) for table in tables
    )
    return converted, {
        "compact_event_count": event_count,
        "compact_concept_count": concept_count,
        "compact_table_reference_count": reference_count,
        "compact_table_cardinalities": [len(table) for table in tables],
        "compact_table_duplicate_count": duplicate_count,
        "compact_table_manifest_sha256": _sha256_text(_canonical_json(table_object)),
        "inventory_item_count": inventory_count,
        "inventory_event_count": event_count,
        "inventory_evidence_identity_count": inventory_count,
        "inventory_metric_ordinal_pair_count": metric_ordinal_pair_count,
        "inventory_manifest_sha256": _sha256_text(_canonical_json(inventory_manifest)),
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
            "inventory_protocol_version": INVENTORY_PROTOCOL_VERSION,
            "compact_table_manifest_sha256": diagnostics["compact_table_manifest_sha256"],
            "inventory_manifest_sha256": diagnostics["inventory_manifest_sha256"],
        }
    )
    fidelity = copy.deepcopy(projected["fidelity"])
    fidelity.update(
        {
            "schema_version": FIDELITY_SCHEMA_VERSION,
            "candidate_system_id": CANDIDATE_SYSTEM_ID,
            "reasoning_effort": EFFORT,
            "inventory_protocol_version": INVENTORY_PROTOCOL_VERSION,
            **diagnostics,
            "event_count_is_diagnostic_only": True,
            "table_duplicate_count_is_diagnostic_only": True,
            "deterministic_inventory_removal_only": True,
            "deterministic_evidence_relative_metric_ordinal_projection_only": True,
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


def _intern(table: list[Any], indexes: dict[str, int], value: Any) -> int:
    key = _canonical_json(value)
    index = indexes.get(key)
    if index is None:
        index = len(table)
        indexes[key] = index
        table.append(copy.deepcopy(value))
    return index


def _metric_ordinals_for_test(
    value: Mapping[str, Any], source: Mapping[str, Any], evidence_pointer: int
) -> dict[str, Any]:
    tokens = _evidence_tokens(source, evidence_pointer)
    lookup = {token["literal_token_id"]: index for index, token in enumerate(tokens)}
    converted = copy.deepcopy(dict(value))
    for metric in converted.get("0", []):
        for key in ("1", "2", "3", "4"):
            pair = metric[key]
            if pair is None:
                continue
            try:
                metric[key] = {"0": lookup[pair["0"]], "1": lookup[pair["1"]]}
            except (KeyError, TypeError) as exc:
                raise CanonicalV31OutputError(
                    "fixture metric token is outside selected evidence"
                ) from exc
    return converted


def encode_parent_output_for_test(
    request: Mapping[str, Any], parent_output: Mapping[str, Any]
) -> dict[str, Any]:
    validate_prepared_request(request)
    tables: list[list[Any]] = [[] for _ in EVENT_TABLE_FIELDS]
    indexes: list[dict[str, int]] = [{} for _ in EVENT_TABLE_FIELDS]
    segments = copy.deepcopy(dict(parent_output))["s"]
    for segment_index, segment in enumerate(segments):
        source = request["private_input"]["segments"][segment_index]
        for unit_index, unit_row in enumerate(segment["2"]):
            compact_events = []
            for event in unit_row["0"]:
                evidence_pointer = copy.deepcopy(event[str(EVENT_INLINE_FIELDS[2])])
                compact_events.append(
                    {
                        "0": [copy.deepcopy(event[str(field)]) for field in EVENT_ENUM_FIELDS],
                        "1": [copy.deepcopy(event[str(field)]) for field in EVENT_TEXT_FIELDS],
                        "2": [
                            _intern(tables[index], indexes[index], event[str(field)])
                            for index, field in enumerate(EVENT_TABLE_FIELDS)
                        ],
                        "3": _metric_ordinals_for_test(
                            event[str(EVENT_INLINE_FIELDS[0])],
                            source,
                            evidence_pointer,
                        ),
                        "4": copy.deepcopy(event[str(EVENT_INLINE_FIELDS[1])]),
                        "5": evidence_pointer,
                    }
                )
            compact_concepts = []
            for concept in unit_row["1"]:
                compact_concepts.append(
                    {
                        "0": [copy.deepcopy(concept["0"]), copy.deepcopy(concept["2"])],
                        "1": _intern(
                            tables[CONCEPT_SURFACE_TABLE_INDEX],
                            indexes[CONCEPT_SURFACE_TABLE_INDEX],
                            concept["1"],
                        ),
                        "2": [copy.deepcopy(concept["3"]), copy.deepcopy(concept["4"])],
                        "3": copy.deepcopy(concept["5"]),
                    }
                )
            inventory = [
                {
                    "0": (
                        f"Fixture proposition {segment_index}-{unit_index}-{event_index} "
                        "for exact inventory structural round trip"
                    ),
                    "1": copy.deepcopy(event["5"]),
                }
                for event_index, event in enumerate(compact_events)
            ]
            unit_row["0"] = inventory
            unit_row["1"] = compact_events
            unit_row["2"] = compact_concepts
    return {
        "0": {str(index): table for index, table in enumerate(tables)},
        "1": segments,
    }


def validate_turn_sidecar(
    request: Mapping[str, Any],
    sidecar_path: Path,
    *,
    output_path: Path,
    expected_thread: Any,
) -> dict[str, Any]:
    validated = validate_prepared_request(request)
    sidecar = base._load_json(sidecar_path, "compact inventory sidecar")  # noqa: SLF001
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
        raise CanonicalV31TelemetryError("managed-auth compact inventory sidecar contract failed")
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
        raise CanonicalV31TelemetryError(
            "sidecar-bound compact inventory output is absent or changed"
        )
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
        "schema_version": "pif_canonical_v31_compact_inventory_evidence_ordinal_binding_v1",
        "parent_binding": parent.build_six_arm_matrix_binding(),
        "parent_adapter_module_sha256": hashlib.sha256(Path(parent.__file__).read_bytes()).hexdigest(),
        "adapter_module_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "candidate_system_id": CANDIDATE_SYSTEM_ID,
        "canonical_schema_strategy": CANONICAL_SCHEMA_STRATEGY,
        "inventory_protocol_version": INVENTORY_PROTOCOL_VERSION,
        "model": MODEL,
        "effort": EFFORT,
        "deterministic_inventory_removal_only": True,
        "deterministic_evidence_relative_metric_ordinal_projection_only": True,
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
