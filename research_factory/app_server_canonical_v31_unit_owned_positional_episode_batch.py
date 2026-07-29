from __future__ import annotations

"""Full canonical v3.1 semantics in a unit-owned positional wire format.

The model authors every canonical semantic value.  Deterministic code only
decodes fixed field order, maps exact source/evidence IDs from integer pointers,
derives coverage receipts from array ownership, and invokes the frozen canonical
validator/projection.
"""

import copy
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_canonical_v31_single_message_compact_pointer_episode_batch as parent


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

ADAPTER_SCHEMA_VERSION = "pif_canonical_v31_unit_owned_positional_episode_batch_v1"
RAW_OUTPUT_SCHEMA_VERSION = "pif_canonical_v31_unit_owned_positional_raw_output_v1"
PROJECTION_SCHEMA_VERSION = "pif_canonical_v31_unit_owned_positional_projection_v1"
PROVENANCE_SCHEMA_VERSION = "pif_canonical_v31_unit_owned_positional_provenance_v1"
FIDELITY_SCHEMA_VERSION = "pif_canonical_v31_unit_owned_positional_fidelity_v1"
CANDIDATE_SYSTEM_ID = "pif_unit_owned_positional_ai_discourse_v3_1_v1"
CANONICAL_SCHEMA_STRATEGY = "canonical_v31_unit_owned_positional_full_schema_v1"
POSITIONAL_PROTOCOL_VERSION = "pif_unit_owned_positional_full_schema_v1"
PROMPT_PREFIX = "# Canonical v3.1 unit-owned positional packet\n"

SEGMENT_FIELD_ORDER = (
    "extraction_status",
    "segment_source_context",
    "unit_rows",
    "rejected_candidates",
    "no_signal_reason",
    "overall_confidence",
    "needs_review",
    "review_reason",
)
UNIT_ROW_FIELD_ORDER = ("discourse_events", "concept_candidates")


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _object_layout(schema: Mapping[str, Any]) -> Any:
    required = schema.get("required")
    properties = schema.get("properties")
    if not isinstance(required, list) or not isinstance(properties, Mapping):
        raise CanonicalV31EpisodeBatchError("canonical object layout is malformed")
    result = []
    for field in required:
        child = properties.get(field)
        if not isinstance(child, Mapping):
            raise CanonicalV31EpisodeBatchError("canonical field layout is malformed")
        child_type = child.get("type")
        nonnull = [item for item in child_type if item != "null"] if isinstance(child_type, list) else [child_type]
        if nonnull == ["object"]:
            result.append([field, _object_layout(child)])
        else:
            result.append(field)
    return result


def _field_layout(parent_schema: Mapping[str, Any]) -> dict[str, Any]:
    segment = parent_schema["properties"]["segments"]["items"]
    event = segment["properties"]["discourse_events"]["items"]
    concept = segment["properties"]["concept_candidates"]["items"]
    rejected = segment["properties"]["rejected_candidates"]["items"]
    return {
        "root": {"s": "segments_in_input_order"},
        "segment_row": list(SEGMENT_FIELD_ORDER),
        "unit_row": list(UNIT_ROW_FIELD_ORDER),
        "discourse_event_row": _object_layout(event),
        "concept_candidate_row": _object_layout(concept),
        "rejected_candidate_row": _object_layout(rejected),
        "pointer_rule": (
            "evidence_span_id_position_is_an_integer_evidence_span_index; "
            "the selected span must start in the owning unit row"
        ),
    }


def _positional_instructions(parent_schema: Mapping[str, Any]) -> str:
    layout = json.dumps(_field_layout(parent_schema), ensure_ascii=True, separators=(",", ":"))
    return (
        "# Unit-owned positional full-schema contract\n"
        "Return the compact positional wire schema supplied for this turn. The top-level "
        "key s contains one segment row per input segment. Each segment row contains one "
        "unit row per source unit in exact input order. Place every independent grounded "
        "proposition in the discourse-event array owned by the source unit where its "
        "smallest supporting evidence span begins. Do not collapse distinct propositions, "
        "and do not aim for a target count. Every canonical semantic field remains model "
        "authored; arrays encode only fixed field order. Evidence-span integers index the "
        "segment evidence_spans array. Return one final JSON object only.\n"
        f"Exact positional field layout: {layout}\n\n"
    )


def semantic_integrity_contract() -> dict[str, Any]:
    value = copy.deepcopy(parent.semantic_integrity_contract())
    value.update(
        {
            "positional_protocol_version": POSITIONAL_PROTOCOL_VERSION,
            "model_authors_all_canonical_semantic_fields": True,
            "unit_owned_event_and_concept_arrays": True,
            "deterministic_fixed_field_order_decode_only": True,
            "deterministic_exact_id_pointer_projection_only": True,
            "deterministic_coverage_receipt_derivation_only": True,
            "deterministic_semantic_pruning": False,
            "deterministic_support_filtering": False,
            "deterministic_deduplication": False,
            "deterministic_relabeling": False,
            "semantic_regex_or_keyword_rules": False,
        }
    )
    return value


def _schema_type(schema: Mapping[str, Any]) -> tuple[str | None, bool]:
    value = schema.get("type")
    if isinstance(value, list):
        nullable = "null" in value
        nonnull = [item for item in value if item != "null"]
        return (nonnull[0] if len(nonnull) == 1 else None), nullable
    return value if isinstance(value, str) else None, False


def _positional_schema_for_value(
    schema: Mapping[str, Any], *, evidence_span_max_index: int | None = None
) -> dict[str, Any]:
    value_type, nullable = _schema_type(schema)
    if value_type == "object":
        required = schema.get("required")
        properties = schema.get("properties")
        if not isinstance(required, list) or not isinstance(properties, Mapping):
            raise CanonicalV31EpisodeBatchError("canonical object schema is malformed")
        items = []
        for field in required:
            child = properties[field]
            if field == "evidence_span_id" and evidence_span_max_index is not None:
                items.append(
                    {
                        "type": "integer",
                        "minimum": 0,
                        "maximum": evidence_span_max_index,
                    }
                )
            else:
                items.append(
                    _positional_schema_for_value(
                        child, evidence_span_max_index=evidence_span_max_index
                    )
                )
        return {
            "type": ["array", "null"] if nullable else "array",
            "prefixItems": items,
            "items": False,
            "minItems": len(items),
            "maxItems": len(items),
        }
    if value_type == "array":
        result = {
            key: copy.deepcopy(value)
            for key, value in schema.items()
            if key not in {"items", "type"}
        }
        result["type"] = ["array", "null"] if nullable else "array"
        result["items"] = _positional_schema_for_value(
            schema["items"], evidence_span_max_index=evidence_span_max_index
        )
        return result
    return copy.deepcopy(dict(schema))


def _fixed_tuple_schema(items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "type": "array",
        "prefixItems": [copy.deepcopy(dict(item)) for item in items],
        "items": False,
        "minItems": len(items),
        "maxItems": len(items),
    }


def _output_schema(request: Mapping[str, Any], parent_schema: Mapping[str, Any]) -> dict[str, Any]:
    parent_segment = parent_schema["properties"]["segments"]["items"]
    event_schema = parent_segment["properties"]["discourse_events"]["items"]
    concept_schema = parent_segment["properties"]["concept_candidates"]["items"]
    rejected_schema = parent_segment["properties"]["rejected_candidates"]["items"]
    segment_schemas = []
    for segment in request["private_input"]["segments"]:
        span_max = len(segment["evidence_spans"]) - 1
        event = _positional_schema_for_value(
            event_schema, evidence_span_max_index=span_max
        )
        concept = _positional_schema_for_value(
            concept_schema, evidence_span_max_index=span_max
        )
        rejected = _positional_schema_for_value(rejected_schema)
        unit_row = _fixed_tuple_schema(
            (
                {"type": "array", "items": event},
                {"type": "array", "items": concept},
            )
        )
        unit_rows = {
            "type": "array",
            "items": unit_row,
            "minItems": len(segment["units"]),
            "maxItems": len(segment["units"]),
        }
        segment_schemas.append(
            _fixed_tuple_schema(
                (
                    parent_segment["properties"]["extraction_status"],
                    parent_segment["properties"]["segment_source_context"],
                    unit_rows,
                    {"type": "array", "items": rejected},
                    parent_segment["properties"]["no_signal_reason"],
                    parent_segment["properties"]["overall_confidence"],
                    parent_segment["properties"]["needs_review"],
                    parent_segment["properties"]["review_reason"],
                )
            )
        )
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": RAW_OUTPUT_SCHEMA_VERSION,
        "type": "object",
        "additionalProperties": False,
        "required": ["s"],
        "properties": {
            "s": {
                "type": "array",
                "prefixItems": segment_schemas,
                "items": False,
                "minItems": len(segment_schemas),
                "maxItems": len(segment_schemas),
            }
        },
    }


def _compact_prompt(parent_prompt: str) -> str:
    old_prefix = "# Canonical v3.1 compact unit-pointer packet\n"
    if not parent_prompt.startswith(old_prefix) or not parent_prompt.endswith("\n"):
        raise CanonicalV31EpisodeBatchError("parent compact prompt envelope drifted")
    packet = json.loads(parent_prompt[len(old_prefix) : -1])
    for segment in packet.get("segments", []):
        unit_index_by_id = {
            unit["unit_id"]: index
            for index, unit in enumerate(segment.get("source_units", []))
        }
        segment["source_units"] = [
            {
                "source_unit_index": index,
                "text": unit["text"],
                "literal_token_count": unit["literal_token_count"],
            }
            for index, unit in enumerate(segment.get("source_units", []))
        ]
        segment["evidence_spans"] = [
            {
                "evidence_span_index": index,
                "start_source_unit_index": unit_index_by_id[span["evidence_start_unit_id"]],
                "end_source_unit_index": unit_index_by_id[span["evidence_end_unit_id"]],
                "character_count": span["character_count"],
            }
            for index, span in enumerate(segment.get("evidence_spans", []))
        ]
    return PROMPT_PREFIX + json.dumps(packet, ensure_ascii=True, separators=(",", ":")) + "\n"


def _positional_request(request: Mapping[str, Any]) -> dict[str, Any]:
    parent.validate_prepared_request(request)
    value = copy.deepcopy(dict(request))
    parent_schema = copy.deepcopy(value["output_schema"])
    instructions = _positional_instructions(parent_schema)
    value["schema_version"] = ADAPTER_SCHEMA_VERSION
    value["candidate_system_id"] = CANDIDATE_SYSTEM_ID
    value["canonical_schema_strategy"] = CANONICAL_SCHEMA_STRATEGY
    value["batch_id"] = "cv31uop_" + _sha256_text(
        _canonical_json(
            {
                "parent_batch_id": request["batch_id"],
                "candidate_system_id": CANDIDATE_SYSTEM_ID,
            }
        )
    )[:24]
    value["semantic_integrity"] = semantic_integrity_contract()
    value["private_input"]["schema_version"] = ADAPTER_SCHEMA_VERSION
    value["base_instructions"] = instructions + value["base_instructions"]
    value["base_instructions_sha256"] = _sha256_text(value["base_instructions"])
    value["prompt"] = _compact_prompt(value["prompt"])
    value["prompt_sha256"] = _sha256_text(value["prompt"])
    value["output_schema"] = _output_schema(value, parent_schema)
    value["output_schema_sha256"] = _sha256_text(_canonical_json(value["output_schema"]))
    return value


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


def _parent_request(request: Mapping[str, Any]) -> dict[str, Any]:
    parents = parent.prepare_episode_batches(
        _episode_from_request(request),
        batch_size=int(request["batch_size_ceiling"]),
        thread_mode=str(request["thread_mode"]),
    )
    matches = [item for item in parents if item["segment_ids"] == request["segment_ids"]]
    if len(matches) != 1:
        raise CanonicalV31EpisodeBatchError("parent positional request cannot be reconstructed")
    return matches[0]


def prepare_episode_batches(
    episode: Mapping[str, Any], *, batch_size: int, thread_mode: str
) -> list[dict[str, Any]]:
    values = [
        _positional_request(request)
        for request in parent.prepare_episode_batches(
            episode, batch_size=batch_size, thread_mode=thread_mode
        )
    ]
    for value in values:
        validate_prepared_request(value)
    return values


def validate_prepared_request(request: Mapping[str, Any]) -> dict[str, Any]:
    parent_request = _parent_request(request)
    expected = _positional_request(parent_request)
    if dict(request) != expected:
        raise CanonicalV31EpisodeBatchError("unit-owned positional request drifted")
    return copy.deepcopy(dict(request))


def _decode_value(value: Any, schema: Mapping[str, Any], path: str) -> Any:
    value_type, nullable = _schema_type(schema)
    if value is None and nullable:
        return None
    if value_type == "object":
        required = schema.get("required")
        properties = schema.get("properties")
        if not isinstance(value, list) or not isinstance(required, list) or len(value) != len(required):
            raise CanonicalV31OutputError(f"{path} positional object length drifted")
        return {
            field: _decode_value(value[index], properties[field], f"{path}.{field}")
            for index, field in enumerate(required)
        }
    if value_type == "array":
        if not isinstance(value, list):
            raise CanonicalV31OutputError(f"{path} must be an array")
        return [
            _decode_value(item, schema["items"], f"{path}[{index}]")
            for index, item in enumerate(value)
        ]
    return copy.deepcopy(value)


def _encode_value(value: Any, schema: Mapping[str, Any], path: str) -> Any:
    value_type, nullable = _schema_type(schema)
    if value is None and nullable:
        return None
    if value_type == "object":
        required = schema.get("required")
        properties = schema.get("properties")
        if not isinstance(value, Mapping) or not isinstance(required, list):
            raise CanonicalV31OutputError(f"{path} canonical object drifted")
        return [
            _encode_value(value[field], properties[field], f"{path}.{field}")
            for field in required
        ]
    if value_type == "array":
        if not isinstance(value, list):
            raise CanonicalV31OutputError(f"{path} canonical array drifted")
        return [
            _encode_value(item, schema["items"], f"{path}[{index}]")
            for index, item in enumerate(value)
        ]
    return copy.deepcopy(value)


def _pointer_index(
    value: Any, spans: Sequence[Mapping[str, Any]], path: str
) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value < len(spans):
        raise CanonicalV31OutputError(f"{path} evidence span index is invalid")
    return value


def _decode_output(request: Mapping[str, Any], output: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(output, Mapping) or set(output) != {"s"} or not isinstance(output["s"], list):
        raise CanonicalV31OutputError("unit-owned positional output shape drifted")
    segments = request["private_input"]["segments"]
    if len(output["s"]) != len(segments):
        raise CanonicalV31OutputError("unit-owned positional segment count drifted")
    parent_schema = _parent_request(request)["output_schema"]
    parent_segment = parent_schema["properties"]["segments"]["items"]
    event_schema = parent_segment["properties"]["discourse_events"]["items"]
    concept_schema = parent_segment["properties"]["concept_candidates"]["items"]
    rejected_schema = parent_segment["properties"]["rejected_candidates"]["items"]
    raw_segments = []
    for segment_index, (private_segment, row) in enumerate(zip(segments, output["s"])):
        if not isinstance(row, list) or len(row) != len(SEGMENT_FIELD_ORDER):
            raise CanonicalV31OutputError("unit-owned positional segment row drifted")
        unit_rows = row[2]
        units = private_segment["units"]
        spans = private_segment["evidence_spans"]
        if not isinstance(unit_rows, list) or len(unit_rows) != len(units):
            raise CanonicalV31OutputError("unit-owned positional unit coverage drifted")
        events = []
        concepts = []
        receipts = []
        for unit_index, (unit, unit_row) in enumerate(zip(units, unit_rows)):
            if not isinstance(unit_row, list) or len(unit_row) != len(UNIT_ROW_FIELD_ORDER):
                raise CanonicalV31OutputError("unit-owned positional unit row drifted")
            raw_events, raw_concepts = unit_row
            if not isinstance(raw_events, list) or not isinstance(raw_concepts, list):
                raise CanonicalV31OutputError("unit-owned positional collections drifted")
            decoded_events = []
            for event_index, encoded in enumerate(raw_events):
                event = _decode_value(encoded, event_schema, f"$.s[{segment_index}].u[{unit_index}].e[{event_index}]")
                pointer = _pointer_index(event["evidence_span_id"], spans, "event")
                span = spans[pointer]
                if span["evidence_start_unit_id"] != unit["unit_id"]:
                    raise CanonicalV31OutputError("event is not owned by its evidence-start unit")
                event["evidence_span_id"] = span["evidence_span_id"]
                decoded_events.append(event)
            decoded_concepts = []
            for concept_index, encoded in enumerate(raw_concepts):
                concept = _decode_value(encoded, concept_schema, f"$.s[{segment_index}].u[{unit_index}].c[{concept_index}]")
                pointer = _pointer_index(concept["evidence_span_id"], spans, "concept")
                span = spans[pointer]
                if span["evidence_start_unit_id"] != unit["unit_id"]:
                    raise CanonicalV31OutputError("concept is not owned by its evidence-start unit")
                concept["evidence_span_id"] = span["evidence_span_id"]
                decoded_concepts.append(concept)
            events.extend(decoded_events)
            concepts.extend(decoded_concepts)
            receipts.append(
                {
                    "unit_id": unit["unit_id"],
                    "reviewed": True,
                    "grounded_event_count": len(decoded_events),
                    "grounded_concept_candidate_count": len(decoded_concepts),
                    "unresolved_count": 0,
                }
            )
        rejected = [
            _decode_value(item, rejected_schema, f"$.s[{segment_index}].rejected[{index}]")
            for index, item in enumerate(row[3])
        ] if isinstance(row[3], list) else row[3]
        raw_segments.append(
            {
                "segment_id": private_segment["segment_id"],
                "extraction_status": row[0],
                "segment_source_context": row[1],
                "discourse_events": events,
                "concept_candidates": concepts,
                "rejected_candidates": rejected,
                "no_signal_reason": row[4],
                "overall_confidence": row[5],
                "needs_review": row[6],
                "review_reason": row[7],
                "unit_receipts": receipts,
                "coverage_audit": {
                    "all_source_units_reviewed": True,
                    "unresolved_count": 0,
                },
            }
        )
    return {"episode_id": request["episode_id"], "segments": raw_segments}


def encode_parent_output_for_test(
    request: Mapping[str, Any], parent_output: Mapping[str, Any]
) -> dict[str, Any]:
    """Losslessly encode a validated parent output for fixture proof only."""

    validated = validate_prepared_request(request)
    parent_request = _parent_request(validated)
    parent.validate_and_project_output(parent_request, parent_output)
    parent_schema = parent_request["output_schema"]
    parent_segment = parent_schema["properties"]["segments"]["items"]
    event_schema = parent_segment["properties"]["discourse_events"]["items"]
    concept_schema = parent_segment["properties"]["concept_candidates"]["items"]
    rejected_schema = parent_segment["properties"]["rejected_candidates"]["items"]
    encoded_segments = []
    for private_segment, raw_segment in zip(
        validated["private_input"]["segments"], parent_output["segments"]
    ):
        spans = private_segment["evidence_spans"]
        span_index = {span["evidence_span_id"]: index for index, span in enumerate(spans)}
        event_rows = {unit["unit_id"]: [] for unit in private_segment["units"]}
        concept_rows = {unit["unit_id"]: [] for unit in private_segment["units"]}
        for event in raw_segment["discourse_events"]:
            value = copy.deepcopy(event)
            pointer = span_index[value["evidence_span_id"]]
            value["evidence_span_id"] = pointer
            owner = spans[pointer]["evidence_start_unit_id"]
            event_rows[owner].append(_encode_value(value, event_schema, "event"))
        for concept in raw_segment["concept_candidates"]:
            value = copy.deepcopy(concept)
            pointer = span_index[value["evidence_span_id"]]
            value["evidence_span_id"] = pointer
            owner = spans[pointer]["evidence_start_unit_id"]
            concept_rows[owner].append(_encode_value(value, concept_schema, "concept"))
        unit_rows = [
            [event_rows[unit["unit_id"]], concept_rows[unit["unit_id"]]]
            for unit in private_segment["units"]
        ]
        encoded_segments.append(
            [
                raw_segment["extraction_status"],
                raw_segment["segment_source_context"],
                unit_rows,
                [
                    _encode_value(item, rejected_schema, "rejected")
                    for item in raw_segment["rejected_candidates"]
                ],
                raw_segment["no_signal_reason"],
                raw_segment["overall_confidence"],
                raw_segment["needs_review"],
                raw_segment["review_reason"],
            ]
        )
    encoded = {"s": encoded_segments}
    if _decode_output(validated, encoded) != parent_output:
        raise CanonicalV31OutputError("positional fixture round trip drifted")
    return encoded


def validate_and_project_output(
    request: Mapping[str, Any], output: Mapping[str, Any]
) -> dict[str, Any]:
    validated = validate_prepared_request(request)
    raw = _decode_output(validated, output)
    projected = parent.validate_and_project_output(_parent_request(validated), raw)
    provenance = copy.deepcopy(projected["provenance"])
    provenance.update(
        {
            "schema_version": PROVENANCE_SCHEMA_VERSION,
            "candidate_system_id": CANDIDATE_SYSTEM_ID,
            "positional_protocol_version": POSITIONAL_PROTOCOL_VERSION,
        }
    )
    fidelity = copy.deepcopy(projected["fidelity"])
    fidelity.update(
        {
            "schema_version": FIDELITY_SCHEMA_VERSION,
            "candidate_system_id": CANDIDATE_SYSTEM_ID,
            "positional_protocol_version": POSITIONAL_PROTOCOL_VERSION,
            "model_authored_all_canonical_semantic_fields": True,
            "deterministic_semantic_field_defaults": False,
            "deterministic_semantic_pruning": False,
        }
    )
    return {
        "schema_version": PROJECTION_SCHEMA_VERSION,
        "labels": projected["labels"],
        "provenance": provenance,
        "fidelity": fidelity,
    }


def validate_turn_sidecar(
    request: Mapping[str, Any],
    sidecar_path: Path,
    *,
    output_path: Path,
    expected_thread: Any,
) -> dict[str, Any]:
    validated = validate_prepared_request(request)
    sidecar = base._load_json(sidecar_path, "unit-owned positional sidecar")  # noqa: SLF001
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
        raise CanonicalV31TelemetryError("managed-auth positional sidecar contract failed")
    usage = base._usage_values(sidecar.get("usage"), "usage")  # noqa: SLF001
    total = base._usage_values(sidecar.get("thread_total_usage"), "thread_total_usage")  # noqa: SLF001
    if any(total[field] < usage[field] for field in USAGE_FIELDS):
        raise CanonicalV31TelemetryError("thread total usage is below last usage")
    wall = base._nonnegative_number(sidecar.get("wall_elapsed_seconds"), "wall_elapsed_seconds")  # noqa: SLF001
    if (
        not output_path.is_file()
        or sidecar.get("output_sha256") != base._output_message_hash(output_path)  # noqa: SLF001
        or Path(str(sidecar.get("output_path") or "")).expanduser().resolve()
        != output_path.expanduser().resolve()
    ):
        raise CanonicalV31TelemetryError("sidecar-bound positional output is absent or changed")
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
        "schema_version": "pif_canonical_v31_unit_owned_positional_binding_v1",
        "parent_binding": parent.build_six_arm_matrix_binding(),
        "parent_adapter_module_sha256": hashlib.sha256(Path(parent.__file__).read_bytes()).hexdigest(),
        "adapter_module_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "candidate_system_id": CANDIDATE_SYSTEM_ID,
        "canonical_schema_strategy": CANONICAL_SCHEMA_STRATEGY,
        "positional_protocol_version": POSITIONAL_PROTOCOL_VERSION,
        "model_authored_all_canonical_semantic_fields": True,
        "deterministic_semantic_pruning": False,
        "deterministic_deduplication": False,
        "deterministic_relabeling": False,
    }


__all__: Sequence[str] = (
    "ADAPTER_SCHEMA_VERSION",
    "CANDIDATE_SYSTEM_ID",
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
