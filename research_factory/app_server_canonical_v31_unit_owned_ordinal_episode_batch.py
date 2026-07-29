from __future__ import annotations

"""Provider-compatible ordinal-key wire format for unit-owned canonical v3.1."""

import copy
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_canonical_v31_unit_owned_positional_episode_batch as positional


parent = positional.parent
base = positional.base
PROJECT_ROOT = positional.PROJECT_ROOT
MODEL = positional.MODEL
EFFORT = positional.EFFORT
PINNED_CODEX = positional.PINNED_CODEX
CANONICAL_LABEL_PACK = positional.CANONICAL_LABEL_PACK
CANONICAL_LABEL_SCHEMA_SHA256 = positional.CANONICAL_LABEL_SCHEMA_SHA256
SUPPORTED_BATCH_SIZES = positional.SUPPORTED_BATCH_SIZES
SUPPORTED_THREAD_MODES = positional.SUPPORTED_THREAD_MODES
RETRY_COUNT = positional.RETRY_COUNT
USAGE_FIELDS = positional.USAGE_FIELDS
CanonicalV31EpisodeBatchError = positional.CanonicalV31EpisodeBatchError
CanonicalV31OutputError = positional.CanonicalV31OutputError
CanonicalV31TelemetryError = positional.CanonicalV31TelemetryError
_client_factory = positional._client_factory
_verify_started_thread = positional._verify_started_thread
expected_instruction_source_contract = positional.expected_instruction_source_contract
verified_context_control_overlay = positional.verified_context_control_overlay

ADAPTER_SCHEMA_VERSION = "pif_canonical_v31_unit_owned_ordinal_episode_batch_v1"
RAW_OUTPUT_SCHEMA_VERSION = "pif_canonical_v31_unit_owned_ordinal_raw_output_v1"
PROJECTION_SCHEMA_VERSION = "pif_canonical_v31_unit_owned_ordinal_projection_v1"
PROVENANCE_SCHEMA_VERSION = "pif_canonical_v31_unit_owned_ordinal_provenance_v1"
FIDELITY_SCHEMA_VERSION = "pif_canonical_v31_unit_owned_ordinal_fidelity_v1"
CANDIDATE_SYSTEM_ID = "pif_unit_owned_ordinal_ai_discourse_v3_1_v1"
CANONICAL_SCHEMA_STRATEGY = "canonical_v31_unit_owned_ordinal_full_schema_v1"
ORDINAL_PROTOCOL_VERSION = "pif_unit_owned_ordinal_full_schema_v1"


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def semantic_integrity_contract() -> dict[str, Any]:
    value = copy.deepcopy(positional.semantic_integrity_contract())
    value.pop("positional_protocol_version", None)
    value.update(
        {
            "ordinal_protocol_version": ORDINAL_PROTOCOL_VERSION,
            "provider_compatible_closed_ordinal_objects": True,
            "model_authors_all_canonical_semantic_fields": True,
            "deterministic_ordinal_field_order_decode_only": True,
            "deterministic_semantic_pruning": False,
            "deterministic_support_filtering": False,
            "deterministic_deduplication": False,
            "deterministic_relabeling": False,
        }
    )
    return value


def _ordinal_instructions(parent_schema: Mapping[str, Any]) -> str:
    layout = json.dumps(
        positional._field_layout(parent_schema),  # noqa: SLF001
        ensure_ascii=True,
        separators=(",", ":"),
    )
    return (
        "# Unit-owned ordinal full-schema contract\n"
        "Return the supplied closed-object schema. The key s contains one segment "
        "object per input segment. Every closed object uses ordinal keys 0,1,2,... "
        "in the exact field order below. Each segment contains one unit row per source "
        "unit in input order. Place every independent grounded proposition in the "
        "event collection owned by the unit where its smallest evidence span begins. "
        "Do not collapse distinct propositions, and do not aim for a target count. "
        "All canonical semantic values are model authored; ordinal keys are structural "
        "only. Evidence-span integers index the supplied evidence_spans array. Return "
        "one final JSON object only.\n"
        f"Exact ordinal field layout: {layout}\n\n"
    )


def _ordinal_schema_for_value(
    schema: Mapping[str, Any], *, evidence_span_max_index: int | None = None
) -> dict[str, Any]:
    value_type, nullable = positional._schema_type(schema)  # noqa: SLF001
    if value_type == "object":
        required = schema.get("required")
        properties = schema.get("properties")
        if not isinstance(required, list) or not isinstance(properties, Mapping):
            raise CanonicalV31EpisodeBatchError("canonical object schema is malformed")
        ordinal_properties = {}
        for index, field in enumerate(required):
            if field == "evidence_span_id" and evidence_span_max_index is not None:
                child = {
                    "type": "integer",
                    "minimum": 0,
                    "maximum": evidence_span_max_index,
                }
            else:
                child = _ordinal_schema_for_value(
                    properties[field],
                    evidence_span_max_index=evidence_span_max_index,
                )
            ordinal_properties[str(index)] = child
        return {
            "type": ["object", "null"] if nullable else "object",
            "additionalProperties": False,
            "required": list(ordinal_properties),
            "properties": ordinal_properties,
        }
    if value_type == "array":
        result = {
            key: copy.deepcopy(value)
            for key, value in schema.items()
            if key not in {"items", "type"}
        }
        result["type"] = ["array", "null"] if nullable else "array"
        result["items"] = _ordinal_schema_for_value(
            schema["items"], evidence_span_max_index=evidence_span_max_index
        )
        return result
    return copy.deepcopy(dict(schema))


def _closed_ordinal_object(items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    properties = {str(index): copy.deepcopy(dict(item)) for index, item in enumerate(items)}
    return {
        "type": "object",
        "additionalProperties": False,
        "required": list(properties),
        "properties": properties,
    }


def _output_schema(request: Mapping[str, Any], parent_schema: Mapping[str, Any]) -> dict[str, Any]:
    segment_schema = parent_schema["properties"]["segments"]["items"]
    event_schema = segment_schema["properties"]["discourse_events"]["items"]
    concept_schema = segment_schema["properties"]["concept_candidates"]["items"]
    rejected_schema = segment_schema["properties"]["rejected_candidates"]["items"]
    span_counts = {len(segment["evidence_spans"]) for segment in request["private_input"]["segments"]}
    unit_counts = {len(segment["units"]) for segment in request["private_input"]["segments"]}
    if len(span_counts) != 1 or len(unit_counts) != 1:
        raise CanonicalV31EpisodeBatchError("ordinal schema requires uniform source packet cardinality")
    span_max = next(iter(span_counts)) - 1
    unit_count = next(iter(unit_counts))
    event = _ordinal_schema_for_value(event_schema, evidence_span_max_index=span_max)
    concept = _ordinal_schema_for_value(concept_schema, evidence_span_max_index=span_max)
    rejected = _ordinal_schema_for_value(rejected_schema)
    unit_row = _closed_ordinal_object(
        (
            {"type": "array", "items": event},
            {"type": "array", "items": concept},
        )
    )
    segment = _closed_ordinal_object(
        (
            segment_schema["properties"]["extraction_status"],
            segment_schema["properties"]["segment_source_context"],
            {
                "type": "array",
                "items": unit_row,
                "minItems": unit_count,
                "maxItems": unit_count,
            },
            {"type": "array", "items": rejected},
            segment_schema["properties"]["no_signal_reason"],
            segment_schema["properties"]["overall_confidence"],
            segment_schema["properties"]["needs_review"],
            segment_schema["properties"]["review_reason"],
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
                "items": segment,
                "minItems": len(request["private_input"]["segments"]),
                "maxItems": len(request["private_input"]["segments"]),
            }
        },
    }


def _ordinal_request(request: Mapping[str, Any]) -> dict[str, Any]:
    parent.validate_prepared_request(request)
    value = copy.deepcopy(dict(request))
    parent_schema = copy.deepcopy(value["output_schema"])
    instructions = _ordinal_instructions(parent_schema)
    value["schema_version"] = ADAPTER_SCHEMA_VERSION
    value["candidate_system_id"] = CANDIDATE_SYSTEM_ID
    value["canonical_schema_strategy"] = CANONICAL_SCHEMA_STRATEGY
    value["batch_id"] = "cv31uoo_" + _sha256_text(
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
    value["prompt"] = positional._compact_prompt(value["prompt"])  # noqa: SLF001
    value["prompt_sha256"] = _sha256_text(value["prompt"])
    value["output_schema"] = _output_schema(value, parent_schema)
    value["output_schema_sha256"] = _sha256_text(_canonical_json(value["output_schema"]))
    return value


def _parent_request(request: Mapping[str, Any]) -> dict[str, Any]:
    return positional._parent_request(request)  # noqa: SLF001


def prepare_episode_batches(
    episode: Mapping[str, Any], *, batch_size: int, thread_mode: str
) -> list[dict[str, Any]]:
    values = [
        _ordinal_request(request)
        for request in parent.prepare_episode_batches(
            episode, batch_size=batch_size, thread_mode=thread_mode
        )
    ]
    for value in values:
        validate_prepared_request(value)
    return values


def validate_prepared_request(request: Mapping[str, Any]) -> dict[str, Any]:
    expected = _ordinal_request(_parent_request(request))
    if dict(request) != expected:
        raise CanonicalV31EpisodeBatchError("unit-owned ordinal request drifted")
    return copy.deepcopy(dict(request))


def _ordinal_to_positional(value: Any, schema: Mapping[str, Any], path: str) -> Any:
    value_type, nullable = positional._schema_type(schema)  # noqa: SLF001
    if value is None and nullable:
        return None
    if value_type == "object":
        required = schema.get("required")
        properties = schema.get("properties")
        keys = [str(index) for index in range(len(required or []))]
        if not isinstance(value, Mapping) or set(value) != set(keys):
            raise CanonicalV31OutputError(f"{path} ordinal object shape drifted")
        return [
            _ordinal_to_positional(value[str(index)], properties[field], f"{path}.{index}")
            for index, field in enumerate(required)
        ]
    if value_type == "array":
        if not isinstance(value, list):
            raise CanonicalV31OutputError(f"{path} must be an array")
        return [
            _ordinal_to_positional(item, schema["items"], f"{path}[{index}]")
            for index, item in enumerate(value)
        ]
    return copy.deepcopy(value)


def _positional_to_ordinal(value: Any, schema: Mapping[str, Any], path: str) -> Any:
    value_type, nullable = positional._schema_type(schema)  # noqa: SLF001
    if value is None and nullable:
        return None
    if value_type == "object":
        required = schema.get("required")
        properties = schema.get("properties")
        if not isinstance(value, list) or len(value) != len(required or []):
            raise CanonicalV31OutputError(f"{path} positional fixture shape drifted")
        return {
            str(index): _positional_to_ordinal(value[index], properties[field], f"{path}.{index}")
            for index, field in enumerate(required)
        }
    if value_type == "array":
        if not isinstance(value, list):
            raise CanonicalV31OutputError(f"{path} positional fixture must be an array")
        return [
            _positional_to_ordinal(item, schema["items"], f"{path}[{index}]")
            for index, item in enumerate(value)
        ]
    return copy.deepcopy(value)


def _positional_wire(request: Mapping[str, Any], output: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(output, Mapping) or set(output) != {"s"} or not isinstance(output["s"], list):
        raise CanonicalV31OutputError("unit-owned ordinal output shape drifted")
    parent_schema = _parent_request(request)["output_schema"]
    segment_schema = parent_schema["properties"]["segments"]["items"]
    event_schema = segment_schema["properties"]["discourse_events"]["items"]
    concept_schema = segment_schema["properties"]["concept_candidates"]["items"]
    rejected_schema = segment_schema["properties"]["rejected_candidates"]["items"]
    segment_rows = []
    for segment_index, segment in enumerate(output["s"]):
        keys = [str(index) for index in range(len(positional.SEGMENT_FIELD_ORDER))]
        if not isinstance(segment, Mapping) or set(segment) != set(keys):
            raise CanonicalV31OutputError("ordinal segment row drifted")
        unit_rows = []
        if not isinstance(segment["2"], list):
            raise CanonicalV31OutputError("ordinal unit rows drifted")
        for unit_index, unit in enumerate(segment["2"]):
            if not isinstance(unit, Mapping) or set(unit) != {"0", "1"}:
                raise CanonicalV31OutputError("ordinal unit row drifted")
            unit_rows.append(
                [
                    [
                        _ordinal_to_positional(item, event_schema, f"$.s[{segment_index}].u[{unit_index}].e")
                        for item in unit["0"]
                    ],
                    [
                        _ordinal_to_positional(item, concept_schema, f"$.s[{segment_index}].u[{unit_index}].c")
                        for item in unit["1"]
                    ],
                ]
            )
        rejected = [
            _ordinal_to_positional(item, rejected_schema, "rejected")
            for item in segment["3"]
        ]
        segment_rows.append(
            [
                segment["0"],
                segment["1"],
                unit_rows,
                rejected,
                segment["4"],
                segment["5"],
                segment["6"],
                segment["7"],
            ]
        )
    return {"s": segment_rows}


def encode_parent_output_for_test(
    request: Mapping[str, Any], parent_output: Mapping[str, Any]
) -> dict[str, Any]:
    validated = validate_prepared_request(request)
    parent_request = _parent_request(validated)
    positional_request = positional._positional_request(parent_request)  # noqa: SLF001
    wire = positional.encode_parent_output_for_test(positional_request, parent_output)
    parent_schema = parent_request["output_schema"]
    segment_schema = parent_schema["properties"]["segments"]["items"]
    event_schema = segment_schema["properties"]["discourse_events"]["items"]
    concept_schema = segment_schema["properties"]["concept_candidates"]["items"]
    rejected_schema = segment_schema["properties"]["rejected_candidates"]["items"]
    ordinal_segments = []
    for segment in wire["s"]:
        unit_rows = []
        for unit in segment[2]:
            unit_rows.append(
                {
                    "0": [_positional_to_ordinal(item, event_schema, "event") for item in unit[0]],
                    "1": [_positional_to_ordinal(item, concept_schema, "concept") for item in unit[1]],
                }
            )
        ordinal_segments.append(
            {
                "0": segment[0],
                "1": segment[1],
                "2": unit_rows,
                "3": [_positional_to_ordinal(item, rejected_schema, "rejected") for item in segment[3]],
                "4": segment[4],
                "5": segment[5],
                "6": segment[6],
                "7": segment[7],
            }
        )
    output = {"s": ordinal_segments}
    raw = positional._decode_output(validated, _positional_wire(validated, output))  # noqa: SLF001
    if raw != parent_output:
        raise CanonicalV31OutputError("ordinal fixture round trip drifted")
    return output


def validate_and_project_output(
    request: Mapping[str, Any], output: Mapping[str, Any]
) -> dict[str, Any]:
    validated = validate_prepared_request(request)
    raw = positional._decode_output(  # noqa: SLF001
        validated, _positional_wire(validated, output)
    )
    projected = parent.validate_and_project_output(_parent_request(validated), raw)
    provenance = copy.deepcopy(projected["provenance"])
    provenance.update(
        {
            "schema_version": PROVENANCE_SCHEMA_VERSION,
            "candidate_system_id": CANDIDATE_SYSTEM_ID,
            "ordinal_protocol_version": ORDINAL_PROTOCOL_VERSION,
        }
    )
    fidelity = copy.deepcopy(projected["fidelity"])
    fidelity.update(
        {
            "schema_version": FIDELITY_SCHEMA_VERSION,
            "candidate_system_id": CANDIDATE_SYSTEM_ID,
            "ordinal_protocol_version": ORDINAL_PROTOCOL_VERSION,
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
    sidecar = base._load_json(sidecar_path, "unit-owned ordinal sidecar")  # noqa: SLF001
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
        raise CanonicalV31TelemetryError("managed-auth ordinal sidecar contract failed")
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
        raise CanonicalV31TelemetryError("sidecar-bound ordinal output is absent or changed")
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
        "schema_version": "pif_canonical_v31_unit_owned_ordinal_binding_v1",
        "positional_predecessor_binding": positional.build_six_arm_matrix_binding(),
        "positional_adapter_module_sha256": hashlib.sha256(Path(positional.__file__).read_bytes()).hexdigest(),
        "adapter_module_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "candidate_system_id": CANDIDATE_SYSTEM_ID,
        "canonical_schema_strategy": CANONICAL_SCHEMA_STRATEGY,
        "ordinal_protocol_version": ORDINAL_PROTOCOL_VERSION,
        "provider_compatible_closed_ordinal_objects": True,
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
