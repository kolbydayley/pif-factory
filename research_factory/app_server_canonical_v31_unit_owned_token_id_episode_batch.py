from __future__ import annotations

"""Unit-reviewed canonical v3.1 wire format with exact literal-token IDs."""

import copy
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_canonical_v31_unit_owned_ordinal_episode_batch as ordinal


parent = ordinal
base = ordinal.base
PROJECT_ROOT = ordinal.PROJECT_ROOT
MODEL = ordinal.MODEL
EFFORT = ordinal.EFFORT
PINNED_CODEX = ordinal.PINNED_CODEX
CANONICAL_LABEL_PACK = ordinal.CANONICAL_LABEL_PACK
CANONICAL_LABEL_SCHEMA_SHA256 = ordinal.CANONICAL_LABEL_SCHEMA_SHA256
SUPPORTED_BATCH_SIZES = ordinal.SUPPORTED_BATCH_SIZES
SUPPORTED_THREAD_MODES = ordinal.SUPPORTED_THREAD_MODES
RETRY_COUNT = ordinal.RETRY_COUNT
USAGE_FIELDS = ordinal.USAGE_FIELDS
CanonicalV31EpisodeBatchError = ordinal.CanonicalV31EpisodeBatchError
CanonicalV31OutputError = ordinal.CanonicalV31OutputError
CanonicalV31TelemetryError = ordinal.CanonicalV31TelemetryError
_client_factory = ordinal._client_factory
_verify_started_thread = ordinal._verify_started_thread
expected_instruction_source_contract = ordinal.expected_instruction_source_contract
verified_context_control_overlay = ordinal.verified_context_control_overlay

ADAPTER_SCHEMA_VERSION = "pif_canonical_v31_unit_owned_token_id_episode_batch_v1"
RAW_OUTPUT_SCHEMA_VERSION = "pif_canonical_v31_unit_owned_token_id_raw_output_v1"
PROJECTION_SCHEMA_VERSION = "pif_canonical_v31_unit_owned_token_id_projection_v1"
PROVENANCE_SCHEMA_VERSION = "pif_canonical_v31_unit_owned_token_id_provenance_v1"
FIDELITY_SCHEMA_VERSION = "pif_canonical_v31_unit_owned_token_id_fidelity_v1"
CANDIDATE_SYSTEM_ID = "pif_unit_owned_token_id_ai_discourse_v3_1_v1"
CANONICAL_SCHEMA_STRATEGY = "canonical_v31_unit_owned_exact_token_id_full_schema_v1"
TOKEN_ID_PROTOCOL_VERSION = "pif_unit_owned_exact_literal_token_id_v1"
METRIC_RANGE_FIELDS = (
    "value_range",
    "unit_range",
    "comparator_range",
    "raw_text_range",
)


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def semantic_integrity_contract() -> dict[str, Any]:
    value = copy.deepcopy(ordinal.semantic_integrity_contract())
    value.update(
        {
            "token_id_protocol_version": TOKEN_ID_PROTOCOL_VERSION,
            "model_selects_exact_literal_token_ids": True,
            "model_authored_evidence_span_is_owner_authority": True,
            "deterministic_owner_container_normalization_only": True,
            "deterministic_exact_token_id_projection_only": True,
            "deterministic_metric_pointer_clamping": False,
            "deterministic_semantic_defaults": {},
            "deterministic_semantic_pruning": False,
            "deterministic_support_filtering": False,
            "deterministic_deduplication": False,
            "deterministic_relabeling": False,
        }
    )
    return value


def _token_id_instructions() -> str:
    return (
        "# Exact literal-token ID transport\n"
        "Keep the unit-row review surface and author every canonical semantic field. "
        "The evidence-span integer is the authoritative owner: each returned event or "
        "concept is placed deterministically under the source unit where that authored "
        "span begins, without changing, deleting, or deduplicating it. For each non-null "
        "metric value_range, unit_range, comparator_range, or raw_text_range, return the "
        "closed object {\"0\": START_LITERAL_TOKEN_ID, \"1\": END_LITERAL_TOKEN_ID}. "
        "Both IDs must come from literal_tokens in the supplied source packet, be in "
        "source order, and lie wholly inside the event's selected evidence span. Use null "
        "when the canonical metric literal is not applicable. Never invent a token ID, "
        "numeric token coordinate, semantic default, or target count. Return one final "
        "structured JSON object only.\n\n"
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


def _parent_request(request: Mapping[str, Any]) -> dict[str, Any]:
    parents = ordinal.prepare_episode_batches(
        _episode_from_request(request),
        batch_size=int(request["batch_size_ceiling"]),
        thread_mode=str(request["thread_mode"]),
    )
    matches = [item for item in parents if item["segment_ids"] == request["segment_ids"]]
    if len(matches) != 1:
        raise CanonicalV31EpisodeBatchError("token-ID parent request cannot be reconstructed")
    return matches[0]


def _token_ids(request: Mapping[str, Any]) -> list[str]:
    values = [
        token["literal_token_id"]
        for segment in request["private_input"]["segments"]
        for unit in segment["units"]
        for token in unit["literal_tokens"]
    ]
    if (
        not values
        or any(not isinstance(value, str) or not value for value in values)
        or len(values) != len(set(values))
    ):
        raise CanonicalV31EpisodeBatchError("literal-token ID catalog drifted")
    return values


def _canonical_event_and_metric_schema(
    request: Mapping[str, Any]
) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    canonical = ordinal._parent_request(_parent_request(request))  # noqa: SLF001
    segment = canonical["output_schema"]["properties"]["segments"]["items"]
    event = segment["properties"]["discourse_events"]["items"]
    metric = event["properties"]["metric"]
    return event, metric


def _token_pair_schema(token_ids: Sequence[str]) -> dict[str, Any]:
    lengths = {len(token_id) for token_id in token_ids}
    if len(lengths) != 1:
        raise CanonicalV31EpisodeBatchError("literal-token ID width drifted")
    width = next(iter(lengths))
    token = {"type": "string", "minLength": width, "maxLength": width}
    return {
        "type": ["object", "null"],
        "additionalProperties": False,
        "required": ["0", "1"],
        "properties": {"0": copy.deepcopy(token), "1": copy.deepcopy(token)},
    }


def _output_schema(request: Mapping[str, Any]) -> dict[str, Any]:
    schema = copy.deepcopy(request["output_schema"])
    event, metric = _canonical_event_and_metric_schema(request)
    event_metric_index = event["required"].index("metric")
    metric_node = (
        schema["properties"]["s"]["items"]["properties"]["2"]["items"]
        ["properties"]["0"]["items"]["properties"][str(event_metric_index)]
    )
    pair = _token_pair_schema(_token_ids(request))
    for field in METRIC_RANGE_FIELDS:
        metric_index = metric["required"].index(field)
        metric_node["properties"][str(metric_index)] = copy.deepcopy(pair)
    schema["$id"] = RAW_OUTPUT_SCHEMA_VERSION
    return schema


def _token_id_request(request: Mapping[str, Any]) -> dict[str, Any]:
    ordinal.validate_prepared_request(request)
    value = copy.deepcopy(dict(request))
    value["schema_version"] = ADAPTER_SCHEMA_VERSION
    value["candidate_system_id"] = CANDIDATE_SYSTEM_ID
    value["canonical_schema_strategy"] = CANONICAL_SCHEMA_STRATEGY
    value["batch_id"] = "cv31uoti_" + _sha256_text(
        _canonical_json(
            {
                "parent_batch_id": request["batch_id"],
                "candidate_system_id": CANDIDATE_SYSTEM_ID,
            }
        )
    )[:24]
    value["semantic_integrity"] = semantic_integrity_contract()
    value["private_input"]["schema_version"] = ADAPTER_SCHEMA_VERSION
    value["base_instructions"] = _token_id_instructions() + value["base_instructions"]
    value["base_instructions_sha256"] = _sha256_text(value["base_instructions"])
    value["output_schema"] = _output_schema(request)
    value["output_schema_sha256"] = _sha256_text(_canonical_json(value["output_schema"]))
    return value


def prepare_episode_batches(
    episode: Mapping[str, Any], *, batch_size: int, thread_mode: str
) -> list[dict[str, Any]]:
    values = [
        _token_id_request(request)
        for request in ordinal.prepare_episode_batches(
            episode, batch_size=batch_size, thread_mode=thread_mode
        )
    ]
    for value in values:
        validate_prepared_request(value)
    return values


def validate_prepared_request(request: Mapping[str, Any]) -> dict[str, Any]:
    expected = _token_id_request(_parent_request(request))
    if dict(request) != expected:
        raise CanonicalV31EpisodeBatchError("unit-owned token-ID request drifted")
    return copy.deepcopy(dict(request))


def _layout(request: Mapping[str, Any]) -> dict[str, Any]:
    event, metric = _canonical_event_and_metric_schema(request)
    canonical = ordinal._parent_request(_parent_request(request))  # noqa: SLF001
    segment = canonical["output_schema"]["properties"]["segments"]["items"]
    concept = segment["properties"]["concept_candidates"]["items"]
    return {
        "event_evidence": event["required"].index("evidence_span_id"),
        "event_metric": event["required"].index("metric"),
        "concept_evidence": concept["required"].index("evidence_span_id"),
        "metric_fields": {
            field: metric["required"].index(field) for field in METRIC_RANGE_FIELDS
        },
        "event_field_count": len(event["required"]),
        "concept_field_count": len(concept["required"]),
        "metric_field_count": len(metric["required"]),
    }


def _token_lookup(segment: Mapping[str, Any]) -> dict[str, tuple[int, int, Mapping[str, Any]]]:
    result: dict[str, tuple[int, int, Mapping[str, Any]]] = {}
    for unit_index, unit in enumerate(segment["units"]):
        for token_index, token in enumerate(unit["literal_tokens"]):
            token_id = token["literal_token_id"]
            if token_id in result:
                raise CanonicalV31OutputError("literal-token ID is not unique within segment")
            result[token_id] = (unit_index, token_index, token)
    return result


def _metric_range(
    value: Any,
    *,
    token_lookup: Mapping[str, tuple[int, int, Mapping[str, Any]]],
    evidence_span: Mapping[str, Any],
    path: str,
) -> list[int] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping) or set(value) != {"0", "1"}:
        raise CanonicalV31OutputError(f"{path} exact-token pair is malformed")
    start = token_lookup.get(value["0"])
    end = token_lookup.get(value["1"])
    if start is None or end is None:
        raise CanonicalV31OutputError(f"{path} literal-token ID is unknown")
    start_unit, start_index, start_token = start
    end_unit, end_index, end_token = end
    if (start_unit, start_index) > (end_unit, end_index):
        raise CanonicalV31OutputError(f"{path} literal-token range is reversed")
    if (
        start_token["start_char"] < evidence_span["start_char"]
        or end_token["end_char"] > evidence_span["end_char"]
    ):
        raise CanonicalV31OutputError(f"{path} literal-token range is outside evidence")
    return [start_unit, start_index, end_unit, end_index]


def _normalize_transport(
    request: Mapping[str, Any], output: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, int]]:
    if not isinstance(output, Mapping) or set(output) != {"s"} or not isinstance(output["s"], list):
        raise CanonicalV31OutputError("unit-owned token-ID output shape drifted")
    segments = request["private_input"]["segments"]
    if len(output["s"]) != len(segments):
        raise CanonicalV31OutputError("unit-owned token-ID segment count drifted")
    layout = _layout(request)
    normalized = copy.deepcopy(dict(output))
    diagnostics = {
        "event_owner_container_normalization_count": 0,
        "concept_owner_container_normalization_count": 0,
        "exact_metric_token_pair_count": 0,
    }
    before_events = 0
    before_concepts = 0
    for segment_index, (source, segment) in enumerate(zip(segments, output["s"])):
        if not isinstance(segment, Mapping) or set(segment) != {
            str(index) for index in range(len(ordinal.positional.SEGMENT_FIELD_ORDER))
        }:
            raise CanonicalV31OutputError("unit-owned token-ID segment row drifted")
        unit_rows = segment["2"]
        units = source["units"]
        spans = source["evidence_spans"]
        if not isinstance(unit_rows, list) or len(unit_rows) != len(units):
            raise CanonicalV31OutputError("unit-owned token-ID unit coverage drifted")
        owner_index = {unit["unit_id"]: index for index, unit in enumerate(units)}
        tokens = _token_lookup(source)
        new_rows = [{"0": [], "1": []} for _ in units]
        for unit_index, unit_row in enumerate(unit_rows):
            if not isinstance(unit_row, Mapping) or set(unit_row) != {"0", "1"}:
                raise CanonicalV31OutputError("unit-owned token-ID unit row drifted")
            events = unit_row["0"]
            concepts = unit_row["1"]
            if not isinstance(events, list) or not isinstance(concepts, list):
                raise CanonicalV31OutputError("unit-owned token-ID collections drifted")
            before_events += len(events)
            before_concepts += len(concepts)
            for event_index, event in enumerate(events):
                if not isinstance(event, Mapping) or set(event) != {
                    str(index) for index in range(layout["event_field_count"])
                }:
                    raise CanonicalV31OutputError("unit-owned token-ID event row drifted")
                pointer = event[str(layout["event_evidence"])]
                if isinstance(pointer, bool) or not isinstance(pointer, int) or not 0 <= pointer < len(spans):
                    raise CanonicalV31OutputError("event evidence span index is invalid")
                span = spans[pointer]
                target = owner_index[span["evidence_start_unit_id"]]
                converted = copy.deepcopy(dict(event))
                metric = converted[str(layout["event_metric"])]
                if not isinstance(metric, Mapping) or set(metric) != {
                    str(index) for index in range(layout["metric_field_count"])
                }:
                    raise CanonicalV31OutputError("unit-owned token-ID metric row drifted")
                metric = copy.deepcopy(dict(metric))
                for field, field_index in layout["metric_fields"].items():
                    value = metric[str(field_index)]
                    metric[str(field_index)] = _metric_range(
                        value,
                        token_lookup=tokens,
                        evidence_span=span,
                        path=f"$.s[{segment_index}].u[{unit_index}].e[{event_index}].{field}",
                    )
                    if value is not None:
                        diagnostics["exact_metric_token_pair_count"] += 1
                converted[str(layout["event_metric"])] = metric
                new_rows[target]["0"].append(converted)
                if target != unit_index:
                    diagnostics["event_owner_container_normalization_count"] += 1
            for concept in concepts:
                if not isinstance(concept, Mapping) or set(concept) != {
                    str(index) for index in range(layout["concept_field_count"])
                }:
                    raise CanonicalV31OutputError("unit-owned token-ID concept row drifted")
                pointer = concept[str(layout["concept_evidence"])]
                if isinstance(pointer, bool) or not isinstance(pointer, int) or not 0 <= pointer < len(spans):
                    raise CanonicalV31OutputError("concept evidence span index is invalid")
                target = owner_index[spans[pointer]["evidence_start_unit_id"]]
                new_rows[target]["1"].append(copy.deepcopy(dict(concept)))
                if target != unit_index:
                    diagnostics["concept_owner_container_normalization_count"] += 1
        normalized["s"][segment_index]["2"] = new_rows
    after_events = sum(
        len(unit["0"]) for segment in normalized["s"] for unit in segment["2"]
    )
    after_concepts = sum(
        len(unit["1"]) for segment in normalized["s"] for unit in segment["2"]
    )
    if before_events != after_events or before_concepts != after_concepts:
        raise CanonicalV31OutputError("owner normalization changed semantic cardinality")
    diagnostics["emitted_event_count"] = after_events
    diagnostics["emitted_concept_count"] = after_concepts
    return normalized, diagnostics


def _old_range_to_token_pair(
    value: Any, source: Mapping[str, Any]
) -> dict[str, str] | None:
    if value is None:
        return None
    if (
        not isinstance(value, list)
        or len(value) != 4
        or any(isinstance(item, bool) or not isinstance(item, int) for item in value)
    ):
        raise CanonicalV31OutputError("fixture metric range is malformed")
    start_unit, start_index, end_unit, end_index = value
    try:
        start = source["units"][start_unit]["literal_tokens"][start_index]
        end = source["units"][end_unit]["literal_tokens"][end_index]
    except (IndexError, KeyError, TypeError) as exc:
        raise CanonicalV31OutputError("fixture metric range is invalid") from exc
    return {"0": start["literal_token_id"], "1": end["literal_token_id"]}


def encode_parent_output_for_test(
    request: Mapping[str, Any], parent_output: Mapping[str, Any]
) -> dict[str, Any]:
    validated = validate_prepared_request(request)
    old_request = _parent_request(validated)
    output = ordinal.encode_parent_output_for_test(old_request, parent_output)
    layout = _layout(validated)
    for source, segment in zip(validated["private_input"]["segments"], output["s"]):
        for unit in segment["2"]:
            for event in unit["0"]:
                metric = event[str(layout["event_metric"])]
                for field_index in layout["metric_fields"].values():
                    key = str(field_index)
                    metric[key] = _old_range_to_token_pair(metric[key], source)
    projected = validate_and_project_output(validated, output)
    expected = ordinal.parent.validate_and_project_output(  # noqa: SLF001
        ordinal._parent_request(old_request), parent_output  # noqa: SLF001
    )["labels"]
    if projected["labels"] != expected:
        raise CanonicalV31OutputError("token-ID fixture round trip drifted")
    return output


def validate_and_project_output(
    request: Mapping[str, Any], output: Mapping[str, Any]
) -> dict[str, Any]:
    validated = validate_prepared_request(request)
    normalized, diagnostics = _normalize_transport(validated, output)
    projected = ordinal.validate_and_project_output(_parent_request(validated), normalized)
    provenance = copy.deepcopy(projected["provenance"])
    provenance.update(
        {
            "schema_version": PROVENANCE_SCHEMA_VERSION,
            "candidate_system_id": CANDIDATE_SYSTEM_ID,
            "token_id_protocol_version": TOKEN_ID_PROTOCOL_VERSION,
            **diagnostics,
        }
    )
    fidelity = copy.deepcopy(projected["fidelity"])
    fidelity.update(
        {
            "schema_version": FIDELITY_SCHEMA_VERSION,
            "candidate_system_id": CANDIDATE_SYSTEM_ID,
            "token_id_protocol_version": TOKEN_ID_PROTOCOL_VERSION,
            "model_authored_all_canonical_semantic_fields": True,
            "deterministic_semantic_field_defaults": False,
            "deterministic_semantic_pruning": False,
            "deterministic_owner_container_normalization_only": True,
            **diagnostics,
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
    sidecar = base._load_json(sidecar_path, "unit-owned token-ID sidecar")  # noqa: SLF001
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
        raise CanonicalV31TelemetryError("managed-auth token-ID sidecar contract failed")
    usage = base._usage_values(sidecar.get("usage"), "usage")  # noqa: SLF001
    total = base._usage_values(sidecar.get("thread_total_usage"), "thread_total_usage")  # noqa: SLF001
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
        raise CanonicalV31TelemetryError("sidecar-bound token-ID output is absent or changed")
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
        "schema_version": "pif_canonical_v31_unit_owned_token_id_binding_v1",
        "ordinal_predecessor_binding": ordinal.build_six_arm_matrix_binding(),
        "ordinal_adapter_module_sha256": hashlib.sha256(
            Path(ordinal.__file__).read_bytes()
        ).hexdigest(),
        "adapter_module_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "candidate_system_id": CANDIDATE_SYSTEM_ID,
        "canonical_schema_strategy": CANONICAL_SCHEMA_STRATEGY,
        "token_id_protocol_version": TOKEN_ID_PROTOCOL_VERSION,
        "model_selects_exact_literal_token_ids": True,
        "deterministic_owner_container_normalization_only": True,
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
