from __future__ import annotations

"""Canonical exact-token-ID adapter with an explicit tagged metric transport."""

import copy
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_canonical_v31_unit_owned_token_id_episode_batch as parent


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

ADAPTER_SCHEMA_VERSION = "pif_canonical_v31_tagged_metric_token_id_episode_batch_v1"
RAW_OUTPUT_SCHEMA_VERSION = "pif_canonical_v31_tagged_metric_token_id_raw_output_v1"
PROJECTION_SCHEMA_VERSION = "pif_canonical_v31_tagged_metric_token_id_projection_v1"
PROVENANCE_SCHEMA_VERSION = "pif_canonical_v31_tagged_metric_token_id_provenance_v1"
FIDELITY_SCHEMA_VERSION = "pif_canonical_v31_tagged_metric_token_id_fidelity_v1"
CANDIDATE_SYSTEM_ID = "pif_tagged_metric_token_id_ai_discourse_v3_1_v1"
CANONICAL_SCHEMA_STRATEGY = "canonical_v31_explicit_tagged_metric_exact_token_id_v1"
TAGGED_METRIC_PROTOCOL_VERSION = "pif_explicit_sparse_metric_bundle_v1"
TAGGED_METRIC_FIELDS = (
    "direction",
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
    value = copy.deepcopy(parent.semantic_integrity_contract())
    value.update(
        {
            "reasoning_effort": EFFORT,
            "tagged_metric_protocol_version": TAGGED_METRIC_PROTOCOL_VERSION,
            "model_authors_metric_applicability_by_bundle_cardinality": True,
            "empty_metric_bundle_means_not_applicable": True,
            "nonempty_metric_bundle_requires_exact_raw_text_token_pair": True,
            "deterministic_tagged_metric_structural_projection_only": True,
            "deterministic_semantic_defaults": {},
            "deterministic_semantic_pruning": False,
            "deterministic_support_filtering": False,
            "deterministic_deduplication": False,
            "deterministic_relabeling": False,
        }
    )
    return value


def _tagged_metric_instructions() -> str:
    return (
        "# Explicit tagged metric transport\n"
        "Author metric applicability through the metric bundle array. Return an empty "
        "bundle only when the entire canonical metric is not applicable; that explicit "
        "empty choice projects to direction=not_applicable with all four metric literals "
        "null. Otherwise return exactly one bundle entry, choose a non-not_applicable "
        "direction, and provide the mandatory raw_text exact literal-token pair. The "
        "value, unit, and comparator pairs may be null only when that individual literal "
        "is absent from the selected evidence. This is an authored applicability choice, "
        "not a target-count hint. Never invent, pad, prune, or repair a metric.\n\n"
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
    values = parent.prepare_episode_batches(
        _episode_from_request(request),
        batch_size=int(request["batch_size_ceiling"]),
        thread_mode=str(request["thread_mode"]),
    )
    matches = [value for value in values if value["segment_ids"] == request["segment_ids"]]
    if len(matches) != 1:
        raise CanonicalV31EpisodeBatchError("tagged-metric parent request cannot be reconstructed")
    return matches[0]


def _metric_layout(request: Mapping[str, Any]) -> dict[str, Any]:
    layout = parent._layout(request)  # noqa: SLF001
    _, metric = parent._canonical_event_and_metric_schema(request)  # noqa: SLF001
    return {
        **layout,
        "metric_direction": metric["required"].index("direction"),
        "metric_field_count": len(metric["required"]),
    }


def _tagged_metric_schema(request: Mapping[str, Any]) -> dict[str, Any]:
    token_pair = parent._token_pair_schema(parent._token_ids(request))  # noqa: SLF001
    required_pair = copy.deepcopy(token_pair)
    required_pair["type"] = "object"
    _, canonical_metric = parent._canonical_event_and_metric_schema(request)  # noqa: SLF001
    direction = copy.deepcopy(canonical_metric["properties"]["direction"])
    direction["enum"] = [
        value for value in direction["enum"] if value != "not_applicable"
    ]
    entry = {
        "type": "object",
        "additionalProperties": False,
        "required": ["0", "1", "2", "3", "4"],
        "properties": {
            "0": direction,
            "1": copy.deepcopy(token_pair),
            "2": copy.deepcopy(token_pair),
            "3": copy.deepcopy(token_pair),
            "4": required_pair,
        },
    }
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["0"],
        "properties": {
            "0": {
                "type": "array",
                "maxItems": 1,
                "items": entry,
            }
        },
    }


def _output_schema(request: Mapping[str, Any]) -> dict[str, Any]:
    schema = copy.deepcopy(request["output_schema"])
    layout = _metric_layout(request)
    metric_node = (
        schema["properties"]["s"]["items"]["properties"]["2"]["items"]
        ["properties"]["0"]["items"]["properties"][str(layout["event_metric"])]
    )
    metric_node.clear()
    metric_node.update(_tagged_metric_schema(request))
    schema["$id"] = RAW_OUTPUT_SCHEMA_VERSION
    return schema


def _tagged_request(request: Mapping[str, Any]) -> dict[str, Any]:
    parent.validate_prepared_request(request)
    value = copy.deepcopy(dict(request))
    value["schema_version"] = ADAPTER_SCHEMA_VERSION
    value["candidate_system_id"] = CANDIDATE_SYSTEM_ID
    value["canonical_schema_strategy"] = CANONICAL_SCHEMA_STRATEGY
    value["effort"] = EFFORT
    value["batch_id"] = "cv31tmti_" + _sha256_text(
        _canonical_json(
            {
                "parent_batch_id": request["batch_id"],
                "candidate_system_id": CANDIDATE_SYSTEM_ID,
                "effort": EFFORT,
            }
        )
    )[:24]
    value["semantic_integrity"] = semantic_integrity_contract()
    value["private_input"]["schema_version"] = ADAPTER_SCHEMA_VERSION
    value["base_instructions"] = _tagged_metric_instructions() + value["base_instructions"]
    value["base_instructions_sha256"] = _sha256_text(value["base_instructions"])
    value["output_schema"] = _output_schema(request)
    value["output_schema_sha256"] = _sha256_text(_canonical_json(value["output_schema"]))
    return value


def prepare_episode_batches(
    episode: Mapping[str, Any], *, batch_size: int, thread_mode: str
) -> list[dict[str, Any]]:
    values = [
        _tagged_request(request)
        for request in parent.prepare_episode_batches(
            episode, batch_size=batch_size, thread_mode=thread_mode
        )
    ]
    for value in values:
        validate_prepared_request(value)
    return values


def validate_prepared_request(request: Mapping[str, Any]) -> dict[str, Any]:
    expected = _tagged_request(_parent_request(request))
    if dict(request) != expected:
        raise CanonicalV31EpisodeBatchError("tagged-metric token-ID request drifted")
    return copy.deepcopy(dict(request))


def _parent_wire_output(
    request: Mapping[str, Any], output: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, int]]:
    if not isinstance(output, Mapping) or set(output) != {"s"} or not isinstance(output["s"], list):
        raise CanonicalV31OutputError("tagged-metric output shape drifted")
    sources = request["private_input"]["segments"]
    if len(output["s"]) != len(sources):
        raise CanonicalV31OutputError("tagged-metric segment count drifted")
    parent_request = _parent_request(request)
    layout = _metric_layout(parent_request)
    converted = copy.deepcopy(dict(output))
    diagnostics = {
        "explicit_not_applicable_metric_bundle_count": 0,
        "explicit_applicable_metric_bundle_count": 0,
    }
    allowed_directions = {"increase", "decrease", "stable", "mixed", "unknown"}
    for segment in converted["s"]:
        unit_rows = segment.get("2") if isinstance(segment, Mapping) else None
        if not isinstance(unit_rows, list):
            raise CanonicalV31OutputError("tagged-metric unit rows drifted")
        for unit_row in unit_rows:
            events = unit_row.get("0") if isinstance(unit_row, Mapping) else None
            if not isinstance(events, list):
                raise CanonicalV31OutputError("tagged-metric event rows drifted")
            for event in events:
                if not isinstance(event, Mapping):
                    raise CanonicalV31OutputError("tagged-metric event is malformed")
                key = str(layout["event_metric"])
                tagged = event.get(key)
                if not isinstance(tagged, Mapping) or set(tagged) != {"0"}:
                    raise CanonicalV31OutputError("tagged metric is malformed")
                entries = tagged["0"]
                if not isinstance(entries, list) or len(entries) > 1:
                    raise CanonicalV31OutputError("tagged metric cardinality drifted")
                metric = {
                    str(index): None for index in range(layout["metric_field_count"])
                }
                if not entries:
                    metric[str(layout["metric_direction"])] = "not_applicable"
                    diagnostics["explicit_not_applicable_metric_bundle_count"] += 1
                else:
                    entry = entries[0]
                    if not isinstance(entry, Mapping) or set(entry) != {"0", "1", "2", "3", "4"}:
                        raise CanonicalV31OutputError("tagged metric entry is malformed")
                    if entry["0"] not in allowed_directions or not isinstance(entry["4"], Mapping):
                        raise CanonicalV31OutputError("tagged applicable metric contract failed")
                    metric[str(layout["metric_direction"])] = entry["0"]
                    for bundle_index, field in enumerate(parent.METRIC_RANGE_FIELDS, start=1):
                        metric[str(layout["metric_fields"][field])] = copy.deepcopy(
                            entry[str(bundle_index)]
                        )
                    diagnostics["explicit_applicable_metric_bundle_count"] += 1
                event[key] = metric
    return converted, diagnostics


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
            "tagged_metric_protocol_version": TAGGED_METRIC_PROTOCOL_VERSION,
        }
    )
    fidelity = copy.deepcopy(projected["fidelity"])
    fidelity.update(
        {
            "schema_version": FIDELITY_SCHEMA_VERSION,
            "candidate_system_id": CANDIDATE_SYSTEM_ID,
            "reasoning_effort": EFFORT,
            "tagged_metric_protocol_version": TAGGED_METRIC_PROTOCOL_VERSION,
            **diagnostics,
            "deterministic_tagged_metric_structural_projection_only": True,
            "deterministic_semantic_defaults": {},
            "deterministic_semantic_pruning": False,
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
    validated = validate_prepared_request(request)
    layout = _metric_layout(_parent_request(validated))
    value = copy.deepcopy(dict(parent_output))
    for segment in value["s"]:
        for unit_row in segment["2"]:
            for event in unit_row["0"]:
                key = str(layout["event_metric"])
                metric = event[key]
                direction = metric[str(layout["metric_direction"])]
                ranges = [
                    copy.deepcopy(metric[str(layout["metric_fields"][field])])
                    for field in parent.METRIC_RANGE_FIELDS
                ]
                if direction == "not_applicable" and all(item is None for item in ranges):
                    event[key] = {"0": []}
                elif direction != "not_applicable" and isinstance(ranges[-1], Mapping):
                    event[key] = {"0": [{"0": direction, **{str(i): item for i, item in enumerate(ranges, 1)}}]}
                else:
                    raise CanonicalV31OutputError("parent metric cannot enter tagged test transport")
    return value


def validate_turn_sidecar(
    request: Mapping[str, Any],
    sidecar_path: Path,
    *,
    output_path: Path,
    expected_thread: Any,
) -> dict[str, Any]:
    validated = validate_prepared_request(request)
    sidecar = base._load_json(sidecar_path, "tagged-metric sidecar")  # noqa: SLF001
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
        raise CanonicalV31TelemetryError("managed-auth tagged-metric sidecar contract failed")
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
        raise CanonicalV31TelemetryError("sidecar-bound tagged-metric output is absent or changed")
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
        "schema_version": "pif_canonical_v31_tagged_metric_token_id_binding_v1",
        "parent_binding": parent.build_six_arm_matrix_binding(),
        "parent_adapter_module_sha256": hashlib.sha256(Path(parent.__file__).read_bytes()).hexdigest(),
        "adapter_module_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "candidate_system_id": CANDIDATE_SYSTEM_ID,
        "canonical_schema_strategy": CANONICAL_SCHEMA_STRATEGY,
        "tagged_metric_protocol_version": TAGGED_METRIC_PROTOCOL_VERSION,
        "model": MODEL,
        "effort": EFFORT,
        "deterministic_semantic_defaults": {},
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
