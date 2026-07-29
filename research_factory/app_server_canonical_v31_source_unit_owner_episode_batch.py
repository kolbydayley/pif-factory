from __future__ import annotations

"""Canonical v3.1 extraction with LLM-owned exact metric source units."""

import copy
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_canonical_v31_bounded_span_episode_batch as bounded_span


base = bounded_span.base
PROJECT_ROOT = bounded_span.PROJECT_ROOT
MODEL = bounded_span.MODEL
EFFORT = bounded_span.EFFORT
PINNED_CODEX = bounded_span.PINNED_CODEX
CANONICAL_LABEL_PACK = bounded_span.CANONICAL_LABEL_PACK
CANONICAL_LABEL_SCHEMA_SHA256 = bounded_span.CANONICAL_LABEL_SCHEMA_SHA256
CANONICAL_EVIDENCE_MAX_CHARS = bounded_span.CANONICAL_EVIDENCE_MAX_CHARS
SOURCE_UNIT_MAX_CHARS = bounded_span.SOURCE_UNIT_MAX_CHARS
SUPPORTED_BATCH_SIZES = bounded_span.SUPPORTED_BATCH_SIZES
SUPPORTED_THREAD_MODES = bounded_span.SUPPORTED_THREAD_MODES
RETRY_COUNT = bounded_span.RETRY_COUNT
USAGE_FIELDS = bounded_span.USAGE_FIELDS
CanonicalV31EpisodeBatchError = bounded_span.CanonicalV31EpisodeBatchError
CanonicalV31OutputError = bounded_span.CanonicalV31OutputError
CanonicalV31TelemetryError = bounded_span.CanonicalV31TelemetryError
_client_factory = bounded_span._client_factory
_verify_started_thread = bounded_span._verify_started_thread
expected_instruction_source_contract = bounded_span.expected_instruction_source_contract
verified_context_control_overlay = bounded_span.verified_context_control_overlay

ADAPTER_SCHEMA_VERSION = "pif_canonical_v31_source_unit_owner_episode_batch_v1"
RAW_OUTPUT_SCHEMA_VERSION = "pif_canonical_v31_source_unit_owner_raw_output_v1"
PROJECTION_SCHEMA_VERSION = "pif_canonical_v31_source_unit_owner_projection_v1"
PROVENANCE_SCHEMA_VERSION = "pif_canonical_v31_source_unit_owner_provenance_v1"
FIDELITY_SCHEMA_VERSION = "pif_canonical_v31_source_unit_owner_fidelity_v1"
CANDIDATE_SYSTEM_ID = "pif_windowed_source_unit_owner_ai_discourse_v3_1_v1"
CANONICAL_SCHEMA_STRATEGY = (
    "canonical_v31_windowed_full_schema_with_llm_owned_exact_metric_source_units_v1"
)
METRIC_LITERAL_FIELDS = ("value", "unit", "comparator", "raw_text")

_OLD_SPAN_METRIC_SENTENCE = (
    "Every nonempty metric value, unit, comparator, and raw_text must be a "
    "literal substring of that selected evidence; raw_text must itself be a "
    "contiguous evidence substring."
)
_NEW_SPAN_METRIC_SENTENCE = (
    "For every non-null metric value, unit, comparator, and raw_text, also return "
    "that field's start_source_unit_id and end_source_unit_id and copy the field "
    "text exactly from the selected contiguous source-unit span. Both selected "
    "units must be inside the event evidence span. A null metric field must have "
    "both source-unit IDs null. Deterministic code will only prove the copied "
    "literal and its exact source offsets."
)
_OLD_AUDIT_METRIC_SENTENCES = (
    "For each metric, use the fully not-applicable form only when value, unit, "
    "comparator, and raw_text are all null and direction is not_applicable. Any "
    "other direction requires non-null raw_text that is an exact contiguous "
    "substring of the selected evidence, and every non-null value, unit, or "
    "comparator must occur in raw_text or that evidence."
)
_NEW_AUDIT_METRIC_SENTENCES = (
    "For each metric, use the fully not-applicable form only when value, unit, "
    "comparator, raw_text, and all eight source-unit ID fields are null and direction "
    "is not_applicable. Any other direction or non-null metric literal requires "
    "non-null raw_text plus raw_text start/end source-unit IDs. Every non-null "
    "literal must be copied exactly from its selected contiguous source-unit span "
    "inside the event evidence span."
)


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _owner_base_instructions(value: str) -> str:
    if value.count(_OLD_SPAN_METRIC_SENTENCE) != 1:
        raise CanonicalV31EpisodeBatchError("bounded-span metric instructions drifted")
    if value.count(_OLD_AUDIT_METRIC_SENTENCES) != 1:
        raise CanonicalV31EpisodeBatchError("canonical metric audit instructions drifted")
    result = value.replace(_OLD_SPAN_METRIC_SENTENCE, _NEW_SPAN_METRIC_SENTENCE)
    result = result.replace(_OLD_AUDIT_METRIC_SENTENCES, _NEW_AUDIT_METRIC_SENTENCES)
    if _OLD_SPAN_METRIC_SENTENCE in result or _OLD_AUDIT_METRIC_SENTENCES in result:
        raise CanonicalV31EpisodeBatchError("stale metric copy instructions remain")
    return result


def _bounded_base_instructions(value: str) -> str:
    if value.count(_NEW_SPAN_METRIC_SENTENCE) != 1:
        raise CanonicalV31EpisodeBatchError("metric source-unit instructions drifted")
    if value.count(_NEW_AUDIT_METRIC_SENTENCES) != 1:
        raise CanonicalV31EpisodeBatchError("metric source-unit audit drifted")
    return value.replace(_NEW_SPAN_METRIC_SENTENCE, _OLD_SPAN_METRIC_SENTENCE).replace(
        _NEW_AUDIT_METRIC_SENTENCES, _OLD_AUDIT_METRIC_SENTENCES
    )


def _owner_metric_schema(
    metric: Mapping[str, Any], *, unit_ids: Sequence[str]
) -> dict[str, Any]:
    result = copy.deepcopy(dict(metric))
    properties = result["properties"]
    required = result["required"]
    for field in METRIC_LITERAL_FIELDS:
        for endpoint in ("start", "end"):
            owner = f"{field}_{endpoint}_source_unit_id"
            properties[owner] = {
                "type": ["string", "null"],
                "enum": [None, *unit_ids],
                "description": (
                    "The selected "
                    + endpoint
                    + " source unit for the exact copied metric literal, or null "
                    "when the metric field is null."
                ),
            }
            required.append(owner)
    return result


def _owner_schema(value: Mapping[str, Any], *, unit_ids: Sequence[str]) -> dict[str, Any]:
    result = copy.deepcopy(dict(value))
    result["$id"] = RAW_OUTPUT_SCHEMA_VERSION
    event = result["properties"]["segments"]["items"]["properties"][
        "discourse_events"
    ]["items"]
    event["properties"]["metric"] = _owner_metric_schema(
        event["properties"]["metric"], unit_ids=unit_ids
    )
    return result


def _bounded_schema(request: Mapping[str, Any]) -> dict[str, Any]:
    segments = request["private_input"]["segments"]
    unit_ids = [unit["unit_id"] for segment in segments for unit in segment["units"]]
    span_ids = [
        span["evidence_span_id"]
        for segment in segments
        for span in segment["evidence_spans"]
    ]
    return bounded_span.build_output_schema(
        episode_id=request["episode_id"],
        segment_ids=request["segment_ids"],
        unit_ids=unit_ids,
        span_ids=span_ids,
        maximum_unit_count=max(len(segment["units"]) for segment in segments),
    )


def semantic_integrity_contract() -> dict[str, Any]:
    value = copy.deepcopy(bounded_span.semantic_integrity_contract())
    value.update(
        {
            "model_selects_source_unit_span_for_every_nonnull_metric_literal": True,
            "model_copies_every_metric_literal_exactly_from_selected_source_unit_span": True,
            "deterministic_exact_substring_and_offset_projection_only": True,
            "deterministic_coverage_owner_count_reconciliation_only": True,
            "deterministic_semantic_pruning": False,
            "deterministic_support_filtering": False,
            "deterministic_deduplication": False,
            "deterministic_relabeling": False,
            "semantic_regex_or_keyword_rules": False,
        }
    )
    return value


def _owner_request(request: Mapping[str, Any]) -> dict[str, Any]:
    bounded_span.validate_prepared_request(request)
    value = copy.deepcopy(dict(request))
    unit_ids = [
        unit["unit_id"]
        for segment in value["private_input"]["segments"]
        for unit in segment["units"]
    ]
    value["schema_version"] = ADAPTER_SCHEMA_VERSION
    value["candidate_system_id"] = CANDIDATE_SYSTEM_ID
    value["canonical_schema_strategy"] = CANONICAL_SCHEMA_STRATEGY
    value["batch_id"] = "cv31suo_" + base.sha256_text(
        _canonical_json(
            {
                "bounded_span_batch_id": request["batch_id"],
                "candidate_system_id": CANDIDATE_SYSTEM_ID,
            }
        )
    )[:24]
    value["semantic_integrity"] = semantic_integrity_contract()
    value["private_input"]["schema_version"] = ADAPTER_SCHEMA_VERSION
    value["base_instructions"] = _owner_base_instructions(request["base_instructions"])
    value["output_schema"] = _owner_schema(request["output_schema"], unit_ids=unit_ids)
    value["base_instructions_sha256"] = base.sha256_text(value["base_instructions"])
    value["output_schema_sha256"] = base.sha256_text(
        _canonical_json(value["output_schema"])
    )
    return value


def _bounded_request(request: Mapping[str, Any]) -> dict[str, Any]:
    value = copy.deepcopy(dict(request))
    value["schema_version"] = bounded_span.ADAPTER_SCHEMA_VERSION
    value["candidate_system_id"] = bounded_span.CANDIDATE_SYSTEM_ID
    value["canonical_schema_strategy"] = bounded_span.CANONICAL_SCHEMA_STRATEGY
    value["batch_id"] = bounded_span._batch_id(  # noqa: SLF001
        value["episode_id"],
        value["segment_ids"],
        batch_size=value["batch_size_ceiling"],
        thread_mode=value["thread_mode"],
        batch_index=value["batch_index"],
    )
    value["semantic_integrity"] = bounded_span.semantic_integrity_contract()
    value["private_input"]["schema_version"] = bounded_span.ADAPTER_SCHEMA_VERSION
    value["base_instructions"] = _bounded_base_instructions(value["base_instructions"])
    value["output_schema"] = _bounded_schema(value)
    value["base_instructions_sha256"] = base.sha256_text(value["base_instructions"])
    value["output_schema_sha256"] = base.sha256_text(
        _canonical_json(value["output_schema"])
    )
    return value


def prepare_episode_batches(
    episode: Mapping[str, Any], *, batch_size: int, thread_mode: str
) -> list[dict[str, Any]]:
    requests = bounded_span.prepare_episode_batches(
        episode, batch_size=batch_size, thread_mode=thread_mode
    )
    values = [_owner_request(request) for request in requests]
    for value in values:
        validate_prepared_request(value)
    return values


def validate_prepared_request(request: Mapping[str, Any]) -> dict[str, Any]:
    bounded = _bounded_request(request)
    validated = bounded_span.validate_prepared_request(bounded)
    expected = _owner_request(validated)
    if dict(request) != expected:
        raise CanonicalV31EpisodeBatchError("metric source-unit request drifted")
    return copy.deepcopy(dict(request))


def _metric_literal(
    metric: Mapping[str, Any],
    field: str,
    *,
    source_segment: Mapping[str, Any],
    evidence_span: Mapping[str, Any],
) -> tuple[str | None, dict[str, Any]]:
    value = metric[field]
    start_owner = metric[f"{field}_start_source_unit_id"]
    end_owner = metric[f"{field}_end_source_unit_id"]
    if value is None and (start_owner is not None or end_owner is not None):
        raise CanonicalV31OutputError(
            f"metric {field} and source-unit owners must all be null or non-null"
        )
    if value is not None and (start_owner is None or end_owner is None):
        raise CanonicalV31OutputError(
            f"metric {field} and source-unit owners must all be null or non-null"
        )
    if value is None:
        return None, {
            "field": field,
            "start_source_unit_id": None,
            "end_source_unit_id": None,
            "start_char": None,
            "end_char": None,
            "exact_occurrence_count": 0,
            "projected_value_sha256": None,
        }
    unit_rows = source_segment["units"]
    units = {unit["unit_id"]: unit for unit in unit_rows}
    start_unit = units.get(start_owner)
    end_unit = units.get(end_owner)
    if start_unit is None or end_unit is None:
        raise CanonicalV31OutputError(f"metric {field} source unit belongs elsewhere")
    positions = {unit["unit_id"]: index for index, unit in enumerate(unit_rows)}
    if positions[start_owner] > positions[end_owner]:
        raise CanonicalV31OutputError(f"metric {field} source-unit span is reversed")
    if not isinstance(value, str) or not value:
        raise CanonicalV31OutputError(f"metric {field} literal is empty")
    evidence = source_segment["segment_text"][
        evidence_span["start_char"] : evidence_span["end_char"]
    ]
    starts: list[int] = []
    cursor = 0
    while True:
        relative = evidence.find(value, cursor)
        if relative < 0:
            break
        starts.append(evidence_span["start_char"] + relative)
        cursor = relative + 1
    owned_occurrences = [
        start
        for start in starts
        if start_unit["start_char"] <= start < start_unit["end_char"]
        and end_unit["start_char"] < start + len(value) <= end_unit["end_char"]
    ]
    if not owned_occurrences:
        raise CanonicalV31OutputError(
            f"metric {field} is not an exact substring of its selected source-unit span and evidence"
        )
    start = owned_occurrences[0]
    projected = source_segment["segment_text"][start : start + len(value)]
    if projected != value:
        raise CanonicalV31OutputError(f"metric {field} exact projection drifted")
    return projected, {
        "field": field,
        "start_source_unit_id": start_owner,
        "end_source_unit_id": end_owner,
        "start_char": start,
        "end_char": start + len(value),
        "exact_occurrence_count": len(owned_occurrences),
        "selected_occurrence_index": 0,
        "projected_value_sha256": base.sha256_text(projected),
    }


def _convert_metric_event(
    event: Mapping[str, Any], source_segment: Mapping[str, Any]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    spans = {
        span["evidence_span_id"]: span for span in source_segment["evidence_spans"]
    }
    span_id = event.get("evidence_span_id")
    if span_id not in spans:
        raise CanonicalV31OutputError("event evidence span belongs to another segment")
    metric = event.get("metric")
    if not isinstance(metric, Mapping):
        raise CanonicalV31OutputError("event metric is malformed")
    canonical_metric: dict[str, Any] = {"direction": metric["direction"]}
    provenance = []
    for field in METRIC_LITERAL_FIELDS:
        projected, record = _metric_literal(
            metric,
            field,
            source_segment=source_segment,
            evidence_span=spans[span_id],
        )
        canonical_metric[field] = projected
        provenance.append(record)
    any_literal = any(
        canonical_metric[field] is not None for field in METRIC_LITERAL_FIELDS
    )
    if not any_literal and canonical_metric["direction"] != "not_applicable":
        raise CanonicalV31OutputError(
            "metric without source-unit literals must use direction=not_applicable"
        )
    if any_literal and canonical_metric["raw_text"] is None:
        raise CanonicalV31OutputError("applicable metric requires raw_text source owner")
    value = copy.deepcopy(dict(event))
    value["metric"] = canonical_metric
    return value, provenance


def _reconcile_coverage_counts(
    raw_output: dict[str, Any], request: Mapping[str, Any]
) -> list[dict[str, Any]]:
    records = []
    for raw_segment, source_segment in zip(
        raw_output["segments"], request["private_input"]["segments"]
    ):
        spans = {
            span["evidence_span_id"]: span
            for span in source_segment["evidence_spans"]
        }
        event_counts = {unit["unit_id"]: 0 for unit in source_segment["units"]}
        candidate_counts = {unit["unit_id"]: 0 for unit in source_segment["units"]}
        for event in raw_segment["discourse_events"]:
            event_counts[spans[event["evidence_span_id"]]["evidence_start_unit_id"]] += 1
        for candidate in raw_segment["concept_candidates"]:
            candidate_counts[
                spans[candidate["evidence_span_id"]]["evidence_start_unit_id"]
            ] += 1
        before = copy.deepcopy(raw_segment["unit_receipts"])
        for receipt in raw_segment["unit_receipts"]:
            unit_id = receipt["unit_id"]
            receipt["grounded_event_count"] = event_counts[unit_id]
            receipt["grounded_concept_candidate_count"] = candidate_counts[unit_id]
        records.append(
            {
                "segment_id": source_segment["segment_id"],
                "before_sha256": base.sha256_text(_canonical_json(before)),
                "after_sha256": base.sha256_text(
                    _canonical_json(raw_segment["unit_receipts"])
                ),
                "event_count": sum(event_counts.values()),
                "concept_candidate_count": sum(candidate_counts.values()),
            }
        )
    return records


def validate_and_project_output(
    request: Mapping[str, Any], output: Mapping[str, Any]
) -> dict[str, Any]:
    validated = validate_prepared_request(request)
    if not isinstance(output, Mapping):
        raise CanonicalV31OutputError("completed output must be an object")
    untouched = copy.deepcopy(dict(output))
    base._validate_schema(output, validated["output_schema"])  # noqa: SLF001
    converted = copy.deepcopy(dict(output))
    metric_provenance: list[list[list[dict[str, Any]]]] = []
    for raw_segment, source_segment in zip(
        converted["segments"], validated["private_input"]["segments"]
    ):
        if raw_segment["segment_id"] != source_segment["segment_id"]:
            raise CanonicalV31OutputError("output segment order or identity drifted")
        converted_events = []
        segment_provenance = []
        for event in raw_segment["discourse_events"]:
            converted_event, records = _convert_metric_event(event, source_segment)
            converted_events.append(converted_event)
            segment_provenance.append(records)
        raw_segment["discourse_events"] = converted_events
        metric_provenance.append(segment_provenance)
    coverage_records = _reconcile_coverage_counts(converted, validated)
    projected = bounded_span.validate_and_project_output(
        _bounded_request(validated), converted
    )
    if dict(output) != untouched:
        raise CanonicalV31OutputError("source-unit projection mutated model output")
    provenance = copy.deepcopy(projected["provenance"])
    provenance["schema_version"] = PROVENANCE_SCHEMA_VERSION
    provenance["candidate_system_id"] = CANDIDATE_SYSTEM_ID
    for segment, records in zip(provenance["segments"], metric_provenance):
        for event, metric_records in zip(segment["discourse_events"], records):
            event["metric_source_unit_records"] = metric_records
    provenance["deterministic_coverage_owner_count_records"] = coverage_records
    fidelity = copy.deepcopy(projected["fidelity"])
    fidelity["schema_version"] = FIDELITY_SCHEMA_VERSION
    fidelity["candidate_system_id"] = CANDIDATE_SYSTEM_ID
    fidelity["model_owned_metric_source_units_projected_exactly"] = True
    fidelity["deterministic_coverage_owner_counts_only"] = True
    fidelity["deterministic_metric_literal_defaults"] = {}
    fidelity["deterministic_semantic_pruning"] = False
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
    sidecar = base._load_json(sidecar_path, "source-unit-owner sidecar")  # noqa: SLF001
    if not isinstance(sidecar, Mapping):
        raise CanonicalV31TelemetryError("turn sidecar is not an object")
    thread_preflight = _verify_started_thread(expected_thread, validated)
    if (
        sidecar.get("schema_version")
        != base.codex_app_server.TURN_SIDECAR_SCHEMA_VERSION
        or not base._is_iso_timestamp(sidecar.get("started_at"))  # noqa: SLF001
        or not base._is_iso_timestamp(sidecar.get("finished_at"))  # noqa: SLF001
        or sidecar.get("client_version")
        != base.codex_app_server.APP_SERVER_CLIENT_VERSION
        or sidecar.get("cli_version")
        != base.codex_app_server.PINNED_CODEX_CLI_VERSION
        or sidecar.get("protocol_schema_sha256")
        != base._sha256_file(base.codex_app_server.PROTOCOL_SCHEMA_PATH)  # noqa: SLF001
        or sidecar.get("transport") != "stdio"
        or not isinstance(sidecar.get("app_server_user_agent"), str)
        or not sidecar.get("app_server_user_agent")
        or isinstance(sidecar.get("max_message_bytes"), bool)
        or not isinstance(sidecar.get("max_message_bytes"), int)
        or sidecar.get("max_message_bytes") < 64 * 1024
        or sidecar.get("synthetic_debug_errors") is not False
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
        or sidecar.get("base_instructions_sha256")
        != validated["base_instructions_sha256"]
        or sidecar.get("base_instructions_bytes")
        != len(validated["base_instructions"].encode("utf-8"))
        or sidecar.get("instruction_sources_sha256")
        != thread_preflight["instruction_sources_sha256"]
        or sidecar.get("instruction_sources_count")
        != thread_preflight["instruction_sources_count"]
        or sidecar.get("output_schema_sha256")
        != validated["output_schema_sha256"]
        or sidecar.get("output_schema_bytes")
        != len(_canonical_json(validated["output_schema"]).encode("utf-8"))
        or sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("error_class") is not None
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or sidecar.get("recovery_reran_model") is not False
        or not isinstance(sidecar.get("stderr_sha256"), str)
        or len(sidecar.get("stderr_sha256")) != 64
        or isinstance(sidecar.get("stderr_bytes"), bool)
        or not isinstance(sidecar.get("stderr_bytes"), int)
        or sidecar.get("stderr_bytes") < 0
    ):
        raise CanonicalV31TelemetryError(
            "managed-auth completed source-unit-owner sidecar contract failed"
        )
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
        raise CanonicalV31TelemetryError("sidecar-bound raw output is absent or changed")
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
        "schema_version": "pif_canonical_v31_source_unit_owner_binding_v1",
        "bounded_span_adapter_binding": bounded_span.build_six_arm_matrix_binding(),
        "bounded_span_adapter_module_sha256": hashlib.sha256(
            Path(bounded_span.__file__).read_bytes()
        ).hexdigest(),
        "source_unit_owner_adapter_module_sha256": hashlib.sha256(
            Path(__file__).read_bytes()
        ).hexdigest(),
        "candidate_system_id": CANDIDATE_SYSTEM_ID,
        "canonical_schema_strategy": CANONICAL_SCHEMA_STRATEGY,
        "metric_literal_fields": list(METRIC_LITERAL_FIELDS),
        "model_selects_source_unit_span_for_every_nonnull_metric_literal": True,
        "model_copies_every_metric_literal_exactly_from_selected_source_unit_span": True,
        "deterministic_exact_substring_and_offset_projection_only": True,
        "deterministic_coverage_owner_count_reconciliation_only": True,
        "semantic_postprocessing": False,
        "deterministic_semantic_pruning": False,
        "deterministic_deduplication": False,
        "deterministic_relabeling": False,
    }


__all__: Sequence[str] = (
    "ADAPTER_SCHEMA_VERSION",
    "CANONICAL_EVIDENCE_MAX_CHARS",
    "CANONICAL_LABEL_PACK",
    "CANONICAL_SCHEMA_STRATEGY",
    "CANDIDATE_SYSTEM_ID",
    "EFFORT",
    "METRIC_LITERAL_FIELDS",
    "MODEL",
    "PINNED_CODEX",
    "PROJECT_ROOT",
    "SOURCE_UNIT_MAX_CHARS",
    "USAGE_FIELDS",
    "build_six_arm_matrix_binding",
    "expected_instruction_source_contract",
    "prepare_episode_batches",
    "semantic_integrity_contract",
    "validate_and_project_output",
    "validate_prepared_request",
    "validate_turn_sidecar",
    "verified_context_control_overlay",
)
