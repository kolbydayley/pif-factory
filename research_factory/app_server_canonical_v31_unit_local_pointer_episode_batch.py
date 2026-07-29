from __future__ import annotations

"""Canonical v3.1 extraction with LLM-selected unit-local metric pointers."""

import copy
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_canonical_v31_bounded_span_episode_batch as bounded_span
from . import app_server_canonical_v31_literal_pointer_episode_batch as pointer


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

ADAPTER_SCHEMA_VERSION = "pif_canonical_v31_unit_local_pointer_episode_batch_v1"
RAW_OUTPUT_SCHEMA_VERSION = "pif_canonical_v31_unit_local_pointer_raw_output_v1"
PROJECTION_SCHEMA_VERSION = "pif_canonical_v31_unit_local_pointer_projection_v1"
PROVENANCE_SCHEMA_VERSION = "pif_canonical_v31_unit_local_pointer_provenance_v1"
FIDELITY_SCHEMA_VERSION = "pif_canonical_v31_unit_local_pointer_fidelity_v1"
CANDIDATE_SYSTEM_ID = "pif_unit_local_pointer_ai_discourse_v3_1_v1"
CANONICAL_SCHEMA_STRATEGY = (
    "canonical_v31_full_schema_with_unit_local_exact_metric_lexical_pointers_v1"
)
LOCAL_TOKENIZATION_VERSION = "pif_unit_local_exact_structural_tokens_v1"
METRIC_LITERAL_FIELDS = pointer.METRIC_LITERAL_FIELDS

_OLD_SPAN_METRIC_SENTENCE = pointer._OLD_SPAN_METRIC_SENTENCE  # noqa: SLF001
_OLD_AUDIT_METRIC_SENTENCES = pointer._OLD_AUDIT_METRIC_SENTENCES  # noqa: SLF001
_NEW_SPAN_METRIC_SENTENCE = (
    "For metric value, unit, comparator, and raw_text, return nullable start and "
    "end source_unit_id plus zero-based start and end literal-token index rather "
    "than rewriting source text. Indices use this exact structural rule inside "
    "each source unit: skip whitespace; each maximal alphanumeric-or-underscore "
    "run is one token; every other non-whitespace character is one token. The "
    "prompt gives each source unit's token count. Each non-null pointer range must "
    "be ordered and lie inside the selected event evidence span. Deterministic "
    "code will project only the selected exact source substring."
)
_NEW_AUDIT_METRIC_SENTENCES = (
    "For each metric, use the fully not-applicable form only when all source-unit "
    "IDs and token indices are null and direction is not_applicable. Any other "
    "direction or non-null metric pointer requires a non-null raw_text pointer. "
    "Every non-null pointer range must be ordered, valid under the stated unit-local "
    "tokenization rule, and fully contained in the selected event evidence span."
)


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _local_tokens(unit: Mapping[str, Any], *, unit_position: int) -> list[dict[str, Any]]:
    rows = pointer._literal_tokens(  # noqa: SLF001
        unit["text"], segment_position=unit_position
    )
    return [
        {
            **row,
            "start_char": unit["start_char"] + row["start_char"],
            "end_char": unit["start_char"] + row["end_char"],
        }
        for row in rows
    ]


def _pointer_base_instructions(value: str) -> str:
    if value.count(_OLD_SPAN_METRIC_SENTENCE) != 1:
        raise CanonicalV31EpisodeBatchError("bounded-span metric instructions drifted")
    if value.count(_OLD_AUDIT_METRIC_SENTENCES) != 1:
        raise CanonicalV31EpisodeBatchError("canonical metric audit instructions drifted")
    result = value.replace(_OLD_SPAN_METRIC_SENTENCE, _NEW_SPAN_METRIC_SENTENCE)
    result = result.replace(_OLD_AUDIT_METRIC_SENTENCES, _NEW_AUDIT_METRIC_SENTENCES)
    if _OLD_SPAN_METRIC_SENTENCE in result or _OLD_AUDIT_METRIC_SENTENCES in result:
        raise CanonicalV31EpisodeBatchError("stale free-form metric instructions remain")
    return result


def _bounded_base_instructions(value: str) -> str:
    if value.count(_NEW_SPAN_METRIC_SENTENCE) != 1:
        raise CanonicalV31EpisodeBatchError("unit-local pointer instructions drifted")
    if value.count(_NEW_AUDIT_METRIC_SENTENCES) != 1:
        raise CanonicalV31EpisodeBatchError("unit-local pointer audit drifted")
    return value.replace(_NEW_SPAN_METRIC_SENTENCE, _OLD_SPAN_METRIC_SENTENCE).replace(
        _NEW_AUDIT_METRIC_SENTENCES, _OLD_AUDIT_METRIC_SENTENCES
    )


def _pointer_prompt(value: str, segments: Sequence[Mapping[str, Any]]) -> str:
    prefix = "# Canonical v3.1 bounded evidence-span packet\n"
    if not value.startswith(prefix) or not value.endswith("\n"):
        raise CanonicalV31EpisodeBatchError("bounded-span prompt envelope drifted")
    packet = json.loads(value[len(prefix) : -1])
    if [row.get("segment_id") for row in packet.get("segments", [])] != [
        segment["segment_id"] for segment in segments
    ]:
        raise CanonicalV31EpisodeBatchError("unit-local prompt segment order drifted")
    for public_segment, private_segment in zip(packet["segments"], segments):
        if [row.get("unit_id") for row in public_segment["source_units"]] != [
            row["unit_id"] for row in private_segment["units"]
        ]:
            raise CanonicalV31EpisodeBatchError("unit-local prompt unit order drifted")
        for public_unit, private_unit in zip(
            public_segment["source_units"], private_segment["units"]
        ):
            public_unit["literal_token_count"] = len(private_unit["literal_tokens"])
    return "# Canonical v3.1 unit-local metric-pointer packet\n" + json.dumps(
        packet, ensure_ascii=True, separators=(",", ":")
    ) + "\n"


def _bounded_prompt(value: str) -> str:
    prefix = "# Canonical v3.1 unit-local metric-pointer packet\n"
    if not value.startswith(prefix) or not value.endswith("\n"):
        raise CanonicalV31EpisodeBatchError("unit-local prompt envelope drifted")
    packet = json.loads(value[len(prefix) : -1])
    for segment in packet.get("segments", []):
        for unit in segment.get("source_units", []):
            if not isinstance(unit.pop("literal_token_count", None), int):
                raise CanonicalV31EpisodeBatchError("unit-local token count is absent")
    return "# Canonical v3.1 bounded evidence-span packet\n" + json.dumps(
        packet, ensure_ascii=True, separators=(",", ":")
    ) + "\n"


def _pointer_metric_schema(
    metric: Mapping[str, Any], *, unit_ids: Sequence[str]
) -> dict[str, Any]:
    properties: dict[str, Any] = {
        "direction": copy.deepcopy(metric["properties"]["direction"])
    }
    required = ["direction"]
    for field in METRIC_LITERAL_FIELDS:
        for endpoint in ("start", "end"):
            unit_name = f"{field}_{endpoint}_source_unit_id"
            index_name = f"{field}_{endpoint}_token_index"
            properties[unit_name] = {
                "type": ["string", "null"],
                "enum": [None, *unit_ids],
            }
            properties[index_name] = {
                "type": ["integer", "null"],
                "minimum": 0,
            }
            required.extend((unit_name, index_name))
    return {
        "type": "object",
        "additionalProperties": False,
        "required": required,
        "properties": properties,
    }


def _pointer_schema(value: Mapping[str, Any], *, unit_ids: Sequence[str]) -> dict[str, Any]:
    result = copy.deepcopy(dict(value))
    result["$id"] = RAW_OUTPUT_SCHEMA_VERSION
    event = result["properties"]["segments"]["items"]["properties"][
        "discourse_events"
    ]["items"]
    event["properties"]["metric"] = _pointer_metric_schema(
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
            "model_selects_unit_local_exact_metric_token_ranges": True,
            "unit_local_tokenization_version": LOCAL_TOKENIZATION_VERSION,
            "deterministic_exact_metric_pointer_projection_only": True,
            "deterministic_coverage_owner_count_reconciliation_only": True,
            "model_authored_free_form_metric_literals": False,
            "deterministic_semantic_pruning": False,
            "deterministic_support_filtering": False,
            "deterministic_deduplication": False,
            "deterministic_relabeling": False,
            "semantic_regex_or_keyword_rules": False,
        }
    )
    return value


def _pointer_request(request: Mapping[str, Any]) -> dict[str, Any]:
    bounded_span.validate_prepared_request(request)
    value = copy.deepcopy(dict(request))
    unit_position = 0
    for segment in value["private_input"]["segments"]:
        for unit in segment["units"]:
            unit["literal_tokens"] = _local_tokens(unit, unit_position=unit_position)
            unit_position += 1
    unit_ids = [
        unit["unit_id"]
        for segment in value["private_input"]["segments"]
        for unit in segment["units"]
    ]
    value["schema_version"] = ADAPTER_SCHEMA_VERSION
    value["candidate_system_id"] = CANDIDATE_SYSTEM_ID
    value["canonical_schema_strategy"] = CANONICAL_SCHEMA_STRATEGY
    value["batch_id"] = "cv31ulp_" + base.sha256_text(
        _canonical_json(
            {
                "bounded_span_batch_id": request["batch_id"],
                "candidate_system_id": CANDIDATE_SYSTEM_ID,
            }
        )
    )[:24]
    value["semantic_integrity"] = semantic_integrity_contract()
    value["private_input"]["schema_version"] = ADAPTER_SCHEMA_VERSION
    value["prompt"] = _pointer_prompt(request["prompt"], value["private_input"]["segments"])
    value["base_instructions"] = _pointer_base_instructions(request["base_instructions"])
    value["output_schema"] = _pointer_schema(request["output_schema"], unit_ids=unit_ids)
    value["prompt_sha256"] = base.sha256_text(value["prompt"])
    value["base_instructions_sha256"] = base.sha256_text(value["base_instructions"])
    value["output_schema_sha256"] = base.sha256_text(
        _canonical_json(value["output_schema"])
    )
    return value


def _bounded_request(request: Mapping[str, Any]) -> dict[str, Any]:
    value = copy.deepcopy(dict(request))
    unit_position = 0
    for segment in value["private_input"]["segments"]:
        for unit in segment["units"]:
            tokens = unit.pop("literal_tokens", None)
            if tokens != _local_tokens(unit, unit_position=unit_position):
                raise CanonicalV31EpisodeBatchError("unit-local token projection drifted")
            unit_position += 1
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
    value["prompt"] = _bounded_prompt(value["prompt"])
    value["base_instructions"] = _bounded_base_instructions(value["base_instructions"])
    value["output_schema"] = _bounded_schema(value)
    value["prompt_sha256"] = base.sha256_text(value["prompt"])
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
    values = [_pointer_request(request) for request in requests]
    for value in values:
        validate_prepared_request(value)
    return values


def validate_prepared_request(request: Mapping[str, Any]) -> dict[str, Any]:
    bounded = _bounded_request(request)
    validated = bounded_span.validate_prepared_request(bounded)
    expected = _pointer_request(validated)
    if dict(request) != expected:
        raise CanonicalV31EpisodeBatchError("unit-local pointer request drifted")
    return copy.deepcopy(dict(request))


def _metric_literal(
    metric: Mapping[str, Any],
    field: str,
    *,
    source_segment: Mapping[str, Any],
    evidence_span: Mapping[str, Any],
) -> tuple[str | None, dict[str, Any]]:
    names = {
        "start_unit": f"{field}_start_source_unit_id",
        "end_unit": f"{field}_end_source_unit_id",
        "start_index": f"{field}_start_token_index",
        "end_index": f"{field}_end_token_index",
    }
    values = {role: metric[name] for role, name in names.items()}
    null_count = sum(value is None for value in values.values())
    if null_count not in (0, 4):
        raise CanonicalV31OutputError(f"metric {field} pointer is partially null")
    if null_count == 4:
        return None, {
            "field": field,
            **values,
            "start_char": None,
            "end_char": None,
            "projected_value_sha256": None,
        }
    units = source_segment["units"]
    by_id = {unit["unit_id"]: unit for unit in units}
    positions = {unit["unit_id"]: index for index, unit in enumerate(units)}
    start_unit = by_id.get(values["start_unit"])
    end_unit = by_id.get(values["end_unit"])
    if start_unit is None or end_unit is None:
        raise CanonicalV31OutputError(f"metric {field} pointer belongs elsewhere")
    if positions[values["start_unit"]] > positions[values["end_unit"]]:
        raise CanonicalV31OutputError(f"metric {field} source-unit range is reversed")
    start_index = values["start_index"]
    end_index = values["end_index"]
    if (
        isinstance(start_index, bool)
        or not isinstance(start_index, int)
        or isinstance(end_index, bool)
        or not isinstance(end_index, int)
        or start_index < 0
        or end_index < 0
        or start_index >= len(start_unit["literal_tokens"])
        or end_index >= len(end_unit["literal_tokens"])
    ):
        raise CanonicalV31OutputError(f"metric {field} token index is invalid")
    if start_unit is end_unit and start_index > end_index:
        raise CanonicalV31OutputError(f"metric {field} token range is reversed")
    start_token = start_unit["literal_tokens"][start_index]
    end_token = end_unit["literal_tokens"][end_index]
    start = start_token["start_char"]
    end = end_token["end_char"]
    if start < evidence_span["start_char"] or end > evidence_span["end_char"]:
        raise CanonicalV31OutputError(f"metric {field} token range is outside evidence")
    text = source_segment["segment_text"][start:end]
    if not text:
        raise CanonicalV31OutputError(f"metric {field} token range is empty")
    return text, {
        "field": field,
        **values,
        "start_char": start,
        "end_char": end,
        "start_token_id": start_token["literal_token_id"],
        "end_token_id": end_token["literal_token_id"],
        "projected_value_sha256": base.sha256_text(text),
    }


def _convert_metric_event(
    event: Mapping[str, Any], source_segment: Mapping[str, Any]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    spans = {
        span["evidence_span_id"]: span for span in source_segment["evidence_spans"]
    }
    span = spans.get(event.get("evidence_span_id"))
    if span is None:
        raise CanonicalV31OutputError("event evidence span belongs elsewhere")
    metric = event.get("metric")
    if not isinstance(metric, Mapping):
        raise CanonicalV31OutputError("event metric is malformed")
    canonical = {"direction": metric["direction"]}
    records = []
    for field in METRIC_LITERAL_FIELDS:
        value, record = _metric_literal(
            metric, field, source_segment=source_segment, evidence_span=span
        )
        canonical[field] = value
        records.append(record)
    any_literal = any(canonical[field] is not None for field in METRIC_LITERAL_FIELDS)
    if not any_literal and canonical["direction"] != "not_applicable":
        raise CanonicalV31OutputError(
            "metric without pointers must use direction=not_applicable"
        )
    if any_literal and canonical["raw_text"] is None:
        raise CanonicalV31OutputError("applicable metric requires raw_text pointer")
    value = copy.deepcopy(dict(event))
    value["metric"] = canonical
    return value, records


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
    metric_records: list[list[list[dict[str, Any]]]] = []
    for raw_segment, source_segment in zip(
        converted["segments"], validated["private_input"]["segments"]
    ):
        if raw_segment["segment_id"] != source_segment["segment_id"]:
            raise CanonicalV31OutputError("output segment order or identity drifted")
        converted_events = []
        segment_records = []
        for event in raw_segment["discourse_events"]:
            converted_event, records = _convert_metric_event(event, source_segment)
            converted_events.append(converted_event)
            segment_records.append(records)
        raw_segment["discourse_events"] = converted_events
        metric_records.append(segment_records)
    coverage_records = _reconcile_coverage_counts(converted, validated)
    projected = bounded_span.validate_and_project_output(
        _bounded_request(validated), converted
    )
    if dict(output) != untouched:
        raise CanonicalV31OutputError("unit-local projection mutated model output")
    provenance = copy.deepcopy(projected["provenance"])
    provenance["schema_version"] = PROVENANCE_SCHEMA_VERSION
    provenance["candidate_system_id"] = CANDIDATE_SYSTEM_ID
    provenance["unit_local_tokenization_version"] = LOCAL_TOKENIZATION_VERSION
    for segment, records in zip(provenance["segments"], metric_records):
        for event, event_records in zip(segment["discourse_events"], records):
            event["metric_unit_local_pointer_records"] = event_records
    provenance["deterministic_coverage_owner_count_records"] = coverage_records
    fidelity = copy.deepcopy(projected["fidelity"])
    fidelity["schema_version"] = FIDELITY_SCHEMA_VERSION
    fidelity["candidate_system_id"] = CANDIDATE_SYSTEM_ID
    fidelity["model_selected_unit_local_metric_pointers_projected_exactly"] = True
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
    sidecar = base._load_json(sidecar_path, "unit-local pointer sidecar")  # noqa: SLF001
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
            "managed-auth completed unit-local pointer sidecar contract failed"
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
        "schema_version": "pif_canonical_v31_unit_local_pointer_binding_v1",
        "bounded_span_adapter_binding": bounded_span.build_six_arm_matrix_binding(),
        "bounded_span_adapter_module_sha256": hashlib.sha256(
            Path(bounded_span.__file__).read_bytes()
        ).hexdigest(),
        "unit_local_pointer_adapter_module_sha256": hashlib.sha256(
            Path(__file__).read_bytes()
        ).hexdigest(),
        "candidate_system_id": CANDIDATE_SYSTEM_ID,
        "canonical_schema_strategy": CANONICAL_SCHEMA_STRATEGY,
        "unit_local_tokenization_version": LOCAL_TOKENIZATION_VERSION,
        "metric_literal_fields": list(METRIC_LITERAL_FIELDS),
        "model_selects_unit_local_exact_metric_token_ranges": True,
        "deterministic_exact_metric_pointer_projection_only": True,
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
    "LOCAL_TOKENIZATION_VERSION",
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
