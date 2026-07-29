from __future__ import annotations

"""Canonical v3.1 adapter with LLM-selected exact-source metric literals.

The model retains every semantic decision, including whether a metric applies,
its direction, and the exact source tokens that express value, unit,
comparator, and raw text.  It emits opaque token ranges instead of rewriting
those source literals.  Deterministic code only maps the selected token IDs to
the corresponding exact substring before canonical validation.
"""

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

ADAPTER_SCHEMA_VERSION = "pif_canonical_v31_literal_pointer_episode_batch_adapter_v1"
RAW_OUTPUT_SCHEMA_VERSION = "pif_canonical_v31_literal_pointer_raw_output_v1"
PROJECTION_SCHEMA_VERSION = "pif_canonical_v31_literal_pointer_projection_v1"
PROVENANCE_SCHEMA_VERSION = "pif_canonical_v31_literal_pointer_provenance_v1"
FIDELITY_SCHEMA_VERSION = "pif_canonical_v31_literal_pointer_fidelity_v1"
CANDIDATE_SYSTEM_ID = "pif_direct_full_canonical_literal_pointer_ai_discourse_v3_1_v1"
CANONICAL_SCHEMA_STRATEGY = (
    "canonical_v31_bounded_evidence_and_llm_selected_exact_metric_literal_pointers_v1"
)
LITERAL_TOKENIZATION_VERSION = "pif_exact_structural_literal_tokens_v1"
METRIC_LITERAL_FIELDS = ("value", "unit", "comparator", "raw_text")

_OLD_SPAN_METRIC_SENTENCE = (
    "Every nonempty metric value, unit, comparator, and raw_text must be a "
    "literal substring of that selected evidence; raw_text must itself be a "
    "contiguous evidence substring."
)
_NEW_SPAN_METRIC_SENTENCE = (
    "For metric value, unit, comparator, and raw_text, return the corresponding "
    "nullable zero-based start/end indices into that segment's ordered "
    "literal_token_texts array rather than rewriting source text. Each non-null "
    "pair must be ordered and lie inside the selected event evidence span. "
    "Deterministic code will project the model-selected token range to the exact "
    "source substring."
)
_OLD_AUDIT_METRIC_SENTENCES = (
    "For each metric, use the fully not-applicable form only when value, unit, "
    "comparator, and raw_text are all null and direction is not_applicable. Any "
    "other direction requires non-null raw_text that is an exact contiguous "
    "substring of the selected evidence, and every non-null value, unit, or "
    "comparator must occur in raw_text or that evidence."
)
_NEW_AUDIT_METRIC_SENTENCES = (
    "For each metric, use the fully not-applicable form only when all metric "
    "literal-token start/end indices are null and direction is not_applicable. Any "
    "other direction or non-null metric literal requires a non-null raw_text "
    "token range, and every non-null token pair must be fully contained in the "
    "selected event evidence span."
)


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _literal_tokens(text: str, *, segment_position: int) -> list[dict[str, Any]]:
    """Create exact lexical pointers using only character classes and offsets."""

    tokens: list[dict[str, Any]] = []
    cursor = 0
    while cursor < len(text):
        if text[cursor].isspace():
            cursor += 1
            continue
        start = cursor
        if text[cursor].isalnum() or text[cursor] == "_":
            cursor += 1
            while cursor < len(text) and (
                text[cursor].isalnum() or text[cursor] == "_"
            ):
                cursor += 1
        else:
            cursor += 1
        token_text = text[start:cursor]
        tokens.append(
            {
                "literal_token_index": len(tokens),
                "literal_token_id": f"L{segment_position:04d}T{len(tokens):05d}",
                "start_char": start,
                "end_char": cursor,
                "text": token_text,
            }
        )
    if not tokens or len({token["literal_token_id"] for token in tokens}) != len(tokens):
        raise CanonicalV31EpisodeBatchError("literal token projection is invalid")
    represented = [False] * len(text)
    for token in tokens:
        if text[token["start_char"] : token["end_char"]] != token["text"]:
            raise CanonicalV31EpisodeBatchError("literal token offsets drifted")
        for index in range(token["start_char"], token["end_char"]):
            if represented[index]:
                raise CanonicalV31EpisodeBatchError("literal token projection overlaps")
            represented[index] = True
    if any(not represented[index] for index, char in enumerate(text) if not char.isspace()):
        raise CanonicalV31EpisodeBatchError("literal token projection omitted source content")
    return tokens


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
        raise CanonicalV31EpisodeBatchError("metric pointer instructions drifted")
    if value.count(_NEW_AUDIT_METRIC_SENTENCES) != 1:
        raise CanonicalV31EpisodeBatchError("metric pointer audit instructions drifted")
    result = value.replace(_NEW_SPAN_METRIC_SENTENCE, _OLD_SPAN_METRIC_SENTENCE)
    result = result.replace(_NEW_AUDIT_METRIC_SENTENCES, _OLD_AUDIT_METRIC_SENTENCES)
    return result


def _pointer_prompt(value: str, segments: Sequence[Mapping[str, Any]]) -> str:
    prefix = "# Canonical v3.1 bounded evidence-span packet\n"
    if not value.startswith(prefix) or not value.endswith("\n"):
        raise CanonicalV31EpisodeBatchError("bounded-span prompt envelope drifted")
    packet = json.loads(value[len(prefix) : -1])
    if not isinstance(packet, dict) or not isinstance(packet.get("segments"), list):
        raise CanonicalV31EpisodeBatchError("bounded-span prompt packet drifted")
    if [row.get("segment_id") for row in packet["segments"]] != [
        segment["segment_id"] for segment in segments
    ]:
        raise CanonicalV31EpisodeBatchError("metric pointer prompt segment order drifted")
    for row, segment in zip(packet["segments"], segments):
        row["literal_token_texts"] = [
            token["text"] for token in segment["literal_tokens"]
        ]
    return "# Canonical v3.1 bounded evidence and metric-literal pointer packet\n" + json.dumps(
        packet, ensure_ascii=True, separators=(",", ":")
    ) + "\n"


def _bounded_prompt(value: str) -> str:
    prefix = "# Canonical v3.1 bounded evidence and metric-literal pointer packet\n"
    if not value.startswith(prefix) or not value.endswith("\n"):
        raise CanonicalV31EpisodeBatchError("metric pointer prompt envelope drifted")
    packet = json.loads(value[len(prefix) : -1])
    if not isinstance(packet, dict) or not isinstance(packet.get("segments"), list):
        raise CanonicalV31EpisodeBatchError("metric pointer prompt packet drifted")
    for row in packet["segments"]:
        if not isinstance(row, dict) or "literal_token_texts" not in row:
            raise CanonicalV31EpisodeBatchError("metric pointer prompt tokens are absent")
        row.pop("literal_token_texts")
    return "# Canonical v3.1 bounded evidence-span packet\n" + json.dumps(
        packet, ensure_ascii=True, separators=(",", ":")
    ) + "\n"


def _pointer_metric_schema(metric: Mapping[str, Any]) -> dict[str, Any]:
    direction = copy.deepcopy(metric["properties"]["direction"])
    properties: dict[str, Any] = {"direction": direction}
    required = ["direction"]
    for field in METRIC_LITERAL_FIELDS:
        for endpoint in ("start", "end"):
            name = f"{field}_{endpoint}_token_index"
            properties[name] = {
                "type": ["integer", "null"],
                "minimum": 0,
                "description": (
                    "Zero-based index into this segment's ordered "
                    "literal_token_texts array, or null when the canonical "
                    "metric field is null."
                ),
            }
            required.append(name)
    return {
        "type": "object",
        "additionalProperties": False,
        "required": required,
        "properties": properties,
    }


def _pointer_schema(value: Mapping[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(dict(value))
    result["$id"] = RAW_OUTPUT_SCHEMA_VERSION
    event = result["properties"]["segments"]["items"]["properties"][
        "discourse_events"
    ]["items"]
    event["properties"]["metric"] = _pointer_metric_schema(
        event["properties"]["metric"]
    )
    return result


def _bounded_schema(value: Mapping[str, Any], request: Mapping[str, Any]) -> dict[str, Any]:
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
            "model_selects_exact_metric_literal_token_ranges": True,
            "literal_tokenization_version": LITERAL_TOKENIZATION_VERSION,
            "deterministic_exact_metric_literal_projection_only": True,
            "model_authored_free_form_metric_literals": False,
        }
    )
    return value


def _pointer_request(request: Mapping[str, Any]) -> dict[str, Any]:
    bounded_span.validate_prepared_request(request)
    value = copy.deepcopy(dict(request))
    segments = value["private_input"]["segments"]
    for segment in segments:
        segment["literal_tokens"] = _literal_tokens(
            segment["segment_text"], segment_position=segment["segment_position"]
        )
    value["schema_version"] = ADAPTER_SCHEMA_VERSION
    value["candidate_system_id"] = CANDIDATE_SYSTEM_ID
    value["canonical_schema_strategy"] = CANONICAL_SCHEMA_STRATEGY
    value["batch_id"] = "cv31lp_" + base.sha256_text(
        _canonical_json(
            {
                "bounded_span_batch_id": request["batch_id"],
                "candidate_system_id": CANDIDATE_SYSTEM_ID,
            }
        )
    )[:24]
    value["semantic_integrity"] = semantic_integrity_contract()
    value["private_input"]["schema_version"] = ADAPTER_SCHEMA_VERSION
    value["prompt"] = _pointer_prompt(request["prompt"], segments)
    value["base_instructions"] = _pointer_base_instructions(
        request["base_instructions"]
    )
    value["output_schema"] = _pointer_schema(request["output_schema"])
    value["prompt_sha256"] = base.sha256_text(value["prompt"])
    value["base_instructions_sha256"] = base.sha256_text(value["base_instructions"])
    value["output_schema_sha256"] = base.sha256_text(
        _canonical_json(value["output_schema"])
    )
    return value


def _bounded_request(request: Mapping[str, Any]) -> dict[str, Any]:
    value = copy.deepcopy(dict(request))
    for segment in value["private_input"]["segments"]:
        tokens = segment.pop("literal_tokens", None)
        expected_tokens = _literal_tokens(
            segment["segment_text"], segment_position=segment["segment_position"]
        )
        if tokens != expected_tokens:
            raise CanonicalV31EpisodeBatchError("literal token projection drifted")
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
    value["base_instructions"] = _bounded_base_instructions(
        value["base_instructions"]
    )
    value["output_schema"] = _bounded_schema(value["output_schema"], value)
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
    pointer_requests = [_pointer_request(request) for request in requests]
    for request in pointer_requests:
        validate_prepared_request(request)
    return pointer_requests


def validate_prepared_request(request: Mapping[str, Any]) -> dict[str, Any]:
    bounded = _bounded_request(request)
    validated = bounded_span.validate_prepared_request(bounded)
    expected = _pointer_request(validated)
    if dict(request) != expected:
        raise CanonicalV31EpisodeBatchError("metric literal pointer request drifted")
    return copy.deepcopy(dict(request))


def _metric_literal(
    metric: Mapping[str, Any],
    field: str,
    *,
    source_segment: Mapping[str, Any],
    evidence_span: Mapping[str, Any],
) -> tuple[str | None, dict[str, Any]]:
    start_name = f"{field}_start_token_index"
    end_name = f"{field}_end_token_index"
    start_index = metric[start_name]
    end_index = metric[end_name]
    if (start_index is None) != (end_index is None):
        raise CanonicalV31OutputError(f"metric {field} token endpoints must both be null or non-null")
    if start_index is None:
        return None, {
            "field": field,
            "start_token_index": None,
            "end_token_index": None,
            "projected_value_sha256": None,
        }
    tokens = source_segment["literal_tokens"]
    if (
        isinstance(start_index, bool)
        or not isinstance(start_index, int)
        or isinstance(end_index, bool)
        or not isinstance(end_index, int)
        or start_index < 0
        or end_index < 0
        or start_index >= len(tokens)
        or end_index >= len(tokens)
    ):
        raise CanonicalV31OutputError(f"metric {field} token belongs to another segment")
    if start_index > end_index:
        raise CanonicalV31OutputError(f"metric {field} token range is reversed")
    start_token = tokens[start_index]
    end_token = tokens[end_index]
    start = start_token["start_char"]
    end = end_token["end_char"]
    if start < evidence_span["start_char"] or end > evidence_span["end_char"]:
        raise CanonicalV31OutputError(f"metric {field} token range is outside event evidence")
    text = source_segment["segment_text"][start:end]
    if not text:
        raise CanonicalV31OutputError(f"metric {field} token range is empty")
    return text, {
        "field": field,
        "start_token_index": start_index,
        "end_token_index": end_index,
        "start_token_id": start_token["literal_token_id"],
        "end_token_id": end_token["literal_token_id"],
        "start_char": start,
        "end_char": end,
        "projected_value_sha256": base.sha256_text(text),
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
    provenance: list[dict[str, Any]] = []
    for field in METRIC_LITERAL_FIELDS:
        value, record = _metric_literal(
            metric,
            field,
            source_segment=source_segment,
            evidence_span=spans[span_id],
        )
        canonical_metric[field] = value
        provenance.append(record)
    any_literal = any(canonical_metric[field] is not None for field in METRIC_LITERAL_FIELDS)
    if not any_literal and canonical_metric["direction"] != "not_applicable":
        raise CanonicalV31OutputError(
            "metric without literal pointers must use direction=not_applicable"
        )
    if any_literal and canonical_metric["raw_text"] is None:
        raise CanonicalV31OutputError("applicable metric requires raw_text token pointers")
    value = copy.deepcopy(dict(event))
    value["metric"] = canonical_metric
    return value, provenance


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
    projected = bounded_span.validate_and_project_output(
        _bounded_request(validated), converted
    )
    if dict(output) != untouched:
        raise CanonicalV31OutputError("metric pointer projection mutated model output")
    provenance = copy.deepcopy(projected["provenance"])
    provenance["schema_version"] = PROVENANCE_SCHEMA_VERSION
    provenance["candidate_system_id"] = CANDIDATE_SYSTEM_ID
    provenance["metric_literal_tokenization_version"] = LITERAL_TOKENIZATION_VERSION
    for segment, records in zip(provenance["segments"], metric_provenance):
        for event, metric_records in zip(segment["discourse_events"], records):
            event["metric_literal_pointers"] = metric_records
    fidelity = copy.deepcopy(projected["fidelity"])
    fidelity["schema_version"] = FIDELITY_SCHEMA_VERSION
    fidelity["candidate_system_id"] = CANDIDATE_SYSTEM_ID
    fidelity["model_selected_metric_literal_pointers_projected_exactly"] = True
    fidelity["deterministic_metric_literal_defaults"] = {}
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
    """Validate one managed-auth turn against the pointer request byte stream."""

    validated = validate_prepared_request(request)
    sidecar = base._load_json(sidecar_path, "literal-pointer turn sidecar")  # noqa: SLF001
    if not isinstance(sidecar, Mapping):
        raise CanonicalV31TelemetryError("turn sidecar is not an object")
    thread_preflight = bounded_span._verify_started_thread(  # noqa: SLF001
        expected_thread, validated
    )
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
            "managed-auth completed literal-pointer sidecar contract failed"
        )
    usage = base._usage_values(sidecar.get("usage"), "usage")  # noqa: SLF001
    total = base._usage_values(  # noqa: SLF001
        sidecar.get("thread_total_usage"), "thread_total_usage"
    )
    if any(total[field] < usage[field] for field in USAGE_FIELDS):
        raise CanonicalV31TelemetryError("thread total usage is below turn usage")
    if validated["thread_mode"] == "new_thread" and total != usage:
        raise CanonicalV31TelemetryError("new-thread total usage differs from turn usage")
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
            "sidecar-bound literal-pointer raw output is absent or changed"
        )
    return {
        "sidecar": copy.deepcopy(dict(sidecar)),
        "thread_id": sidecar["thread_id"],
        "turn_id": sidecar["turn_id"],
        "usage": usage,
        "thread_total_usage": total,
        "wall_elapsed_seconds": wall,
        "cached_input_tokens": usage["cached_input_tokens"],
        "reasoning_output_tokens": usage["reasoning_output_tokens"],
    }


def build_six_arm_matrix_binding() -> dict[str, Any]:
    return {
        "schema_version": "pif_canonical_v31_literal_pointer_adapter_binding_v1",
        "bounded_span_adapter_binding": bounded_span.build_six_arm_matrix_binding(),
        "bounded_span_adapter_module_sha256": hashlib.sha256(
            Path(bounded_span.__file__).read_bytes()
        ).hexdigest(),
        "literal_pointer_adapter_module_sha256": hashlib.sha256(
            Path(__file__).read_bytes()
        ).hexdigest(),
        "candidate_system_id": CANDIDATE_SYSTEM_ID,
        "literal_tokenization_version": LITERAL_TOKENIZATION_VERSION,
        "metric_literal_fields": list(METRIC_LITERAL_FIELDS),
        "model_selects_exact_metric_literal_token_ranges": True,
        "deterministic_exact_metric_literal_projection_only": True,
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
    "LITERAL_TOKENIZATION_VERSION",
    "METRIC_LITERAL_FIELDS",
    "MODEL",
    "PINNED_CODEX",
    "PROJECT_ROOT",
    "SOURCE_UNIT_MAX_CHARS",
    "build_six_arm_matrix_binding",
    "expected_instruction_source_contract",
    "prepare_episode_batches",
    "semantic_integrity_contract",
    "validate_and_project_output",
    "validate_prepared_request",
    "validate_turn_sidecar",
    "verified_context_control_overlay",
)
