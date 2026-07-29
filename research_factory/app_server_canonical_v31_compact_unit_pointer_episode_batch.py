from __future__ import annotations

"""Canonical v3.1 extraction with compact unit-local metric ranges."""

import copy
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_canonical_v31_unit_local_pointer_episode_batch as local


base = local.base
PROJECT_ROOT = local.PROJECT_ROOT
MODEL = local.MODEL
EFFORT = local.EFFORT
PINNED_CODEX = local.PINNED_CODEX
CANONICAL_LABEL_PACK = local.CANONICAL_LABEL_PACK
CANONICAL_LABEL_SCHEMA_SHA256 = local.CANONICAL_LABEL_SCHEMA_SHA256
CANONICAL_EVIDENCE_MAX_CHARS = local.CANONICAL_EVIDENCE_MAX_CHARS
SOURCE_UNIT_MAX_CHARS = local.SOURCE_UNIT_MAX_CHARS
SUPPORTED_BATCH_SIZES = local.SUPPORTED_BATCH_SIZES
SUPPORTED_THREAD_MODES = local.SUPPORTED_THREAD_MODES
RETRY_COUNT = local.RETRY_COUNT
USAGE_FIELDS = local.USAGE_FIELDS
CanonicalV31EpisodeBatchError = local.CanonicalV31EpisodeBatchError
CanonicalV31OutputError = local.CanonicalV31OutputError
CanonicalV31TelemetryError = local.CanonicalV31TelemetryError
_client_factory = local._client_factory
_verify_started_thread = local._verify_started_thread
expected_instruction_source_contract = local.expected_instruction_source_contract
verified_context_control_overlay = local.verified_context_control_overlay

ADAPTER_SCHEMA_VERSION = "pif_canonical_v31_compact_unit_pointer_episode_batch_v1"
RAW_OUTPUT_SCHEMA_VERSION = "pif_canonical_v31_compact_unit_pointer_raw_output_v1"
PROJECTION_SCHEMA_VERSION = "pif_canonical_v31_compact_unit_pointer_projection_v1"
PROVENANCE_SCHEMA_VERSION = "pif_canonical_v31_compact_unit_pointer_provenance_v1"
FIDELITY_SCHEMA_VERSION = "pif_canonical_v31_compact_unit_pointer_fidelity_v1"
CANDIDATE_SYSTEM_ID = "pif_compact_unit_pointer_ai_discourse_v3_1_v1"
CANONICAL_SCHEMA_STRATEGY = (
    "canonical_v31_full_schema_with_compact_positional_unit_local_metric_ranges_v1"
)
COMPACT_RANGE_VERSION = "pif_compact_unit_local_metric_range_v1"
METRIC_LITERAL_FIELDS = local.METRIC_LITERAL_FIELDS

_OLD_METRIC_SENTENCE = local._NEW_SPAN_METRIC_SENTENCE  # noqa: SLF001
_OLD_METRIC_AUDIT = local._NEW_AUDIT_METRIC_SENTENCES  # noqa: SLF001
_NEW_METRIC_SENTENCE = (
    "For metric value, unit, comparator, and raw_text, return one nullable compact "
    "range for each named field rather than rewriting source text. A non-null range "
    "is exactly [start_source_unit_index, start_token_index, end_source_unit_index, "
    "end_token_index]. Source-unit indices are given in the packet. Token indices "
    "are zero-based under this exact structural rule inside each source unit: skip "
    "whitespace; each maximal alphanumeric-or-underscore run is one token; every "
    "other non-whitespace character is one token. Each range must be ordered and lie "
    "inside the selected event evidence span. Deterministic code projects only the "
    "selected exact source substring."
)
_NEW_METRIC_AUDIT = (
    "For each metric, use four null ranges only when direction is not_applicable. "
    "Any other direction or non-null value_range, unit_range, or comparator_range "
    "requires a non-null raw_text_range. Every non-null four-integer range must be "
    "ordered, valid under the stated unit-local tokenization rule, and fully "
    "contained in the selected event evidence span."
)


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _compact_base_instructions(value: str) -> str:
    if value.count(_OLD_METRIC_SENTENCE) != 1:
        raise CanonicalV31EpisodeBatchError("unit-local metric instructions drifted")
    if value.count(_OLD_METRIC_AUDIT) != 1:
        raise CanonicalV31EpisodeBatchError("unit-local metric audit drifted")
    result = value.replace(_OLD_METRIC_SENTENCE, _NEW_METRIC_SENTENCE)
    result = result.replace(_OLD_METRIC_AUDIT, _NEW_METRIC_AUDIT)
    if _OLD_METRIC_SENTENCE in result or _OLD_METRIC_AUDIT in result:
        raise CanonicalV31EpisodeBatchError("stale verbose metric instructions remain")
    return result


def _local_base_instructions(value: str) -> str:
    if value.count(_NEW_METRIC_SENTENCE) != 1 or value.count(_NEW_METRIC_AUDIT) != 1:
        raise CanonicalV31EpisodeBatchError("compact metric instructions drifted")
    return value.replace(_NEW_METRIC_SENTENCE, _OLD_METRIC_SENTENCE).replace(
        _NEW_METRIC_AUDIT, _OLD_METRIC_AUDIT
    )


def _compact_prompt(value: str, segments: Sequence[Mapping[str, Any]]) -> str:
    prefix = "# Canonical v3.1 unit-local metric-pointer packet\n"
    if not value.startswith(prefix) or not value.endswith("\n"):
        raise CanonicalV31EpisodeBatchError("unit-local prompt envelope drifted")
    packet = json.loads(value[len(prefix) : -1])
    if [row.get("segment_id") for row in packet.get("segments", [])] != [
        segment["segment_id"] for segment in segments
    ]:
        raise CanonicalV31EpisodeBatchError("compact prompt segment order drifted")
    for public_segment, private_segment in zip(packet["segments"], segments):
        if [row.get("unit_id") for row in public_segment["source_units"]] != [
            row["unit_id"] for row in private_segment["units"]
        ]:
            raise CanonicalV31EpisodeBatchError("compact prompt unit order drifted")
        for unit_index, public_unit in enumerate(public_segment["source_units"]):
            public_unit["source_unit_index"] = unit_index
    return "# Canonical v3.1 compact unit-pointer packet\n" + json.dumps(
        packet, ensure_ascii=True, separators=(",", ":")
    ) + "\n"


def _local_prompt(value: str) -> str:
    prefix = "# Canonical v3.1 compact unit-pointer packet\n"
    if not value.startswith(prefix) or not value.endswith("\n"):
        raise CanonicalV31EpisodeBatchError("compact prompt envelope drifted")
    packet = json.loads(value[len(prefix) : -1])
    for segment in packet.get("segments", []):
        for unit_index, unit in enumerate(segment.get("source_units", [])):
            if unit.pop("source_unit_index", None) != unit_index:
                raise CanonicalV31EpisodeBatchError("compact source-unit index drifted")
    return "# Canonical v3.1 unit-local metric-pointer packet\n" + json.dumps(
        packet, ensure_ascii=True, separators=(",", ":")
    ) + "\n"


def _compact_range_schema() -> dict[str, Any]:
    return {
        "type": ["array", "null"],
        "minItems": 4,
        "maxItems": 4,
        "items": {"type": "integer", "minimum": 0},
    }


def _compact_schema(value: Mapping[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(dict(value))
    result["$id"] = RAW_OUTPUT_SCHEMA_VERSION
    event = result["properties"]["segments"]["items"]["properties"][
        "discourse_events"
    ]["items"]
    direction = copy.deepcopy(event["properties"]["metric"]["properties"]["direction"])
    range_fields = [f"{field}_range" for field in METRIC_LITERAL_FIELDS]
    event["properties"]["metric"] = {
        "type": "object",
        "additionalProperties": False,
        "required": ["direction", *range_fields],
        "properties": {
            "direction": direction,
            **{field: _compact_range_schema() for field in range_fields},
        },
    }
    return result


def semantic_integrity_contract() -> dict[str, Any]:
    value = copy.deepcopy(local.semantic_integrity_contract())
    value.update(
        {
            "model_selects_compact_positional_unit_local_metric_ranges": True,
            "compact_range_version": COMPACT_RANGE_VERSION,
            "model_authored_free_form_metric_literals": False,
            "deterministic_exact_metric_pointer_projection_only": True,
            "deterministic_coverage_owner_count_reconciliation_only": True,
            "deterministic_semantic_pruning": False,
            "deterministic_support_filtering": False,
            "deterministic_deduplication": False,
            "deterministic_relabeling": False,
            "semantic_regex_or_keyword_rules": False,
        }
    )
    return value


def _compact_request(request: Mapping[str, Any]) -> dict[str, Any]:
    local.validate_prepared_request(request)
    value = copy.deepcopy(dict(request))
    value["schema_version"] = ADAPTER_SCHEMA_VERSION
    value["candidate_system_id"] = CANDIDATE_SYSTEM_ID
    value["canonical_schema_strategy"] = CANONICAL_SCHEMA_STRATEGY
    value["batch_id"] = "cv31cup_" + base.sha256_text(
        _canonical_json(
            {
                "unit_local_batch_id": request["batch_id"],
                "candidate_system_id": CANDIDATE_SYSTEM_ID,
            }
        )
    )[:24]
    value["semantic_integrity"] = semantic_integrity_contract()
    value["private_input"]["schema_version"] = ADAPTER_SCHEMA_VERSION
    value["prompt"] = _compact_prompt(
        request["prompt"], value["private_input"]["segments"]
    )
    value["base_instructions"] = _compact_base_instructions(
        request["base_instructions"]
    )
    value["output_schema"] = _compact_schema(request["output_schema"])
    value["prompt_sha256"] = base.sha256_text(value["prompt"])
    value["base_instructions_sha256"] = base.sha256_text(value["base_instructions"])
    value["output_schema_sha256"] = base.sha256_text(
        _canonical_json(value["output_schema"])
    )
    return value


def _local_request(request: Mapping[str, Any]) -> dict[str, Any]:
    value = copy.deepcopy(dict(request))
    value["prompt"] = _local_prompt(value["prompt"])
    value["base_instructions"] = _local_base_instructions(value["base_instructions"])
    bounded = local._bounded_request(value)  # noqa: SLF001
    return local._pointer_request(bounded)  # noqa: SLF001


def prepare_episode_batches(
    episode: Mapping[str, Any], *, batch_size: int, thread_mode: str
) -> list[dict[str, Any]]:
    requests = local.prepare_episode_batches(
        episode, batch_size=batch_size, thread_mode=thread_mode
    )
    values = [_compact_request(request) for request in requests]
    for value in values:
        validate_prepared_request(value)
    return values


def validate_prepared_request(request: Mapping[str, Any]) -> dict[str, Any]:
    local_request = _local_request(request)
    validated = local.validate_prepared_request(local_request)
    expected = _compact_request(validated)
    if dict(request) != expected:
        raise CanonicalV31EpisodeBatchError("compact unit-pointer request drifted")
    return copy.deepcopy(dict(request))


def _verbose_metric(
    metric: Mapping[str, Any], source_segment: Mapping[str, Any]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    units = source_segment["units"]
    value: dict[str, Any] = {"direction": metric["direction"]}
    records = []
    for field in METRIC_LITERAL_FIELDS:
        raw_range = metric[f"{field}_range"]
        if raw_range is None:
            coordinates = [None, None, None, None]
        else:
            if (
                not isinstance(raw_range, list)
                or len(raw_range) != 4
                or any(isinstance(item, bool) or not isinstance(item, int) for item in raw_range)
            ):
                raise CanonicalV31OutputError(f"metric {field} range is malformed")
            coordinates = raw_range
        start_unit_index, start_token_index, end_unit_index, end_token_index = coordinates
        if raw_range is not None and (
            start_unit_index < 0
            or end_unit_index < 0
            or start_unit_index >= len(units)
            or end_unit_index >= len(units)
        ):
            raise CanonicalV31OutputError(f"metric {field} source-unit index is invalid")
        value[f"{field}_start_source_unit_id"] = (
            None if raw_range is None else units[start_unit_index]["unit_id"]
        )
        value[f"{field}_start_token_index"] = start_token_index
        value[f"{field}_end_source_unit_id"] = (
            None if raw_range is None else units[end_unit_index]["unit_id"]
        )
        value[f"{field}_end_token_index"] = end_token_index
        records.append(
            {
                "field": field,
                "compact_range": copy.deepcopy(raw_range),
                "start_source_unit_id": value[f"{field}_start_source_unit_id"],
                "end_source_unit_id": value[f"{field}_end_source_unit_id"],
            }
        )
    return value, records


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
        segment_records = []
        for event in raw_segment["discourse_events"]:
            event["metric"], records = _verbose_metric(event["metric"], source_segment)
            segment_records.append(records)
        metric_records.append(segment_records)
    projected = local.validate_and_project_output(_local_request(validated), converted)
    if dict(output) != untouched:
        raise CanonicalV31OutputError("compact range projection mutated model output")
    provenance = copy.deepcopy(projected["provenance"])
    provenance["schema_version"] = PROVENANCE_SCHEMA_VERSION
    provenance["candidate_system_id"] = CANDIDATE_SYSTEM_ID
    provenance["compact_range_version"] = COMPACT_RANGE_VERSION
    for segment, records in zip(provenance["segments"], metric_records):
        for event, event_records in zip(segment["discourse_events"], records):
            event["metric_compact_range_records"] = event_records
    fidelity = copy.deepcopy(projected["fidelity"])
    fidelity["schema_version"] = FIDELITY_SCHEMA_VERSION
    fidelity["candidate_system_id"] = CANDIDATE_SYSTEM_ID
    fidelity["model_selected_compact_metric_ranges_projected_exactly"] = True
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
    sidecar = base._load_json(sidecar_path, "compact unit-pointer sidecar")  # noqa: SLF001
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
            "managed-auth completed compact unit-pointer sidecar contract failed"
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
        "schema_version": "pif_canonical_v31_compact_unit_pointer_binding_v1",
        "unit_local_adapter_binding": local.build_six_arm_matrix_binding(),
        "unit_local_adapter_module_sha256": hashlib.sha256(
            Path(local.__file__).read_bytes()
        ).hexdigest(),
        "compact_unit_pointer_adapter_module_sha256": hashlib.sha256(
            Path(__file__).read_bytes()
        ).hexdigest(),
        "candidate_system_id": CANDIDATE_SYSTEM_ID,
        "canonical_schema_strategy": CANONICAL_SCHEMA_STRATEGY,
        "compact_range_version": COMPACT_RANGE_VERSION,
        "metric_literal_fields": list(METRIC_LITERAL_FIELDS),
        "model_selects_compact_positional_unit_local_metric_ranges": True,
        "deterministic_exact_metric_pointer_projection_only": True,
        "deterministic_coverage_owner_count_reconciliation_only": True,
        "semantic_postprocessing": False,
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
    "prepare_episode_batches",
    "validate_and_project_output",
    "validate_prepared_request",
    "validate_turn_sidecar",
)
