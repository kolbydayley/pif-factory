from __future__ import annotations

"""LLM-only second-pass compiler for exact canonical v3.1 metric literals."""

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
EFFORT = "low"
PINNED_CODEX = bounded_span.PINNED_CODEX
CANONICAL_LABEL_PACK = bounded_span.CANONICAL_LABEL_PACK
CANONICAL_EVIDENCE_MAX_CHARS = bounded_span.CANONICAL_EVIDENCE_MAX_CHARS
SOURCE_UNIT_MAX_CHARS = bounded_span.SOURCE_UNIT_MAX_CHARS
USAGE_FIELDS = bounded_span.USAGE_FIELDS
CanonicalV31EpisodeBatchError = bounded_span.CanonicalV31EpisodeBatchError
CanonicalV31OutputError = bounded_span.CanonicalV31OutputError
CanonicalV31TelemetryError = bounded_span.CanonicalV31TelemetryError
_client_factory = bounded_span._client_factory
_verify_started_thread = bounded_span._verify_started_thread
expected_instruction_source_contract = bounded_span.expected_instruction_source_contract
verified_context_control_overlay = bounded_span.verified_context_control_overlay

ADAPTER_SCHEMA_VERSION = "pif_canonical_v31_metric_compiler_request_v1"
RAW_OUTPUT_SCHEMA_VERSION = "pif_canonical_v31_metric_compiler_output_v1"
PROJECTION_SCHEMA_VERSION = "pif_canonical_v31_metric_compiler_projection_v1"
PROVENANCE_SCHEMA_VERSION = "pif_canonical_v31_metric_compiler_provenance_v1"
FIDELITY_SCHEMA_VERSION = "pif_canonical_v31_metric_compiler_fidelity_v1"
CANDIDATE_SYSTEM_ID = "pif_two_pass_canonical_metric_compiler_ai_discourse_v3_1_v1"
COMPILER_ARCHITECTURE = "bounded_span_extraction_plus_llm_exact_metric_compiler_v1"
METRIC_LITERAL_FIELDS = pointer.METRIC_LITERAL_FIELDS
METRIC_DIRECTIONS = (
    "increase",
    "decrease",
    "stable",
    "mixed",
    "not_applicable",
    "unknown",
)

BASE_INSTRUCTIONS = """You are the exact-source metric compiler for a completed canonical ai_discourse_v3_1 extraction.

The extraction events and their selected evidence spans are immutable. Do not add, delete, merge, reorder, relabel, or otherwise rewrite events. Deterministic exact-substring validation selected every and only metric case whose existing non-null source literal was not an exact substring of its selected evidence. For every supplied disputed case, independently decide whether the event has an applicable metric, its direction, and the exact source tokens expressing value, unit, comparator, and raw_text. Existing exact metric cases remain unchanged.

Each case contains one exact evidence substring and an ordered zero-based literal_token_texts array derived mechanically from it. Return nullable inclusive start/end indices into that case's array. Do not return rewritten source strings. A non-null pair must be ordered. Use the fully not-applicable form only when all eight token indices are null and direction is not_applicable. Any other direction or any non-null literal requires a non-null raw_text range. Preserve the supplied metric_case_id order exactly and return one row for every case. Abstain with not_applicable rather than inventing a metric.

This is the only semantic repair pass. Deterministic code will project your selected token ranges to exact source substrings, replace only the metric objects you authored, and recompute only redundant evidence-owner coverage counts. It will not repair or reinterpret your decisions.
"""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _metric_applies(metric: Mapping[str, Any]) -> bool:
    return bool(
        metric.get("direction") != "not_applicable"
        or any(metric.get(field) is not None for field in METRIC_LITERAL_FIELDS)
    )


def _output_schema(case_ids: Sequence[str]) -> dict[str, Any]:
    properties: dict[str, Any] = {
        "metric_case_id": {"type": "string", "enum": list(case_ids)},
        "direction": {"type": "string", "enum": list(METRIC_DIRECTIONS)},
    }
    required = ["metric_case_id", "direction"]
    for field in METRIC_LITERAL_FIELDS:
        for endpoint in ("start", "end"):
            name = f"{field}_{endpoint}_token_index"
            properties[name] = {"type": ["integer", "null"], "minimum": 0}
            required.append(name)
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": RAW_OUTPUT_SCHEMA_VERSION,
        "type": "object",
        "additionalProperties": False,
        "required": ["metric_repairs"],
        "properties": {
            "metric_repairs": {
                "type": "array",
                "minItems": len(case_ids),
                "maxItems": len(case_ids),
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": required,
                    "properties": properties,
                },
            }
        },
    }


def _source_span(
    source_segment: Mapping[str, Any], evidence_span_id: str
) -> Mapping[str, Any]:
    matches = [
        span
        for span in source_segment["evidence_spans"]
        if span["evidence_span_id"] == evidence_span_id
    ]
    if len(matches) != 1:
        raise CanonicalV31EpisodeBatchError("metric compiler evidence span drifted")
    return matches[0]


def prepare_request(
    source_request: Mapping[str, Any], source_output: Mapping[str, Any]
) -> dict[str, Any]:
    validated_source = bounded_span.validate_prepared_request(source_request)
    if not isinstance(source_output, Mapping):
        raise CanonicalV31EpisodeBatchError("metric compiler source output is malformed")
    base._validate_schema(source_output, validated_source["output_schema"])  # noqa: SLF001
    if [row.get("segment_id") for row in source_output.get("segments", [])] != list(
        validated_source["segment_ids"]
    ):
        raise CanonicalV31EpisodeBatchError("metric compiler source segment order drifted")
    cases: list[dict[str, Any]] = []
    public_cases: list[dict[str, Any]] = []
    applicable_case_count = 0
    exact_case_count = 0
    for segment_index, (raw_segment, source_segment) in enumerate(
        zip(source_output["segments"], validated_source["private_input"]["segments"])
    ):
        for event_index, event in enumerate(raw_segment["discourse_events"]):
            metric = event.get("metric")
            if not isinstance(metric, Mapping):
                raise CanonicalV31EpisodeBatchError("metric compiler source metric is malformed")
            if not _metric_applies(metric):
                continue
            applicable_case_count += 1
            span_id = event.get("evidence_span_id")
            if not isinstance(span_id, str):
                raise CanonicalV31EpisodeBatchError("metric compiler event span is malformed")
            span = _source_span(source_segment, span_id)
            evidence = source_segment["segment_text"][
                span["start_char"] : span["end_char"]
            ]
            if not evidence or len(evidence) > CANONICAL_EVIDENCE_MAX_CHARS:
                raise CanonicalV31EpisodeBatchError("metric compiler evidence is invalid")
            disputed_fields = [
                field
                for field in METRIC_LITERAL_FIELDS
                if metric.get(field) not in (None, "")
                and metric[field] not in evidence
            ]
            if not disputed_fields:
                exact_case_count += 1
                continue
            case_index = len(cases)
            tokens = pointer._literal_tokens(evidence, segment_position=case_index)  # noqa: SLF001
            case_id = f"MC{case_index:04d}"
            private_case = {
                "metric_case_id": case_id,
                "segment_index": segment_index,
                "segment_id": source_segment["segment_id"],
                "event_index": event_index,
                "evidence_span_id": span_id,
                "evidence": evidence,
                "literal_tokens": tokens,
                "source_metric": copy.deepcopy(dict(metric)),
                "disputed_fields": disputed_fields,
                "claim_text": event["claim_text"],
                "event_type": event["event_type"],
                "event_subtype": event["event_subtype"],
            }
            cases.append(private_case)
            public_cases.append(
                {
                    "metric_case_id": case_id,
                    "claim_text": event["claim_text"],
                    "current_metric": copy.deepcopy(dict(metric)),
                    "disputed_fields": disputed_fields,
                    "literal_token_texts": [token["text"] for token in tokens],
                }
            )
    if not cases:
        raise CanonicalV31EpisodeBatchError("metric compiler has no applicable cases")
    prompt = "# Canonical v3.1 exact metric compiler cases\n" + json.dumps(
        {"metric_cases": public_cases}, ensure_ascii=True, separators=(",", ":")
    ) + "\n"
    output_schema = _output_schema([case["metric_case_id"] for case in cases])
    request = {
        "schema_version": ADAPTER_SCHEMA_VERSION,
        "candidate_system_id": CANDIDATE_SYSTEM_ID,
        "compiler_architecture": COMPILER_ARCHITECTURE,
        "episode_id": validated_source["episode_id"],
        "segment_ids": copy.deepcopy(validated_source["segment_ids"]),
        "thread_mode": "new_thread",
        "effective_batch_size": len(cases),
        "batch_id": "cv31mc_"
        + base.sha256_text(
            _canonical_json(
                {
                    "source_batch_id": validated_source["batch_id"],
                    "case_ids": [case["metric_case_id"] for case in cases],
                }
            )
        )[:24],
        "base_instructions": BASE_INSTRUCTIONS,
        "prompt": prompt,
        "output_schema": output_schema,
        "base_instructions_sha256": base.sha256_text(BASE_INSTRUCTIONS),
        "prompt_sha256": base.sha256_text(prompt),
        "output_schema_sha256": base.sha256_text(_canonical_json(output_schema)),
        "context_control_overlay_sha256": validated_source[
            "context_control_overlay_sha256"
        ],
        "private_input": {
            "source_request": copy.deepcopy(dict(validated_source)),
            "source_output": copy.deepcopy(dict(source_output)),
            "source_request_sha256": base.sha256_text(_canonical_json(validated_source)),
            "source_output_sha256": base.sha256_text(_canonical_json(source_output)),
            "segments": copy.deepcopy(validated_source["private_input"]["segments"]),
            "metric_cases": cases,
        },
        "semantic_integrity": {
            "deterministic_exact_substring_dispute_selection_only": True,
            "applicable_metric_case_count": applicable_case_count,
            "exact_metric_case_count_preserved": exact_case_count,
            "llm_reauthors_every_disputed_metric_case": True,
            "deterministic_exact_token_projection_only": True,
            "deterministic_coverage_owner_count_reconciliation_only": True,
            "deterministic_semantic_defaults": {},
            "deterministic_semantic_pruning": False,
            "deterministic_deduplication": False,
            "deterministic_relabeling": False,
        },
    }
    return request


def validate_prepared_request(request: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(request, Mapping):
        raise CanonicalV31EpisodeBatchError("metric compiler request is malformed")
    private = request.get("private_input")
    if not isinstance(private, Mapping):
        raise CanonicalV31EpisodeBatchError("metric compiler private input is malformed")
    source_request = private.get("source_request")
    source_output = private.get("source_output")
    if not isinstance(source_request, Mapping) or not isinstance(source_output, Mapping):
        raise CanonicalV31EpisodeBatchError("metric compiler source lineage is absent")
    expected = prepare_request(source_request, source_output)
    if dict(request) != expected:
        raise CanonicalV31EpisodeBatchError("metric compiler request drifted")
    return copy.deepcopy(dict(request))


def _project_literal(
    repair: Mapping[str, Any], case: Mapping[str, Any], field: str
) -> tuple[str | None, dict[str, Any]]:
    start = repair[f"{field}_start_token_index"]
    end = repair[f"{field}_end_token_index"]
    if (start is None) != (end is None):
        raise CanonicalV31OutputError(f"metric compiler {field} endpoints are incomplete")
    if start is None:
        return None, {
            "field": field,
            "start_token_index": None,
            "end_token_index": None,
            "value_sha256": None,
        }
    tokens = case["literal_tokens"]
    if (
        isinstance(start, bool)
        or not isinstance(start, int)
        or isinstance(end, bool)
        or not isinstance(end, int)
        or start < 0
        or end < start
        or end >= len(tokens)
    ):
        raise CanonicalV31OutputError(f"metric compiler {field} token range is invalid")
    start_char = tokens[start]["start_char"]
    end_char = tokens[end]["end_char"]
    value = case["evidence"][start_char:end_char]
    if not value:
        raise CanonicalV31OutputError(f"metric compiler {field} projection is empty")
    return value, {
        "field": field,
        "start_token_index": start,
        "end_token_index": end,
        "start_char_in_evidence": start_char,
        "end_char_in_evidence": end_char,
        "value_sha256": base.sha256_text(value),
    }


def _metric_from_repair(
    repair: Mapping[str, Any], case: Mapping[str, Any]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    metric: dict[str, Any] = {"direction": repair["direction"]}
    records: list[dict[str, Any]] = []
    for field in METRIC_LITERAL_FIELDS:
        value, record = _project_literal(repair, case, field)
        metric[field] = value
        records.append(record)
    any_literal = any(metric[field] is not None for field in METRIC_LITERAL_FIELDS)
    if not any_literal and metric["direction"] != "not_applicable":
        raise CanonicalV31OutputError(
            "metric compiler empty metric must be not_applicable"
        )
    if any_literal and metric["raw_text"] is None:
        raise CanonicalV31OutputError(
            "metric compiler applicable metric requires raw_text"
        )
    return metric, records


def _reconcile_coverage_counts(
    raw_output: dict[str, Any], source_request: Mapping[str, Any]
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for raw_segment, source_segment in zip(
        raw_output["segments"], source_request["private_input"]["segments"]
    ):
        spans = {
            span["evidence_span_id"]: span
            for span in source_segment["evidence_spans"]
        }
        event_counts = {unit["unit_id"]: 0 for unit in source_segment["units"]}
        candidate_counts = {unit["unit_id"]: 0 for unit in source_segment["units"]}
        for event in raw_segment["discourse_events"]:
            span = spans[event["evidence_span_id"]]
            event_counts[span["evidence_start_unit_id"]] += 1
        for candidate in raw_segment["concept_candidates"]:
            span = spans[candidate["evidence_span_id"]]
            candidate_counts[span["evidence_start_unit_id"]] += 1
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
        raise CanonicalV31OutputError("metric compiler output must be an object")
    untouched = copy.deepcopy(dict(output))
    base._validate_schema(output, validated["output_schema"])  # noqa: SLF001
    cases = validated["private_input"]["metric_cases"]
    repairs = output["metric_repairs"]
    if [repair["metric_case_id"] for repair in repairs] != [
        case["metric_case_id"] for case in cases
    ]:
        raise CanonicalV31OutputError("metric compiler case order or identity drifted")
    repaired_raw = copy.deepcopy(validated["private_input"]["source_output"])
    pointer_records: list[dict[str, Any]] = []
    for repair, case in zip(repairs, cases):
        metric, records = _metric_from_repair(repair, case)
        event = repaired_raw["segments"][case["segment_index"]]["discourse_events"][
            case["event_index"]
        ]
        event["metric"] = metric
        pointer_records.append(
            {
                "metric_case_id": case["metric_case_id"],
                "segment_id": case["segment_id"],
                "event_index": case["event_index"],
                "evidence_span_id": case["evidence_span_id"],
                "metric_literal_pointers": records,
            }
        )
    coverage_records = _reconcile_coverage_counts(
        repaired_raw, validated["private_input"]["source_request"]
    )
    projected = bounded_span.validate_and_project_output(
        validated["private_input"]["source_request"], repaired_raw
    )
    if dict(output) != untouched:
        raise CanonicalV31OutputError("metric compiler projection mutated model output")
    provenance = copy.deepcopy(projected["provenance"])
    provenance["schema_version"] = PROVENANCE_SCHEMA_VERSION
    provenance["candidate_system_id"] = CANDIDATE_SYSTEM_ID
    provenance["metric_compiler_pointer_records"] = pointer_records
    provenance["deterministic_coverage_owner_count_records"] = coverage_records
    fidelity = copy.deepcopy(projected["fidelity"])
    fidelity["schema_version"] = FIDELITY_SCHEMA_VERSION
    fidelity["candidate_system_id"] = CANDIDATE_SYSTEM_ID
    fidelity["llm_reauthored_disputed_metric_case_count"] = len(cases)
    fidelity["preserved_exact_metric_case_count"] = validated["semantic_integrity"][
        "exact_metric_case_count_preserved"
    ]
    fidelity["all_metric_literals_projected_from_llm_selected_exact_tokens"] = True
    fidelity["deterministic_coverage_owner_counts_only"] = True
    fidelity["deterministic_semantic_defaults"] = {}
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
    sidecar = base._load_json(sidecar_path, "metric compiler sidecar")  # noqa: SLF001
    if not isinstance(sidecar, Mapping):
        raise CanonicalV31TelemetryError("metric compiler sidecar is not an object")
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
        or sidecar.get("thread_mode") != "new_thread"
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
        raise CanonicalV31TelemetryError("metric compiler sidecar contract failed")
    usage = base._usage_values(sidecar.get("usage"), "usage")  # noqa: SLF001
    total = base._usage_values(  # noqa: SLF001
        sidecar.get("thread_total_usage"), "thread_total_usage"
    )
    if any(total[field] < usage[field] for field in USAGE_FIELDS):
        raise CanonicalV31TelemetryError("metric compiler cumulative usage is below last usage")
    wall = base._nonnegative_number(  # noqa: SLF001
        sidecar.get("wall_elapsed_seconds"), "wall_elapsed_seconds"
    )
    if (
        not output_path.is_file()
        or sidecar.get("output_sha256") != base._output_message_hash(output_path)  # noqa: SLF001
        or Path(str(sidecar.get("output_path") or "")).expanduser().resolve()
        != output_path.expanduser().resolve()
    ):
        raise CanonicalV31TelemetryError("metric compiler output lineage drifted")
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
        "schema_version": "pif_canonical_v31_metric_compiler_binding_v1",
        "bounded_span_adapter_binding": bounded_span.build_six_arm_matrix_binding(),
        "bounded_span_adapter_module_sha256": hashlib.sha256(
            Path(bounded_span.__file__).read_bytes()
        ).hexdigest(),
        "metric_compiler_module_sha256": hashlib.sha256(
            Path(__file__).read_bytes()
        ).hexdigest(),
        "candidate_system_id": CANDIDATE_SYSTEM_ID,
        "compiler_architecture": COMPILER_ARCHITECTURE,
        "compiler_model": MODEL,
        "compiler_effort": EFFORT,
        "deterministic_exact_substring_dispute_selection_only": True,
        "llm_reauthors_every_disputed_metric_case": True,
        "deterministic_exact_token_projection_only": True,
        "deterministic_coverage_owner_count_reconciliation_only": True,
        "semantic_postprocessing": False,
        "deterministic_semantic_pruning": False,
        "deterministic_deduplication": False,
        "deterministic_relabeling": False,
    }


__all__: Sequence[str] = (
    "ADAPTER_SCHEMA_VERSION",
    "BASE_INSTRUCTIONS",
    "CANDIDATE_SYSTEM_ID",
    "COMPILER_ARCHITECTURE",
    "EFFORT",
    "METRIC_LITERAL_FIELDS",
    "MODEL",
    "PINNED_CODEX",
    "PROJECT_ROOT",
    "USAGE_FIELDS",
    "build_six_arm_matrix_binding",
    "expected_instruction_source_contract",
    "prepare_request",
    "validate_and_project_output",
    "validate_prepared_request",
    "validate_turn_sidecar",
    "verified_context_control_overlay",
)
