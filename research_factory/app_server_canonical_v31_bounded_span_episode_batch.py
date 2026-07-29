from __future__ import annotations

"""Full-canonical adapter with structurally bounded evidence ownership.

The canonical v3.1 storage schema caps evidence at 1,000 characters.  Earlier
adapters let the model choose arbitrary source-unit start/end pairs and could
therefore emit an otherwise valid event whose deterministic reconstruction was
too long.  This additive adapter makes that state unrepresentable: source text
is partitioned into exact structural units of at most 450 characters, every
permitted contiguous span of at most 1,000 characters receives an opaque ID,
and the model selects exactly one such ID per event or concept candidate.

The span choice remains an LLM semantic decision.  Deterministic code only
constructs the lossless source partition, maps the chosen ID to exact offsets,
and validates the unchanged canonical output.
"""

import copy
import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_canonical_v31_audited_episode_batch as audited


base = audited.bounded.base

PROJECT_ROOT = base.PROJECT_ROOT
MODEL = base.MODEL
EFFORT = base.EFFORT
PINNED_CODEX = base.PINNED_CODEX
CANONICAL_LABEL_PACK = base.CANONICAL_LABEL_PACK
CANONICAL_LABEL_SCHEMA_SHA256 = base.CANONICAL_LABEL_SCHEMA_SHA256
CANONICAL_EVIDENCE_MAX_CHARS = base.CANONICAL_EVIDENCE_MAX_CHARS
SUPPORTED_BATCH_SIZES = base.SUPPORTED_BATCH_SIZES
SUPPORTED_THREAD_MODES = base.SUPPORTED_THREAD_MODES
RETRY_COUNT = base.RETRY_COUNT
USAGE_FIELDS = base.USAGE_FIELDS
CanonicalV31EpisodeBatchError = base.CanonicalV31EpisodeBatchError
CanonicalV31OutputError = base.CanonicalV31OutputError
CanonicalV31TelemetryError = base.CanonicalV31TelemetryError
_client_factory = base._client_factory
expected_instruction_source_contract = base.expected_instruction_source_contract
verified_context_control_overlay = base.verified_context_control_overlay

ADAPTER_SCHEMA_VERSION = "pif_canonical_v31_bounded_span_episode_batch_adapter_v1"
RAW_OUTPUT_SCHEMA_VERSION = "pif_canonical_v31_bounded_span_raw_output_v1"
PROJECTION_SCHEMA_VERSION = "pif_canonical_v31_bounded_span_projection_v1"
PROVENANCE_SCHEMA_VERSION = "pif_canonical_v31_bounded_span_provenance_v1"
FIDELITY_SCHEMA_VERSION = "pif_canonical_v31_bounded_span_fidelity_v1"
CANDIDATE_SYSTEM_ID = "pif_direct_full_canonical_bounded_span_ai_discourse_v3_1_v1"
CANONICAL_SCHEMA_STRATEGY = (
    "canonical_v31_full_semantics_exact_structural_bounded_span_ownership_v1"
)
SOURCE_UNIT_MAX_CHARS = 450
SOURCE_UNIT_CHUNKING_VERSION = "pif_exact_offset_contiguous_source_units_v2"
EVIDENCE_SPAN_CATALOG_VERSION = "pif_exact_bounded_evidence_span_catalog_v1"
EVIDENCE_SPAN_FIELD = "evidence_span_id"

_OLD_EVIDENCE_PARAGRAPH = (
    "For each discourse event and concept candidate, return only "
    "evidence_start_unit_id and evidence_end_unit_id from the same segment. "
    "Choose the smallest contiguous source-unit range supporting every material "
    "emitted field. Deterministic code will attach the exact source substring "
    "and character offsets. Every nonempty metric value, unit, comparator, and "
    "raw_text must be a literal substring of that selected evidence; raw_text "
    "must itself be a contiguous evidence substring."
)
_SPAN_EVIDENCE_PARAGRAPH = (
    "For each discourse event and concept candidate, return only one "
    "evidence_span_id listed for that segment. Each listed span is an exact "
    "contiguous source-unit range whose reconstructed source text is at most "
    "1000 characters. Choose the smallest listed span supporting every material "
    "emitted field; split genuinely independent propositions or omit an item "
    "that cannot be fully grounded within one listed span. Deterministic code "
    "will attach the exact source substring and character offsets. Every "
    "nonempty metric value, unit, comparator, and raw_text must be a literal "
    "substring of that selected evidence; raw_text must itself be a contiguous "
    "evidence substring."
)


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _source_units(
    text: str, boundaries: Sequence[Mapping[str, Any]], *, segment_position: int
) -> list[dict[str, Any]]:
    """Partition exact source text into compact nonsemantic contiguous units."""

    ordered_owner_ranges = sorted(
        (int(row["owner_start"]), int(row["owner_end"])) for row in boundaries
    )
    if any(
        current_start < previous_end
        for (_previous_start, previous_end), (current_start, _current_end) in zip(
            ordered_owner_ranges, ordered_owner_ranges[1:]
        )
    ):
        raise CanonicalV31EpisodeBatchError("source unit owner windows overlap")
    units: list[dict[str, Any]] = []
    cursor = 0
    while cursor < len(text):
        while cursor < len(text) and text[cursor].isspace():
            cursor += 1
        if cursor >= len(text):
            break
        owner = base._owner_boundary(cursor, boundaries)  # noqa: SLF001
        limit = min(
            len(text),
            cursor + SOURCE_UNIT_MAX_CHARS,
            int(owner["owner_end"]),
        )
        if limit <= cursor:
            raise CanonicalV31EpisodeBatchError(
                "source unit owner interval cannot represent source content"
            )
        end = limit
        while end > cursor and text[end - 1].isspace():
            end -= 1
        unit_text = text[cursor:end]
        if (
            not unit_text.strip()
            or len(unit_text) > SOURCE_UNIT_MAX_CHARS
            or text[cursor:end] != unit_text
        ):
            raise CanonicalV31EpisodeBatchError(
                "bounded-span source unit structural chunking failed"
            )
        units.append(
            {
                "unit_id": f"S{segment_position:04d}U{len(units):04d}",
                "start_char": cursor,
                "end_char": end,
                "window_id": int(owner["window_id"]),
                "text": unit_text,
            }
        )
        cursor = limit
    if not units:
        raise CanonicalV31EpisodeBatchError("source unit projection is empty")
    represented = [False] * len(text)
    for unit in units:
        for index in range(unit["start_char"], unit["end_char"]):
            if represented[index] or text[index] != unit["text"][index - unit["start_char"]]:
                raise CanonicalV31EpisodeBatchError("source unit projection overlaps or drifted")
            represented[index] = True
    if any(not represented[index] for index, char in enumerate(text) if not char.isspace()):
        raise CanonicalV31EpisodeBatchError("source unit projection omitted source content")
    return units


def _evidence_spans(
    text: str, units: Sequence[Mapping[str, Any]], *, segment_position: int
) -> list[dict[str, Any]]:
    spans: list[dict[str, Any]] = []
    for start_index, start_unit in enumerate(units):
        for end_index in range(start_index, len(units)):
            end_unit = units[end_index]
            start = int(start_unit["start_char"])
            end = int(end_unit["end_char"])
            character_count = end - start
            if character_count > CANONICAL_EVIDENCE_MAX_CHARS:
                break
            if character_count <= 0 or text[start:end] == "":
                raise CanonicalV31EpisodeBatchError("bounded evidence span is empty")
            spans.append(
                {
                    "evidence_span_id": (
                        f"E{segment_position:04d}S{start_index:04d}E{end_index:04d}"
                    ),
                    "evidence_start_unit_id": start_unit["unit_id"],
                    "evidence_end_unit_id": end_unit["unit_id"],
                    "start_char": start,
                    "end_char": end,
                    "character_count": character_count,
                    "evidence_sha256": base.sha256_text(text[start:end]),
                }
            )
    if not spans or len({span[EVIDENCE_SPAN_FIELD] for span in spans}) != len(spans):
        raise CanonicalV31EpisodeBatchError("bounded evidence span catalog is invalid")
    covered_units = {span["evidence_start_unit_id"] for span in spans}
    if covered_units != {unit["unit_id"] for unit in units}:
        raise CanonicalV31EpisodeBatchError("bounded evidence span catalog is incomplete")
    return spans


def _prepare_segment(segment: Mapping[str, Any], *, segment_position: int) -> dict[str, Any]:
    segment_id = base._nonempty_identifier(segment.get("segment_id"), "segment_id")  # noqa: SLF001
    text = segment.get("segment_text")
    if not isinstance(text, str) or not text:
        raise CanonicalV31EpisodeBatchError("segment_text must be non-empty")
    boundaries = base._validate_boundaries(text, segment.get("boundaries"))  # noqa: SLF001
    units = _source_units(text, boundaries, segment_position=segment_position)
    boundary_by_id = {int(row["window_id"]): row for row in boundaries}
    for unit in units:
        owner = boundary_by_id[unit["window_id"]]
        start = unit["start_char"]
        end = unit["end_char"]
        if not (
            text[start:end] == unit["text"]
            and len(unit["text"]) <= SOURCE_UNIT_MAX_CHARS
            and int(owner["owner_start"]) <= start < int(owner["owner_end"])
            and end <= int(owner["owner_end"])
            and int(owner["extract_start"]) <= start
            and end <= int(owner["extract_end"])
        ):
            raise CanonicalV31EpisodeBatchError("source unit offset projection failed")
    if "segment_quality" not in segment or not isinstance(segment["segment_quality"], Mapping):
        raise CanonicalV31EpisodeBatchError("segment_quality provenance is required")
    quality = copy.deepcopy(dict(segment["segment_quality"]))
    try:
        base._validate_schema(  # noqa: SLF001
            quality,
            base.canonical_label_schema()["properties"]["segment_quality"],
            "$.segment_quality",
        )
    except CanonicalV31OutputError as exc:
        raise CanonicalV31EpisodeBatchError("segment_quality provenance is invalid") from exc
    density = segment.get("density_stratum")
    if not isinstance(density, str) or not density:
        raise CanonicalV31EpisodeBatchError("density_stratum provenance is required")
    return {
        "segment_id": segment_id,
        "segment_text": text,
        "segment_quality": quality,
        "boundaries": boundaries,
        "units": units,
        "evidence_spans": _evidence_spans(
            text, units, segment_position=segment_position
        ),
        "density_stratum": density,
        "segment_position": segment_position,
    }


def _render_base_instructions(context: Mapping[str, Any]) -> str:
    rendered = base._render_base_instructions(context)  # noqa: SLF001
    if rendered.count(_OLD_EVIDENCE_PARAGRAPH) != 1:
        raise CanonicalV31EpisodeBatchError("base evidence instructions drifted")
    rendered = rendered.replace(_OLD_EVIDENCE_PARAGRAPH, _SPAN_EVIDENCE_PARAGRAPH)
    rendered += audited.CANONICAL_SELF_AUDIT_INSTRUCTION
    if "evidence_start_unit_id" in rendered or "evidence_end_unit_id" in rendered:
        raise CanonicalV31EpisodeBatchError("stale open-ended evidence fields remain")
    return rendered


def _render_prompt(episode_id: str, segments: Sequence[Mapping[str, Any]]) -> str:
    packet = {
        "episode_id": episode_id,
        "segments": [
            {
                "segment_id": segment["segment_id"],
                "source_units": [
                    {
                        "unit_id": unit["unit_id"],
                        "window_id": unit["window_id"],
                        "text": unit["text"],
                    }
                    for unit in segment["units"]
                ],
                "evidence_spans": [
                    {
                        "evidence_span_id": span["evidence_span_id"],
                        "evidence_start_unit_id": span["evidence_start_unit_id"],
                        "evidence_end_unit_id": span["evidence_end_unit_id"],
                        "character_count": span["character_count"],
                    }
                    for span in segment["evidence_spans"]
                ],
            }
            for segment in segments
        ],
    }
    return "# Canonical v3.1 bounded evidence-span packet\n" + json.dumps(
        packet, ensure_ascii=True, separators=(",", ":")
    ) + "\n"


def _span_owned_item_schema(
    source: Mapping[str, Any], span_ids: Sequence[str]
) -> dict[str, Any]:
    item = copy.deepcopy(dict(source))
    properties = item["properties"]
    for field in base._EVIDENCE_FIELDS:  # noqa: SLF001
        properties.pop(field)
    item["required"] = [
        field for field in item["required"] if field not in base._EVIDENCE_FIELDS  # noqa: SLF001
    ]
    properties[EVIDENCE_SPAN_FIELD] = {
        "type": "string",
        "enum": list(span_ids),
        "description": "Opaque ID for one exact prevalidated source span.",
    }
    item["required"].append(EVIDENCE_SPAN_FIELD)
    return item


def build_output_schema(
    *,
    episode_id: str,
    segment_ids: Sequence[str],
    unit_ids: Sequence[str],
    span_ids: Sequence[str],
    maximum_unit_count: int,
) -> dict[str, Any]:
    schema = base.build_output_schema(
        episode_id=episode_id,
        segment_ids=segment_ids,
        unit_ids=unit_ids,
        maximum_unit_count=maximum_unit_count,
    )
    schema["$id"] = RAW_OUTPUT_SCHEMA_VERSION
    canonical = base.canonical_label_schema()
    segment_properties = schema["properties"]["segments"]["items"]["properties"]
    segment_properties["discourse_events"]["items"] = _span_owned_item_schema(
        canonical["properties"]["discourse_events"]["items"], span_ids
    )
    segment_properties["concept_candidates"]["items"] = _span_owned_item_schema(
        canonical["properties"]["concept_candidates"]["items"], span_ids
    )
    return schema


def semantic_integrity_contract() -> dict[str, Any]:
    return {
        "one_llm_turn_per_prepared_batch": True,
        "model_emits_every_model_owned_semantic_field": True,
        "model_selects_one_prevalidated_exact_evidence_span": True,
        "model_emits_deterministic_provenance_fields": False,
        "full_canonical_final_output_after_provenance_projection": True,
        "deterministic_identity_attachment_only": True,
        "deterministic_segment_quality_attachment_only": True,
        "deterministic_exact_evidence_projection_only": True,
        "source_unit_chunking_version": SOURCE_UNIT_CHUNKING_VERSION,
        "source_unit_chunking_is_structural_not_semantic": True,
        "source_unit_max_characters": SOURCE_UNIT_MAX_CHARS,
        "evidence_span_catalog_version": EVIDENCE_SPAN_CATALOG_VERSION,
        "canonical_evidence_max_characters": CANONICAL_EVIDENCE_MAX_CHARS,
        "every_selectable_span_prevalidated_within_canonical_limit": True,
        "deterministic_semantic_defaults": {},
        "deterministic_semantic_pruning": False,
        "deterministic_sponsor_pruning": False,
        "deterministic_keyword_pruning": False,
        "deterministic_regex_pruning": False,
        "deterministic_truncation": False,
        "deterministic_padding": False,
        "deterministic_deduplication": False,
        "deterministic_relabeling": False,
        "completed_invalid_output_action": "reject_whole_batch_without_mutating_output",
    }


def _batch_id(
    episode_id: str,
    segment_ids: Sequence[str],
    *,
    batch_size: int,
    thread_mode: str,
    batch_index: int,
) -> str:
    identity = {
        "candidate_system_id": CANDIDATE_SYSTEM_ID,
        "episode_id": episode_id,
        "segment_ids": list(segment_ids),
        "batch_size": batch_size,
        "thread_mode": thread_mode,
        "batch_index": batch_index,
    }
    return "cv31bs_" + base.sha256_text(_canonical_json(identity))[:24]


def prepare_episode_batches(
    episode: Mapping[str, Any], *, batch_size: int, thread_mode: str
) -> list[dict[str, Any]]:
    base._validate_arm_choice(batch_size, thread_mode)  # noqa: SLF001
    context = base._episode_context(episode)  # noqa: SLF001
    raw_segments = episode.get("segments")
    if (
        not isinstance(raw_segments, Sequence)
        or isinstance(raw_segments, (str, bytes))
        or not raw_segments
    ):
        raise CanonicalV31EpisodeBatchError("episode segments are empty")
    prepared = [
        _prepare_segment(segment, segment_position=position)
        for position, segment in enumerate(raw_segments)
    ]
    segment_ids = [segment["segment_id"] for segment in prepared]
    if len(segment_ids) != len(set(segment_ids)):
        raise CanonicalV31EpisodeBatchError("episode segment IDs are not unique")
    requests: list[dict[str, Any]] = []
    rendered_base = _render_base_instructions(context)
    for batch_index, start in enumerate(range(0, len(prepared), batch_size)):
        batch_segments = prepared[start : start + batch_size]
        batch_segment_ids = [segment["segment_id"] for segment in batch_segments]
        unit_ids = [unit["unit_id"] for segment in batch_segments for unit in segment["units"]]
        span_ids = [
            span[EVIDENCE_SPAN_FIELD]
            for segment in batch_segments
            for span in segment["evidence_spans"]
        ]
        if len(unit_ids) != len(set(unit_ids)) or len(span_ids) != len(set(span_ids)):
            raise CanonicalV31EpisodeBatchError("batch source IDs are not unique")
        output_schema = build_output_schema(
            episode_id=context["episode_id"],
            segment_ids=batch_segment_ids,
            unit_ids=unit_ids,
            span_ids=span_ids,
            maximum_unit_count=max(len(segment["units"]) for segment in batch_segments),
        )
        prompt = _render_prompt(context["episode_id"], batch_segments)
        private_input = {
            "schema_version": ADAPTER_SCHEMA_VERSION,
            "episode_id": context["episode_id"],
            "segments": copy.deepcopy(batch_segments),
            "privacy": "private source text units and preparation provenance",
        }
        request = {
            "schema_version": ADAPTER_SCHEMA_VERSION,
            "candidate_system_id": CANDIDATE_SYSTEM_ID,
            "canonical_label_pack": CANONICAL_LABEL_PACK,
            "canonical_label_schema_sha256": CANONICAL_LABEL_SCHEMA_SHA256,
            "canonical_schema_strategy": CANONICAL_SCHEMA_STRATEGY,
            "batch_id": _batch_id(
                context["episode_id"],
                batch_segment_ids,
                batch_size=batch_size,
                thread_mode=thread_mode,
                batch_index=batch_index,
            ),
            "batch_index": batch_index,
            "episode_id": context["episode_id"],
            "segment_ids": batch_segment_ids,
            "batch_size_ceiling": batch_size,
            "effective_batch_size": len(batch_segments),
            "thread_mode": thread_mode,
            "model": MODEL,
            "effort": EFFORT,
            "retry_count": RETRY_COUNT,
            "semantic_postprocessing": False,
            "semantic_integrity": semantic_integrity_contract(),
            "episode_context": copy.deepcopy(context),
            "private_input": private_input,
            "prompt": prompt,
            "base_instructions": rendered_base,
            "output_schema": output_schema,
            "prompt_sha256": base.sha256_text(prompt),
            "base_instructions_sha256": base.sha256_text(rendered_base),
            "output_schema_sha256": base.sha256_text(_canonical_json(output_schema)),
            "context_control_overlay_sha256": base.sha256_text(
                _canonical_json(verified_context_control_overlay())
            ),
        }
        validate_prepared_request(request)
        requests.append(request)
    return requests


def validate_prepared_request(request: Mapping[str, Any]) -> dict[str, Any]:
    required_keys = {
        "schema_version", "candidate_system_id", "canonical_label_pack",
        "canonical_label_schema_sha256", "canonical_schema_strategy", "batch_id",
        "batch_index", "episode_id", "segment_ids", "batch_size_ceiling",
        "effective_batch_size", "thread_mode", "model", "effort", "retry_count",
        "semantic_postprocessing", "semantic_integrity", "episode_context",
        "private_input", "prompt", "base_instructions", "output_schema",
        "prompt_sha256", "base_instructions_sha256", "output_schema_sha256",
        "context_control_overlay_sha256",
    }
    if set(request) != required_keys:
        raise CanonicalV31EpisodeBatchError("prepared request keys drifted")
    if (
        request["schema_version"] != ADAPTER_SCHEMA_VERSION
        or request["candidate_system_id"] != CANDIDATE_SYSTEM_ID
        or request["canonical_label_pack"] != CANONICAL_LABEL_PACK
        or request["canonical_label_schema_sha256"] != CANONICAL_LABEL_SCHEMA_SHA256
        or request["canonical_schema_strategy"] != CANONICAL_SCHEMA_STRATEGY
        or request["model"] != MODEL
        or request["effort"] != EFFORT
        or request["retry_count"] != RETRY_COUNT
        or request["semantic_postprocessing"] is not False
        or request["semantic_integrity"] != semantic_integrity_contract()
    ):
        raise CanonicalV31EpisodeBatchError("prepared request fixed contract drifted")
    batch_size = request["batch_size_ceiling"]
    thread_mode = request["thread_mode"]
    base._validate_arm_choice(batch_size, thread_mode)  # noqa: SLF001
    episode_id = base._nonempty_identifier(request["episode_id"], "episode_id")  # noqa: SLF001
    context = request["episode_context"]
    private_input = request["private_input"]
    if not isinstance(context, Mapping) or not isinstance(private_input, Mapping):
        raise CanonicalV31EpisodeBatchError("prepared request context is invalid")
    expected_context = base._episode_context(context)  # noqa: SLF001
    if dict(context) != expected_context or context["episode_id"] != episode_id:
        raise CanonicalV31EpisodeBatchError("prepared episode context drifted")
    if set(private_input) != {"schema_version", "episode_id", "segments", "privacy"}:
        raise CanonicalV31EpisodeBatchError("prepared private input drifted")
    if (
        private_input["schema_version"] != ADAPTER_SCHEMA_VERSION
        or private_input["episode_id"] != episode_id
        or private_input["privacy"] != "private source text units and preparation provenance"
        or not isinstance(private_input["segments"], list)
        or not private_input["segments"]
    ):
        raise CanonicalV31EpisodeBatchError("prepared private input contract drifted")
    rederived: list[dict[str, Any]] = []
    for segment in private_input["segments"]:
        if not isinstance(segment, Mapping):
            raise CanonicalV31EpisodeBatchError("prepared segment is invalid")
        position = segment.get("segment_position")
        if isinstance(position, bool) or not isinstance(position, int) or position < 0:
            raise CanonicalV31EpisodeBatchError("prepared segment position is invalid")
        expected = _prepare_segment(segment, segment_position=position)
        if dict(segment) != expected:
            raise CanonicalV31EpisodeBatchError("prepared bounded-span projection drifted")
        rederived.append(expected)
    positions = [segment["segment_position"] for segment in rederived]
    if positions != sorted(positions) or len(positions) != len(set(positions)):
        raise CanonicalV31EpisodeBatchError("prepared segment ordering drifted")
    segment_ids = [segment["segment_id"] for segment in rederived]
    if request["segment_ids"] != segment_ids or request["effective_batch_size"] != len(segment_ids):
        raise CanonicalV31EpisodeBatchError("prepared segment identity drifted")
    if len(segment_ids) > batch_size:
        raise CanonicalV31EpisodeBatchError("prepared batch exceeds arm ceiling")
    unit_ids = [unit["unit_id"] for segment in rederived for unit in segment["units"]]
    span_ids = [
        span[EVIDENCE_SPAN_FIELD]
        for segment in rederived
        for span in segment["evidence_spans"]
    ]
    expected_schema = build_output_schema(
        episode_id=episode_id,
        segment_ids=segment_ids,
        unit_ids=unit_ids,
        span_ids=span_ids,
        maximum_unit_count=max(len(segment["units"]) for segment in rederived),
    )
    expected_prompt = _render_prompt(episode_id, rederived)
    expected_base = _render_base_instructions(expected_context)
    if (
        request["output_schema"] != expected_schema
        or request["prompt"] != expected_prompt
        or request["base_instructions"] != expected_base
        or request["prompt_sha256"] != base.sha256_text(expected_prompt)
        or request["base_instructions_sha256"] != base.sha256_text(expected_base)
        or request["output_schema_sha256"] != base.sha256_text(_canonical_json(expected_schema))
        or request["context_control_overlay_sha256"]
        != base.sha256_text(_canonical_json(verified_context_control_overlay()))
    ):
        raise CanonicalV31EpisodeBatchError("prepared schema, text, or overlay hash drifted")
    expected_batch_id = _batch_id(
        episode_id,
        segment_ids,
        batch_size=batch_size,
        thread_mode=thread_mode,
        batch_index=request["batch_index"],
    )
    if request["batch_id"] != expected_batch_id:
        raise CanonicalV31EpisodeBatchError("prepared batch identity drifted")
    return copy.deepcopy(dict(request))


def _span_map(segment: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    return {span[EVIDENCE_SPAN_FIELD]: span for span in segment["evidence_spans"]}


def _convert_owned_items(
    items: Sequence[Mapping[str, Any]], segment: Mapping[str, Any], *, path: str
) -> tuple[list[dict[str, Any]], list[str]]:
    by_span = _span_map(segment)
    converted: list[dict[str, Any]] = []
    selected: list[str] = []
    for index, item in enumerate(items):
        span_id = item.get(EVIDENCE_SPAN_FIELD)
        if not isinstance(span_id, str) or span_id not in by_span:
            raise CanonicalV31OutputError(f"{path}[{index}] evidence span belongs to another segment")
        span = by_span[span_id]
        value = copy.deepcopy(dict(item))
        value.pop(EVIDENCE_SPAN_FIELD)
        value["evidence_start_unit_id"] = span["evidence_start_unit_id"]
        value["evidence_end_unit_id"] = span["evidence_end_unit_id"]
        converted.append(value)
        selected.append(span_id)
    return converted, selected


def _semantic_view_from_raw(segment: Mapping[str, Any]) -> dict[str, Any]:
    canonical = base.canonical_label_schema()
    semantic_names = [
        field for field in canonical["required"] if field not in base._DETERMINISTIC_LABEL_FIELDS  # noqa: SLF001
    ]
    result: dict[str, Any] = {}
    for field in semantic_names:
        if field not in {"discourse_events", "concept_candidates"}:
            result[field] = copy.deepcopy(segment[field])
            continue
        result[field] = [
            {name: copy.deepcopy(value) for name, value in item.items() if name != EVIDENCE_SPAN_FIELD}
            for item in segment[field]
        ]
    return result


def validate_and_project_output(
    request: Mapping[str, Any], output: Mapping[str, Any]
) -> dict[str, Any]:
    validated = validate_prepared_request(request)
    if not isinstance(output, Mapping):
        raise CanonicalV31OutputError("completed output must be an object")
    untouched = copy.deepcopy(dict(output))
    base._validate_schema(output, validated["output_schema"])  # noqa: SLF001
    source_segments = validated["private_input"]["segments"]
    raw_segments = output["segments"]
    if [segment["segment_id"] for segment in raw_segments] != [
        segment["segment_id"] for segment in source_segments
    ]:
        raise CanonicalV31OutputError("output segments must preserve exact source order and identity")
    canonical = base.canonical_label_schema()
    event_schema = canonical["properties"]["discourse_events"]["items"]
    candidate_schema = canonical["properties"]["concept_candidates"]["items"]
    semantic_names = [
        field for field in canonical["required"] if field not in base._DETERMINISTIC_LABEL_FIELDS  # noqa: SLF001
    ]
    labels: list[dict[str, Any]] = []
    provenance_segments: list[dict[str, Any]] = []
    raw_views: list[dict[str, Any]] = []
    projected_views: list[dict[str, Any]] = []
    for segment_index, (raw_segment, source_segment) in enumerate(zip(raw_segments, source_segments)):
        converted_events, event_span_ids = _convert_owned_items(
            raw_segment["discourse_events"], source_segment, path="$.discourse_events"
        )
        converted_candidates, candidate_span_ids = _convert_owned_items(
            raw_segment["concept_candidates"], source_segment, path="$.concept_candidates"
        )
        converted_segment = copy.deepcopy(dict(raw_segment))
        converted_segment["discourse_events"] = converted_events
        converted_segment["concept_candidates"] = converted_candidates
        base._validate_coverage_receipts(converted_segment, source_segment)  # noqa: SLF001
        status = raw_segment["extraction_status"]
        event_count = len(raw_segment["discourse_events"])
        if (status == "coded") != (event_count > 0):
            raise CanonicalV31OutputError(
                f"$.segments[{segment_index}] must be coded iff grounded events remain"
            )
        events, event_provenance = base._project_owned_items(  # noqa: SLF001
            converted_events,
            event_schema,
            source_segment,
            collection_name="discourse_events",
        )
        candidates, candidate_provenance = base._project_owned_items(  # noqa: SLF001
            converted_candidates,
            candidate_schema,
            source_segment,
            collection_name="concept_candidates",
        )
        for record, span_id in zip(event_provenance, event_span_ids):
            record[EVIDENCE_SPAN_FIELD] = span_id
        for record, span_id in zip(candidate_provenance, candidate_span_ids):
            record[EVIDENCE_SPAN_FIELD] = span_id
        label: dict[str, Any] = {
            "schema_version": CANONICAL_LABEL_PACK,
            "segment_id": source_segment["segment_id"],
            "episode_id": validated["episode_id"],
            "segment_quality": copy.deepcopy(source_segment["segment_quality"]),
        }
        for field in semantic_names:
            if field == "discourse_events":
                label[field] = events
            elif field == "concept_candidates":
                label[field] = candidates
            else:
                label[field] = copy.deepcopy(raw_segment[field])
        if set(label) != set(canonical["properties"]):
            raise CanonicalV31OutputError("canonical label field set drifted")
        try:
            base.validate_label_output(
                CANONICAL_LABEL_PACK,
                label,
                segment_text=source_segment["segment_text"],
            )
        except base.ValidationError as exc:
            raise CanonicalV31OutputError(
                f"segment {source_segment['segment_id']} failed canonical v3.1 validation"
            ) from exc
        raw_view = _semantic_view_from_raw(raw_segment)
        projected_view = base._semantic_view_from_label(label)  # noqa: SLF001
        if raw_view != projected_view:
            raise CanonicalV31OutputError("deterministic projection changed emitted semantic values")
        labels.append(label)
        raw_views.append(raw_view)
        projected_views.append(projected_view)
        provenance_segments.append(
            {
                "segment_index": segment_index,
                "segment_id": source_segment["segment_id"],
                "segment_position": source_segment["segment_position"],
                "segment_text_sha256": base.sha256_text(source_segment["segment_text"]),
                "segment_quality_sha256": base.sha256_text(
                    _canonical_json(source_segment["segment_quality"])
                ),
                "source_unit_receipts": copy.deepcopy(raw_segment["unit_receipts"]),
                "coverage_audit": copy.deepcopy(raw_segment["coverage_audit"]),
                "discourse_events": event_provenance,
                "concept_candidates": candidate_provenance,
            }
        )
    if dict(output) != untouched:
        raise CanonicalV31OutputError("projection mutated the completed model output")
    emitted_sha256 = base.sha256_text(_canonical_json(raw_views))
    projected_sha256 = base.sha256_text(_canonical_json(projected_views))
    if emitted_sha256 != projected_sha256:
        raise CanonicalV31OutputError("semantic fidelity hash differs after projection")
    provenance = {
        "schema_version": PROVENANCE_SCHEMA_VERSION,
        "candidate_system_id": CANDIDATE_SYSTEM_ID,
        "batch_id": validated["batch_id"],
        "episode_id": validated["episode_id"],
        "segments": provenance_segments,
        "evidence_span_ids_removed_from_canonical_labels": True,
        "exact_evidence_projected_from_private_source_text": True,
    }
    fidelity = {
        "schema_version": FIDELITY_SCHEMA_VERSION,
        "candidate_system_id": CANDIDATE_SYSTEM_ID,
        "batch_id": validated["batch_id"],
        "segment_count": len(labels),
        "emitted_event_count": sum(len(segment["discourse_events"]) for segment in raw_segments),
        "projected_event_count": sum(len(label["discourse_events"]) for label in labels),
        "emitted_concept_candidate_count": sum(
            len(segment["concept_candidates"]) for segment in raw_segments
        ),
        "projected_concept_candidate_count": sum(
            len(label["concept_candidates"]) for label in labels
        ),
        "emitted_semantics_sha256": emitted_sha256,
        "projected_emitted_semantics_sha256": projected_sha256,
        "all_emitted_semantic_values_preserved": True,
        "event_order_preserved": True,
        "concept_candidate_order_preserved": True,
        "semantic_postprocessing": False,
        "deterministic_semantic_defaults": {},
        "deterministic_semantic_pruning": False,
        "deterministic_deduplication": False,
        "deterministic_relabeling": False,
    }
    return {
        "schema_version": PROJECTION_SCHEMA_VERSION,
        "labels": labels,
        "provenance": provenance,
        "fidelity": fidelity,
    }


def _verify_started_thread(thread: Any, request: Mapping[str, Any]) -> dict[str, Any]:
    instruction_sources = expected_instruction_source_contract()
    if (
        getattr(thread, "model", None) != MODEL
        or getattr(thread, "ephemeral", None) is not True
        or getattr(thread, "base_instructions_sha256", None)
        != request["base_instructions_sha256"]
        or getattr(thread, "base_instructions_bytes", None)
        != len(request["base_instructions"].encode("utf-8"))
        or not isinstance(getattr(thread, "thread_id", None), str)
        or not thread.thread_id
        or getattr(thread, "instruction_sources_sha256", None)
        != instruction_sources["effective_instruction_sources_sha256"]
        or getattr(thread, "instruction_sources_count", None)
        != instruction_sources["effective_instruction_sources_count"]
    ):
        raise CanonicalV31TelemetryError(
            "started thread context-isolation contract failed"
        )
    return {
        "thread_id": thread.thread_id,
        "instruction_sources_sha256": instruction_sources[
            "effective_instruction_sources_sha256"
        ],
        "instruction_sources_count": instruction_sources[
            "effective_instruction_sources_count"
        ],
        "project_instruction_content_byte_budget": 0,
        "project_instruction_content_included": False,
        "context_control_overlay_sha256": request[
            "context_control_overlay_sha256"
        ],
    }


def validate_turn_sidecar(
    request: Mapping[str, Any],
    sidecar_path: Path,
    *,
    output_path: Path,
    expected_thread: Any,
) -> dict[str, Any]:
    """Validate the exact managed-auth sidecar for one bounded-span turn."""

    validated = validate_prepared_request(request)
    sidecar = base._load_json(sidecar_path, "bounded-span turn sidecar")  # noqa: SLF001
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
            "managed-auth completed sidecar contract failed"
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
        raise CanonicalV31TelemetryError("sidecar-bound raw output is absent or changed")
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


def model_evidence_ownership_field_paths() -> list[str]:
    return [
        "$.segments[].discourse_events[].evidence_span_id",
        "$.segments[].concept_candidates[].evidence_span_id",
        *[
            path
            for path in base.model_evidence_ownership_field_paths()
            if "evidence_start_unit_id" not in path and "evidence_end_unit_id" not in path
        ],
    ]


def build_six_arm_matrix_binding() -> dict[str, Any]:
    spans = model_evidence_ownership_field_paths()
    return {
        "schema_version": "pif_canonical_v31_bounded_span_adapter_binding_v1",
        "audited_adapter_binding": audited.build_six_arm_matrix_binding(),
        "audited_adapter_module_sha256": hashlib.sha256(
            Path(audited.__file__).read_bytes()
        ).hexdigest(),
        "bounded_span_adapter_module_sha256": hashlib.sha256(
            Path(__file__).read_bytes()
        ).hexdigest(),
        "candidate_system_id": CANDIDATE_SYSTEM_ID,
        "source_unit_chunking_version": SOURCE_UNIT_CHUNKING_VERSION,
        "source_unit_max_characters": SOURCE_UNIT_MAX_CHARS,
        "evidence_span_catalog_version": EVIDENCE_SPAN_CATALOG_VERSION,
        "canonical_evidence_max_characters": CANONICAL_EVIDENCE_MAX_CHARS,
        "model_evidence_ownership_field_paths": spans,
        "model_evidence_ownership_field_paths_sha256": base.sha256_text(
            _canonical_json(spans)
        ),
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
    "EVIDENCE_SPAN_CATALOG_VERSION",
    "EVIDENCE_SPAN_FIELD",
    "MODEL",
    "PINNED_CODEX",
    "PROJECT_ROOT",
    "SOURCE_UNIT_CHUNKING_VERSION",
    "SOURCE_UNIT_MAX_CHARS",
    "build_output_schema",
    "build_six_arm_matrix_binding",
    "expected_instruction_source_contract",
    "model_evidence_ownership_field_paths",
    "prepare_episode_batches",
    "semantic_integrity_contract",
    "validate_and_project_output",
    "validate_prepared_request",
    "validate_turn_sidecar",
    "verified_context_control_overlay",
)
