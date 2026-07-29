from __future__ import annotations

"""Reusable episode-batch adapter for the proven epoch-4 extractor.

The immutable epoch-4 canary proved one semantic algorithm over a two-segment
packet.  This module adapts that exact algorithm to evaluation episode batches
without changing its semantic instructions, full explicit-applicability event
schema, or duplicate-preserving projection.  The only dynamic schema changes
are request identity enums and exact request cardinalities.

Live-capacity admission is deliberately not embedded here.  The development
coordinator must supply and record that gate before invoking this adapter.

Deterministic work in this module is deliberately structural: packet planning,
schema/enumeration validation, exact evidence and offset projection, metric
substring checks, provenance, telemetry, and accounting.  It never prunes,
filters, deduplicates, merges, splits, or relabels an emitted event.
"""

import asyncio
import copy
import hashlib
import json
import math
import time
from collections import Counter
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from . import app_server_candidate_expanded_cap_exhaustive_full_schema as epoch4
from . import app_server_judge_v5_selection_v232_window_ledger as v232
from . import codex_app_server
from .util import now_iso, sha256_text, write_text_atomic


ADAPTER_SCHEMA_VERSION = "pif_expanded_cap_episode_batch_adapter_v1"
PRECOMMIT_SURFACE_VERSION = "pif_expanded_cap_precommit_surface_v1"
FROZEN_CONFIGURATION_VERSION = "pif_expanded_cap_frozen_configuration_v1"
RUN_SPEC_VERSION = "pif_expanded_cap_episode_batch_run_spec_v1"
RUN_REPORT_VERSION = "pif_expanded_cap_episode_batch_report_v1"
PRIVATE_MAPPING_VERSION = "pif_expanded_cap_episode_batch_private_mapping_v1"
NO_MODEL_PREFLIGHT_VERSION = "pif_expanded_cap_episode_batch_no_model_preflight_v1"
ATTEMPT_RECEIPT_VERSION = "pif_expanded_cap_semantic_call_attempt_v1"
CAPACITY_ADMISSION_VERSION = "pif_expanded_cap_capacity_admission_v1"
CAPACITY_BINDING_VERSION = "pif_expanded_cap_capacity_binding_v1"
RUN_CONFIGURATION_VERSION = "pif_expanded_cap_run_configuration_v1"
WINNER_SYSTEM_ID = "pif_epoch4_expanded_cap_exhaustive_full_schema_v4"
CANONICAL_DYNAMIC_SCHEMA_STRATEGY = (
    "epoch4_exact_template_replace_episode_segment_unit_enums_and_exact_cardinalities_v1"
)
SUPPORTED_BATCH_SIZES = (3, 5, 8)
SUPPORTED_THREAD_MODES = ("new_thread", "same_thread")
MODEL = epoch4.MODEL
EFFORT = epoch4.EFFORT
MAX_EVENTS_PER_SEGMENT = epoch4.MAX_EVENTS_PER_SEGMENT
RETRY_COUNT = 0
SEMANTIC_POSTPROCESSING = False
USAGE_FIELDS = tuple(epoch4.USAGE_FIELDS)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
EPOCH4_RUNTIME_LOCK_SHA256 = (
    "4eb40c371407366d4d66d378e9365f64c34e97db02b6b0b11cceda81a20f8eb9"
)
EPOCH4_TURN_NAME = "expanded-cap-exhaustive-full-schema"

_CONTEXT_MARKER = "\n\n# Immutable episode context\n"
_APPLICABILITY_MARKER = "\n\n# Explicit applicability contract\n"
_CONTEXT_PLACEHOLDER = "{EPISODE_CONTEXT_JSON}"
_SOURCE_PACKET_PLACEHOLDER = "{SOURCE_PACKET_JSON}"
_DYNAMIC_EPISODE_ID = "__DYNAMIC_EPISODE_ID__"
_DYNAMIC_SEGMENT_IDS = "__DYNAMIC_SEGMENT_IDS__"
_DYNAMIC_UNIT_IDS = "__DYNAMIC_UNIT_IDS__"
_DYNAMIC_SEGMENT_COUNT = "__DYNAMIC_SEGMENT_COUNT__"
_DYNAMIC_MAX_UNIT_COUNT = "__DYNAMIC_MAX_UNIT_COUNT__"
_PREPARED_REQUEST_KEYS = {
    "schema_version",
    "winner_system_id",
    "batch_id",
    "batch_index",
    "episode_id",
    "segment_ids",
    "batch_size_ceiling",
    "effective_batch_size",
    "thread_mode",
    "model",
    "effort",
    "max_events_per_segment",
    "retry_count",
    "semantic_postprocessing",
    "episode_context",
    "private_input",
    "prompt",
    "base_instructions",
    "schema",
    "projection_schema",
    "canonical_dynamic_schema_strategy",
    "canonical_dynamic_schema_sha256",
    "prompt_sha256",
    "base_instructions_sha256",
    "output_schema_sha256",
    "projection_schema_sha256",
    "semantic_parity",
}


class ExpandedCapEpisodeBatchError(RuntimeError):
    """The reusable epoch-4 batch contract or its integrity failed."""


class ExpandedCapEpisodeBatchOutputError(ExpandedCapEpisodeBatchError):
    """A completed model output failed deterministic structural validation."""


class ExpandedCapEpisodeBatchTelemetryError(ExpandedCapEpisodeBatchError):
    """A turn sidecar did not provide complete, trustworthy telemetry."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.expanduser().resolve().open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _record(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve(strict=True)
    return {
        "path": str(resolved),
        "sha256": _sha256_file(resolved),
        "size_bytes": resolved.stat().st_size,
    }


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.expanduser().resolve().read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ExpandedCapEpisodeBatchError(f"cannot read {label}") from exc


def _write_immutable_json(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    resolved = path.expanduser().resolve()
    if resolved.exists():
        if not resolved.is_file() or resolved.read_text(encoding="utf-8") != payload:
            raise ExpandedCapEpisodeBatchError(f"frozen {resolved.name} drifted")
        return
    resolved.parent.mkdir(parents=True, exist_ok=True)
    write_text_atomic(resolved, payload)


def _write_immutable_text(path: Path, value: str) -> None:
    resolved = path.expanduser().resolve()
    if resolved.exists():
        if not resolved.is_file() or resolved.read_text(encoding="utf-8") != value:
            raise ExpandedCapEpisodeBatchError(f"frozen {resolved.name} drifted")
        return
    resolved.parent.mkdir(parents=True, exist_ok=True)
    write_text_atomic(resolved, value)


def _nonempty_identifier(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ExpandedCapEpisodeBatchError(f"{label} must be a non-empty identifier")
    return value


def _nonnegative_number(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ExpandedCapEpisodeBatchTelemetryError(f"{label} is not numeric")
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise ExpandedCapEpisodeBatchTelemetryError(f"{label} is not finite and nonnegative")
    return result


def _is_iso_timestamp(value: Any) -> bool:
    if not isinstance(value, str) or not value or value != value.strip():
        return False
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return True


def _usage_values(value: Any, label: str) -> dict[str, int]:
    if not isinstance(value, Mapping):
        raise ExpandedCapEpisodeBatchTelemetryError(f"{label} is absent")
    result: dict[str, int] = {}
    for field in USAGE_FIELDS:
        item = value.get(field)
        if isinstance(item, bool) or not isinstance(item, int) or item < 0:
            raise ExpandedCapEpisodeBatchTelemetryError(f"{label}.{field} is invalid")
        result[field] = item
    if (
        result["cached_input_tokens"] > result["input_tokens"]
        or result["reasoning_output_tokens"] > result["output_tokens"]
        or result["total_tokens"]
        != result["input_tokens"] + result["output_tokens"]
    ):
        raise ExpandedCapEpisodeBatchTelemetryError(f"{label} is inconsistent")
    return result


@lru_cache(maxsize=1)
def _epoch4_snapshot() -> dict[str, Any]:
    """Verify the immutable epoch-4 request without re-reading ambient policy.

    Epoch 4 intentionally set ``project_doc_max_bytes`` to zero. Codex still
    reports discovered AGENTS.md paths in ``instructionSources``, but none of
    those file bytes are eligible for the model-visible project-instruction
    block. The historical runtime lock nevertheless checksum-bound the current
    contents of the reported path. Re-validating that irrelevant ambient file
    would make a proven semantic request unusable whenever global operations
    policy changes.

    This verifier therefore pins the historical runtime lock itself, all five
    frozen request artifacts, the strict config overlay, and the reported path
    contract. It deliberately does not re-hash the contents of an instruction
    source whose configured inclusion budget is exactly zero bytes.
    """

    lock_path = epoch4.DEFAULT_OUTPUT_ROOT / "runtime-lock.json"
    if _sha256_file(lock_path) != EPOCH4_RUNTIME_LOCK_SHA256:
        raise ExpandedCapEpisodeBatchError("epoch-4 runtime-lock snapshot drifted")
    lock = _load_json(lock_path, "epoch-4 runtime-lock snapshot")
    if not isinstance(lock, Mapping):
        raise ExpandedCapEpisodeBatchError("epoch-4 runtime-lock snapshot is malformed")

    overlay_path = epoch4.DEFAULT_OUTPUT_ROOT / "config-overlay.json"
    overlay = _load_json(overlay_path, "epoch-4 context-control overlay")
    if (
        not isinstance(overlay, Mapping)
        or overlay.get("project_doc_max_bytes") != 0
        or lock.get("config_overlay") != _record(overlay_path)
    ):
        raise ExpandedCapEpisodeBatchError(
            "epoch-4 zero-byte project-instruction boundary drifted"
        )

    turn_root = epoch4.DEFAULT_OUTPUT_ROOT / "turns" / EPOCH4_TURN_NAME
    paths = {
        "input": turn_root / "input.private.json",
        "prompt": turn_root / "prompt.private.md",
        "base": turn_root / "base-instructions.private.md",
        "schema": turn_root / "schema.json",
        "projection_schema": turn_root / "projection-schema.json",
    }
    expected_records = [_record(path) for path in paths.values()]
    if lock.get("frozen_request") != expected_records:
        raise ExpandedCapEpisodeBatchError("epoch-4 frozen request snapshot drifted")

    source_paths = lock.get("effective_instruction_source_paths")
    source_records = lock.get("effective_instruction_source_records")
    if (
        not isinstance(source_paths, list)
        or any(not isinstance(path, str) or not path for path in source_paths)
        or not isinstance(source_records, list)
        or len(source_records) != len(source_paths)
        or [record.get("path") for record in source_records if isinstance(record, Mapping)]
        != source_paths
        or any(
            not isinstance(record, Mapping)
            or set(record) != {"path", "sha256", "size_bytes"}
            or not isinstance(record.get("sha256"), str)
            or len(str(record["sha256"])) != 64
            or isinstance(record.get("size_bytes"), bool)
            or not isinstance(record.get("size_bytes"), int)
            or int(record["size_bytes"]) < 0
            for record in source_records
        )
        or lock.get("effective_instruction_sources_sha256")
        != sha256_text(_canonical_json(source_paths))
    ):
        raise ExpandedCapEpisodeBatchError(
            "epoch-4 reported instruction-source path contract drifted"
        )
    return {
        "lock": dict(lock),
        "lock_path": lock_path,
        "overlay": copy.deepcopy(dict(overlay)),
        "paths": paths,
        "source_paths": list(source_paths),
        "source_records": copy.deepcopy(list(source_records)),
    }


@lru_cache(maxsize=1)
def _epoch4_template_json() -> dict[str, str]:
    """Load the checksum-verified immutable epoch-4 request artifacts once."""

    paths = _epoch4_snapshot()["paths"]
    return {
        "base": paths["base"].read_text(encoding="utf-8"),
        "prompt": paths["prompt"].read_text(encoding="utf-8"),
        "schema": _canonical_json(_load_json(paths["schema"], "epoch-4 schema")),
        "projection_schema": _canonical_json(
            _load_json(paths["projection_schema"], "epoch-4 projection schema")
        ),
    }


def _epoch4_template() -> dict[str, Any]:
    frozen = _epoch4_template_json()
    return {
        "base": frozen["base"],
        "prompt": frozen["prompt"],
        "schema": json.loads(frozen["schema"]),
        "projection_schema": json.loads(frozen["projection_schema"]),
    }


def _epoch4_instruction_source_contract() -> dict[str, Any]:
    """Bind reported paths while proving their model-visible byte budget is zero."""

    snapshot = _epoch4_snapshot()
    paths = snapshot["source_paths"]
    records = snapshot["source_records"]
    records_sha256 = sha256_text(_canonical_json(records))
    return {
        "effective_instruction_source_paths": list(paths),
        "effective_instruction_sources_count": len(paths),
        "effective_instruction_sources_sha256": sha256_text(
            _canonical_json(paths)
        ),
        # Retain the established field for downstream receipt compatibility,
        # while naming its historical-only meaning explicitly as well.
        "effective_instruction_source_records_sha256": records_sha256,
        "historical_effective_instruction_source_records_sha256": records_sha256,
        "project_instruction_content_byte_budget": 0,
        "project_instruction_content_included": False,
    }


def verified_context_control_overlay() -> dict[str, Any]:
    """Return the immutable strict-config overlay used by every new run."""

    return copy.deepcopy(dict(_epoch4_snapshot()["overlay"]))


def semantic_instruction_template() -> str:
    """Return epoch-4 instructions with only episode context parameterized."""

    base = str(_epoch4_template()["base"])
    if base.count(_CONTEXT_MARKER) != 1 or base.count(_APPLICABILITY_MARKER) != 1:
        raise ExpandedCapEpisodeBatchError("epoch-4 instruction markers drifted")
    prefix, context_and_suffix = base.split(_CONTEXT_MARKER, 1)
    old_context, suffix = context_and_suffix.split(_APPLICABILITY_MARKER, 1)
    trailing_context_newlines = old_context[len(old_context.rstrip("\r\n")) :]
    if trailing_context_newlines != "\n":
        raise ExpandedCapEpisodeBatchError(
            "epoch-4 episode-context whitespace drifted"
        )
    rendered = (
        prefix
        + _CONTEXT_MARKER
        + _CONTEXT_PLACEHOLDER
        + trailing_context_newlines
        + _APPLICABILITY_MARKER
        + suffix
    )
    if (
        epoch4.EXHAUSTIVE_INSTRUCTIONS not in rendered
        or epoch4.NEW_CAP_INSTRUCTION not in rendered
        or epoch4.OLD_CAP_INSTRUCTION in rendered
    ):
        raise ExpandedCapEpisodeBatchError("epoch-4 exhaustive instructions drifted")
    return rendered


def semantic_prompt_template() -> str:
    """Return the epoch-4 blind source-packet prompt framing."""

    prompt = str(_epoch4_template()["prompt"])
    prefix = "# Blind source-unit packet\n"
    if not prompt.startswith(prefix) or not prompt.endswith("\n"):
        raise ExpandedCapEpisodeBatchError("epoch-4 prompt framing drifted")
    return prefix + _SOURCE_PACKET_PLACEHOLDER + "\n"


def _schema_dynamic_nodes(schema: dict[str, Any]) -> dict[str, Any]:
    try:
        segments = schema["properties"]["segments"]
        segment = segments["items"]
        properties = segment["properties"]
        events = properties["events"]
        receipts = properties["unit_receipts"]
        event_properties = events["items"]["properties"]
        receipt_properties = receipts["items"]["properties"]
        return {
            "episode": schema["properties"]["episode_id"],
            "segments": segments,
            "segment_id": properties["segment_id"],
            "events": events,
            "receipts": receipts,
            "receipt_unit": receipt_properties["unit_id"],
            "eligible_count": receipt_properties["eligible_event_count"],
            "unresolved_count": receipt_properties["unresolved_count"],
            "evidence_start": event_properties["evidence_start_unit_id"],
            "evidence_end": event_properties["evidence_end_unit_id"],
        }
    except (KeyError, TypeError) as exc:
        raise ExpandedCapEpisodeBatchError("epoch-4 schema shape drifted") from exc


def _assert_epoch4_schema_semantics(schema: Mapping[str, Any]) -> None:
    copy_schema = copy.deepcopy(dict(schema))
    nodes = _schema_dynamic_nodes(copy_schema)
    if (
        nodes["events"].get("maxItems") != MAX_EVENTS_PER_SEGMENT
        or nodes["eligible_count"].get("maximum") != MAX_EVENTS_PER_SEGMENT
        or nodes["unresolved_count"].get("maximum")
        != epoch4.PRIOR_MAX_EVENTS_PER_SEGMENT
    ):
        raise ExpandedCapEpisodeBatchError("epoch-4 48-event schema semantics drifted")


def _canonicalize_dynamic_schema(schema: Mapping[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(dict(schema))
    nodes = _schema_dynamic_nodes(result)
    nodes["episode"]["enum"] = [_DYNAMIC_EPISODE_ID]
    nodes["segments"]["minItems"] = _DYNAMIC_SEGMENT_COUNT
    nodes["segments"]["maxItems"] = _DYNAMIC_SEGMENT_COUNT
    nodes["segment_id"]["enum"] = [_DYNAMIC_SEGMENT_IDS]
    nodes["receipts"]["maxItems"] = _DYNAMIC_MAX_UNIT_COUNT
    nodes["receipt_unit"]["enum"] = [_DYNAMIC_UNIT_IDS]
    nodes["evidence_start"]["enum"] = [_DYNAMIC_UNIT_IDS]
    nodes["evidence_end"]["enum"] = [_DYNAMIC_UNIT_IDS]
    return result


def canonical_dynamic_schema_bundle() -> dict[str, Any]:
    """Return the ID-agnostic exact epoch-4 schema-template bundle."""

    template = _epoch4_template()
    _assert_epoch4_schema_semantics(template["schema"])
    _assert_epoch4_schema_semantics(template["projection_schema"])
    bundle = {
        "strategy": CANONICAL_DYNAMIC_SCHEMA_STRATEGY,
        "explicit_applicability_schema": _canonicalize_dynamic_schema(
            template["schema"]
        ),
        "projection_schema": _canonicalize_dynamic_schema(
            template["projection_schema"]
        ),
    }
    bundle["canonical_dynamic_schema_sha256"] = sha256_text(
        _canonical_json(bundle)
    )
    return bundle


def _specialize_schema(
    template: Mapping[str, Any],
    *,
    episode_id: str,
    segment_ids: Sequence[str],
    unit_ids: Sequence[str],
    maximum_unit_count: int,
) -> dict[str, Any]:
    if (
        not segment_ids
        or len(set(segment_ids)) != len(segment_ids)
        or not unit_ids
        or len(set(unit_ids)) != len(unit_ids)
        or maximum_unit_count < 1
    ):
        raise ExpandedCapEpisodeBatchError("dynamic schema identities are invalid")
    result = copy.deepcopy(dict(template))
    nodes = _schema_dynamic_nodes(result)
    nodes["episode"]["enum"] = [episode_id]
    nodes["segments"]["minItems"] = len(segment_ids)
    nodes["segments"]["maxItems"] = len(segment_ids)
    nodes["segment_id"]["enum"] = list(segment_ids)
    nodes["receipts"]["maxItems"] = maximum_unit_count
    nodes["receipt_unit"]["enum"] = list(unit_ids)
    nodes["evidence_start"]["enum"] = list(unit_ids)
    nodes["evidence_end"]["enum"] = list(unit_ids)
    _assert_epoch4_schema_semantics(result)
    return result


def specialize_dynamic_schemas(
    *,
    episode_id: str,
    segment_ids: Sequence[str],
    unit_ids: Sequence[str],
    maximum_unit_count: int,
) -> dict[str, Any]:
    """Specialize only epoch-4 identity enums and exact request cardinalities."""

    validated_episode = _nonempty_identifier(episode_id, "episode_id")
    validated_segments = tuple(
        _nonempty_identifier(value, "segment_id") for value in segment_ids
    )
    validated_units = tuple(
        _nonempty_identifier(value, "unit_id") for value in unit_ids
    )
    template = _epoch4_template()
    schema = _specialize_schema(
        template["schema"],
        episode_id=validated_episode,
        segment_ids=validated_segments,
        unit_ids=validated_units,
        maximum_unit_count=maximum_unit_count,
    )
    projection_schema = _specialize_schema(
        template["projection_schema"],
        episode_id=validated_episode,
        segment_ids=validated_segments,
        unit_ids=validated_units,
        maximum_unit_count=maximum_unit_count,
    )
    canonical = canonical_dynamic_schema_bundle()
    if (
        _canonicalize_dynamic_schema(schema)
        != canonical["explicit_applicability_schema"]
        or _canonicalize_dynamic_schema(projection_schema)
        != canonical["projection_schema"]
    ):
        raise ExpandedCapEpisodeBatchError("dynamic schema weakened epoch-4 template")
    return {
        "schema": schema,
        "projection_schema": projection_schema,
        "canonical_dynamic_schema_strategy": CANONICAL_DYNAMIC_SCHEMA_STRATEGY,
        "canonical_dynamic_schema_sha256": canonical[
            "canonical_dynamic_schema_sha256"
        ],
    }


def _episode_context(episode: Mapping[str, Any]) -> dict[str, Any]:
    episode_id = _nonempty_identifier(episode.get("episode_id"), "episode_id")
    supplied = episode.get("episode_context")
    context = supplied if isinstance(supplied, Mapping) else episode
    context_episode_id = context.get("episode_id", episode_id)
    if context_episode_id != episode_id:
        raise ExpandedCapEpisodeBatchError("episode context id drifted")
    values: dict[str, Any] = {"episode_id": episode_id}
    for field in ("source_name", "episode_title", "context_summary"):
        value = context.get(field)
        if value is None:
            value = episode.get(field) or ""
        if not isinstance(value, str):
            raise ExpandedCapEpisodeBatchError(f"episode context {field} is invalid")
        values[field] = value
    speakers = context.get("speaker_map")
    if speakers is None:
        speakers = episode.get("speaker_map") or []
    if not isinstance(speakers, list) or any(
        not isinstance(item, Mapping) for item in speakers
    ):
        raise ExpandedCapEpisodeBatchError("episode context speaker_map is invalid")
    values["speaker_map"] = copy.deepcopy(speakers)

    section_map = context.get("section_map")
    if section_map is None:
        section_map = episode.get("section_map")
    if not isinstance(section_map, list) or any(
        not isinstance(item, Mapping) for item in section_map
    ):
        raise ExpandedCapEpisodeBatchError("episode context section_map is invalid")
    values["section_map"] = copy.deepcopy(section_map)

    entity_seed = context.get("entity_seed")
    if entity_seed is None:
        entity_seed = episode.get("entity_seed")
    if not isinstance(entity_seed, Mapping):
        raise ExpandedCapEpisodeBatchError("episode context entity_seed is invalid")
    values["entity_seed"] = copy.deepcopy(dict(entity_seed))

    concept_seed = context.get("concept_seed")
    if concept_seed is None:
        concept_seed = episode.get("concept_seed")
    if not isinstance(concept_seed, list) or any(
        not isinstance(item, str) for item in concept_seed
    ):
        raise ExpandedCapEpisodeBatchError("episode context concept_seed is invalid")
    values["concept_seed"] = copy.deepcopy(concept_seed)

    guidance = context.get("extraction_guidance")
    if guidance is None:
        guidance = episode.get("extraction_guidance")
    if not isinstance(guidance, str):
        raise ExpandedCapEpisodeBatchError(
            "episode context extraction_guidance is invalid"
        )
    values["extraction_guidance"] = guidance

    excluded = context.get("excluded_source_context")
    if excluded is None:
        excluded = episode.get("excluded_source_context")
    if not isinstance(excluded, list) or any(
        not isinstance(item, str) for item in excluded
    ):
        raise ExpandedCapEpisodeBatchError(
            "episode context excluded_source_context is invalid"
        )
    values["excluded_source_context"] = copy.deepcopy(excluded)
    return values


def _validate_boundaries(
    segment_text: str, boundaries: Sequence[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    if not boundaries:
        raise ExpandedCapEpisodeBatchError("segment boundaries are empty")
    copied: list[dict[str, Any]] = []
    window_ids: set[int] = set()
    required = (
        "window_id",
        "owner_start",
        "owner_end",
        "extract_start",
        "extract_end",
    )
    for row in boundaries:
        if not isinstance(row, Mapping):
            raise ExpandedCapEpisodeBatchError("segment boundary is not an object")
        values: dict[str, int] = {}
        for field in required:
            value = row.get(field)
            if isinstance(value, bool) or not isinstance(value, int):
                raise ExpandedCapEpisodeBatchError(
                    f"segment boundary {field} is invalid"
                )
            values[field] = value
        if (
            values["window_id"] in window_ids
            or values["extract_start"] < 0
            or values["extract_start"] > values["owner_start"]
            or values["owner_start"] > values["owner_end"]
            or values["owner_end"] > values["extract_end"]
            or values["extract_end"] > len(segment_text)
        ):
            raise ExpandedCapEpisodeBatchError("segment boundary offsets are invalid")
        window_ids.add(values["window_id"])
        copied.append(copy.deepcopy(dict(row)))
    return copied


def _prepare_segment(
    segment: Mapping[str, Any], *, segment_position: int
) -> dict[str, Any]:
    segment_id = _nonempty_identifier(segment.get("segment_id"), "segment_id")
    text = segment.get("segment_text")
    if not isinstance(text, str) or not text:
        raise ExpandedCapEpisodeBatchError("segment_text must be non-empty")
    raw_boundaries = segment.get("boundaries")
    if not isinstance(raw_boundaries, Sequence) or isinstance(
        raw_boundaries, (str, bytes)
    ):
        raise ExpandedCapEpisodeBatchError("segment boundaries are invalid")
    boundaries = _validate_boundaries(text, raw_boundaries)
    try:
        units = v232.source_units(
            text, boundaries, segment_position=segment_position
        )
    except (v232.V232WindowLedgerError, KeyError, TypeError, ValueError) as exc:
        raise ExpandedCapEpisodeBatchError("cannot build exact source units") from exc
    for unit in units:
        start = int(unit["start_char"])
        end = int(unit["end_char"])
        if text[start:end] != unit["text"] or not any(
            int(boundary["extract_start"]) <= start
            and end <= int(boundary["extract_end"])
            for boundary in boundaries
        ):
            raise ExpandedCapEpisodeBatchError("source unit offset projection failed")
    density = segment.get("density_stratum", "unstratified")
    if not isinstance(density, str) or not density:
        raise ExpandedCapEpisodeBatchError("density_stratum is invalid")
    return {
        "segment_id": segment_id,
        "segment_text": text,
        "boundaries": boundaries,
        "units": units,
        "density_stratum": density,
        "segment_position": segment_position,
    }


def _render_base_instructions(context: Mapping[str, Any]) -> str:
    template = semantic_instruction_template()
    context_json = json.dumps(context, ensure_ascii=True, separators=(",", ":"))
    rendered = template.replace(_CONTEXT_PLACEHOLDER, context_json)
    if rendered.count(context_json) != 1 or _CONTEXT_PLACEHOLDER in rendered:
        raise ExpandedCapEpisodeBatchError("episode context rendering failed")
    return rendered


def _render_prompt(episode_id: str, segments: Sequence[Mapping[str, Any]]) -> str:
    prompt_segments = []
    for segment in segments:
        prompt_segments.append(
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
            }
        )
    packet = {"episode_id": episode_id, "segments": prompt_segments}
    packet_json = json.dumps(packet, ensure_ascii=True, separators=(",", ":"))
    return semantic_prompt_template().replace(_SOURCE_PACKET_PLACEHOLDER, packet_json)


def _batch_id(
    *,
    episode_id: str,
    batch_size: int,
    thread_mode: str,
    batch_index: int,
    segment_ids: Sequence[str],
) -> str:
    identity = {
        "winner_system_id": WINNER_SYSTEM_ID,
        "episode_id": episode_id,
        "batch_size": batch_size,
        "thread_mode": thread_mode,
        "batch_index": batch_index,
        "segment_ids": list(segment_ids),
    }
    return "ecb_" + sha256_text(_canonical_json(identity))[:24]


def _validate_arm_choice(batch_size: int, thread_mode: str) -> None:
    if isinstance(batch_size, bool) or batch_size not in SUPPORTED_BATCH_SIZES:
        raise ExpandedCapEpisodeBatchError(
            "batch_size must be one of the frozen development sizes 3, 5, or 8"
        )
    if thread_mode not in SUPPORTED_THREAD_MODES:
        raise ExpandedCapEpisodeBatchError(
            "thread_mode must be new_thread or same_thread"
        )


def _semantic_parity_receipt() -> dict[str, bool]:
    return {
        "epoch4_exhaustive_instructions_exact": True,
        "epoch4_explicit_applicability_schema_exact_except_dynamic_ids_and_cardinalities": True,
        "epoch4_duplicate_preserving_projection": True,
        "deterministic_semantic_pruning": False,
        "deterministic_support_filtering": False,
        "deterministic_deduplication": False,
        "deterministic_relabeling": False,
    }


def prepare_episode_batches(
    episode: Mapping[str, Any], *, batch_size: int, thread_mode: str
) -> list[dict[str, Any]]:
    """Prepare exact epoch-4 requests for one episode without a model call."""

    _validate_arm_choice(batch_size, thread_mode)
    context = _episode_context(episode)
    raw_segments = episode.get("segments")
    if not isinstance(raw_segments, Sequence) or isinstance(
        raw_segments, (str, bytes)
    ) or not raw_segments:
        raise ExpandedCapEpisodeBatchError("episode segments are empty")
    prepared_segments = [
        _prepare_segment(segment, segment_position=index)
        for index, segment in enumerate(raw_segments)
        if isinstance(segment, Mapping)
    ]
    if len(prepared_segments) != len(raw_segments):
        raise ExpandedCapEpisodeBatchError("episode segment is not an object")
    segment_ids = [row["segment_id"] for row in prepared_segments]
    if len(set(segment_ids)) != len(segment_ids):
        raise ExpandedCapEpisodeBatchError("episode segment ids are not unique")
    episode_id = context["episode_id"]
    base = _render_base_instructions(context)
    requests: list[dict[str, Any]] = []
    for batch_index, start in enumerate(range(0, len(prepared_segments), batch_size)):
        segments = prepared_segments[start : start + batch_size]
        batch_segment_ids = [row["segment_id"] for row in segments]
        unit_ids = [
            str(unit["unit_id"])
            for segment in segments
            for unit in segment["units"]
        ]
        maximum_unit_count = max(len(segment["units"]) for segment in segments)
        specialized = specialize_dynamic_schemas(
            episode_id=episode_id,
            segment_ids=batch_segment_ids,
            unit_ids=unit_ids,
            maximum_unit_count=maximum_unit_count,
        )
        private_input = {
            "schema_version": ADAPTER_SCHEMA_VERSION,
            "episode_id": episode_id,
            "segments": [
                {
                    key: copy.deepcopy(segment[key])
                    for key in (
                        "segment_id",
                        "segment_text",
                        "boundaries",
                        "units",
                        "density_stratum",
                        "segment_position",
                    )
                }
                for segment in segments
            ],
            "privacy": "private source units and episode context",
        }
        prompt = _render_prompt(episode_id, segments)
        batch_id = _batch_id(
            episode_id=episode_id,
            batch_size=batch_size,
            thread_mode=thread_mode,
            batch_index=batch_index,
            segment_ids=batch_segment_ids,
        )
        request = {
            "schema_version": ADAPTER_SCHEMA_VERSION,
            "winner_system_id": WINNER_SYSTEM_ID,
            "batch_id": batch_id,
            "batch_index": batch_index,
            "episode_id": episode_id,
            "segment_ids": batch_segment_ids,
            "batch_size_ceiling": batch_size,
            "effective_batch_size": len(segments),
            "thread_mode": thread_mode,
            "model": MODEL,
            "effort": EFFORT,
            "max_events_per_segment": MAX_EVENTS_PER_SEGMENT,
            "retry_count": RETRY_COUNT,
            "semantic_postprocessing": SEMANTIC_POSTPROCESSING,
            "episode_context": copy.deepcopy(context),
            "private_input": private_input,
            "prompt": prompt,
            "base_instructions": base,
            "schema": specialized["schema"],
            "projection_schema": specialized["projection_schema"],
            "canonical_dynamic_schema_strategy": specialized[
                "canonical_dynamic_schema_strategy"
            ],
            "canonical_dynamic_schema_sha256": specialized[
                "canonical_dynamic_schema_sha256"
            ],
            "prompt_sha256": sha256_text(prompt),
            "base_instructions_sha256": sha256_text(base),
            "output_schema_sha256": sha256_text(
                _canonical_json(specialized["schema"])
            ),
            "projection_schema_sha256": sha256_text(
                _canonical_json(specialized["projection_schema"])
            ),
            "semantic_parity": _semantic_parity_receipt(),
        }
        validate_prepared_request(request)
        requests.append(request)
    return requests


def validate_prepared_request(request: Mapping[str, Any]) -> dict[str, Any]:
    """Fail closed if a prepared batch no longer matches epoch-4."""

    if (
        set(request) != _PREPARED_REQUEST_KEYS
        or request.get("schema_version") != ADAPTER_SCHEMA_VERSION
        or request.get("winner_system_id") != WINNER_SYSTEM_ID
        or request.get("model") != MODEL
        or request.get("effort") != EFFORT
        or request.get("max_events_per_segment") != MAX_EVENTS_PER_SEGMENT
        or request.get("retry_count") != RETRY_COUNT
        or request.get("semantic_postprocessing") is not False
        or request.get("semantic_parity") != _semantic_parity_receipt()
    ):
        raise ExpandedCapEpisodeBatchError("prepared request fixed contract drifted")
    batch_size = request.get("batch_size_ceiling")
    thread_mode = request.get("thread_mode")
    if not isinstance(batch_size, int) or not isinstance(thread_mode, str):
        raise ExpandedCapEpisodeBatchError("prepared request arm choice is invalid")
    _validate_arm_choice(batch_size, thread_mode)
    episode_id = _nonempty_identifier(request.get("episode_id"), "episode_id")
    context = request.get("episode_context")
    private_input = request.get("private_input")
    segment_ids = request.get("segment_ids")
    if (
        not isinstance(context, Mapping)
        or not isinstance(private_input, Mapping)
        or set(private_input)
        != {"schema_version", "episode_id", "segments", "privacy"}
        or private_input.get("schema_version") != ADAPTER_SCHEMA_VERSION
        or private_input.get("privacy")
        != "private source units and episode context"
        or not isinstance(segment_ids, list)
        or not segment_ids
        or private_input.get("episode_id") != episode_id
        or [row.get("segment_id") for row in private_input.get("segments") or []]
        != segment_ids
        or request.get("effective_batch_size") != len(segment_ids)
        or len(segment_ids) > batch_size
    ):
        raise ExpandedCapEpisodeBatchError("prepared request identity drifted")
    expected_context = _episode_context(
        {"episode_id": episode_id, "episode_context": context}
    )
    if dict(context) != expected_context:
        raise ExpandedCapEpisodeBatchError("prepared request episode context drifted")
    private_segments = list(private_input["segments"])
    rederived_segments: list[dict[str, Any]] = []
    for segment in private_segments:
        if not isinstance(segment, Mapping):
            raise ExpandedCapEpisodeBatchError(
                "prepared request segment is not an object"
            )
        position = segment.get("segment_position")
        if isinstance(position, bool) or not isinstance(position, int) or position < 0:
            raise ExpandedCapEpisodeBatchError(
                "prepared request segment position is invalid"
            )
        expected_segment = _prepare_segment(segment, segment_position=position)
        if dict(segment) != expected_segment:
            raise ExpandedCapEpisodeBatchError(
                "prepared request source-unit projection drifted"
            )
        rederived_segments.append(expected_segment)
    positions = [segment["segment_position"] for segment in rederived_segments]
    if positions != sorted(positions) or len(set(positions)) != len(positions):
        raise ExpandedCapEpisodeBatchError(
            "prepared request segment positions drifted"
        )
    unit_ids = [
        str(unit["unit_id"])
        for segment in rederived_segments
        for unit in segment["units"]
    ]
    maximum_unit_count = max(
        len(segment["units"]) for segment in rederived_segments
    )
    specialized = specialize_dynamic_schemas(
        episode_id=episode_id,
        segment_ids=segment_ids,
        unit_ids=unit_ids,
        maximum_unit_count=maximum_unit_count,
    )
    if (
        request.get("schema") != specialized["schema"]
        or request.get("projection_schema") != specialized["projection_schema"]
        or request.get("canonical_dynamic_schema_strategy")
        != CANONICAL_DYNAMIC_SCHEMA_STRATEGY
        or request.get("canonical_dynamic_schema_sha256")
        != specialized["canonical_dynamic_schema_sha256"]
    ):
        raise ExpandedCapEpisodeBatchError("prepared request schema drifted")
    reconstructed_segments = [
        {
            "segment_id": segment["segment_id"],
            "units": segment["units"],
        }
        for segment in rederived_segments
    ]
    expected_prompt = _render_prompt(episode_id, reconstructed_segments)
    expected_base = _render_base_instructions(expected_context)
    if (
        request.get("prompt") != expected_prompt
        or request.get("base_instructions") != expected_base
        or request.get("prompt_sha256") != sha256_text(expected_prompt)
        or request.get("base_instructions_sha256") != sha256_text(expected_base)
        or request.get("output_schema_sha256")
        != sha256_text(_canonical_json(specialized["schema"]))
        or request.get("projection_schema_sha256")
        != sha256_text(_canonical_json(specialized["projection_schema"]))
    ):
        raise ExpandedCapEpisodeBatchError("prepared request text hash drifted")
    expected_batch_id = _batch_id(
        episode_id=episode_id,
        batch_size=batch_size,
        thread_mode=thread_mode,
        batch_index=int(request.get("batch_index")),
        segment_ids=segment_ids,
    )
    if request.get("batch_id") != expected_batch_id:
        raise ExpandedCapEpisodeBatchError("prepared request batch id drifted")
    return copy.deepcopy(dict(request))


def validate_and_project_output(
    request: Mapping[str, Any], output: Mapping[str, Any]
) -> dict[str, Any]:
    """Apply only epoch-4 validation and duplicate-preserving projection."""

    validated = validate_prepared_request(request)
    turn = {
        "episode_id": validated["episode_id"],
        "segment_ids": list(validated["segment_ids"]),
        "private_input": copy.deepcopy(validated["private_input"]),
        "schema": copy.deepcopy(validated["schema"]),
        "direct_schema": copy.deepcopy(validated["projection_schema"]),
    }
    try:
        normalized, provenance, diagnostics, applicability = epoch4.project_output(
            output, turn
        )
    except (epoch4.StructuralRejectionError, KeyError, TypeError, ValueError) as exc:
        raise ExpandedCapEpisodeBatchOutputError(
            "completed batch output failed epoch-4 structural validation"
        ) from exc
    emitted = sum(
        len(segment.get("events") or []) for segment in output.get("segments") or []
    )
    projected = sum(
        len(segment.get("events") or [])
        for segment in normalized.get("segments") or []
    )
    if (
        emitted != projected
        or applicability.get("all_emitted_events_preserved") is not True
        or applicability.get("semantic_pruning_performed") is not False
        or applicability.get("support_filtering_performed") is not False
        or applicability.get("deduplication_performed") is not False
        or applicability.get("relabeling_performed") is not False
    ):
        raise ExpandedCapEpisodeBatchOutputError(
            "epoch-4 projection changed emitted semantics"
        )
    return {
        "normalized": normalized,
        "provenance": provenance,
        "diagnostics": diagnostics,
        "applicability": applicability,
    }


def _prompt_instruction_hash() -> str:
    return sha256_text(
        _canonical_json(
            {
                "semantic_instruction_template": semantic_instruction_template(),
                "semantic_prompt_template": semantic_prompt_template(),
            }
        )
    )


def capacity_admission_contract() -> dict[str, Any]:
    """Describe the coordinator-owned receipt this adapter only verifies."""

    base = {
        "schema_version": CAPACITY_ADMISSION_VERSION,
        "canonicalization": "utf8_sorted_compact_json_v1",
        "hash_algorithm": "sha256",
        "required_before_app_server_start": True,
        "measurement_owned_by_adapter": False,
        "issuer_role": "evaluation_coordinator",
        "maximum_validity_seconds": 900,
        "scope_fields": [
            "winner_system_id",
            "batch_size",
            "thread_mode",
            "model",
            "effort",
            "concurrency",
            "episode_ids_sha256",
            "episode_count",
            "segment_count",
            "batch_count",
        ],
    }
    return {
        **base,
        "contract_sha256": sha256_text(_canonical_json(base)),
    }


def capacity_admission_sha256(value: Mapping[str, Any] | Path) -> str:
    payload = (
        _load_json(value, "capacity admission receipt")
        if isinstance(value, Path)
        else copy.deepcopy(value)
    )
    if not isinstance(payload, Mapping):
        raise ExpandedCapEpisodeBatchError(
            "capacity admission receipt is not an object"
        )
    return sha256_text(_canonical_json(payload))


def _aware_timestamp(value: Any, label: str) -> datetime:
    if not _is_iso_timestamp(value):
        raise ExpandedCapEpisodeBatchTelemetryError(
            f"capacity admission {label} is not an ISO timestamp"
        )
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ExpandedCapEpisodeBatchTelemetryError(
            f"capacity admission {label} is not timezone-aware"
        )
    return parsed.astimezone(timezone.utc)


def validate_capacity_admission(
    value: Mapping[str, Any] | Path,
    *,
    expected_sha256: str,
    batch_size: int,
    thread_mode: str,
    concurrency: int,
    episode_ids: Sequence[str],
    segment_count: int,
    batch_count: int,
    fixture_mode: bool = False,
) -> dict[str, Any]:
    """Verify an exact coordinator receipt without measuring live capacity."""

    payload = (
        _load_json(value, "capacity admission receipt")
        if isinstance(value, Path)
        else copy.deepcopy(value)
    )
    if not isinstance(payload, Mapping):
        raise ExpandedCapEpisodeBatchTelemetryError(
            "capacity admission receipt is not an object"
        )
    expected_keys = {
        "schema_version",
        "admission_id",
        "issued_at",
        "expires_at",
        "state",
        "issued_by",
        "verification_mode",
        "fixture",
        "winner_system_id",
        "batch_size",
        "thread_mode",
        "model",
        "effort",
        "concurrency",
        "episode_ids_sha256",
        "episode_count",
        "segment_count",
        "batch_count",
        "live_capacity_available",
        "managed_chatgpt_auth_only",
        "verification_evidence_sha256",
    }
    actual_sha256 = capacity_admission_sha256(payload)
    expected_hash_valid = bool(
        isinstance(expected_sha256, str)
        and len(expected_sha256) == 64
        and all(character in "0123456789abcdef" for character in expected_sha256)
    )
    episode_ids_sha256 = sha256_text(_canonical_json(list(episode_ids)))
    evidence_sha256 = payload.get("verification_evidence_sha256")
    if (
        set(payload) != expected_keys
        or not expected_hash_valid
        or actual_sha256 != expected_sha256
        or payload.get("schema_version") != CAPACITY_ADMISSION_VERSION
        or not isinstance(payload.get("admission_id"), str)
        or not payload.get("admission_id")
        or payload.get("state") != "admitted"
        or payload.get("issued_by") != "evaluation_coordinator"
        or payload.get("verification_mode")
        != ("fixture_verified" if fixture_mode else "live_verified")
        or payload.get("fixture") is not fixture_mode
        or payload.get("winner_system_id") != WINNER_SYSTEM_ID
        or payload.get("batch_size") != batch_size
        or payload.get("thread_mode") != thread_mode
        or payload.get("model") != MODEL
        or payload.get("effort") != EFFORT
        or payload.get("concurrency") != concurrency
        or payload.get("episode_ids_sha256") != episode_ids_sha256
        or payload.get("episode_count") != len(episode_ids)
        or payload.get("segment_count") != segment_count
        or payload.get("batch_count") != batch_count
        or payload.get("live_capacity_available") is not True
        or payload.get("managed_chatgpt_auth_only") is not True
        or not isinstance(evidence_sha256, str)
        or len(evidence_sha256) != 64
        or any(
            character not in "0123456789abcdef"
            for character in evidence_sha256
        )
    ):
        raise ExpandedCapEpisodeBatchTelemetryError(
            "capacity admission receipt contract or exact hash binding failed"
        )
    issued_at = _aware_timestamp(payload.get("issued_at"), "issued_at")
    expires_at = _aware_timestamp(payload.get("expires_at"), "expires_at")
    now = datetime.now(timezone.utc)
    validity_seconds = (expires_at - issued_at).total_seconds()
    if (
        issued_at > now
        or expires_at <= now
        or validity_seconds <= 0
        or validity_seconds
        > int(capacity_admission_contract()["maximum_validity_seconds"])
    ):
        raise ExpandedCapEpisodeBatchTelemetryError(
            "capacity admission receipt is not currently valid"
        )
    return {
        "receipt": copy.deepcopy(dict(payload)),
        "canonical_sha256": actual_sha256,
        "fixture": fixture_mode,
    }


def build_frozen_configuration(
    *, batch_size: int, thread_mode: str
) -> dict[str, Any]:
    """Build the selection artifact later holdout/prod receipts can bind."""

    _validate_arm_choice(batch_size, thread_mode)
    canonical = canonical_dynamic_schema_bundle()
    instruction_sources = _epoch4_instruction_source_contract()
    overlay = verified_context_control_overlay()
    return {
        "schema_version": FROZEN_CONFIGURATION_VERSION,
        "state": "precommit_configuration",
        "winner_system_id": WINNER_SYSTEM_ID,
        "batch_size": batch_size,
        "thread_mode": thread_mode,
        "cache_regime": (
            "development_cold"
            if thread_mode == "new_thread"
            else "development_warm_same_episode_thread"
        ),
        "cache_interpretation": "measured_cached_input_tokens_only_no_cache_claim",
        "model": MODEL,
        "effort": EFFORT,
        "semantic_instruction_template_sha256": sha256_text(
            semantic_instruction_template()
        ),
        "semantic_prompt_template_sha256": sha256_text(
            semantic_prompt_template()
        ),
        "prompt_instructions_sha256": _prompt_instruction_hash(),
        "canonical_dynamic_schema_strategy": CANONICAL_DYNAMIC_SCHEMA_STRATEGY,
        "canonical_dynamic_schema_sha256": canonical[
            "canonical_dynamic_schema_sha256"
        ],
        "context_control_overlay_sha256": sha256_text(_canonical_json(overlay)),
        **instruction_sources,
        "epoch4_algorithm_module_sha256": _sha256_file(
            Path(str(epoch4.__file__)).resolve()
        ),
        "epoch4_runtime_lock": _record(
            epoch4.DEFAULT_OUTPUT_ROOT / "runtime-lock.json"
        ),
        "adapter_module_sha256": _sha256_file(Path(__file__).resolve()),
        "transport_client_version": codex_app_server.APP_SERVER_CLIENT_VERSION,
        "pinned_codex_cli_version": codex_app_server.PINNED_CODEX_CLI_VERSION,
        "protocol_schema_sha256": _sha256_file(
            codex_app_server.PROTOCOL_SCHEMA_PATH
        ),
        "max_events_per_segment": MAX_EVENTS_PER_SEGMENT,
        "semantic_postprocessing": SEMANTIC_POSTPROCESSING,
        "deterministic_semantic_pruning": False,
        "deterministic_support_filtering": False,
        "deterministic_deduplication": False,
        "deterministic_relabeling": False,
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "retry_count": RETRY_COUNT,
        "capacity_admission_contract": capacity_admission_contract(),
        "semantic_parity_boundary": {
            "exact": [
                "epoch4_semantic_instruction_text_outside_episode_context",
                "epoch4_blind_source_packet_prompt_framing",
                "epoch4_explicit_applicability_event_schema",
                "epoch4_48_event_cap",
                "epoch4_duplicate_preserving_projection",
                "epoch4_model_and_reasoning_effort",
                "epoch4_context_control_overlay_and_effective_instruction_sources",
            ],
            "parameterized_only": [
                "episode_context_json",
                "blind_source_packet_json",
                "episode_segment_and_unit_id_enums",
                "exact_batch_segment_and_unit_cardinalities",
            ],
        },
        "operational_non_parity_boundary": {
            "epoch4_single_canary_plan_directive_and_receipt_reused": False,
            "epoch4_single_turn_token_ceiling_reused": False,
            "epoch4_canary_production_cost_gate_reused": False,
            "live_capacity_gating": (
                "coordinator_supplied_exact_hash_bound_not_measured_by_adapter"
            ),
            "dataset_selection_owned_by_adapter": False,
            "semantic_quality_judging_owned_by_adapter": False,
            "holdout_authorization_owned_by_adapter": False,
        },
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }


def verify_frozen_configuration(value: Mapping[str, Any] | Path) -> dict[str, Any]:
    payload = (
        _load_json(value, "frozen expanded-cap configuration")
        if isinstance(value, Path)
        else copy.deepcopy(dict(value))
    )
    batch_size = payload.get("batch_size")
    thread_mode = payload.get("thread_mode")
    if not isinstance(batch_size, int) or not isinstance(thread_mode, str):
        raise ExpandedCapEpisodeBatchError("frozen configuration drifted")
    expected = build_frozen_configuration(
        batch_size=batch_size, thread_mode=thread_mode
    )
    if payload != expected:
        raise ExpandedCapEpisodeBatchError("frozen configuration drifted")
    return payload


def freeze_precommit_selection(
    path: Path, *, batch_size: int, thread_mode: str
) -> dict[str, Any]:
    payload = build_frozen_configuration(
        batch_size=batch_size, thread_mode=thread_mode
    )
    _write_immutable_json(path, payload)
    return verify_frozen_configuration(path)


def precommit_selection_surface() -> dict[str, Any]:
    """Expose all six frozen development cold/warm regression arms."""

    arms = [
        build_frozen_configuration(batch_size=batch_size, thread_mode=thread_mode)
        for batch_size in SUPPORTED_BATCH_SIZES
        for thread_mode in SUPPORTED_THREAD_MODES
    ]
    return {
        "schema_version": PRECOMMIT_SURFACE_VERSION,
        "evaluation_role": "development_cold_warm_regression_precommit",
        "winner_system_id": WINNER_SYSTEM_ID,
        "required_batch_sizes": list(SUPPORTED_BATCH_SIZES),
        "required_thread_modes": list(SUPPORTED_THREAD_MODES),
        "arms": arms,
        "arm_count": len(arms),
        "selection_requires_complete_usage_cache_reasoning_and_wall_telemetry": True,
        "selection_requires_semantic_judging": True,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }


class ExpandedCapBatchCodexAppServerClient(epoch4.Epoch4CodexAppServerClient):
    """Official persistent app-server client with epoch-4 context isolation."""


def _client_factory() -> ExpandedCapBatchCodexAppServerClient:
    overlay = verified_context_control_overlay()
    return ExpandedCapBatchCodexAppServerClient(
        config_overlay=overlay,
        command=[str(epoch4.PINNED_CODEX), "app-server", "--stdio", "--strict-config"],
    )


def _batch_paths(root: Path, batch_id: str) -> dict[str, Path]:
    batch_root = root / "batches" / batch_id
    return {
        "root": batch_root,
        "input": batch_root / "input.private.json",
        "prompt": batch_root / "prompt.private.md",
        "base": batch_root / "base-instructions.private.md",
        "schema": batch_root / "schema.json",
        "projection_schema": batch_root / "projection-schema.json",
        "attempt": batch_root / "semantic-call-attempt.json",
        "sidecar": batch_root / "sidecar.json",
        "output": batch_root / "output.private.json",
        "normalized": batch_root / "normalized-output.private.json",
        "provenance": batch_root / "evidence-provenance.private.json",
        "diagnostics": batch_root / "diagnostics.private.json",
        "applicability": batch_root / "applicability-receipt.json",
        "result": batch_root / "result.json",
    }


async def _run_no_model_preflight(
    client: Any,
    *,
    root: Path,
    first_request: Mapping[str, Any],
) -> dict[str, Any]:
    """Verify the effective instruction sources before any semantic turn."""

    thread = await client.start_thread(
        model=MODEL,
        base_instructions=first_request["base_instructions"],
        cwd=PROJECT_ROOT,
        ephemeral=True,
    )
    _verify_started_thread(thread, first_request)
    source_contract = _epoch4_instruction_source_contract()
    payload = {
        "schema_version": NO_MODEL_PREFLIGHT_VERSION,
        "created_at": now_iso(),
        "state": "passed",
        "managed_chatgpt_auth_verified": True,
        "plan_type": "pro",
        "model": MODEL,
        "semantic_thread_started": False,
        "semantic_turn_started": False,
        "semantic_model_call_count": 0,
        "preflight_thread_id": thread.thread_id,
        "preflight_thread_ephemeral": True,
        "base_instructions_sha256": first_request["base_instructions_sha256"],
        "context_control_overlay_sha256": build_frozen_configuration(
            batch_size=int(first_request["batch_size_ceiling"]),
            thread_mode=str(first_request["thread_mode"]),
        )["context_control_overlay_sha256"],
        **source_contract,
        "production_mutated": False,
    }
    _write_immutable_json(root / "no-model-preflight.json", payload)
    return payload


def _verify_started_thread(thread: Any, request: Mapping[str, Any]) -> None:
    """Verify identity and instruction sources before a semantic turn starts."""

    source_contract = _epoch4_instruction_source_contract()
    if (
        getattr(thread, "model", None) != MODEL
        or getattr(thread, "ephemeral", None) is not True
        or getattr(thread, "base_instructions_sha256", None)
        != request["base_instructions_sha256"]
        or getattr(thread, "base_instructions_bytes", None)
        != len(request["base_instructions"].encode("utf-8"))
        or getattr(thread, "instruction_sources_sha256", None)
        != source_contract["effective_instruction_sources_sha256"]
        or getattr(thread, "instruction_sources_count", None)
        != source_contract["effective_instruction_sources_count"]
        or not isinstance(getattr(thread, "thread_id", None), str)
        or not thread.thread_id
    ):
        raise ExpandedCapEpisodeBatchTelemetryError(
            "started thread instruction-source preflight contract failed"
        )


def _write_prepared_request(root: Path, request: Mapping[str, Any]) -> dict[str, Path]:
    validated = validate_prepared_request(request)
    paths = _batch_paths(root, validated["batch_id"])
    _write_immutable_json(paths["input"], validated["private_input"])
    _write_immutable_text(paths["prompt"], validated["prompt"])
    _write_immutable_text(paths["base"], validated["base_instructions"])
    _write_immutable_json(paths["schema"], validated["schema"])
    _write_immutable_json(
        paths["projection_schema"], validated["projection_schema"]
    )
    return paths


def _output_message_hash(path: Path) -> str:
    value = path.read_text(encoding="utf-8")
    message = value[:-1] if value.endswith("\n") else value
    return sha256_text(message)


def _write_attempt_receipt(
    request: Mapping[str, Any], paths: Mapping[str, Path], *, thread_id: str
) -> dict[str, Any]:
    """Commit a conservative call-attempt receipt before transport dispatch."""

    payload = {
        "schema_version": ATTEMPT_RECEIPT_VERSION,
        "created_at": now_iso(),
        "state": "semantic_call_dispatch_committed",
        "winner_system_id": WINNER_SYSTEM_ID,
        "batch_id": request["batch_id"],
        "episode_id": request["episode_id"],
        "thread_id": thread_id,
        "retry_count": RETRY_COUNT,
    }
    _write_immutable_json(paths["attempt"], payload)
    return payload


def _validate_attempt_receipt(
    request: Mapping[str, Any], attempt_path: Path
) -> dict[str, Any]:
    payload = _load_json(attempt_path, "semantic call attempt receipt")
    expected_keys = {
        "schema_version",
        "created_at",
        "state",
        "winner_system_id",
        "batch_id",
        "episode_id",
        "thread_id",
        "retry_count",
    }
    if (
        not isinstance(payload, Mapping)
        or set(payload) != expected_keys
        or payload.get("schema_version") != ATTEMPT_RECEIPT_VERSION
        or not _is_iso_timestamp(payload.get("created_at"))
        or payload.get("state") != "semantic_call_dispatch_committed"
        or payload.get("winner_system_id") != WINNER_SYSTEM_ID
        or payload.get("batch_id") != request["batch_id"]
        or payload.get("episode_id") != request["episode_id"]
        or not isinstance(payload.get("thread_id"), str)
        or not payload.get("thread_id")
        or payload.get("retry_count") != RETRY_COUNT
    ):
        raise ExpandedCapEpisodeBatchTelemetryError(
            "semantic call attempt receipt contract failed"
        )
    return copy.deepcopy(dict(payload))


def validate_turn_sidecar(
    request: Mapping[str, Any],
    sidecar_path: Path,
    *,
    output_path: Path | None = None,
    require_completed: bool = True,
    expected_thread_id: str | None = None,
) -> dict[str, Any]:
    """Validate managed-auth identity plus usage/cache/reasoning/wall telemetry."""

    validated = validate_prepared_request(request)
    sidecar = _load_json(sidecar_path, "expanded-cap turn sidecar")
    if not isinstance(sidecar, Mapping):
        raise ExpandedCapEpisodeBatchTelemetryError("turn sidecar is not an object")
    terminal_states = {"completed", "failed", "interrupted", "cancelled"}
    thread_id = sidecar.get("thread_id")
    turn_id = sidecar.get("turn_id")
    common_ok = bool(
        sidecar.get("schema_version")
        == codex_app_server.TURN_SIDECAR_SCHEMA_VERSION
        and _is_iso_timestamp(sidecar.get("started_at"))
        and _is_iso_timestamp(sidecar.get("finished_at"))
        and sidecar.get("client_version")
        == codex_app_server.APP_SERVER_CLIENT_VERSION
        and sidecar.get("cli_version")
        == codex_app_server.PINNED_CODEX_CLI_VERSION
        and sidecar.get("protocol_schema_sha256")
        == _sha256_file(codex_app_server.PROTOCOL_SCHEMA_PATH)
        and sidecar.get("transport") == "stdio"
        and isinstance(sidecar.get("app_server_user_agent"), str)
        and bool(sidecar.get("app_server_user_agent"))
        and isinstance(sidecar.get("max_message_bytes"), int)
        and not isinstance(sidecar.get("max_message_bytes"), bool)
        and sidecar.get("max_message_bytes") >= 64 * 1024
        and sidecar.get("synthetic_debug_errors") is False
        and sidecar.get("auth_type") == "chatgpt"
        and sidecar.get("plan_type") == "pro"
        and sidecar.get("model") == MODEL
        and sidecar.get("effort") == EFFORT
        and sidecar.get("thread_mode") == validated["thread_mode"]
        and sidecar.get("batch_size") == validated["effective_batch_size"]
        and sidecar.get("prompt_sha256") == validated["prompt_sha256"]
        and sidecar.get("prompt_bytes")
        == len(validated["prompt"].encode("utf-8"))
        and sidecar.get("base_instructions_sha256")
        == validated["base_instructions_sha256"]
        and sidecar.get("base_instructions_bytes")
        == len(validated["base_instructions"].encode("utf-8"))
        and sidecar.get("output_schema_sha256")
        == validated["output_schema_sha256"]
        and sidecar.get("output_schema_bytes")
        == len(_canonical_json(validated["schema"]).encode("utf-8"))
        and sidecar.get("state") in terminal_states
        and sidecar.get("recovery_reran_model") is False
        and isinstance(thread_id, str)
        and bool(thread_id)
        and (expected_thread_id is None or thread_id == expected_thread_id)
        and isinstance(turn_id, str)
        and bool(turn_id)
        and isinstance(sidecar.get("instruction_sources_sha256"), str)
        and sidecar.get("instruction_sources_sha256")
        == _epoch4_instruction_source_contract()[
            "effective_instruction_sources_sha256"
        ]
        and sidecar.get("instruction_sources_count")
        == _epoch4_instruction_source_contract()[
            "effective_instruction_sources_count"
        ]
        and isinstance(sidecar.get("stderr_sha256"), str)
        and len(sidecar.get("stderr_sha256")) == 64
        and isinstance(sidecar.get("stderr_bytes"), int)
        and not isinstance(sidecar.get("stderr_bytes"), bool)
        and sidecar.get("stderr_bytes") >= 0
    )
    if not common_ok:
        raise ExpandedCapEpisodeBatchTelemetryError(
            "managed-auth turn sidecar contract failed"
        )
    wall = _nonnegative_number(
        sidecar.get("wall_elapsed_seconds"), "wall_elapsed_seconds"
    )
    usage_status = sidecar.get("usage_status")
    usage: dict[str, int] | None
    thread_total_usage: dict[str, int] | None
    if usage_status == "measured":
        if sidecar.get("usage_complete") is not True:
            raise ExpandedCapEpisodeBatchTelemetryError(
                "measured usage is not complete"
            )
        usage = _usage_values(sidecar.get("usage"), "usage")
        thread_total_usage = _usage_values(
            sidecar.get("thread_total_usage"), "thread_total_usage"
        )
        if any(thread_total_usage[field] < usage[field] for field in USAGE_FIELDS):
            raise ExpandedCapEpisodeBatchTelemetryError(
                "thread total usage is below turn usage"
            )
        if (
            validated["thread_mode"] == "new_thread"
            and thread_total_usage != usage
        ):
            raise ExpandedCapEpisodeBatchTelemetryError(
                "new-thread total usage differs from turn usage"
            )
    elif usage_status == "unknown":
        if (
            sidecar.get("usage_complete") is not False
            or sidecar.get("usage") is not None
        ):
            raise ExpandedCapEpisodeBatchTelemetryError(
                "unknown usage telemetry is malformed"
            )
        usage = None
        thread_total_usage = None
    else:
        raise ExpandedCapEpisodeBatchTelemetryError("usage_status is invalid")
    if require_completed:
        if (
            sidecar.get("state") != "completed"
            or sidecar.get("status") != "completed"
            or sidecar.get("error_class") is not None
            or usage is None
            or output_path is None
            or not output_path.is_file()
            or sidecar.get("output_sha256") != _output_message_hash(output_path)
            or Path(str(sidecar.get("output_path") or "")).expanduser().resolve()
            != output_path.expanduser().resolve()
        ):
            raise ExpandedCapEpisodeBatchTelemetryError(
                "completed turn sidecar is incomplete"
            )
    return {
        "sidecar": copy.deepcopy(dict(sidecar)),
        "thread_id": thread_id,
        "turn_id": turn_id,
        "usage_status": "complete" if usage is not None else "unknown",
        "usage": usage,
        "thread_total_usage": thread_total_usage,
        "wall_elapsed_seconds": wall,
        "cached_input_tokens": (
            usage["cached_input_tokens"] if usage is not None else None
        ),
        "reasoning_output_tokens": (
            usage["reasoning_output_tokens"] if usage is not None else None
        ),
    }


def _best_effort_sidecar_telemetry(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {
            "sidecar_present": False,
            "usage_status": "not_started",
            "usage": None,
            "wall_elapsed_seconds": None,
            "thread_id": None,
            "turn_id": None,
        }
    try:
        sidecar = _load_json(path, "best-effort turn sidecar")
    except ExpandedCapEpisodeBatchError:
        return {
            "sidecar_present": True,
            "usage_status": "unknown",
            "usage": None,
            "wall_elapsed_seconds": None,
            "thread_id": None,
            "turn_id": None,
        }
    usage = None
    if isinstance(sidecar, Mapping) and sidecar.get("usage_status") == "measured":
        try:
            usage = _usage_values(sidecar.get("usage"), "usage")
        except ExpandedCapEpisodeBatchTelemetryError:
            usage = None
    wall = sidecar.get("wall_elapsed_seconds") if isinstance(sidecar, Mapping) else None
    if isinstance(wall, bool) or not isinstance(wall, (int, float)) or wall < 0:
        wall = None
    return {
        "sidecar_present": True,
        "usage_status": "complete" if usage is not None else "unknown",
        "usage": usage,
        "wall_elapsed_seconds": wall,
        "thread_id": sidecar.get("thread_id") if isinstance(sidecar, Mapping) else None,
        "turn_id": sidecar.get("turn_id") if isinstance(sidecar, Mapping) else None,
        "state": sidecar.get("state") if isinstance(sidecar, Mapping) else None,
        "status": sidecar.get("status") if isinstance(sidecar, Mapping) else None,
        "error_class": sidecar.get("error_class") if isinstance(sidecar, Mapping) else None,
        "recovery_reran_model": (
            sidecar.get("recovery_reran_model")
            if isinstance(sidecar, Mapping)
            else None
        ),
    }


async def _execute_batch(
    client: Any,
    request: Mapping[str, Any],
    paths: Mapping[str, Path],
    *,
    thread: Any,
    timeout_seconds: float,
) -> dict[str, Any]:
    try:
        _verify_started_thread(thread, request)
        expected_thread_id = str(thread.thread_id)
        _write_attempt_receipt(
            request, paths, thread_id=expected_thread_id
        )
        result = await client.run_structured_turn(
            thread=thread,
            effort=EFFORT,
            prompt=request["prompt"],
            output_schema=request["schema"],
            sidecar_path=paths["sidecar"],
            output_path=paths["output"],
            batch_size=request["effective_batch_size"],
            thread_mode=request["thread_mode"],
            timeout_seconds=timeout_seconds,
        )
        if result.status_ok is not True or not isinstance(result.output, Mapping):
            return {
                "batch_id": request["batch_id"],
                "episode_id": request["episode_id"],
                "state": "failed",
                "failure_class": result.error_class or f"turn_{result.status}",
                "thread_reusable": False,
                "thread_id": getattr(result, "thread_id", None),
                "turn_id": getattr(result, "turn_id", None),
            }
        telemetry = validate_turn_sidecar(
            request,
            paths["sidecar"],
            output_path=paths["output"],
            expected_thread_id=expected_thread_id,
        )
        if (
            getattr(result, "thread_id", None) != expected_thread_id
            or telemetry["thread_id"] != expected_thread_id
            or getattr(result, "turn_id", None) != telemetry["turn_id"]
        ):
            raise ExpandedCapEpisodeBatchTelemetryError(
                "started thread, result, and sidecar lifecycle ids differ"
            )
        try:
            raw_text = paths["output"].read_text(encoding="utf-8")
            raw_message = raw_text[:-1] if raw_text.endswith("\n") else raw_text
            if (
                sha256_text(raw_message)
                != telemetry["sidecar"]["output_sha256"]
            ):
                raise ExpandedCapEpisodeBatchTelemetryError(
                    "raw output bytes differ from the validated sidecar hash"
                )
            raw_output = json.loads(raw_message)
            raw_canonical = _canonical_json(raw_output)
            result_canonical = _canonical_json(result.output)
        except ExpandedCapEpisodeBatchTelemetryError:
            raise
        except (
            OSError,
            UnicodeDecodeError,
            json.JSONDecodeError,
            TypeError,
            ValueError,
        ) as exc:
            raise ExpandedCapEpisodeBatchTelemetryError(
                "sidecar-bound raw output is unreadable"
            ) from exc
        if not isinstance(raw_output, Mapping) or raw_canonical != result_canonical:
            raise ExpandedCapEpisodeBatchTelemetryError(
                "in-memory result differs from sidecar-bound raw output"
            )
        projected = validate_and_project_output(request, raw_output)
        _write_immutable_json(paths["normalized"], projected["normalized"])
        _write_immutable_json(paths["provenance"], projected["provenance"])
        _write_immutable_json(
            paths["diagnostics"], {"segments": projected["diagnostics"]}
        )
        _write_immutable_json(paths["applicability"], projected["applicability"])
        return {
            "batch_id": request["batch_id"],
            "episode_id": request["episode_id"],
            "state": "validated",
            "failure_class": None,
            "thread_reusable": True,
            "thread_id": telemetry["thread_id"],
            "turn_id": telemetry["turn_id"],
            "emitted_event_count": projected["applicability"][
                "emitted_event_count"
            ],
            "exact_identity_duplicate_count": projected["applicability"][
                "exact_identity_duplicate_count"
            ],
            "records": {
                "attempt": _record(paths["attempt"]),
                "sidecar": _record(paths["sidecar"]),
                "raw_output": _record(paths["output"]),
                "normalized_output": _record(paths["normalized"]),
                "evidence_provenance": _record(paths["provenance"]),
                "diagnostics": _record(paths["diagnostics"]),
                "applicability_receipt": _record(paths["applicability"]),
            },
        }
    except ExpandedCapEpisodeBatchOutputError as exc:
        return {
            "batch_id": request["batch_id"],
            "episode_id": request["episode_id"],
            "state": "rejected",
            "failure_class": type(exc).__name__,
            "thread_reusable": True,
        }
    except ExpandedCapEpisodeBatchTelemetryError as exc:
        return {
            "batch_id": request["batch_id"],
            "episode_id": request["episode_id"],
            "state": "failed",
            "failure_class": type(exc).__name__,
            "thread_reusable": False,
        }
    except (
        codex_app_server.AppServerError,
        asyncio.TimeoutError,
        OSError,
    ) as exc:
        return {
            "batch_id": request["batch_id"],
            "episode_id": request["episode_id"],
            "state": "waiting",
            "failure_class": type(exc).__name__,
            "thread_reusable": False,
        }


def _validate_lifecycle_ids(
    requests: Sequence[Mapping[str, Any]], telemetry: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    by_batch = {str(row["batch_id"]): row for row in telemetry}
    rows = [by_batch.get(str(request["batch_id"])) for request in requests]
    complete_rows = [row for row in rows if isinstance(row, Mapping)]
    turn_ids = [str(row.get("turn_id")) for row in complete_rows if row.get("turn_id")]
    thread_ids = [
        str(row.get("thread_id")) for row in complete_rows if row.get("thread_id")
    ]
    turn_ids_unique = len(turn_ids) == len(complete_rows) == len(set(turn_ids))
    receipt_lineage_valid = bool(
        len(complete_rows) == len(requests)
        and all(row.get("thread_matches_attempt") is True for row in complete_rows)
    )
    thread_mode = str(requests[0]["thread_mode"]) if requests else ""
    if thread_mode == "new_thread":
        thread_lineage_valid = receipt_lineage_valid and (
            len(thread_ids) == len(complete_rows) == len(set(thread_ids))
        )
    else:
        episode_threads: dict[str, set[str]] = {}
        for request, row in zip(requests, rows):
            if not isinstance(row, Mapping) or not row.get("thread_id"):
                continue
            episode_threads.setdefault(str(request["episode_id"]), set()).add(
                str(row["thread_id"])
            )
        all_episode_threads = [
            next(iter(values))
            for values in episode_threads.values()
            if len(values) == 1
        ]
        expected_episode_ids = {str(row["episode_id"]) for row in requests}
        thread_lineage_valid = bool(
            receipt_lineage_valid
            and set(episode_threads) == expected_episode_ids
            and all(len(values) == 1 for values in episode_threads.values())
            and len(all_episode_threads) == len(set(all_episode_threads))
        )
    return {
        "turn_ids_unique": turn_ids_unique,
        "thread_lineage_valid": thread_lineage_valid,
        "observed_thread_count": len(set(thread_ids)),
        "observed_turn_count": len(set(turn_ids)),
    }


def _aggregate_report(
    *,
    root: Path,
    configuration: Mapping[str, Any],
    requests: Sequence[Mapping[str, Any]],
    results: Sequence[Mapping[str, Any]],
    wall_elapsed_seconds: float,
) -> dict[str, Any]:
    result_by_batch = {str(row["batch_id"]): row for row in results}
    telemetry_rows: list[dict[str, Any]] = []
    usage_sum = Counter()
    usage_measured = 0
    usage_unknown = 0
    terminal_sidecars = 0
    attempt_contract_failures = 0
    sidecar_contract_failures = 0
    for request in requests:
        paths = _batch_paths(root, str(request["batch_id"]))
        result = result_by_batch[str(request["batch_id"])]
        attempt: dict[str, Any] | None = None
        attempt_present = paths["attempt"].is_file()
        sidecar_present = paths["sidecar"].is_file()
        if attempt_present:
            try:
                attempt = _validate_attempt_receipt(request, paths["attempt"])
            except ExpandedCapEpisodeBatchError:
                attempt_contract_failures += 1
        elif sidecar_present:
            attempt_contract_failures += 1
        attempted = attempt_present or sidecar_present
        strict: dict[str, Any] | None = None
        if sidecar_present:
            terminal_sidecars += 1
            try:
                strict = validate_turn_sidecar(
                    request,
                    paths["sidecar"],
                    output_path=paths["output"] if paths["output"].is_file() else None,
                    require_completed=result.get("state") == "validated",
                    expected_thread_id=(
                        str(attempt["thread_id"])
                        if attempt is not None
                        else None
                    ),
                )
            except ExpandedCapEpisodeBatchTelemetryError:
                sidecar_contract_failures += 1
        best = (
            strict
            if strict is not None
            else _best_effort_sidecar_telemetry(paths["sidecar"])
        )
        usage = strict.get("usage") if strict is not None else None
        if isinstance(usage, Mapping):
            usage_measured += 1
            usage_sum.update({field: int(usage[field]) for field in USAGE_FIELDS})
        elif attempted:
            usage_unknown += 1
        expected_thread_id = (
            str(attempt["thread_id"]) if attempt is not None else None
        )
        observed_thread_id = best.get("thread_id")
        telemetry_rows.append(
            {
                "batch_id": request["batch_id"],
                "episode_id": request["episode_id"],
                "state": result.get("state"),
                "failure_class": result.get("failure_class"),
                "attempted": attempted,
                "attempt_contract_valid": attempt is not None,
                "attempt_receipt": _record(paths["attempt"])
                if attempt_present
                else None,
                "sidecar_contract_valid": strict is not None,
                "sidecar": _record(paths["sidecar"])
                if sidecar_present
                else None,
                "expected_thread_id": expected_thread_id,
                "thread_id": observed_thread_id,
                "thread_matches_attempt": bool(
                    expected_thread_id
                    and observed_thread_id == expected_thread_id
                ),
                "turn_id": best.get("turn_id"),
                "usage_status": (
                    "complete"
                    if isinstance(usage, Mapping)
                    else "unknown"
                    if attempted
                    else "not_started"
                ),
                "usage": copy.deepcopy(usage),
                "best_effort_usage": copy.deepcopy(best.get("usage")),
                "thread_total_usage": copy.deepcopy(
                    strict.get("thread_total_usage")
                    if strict is not None
                    else None
                ),
                "cached_input_tokens": (
                    usage.get("cached_input_tokens")
                    if isinstance(usage, Mapping)
                    else None
                ),
                "reasoning_output_tokens": (
                    usage.get("reasoning_output_tokens")
                    if isinstance(usage, Mapping)
                    else None
                ),
                "wall_elapsed_seconds": best.get("wall_elapsed_seconds"),
            }
        )
    lifecycle = _validate_lifecycle_ids(requests, telemetry_rows)
    preflight_path = root / "no-model-preflight.json"
    preflight = (
        _load_json(preflight_path, "expanded-cap no-model preflight")
        if preflight_path.is_file()
        else None
    )
    semantic_thread_ids = {
        str(thread_id)
        for row in telemetry_rows
        for thread_id in (row.get("thread_id"), row.get("expected_thread_id"))
        if isinstance(thread_id, str) and thread_id
    }
    preflight_lineage_valid = bool(
        isinstance(preflight, Mapping)
        and preflight.get("schema_version") == NO_MODEL_PREFLIGHT_VERSION
        and preflight.get("state") == "passed"
        and preflight.get("managed_chatgpt_auth_verified") is True
        and preflight.get("plan_type") == "pro"
        and preflight.get("model") == MODEL
        and preflight.get("semantic_model_call_count") == 0
        and preflight.get("semantic_turn_started") is False
        and isinstance(preflight.get("preflight_thread_id"), str)
        and preflight.get("preflight_thread_id") not in semantic_thread_ids
        and preflight.get("base_instructions_sha256")
        == requests[0]["base_instructions_sha256"]
        and preflight.get("effective_instruction_sources_sha256")
        == configuration["effective_instruction_sources_sha256"]
        and preflight.get("effective_instruction_sources_count")
        == configuration["effective_instruction_sources_count"]
        and preflight.get("effective_instruction_source_records_sha256")
        == configuration["effective_instruction_source_records_sha256"]
        and preflight.get("context_control_overlay_sha256")
        == configuration["context_control_overlay_sha256"]
    )
    capacity_path = root / "capacity-admission.json"
    capacity_binding_path = root / "capacity-binding.json"
    run_configuration_path = root / "run-configuration.json"
    try:
        capacity_binding = _load_json(
            capacity_binding_path, "capacity binding"
        )
        run_configuration = _load_json(
            run_configuration_path, "capacity-bound run configuration"
        )
        observed_capacity_sha256 = capacity_admission_sha256(capacity_path)
        capacity_binding_valid = bool(
            isinstance(capacity_binding, Mapping)
            and capacity_binding.get("schema_version")
            == CAPACITY_BINDING_VERSION
            and capacity_binding.get("state")
            == "verified_before_app_server_start"
            and capacity_binding.get("winner_system_id") == WINNER_SYSTEM_ID
            and capacity_binding.get("contract_sha256")
            == configuration["capacity_admission_contract"]["contract_sha256"]
            and capacity_binding.get("expected_canonical_sha256")
            == observed_capacity_sha256
            and capacity_binding.get("observed_canonical_sha256")
            == observed_capacity_sha256
            and capacity_binding.get("capacity_admission")
            == _record(capacity_path)
            and isinstance(capacity_binding.get("fixture"), bool)
            and isinstance(run_configuration, Mapping)
            and run_configuration.get("schema_version")
            == RUN_CONFIGURATION_VERSION
            and run_configuration.get("state")
            == "capacity_bound_configuration"
            and run_configuration.get("winner_system_id") == WINNER_SYSTEM_ID
            and run_configuration.get("batch_size") == configuration["batch_size"]
            and run_configuration.get("thread_mode")
            == configuration["thread_mode"]
            and run_configuration.get("model") == MODEL
            and run_configuration.get("effort") == EFFORT
            and run_configuration.get("frozen_configuration")
            == _record(root / "frozen-configuration.json")
            and run_configuration.get("capacity_binding")
            == _record(capacity_binding_path)
            and run_configuration.get("capacity_admission_canonical_sha256")
            == observed_capacity_sha256
            and run_configuration.get("fixture_mode")
            is capacity_binding.get("fixture")
        )
    except (ExpandedCapEpisodeBatchError, OSError, TypeError, ValueError):
        capacity_binding = None
        run_configuration = None
        capacity_binding_valid = False
    attempted_calls = sum(row["attempted"] is True for row in telemetry_rows)
    accounting_complete = bool(
        attempted_calls == len(requests)
        and usage_measured == len(requests)
        and usage_unknown == 0
        and attempt_contract_failures == 0
        and sidecar_contract_failures == 0
    )
    all_validated = bool(
        len(results) == len(requests)
        and all(row.get("state") == "validated" for row in results)
    )
    ambiguous_retry_count = sum(
        bool(
            isinstance(_best_effort_sidecar_telemetry(
                _batch_paths(root, str(request["batch_id"]))["sidecar"]
            ).get("recovery_reran_model"), bool)
            and _best_effort_sidecar_telemetry(
                _batch_paths(root, str(request["batch_id"]))["sidecar"]
            ).get("recovery_reran_model")
            is not False
        )
        for request in requests
        if _batch_paths(root, str(request["batch_id"]))["sidecar"].is_file()
    )
    ambiguous_outcome_attempts = sum(
        row["attempted"] is True and row["usage_status"] != "complete"
        for row in telemetry_rows
    )
    regression_eligible = bool(
        all_validated
        and accounting_complete
        and lifecycle["turn_ids_unique"]
        and lifecycle["thread_lineage_valid"]
        and preflight_lineage_valid
        and capacity_binding_valid
        and ambiguous_retry_count == 0
    )
    measured_usage = {field: int(usage_sum[field]) for field in USAGE_FIELDS}
    usage_status = (
        "complete"
        if accounting_complete
        else "partial_unknown"
        if usage_measured
        else "unknown"
        if attempted_calls
        else "not_started"
    )
    return {
        "schema_version": RUN_REPORT_VERSION,
        "state": "passed" if regression_eligible else "not_eligible",
        "evaluation_role": "development_cold_warm_regression_precommit",
        "winner_system_id": WINNER_SYSTEM_ID,
        "frozen_configuration": _record(root / "frozen-configuration.json"),
        "no_model_preflight": _record(preflight_path)
        if preflight_path.is_file()
        else None,
        "preflight_lineage_valid": preflight_lineage_valid,
        "run_configuration": _record(run_configuration_path)
        if run_configuration_path.is_file()
        else None,
        "capacity_binding": _record(capacity_binding_path)
        if capacity_binding_path.is_file()
        else None,
        "capacity_binding_valid": capacity_binding_valid,
        "fixture_mode": (
            capacity_binding.get("fixture")
            if isinstance(capacity_binding, Mapping)
            else None
        ),
        "batch_size": configuration["batch_size"],
        "thread_mode": configuration["thread_mode"],
        "model": MODEL,
        "effort": EFFORT,
        "max_events_per_segment": MAX_EVENTS_PER_SEGMENT,
        "semantic_postprocessing": False,
        "requested_calls": len(requests),
        "attempted_calls": attempted_calls,
        "validated_calls": sum(row.get("state") == "validated" for row in results),
        "attempt_contract_failures": attempt_contract_failures,
        "terminal_sidecars": terminal_sidecars,
        "sidecar_contract_failures": sidecar_contract_failures,
        "usage_status": usage_status,
        "accounting_complete": accounting_complete,
        "usage": measured_usage if accounting_complete else None,
        "measured_partial_usage": measured_usage if usage_measured else None,
        "usage_measured_attempts": usage_measured,
        "usage_unknown_attempts": usage_unknown,
        "ambiguous_outcome_attempts": ambiguous_outcome_attempts,
        "cache_interpretation": "observed_cached_input_tokens_only_no_cache_claim",
        "cached_input_tokens": (
            measured_usage["cached_input_tokens"] if usage_measured else None
        ),
        "reasoning_output_tokens": (
            measured_usage["reasoning_output_tokens"] if usage_measured else None
        ),
        "wall_elapsed_seconds": round(wall_elapsed_seconds, 3),
        "turn_wall_elapsed_seconds_sum": round(
            sum(
                float(row["wall_elapsed_seconds"])
                for row in telemetry_rows
                if isinstance(row.get("wall_elapsed_seconds"), (int, float))
            ),
            3,
        ),
        **lifecycle,
        "retry_count": RETRY_COUNT,
        "ambiguous_retry_count": ambiguous_retry_count,
        "all_emitted_events_preserved": all_validated,
        "emitted_event_count": sum(
            int(row.get("emitted_event_count") or 0) for row in results
        ),
        "exact_identity_duplicate_count": sum(
            int(row.get("exact_identity_duplicate_count") or 0)
            for row in results
        ),
        "semantic_event_count_is_acceptance_proxy": False,
        "exact_identity_duplicates_are_diagnostic_only": True,
        "development_regression_eligible": regression_eligible,
        "semantic_quality_status": "requires_separate_calibrated_full_event_evaluation",
        "holdout_authorized": False,
        "production_mutated": False,
        "turn_telemetry": telemetry_rows,
    }


async def run_episode_batch_arm(
    episodes: Sequence[Mapping[str, Any]],
    *,
    output_dir: Path,
    batch_size: int,
    thread_mode: str,
    capacity_admission: Mapping[str, Any] | Path,
    capacity_admission_sha256: str,
    frozen_configuration: Mapping[str, Any] | Path | None = None,
    concurrency: int = 1,
    fixture_mode: bool = False,
    timeout_seconds: float = epoch4.TIMEOUT_SECONDS,
    client_factory: Callable[[], Any] = _client_factory,
) -> dict[str, Any]:
    """Run one frozen development arm through one persistent app-server.

    A non-empty output directory is never resumed.  Any interrupted or partial
    attempt therefore remains terminal evidence and cannot be ambiguously
    replayed by this adapter.
    """

    _validate_arm_choice(batch_size, thread_mode)
    if isinstance(concurrency, bool) or not isinstance(concurrency, int) or concurrency < 1:
        raise ExpandedCapEpisodeBatchError("concurrency must be positive")
    if not isinstance(fixture_mode, bool):
        raise ExpandedCapEpisodeBatchError("fixture_mode must be boolean")
    if not episodes:
        raise ExpandedCapEpisodeBatchError("episode arm is empty")
    configuration = (
        verify_frozen_configuration(frozen_configuration)
        if frozen_configuration is not None
        else build_frozen_configuration(batch_size=batch_size, thread_mode=thread_mode)
    )
    if (
        configuration["batch_size"] != batch_size
        or configuration["thread_mode"] != thread_mode
    ):
        raise ExpandedCapEpisodeBatchError(
            "frozen configuration does not match requested arm"
        )
    root = output_dir.expanduser().resolve()
    if root.exists() and any(root.iterdir()):
        raise ExpandedCapEpisodeBatchError(
            "arm output already exists; explicit recovery or a new root is required"
        )
    root.mkdir(parents=True, exist_ok=True)
    _write_immutable_json(root / "frozen-configuration.json", configuration)
    _write_immutable_json(root / "precommit-selection-surface.json", precommit_selection_surface())

    episode_ids = [_nonempty_identifier(row.get("episode_id"), "episode_id") for row in episodes]
    if len(set(episode_ids)) != len(episode_ids):
        raise ExpandedCapEpisodeBatchError("episode ids are not unique")
    requests: list[dict[str, Any]] = []
    all_segment_ids: list[str] = []
    for episode in episodes:
        episode_requests = prepare_episode_batches(
            episode, batch_size=batch_size, thread_mode=thread_mode
        )
        requests.extend(episode_requests)
        all_segment_ids.extend(
            segment_id
            for request in episode_requests
            for segment_id in request["segment_ids"]
        )
    if len(set(all_segment_ids)) != len(all_segment_ids):
        raise ExpandedCapEpisodeBatchError("segment ids are not globally unique")
    if len({request["batch_id"] for request in requests}) != len(requests):
        raise ExpandedCapEpisodeBatchError("batch ids are not unique")

    paths_by_batch = {
        request["batch_id"]: _write_prepared_request(root, request)
        for request in requests
    }
    capacity = validate_capacity_admission(
        capacity_admission,
        expected_sha256=capacity_admission_sha256,
        batch_size=batch_size,
        thread_mode=thread_mode,
        concurrency=concurrency,
        episode_ids=episode_ids,
        segment_count=len(all_segment_ids),
        batch_count=len(requests),
        fixture_mode=fixture_mode,
    )
    capacity_path = root / "capacity-admission.json"
    _write_immutable_json(capacity_path, capacity["receipt"])
    capacity_binding = {
        "schema_version": CAPACITY_BINDING_VERSION,
        "verified_at": now_iso(),
        "state": "verified_before_app_server_start",
        "winner_system_id": WINNER_SYSTEM_ID,
        "contract_sha256": configuration["capacity_admission_contract"][
            "contract_sha256"
        ],
        "expected_canonical_sha256": capacity_admission_sha256,
        "observed_canonical_sha256": capacity["canonical_sha256"],
        "capacity_admission": _record(capacity_path),
        "fixture": fixture_mode,
    }
    capacity_binding_path = root / "capacity-binding.json"
    _write_immutable_json(capacity_binding_path, capacity_binding)
    run_configuration = {
        "schema_version": RUN_CONFIGURATION_VERSION,
        "state": "capacity_bound_configuration",
        "winner_system_id": WINNER_SYSTEM_ID,
        "batch_size": batch_size,
        "thread_mode": thread_mode,
        "model": MODEL,
        "effort": EFFORT,
        "concurrency": concurrency,
        "frozen_configuration": _record(root / "frozen-configuration.json"),
        "capacity_binding": _record(capacity_binding_path),
        "capacity_admission_canonical_sha256": capacity[
            "canonical_sha256"
        ],
        "fixture_mode": fixture_mode,
    }
    run_configuration_path = root / "run-configuration.json"
    _write_immutable_json(run_configuration_path, run_configuration)
    private_mapping = {
        "schema_version": PRIVATE_MAPPING_VERSION,
        "winner_system_id": WINNER_SYSTEM_ID,
        "batches": [
            {
                "batch_id": request["batch_id"],
                "episode_id": request["episode_id"],
                "segment_ids": request["segment_ids"],
                "input_path": str(paths_by_batch[request["batch_id"]]["input"]),
                "attempt_receipt_path": str(
                    paths_by_batch[request["batch_id"]]["attempt"]
                ),
                "sidecar_path": str(paths_by_batch[request["batch_id"]]["sidecar"]),
                "raw_output_path": str(paths_by_batch[request["batch_id"]]["output"]),
                "normalized_output_path": str(
                    paths_by_batch[request["batch_id"]]["normalized"]
                ),
            }
            for request in requests
        ],
        "privacy": "private analysis only contains episode segment and artifact identities",
    }
    _write_immutable_json(root / "private-mapping.json", private_mapping)
    run_spec = {
        "schema_version": RUN_SPEC_VERSION,
        "created_at": now_iso(),
        "winner_system_id": WINNER_SYSTEM_ID,
        "frozen_configuration": _record(root / "frozen-configuration.json"),
        "run_configuration": _record(run_configuration_path),
        "capacity_binding": _record(capacity_binding_path),
        "episode_count": len(episodes),
        "segment_count": len(all_segment_ids),
        "batch_count": len(requests),
        "effective_batch_sizes": dict(
            sorted(Counter(request["effective_batch_size"] for request in requests).items())
        ),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "app_server_process_count": 1,
        "retry_count": RETRY_COUNT,
        "semantic_postprocessing": False,
        "fixture_mode": fixture_mode,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    _write_immutable_json(root / "run-spec.json", run_spec)

    wall_started = time.monotonic()
    results: list[dict[str, Any]] = []
    try:
        async with client_factory() as client:
            account = getattr(client, "account_summary", None)
            if (
                not isinstance(account, Mapping)
                or account.get("type") != "chatgpt"
                or account.get("plan_type") != "pro"
            ):
                raise codex_app_server.AppServerAuthError(
                    "managed ChatGPT Pro auth is unavailable"
                )
            await _run_no_model_preflight(
                client,
                root=root,
                first_request=requests[0],
            )
            if thread_mode == "new_thread":
                semaphore = asyncio.Semaphore(concurrency)

                async def bounded(request: Mapping[str, Any]) -> dict[str, Any]:
                    async with semaphore:
                        try:
                            thread = await client.start_thread(
                                model=MODEL,
                                base_instructions=request["base_instructions"],
                                cwd=PROJECT_ROOT,
                                ephemeral=True,
                            )
                        except codex_app_server.AppServerError as exc:
                            return {
                                "batch_id": request["batch_id"],
                                "episode_id": request["episode_id"],
                                "state": "waiting",
                                "failure_class": f"thread_start_{type(exc).__name__}",
                                "thread_reusable": False,
                            }
                        _verify_started_thread(thread, request)
                        return await _execute_batch(
                            client,
                            request,
                            paths_by_batch[str(request["batch_id"])],
                            thread=thread,
                            timeout_seconds=timeout_seconds,
                        )

                results = list(await asyncio.gather(*(bounded(row) for row in requests)))
            else:
                by_episode: dict[str, list[Mapping[str, Any]]] = {}
                for request in requests:
                    by_episode.setdefault(str(request["episode_id"]), []).append(request)
                semaphore = asyncio.Semaphore(concurrency)

                async def run_episode(
                    episode_requests: Sequence[Mapping[str, Any]],
                ) -> list[dict[str, Any]]:
                    async with semaphore:
                        first = episode_requests[0]
                        try:
                            thread = await client.start_thread(
                                model=MODEL,
                                base_instructions=first["base_instructions"],
                                cwd=PROJECT_ROOT,
                                ephemeral=True,
                            )
                            _verify_started_thread(thread, first)
                        except codex_app_server.AppServerError as exc:
                            return [
                                {
                                    "batch_id": request["batch_id"],
                                    "episode_id": request["episode_id"],
                                    "state": "waiting",
                                    "failure_class": f"thread_start_{type(exc).__name__}",
                                    "thread_reusable": False,
                                }
                                for request in episode_requests
                            ]
                        episode_results: list[dict[str, Any]] = []
                        reusable = True
                        for request in episode_requests:
                            if not reusable:
                                episode_results.append(
                                    {
                                        "batch_id": request["batch_id"],
                                        "episode_id": request["episode_id"],
                                        "state": "not_started",
                                        "failure_class": "not_started_after_same_thread_failure",
                                        "thread_reusable": False,
                                    }
                                )
                                continue
                            result = await _execute_batch(
                                client,
                                request,
                                paths_by_batch[str(request["batch_id"])],
                                thread=thread,
                                timeout_seconds=timeout_seconds,
                            )
                            episode_results.append(result)
                            reusable = bool(result.get("thread_reusable"))
                        return episode_results

                grouped = await asyncio.gather(
                    *(run_episode(rows) for rows in by_episode.values())
                )
                results = [item for group in grouped for item in group]
    except codex_app_server.AppServerError as exc:
        if results:
            raise
        results = [
            {
                "batch_id": request["batch_id"],
                "episode_id": request["episode_id"],
                "state": "waiting",
                "failure_class": type(exc).__name__,
                "thread_reusable": False,
            }
            for request in requests
        ]

    for result in results:
        _write_immutable_json(
            paths_by_batch[str(result["batch_id"])]["result"], result
        )
    report = _aggregate_report(
        root=root,
        configuration=configuration,
        requests=requests,
        results=results,
        wall_elapsed_seconds=time.monotonic() - wall_started,
    )
    _write_immutable_json(root / "report.json", report)
    return report


# Explicit compatibility aliases for future development and holdout runners.
prepare = prepare_episode_batches
validate = validate_and_project_output
run = run_episode_batch_arm
precommit = precommit_selection_surface
