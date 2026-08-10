from __future__ import annotations

"""Direct full-canonical ``ai_discourse_v3_1`` episode-batch adapter.

This is the production-compatible semantic surface for the next development
matrix.  One managed Codex app-server turn emits every semantic value used by
the canonical label pack.  Deterministic code is limited to source packet
construction, exact identity/provenance attachment, exact source-unit evidence
projection, validation, accounting, and rejection.

In particular this module never invents, defaults, truncates, pads, prunes,
deduplicates, merges, splits, or relabels semantic output.  A malformed or
ungrounded completed output is rejected as a whole.
"""

import asyncio
import copy
import hashlib
import json
import math
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from . import codex_app_server
from . import labels as labels_module
from .labels import ValidationError, validate_label_output
from .util import now_iso, sha256_text, write_text_atomic


ADAPTER_SCHEMA_VERSION = "pif_canonical_v31_episode_batch_adapter_v1"
RAW_OUTPUT_SCHEMA_VERSION = "pif_canonical_v31_episode_batch_raw_output_v1"
PROJECTION_SCHEMA_VERSION = "pif_canonical_v31_episode_batch_projection_v1"
PROVENANCE_SCHEMA_VERSION = "pif_canonical_v31_evidence_provenance_v1"
FIDELITY_SCHEMA_VERSION = "pif_canonical_v31_semantic_fidelity_v1"
ATTEMPT_RECEIPT_VERSION = "pif_canonical_v31_semantic_call_attempt_v1"
RUN_REPORT_VERSION = "pif_canonical_v31_episode_batch_report_v1"
MATRIX_BINDING_VERSION = "pif_canonical_v31_six_arm_matrix_binding_v1"

CANDIDATE_SYSTEM_ID = "pif_direct_full_canonical_ai_discourse_v3_1_v1"
CANONICAL_LABEL_PACK = "ai_discourse_v3_1"
CANONICAL_LABEL_SCHEMA_SHA256 = (
    "01784bafc6aa5869eeffc487548acf6cca06fa9816ffb26d6c423585657308eb"
)
CANONICAL_SCHEMA_STRATEGY = (
    "canonical_v31_copy_all_semantic_fields_replace_only_event_and_candidate_"
    "evidence_with_exact_source_unit_ownership_v1"
)
MODEL = "gpt-5.6-sol"
EFFORT = "high"
SUPPORTED_BATCH_SIZES = (3, 5, 8)
SUPPORTED_THREAD_MODES = ("new_thread", "same_thread")
RETRY_COUNT = 0
SEMANTIC_POSTPROCESSING = False
# Canonical event and concept-candidate evidence is capped at 1,000 characters.
# Keep each structural source unit at most 900 characters so a single selected
# unit has a documented 100-character safety margin.  Multi-unit selections are
# never shortened; canonical validation rejects a reconstructed span over 1,000.
CANONICAL_EVIDENCE_MAX_CHARS = 1000
SOURCE_UNIT_MAX_CHARS = 900
SOURCE_UNIT_SAFETY_MARGIN_CHARS = (
    CANONICAL_EVIDENCE_MAX_CHARS - SOURCE_UNIT_MAX_CHARS
)
SOURCE_UNIT_CHUNKING_VERSION = "pif_exact_offset_structural_source_units_v1"
USAGE_FIELDS = (
    "input_tokens",
    "cached_input_tokens",
    "output_tokens",
    "reasoning_output_tokens",
    "total_tokens",
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
LABEL_SCHEMA_PATH = PROJECT_ROOT / "label_packs/ai_discourse_v3_1/schema.json"
PINNED_CODEX = (
    PROJECT_ROOT
    / "work/app-server-development-v2/pinned-runtime/codex-0.144.1/bin/codex"
)
EXPECTED_INSTRUCTION_SOURCE_PATHS = (
    str((Path.home() / ".codex/AGENTS.md").expanduser().resolve()),
)
EXPECTED_INSTRUCTION_SOURCES_COUNT = len(EXPECTED_INSTRUCTION_SOURCE_PATHS)
EXPECTED_INSTRUCTION_SOURCES_SHA256 = sha256_text(
    json.dumps(
        list(EXPECTED_INSTRUCTION_SOURCE_PATHS),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
)

CONTEXT_CONTROL_OVERLAY: dict[str, Any] = {
    "features": {
        "apps": False,
        "goals": False,
        "image_generation": False,
        "memories": False,
        "multi_agent": False,
        "plugins": False,
        "remote_plugin": False,
        "standalone_web_search": False,
        "tool_suggest": False,
    },
    "include_apps_instructions": False,
    "include_collaboration_mode_instructions": False,
    "include_environment_context": False,
    "include_permissions_instructions": False,
    "mcp_servers": {
        "computer-use": {"enabled": False},
        "node_repl": {"enabled": False},
        "openaiDeveloperDocs": {"enabled": False},
    },
    "orchestrator": {
        "mcp": {"enabled": False},
        "skills": {"enabled": False},
    },
    "personality": "none",
    "project_doc_max_bytes": 0,
    "skills": {"include_instructions": False},
    "tools": {"experimental_request_user_input": {"enabled": False}},
    "web_search": "disabled",
}

_DETERMINISTIC_LABEL_FIELDS = frozenset(
    {"schema_version", "segment_id", "episode_id", "segment_quality"}
)
_EVIDENCE_FIELDS = frozenset({"evidence", "evidence_start", "evidence_end"})
_UNIT_EVIDENCE_FIELDS = (
    "evidence_start_unit_id",
    "evidence_end_unit_id",
)
_EPISODE_CONTEXT_FIELDS = (
    "episode_id",
    "source_name",
    "episode_title",
    "context_summary",
    "speaker_map",
    "section_map",
    "entity_seed",
    "concept_seed",
    "extraction_guidance",
    "excluded_source_context",
)


class CanonicalV31EpisodeBatchError(RuntimeError):
    """The prepared request, fixed adapter contract, or run lifecycle drifted."""


class CanonicalV31OutputError(CanonicalV31EpisodeBatchError):
    """A completed model output failed fail-closed canonical validation."""


class CanonicalV31TelemetryError(CanonicalV31EpisodeBatchError):
    """Managed-auth lifecycle or complete sidecar accounting was invalid."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.expanduser().resolve(strict=True).open("rb") as handle:
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
        raise CanonicalV31EpisodeBatchError(f"cannot read {label}") from exc


def _write_immutable_json(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    resolved = path.expanduser().resolve()
    if resolved.exists():
        if not resolved.is_file() or resolved.read_text(encoding="utf-8") != payload:
            raise CanonicalV31EpisodeBatchError(f"frozen {resolved.name} drifted")
        return
    resolved.parent.mkdir(parents=True, exist_ok=True)
    write_text_atomic(resolved, payload)


def _write_immutable_text(path: Path, value: str) -> None:
    resolved = path.expanduser().resolve()
    if resolved.exists():
        if not resolved.is_file() or resolved.read_text(encoding="utf-8") != value:
            raise CanonicalV31EpisodeBatchError(f"frozen {resolved.name} drifted")
        return
    resolved.parent.mkdir(parents=True, exist_ok=True)
    write_text_atomic(resolved, value)


def _nonempty_identifier(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise CanonicalV31EpisodeBatchError(f"{label} must be a non-empty identifier")
    return value


def _is_iso_timestamp(value: Any) -> bool:
    if not isinstance(value, str) or not value or value != value.strip():
        return False
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return True


def _nonnegative_number(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CanonicalV31TelemetryError(f"{label} is not numeric")
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise CanonicalV31TelemetryError(f"{label} is not finite and nonnegative")
    return result


def _usage_values(value: Any, label: str) -> dict[str, int]:
    if not isinstance(value, Mapping):
        raise CanonicalV31TelemetryError(f"{label} is absent")
    result: dict[str, int] = {}
    for field in USAGE_FIELDS:
        item = value.get(field)
        if isinstance(item, bool) or not isinstance(item, int) or item < 0:
            raise CanonicalV31TelemetryError(f"{label}.{field} is invalid")
        result[field] = item
    if (
        result["cached_input_tokens"] > result["input_tokens"]
        or result["reasoning_output_tokens"] > result["output_tokens"]
        or result["total_tokens"]
        != result["input_tokens"] + result["output_tokens"]
    ):
        raise CanonicalV31TelemetryError(f"{label} is inconsistent")
    return result


def verified_context_control_overlay() -> dict[str, Any]:
    """Return the exact strict overlay after checking the zero-byte boundary."""

    overlay = copy.deepcopy(CONTEXT_CONTROL_OVERLAY)
    if (
        overlay.get("project_doc_max_bytes") != 0
        or overlay.get("include_apps_instructions") is not False
        or overlay.get("include_environment_context") is not False
        or overlay.get("include_permissions_instructions") is not False
        or overlay.get("web_search") != "disabled"
        or overlay.get("personality") != "none"
    ):
        raise CanonicalV31EpisodeBatchError("zero-byte context overlay drifted")
    return overlay


def expected_instruction_source_contract() -> dict[str, Any]:
    """Bind exact reported paths while proving their model-visible budget is zero."""

    overlay = verified_context_control_overlay()
    paths = list(EXPECTED_INSTRUCTION_SOURCE_PATHS)
    if (
        not paths
        or len(paths) != len(set(paths))
        or EXPECTED_INSTRUCTION_SOURCES_COUNT != len(paths)
        or EXPECTED_INSTRUCTION_SOURCES_SHA256 != sha256_text(_canonical_json(paths))
        or overlay["project_doc_max_bytes"] != 0
    ):
        raise CanonicalV31EpisodeBatchError("instruction-source isolation contract drifted")
    return {
        "effective_instruction_source_paths": paths,
        "effective_instruction_sources_count": EXPECTED_INSTRUCTION_SOURCES_COUNT,
        "effective_instruction_sources_sha256": EXPECTED_INSTRUCTION_SOURCES_SHA256,
        "project_instruction_content_byte_budget": 0,
        "project_instruction_content_included": False,
        "context_control_overlay_sha256": sha256_text(_canonical_json(overlay)),
    }


def canonical_label_schema() -> dict[str, Any]:
    """Load the exact canonical v3.1 schema bound by this adapter version."""

    if _sha256_file(LABEL_SCHEMA_PATH) != CANONICAL_LABEL_SCHEMA_SHA256:
        raise CanonicalV31EpisodeBatchError("canonical v3.1 label schema drifted")
    value = _load_json(LABEL_SCHEMA_PATH, "canonical v3.1 label schema")
    if (
        not isinstance(value, Mapping)
        or value.get("$id") != CANONICAL_LABEL_PACK
        or value.get("additionalProperties") is not False
    ):
        raise CanonicalV31EpisodeBatchError("canonical v3.1 label schema is malformed")
    try:
        evidence_limits = {
            value["properties"][collection]["items"]["properties"]["evidence"][
                "maxLength"
            ]
            for collection in ("discourse_events", "concept_candidates")
        }
    except (KeyError, TypeError) as exc:
        raise CanonicalV31EpisodeBatchError(
            "canonical v3.1 evidence schema is malformed"
        ) from exc
    if evidence_limits != {CANONICAL_EVIDENCE_MAX_CHARS}:
        raise CanonicalV31EpisodeBatchError(
            "canonical v3.1 evidence character limit drifted"
        )
    return copy.deepcopy(dict(value))


def _validate_schema(value: Any, schema: Mapping[str, Any], path: str = "$") -> None:
    """Validate the JSON-Schema subset used by the canonical structured output."""

    if "const" in schema and value != schema["const"]:
        raise CanonicalV31OutputError(f"{path} must equal {schema['const']!r}")
    expected_type = schema.get("type")
    if isinstance(expected_type, list):
        errors = []
        for candidate in expected_type:
            try:
                _validate_schema(value, {**schema, "type": candidate}, path)
                return
            except CanonicalV31OutputError as exc:
                errors.append(str(exc))
        raise CanonicalV31OutputError("; ".join(errors))
    if expected_type == "object":
        if not isinstance(value, Mapping):
            raise CanonicalV31OutputError(f"{path} must be an object")
        properties = schema.get("properties")
        required = schema.get("required")
        if not isinstance(properties, Mapping) or not isinstance(required, list):
            raise CanonicalV31EpisodeBatchError(f"{path} schema is malformed")
        for key in required:
            if key not in value:
                raise CanonicalV31OutputError(f"{path}.{key} is required")
        if schema.get("additionalProperties") is False:
            extras = sorted(set(value) - set(properties))
            if extras:
                raise CanonicalV31OutputError(
                    f"{path} has unexpected keys: {', '.join(extras)}"
                )
        for key, child in properties.items():
            if key in value:
                if not isinstance(child, Mapping):
                    raise CanonicalV31EpisodeBatchError(f"{path}.{key} schema is malformed")
                _validate_schema(value[key], child, f"{path}.{key}")
    elif expected_type == "array":
        if not isinstance(value, list):
            raise CanonicalV31OutputError(f"{path} must be an array")
        minimum = schema.get("minItems")
        maximum = schema.get("maxItems")
        if isinstance(minimum, int) and len(value) < minimum:
            raise CanonicalV31OutputError(f"{path} must have at least {minimum} items")
        if isinstance(maximum, int) and len(value) > maximum:
            raise CanonicalV31OutputError(f"{path} must have at most {maximum} items")
        item_schema = schema.get("items")
        if not isinstance(item_schema, Mapping):
            raise CanonicalV31EpisodeBatchError(f"{path} item schema is malformed")
        for index, item in enumerate(value):
            _validate_schema(item, item_schema, f"{path}[{index}]")
    elif expected_type == "string":
        if not isinstance(value, str):
            raise CanonicalV31OutputError(f"{path} must be a string")
        if "minLength" in schema and len(value) < int(schema["minLength"]):
            raise CanonicalV31OutputError(
                f"{path} must be at least {schema['minLength']} characters"
            )
        if "maxLength" in schema and len(value) > int(schema["maxLength"]):
            raise CanonicalV31OutputError(
                f"{path} must be at most {schema['maxLength']} characters"
            )
    elif expected_type == "number":
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise CanonicalV31OutputError(f"{path} must be a number")
        if not math.isfinite(float(value)):
            raise CanonicalV31OutputError(f"{path} must be finite")
        if "minimum" in schema and value < schema["minimum"]:
            raise CanonicalV31OutputError(f"{path} must be >= {schema['minimum']}")
        if "maximum" in schema and value > schema["maximum"]:
            raise CanonicalV31OutputError(f"{path} must be <= {schema['maximum']}")
    elif expected_type == "integer":
        if isinstance(value, bool) or not isinstance(value, int):
            raise CanonicalV31OutputError(f"{path} must be an integer")
        if "minimum" in schema and value < schema["minimum"]:
            raise CanonicalV31OutputError(f"{path} must be >= {schema['minimum']}")
        if "maximum" in schema and value > schema["maximum"]:
            raise CanonicalV31OutputError(f"{path} must be <= {schema['maximum']}")
    elif expected_type == "boolean":
        if not isinstance(value, bool):
            raise CanonicalV31OutputError(f"{path} must be a boolean")
    elif expected_type == "null":
        if value is not None:
            raise CanonicalV31OutputError(f"{path} must be null")
    elif expected_type is not None:
        raise CanonicalV31EpisodeBatchError(f"{path} uses unsupported schema type")
    if "enum" in schema and value not in schema["enum"]:
        raise CanonicalV31OutputError(f"{path} must be one of {schema['enum']}")


def _unit_owned_item_schema(source: Mapping[str, Any], unit_ids: Sequence[str]) -> dict[str, Any]:
    item = copy.deepcopy(dict(source))
    properties = item["properties"]
    for field in _EVIDENCE_FIELDS:
        properties.pop(field)
    item["required"] = [field for field in item["required"] if field not in _EVIDENCE_FIELDS]
    unit_schema = {
        "type": "string",
        "enum": list(unit_ids),
        "description": "Opaque exact source unit ID; deterministic code projects text and offsets.",
    }
    for field in _UNIT_EVIDENCE_FIELDS:
        properties[field] = copy.deepcopy(unit_schema)
        item["required"].append(field)
    return item


def build_output_schema(
    *, episode_id: str, segment_ids: Sequence[str], unit_ids: Sequence[str], maximum_unit_count: int
) -> dict[str, Any]:
    """Build a structured-output schema containing every canonical semantic field."""

    canonical = canonical_label_schema()
    properties = canonical["properties"]
    semantic_names = [
        name for name in canonical["required"] if name not in _DETERMINISTIC_LABEL_FIELDS
    ]
    semantic_properties = {
        name: copy.deepcopy(properties[name]) for name in semantic_names
    }
    semantic_properties["discourse_events"]["items"] = _unit_owned_item_schema(
        properties["discourse_events"]["items"], unit_ids
    )
    semantic_properties["concept_candidates"]["items"] = _unit_owned_item_schema(
        properties["concept_candidates"]["items"], unit_ids
    )
    receipt = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "unit_id",
            "reviewed",
            "grounded_event_count",
            "grounded_concept_candidate_count",
            "unresolved_count",
        ],
        "properties": {
            "unit_id": {"type": "string", "enum": list(unit_ids)},
            "reviewed": {"type": "boolean", "const": True},
            "grounded_event_count": {"type": "integer", "minimum": 0},
            "grounded_concept_candidate_count": {"type": "integer", "minimum": 0},
            "unresolved_count": {"type": "integer", "const": 0},
        },
    }
    segment_properties: dict[str, Any] = {
        "segment_id": {"type": "string", "enum": list(segment_ids)},
        **semantic_properties,
        "unit_receipts": {
            "type": "array",
            "minItems": 1,
            "maxItems": maximum_unit_count,
            "items": receipt,
        },
        "coverage_audit": {
            "type": "object",
            "additionalProperties": False,
            "required": ["all_source_units_reviewed", "unresolved_count"],
            "properties": {
                "all_source_units_reviewed": {"type": "boolean", "const": True},
                "unresolved_count": {"type": "integer", "const": 0},
            },
        },
    }
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": RAW_OUTPUT_SCHEMA_VERSION,
        "type": "object",
        "additionalProperties": False,
        "required": ["episode_id", "segments"],
        "properties": {
            "episode_id": {"type": "string", "enum": [episode_id]},
            "segments": {
                "type": "array",
                "minItems": len(segment_ids),
                "maxItems": len(segment_ids),
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [
                        "segment_id",
                        *semantic_names,
                        "unit_receipts",
                        "coverage_audit",
                    ],
                    "properties": segment_properties,
                },
            },
        },
    }


def _schema_leaf_paths(
    schema: Mapping[str, Any],
    *,
    skip: frozenset[str] = frozenset(),
) -> list[str]:
    paths: list[str] = []

    def walk(node: Mapping[str, Any], path: str) -> None:
        if path in skip:
            return
        if node.get("type") == "object":
            for name, child in node.get("properties", {}).items():
                walk(child, f"{path}.{name}")
        elif node.get("type") == "array":
            walk(node["items"], path + "[]")
        else:
            paths.append(path)

    walk(schema, "$")
    return sorted(paths)


def canonical_final_field_paths() -> list[str]:
    """Enumerate every leaf in the final canonical v3.1 label."""

    return _schema_leaf_paths(canonical_label_schema())


def deterministic_provenance_field_paths() -> list[str]:
    """Enumerate canonical leaves attached only from exact source provenance."""

    canonical = canonical_label_schema()
    quality_paths = _schema_leaf_paths(
        canonical["properties"]["segment_quality"]
    )
    return sorted(
        [
            "$.schema_version",
            "$.segment_id",
            "$.episode_id",
            *("$.segment_quality" + path[1:] for path in quality_paths),
            "$.discourse_events[].evidence",
            "$.discourse_events[].evidence_start",
            "$.discourse_events[].evidence_end",
            "$.concept_candidates[].evidence",
            "$.concept_candidates[].evidence_start",
            "$.concept_candidates[].evidence_end",
        ]
    )


def model_semantic_field_paths() -> list[str]:
    """Enumerate canonical semantic leaves that the model emits directly."""

    deterministic = frozenset(deterministic_provenance_field_paths())
    return [
        path for path in canonical_final_field_paths() if path not in deterministic
    ]


def model_evidence_ownership_field_paths() -> list[str]:
    """Enumerate raw unit-span and coverage fields owned by the model turn."""

    return [
        "$.segments[].discourse_events[].evidence_start_unit_id",
        "$.segments[].discourse_events[].evidence_end_unit_id",
        "$.segments[].concept_candidates[].evidence_start_unit_id",
        "$.segments[].concept_candidates[].evidence_end_unit_id",
        "$.segments[].unit_receipts[].unit_id",
        "$.segments[].unit_receipts[].reviewed",
        "$.segments[].unit_receipts[].grounded_event_count",
        "$.segments[].unit_receipts[].grounded_concept_candidate_count",
        "$.segments[].unit_receipts[].unresolved_count",
        "$.segments[].coverage_audit.all_source_units_reviewed",
        "$.segments[].coverage_audit.unresolved_count",
    ]


def semantic_field_paths() -> list[str]:
    """Compatibility alias for the direct model-owned semantic leaf set."""

    return model_semantic_field_paths()


def semantic_integrity_contract() -> dict[str, Any]:
    """Machine-readable boundary between model semantics and deterministic work."""

    return {
        "one_llm_turn_per_prepared_batch": True,
        "model_emits_every_model_owned_semantic_field": True,
        "model_selects_exact_evidence_unit_span_ownership": True,
        "model_emits_deterministic_provenance_fields": False,
        "full_canonical_final_output_after_provenance_projection": True,
        "raw_schema_contains_every_canonical_final_leaf": False,
        "deterministic_identity_attachment_only": True,
        "deterministic_segment_quality_attachment_only": True,
        "deterministic_exact_evidence_projection_only": True,
        "source_unit_chunking_version": SOURCE_UNIT_CHUNKING_VERSION,
        "source_unit_chunking_is_structural_not_semantic": True,
        "source_unit_max_characters": SOURCE_UNIT_MAX_CHARS,
        "canonical_evidence_max_characters": CANONICAL_EVIDENCE_MAX_CHARS,
        "source_unit_safety_margin_characters": SOURCE_UNIT_SAFETY_MARGIN_CHARS,
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


def _episode_context(episode: Mapping[str, Any]) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for field in _EPISODE_CONTEXT_FIELDS:
        if field not in episode:
            raise CanonicalV31EpisodeBatchError(f"episode context {field} is required")
        value = episode[field]
        if field in {"episode_id", "source_name", "episode_title", "context_summary", "extraction_guidance"}:
            if not isinstance(value, str) or (field == "episode_id" and not value):
                raise CanonicalV31EpisodeBatchError(f"episode context {field} is invalid")
        elif field in {"speaker_map", "section_map"}:
            if not isinstance(value, list) or any(not isinstance(row, Mapping) for row in value):
                raise CanonicalV31EpisodeBatchError(f"episode context {field} is invalid")
        elif field == "entity_seed":
            if not isinstance(value, Mapping):
                raise CanonicalV31EpisodeBatchError(f"episode context {field} is invalid")
        elif field in {"concept_seed", "excluded_source_context"}:
            if not isinstance(value, list) or any(not isinstance(row, str) for row in value):
                raise CanonicalV31EpisodeBatchError(f"episode context {field} is invalid")
        values[field] = copy.deepcopy(value)
    return values


def _owner_boundary(
    offset: int, boundaries: Sequence[Mapping[str, Any]]
) -> Mapping[str, Any]:
    """Return the one half-open owner interval containing a source character."""

    matches = [
        row
        for row in boundaries
        if int(row["owner_start"]) <= offset < int(row["owner_end"])
    ]
    if len(matches) != 1:
        raise CanonicalV31EpisodeBatchError("source unit owner window is ambiguous")
    return matches[0]


def _owner_window(offset: int, boundaries: Sequence[Mapping[str, Any]]) -> int:
    return int(_owner_boundary(offset, boundaries)["window_id"])


def _validate_boundaries(text: str, raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)) or not raw:
        raise CanonicalV31EpisodeBatchError("segment boundaries are empty or invalid")
    result: list[dict[str, Any]] = []
    seen: set[int] = set()
    for row in raw:
        if not isinstance(row, Mapping):
            raise CanonicalV31EpisodeBatchError("segment boundary is not an object")
        values = {}
        for field in ("window_id", "owner_start", "owner_end", "extract_start", "extract_end"):
            value = row.get(field)
            if isinstance(value, bool) or not isinstance(value, int):
                raise CanonicalV31EpisodeBatchError(f"segment boundary {field} is invalid")
            values[field] = value
        if (
            values["window_id"] in seen
            or values["extract_start"] < 0
            or values["extract_start"] > values["owner_start"]
            or values["owner_start"] > values["owner_end"]
            or values["owner_end"] > values["extract_end"]
            or values["extract_end"] > len(text)
        ):
            raise CanonicalV31EpisodeBatchError("segment boundary offsets are invalid")
        seen.add(values["window_id"])
        result.append(copy.deepcopy(dict(row)))
    return result


def _source_units(
    text: str, boundaries: Sequence[Mapping[str, Any]], *, segment_position: int
) -> list[dict[str, Any]]:
    """Partition non-whitespace line content into exact, bounded source slices.

    Line endings plus leading, trailing, and between-unit whitespace are
    structural delimiters.  They remain in ``segment_text`` and therefore in a
    reconstructed multi-unit evidence span, but are not emitted as standalone
    units.  Every non-whitespace character is represented exactly once.  The
    partition uses only character offsets, whitespace, and owner intervals; it
    makes no semantic selection.
    """

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
    for raw in text.splitlines(keepends=True):
        content = raw.rstrip("\r\n")
        if content.strip():
            line_end = cursor + len(content)
            content_start = cursor
            while content_start < line_end and text[content_start].isspace():
                content_start += 1
            content_end = line_end
            while content_end > content_start and text[content_end - 1].isspace():
                content_end -= 1

            start = content_start
            while start < content_end:
                while start < content_end and text[start].isspace():
                    start += 1
                if start >= content_end:
                    break
                owner = _owner_boundary(start, boundaries)
                limit = min(
                    content_end,
                    start + SOURCE_UNIT_MAX_CHARS,
                    int(owner["owner_end"]),
                )
                if limit <= start:
                    raise CanonicalV31EpisodeBatchError(
                        "source unit owner interval cannot represent source content"
                    )
                end = limit
                while end > start and text[end - 1].isspace():
                    end -= 1
                unit_text = text[start:end]
                if (
                    not unit_text.strip()
                    or len(unit_text) > SOURCE_UNIT_MAX_CHARS
                    or text[start:end] != unit_text
                ):
                    raise CanonicalV31EpisodeBatchError(
                        "source unit structural chunking failed"
                    )
                units.append(
                    {
                        # Stable and opaque: this ordinal exposes no source text
                        # or semantic label and is deterministic for an identical
                        # segment position and exact source partition.
                        "unit_id": f"S{segment_position:04d}U{len(units):04d}",
                        "start_char": start,
                        "end_char": end,
                        "window_id": int(owner["window_id"]),
                        "text": unit_text,
                    }
                )
                start = limit
        cursor += len(raw)
    if cursor != len(text) or not units:
        raise CanonicalV31EpisodeBatchError("source unit projection is empty or truncated")
    if len({unit["unit_id"] for unit in units}) != len(units):
        raise CanonicalV31EpisodeBatchError("source unit IDs are not unique")
    return units


def _prepare_segment(segment: Mapping[str, Any], *, segment_position: int) -> dict[str, Any]:
    segment_id = _nonempty_identifier(segment.get("segment_id"), "segment_id")
    text = segment.get("segment_text")
    if not isinstance(text, str) or not text:
        raise CanonicalV31EpisodeBatchError("segment_text must be non-empty")
    boundaries = _validate_boundaries(text, segment.get("boundaries"))
    units = _source_units(text, boundaries, segment_position=segment_position)
    boundary_by_id = {int(row["window_id"]): row for row in boundaries}
    for unit in units:
        start = unit["start_char"]
        end = unit["end_char"]
        owner = boundary_by_id[unit["window_id"]]
        if (
            text[start:end] != unit["text"]
            or len(unit["text"]) > SOURCE_UNIT_MAX_CHARS
            or not (
                int(owner["owner_start"]) <= start < int(owner["owner_end"])
                and end <= int(owner["owner_end"])
                and int(owner["extract_start"]) <= start
                and end <= int(owner["extract_end"])
            )
        ):
            raise CanonicalV31EpisodeBatchError("source unit offset projection failed")
    if "segment_quality" not in segment or not isinstance(segment["segment_quality"], Mapping):
        raise CanonicalV31EpisodeBatchError("segment_quality provenance is required")
    quality = copy.deepcopy(dict(segment["segment_quality"]))
    try:
        _validate_schema(
            quality,
            canonical_label_schema()["properties"]["segment_quality"],
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
        "density_stratum": density,
        "segment_position": segment_position,
    }


BASE_INSTRUCTIONS_TEMPLATE = """You are the sole semantic extractor for a private podcast research corpus. Read every source unit in every segment before finalizing. All semantic decisions must come from understanding the language; never use keyword, regex, phrase, topic-list, sponsor-list, or event-count heuristics.

For each segment return every semantic field in the structured schema. Populate the complete canonical ai_discourse_v3_1 surface: extraction_status; segment_source_context; every nested discourse-event actor, speaker_context, reported_actor, source_context, target, term/frame/entity list, stance, claim, certainty, horizon, mechanism, counterclaim, metric, signal reason, flags, confidence, and audit notes; concept_candidates; rejected_candidates; no_signal_reason; overall_confidence; needs_review; and review_reason. Do not omit a field and do not rely on downstream defaults.

Enumerate each independent grounded research-useful proposition once, in source order. Do not collapse distinct premises, mechanisms, capabilities, constraints, comparisons, outcomes, alternatives, frames, uncertainties, counterclaims, product signals, market signals, risks, adoption signals, actors, or entity references. Do not infer beyond the source. Preserve repeated propositions when the source truly repeats them; downstream code does not deduplicate.

For each discourse event and concept candidate, return only evidence_start_unit_id and evidence_end_unit_id from the same segment. Choose the smallest contiguous source-unit range supporting every material emitted field. Deterministic code will attach the exact source substring and character offsets. Every nonempty metric value, unit, comparator, and raw_text must be a literal substring of that selected evidence; raw_text must itself be a contiguous evidence substring.

Return one unit_receipt for every supplied unit, in supplied order. grounded_event_count and grounded_concept_candidate_count are owned by the unit where each emitted evidence range starts. Counts must exactly reconcile to the emitted arrays. Mark every unit reviewed and leave zero unresolved units only after completing the audit.

Use extraction_status=coded if and only if at least one grounded discourse event is emitted. Every other status requires discourse_events=[] and a nonempty no_signal_reason. Source-context classification, including sponsor/ad, setup, chrome, mixed, and quoted material, is a model semantic decision governed by the source and episode context; deterministic code performs no sponsor, keyword, or regex pruning.

Keep episode and segment order equal to the input. Return schema-valid JSON only.

# Episode context
{EPISODE_CONTEXT_JSON}
"""


def _render_base_instructions(context: Mapping[str, Any]) -> str:
    context_json = json.dumps(context, ensure_ascii=True, separators=(",", ":"))
    rendered = BASE_INSTRUCTIONS_TEMPLATE.replace("{EPISODE_CONTEXT_JSON}", context_json)
    if rendered.count(context_json) != 1 or "{EPISODE_CONTEXT_JSON}" in rendered:
        raise CanonicalV31EpisodeBatchError("episode context rendering failed")
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
            }
            for segment in segments
        ],
    }
    return "# Canonical v3.1 source-unit packet\n" + json.dumps(
        packet, ensure_ascii=True, separators=(",", ":")
    ) + "\n"


def _validate_arm_choice(batch_size: int, thread_mode: str) -> None:
    if isinstance(batch_size, bool) or batch_size not in SUPPORTED_BATCH_SIZES:
        raise CanonicalV31EpisodeBatchError("batch_size must be 3, 5, or 8")
    if thread_mode not in SUPPORTED_THREAD_MODES:
        raise CanonicalV31EpisodeBatchError("thread_mode must be new_thread or same_thread")


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
    return "cv31_" + sha256_text(_canonical_json(identity))[:24]


def prepare_episode_batches(
    episode: Mapping[str, Any], *, batch_size: int, thread_mode: str
) -> list[dict[str, Any]]:
    """Prepare one-turn full-canonical requests for a real episode packet."""

    _validate_arm_choice(batch_size, thread_mode)
    context = _episode_context(episode)
    raw_segments = episode.get("segments")
    if not isinstance(raw_segments, Sequence) or isinstance(raw_segments, (str, bytes)) or not raw_segments:
        raise CanonicalV31EpisodeBatchError("episode segments are empty")
    prepared = []
    for position, segment in enumerate(raw_segments):
        if not isinstance(segment, Mapping):
            raise CanonicalV31EpisodeBatchError("episode segment is not an object")
        prepared.append(_prepare_segment(segment, segment_position=position))
    segment_ids = [segment["segment_id"] for segment in prepared]
    if len(segment_ids) != len(set(segment_ids)):
        raise CanonicalV31EpisodeBatchError("episode segment IDs are not unique")
    requests = []
    base = _render_base_instructions(context)
    for batch_index, start in enumerate(range(0, len(prepared), batch_size)):
        batch_segments = prepared[start : start + batch_size]
        batch_segment_ids = [segment["segment_id"] for segment in batch_segments]
        unit_ids = [unit["unit_id"] for segment in batch_segments for unit in segment["units"]]
        if len(unit_ids) != len(set(unit_ids)):
            raise CanonicalV31EpisodeBatchError("batch source-unit IDs are not unique")
        schema = build_output_schema(
            episode_id=context["episode_id"],
            segment_ids=batch_segment_ids,
            unit_ids=unit_ids,
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
            "semantic_postprocessing": SEMANTIC_POSTPROCESSING,
            "semantic_integrity": semantic_integrity_contract(),
            "episode_context": copy.deepcopy(context),
            "private_input": private_input,
            "prompt": prompt,
            "base_instructions": base,
            "output_schema": schema,
            "prompt_sha256": sha256_text(prompt),
            "base_instructions_sha256": sha256_text(base),
            "output_schema_sha256": sha256_text(_canonical_json(schema)),
            "context_control_overlay_sha256": sha256_text(
                _canonical_json(verified_context_control_overlay())
            ),
        }
        validate_prepared_request(request)
        requests.append(request)
    return requests


def validate_prepared_request(request: Mapping[str, Any]) -> dict[str, Any]:
    """Re-derive all prepared artifacts and fail closed on any drift."""

    required_keys = {
        "schema_version",
        "candidate_system_id",
        "canonical_label_pack",
        "canonical_label_schema_sha256",
        "canonical_schema_strategy",
        "batch_id",
        "batch_index",
        "episode_id",
        "segment_ids",
        "batch_size_ceiling",
        "effective_batch_size",
        "thread_mode",
        "model",
        "effort",
        "retry_count",
        "semantic_postprocessing",
        "semantic_integrity",
        "episode_context",
        "private_input",
        "prompt",
        "base_instructions",
        "output_schema",
        "prompt_sha256",
        "base_instructions_sha256",
        "output_schema_sha256",
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
    _validate_arm_choice(batch_size, thread_mode)
    episode_id = _nonempty_identifier(request["episode_id"], "episode_id")
    context = request["episode_context"]
    private_input = request["private_input"]
    if not isinstance(context, Mapping) or not isinstance(private_input, Mapping):
        raise CanonicalV31EpisodeBatchError("prepared request context is invalid")
    expected_context = _episode_context(context)
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
    rederived = []
    for segment in private_input["segments"]:
        if not isinstance(segment, Mapping):
            raise CanonicalV31EpisodeBatchError("prepared segment is invalid")
        position = segment.get("segment_position")
        if isinstance(position, bool) or not isinstance(position, int) or position < 0:
            raise CanonicalV31EpisodeBatchError("prepared segment position is invalid")
        expected = _prepare_segment(segment, segment_position=position)
        if dict(segment) != expected:
            raise CanonicalV31EpisodeBatchError("prepared source-unit projection drifted")
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
    expected_schema = build_output_schema(
        episode_id=episode_id,
        segment_ids=segment_ids,
        unit_ids=unit_ids,
        maximum_unit_count=max(len(segment["units"]) for segment in rederived),
    )
    expected_prompt = _render_prompt(episode_id, rederived)
    expected_base = _render_base_instructions(expected_context)
    if (
        request["output_schema"] != expected_schema
        or request["prompt"] != expected_prompt
        or request["base_instructions"] != expected_base
        or request["prompt_sha256"] != sha256_text(expected_prompt)
        or request["base_instructions_sha256"] != sha256_text(expected_base)
        or request["output_schema_sha256"] != sha256_text(_canonical_json(expected_schema))
        or request["context_control_overlay_sha256"]
        != sha256_text(_canonical_json(verified_context_control_overlay()))
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


def _owned_evidence(
    item: Mapping[str, Any], segment: Mapping[str, Any], *, path: str
) -> tuple[str, int, int, list[str]]:
    unit_list = segment["units"]
    by_id = {unit["unit_id"]: (index, unit) for index, unit in enumerate(unit_list)}
    start_id = item["evidence_start_unit_id"]
    end_id = item["evidence_end_unit_id"]
    if start_id not in by_id or end_id not in by_id:
        raise CanonicalV31OutputError(f"{path} evidence unit belongs to another segment")
    start_index, start_unit = by_id[start_id]
    end_index, end_unit = by_id[end_id]
    if start_index > end_index:
        raise CanonicalV31OutputError(f"{path} evidence unit range is reversed")
    start = start_unit["start_char"]
    end = end_unit["end_char"]
    text = segment["segment_text"]
    if not isinstance(start, int) or not isinstance(end, int) or start < 0 or start >= end or end > len(text):
        raise CanonicalV31OutputError(f"{path} projected evidence offsets are invalid")
    evidence = text[start:end]
    if not evidence:
        raise CanonicalV31OutputError(f"{path} projected evidence is empty")
    if len(evidence) > CANONICAL_EVIDENCE_MAX_CHARS:
        raise CanonicalV31OutputError(
            f"{path} projected evidence exceeds canonical maxLength="
            f"{CANONICAL_EVIDENCE_MAX_CHARS}"
        )
    return evidence, start, end, [
        unit["unit_id"] for unit in unit_list[start_index : end_index + 1]
    ]


def _project_owned_items(
    items: Sequence[Mapping[str, Any]],
    canonical_item_schema: Mapping[str, Any],
    segment: Mapping[str, Any],
    *,
    collection_name: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    canonical_properties = canonical_item_schema["properties"]
    projected: list[dict[str, Any]] = []
    provenance: list[dict[str, Any]] = []
    for index, item in enumerate(items):
        path = f"$.{collection_name}[{index}]"
        evidence, start, end, evidence_unit_ids = _owned_evidence(item, segment, path=path)
        value: dict[str, Any] = {}
        for field in canonical_item_schema["required"]:
            if field == "evidence":
                value[field] = evidence
            elif field == "evidence_start":
                value[field] = start
            elif field == "evidence_end":
                value[field] = end
            else:
                value[field] = copy.deepcopy(item[field])
        if set(value) != set(canonical_properties):
            raise CanonicalV31OutputError(f"{path} canonical projection field set drifted")
        projected.append(value)
        provenance.append(
            {
                "item_index": index,
                "evidence_start_unit_id": item["evidence_start_unit_id"],
                "evidence_end_unit_id": item["evidence_end_unit_id"],
                "evidence_unit_ids": evidence_unit_ids,
                "evidence_start": start,
                "evidence_end": end,
                "evidence_sha256": sha256_text(evidence),
            }
        )
    return projected, provenance


def _semantic_view_from_raw(segment: Mapping[str, Any]) -> dict[str, Any]:
    canonical = canonical_label_schema()
    semantic_names = [
        field for field in canonical["required"] if field not in _DETERMINISTIC_LABEL_FIELDS
    ]
    result = {}
    for field in semantic_names:
        if field not in {"discourse_events", "concept_candidates"}:
            result[field] = copy.deepcopy(segment[field])
            continue
        result[field] = [
            {
                name: copy.deepcopy(value)
                for name, value in item.items()
                if name not in _UNIT_EVIDENCE_FIELDS
            }
            for item in segment[field]
        ]
    return result


def _semantic_view_from_label(label: Mapping[str, Any]) -> dict[str, Any]:
    canonical = canonical_label_schema()
    semantic_names = [
        field for field in canonical["required"] if field not in _DETERMINISTIC_LABEL_FIELDS
    ]
    result = {}
    for field in semantic_names:
        if field not in {"discourse_events", "concept_candidates"}:
            result[field] = copy.deepcopy(label[field])
            continue
        result[field] = [
            {
                name: copy.deepcopy(value)
                for name, value in item.items()
                if name not in _EVIDENCE_FIELDS
            }
            for item in label[field]
        ]
    return result


def _validate_coverage_receipts(
    raw_segment: Mapping[str, Any], source_segment: Mapping[str, Any]
) -> None:
    expected_unit_ids = [unit["unit_id"] for unit in source_segment["units"]]
    receipts = raw_segment["unit_receipts"]
    observed_unit_ids = [receipt["unit_id"] for receipt in receipts]
    if observed_unit_ids != expected_unit_ids:
        raise CanonicalV31OutputError("unit receipts must cover each segment unit once in source order")
    event_owners = Counter(
        event["evidence_start_unit_id"] for event in raw_segment["discourse_events"]
    )
    candidate_owners = Counter(
        item["evidence_start_unit_id"] for item in raw_segment["concept_candidates"]
    )
    for receipt in receipts:
        unit_id = receipt["unit_id"]
        if (
            receipt["grounded_event_count"] != event_owners[unit_id]
            or receipt["grounded_concept_candidate_count"] != candidate_owners[unit_id]
        ):
            raise CanonicalV31OutputError("unit coverage receipt counts do not reconcile")
    if sum(event_owners.values()) != len(raw_segment["discourse_events"]):
        raise CanonicalV31OutputError("event ownership accounting is incomplete")
    if sum(candidate_owners.values()) != len(raw_segment["concept_candidates"]):
        raise CanonicalV31OutputError("candidate ownership accounting is incomplete")


def validate_and_project_output(
    request: Mapping[str, Any], output: Mapping[str, Any]
) -> dict[str, Any]:
    """Project exact evidence/IDs and reject any semantic or accounting drift."""

    validated = validate_prepared_request(request)
    if not isinstance(output, Mapping):
        raise CanonicalV31OutputError("completed output must be an object")
    untouched = copy.deepcopy(dict(output))
    _validate_schema(output, validated["output_schema"])
    source_segments = validated["private_input"]["segments"]
    raw_segments = output["segments"]
    expected_ids = [segment["segment_id"] for segment in source_segments]
    observed_ids = [segment["segment_id"] for segment in raw_segments]
    if observed_ids != expected_ids:
        raise CanonicalV31OutputError("output segments must preserve exact source order and identity")
    canonical = canonical_label_schema()
    event_schema = canonical["properties"]["discourse_events"]["items"]
    candidate_schema = canonical["properties"]["concept_candidates"]["items"]
    semantic_names = [
        field for field in canonical["required"] if field not in _DETERMINISTIC_LABEL_FIELDS
    ]
    labels: list[dict[str, Any]] = []
    provenance_segments: list[dict[str, Any]] = []
    raw_views = []
    projected_views = []
    for segment_index, (raw_segment, source_segment) in enumerate(zip(raw_segments, source_segments)):
        _validate_coverage_receipts(raw_segment, source_segment)
        status = raw_segment["extraction_status"]
        event_count = len(raw_segment["discourse_events"])
        if (status == "coded") != (event_count > 0):
            raise CanonicalV31OutputError(
                f"$.segments[{segment_index}] must be coded iff grounded events remain"
            )
        events, event_provenance = _project_owned_items(
            raw_segment["discourse_events"],
            event_schema,
            source_segment,
            collection_name="discourse_events",
        )
        candidates, candidate_provenance = _project_owned_items(
            raw_segment["concept_candidates"],
            candidate_schema,
            source_segment,
            collection_name="concept_candidates",
        )
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
            validate_label_output(
                CANONICAL_LABEL_PACK,
                label,
                segment_text=source_segment["segment_text"],
            )
        except ValidationError as exc:
            raise CanonicalV31OutputError(
                f"segment {source_segment['segment_id']} failed canonical v3.1 validation"
            ) from exc
        raw_view = _semantic_view_from_raw(raw_segment)
        projected_view = _semantic_view_from_label(label)
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
                "segment_text_sha256": sha256_text(source_segment["segment_text"]),
                "segment_quality_sha256": sha256_text(
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
    emitted_sha256 = sha256_text(_canonical_json(raw_views))
    projected_sha256 = sha256_text(_canonical_json(projected_views))
    if emitted_sha256 != projected_sha256:
        raise CanonicalV31OutputError("semantic fidelity hash differs after projection")
    provenance = {
        "schema_version": PROVENANCE_SCHEMA_VERSION,
        "candidate_system_id": CANDIDATE_SYSTEM_ID,
        "batch_id": validated["batch_id"],
        "episode_id": validated["episode_id"],
        "segments": provenance_segments,
        "unit_ids_removed_from_canonical_labels": True,
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


def build_six_arm_matrix_binding() -> dict[str, Any]:
    """Return the exact semantic/runtime contract every corrected arm must bind."""

    overlay = verified_context_control_overlay()
    instruction_sources = expected_instruction_source_contract()
    final_fields = canonical_final_field_paths()
    model_fields = model_semantic_field_paths()
    provenance_fields = deterministic_provenance_field_paths()
    ownership_fields = model_evidence_ownership_field_paths()
    arms = [
        {
            "batch_size": batch_size,
            "thread_mode": thread_mode,
            "cache_regime": (
                "cold_new_thread_per_batch"
                if thread_mode == "new_thread"
                else "warm_same_episode_thread_across_batches"
            ),
            "candidate_system_id": CANDIDATE_SYSTEM_ID,
            "adapter_entrypoint": (
                "research_factory.app_server_canonical_v31_episode_batch:"
                "run_episode_batch_arm"
            ),
        }
        for batch_size in SUPPORTED_BATCH_SIZES
        for thread_mode in SUPPORTED_THREAD_MODES
    ]
    return {
        "schema_version": MATRIX_BINDING_VERSION,
        "candidate_system_id": CANDIDATE_SYSTEM_ID,
        "evaluation_role": "next_development_full_canonical_cold_warm_matrix",
        "model": MODEL,
        "effort": EFFORT,
        "canonical_label_pack": CANONICAL_LABEL_PACK,
        "canonical_label_schema": _record(LABEL_SCHEMA_PATH),
        "adapter_module": _record(Path(__file__).resolve()),
        "canonical_validator_module": _record(Path(labels_module.__file__).resolve()),
        "app_server_transport_module": _record(Path(codex_app_server.__file__).resolve()),
        "app_server_protocol_schema": _record(codex_app_server.PROTOCOL_SCHEMA_PATH),
        "pinned_codex_cli": _record(PINNED_CODEX),
        "pinned_codex_cli_version": codex_app_server.PINNED_CODEX_CLI_VERSION,
        "canonical_schema_strategy": CANONICAL_SCHEMA_STRATEGY,
        "canonical_final_field_paths": final_fields,
        "canonical_final_field_paths_sha256": sha256_text(
            _canonical_json(final_fields)
        ),
        "model_semantic_field_paths": model_fields,
        "model_semantic_field_paths_sha256": sha256_text(
            _canonical_json(model_fields)
        ),
        "deterministic_provenance_field_paths": provenance_fields,
        "deterministic_provenance_field_paths_sha256": sha256_text(
            _canonical_json(provenance_fields)
        ),
        "model_evidence_ownership_field_paths": ownership_fields,
        "model_evidence_ownership_field_paths_sha256": sha256_text(
            _canonical_json(ownership_fields)
        ),
        "base_instructions_template_sha256": sha256_text(BASE_INSTRUCTIONS_TEMPLATE),
        "context_control_overlay": overlay,
        "context_control_overlay_sha256": sha256_text(_canonical_json(overlay)),
        "instruction_source_contract": instruction_sources,
        "instruction_source_contract_sha256": sha256_text(
            _canonical_json(instruction_sources)
        ),
        "project_instruction_content_byte_budget": 0,
        "managed_chatgpt_auth_only": True,
        "managed_chatgpt_plan_type": "pro",
        "official_persistent_codex_app_server_only": True,
        "structured_output_required": True,
        "complete_sidecar_usage_required": True,
        "retry_count": RETRY_COUNT,
        "semantic_integrity": semantic_integrity_contract(),
        "required_batch_sizes": list(SUPPORTED_BATCH_SIZES),
        "required_thread_modes": list(SUPPORTED_THREAD_MODES),
        "arms": arms,
        "arm_count": len(arms),
        "epoch6_artifact_policy": (
            "immutable_exploratory_evidence_only_never_runtime_prompt_schema_or_"
            "semantic_surface_lineage"
        ),
        "epoch6_artifact_records_must_remain_checksum_bound_in_evaluation_receipt": True,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }


class CanonicalV31CodexAppServerClient(codex_app_server.CodexAppServerClient):
    """Official persistent app-server client with the zero-byte context overlay."""

    def __init__(self, *, config_overlay: Mapping[str, Any], **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._canonical_v31_config_overlay = copy.deepcopy(dict(config_overlay))

    async def _request(self, method: str, params: dict[str, Any]) -> Any:
        request_params = copy.deepcopy(params)
        if method == "thread/start":
            if "config" in request_params:
                raise CanonicalV31EpisodeBatchError("thread/start config overlay supplied twice")
            request_params["config"] = copy.deepcopy(self._canonical_v31_config_overlay)
            request_params["personality"] = "none"
            request_params["environments"] = []
            request_params["dynamicTools"] = []
        elif method == "turn/start":
            request_params["summary"] = "none"
        result = await super()._request(method, request_params)
        if method == "thread/start":
            sources = result.get("instructionSources") if isinstance(result, Mapping) else None
            if sources != list(EXPECTED_INSTRUCTION_SOURCE_PATHS):
                raise codex_app_server.AppServerProtocolError(
                    "thread/start effective instruction-source paths drifted"
                )
        return result


def _client_factory() -> CanonicalV31CodexAppServerClient:
    if not PINNED_CODEX.is_file():
        raise CanonicalV31EpisodeBatchError("pinned official Codex app-server binary is absent")
    return CanonicalV31CodexAppServerClient(
        config_overlay=verified_context_control_overlay(),
        command=[str(PINNED_CODEX), "app-server", "--stdio", "--strict-config"],
    )


def _batch_paths(root: Path, batch_id: str) -> dict[str, Path]:
    batch_root = root / "batches" / batch_id
    return {
        "root": batch_root,
        "input": batch_root / "input.private.json",
        "prompt": batch_root / "prompt.private.md",
        "base": batch_root / "base-instructions.private.md",
        "schema": batch_root / "schema.json",
        "attempt": batch_root / "semantic-call-attempt.json",
        "sidecar": batch_root / "sidecar.json",
        "output": batch_root / "output.private.json",
        "labels": batch_root / "canonical-labels.private.json",
        "provenance": batch_root / "evidence-provenance.private.json",
        "fidelity": batch_root / "semantic-fidelity.json",
        "result": batch_root / "result.json",
    }


def _write_prepared_request(root: Path, request: Mapping[str, Any]) -> dict[str, Path]:
    validated = validate_prepared_request(request)
    paths = _batch_paths(root, validated["batch_id"])
    _write_immutable_json(paths["input"], validated["private_input"])
    _write_immutable_text(paths["prompt"], validated["prompt"])
    _write_immutable_text(paths["base"], validated["base_instructions"])
    _write_immutable_json(paths["schema"], validated["output_schema"])
    return paths


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
        raise CanonicalV31TelemetryError("started thread context-isolation contract failed")
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
        "context_control_overlay_sha256": request["context_control_overlay_sha256"],
    }


def _write_attempt_receipt(
    request: Mapping[str, Any], paths: Mapping[str, Path], *, thread: Any
) -> dict[str, Any]:
    preflight = _verify_started_thread(thread, request)
    payload = {
        "schema_version": ATTEMPT_RECEIPT_VERSION,
        "created_at": now_iso(),
        "state": "semantic_call_dispatch_committed",
        "candidate_system_id": CANDIDATE_SYSTEM_ID,
        "batch_id": request["batch_id"],
        "episode_id": request["episode_id"],
        "thread_mode": request["thread_mode"],
        "thread_id": thread.thread_id,
        "retry_count": RETRY_COUNT,
        "preflight": preflight,
    }
    _write_immutable_json(paths["attempt"], payload)
    return payload


def _output_message_hash(path: Path) -> str:
    value = path.read_text(encoding="utf-8")
    message = value[:-1] if value.endswith("\n") else value
    return sha256_text(message)


def validate_turn_sidecar(
    request: Mapping[str, Any],
    sidecar_path: Path,
    *,
    output_path: Path,
    expected_thread: Any,
) -> dict[str, Any]:
    """Validate complete managed-auth usage/cache/reasoning/wall accounting."""

    validated = validate_prepared_request(request)
    sidecar = _load_json(sidecar_path, "canonical v3.1 turn sidecar")
    if not isinstance(sidecar, Mapping):
        raise CanonicalV31TelemetryError("turn sidecar is not an object")
    thread_preflight = _verify_started_thread(expected_thread, validated)
    if (
        sidecar.get("schema_version") != codex_app_server.TURN_SIDECAR_SCHEMA_VERSION
        or not _is_iso_timestamp(sidecar.get("started_at"))
        or not _is_iso_timestamp(sidecar.get("finished_at"))
        or sidecar.get("client_version") != codex_app_server.APP_SERVER_CLIENT_VERSION
        or sidecar.get("cli_version") != codex_app_server.PINNED_CODEX_CLI_VERSION
        or sidecar.get("protocol_schema_sha256")
        != _sha256_file(codex_app_server.PROTOCOL_SCHEMA_PATH)
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
        or sidecar.get("output_schema_sha256") != validated["output_schema_sha256"]
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
        raise CanonicalV31TelemetryError("managed-auth completed sidecar contract failed")
    usage = _usage_values(sidecar.get("usage"), "usage")
    total = _usage_values(sidecar.get("thread_total_usage"), "thread_total_usage")
    if any(total[field] < usage[field] for field in USAGE_FIELDS):
        raise CanonicalV31TelemetryError("thread total usage is below turn usage")
    if validated["thread_mode"] == "new_thread" and total != usage:
        raise CanonicalV31TelemetryError("new-thread total usage differs from turn usage")
    wall = _nonnegative_number(sidecar.get("wall_elapsed_seconds"), "wall_elapsed_seconds")
    if (
        not output_path.is_file()
        or sidecar.get("output_sha256") != _output_message_hash(output_path)
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


async def _execute_batch(
    client: Any,
    request: Mapping[str, Any],
    paths: Mapping[str, Path],
    *,
    thread: Any,
    timeout_seconds: float,
) -> dict[str, Any]:
    try:
        _write_attempt_receipt(request, paths, thread=thread)
        result = await client.run_structured_turn(
            thread=thread,
            effort=EFFORT,
            prompt=request["prompt"],
            output_schema=request["output_schema"],
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
            expected_thread=thread,
        )
        if (
            getattr(result, "thread_id", None) != thread.thread_id
            or getattr(result, "turn_id", None) != telemetry["turn_id"]
        ):
            raise CanonicalV31TelemetryError("thread, result, and sidecar lifecycle IDs differ")
        raw_text = paths["output"].read_text(encoding="utf-8")
        raw_message = raw_text[:-1] if raw_text.endswith("\n") else raw_text
        raw_output = json.loads(raw_message)
        if (
            not isinstance(raw_output, Mapping)
            or _canonical_json(raw_output) != _canonical_json(result.output)
            or sha256_text(raw_message) != telemetry["sidecar"]["output_sha256"]
        ):
            raise CanonicalV31TelemetryError("in-memory output differs from sidecar-bound bytes")
        projected = validate_and_project_output(request, raw_output)
        _write_immutable_json(paths["labels"], projected["labels"])
        _write_immutable_json(paths["provenance"], projected["provenance"])
        _write_immutable_json(paths["fidelity"], projected["fidelity"])
        return {
            "batch_id": request["batch_id"],
            "episode_id": request["episode_id"],
            "state": "validated",
            "failure_class": None,
            "thread_reusable": True,
            "thread_id": telemetry["thread_id"],
            "turn_id": telemetry["turn_id"],
            "usage": telemetry["usage"],
            "wall_elapsed_seconds": telemetry["wall_elapsed_seconds"],
            "emitted_event_count": projected["fidelity"]["emitted_event_count"],
            "emitted_concept_candidate_count": projected["fidelity"][
                "emitted_concept_candidate_count"
            ],
            "records": {
                "attempt": _record(paths["attempt"]),
                "sidecar": _record(paths["sidecar"]),
                "raw_output": _record(paths["output"]),
                "canonical_labels": _record(paths["labels"]),
                "evidence_provenance": _record(paths["provenance"]),
                "semantic_fidelity": _record(paths["fidelity"]),
            },
        }
    except CanonicalV31OutputError as exc:
        return {
            "batch_id": request["batch_id"],
            "episode_id": request["episode_id"],
            "state": "rejected",
            "failure_class": type(exc).__name__,
            "failure_detail_sha256": sha256_text(str(exc)),
            "thread_reusable": True,
            "thread_id": thread.thread_id,
        }
    except (
        CanonicalV31TelemetryError,
        codex_app_server.AppServerError,
        asyncio.TimeoutError,
        OSError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as exc:
        return {
            "batch_id": request["batch_id"],
            "episode_id": request["episode_id"],
            "state": "failed",
            "failure_class": type(exc).__name__,
            "thread_reusable": False,
            "thread_id": thread.thread_id,
        }


def _lifecycle_receipt(
    requests: Sequence[Mapping[str, Any]], results: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    turn_ids = [result.get("turn_id") for result in results if result.get("turn_id")]
    thread_ids = [result.get("thread_id") for result in results if result.get("thread_id")]
    turn_ids_unique = len(turn_ids) == len(results) == len(set(turn_ids))
    mode = requests[0]["thread_mode"]
    if mode == "new_thread":
        thread_lineage_valid = len(thread_ids) == len(results) == len(set(thread_ids))
    else:
        episode_threads: dict[str, set[str]] = {}
        for request, result in zip(requests, results):
            if result.get("thread_id"):
                episode_threads.setdefault(request["episode_id"], set()).add(result["thread_id"])
        one_per_episode = all(len(values) == 1 for values in episode_threads.values())
        selected = [next(iter(values)) for values in episode_threads.values() if len(values) == 1]
        thread_lineage_valid = (
            set(episode_threads) == {request["episode_id"] for request in requests}
            and one_per_episode
            and len(selected) == len(set(selected))
        )
    return {
        "turn_ids_unique": turn_ids_unique,
        "thread_lineage_valid": thread_lineage_valid,
        "observed_thread_count": len(set(thread_ids)),
        "observed_turn_count": len(set(turn_ids)),
    }


async def run_episode_batch_arm(
    episodes: Sequence[Mapping[str, Any]],
    *,
    output_dir: Path,
    batch_size: int,
    thread_mode: str,
    timeout_seconds: float = 900.0,
    client_factory: Callable[[], Any] = _client_factory,
) -> dict[str, Any]:
    """Run one corrected matrix arm through one persistent managed app-server.

    The output root is single-use.  No failed or ambiguous attempt is retried.
    In ``same_thread`` mode each episode owns exactly one ephemeral thread and
    its batches run sequentially on that thread.  In ``new_thread`` mode every
    prepared batch owns a distinct ephemeral thread.
    """

    _validate_arm_choice(batch_size, thread_mode)
    if not episodes:
        raise CanonicalV31EpisodeBatchError("episode arm is empty")
    root = output_dir.expanduser().resolve()
    if root.exists() and any(root.iterdir()):
        raise CanonicalV31EpisodeBatchError("arm output exists; ambiguous replay is forbidden")
    root.mkdir(parents=True, exist_ok=True)
    matrix_binding = build_six_arm_matrix_binding()
    _write_immutable_json(root / "matrix-binding.json", matrix_binding)
    requests: list[dict[str, Any]] = []
    episode_ids = []
    all_segment_ids = []
    for episode in episodes:
        episode_id = _nonempty_identifier(episode.get("episode_id"), "episode_id")
        episode_ids.append(episode_id)
        episode_requests = prepare_episode_batches(
            episode, batch_size=batch_size, thread_mode=thread_mode
        )
        requests.extend(episode_requests)
        all_segment_ids.extend(
            segment_id for request in episode_requests for segment_id in request["segment_ids"]
        )
    if len(episode_ids) != len(set(episode_ids)):
        raise CanonicalV31EpisodeBatchError("episode IDs are not unique")
    if len(all_segment_ids) != len(set(all_segment_ids)):
        raise CanonicalV31EpisodeBatchError("segment IDs are not globally unique")
    if len({request["batch_id"] for request in requests}) != len(requests):
        raise CanonicalV31EpisodeBatchError("batch IDs are not unique")
    paths_by_batch = {
        request["batch_id"]: _write_prepared_request(root, request) for request in requests
    }
    run_spec = {
        "schema_version": ADAPTER_SCHEMA_VERSION,
        "created_at": now_iso(),
        "candidate_system_id": CANDIDATE_SYSTEM_ID,
        "batch_size": batch_size,
        "thread_mode": thread_mode,
        "episode_count": len(episode_ids),
        "segment_count": len(all_segment_ids),
        "batch_count": len(requests),
        "effective_batch_sizes": dict(
            sorted(Counter(request["effective_batch_size"] for request in requests).items())
        ),
        "model": MODEL,
        "effort": EFFORT,
        "managed_chatgpt_auth_only": True,
        "managed_chatgpt_plan_type": "pro",
        "official_persistent_codex_app_server_only": True,
        "app_server_process_count": 1,
        "retry_count": RETRY_COUNT,
        "semantic_postprocessing": False,
        "context_control_overlay_sha256": sha256_text(
            _canonical_json(verified_context_control_overlay())
        ),
        "instruction_source_contract": expected_instruction_source_contract(),
        "instruction_source_contract_sha256": sha256_text(
            _canonical_json(expected_instruction_source_contract())
        ),
        "project_instruction_content_byte_budget": 0,
        "production_mutation_allowed": False,
    }
    _write_immutable_json(root / "run-spec.json", run_spec)
    wall_started = time.monotonic()
    results: list[dict[str, Any]] = []
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
        if thread_mode == "new_thread":
            for request in requests:
                try:
                    thread = await client.start_thread(
                        model=MODEL,
                        base_instructions=request["base_instructions"],
                        cwd=PROJECT_ROOT,
                        ephemeral=True,
                    )
                    _verify_started_thread(thread, request)
                except (codex_app_server.AppServerError, CanonicalV31TelemetryError) as exc:
                    results.append(
                        {
                            "batch_id": request["batch_id"],
                            "episode_id": request["episode_id"],
                            "state": "failed",
                            "failure_class": type(exc).__name__,
                            "thread_reusable": False,
                        }
                    )
                    continue
                results.append(
                    await _execute_batch(
                        client,
                        request,
                        paths_by_batch[request["batch_id"]],
                        thread=thread,
                        timeout_seconds=timeout_seconds,
                    )
                )
        else:
            by_episode: dict[str, list[Mapping[str, Any]]] = {}
            for request in requests:
                by_episode.setdefault(request["episode_id"], []).append(request)
            for episode_requests in by_episode.values():
                first = episode_requests[0]
                try:
                    thread = await client.start_thread(
                        model=MODEL,
                        base_instructions=first["base_instructions"],
                        cwd=PROJECT_ROOT,
                        ephemeral=True,
                    )
                    _verify_started_thread(thread, first)
                except (codex_app_server.AppServerError, CanonicalV31TelemetryError) as exc:
                    results.extend(
                        {
                            "batch_id": request["batch_id"],
                            "episode_id": request["episode_id"],
                            "state": "failed",
                            "failure_class": type(exc).__name__,
                            "thread_reusable": False,
                        }
                        for request in episode_requests
                    )
                    continue
                reusable = True
                for request in episode_requests:
                    if not reusable:
                        results.append(
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
                        paths_by_batch[request["batch_id"]],
                        thread=thread,
                        timeout_seconds=timeout_seconds,
                    )
                    results.append(result)
                    reusable = bool(result["thread_reusable"])
    for result in results:
        _write_immutable_json(paths_by_batch[result["batch_id"]]["result"], result)
    lifecycle = _lifecycle_receipt(requests, results)
    complete = len(results) == len(requests) and all(result["state"] == "validated" for result in results)
    usage_complete = complete and all(isinstance(result.get("usage"), Mapping) for result in results)
    usage = {
        field: sum(int(result["usage"][field]) for result in results)
        for field in USAGE_FIELDS
    } if usage_complete else None
    report = {
        "schema_version": RUN_REPORT_VERSION,
        "state": "passed" if complete and all(lifecycle.values()) else "not_eligible",
        "candidate_system_id": CANDIDATE_SYSTEM_ID,
        "batch_size": batch_size,
        "thread_mode": thread_mode,
        "model": MODEL,
        "effort": EFFORT,
        "requested_calls": len(requests),
        "attempted_calls": sum(
            paths_by_batch[request["batch_id"]]["attempt"].is_file() for request in requests
        ),
        "validated_calls": sum(result["state"] == "validated" for result in results),
        "rejected_calls": sum(result["state"] == "rejected" for result in results),
        "failed_calls": sum(result["state"] in {"failed", "not_started"} for result in results),
        "usage_status": "complete" if usage_complete else "unknown_or_incomplete",
        "usage": usage,
        "cached_input_tokens": usage["cached_input_tokens"] if usage else None,
        "reasoning_output_tokens": usage["reasoning_output_tokens"] if usage else None,
        "wall_elapsed_seconds": round(time.monotonic() - wall_started, 3),
        "retry_count": RETRY_COUNT,
        "ambiguous_retry_count": 0,
        "semantic_postprocessing": False,
        "all_emitted_semantic_values_preserved": complete,
        "emitted_event_count": sum(int(result.get("emitted_event_count") or 0) for result in results),
        "emitted_concept_candidate_count": sum(
            int(result.get("emitted_concept_candidate_count") or 0) for result in results
        ),
        **lifecycle,
        "managed_chatgpt_auth_verified": True,
        "managed_chatgpt_plan_type": "pro",
        "instruction_source_contract": expected_instruction_source_contract(),
        "instruction_source_contract_sha256": sha256_text(
            _canonical_json(expected_instruction_source_contract())
        ),
        "project_instruction_content_byte_budget": 0,
        "production_mutated": False,
        "holdout_authorized": False,
        "results": results,
    }
    _write_immutable_json(root / "report.json", report)
    return report
