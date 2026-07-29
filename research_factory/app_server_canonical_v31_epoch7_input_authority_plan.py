from __future__ import annotations

import argparse
import copy
import hashlib
import json
import math
import os
import sqlite3
import stat
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_canonical_v31_episode_batch as adapter
from . import app_server_canonical_v31_epoch7_capacity_readiness as capacity_readiness
from . import app_server_canonical_v31_epoch7_input_package as input_package
from . import app_server_expanded_cap_development_matrix as legacy_matrix
from .labels import ValidationError, validate_label_output


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ROOT = (
    PROJECT_ROOT
    / "work"
    / "app-server-development-v2"
    / "unattended-pipeline-v5"
    / "canonical-v31-epoch7-input-authority-plan-v1"
)
DEFAULT_DATABASE_PATH = PROJECT_ROOT / "data" / "factory.sqlite"
DEFAULT_INPUT_ROOT = input_package.DEFAULT_ROOT
DEFAULT_CAPACITY_ROOT = capacity_readiness.DEFAULT_ROOT

PLAN_VERSION = "pif_canonical_v31_epoch7_input_authority_plan_v1"
RECEIPT_VERSION = "pif_canonical_v31_epoch7_input_authority_plan_receipt_v1"
TURN_VERSION = "pif_canonical_v31_epoch7_input_authority_turn_v1"
OUTPUT_VERSION = "pif_canonical_v31_epoch7_input_authority_output_v1"
CAPACITY_VERSION = "pif_canonical_v31_epoch7_input_authority_capacity_policy_v1"
RUNTIME_LOCK_VERSION = "pif_canonical_v31_epoch7_input_authority_runtime_lock_v1"

PLAN_FILENAME = "authority-plan.json"
CAPACITY_FILENAME = "authority-capacity-policy.json"
RUNTIME_LOCK_FILENAME = "runtime-lock.json"
RECEIPT_FILENAME = "authority-plan-receipt.json"
TERMINAL_FILENAME = "terminal.json"
TURNS_DIRECTORY = "turns"

MODEL = "gpt-5.6-sol"
EFFORT = "high"
EXACT_TURN_COUNT = 4
MAXIMUM_TOTAL_TOKENS_PER_TURN = 102_000
MAXIMUM_WALL_SECONDS_PER_TURN = 714
OPERATOR_WALL_SAFETY_MARGIN_SECONDS = 60
CAPACITY_SAFETY_MARGIN_PERCENT = 1

BASE_INSTRUCTIONS = """You are the sole LLM authority for a private development-only canonical reference repair. This is not production extraction and not holdout work.

For the one supplied episode, author exactly two things:
1. excluded_source_context: the episode-level source categories or regions that future extraction should exclude because they are non-substantive framing, setup, advertising, credits, navigation, or other non-research context. Base every entry on the complete checksum-bound episode source. Return an empty list only when the source supports that conclusion.
2. repaired_reference_labels: one complete ai_discourse_v3_1 label for each listed invalid development reference, in the exact supplied segment order.

For repaired labels, understand the source semantically. Preserve a prior claim only when the source supports it, and correct or remove unsupported semantics. Every evidence string and every nonempty metric raw_text, value, unit, and comparator must be an exact contiguous substring of that event's evidence. Do not use keyword, regex, topic-list, majority-vote, or deterministic semantic rules. Do not copy a malformed metric merely to preserve the old label. Do not alter or reproduce references listed as already valid; deterministic code preserves those exact bytes.

Return only schema-valid JSON. Never reproduce the full episode transcript in the response.
"""


class CanonicalV31Epoch7InputAuthorityPlanError(RuntimeError):
    pass


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _pretty_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True) + "\n"


def _is_sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _safe_path(
    path: Path,
    *,
    project_root: Path,
    label: str,
    require_file: bool = False,
) -> Path:
    project = project_root.expanduser().resolve()
    candidate = Path(os.path.abspath(os.path.expanduser(str(path))))
    try:
        candidate.relative_to(project)
    except ValueError as exc:
        raise CanonicalV31Epoch7InputAuthorityPlanError(
            f"{label} is outside the project root"
        ) from exc
    cursor = candidate
    while cursor != project:
        try:
            mode = cursor.lstat().st_mode
        except FileNotFoundError:
            mode = None
        except OSError as exc:
            raise CanonicalV31Epoch7InputAuthorityPlanError(
                f"{label} metadata is unavailable"
            ) from exc
        if mode is not None and stat.S_ISLNK(mode):
            raise CanonicalV31Epoch7InputAuthorityPlanError(
                f"{label} traverses a symlink"
            )
        cursor = cursor.parent
    if require_file:
        try:
            mode = candidate.lstat().st_mode
        except OSError as exc:
            raise CanonicalV31Epoch7InputAuthorityPlanError(
                f"{label} is unavailable"
            ) from exc
        if not stat.S_ISREG(mode):
            raise CanonicalV31Epoch7InputAuthorityPlanError(
                f"{label} is not a regular file"
            )
    return candidate


def _record(path: Path, *, project_root: Path) -> dict[str, Any]:
    resolved = _safe_path(
        path,
        project_root=project_root,
        label="artifact record",
        require_file=True,
    )
    payload = resolved.read_bytes()
    return {
        "path": str(resolved),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "size_bytes": len(payload),
    }


def _verify_record(
    value: Any,
    *,
    label: str,
    project_root: Path,
) -> Path:
    if not isinstance(value, Mapping) or set(value) != {
        "path",
        "sha256",
        "size_bytes",
    }:
        raise CanonicalV31Epoch7InputAuthorityPlanError(f"{label} record is malformed")
    path_value = value.get("path")
    size = value.get("size_bytes")
    if (
        not isinstance(path_value, str)
        or not _is_sha256(value.get("sha256"))
        or isinstance(size, bool)
        or not isinstance(size, int)
        or size < 0
    ):
        raise CanonicalV31Epoch7InputAuthorityPlanError(
            f"{label} record fields drifted"
        )
    path = Path(path_value).expanduser().resolve()
    if _record(path, project_root=project_root) != dict(value):
        raise CanonicalV31Epoch7InputAuthorityPlanError(f"{label} record drifted")
    return path


def _load_json(path: Path, *, label: str) -> Any:
    try:
        return json.loads(path.read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CanonicalV31Epoch7InputAuthorityPlanError(f"{label} is malformed") from exc


def _load_object(path: Path, *, label: str) -> dict[str, Any]:
    value = _load_json(path, label=label)
    if not isinstance(value, dict):
        raise CanonicalV31Epoch7InputAuthorityPlanError(f"{label} is not an object")
    return value


def _write_bytes(path: Path, payload: bytes, *, project_root: Path) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags, 0o600)
    except FileExistsError as exc:
        raise CanonicalV31Epoch7InputAuthorityPlanError(
            f"immutable artifact already exists: {path}"
        ) from exc
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        try:
            path.unlink()
        except OSError:
            pass
        raise
    return _record(path, project_root=project_root)


def _write_json(path: Path, value: Any, *, project_root: Path) -> dict[str, Any]:
    return _write_bytes(path, _pretty_json(value).encode("ascii"), project_root=project_root)


def _open_read_only_database(path: Path) -> sqlite3.Connection:
    resolved = path.expanduser().resolve()
    try:
        connection = sqlite3.connect(f"{resolved.as_uri()}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only = ON")
        if int(connection.execute("PRAGMA query_only").fetchone()[0]) != 1:
            raise CanonicalV31Epoch7InputAuthorityPlanError(
                "authority source query_only enforcement failed"
            )
        return connection
    except CanonicalV31Epoch7InputAuthorityPlanError:
        raise
    except sqlite3.Error as exc:
        raise CanonicalV31Epoch7InputAuthorityPlanError(
            "authority source database is unavailable read-only"
        ) from exc


def _output_schema(
    *, episode_id: str, invalid_segment_ids: Sequence[str]
) -> dict[str, Any]:
    label_schema = copy.deepcopy(adapter.canonical_label_schema())
    label_schema["properties"]["episode_id"] = {
        "type": "string",
        "const": episode_id,
    }
    label_schema["properties"]["segment_id"] = {
        "type": "string",
        "enum": list(invalid_segment_ids),
    }
    return {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "schema_version",
            "episode_id",
            "excluded_source_context",
            "repaired_reference_labels",
        ],
        "properties": {
            "schema_version": {"type": "string", "const": OUTPUT_VERSION},
            "episode_id": {"type": "string", "const": episode_id},
            "excluded_source_context": {
                "type": "array",
                "items": {"type": "string", "minLength": 1},
            },
            "repaired_reference_labels": {
                "type": "array",
                "minItems": len(invalid_segment_ids),
                "maxItems": len(invalid_segment_ids),
                "items": label_schema,
            },
        },
    }


def _turn_id(episode_id: str, invalid_segment_ids: Sequence[str]) -> str:
    identity = {
        "schema_version": TURN_VERSION,
        "episode_id": episode_id,
        "invalid_segment_ids": list(invalid_segment_ids),
    }
    return "authority_" + hashlib.sha256(
        _canonical_json(identity).encode("ascii")
    ).hexdigest()[:24]


def _build_turn(
    *,
    manifest_episode: Mapping[str, Any],
    prepared_episode: Mapping[str, Any],
    full_source: Mapping[str, Any],
    legacy_references: Mapping[str, Any],
    reference_diagnostics: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    episode_id = str(prepared_episode.get("episode_id") or "")
    invalid_segment_ids = [
        str(row["segment_id"])
        for row in reference_diagnostics
        if row.get("episode_id") == episode_id
        and row.get("canonical_v31_valid") is not True
    ]
    if not episode_id or not invalid_segment_ids:
        raise CanonicalV31Epoch7InputAuthorityPlanError(
            "authority episode has no exact invalid reference partition"
        )
    segments_by_id = {
        str(segment["segment_id"]): segment
        for segment in prepared_episode.get("segments") or []
        if isinstance(segment, Mapping)
    }
    repairs: list[dict[str, Any]] = []
    for segment_id in invalid_segment_ids:
        segment = segments_by_id.get(segment_id)
        reference = legacy_references.get(segment_id)
        label = reference.get("golden_output") if isinstance(reference, Mapping) else None
        if not isinstance(segment, Mapping) or not isinstance(label, Mapping):
            raise CanonicalV31Epoch7InputAuthorityPlanError(
                "authority repair source is incomplete"
            )
        repairs.append(
            {
                "segment_id": segment_id,
                "segment_text": segment["segment_text"],
                "segment_text_sha256": hashlib.sha256(
                    str(segment["segment_text"]).encode("utf-8")
                ).hexdigest(),
                "legacy_reference_label": copy.deepcopy(dict(label)),
                "legacy_reference_label_sha256": hashlib.sha256(
                    _canonical_json(label).encode("ascii")
                ).hexdigest(),
                "validation_diagnostic_path": next(
                    str(row.get("diagnostic_path") or "$")
                    for row in reference_diagnostics
                    if row.get("segment_id") == segment_id
                ),
            }
        )
    context = prepared_episode.get("episode_context")
    if not isinstance(context, Mapping) or "excluded_source_context" in context:
        raise CanonicalV31Epoch7InputAuthorityPlanError(
            "authority context gap is no longer exactly one absent field"
        )
    valid_segment_ids = [
        str(row["segment_id"])
        for row in reference_diagnostics
        if row.get("episode_id") == episode_id
        and row.get("canonical_v31_valid") is True
    ]
    private_input = {
        "schema_version": TURN_VERSION,
        "episode_id": episode_id,
        "episode_metadata": copy.deepcopy(full_source["episode"]),
        "full_segmented_episode_text": full_source["full_segmented_episode_text"],
        "full_source_binding": copy.deepcopy(full_source["source_binding"]),
        "original_context": copy.deepcopy(dict(context)),
        "original_context_sha256": hashlib.sha256(
            _canonical_json(context).encode("ascii")
        ).hexdigest(),
        "missing_context_fields": ["excluded_source_context"],
        "reference_repairs": repairs,
        "invalid_reference_segment_ids": invalid_segment_ids,
        "preserved_valid_reference_segment_ids": valid_segment_ids,
        "privacy": "private full transcript and development reference labels",
    }
    prompt = (
        "# Private checksum-bound authority packet\n"
        + _canonical_json(private_input)
        + "\n"
    )
    schema = _output_schema(
        episode_id=episode_id,
        invalid_segment_ids=invalid_segment_ids,
    )
    return {
        "turn_id": _turn_id(episode_id, invalid_segment_ids),
        "episode_id": episode_id,
        "invalid_reference_segment_ids": invalid_segment_ids,
        "preserved_valid_reference_segment_ids": valid_segment_ids,
        "input": private_input,
        "prompt": prompt,
        "base_instructions": BASE_INSTRUCTIONS,
        "output_schema": schema,
        "source_id": manifest_episode.get("source_id"),
    }


def validate_authority_output(
    value: Mapping[str, Any],
    *,
    turn_input: Mapping[str, Any],
    output_schema: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise CanonicalV31Epoch7InputAuthorityPlanError(
            "authority output is not an object"
        )
    try:
        adapter._validate_schema(dict(value), output_schema, "$")
    except adapter.CanonicalV31OutputError as exc:
        raise CanonicalV31Epoch7InputAuthorityPlanError(
            "authority output schema validation failed"
        ) from exc
    episode_id = str(turn_input.get("episode_id") or "")
    expected_ids = list(turn_input.get("invalid_reference_segment_ids") or [])
    labels = value.get("repaired_reference_labels")
    observed_ids = [row.get("segment_id") for row in labels]
    if observed_ids != expected_ids or len(observed_ids) != len(set(observed_ids)):
        raise CanonicalV31Epoch7InputAuthorityPlanError(
            "authority repaired reference order or membership drifted"
        )
    repairs_by_id = {
        str(row["segment_id"]): row for row in turn_input["reference_repairs"]
    }
    validated: list[dict[str, Any]] = []
    for label in labels:
        segment_id = str(label["segment_id"])
        source = repairs_by_id[segment_id]
        if (
            label.get("episode_id") != episode_id
            or label.get("segment_quality")
            != source["legacy_reference_label"].get("segment_quality")
        ):
            raise CanonicalV31Epoch7InputAuthorityPlanError(
                "authority label identity or deterministic quality provenance drifted"
            )
        try:
            validate_label_output(
                adapter.CANONICAL_LABEL_PACK,
                label,
                segment_text=source["segment_text"],
            )
        except ValidationError as exc:
            raise CanonicalV31Epoch7InputAuthorityPlanError(
                "authority label failed current canonical validation"
            ) from exc
        validated.append(copy.deepcopy(dict(label)))
    excluded = value.get("excluded_source_context")
    if not isinstance(excluded, list) or any(
        not isinstance(item, str) or not item for item in excluded
    ):
        raise CanonicalV31Epoch7InputAuthorityPlanError(
            "authority excluded-source context is malformed"
        )
    return {
        "schema_version": OUTPUT_VERSION,
        "episode_id": episode_id,
        "excluded_source_context": copy.deepcopy(excluded),
        "repaired_reference_labels": validated,
    }


def _capacity_policy(
    capacity_analysis: Mapping[str, Any], *, exact_turn_count: int
) -> dict[str, Any]:
    turn_bound = int(capacity_analysis["source_maximum_total_tokens_per_turn"])
    quota_rate = int(capacity_analysis["quota_points_per_million_tokens"])
    reserve = int(capacity_analysis["minimum_remaining_reserve_percent"])
    phase_bound = exact_turn_count * turn_bound
    projected_points = math.ceil(phase_bound * quota_rate / 1_000_000)
    maximum_usable = 100 - reserve - CAPACITY_SAFETY_MARGIN_PERCENT
    if projected_points > maximum_usable:
        raise CanonicalV31Epoch7InputAuthorityPlanError(
            "authority phase cannot fit measured reserve policy"
        )
    return {
        "schema_version": CAPACITY_VERSION,
        "state": "frozen_offline_no_probe_or_dispatch",
        "candidate_system_id": adapter.CANDIDATE_SYSTEM_ID,
        "model": MODEL,
        "effort": EFFORT,
        "exact_turn_count": exact_turn_count,
        "maximum_total_tokens_per_turn": turn_bound,
        "phase_total_token_bound": phase_bound,
        "quota_points_per_million_tokens": quota_rate,
        "projected_phase_quota_points": projected_points,
        "minimum_remaining_reserve_percent": reserve,
        "capacity_safety_margin_percent": CAPACITY_SAFETY_MARGIN_PERCENT,
        "maximum_possible_usable_quota_points": maximum_usable,
        "maximum_wall_seconds_per_turn": MAXIMUM_WALL_SECONDS_PER_TURN,
        "phase_wall_seconds_ceiling": exact_turn_count
        * MAXIMUM_WALL_SECONDS_PER_TURN,
        "operator_wall_safety_margin_seconds": (
            OPERATOR_WALL_SAFETY_MARGIN_SECONDS
        ),
        "official_persistent_codex_app_server_only": True,
        "managed_chatgpt_auth_only": True,
        "managed_chatgpt_plan_type": "pro",
        "fresh_no_turn_probe_required_before_each_turn": True,
        "preturn_reprobe_required": True,
        "postturn_measured_stop_required": True,
        "unknown_usage_hard_stop": True,
        "rate_limit_reached_type_must_be_null": True,
        "semantic_retry_count": 0,
        "token_capacity_is_estimate_not_reservation": True,
        "operator_authorization_present": False,
        "executable": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }


def _runtime_lock(project_root: Path) -> dict[str, Any]:
    binding = adapter.build_six_arm_matrix_binding()
    return {
        "schema_version": RUNTIME_LOCK_VERSION,
        "state": "frozen_zero_call",
        "authority_plan_module": _record(Path(__file__), project_root=project_root),
        "canonical_adapter_binding": copy.deepcopy(binding),
        "canonical_adapter_binding_sha256": hashlib.sha256(
            _canonical_json(binding).encode("ascii")
        ).hexdigest(),
        "model": MODEL,
        "effort": EFFORT,
        "official_persistent_codex_app_server_only": True,
        "managed_chatgpt_auth_only": True,
        "managed_chatgpt_plan_type": "pro",
        "semantic_retry_count": 0,
        "semantic_deterministic_defaults": {},
        "semantic_deterministic_pruning": False,
    }


def _verify_turn_source_lineage(
    turn_input: Mapping[str, Any],
    *,
    project_root: Path,
) -> None:
    expected_input_keys = {
        "schema_version",
        "episode_id",
        "episode_metadata",
        "full_segmented_episode_text",
        "full_source_binding",
        "original_context",
        "original_context_sha256",
        "missing_context_fields",
        "reference_repairs",
        "invalid_reference_segment_ids",
        "preserved_valid_reference_segment_ids",
        "privacy",
    }
    if set(turn_input) != expected_input_keys:
        raise CanonicalV31Epoch7InputAuthorityPlanError(
            "authority private input fields drifted"
        )
    episode_id = turn_input.get("episode_id")
    episode = turn_input.get("episode_metadata")
    full_text = turn_input.get("full_segmented_episode_text")
    source = turn_input.get("full_source_binding")
    context = turn_input.get("original_context")
    invalid_ids = turn_input.get("invalid_reference_segment_ids")
    preserved_ids = turn_input.get("preserved_valid_reference_segment_ids")
    repairs = turn_input.get("reference_repairs")
    if (
        turn_input.get("schema_version") != TURN_VERSION
        or not isinstance(episode_id, str)
        or not episode_id
        or not isinstance(episode, Mapping)
        or episode.get("episode_id") != episode_id
        or not isinstance(full_text, str)
        or not full_text
        or not isinstance(source, Mapping)
        or not isinstance(context, Mapping)
        or "excluded_source_context" in context
        or context.get("episode_id") != episode_id
        or any(
            field not in context
            or not input_package._context_value_valid(field, context[field])
            for field in input_package._CONTEXT_AUTHORITY_FIELDS
            if field != "excluded_source_context"
        )
        or turn_input.get("missing_context_fields") != ["excluded_source_context"]
        or not isinstance(invalid_ids, list)
        or not isinstance(preserved_ids, list)
        or not isinstance(repairs, list)
        or turn_input.get("privacy")
        != "private full transcript and development reference labels"
    ):
        raise CanonicalV31Epoch7InputAuthorityPlanError(
            "authority private input contract drifted"
        )
    if (
        turn_input.get("original_context_sha256")
        != hashlib.sha256(_canonical_json(context).encode("ascii")).hexdigest()
    ):
        raise CanonicalV31Epoch7InputAuthorityPlanError(
            "authority original context binding drifted"
        )

    expected_source_keys = {
        "canonical_transcript",
        "raw_transcript",
        "prepared_transcript",
        "full_segmented_episode_text_sha256",
        "full_segmented_episode_text_chars",
        "source_assembly_policy",
        "overlapping_segment_count",
        "within_prepared_text_bounds_count",
        "exact_prepared_text_projection_count",
        "normalized_projection_count",
        "segments",
    }
    if set(source) != expected_source_keys:
        raise CanonicalV31Epoch7InputAuthorityPlanError(
            "authority full-source binding fields drifted"
        )
    raw_path = _verify_record(
        source.get("raw_transcript"),
        label="authority raw transcript",
        project_root=project_root,
    )
    prepared_path = _verify_record(
        source.get("prepared_transcript"),
        label="authority prepared transcript",
        project_root=project_root,
    )
    canonical = source.get("canonical_transcript")
    source_segments = source.get("segments")
    if (
        not isinstance(canonical, Mapping)
        or not isinstance(source_segments, list)
        or not source_segments
        or source.get("full_segmented_episode_text_sha256")
        != hashlib.sha256(full_text.encode("utf-8")).hexdigest()
        or source.get("full_segmented_episode_text_chars") != len(full_text)
        or canonical.get("transcript_id") != episode.get("transcript_id")
        or canonical.get("preparation_id")
        != episode.get("transcript_preparation_id")
        or canonical.get("raw_text_sha256")
        != source["raw_transcript"]["sha256"]
        or canonical.get("prepared_text_sha256")
        != source["prepared_transcript"]["sha256"]
        or raw_path == prepared_path
    ):
        raise CanonicalV31Epoch7InputAuthorityPlanError(
            "authority full-source content binding drifted"
        )
    expected_segment_keys = {
        "segment_id",
        "segment_index",
        "text_sha256",
        "within_prepared_text_bounds",
        "prepared_text_projection_exact",
        "artifact",
    }
    source_segment_ids: list[str] = []
    for ordinal, segment in enumerate(source_segments):
        if not isinstance(segment, Mapping) or set(segment) != expected_segment_keys:
            raise CanonicalV31Epoch7InputAuthorityPlanError(
                "authority source segment record drifted"
            )
        artifact_path = _verify_record(
            segment.get("artifact"),
            label=f"authority source segment {ordinal}",
            project_root=project_root,
        )
        segment_id = segment.get("segment_id")
        if (
            not isinstance(segment_id, str)
            or not segment_id
            or segment_id in source_segment_ids
            or isinstance(segment.get("segment_index"), bool)
            or not isinstance(segment.get("segment_index"), int)
            or segment.get("segment_index") < 0
            or segment.get("text_sha256") != segment["artifact"]["sha256"]
            or not artifact_path.is_file()
            or not isinstance(segment.get("within_prepared_text_bounds"), bool)
            or not isinstance(segment.get("prepared_text_projection_exact"), bool)
        ):
            raise CanonicalV31Epoch7InputAuthorityPlanError(
                "authority source segment lineage drifted"
            )
        source_segment_ids.append(segment_id)

    expected_repair_keys = {
        "segment_id",
        "segment_text",
        "segment_text_sha256",
        "legacy_reference_label",
        "legacy_reference_label_sha256",
        "validation_diagnostic_path",
    }
    repair_ids: list[str] = []
    for repair in repairs:
        if not isinstance(repair, Mapping) or set(repair) != expected_repair_keys:
            raise CanonicalV31Epoch7InputAuthorityPlanError(
                "authority reference repair record drifted"
            )
        segment_id = repair.get("segment_id")
        segment_text = repair.get("segment_text")
        legacy_label = repair.get("legacy_reference_label")
        if (
            not isinstance(segment_id, str)
            or segment_id not in source_segment_ids
            or segment_id in repair_ids
            or not isinstance(segment_text, str)
            or not segment_text
            or repair.get("segment_text_sha256")
            != hashlib.sha256(segment_text.encode("utf-8")).hexdigest()
            or not isinstance(legacy_label, Mapping)
            or legacy_label.get("episode_id") != episode_id
            or legacy_label.get("segment_id") != segment_id
            or repair.get("legacy_reference_label_sha256")
            != hashlib.sha256(
                _canonical_json(legacy_label).encode("ascii")
            ).hexdigest()
            or not isinstance(repair.get("validation_diagnostic_path"), str)
            or not repair["validation_diagnostic_path"].startswith("$")
        ):
            raise CanonicalV31Epoch7InputAuthorityPlanError(
                "authority reference repair lineage drifted"
            )
        repair_ids.append(segment_id)
    if (
        invalid_ids != repair_ids
        or len(invalid_ids) != len(set(invalid_ids))
        or len(preserved_ids) != len(set(preserved_ids))
        or set(invalid_ids) & set(preserved_ids)
        or not set(invalid_ids + preserved_ids).issubset(source_segment_ids)
    ):
        raise CanonicalV31Epoch7InputAuthorityPlanError(
            "authority reference partition drifted"
        )


def freeze_authority_plan(
    *,
    root: Path = DEFAULT_ROOT,
    database_path: Path = DEFAULT_DATABASE_PATH,
    input_root: Path = DEFAULT_INPUT_ROOT,
    capacity_root: Path = DEFAULT_CAPACITY_ROOT,
    project_root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    project = project_root.expanduser().resolve()
    output_root = _safe_path(root, project_root=project, label="authority plan root")
    if output_root.exists():
        raise CanonicalV31Epoch7InputAuthorityPlanError(
            "authority plan root must be fresh and absent"
        )
    input_receipt = input_package.verify_input_package(
        input_root, project_root=project
    )
    capacity_receipt = capacity_readiness.verify_readiness_receipt(
        capacity_root, project_root=project
    )
    if (
        input_receipt.get("state") != "waiting"
        or input_receipt.get("semantic_model_call_count") != 0
        or capacity_receipt.get("state") != "waiting"
        or capacity_receipt.get("semantic_model_call_count") != 0
    ):
        raise CanonicalV31Epoch7InputAuthorityPlanError(
            "authority prerequisite receipts are not exact zero-call waiting evidence"
        )
    input_gap = _load_object(
        Path(input_receipt["input_gap"]["path"]), label="input gap"
    )
    source_binding = _load_object(
        Path(input_receipt["source_binding"]["path"]), label="input source binding"
    )
    capacity_analysis = _load_object(
        Path(capacity_receipt["capacity_readiness"]["path"]),
        label="capacity readiness",
    )
    legacy_manifest_path = Path(source_binding["legacy_manifest"]["path"])
    legacy_info = legacy_matrix.verify_development_manifest(
        legacy_manifest_path,
        expected_sha256=source_binding["legacy_manifest"]["sha256"],
    )
    database = _safe_path(
        database_path,
        project_root=project,
        label="authority source database",
        require_file=True,
    )
    connection = _open_read_only_database(database)
    try:
        prepared = legacy_matrix.prepare_development_episodes(
            connection,
            manifest=legacy_info["payload"],
            manifest_rows=legacy_info["rows"],
        )
        manifest_by_episode = {
            str(row["episode_id"]): row for row in legacy_info["payload"]["episodes"]
        }
        full_sources = {
            str(episode["episode_id"]): legacy_matrix._full_development_context_source(
                connection,
                manifest_episode=manifest_by_episode[str(episode["episode_id"])],
            )
            for episode in prepared
        }
    except legacy_matrix.ExpandedCapDevelopmentMatrixError as exc:
        raise CanonicalV31Epoch7InputAuthorityPlanError(
            "authority source reconstruction failed"
        ) from exc
    finally:
        connection.close()
    rebuilt_source, _private = input_package._source_binding(
        legacy_info=legacy_info,
        prepared_episodes=prepared,
        capacity_policy_path=Path(source_binding["capacity_policy"]["path"]),
        capacity_policy_sha256=source_binding["capacity_policy"]["sha256"],
        project_root=project,
    )
    if rebuilt_source != source_binding:
        raise CanonicalV31Epoch7InputAuthorityPlanError(
            "authority source reconstruction differs from frozen input gap"
        )
    reference_rows = rebuilt_source["canonical_reference_validation"]
    turns = [
        _build_turn(
            manifest_episode=manifest_by_episode[str(episode["episode_id"])],
            prepared_episode=episode,
            full_source=full_sources[str(episode["episode_id"])],
            legacy_references=legacy_info["reference_by_segment"],
            reference_diagnostics=reference_rows,
        )
        for episode in prepared
    ]
    if (
        len(turns) != EXACT_TURN_COUNT
        or len({turn["turn_id"] for turn in turns}) != EXACT_TURN_COUNT
        or sum(len(turn["invalid_reference_segment_ids"]) for turn in turns) != 26
        or [len(turn["invalid_reference_segment_ids"]) for turn in turns]
        != [7, 7, 6, 6]
    ):
        raise CanonicalV31Epoch7InputAuthorityPlanError(
            "authority turn partition drifted"
        )
    capacity_policy = _capacity_policy(
        capacity_analysis, exact_turn_count=len(turns)
    )
    runtime_lock = _runtime_lock(project)
    output_root.mkdir(parents=True, exist_ok=False)
    turn_manifests: list[dict[str, Any]] = []
    for turn in turns:
        turn_root = output_root / TURNS_DIRECTORY / turn["turn_id"]
        input_record = _write_json(
            turn_root / "input.private.json", turn["input"], project_root=project
        )
        prompt_record = _write_bytes(
            turn_root / "prompt.private.txt",
            turn["prompt"].encode("ascii"),
            project_root=project,
        )
        base_record = _write_bytes(
            turn_root / "base-instructions.txt",
            turn["base_instructions"].encode("ascii"),
            project_root=project,
        )
        schema_record = _write_json(
            turn_root / "output-schema.json",
            turn["output_schema"],
            project_root=project,
        )
        turn_manifest = {
            "schema_version": TURN_VERSION,
            "state": "frozen_zero_call",
            "turn_id": turn["turn_id"],
            "episode_id": turn["episode_id"],
            "source_id": turn["source_id"],
            "invalid_reference_segment_ids": turn["invalid_reference_segment_ids"],
            "preserved_valid_reference_segment_ids": turn[
                "preserved_valid_reference_segment_ids"
            ],
            "input": input_record,
            "prompt": prompt_record,
            "base_instructions": base_record,
            "output_schema": schema_record,
            "model": MODEL,
            "effort": EFFORT,
            "semantic_retry_count": 0,
            "semantic_model_call_count": 0,
            "operator_authorization_present": False,
            "executable": False,
        }
        manifest_record = _write_json(
            turn_root / "turn-manifest.json",
            turn_manifest,
            project_root=project,
        )
        turn_manifests.append(
            {
                "turn_id": turn["turn_id"],
                "episode_id": turn["episode_id"],
                "invalid_reference_count": len(
                    turn["invalid_reference_segment_ids"]
                ),
                "turn_manifest": manifest_record,
            }
        )
    capacity_record = _write_json(
        output_root / CAPACITY_FILENAME,
        capacity_policy,
        project_root=project,
    )
    runtime_record = _write_json(
        output_root / RUNTIME_LOCK_FILENAME,
        runtime_lock,
        project_root=project,
    )
    plan = {
        "schema_version": PLAN_VERSION,
        "state": "planned_zero_call_operator_authorization_required",
        "scope": "development_context_and_reference_authority_only",
        "input_package_receipt": _record(
            Path(input_root) / input_package.RECEIPT_FILENAME,
            project_root=project,
        ),
        "input_gap": copy.deepcopy(input_receipt["input_gap"]),
        "capacity_readiness_receipt": _record(
            Path(capacity_root) / capacity_readiness.RECEIPT_FILENAME,
            project_root=project,
        ),
        "capacity_policy": capacity_record,
        "runtime_lock": runtime_record,
        "turns": turn_manifests,
        "exact_turn_count": len(turns),
        "invalid_reference_count": 26,
        "missing_context_field_count": 4,
        "preservation_policy": (
            "preserve_all_existing_context_fields_and_all_six_currently-valid "
            "reference labels byte-for-byte; model authors only excluded_source_context "
            "and the 26 current-validator-invalid full labels"
        ),
        "model": MODEL,
        "effort": EFFORT,
        "semantic_retry_count": 0,
        "semantic_model_call_count": 0,
        "operator_authorization_present": False,
        "executable_semantic_plan_created": False,
        "extraction_authorized": False,
        "quality_authorized": False,
        "holdout_inspected": False,
        "production_mutated": False,
    }
    plan_record = _write_json(
        output_root / PLAN_FILENAME, plan, project_root=project
    )
    receipt = {
        "schema_version": RECEIPT_VERSION,
        "state": "passed_zero_call_plan_only",
        "terminal_reason": "epoch7_input_authority_plan_frozen_no_dispatch",
        "scope": plan["scope"],
        "output_root": str(output_root),
        "authority_plan": plan_record,
        "capacity_policy": capacity_record,
        "runtime_lock": runtime_record,
        "exact_turn_count": len(turns),
        "invalid_reference_count": 26,
        "missing_context_field_count": 4,
        "semantic_model_call_count": 0,
        "semantic_retry_count": 0,
        "operator_authorization_present": False,
        "executable_semantic_plan_created": False,
        "extraction_authorized": False,
        "quality_authorized": False,
        "holdout_inspected": False,
        "production_mutated": False,
    }
    raw = _pretty_json(receipt).encode("ascii")
    _write_bytes(output_root / RECEIPT_FILENAME, raw, project_root=project)
    _write_bytes(output_root / TERMINAL_FILENAME, raw, project_root=project)
    return verify_authority_plan(output_root, project_root=project)


def verify_authority_plan(
    root: Path = DEFAULT_ROOT,
    *,
    project_root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    project = project_root.expanduser().resolve()
    output_root = _safe_path(root, project_root=project, label="authority plan root")
    if not output_root.is_dir() or output_root.is_symlink():
        raise CanonicalV31Epoch7InputAuthorityPlanError(
            "authority plan root is unavailable"
        )
    expected_root = {
        PLAN_FILENAME,
        CAPACITY_FILENAME,
        RUNTIME_LOCK_FILENAME,
        RECEIPT_FILENAME,
        TERMINAL_FILENAME,
        TURNS_DIRECTORY,
    }
    if {path.name for path in output_root.iterdir()} != expected_root:
        raise CanonicalV31Epoch7InputAuthorityPlanError(
            "authority plan root artifact set drifted"
        )
    receipt_path = output_root / RECEIPT_FILENAME
    terminal_path = output_root / TERMINAL_FILENAME
    receipt = _load_object(receipt_path, label="authority plan receipt")
    terminal = _load_object(terminal_path, label="authority plan terminal")
    if receipt != terminal or receipt_path.read_bytes() != terminal_path.read_bytes():
        raise CanonicalV31Epoch7InputAuthorityPlanError(
            "authority plan receipt mirrors drifted"
        )
    plan_path = _verify_record(
        receipt.get("authority_plan"), label="authority plan", project_root=project
    )
    capacity_path = _verify_record(
        receipt.get("capacity_policy"),
        label="authority capacity policy",
        project_root=project,
    )
    runtime_path = _verify_record(
        receipt.get("runtime_lock"),
        label="authority runtime lock",
        project_root=project,
    )
    if any(path.parent != output_root for path in (plan_path, capacity_path, runtime_path)):
        raise CanonicalV31Epoch7InputAuthorityPlanError(
            "authority root record escaped its root"
        )
    plan = _load_object(plan_path, label="authority plan")
    capacity_policy = _load_object(capacity_path, label="authority capacity policy")
    runtime_lock = _load_object(runtime_path, label="authority runtime lock")
    if runtime_lock != _runtime_lock(project):
        raise CanonicalV31Epoch7InputAuthorityPlanError(
            "authority runtime lock drifted"
        )

    input_receipt_path = _verify_record(
        plan.get("input_package_receipt"),
        label="authority predecessor input receipt",
        project_root=project,
    )
    capacity_receipt_path = _verify_record(
        plan.get("capacity_readiness_receipt"),
        label="authority predecessor capacity receipt",
        project_root=project,
    )
    if (
        input_receipt_path.name != input_package.RECEIPT_FILENAME
        or capacity_receipt_path.name != capacity_readiness.RECEIPT_FILENAME
    ):
        raise CanonicalV31Epoch7InputAuthorityPlanError(
            "authority predecessor receipt path drifted"
        )
    try:
        input_receipt = input_package.verify_input_package(
            input_receipt_path.parent, project_root=project
        )
        capacity_receipt = capacity_readiness.verify_readiness_receipt(
            capacity_receipt_path.parent, project_root=project
        )
    except (
        input_package.CanonicalV31Epoch7InputPackageError,
        capacity_readiness.CanonicalV31Epoch7CapacityReadinessError,
    ) as exc:
        raise CanonicalV31Epoch7InputAuthorityPlanError(
            "authority predecessor verification failed"
        ) from exc
    input_gap_path = _verify_record(
        plan.get("input_gap"),
        label="authority predecessor input gap",
        project_root=project,
    )
    source_binding_path = _verify_record(
        input_receipt.get("source_binding"),
        label="authority predecessor source binding",
        project_root=project,
    )
    capacity_analysis_path = _verify_record(
        capacity_receipt.get("capacity_readiness"),
        label="authority predecessor capacity analysis",
        project_root=project,
    )
    if (
        input_receipt.get("state") != "waiting"
        or capacity_receipt.get("state") != "waiting"
        or plan.get("input_gap") != input_receipt.get("input_gap")
        or input_gap_path.parent != input_receipt_path.parent
        or source_binding_path.parent != input_receipt_path.parent
        or capacity_analysis_path.parent != capacity_receipt_path.parent
    ):
        raise CanonicalV31Epoch7InputAuthorityPlanError(
            "authority predecessor state drifted"
        )
    source_binding = _load_object(
        source_binding_path, label="authority predecessor source binding"
    )
    capacity_analysis = _load_object(
        capacity_analysis_path, label="authority predecessor capacity analysis"
    )
    if capacity_policy != _capacity_policy(
        capacity_analysis, exact_turn_count=EXACT_TURN_COUNT
    ):
        raise CanonicalV31Epoch7InputAuthorityPlanError(
            "authority capacity policy drifted"
        )
    turns = plan.get("turns")
    if (
        plan.get("schema_version") != PLAN_VERSION
        or plan.get("state") != "planned_zero_call_operator_authorization_required"
        or not isinstance(turns, list)
        or len(turns) != EXACT_TURN_COUNT
        or plan.get("exact_turn_count") != EXACT_TURN_COUNT
        or plan.get("invalid_reference_count") != 26
        or plan.get("missing_context_field_count") != 4
        or plan.get("semantic_model_call_count") != 0
        or plan.get("semantic_retry_count") != 0
        or plan.get("capacity_policy") != receipt.get("capacity_policy")
        or plan.get("runtime_lock") != receipt.get("runtime_lock")
        or plan.get("model") != MODEL
        or plan.get("effort") != EFFORT
        or plan.get("operator_authorization_present") is not False
        or plan.get("executable_semantic_plan_created") is not False
        or plan.get("extraction_authorized") is not False
        or plan.get("quality_authorized") is not False
        or plan.get("holdout_inspected") is not False
        or plan.get("production_mutated") is not False
    ):
        raise CanonicalV31Epoch7InputAuthorityPlanError(
            "authority plan fixed contract drifted"
        )
    turns_root = output_root / TURNS_DIRECTORY
    if not turns_root.is_dir() or turns_root.is_symlink():
        raise CanonicalV31Epoch7InputAuthorityPlanError(
            "authority turns root drifted"
        )
    expected_turn_ids = [str(row.get("turn_id") or "") for row in turns]
    if (
        len(expected_turn_ids) != len(set(expected_turn_ids))
        or {path.name for path in turns_root.iterdir()} != set(expected_turn_ids)
    ):
        raise CanonicalV31Epoch7InputAuthorityPlanError(
            "authority turn directory membership drifted"
        )
    expected_episode_ids = source_binding.get("episode_ids")
    reference_rows = source_binding.get("canonical_reference_validation")
    if (
        not isinstance(expected_episode_ids, list)
        or len(expected_episode_ids) != EXACT_TURN_COUNT
        or not isinstance(reference_rows, list)
        or [row.get("episode_id") for row in turns] != expected_episode_ids
    ):
        raise CanonicalV31Epoch7InputAuthorityPlanError(
            "authority episode order drifted"
        )
    invalid_total = 0
    preserved_total = 0
    for row in turns:
        if set(row) != {
            "turn_id",
            "episode_id",
            "invalid_reference_count",
            "turn_manifest",
        }:
            raise CanonicalV31Epoch7InputAuthorityPlanError(
                "authority plan turn row drifted"
            )
        manifest_path = _verify_record(
            row.get("turn_manifest"),
            label="authority turn manifest",
            project_root=project,
        )
        turn_root = turns_root / str(row["turn_id"])
        if manifest_path.parent != turn_root or {
            path.name for path in turn_root.iterdir()
        } != {
            "input.private.json",
            "prompt.private.txt",
            "base-instructions.txt",
            "output-schema.json",
            "turn-manifest.json",
        }:
            raise CanonicalV31Epoch7InputAuthorityPlanError(
                "authority turn artifact set drifted"
            )
        manifest = _load_object(manifest_path, label="authority turn manifest")
        if set(manifest) != {
            "schema_version",
            "state",
            "turn_id",
            "episode_id",
            "source_id",
            "invalid_reference_segment_ids",
            "preserved_valid_reference_segment_ids",
            "input",
            "prompt",
            "base_instructions",
            "output_schema",
            "model",
            "effort",
            "semantic_retry_count",
            "semantic_model_call_count",
            "operator_authorization_present",
            "executable",
        }:
            raise CanonicalV31Epoch7InputAuthorityPlanError(
                "authority turn manifest fields drifted"
            )
        input_path = _verify_record(
            manifest.get("input"), label="authority input", project_root=project
        )
        prompt_path = _verify_record(
            manifest.get("prompt"), label="authority prompt", project_root=project
        )
        base_path = _verify_record(
            manifest.get("base_instructions"),
            label="authority base instructions",
            project_root=project,
        )
        schema_path = _verify_record(
            manifest.get("output_schema"),
            label="authority output schema",
            project_root=project,
        )
        if any(
            path.parent != turn_root
            for path in (input_path, prompt_path, base_path, schema_path)
        ):
            raise CanonicalV31Epoch7InputAuthorityPlanError(
                "authority turn record escaped its root"
            )
        turn_input = _load_object(input_path, label="authority input")
        schema = _load_object(schema_path, label="authority output schema")
        expected_prompt = (
            "# Private checksum-bound authority packet\n"
            + _canonical_json(turn_input)
            + "\n"
        ).encode("ascii")
        invalid_ids = turn_input.get("invalid_reference_segment_ids")
        preserved_ids = turn_input.get("preserved_valid_reference_segment_ids")
        expected_invalid_ids = [
            str(reference["segment_id"])
            for reference in reference_rows
            if reference.get("episode_id") == row.get("episode_id")
            and reference.get("canonical_v31_valid") is not True
        ]
        expected_preserved_ids = [
            str(reference["segment_id"])
            for reference in reference_rows
            if reference.get("episode_id") == row.get("episode_id")
            and reference.get("canonical_v31_valid") is True
        ]
        _verify_turn_source_lineage(turn_input, project_root=project)
        if (
            prompt_path.read_bytes() != expected_prompt
            or base_path.read_text(encoding="ascii") != BASE_INSTRUCTIONS
            or schema != _output_schema(
                episode_id=str(turn_input.get("episode_id") or ""),
                invalid_segment_ids=invalid_ids,
            )
            or manifest.get("schema_version") != TURN_VERSION
            or manifest.get("state") != "frozen_zero_call"
            or manifest.get("turn_id") != row.get("turn_id")
            or manifest.get("turn_id")
            != _turn_id(str(turn_input.get("episode_id") or ""), invalid_ids)
            or manifest.get("episode_id") != turn_input.get("episode_id")
            or manifest.get("source_id")
            != turn_input.get("episode_metadata", {}).get("source_id")
            or manifest.get("model") != MODEL
            or manifest.get("effort") != EFFORT
            or manifest.get("invalid_reference_segment_ids") != invalid_ids
            or manifest.get("preserved_valid_reference_segment_ids") != preserved_ids
            or invalid_ids != expected_invalid_ids
            or preserved_ids != expected_preserved_ids
            or row.get("invalid_reference_count") != len(invalid_ids)
            or manifest.get("semantic_model_call_count") != 0
            or manifest.get("semantic_retry_count") != 0
            or manifest.get("operator_authorization_present") is not False
            or manifest.get("executable") is not False
            or not isinstance(invalid_ids, list)
            or not invalid_ids
            or len(invalid_ids) != len(set(invalid_ids))
            or not isinstance(preserved_ids, list)
            or set(invalid_ids) & set(preserved_ids)
        ):
            raise CanonicalV31Epoch7InputAuthorityPlanError(
                "authority turn contract drifted"
            )
        invalid_total += len(invalid_ids)
        preserved_total += len(preserved_ids)
    if invalid_total != 26 or preserved_total != 6:
        raise CanonicalV31Epoch7InputAuthorityPlanError(
            "authority reference coverage drifted"
        )
    if (
        receipt.get("schema_version") != RECEIPT_VERSION
        or receipt.get("state") != "passed_zero_call_plan_only"
        or receipt.get("terminal_reason")
        != "epoch7_input_authority_plan_frozen_no_dispatch"
        or receipt.get("output_root") != str(output_root)
        or receipt.get("exact_turn_count") != EXACT_TURN_COUNT
        or receipt.get("invalid_reference_count") != 26
        or receipt.get("missing_context_field_count") != 4
        or receipt.get("semantic_model_call_count") != 0
        or receipt.get("semantic_retry_count") != 0
        or receipt.get("operator_authorization_present") is not False
        or receipt.get("executable_semantic_plan_created") is not False
        or receipt.get("extraction_authorized") is not False
        or receipt.get("quality_authorized") is not False
        or receipt.get("holdout_inspected") is not False
        or receipt.get("production_mutated") is not False
    ):
        raise CanonicalV31Epoch7InputAuthorityPlanError(
            "authority plan receipt contract drifted"
        )
    return copy.deepcopy(receipt)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Freeze or verify the zero-dispatch epoch-7 input authority plan."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    freeze = subparsers.add_parser("freeze")
    freeze.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    freeze.add_argument("--database", type=Path, default=DEFAULT_DATABASE_PATH)
    verify = subparsers.add_parser("verify")
    verify.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            result = freeze_authority_plan(root=args.root, database_path=args.database)
        else:
            result = verify_authority_plan(args.root)
    except CanonicalV31Epoch7InputAuthorityPlanError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(_pretty_json(result), end="")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__ = [
    "CanonicalV31Epoch7InputAuthorityPlanError",
    "DEFAULT_ROOT",
    "freeze_authority_plan",
    "main",
    "validate_authority_output",
    "verify_authority_plan",
]
