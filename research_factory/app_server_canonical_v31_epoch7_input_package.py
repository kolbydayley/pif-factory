from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import sqlite3
import stat
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_canonical_v31_development_matrix as matrix
from . import app_server_canonical_v31_episode_batch as adapter
from . import app_server_expanded_cap_development_matrix as legacy_matrix
from .labels import ValidationError, validate_label_output
from .util import sha256_text


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATABASE_PATH = PROJECT_ROOT / "data" / "factory.sqlite"
DEFAULT_LEGACY_MANIFEST_PATH = (
    PROJECT_ROOT / "work" / "app-server-development-v2" / "manifest.json"
)
DEFAULT_LEGACY_MANIFEST_SHA256 = (
    "a25d1e9189e13aa6faeba48ea97da5e5702361b741f5d2970a97e5e8af674391"
)
DEFAULT_CAPACITY_POLICY_PATH = (
    PROJECT_ROOT
    / "work"
    / "app-server-development-v2"
    / "unattended-pipeline-v5"
    / "development-selection-v249-expanded-cap-full-event-direct-reference-reserve-v6"
    / "capacity-policy.json"
)
DEFAULT_CAPACITY_POLICY_SHA256 = (
    "a9820940e19dafd612e2563039a9fbb53b49035f68f49e1914ae82fa3f825993"
)
DEFAULT_ROOT = (
    PROJECT_ROOT
    / "work"
    / "app-server-development-v2"
    / "unattended-pipeline-v5"
    / "canonical-v31-epoch7-input-package-v1"
)

RECEIPT_VERSION = "pif_canonical_v31_epoch7_input_package_receipt_v1"
GAP_VERSION = "pif_canonical_v31_epoch7_input_gap_v1"
SOURCE_BINDING_VERSION = "pif_canonical_v31_epoch7_source_binding_v1"

RECEIPT_FILENAME = "input-package-receipt.json"
TERMINAL_FILENAME = "terminal.json"
GAP_FILENAME = "input-gap.json"
SOURCE_BINDING_FILENAME = "source-binding.json"
EPISODES_FILENAME = "episodes.json"
MANIFEST_FILENAME = "development-manifest.json"
REFERENCE_FILENAME = "shared-reference-seed.json"
CAPACITY_POLICY_FILENAME = "capacity-policy.json"
PREFLIGHT_FILENAME = "dry-preflight.json"
PRECOMMIT_FILENAME = "precommit.json"
CONTEXT_DIRECTORY = "contexts"

_CONTEXT_FIELDS = tuple(matrix._CONTEXT_FIELDS)
_CONTEXT_AUTHORITY_FIELDS = tuple(
    field
    for field in _CONTEXT_FIELDS
    if field not in {"episode_id", "source_name", "episode_title"}
)


class CanonicalV31Epoch7InputPackageError(RuntimeError):
    pass


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _pretty_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True) + "\n"


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


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
        raise CanonicalV31Epoch7InputPackageError(
            f"{label} is outside the project root"
        ) from exc
    cursor = candidate
    while cursor != project:
        try:
            mode = cursor.lstat().st_mode
        except FileNotFoundError:
            mode = None
        except OSError as exc:
            raise CanonicalV31Epoch7InputPackageError(
                f"{label} metadata is unavailable"
            ) from exc
        if mode is not None and stat.S_ISLNK(mode):
            raise CanonicalV31Epoch7InputPackageError(f"{label} traverses a symlink")
        cursor = cursor.parent
    if require_file:
        try:
            mode = candidate.lstat().st_mode
        except OSError as exc:
            raise CanonicalV31Epoch7InputPackageError(f"{label} is unavailable") from exc
        if not stat.S_ISREG(mode):
            raise CanonicalV31Epoch7InputPackageError(f"{label} is not a regular file")
    return candidate


def _record(path: Path, *, allowed_root: Path) -> dict[str, Any]:
    resolved = _safe_path(
        path,
        project_root=allowed_root,
        label="artifact record",
        require_file=True,
    )
    try:
        payload = resolved.read_bytes()
        size = resolved.stat().st_size
    except OSError as exc:
        raise CanonicalV31Epoch7InputPackageError(
            f"artifact record is unavailable: {resolved}"
        ) from exc
    return {
        "path": str(resolved),
        "sha256": _sha256_bytes(payload),
        "size_bytes": size,
    }


def _verify_record(
    value: Any,
    *,
    label: str,
    allowed_root: Path,
) -> Path:
    if not isinstance(value, Mapping) or set(value) != {
        "path",
        "sha256",
        "size_bytes",
    }:
        raise CanonicalV31Epoch7InputPackageError(f"{label} record is malformed")
    path_value = value.get("path")
    size = value.get("size_bytes")
    if (
        not isinstance(path_value, str)
        or not _is_sha256(value.get("sha256"))
        or isinstance(size, bool)
        or not isinstance(size, int)
        or size < 0
    ):
        raise CanonicalV31Epoch7InputPackageError(f"{label} record fields drifted")
    path = Path(path_value).expanduser().resolve()
    if _record(path, allowed_root=allowed_root) != dict(value):
        raise CanonicalV31Epoch7InputPackageError(f"{label} record drifted")
    return path


def _load_json(path: Path, *, label: str) -> Any:
    try:
        return json.loads(path.read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CanonicalV31Epoch7InputPackageError(f"{label} is malformed") from exc


def _load_object(path: Path, *, label: str) -> dict[str, Any]:
    value = _load_json(path, label=label)
    if not isinstance(value, dict):
        raise CanonicalV31Epoch7InputPackageError(f"{label} is not an object")
    return value


def _load_array(path: Path, *, label: str) -> list[Any]:
    value = _load_json(path, label=label)
    if not isinstance(value, list):
        raise CanonicalV31Epoch7InputPackageError(f"{label} is not an array")
    return value


def _write_immutable(path: Path, payload: bytes) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags, 0o600)
    except FileExistsError as exc:
        raise CanonicalV31Epoch7InputPackageError(
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
    return _record(path, allowed_root=PROJECT_ROOT)


def _write_json(path: Path, value: Any) -> dict[str, Any]:
    return _write_immutable(path, _pretty_json(value).encode("ascii"))


def _copy_immutable(source: Path, destination: Path) -> dict[str, Any]:
    try:
        payload = source.read_bytes()
    except OSError as exc:
        raise CanonicalV31Epoch7InputPackageError(
            "capacity policy source is unavailable"
        ) from exc
    return _write_immutable(destination, payload)


def _open_read_only_database(path: Path) -> sqlite3.Connection:
    resolved = path.expanduser().resolve()
    try:
        connection = sqlite3.connect(f"{resolved.as_uri()}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only = ON")
        if int(connection.execute("PRAGMA query_only").fetchone()[0]) != 1:
            raise CanonicalV31Epoch7InputPackageError(
                "read-only database query_only enforcement failed"
            )
        return connection
    except CanonicalV31Epoch7InputPackageError:
        raise
    except sqlite3.Error as exc:
        raise CanonicalV31Epoch7InputPackageError(
            "read-only development database is unavailable"
        ) from exc


def _context_value_valid(field: str, value: Any) -> bool:
    if field in {
        "episode_id",
        "source_name",
        "episode_title",
        "context_summary",
        "extraction_guidance",
    }:
        return isinstance(value, str)
    if field in {"speaker_map", "section_map"}:
        return isinstance(value, list) and all(isinstance(row, Mapping) for row in value)
    if field == "entity_seed":
        return isinstance(value, Mapping)
    if field in {"concept_seed", "excluded_source_context"}:
        return isinstance(value, list) and all(isinstance(row, str) for row in value)
    raise CanonicalV31Epoch7InputPackageError(f"unknown context field: {field}")


def _legacy_context_binding(
    *,
    manifest_episode: Mapping[str, Any],
    prepared_episode: Mapping[str, Any],
    project_root: Path,
) -> tuple[dict[str, Any], list[str], list[str]]:
    raw_record = manifest_episode.get("episode_context")
    if not isinstance(raw_record, Mapping):
        raise CanonicalV31Epoch7InputPackageError(
            "legacy development context record is absent"
        )
    path_value = raw_record.get("artifact_path")
    expected_sha = raw_record.get("artifact_sha256")
    if not isinstance(path_value, str) or not _is_sha256(expected_sha):
        raise CanonicalV31Epoch7InputPackageError(
            "legacy development context record is malformed"
        )
    path = _safe_path(
        Path(path_value),
        project_root=project_root,
        label="legacy development context",
        require_file=True,
    )
    record = _record(path, allowed_root=project_root)
    if record["sha256"] != expected_sha:
        raise CanonicalV31Epoch7InputPackageError(
            "legacy development context checksum drifted"
        )
    artifact = _load_object(path, label="legacy development context")
    prepared_context = prepared_episode.get("episode_context")
    if not isinstance(prepared_context, Mapping) or _canonical_json(artifact) != _canonical_json(
        prepared_context
    ):
        raise CanonicalV31Epoch7InputPackageError(
            "prepared development context differs from the manifest artifact"
        )
    if artifact.get("episode_id") != prepared_episode.get("episode_id"):
        raise CanonicalV31Epoch7InputPackageError(
            "legacy development context episode identity drifted"
        )
    missing = [field for field in _CONTEXT_AUTHORITY_FIELDS if field not in artifact]
    invalid = [
        field
        for field in _CONTEXT_AUTHORITY_FIELDS
        if field in artifact and not _context_value_valid(field, artifact[field])
    ]
    return record, missing, invalid


def _canonical_context(
    *,
    legacy_episode: Mapping[str, Any],
    prepared_episode: Mapping[str, Any],
) -> dict[str, Any]:
    context = prepared_episode.get("episode_context")
    if not isinstance(context, Mapping):
        raise CanonicalV31Epoch7InputPackageError(
            "prepared canonical episode context is absent"
        )
    values = {
        "episode_id": prepared_episode.get("episode_id"),
        "source_name": prepared_episode.get("source_name", legacy_episode.get("source_name")),
        "episode_title": prepared_episode.get(
            "episode_title", legacy_episode.get("episode_title")
        ),
        **{
            field: copy.deepcopy(context.get(field))
            for field in _CONTEXT_AUTHORITY_FIELDS
        },
    }
    if any(
        field not in values or not _context_value_valid(field, values[field])
        for field in _CONTEXT_FIELDS
    ):
        raise CanonicalV31Epoch7InputPackageError(
            "prepared canonical episode context is incomplete"
        )
    return values


def _reference_diagnostic(exc: ValidationError) -> str:
    message = str(exc).strip()
    path = message.split(" ", 1)[0]
    return path if path.startswith("$") and len(path) <= 160 else "$"


def _source_binding(
    *,
    legacy_info: Mapping[str, Any],
    prepared_episodes: Sequence[Mapping[str, Any]],
    capacity_policy_path: Path,
    capacity_policy_sha256: str,
    project_root: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    manifest = legacy_info.get("payload")
    rows = legacy_info.get("rows")
    references = legacy_info.get("reference_by_segment")
    manifest_episodes = manifest.get("episodes") if isinstance(manifest, Mapping) else None
    if (
        not isinstance(manifest_episodes, list)
        or not isinstance(rows, list)
        or not isinstance(references, Mapping)
        or len(prepared_episodes) != len(manifest_episodes)
    ):
        raise CanonicalV31Epoch7InputPackageError(
            "legacy development source binding is incomplete"
        )
    expected_episode_ids = [row.get("episode_id") for row in manifest_episodes]
    observed_episode_ids = [row.get("episode_id") for row in prepared_episodes]
    if observed_episode_ids != expected_episode_ids:
        raise CanonicalV31Epoch7InputPackageError(
            "prepared development episode order drifted"
        )
    manifest_path_value = legacy_info.get("path")
    if not isinstance(manifest_path_value, str):
        raise CanonicalV31Epoch7InputPackageError("legacy manifest path is absent")
    manifest_record = _record(Path(manifest_path_value), allowed_root=project_root)
    if manifest_record["sha256"] != legacy_info.get("sha256"):
        raise CanonicalV31Epoch7InputPackageError("legacy manifest record drifted")

    capacity_record = _record(capacity_policy_path, allowed_root=project_root)
    if capacity_record["sha256"] != capacity_policy_sha256:
        raise CanonicalV31Epoch7InputPackageError("capacity policy record drifted")
    try:
        matrix.verify_capacity_policy(
            capacity_policy_path,
            expected_sha256=capacity_policy_sha256,
            allowed_root=project_root,
        )
        capacity_policy_validation = {
            "canonical_epoch7_valid": True,
            "diagnostic": None,
        }
    except matrix.CanonicalV31DevelopmentMatrixError:
        capacity_policy_validation = {
            "canonical_epoch7_valid": False,
            "diagnostic": "canonical_epoch7_capacity_policy_contract_mismatch",
        }

    context_rows: list[dict[str, Any]] = []
    segment_text_by_id: dict[str, str] = {}
    prepared_by_episode: dict[str, Mapping[str, Any]] = {}
    for manifest_episode, prepared in zip(manifest_episodes, prepared_episodes):
        if not isinstance(manifest_episode, Mapping) or not isinstance(prepared, Mapping):
            raise CanonicalV31Epoch7InputPackageError(
                "development episode source row is malformed"
            )
        record, missing, invalid = _legacy_context_binding(
            manifest_episode=manifest_episode,
            prepared_episode=prepared,
            project_root=project_root,
        )
        episode_id = str(prepared.get("episode_id") or "")
        prepared_by_episode[episode_id] = prepared
        context_rows.append(
            {
                "episode_id": episode_id,
                "legacy_context_artifact": record,
                "missing_required_fields": missing,
                "invalid_required_fields": invalid,
            }
        )
        segments = prepared.get("segments")
        if not isinstance(segments, list):
            raise CanonicalV31Epoch7InputPackageError(
                "prepared development segment list is malformed"
            )
        for segment in segments:
            if not isinstance(segment, Mapping):
                raise CanonicalV31Epoch7InputPackageError(
                    "prepared development segment is malformed"
                )
            segment_id = segment.get("segment_id")
            text = segment.get("segment_text")
            if (
                not isinstance(segment_id, str)
                or not isinstance(text, str)
                or not text
                or segment_id in segment_text_by_id
            ):
                raise CanonicalV31Epoch7InputPackageError(
                    "prepared development segment identity drifted"
                )
            segment_text_by_id[segment_id] = text

    reference_rows: list[dict[str, Any]] = []
    validated_labels: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            raise CanonicalV31Epoch7InputPackageError(
                "legacy development manifest row is malformed"
            )
        segment_id = str(row.get("segment_id") or "")
        episode_id = str(row.get("episode_id") or "")
        reference = references.get(segment_id)
        text = segment_text_by_id.get(segment_id)
        label = reference.get("golden_output") if isinstance(reference, Mapping) else None
        diagnostic_path: str | None = None
        if not isinstance(label, dict) or not isinstance(text, str):
            diagnostic_path = "$"
        else:
            try:
                validate_label_output(
                    adapter.CANONICAL_LABEL_PACK,
                    label,
                    segment_text=text,
                )
            except ValidationError as exc:
                diagnostic_path = _reference_diagnostic(exc)
        if (
            isinstance(label, dict)
            and (
                label.get("segment_id") != segment_id
                or label.get("episode_id") != episode_id
            )
        ):
            raise CanonicalV31Epoch7InputPackageError(
                "legacy reference identity drifted"
            )
        if diagnostic_path is None:
            validated_labels[segment_id] = copy.deepcopy(label)
        reference_rows.append(
            {
                "episode_id": episode_id,
                "segment_id": segment_id,
                "canonical_v31_valid": diagnostic_path is None,
                "diagnostic_path": diagnostic_path,
            }
        )
    if len(segment_text_by_id) != len(rows) or len(reference_rows) != len(rows):
        raise CanonicalV31Epoch7InputPackageError(
            "development segment/reference cardinality drifted"
        )
    source = {
        "schema_version": SOURCE_BINDING_VERSION,
        "state": "verified_read_only_source",
        "legacy_manifest": manifest_record,
        "capacity_policy": capacity_record,
        "capacity_policy_validation": capacity_policy_validation,
        "database_access": "sqlite_uri_mode_ro_query_only",
        "episode_count": len(prepared_episodes),
        "case_count": len(rows),
        "episode_ids": observed_episode_ids,
        "segment_ids": [str(row["segment_id"]) for row in rows],
        "context_authority": context_rows,
        "canonical_reference_validation": reference_rows,
        "semantic_model_call_count": 0,
        "holdout_inspected": False,
        "production_mutated": False,
    }
    private = {
        "manifest": copy.deepcopy(dict(manifest)),
        "rows": copy.deepcopy(rows),
        "prepared_episodes": copy.deepcopy(list(prepared_episodes)),
        "prepared_by_episode": prepared_by_episode,
        "validated_labels": validated_labels,
    }
    return source, private


def _opaque_case_id(manifest_sha256: str, episode_id: str, segment_id: str) -> str:
    identity = _canonical_json(
        {
            "schema_version": "pif_canonical_v31_opaque_case_identity_v1",
            "legacy_manifest_sha256": manifest_sha256,
            "episode_id": episode_id,
            "segment_id": segment_id,
        }
    )
    return "case_" + sha256_text(identity)[:24]


def _blocking_conditions(source: Mapping[str, Any]) -> list[dict[str, Any]]:
    missing = [
        {
            "episode_id": row["episode_id"],
            "missing_required_fields": row["missing_required_fields"],
            "invalid_required_fields": row["invalid_required_fields"],
        }
        for row in source["context_authority"]
        if row["missing_required_fields"] or row["invalid_required_fields"]
    ]
    invalid_references = [
        {
            "episode_id": row["episode_id"],
            "segment_id": row["segment_id"],
            "diagnostic_path": row["diagnostic_path"],
        }
        for row in source["canonical_reference_validation"]
        if row["canonical_v31_valid"] is not True
    ]
    conditions: list[dict[str, Any]] = []
    if missing:
        conditions.append(
            {
                "blocker_class": "canonical_episode_context_authority_incomplete",
                "episode_count": len(missing),
                "episodes": missing,
                "required_artifact": (
                    "checksum-bound LLM-authored context authority preserving all existing "
                    "fields and supplying only absent canonical fields"
                ),
            }
        )
    if invalid_references:
        conditions.append(
            {
                "blocker_class": "legacy_reference_not_current_canonical_v31",
                "case_count": len(invalid_references),
                "cases": invalid_references,
                "required_artifact": (
                    "checksum-bound full-canonical reference authority that passes the "
                    "current exact evidence and metric-grounding validator"
                ),
            }
        )
    capacity_validation = source.get("capacity_policy_validation")
    if (
        not isinstance(capacity_validation, Mapping)
        or capacity_validation.get("canonical_epoch7_valid") is not True
    ):
        conditions.append(
            {
                "blocker_class": "canonical_epoch7_capacity_policy_absent_or_incompatible",
                "diagnostic": (
                    capacity_validation.get("diagnostic")
                    if isinstance(capacity_validation, Mapping)
                    else "capacity_policy_validation_absent"
                ),
                "required_artifact": (
                    "checksum-bound canonical six-arm reserve policy with measured numeric "
                    "calibration, conservative phase arithmetic, and zero-retry gates"
                ),
            }
        )
    return conditions


def _receipt_base(*, root: Path, source_record: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": RECEIPT_VERSION,
        "scope": "canonical_v31_epoch7_input_package_only",
        "input_root": str(root.resolve()),
        "source_binding": copy.deepcopy(dict(source_record)),
        "semantic_model_call_count": 0,
        "semantic_retry_count": 0,
        "operator_authorization_present": False,
        "executable_plan_created": False,
        "extraction_authorized": False,
        "quality_authorized": False,
        "holdout_inspected": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "api_key_auth_used": False,
        "raw_session_token_used": False,
        "codex_exec_used": False,
    }


def _write_terminal_pair(root: Path, receipt: Mapping[str, Any]) -> None:
    raw = _pretty_json(receipt).encode("ascii")
    _write_immutable(root / RECEIPT_FILENAME, raw)
    _write_immutable(root / TERMINAL_FILENAME, raw)


def _freeze_waiting(
    *,
    root: Path,
    source: Mapping[str, Any],
    project_root: Path,
) -> dict[str, Any]:
    root.mkdir(parents=True, exist_ok=False)
    source_record = _write_json(root / SOURCE_BINDING_FILENAME, source)
    conditions = _blocking_conditions(source)
    gap = {
        "schema_version": GAP_VERSION,
        "state": "waiting",
        "terminal_reason": "epoch7_canonical_inputs_require_semantic_authority",
        "source_binding": source_record,
        "blocking_conditions": conditions,
        "blocking_condition_count": len(conditions),
        "next_safe_action": (
            "freeze separately authorized checksum-bound authority artifacts, then build a "
            "fresh input-package root; never mutate or resume this waiting root"
        ),
        "semantic_model_call_count": 0,
        "production_mutated": False,
        "holdout_inspected": False,
    }
    gap_record = _write_json(root / GAP_FILENAME, gap)
    receipt = {
        **_receipt_base(root=root, source_record=source_record),
        "state": "waiting",
        "terminal_reason": "epoch7_canonical_inputs_require_semantic_authority",
        "input_gap": gap_record,
        "blocking_condition_count": len(conditions),
        "canonical_manifest": None,
        "episodes": None,
        "capacity_policy": None,
        "dry_preflight": None,
        "precommit": None,
    }
    _write_terminal_pair(root, receipt)
    return verify_input_package(root, project_root=project_root)


def _build_ready_package(
    *,
    root: Path,
    source: Mapping[str, Any],
    private: Mapping[str, Any],
    capacity_policy_path: Path,
    project_root: Path,
) -> dict[str, Any]:
    root.mkdir(parents=True, exist_ok=False)
    source_record = _write_json(root / SOURCE_BINDING_FILENAME, source)
    capacity_record = _copy_immutable(
        capacity_policy_path, root / CAPACITY_POLICY_FILENAME
    )
    legacy_manifest = private["manifest"]
    rows = private["rows"]
    prepared_by_episode = private["prepared_by_episode"]
    labels = private["validated_labels"]
    legacy_manifest_sha = source["legacy_manifest"]["sha256"]
    row_by_episode: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        row_by_episode.setdefault(str(row["episode_id"]), []).append(row)

    episodes_payload: list[dict[str, Any]] = []
    manifest_episodes: list[dict[str, Any]] = []
    context_records: list[dict[str, Any]] = []
    references: list[dict[str, Any]] = []
    case_order: list[str] = []
    for legacy_episode in legacy_manifest["episodes"]:
        episode_id = str(legacy_episode["episode_id"])
        prepared = prepared_by_episode[episode_id]
        context = _canonical_context(
            legacy_episode=legacy_episode,
            prepared_episode=prepared,
        )
        context_payload = {
            "schema_version": matrix.CONTEXT_ARTIFACT_VERSION,
            "episode_id": episode_id,
            "episode_context": {
                field: copy.deepcopy(context[field]) for field in _CONTEXT_FIELDS
            },
            "semantic_deterministic_defaults": {},
            "semantic_deterministic_pruning": False,
        }
        context_record = _write_json(
            root / CONTEXT_DIRECTORY / f"{episode_id}.json", context_payload
        )
        context_records.append(context_record)
        prepared_by_segment = {
            str(segment["segment_id"]): segment for segment in prepared["segments"]
        }
        canonical_segments: list[dict[str, Any]] = []
        manifest_segments: list[dict[str, Any]] = []
        for segment_index, row in enumerate(row_by_episode[episode_id]):
            segment_id = str(row["segment_id"])
            segment = prepared_by_segment[segment_id]
            label = labels[segment_id]
            quality = copy.deepcopy(label["segment_quality"])
            canonical_segment = {
                "segment_id": segment_id,
                "segment_text": segment["segment_text"],
                "segment_quality": quality,
                "density_stratum": row["density_stratum"],
                "boundaries": copy.deepcopy(segment["boundaries"]),
            }
            canonical_segments.append(canonical_segment)
            case_id = _opaque_case_id(legacy_manifest_sha, episode_id, segment_id)
            case_order.append(case_id)
            manifest_segments.append(
                {
                    "segment_id": segment_id,
                    "segment_index": segment_index,
                    "opaque_case_id": case_id,
                    "text_sha256": sha256_text(segment["segment_text"]),
                    "segment_quality_sha256": sha256_text(_canonical_json(quality)),
                    "density_stratum": row["density_stratum"],
                }
            )
            references.append(
                {
                    "opaque_case_id": case_id,
                    "segment_id": segment_id,
                    "episode_id": episode_id,
                    "text_sha256": sha256_text(segment["segment_text"]),
                    "label": copy.deepcopy(label),
                }
            )
        episodes_payload.append(
            {
                **{field: copy.deepcopy(context[field]) for field in _CONTEXT_FIELDS},
                "segments": canonical_segments,
            }
        )
        manifest_episodes.append(
            {
                "episode_id": episode_id,
                "source_id": legacy_episode["source_id"],
                "context_artifact": context_record,
                "segments": manifest_segments,
            }
        )

    reference_payload = {
        "schema_version": matrix.REFERENCE_SEED_VERSION,
        "case_order": case_order,
        "one_shared_reference_for_all_arms": True,
        "semantic_deterministic_defaults": {},
        "semantic_deterministic_pruning": False,
        "references": references,
    }
    reference_record = _write_json(root / REFERENCE_FILENAME, reference_payload)
    manifest_payload = {
        "schema_version": matrix.MANIFEST_VERSION,
        "evaluation_role": "full_canonical_v31_development_only",
        "candidate_system_id": adapter.CANDIDATE_SYSTEM_ID,
        "case_count": len(rows),
        "episode_count": len(manifest_episodes),
        "holdout_excluded": True,
        "production_excluded": True,
        "selection_policy": "opaque_identity_only_no_transcript_semantic_selection_rules",
        "semantic_deterministic_pruning": False,
        "semantic_deterministic_defaults": {},
        "episodes": manifest_episodes,
        "opaque_case_order": case_order,
        "shared_reference_seed": reference_record,
    }
    episodes_record = _write_json(root / EPISODES_FILENAME, episodes_payload)
    manifest_record = _write_json(root / MANIFEST_FILENAME, manifest_payload)
    preflight = matrix.build_matrix_dry_preflight(
        manifest_path=root / MANIFEST_FILENAME,
        manifest_sha256=manifest_record["sha256"],
        episodes=episodes_payload,
        capacity_policy_path=root / CAPACITY_POLICY_FILENAME,
        capacity_policy_sha256=capacity_record["sha256"],
        allowed_root=project_root,
    )
    precommit = matrix.build_matrix_precommit(preflight)
    preflight_record = _write_json(root / PREFLIGHT_FILENAME, preflight)
    precommit_record = _write_json(root / PRECOMMIT_FILENAME, precommit)
    receipt = {
        **_receipt_base(root=root, source_record=source_record),
        "state": "passed",
        "terminal_reason": "epoch7_canonical_input_package_verified_zero_call",
        "case_count": len(rows),
        "episode_count": len(manifest_episodes),
        "context_artifacts": context_records,
        "shared_reference_seed": reference_record,
        "canonical_manifest": manifest_record,
        "episodes": episodes_record,
        "capacity_policy": capacity_record,
        "dry_preflight": preflight_record,
        "precommit": precommit_record,
        "input_gap": None,
        "blocking_condition_count": 0,
    }
    _write_terminal_pair(root, receipt)
    return verify_input_package(root, project_root=project_root)


def freeze_input_package(
    *,
    root: Path = DEFAULT_ROOT,
    database_path: Path = DEFAULT_DATABASE_PATH,
    legacy_manifest_path: Path = DEFAULT_LEGACY_MANIFEST_PATH,
    legacy_manifest_sha256: str = DEFAULT_LEGACY_MANIFEST_SHA256,
    capacity_policy_path: Path = DEFAULT_CAPACITY_POLICY_PATH,
    capacity_policy_sha256: str = DEFAULT_CAPACITY_POLICY_SHA256,
    project_root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    project = project_root.expanduser().resolve()
    output_root = _safe_path(root, project_root=project, label="input package root")
    if output_root.exists():
        raise CanonicalV31Epoch7InputPackageError(
            "input package root must be fresh and absent"
        )
    database = _safe_path(
        database_path,
        project_root=project,
        label="development database",
        require_file=True,
    )
    manifest_path = _safe_path(
        legacy_manifest_path,
        project_root=project,
        label="legacy development manifest",
        require_file=True,
    )
    capacity_path = _safe_path(
        capacity_policy_path,
        project_root=project,
        label="capacity policy",
        require_file=True,
    )
    if not _is_sha256(legacy_manifest_sha256) or not _is_sha256(
        capacity_policy_sha256
    ):
        raise CanonicalV31Epoch7InputPackageError("input checksum binding is malformed")
    try:
        legacy_info = legacy_matrix.verify_development_manifest(
            manifest_path,
            expected_sha256=legacy_manifest_sha256,
        )
    except legacy_matrix.ExpandedCapDevelopmentMatrixError as exc:
        raise CanonicalV31Epoch7InputPackageError(
            "legacy development manifest verification failed"
        ) from exc
    connection = _open_read_only_database(database)
    try:
        try:
            episodes = legacy_matrix.prepare_development_episodes(
                connection,
                manifest=legacy_info["payload"],
                manifest_rows=legacy_info["rows"],
            )
        except legacy_matrix.ExpandedCapDevelopmentMatrixError as exc:
            raise CanonicalV31Epoch7InputPackageError(
                "read-only development source preparation failed"
            ) from exc
    finally:
        connection.close()
    source, private = _source_binding(
        legacy_info=legacy_info,
        prepared_episodes=episodes,
        capacity_policy_path=capacity_path,
        capacity_policy_sha256=capacity_policy_sha256,
        project_root=project,
    )
    if _blocking_conditions(source):
        return _freeze_waiting(
            root=output_root,
            source=source,
            project_root=project,
        )
    return _build_ready_package(
        root=output_root,
        source=source,
        private=private,
        capacity_policy_path=capacity_path,
        project_root=project,
    )


def _verify_terminal_pair(root: Path, *, project_root: Path) -> dict[str, Any]:
    receipt_path = root / RECEIPT_FILENAME
    terminal_path = root / TERMINAL_FILENAME
    receipt = _load_object(receipt_path, label="input package receipt")
    terminal = _load_object(terminal_path, label="input package terminal")
    if receipt != terminal or receipt_path.read_bytes() != terminal_path.read_bytes():
        raise CanonicalV31Epoch7InputPackageError(
            "input package receipt/terminal mirrors drifted"
        )
    if (
        receipt.get("schema_version") != RECEIPT_VERSION
        or receipt.get("scope") != "canonical_v31_epoch7_input_package_only"
        or receipt.get("input_root") != str(root.resolve())
        or receipt.get("semantic_model_call_count") != 0
        or receipt.get("semantic_retry_count") != 0
        or receipt.get("operator_authorization_present") is not False
        or receipt.get("executable_plan_created") is not False
        or receipt.get("extraction_authorized") is not False
        or receipt.get("quality_authorized") is not False
        or receipt.get("holdout_inspected") is not False
        or receipt.get("holdout_authorized") is not False
        or receipt.get("production_mutated") is not False
        or receipt.get("api_key_auth_used") is not False
        or receipt.get("raw_session_token_used") is not False
        or receipt.get("codex_exec_used") is not False
    ):
        raise CanonicalV31Epoch7InputPackageError(
            "input package receipt fixed contract drifted"
        )
    _verify_record(
        receipt.get("source_binding"),
        label="source binding",
        allowed_root=root,
    )
    return receipt


def verify_input_package(
    root: Path = DEFAULT_ROOT,
    *,
    project_root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    project = project_root.expanduser().resolve()
    package_root = _safe_path(
        root,
        project_root=project,
        label="input package root",
    )
    if not package_root.is_dir() or package_root.is_symlink():
        raise CanonicalV31Epoch7InputPackageError(
            "input package root is unavailable"
        )
    receipt = _verify_terminal_pair(package_root, project_root=project)
    state = receipt.get("state")
    if state == "waiting":
        expected = {
            RECEIPT_FILENAME,
            TERMINAL_FILENAME,
            GAP_FILENAME,
            SOURCE_BINDING_FILENAME,
        }
        if {path.name for path in package_root.iterdir()} != expected:
            raise CanonicalV31Epoch7InputPackageError(
                "waiting input package artifact set drifted"
            )
        gap_path = _verify_record(
            receipt.get("input_gap"),
            label="input gap",
            allowed_root=package_root,
        )
        gap = _load_object(gap_path, label="input gap")
        if (
            receipt.get("terminal_reason")
            != "epoch7_canonical_inputs_require_semantic_authority"
            or receipt.get("canonical_manifest") is not None
            or receipt.get("episodes") is not None
            or receipt.get("capacity_policy") is not None
            or receipt.get("dry_preflight") is not None
            or receipt.get("precommit") is not None
            or gap.get("schema_version") != GAP_VERSION
            or gap.get("state") != "waiting"
            or gap.get("source_binding") != receipt.get("source_binding")
            or gap.get("semantic_model_call_count") != 0
            or gap.get("production_mutated") is not False
            or gap.get("holdout_inspected") is not False
            or not isinstance(gap.get("blocking_conditions"), list)
            or not gap["blocking_conditions"]
            or gap.get("blocking_condition_count") != len(gap["blocking_conditions"])
            or receipt.get("blocking_condition_count") != len(gap["blocking_conditions"])
        ):
            raise CanonicalV31Epoch7InputPackageError(
                "waiting input package contract drifted"
            )
        return copy.deepcopy(receipt)
    if state != "passed":
        raise CanonicalV31Epoch7InputPackageError("input package state is invalid")
    required_files = {
        RECEIPT_FILENAME,
        TERMINAL_FILENAME,
        SOURCE_BINDING_FILENAME,
        EPISODES_FILENAME,
        MANIFEST_FILENAME,
        REFERENCE_FILENAME,
        CAPACITY_POLICY_FILENAME,
        PREFLIGHT_FILENAME,
        PRECOMMIT_FILENAME,
        CONTEXT_DIRECTORY,
    }
    if {path.name for path in package_root.iterdir()} != required_files:
        raise CanonicalV31Epoch7InputPackageError(
            "passed input package artifact set drifted"
        )
    context_dir = package_root / CONTEXT_DIRECTORY
    if not context_dir.is_dir() or context_dir.is_symlink():
        raise CanonicalV31Epoch7InputPackageError(
            "input package context directory drifted"
        )
    manifest_path = _verify_record(
        receipt.get("canonical_manifest"),
        label="canonical manifest",
        allowed_root=package_root,
    )
    episodes_path = _verify_record(
        receipt.get("episodes"),
        label="prepared episodes",
        allowed_root=package_root,
    )
    capacity_path = _verify_record(
        receipt.get("capacity_policy"),
        label="capacity policy",
        allowed_root=package_root,
    )
    reference_path = _verify_record(
        receipt.get("shared_reference_seed"),
        label="shared reference seed",
        allowed_root=package_root,
    )
    context_records = receipt.get("context_artifacts")
    if (
        not isinstance(context_records, list)
        or len(context_records) != receipt.get("episode_count")
        or len(list(context_dir.iterdir())) != len(context_records)
    ):
        raise CanonicalV31Epoch7InputPackageError(
            "input package context artifact cardinality drifted"
        )
    for index, record in enumerate(context_records):
        _verify_record(
            record,
            label=f"context artifact {index}",
            allowed_root=context_dir,
        )
    manifest = _load_object(manifest_path, label="canonical manifest")
    if manifest.get("shared_reference_seed") != receipt.get("shared_reference_seed"):
        raise CanonicalV31Epoch7InputPackageError(
            "canonical manifest reference binding drifted"
        )
    if Path(reference_path) != Path(receipt["shared_reference_seed"]["path"]):
        raise CanonicalV31Epoch7InputPackageError(
            "shared reference path binding drifted"
        )
    episodes = _load_array(episodes_path, label="prepared episodes")
    preflight = matrix.build_matrix_dry_preflight(
        manifest_path=manifest_path,
        manifest_sha256=receipt["canonical_manifest"]["sha256"],
        episodes=episodes,
        capacity_policy_path=capacity_path,
        capacity_policy_sha256=receipt["capacity_policy"]["sha256"],
        allowed_root=project,
    )
    precommit = matrix.build_matrix_precommit(preflight)
    preflight_path = _verify_record(
        receipt.get("dry_preflight"),
        label="dry preflight",
        allowed_root=package_root,
    )
    precommit_path = _verify_record(
        receipt.get("precommit"),
        label="precommit",
        allowed_root=package_root,
    )
    if (
        _canonical_json(_load_object(preflight_path, label="dry preflight"))
        != _canonical_json(preflight)
        or _canonical_json(_load_object(precommit_path, label="precommit"))
        != _canonical_json(precommit)
        or receipt.get("terminal_reason")
        != "epoch7_canonical_input_package_verified_zero_call"
        or receipt.get("blocking_condition_count") != 0
        or receipt.get("input_gap") is not None
        or receipt.get("case_count") != preflight["receipt"].get("case_count")
        or receipt.get("episode_count") != preflight["receipt"].get("episode_count")
    ):
        raise CanonicalV31Epoch7InputPackageError(
            "passed input package verification drifted"
        )
    return copy.deepcopy(receipt)


def status_input_package(
    root: Path = DEFAULT_ROOT,
    *,
    project_root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    try:
        receipt = verify_input_package(root, project_root=project_root)
    except CanonicalV31Epoch7InputPackageError as exc:
        return {
            "schema_version": RECEIPT_VERSION,
            "state": "absent_or_invalid",
            "reason": str(exc),
            "semantic_model_call_count": 0,
            "production_mutated": False,
            "holdout_inspected": False,
        }
    return {
        "schema_version": RECEIPT_VERSION,
        "state": receipt["state"],
        "terminal_reason": receipt["terminal_reason"],
        "blocking_condition_count": receipt.get("blocking_condition_count", 0),
        "semantic_model_call_count": 0,
        "production_mutated": False,
        "holdout_inspected": False,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Freeze or verify the zero-call canonical v3.1 epoch-7 input package."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    prepare = subparsers.add_parser("prepare")
    prepare.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    prepare.add_argument("--database", type=Path, default=DEFAULT_DATABASE_PATH)
    prepare.add_argument(
        "--legacy-manifest", type=Path, default=DEFAULT_LEGACY_MANIFEST_PATH
    )
    prepare.add_argument(
        "--legacy-manifest-sha256", default=DEFAULT_LEGACY_MANIFEST_SHA256
    )
    prepare.add_argument(
        "--capacity-policy", type=Path, default=DEFAULT_CAPACITY_POLICY_PATH
    )
    prepare.add_argument(
        "--capacity-policy-sha256", default=DEFAULT_CAPACITY_POLICY_SHA256
    )
    verify = subparsers.add_parser("verify")
    verify.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    status = subparsers.add_parser("status")
    status.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "prepare":
            result = freeze_input_package(
                root=args.root,
                database_path=args.database,
                legacy_manifest_path=args.legacy_manifest,
                legacy_manifest_sha256=args.legacy_manifest_sha256,
                capacity_policy_path=args.capacity_policy,
                capacity_policy_sha256=args.capacity_policy_sha256,
            )
        elif args.command == "verify":
            result = verify_input_package(args.root)
        else:
            result = status_input_package(args.root)
    except CanonicalV31Epoch7InputPackageError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(_pretty_json(result), end="")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__ = [
    "CanonicalV31Epoch7InputPackageError",
    "DEFAULT_ROOT",
    "freeze_input_package",
    "main",
    "status_input_package",
    "verify_input_package",
]
