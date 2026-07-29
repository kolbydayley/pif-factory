from __future__ import annotations

"""Managed app-server runner for the episode-context queue.

Fixture execution remains non-promotable.  Live execution requires a separate
operator authorization, exact runtime bindings, and internally constructed
official transport and capacity implementations.
"""

import argparse
import asyncio
import datetime as dt
import copy
import fcntl
import hashlib
import inspect
import json
import math
import os
import sqlite3
import stat
import uuid
from collections import Counter
from contextlib import AbstractContextManager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, Sequence

from . import db
from .codex_app_server import (
    APP_SERVER_CLIENT_VERSION,
    PINNED_CODEX_CLI_VERSION,
    PROTOCOL_SCHEMA_PATH,
    TURN_SIDECAR_SCHEMA_VERSION,
    AppServerError,
    AppServerAuthError,
    AppServerProtocolError,
    AppServerThread,
    CodexAppServerClient,
)
from .pipeline_babysitter import verify_evaluation_receipt
from .app_server_source_integrity import SourceIntegrityError, VerifiedSourceLoader
from .util import loads_json, now_iso, stable_id
from .worker import (
    EPISODE_CONTEXT_SCHEMA_VERSION,
    complete_job,
    validate_episode_context_output,
)


CONTRACT_VERSION = "pif_app_server_episode_context_runner_contract_v2"
FROZEN_CONFIGURATION_VERSION = "pif_frozen_episode_context_configuration_v2"
HOLDOUT_AUTHORIZATION_VERSION = "pif_episode_context_holdout_authorization_v1"
COMPLETION_RECEIPT_VERSION = "pif_episode_context_completion_receipt_v2"
VERIFIED_COMPLETION_VERSION = "pif_verified_episode_context_completion_v2"
TURN_INPUT_VERSION = "pif_episode_context_turn_input_v2"
TURN_LAUNCH_VERSION = "pif_episode_context_turn_launch_v2"
TURN_TERMINAL_VERSION = "pif_episode_context_turn_terminal_v2"
USAGE_TELEMETRY_VERSION = "pif_episode_context_usage_telemetry_v2"
RUN_RECEIPT_VERSION = "pif_episode_context_bounded_run_receipt_v2"
PROVISION_PLAN_VERSION = "pif_episode_context_provision_plan_v2"
REQUIRED_EPISODE_MANIFEST_VERSION = "pif_episode_context_required_episode_manifest_v2"
CAPACITY_REQUEST_VERSION = "pif_episode_context_capacity_request_v1"
CAPACITY_ADMISSION_VERSION = "pif_episode_context_capacity_admission_v1"
ARTIFACT_LINEAGE_VERSION = "pif_episode_context_artifact_lineage_v1"
ATTEMPT_HISTORY_VERSION = "pif_episode_context_run_attempt_history_v1"
ATTEMPT_SCHEMA_CUTOVER_VERSION = "pif_episode_context_attempt_schema_cutover_v1"
ATTEMPT_SCHEMA_MIGRATION_PLAN_VERSION = (
    "pif_episode_context_attempt_schema_migration_plan_v1"
)
LIVE_PROVISION_PLAN_VERSION = "pif_episode_context_live_provision_plan_v1"
LIVE_PROVISION_AUTHORIZATION_VERSION = (
    "pif_episode_context_live_provision_authorization_v2"
)
LIVE_PROVISION_EXECUTION_VERSION = (
    "pif_episode_context_live_provision_execution_receipt_v1"
)
LIVE_RUNTIME_AUTHORIZATION_VERSION = (
    "pif_episode_context_live_runtime_authorization_v3"
)
LIVE_CAPACITY_POLICY_VERSION = "pif_episode_context_live_capacity_policy_v1"
LIVE_CAPACITY_REQUEST_VERSION = "pif_episode_context_live_capacity_request_v2"
LIVE_CAPACITY_ADMISSION_VERSION = "pif_episode_context_live_capacity_admission_v2"
LIVE_CAPACITY_ABANDONMENT_VERSION = (
    "pif_episode_context_live_capacity_abandonment_v1"
)
LIVE_CAPACITY_WAIT_CHECKPOINT_VERSION = (
    "pif_episode_context_live_capacity_wait_checkpoint_v1"
)
LIVE_COMPLETION_RECEIPT_VERSION = "pif_episode_context_live_completion_receipt_v2"
LIVE_WRITER_LOCK_VERSION = "pif_episode_context_live_writer_lock_v1"

TRANSPORT = "official_codex_app_server_stdio_managed_chatgpt_auth"
PINNED_CLI_VERSION = "0.144.1"
THREAD_MODE = "one_thread_per_episode"
LIVE_REQUIRED_EPISODE_COUNT = 5_032
LIVE_REQUIRED_SEGMENT_COUNT = 69_622
LIVE_REQUIRED_SOURCE_WORD_COUNT = 53_865_361
LIVE_MAX_EPISODE_SOURCE_WORD_COUNT = 96_403
LIVE_LEGACY_COMPLETED_RERUN_COUNT = 49
LIVE_FAILED_CONTEXT_RESET_COUNT = 10
LIVE_NEW_CONTEXT_JOB_COUNT = 4_973
LIVE_LANE = "podcast"
LIVE_LABEL_PACK = "ai_discourse_v3_1"
LIVE_QUEUE_PAYLOAD_MODEL = "gpt-5.5"
LIVE_SEMANTIC_MODEL = "gpt-5.6-sol"
LIVE_REASONING_EFFORT = "high"
PROJECT_ROOT = Path(__file__).resolve().parents[1]
LIVE_EXPECTED_INSTRUCTION_SOURCE_PATHS = (
    str((Path.home() / ".codex/AGENTS.md").expanduser().resolve()),
)
LIVE_CONTEXT_CONTROL_OVERLAY: dict[str, Any] = {
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
CAPACITY_ADMISSION_CONTRACT = {
    "schema_version": CAPACITY_ADMISSION_VERSION,
    "admission_before_claim": True,
    "reserve_verified": True,
    "one_turn_per_episode": True,
    "max_concurrency": 1,
    "receipt_required": True,
}
SEMANTIC_AUTHORITY_FIELDS = (
    "episode_context",
    "extraction_guidance",
    "excluded_source_context",
)
USAGE_FIELDS = (
    "input_tokens",
    "cached_input_tokens",
    "output_tokens",
    "reasoning_output_tokens",
    "total_tokens",
)
JOB_STATUS_FIELDS = ("pending", "claimed", "failed", "completed", "other")
PERSISTED_CONTEXT_FIELDS = (
    "context_summary",
    "speaker_map",
    "section_map",
    "entity_seed",
    "concept_seed",
)


class EpisodeContextRunnerError(RuntimeError):
    pass


class EpisodeContextRunnerWaiting(EpisodeContextRunnerError):
    pass


class LiveCapacityAdmissionDenied(EpisodeContextRunnerWaiting):
    def __init__(self, message: str, *, binding: Mapping[str, Any]) -> None:
        super().__init__(message)
        self.binding = dict(binding)


@dataclass(frozen=True)
class EpisodeContextItem:
    job_id: int
    episode_id: str
    context_run_id: str
    claim_id: str
    attempt_id: str
    source_input: Mapping[str, Any]
    queue_attempt_number: int = 1
    submission_output_path: str | None = None


class EpisodeContextQueueOperations(Protocol):
    fixture_only: bool

    def preview(self, *, limit: int) -> list[EpisodeContextItem]: ...

    def discover_required_episode_ids(self) -> list[str]: ...

    def bind_required_episode_manifest(
        self, manifest: Mapping[str, Any], record: Mapping[str, Any]
    ) -> None: ...

    def bind_execution_contract(self, loaded: Mapping[str, Any]) -> None: ...

    def stale_claims(self, *, limit: int) -> list[EpisodeContextItem]: ...

    def recover_stale_unbound_claims(self, *, limit: int) -> int: ...

    def release_prelaunch_claim(
        self, item: EpisodeContextItem, *, reason: str
    ) -> None: ...

    def bind_prelaunch_capacity(
        self, item: EpisodeContextItem, capacity_binding: Mapping[str, Any]
    ) -> None: ...

    def reverify_source(self, item: EpisodeContextItem) -> Mapping[str, Any]: ...

    def claim(self, *, limit: int) -> list[EpisodeContextItem]: ...

    def bind_launch(self, item: EpisodeContextItem, launch_path: Path) -> None: ...

    def bind_terminal(self, item: EpisodeContextItem, terminal_path: Path) -> None: ...

    def submit(self, item: EpisodeContextItem, output_path: Path) -> Mapping[str, Any]: ...

    def accounting(self) -> Mapping[str, Any]: ...


class EpisodeContextCapacityAdmissionProvider(Protocol):
    fixture_only: bool

    def __call__(self, *, request: Mapping[str, Any]) -> Mapping[str, Any] | Path: ...


class LiveEpisodeContextCapacityAdmissionProvider(Protocol):
    fixture_only: bool

    def __call__(
        self, *, request: Mapping[str, Any]
    ) -> Mapping[str, Any] | Path | Any: ...


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _pretty_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n"


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _is_sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _record(path: Path) -> dict[str, Any]:
    source = path.expanduser().resolve()
    if not source.is_file():
        raise EpisodeContextRunnerError(f"required artifact is missing: {source}")
    return {
        "path": str(source),
        "sha256": _sha256_file(source),
        "size_bytes": source.stat().st_size,
    }


def _verify_record(value: Any, *, label: str) -> Path:
    if not isinstance(value, Mapping) or set(value) != {"path", "sha256", "size_bytes"}:
        raise EpisodeContextRunnerError(f"{label} record shape drifted")
    path = Path(str(value.get("path") or "")).expanduser().resolve()
    size = value.get("size_bytes")
    digest = value.get("sha256")
    if (
        not path.is_file()
        or isinstance(size, bool)
        or not isinstance(size, int)
        or size < 0
        or path.stat().st_size != size
        or not _is_sha256(digest)
        or _sha256_file(path) != digest
    ):
        raise EpisodeContextRunnerError(f"{label} artifact drifted")
    return path


def _load_json(path: Path, *, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EpisodeContextRunnerError(f"{label} is missing or malformed") from exc
    if not isinstance(value, dict):
        raise EpisodeContextRunnerError(f"{label} must be a JSON object")
    return value


def _write_immutable(path: Path, content: bytes) -> None:
    target = path.expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        if target.is_file() and target.read_bytes() == content:
            return
        raise EpisodeContextRunnerError(
            f"immutable artifact already exists with different bytes: {target}"
        )
    descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        target.unlink(missing_ok=True)
        raise


def _freeze_validated_control_artifact(
    path: Path,
    payload: Mapping[str, Any],
    validator: Callable[[Path], Any],
) -> Path:
    """Validate an immutable control artifact before publishing its final name."""

    target = path.expanduser().resolve()
    content = _pretty_json(dict(payload)).encode()
    if target.exists():
        _write_immutable(target, content)
        validator(target)
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = target.parent / f".{target.name}.{uuid.uuid4().hex}.staging"
    try:
        _write_immutable(staging, content)
        validator(staging)
        os.rename(staging, target)
        _fsync_directory(target.parent)
        validator(target)
    except BaseException:
        staging.unlink(missing_ok=True)
        raise
    return target


_ATTEMPT_EXISTING_COLUMNS = (
    ("id", "TEXT", 0, None, 1),
    ("canonical_run_id", "TEXT", 1, None, 0),
    ("job_id", "INTEGER", 0, None, 0),
    ("episode_id", "TEXT", 1, None, 0),
    ("transcript_id", "TEXT", 0, None, 0),
    ("label_pack", "TEXT", 1, None, 0),
    ("queue_payload_model", "TEXT", 1, None, 0),
    ("semantic_model", "TEXT", 1, None, 0),
    ("reasoning_effort", "TEXT", 0, None, 0),
    ("attempt_kind", "TEXT", 1, None, 0),
    ("status", "TEXT", 1, None, 0),
    ("attempt_number", "INTEGER", 1, None, 0),
    ("claim_id", "TEXT", 0, None, 0),
    ("contract_sha256", "TEXT", 0, None, 0),
    ("frozen_configuration_sha256", "TEXT", 0, None, 0),
    ("prompt_path", "TEXT", 0, None, 0),
    ("output_path", "TEXT", 0, None, 0),
    ("context_artifact_path", "TEXT", 0, None, 0),
    ("launch_path", "TEXT", 0, None, 0),
    ("sidecar_path", "TEXT", 0, None, 0),
    ("canonical_snapshot_json", "TEXT", 1, "'{}'", 0),
    ("artifact_records_json", "TEXT", 1, "'{}'", 0),
    ("error", "TEXT", 0, None, 0),
    ("created_at", "TEXT", 1, None, 0),
    ("updated_at", "TEXT", 1, None, 0),
    ("completed_at", "TEXT", 0, None, 0),
)
_ATTEMPT_TARGET_COLUMNS = _ATTEMPT_EXISTING_COLUMNS[:-6] + (
    ("turn_input_path", "TEXT", 0, None, 0),
    ("source_input_sha256", "TEXT", 0, None, 0),
    ("preclaim_capacity_request_path", "TEXT", 0, None, 0),
    ("preclaim_capacity_admission_path", "TEXT", 0, None, 0),
    ("preturn_capacity_request_path", "TEXT", 0, None, 0),
    ("preturn_capacity_admission_path", "TEXT", 0, None, 0),
    ("usage_json", "TEXT", 1, "'{}'", 0),
    ("terminal_path", "TEXT", 0, None, 0),
) + _ATTEMPT_EXISTING_COLUMNS[-6:]
_ATTEMPT_TARGET_FOREIGN_KEYS = frozenset(
    {
        ("canonical_run_id", "episode_context_runs", "id"),
        ("job_id", "jobs", "id"),
        ("episode_id", "episodes", "id"),
        ("transcript_id", "transcripts", "id"),
    }
)
_ATTEMPT_ALLOWED_KINDS = (
    "legacy_canonical_snapshot",
    "managed_app_server",
)
_ATTEMPT_ALLOWED_STATUSES = (
    "claimed",
    "launched",
    "in_progress",
    "db_completed_unterminalized",
    "completed",
    "failed",
    "released_prelaunch",
    "superseded",
)
_ATTEMPT_ACTIVE_STATUSES = (
    "claimed",
    "launched",
    "in_progress",
    "db_completed_unterminalized",
)


def _query_rows(
    connection: sqlite3.Connection, sql: str, parameters: Sequence[Any] = ()
) -> list[dict[str, Any]]:
    cursor = connection.execute(sql, tuple(parameters))
    names = [str(item[0]) for item in cursor.description or ()]
    return [
        {
            name: row[name] if isinstance(row, sqlite3.Row) else row[index]
            for index, name in enumerate(names)
        }
        for row in cursor.fetchall()
    ]


def _open_read_only_sqlite(path: Path) -> sqlite3.Connection:
    database_path = path.expanduser().resolve()
    if not database_path.is_file():
        raise EpisodeContextRunnerError("read-only context database is unavailable")
    connection = sqlite3.connect(
        f"file:{database_path}?mode=ro", uri=True, timeout=5.0
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only = ON")
    if int(connection.execute("PRAGMA query_only").fetchone()[0]) != 1:
        connection.close()
        raise EpisodeContextRunnerError("SQLite read-only planning guard failed")
    return connection


def _database_identity(path: Path) -> dict[str, Any]:
    source = path.expanduser().resolve()
    stat = source.stat()
    return {
        "path": str(source),
        "device": int(stat.st_dev),
        "inode": int(stat.st_ino),
    }


def _validate_database_identity(value: Any, *, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != {"path", "device", "inode"}:
        raise EpisodeContextRunnerError(f"{label} database identity shape drifted")
    path = Path(str(value.get("path") or "")).expanduser().resolve()
    device = value.get("device")
    inode = value.get("inode")
    if (
        not path.is_file()
        or isinstance(device, bool)
        or not isinstance(device, int)
        or isinstance(inode, bool)
        or not isinstance(inode, int)
        or _database_identity(path) != dict(value)
    ):
        raise EpisodeContextRunnerError(f"{label} database identity drifted")
    return dict(value)


def _live_writer_lock_path(database_identity: Mapping[str, Any]) -> Path:
    verified = _validate_database_identity(
        database_identity, label="live context writer lock"
    )
    database_path = Path(str(verified["path"]))
    return database_path.with_name(f".{database_path.name}.episode-context-live.lock")


class LiveContextWriterLock(AbstractContextManager["LiveContextWriterLock"]):
    """One nonblocking writer across cutover, provision, and semantic runs."""

    def __init__(self, database_identity: Mapping[str, Any], *, phase: str) -> None:
        if phase not in {"migrate_provision", "run", "verify_completion"}:
            raise EpisodeContextRunnerError("live context writer-lock phase drifted")
        self.database_identity = _validate_database_identity(
            database_identity, label="live context writer lock"
        )
        self.phase = phase
        self.path = _live_writer_lock_path(self.database_identity)
        self._descriptor: int | None = None

    def __enter__(self) -> "LiveContextWriterLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        parent_mode = self.path.parent.lstat().st_mode
        if not stat.S_ISDIR(parent_mode) or self.path.parent.is_symlink():
            raise EpisodeContextRunnerError(
                "live context writer-lock directory is not a real directory"
            )
        flags = os.O_RDWR | os.O_CREAT
        if hasattr(os, "O_CLOEXEC"):
            flags |= os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(self.path, flags, 0o600)
        except OSError as exc:
            raise EpisodeContextRunnerError(
                "live context writer lock is not a real file"
            ) from exc
        opened = os.fstat(descriptor)
        try:
            lexical = self.path.lstat()
        except OSError:
            os.close(descriptor)
            raise
        if (
            not stat.S_ISREG(opened.st_mode)
            or not stat.S_ISREG(lexical.st_mode)
            or (opened.st_dev, opened.st_ino) != (lexical.st_dev, lexical.st_ino)
        ):
            os.close(descriptor)
            raise EpisodeContextRunnerError(
                "live context writer lock is not a real file"
            )
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(descriptor)
            raise EpisodeContextRunnerWaiting(
                "another live episode-context writer already owns the queue"
            ) from exc
        payload = {
            "schema_version": LIVE_WRITER_LOCK_VERSION,
            "pid": os.getpid(),
            "phase": self.phase,
            "database": self.database_identity,
            "acquired_at": now_iso(),
        }
        os.ftruncate(descriptor, 0)
        os.write(descriptor, _pretty_json(payload).encode("utf-8"))
        os.fsync(descriptor)
        self._descriptor = descriptor
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        descriptor = self._descriptor
        self._descriptor = None
        if descriptor is not None:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)
        return None


_ATTEMPT_TARGET_TABLE_SQL = """CREATE TABLE episode_context_run_attempts (
  id TEXT PRIMARY KEY,
  canonical_run_id TEXT NOT NULL REFERENCES episode_context_runs(id),
  job_id INTEGER REFERENCES jobs(id),
  episode_id TEXT NOT NULL REFERENCES episodes(id),
  transcript_id TEXT REFERENCES transcripts(id),
  label_pack TEXT NOT NULL,
  queue_payload_model TEXT NOT NULL,
  semantic_model TEXT NOT NULL,
  reasoning_effort TEXT,
  attempt_kind TEXT NOT NULL CHECK (attempt_kind IN ('legacy_canonical_snapshot','managed_app_server')),
  status TEXT NOT NULL CHECK (status IN ('claimed','launched','in_progress','db_completed_unterminalized','completed','failed','released_prelaunch','superseded')),
  attempt_number INTEGER NOT NULL,
  claim_id TEXT UNIQUE,
  contract_sha256 TEXT,
  frozen_configuration_sha256 TEXT,
  prompt_path TEXT,
  output_path TEXT,
  context_artifact_path TEXT,
  launch_path TEXT,
  sidecar_path TEXT,
  turn_input_path TEXT,
  source_input_sha256 TEXT,
  preclaim_capacity_request_path TEXT,
  preclaim_capacity_admission_path TEXT,
  preturn_capacity_request_path TEXT,
  preturn_capacity_admission_path TEXT,
  usage_json TEXT NOT NULL DEFAULT '{}',
  terminal_path TEXT,
  canonical_snapshot_json TEXT NOT NULL DEFAULT '{}',
  artifact_records_json TEXT NOT NULL DEFAULT '{}',
  error TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  completed_at TEXT,
  UNIQUE(canonical_run_id, attempt_number)
)"""
_ATTEMPT_ACTIVE_INDEX_SQL = (
    "CREATE UNIQUE INDEX episode_context_run_attempts_one_active_job "
    "ON episode_context_run_attempts(job_id) WHERE job_id IS NOT NULL AND "
    "status IN ('claimed','launched','in_progress','db_completed_unterminalized')"
)


def _attempt_schema_observation(connection: sqlite3.Connection) -> dict[str, Any]:
    columns = tuple(
        (
            str(row["name"]),
            str(row["type"]),
            int(row["notnull"]),
            row["dflt_value"],
            int(row["pk"]),
        )
        for row in _query_rows(
            connection, "PRAGMA table_info(episode_context_run_attempts)"
        )
    )
    foreign_keys = frozenset(
        (str(row["from"]), str(row["table"]), str(row["to"]))
        for row in _query_rows(
            connection, "PRAGMA foreign_key_list(episode_context_run_attempts)"
        )
    )
    unique_indexes: list[dict[str, Any]] = []
    for row in _query_rows(
        connection, "PRAGMA index_list(episode_context_run_attempts)"
    ):
        if int(row["unique"]) != 1:
            continue
        name = str(row["name"])
        sql_row = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?",
            (name,),
        ).fetchone()
        unique_indexes.append(
            {
                "name": name,
                "columns": [
                    str(item["name"])
                    for item in _query_rows(
                        connection, f'PRAGMA index_info("{name}")'
                    )
                ],
                "partial": int(row.get("partial") or 0) == 1,
                "sql": str(sql_row[0]) if sql_row is not None and sql_row[0] else None,
            }
        )
    table_row = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' "
        "AND name = 'episode_context_run_attempts'"
    ).fetchone()
    table_sql = str(table_row[0]) if table_row is not None and table_row[0] else ""
    compact_sql = "".join(table_sql.lower().split())
    constraints = {
        "attempt_kind_check": all(item in compact_sql for item in _ATTEMPT_ALLOWED_KINDS),
        "status_check": all(item in compact_sql for item in _ATTEMPT_ALLOWED_STATUSES),
        "unique_claim_id": any(
            row["columns"] == ["claim_id"] for row in unique_indexes
        ),
        "partial_unique_active_job": any(
            row["columns"] == ["job_id"]
            and row["partial"] is True
            and all(item in str(row["sql"] or "") for item in _ATTEMPT_ACTIVE_STATUSES)
            for row in unique_indexes
        ),
    }
    return {
        "columns": columns,
        "foreign_keys": foreign_keys,
        "unique_indexes": sorted(unique_indexes, key=lambda item: item["name"]),
        "constraints": constraints,
        "table_sql_sha256": _sha256_bytes(table_sql.encode()),
        "attempt_row_count": int(
            connection.execute(
                "SELECT COUNT(*) FROM episode_context_run_attempts"
            ).fetchone()[0]
        ),
    }


def build_attempt_schema_migration_plan(database_path: Path) -> dict[str, Any]:
    """Describe, but never execute, the empty-table production cutover."""

    source = database_path.expanduser().resolve()
    connection = _open_read_only_sqlite(source)
    try:
        observed = _attempt_schema_observation(connection)
    finally:
        connection.close()
    existing_schema = observed["columns"] == _ATTEMPT_EXISTING_COLUMNS
    target_schema = (
        observed["columns"] == _ATTEMPT_TARGET_COLUMNS
        and observed["foreign_keys"] == _ATTEMPT_TARGET_FOREIGN_KEYS
        and all(observed["constraints"].values())
    )
    statements = [
        "BEGIN IMMEDIATE",
        "ALTER TABLE episode_context_run_attempts RENAME TO episode_context_run_attempts_preproduction_v1",
        _ATTEMPT_TARGET_TABLE_SQL,
        _ATTEMPT_ACTIVE_INDEX_SQL,
        "COMMIT",
    ]
    payload = {
        "schema_version": ATTEMPT_SCHEMA_MIGRATION_PLAN_VERSION,
        "state": (
            "already_applied"
            if target_schema
            else "ready_for_separately_authorized_empty_table_cutover"
            if existing_schema and observed["attempt_row_count"] == 0
            else "waiting"
        ),
        "checked_at": now_iso(),
        "database": _database_identity(source),
        "database_open_mode": "sqlite_uri_mode_ro_query_only",
        "preconditions": {
            "existing_columns_sha256": _sha256_bytes(
                _canonical_json([list(item) for item in _ATTEMPT_EXISTING_COLUMNS]).encode()
            ),
            "attempt_row_count_must_equal": 0,
            "legacy_table_preserved_as": "episode_context_run_attempts_preproduction_v1",
        },
        "target": {
            "columns": [list(item) for item in _ATTEMPT_TARGET_COLUMNS],
            "columns_sha256": _sha256_bytes(
                _canonical_json([list(item) for item in _ATTEMPT_TARGET_COLUMNS]).encode()
            ),
            "foreign_keys": [list(item) for item in sorted(_ATTEMPT_TARGET_FOREIGN_KEYS)],
            "unique_claim_id": True,
            "partial_unique_active_job": list(_ATTEMPT_ACTIVE_STATUSES),
            "allowed_attempt_kinds": list(_ATTEMPT_ALLOWED_KINDS),
            "allowed_statuses": list(_ATTEMPT_ALLOWED_STATUSES),
            "explicit_lineage_columns": [
                "turn_input_path",
                "source_input_sha256",
                "preclaim_capacity_request_path",
                "preclaim_capacity_admission_path",
                "preturn_capacity_request_path",
                "preturn_capacity_admission_path",
                "usage_json",
                "terminal_path",
            ],
        },
        "statements": statements,
        "statements_sha256": _sha256_bytes(_canonical_json(statements).encode()),
        "mutation_performed": False,
        "execution_authorized": False,
    }
    unhashed = dict(payload)
    payload["plan_sha256"] = _sha256_bytes(_canonical_json(unhashed).encode())
    return payload


def verify_attempt_schema_migration_plan(value: Mapping[str, Any]) -> dict[str, Any]:
    unhashed = dict(value)
    supplied = unhashed.pop("plan_sha256", None)
    target = value.get("target")
    statements = value.get("statements")
    if (
        value.get("schema_version") != ATTEMPT_SCHEMA_MIGRATION_PLAN_VERSION
        or value.get("state")
        not in {
            "ready_for_separately_authorized_empty_table_cutover",
            "already_applied",
        }
        or value.get("database_open_mode") != "sqlite_uri_mode_ro_query_only"
        or not isinstance(target, Mapping)
        or target.get("columns") != [list(item) for item in _ATTEMPT_TARGET_COLUMNS]
        or target.get("foreign_keys")
        != [list(item) for item in sorted(_ATTEMPT_TARGET_FOREIGN_KEYS)]
        or target.get("unique_claim_id") is not True
        or target.get("partial_unique_active_job") != list(_ATTEMPT_ACTIVE_STATUSES)
        or target.get("allowed_attempt_kinds") != list(_ATTEMPT_ALLOWED_KINDS)
        or target.get("allowed_statuses") != list(_ATTEMPT_ALLOWED_STATUSES)
        or statements
        != [
            "BEGIN IMMEDIATE",
            "ALTER TABLE episode_context_run_attempts RENAME TO episode_context_run_attempts_preproduction_v1",
            _ATTEMPT_TARGET_TABLE_SQL,
            _ATTEMPT_ACTIVE_INDEX_SQL,
            "COMMIT",
        ]
        or value.get("statements_sha256")
        != _sha256_bytes(_canonical_json(statements).encode())
        or value.get("mutation_performed") is not False
        or value.get("execution_authorized") is not False
        or not _is_sha256(supplied)
        or supplied != _sha256_bytes(_canonical_json(unhashed).encode())
    ):
        raise EpisodeContextRunnerError("attempt-schema migration plan drifted")
    return dict(value)


def build_attempt_schema_cutover_receipt(
    database_path: Path, *, migration_plan_path: Path | None = None
) -> dict[str, Any]:
    """Classify the installed ledger and bind the nonexecuted migration plan."""

    source = database_path.expanduser().resolve()
    connection = _open_read_only_sqlite(source)
    try:
        observed = _attempt_schema_observation(connection)
        table_counts = {
            table: int(connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])
            for table in ("episode_context_runs", "jobs", "labels")
        }
    finally:
        connection.close()
    target_complete = bool(
        observed["columns"] == _ATTEMPT_TARGET_COLUMNS
        and observed["foreign_keys"] == _ATTEMPT_TARGET_FOREIGN_KEYS
        and all(observed["constraints"].values())
    )
    current_incomplete = bool(
        observed["columns"] == _ATTEMPT_EXISTING_COLUMNS
        and not target_complete
        and observed["attempt_row_count"] == 0
    )
    migration_record = None
    if migration_plan_path is not None:
        plan_path = migration_plan_path.expanduser().resolve()
        verify_attempt_schema_migration_plan(
            _load_json(plan_path, label="attempt-schema migration plan")
        )
        migration_record = _record(plan_path)
    if current_incomplete and migration_record is None:
        raise EpisodeContextRunnerError(
            "incomplete attempt schema lacks a frozen additive migration plan"
        )
    target_columns = [list(item) for item in _ATTEMPT_TARGET_COLUMNS]
    observed_columns = [list(item) for item in observed["columns"]]
    payload = {
        "schema_version": ATTEMPT_SCHEMA_CUTOVER_VERSION,
        "state": (
            "verified_target_schema"
            if target_complete and observed["attempt_row_count"] == 0
            else "existing_incomplete_schema"
            if current_incomplete
            else "waiting"
        ),
        "checked_at": now_iso(),
        "database": _database_identity(source),
        "database_open_mode": "sqlite_uri_mode_ro_query_only",
        "table": "episode_context_run_attempts",
        "target_columns": target_columns,
        "target_columns_sha256": _sha256_bytes(
            _canonical_json(target_columns).encode()
        ),
        "observed_columns": observed_columns,
        "observed_columns_sha256": _sha256_bytes(
            _canonical_json(observed_columns).encode()
        ),
        "observed_foreign_keys": [
            list(item) for item in sorted(observed["foreign_keys"])
        ],
        "observed_unique_indexes": observed["unique_indexes"],
        "observed_constraints": observed["constraints"],
        "target_schema_complete": target_complete,
        "attempt_row_count": observed["attempt_row_count"],
        "baseline_table_counts": table_counts,
        "migration_plan": migration_record,
        "mutation_performed": False,
        "live_execution_authorized": False,
        "privacy": "schema_counts_and_database_identity_no_prompt_output_or_credentials",
    }
    unhashed = dict(payload)
    payload["receipt_sha256"] = _sha256_bytes(_canonical_json(unhashed).encode())
    return payload


def verify_attempt_schema_cutover_receipt(
    value: Mapping[str, Any], *, require_ready: bool = False
) -> dict[str, Any]:
    expected_keys = {
        "schema_version",
        "state",
        "checked_at",
        "database",
        "database_open_mode",
        "table",
        "target_columns",
        "target_columns_sha256",
        "observed_columns",
        "observed_columns_sha256",
        "observed_foreign_keys",
        "observed_unique_indexes",
        "observed_constraints",
        "target_schema_complete",
        "attempt_row_count",
        "baseline_table_counts",
        "migration_plan",
        "mutation_performed",
        "live_execution_authorized",
        "privacy",
        "receipt_sha256",
    }
    unhashed = dict(value)
    supplied = unhashed.pop("receipt_sha256", None)
    target_columns = [list(item) for item in _ATTEMPT_TARGET_COLUMNS]
    attempt_count = value.get("attempt_row_count")
    state = value.get("state")
    target_complete = value.get("target_schema_complete") is True
    migration_record = value.get("migration_plan")
    if migration_record is not None:
        plan_path = _verify_record(
            migration_record, label="attempt-schema migration plan"
        )
        verify_attempt_schema_migration_plan(
            _load_json(plan_path, label="attempt-schema migration plan")
        )
    if (
        set(value) != expected_keys
        or value.get("schema_version") != ATTEMPT_SCHEMA_CUTOVER_VERSION
        or state not in {"verified_target_schema", "existing_incomplete_schema"}
        or (require_ready and state != "verified_target_schema")
        or value.get("database_open_mode") != "sqlite_uri_mode_ro_query_only"
        or value.get("table") != "episode_context_run_attempts"
        or value.get("target_columns") != target_columns
        or value.get("target_columns_sha256")
        != _sha256_bytes(_canonical_json(target_columns).encode())
        or not isinstance(value.get("observed_columns"), list)
        or value.get("observed_columns_sha256")
        != _sha256_bytes(_canonical_json(value["observed_columns"]).encode())
        or not isinstance(value.get("observed_foreign_keys"), list)
        or not isinstance(value.get("observed_unique_indexes"), list)
        or not isinstance(value.get("observed_constraints"), Mapping)
        or (state == "verified_target_schema" and not target_complete)
        or (state == "existing_incomplete_schema" and target_complete)
        or (state == "existing_incomplete_schema" and migration_record is None)
        or isinstance(attempt_count, bool)
        or not isinstance(attempt_count, int)
        or attempt_count < 0
        or attempt_count != 0
        or value.get("mutation_performed") is not False
        or value.get("live_execution_authorized") is not False
        or not _is_sha256(supplied)
        or supplied != _sha256_bytes(_canonical_json(unhashed).encode())
    ):
        raise EpisodeContextRunnerError("attempt-schema cutover receipt drifted")
    return dict(value)


def _queue_model_scope_sql(alias: str) -> str:
    return f"""
      AND (
        json_extract({alias}.payload_json, '$.queue_payload_model') = ?
        OR (
          json_extract({alias}.payload_json, '$.queue_payload_model') IS NULL
          AND (
            json_extract({alias}.payload_json, '$.model') IS NULL
            OR json_extract({alias}.payload_json, '$.model') = ?
          )
        )
      )
    """


def build_live_context_provision_plan(
    database_path: Path,
    *,
    expected_population: Mapping[str, int] | None = None,
) -> dict[str, Any]:
    """Build the exact managed-context denominator from a read-only DB handle."""

    expected = dict(
        expected_population
        or {
            "required_episode_count": LIVE_REQUIRED_EPISODE_COUNT,
            "required_segment_count": LIVE_REQUIRED_SEGMENT_COUNT,
            "required_source_word_count": LIVE_REQUIRED_SOURCE_WORD_COUNT,
            "max_episode_source_word_count": LIVE_MAX_EPISODE_SOURCE_WORD_COUNT,
            "legacy_completed_context_rerun_count": LIVE_LEGACY_COMPLETED_RERUN_COUNT,
            "failed_context_job_reset_count": LIVE_FAILED_CONTEXT_RESET_COUNT,
            "new_context_job_count": LIVE_NEW_CONTEXT_JOB_COUNT,
            "attempt_row_count": 0,
        }
    )
    expected_keys = {
        "required_episode_count",
        "required_segment_count",
        "required_source_word_count",
        "max_episode_source_word_count",
        "legacy_completed_context_rerun_count",
        "failed_context_job_reset_count",
        "new_context_job_count",
        "attempt_row_count",
    }
    if set(expected) != expected_keys or any(
        isinstance(value, bool) or not isinstance(value, int) or value < 0
        for value in expected.values()
    ):
        raise EpisodeContextRunnerError("live context population expectation drifted")
    source = database_path.expanduser().resolve()
    connection = _open_read_only_sqlite(source)
    try:
        required_rows = _query_rows(
            connection,
            f"""
            SELECT DISTINCT segments.episode_id AS episode_id
            FROM jobs AS label_jobs
            JOIN segments ON segments.id = label_jobs.target_id
            WHERE label_jobs.lane = ?
              AND label_jobs.job_type = 'label_segment'
              AND label_jobs.status IN ('pending', 'claimed', 'failed')
              AND json_extract(label_jobs.payload_json, '$.label_pack') = ?
              {_queue_model_scope_sql('label_jobs')}
            ORDER BY segments.episode_id
            """,
            (
                LIVE_LANE,
                LIVE_LABEL_PACK,
                LIVE_QUEUE_PAYLOAD_MODEL,
                LIVE_QUEUE_PAYLOAD_MODEL,
            ),
        )
        required_ids = [str(row["episode_id"]) for row in required_rows]
        required_json = _canonical_json(required_ids)
        source_totals = _query_rows(
            connection,
            """
            SELECT COUNT(*) AS segment_count,
                   COALESCE(SUM(word_count), 0) AS source_word_count
            FROM segments
            WHERE episode_id IN (SELECT CAST(value AS TEXT) FROM json_each(?))
            """,
            (required_json,),
        )[0]
        max_episode_words = int(
            connection.execute(
                """
                SELECT COALESCE(MAX(episode_words), 0)
                FROM (
                  SELECT episode_id, SUM(word_count) AS episode_words
                  FROM segments
                  WHERE episode_id IN (
                    SELECT CAST(value AS TEXT) FROM json_each(?)
                  )
                  GROUP BY episode_id
                )
                """,
                (required_json,),
            ).fetchone()[0]
        )
        context_rows = _query_rows(
            connection,
            f"""
            SELECT id, target_id, status, payload_json
            FROM jobs AS context_jobs
            WHERE context_jobs.lane = ?
              AND context_jobs.job_type = 'episode_context'
              AND json_extract(context_jobs.payload_json, '$.label_pack') = ?
              {_queue_model_scope_sql('context_jobs')}
              AND context_jobs.target_id IN (
                SELECT CAST(value AS TEXT) FROM json_each(?)
              )
            ORDER BY target_id, id
            """,
            (
                LIVE_LANE,
                LIVE_LABEL_PACK,
                LIVE_QUEUE_PAYLOAD_MODEL,
                LIVE_QUEUE_PAYLOAD_MODEL,
                required_json,
            ),
        )
        attempt_count = int(
            connection.execute(
                "SELECT COUNT(*) FROM episode_context_run_attempts"
            ).fetchone()[0]
        )
        run_rows = _query_rows(
            connection,
            """
            SELECT episode_id, status, context_artifact_path
            FROM episode_context_runs
            WHERE label_pack = ? AND model = ?
              AND episode_id IN (
                SELECT CAST(value AS TEXT) FROM json_each(?)
              )
            """,
            (LIVE_LABEL_PACK, LIVE_QUEUE_PAYLOAD_MODEL, required_json),
        )
    finally:
        connection.close()
    by_episode: dict[str, list[dict[str, Any]]] = {}
    for row in context_rows:
        by_episode.setdefault(str(row["target_id"]), []).append(row)
    duplicate_episode_ids = sorted(
        episode_id for episode_id, rows in by_episode.items() if len(rows) != 1
    )
    active_ids = sorted(
        episode_id
        for episode_id, rows in by_episode.items()
        if len(rows) == 1 and str(rows[0]["status"]) in {"pending", "claimed"}
    )
    failed_ids = sorted(
        episode_id
        for episode_id, rows in by_episode.items()
        if len(rows) == 1 and str(rows[0]["status"]) == "failed"
    )
    completed_ids = sorted(
        episode_id
        for episode_id, rows in by_episode.items()
        if len(rows) == 1 and str(rows[0]["status"]) == "completed"
    )
    run_by_episode = {str(row["episode_id"]): row for row in run_rows}
    managed_lineage_present: list[str] = []
    rerun_ids: list[str] = []
    for episode_id in completed_ids:
        run = run_by_episode.get(episode_id)
        artifact_value = run.get("context_artifact_path") if run else None
        artifact = (
            Path(str(artifact_value)).expanduser().resolve()
            if artifact_value
            else None
        )
        if (
            run is not None
            and str(run.get("status")) == "completed"
            and artifact is not None
            and artifact.is_file()
            and _artifact_lineage_path(artifact).is_file()
        ):
            managed_lineage_present.append(episode_id)
        else:
            rerun_ids.append(episode_id)
    new_ids = sorted(set(required_ids) - set(by_episode))
    population = {
        "required_episode_count": len(required_ids),
        "required_segment_count": int(source_totals["segment_count"]),
        "required_source_word_count": int(source_totals["source_word_count"]),
        "max_episode_source_word_count": max_episode_words,
        "legacy_completed_context_rerun_count": len(rerun_ids),
        "failed_context_job_reset_count": len(failed_ids),
        "new_context_job_count": len(new_ids),
        "attempt_row_count": attempt_count,
    }
    exact_population = population == expected
    ready = bool(
        exact_population
        and not duplicate_episode_ids
        and not active_ids
        and not managed_lineage_present
        and len(rerun_ids) + len(failed_ids) + len(new_ids) == len(required_ids)
    )
    payload = {
        "schema_version": LIVE_PROVISION_PLAN_VERSION,
        "state": "ready_read_only" if ready else "waiting",
        "checked_at": now_iso(),
        "database": _database_identity(source),
        "database_open_mode": "sqlite_uri_mode_ro_query_only",
        "scope": {
            "lane": LIVE_LANE,
            "label_pack": LIVE_LABEL_PACK,
            "queue_payload_model": LIVE_QUEUE_PAYLOAD_MODEL,
            "semantic_model": LIVE_SEMANTIC_MODEL,
            "reasoning_effort": LIVE_REASONING_EFFORT,
        },
        "expected_population": expected,
        "observed_population": population,
        "expected_population_matched": exact_population,
        "required_episode_ids": required_ids,
        "required_episode_ids_sha256": _sha256_bytes(required_json.encode()),
        "legacy_completed_episode_ids_to_rerun": rerun_ids,
        "legacy_completed_episode_ids_sha256": _sha256_bytes(
            _canonical_json(rerun_ids).encode()
        ),
        "failed_episode_ids_to_reset": failed_ids,
        "failed_episode_ids_sha256": _sha256_bytes(
            _canonical_json(failed_ids).encode()
        ),
        "new_episode_ids_to_enqueue": new_ids,
        "new_episode_ids_sha256": _sha256_bytes(
            _canonical_json(new_ids).encode()
        ),
        "active_episode_ids": active_ids,
        "duplicate_context_job_episode_ids": duplicate_episode_ids,
        "managed_lineage_present_requires_contract_verification": (
            managed_lineage_present
        ),
        "provisioning_sequence": [
            "snapshot_legacy_completed_attempt_history",
            "reset_legacy_completed_job_for_managed_rerun",
            "reset_failed_job_without_duplicate",
            "enqueue_missing_job_once",
        ],
        "provisioning_executed": False,
        "production_mutated": False,
        "live_model_call_count": 0,
        "privacy": "opaque_episode_ids_counts_and_source_volume_no_transcript_text",
    }
    unhashed = dict(payload)
    payload["plan_sha256"] = _sha256_bytes(_canonical_json(unhashed).encode())
    return payload


def verify_live_context_provision_plan(
    value: Mapping[str, Any], *, require_exact_live_population: bool = True
) -> dict[str, Any]:
    unhashed = dict(value)
    supplied = unhashed.pop("plan_sha256", None)
    observed = value.get("observed_population")
    required_ids = value.get("required_episode_ids")
    rerun_ids = value.get("legacy_completed_episode_ids_to_rerun")
    failed_ids = value.get("failed_episode_ids_to_reset")
    new_ids = value.get("new_episode_ids_to_enqueue")
    expected_live = {
        "required_episode_count": LIVE_REQUIRED_EPISODE_COUNT,
        "required_segment_count": LIVE_REQUIRED_SEGMENT_COUNT,
        "required_source_word_count": LIVE_REQUIRED_SOURCE_WORD_COUNT,
        "max_episode_source_word_count": LIVE_MAX_EPISODE_SOURCE_WORD_COUNT,
        "legacy_completed_context_rerun_count": LIVE_LEGACY_COMPLETED_RERUN_COUNT,
        "failed_context_job_reset_count": LIVE_FAILED_CONTEXT_RESET_COUNT,
        "new_context_job_count": LIVE_NEW_CONTEXT_JOB_COUNT,
        "attempt_row_count": 0,
    }
    if (
        value.get("schema_version") != LIVE_PROVISION_PLAN_VERSION
        or value.get("state") != "ready_read_only"
        or value.get("database_open_mode") != "sqlite_uri_mode_ro_query_only"
        or value.get("scope")
        != {
            "lane": LIVE_LANE,
            "label_pack": LIVE_LABEL_PACK,
            "queue_payload_model": LIVE_QUEUE_PAYLOAD_MODEL,
            "semantic_model": LIVE_SEMANTIC_MODEL,
            "reasoning_effort": LIVE_REASONING_EFFORT,
        }
        or not isinstance(observed, Mapping)
        or value.get("expected_population_matched") is not True
        or (require_exact_live_population and dict(observed) != expected_live)
        or (require_exact_live_population and value.get("expected_population") != expected_live)
        or not isinstance(required_ids, list)
        or len(required_ids) != int(observed.get("required_episode_count", -1))
        or len(set(required_ids)) != len(required_ids)
        or required_ids != sorted(required_ids)
        or value.get("required_episode_ids_sha256")
        != _sha256_bytes(_canonical_json(required_ids).encode())
        or not isinstance(rerun_ids, list)
        or len(rerun_ids) != int(observed.get("legacy_completed_context_rerun_count", -1))
        or value.get("legacy_completed_episode_ids_sha256")
        != _sha256_bytes(_canonical_json(rerun_ids).encode())
        or not isinstance(failed_ids, list)
        or len(failed_ids) != int(observed.get("failed_context_job_reset_count", -1))
        or value.get("failed_episode_ids_sha256")
        != _sha256_bytes(_canonical_json(failed_ids).encode())
        or not isinstance(new_ids, list)
        or len(new_ids) != int(observed.get("new_context_job_count", -1))
        or value.get("new_episode_ids_sha256")
        != _sha256_bytes(_canonical_json(new_ids).encode())
        or set(required_ids) != set(rerun_ids) | set(failed_ids) | set(new_ids)
        or set(rerun_ids) & set(failed_ids)
        or set(rerun_ids) & set(new_ids)
        or set(failed_ids) & set(new_ids)
        or value.get("active_episode_ids") != []
        or value.get("duplicate_context_job_episode_ids") != []
        or value.get("managed_lineage_present_requires_contract_verification") != []
        or value.get("provisioning_executed") is not False
        or value.get("production_mutated") is not False
        or value.get("live_model_call_count") != 0
        or not _is_sha256(supplied)
        or supplied != _sha256_bytes(_canonical_json(unhashed).encode())
    ):
        raise EpisodeContextRunnerError("live context provision plan drifted")
    return dict(value)


def _context_table_counts(connection: sqlite3.Connection) -> dict[str, int]:
    return {
        table: int(connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])
        for table in (
            "jobs",
            "episode_context_runs",
            "episode_context_run_attempts",
            "labels",
        )
    }


def _legacy_snapshot_records(
    connection: sqlite3.Connection, *, expected_episode_ids: Sequence[str]
) -> dict[str, Any]:
    expected = sorted(str(item) for item in expected_episode_ids)
    rows = _query_rows(
        connection,
        """
        SELECT episode_id, canonical_snapshot_json, artifact_records_json
        FROM episode_context_run_attempts
        WHERE attempt_kind = 'legacy_canonical_snapshot'
          AND status = 'superseded'
          AND episode_id IN (SELECT CAST(value AS TEXT) FROM json_each(?))
        ORDER BY episode_id
        """,
        (_canonical_json(expected),),
    )
    if [str(row["episode_id"]) for row in rows] != expected:
        raise EpisodeContextRunnerError(
            "legacy context snapshot population drifted"
        )
    snapshots: dict[str, Any] = {}
    for row in rows:
        episode_id = str(row["episode_id"])
        snapshot = loads_json(str(row["canonical_snapshot_json"] or "{}"), {})
        records = loads_json(str(row["artifact_records_json"] or "{}"), {})
        if (
            not isinstance(snapshot, Mapping)
            or str(snapshot.get("episode_id") or "") != episode_id
            or not isinstance(records, Mapping)
            or set(records) != {"prompt_path", "output_path", "context_artifact_path"}
        ):
            raise EpisodeContextRunnerError(
                "legacy context snapshot lineage is malformed"
            )
        verified_records = {
            key: _record(_verify_record(value, label=f"legacy snapshot {key}"))
            for key, value in records.items()
        }
        snapshots[episode_id] = {
            "canonical_snapshot_sha256": _sha256_bytes(
                _canonical_json(dict(snapshot)).encode()
            ),
            "artifact_records": verified_records,
        }
    return snapshots


def _validate_operator_authority(
    *, operator_authorization_id: Any, authorized_by: Any
) -> tuple[str, str]:
    if (
        authorized_by != "kolby"
        or not isinstance(operator_authorization_id, str)
        or not 8 <= len(operator_authorization_id) <= 128
        or not all(
            character.isalnum() or character in "_-"
            for character in operator_authorization_id
        )
    ):
        raise EpisodeContextRunnerError(
            "explicit Kolby operator authorization is invalid"
        )
    return operator_authorization_id, str(authorized_by)


def load_live_context_provision_authorization(
    path: Path,
    *,
    contract_path: Path,
    migration_plan_path: Path,
    cutover_receipt_path: Path,
    provision_plan_path: Path,
) -> dict[str, Any]:
    authorization_path = path.expanduser().resolve()
    value = _load_json(
        authorization_path, label="live context provision authorization"
    )
    migration_record = _record(migration_plan_path)
    cutover_record = _record(cutover_receipt_path)
    provision_record = _record(provision_plan_path)
    provision = verify_live_context_provision_plan(
        _load_json(provision_plan_path, label="live context provision plan")
    )
    cutover = verify_attempt_schema_cutover_receipt(
        _load_json(cutover_receipt_path, label="attempt-schema cutover receipt")
    )
    migration = verify_attempt_schema_migration_plan(
        _load_json(migration_plan_path, label="attempt-schema migration plan")
    )
    expected_operations = {
        "legacy_completed_context_rerun_count": provision["observed_population"][
            "legacy_completed_context_rerun_count"
        ],
        "failed_context_job_reset_count": provision["observed_population"][
            "failed_context_job_reset_count"
        ],
        "new_context_job_count": provision["observed_population"][
            "new_context_job_count"
        ],
        "managed_context_attempt_row_count_before": 0,
        "semantic_model_call_count": 0,
    }
    expected_keys = {
        "schema_version",
        "state",
        "phase",
        "authorized_by",
        "operator_authorization_id",
        "database",
        "contract",
        "migration_plan",
        "pre_cutover_receipt",
        "provision_plan",
        "expected_operations",
        "production_mutation_allowed",
        "semantic_model_calls_allowed",
        "single_writer_required",
        "runner_source",
    }
    if (
        set(value) != expected_keys
        or value.get("schema_version") != LIVE_PROVISION_AUTHORIZATION_VERSION
        or value.get("state") != "frozen_authorized"
        or value.get("phase") != "attempt_schema_cutover_and_context_provision"
        or value.get("authorized_by") != "kolby"
        or _validate_operator_authority(
            operator_authorization_id=value.get("operator_authorization_id"),
            authorized_by=value.get("authorized_by"),
        )[0]
        != value.get("operator_authorization_id")
        or value.get("database") != provision["database"]
        or value.get("database") != cutover["database"]
        or value.get("database") != migration["database"]
        or value.get("contract") != _record(contract_path)
        or value.get("migration_plan") != migration_record
        or value.get("pre_cutover_receipt") != cutover_record
        or value.get("provision_plan") != provision_record
        or value.get("expected_operations") != expected_operations
        or value.get("production_mutation_allowed") is not True
        or value.get("semantic_model_calls_allowed") is not False
        or value.get("single_writer_required") is not True
        or value.get("runner_source") != _record(Path(__file__))
    ):
        raise EpisodeContextRunnerError(
            "live context provision authorization drifted"
        )
    return {
        "path": authorization_path,
        "authorization": value,
        "database": _validate_database_identity(
            value["database"], label="live context provision authorization"
        ),
        "provision_plan": provision,
        "cutover": cutover,
        "migration_plan_path": migration_plan_path.expanduser().resolve(),
        "cutover_path": cutover_receipt_path.expanduser().resolve(),
        "provision_plan_path": provision_plan_path.expanduser().resolve(),
    }


def build_live_context_provision_authorization_artifact(
    *,
    contract_path: Path,
    migration_plan_path: Path,
    cutover_receipt_path: Path,
    provision_plan_path: Path,
    output_path: Path,
    operator_authorization_id: str,
    authorized_by: str,
) -> dict[str, Any]:
    """Freeze a no-model control artifact; never open SQLite for mutation."""

    authorization_id, authority = _validate_operator_authority(
        operator_authorization_id=operator_authorization_id,
        authorized_by=authorized_by,
    )
    load_episode_context_contract(contract_path)
    migration = verify_attempt_schema_migration_plan(
        _load_json(migration_plan_path, label="attempt-schema migration plan")
    )
    cutover = verify_attempt_schema_cutover_receipt(
        _load_json(cutover_receipt_path, label="attempt-schema cutover receipt")
    )
    provision = verify_live_context_provision_plan(
        _load_json(provision_plan_path, label="live context provision plan")
    )
    if (
        cutover["database"] != provision["database"]
        or migration["database"] != provision["database"]
    ):
        raise EpisodeContextRunnerError(
            "provision authorization database lineage drifted"
        )
    payload = {
        "schema_version": LIVE_PROVISION_AUTHORIZATION_VERSION,
        "state": "frozen_authorized",
        "phase": "attempt_schema_cutover_and_context_provision",
        "authorized_by": authority,
        "operator_authorization_id": authorization_id,
        "database": provision["database"],
        "contract": _record(contract_path),
        "migration_plan": _record(migration_plan_path),
        "pre_cutover_receipt": _record(cutover_receipt_path),
        "provision_plan": _record(provision_plan_path),
        "expected_operations": {
            "legacy_completed_context_rerun_count": provision[
                "observed_population"
            ]["legacy_completed_context_rerun_count"],
            "failed_context_job_reset_count": provision["observed_population"]
            ["failed_context_job_reset_count"],
            "new_context_job_count": provision["observed_population"]
            ["new_context_job_count"],
            "managed_context_attempt_row_count_before": 0,
            "semantic_model_call_count": 0,
        },
        "production_mutation_allowed": True,
        "semantic_model_calls_allowed": False,
        "single_writer_required": True,
        "runner_source": _record(Path(__file__)),
    }
    target = output_path.expanduser().resolve()
    _freeze_validated_control_artifact(
        target,
        payload,
        lambda candidate: load_live_context_provision_authorization(
            candidate,
            contract_path=contract_path,
            migration_plan_path=migration_plan_path,
            cutover_receipt_path=cutover_receipt_path,
            provision_plan_path=provision_plan_path,
        ),
    )
    return {
        "state": "frozen_authorized",
        "phase": "attempt_schema_cutover_and_context_provision",
        "operator_authorization_id": authorization_id,
        "authorization": _record(target),
        "production_mutated": False,
        "semantic_model_call_count": 0,
    }


def _apply_attempt_schema_migration(
    connection: sqlite3.Connection,
    *,
    migration_plan: Mapping[str, Any],
) -> bool:
    verified = verify_attempt_schema_migration_plan(migration_plan)
    observed = _attempt_schema_observation(connection)
    if (
        observed["columns"] == _ATTEMPT_TARGET_COLUMNS
        and observed["foreign_keys"] == _ATTEMPT_TARGET_FOREIGN_KEYS
        and all(observed["constraints"].values())
    ):
        return False
    if (
        verified["state"]
        != "ready_for_separately_authorized_empty_table_cutover"
        or observed["columns"] != _ATTEMPT_EXISTING_COLUMNS
        or observed["attempt_row_count"] != 0
        or connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' "
            "AND name='episode_context_run_attempts_preproduction_v1'"
        ).fetchone()
        is not None
    ):
        raise EpisodeContextRunnerError(
            "attempt-schema migration preconditions drifted"
        )
    try:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            "ALTER TABLE episode_context_run_attempts "
            "RENAME TO episode_context_run_attempts_preproduction_v1"
        )
        connection.execute(_ATTEMPT_TARGET_TABLE_SQL)
        connection.execute(_ATTEMPT_ACTIVE_INDEX_SQL)
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    after = _attempt_schema_observation(connection)
    if (
        after["columns"] != _ATTEMPT_TARGET_COLUMNS
        or after["foreign_keys"] != _ATTEMPT_TARGET_FOREIGN_KEYS
        or not all(after["constraints"].values())
        or after["attempt_row_count"] != 0
    ):
        raise EpisodeContextRunnerError("attempt-schema migration verification failed")
    return True


def _verify_live_provision_post_state(
    connection: sqlite3.Connection,
    *,
    plan: Mapping[str, Any],
    baseline_counts: Mapping[str, int],
) -> dict[str, Any]:
    required_ids = [str(item) for item in plan["required_episode_ids"]]
    rows = _query_rows(
        connection,
        f"""
        SELECT target_id, status
        FROM jobs AS context_jobs
        WHERE lane = ? AND job_type = 'episode_context'
          AND json_extract(payload_json, '$.label_pack') = ?
          {_queue_model_scope_sql('context_jobs')}
          AND target_id IN (SELECT CAST(value AS TEXT) FROM json_each(?))
        ORDER BY target_id
        """,
        (
            LIVE_LANE,
            LIVE_LABEL_PACK,
            LIVE_QUEUE_PAYLOAD_MODEL,
            LIVE_QUEUE_PAYLOAD_MODEL,
            _canonical_json(required_ids),
        ),
    )
    if (
        len(rows) != len(required_ids)
        or [str(row["target_id"]) for row in rows] != required_ids
        or any(str(row["status"]) != "pending" for row in rows)
    ):
        raise EpisodeContextRunnerError(
            "live context provisioned queue population drifted"
        )
    snapshots = _legacy_snapshot_records(
        connection,
        expected_episode_ids=plan["legacy_completed_episode_ids_to_rerun"],
    )
    counts = _context_table_counts(connection)
    expected_deltas = {
        "jobs": int(plan["observed_population"]["new_context_job_count"]),
        "episode_context_runs": 0,
        "episode_context_run_attempts": int(
            plan["observed_population"]["legacy_completed_context_rerun_count"]
        ),
        "labels": 0,
    }
    observed_deltas = {
        key: counts[key] - int(baseline_counts[key]) for key in expected_deltas
    }
    if observed_deltas != expected_deltas:
        raise EpisodeContextRunnerError(
            "live context provision table-count deltas drifted"
        )
    return {
        "table_counts": counts,
        "table_count_deltas": observed_deltas,
        "legacy_snapshots": snapshots,
        "legacy_snapshots_sha256": _sha256_bytes(
            _canonical_json(snapshots).encode()
        ),
        "provisioned_episode_count": len(rows),
    }


def _contained(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def _expected_record(
    value: str | os.PathLike[str] | Mapping[str, Any], *, label: str
) -> dict[str, Any]:
    if isinstance(value, (str, os.PathLike)):
        return _record(Path(value))
    path = _verify_record(value, label=label)
    return _record(path)


def _implementation_binding(value: Any) -> dict[str, Any]:
    source_file = inspect.getsourcefile(value)
    if source_file is None:
        raise EpisodeContextRunnerError("runtime implementation source is unavailable")
    source = inspect.getsource(value).encode("utf-8")
    return {
        "qualified_name": f"{value.__module__}.{value.__qualname__}",
        "source_file": _record(Path(source_file)),
        "implementation_sha256": _sha256_bytes(source),
    }


def _reject_live_credential_environment() -> None:
    forbidden = (
        "OPENAI_API_KEY",
        "CODEX_API_KEY",
        "OPENAI_ACCESS_TOKEN",
        "CHATGPT_ACCESS_TOKEN",
        "CODEX_AUTH_TOKEN",
    )
    present = [name for name in forbidden if os.environ.get(name)]
    if present:
        raise EpisodeContextRunnerError(
            "API-key or raw-token environment must be absent for managed ChatGPT auth"
        )


def _validate_schema_value(schema: Mapping[str, Any], value: Any, *, path: str) -> None:
    expected = schema.get("type")
    if isinstance(expected, list):
        for item_type in expected:
            try:
                _validate_schema_value({**schema, "type": item_type}, value, path=path)
                return
            except EpisodeContextRunnerWaiting:
                continue
        raise EpisodeContextRunnerWaiting(f"{path} does not match an allowed schema type")
    if expected == "object":
        if not isinstance(value, dict):
            raise EpisodeContextRunnerWaiting(f"{path} must be an object")
        properties = schema.get("properties")
        if not isinstance(properties, Mapping):
            properties = {}
        required = schema.get("required")
        if not isinstance(required, list):
            required = []
        for key in required:
            if key not in value:
                raise EpisodeContextRunnerWaiting(f"{path}.{key} is required")
        if schema.get("additionalProperties") is False and set(value) - set(properties):
            raise EpisodeContextRunnerWaiting(f"{path} contains unexpected fields")
        for key, child in properties.items():
            if key in value and isinstance(child, Mapping):
                _validate_schema_value(child, value[key], path=f"{path}.{key}")
    elif expected == "array":
        if not isinstance(value, list):
            raise EpisodeContextRunnerWaiting(f"{path} must be an array")
        if "minItems" in schema and len(value) < int(schema["minItems"]):
            raise EpisodeContextRunnerWaiting(f"{path} is below minItems")
        if "maxItems" in schema and len(value) > int(schema["maxItems"]):
            raise EpisodeContextRunnerWaiting(f"{path} exceeds maxItems")
        child = schema.get("items")
        if isinstance(child, Mapping):
            for index, item in enumerate(value):
                _validate_schema_value(child, item, path=f"{path}[{index}]")
    elif expected == "string" and not isinstance(value, str):
        raise EpisodeContextRunnerWaiting(f"{path} must be a string")
    elif expected == "boolean" and not isinstance(value, bool):
        raise EpisodeContextRunnerWaiting(f"{path} must be a boolean")
    elif expected == "integer" and (isinstance(value, bool) or not isinstance(value, int)):
        raise EpisodeContextRunnerWaiting(f"{path} must be an integer")
    elif expected == "number" and (
        isinstance(value, bool) or not isinstance(value, (int, float))
    ):
        raise EpisodeContextRunnerWaiting(f"{path} must be a number")
    elif expected == "null" and value is not None:
        raise EpisodeContextRunnerWaiting(f"{path} must be null")
    if "const" in schema and value != schema["const"]:
        raise EpisodeContextRunnerWaiting(f"{path} does not match const")
    if "enum" in schema and value not in schema["enum"]:
        raise EpisodeContextRunnerWaiting(f"{path} is outside enum")
    if expected in {"integer", "number"} and isinstance(value, (int, float)):
        if "minimum" in schema and value < schema["minimum"]:
            raise EpisodeContextRunnerWaiting(f"{path} is below minimum")
        if "maximum" in schema and value > schema["maximum"]:
            raise EpisodeContextRunnerWaiting(f"{path} exceeds maximum")


def _validate_output_schema(schema: Mapping[str, Any]) -> None:
    properties = schema.get("properties")
    required = schema.get("required")
    episode_context = properties.get("episode_context") if isinstance(properties, Mapping) else None
    nested_properties = (
        episode_context.get("properties") if isinstance(episode_context, Mapping) else None
    )
    nested_required = (
        episode_context.get("required") if isinstance(episode_context, Mapping) else None
    )
    exclusions = properties.get("excluded_source_context") if isinstance(properties, Mapping) else None
    persisted_types = {
        "context_summary": "string",
        "speaker_map": "array",
        "section_map": "array",
        "entity_seed": "object",
        "concept_seed": "array",
    }
    if (
        schema.get("type") != "object"
        or schema.get("additionalProperties") is not False
        or not isinstance(properties, Mapping)
        or not isinstance(required, list)
        or properties.get("episode_id", {}).get("type") != "string"
        or "episode_id" not in required
        or any(field not in required or field not in properties for field in SEMANTIC_AUTHORITY_FIELDS)
        or not isinstance(episode_context, Mapping)
        or episode_context.get("type") != "object"
        or episode_context.get("additionalProperties") is not False
        or not isinstance(nested_properties, Mapping)
        or not isinstance(nested_required, list)
        or any(
            field not in required
            or field not in properties
            or properties[field].get("type") != expected_type
            or field not in nested_required
            or field not in nested_properties
            or nested_properties[field].get("type") != expected_type
            for field, expected_type in persisted_types.items()
        )
        or properties.get("extraction_guidance", {}).get("type") != "string"
        or not isinstance(exclusions, Mapping)
        or exclusions.get("type") != "array"
        or not isinstance(exclusions.get("items"), Mapping)
        or exclusions["items"].get("type") != "string"
        or "schema_version" not in required
        or properties.get("schema_version", {}).get("const")
        != EPISODE_CONTEXT_SCHEMA_VERSION
        or any(
            field not in required
            for field in (
                "quality_flags",
                "overall_confidence",
                "needs_review",
                "review_reason",
            )
        )
        or properties.get("quality_flags", {}).get("type") != "array"
        or properties.get("overall_confidence", {}).get("type") != "number"
        or properties.get("needs_review", {}).get("type") != "boolean"
        or properties.get("review_reason", {}).get("type") != ["string", "null"]
    ):
        raise EpisodeContextRunnerError(
            "frozen output schema does not preserve the required LLM semantic authority"
        )


def _validate_canonical_output(
    value: Mapping[str, Any], *, expected_episode_id: str
) -> dict[str, Any]:
    try:
        artifact = validate_episode_context_output(
            dict(value), expected_episode_id=expected_episode_id
        )
    except (TypeError, ValueError) as exc:
        raise EpisodeContextRunnerWaiting(
            "episode-context output failed canonical persistence validation"
        ) from exc
    nested = artifact.get("episode_context")
    exclusions = artifact.get("excluded_source_context")
    if (
        not isinstance(nested, Mapping)
        or not isinstance(artifact.get("extraction_guidance"), str)
        or not isinstance(exclusions, list)
        or any(not isinstance(item, str) for item in exclusions)
        or any(nested.get(field) != artifact.get(field) for field in PERSISTED_CONTEXT_FIELDS)
    ):
        raise EpisodeContextRunnerWaiting(
            "episode-context semantic authority fields do not match persisted maps"
        )
    return artifact


def _semantic_fields(value: Mapping[str, Any]) -> dict[str, Any]:
    return {field: value[field] for field in SEMANTIC_AUTHORITY_FIELDS}


def _valid_usage(value: Any) -> dict[str, int]:
    if not isinstance(value, Mapping):
        raise EpisodeContextRunnerWaiting("turn sidecar has unknown usage")
    result: dict[str, int] = {}
    for field in USAGE_FIELDS:
        item = value.get(field)
        if isinstance(item, bool) or not isinstance(item, int) or item < 0:
            raise EpisodeContextRunnerWaiting("turn sidecar has malformed usage")
        result[field] = item
    if (
        result["total_tokens"] != result["input_tokens"] + result["output_tokens"]
        or result["cached_input_tokens"] > result["input_tokens"]
        or result["reasoning_output_tokens"] > result["output_tokens"]
    ):
        raise EpisodeContextRunnerWaiting("turn sidecar usage accounting is inconsistent")
    return result


def _required_manifest_payload(
    *, loaded: Mapping[str, Any], episode_ids: Sequence[str]
) -> dict[str, Any]:
    normalized = sorted(set(str(item) for item in episode_ids))
    if any(not item for item in normalized):
        raise EpisodeContextRunnerError("required episode manifest contains an empty id")
    configuration = loaded["configuration"]
    payload = {
        "schema_version": REQUIRED_EPISODE_MANIFEST_VERSION,
        "state": "frozen",
        "contract_sha256": loaded["sha256"],
        "frozen_configuration_sha256": loaded["configuration_sha256"],
        "lane": configuration["lane"],
        "label_pack": configuration["label_pack"],
        "model": configuration["model"],
        "queue_payload_model": configuration["queue_payload_model"],
        "model_policy": "null_or_exact_frozen_queue_payload_model",
        "label_job_status_policy": "pending_claimed_failed_only",
        "context_job_status_policy": "cannot_expand_label_derived_manifest",
        "episode_ids": normalized,
        "episode_count": len(normalized),
        "episode_ids_sha256": _sha256_bytes(_canonical_json(normalized).encode()),
    }
    payload["manifest_sha256"] = _sha256_bytes(_canonical_json(payload).encode())
    return payload


def _validate_required_manifest(
    value: Any, *, loaded: Mapping[str, Any]
) -> dict[str, Any]:
    expected = {
        "schema_version",
        "state",
        "contract_sha256",
        "frozen_configuration_sha256",
        "lane",
        "label_pack",
        "model",
        "queue_payload_model",
        "model_policy",
        "label_job_status_policy",
        "context_job_status_policy",
        "episode_ids",
        "episode_count",
        "episode_ids_sha256",
        "manifest_sha256",
    }
    configuration = loaded["configuration"]
    if not isinstance(value, Mapping) or set(value) != expected:
        raise EpisodeContextRunnerError("required episode manifest shape drifted")
    episode_ids = value.get("episode_ids")
    if (
        value.get("schema_version") != REQUIRED_EPISODE_MANIFEST_VERSION
        or value.get("state") != "frozen"
        or value.get("contract_sha256") != loaded["sha256"]
        or value.get("frozen_configuration_sha256")
        != loaded["configuration_sha256"]
        or value.get("lane") != configuration["lane"]
        or value.get("label_pack") != configuration["label_pack"]
        or value.get("model") != configuration["model"]
        or value.get("queue_payload_model") != configuration["queue_payload_model"]
        or value.get("model_policy")
        != "null_or_exact_frozen_queue_payload_model"
        or value.get("label_job_status_policy") != "pending_claimed_failed_only"
        or value.get("context_job_status_policy")
        != "cannot_expand_label_derived_manifest"
        or not isinstance(episode_ids, list)
        or any(not isinstance(item, str) or not item for item in episode_ids)
        or episode_ids != sorted(set(episode_ids))
        or value.get("episode_count") != len(episode_ids)
        or value.get("episode_ids_sha256")
        != _sha256_bytes(_canonical_json(episode_ids).encode())
    ):
        raise EpisodeContextRunnerError("required episode manifest values drifted")
    unhashed = dict(value)
    supplied = unhashed.pop("manifest_sha256")
    if not _is_sha256(supplied) or supplied != _sha256_bytes(
        _canonical_json(unhashed).encode()
    ):
        raise EpisodeContextRunnerError("required episode manifest hash drifted")
    return dict(value)


def _freeze_required_manifest(
    *, root: Path, loaded: Mapping[str, Any], episode_ids: Sequence[str]
) -> tuple[dict[str, Any], dict[str, Any]]:
    payload = _required_manifest_payload(loaded=loaded, episode_ids=episode_ids)
    path = root / "required-episode-manifest.json"
    _write_immutable(path, _pretty_json(payload).encode())
    persisted = _load_json(path, label="required episode manifest")
    verified = _validate_required_manifest(persisted, loaded=loaded)
    return verified, _record(path)


def _capacity_request_payload(
    *,
    loaded: Mapping[str, Any],
    run_id: str,
    backlog: Mapping[str, Any],
    required_episode_manifest: Mapping[str, Any],
    items: Sequence[EpisodeContextItem],
) -> dict[str, Any]:
    identities = [
        {"job_id": item.job_id, "episode_id": item.episode_id} for item in items
    ]
    payload = {
        "schema_version": CAPACITY_REQUEST_VERSION,
        "state": "requested",
        "fixture_only": True,
        "run_id": run_id,
        "contract_sha256": loaded["sha256"],
        "frozen_configuration_sha256": loaded["configuration_sha256"],
        "required_episode_manifest": dict(required_episode_manifest),
        "backlog_snapshot_sha256": backlog["snapshot_sha256"],
        "remaining_episode_context_jobs": backlog[
            "remaining_episode_context_jobs"
        ],
        "requested_items": identities,
        "requested_items_sha256": _sha256_bytes(
            _canonical_json(identities).encode()
        ),
        "requested_turn_count": len(identities),
        "max_concurrency": 1,
        "one_turn_per_episode": True,
        "admission_before_claim": True,
    }
    payload["request_sha256"] = _sha256_bytes(_canonical_json(payload).encode())
    return payload


def build_fixture_capacity_admission(
    request: Mapping[str, Any], *, admission_id: str = "fixture-reserve-admission"
) -> dict[str, Any]:
    """Build an exact fixture receipt for a capacity request.

    This helper does not probe or reserve real service capacity.  Its explicit
    fixture marker lets tests exercise the same hash-bound admission boundary
    while the live runner remains disabled.
    """

    return {
        "schema_version": CAPACITY_ADMISSION_VERSION,
        "state": "admitted",
        "fixture_only": True,
        "admission_id": admission_id,
        "request_sha256": request.get("request_sha256"),
        "contract_sha256": request.get("contract_sha256"),
        "frozen_configuration_sha256": request.get(
            "frozen_configuration_sha256"
        ),
        "required_episode_manifest": request.get("required_episode_manifest"),
        "backlog_snapshot_sha256": request.get("backlog_snapshot_sha256"),
        "admitted_items": request.get("requested_items"),
        "admitted_items_sha256": request.get("requested_items_sha256"),
        "admitted_turn_count": request.get("requested_turn_count"),
        "max_concurrency": 1,
        "one_turn_per_episode": True,
        "admission_before_claim": True,
        "reserve_verified": True,
    }


def _validate_capacity_request(value: Any, *, loaded: Mapping[str, Any]) -> dict[str, Any]:
    expected = {
        "schema_version",
        "state",
        "fixture_only",
        "run_id",
        "contract_sha256",
        "frozen_configuration_sha256",
        "required_episode_manifest",
        "backlog_snapshot_sha256",
        "remaining_episode_context_jobs",
        "requested_items",
        "requested_items_sha256",
        "requested_turn_count",
        "max_concurrency",
        "one_turn_per_episode",
        "admission_before_claim",
        "request_sha256",
    }
    if not isinstance(value, Mapping) or set(value) != expected:
        raise EpisodeContextRunnerWaiting("capacity request shape drifted")
    items = value.get("requested_items")
    unhashed = dict(value)
    supplied = unhashed.pop("request_sha256")
    if (
        value.get("schema_version") != CAPACITY_REQUEST_VERSION
        or value.get("state") != "requested"
        or value.get("fixture_only") is not True
        or not isinstance(value.get("run_id"), str)
        or not value["run_id"]
        or value.get("contract_sha256") != loaded["sha256"]
        or value.get("frozen_configuration_sha256")
        != loaded["configuration_sha256"]
        or not isinstance(items, list)
        or not items
        or any(
            not isinstance(item, Mapping)
            or set(item) != {"job_id", "episode_id"}
            or isinstance(item.get("job_id"), bool)
            or not isinstance(item.get("job_id"), int)
            or item["job_id"] < 1
            or not isinstance(item.get("episode_id"), str)
            or not item["episode_id"]
            for item in items
        )
        or len({(item["job_id"], item["episode_id"]) for item in items})
        != len(items)
        or value.get("requested_items_sha256")
        != _sha256_bytes(_canonical_json(items).encode())
        or value.get("requested_turn_count") != len(items)
        or value.get("max_concurrency") != 1
        or value.get("one_turn_per_episode") is not True
        or value.get("admission_before_claim") is not True
        or isinstance(value.get("remaining_episode_context_jobs"), bool)
        or not isinstance(value.get("remaining_episode_context_jobs"), int)
        or value["remaining_episode_context_jobs"] < len(items)
        or not _is_sha256(value.get("backlog_snapshot_sha256"))
        or not _is_sha256(supplied)
        or supplied != _sha256_bytes(_canonical_json(unhashed).encode())
    ):
        raise EpisodeContextRunnerWaiting("capacity request values drifted")
    return dict(value)


def _validate_capacity_admission(
    value: Any, *, request: Mapping[str, Any], loaded: Mapping[str, Any]
) -> dict[str, Any]:
    expected = {
        "schema_version",
        "state",
        "fixture_only",
        "admission_id",
        "request_sha256",
        "contract_sha256",
        "frozen_configuration_sha256",
        "required_episode_manifest",
        "backlog_snapshot_sha256",
        "admitted_items",
        "admitted_items_sha256",
        "admitted_turn_count",
        "max_concurrency",
        "one_turn_per_episode",
        "admission_before_claim",
        "reserve_verified",
    }
    if (
        not isinstance(value, Mapping)
        or set(value) != expected
        or value.get("schema_version") != CAPACITY_ADMISSION_VERSION
        or value.get("state") != "admitted"
        or value.get("fixture_only") is not True
        or not isinstance(value.get("admission_id"), str)
        or not value["admission_id"]
        or value.get("request_sha256") != request["request_sha256"]
        or value.get("contract_sha256") != loaded["sha256"]
        or value.get("frozen_configuration_sha256")
        != loaded["configuration_sha256"]
        or value.get("required_episode_manifest")
        != request["required_episode_manifest"]
        or value.get("backlog_snapshot_sha256")
        != request["backlog_snapshot_sha256"]
        or value.get("admitted_items") != request["requested_items"]
        or value.get("admitted_items_sha256")
        != request["requested_items_sha256"]
        or value.get("admitted_turn_count") != request["requested_turn_count"]
        or value.get("max_concurrency") != 1
        or value.get("one_turn_per_episode") is not True
        or value.get("admission_before_claim") is not True
        or value.get("reserve_verified") is not True
    ):
        raise EpisodeContextRunnerWaiting(
            "exact reserve-capacity admission was not verified"
        )
    return dict(value)


def _persist_capacity_admission(
    *,
    root: Path,
    loaded: Mapping[str, Any],
    request: Mapping[str, Any],
    provider: EpisodeContextCapacityAdmissionProvider,
) -> tuple[dict[str, Any], dict[str, Any]]:
    request_payload = _validate_capacity_request(request, loaded=loaded)
    request_path = root / "capacity" / f"{request_payload['run_id']}.request.json"
    _write_immutable(request_path, _pretty_json(request_payload).encode())
    supplied = provider(request=request_payload)
    if isinstance(supplied, Path):
        admission = _load_json(supplied.expanduser().resolve(), label="capacity admission")
    elif isinstance(supplied, Mapping):
        admission = dict(supplied)
    else:
        raise EpisodeContextRunnerWaiting("capacity provider returned no admission")
    verified = _validate_capacity_admission(
        admission, request=request_payload, loaded=loaded
    )
    admission_path = root / "capacity" / f"{request_payload['run_id']}.admission.json"
    _write_immutable(admission_path, _pretty_json(verified).encode())
    return verified, {
        "request": _record(request_path),
        "admission": _record(admission_path),
    }


def _verify_capacity_binding(
    value: Any, *, loaded: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    if not isinstance(value, Mapping) or set(value) != {"request", "admission"}:
        raise EpisodeContextRunnerError("capacity binding shape drifted")
    request_path = _verify_record(value["request"], label="capacity request")
    admission_path = _verify_record(value["admission"], label="capacity admission")
    request = _validate_capacity_request(
        _load_json(request_path, label="capacity request"), loaded=loaded
    )
    admission = _validate_capacity_admission(
        _load_json(admission_path, label="capacity admission"),
        request=request,
        loaded=loaded,
    )
    return request, admission


def _validate_live_capacity_policy(
    value: Any, *, provision_plan: Mapping[str, Any]
) -> dict[str, Any]:
    expected = {
        "schema_version",
        "minimum_remaining_reserve_percent",
        "quota_points_per_million_tokens",
        "maximum_total_tokens_per_episode",
        "population_episode_count",
        "population_source_segment_count",
        "population_source_word_count",
        "max_episode_source_word_count",
        "population_total_token_bound",
        "admission_scope",
        "multi_window_execution_required",
        "managed_chatgpt_auth_only",
        "official_persistent_codex_app_server_only",
        "rate_limit_reached_type_must_be_null",
        "unknown_usage_hard_stop",
        "retry_count_per_episode",
    }
    observed = provision_plan["observed_population"]
    integers = {
        field: value.get(field) if isinstance(value, Mapping) else None
        for field in (
            "minimum_remaining_reserve_percent",
            "quota_points_per_million_tokens",
            "maximum_total_tokens_per_episode",
            "population_episode_count",
            "population_source_segment_count",
            "population_source_word_count",
            "max_episode_source_word_count",
            "population_total_token_bound",
        )
    }
    if (
        not isinstance(value, Mapping)
        or set(value) != expected
        or value.get("schema_version") != LIVE_CAPACITY_POLICY_VERSION
        or any(
            isinstance(item, bool) or not isinstance(item, int) or item <= 0
            for item in integers.values()
        )
        or integers["minimum_remaining_reserve_percent"] < 20
        or integers["population_episode_count"]
        != observed["required_episode_count"]
        or integers["population_source_segment_count"]
        != observed["required_segment_count"]
        or integers["population_source_word_count"]
        != observed["required_source_word_count"]
        or integers["max_episode_source_word_count"]
        != observed["max_episode_source_word_count"]
        or integers["population_total_token_bound"]
        != integers["population_episode_count"]
        * integers["maximum_total_tokens_per_episode"]
        or value.get("admission_scope")
        != "one_fresh_no_thread_probe_immediately_before_each_episode_claim_thread_and_turn"
        or value.get("multi_window_execution_required") is not True
        or value.get("managed_chatgpt_auth_only") is not True
        or value.get("official_persistent_codex_app_server_only") is not True
        or value.get("rate_limit_reached_type_must_be_null") is not True
        or value.get("unknown_usage_hard_stop") is not True
        or value.get("retry_count_per_episode") != 0
    ):
        raise EpisodeContextRunnerError("live context capacity policy drifted")
    return dict(value)


def _live_capacity_request_payload(
    *,
    loaded: Mapping[str, Any],
    run_id: str,
    ordinal: int,
    admission_stage: str,
    item: EpisodeContextItem,
    backlog: Mapping[str, Any],
    required_episode_manifest: Mapping[str, Any],
    provision_plan_record: Mapping[str, Any],
    capacity_policy_record: Mapping[str, Any],
    runtime_authorization_record: Mapping[str, Any],
    policy: Mapping[str, Any],
    cumulative_measured_total_tokens: int,
) -> dict[str, Any]:
    if admission_stage not in {"preclaim", "preturn"}:
        raise EpisodeContextRunnerError("live capacity admission stage is invalid")
    identity = {
        "job_id": item.job_id,
        "episode_id": item.episode_id,
        "claim_id": item.claim_id if admission_stage == "preturn" else None,
        "attempt_id": item.attempt_id if admission_stage == "preturn" else None,
    }
    requested_source_input_sha256 = (
        _sha256_bytes(_canonical_json(dict(item.source_input)).encode())
        if admission_stage == "preturn"
        else None
    )
    remaining = int(backlog["missing_required_episode_count"])
    payload = {
        "schema_version": LIVE_CAPACITY_REQUEST_VERSION,
        "state": "requested",
        "fixture_only": False,
        "run_id": run_id,
        "episode_ordinal": ordinal,
        "admission_stage": admission_stage,
        "contract_sha256": loaded["sha256"],
        "frozen_configuration_sha256": loaded["configuration_sha256"],
        "required_episode_manifest": dict(required_episode_manifest),
        "provision_plan": dict(provision_plan_record),
        "capacity_policy": dict(capacity_policy_record),
        "runtime_authorization": dict(runtime_authorization_record),
        "backlog_snapshot_sha256": backlog["snapshot_sha256"],
        "remaining_population_episode_count": remaining,
        "requested_item": identity,
        "requested_item_sha256": _sha256_bytes(_canonical_json(identity).encode()),
        "requested_source_input_sha256": requested_source_input_sha256,
        "requested_turn_count": 1,
        "cumulative_measured_total_tokens": cumulative_measured_total_tokens,
        "population_total_token_bound": policy["population_total_token_bound"],
        "maximum_total_tokens_per_episode": policy[
            "maximum_total_tokens_per_episode"
        ],
        "max_concurrency": 1,
        "one_turn_per_episode": True,
        "claim_started": admission_stage == "preturn",
        "thread_started": False,
        "turn_started": False,
        "sidecar_started": False,
    }
    payload["request_sha256"] = _sha256_bytes(_canonical_json(payload).encode())
    return payload


def _validate_live_capacity_request(
    value: Any,
    *,
    loaded: Mapping[str, Any],
    policy: Mapping[str, Any],
    provision_plan_record: Mapping[str, Any],
    capacity_policy_record: Mapping[str, Any],
    runtime_authorization_record: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise EpisodeContextRunnerWaiting("live capacity request is absent")
    unhashed = dict(value)
    supplied = unhashed.pop("request_sha256", None)
    identity = value.get("requested_item")
    cumulative = value.get("cumulative_measured_total_tokens")
    remaining = value.get("remaining_population_episode_count")
    stage = value.get("admission_stage")
    run_id = value.get("run_id")
    ordinal = value.get("episode_ordinal")
    expected_keys = {
        "schema_version",
        "state",
        "fixture_only",
        "run_id",
        "episode_ordinal",
        "admission_stage",
        "contract_sha256",
        "frozen_configuration_sha256",
        "required_episode_manifest",
        "provision_plan",
        "capacity_policy",
        "runtime_authorization",
        "backlog_snapshot_sha256",
        "remaining_population_episode_count",
        "requested_item",
        "requested_item_sha256",
        "requested_source_input_sha256",
        "requested_turn_count",
        "cumulative_measured_total_tokens",
        "population_total_token_bound",
        "maximum_total_tokens_per_episode",
        "max_concurrency",
        "one_turn_per_episode",
        "claim_started",
        "thread_started",
        "turn_started",
        "sidecar_started",
        "request_sha256",
    }
    if (
        set(value) != expected_keys
        or value.get("schema_version") != LIVE_CAPACITY_REQUEST_VERSION
        or value.get("state") != "requested"
        or value.get("fixture_only") is not False
        or stage not in {"preclaim", "preturn"}
        or not isinstance(run_id, str)
        or not run_id.startswith("ectxlive_")
        or not all(character.isalnum() or character in "_-" for character in run_id)
        or isinstance(ordinal, bool)
        or not isinstance(ordinal, int)
        or ordinal < 1
        or value.get("contract_sha256") != loaded["sha256"]
        or value.get("frozen_configuration_sha256")
        != loaded["configuration_sha256"]
        or value.get("provision_plan") != provision_plan_record
        or value.get("capacity_policy") != capacity_policy_record
        or value.get("runtime_authorization") != runtime_authorization_record
        or not isinstance(identity, Mapping)
        or set(identity) != {"job_id", "episode_id", "claim_id", "attempt_id"}
        or isinstance(identity.get("job_id"), bool)
        or not isinstance(identity.get("job_id"), int)
        or identity["job_id"] < 1
        or not isinstance(identity.get("episode_id"), str)
        or not identity["episode_id"]
        or (
            stage == "preclaim"
            and (identity.get("claim_id") is not None or identity.get("attempt_id") is not None)
        )
        or (
            stage == "preturn"
            and (
                not isinstance(identity.get("claim_id"), str)
                or not identity["claim_id"]
                or not isinstance(identity.get("attempt_id"), str)
                or not identity["attempt_id"]
                or any(
                    not all(character.isalnum() or character in "_-" for character in identity[field])
                    for field in ("claim_id", "attempt_id")
                )
            )
        )
        or value.get("requested_item_sha256")
        != _sha256_bytes(_canonical_json(identity).encode())
        or (
            stage == "preclaim"
            and value.get("requested_source_input_sha256") is not None
        )
        or (
            stage == "preturn"
            and not _is_sha256(value.get("requested_source_input_sha256"))
        )
        or value.get("requested_turn_count") != 1
        or isinstance(cumulative, bool)
        or not isinstance(cumulative, int)
        or cumulative < 0
        or cumulative > policy["population_total_token_bound"]
        or isinstance(remaining, bool)
        or not isinstance(remaining, int)
        or remaining < 1
        or remaining > policy["population_episode_count"]
        or value.get("population_total_token_bound")
        != policy["population_total_token_bound"]
        or value.get("maximum_total_tokens_per_episode")
        != policy["maximum_total_tokens_per_episode"]
        or cumulative + policy["maximum_total_tokens_per_episode"]
        > policy["population_total_token_bound"]
        or value.get("max_concurrency") != 1
        or value.get("one_turn_per_episode") is not True
        or value.get("claim_started")
        is not (value.get("admission_stage") == "preturn")
        or any(value.get(field) is not False for field in (
            "thread_started", "turn_started", "sidecar_started"
        ))
        or not _is_sha256(supplied)
        or supplied != _sha256_bytes(_canonical_json(unhashed).encode())
    ):
        raise EpisodeContextRunnerWaiting("live capacity request drifted")
    return dict(value)


def _live_capacity_artifact_stem(request: Mapping[str, Any]) -> str:
    stage = str(request["admission_stage"])
    ordinal = int(request["episode_ordinal"])
    run_id = str(request["run_id"])
    identity = request["requested_item"]
    if stage == "preclaim":
        return f"{run_id}.episode-{ordinal:06d}.preclaim"
    return (
        f"{str(identity['attempt_id'])}.{str(identity['claim_id'])}.preturn"
    )


_KNOWN_RATE_LIMIT_REACHED_TYPES = {
    "rate_limit_reached",
    "workspace_owner_credits_depleted",
    "workspace_member_credits_depleted",
    "workspace_owner_usage_limit_reached",
    "workspace_member_usage_limit_reached",
}


def _live_rate_limit_window(
    value: Any, *, limit_id: str, name: str
) -> dict[str, Any]:
    if not isinstance(value, Mapping) or not set(value).issubset(
        {"usedPercent", "resetsAt", "windowDurationMins"}
    ):
        raise EpisodeContextRunnerWaiting(
            f"official {limit_id} {name} rate window is malformed"
        )
    used = value.get("usedPercent")
    resets_at = value.get("resetsAt")
    duration = value.get("windowDurationMins")
    if (
        isinstance(used, bool)
        or not isinstance(used, int)
        or not 0 <= used <= 100
        or (
            resets_at is not None
            and (isinstance(resets_at, bool) or not isinstance(resets_at, int))
        )
        or (
            duration is not None
            and (
                isinstance(duration, bool)
                or not isinstance(duration, int)
                or duration < 0
            )
        )
    ):
        raise EpisodeContextRunnerWaiting(
            f"official {limit_id} {name} rate window fields drifted"
        )
    return {
        "limit_id": limit_id,
        "control_type": name,
        "used_percent": used,
        "remaining_percent": 100 - used,
        "resets_at": resets_at,
        "window_duration_minutes": duration,
    }


def _live_rate_limit_snapshot(
    value: Any, *, expected_limit_id: str
) -> dict[str, Any]:
    allowed = {
        "credits",
        "individualLimit",
        "limitId",
        "limitName",
        "planType",
        "primary",
        "rateLimitReachedType",
        "secondary",
    }
    if not isinstance(value, Mapping) or not set(value).issubset(allowed):
        raise EpisodeContextRunnerWaiting(
            f"official {expected_limit_id} rate-limit bucket is ambiguous"
        )
    observed_limit_id = value.get("limitId")
    reached = value.get("rateLimitReachedType")
    plan_type = value.get("planType")
    limit_name = value.get("limitName")
    if (
        observed_limit_id not in {None, expected_limit_id}
        or (limit_name is not None and not isinstance(limit_name, str))
        or plan_type not in {None, "pro"}
        or (reached is not None and reached not in _KNOWN_RATE_LIMIT_REACHED_TYPES)
    ):
        raise EpisodeContextRunnerWaiting(
            f"official {expected_limit_id} rate-limit bucket identity drifted"
        )
    controls = [
        _live_rate_limit_window(
            value.get("primary"), limit_id=expected_limit_id, name="primary"
        )
    ]
    if value.get("secondary") is not None:
        controls.append(
            _live_rate_limit_window(
                value["secondary"],
                limit_id=expected_limit_id,
                name="secondary",
            )
        )
    individual = value.get("individualLimit")
    if individual is not None:
        if not isinstance(individual, Mapping) or set(individual) != {
            "limit",
            "remainingPercent",
            "resetsAt",
            "used",
        }:
            raise EpisodeContextRunnerWaiting(
                f"official {expected_limit_id} individual spend limit is malformed"
            )
        remaining = individual.get("remainingPercent")
        resets_at = individual.get("resetsAt")
        if (
            isinstance(remaining, bool)
            or not isinstance(remaining, int)
            or not 0 <= remaining <= 100
            or isinstance(resets_at, bool)
            or not isinstance(resets_at, int)
            or not isinstance(individual.get("limit"), str)
            or not isinstance(individual.get("used"), str)
        ):
            raise EpisodeContextRunnerWaiting(
                f"official {expected_limit_id} individual spend fields drifted"
            )
        controls.append(
            {
                "limit_id": expected_limit_id,
                "control_type": "individual_spend_limit",
                "used_percent": 100 - remaining,
                "remaining_percent": remaining,
                "resets_at": resets_at,
                "window_duration_minutes": None,
            }
        )
    credits = value.get("credits")
    if credits is not None:
        if not isinstance(credits, Mapping) or set(credits) != {
            "balance",
            "hasCredits",
            "unlimited",
        }:
            raise EpisodeContextRunnerWaiting(
                f"official {expected_limit_id} credit evidence is malformed"
            )
        if (
            not isinstance(credits.get("hasCredits"), bool)
            or not isinstance(credits.get("unlimited"), bool)
            or (
                credits.get("balance") is not None
                and not isinstance(credits.get("balance"), str)
            )
        ):
            raise EpisodeContextRunnerWaiting(
                f"official {expected_limit_id} credit fields drifted"
            )
    return {
        "limit_id": expected_limit_id,
        "limit_name": limit_name,
        "plan_type": plan_type,
        "rate_limit_reached_type": reached,
        "credits": copy.deepcopy(credits),
        "controls": controls,
    }


def _live_reset_credit_evidence(value: Any) -> Any:
    if value is None:
        return None
    if not isinstance(value, Mapping) or not set(value).issubset(
        {"availableCount", "credits"}
    ):
        raise EpisodeContextRunnerWaiting(
            "official reset-credit summary is malformed"
        )
    count = value.get("availableCount")
    credits = value.get("credits")
    if (
        isinstance(count, bool)
        or not isinstance(count, int)
        or count < 0
        or (credits is not None and not isinstance(credits, list))
    ):
        raise EpisodeContextRunnerWaiting(
            "official reset-credit summary fields drifted"
        )
    if isinstance(credits, list):
        required = {"grantedAt", "id", "resetType", "status"}
        allowed = required | {"description", "expiresAt", "title"}
        for credit in credits:
            if (
                not isinstance(credit, Mapping)
                or not required.issubset(credit)
                or not set(credit).issubset(allowed)
                or isinstance(credit.get("grantedAt"), bool)
                or not isinstance(credit.get("grantedAt"), int)
                or not isinstance(credit.get("id"), str)
                or not credit["id"]
                or credit.get("resetType") not in {"codexRateLimits", "unknown"}
                or credit.get("status")
                not in {"available", "redeeming", "redeemed", "unknown"}
            ):
                raise EpisodeContextRunnerWaiting(
                    "official reset-credit detail drifted"
                )
    return copy.deepcopy(dict(value))


def _live_rate_limit_evidence(response: Any) -> dict[str, Any]:
    if not isinstance(response, Mapping) or not set(response).issubset(
        {"rateLimitResetCredits", "rateLimits", "rateLimitsByLimitId"}
    ):
        raise EpisodeContextRunnerWaiting(
            "official rate-limit response is malformed or has unknown controls"
        )
    fallback = response.get("rateLimits")
    if not isinstance(fallback, Mapping):
        raise EpisodeContextRunnerWaiting(
            "official fallback rate-limit snapshot is absent"
        )
    by_id = response.get("rateLimitsByLimitId")
    snapshots: list[dict[str, Any]] = []
    if by_id is None:
        snapshots.append(
            _live_rate_limit_snapshot(fallback, expected_limit_id="codex")
        )
    else:
        if (
            not isinstance(by_id, Mapping)
            or not by_id
            or "codex" not in by_id
            or any(not isinstance(key, str) or not key for key in by_id)
        ):
            raise EpisodeContextRunnerWaiting(
                "official multi-limit bucket set is ambiguous"
            )
        for limit_id in sorted(by_id):
            snapshots.append(
                _live_rate_limit_snapshot(
                    by_id[limit_id], expected_limit_id=str(limit_id)
                )
            )
        fallback_normalized = _live_rate_limit_snapshot(
            fallback, expected_limit_id="codex"
        )
        selected = next(item for item in snapshots if item["limit_id"] == "codex")
        if fallback_normalized != selected:
            raise EpisodeContextRunnerWaiting(
                "official fallback and Codex multi-limit snapshots disagree"
            )
    controls = [
        control for snapshot in snapshots for control in snapshot["controls"]
    ]
    reached = [
        {
            "limit_id": snapshot["limit_id"],
            "rate_limit_reached_type": snapshot["rate_limit_reached_type"],
        }
        for snapshot in snapshots
        if snapshot["rate_limit_reached_type"] is not None
    ]
    selected = next(item for item in snapshots if item["limit_id"] == "codex")
    primary = next(
        item for item in selected["controls"] if item["control_type"] == "primary"
    )
    return {
        "official_method": "account/rateLimits/read",
        "raw_capacity_snapshot": copy.deepcopy(dict(response)),
        "raw_snapshot_sha256": _sha256_bytes(
            _canonical_json(dict(response)).encode("utf-8")
        ),
        "selected_limit_id": "codex",
        "observed_limit_ids": [item["limit_id"] for item in snapshots],
        "observed_limit_snapshots": snapshots,
        "applicable_capacity_controls": controls,
        "applicable_capacity_controls_sha256": _sha256_bytes(
            _canonical_json(controls).encode("utf-8")
        ),
        "minimum_applicable_remaining_percent": min(
            int(item["remaining_percent"]) for item in controls
        ),
        "selected_primary_used_percent": primary["used_percent"],
        "selected_primary_remaining_percent": primary["remaining_percent"],
        "selected_primary_resets_at": primary["resets_at"],
        "rate_limit_reached_controls": reached,
        "reset_credit_evidence": _live_reset_credit_evidence(
            response.get("rateLimitResetCredits")
        ),
        "reset_credits_counted_as_capacity": False,
    }


def _validate_live_rate_limit_evidence_fields(
    value: Mapping[str, Any]
) -> dict[str, Any]:
    raw = value.get("raw_capacity_snapshot")
    reparsed = _live_rate_limit_evidence(raw)
    evidence_fields = {
        "official_method",
        "raw_capacity_snapshot",
        "raw_snapshot_sha256",
        "selected_limit_id",
        "observed_limit_ids",
        "observed_limit_snapshots",
        "applicable_capacity_controls",
        "applicable_capacity_controls_sha256",
        "minimum_applicable_remaining_percent",
        "selected_primary_used_percent",
        "selected_primary_remaining_percent",
        "selected_primary_resets_at",
        "rate_limit_reached_controls",
        "reset_credit_evidence",
        "reset_credits_counted_as_capacity",
    }
    if any(value.get(field) != reparsed.get(field) for field in evidence_fields):
        raise EpisodeContextRunnerWaiting(
            "live capacity raw and parsed evidence disagree"
        )
    snapshots = value.get("observed_limit_snapshots")
    controls = value.get("applicable_capacity_controls")
    observed_ids = value.get("observed_limit_ids")
    expected_snapshot_keys = {
        "limit_id",
        "limit_name",
        "plan_type",
        "rate_limit_reached_type",
        "credits",
        "controls",
    }
    expected_control_keys = {
        "limit_id",
        "control_type",
        "used_percent",
        "remaining_percent",
        "resets_at",
        "window_duration_minutes",
    }
    if (
        not isinstance(snapshots, list)
        or not snapshots
        or not isinstance(controls, list)
        or not controls
        or not isinstance(observed_ids, list)
        or not observed_ids
        or observed_ids != sorted(set(observed_ids))
        or "codex" not in observed_ids
    ):
        raise EpisodeContextRunnerWaiting(
            "live capacity multi-window evidence is malformed"
        )
    normalized_controls: list[dict[str, Any]] = []
    reached: list[dict[str, Any]] = []
    for snapshot in snapshots:
        if (
            not isinstance(snapshot, Mapping)
            or set(snapshot) != expected_snapshot_keys
            or snapshot.get("limit_id") not in observed_ids
            or snapshot.get("plan_type") not in {None, "pro"}
            or (
                snapshot.get("limit_name") is not None
                and not isinstance(snapshot.get("limit_name"), str)
            )
            or (
                snapshot.get("rate_limit_reached_type") is not None
                and snapshot.get("rate_limit_reached_type")
                not in _KNOWN_RATE_LIMIT_REACHED_TYPES
            )
        ):
            raise EpisodeContextRunnerWaiting(
                "live capacity limit snapshot drifted"
            )
        credits = snapshot.get("credits")
        if credits is not None and (
            not isinstance(credits, Mapping)
            or set(credits) != {"balance", "hasCredits", "unlimited"}
            or not isinstance(credits.get("hasCredits"), bool)
            or not isinstance(credits.get("unlimited"), bool)
            or (
                credits.get("balance") is not None
                and not isinstance(credits.get("balance"), str)
            )
        ):
            raise EpisodeContextRunnerWaiting(
                "live capacity credit snapshot drifted"
            )
        snapshot_controls = snapshot.get("controls")
        if not isinstance(snapshot_controls, list) or not snapshot_controls:
            raise EpisodeContextRunnerWaiting(
                "live capacity snapshot has no controls"
            )
        if (
            not isinstance(snapshot_controls[0], Mapping)
            or snapshot_controls[0].get("control_type") != "primary"
        ):
            raise EpisodeContextRunnerWaiting(
                "live capacity snapshot primary control drifted"
            )
        for control in snapshot_controls:
            if not isinstance(control, Mapping) or set(control) != expected_control_keys:
                raise EpisodeContextRunnerWaiting(
                    "live capacity control shape drifted"
                )
            used = control.get("used_percent")
            remaining = control.get("remaining_percent")
            resets_at = control.get("resets_at")
            duration = control.get("window_duration_minutes")
            if (
                control.get("limit_id") != snapshot["limit_id"]
                or control.get("control_type")
                not in {"primary", "secondary", "individual_spend_limit"}
                or isinstance(used, bool)
                or not isinstance(used, int)
                or isinstance(remaining, bool)
                or not isinstance(remaining, int)
                or not 0 <= used <= 100
                or remaining != 100 - used
                or (
                    resets_at is not None
                    and (
                        isinstance(resets_at, bool)
                        or not isinstance(resets_at, int)
                    )
                )
                or (
                    duration is not None
                    and (
                        isinstance(duration, bool)
                        or not isinstance(duration, int)
                        or duration < 0
                    )
                )
            ):
                raise EpisodeContextRunnerWaiting(
                    "live capacity control values drifted"
                )
            normalized_controls.append(dict(control))
        if snapshot.get("rate_limit_reached_type") is not None:
            reached.append(
                {
                    "limit_id": snapshot["limit_id"],
                    "rate_limit_reached_type": snapshot[
                        "rate_limit_reached_type"
                    ],
                }
            )
    if (
        [snapshot["limit_id"] for snapshot in snapshots] != observed_ids
        or normalized_controls != controls
        or len({(item["limit_id"], item["control_type"]) for item in controls})
        != len(controls)
        or value.get("applicable_capacity_controls_sha256")
        != _sha256_bytes(_canonical_json(controls).encode("utf-8"))
        or value.get("rate_limit_reached_controls") != reached
    ):
        raise EpisodeContextRunnerWaiting(
            "live capacity flattened evidence drifted"
        )
    _live_reset_credit_evidence(value.get("reset_credit_evidence"))
    selected = next(item for item in snapshots if item["limit_id"] == "codex")
    primary = selected["controls"][0]
    minimum = min(int(item["remaining_percent"]) for item in controls)
    if (
        value.get("selected_primary_used_percent") != primary["used_percent"]
        or value.get("selected_primary_remaining_percent")
        != primary["remaining_percent"]
        or value.get("selected_primary_resets_at") != primary["resets_at"]
        or value.get("minimum_applicable_remaining_percent") != minimum
    ):
        raise EpisodeContextRunnerWaiting(
            "live capacity selected control evidence drifted"
        )
    return {
        "minimum_remaining_percent": minimum,
        "rate_limit_reached_controls": reached,
    }


class ManagedEpisodeContextReserveCapacityProvider:
    """One sticky, no-turn reserve probe per episode on the persistent client."""

    fixture_only = False

    def __init__(
        self,
        *,
        client: Any,
        policy: Mapping[str, Any],
        policy_record: Mapping[str, Any],
    ) -> None:
        self.client = client
        self.policy = dict(policy)
        self.policy_record = dict(policy_record)
        self._sticky_failure: str | None = None
        self.calls: list[dict[str, Any]] = []

    async def __call__(self, *, request: Mapping[str, Any]) -> Mapping[str, Any]:
        if self._sticky_failure is not None:
            raise EpisodeContextRunnerWaiting(
                f"live context capacity previously stopped: {self._sticky_failure}"
            )
        account = getattr(self.client, "account_summary", None)
        if account != {"type": "chatgpt", "plan_type": "pro", "requires_openai_auth": False}:
            self._sticky_failure = "managed_chatgpt_pro_auth_drift"
            raise EpisodeContextRunnerWaiting("managed ChatGPT Pro auth drifted")
        try:
            response = await self.client._request("account/rateLimits/read", {})
            evidence = _live_rate_limit_evidence(response)
        except BaseException:
            self._sticky_failure = "capacity_probe_exception_or_cancellation"
            raise
        cumulative = int(request["cumulative_measured_total_tokens"])
        global_remaining = (
            int(self.policy["population_total_token_bound"]) - cumulative
        )
        projected_tokens = int(self.policy["maximum_total_tokens_per_episode"])
        quota_rate = int(self.policy["quota_points_per_million_tokens"])
        projected_points = math.ceil(projected_tokens * quota_rate / 1_000_000)
        reserve = int(self.policy["minimum_remaining_reserve_percent"])
        minimum_remaining = int(evidence["minimum_applicable_remaining_percent"])
        usable_above_reserve = max(0, minimum_remaining - reserve)
        projected_terminal = minimum_remaining - projected_points
        reached = list(evidence["rate_limit_reached_controls"])
        cleared = bool(
            not reached
            and projected_points <= usable_above_reserve
            and global_remaining >= projected_tokens
        )
        payload = {
            "schema_version": LIVE_CAPACITY_ADMISSION_VERSION,
            "state": "admitted" if cleared else "denied",
            "fixture_only": False,
            "admission_id": f"ectxcap_{uuid.uuid4().hex}",
            "request_sha256": request["request_sha256"],
            "capacity_policy": dict(self.policy_record),
            "managed_chatgpt_auth_verified": True,
            "plan_type": "pro",
            **evidence,
            "minimum_remaining_reserve_percent": reserve,
            "usable_percent_above_reserve": usable_above_reserve,
            "policy_projected_episode_token_bound": projected_tokens,
            "policy_quota_points_per_million_tokens": quota_rate,
            "policy_projected_episode_quota_points": projected_points,
            "policy_projected_terminal_remaining_percent": projected_terminal,
            "global_remaining_token_bound": global_remaining,
            "cleared_for_semantic_turn": cleared,
            "admission_stage": request["admission_stage"],
            "claim_started": request["claim_started"],
            "thread_started": False,
            "turn_started": False,
            "sidecar_started": False,
            "retry_allowed": False,
        }
        self.calls.append(copy.deepcopy(payload))
        if not cleared:
            self._sticky_failure = (
                "provider_rate_limit_reached"
                if reached
                else "minimum_reserve_or_episode_bound_lost"
            )
        return payload


def _validate_live_capacity_admission(
    value: Any,
    *,
    request: Mapping[str, Any],
    policy: Mapping[str, Any],
    policy_record: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise EpisodeContextRunnerWaiting("live capacity admission is absent")
    projected_points = math.ceil(
        policy["maximum_total_tokens_per_episode"]
        * policy["quota_points_per_million_tokens"]
        / 1_000_000
    )
    evidence = _validate_live_rate_limit_evidence_fields(value)
    remaining = evidence["minimum_remaining_percent"]
    reached = evidence["rate_limit_reached_controls"]
    expected_keys = {
        "schema_version",
        "state",
        "fixture_only",
        "admission_id",
        "request_sha256",
        "capacity_policy",
        "managed_chatgpt_auth_verified",
        "plan_type",
        "official_method",
        "raw_capacity_snapshot",
        "raw_snapshot_sha256",
        "selected_limit_id",
        "observed_limit_ids",
        "observed_limit_snapshots",
        "applicable_capacity_controls",
        "applicable_capacity_controls_sha256",
        "minimum_applicable_remaining_percent",
        "selected_primary_used_percent",
        "selected_primary_remaining_percent",
        "selected_primary_resets_at",
        "rate_limit_reached_controls",
        "reset_credit_evidence",
        "reset_credits_counted_as_capacity",
        "minimum_remaining_reserve_percent",
        "usable_percent_above_reserve",
        "policy_projected_episode_token_bound",
        "policy_quota_points_per_million_tokens",
        "policy_projected_episode_quota_points",
        "policy_projected_terminal_remaining_percent",
        "global_remaining_token_bound",
        "cleared_for_semantic_turn",
        "admission_stage",
        "claim_started",
        "thread_started",
        "turn_started",
        "sidecar_started",
        "retry_allowed",
    }
    if (
        set(value) != expected_keys
        or value.get("schema_version") != LIVE_CAPACITY_ADMISSION_VERSION
        or value.get("state") not in {"admitted", "denied"}
        or value.get("fixture_only") is not False
        or not isinstance(value.get("admission_id"), str)
        or not value["admission_id"]
        or value.get("request_sha256") != request["request_sha256"]
        or value.get("admission_stage") != request["admission_stage"]
        or value.get("claim_started") is not request["claim_started"]
        or value.get("capacity_policy") != policy_record
        or value.get("managed_chatgpt_auth_verified") is not True
        or value.get("plan_type") != "pro"
        or value.get("official_method") != "account/rateLimits/read"
        or not _is_sha256(value.get("raw_snapshot_sha256"))
        or value.get("selected_limit_id") != "codex"
        or isinstance(remaining, bool)
        or not isinstance(remaining, int)
        or not 0 <= remaining <= 100
        or value.get("reset_credits_counted_as_capacity") is not False
        or value.get("minimum_remaining_reserve_percent")
        != policy["minimum_remaining_reserve_percent"]
        or value.get("usable_percent_above_reserve")
        != max(0, remaining - policy["minimum_remaining_reserve_percent"])
        or value.get("policy_projected_episode_token_bound")
        != policy["maximum_total_tokens_per_episode"]
        or value.get("policy_quota_points_per_million_tokens")
        != policy["quota_points_per_million_tokens"]
        or value.get("policy_projected_episode_quota_points") != projected_points
        or value.get("policy_projected_terminal_remaining_percent")
        != remaining - projected_points
        or value.get("global_remaining_token_bound")
        != policy["population_total_token_bound"]
        - request["cumulative_measured_total_tokens"]
        or any(value.get(field) is not False for field in (
            "thread_started", "turn_started", "sidecar_started"
        ))
        or value.get("retry_allowed") is not False
        or (
            value.get("state") == "denied"
            and value.get("cleared_for_semantic_turn") is not False
        )
        or (
            value.get("state") == "admitted"
            and (
                value.get("cleared_for_semantic_turn") is not True
                or reached
                or value.get("policy_projected_terminal_remaining_percent")
                < policy["minimum_remaining_reserve_percent"]
            )
        )
    ):
        raise EpisodeContextRunnerWaiting("live reserve-capacity admission drifted")
    return dict(value)


async def _persist_live_capacity_admission(
    *,
    root: Path,
    loaded: Mapping[str, Any],
    request: Mapping[str, Any],
    policy: Mapping[str, Any],
    provision_plan_record: Mapping[str, Any],
    capacity_policy_record: Mapping[str, Any],
    runtime_authorization_record: Mapping[str, Any],
    provider: LiveEpisodeContextCapacityAdmissionProvider,
) -> tuple[dict[str, Any], dict[str, Any]]:
    verified_request = _validate_live_capacity_request(
        request,
        loaded=loaded,
        policy=policy,
        provision_plan_record=provision_plan_record,
        capacity_policy_record=capacity_policy_record,
        runtime_authorization_record=runtime_authorization_record,
    )
    stem = _live_capacity_artifact_stem(verified_request)
    supplied = provider(request=verified_request)
    if inspect.isawaitable(supplied):
        supplied = await supplied
    if isinstance(supplied, Path):
        admission = _load_json(supplied.resolve(), label="live capacity admission")
    elif isinstance(supplied, Mapping):
        admission = dict(supplied)
    else:
        raise EpisodeContextRunnerWaiting("live capacity provider returned no receipt")
    verified_admission = _validate_live_capacity_admission(
        admission,
        request=verified_request,
        policy=policy,
        policy_record=capacity_policy_record,
    )
    request_path, admission_path = _publish_live_capacity_bundle(
        root=root,
        stem=stem,
        request=verified_request,
        admission=verified_admission,
    )
    binding = {
        "request": _record(request_path),
        "admission": _record(admission_path),
    }
    if verified_admission["state"] != "admitted":
        raise LiveCapacityAdmissionDenied(
            "fresh episode capacity was not admitted", binding=binding
        )
    return verified_admission, binding


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _publish_live_capacity_bundle(
    *,
    root: Path,
    stem: str,
    request: Mapping[str, Any],
    admission: Mapping[str, Any],
) -> tuple[Path, Path]:
    """Atomically publish one complete no-turn capacity measurement bundle."""

    capacity_root = (root / "capacity").resolve()
    staging_root = (root / ".capacity-staging").resolve()
    capacity_root.mkdir(parents=True, exist_ok=True)
    staging_root.mkdir(parents=True, exist_ok=True)
    target = capacity_root / stem
    if target.exists():
        raise EpisodeContextRunnerError(
            f"immutable live capacity bundle already exists: {target}"
        )
    staging = staging_root / f"{stem}.{uuid.uuid4().hex}"
    staging.mkdir(mode=0o700)
    request_path = staging / "request.json"
    admission_path = staging / "admission.json"
    try:
        _write_immutable(request_path, _pretty_json(dict(request)).encode())
        _write_immutable(admission_path, _pretty_json(dict(admission)).encode())
        _fsync_directory(staging)
        os.rename(staging, target)
        _fsync_directory(capacity_root)
    except BaseException:
        if staging.exists():
            for child in staging.iterdir():
                if child.is_file():
                    child.unlink(missing_ok=True)
            staging.rmdir()
        raise
    return target / "request.json", target / "admission.json"


def _capacity_abandonment_payload(
    *,
    request_path: Path,
    admission_path: Path,
    request: Mapping[str, Any],
    admission: Mapping[str, Any],
    reason: str,
    ownership_proof: Mapping[str, Any],
) -> dict[str, Any]:
    if request.get("admission_stage") != "preclaim":
        raise EpisodeContextRunnerError(
            "only an unclaimed preclaim probe may be abandoned"
        )
    payload = {
        "schema_version": LIVE_CAPACITY_ABANDONMENT_VERSION,
        "state": "abandoned_no_dispatch",
        "reason": reason,
        "request": _record(request_path),
        "admission": _record(admission_path),
        "request_sha256": request["request_sha256"],
        "admission_id": admission["admission_id"],
        "admission_state": admission["state"],
        "admission_stage": "preclaim",
        "requested_item": dict(request["requested_item"]),
        "contract_sha256": request["contract_sha256"],
        "frozen_configuration_sha256": request[
            "frozen_configuration_sha256"
        ],
        "runtime_authorization": dict(request["runtime_authorization"]),
        "ownership_proof": dict(ownership_proof),
        "claim_started": False,
        "thread_started": False,
        "turn_started": False,
        "sidecar_started": False,
        "semantic_model_call_count": 0,
        "production_context_mutated": False,
    }
    payload["abandonment_sha256"] = _sha256_bytes(
        _canonical_json(payload).encode()
    )
    return payload


def _validate_capacity_abandonment(
    value: Any,
    *,
    request_path: Path,
    admission_path: Path,
    request: Mapping[str, Any],
    admission: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise EpisodeContextRunnerError("capacity abandonment is malformed")
    unhashed = dict(value)
    supplied = unhashed.pop("abandonment_sha256", None)
    expected = _capacity_abandonment_payload(
        request_path=request_path,
        admission_path=admission_path,
        request=request,
        admission=admission,
        reason=str(value.get("reason") or ""),
        ownership_proof=(
            value.get("ownership_proof")
            if isinstance(value.get("ownership_proof"), Mapping)
            else {}
        ),
    )
    if (
        value.get("reason")
        not in {
            "orphan_preclaim_probe_completed_before_claim",
            "denied_preclaim_probe_completed_before_claim",
        }
        or not isinstance(value.get("ownership_proof"), Mapping)
        or set(value.get("ownership_proof", {}))
        != {
            "job_id",
            "episode_id",
            "observed_job_status",
            "matching_zero_dispatch_attempt_ids",
            "matching_dispatched_attempt_count",
        }
        or value["ownership_proof"].get("job_id")
        != request["requested_item"]["job_id"]
        or value["ownership_proof"].get("episode_id")
        != request["requested_item"]["episode_id"]
        or value["ownership_proof"].get("matching_zero_dispatch_attempt_ids")
        != []
        or value["ownership_proof"].get("matching_dispatched_attempt_count")
        != 0
        or not _is_sha256(supplied)
        or supplied != _sha256_bytes(_canonical_json(unhashed).encode())
        or dict(value) != expected
    ):
        raise EpisodeContextRunnerError("capacity abandonment lineage drifted")
    return dict(value)


def _publish_capacity_abandonment(
    *,
    bundle_dir: Path,
    payload: Mapping[str, Any],
) -> Path:
    target = bundle_dir / "abandoned-no-dispatch.json"
    if target.exists():
        existing = target.read_bytes()
        expected = _pretty_json(dict(payload)).encode()
        if existing != expected:
            raise EpisodeContextRunnerError(
                "immutable capacity abandonment already exists with different bytes"
            )
        return target
    staging_root = bundle_dir.parent.parent / ".capacity-staging"
    staging_root.mkdir(parents=True, exist_ok=True)
    staging = staging_root / f"abandonment.{bundle_dir.name}.{uuid.uuid4().hex}.json"
    try:
        _write_immutable(staging, _pretty_json(dict(payload)).encode())
        os.rename(staging, target)
        _fsync_directory(bundle_dir)
    except BaseException:
        staging.unlink(missing_ok=True)
        raise
    return target


def live_context_control_overlay() -> dict[str, Any]:
    overlay = copy.deepcopy(LIVE_CONTEXT_CONTROL_OVERLAY)
    if (
        overlay.get("project_doc_max_bytes") != 0
        or overlay.get("include_apps_instructions") is not False
        or overlay.get("include_environment_context") is not False
        or overlay.get("include_permissions_instructions") is not False
        or overlay.get("web_search") != "disabled"
        or overlay.get("personality") != "none"
        or overlay.get("tools")
        != {"experimental_request_user_input": {"enabled": False}}
    ):
        raise EpisodeContextRunnerError("live context-control overlay drifted")
    return overlay


def live_instruction_source_contract() -> dict[str, Any]:
    paths = list(LIVE_EXPECTED_INSTRUCTION_SOURCE_PATHS)
    return {
        "effective_instruction_source_paths": paths,
        "effective_instruction_sources_count": len(paths),
        "effective_instruction_sources_sha256": _sha256_bytes(
            _canonical_json(paths).encode()
        ),
        "project_instruction_content_byte_budget": 0,
        "project_instruction_content_included": False,
        "context_control_overlay_sha256": _sha256_bytes(
            _canonical_json(live_context_control_overlay()).encode()
        ),
    }


def load_live_episode_context_runtime_authorization(
    path: Path, *, loaded: Mapping[str, Any]
) -> dict[str, Any]:
    authorization_path = path.expanduser().resolve()
    value = _load_json(
        authorization_path, label="live episode-context runtime authorization"
    )
    expected = {
        "schema_version",
        "state",
        "phase",
        "authorized_by",
        "operator_authorization_id",
        "predecessor_contract",
        "attempt_schema_cutover_receipt",
        "provision_plan",
        "provision_execution_receipt",
        "capacity_policy",
        "context_control_overlay",
        "instruction_source_contract",
        "runtime_artifacts",
        "execution",
    }
    runtime = value.get("runtime_artifacts")
    execution = value.get("execution")
    if (
        set(value) != expected
        or value.get("schema_version") != LIVE_RUNTIME_AUTHORIZATION_VERSION
        or value.get("state") != "frozen_authorized"
        or value.get("phase") != "episode_context"
        or value.get("authorized_by") != "kolby"
        or _validate_operator_authority(
            operator_authorization_id=value.get("operator_authorization_id"),
            authorized_by=value.get("authorized_by"),
        )[0]
        != value.get("operator_authorization_id")
        or value.get("predecessor_contract") != _record(loaded["path"])
        or not isinstance(runtime, Mapping)
        or dict(runtime) != _expected_live_runtime_artifacts(loaded)
        or execution
        != {
            "lane": LIVE_LANE,
            "label_pack": LIVE_LABEL_PACK,
            "queue_payload_model": LIVE_QUEUE_PAYLOAD_MODEL,
            "semantic_model": LIVE_SEMANTIC_MODEL,
            "reasoning_effort": LIVE_REASONING_EFFORT,
            "cwd": str(PROJECT_ROOT),
            "transport": TRANSPORT,
            "cli_version": PINNED_CLI_VERSION,
            "persistent_app_server_processes": 1,
            "one_fresh_ephemeral_thread_per_episode": True,
            "one_turn_per_episode": True,
            "fresh_capacity_before_each_claim_thread_and_turn": True,
            "provision_execution_receipt_required_before_semantic_run": True,
            "semantic_retry_count": 0,
            "live_model_calls_allowed": True,
            "live_database_mutation_allowed": True,
            "deterministic_semantic_pruning": False,
        }
    ):
        raise EpisodeContextRunnerError(
            "live episode-context runtime authorization drifted"
        )
    cutover_path = _verify_record(
        value["attempt_schema_cutover_receipt"],
        label="attempt-schema cutover receipt",
    )
    cutover = verify_attempt_schema_cutover_receipt(
        _load_json(cutover_path, label="attempt-schema cutover receipt"),
        require_ready=True,
    )
    provision_path = _verify_record(
        value["provision_plan"], label="live context provision plan"
    )
    provision = verify_live_context_provision_plan(
        _load_json(provision_path, label="live context provision plan")
    )
    provision_execution_path = _verify_record(
        value["provision_execution_receipt"],
        label="live context provision execution receipt",
    )
    provision_execution = verify_live_context_provision_execution_receipt(
        provision_execution_path,
        database_path=Path(str(cutover["database"]["path"])),
    )
    if (
        cutover["database"] != provision["database"]
        or cutover["database"] != provision_execution["database"]
        or provision_execution["contract"] != _record(loaded["path"])
        or provision_execution["provision_plan"] != value["provision_plan"]
        or provision_execution["post_cutover_receipt"]
        != value["attempt_schema_cutover_receipt"]
    ):
        raise EpisodeContextRunnerError(
            "live context database or provision-execution lineage drifted"
        )
    policy_path = _verify_record(
        value["capacity_policy"], label="live context capacity policy"
    )
    policy = _validate_live_capacity_policy(
        _load_json(policy_path, label="live context capacity policy"),
        provision_plan=provision,
    )
    overlay_path = _verify_record(
        value["context_control_overlay"], label="live context-control overlay"
    )
    overlay = _load_json(overlay_path, label="live context-control overlay")
    if overlay != live_context_control_overlay():
        raise EpisodeContextRunnerError("live context-control overlay artifact drifted")
    source_path = _verify_record(
        value["instruction_source_contract"],
        label="live instruction-source contract",
    )
    instruction_sources = _load_json(
        source_path, label="live instruction-source contract"
    )
    if instruction_sources != live_instruction_source_contract():
        raise EpisodeContextRunnerError("live instruction-source contract drifted")
    configuration = loaded["configuration"]
    if (
        configuration["lane"] != LIVE_LANE
        or configuration["label_pack"] != LIVE_LABEL_PACK
        or configuration["queue_payload_model"] != LIVE_QUEUE_PAYLOAD_MODEL
        or configuration["model"] != LIVE_SEMANTIC_MODEL
        or configuration["reasoning_effort"] != LIVE_REASONING_EFFORT
        or configuration["cli_version"] != PINNED_CODEX_CLI_VERSION
        or loaded["artifact_paths"]["codex_binary"].name != "codex"
        or PINNED_CODEX_CLI_VERSION != PINNED_CLI_VERSION
    ):
        raise EpisodeContextRunnerError("live frozen configuration is incompatible")
    return {
        "path": authorization_path,
        "sha256": _sha256_file(authorization_path),
        "authorization": value,
        "cutover": cutover,
        "cutover_path": cutover_path,
        "provision_plan": provision,
        "provision_plan_path": provision_path,
        "provision_execution": provision_execution,
        "provision_execution_path": provision_execution_path,
        "capacity_policy": policy,
        "capacity_policy_path": policy_path,
        "overlay": overlay,
        "overlay_path": overlay_path,
        "instruction_sources": instruction_sources,
        "instruction_sources_path": source_path,
    }


def build_live_episode_context_runtime_authorization_artifact(
    *,
    contract_path: Path,
    cutover_receipt_path: Path,
    provision_plan_path: Path,
    provision_execution_receipt_path: Path,
    capacity_policy_path: Path,
    context_control_overlay_path: Path,
    instruction_source_contract_path: Path,
    output_path: Path,
    operator_authorization_id: str,
    authorized_by: str,
) -> dict[str, Any]:
    """Freeze exact live runtime lineage without opening a transport or DB writer."""

    authorization_id, authority = _validate_operator_authority(
        operator_authorization_id=operator_authorization_id,
        authorized_by=authorized_by,
    )
    loaded = load_episode_context_contract(contract_path)
    payload = {
        "schema_version": LIVE_RUNTIME_AUTHORIZATION_VERSION,
        "state": "frozen_authorized",
        "phase": "episode_context",
        "authorized_by": authority,
        "operator_authorization_id": authorization_id,
        "predecessor_contract": _record(contract_path),
        "attempt_schema_cutover_receipt": _record(cutover_receipt_path),
        "provision_plan": _record(provision_plan_path),
        "provision_execution_receipt": _record(
            provision_execution_receipt_path
        ),
        "capacity_policy": _record(capacity_policy_path),
        "context_control_overlay": _record(context_control_overlay_path),
        "instruction_source_contract": _record(
            instruction_source_contract_path
        ),
        "runtime_artifacts": _expected_live_runtime_artifacts(loaded),
        "execution": {
            "lane": LIVE_LANE,
            "label_pack": LIVE_LABEL_PACK,
            "queue_payload_model": LIVE_QUEUE_PAYLOAD_MODEL,
            "semantic_model": LIVE_SEMANTIC_MODEL,
            "reasoning_effort": LIVE_REASONING_EFFORT,
            "cwd": str(PROJECT_ROOT),
            "transport": TRANSPORT,
            "cli_version": PINNED_CLI_VERSION,
            "persistent_app_server_processes": 1,
            "one_fresh_ephemeral_thread_per_episode": True,
            "one_turn_per_episode": True,
            "fresh_capacity_before_each_claim_thread_and_turn": True,
            "provision_execution_receipt_required_before_semantic_run": True,
            "semantic_retry_count": 0,
            "live_model_calls_allowed": True,
            "live_database_mutation_allowed": True,
            "deterministic_semantic_pruning": False,
        },
    }
    target = output_path.expanduser().resolve()
    _freeze_validated_control_artifact(
        target,
        payload,
        lambda candidate: load_live_episode_context_runtime_authorization(
            candidate, loaded=loaded
        ),
    )
    return {
        "state": "frozen_authorized",
        "phase": "episode_context",
        "operator_authorization_id": authorization_id,
        "authorization": _record(target),
        "production_mutated": False,
        "semantic_model_call_count": 0,
    }


class ManagedEpisodeContextCodexAppServerClient(CodexAppServerClient):
    """Official persistent transport with an exact zero-byte instruction overlay."""

    def __init__(
        self,
        *,
        config_overlay: Mapping[str, Any],
        instruction_source_contract: Mapping[str, Any],
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._context_overlay = copy.deepcopy(dict(config_overlay))
        self._instruction_source_contract = copy.deepcopy(
            dict(instruction_source_contract)
        )

    async def _request(self, method: str, params: dict[str, Any]) -> Any:
        request = copy.deepcopy(params)
        if method == "thread/start":
            if "config" in request:
                raise EpisodeContextRunnerError(
                    "live context thread config overlay was supplied twice"
                )
            request["config"] = copy.deepcopy(self._context_overlay)
            request["personality"] = "none"
            request["environments"] = []
            request["dynamicTools"] = []
        elif method == "turn/start":
            request["summary"] = "none"
        result = await super()._request(method, request)
        if method == "thread/start":
            sources = result.get("instructionSources") if isinstance(result, Mapping) else None
            if sources != self._instruction_source_contract[
                "effective_instruction_source_paths"
            ]:
                raise AppServerProtocolError(
                    "live context effective instruction-source paths drifted"
                )
        return result


def _managed_episode_context_client(
    *, loaded: Mapping[str, Any], runtime: Mapping[str, Any]
) -> ManagedEpisodeContextCodexAppServerClient:
    binary = loaded["artifact_paths"]["codex_binary"]
    return ManagedEpisodeContextCodexAppServerClient(
        config_overlay=runtime["overlay"],
        instruction_source_contract=runtime["instruction_sources"],
        command=[str(binary), "app-server", "--stdio", "--strict-config"],
    )


def _expected_live_runtime_artifacts(
    loaded: Mapping[str, Any],
) -> dict[str, Any]:
    artifacts = loaded["contract"]["artifacts"]
    return {
        "runner_source": _record(Path(__file__)),
        "codex_app_server_source": _record(
            Path(__file__).with_name("codex_app_server.py")
        ),
        "protocol_schema": _record(PROTOCOL_SCHEMA_PATH),
        "pinned_codex_binary": _record(
            loaded["artifact_paths"]["codex_binary"]
        ),
        "frozen_configuration": _record(
            loaded["artifact_paths"]["frozen_configuration"]
        ),
        "prompt": _record(loaded["prompt_path"]),
        "output_schema": _record(loaded["schema_path"]),
        "managed_client_implementation": _implementation_binding(
            ManagedEpisodeContextCodexAppServerClient
        ),
        "capacity_provider_implementation": _implementation_binding(
            ManagedEpisodeContextReserveCapacityProvider
        ),
    }


def _verify_live_runtime_files_now(
    *, loaded: Mapping[str, Any], runtime: Mapping[str, Any]
) -> dict[str, Any]:
    expected = _expected_live_runtime_artifacts(loaded)
    artifacts = loaded["contract"]["artifacts"]
    if runtime["authorization"]["runtime_artifacts"] != expected:
        raise EpisodeContextRunnerError(
            "live episode-context runtime artifact drifted"
        )
    if (
        expected["pinned_codex_binary"]
        != loaded["contract"]["artifacts"]["codex_binary"]
        or expected["frozen_configuration"]
        != loaded["contract"]["artifacts"]["frozen_configuration"]
        or expected["prompt"] != artifacts["prompt"]
        or expected["output_schema"] != artifacts["output_schema"]
    ):
        raise EpisodeContextRunnerError(
            "live episode-context contract artifact binding drifted"
        )
    return expected


def _verify_live_capacity_binding(
    value: Any,
    *,
    loaded: Mapping[str, Any],
    runtime: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    if not isinstance(value, Mapping) or set(value) != {"preclaim", "preturn"}:
        raise EpisodeContextRunnerError("live capacity binding shape drifted")
    verified: dict[str, Any] = {}
    policy = runtime["capacity_policy"]
    provision_record = runtime["authorization"]["provision_plan"]
    policy_record = runtime["authorization"]["capacity_policy"]
    for stage in ("preclaim", "preturn"):
        binding = value[stage]
        if not isinstance(binding, Mapping) or set(binding) != {"request", "admission"}:
            raise EpisodeContextRunnerError("live capacity stage binding drifted")
        request_path = _verify_record(
            binding["request"], label=f"live {stage} capacity request"
        )
        admission_path = _verify_record(
            binding["admission"], label=f"live {stage} capacity admission"
        )
        request = _validate_live_capacity_request(
            _load_json(request_path, label=f"live {stage} capacity request"),
            loaded=loaded,
            policy=policy,
            provision_plan_record=provision_record,
            capacity_policy_record=policy_record,
            runtime_authorization_record=_record(runtime["path"]),
        )
        admission = _validate_live_capacity_admission(
            _load_json(admission_path, label=f"live {stage} capacity admission"),
            request=request,
            policy=policy,
            policy_record=policy_record,
        )
        if (
            request["admission_stage"] != stage
            or admission["state"] != "admitted"
        ):
            raise EpisodeContextRunnerError("live capacity stage was not admitted")
        verified[stage] = {"request": request, "admission": admission}
    preclaim = verified["preclaim"]["request"]
    preturn = verified["preturn"]["request"]
    if (
        preclaim["run_id"] != preturn["run_id"]
        or preclaim["episode_ordinal"] != preturn["episode_ordinal"]
        or {
            "job_id": preclaim["requested_item"]["job_id"],
            "episode_id": preclaim["requested_item"]["episode_id"],
        }
        != {
            "job_id": preturn["requested_item"]["job_id"],
            "episode_id": preturn["requested_item"]["episode_id"],
        }
        or preclaim["required_episode_manifest"]
        != preturn["required_episode_manifest"]
        or preclaim["cumulative_measured_total_tokens"]
        != preturn["cumulative_measured_total_tokens"]
    ):
        raise EpisodeContextRunnerError("live preclaim/preturn capacity lineage drifted")
    return verified["preclaim"], verified["preturn"]


def build_backlog_snapshot(
    *,
    job_status_counts: Mapping[str, int],
    required_episode_ids: Sequence[str],
    completed_episode_ids: Sequence[str],
    required_episode_manifest: Mapping[str, Any],
) -> dict[str, Any]:
    counts: dict[str, int] = {}
    for field in JOB_STATUS_FIELDS:
        value = job_status_counts.get(field, 0)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise EpisodeContextRunnerError("episode-context status accounting is malformed")
        counts[field] = value
    if set(job_status_counts) - set(JOB_STATUS_FIELDS):
        raise EpisodeContextRunnerError("episode-context status accounting has unknown buckets")
    required = sorted(set(str(item) for item in required_episode_ids))
    completed = sorted(set(str(item) for item in completed_episode_ids))
    if not set(completed).issubset(required):
        raise EpisodeContextRunnerError("completed episode accounting exceeds required episodes")
    manifest_path = _verify_record(
        required_episode_manifest, label="required episode manifest"
    )
    manifest = _load_json(manifest_path, label="required episode manifest")
    if (
        manifest.get("schema_version") != REQUIRED_EPISODE_MANIFEST_VERSION
        or manifest.get("episode_ids") != required
        or manifest.get("episode_count") != len(required)
        or manifest.get("episode_ids_sha256")
        != _sha256_bytes(_canonical_json(required).encode())
    ):
        raise EpisodeContextRunnerError("backlog required episode manifest drifted")
    missing = sorted(set(required) - set(completed))
    nonterminal_jobs = counts["pending"] + counts["claimed"] + counts["failed"] + counts["other"]
    snapshot = {
        "job_status_counts": counts,
        "required_episode_manifest": dict(required_episode_manifest),
        "required_episode_count": len(required),
        "completed_required_episode_count": len(completed),
        "missing_required_episode_count": len(missing),
        "remaining_episode_context_jobs": nonterminal_jobs + len(missing),
        "completed_episode_ids": completed,
        "missing_episode_ids": missing,
        "required_episode_ids_sha256": _sha256_bytes(_canonical_json(required).encode()),
        "completed_episode_ids_sha256": _sha256_bytes(_canonical_json(completed).encode()),
        "missing_episode_ids_sha256": _sha256_bytes(_canonical_json(missing).encode()),
    }
    snapshot["snapshot_sha256"] = _sha256_bytes(_canonical_json(snapshot).encode())
    return snapshot


def _normalize_backlog(value: Any) -> dict[str, Any]:
    expected = {
        "job_status_counts",
        "required_episode_manifest",
        "required_episode_count",
        "completed_required_episode_count",
        "missing_required_episode_count",
        "remaining_episode_context_jobs",
        "completed_episode_ids",
        "missing_episode_ids",
        "required_episode_ids_sha256",
        "completed_episode_ids_sha256",
        "missing_episode_ids_sha256",
        "snapshot_sha256",
    }
    if not isinstance(value, Mapping) or set(value) != expected:
        raise EpisodeContextRunnerError("episode-context backlog snapshot shape drifted")
    counts = value.get("job_status_counts")
    if not isinstance(counts, Mapping) or set(counts) != set(JOB_STATUS_FIELDS):
        raise EpisodeContextRunnerError("episode-context backlog status buckets drifted")
    normalized_counts = {}
    for field in JOB_STATUS_FIELDS:
        item = counts[field]
        if isinstance(item, bool) or not isinstance(item, int) or item < 0:
            raise EpisodeContextRunnerError("episode-context backlog counts are malformed")
        normalized_counts[field] = item
    normalized = dict(value)
    normalized["job_status_counts"] = normalized_counts
    manifest_path = _verify_record(
        normalized["required_episode_manifest"], label="backlog required episode manifest"
    )
    manifest = _load_json(manifest_path, label="backlog required episode manifest")
    required_ids = manifest.get("episode_ids")
    completed_ids = normalized.get("completed_episode_ids")
    missing_ids = normalized.get("missing_episode_ids")
    if (
        manifest.get("schema_version") != REQUIRED_EPISODE_MANIFEST_VERSION
        or not isinstance(required_ids, list)
        or required_ids != sorted(set(required_ids))
        or not isinstance(completed_ids, list)
        or completed_ids != sorted(set(completed_ids))
        or not isinstance(missing_ids, list)
        or missing_ids != sorted(set(missing_ids))
        or any(not isinstance(item, str) or not item for item in required_ids)
        or any(not isinstance(item, str) or not item for item in completed_ids)
        or any(not isinstance(item, str) or not item for item in missing_ids)
        or not set(completed_ids).issubset(required_ids)
        or missing_ids != sorted(set(required_ids) - set(completed_ids))
    ):
        raise EpisodeContextRunnerError("episode-context backlog episode sets drifted")
    for field in (
        "required_episode_count",
        "completed_required_episode_count",
        "missing_required_episode_count",
        "remaining_episode_context_jobs",
    ):
        item = normalized[field]
        if isinstance(item, bool) or not isinstance(item, int) or item < 0:
            raise EpisodeContextRunnerError("episode-context backlog totals are malformed")
    blocking_jobs = sum(
        normalized_counts[field] for field in ("pending", "claimed", "failed", "other")
    )
    if (
        normalized["required_episode_count"] != len(required_ids)
        or normalized["completed_required_episode_count"] != len(completed_ids)
        or normalized["missing_required_episode_count"] != len(missing_ids)
        or normalized["remaining_episode_context_jobs"]
        != blocking_jobs + len(missing_ids)
        or normalized["required_episode_ids_sha256"]
        != _sha256_bytes(_canonical_json(required_ids).encode())
        or normalized["completed_episode_ids_sha256"]
        != _sha256_bytes(_canonical_json(completed_ids).encode())
        or normalized["missing_episode_ids_sha256"]
        != _sha256_bytes(_canonical_json(missing_ids).encode())
        or manifest.get("episode_count") != len(required_ids)
        or manifest.get("episode_ids_sha256")
        != normalized["required_episode_ids_sha256"]
    ):
        raise EpisodeContextRunnerError("episode-context backlog arithmetic drifted")
    for field in (
        "required_episode_ids_sha256",
        "completed_episode_ids_sha256",
        "missing_episode_ids_sha256",
        "snapshot_sha256",
    ):
        if not _is_sha256(normalized[field]):
            raise EpisodeContextRunnerError("episode-context backlog hashes are malformed")
    supplied = normalized.pop("snapshot_sha256")
    if _sha256_bytes(_canonical_json(normalized).encode()) != supplied:
        raise EpisodeContextRunnerError("episode-context backlog snapshot hash drifted")
    normalized["snapshot_sha256"] = supplied
    return normalized


def _verified_evaluation(lineage: Mapping[str, Any]) -> tuple[dict[str, Any], Path]:
    evaluation_root = Path(str(lineage.get("evaluation_root") or "")).expanduser().resolve()
    if not evaluation_root.is_dir():
        raise EpisodeContextRunnerError("evaluation root is unavailable")
    receipt_path = _verify_record(lineage.get("evaluation_receipt"), label="evaluation receipt")
    try:
        verified = verify_evaluation_receipt(receipt_path, evaluation_root)
    except (OSError, ValueError) as exc:
        raise EpisodeContextRunnerError("strict accepted evaluation verification failed") from exc
    if (
        verified.get("evaluation_id") != lineage.get("evaluation_id")
        or verified.get("artifact_hashes") != lineage.get("verified_artifact_hashes")
    ):
        raise EpisodeContextRunnerError("accepted evaluation lineage drifted")
    return verified, receipt_path


def _verify_holdout_authorization(
    path: Path,
    *,
    evaluation_id: str,
    evaluation_receipt: Mapping[str, Any],
    frozen_configuration_sha256: str,
) -> dict[str, Any]:
    value = _load_json(path, label="episode-context holdout authorization")
    expected = {
        "schema_version",
        "state",
        "phase",
        "evaluation_id",
        "evaluation_receipt",
        "frozen_configuration_sha256",
        "production_authorized",
        "untouched_holdout_passed",
        "semantic_noninferior_or_better",
        "managed_app_server_auth_only",
        "api_key_billing_allowed",
        "raw_session_token_replay_allowed",
        "semantic_retry_count",
    }
    if (
        set(value) != expected
        or value.get("schema_version") != HOLDOUT_AUTHORIZATION_VERSION
        or value.get("state") != "accepted"
        or value.get("phase") != "episode_context"
        or value.get("evaluation_id") != evaluation_id
        or value.get("evaluation_receipt") != evaluation_receipt
        or value.get("frozen_configuration_sha256") != frozen_configuration_sha256
        or value.get("production_authorized") is not True
        or value.get("untouched_holdout_passed") is not True
        or value.get("semantic_noninferior_or_better") is not True
        or value.get("managed_app_server_auth_only") is not True
        or value.get("api_key_billing_allowed") is not False
        or value.get("raw_session_token_replay_allowed") is not False
        or value.get("semantic_retry_count") != 0
    ):
        raise EpisodeContextRunnerError("episode-context holdout did not authorize production")
    return value


def _validate_frozen_configuration(
    path: Path,
    *,
    prompt_record: Mapping[str, Any],
    schema_record: Mapping[str, Any],
) -> tuple[dict[str, Any], Path, Path, dict[str, Any]]:
    value = _load_json(path, label="frozen episode-context configuration")
    prompt_path = _verify_record(prompt_record, label="frozen episode-context prompt")
    schema_path = _verify_record(schema_record, label="frozen episode-context schema")
    schema = _load_json(schema_path, label="frozen episode-context schema")
    _validate_output_schema(schema)
    expected = {
        "schema_version",
        "configuration_id",
        "lane",
        "label_pack",
        "model",
        "queue_payload_model",
        "reasoning_effort",
        "batch_size",
        "timeout_seconds",
        "transport",
        "auth_mode",
        "plan_type",
        "persistent_transport",
        "structured_output",
        "cli_version",
        "thread_mode",
        "capacity_admission",
        "semantic_retry_count",
        "ambiguous_retry_allowed",
        "deterministic_semantic_pruning",
        "semantic_authority_fields",
        "prompt",
        "prompt_sha256",
        "output_schema",
        "output_schema_sha256",
    }
    batch_size = value.get("batch_size")
    timeout = value.get("timeout_seconds")
    if (
        set(value) != expected
        or value.get("schema_version") != FROZEN_CONFIGURATION_VERSION
        or not isinstance(value.get("configuration_id"), str)
        or not value["configuration_id"]
        or not isinstance(value.get("lane"), str)
        or not value["lane"]
        or not isinstance(value.get("label_pack"), str)
        or not value["label_pack"]
        or not isinstance(value.get("model"), str)
        or not value["model"]
        or not isinstance(value.get("queue_payload_model"), str)
        or not value["queue_payload_model"]
        or value.get("reasoning_effort") not in {"low", "medium", "high", "xhigh"}
        or isinstance(batch_size, bool)
        or not isinstance(batch_size, int)
        or batch_size < 1
        or batch_size > 64
        or isinstance(timeout, bool)
        or not isinstance(timeout, (int, float))
        or not math.isfinite(float(timeout))
        or float(timeout) <= 0
        or value.get("transport") != TRANSPORT
        or value.get("auth_mode") != "chatgpt"
        or value.get("plan_type") != "pro"
        or value.get("persistent_transport") is not True
        or value.get("structured_output") is not True
        or value.get("cli_version") != PINNED_CLI_VERSION
        or value.get("thread_mode") != THREAD_MODE
        or value.get("capacity_admission") != CAPACITY_ADMISSION_CONTRACT
        or value.get("semantic_retry_count") != 0
        or value.get("ambiguous_retry_allowed") is not False
        or value.get("deterministic_semantic_pruning") is not False
        or value.get("semantic_authority_fields") != list(SEMANTIC_AUTHORITY_FIELDS)
        or value.get("prompt") != prompt_record
        or value.get("prompt_sha256") != _sha256_file(prompt_path)
        or value.get("output_schema") != schema_record
        or value.get("output_schema_sha256")
        != _sha256_bytes(_canonical_json(schema).encode())
    ):
        raise EpisodeContextRunnerError("frozen episode-context configuration drifted")
    return value, prompt_path, schema_path, schema


def load_episode_context_contract(path: Path) -> dict[str, Any]:
    contract_path = path.expanduser().resolve()
    contract = _load_json(contract_path, label="episode-context runner contract")
    expected = {
        "schema_version",
        "state",
        "phase",
        "transport",
        "auth",
        "lineage",
        "artifacts",
        "execution",
    }
    lineage = contract.get("lineage")
    artifacts = contract.get("artifacts")
    execution = contract.get("execution")
    if (
        set(contract) != expected
        or contract.get("schema_version") != CONTRACT_VERSION
        or contract.get("state") != "frozen_fixture_only"
        or contract.get("phase") != "episode_context"
        or contract.get("transport") != TRANSPORT
        or contract.get("auth")
        != {
            "type": "chatgpt",
            "plan_type": "pro",
            "api_key_billing_allowed": False,
            "raw_session_token_access_allowed": False,
        }
        or not isinstance(lineage, Mapping)
        or set(lineage)
        != {
            "evaluation_root",
            "evaluation_receipt",
            "holdout_receipt",
            "evaluation_id",
            "verified_artifact_hashes",
            "frozen_configuration_sha256",
        }
        or not isinstance(artifacts, Mapping)
        or set(artifacts)
        != {
            "codex_binary",
            "runner_source",
            "frozen_configuration",
            "prompt",
            "output_schema",
        }
        or execution
        != {
            "fixture_only": True,
            "live_model_calls_allowed": False,
            "live_database_mutation_allowed": False,
            "persistent_app_server_processes": 1,
            "thread_mode": THREAD_MODE,
            "required_episode_manifest": "immutable_before_claim",
            "capacity_admission": CAPACITY_ADMISSION_CONTRACT,
            "semantic_retry_count": 0,
            "ambiguous_retry_allowed": False,
            "stale_claim_replay_allowed": False,
            "deterministic_semantic_pruning": False,
        }
    ):
        raise EpisodeContextRunnerError("episode-context runner contract values drifted")
    artifact_paths = {
        key: _verify_record(value, label=key) for key, value in sorted(artifacts.items())
    }
    if artifact_paths["runner_source"] != Path(__file__).resolve():
        raise EpisodeContextRunnerError("episode-context runner source binding drifted")
    if artifact_paths["codex_binary"].name != "codex":
        raise EpisodeContextRunnerError("pinned official Codex binary binding is invalid")
    configuration, prompt_path, schema_path, schema = _validate_frozen_configuration(
        artifact_paths["frozen_configuration"],
        prompt_record=artifacts["prompt"],
        schema_record=artifacts["output_schema"],
    )
    configuration_sha256 = _sha256_file(artifact_paths["frozen_configuration"])
    if lineage.get("frozen_configuration_sha256") != configuration_sha256:
        raise EpisodeContextRunnerError("accepted configuration hash drifted")
    verified_evaluation, evaluation_receipt_path = _verified_evaluation(lineage)
    holdout_path = _verify_record(lineage.get("holdout_receipt"), label="holdout receipt")
    _verify_holdout_authorization(
        holdout_path,
        evaluation_id=str(lineage["evaluation_id"]),
        evaluation_receipt=lineage["evaluation_receipt"],
        frozen_configuration_sha256=configuration_sha256,
    )
    return {
        "path": contract_path,
        "sha256": _sha256_file(contract_path),
        "contract": contract,
        "configuration": configuration,
        "configuration_sha256": configuration_sha256,
        "prompt_path": prompt_path,
        "schema_path": schema_path,
        "schema": schema,
        "artifact_paths": artifact_paths,
        "verified_evaluation": verified_evaluation,
        "evaluation_receipt_path": evaluation_receipt_path,
        "holdout_receipt_path": holdout_path,
    }


def _artifact_lineage_path(artifact_path: Path) -> Path:
    source = artifact_path.expanduser().resolve()
    return source.with_suffix(source.suffix + ".app-server-lineage.json")


def _artifact_lineage_payload(
    *,
    loaded: Mapping[str, Any],
    item: EpisodeContextItem,
    raw_output_path: Path,
    persisted_artifact_path: Path,
) -> dict[str, Any]:
    raw = _load_json(raw_output_path, label="raw episode-context output")
    persisted = _load_json(
        persisted_artifact_path, label="persisted episode-context artifact"
    )
    raw_artifact = _validate_canonical_output(raw, expected_episode_id=item.episode_id)
    persisted_artifact = _validate_canonical_output(
        persisted, expected_episode_id=item.episode_id
    )
    if _semantic_fields(raw_artifact) != _semantic_fields(persisted_artifact):
        raise EpisodeContextRunnerError(
            "persisted episode-context artifact changed semantic authority fields"
        )
    sidecar_path = raw_output_path.parent / "sidecar.json"
    launch_path = raw_output_path.parent / "launch.json"
    return {
        "schema_version": ARTIFACT_LINEAGE_VERSION,
        "state": "verified",
        "contract_sha256": loaded["sha256"],
        "frozen_configuration_sha256": loaded["configuration_sha256"],
        "output_schema_sha256": loaded["configuration"]["output_schema_sha256"],
        "episode_id": item.episode_id,
        "context_run_id": item.context_run_id,
        "claim_id": item.claim_id,
        "attempt_id": item.attempt_id,
        "queue_attempt_number": item.queue_attempt_number,
        "semantic_authority_fields": list(SEMANTIC_AUTHORITY_FIELDS),
        "semantic_fields_sha256": _sha256_bytes(
            _canonical_json(_semantic_fields(persisted_artifact)).encode()
        ),
        "raw_output": _record(raw_output_path),
        "sidecar": _record(sidecar_path),
        "launch": _record(launch_path),
        "persisted_artifact": _record(persisted_artifact_path),
    }


def _verify_managed_context_artifact(
    *,
    artifact_path: Path,
    episode_id: str,
    label_pack: str,
    model: str,
    loaded: Mapping[str, Any] | None,
) -> dict[str, Any]:
    del label_pack, model  # Bound by the queue query; kept explicit at the call boundary.
    if loaded is None:
        raise EpisodeContextRunnerError("artifact verification lacks execution binding")
    artifact = _load_json(artifact_path, label="completed episode-context artifact")
    canonical = _validate_canonical_output(artifact, expected_episode_id=episode_id)
    lineage_path = _artifact_lineage_path(artifact_path)
    lineage = _load_json(lineage_path, label="episode-context artifact lineage")
    expected = {
        "schema_version",
        "state",
        "contract_sha256",
        "frozen_configuration_sha256",
        "output_schema_sha256",
        "episode_id",
        "context_run_id",
        "claim_id",
        "attempt_id",
        "queue_attempt_number",
        "semantic_authority_fields",
        "semantic_fields_sha256",
        "raw_output",
        "sidecar",
        "launch",
        "persisted_artifact",
    }
    if (
        set(lineage) != expected
        or lineage.get("schema_version") != ARTIFACT_LINEAGE_VERSION
        or lineage.get("state") != "verified"
        or lineage.get("contract_sha256") != loaded["sha256"]
        or lineage.get("frozen_configuration_sha256")
        != loaded["configuration_sha256"]
        or lineage.get("output_schema_sha256")
        != loaded["configuration"]["output_schema_sha256"]
        or lineage.get("episode_id") != episode_id
        or not isinstance(lineage.get("context_run_id"), str)
        or not lineage["context_run_id"]
        or not isinstance(lineage.get("claim_id"), str)
        or not lineage["claim_id"]
        or not isinstance(lineage.get("attempt_id"), str)
        or not lineage["attempt_id"]
        or isinstance(lineage.get("queue_attempt_number"), bool)
        or not isinstance(lineage.get("queue_attempt_number"), int)
        or lineage["queue_attempt_number"] < 1
        or lineage.get("semantic_authority_fields")
        != list(SEMANTIC_AUTHORITY_FIELDS)
        or lineage.get("semantic_fields_sha256")
        != _sha256_bytes(_canonical_json(_semantic_fields(canonical)).encode())
    ):
        raise EpisodeContextRunnerError("episode-context artifact lineage drifted")
    persisted = _verify_record(
        lineage["persisted_artifact"], label="lineage persisted artifact"
    )
    if persisted != artifact_path.expanduser().resolve():
        raise EpisodeContextRunnerError("episode-context artifact path binding drifted")
    for field in ("raw_output", "sidecar", "launch"):
        _verify_record(lineage[field], label=f"lineage {field}")
    return {"artifact": canonical, "lineage": lineage, "lineage_path": lineage_path}


class SQLiteEpisodeContextQueue:
    """Future live adapter, deliberately rejected by the v1 fixture runner.

    Its mutations are serialized and delegated to the existing worker
    operations.  Keeping the adapter here makes the intended cutover explicit
    while the runner's fixture gate prevents accidental use against a live DB.
    """

    fixture_only = False

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        lane: str,
        worker_id: str,
        label_pack: str,
        model: str,
        attempt_root: Path | None = None,
        source_loader: VerifiedSourceLoader | None = None,
    ) -> None:
        self.conn = conn
        self.lane = lane
        self.worker_id = worker_id
        self.label_pack = label_pack
        self.model = model
        self.attempt_root = (
            attempt_root.expanduser().resolve()
            if attempt_root is not None
            else None
        )
        self._required_manifest: dict[str, Any] | None = None
        self._required_manifest_record: dict[str, Any] | None = None
        self._loaded_contract: Mapping[str, Any] | None = None
        self._source_loader = source_loader or VerifiedSourceLoader(conn)

    def _item(self, row: Mapping[str, Any], *, source_input: Mapping[str, Any]) -> EpisodeContextItem:
        payload = loads_json(str(row.get("payload_json") or "{}"), {})
        attempt = int(row.get("attempts") or 0)
        claim_id = str(payload.get("app_server_claim_id") or stable_id(
            str(row["id"]), self.worker_id, str(attempt), prefix="ectxc_"
        ))
        attempt_id = str(payload.get("app_server_attempt_id") or stable_id(
            claim_id, str(row.get("updated_at") or ""), prefix="ectxa_"
        ))
        return EpisodeContextItem(
            job_id=int(row["id"]),
            episode_id=str(row["target_id"]),
            context_run_id=str(payload.get("episode_context_run_id") or ""),
            claim_id=claim_id,
            attempt_id=attempt_id,
            source_input=dict(source_input),
            queue_attempt_number=max(1, attempt),
            submission_output_path=(
                str(payload["output_path"]) if payload.get("output_path") else None
            ),
        )

    def discover_required_episode_ids(self) -> list[str]:
        rows = self.conn.execute(
            """
            SELECT DISTINCT segments.episode_id AS episode_id
            FROM jobs AS label_jobs
            JOIN segments ON segments.id = label_jobs.target_id
            WHERE label_jobs.lane = ?
              AND label_jobs.job_type = 'label_segment'
              AND label_jobs.status IN ('pending', 'claimed', 'failed')
              AND json_extract(label_jobs.payload_json, '$.label_pack') = ?
              AND (
                json_extract(label_jobs.payload_json, '$.queue_payload_model') = ?
                OR (
                  json_extract(label_jobs.payload_json, '$.queue_payload_model') IS NULL
                  AND (
                    json_extract(label_jobs.payload_json, '$.model') IS NULL
                    OR json_extract(label_jobs.payload_json, '$.model') = ?
                  )
                )
              )
            ORDER BY segments.episode_id
            """,
            (self.lane, self.label_pack, self.model, self.model),
        ).fetchall()
        return [str(row["episode_id"]) for row in rows]

    def bind_required_episode_manifest(
        self, manifest: Mapping[str, Any], record: Mapping[str, Any]
    ) -> None:
        episode_ids = manifest.get("episode_ids")
        if not isinstance(episode_ids, list):
            raise EpisodeContextRunnerError("required manifest lacks episode ids")
        self._required_manifest = dict(manifest)
        self._required_manifest_record = dict(record)

    def bind_execution_contract(self, loaded: Mapping[str, Any]) -> None:
        configuration = loaded["configuration"]
        if (
            configuration["lane"] != self.lane
            or configuration["label_pack"] != self.label_pack
            or configuration["queue_payload_model"] != self.model
        ):
            raise EpisodeContextRunnerError("SQLite queue scope differs from frozen contract")
        self._loaded_contract = loaded
        if self.attempt_root is None:
            self.attempt_root = (
                Path(loaded["path"]).resolve().parent
                / "managed-context-attempts"
            )

    def _require_attempt_history_table(self) -> None:
        observed = _attempt_schema_observation(self.conn)
        if (
            observed["columns"] != _ATTEMPT_TARGET_COLUMNS
            or observed["foreign_keys"] != _ATTEMPT_TARGET_FOREIGN_KEYS
            or not all(observed["constraints"].values())
        ):
            raise EpisodeContextRunnerError(
                "episode-context attempt-history target schema is unavailable"
            )

    def _attempt_paths(self, attempt_id: str) -> dict[str, Path]:
        if self.attempt_root is None:
            raise EpisodeContextRunnerError(
                "episode-context managed attempt root is not bound"
            )
        root = self.attempt_root / attempt_id
        return {
            "root": root,
            "prompt": root / "prompt.private.json",
            "output": root / "output.private.json",
            "artifact": root / "episode-context.json",
            "launch": root / "launch.json",
            "sidecar": root / "sidecar.json",
        }

    def _required_ids(self) -> set[str]:
        if self._required_manifest is None or self._required_manifest_record is None:
            raise EpisodeContextRunnerError("required episode manifest is not bound")
        return set(str(item) for item in self._required_manifest["episode_ids"])

    def preview(self, *, limit: int) -> list[EpisodeContextItem]:
        rows = self.conn.execute(
            """
            SELECT * FROM jobs
            WHERE lane = ? AND job_type = 'episode_context' AND status = 'pending'
              AND attempts < max_attempts
              AND json_extract(payload_json, '$.label_pack') = ?
              AND (
                json_extract(payload_json, '$.model') IS NULL
                OR json_extract(payload_json, '$.model') = ?
              )
              AND target_id IN (
                SELECT CAST(value AS TEXT) FROM json_each(?)
              )
            ORDER BY priority, id LIMIT ?
            """,
            (
                self.lane,
                self.label_pack,
                self.model,
                _canonical_json(sorted(self._required_ids())),
                int(limit),
            ),
        ).fetchall()
        return [
            self._item(
                dict(row),
                source_input={"episode_id": str(row["target_id"]), "preview_only": True},
            )
            for row in rows
        ]

    def stale_claims(self, *, limit: int) -> list[EpisodeContextItem]:
        rows = self.conn.execute(
            """
            SELECT * FROM jobs
            WHERE lane = ? AND job_type = 'episode_context' AND status = 'claimed'
              AND leased_until < ?
              AND json_extract(payload_json, '$.label_pack') = ?
              AND (
                json_extract(payload_json, '$.model') IS NULL
                OR json_extract(payload_json, '$.model') = ?
              )
              AND target_id IN (
                SELECT CAST(value AS TEXT) FROM json_each(?)
              )
            ORDER BY priority, id LIMIT ?
            """,
            (
                self.lane,
                now_iso(),
                self.label_pack,
                self.model,
                _canonical_json(sorted(self._required_ids())),
                int(limit),
            ),
        ).fetchall()
        items = []
        for row in rows:
            payload = loads_json(str(row["payload_json"] or "{}"), {})
            prompt_path = Path(str(payload.get("prompt_path") or "")).expanduser()
            source = {
                "episode_id": str(row["target_id"]),
                "existing_full_episode_prompt": (
                    prompt_path.read_text(encoding="utf-8") if prompt_path.is_file() else None
                ),
            }
            items.append(self._item(dict(row), source_input=source))
        return items

    def recover_stale_unbound_claims(self, *, limit: int) -> int:
        """Release stale prelaunch claims without replaying semantic work."""

        self._require_attempt_history_table()
        rows = self.conn.execute(
            """
            SELECT * FROM jobs
            WHERE lane = ? AND job_type = 'episode_context' AND status = 'claimed'
              AND leased_until < ?
              AND json_extract(payload_json, '$.label_pack') = ?
              AND (
                json_extract(payload_json, '$.model') IS NULL
                OR json_extract(payload_json, '$.model') = ?
              )
              AND target_id IN (
                SELECT CAST(value AS TEXT) FROM json_each(?)
              )
            ORDER BY priority, id LIMIT ?
            """,
            (
                self.lane,
                now_iso(),
                self.label_pack,
                self.model,
                _canonical_json(sorted(self._required_ids())),
                int(limit),
            ),
        ).fetchall()
        recovered = 0
        for row in rows:
            payload = loads_json(str(row["payload_json"] or "{}"), {})
            attempt_id = payload.get("episode_context_attempt_id")
            attempt = (
                self.conn.execute(
                    "SELECT * FROM episode_context_run_attempts WHERE id = ?",
                    (str(attempt_id),),
                ).fetchone()
                if isinstance(attempt_id, str) and attempt_id
                else None
            )
            bare_claim = attempt is None and not any(
                payload.get(field)
                for field in (
                    "episode_context_run_id",
                    "prompt_path",
                    "output_path",
                    "context_artifact_path",
                    "app_server_claim_id",
                    "app_server_attempt_id",
                    "app_server_launch_path",
                    "app_server_sidecar_path",
                )
            )
            managed_prelaunch = False
            if attempt is not None:
                expected_paths = self._attempt_paths(str(attempt_id))
                managed_prelaunch = (
                    str(attempt["attempt_kind"]) == "managed_app_server"
                    and str(attempt["status"]) == "claimed"
                    and int(attempt["job_id"]) == int(row["id"])
                    and str(attempt["episode_id"]) == str(row["target_id"])
                    and str(attempt["canonical_run_id"])
                    == str(payload.get("episode_context_run_id") or "")
                    and str(attempt["claim_id"])
                    == str(payload.get("app_server_claim_id") or "")
                    and not attempt["launch_path"]
                    and not attempt["sidecar_path"]
                    and not payload.get("app_server_launch_path")
                    and not payload.get("app_server_sidecar_path")
                    and not expected_paths["launch"].exists()
                    and not expected_paths["sidecar"].exists()
                    and not expected_paths["output"].exists()
                    and not expected_paths["artifact"].exists()
                )
            if not bare_claim and not managed_prelaunch:
                continue
            timestamp = now_iso()
            scrubbed = {
                key: value
                for key, value in payload.items()
                if key
                not in {
                    "semantic_model",
                    "episode_context_run_id",
                    "episode_context_attempt_id",
                    "episode_context_schema_version",
                    "prompt_path",
                    "output_path",
                    "context_artifact_path",
                    "app_server_claim_id",
                    "app_server_attempt_id",
                    "app_server_contract_sha256",
                    "app_server_frozen_configuration_sha256",
                    "app_server_launch_path",
                    "app_server_sidecar_path",
                    "app_server_attempt_launch_path",
                    "app_server_attempt_sidecar_path",
                }
            }
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                if managed_prelaunch:
                    updated_attempt = self.conn.execute(
                        """
                        UPDATE episode_context_run_attempts
                        SET status = 'released_prelaunch',
                            error = 'stale_prelaunch_claim_released_without_semantic_turn',
                            completed_at = ?, updated_at = ?
                        WHERE id = ? AND status = 'claimed'
                          AND launch_path IS NULL AND sidecar_path IS NULL
                        """,
                        (timestamp, timestamp, str(attempt_id)),
                    )
                    if updated_attempt.rowcount != 1:
                        raise EpisodeContextRunnerWaiting(
                            "prelaunch context attempt changed before release"
                        )
                    canonical = self.conn.execute(
                        "SELECT * FROM episode_context_runs WHERE id = ?",
                        (str(attempt["canonical_run_id"]),),
                    ).fetchone()
                    if canonical is None:
                        raise EpisodeContextRunnerError(
                            "prelaunch context attempt lost its canonical run"
                        )
                    legacy_count = int(
                        self.conn.execute(
                            """
                            SELECT COUNT(*) FROM episode_context_run_attempts
                            WHERE canonical_run_id = ?
                              AND attempt_kind = 'legacy_canonical_snapshot'
                            """,
                            (str(attempt["canonical_run_id"]),),
                        ).fetchone()[0]
                    )
                    if legacy_count == 0 and str(canonical["status"]) == "claimed":
                        self.conn.execute(
                            """
                            UPDATE episode_context_runs
                            SET status = 'failed',
                                error = 'managed_prelaunch_attempt_released',
                                updated_at = ?
                            WHERE id = ? AND status = 'claimed'
                            """,
                            (timestamp, str(attempt["canonical_run_id"])),
                        )
                updated_job = self.conn.execute(
                    """
                    UPDATE jobs
                    SET status = 'pending', payload_json = ?, lease_owner = NULL,
                        leased_until = NULL,
                        attempts = CASE WHEN attempts > 0 THEN attempts - 1 ELSE 0 END,
                        error = NULL, updated_at = ?
                    WHERE id = ? AND status = 'claimed' AND leased_until < ?
                      AND payload_json = ?
                    """,
                    (
                        _canonical_json(scrubbed),
                        timestamp,
                        int(row["id"]),
                        timestamp,
                        str(row["payload_json"]),
                    ),
                )
                if updated_job.rowcount != 1:
                    raise EpisodeContextRunnerWaiting(
                        "stale prelaunch context job changed before release"
                    )
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise
            recovered += 1
        return recovered

    def release_prelaunch_claim(
        self, item: EpisodeContextItem, *, reason: str
    ) -> None:
        """Release one exact unlaunched attempt after a second capacity stop."""

        if not reason or len(reason) > 200:
            raise EpisodeContextRunnerError("prelaunch release reason is invalid")
        self._require_attempt_history_table()
        attempt = self.conn.execute(
            "SELECT * FROM episode_context_run_attempts WHERE id = ?",
            (item.attempt_id,),
        ).fetchone()
        job = self.conn.execute(
            "SELECT * FROM jobs WHERE id = ?", (item.job_id,)
        ).fetchone()
        if attempt is None or job is None:
            raise EpisodeContextRunnerError("prelaunch release lineage is absent")
        payload = loads_json(str(job["payload_json"] or "{}"), {})
        paths = self._attempt_paths(item.attempt_id)
        if (
            str(attempt["status"]) != "claimed"
            or int(attempt["job_id"]) != item.job_id
            or str(attempt["episode_id"]) != item.episode_id
            or str(attempt["canonical_run_id"]) != item.context_run_id
            or str(attempt["claim_id"]) != item.claim_id
            or str(job["status"]) != "claimed"
            or payload.get("episode_context_attempt_id") != item.attempt_id
            or attempt["launch_path"]
            or attempt["sidecar_path"]
            or any(
                paths[field].exists()
                for field in ("launch", "sidecar", "output", "artifact")
            )
        ):
            raise EpisodeContextRunnerError(
                "prelaunch release cannot prove zero semantic dispatch"
            )
        scrubbed = {
            key: value
            for key, value in payload.items()
            if key
            not in {
                "semantic_model",
                "episode_context_run_id",
                "episode_context_attempt_id",
                "episode_context_schema_version",
                "prompt_path",
                "output_path",
                "context_artifact_path",
                "app_server_claim_id",
                "app_server_attempt_id",
                "app_server_contract_sha256",
                "app_server_frozen_configuration_sha256",
                "app_server_launch_path",
                "app_server_sidecar_path",
                "app_server_attempt_launch_path",
                "app_server_attempt_sidecar_path",
            }
        }
        timestamp = now_iso()
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            updated_attempt = self.conn.execute(
                """
                UPDATE episode_context_run_attempts
                SET status = 'released_prelaunch', error = ?,
                    completed_at = ?, updated_at = ?
                WHERE id = ? AND status = 'claimed'
                  AND launch_path IS NULL AND sidecar_path IS NULL
                """,
                (reason, timestamp, timestamp, item.attempt_id),
            )
            updated_job = self.conn.execute(
                """
                UPDATE jobs
                SET status = 'pending', payload_json = ?, lease_owner = NULL,
                    leased_until = NULL,
                    attempts = CASE WHEN attempts > 0 THEN attempts - 1 ELSE 0 END,
                    error = NULL, updated_at = ?
                WHERE id = ? AND status = 'claimed' AND payload_json = ?
                """,
                (
                    _canonical_json(scrubbed),
                    timestamp,
                    item.job_id,
                    str(job["payload_json"]),
                ),
            )
            if updated_attempt.rowcount != 1 or updated_job.rowcount != 1:
                raise EpisodeContextRunnerWaiting(
                    "prelaunch release changed during atomic reconciliation"
                )
            legacy_count = int(
                self.conn.execute(
                    """
                    SELECT COUNT(*) FROM episode_context_run_attempts
                    WHERE canonical_run_id = ?
                      AND attempt_kind = 'legacy_canonical_snapshot'
                    """,
                    (item.context_run_id,),
                ).fetchone()[0]
            )
            if legacy_count == 0:
                self.conn.execute(
                    """
                    UPDATE episode_context_runs
                    SET status = 'failed', error = ?, updated_at = ?
                    WHERE id = ? AND status = 'claimed'
                    """,
                    (reason, timestamp, item.context_run_id),
                )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def bind_prelaunch_capacity(
        self, item: EpisodeContextItem, capacity_binding: Mapping[str, Any]
    ) -> None:
        """Bind complete no-turn probes, including a preturn-probe crash shape."""

        self._require_attempt_history_table()
        if (
            not isinstance(capacity_binding, Mapping)
            or set(capacity_binding) not in ({"preclaim"}, {"preclaim", "preturn"})
        ):
            raise EpisodeContextRunnerError(
                "managed context prelaunch capacity binding shape drifted"
            )
        paths: dict[str, str | None] = {
            "preclaim_capacity_request_path": None,
            "preclaim_capacity_admission_path": None,
            "preturn_capacity_request_path": None,
            "preturn_capacity_admission_path": None,
        }
        for stage in capacity_binding:
            binding = capacity_binding[stage]
            if not isinstance(binding, Mapping) or set(binding) != {
                "request",
                "admission",
            }:
                raise EpisodeContextRunnerError(
                    "managed context prelaunch capacity record shape drifted"
                )
            request_path = _verify_record(
                binding["request"], label=f"managed context {stage} request"
            )
            admission_path = _verify_record(
                binding["admission"], label=f"managed context {stage} admission"
            )
            request = _load_json(
                request_path, label=f"managed context {stage} request"
            )
            admission = _load_json(
                admission_path, label=f"managed context {stage} admission"
            )
            identity = request.get("requested_item")
            if (
                request.get("schema_version") != LIVE_CAPACITY_REQUEST_VERSION
                or request.get("admission_stage") != stage
                or not isinstance(identity, Mapping)
                or identity.get("job_id") != item.job_id
                or identity.get("episode_id") != item.episode_id
                or (
                    stage == "preclaim"
                    and (
                        identity.get("claim_id") is not None
                        or identity.get("attempt_id") is not None
                    )
                )
                or (
                    stage == "preturn"
                    and (
                        identity.get("claim_id") != item.claim_id
                        or identity.get("attempt_id") != item.attempt_id
                    )
                )
                or admission.get("schema_version")
                != LIVE_CAPACITY_ADMISSION_VERSION
                or admission.get("request_sha256") != request.get("request_sha256")
                or admission.get("admission_stage") != stage
                or admission.get("state") not in {"admitted", "denied"}
            ):
                raise EpisodeContextRunnerError(
                    "managed context prelaunch capacity lifecycle drifted"
                )
            paths[f"{stage}_capacity_request_path"] = str(request_path)
            paths[f"{stage}_capacity_admission_path"] = str(admission_path)
        attempt = self.conn.execute(
            "SELECT * FROM episode_context_run_attempts WHERE id = ?",
            (item.attempt_id,),
        ).fetchone()
        if (
            attempt is None
            or str(attempt["status"]) != "claimed"
            or int(attempt["job_id"]) != item.job_id
            or str(attempt["claim_id"]) != item.claim_id
            or any(
                attempt[field] not in {None, value}
                for field, value in paths.items()
            )
        ):
            raise EpisodeContextRunnerError(
                "managed context prelaunch attempt lineage drifted"
            )
        records = loads_json(str(attempt["artifact_records_json"] or "{}"), {})
        if not isinstance(records, dict) or "prompt" not in records:
            raise EpisodeContextRunnerError(
                "managed context prelaunch source lineage is absent"
            )
        records["capacity"] = copy.deepcopy(dict(capacity_binding))
        timestamp = now_iso()
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            updated = self.conn.execute(
                """
                UPDATE episode_context_run_attempts
                SET preclaim_capacity_request_path = ?,
                    preclaim_capacity_admission_path = ?,
                    preturn_capacity_request_path = ?,
                    preturn_capacity_admission_path = ?,
                    artifact_records_json = ?, updated_at = ?
                WHERE id = ? AND status = 'claimed' AND claim_id = ?
                """,
                (
                    paths["preclaim_capacity_request_path"],
                    paths["preclaim_capacity_admission_path"],
                    paths["preturn_capacity_request_path"],
                    paths["preturn_capacity_admission_path"],
                    _canonical_json(records),
                    timestamp,
                    item.attempt_id,
                    item.claim_id,
                ),
            )
            if updated.rowcount != 1:
                raise EpisodeContextRunnerWaiting(
                    "managed context prelaunch capacity changed before binding"
                )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def reverify_source(self, item: EpisodeContextItem) -> Mapping[str, Any]:
        """Freshly hash/re-offset every claimed source and compare its snapshot."""

        self._require_attempt_history_table()
        attempt = self.conn.execute(
            "SELECT * FROM episode_context_run_attempts WHERE id = ?",
            (item.attempt_id,),
        ).fetchone()
        if (
            attempt is None
            or str(attempt["status"]) != "claimed"
            or int(attempt["job_id"]) != item.job_id
            or str(attempt["episode_id"]) != item.episode_id
            or str(attempt["claim_id"]) != item.claim_id
        ):
            raise EpisodeContextRunnerError(
                "managed context source revalidation attempt drifted"
            )
        try:
            fresh_loader = getattr(self._source_loader, "fresh_episode_packet", None)
            source_input = (
                fresh_loader(item.episode_id)
                if callable(fresh_loader)
                else self._source_loader.episode_packet(item.episode_id)
            )
        except SourceIntegrityError as exc:
            raise EpisodeContextRunnerWaiting(
                "managed episode-context source integrity drifted after claim"
            ) from exc
        if (
            not isinstance(source_input, Mapping)
            or _canonical_json(dict(source_input))
            != _canonical_json(dict(item.source_input))
        ):
            raise EpisodeContextRunnerWaiting(
                "managed episode-context source packet drifted after claim"
            )
        records = loads_json(str(attempt["artifact_records_json"] or "{}"), {})
        if not isinstance(records, Mapping) or "prompt" not in records:
            raise EpisodeContextRunnerError(
                "managed context source revalidation lacks frozen snapshot"
            )
        prompt_path = _verify_record(
            records["prompt"], label="managed context source revalidation snapshot"
        )
        prompt_value = _load_json(
            prompt_path, label="managed context source revalidation snapshot"
        )
        if _canonical_json(prompt_value) != _canonical_json(dict(source_input)):
            raise EpisodeContextRunnerError(
                "managed context fresh source differs from attempt snapshot"
            )
        return dict(source_input)

    def claim(self, *, limit: int) -> list[EpisodeContextItem]:
        if self._loaded_contract is None:
            raise EpisodeContextRunnerError(
                "episode-context claim lacks frozen execution contract"
            )
        self._require_attempt_history_table()
        items = []
        required_ids_json = _canonical_json(sorted(self._required_ids()))
        for _ in range(int(limit)):
            candidate = self.conn.execute(
                """
                SELECT * FROM jobs
                WHERE lane = ?
                  AND job_type = 'episode_context'
                  AND status = 'pending'
                  AND attempts < max_attempts
                  AND json_extract(payload_json, '$.label_pack') = ?
                  AND (
                    json_extract(payload_json, '$.model') IS NULL
                    OR json_extract(payload_json, '$.model') = ?
                  )
                  AND target_id IN (
                    SELECT CAST(value AS TEXT) FROM json_each(?)
                  )
                ORDER BY priority ASC, id ASC
                LIMIT 1
                """,
                (
                    self.lane,
                    self.label_pack,
                    self.model,
                    required_ids_json,
                ),
            ).fetchone()
            if candidate is None:
                break
            candidate_payload = str(candidate["payload_json"] or "{}")
            episode_id = str(candidate["target_id"])
            try:
                source_input = self._source_loader.episode_packet(episode_id)
            except SourceIntegrityError as exc:
                raise EpisodeContextRunnerError(
                    "managed episode-context source integrity failed before claim"
                ) from exc
            episode = source_input.get("episode")
            if (
                not isinstance(episode, Mapping)
                or episode.get("episode_id") != episode_id
                or not isinstance(
                    source_input.get("full_segmented_episode_text"), str
                )
                or not source_input["full_segmented_episode_text"]
            ):
                raise EpisodeContextRunnerError(
                    "managed episode-context source packet is incomplete"
                )
            now = now_iso()
            leased_until = (
                dt.datetime.fromisoformat(now) + dt.timedelta(minutes=45)
            ).isoformat()
            canonical_row = self.conn.execute(
                """
                SELECT * FROM episode_context_runs
                WHERE episode_id = ? AND label_pack = ? AND model = ?
                """,
                (episode_id, self.label_pack, self.model),
            ).fetchone()
            canonical_run_id = (
                str(canonical_row["id"])
                if canonical_row is not None
                else stable_id(
                    episode_id,
                    self.label_pack,
                    self.model,
                    prefix="ectx_",
                )
            )
            prior_attempt_number = int(
                self.conn.execute(
                    """
                    SELECT COALESCE(MAX(attempt_number), 0)
                    FROM episode_context_run_attempts
                    WHERE canonical_run_id = ?
                    """,
                    (canonical_run_id,),
                ).fetchone()[0]
            )
            attempt_number = prior_attempt_number + 1
            attempt_id = stable_id(
                canonical_run_id,
                str(candidate["id"]),
                str(attempt_number),
                self._loaded_contract["sha256"],
                self._loaded_contract["configuration_sha256"],
                prefix="ectxa_",
            )
            claim_id = stable_id(
                attempt_id,
                self.worker_id,
                prefix="ectxc_",
            )
            attempt_paths = self._attempt_paths(attempt_id)
            _write_immutable(
                attempt_paths["prompt"],
                _pretty_json(dict(source_input)).encode("utf-8"),
            )
            prompt_record = _record(attempt_paths["prompt"])
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                current = self.conn.execute(
                    "SELECT * FROM jobs WHERE id = ?",
                    (int(candidate["id"]),),
                ).fetchone()
                if (
                    current is None
                    or str(current["status"]) != "pending"
                    or str(current["target_id"]) != episode_id
                    or str(current["payload_json"] or "{}")
                    != candidate_payload
                    or int(current["attempts"]) >= int(current["max_attempts"])
                ):
                    raise EpisodeContextRunnerWaiting(
                        "episode-context candidate changed before atomic managed claim"
                    )
                payload = loads_json(candidate_payload, {})
                if (
                    payload.get("label_pack") != self.label_pack
                    or payload.get("model") not in {None, self.model}
                ):
                    raise EpisodeContextRunnerError(
                        "atomic episode-context claim escaped frozen queue scope"
                    )
                transaction_canonical = self.conn.execute(
                    """
                    SELECT * FROM episode_context_runs
                    WHERE episode_id = ? AND label_pack = ? AND model = ?
                    """,
                    (episode_id, self.label_pack, self.model),
                ).fetchone()
                if (
                    (transaction_canonical is None) != (canonical_row is None)
                    or (
                        transaction_canonical is not None
                        and str(transaction_canonical["id"])
                        != canonical_run_id
                    )
                ):
                    raise EpisodeContextRunnerWaiting(
                        "canonical episode-context run changed before managed claim"
                    )
                transaction_prior_attempt = int(
                    self.conn.execute(
                        """
                        SELECT COALESCE(MAX(attempt_number), 0)
                        FROM episode_context_run_attempts
                        WHERE canonical_run_id = ?
                        """,
                        (canonical_run_id,),
                    ).fetchone()[0]
                )
                if transaction_prior_attempt != prior_attempt_number:
                    raise EpisodeContextRunnerWaiting(
                        "episode-context attempt sequence changed before managed claim"
                    )
                if transaction_canonical is None:
                    self.conn.execute(
                        """
                        INSERT INTO episode_context_runs
                          (id, job_id, episode_id, transcript_id, label_pack, model,
                           status, prompt_path, output_path, speaker_map_json,
                           section_map_json, entity_seed_json, concept_seed_json,
                           extraction_guidance, error, created_at, updated_at,
                           completed_at)
                        VALUES (?, ?, ?, ?, ?, ?, 'claimed', ?, ?, '[]', '[]',
                                '{}', '[]', NULL, NULL, ?, ?, NULL)
                        """,
                        (
                            canonical_run_id,
                            int(candidate["id"]),
                            episode_id,
                            episode.get("transcript_id"),
                            self.label_pack,
                            self.model,
                            str(attempt_paths["prompt"]),
                            str(attempt_paths["output"]),
                            now,
                            now,
                        ),
                    )
                elif str(transaction_canonical["status"]) == "completed":
                    snapshot_count = int(
                        self.conn.execute(
                            """
                            SELECT COUNT(*)
                            FROM episode_context_run_attempts
                            WHERE canonical_run_id = ?
                              AND attempt_kind = 'legacy_canonical_snapshot'
                            """,
                            (canonical_run_id,),
                        ).fetchone()[0]
                    )
                    if snapshot_count != 1:
                        raise EpisodeContextRunnerError(
                            "completed legacy context lacks immutable rerun snapshot"
                        )
                elif str(transaction_canonical["status"]) == "failed":
                    self.conn.execute(
                        """
                        UPDATE episode_context_runs
                        SET job_id = ?, transcript_id = ?, status = 'claimed',
                            prompt_path = ?, output_path = ?, error = NULL,
                            completed_at = NULL, updated_at = ?
                        WHERE id = ? AND status = 'failed'
                        """,
                        (
                            int(candidate["id"]),
                            episode.get("transcript_id"),
                            str(attempt_paths["prompt"]),
                            str(attempt_paths["output"]),
                            now,
                            canonical_run_id,
                        ),
                    )
                else:
                    raise EpisodeContextRunnerError(
                        "canonical context run is not claimable for a fresh attempt"
                    )
                payload.update(
                    {
                        "label_pack": self.label_pack,
                        "model": self.model,
                        "semantic_model": self._loaded_contract["configuration"][
                            "model"
                        ],
                        "episode_context_run_id": canonical_run_id,
                        "episode_context_attempt_id": attempt_id,
                        "episode_context_schema_version": (
                            EPISODE_CONTEXT_SCHEMA_VERSION
                        ),
                        "prompt_path": str(attempt_paths["prompt"]),
                        "output_path": str(attempt_paths["output"]),
                        "context_artifact_path": str(attempt_paths["artifact"]),
                        "app_server_claim_id": claim_id,
                        "app_server_attempt_id": attempt_id,
                        "app_server_contract_sha256": self._loaded_contract[
                            "sha256"
                        ],
                        "app_server_frozen_configuration_sha256": (
                            self._loaded_contract["configuration_sha256"]
                        ),
                    }
                )
                self.conn.execute(
                    """
                    INSERT INTO episode_context_run_attempts
                      (id, canonical_run_id, job_id, episode_id, transcript_id,
                       label_pack, queue_payload_model, semantic_model,
                       reasoning_effort, attempt_kind, status, attempt_number,
                       claim_id, contract_sha256, frozen_configuration_sha256,
                       prompt_path, output_path, context_artifact_path,
                       launch_path, sidecar_path, canonical_snapshot_json,
                       artifact_records_json, error, created_at, updated_at,
                       completed_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'managed_app_server',
                            'claimed', ?, ?, ?, ?, ?, ?, ?, NULL, NULL, '{}', ?,
                            NULL, ?, ?, NULL)
                    """,
                    (
                        attempt_id,
                        canonical_run_id,
                        int(candidate["id"]),
                        episode_id,
                        episode.get("transcript_id"),
                        self.label_pack,
                        self.model,
                        self._loaded_contract["configuration"]["model"],
                        self._loaded_contract["configuration"][
                            "reasoning_effort"
                        ],
                        attempt_number,
                        claim_id,
                        self._loaded_contract["sha256"],
                        self._loaded_contract["configuration_sha256"],
                        str(attempt_paths["prompt"]),
                        str(attempt_paths["output"]),
                        str(attempt_paths["artifact"]),
                        _canonical_json({"prompt": prompt_record}),
                        now,
                        now,
                    ),
                )
                updated = self.conn.execute(
                    """
                    UPDATE jobs
                    SET status = 'claimed', lease_owner = ?, leased_until = ?,
                        attempts = attempts + 1, payload_json = ?, updated_at = ?
                    WHERE id = ? AND status = 'pending'
                      AND payload_json = ? AND attempts < max_attempts
                    """,
                    (
                        self.worker_id,
                        leased_until,
                        _canonical_json(payload),
                        now,
                        int(candidate["id"]),
                        candidate_payload,
                    ),
                )
                if updated.rowcount != 1:
                    raise EpisodeContextRunnerWaiting(
                        "managed episode-context claim was not atomic"
                    )
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise
            refreshed = self.conn.execute(
                "SELECT * FROM jobs WHERE id = ?", (int(candidate["id"]),)
            ).fetchone()
            if refreshed is None:
                raise EpisodeContextRunnerError("claimed episode-context job disappeared")
            items.append(self._item(dict(refreshed), source_input=source_input))
        return items

    def bind_launch(self, item: EpisodeContextItem, launch_path: Path) -> None:
        if self._loaded_contract is None:
            raise EpisodeContextRunnerError(
                "episode-context launch lacks frozen execution contract"
            )
        self._require_attempt_history_table()
        resolved_launch = launch_path.expanduser().resolve()
        if not resolved_launch.is_file():
            raise EpisodeContextRunnerError(
                "managed episode-context launch artifact is missing"
            )
        launch_value = _load_json(
            resolved_launch, label="managed episode-context launch"
        )
        input_path = _verify_record(
            launch_value.get("input"), label="managed episode-context launch input"
        )
        _input, launch_value = _validate_turn_input_launch(
            loaded=self._loaded_contract,
            input_path=input_path,
            launch_path=resolved_launch,
        )
        if any(
            launch_value.get(field) != getattr(item, field)
            for field in (
                "job_id",
                "episode_id",
                "context_run_id",
                "claim_id",
                "attempt_id",
                "queue_attempt_number",
            )
        ):
            raise EpisodeContextRunnerError(
                "managed episode-context launch item lineage drifted"
            )
        attempt_paths = self._attempt_paths(item.attempt_id)
        preflight_attempt = self.conn.execute(
            "SELECT * FROM episode_context_run_attempts WHERE id = ?",
            (item.attempt_id,),
        ).fetchone()
        if (
            preflight_attempt is None
            or str(preflight_attempt["status"]) != "claimed"
            or int(preflight_attempt["job_id"]) != item.job_id
            or str(preflight_attempt["canonical_run_id"]) != item.context_run_id
            or str(preflight_attempt["claim_id"]) != item.claim_id
        ):
            raise EpisodeContextRunnerError(
                "managed episode-context launch attempt lineage drifted"
            )
        _validate_attempt_source_binding(
            attempt=preflight_attempt,
            input_path=input_path,
            input_value=_input,
            require_bound_turn_input=False,
        )
        _write_immutable(attempt_paths["launch"], resolved_launch.read_bytes())
        timestamp = now_iso()
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            row = self.conn.execute(
                "SELECT payload_json FROM jobs WHERE id = ?", (item.job_id,)
            ).fetchone()
            if row is None:
                raise EpisodeContextRunnerError("claimed episode-context job disappeared")
            payload = loads_json(str(row["payload_json"] or "{}"), {})
            attempt = self.conn.execute(
                "SELECT * FROM episode_context_run_attempts WHERE id = ?",
                (item.attempt_id,),
            ).fetchone()
            if (
                attempt is None
                or str(attempt["status"]) != "claimed"
                or int(attempt["job_id"]) != item.job_id
                or str(attempt["canonical_run_id"]) != item.context_run_id
                or str(attempt["claim_id"]) != item.claim_id
                or payload.get("episode_context_attempt_id") != item.attempt_id
            ):
                raise EpisodeContextRunnerError(
                    "managed episode-context launch attempt lineage drifted"
                )
            source_records, turn_input_record = _validate_attempt_source_binding(
                attempt=attempt,
                input_path=input_path,
                input_value=_input,
                require_bound_turn_input=False,
            )
            source_records["turn_input"] = turn_input_record
            turn_sidecar_path = resolved_launch.parent / "sidecar.json"
            capacity_binding = launch_value["capacity_admission"]
            if set(capacity_binding) == {"preclaim", "preturn"}:
                preclaim_request = str(
                    _verify_record(
                        capacity_binding["preclaim"]["request"],
                        label="bound preclaim capacity request",
                    )
                )
                preclaim_admission = str(
                    _verify_record(
                        capacity_binding["preclaim"]["admission"],
                        label="bound preclaim capacity admission",
                    )
                )
                preturn_request = str(
                    _verify_record(
                        capacity_binding["preturn"]["request"],
                        label="bound preturn capacity request",
                    )
                )
                preturn_admission = str(
                    _verify_record(
                        capacity_binding["preturn"]["admission"],
                        label="bound preturn capacity admission",
                    )
                )
            elif set(capacity_binding) == {"request", "admission"}:
                preclaim_request = str(
                    _verify_record(
                        capacity_binding["request"],
                        label="bound fixture capacity request",
                    )
                )
                preclaim_admission = str(
                    _verify_record(
                        capacity_binding["admission"],
                        label="bound fixture capacity admission",
                    )
                )
                preturn_request = None
                preturn_admission = None
            else:
                raise EpisodeContextRunnerError(
                    "managed episode-context capacity binding shape drifted"
                )
            payload.update(
                {
                    "app_server_claim_id": item.claim_id,
                    "app_server_attempt_id": item.attempt_id,
                    "app_server_launch_path": str(resolved_launch),
                    "app_server_sidecar_path": str(turn_sidecar_path),
                    "app_server_attempt_launch_path": str(attempt_paths["launch"]),
                    "app_server_attempt_sidecar_path": str(attempt_paths["sidecar"]),
                }
            )
            updated_attempt = self.conn.execute(
                """
                UPDATE episode_context_run_attempts
                SET status = 'launched', launch_path = ?, sidecar_path = ?,
                    turn_input_path = ?, source_input_sha256 = ?,
                    preclaim_capacity_request_path = ?,
                    preclaim_capacity_admission_path = ?,
                    preturn_capacity_request_path = ?,
                    preturn_capacity_admission_path = ?,
                    artifact_records_json = ?, updated_at = ?
                WHERE id = ? AND status = 'claimed'
                  AND job_id = ? AND canonical_run_id = ?
                """,
                (
                    str(attempt_paths["launch"]),
                    str(attempt_paths["sidecar"]),
                    str(input_path),
                    str(launch_value["source_input_sha256"]),
                    preclaim_request,
                    preclaim_admission,
                    preturn_request,
                    preturn_admission,
                    _canonical_json(source_records),
                    timestamp,
                    item.attempt_id,
                    item.job_id,
                    item.context_run_id,
                ),
            )
            if updated_attempt.rowcount != 1:
                raise EpisodeContextRunnerError(
                    "managed episode-context launch binding was not atomic"
                )
            self.conn.execute(
                "UPDATE jobs SET payload_json = ?, updated_at = ? WHERE id = ?",
                (_canonical_json(payload), timestamp, item.job_id),
            )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def submit(self, item: EpisodeContextItem, output_path: Path) -> Mapping[str, Any]:
        if self._loaded_contract is None:
            raise EpisodeContextRunnerError(
                "episode-context submission lacks frozen execution contract"
            )
        self._require_attempt_history_table()
        source = output_path.expanduser().resolve()
        turn_launch_path = source.parent / "launch.json"
        turn_sidecar_path = source.parent / "sidecar.json"
        launch_value = _load_json(
            turn_launch_path, label="managed episode-context turn launch"
        )
        input_path = _verify_record(
            launch_value.get("input"), label="managed episode-context turn input"
        )
        input_value, launch_value = _validate_turn_input_launch(
            loaded=self._loaded_contract,
            input_path=input_path,
            launch_path=turn_launch_path,
        )
        if any(
            launch_value.get(field) != getattr(item, field)
            for field in (
                "job_id",
                "episode_id",
                "context_run_id",
                "claim_id",
                "attempt_id",
                "queue_attempt_number",
            )
        ):
            raise EpisodeContextRunnerError(
                "managed episode-context completed turn item lineage drifted"
            )
        sidecar, validated_output = _validate_completed_turn(
            sidecar_path=turn_sidecar_path,
            output_path=source,
            thread_id=str(launch_value["thread_id"]),
            model=str(self._loaded_contract["configuration"]["model"]),
            effort=str(self._loaded_contract["configuration"]["reasoning_effort"]),
            prompt=input_path.read_text(encoding="utf-8"),
            base_instructions=self._loaded_contract["prompt_path"].read_text(
                encoding="utf-8"
            ),
            schema=self._loaded_contract["schema"],
            **_completed_turn_live_lineage_kwargs(
                loaded=self._loaded_contract, launch=launch_value
            ),
        )
        target = (
            Path(item.submission_output_path).expanduser().resolve()
            if item.submission_output_path
            else source
        )
        canonical = _validate_canonical_output(
            _load_json(source, label="managed episode-context output"),
            expected_episode_id=item.episode_id,
        )
        if canonical != validated_output:
            raise EpisodeContextRunnerError(
                "managed episode-context output validation disagreed"
            )
        if target != source:
            _write_immutable(target, source.read_bytes())
        try:
            job = self.conn.execute(
                "SELECT * FROM jobs WHERE id = ?", (item.job_id,)
            ).fetchone()
            if job is None:
                raise EpisodeContextRunnerError(
                    "managed episode-context job disappeared"
                )
            payload = loads_json(str(job["payload_json"] or "{}"), {})
            attempt = self.conn.execute(
                "SELECT * FROM episode_context_run_attempts WHERE id = ?",
                (item.attempt_id,),
            ).fetchone()
            canonical_run = self.conn.execute(
                "SELECT * FROM episode_context_runs WHERE id = ?",
                (item.context_run_id,),
            ).fetchone()
            common_lineage_valid = (
                attempt is None
                or canonical_run is None
                or int(attempt["job_id"]) != item.job_id
                or str(attempt["episode_id"]) != item.episode_id
                or str(attempt["canonical_run_id"]) != item.context_run_id
                or str(attempt["claim_id"]) != item.claim_id
                or str(attempt["contract_sha256"])
                != self._loaded_contract["sha256"]
                or str(attempt["frozen_configuration_sha256"])
                != self._loaded_contract["configuration_sha256"]
                or str(attempt["semantic_model"])
                != self._loaded_contract["configuration"]["model"]
                or payload.get("episode_context_run_id")
                != item.context_run_id
                or payload.get("episode_context_attempt_id")
                != item.attempt_id
                or payload.get("app_server_claim_id") != item.claim_id
                or Path(str(attempt["output_path"])).resolve() != target
                or not isinstance(attempt["context_artifact_path"], str)
                or not attempt["context_artifact_path"]
            )
            if common_lineage_valid:
                raise EpisodeContextRunnerError(
                    "managed episode-context submission lineage drifted"
                )
            source_records, _turn_input_record = _validate_attempt_source_binding(
                attempt=attempt,
                input_path=input_path,
                input_value=input_value,
                require_bound_turn_input=True,
            )
            _validate_attempt_launch_columns(
                attempt=attempt, input_path=input_path, launch=launch_value
            )
            if (
                str(job["status"]) == "completed"
                and str(attempt["status"])
                in {"db_completed_unterminalized", "completed"}
                and str(canonical_run["status"]) == "completed"
            ):
                return self._adopt_completed_attempt(
                    item=item,
                    source=source,
                    target=target,
                    canonical=canonical,
                    job=job,
                    attempt=attempt,
                    canonical_run=canonical_run,
                    sidecar=sidecar,
                    launch=launch_value,
                )
            if str(job["status"]) != "claimed" or str(attempt["status"]) != "launched":
                raise EpisodeContextRunnerError(
                    "managed episode-context attempt is not claimable or completed"
                )
            artifact_path = Path(
                str(attempt["context_artifact_path"])
            ).expanduser().resolve()
            attempt_launch_path = Path(str(attempt["launch_path"])).resolve()
            attempt_sidecar_path = Path(str(attempt["sidecar_path"])).resolve()
            if not turn_launch_path.is_file() or not turn_sidecar_path.is_file():
                raise EpisodeContextRunnerError(
                    "managed episode-context turn lineage is incomplete"
                )
            _write_immutable(attempt_sidecar_path, turn_sidecar_path.read_bytes())
            if attempt_launch_path.read_bytes() != turn_launch_path.read_bytes():
                raise EpisodeContextRunnerError(
                    "managed episode-context launch copy drifted"
                )
            _write_immutable(
                artifact_path,
                _pretty_json(canonical).encode("utf-8"),
            )
            lineage = _artifact_lineage_payload(
                loaded=self._loaded_contract,
                item=item,
                raw_output_path=source,
                persisted_artifact_path=artifact_path,
            )
            lineage_path = _artifact_lineage_path(artifact_path)
            _write_immutable(lineage_path, _pretty_json(lineage).encode())
            _verify_managed_context_artifact(
                artifact_path=artifact_path,
                episode_id=item.episode_id,
                label_pack=self.label_pack,
                model=self.model,
                loaded=self._loaded_contract,
            )
            timestamp = now_iso()
            artifact_records = {
                "prompt": source_records["prompt"],
                "turn_input": source_records["turn_input"],
                "raw_output": _record(source),
                "submission_output": _record(target),
                "persisted_artifact": _record(artifact_path),
                "launch": _record(Path(str(attempt["launch_path"]))),
                "sidecar": _record(Path(str(attempt["sidecar_path"]))),
                "turn_launch": _record(turn_launch_path),
                "turn_sidecar": _record(turn_sidecar_path),
                "lineage": _record(lineage_path),
                "runner_contract": _record(Path(self._loaded_contract["path"])),
                "frozen_configuration": self._loaded_contract["contract"][
                    "artifacts"
                ]["frozen_configuration"],
                "base_prompt": self._loaded_contract["contract"]["artifacts"][
                    "prompt"
                ],
                "output_schema": self._loaded_contract["contract"]["artifacts"][
                    "output_schema"
                ],
                "execution": {
                    "auth_type": "chatgpt",
                    "plan_type": "pro",
                    "model": sidecar["model"],
                    "reasoning_effort": sidecar["effort"],
                    "thread_id": sidecar["thread_id"],
                    "turn_id": sidecar["turn_id"],
                    "instruction_sources_sha256": launch_value[
                        "instruction_sources_sha256"
                    ],
                    "instruction_sources_count": launch_value[
                        "instruction_sources_count"
                    ],
                    "base_instructions_sha256": launch_value[
                        "base_instructions_sha256"
                    ],
                    "usage": _valid_usage(sidecar["usage"]),
                    "usage_sha256": _sha256_bytes(
                        _canonical_json(_valid_usage(sidecar["usage"])).encode()
                    ),
                    "wall_elapsed_seconds": float(sidecar["wall_elapsed_seconds"]),
                    "capacity_admission": launch_value["capacity_admission"],
                },
            }
            updated_run = self.conn.execute(
                """
                UPDATE episode_context_runs
                SET job_id = ?, transcript_id = ?, status = 'completed',
                    prompt_path = ?, output_path = ?, context_artifact_path = ?,
                    speaker_map_json = ?, section_map_json = ?,
                    entity_seed_json = ?, concept_seed_json = ?,
                    extraction_guidance = ?, error = NULL,
                    completed_at = ?, updated_at = ?
                WHERE id = ? AND episode_id = ?
                  AND label_pack = ? AND model = ?
                """,
                (
                    item.job_id,
                    attempt["transcript_id"],
                    str(attempt["prompt_path"]),
                    str(attempt["output_path"]),
                    str(artifact_path),
                    _canonical_json(canonical["speaker_map"]),
                    _canonical_json(canonical["section_map"]),
                    _canonical_json(canonical["entity_seed"]),
                    _canonical_json(canonical["concept_seed"]),
                    canonical["extraction_guidance"],
                    timestamp,
                    timestamp,
                    item.context_run_id,
                    item.episode_id,
                    self.label_pack,
                    self.model,
                ),
            )
            updated_attempt = self.conn.execute(
                """
                UPDATE episode_context_run_attempts
                SET status = 'db_completed_unterminalized',
                    artifact_records_json = ?, usage_json = ?,
                    error = NULL, completed_at = ?, updated_at = ?
                WHERE id = ? AND status = 'launched'
                  AND job_id = ? AND canonical_run_id = ?
                """,
                (
                    _canonical_json(artifact_records),
                    _canonical_json(_valid_usage(sidecar["usage"])),
                    timestamp,
                    timestamp,
                    item.attempt_id,
                    item.job_id,
                    item.context_run_id,
                ),
            )
            if updated_run.rowcount != 1 or updated_attempt.rowcount != 1:
                raise EpisodeContextRunnerError(
                    "managed episode-context completion was not atomic"
                )
            complete_job(self.conn, item.job_id)
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        return {
            "job_id": str(item.job_id),
            "episode_context_run_id": item.context_run_id,
            "episode_context_attempt_id": item.attempt_id,
            "context_artifact_path": str(artifact_path),
            "artifact_lineage": _record(lineage_path),
            "production_mutated": True,
        }

    def _adopt_completed_attempt(
        self,
        *,
        item: EpisodeContextItem,
        source: Path,
        target: Path,
        canonical: Mapping[str, Any],
        job: sqlite3.Row,
        attempt: sqlite3.Row,
        canonical_run: sqlite3.Row,
        sidecar: Mapping[str, Any],
        launch: Mapping[str, Any],
    ) -> dict[str, Any]:
        records = loads_json(str(attempt["artifact_records_json"] or "{}"), {})
        required_records = {
            "prompt",
            "turn_input",
            "raw_output",
            "submission_output",
            "persisted_artifact",
            "launch",
            "sidecar",
            "turn_launch",
            "turn_sidecar",
            "lineage",
            "runner_contract",
            "frozen_configuration",
            "base_prompt",
            "output_schema",
            "execution",
        }
        if not required_records.issubset(records) or set(records) - (
            required_records | {"terminal"}
        ):
            raise EpisodeContextRunnerError(
                "completed managed context attempt records drifted"
            )
        paths = {
            key: _verify_record(records[key], label=f"completed attempt {key}")
            for key in required_records - {"execution"}
        }
        if "terminal" in records:
            _verify_record(records["terminal"], label="completed attempt terminal")
        execution = records["execution"]
        usage = _valid_usage(sidecar["usage"])
        turn_input_path = paths["turn_input"]
        _validate_attempt_launch_columns(
            attempt=attempt, input_path=turn_input_path, launch=launch
        )
        if (
            not isinstance(execution, Mapping)
            or execution.get("auth_type") != "chatgpt"
            or execution.get("plan_type") != "pro"
            or execution.get("model")
            != self._loaded_contract["configuration"]["model"]
            or execution.get("reasoning_effort")
            != self._loaded_contract["configuration"]["reasoning_effort"]
            or execution.get("thread_id") != sidecar["thread_id"]
            or execution.get("turn_id") != sidecar["turn_id"]
            or execution.get("instruction_sources_sha256")
            != launch["instruction_sources_sha256"]
            or execution.get("instruction_sources_count")
            != launch["instruction_sources_count"]
            or execution.get("base_instructions_sha256")
            != launch["base_instructions_sha256"]
            or execution.get("usage") != usage
            or execution.get("usage_sha256")
            != _sha256_bytes(_canonical_json(usage).encode())
            or execution.get("wall_elapsed_seconds")
            != float(sidecar["wall_elapsed_seconds"])
            or execution.get("capacity_admission") != launch["capacity_admission"]
            or paths["raw_output"] != source
            or paths["submission_output"] != target
            or paths["turn_launch"] != source.parent / "launch.json"
            or paths["turn_sidecar"] != source.parent / "sidecar.json"
            or paths["turn_input"] != source.parent / "input.private.json"
            or paths["runner_contract"] != Path(self._loaded_contract["path"])
            or records["frozen_configuration"]
            != self._loaded_contract["contract"]["artifacts"]["frozen_configuration"]
            or records["base_prompt"]
            != self._loaded_contract["contract"]["artifacts"]["prompt"]
            or records["output_schema"]
            != self._loaded_contract["contract"]["artifacts"]["output_schema"]
            or loads_json(str(attempt["usage_json"] or "{}"), {}) != usage
            or (
                str(attempt["status"]) == "completed"
                and (
                    not isinstance(attempt["terminal_path"], str)
                    or not attempt["terminal_path"]
                    or "terminal" not in records
                    or Path(str(attempt["terminal_path"])).resolve()
                    != _verify_record(
                        records["terminal"], label="completed attempt terminal"
                    )
                )
            )
            or (
                str(attempt["status"]) == "db_completed_unterminalized"
                and (attempt["terminal_path"] is not None or "terminal" in records)
            )
        ):
            raise EpisodeContextRunnerError(
                "completed managed context execution binding drifted"
            )
        artifact_path = paths["persisted_artifact"]
        verified = _verify_managed_context_artifact(
            artifact_path=artifact_path,
            episode_id=item.episode_id,
            label_pack=self.label_pack,
            model=self.model,
            loaded=self._loaded_contract,
        )
        lineage = verified["lineage"]
        payload = loads_json(str(job["payload_json"] or "{}"), {})
        if (
            verified["artifact"] != canonical
            or paths["lineage"] != verified["lineage_path"]
            or lineage.get("context_run_id") != item.context_run_id
            or lineage.get("attempt_id") != item.attempt_id
            or lineage.get("claim_id") != item.claim_id
            or str(canonical_run["context_artifact_path"]) != str(artifact_path)
            or str(canonical_run["prompt_path"]) != str(attempt["prompt_path"])
            or str(canonical_run["output_path"]) != str(attempt["output_path"])
            or payload.get("episode_context_attempt_id") != item.attempt_id
            or payload.get("episode_context_run_id") != item.context_run_id
        ):
            raise EpisodeContextRunnerError(
                "completed managed context canonical projection drifted"
            )
        return {
            "job_id": str(item.job_id),
            "episode_context_run_id": item.context_run_id,
            "episode_context_attempt_id": item.attempt_id,
            "context_artifact_path": str(artifact_path),
            "artifact_lineage": _record(verified["lineage_path"]),
            "production_mutated": False,
            "recovered_without_replay": True,
        }

    def bind_terminal(self, item: EpisodeContextItem, terminal_path: Path) -> None:
        if self._loaded_contract is None:
            raise EpisodeContextRunnerError(
                "episode-context terminal lacks frozen execution contract"
            )
        path = terminal_path.expanduser().resolve()
        try:
            run_root = path.parents[2]
        except IndexError as exc:
            raise EpisodeContextRunnerError(
                "episode-context terminal path is not rooted under turns"
            ) from exc
        terminals = _terminal_evidence(run_root, self._loaded_contract)
        matches = [
            terminal
            for terminal in terminals
            if terminal.get("attempt_id") == item.attempt_id
        ]
        if (
            len(matches) != 1
            or matches[0].get("job_id") != item.job_id
            or matches[0].get("episode_id") != item.episode_id
            or matches[0].get("context_run_id") != item.context_run_id
            or matches[0].get("claim_id") != item.claim_id
            or matches[0].get("queue_attempt_number")
            != item.queue_attempt_number
            or Path(str(matches[0]["terminal"]["path"])).resolve() != path
        ):
            raise EpisodeContextRunnerError(
                "episode-context terminal attempt lineage drifted"
            )
        timestamp = now_iso()
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            attempt = self.conn.execute(
                "SELECT * FROM episode_context_run_attempts WHERE id = ?",
                (item.attempt_id,),
            ).fetchone()
            if attempt is None or str(attempt["status"]) not in {
                "db_completed_unterminalized",
                "completed",
            }:
                raise EpisodeContextRunnerError(
                    "episode-context terminal lacks a completed managed attempt"
                )
            records = loads_json(str(attempt["artifact_records_json"] or "{}"), {})
            terminal_record = _record(path)
            existing = records.get("terminal")
            if existing is not None and existing != terminal_record:
                raise EpisodeContextRunnerError(
                    "episode-context attempt terminal record drifted"
                )
            records["terminal"] = terminal_record
            terminal = _load_json(path, label="managed episode-context terminal")
            terminal_usage = _valid_usage(terminal.get("usage"))
            if (
                loads_json(str(attempt["usage_json"] or "{}"), {})
                != terminal_usage
            ):
                raise EpisodeContextRunnerError(
                    "episode-context terminal usage differs from attempt ledger"
                )
            updated = self.conn.execute(
                """
                UPDATE episode_context_run_attempts
                SET status = 'completed', terminal_path = ?,
                    artifact_records_json = ?, usage_json = ?, updated_at = ?
                WHERE id = ?
                  AND status IN ('db_completed_unterminalized','completed')
                """,
                (
                    str(path),
                    _canonical_json(records),
                    _canonical_json(terminal_usage),
                    timestamp,
                    item.attempt_id,
                ),
            )
            if updated.rowcount != 1:
                raise EpisodeContextRunnerError(
                    "episode-context terminal binding was not atomic"
                )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def accounting(self) -> Mapping[str, Any]:
        required = self._required_ids()
        context_rows = self.conn.execute(
            """
            SELECT id, target_id, status FROM jobs
            WHERE lane = ? AND job_type = 'episode_context'
              AND json_extract(payload_json, '$.label_pack') = ?
              AND (
                json_extract(payload_json, '$.model') IS NULL
                OR json_extract(payload_json, '$.model') = ?
              )
            """,
            (self.lane, self.label_pack, self.model),
        ).fetchall()
        counts = Counter()
        for row in context_rows:
            if str(row["target_id"]) not in required:
                continue
            status = str(row["status"])
            counts[status if status in JOB_STATUS_FIELDS[:-1] else "other"] += 1
        completed = self._completed_context_episode_ids(required)
        return build_backlog_snapshot(
            job_status_counts={field: int(counts[field]) for field in JOB_STATUS_FIELDS},
            required_episode_ids=sorted(required),
            completed_episode_ids=sorted(completed),
            required_episode_manifest=dict(self._required_manifest_record or {}),
        )

    def _completed_context_episode_ids(self, required: set[str]) -> set[str]:
        if not required:
            return set()
        rows = self.conn.execute(
            """
            SELECT episode_id, context_artifact_path
            FROM episode_context_runs
            WHERE label_pack = ? AND model = ? AND status = 'completed'
              AND context_artifact_path IS NOT NULL
            """,
            (self.label_pack, self.model),
        ).fetchall()
        completed: set[str] = set()
        for row in rows:
            episode_id = str(row["episode_id"])
            if episode_id not in required:
                continue
            try:
                _verify_managed_context_artifact(
                    artifact_path=Path(str(row["context_artifact_path"])),
                    episode_id=episode_id,
                    label_pack=self.label_pack,
                    model=self.model,
                    loaded=self._loaded_contract,
                )
            except (EpisodeContextRunnerError, OSError, ValueError):
                continue
            completed.add(episode_id)
        return completed

    def _completed_reconciliation(self, required: set[str]) -> dict[str, list[str]]:
        rows = self.conn.execute(
            """
            SELECT episode_id, context_artifact_path
            FROM episode_context_runs
            WHERE label_pack = ? AND model = ? AND status = 'completed'
              AND context_artifact_path IS NOT NULL
            """,
            (self.label_pack, self.model),
        ).fetchall()
        verified: list[str] = []
        rerun: list[str] = []
        for row in rows:
            episode_id = str(row["episode_id"])
            if episode_id not in required:
                continue
            artifact_path = Path(str(row["context_artifact_path"])).expanduser().resolve()
            try:
                artifact = _load_json(
                    artifact_path, label="completed episode-context artifact"
                )
                _validate_canonical_output(artifact, expected_episode_id=episode_id)
            except (EpisodeContextRunnerError, OSError, ValueError):
                rerun.append(episode_id)
                continue
            if not _artifact_lineage_path(artifact_path).is_file():
                rerun.append(episode_id)
                continue
            try:
                _verify_managed_context_artifact(
                    artifact_path=artifact_path,
                    episode_id=episode_id,
                    label_pack=self.label_pack,
                    model=self.model,
                    loaded=self._loaded_contract,
                )
            except (EpisodeContextRunnerError, OSError, ValueError):
                rerun.append(episode_id)
            else:
                verified.append(episode_id)
        return {
            "verified": sorted(set(verified)),
            "adoption_required": [],
            "rerun_required": sorted(set(rerun)),
        }

    def provision_missing_context_jobs(self, *, execute: bool = False) -> dict[str, Any]:
        """Plan or atomically provision one canonical context job per episode."""

        required = self._required_ids()
        reconciliation = self._completed_reconciliation(required)
        completed = set(reconciliation["verified"])
        rows = self.conn.execute(
            """
            SELECT id, target_id, status, payload_json
            FROM jobs
            WHERE lane = ? AND job_type = 'episode_context'
              AND json_extract(payload_json, '$.label_pack') = ?
              AND (
                json_extract(payload_json, '$.model') IS NULL
                OR json_extract(payload_json, '$.model') = ?
              )
              AND target_id IN (
                SELECT CAST(value AS TEXT) FROM json_each(?)
              )
            ORDER BY target_id, id
            """,
            (
                self.lane,
                self.label_pack,
                self.model,
                _canonical_json(sorted(required)),
            ),
        ).fetchall()
        rows_by_episode: dict[str, list[sqlite3.Row]] = {}
        for row in rows:
            rows_by_episode.setdefault(str(row["target_id"]), []).append(row)
        active = {
            episode_id
            for episode_id, episode_rows in rows_by_episode.items()
            if any(str(row["status"]) in {"pending", "claimed"} for row in episode_rows)
        }
        failed_to_reset: list[str] = []
        completed_to_rerun: list[str] = []
        new_to_enqueue: list[str] = []
        rerun_required = set(reconciliation["rerun_required"])
        for episode_id in sorted(required - completed - active):
            episode_rows = rows_by_episode.get(episode_id, [])
            failed_rows = [row for row in episode_rows if str(row["status"]) == "failed"]
            if episode_id in rerun_required:
                completed_rows = [
                    row
                    for row in episode_rows
                    if str(row["status"]) == "completed"
                ]
                if len(episode_rows) != 1 or len(completed_rows) != 1:
                    raise EpisodeContextRunnerError(
                        "legacy completed context rerun job lineage is not unique"
                    )
                completed_to_rerun.append(episode_id)
                continue
            if failed_rows:
                if len(episode_rows) != 1 or len(failed_rows) != 1:
                    raise EpisodeContextRunnerError(
                        "failed episode-context provisioning lineage is not unique"
                    )
                failed_to_reset.append(episode_id)
            elif not episode_rows:
                new_to_enqueue.append(episode_id)
        missing = sorted(required - completed - active)
        plan = {
            "schema_version": PROVISION_PLAN_VERSION,
            "lane": self.lane,
            "label_pack": self.label_pack,
            "model": self.model,
            "required_episode_count": len(required),
            "completed_context_episode_count": len(completed),
            "existing_context_job_episode_count": len(set(rows_by_episode) & required),
            "missing_context_job_episode_count": len(missing),
            "missing_episode_ids": missing,
            "missing_episode_ids_sha256": _sha256_bytes(
                _canonical_json(missing).encode()
            ),
            "failed_context_job_episode_count": len(failed_to_reset),
            "failed_episode_ids_to_reset": failed_to_reset,
            "legacy_completed_context_rerun_count": len(completed_to_rerun),
            "legacy_completed_episode_ids_to_rerun": completed_to_rerun,
            "new_context_job_episode_count": len(new_to_enqueue),
            "new_episode_ids_to_enqueue": new_to_enqueue,
            "completed_reconciliation_episode_ids": sorted(
                rerun_required
            ),
            "transaction_policy": (
                "one_begin_immediate_snapshot_legacy_reset_failed_or_enqueue_missing_commit_once"
            ),
            "verified_completed_episode_ids": reconciliation["verified"],
            "adoption_required_episode_ids": reconciliation["adoption_required"],
            "rerun_required_episode_ids": reconciliation["rerun_required"],
            "reconciliation_policy": (
                "adopt_only_with_hash_bound_raw_output_sidecar_launch_and_frozen_"
                "configuration_evidence_otherwise_rerun"
            ),
            "required_episode_manifest": dict(self._required_manifest_record or {}),
            "mutation_performed": False,
            "reset_failed_job_count": 0,
            "reset_completed_legacy_job_count": 0,
            "legacy_snapshot_attempt_count": 0,
            "enqueued_job_count": 0,
            "live_execution_authorized": False,
        }
        if not execute:
            return plan

        reset_count = 0
        rerun_reset_count = 0
        legacy_snapshot_count = 0
        enqueued_count = 0
        timestamp = now_iso()
        stale_handoff_fields = {
            "episode_context_run_id",
            "episode_context_schema_version",
            "prompt_path",
            "output_path",
            "app_server_claim_id",
            "app_server_attempt_id",
            "app_server_launch_path",
            "claim_id",
            "attempt_id",
            "launch_path",
        }
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            if completed_to_rerun:
                self._require_attempt_history_table()
            for episode_id in completed_to_rerun:
                episode_rows = rows_by_episode[episode_id]
                job_row = episode_rows[0]
                current_job = self.conn.execute(
                    "SELECT * FROM jobs WHERE id = ?",
                    (int(job_row["id"]),),
                ).fetchone()
                context_rows = self.conn.execute(
                    """
                    SELECT * FROM episode_context_runs
                    WHERE episode_id = ? AND label_pack = ? AND model = ?
                      AND status = 'completed'
                    """,
                    (episode_id, self.label_pack, self.model),
                ).fetchall()
                if (
                    current_job is None
                    or str(current_job["status"]) != "completed"
                    or str(current_job["target_id"]) != episode_id
                    or len(context_rows) != 1
                ):
                    raise EpisodeContextRunnerError(
                        "legacy completed context changed before atomic rerun reset"
                    )
                context_run = context_rows[0]
                snapshot = dict(context_run)
                artifact_records: dict[str, Any] = {}
                for field in (
                    "prompt_path",
                    "output_path",
                    "context_artifact_path",
                ):
                    raw_path = context_run[field]
                    if not isinstance(raw_path, str) or not raw_path:
                        raise EpisodeContextRunnerError(
                            "legacy completed context artifact lineage is incomplete"
                        )
                    artifact_records[field] = _record(Path(raw_path))
                snapshot_json = _canonical_json(snapshot)
                artifact_records_json = _canonical_json(artifact_records)
                snapshot_sha256 = _sha256_bytes(snapshot_json.encode("utf-8"))
                snapshot_attempt_id = stable_id(
                    str(context_run["id"]),
                    snapshot_sha256,
                    _sha256_bytes(artifact_records_json.encode("utf-8")),
                    prefix="ectxlegacy_",
                )
                existing_snapshot = self.conn.execute(
                    """
                    SELECT * FROM episode_context_run_attempts WHERE id = ?
                    """,
                    (snapshot_attempt_id,),
                ).fetchone()
                if existing_snapshot is None:
                    self.conn.execute(
                        """
                        INSERT INTO episode_context_run_attempts
                          (id, canonical_run_id, job_id, episode_id, transcript_id,
                           label_pack, queue_payload_model, semantic_model,
                           reasoning_effort, attempt_kind, status, attempt_number,
                           claim_id, contract_sha256,
                           frozen_configuration_sha256, prompt_path, output_path,
                           context_artifact_path, launch_path, sidecar_path,
                           canonical_snapshot_json, artifact_records_json, error,
                           created_at, updated_at, completed_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL,
                                'legacy_canonical_snapshot', 'superseded', 0,
                                NULL, ?, ?, ?, ?, ?, NULL, NULL, ?, ?,
                                ?, ?, ?, ?)
                        """,
                        (
                            snapshot_attempt_id,
                            str(context_run["id"]),
                            int(job_row["id"]),
                            episode_id,
                            context_run["transcript_id"],
                            self.label_pack,
                            self.model,
                            str(context_run["model"]),
                            self._loaded_contract["sha256"]
                            if self._loaded_contract is not None
                            else None,
                            self._loaded_contract["configuration_sha256"]
                            if self._loaded_contract is not None
                            else None,
                            str(context_run["prompt_path"]),
                            str(context_run["output_path"]),
                            str(context_run["context_artifact_path"]),
                            snapshot_json,
                            artifact_records_json,
                            "Legacy context lacks managed app-server lineage; "
                            "preserved for fresh managed retry.",
                            timestamp,
                            timestamp,
                            context_run["completed_at"],
                        ),
                    )
                    legacy_snapshot_count += 1
                else:
                    if (
                        str(existing_snapshot["canonical_run_id"])
                        != str(context_run["id"])
                        or str(existing_snapshot["attempt_kind"])
                        != "legacy_canonical_snapshot"
                        or str(existing_snapshot["status"]) != "superseded"
                        or str(existing_snapshot["canonical_snapshot_json"])
                        != snapshot_json
                        or str(existing_snapshot["artifact_records_json"])
                        != artifact_records_json
                    ):
                        raise EpisodeContextRunnerError(
                            "legacy context snapshot attempt drifted"
                        )
                payload = loads_json(str(current_job["payload_json"] or "{}"), {})
                scrubbed = {
                    key: value
                    for key, value in payload.items()
                    if key not in stale_handoff_fields
                }
                scrubbed.update(
                    {
                        "label_pack": self.label_pack,
                        "model": self.model,
                        "episode_context_version": (
                            EPISODE_CONTEXT_SCHEMA_VERSION
                        ),
                        "legacy_episode_context_run_id": str(
                            context_run["id"]
                        ),
                        "legacy_snapshot_attempt_id": snapshot_attempt_id,
                    }
                )
                cursor = self.conn.execute(
                    """
                    UPDATE jobs
                    SET status = 'pending', payload_json = ?, attempts = 0,
                        lease_owner = NULL, leased_until = NULL,
                        completed_at = NULL, error = NULL, updated_at = ?
                    WHERE id = ? AND status = 'completed'
                      AND payload_json = ?
                    """,
                    (
                        _canonical_json(scrubbed),
                        timestamp,
                        int(job_row["id"]),
                        str(job_row["payload_json"]),
                    ),
                )
                if cursor.rowcount != 1:
                    raise EpisodeContextRunnerError(
                        "legacy completed context job was not atomically reset"
                    )
                rerun_reset_count += 1
            for episode_id in failed_to_reset:
                episode_rows = rows_by_episode[episode_id]
                row = episode_rows[0]
                payload = loads_json(str(row["payload_json"] or "{}"), {})
                if (
                    payload.get("label_pack") != self.label_pack
                    or payload.get("model") not in {None, self.model}
                ):
                    raise EpisodeContextRunnerError(
                        "failed episode-context reset escaped frozen queue scope"
                    )
                scrubbed = {
                    key: value
                    for key, value in payload.items()
                    if key not in stale_handoff_fields
                }
                cursor = self.conn.execute(
                    """
                    UPDATE jobs
                    SET status = 'pending',
                        payload_json = ?,
                        attempts = 0,
                        lease_owner = NULL,
                        leased_until = NULL,
                        completed_at = NULL,
                        error = NULL,
                        updated_at = ?
                    WHERE id = ? AND status = 'failed'
                    """,
                    (_canonical_json(scrubbed), timestamp, int(row["id"])),
                )
                if cursor.rowcount != 1:
                    raise EpisodeContextRunnerError(
                        "failed episode-context job changed during atomic reset"
                    )
                reset_count += 1
            for episode_id in new_to_enqueue:
                job_id = db.enqueue_job(
                    self.conn,
                    lane=self.lane,
                    job_type="episode_context",
                    target_id=episode_id,
                    payload={
                        "label_pack": self.label_pack,
                        "model": self.model,
                        "episode_context_version": EPISODE_CONTEXT_SCHEMA_VERSION,
                        "priority_reason": "required_before_v3_1_segment_extraction",
                    },
                    priority=99,
                    max_attempts=2,
                )
                if job_id is None:
                    raise EpisodeContextRunnerError(
                        "missing episode-context job was not atomically enqueued"
                    )
                enqueued_count += 1
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        return {
            **plan,
            "mutation_performed": bool(
                reset_count or rerun_reset_count or enqueued_count
            ),
            "reset_failed_job_count": reset_count,
            "reset_completed_legacy_job_count": rerun_reset_count,
            "legacy_snapshot_attempt_count": legacy_snapshot_count,
            "enqueued_job_count": enqueued_count,
        }


def verify_live_context_provision_execution_receipt(
    path: Path,
    *,
    database_path: Path | None = None,
) -> dict[str, Any]:
    receipt_path = path.expanduser().resolve()
    value = _load_json(
        receipt_path, label="live context provision execution receipt"
    )
    expected_keys = {
        "schema_version",
        "state",
        "phase",
        "executed_at",
        "database",
        "authorization",
        "contract",
        "migration_plan",
        "pre_cutover_receipt",
        "post_cutover_receipt",
        "provision_plan",
        "required_episode_manifest",
        "schema_migration_performed",
        "provision_recovered_after_commit",
        "operation_counts",
        "baseline_table_counts",
        "final_table_counts",
        "table_count_deltas",
        "legacy_snapshots",
        "legacy_snapshots_sha256",
        "semantic_model_call_count",
        "production_mutated",
        "single_writer_verified",
        "receipt_sha256",
    }
    unhashed = dict(value)
    supplied = unhashed.pop("receipt_sha256", None)
    if (
        set(value) != expected_keys
        or value.get("schema_version") != LIVE_PROVISION_EXECUTION_VERSION
        or value.get("state") != "passed"
        or value.get("phase") != "attempt_schema_cutover_and_context_provision"
        or value.get("semantic_model_call_count") != 0
        or value.get("production_mutated") is not True
        or value.get("single_writer_verified") is not True
        or not _is_sha256(supplied)
        or supplied != _sha256_bytes(_canonical_json(unhashed).encode())
    ):
        raise EpisodeContextRunnerError(
            "live context provision execution receipt drifted"
        )
    database_identity = _validate_database_identity(
        value.get("database"), label="live context provision execution"
    )
    if (
        database_path is not None
        and _database_identity(database_path) != database_identity
    ):
        raise EpisodeContextRunnerError(
            "live context provision execution database drifted"
        )
    for field in (
        "authorization",
        "contract",
        "migration_plan",
        "pre_cutover_receipt",
        "post_cutover_receipt",
        "provision_plan",
        "required_episode_manifest",
    ):
        _verify_record(value[field], label=f"provision execution {field}")
    post_path = _verify_record(
        value["post_cutover_receipt"], label="post-cutover receipt"
    )
    post_cutover = verify_attempt_schema_cutover_receipt(
        _load_json(post_path, label="post-cutover receipt"), require_ready=True
    )
    plan_path = _verify_record(value["provision_plan"], label="provision plan")
    plan = verify_live_context_provision_plan(
        _load_json(plan_path, label="provision plan")
    )
    expected_operations = {
        "legacy_snapshot_attempt_count": int(
            plan["observed_population"]["legacy_completed_context_rerun_count"]
        ),
        "reset_completed_legacy_job_count": int(
            plan["observed_population"]["legacy_completed_context_rerun_count"]
        ),
        "reset_failed_job_count": int(
            plan["observed_population"]["failed_context_job_reset_count"]
        ),
        "enqueued_job_count": int(
            plan["observed_population"]["new_context_job_count"]
        ),
    }
    baseline = value.get("baseline_table_counts")
    final = value.get("final_table_counts")
    deltas = value.get("table_count_deltas")
    expected_deltas = {
        "jobs": expected_operations["enqueued_job_count"],
        "episode_context_runs": 0,
        "episode_context_run_attempts": expected_operations[
            "legacy_snapshot_attempt_count"
        ],
        "labels": 0,
    }
    snapshots = value.get("legacy_snapshots")
    if (
        post_cutover["database"] != database_identity
        or plan["database"] != database_identity
        or value.get("operation_counts") != expected_operations
        or not isinstance(baseline, Mapping)
        or not isinstance(final, Mapping)
        or not isinstance(deltas, Mapping)
        or dict(deltas) != expected_deltas
        or any(int(final[key]) - int(baseline[key]) != expected_deltas[key] for key in expected_deltas)
        or not isinstance(snapshots, Mapping)
        or sorted(snapshots) != plan["legacy_completed_episode_ids_to_rerun"]
        or value.get("legacy_snapshots_sha256")
        != _sha256_bytes(_canonical_json(snapshots).encode())
    ):
        raise EpisodeContextRunnerError(
            "live context provision execution accounting drifted"
        )
    for episode_id, snapshot in snapshots.items():
        if not isinstance(snapshot, Mapping):
            raise EpisodeContextRunnerError("legacy snapshot receipt drifted")
        records = snapshot.get("artifact_records")
        if not isinstance(records, Mapping):
            raise EpisodeContextRunnerError("legacy snapshot records drifted")
        for name, record in records.items():
            _verify_record(record, label=f"legacy snapshot {episode_id} {name}")
    return dict(value)


def execute_live_context_cutover_and_provision(
    *,
    contract_path: Path,
    database_path: Path,
    migration_plan_path: Path,
    cutover_receipt_path: Path,
    provision_plan_path: Path,
    authorization_path: Path,
    output_root: Path,
    execute: bool = False,
) -> dict[str, Any]:
    """Apply the exact empty-table migration and 5,032-job plan once."""

    loaded = load_episode_context_contract(contract_path)
    authorization = load_live_context_provision_authorization(
        authorization_path,
        contract_path=contract_path,
        migration_plan_path=migration_plan_path,
        cutover_receipt_path=cutover_receipt_path,
        provision_plan_path=provision_plan_path,
    )
    database_identity = _database_identity(database_path)
    if database_identity != authorization["database"]:
        raise EpisodeContextRunnerError(
            "cutover/provision database differs from frozen authorization"
        )
    root = output_root.expanduser().resolve()
    receipt_path = root / "live-provision-execution-receipt.json"
    if receipt_path.is_file():
        return verify_live_context_provision_execution_receipt(
            receipt_path, database_path=database_path
        )
    if not execute:
        raise EpisodeContextRunnerWaiting(
            "separate cutover/provision execute authorization is required"
        )
    with LiveContextWriterLock(database_identity, phase="migrate_provision"):
        if receipt_path.is_file():
            return verify_live_context_provision_execution_receipt(
                receipt_path, database_path=database_path
            )
        connection = sqlite3.connect(str(database_path.expanduser().resolve()), timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        try:
            if _database_identity(database_path) != database_identity:
                raise EpisodeContextRunnerError(
                    "cutover/provision database identity changed after lock"
                )
            baseline = {
                "jobs": int(authorization["cutover"]["baseline_table_counts"]["jobs"]),
                "episode_context_runs": int(
                    authorization["cutover"]["baseline_table_counts"]
                    ["episode_context_runs"]
                ),
                "episode_context_run_attempts": 0,
                "labels": int(
                    authorization["cutover"]["baseline_table_counts"]["labels"]
                ),
            }
            migration = _load_json(
                authorization["migration_plan_path"],
                label="attempt-schema migration plan",
            )
            migrated = _apply_attempt_schema_migration(
                connection, migration_plan=migration
            )
            post_cutover_path = root / "attempt-schema-cutover-after.json"
            if post_cutover_path.is_file():
                post_cutover = verify_attempt_schema_cutover_receipt(
                    _load_json(post_cutover_path, label="post-cutover receipt"),
                    require_ready=True,
                )
            else:
                post_cutover = build_attempt_schema_cutover_receipt(database_path)
                _write_immutable(
                    post_cutover_path, _pretty_json(post_cutover).encode()
                )
                verify_attempt_schema_cutover_receipt(
                    post_cutover, require_ready=True
                )
            if post_cutover["database"] != database_identity:
                raise EpisodeContextRunnerError(
                    "post-cutover database identity drifted"
                )
            queue = SQLiteEpisodeContextQueue(
                connection,
                lane=LIVE_LANE,
                worker_id="managed-context-provisioner",
                label_pack=LIVE_LABEL_PACK,
                model=LIVE_QUEUE_PAYLOAD_MODEL,
                attempt_root=root / "managed-context-attempts",
            )
            queue.bind_execution_contract(loaded)
            plan = authorization["provision_plan"]
            manifest, manifest_record = _freeze_required_manifest(
                root=root,
                loaded=loaded,
                episode_ids=plan["required_episode_ids"],
            )
            queue.bind_required_episode_manifest(manifest, manifest_record)
            current_plan = queue.provision_missing_context_jobs(execute=False)
            current_counts = {
                "legacy_snapshot_attempt_count": int(
                    current_plan["legacy_completed_context_rerun_count"]
                ),
                "reset_completed_legacy_job_count": int(
                    current_plan["legacy_completed_context_rerun_count"]
                ),
                "reset_failed_job_count": int(
                    current_plan["failed_context_job_episode_count"]
                ),
                "enqueued_job_count": int(
                    current_plan["new_context_job_episode_count"]
                ),
            }
            expected_counts = {
                "legacy_snapshot_attempt_count": int(
                    plan["observed_population"]
                    ["legacy_completed_context_rerun_count"]
                ),
                "reset_completed_legacy_job_count": int(
                    plan["observed_population"]
                    ["legacy_completed_context_rerun_count"]
                ),
                "reset_failed_job_count": int(
                    plan["observed_population"]["failed_context_job_reset_count"]
                ),
                "enqueued_job_count": int(
                    plan["observed_population"]["new_context_job_count"]
                ),
            }
            recovered_after_commit = all(value == 0 for value in current_counts.values())
            if not recovered_after_commit:
                if current_counts != expected_counts:
                    raise EpisodeContextRunnerError(
                        "live provision operations differ from frozen plan"
                    )
                result = queue.provision_missing_context_jobs(execute=True)
                observed_result = {
                    key: int(result[key]) for key in expected_counts
                }
                if observed_result != expected_counts:
                    raise EpisodeContextRunnerError(
                        "live provision execution counts drifted"
                    )
            post = _verify_live_provision_post_state(
                connection, plan=plan, baseline_counts=baseline
            )
            payload = {
                "schema_version": LIVE_PROVISION_EXECUTION_VERSION,
                "state": "passed",
                "phase": "attempt_schema_cutover_and_context_provision",
                "executed_at": now_iso(),
                "database": database_identity,
                "authorization": _record(authorization["path"]),
                "contract": _record(contract_path),
                "migration_plan": _record(migration_plan_path),
                "pre_cutover_receipt": _record(cutover_receipt_path),
                "post_cutover_receipt": _record(post_cutover_path),
                "provision_plan": _record(provision_plan_path),
                "required_episode_manifest": manifest_record,
                "schema_migration_performed": migrated,
                "provision_recovered_after_commit": recovered_after_commit,
                "operation_counts": expected_counts,
                "baseline_table_counts": baseline,
                "final_table_counts": post["table_counts"],
                "table_count_deltas": post["table_count_deltas"],
                "legacy_snapshots": post["legacy_snapshots"],
                "legacy_snapshots_sha256": post["legacy_snapshots_sha256"],
                "semantic_model_call_count": 0,
                "production_mutated": True,
                "single_writer_verified": True,
            }
            payload["receipt_sha256"] = _sha256_bytes(
                _canonical_json(payload).encode()
            )
            _write_immutable(receipt_path, _pretty_json(payload).encode())
            return verify_live_context_provision_execution_receipt(
                receipt_path, database_path=database_path
            )
        finally:
            connection.close()


def _turn_key(item: EpisodeContextItem) -> str:
    return stable_id(item.episode_id, item.attempt_id, prefix="ectxt_")


def _turn_paths(root: Path, item: EpisodeContextItem) -> dict[str, Path]:
    turn_root = root / "turns" / _turn_key(item)
    return {
        "root": turn_root,
        "input": turn_root / "input.private.json",
        "launch": turn_root / "launch.json",
        "sidecar": turn_root / "sidecar.json",
        "output": turn_root / "output.private.json",
        "terminal": turn_root / "terminal.json",
    }


def _item_input(item: EpisodeContextItem) -> dict[str, Any]:
    return {
        "schema_version": TURN_INPUT_VERSION,
        "job_id": item.job_id,
        "episode_id": item.episode_id,
        "context_run_id": item.context_run_id,
        "claim_id": item.claim_id,
        "attempt_id": item.attempt_id,
        "queue_attempt_number": item.queue_attempt_number,
        "submission_output_path": item.submission_output_path,
        "source_input": dict(item.source_input),
        "semantic_authority_fields": list(SEMANTIC_AUTHORITY_FIELDS),
        "deterministic_semantic_pruning": False,
    }


def _validate_attempt_source_binding(
    *,
    attempt: Mapping[str, Any],
    input_path: Path,
    input_value: Mapping[str, Any],
    require_bound_turn_input: bool,
) -> tuple[dict[str, Any], dict[str, Any]]:
    records = loads_json(str(attempt["artifact_records_json"] or "{}"), {})
    if not isinstance(records, dict) or "prompt" not in records:
        raise EpisodeContextRunnerError(
            "managed episode-context attempt lacks its frozen source snapshot"
        )
    prompt_path = _verify_record(
        records["prompt"], label="managed episode-context attempt source snapshot"
    )
    if prompt_path != Path(str(attempt["prompt_path"])).expanduser().resolve():
        raise EpisodeContextRunnerError(
            "managed episode-context attempt source snapshot path drifted"
        )
    prompt_value = _load_json(
        prompt_path, label="managed episode-context attempt source snapshot"
    )
    source_input = input_value.get("source_input")
    if (
        not isinstance(prompt_value, Mapping)
        or not isinstance(source_input, Mapping)
        or _canonical_json(dict(prompt_value)).encode("utf-8")
        != _canonical_json(dict(source_input)).encode("utf-8")
    ):
        raise EpisodeContextRunnerError(
            "managed episode-context turn input differs from the claimed source snapshot"
        )
    input_record = _record(input_path)
    bound_input = records.get("turn_input")
    if require_bound_turn_input and bound_input is None:
        raise EpisodeContextRunnerError(
            "managed episode-context attempt lacks its bound turn input"
        )
    if bound_input is not None:
        bound_path = _verify_record(
            bound_input, label="managed episode-context bound turn input"
        )
        if bound_path != input_path.resolve() or bound_input != input_record:
            raise EpisodeContextRunnerError(
                "managed episode-context bound turn input drifted"
            )
    return records, input_record


def _validate_attempt_launch_columns(
    *, attempt: Mapping[str, Any], input_path: Path, launch: Mapping[str, Any]
) -> None:
    capacity = launch.get("capacity_admission")
    if not isinstance(capacity, Mapping):
        raise EpisodeContextRunnerError("attempt capacity binding is absent")
    if set(capacity) == {"preclaim", "preturn"}:
        expected = {
            "preclaim_capacity_request_path": str(
                _verify_record(
                    capacity["preclaim"]["request"],
                    label="attempt preclaim request",
                )
            ),
            "preclaim_capacity_admission_path": str(
                _verify_record(
                    capacity["preclaim"]["admission"],
                    label="attempt preclaim admission",
                )
            ),
            "preturn_capacity_request_path": str(
                _verify_record(
                    capacity["preturn"]["request"],
                    label="attempt preturn request",
                )
            ),
            "preturn_capacity_admission_path": str(
                _verify_record(
                    capacity["preturn"]["admission"],
                    label="attempt preturn admission",
                )
            ),
        }
    elif set(capacity) == {"request", "admission"}:
        expected = {
            "preclaim_capacity_request_path": str(
                _verify_record(capacity["request"], label="attempt capacity request")
            ),
            "preclaim_capacity_admission_path": str(
                _verify_record(
                    capacity["admission"], label="attempt capacity admission"
                )
            ),
            "preturn_capacity_request_path": None,
            "preturn_capacity_admission_path": None,
        }
    else:
        raise EpisodeContextRunnerError("attempt capacity binding shape drifted")
    if (
        str(attempt["turn_input_path"] or "") != str(input_path.resolve())
        or str(attempt["source_input_sha256"] or "")
        != str(launch.get("source_input_sha256") or "")
        or any(attempt[field] != value for field, value in expected.items())
    ):
        raise EpisodeContextRunnerError("attempt explicit launch lineage drifted")


def _validate_item(item: EpisodeContextItem) -> None:
    if (
        isinstance(item.job_id, bool)
        or not isinstance(item.job_id, int)
        or item.job_id < 1
        or not item.episode_id
        or not item.context_run_id
        or not item.claim_id
        or not item.attempt_id
        or isinstance(item.queue_attempt_number, bool)
        or not isinstance(item.queue_attempt_number, int)
        or item.queue_attempt_number < 1
        or not isinstance(item.source_input, Mapping)
    ):
        raise EpisodeContextRunnerWaiting("episode-context claim lifecycle is incomplete")


def _validate_preview_item(item: EpisodeContextItem) -> None:
    if (
        isinstance(item.job_id, bool)
        or not isinstance(item.job_id, int)
        or item.job_id < 1
        or not item.episode_id
    ):
        raise EpisodeContextRunnerWaiting(
            "episode-context pre-claim identity is incomplete"
        )


def _validate_completed_turn(
    *,
    sidecar_path: Path,
    output_path: Path,
    thread_id: str,
    model: str,
    effort: str,
    prompt: str,
    base_instructions: str,
    schema: Mapping[str, Any],
    instruction_sources_sha256: str | None = None,
    instruction_sources_count: int | None = None,
    protocol_schema_sha256: str | None = None,
    require_official_live_lineage: bool = False,
) -> tuple[dict[str, Any], dict[str, Any]]:
    sidecar = _load_json(sidecar_path, label="episode-context turn sidecar")
    output = _load_json(output_path, label="episode-context turn output")
    usage = _valid_usage(sidecar.get("usage"))
    thread_total = _valid_usage(sidecar.get("thread_total_usage"))
    wall = sidecar.get("wall_elapsed_seconds")
    output_raw = output_path.read_bytes()
    accepted_output_hashes = {_sha256_bytes(output_raw)}
    if output_raw.endswith(b"\n"):
        accepted_output_hashes.add(_sha256_bytes(output_raw[:-1]))
    if (
        sidecar.get("schema_version") != TURN_SIDECAR_SCHEMA_VERSION
        or sidecar.get("client_version") != APP_SERVER_CLIENT_VERSION
        or sidecar.get("cli_version") != PINNED_CLI_VERSION
        or not _is_sha256(sidecar.get("protocol_schema_sha256"))
        or sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("transport") != "stdio"
        or sidecar.get("synthetic_debug_errors") is not False
        or sidecar.get("recovery_reran_model") is not False
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("plan_type") != "pro"
        or sidecar.get("model") != model
        or sidecar.get("effort") != effort
        or sidecar.get("thread_mode") != THREAD_MODE
        or sidecar.get("batch_size") != 1
        or sidecar.get("thread_id") != thread_id
        or not isinstance(sidecar.get("turn_id"), str)
        or not sidecar["turn_id"]
        or sidecar.get("prompt_sha256") != _sha256_bytes(prompt.encode())
        or sidecar.get("prompt_bytes") != len(prompt.encode())
        or sidecar.get("base_instructions_sha256")
        != _sha256_bytes(base_instructions.encode())
        or sidecar.get("base_instructions_bytes") != len(base_instructions.encode())
        or sidecar.get("output_schema_sha256")
        != _sha256_bytes(_canonical_json(schema).encode())
        or sidecar.get("output_schema_bytes") != len(_canonical_json(schema).encode())
        or sidecar.get("output_path") != str(output_path.resolve())
        or sidecar.get("output_sha256") not in accepted_output_hashes
        or sidecar.get("usage_complete") is not True
        or sidecar.get("usage_status") != "measured"
        or any(thread_total[field] != usage[field] for field in USAGE_FIELDS)
        or isinstance(wall, bool)
        or not isinstance(wall, (int, float))
        or not math.isfinite(float(wall))
        or float(wall) < 0
        or (
            instruction_sources_sha256 is not None
            and sidecar.get("instruction_sources_sha256")
            != instruction_sources_sha256
        )
        or (
            instruction_sources_count is not None
            and sidecar.get("instruction_sources_count")
            != instruction_sources_count
        )
        or (
            protocol_schema_sha256 is not None
            and sidecar.get("protocol_schema_sha256")
            != protocol_schema_sha256
        )
        or (
            require_official_live_lineage
            and (
                not isinstance(sidecar.get("app_server_user_agent"), str)
                or not sidecar["app_server_user_agent"]
                or sidecar.get("cli_version") != PINNED_CODEX_CLI_VERSION
                or sidecar.get("auth_type") != "chatgpt"
                or sidecar.get("plan_type") != "pro"
            )
        )
    ):
        raise EpisodeContextRunnerWaiting("completed episode-context turn lineage is invalid")
    _validate_schema_value(schema, output, path="$")
    if output.get("episode_id") is None:
        raise EpisodeContextRunnerWaiting("episode-context output omitted episode_id")
    canonical = _validate_canonical_output(
        output, expected_episode_id=str(output["episode_id"])
    )
    return sidecar, canonical


def _launch_for_item(
    *,
    loaded: Mapping[str, Any],
    run_id: str,
    lifecycle_id: str,
    item: EpisodeContextItem,
    thread: AppServerThread,
    input_record: Mapping[str, Any],
    required_episode_manifest: Mapping[str, Any],
    capacity_binding: Mapping[str, Any],
    live_runtime_authorization: Mapping[str, Any] | None = None,
    live_execution_mode: str | None = None,
) -> dict[str, Any]:
    contract = loaded["contract"]
    payload = {
        "schema_version": TURN_LAUNCH_VERSION,
        "run_id": run_id,
        "lifecycle_id": lifecycle_id,
        "contract_sha256": loaded["sha256"],
        "evaluation_id": contract["lineage"]["evaluation_id"],
        "evaluation_receipt": contract["lineage"]["evaluation_receipt"],
        "holdout_receipt": contract["lineage"]["holdout_receipt"],
        "frozen_configuration_sha256": loaded["configuration_sha256"],
        "job_id": item.job_id,
        "episode_id": item.episode_id,
        "context_run_id": item.context_run_id,
        "claim_id": item.claim_id,
        "attempt_id": item.attempt_id,
        "queue_attempt_number": item.queue_attempt_number,
        "thread_id": thread.thread_id,
        "instruction_sources_sha256": thread.instruction_sources_sha256,
        "instruction_sources_count": thread.instruction_sources_count,
        "base_instructions_sha256": thread.base_instructions_sha256,
        "base_instructions_bytes": thread.base_instructions_bytes,
        "model": loaded["configuration"]["model"],
        "reasoning_effort": loaded["configuration"]["reasoning_effort"],
        "configured_batch_size": loaded["configuration"]["batch_size"],
        "turn_batch_size": 1,
        "thread_mode": THREAD_MODE,
        "semantic_retry_count": 0,
        "ambiguous_retry_allowed": False,
        "deterministic_semantic_pruning": False,
        "semantic_authority_fields": list(SEMANTIC_AUTHORITY_FIELDS),
        "source_input_sha256": _sha256_bytes(
            _canonical_json(dict(item.source_input)).encode()
        ),
        "required_episode_manifest": dict(required_episode_manifest),
        "capacity_admission": dict(capacity_binding),
        "input": dict(input_record),
        "prompt": contract["artifacts"]["prompt"],
        "output_schema": contract["artifacts"]["output_schema"],
    }
    if live_runtime_authorization is not None:
        if live_execution_mode not in {"official_live", "offline_test_only"}:
            raise EpisodeContextRunnerError(
                "live episode-context launch authority is invalid"
            )
        payload["live_runtime_authorization"] = dict(
            live_runtime_authorization
        )
        payload["execution_authority"] = live_execution_mode
    elif live_execution_mode is not None:
        raise EpisodeContextRunnerError(
            "fixture launch cannot claim live execution authority"
        )
    return payload


def _terminal_for_turn(
    *,
    loaded: Mapping[str, Any],
    launch_path: Path,
    input_path: Path,
    sidecar_path: Path,
    output_path: Path,
    sidecar: Mapping[str, Any],
    submission: Mapping[str, Any],
    recovered: bool,
) -> dict[str, Any]:
    launch = _load_json(launch_path, label="episode-context turn launch")
    payload = {
        "schema_version": TURN_TERMINAL_VERSION,
        "state": "passed",
        "run_id": launch["run_id"],
        "lifecycle_id": launch["lifecycle_id"],
        "contract_sha256": loaded["sha256"],
        "evaluation_id": launch["evaluation_id"],
        "frozen_configuration_sha256": loaded["configuration_sha256"],
        "job_id": launch["job_id"],
        "episode_id": launch["episode_id"],
        "context_run_id": launch["context_run_id"],
        "claim_id": launch["claim_id"],
        "attempt_id": launch["attempt_id"],
        "queue_attempt_number": launch["queue_attempt_number"],
        "thread_id": launch["thread_id"],
        "turn_id": sidecar["turn_id"],
        "semantic_retry_count": 0,
        "recovered_without_replay": recovered,
        "deterministic_semantic_pruning": False,
        "semantic_authority_fields": list(SEMANTIC_AUTHORITY_FIELDS),
        "required_episode_manifest": launch["required_episode_manifest"],
        "capacity_admission": launch["capacity_admission"],
        "input": _record(input_path),
        "launch": _record(launch_path),
        "sidecar": _record(sidecar_path),
        "output": _record(output_path),
        "usage": _valid_usage(sidecar["usage"]),
        "wall_elapsed_seconds": float(sidecar["wall_elapsed_seconds"]),
        "submission": dict(submission),
        "production_mutated": submission.get("production_mutated") is True,
    }
    if "execution_authority" in launch:
        authority = launch["execution_authority"]
        payload["execution_authority"] = authority
        payload["test_only"] = authority == "offline_test_only"
        payload["promotable"] = authority == "official_live"
    return payload


def _validate_turn_input_launch(
    *,
    loaded: Mapping[str, Any],
    input_path: Path,
    launch_path: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    input_payload = _load_json(input_path, label="episode-context turn input")
    launch = _load_json(launch_path, label="episode-context turn launch")
    expected_input = {
        "schema_version",
        "job_id",
        "episode_id",
        "context_run_id",
        "claim_id",
        "attempt_id",
        "queue_attempt_number",
        "submission_output_path",
        "source_input",
        "semantic_authority_fields",
        "deterministic_semantic_pruning",
    }
    expected_launch = {
        "schema_version",
        "run_id",
        "lifecycle_id",
        "contract_sha256",
        "evaluation_id",
        "evaluation_receipt",
        "holdout_receipt",
        "frozen_configuration_sha256",
        "job_id",
        "episode_id",
        "context_run_id",
        "claim_id",
        "attempt_id",
        "queue_attempt_number",
        "thread_id",
        "instruction_sources_sha256",
        "instruction_sources_count",
        "base_instructions_sha256",
        "base_instructions_bytes",
        "model",
        "reasoning_effort",
        "configured_batch_size",
        "turn_batch_size",
        "thread_mode",
        "semantic_retry_count",
        "ambiguous_retry_allowed",
        "deterministic_semantic_pruning",
        "semantic_authority_fields",
        "source_input_sha256",
        "required_episode_manifest",
        "capacity_admission",
        "input",
        "prompt",
        "output_schema",
    }
    live_runtime_record = launch.get("live_runtime_authorization")
    if live_runtime_record is not None:
        expected_launch.update(
            {"live_runtime_authorization", "execution_authority"}
        )
    lifecycle_fields = (
        "job_id",
        "episode_id",
        "context_run_id",
        "claim_id",
        "attempt_id",
        "queue_attempt_number",
    )
    configuration = loaded["configuration"]
    contract = loaded["contract"]
    if (
        set(input_payload) != expected_input
        or input_payload.get("schema_version") != TURN_INPUT_VERSION
        or set(launch) != expected_launch
        or launch.get("schema_version") != TURN_LAUNCH_VERSION
        or not isinstance(launch.get("run_id"), str)
        or not launch["run_id"]
        or not isinstance(launch.get("lifecycle_id"), str)
        or not launch["lifecycle_id"]
        or launch.get("contract_sha256") != loaded["sha256"]
        or launch.get("evaluation_id") != contract["lineage"]["evaluation_id"]
        or launch.get("evaluation_receipt")
        != contract["lineage"]["evaluation_receipt"]
        or launch.get("holdout_receipt") != contract["lineage"]["holdout_receipt"]
        or launch.get("frozen_configuration_sha256")
        != loaded["configuration_sha256"]
        or any(launch.get(field) != input_payload.get(field) for field in lifecycle_fields)
        or not isinstance(launch.get("thread_id"), str)
        or not launch["thread_id"]
        or not _is_sha256(launch.get("instruction_sources_sha256"))
        or isinstance(launch.get("instruction_sources_count"), bool)
        or not isinstance(launch.get("instruction_sources_count"), int)
        or launch["instruction_sources_count"] < 0
        or launch.get("base_instructions_sha256")
        != _sha256_bytes(loaded["prompt_path"].read_bytes())
        or launch.get("base_instructions_bytes")
        != loaded["prompt_path"].stat().st_size
        or launch.get("model") != configuration["model"]
        or launch.get("reasoning_effort") != configuration["reasoning_effort"]
        or launch.get("configured_batch_size") != configuration["batch_size"]
        or launch.get("turn_batch_size") != 1
        or launch.get("thread_mode") != THREAD_MODE
        or launch.get("semantic_retry_count") != 0
        or launch.get("ambiguous_retry_allowed") is not False
        or launch.get("deterministic_semantic_pruning") is not False
        or input_payload.get("deterministic_semantic_pruning") is not False
        or launch.get("semantic_authority_fields")
        != list(SEMANTIC_AUTHORITY_FIELDS)
        or input_payload.get("semantic_authority_fields")
        != list(SEMANTIC_AUTHORITY_FIELDS)
        or not isinstance(input_payload.get("source_input"), Mapping)
        or launch.get("source_input_sha256")
        != _sha256_bytes(
            _canonical_json(dict(input_payload.get("source_input") or {})).encode()
        )
        or launch.get("input") != _record(input_path)
        or launch.get("prompt") != contract["artifacts"]["prompt"]
        or launch.get("output_schema") != contract["artifacts"]["output_schema"]
        or (
            live_runtime_record is not None
            and launch.get("execution_authority")
            not in {"official_live", "offline_test_only"}
        )
    ):
        raise EpisodeContextRunnerError(
            "episode-context input and launch reconciliation drifted"
        )
    manifest_path = _verify_record(
        launch["required_episode_manifest"], label="launch required episode manifest"
    )
    manifest = _validate_required_manifest(
        _load_json(manifest_path, label="launch required episode manifest"),
        loaded=loaded,
    )
    if live_runtime_record is None:
        request, _admission = _verify_capacity_binding(
            launch["capacity_admission"], loaded=loaded
        )
        requested_identity = request["requested_items"]
    else:
        runtime_path = _verify_record(
            live_runtime_record, label="live runtime authorization"
        )
        runtime = load_live_episode_context_runtime_authorization(
            runtime_path, loaded=loaded
        )
        preclaim, _preturn = _verify_live_capacity_binding(
            launch["capacity_admission"], loaded=loaded, runtime=runtime
        )
        request = preclaim["request"]
        requested_identity = [request["requested_item"]]
    identity = {
        "job_id": launch["job_id"],
        "episode_id": launch["episode_id"],
    }
    if (
        launch["episode_id"] not in manifest["episode_ids"]
        or request["run_id"] != launch["run_id"]
        or identity not in requested_identity
        or request["required_episode_manifest"]
        != launch["required_episode_manifest"]
    ):
        raise EpisodeContextRunnerError(
            "episode-context manifest or capacity launch binding drifted"
        )
    return input_payload, launch


def _completed_turn_live_lineage_kwargs(
    *, loaded: Mapping[str, Any], launch: Mapping[str, Any]
) -> dict[str, Any]:
    runtime_record = launch.get("live_runtime_authorization")
    if runtime_record is None:
        return {}
    runtime_path = _verify_record(
        runtime_record, label="live runtime authorization"
    )
    runtime = load_live_episode_context_runtime_authorization(
        runtime_path, loaded=loaded
    )
    return {
        "instruction_sources_sha256": runtime["instruction_sources"][
            "effective_instruction_sources_sha256"
        ],
        "instruction_sources_count": runtime["instruction_sources"][
            "effective_instruction_sources_count"
        ],
        "protocol_schema_sha256": runtime["authorization"]["runtime_artifacts"]
        ["protocol_schema"]["sha256"],
        "require_official_live_lineage": True,
    }


def _recover_stale_item(
    *,
    loaded: Mapping[str, Any],
    root: Path,
    queue: EpisodeContextQueueOperations,
    item: EpisodeContextItem,
) -> dict[str, Any] | None:
    paths = _turn_paths(root, item)
    if paths["terminal"].exists():
        return None
    if not all(paths[name].is_file() for name in ("input", "launch", "sidecar", "output")):
        return None
    _input_payload, launch = _validate_turn_input_launch(
        loaded=loaded, input_path=paths["input"], launch_path=paths["launch"]
    )
    if (
        launch.get("schema_version") != TURN_LAUNCH_VERSION
        or launch.get("contract_sha256") != loaded["sha256"]
        or launch.get("job_id") != item.job_id
        or launch.get("episode_id") != item.episode_id
        or launch.get("context_run_id") != item.context_run_id
        or launch.get("claim_id") != item.claim_id
        or launch.get("attempt_id") != item.attempt_id
        or launch.get("queue_attempt_number") != item.queue_attempt_number
        or launch.get("semantic_retry_count") != 0
    ):
        return None
    prompt = paths["input"].read_text(encoding="utf-8")
    base = loaded["prompt_path"].read_text(encoding="utf-8")
    sidecar, output = _validate_completed_turn(
        sidecar_path=paths["sidecar"],
        output_path=paths["output"],
        thread_id=str(launch["thread_id"]),
        model=str(loaded["configuration"]["model"]),
        effort=str(loaded["configuration"]["reasoning_effort"]),
        prompt=prompt,
        base_instructions=base,
        schema=loaded["schema"],
        **_completed_turn_live_lineage_kwargs(loaded=loaded, launch=launch),
    )
    if output.get("episode_id") != item.episode_id:
        return None
    submission = queue.submit(item, paths["output"])
    terminal = _terminal_for_turn(
        loaded=loaded,
        launch_path=paths["launch"],
        input_path=paths["input"],
        sidecar_path=paths["sidecar"],
        output_path=paths["output"],
        sidecar=sidecar,
        submission=submission,
        recovered=True,
    )
    _write_immutable(paths["terminal"], _pretty_json(terminal).encode())
    queue.bind_terminal(item, paths["terminal"])
    return terminal


def _recover_db_completed_unterminalized_turns(
    *,
    loaded: Mapping[str, Any],
    root: Path,
    queue: EpisodeContextQueueOperations,
) -> int:
    turns_root = root / "turns"
    if not turns_root.is_dir():
        return 0
    recovered = 0
    for attempt_root in sorted(turns_root.iterdir()):
        if not attempt_root.is_dir():
            continue
        completed_turn = {
            "input.private.json",
            "launch.json",
            "sidecar.json",
            "output.private.json",
        }
        names = {path.name for path in attempt_root.iterdir()}
        if names not in (completed_turn, completed_turn | {"terminal.json"}):
            continue
        input_value, _launch = _validate_turn_input_launch(
            loaded=loaded,
            input_path=attempt_root / "input.private.json",
            launch_path=attempt_root / "launch.json",
        )
        item = EpisodeContextItem(
            job_id=int(input_value["job_id"]),
            episode_id=str(input_value["episode_id"]),
            context_run_id=str(input_value["context_run_id"]),
            claim_id=str(input_value["claim_id"]),
            attempt_id=str(input_value["attempt_id"]),
            queue_attempt_number=int(input_value["queue_attempt_number"]),
            source_input=dict(input_value["source_input"]),
            submission_output_path=(
                str(input_value["submission_output_path"])
                if input_value.get("submission_output_path")
                else None
            ),
        )
        if "terminal.json" in names:
            queue.bind_terminal(item, attempt_root / "terminal.json")
        else:
            terminal = _recover_stale_item(
                loaded=loaded,
                root=root,
                queue=queue,
                item=item,
            )
            if terminal is None:
                raise EpisodeContextRunnerError(
                    "db-completed unterminalized context turn could not be reconciled"
                )
        recovered += 1
    return recovered


def _terminal_evidence(root: Path, loaded: Mapping[str, Any]) -> list[dict[str, Any]]:
    terminals = []
    turns_root = root / "turns"
    expected_attempt_files = {
        "input.private.json",
        "launch.json",
        "sidecar.json",
        "output.private.json",
        "terminal.json",
    }
    attempt_roots: list[Path] = []
    if turns_root.exists():
        for attempt_root in sorted(turns_root.iterdir()):
            if (
                not attempt_root.is_dir()
                or {item.name for item in attempt_root.iterdir()}
                != expected_attempt_files
            ):
                raise EpisodeContextRunnerError(
                    "episode-context orphan or partial attempt artifacts exist"
                )
            attempt_roots.append(attempt_root)
    for attempt_root in attempt_roots:
        path = attempt_root / "terminal.json"
        terminal = _load_json(path, label="episode-context terminal")
        expected = {
            "schema_version",
            "state",
            "run_id",
            "lifecycle_id",
            "contract_sha256",
            "evaluation_id",
            "frozen_configuration_sha256",
            "job_id",
            "episode_id",
            "context_run_id",
            "claim_id",
            "attempt_id",
            "queue_attempt_number",
            "thread_id",
            "turn_id",
            "semantic_retry_count",
            "recovered_without_replay",
            "deterministic_semantic_pruning",
            "semantic_authority_fields",
            "required_episode_manifest",
            "capacity_admission",
            "input",
            "launch",
            "sidecar",
            "output",
            "usage",
            "wall_elapsed_seconds",
            "submission",
            "production_mutated",
        }
        if "execution_authority" in terminal:
            expected.update({"execution_authority", "test_only", "promotable"})
        if (
            set(terminal) != expected
            or terminal.get("schema_version") != TURN_TERMINAL_VERSION
            or terminal.get("state") != "passed"
            or terminal.get("contract_sha256") != loaded["sha256"]
            or terminal.get("evaluation_id")
            != loaded["contract"]["lineage"]["evaluation_id"]
            or terminal.get("frozen_configuration_sha256")
            != loaded["configuration_sha256"]
            or terminal.get("semantic_retry_count") != 0
            or terminal.get("deterministic_semantic_pruning") is not False
            or terminal.get("semantic_authority_fields") != list(SEMANTIC_AUTHORITY_FIELDS)
            or not isinstance(terminal.get("recovered_without_replay"), bool)
            or not isinstance(terminal.get("production_mutated"), bool)
            or isinstance(terminal.get("queue_attempt_number"), bool)
            or not isinstance(terminal.get("queue_attempt_number"), int)
            or terminal["queue_attempt_number"] < 1
        ):
            raise EpisodeContextRunnerError("episode-context terminal lineage drifted")
        input_path = _verify_record(terminal["input"], label="terminal input")
        launch_path = _verify_record(terminal["launch"], label="terminal launch")
        sidecar_path = _verify_record(terminal["sidecar"], label="terminal sidecar")
        output_path = _verify_record(terminal["output"], label="terminal output")
        if any(
            not _contained(item, root / "turns")
            for item in (path, input_path, launch_path, sidecar_path, output_path)
        ):
            raise EpisodeContextRunnerError("episode-context terminal escaped artifact root")
        input_payload, launch = _validate_turn_input_launch(
            loaded=loaded, input_path=input_path, launch_path=launch_path
        )
        prompt = input_path.read_text(encoding="utf-8")
        sidecar, output = _validate_completed_turn(
            sidecar_path=sidecar_path,
            output_path=output_path,
            thread_id=str(terminal["thread_id"]),
            model=str(loaded["configuration"]["model"]),
            effort=str(loaded["configuration"]["reasoning_effort"]),
            prompt=prompt,
            base_instructions=loaded["prompt_path"].read_text(encoding="utf-8"),
            schema=loaded["schema"],
            **_completed_turn_live_lineage_kwargs(loaded=loaded, launch=launch),
        )
        if (
            any(
                launch.get(field) != terminal.get(field)
                for field in (
                    "run_id",
                    "lifecycle_id",
                    "job_id",
                    "episode_id",
                    "context_run_id",
                    "claim_id",
                    "attempt_id",
                    "queue_attempt_number",
                    "thread_id",
                )
            )
            or terminal.get("required_episode_manifest")
            != launch["required_episode_manifest"]
            or terminal.get("capacity_admission") != launch["capacity_admission"]
            or terminal.get("execution_authority")
            != launch.get("execution_authority")
            or (
                "execution_authority" in launch
                and (
                    terminal.get("test_only")
                    is not (launch["execution_authority"] == "offline_test_only")
                    or terminal.get("promotable")
                    is not (launch["execution_authority"] == "official_live")
                )
            )
            or terminal.get("input") != launch["input"]
            or input_payload.get("episode_id") != output.get("episode_id")
            or sidecar.get("turn_id") != terminal["turn_id"]
            or output.get("episode_id") != terminal["episode_id"]
            or terminal.get("usage") != _valid_usage(sidecar["usage"])
            or float(terminal.get("wall_elapsed_seconds"))
            != float(sidecar["wall_elapsed_seconds"])
        ):
            raise EpisodeContextRunnerError("episode-context terminal artifacts disagree")
        terminals.append({**terminal, "terminal": _record(path)})
    return terminals


def _completion_payload(
    *,
    root: Path,
    loaded: Mapping[str, Any],
    backlog: Mapping[str, Any],
) -> dict[str, Any]:
    normalized_backlog = _normalize_backlog(backlog)
    if (
        normalized_backlog["remaining_episode_context_jobs"] != 0
        or normalized_backlog["missing_required_episode_count"] != 0
        or normalized_backlog["completed_required_episode_count"]
        != normalized_backlog["required_episode_count"]
        or any(normalized_backlog["job_status_counts"][key] != 0 for key in ("pending", "claimed", "failed", "other"))
    ):
        raise EpisodeContextRunnerWaiting("episode-context backlog is not exactly zero")
    terminals = _terminal_evidence(root, loaded)
    required_count = normalized_backlog["required_episode_count"]
    if len(terminals) != required_count:
        raise EpisodeContextRunnerWaiting(
            "completed episode-context jobs lack immutable managed-runner evidence"
        )
    identifier_fields = (
        "lifecycle_id",
        "claim_id",
        "attempt_id",
        "context_run_id",
        "thread_id",
        "turn_id",
        "episode_id",
    )
    for field in identifier_fields:
        values = [str(item[field]) for item in terminals]
        if any(not value for value in values) or len(values) != len(set(values)):
            raise EpisodeContextRunnerError(f"episode-context {field} values are not unique")
    usage = Counter()
    wall = 0.0
    turns = []
    production_mutated = False
    capacity_admissions: dict[str, dict[str, Any]] = {}
    for terminal in terminals:
        usage.update(_valid_usage(terminal["usage"]))
        wall += float(terminal["wall_elapsed_seconds"])
        production_mutated = production_mutated or terminal["production_mutated"]
        turns.append(
            {
                key: terminal[key]
                for key in (
                    "run_id",
                    "lifecycle_id",
                    "claim_id",
                    "attempt_id",
                    "queue_attempt_number",
                    "context_run_id",
                    "thread_id",
                    "turn_id",
                )
            }
            | {
                "episode_id_sha256": _sha256_bytes(str(terminal["episode_id"]).encode()),
                "input": terminal["input"],
                "sidecar": terminal["sidecar"],
                "output": terminal["output"],
                "terminal": terminal["terminal"],
                "capacity_admission": terminal["capacity_admission"],
                "recovered_without_replay": terminal["recovered_without_replay"],
            }
        )
        capacity_key = terminal["capacity_admission"]["admission"]["sha256"]
        capacity_admissions[capacity_key] = dict(terminal["capacity_admission"])
    telemetry = {
        "schema_version": USAGE_TELEMETRY_VERSION,
        "telemetry_mode": "fixture_measured",
        "accounting_complete": True,
        "cache_telemetry_complete": True,
        "measured_turn_count": len(terminals),
        **{field: int(usage[field]) for field in USAGE_FIELDS},
        "wall_elapsed_seconds": round(wall, 6),
    }
    context_artifact_index = {
        str(terminal["episode_id"]): dict(terminal["output"])
        for terminal in terminals
    }
    if len(context_artifact_index) != len(terminals):
        raise EpisodeContextRunnerError("episode-context artifact index is not one-per-episode")
    manifest_path = _verify_record(
        normalized_backlog["required_episode_manifest"],
        label="completion required episode manifest",
    )
    manifest = _validate_required_manifest(
        _load_json(manifest_path, label="completion required episode manifest"),
        loaded=loaded,
    )
    if set(context_artifact_index) != set(manifest["episode_ids"]):
        raise EpisodeContextRunnerError(
            "episode-context terminal set differs from the immutable required manifest"
        )
    context_artifact_index_sha256 = _sha256_bytes(
        _canonical_json(context_artifact_index).encode()
    )
    configuration = loaded["configuration"]
    contract = loaded["contract"]
    completion_id = stable_id(
        loaded["sha256"],
        normalized_backlog["snapshot_sha256"],
        normalized_backlog["required_episode_manifest"]["sha256"],
        *sorted(capacity_admissions),
        *[item["terminal"]["sha256"] for item in turns],
        prefix="ectxcomp_",
    )
    return {
        "schema_version": COMPLETION_RECEIPT_VERSION,
        "state": "passed",
        "terminal_reason": "episode_context_backlog_zero_verified",
        "completion_id": completion_id,
        "phase": "episode_context",
        "contract": _record(loaded["path"]),
        "contract_sha256": loaded["sha256"],
        "evaluation_id": contract["lineage"]["evaluation_id"],
        "evaluation_receipt": contract["lineage"]["evaluation_receipt"],
        "holdout_receipt": contract["lineage"]["holdout_receipt"],
        "frozen_configuration": contract["artifacts"]["frozen_configuration"],
        "frozen_configuration_sha256": loaded["configuration_sha256"],
        "prompt": contract["artifacts"]["prompt"],
        "prompt_sha256": configuration["prompt_sha256"],
        "output_schema": contract["artifacts"]["output_schema"],
        "output_schema_sha256": configuration["output_schema_sha256"],
        "runner_source": contract["artifacts"]["runner_source"],
        "model": configuration["model"],
        "lane": configuration["lane"],
        "label_pack": configuration["label_pack"],
        "reasoning_effort": configuration["reasoning_effort"],
        "batch_size": configuration["batch_size"],
        "transport": TRANSPORT,
        "auth_mode": "chatgpt",
        "plan_type": "pro",
        "persistent_transport": True,
        "thread_mode": THREAD_MODE,
        "semantic_authority_fields": list(SEMANTIC_AUTHORITY_FIELDS),
        "deterministic_semantic_pruning": False,
        "semantic_retry_count": 0,
        "ambiguous_retry_count": 0,
        "live_model_call_count": 0,
        "fixture_turn_count": len(terminals),
        "execution_mode": "fixture_only",
        "production_mutated": production_mutated,
        "required_episode_manifest": normalized_backlog[
            "required_episode_manifest"
        ],
        "capacity_admissions": [
            capacity_admissions[key] for key in sorted(capacity_admissions)
        ],
        "backlog": normalized_backlog,
        "usage": telemetry,
        "context_artifact_index": context_artifact_index,
        "context_artifact_index_sha256": context_artifact_index_sha256,
        "turns": turns,
    }


def verify_episode_context_completion_receipt(
    path: Path,
    *,
    expected_contract_sha256: str,
    expected_evaluation_receipt: str | os.PathLike[str] | Mapping[str, Any],
    expected_holdout_receipt: str | os.PathLike[str] | Mapping[str, Any],
) -> dict[str, Any]:
    receipt_path = path.expanduser().resolve()
    receipt = _load_json(receipt_path, label="episode-context completion receipt")
    expected = {
        "schema_version",
        "state",
        "terminal_reason",
        "completion_id",
        "phase",
        "contract",
        "contract_sha256",
        "evaluation_id",
        "evaluation_receipt",
        "holdout_receipt",
        "frozen_configuration",
        "frozen_configuration_sha256",
        "prompt",
        "prompt_sha256",
        "output_schema",
        "output_schema_sha256",
        "runner_source",
        "model",
        "lane",
        "label_pack",
        "reasoning_effort",
        "batch_size",
        "transport",
        "auth_mode",
        "plan_type",
        "persistent_transport",
        "thread_mode",
        "semantic_authority_fields",
        "deterministic_semantic_pruning",
        "semantic_retry_count",
        "ambiguous_retry_count",
        "live_model_call_count",
        "fixture_turn_count",
        "execution_mode",
        "production_mutated",
        "required_episode_manifest",
        "capacity_admissions",
        "backlog",
        "usage",
        "context_artifact_index",
        "context_artifact_index_sha256",
        "turns",
    }
    if (
        set(receipt) != expected
        or receipt.get("schema_version") != COMPLETION_RECEIPT_VERSION
        or receipt.get("state") != "passed"
        or receipt.get("terminal_reason") != "episode_context_backlog_zero_verified"
        or receipt.get("phase") != "episode_context"
        or not _is_sha256(expected_contract_sha256)
        or receipt.get("contract_sha256") != expected_contract_sha256
        or receipt.get("transport") != TRANSPORT
        or receipt.get("auth_mode") != "chatgpt"
        or receipt.get("plan_type") != "pro"
        or receipt.get("persistent_transport") is not True
        or receipt.get("thread_mode") != THREAD_MODE
        or receipt.get("semantic_authority_fields") != list(SEMANTIC_AUTHORITY_FIELDS)
        or receipt.get("deterministic_semantic_pruning") is not False
        or receipt.get("semantic_retry_count") != 0
        or receipt.get("ambiguous_retry_count") != 0
        or receipt.get("live_model_call_count") != 0
        or receipt.get("execution_mode") != "fixture_only"
        or receipt.get("production_mutated") is not False
    ):
        raise EpisodeContextRunnerError("episode-context completion receipt values drifted")
    contract_path = _verify_record(receipt["contract"], label="completion contract")
    loaded = load_episode_context_contract(contract_path)
    if loaded["sha256"] != expected_contract_sha256:
        raise EpisodeContextRunnerError("completion contract hash drifted")
    expected_evaluation = _expected_record(
        expected_evaluation_receipt, label="expected evaluation receipt"
    )
    expected_holdout = _expected_record(
        expected_holdout_receipt, label="expected holdout receipt"
    )
    contract = loaded["contract"]
    configuration = loaded["configuration"]
    if (
        receipt.get("evaluation_receipt") != expected_evaluation
        or receipt.get("holdout_receipt") != expected_holdout
        or contract["lineage"]["evaluation_receipt"] != expected_evaluation
        or contract["lineage"]["holdout_receipt"] != expected_holdout
        or receipt.get("evaluation_id") != contract["lineage"]["evaluation_id"]
        or receipt.get("frozen_configuration")
        != contract["artifacts"]["frozen_configuration"]
        or receipt.get("frozen_configuration_sha256") != loaded["configuration_sha256"]
        or receipt.get("prompt") != contract["artifacts"]["prompt"]
        or receipt.get("prompt_sha256") != configuration["prompt_sha256"]
        or receipt.get("output_schema") != contract["artifacts"]["output_schema"]
        or receipt.get("output_schema_sha256") != configuration["output_schema_sha256"]
        or receipt.get("runner_source") != contract["artifacts"]["runner_source"]
        or receipt.get("model") != configuration["model"]
        or receipt.get("lane") != configuration["lane"]
        or receipt.get("label_pack") != configuration["label_pack"]
        or receipt.get("reasoning_effort") != configuration["reasoning_effort"]
        or receipt.get("batch_size") != configuration["batch_size"]
    ):
        raise EpisodeContextRunnerError("episode-context completion lineage drifted")
    backlog = _normalize_backlog(receipt["backlog"])
    counts = backlog["job_status_counts"]
    if (
        backlog["remaining_episode_context_jobs"] != 0
        or backlog["missing_required_episode_count"] != 0
        or backlog["completed_required_episode_count"] != backlog["required_episode_count"]
        or any(counts[field] != 0 for field in ("pending", "claimed", "failed", "other"))
    ):
        raise EpisodeContextRunnerError("episode-context completion backlog is not zero")
    if receipt.get("required_episode_manifest") != backlog[
        "required_episode_manifest"
    ]:
        raise EpisodeContextRunnerError(
            "completion required episode manifest binding drifted"
        )
    manifest_path = _verify_record(
        receipt["required_episode_manifest"],
        label="completion required episode manifest",
    )
    manifest = _validate_required_manifest(
        _load_json(manifest_path, label="completion required episode manifest"),
        loaded=loaded,
    )
    capacity_admissions = receipt.get("capacity_admissions")
    if not isinstance(capacity_admissions, list):
        raise EpisodeContextRunnerError("completion capacity admissions shape drifted")
    verified_capacity: dict[str, dict[str, Any]] = {}
    for binding in capacity_admissions:
        request, _admission = _verify_capacity_binding(binding, loaded=loaded)
        key = binding["admission"]["sha256"]
        if key in verified_capacity:
            raise EpisodeContextRunnerError("completion capacity admission is duplicated")
        if request["required_episode_manifest"] != receipt[
            "required_episode_manifest"
        ]:
            raise EpisodeContextRunnerError(
                "completion capacity manifest binding drifted"
            )
        verified_capacity[key] = dict(binding)
    turns = receipt.get("turns")
    if not isinstance(turns, list) or len(turns) != backlog["required_episode_count"]:
        raise EpisodeContextRunnerError("episode-context completion turn count drifted")
    independently_verified = _terminal_evidence(receipt_path.parent, loaded)
    if len(independently_verified) != len(turns):
        raise EpisodeContextRunnerError("episode-context terminal evidence count drifted")
    evidence_by_terminal = {
        item["terminal"]["sha256"]: item for item in independently_verified
    }
    if len(evidence_by_terminal) != len(independently_verified):
        raise EpisodeContextRunnerError("episode-context terminal evidence is duplicated")
    artifact_index = receipt.get("context_artifact_index")
    if (
        not isinstance(artifact_index, Mapping)
        or len(artifact_index) != backlog["required_episode_count"]
        or not _is_sha256(receipt.get("context_artifact_index_sha256"))
        or _sha256_bytes(_canonical_json(artifact_index).encode())
        != receipt["context_artifact_index_sha256"]
    ):
        raise EpisodeContextRunnerError("episode-context artifact index drifted")
    independent_by_episode = {
        str(item["episode_id"]): item for item in independently_verified
    }
    if len(independent_by_episode) != len(independently_verified):
        raise EpisodeContextRunnerError("episode-context terminal episodes are duplicated")
    if (
        set(artifact_index) != set(independent_by_episode)
        or set(artifact_index) != set(manifest["episode_ids"])
    ):
        raise EpisodeContextRunnerError("episode-context artifact index is incomplete")
    normalized_artifact_index: dict[str, dict[str, Any]] = {}
    for episode_id, artifact_record in artifact_index.items():
        if not isinstance(episode_id, str) or not episode_id:
            raise EpisodeContextRunnerError("episode-context artifact index has an invalid key")
        artifact_path = _verify_record(
            artifact_record, label=f"episode-context artifact {episode_id}"
        )
        if not _contained(artifact_path, receipt_path.parent / "turns"):
            raise EpisodeContextRunnerError("episode-context artifact escaped artifact root")
        independent = independent_by_episode[episode_id]
        if independent["output"] != artifact_record:
            raise EpisodeContextRunnerError("episode-context artifact index record drifted")
        output = _load_json(artifact_path, label=f"episode-context artifact {episode_id}")
        _validate_schema_value(loaded["schema"], output, path="$")
        if output.get("episode_id") != episode_id:
            raise EpisodeContextRunnerError("episode-context artifact episode binding drifted")
        normalized_artifact_index[episode_id] = dict(artifact_record)
    usage = receipt.get("usage")
    if not isinstance(usage, Mapping) or set(usage) != {
        "schema_version",
        "telemetry_mode",
        "accounting_complete",
        "cache_telemetry_complete",
        "measured_turn_count",
        *USAGE_FIELDS,
        "wall_elapsed_seconds",
    }:
        raise EpisodeContextRunnerError("episode-context aggregate telemetry shape drifted")
    aggregate = Counter()
    wall = 0.0
    identifiers = {field: set() for field in (
        "lifecycle_id", "claim_id", "attempt_id", "context_run_id", "thread_id", "turn_id"
    )}
    expected_turn_keys = set(identifiers) | {
        "run_id",
        "queue_attempt_number",
        "episode_id_sha256",
        "input",
        "sidecar",
        "output",
        "terminal",
        "capacity_admission",
        "recovered_without_replay",
    }
    aggregate_production_mutated = False
    for turn in turns:
        if not isinstance(turn, Mapping) or set(turn) != expected_turn_keys:
            raise EpisodeContextRunnerError("episode-context receipt turn shape drifted")
        if not _is_sha256(turn.get("episode_id_sha256")) or not isinstance(
            turn.get("recovered_without_replay"), bool
        ) or not isinstance(turn.get("run_id"), str) or not turn["run_id"] or (
            isinstance(turn.get("queue_attempt_number"), bool)
            or not isinstance(turn.get("queue_attempt_number"), int)
            or turn["queue_attempt_number"] < 1
        ):
            raise EpisodeContextRunnerError("episode-context receipt turn values drifted")
        for field in identifiers:
            value = turn.get(field)
            if not isinstance(value, str) or not value or value in identifiers[field]:
                raise EpisodeContextRunnerError(f"episode-context {field} is not unique")
            identifiers[field].add(value)
        terminal_path = _verify_record(turn["terminal"], label="completion terminal")
        if not _contained(terminal_path, receipt_path.parent / "turns"):
            raise EpisodeContextRunnerError("completion terminal escaped artifact root")
        terminal = _load_json(terminal_path, label="completion terminal")
        independent = evidence_by_terminal.get(turn["terminal"]["sha256"])
        if independent is None or independent["terminal"] != turn["terminal"]:
            raise EpisodeContextRunnerError("completion terminal was not independently verified")
        if turn["episode_id_sha256"] != _sha256_bytes(
            str(terminal.get("episode_id") or "").encode()
        ):
            raise EpisodeContextRunnerError("completion episode binding drifted")
        for record_field in ("input", "sidecar", "output"):
            if terminal.get(record_field) != turn[record_field]:
                raise EpisodeContextRunnerError("completion turn record disagrees with terminal")
            _verify_record(turn[record_field], label=f"completion {record_field}")
        for id_field in identifiers:
            if terminal.get(id_field) != turn[id_field]:
                raise EpisodeContextRunnerError("completion lifecycle disagrees with terminal")
        if terminal.get("run_id") != turn["run_id"]:
            raise EpisodeContextRunnerError("completion run lifecycle disagrees with terminal")
        capacity_key = turn["capacity_admission"].get("admission", {}).get("sha256")
        if (
            terminal.get("queue_attempt_number") != turn["queue_attempt_number"]
            or terminal.get("capacity_admission") != turn["capacity_admission"]
            or capacity_key not in verified_capacity
            or verified_capacity[capacity_key] != turn["capacity_admission"]
        ):
            raise EpisodeContextRunnerError(
                "completion capacity or queue-attempt binding drifted"
            )
        terminal_usage = _valid_usage(terminal.get("usage"))
        aggregate.update(terminal_usage)
        aggregate_production_mutated = (
            aggregate_production_mutated or terminal.get("production_mutated") is True
        )
        terminal_wall = terminal.get("wall_elapsed_seconds")
        if (
            isinstance(terminal_wall, bool)
            or not isinstance(terminal_wall, (int, float))
            or not math.isfinite(float(terminal_wall))
            or float(terminal_wall) < 0
        ):
            raise EpisodeContextRunnerError("completion terminal wall telemetry is malformed")
        wall += float(terminal_wall)
    if (
        usage.get("schema_version") != USAGE_TELEMETRY_VERSION
        or usage.get("telemetry_mode") != "fixture_measured"
        or usage.get("accounting_complete") is not True
        or usage.get("cache_telemetry_complete") is not True
        or usage.get("measured_turn_count") != len(turns)
        or receipt.get("fixture_turn_count") != len(turns)
        or any(usage.get(field) != int(aggregate[field]) for field in USAGE_FIELDS)
        or usage.get("wall_elapsed_seconds") != round(wall, 6)
        or aggregate_production_mutated is not receipt["production_mutated"]
    ):
        raise EpisodeContextRunnerError("episode-context aggregate telemetry drifted")
    expected_completion_id = stable_id(
        loaded["sha256"],
        backlog["snapshot_sha256"],
        receipt["required_episode_manifest"]["sha256"],
        *sorted(verified_capacity),
        *[item["terminal"]["sha256"] for item in turns],
        prefix="ectxcomp_",
    )
    if receipt.get("completion_id") != expected_completion_id:
        raise EpisodeContextRunnerError("episode-context completion id drifted")
    return {
        "schema_version": VERIFIED_COMPLETION_VERSION,
        "completion_receipt": _record(receipt_path),
        "completion_id": receipt["completion_id"],
        "contract_sha256": loaded["sha256"],
        "evaluation_id": receipt["evaluation_id"],
        "evaluation_receipt": expected_evaluation,
        "holdout_receipt": expected_holdout,
        "frozen_configuration_sha256": loaded["configuration_sha256"],
        "model": configuration["model"],
        "lane": configuration["lane"],
        "label_pack": configuration["label_pack"],
        "reasoning_effort": configuration["reasoning_effort"],
        "batch_size": configuration["batch_size"],
        "required_episode_count": backlog["required_episode_count"],
        "completed_required_episode_count": backlog["completed_required_episode_count"],
        "missing_required_episode_count": backlog["missing_required_episode_count"],
        "remaining_episode_context_jobs": backlog["remaining_episode_context_jobs"],
        "pending_episode_context_jobs": counts["pending"],
        "claimed_episode_context_jobs": counts["claimed"],
        "failed_episode_context_jobs": counts["failed"],
        "completed_episode_context_jobs": counts["completed"],
        "other_episode_context_jobs": counts["other"],
        "required_episode_ids_sha256": backlog["required_episode_ids_sha256"],
        "completed_episode_ids_sha256": backlog["completed_episode_ids_sha256"],
        "missing_episode_ids_sha256": backlog["missing_episode_ids_sha256"],
        "backlog_snapshot_sha256": backlog["snapshot_sha256"],
        "context_artifact_index": normalized_artifact_index,
        "context_artifact_index_sha256": receipt["context_artifact_index_sha256"],
        "usage": dict(usage),
        "unique_thread_count": len(identifiers["thread_id"]),
        "unique_turn_count": len(identifiers["turn_id"]),
        "semantic_retry_count": 0,
        "production_mutated": False,
        "fixture_only": True,
        "production_ready": False,
    }


def verify_episode_context_artifact_for_episode(
    completion_receipt_path: Path,
    episode_id: str,
    *,
    expected_contract_sha256: str,
    expected_evaluation_receipt: str | os.PathLike[str] | Mapping[str, Any],
    expected_holdout_receipt: str | os.PathLike[str] | Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(episode_id, str) or not episode_id:
        raise EpisodeContextRunnerError("episode_id must be a non-empty string")
    verified = verify_episode_context_completion_receipt(
        completion_receipt_path,
        expected_contract_sha256=expected_contract_sha256,
        expected_evaluation_receipt=expected_evaluation_receipt,
        expected_holdout_receipt=expected_holdout_receipt,
    )
    artifact_record = verified["context_artifact_index"].get(episode_id)
    if artifact_record is None:
        raise EpisodeContextRunnerError(
            f"verified completion has no episode-context artifact for {episode_id}"
        )
    artifact_path = _verify_record(
        artifact_record, label=f"verified episode-context artifact {episode_id}"
    )
    output = _load_json(artifact_path, label=f"verified episode-context artifact {episode_id}")
    if (
        output.get("episode_id") != episode_id
        or any(field not in output for field in SEMANTIC_AUTHORITY_FIELDS)
    ):
        raise EpisodeContextRunnerError("verified episode-context semantic artifact drifted")
    return {
        "episode_id": episode_id,
        "artifact": dict(artifact_record),
        "artifact_index_sha256": verified["context_artifact_index_sha256"],
        "frozen_configuration_sha256": verified["frozen_configuration_sha256"],
        "episode_context": output["episode_context"],
        "extraction_guidance": output["extraction_guidance"],
        "excluded_source_context": output["excluded_source_context"],
        "semantic_authority_fields": list(SEMANTIC_AUTHORITY_FIELDS),
        "deterministic_semantic_pruning": False,
    }


def _run_receipt(
    root: Path,
    *,
    run_id: str,
    loaded: Mapping[str, Any],
    state: str,
    terminal_reason: str,
    backlog: Mapping[str, Any] | None,
    fixture_turn_count: int,
    recovered_count: int,
    ambiguous_count: int,
    error: Exception | None = None,
    live_model_call_count: int = 0,
    live_database_mutation_count: int = 0,
    execution_mode: str = "fixture_only",
) -> dict[str, Any]:
    receipt = {
        "schema_version": RUN_RECEIPT_VERSION,
        "run_id": run_id,
        "state": state,
        "terminal_reason": terminal_reason,
        "contract_sha256": loaded["sha256"],
        "semantic_retry_count": 0,
        "live_model_call_count": live_model_call_count,
        "live_database_mutation_count": live_database_mutation_count,
        "fixture_turn_count": fixture_turn_count,
        "execution_mode": execution_mode,
        "recovered_without_replay_count": recovered_count,
        "ambiguous_stale_claim_count": ambiguous_count,
        "backlog": dict(backlog) if backlog is not None else None,
        "error_class": type(error).__name__ if error else None,
        "error_sha256": _sha256_bytes(str(error).encode()) if error else None,
    }
    path = root / "run-receipts" / f"{run_id}.json"
    _write_immutable(path, _pretty_json(receipt).encode())
    return receipt


def _live_run_receipt(
    root: Path,
    *,
    run_id: str,
    loaded: Mapping[str, Any],
    runtime: Mapping[str, Any],
    state: str,
    terminal_reason: str,
    backlog: Mapping[str, Any] | None,
    live_model_call_count: int,
    live_database_mutation_count: int,
    capacity_admission_count: int,
    recovered_count: int,
    usage: Mapping[str, int],
    wall_elapsed_seconds: float,
    capacity_denial_count: int = 0,
    error: Exception | None = None,
    offline_test_mode: bool = False,
) -> dict[str, Any]:
    runtime_artifacts = _verify_live_runtime_files_now(
        loaded=loaded, runtime=runtime
    )
    receipt = {
        "schema_version": RUN_RECEIPT_VERSION,
        "run_id": run_id,
        "state": state,
        "terminal_reason": terminal_reason,
        "execution_mode": (
            "offline_test_only" if offline_test_mode else "managed_live_bounded"
        ),
        "test_only": offline_test_mode,
        "promotable": False,
        "live_completion_evidence_allowed": not offline_test_mode,
        "contract": _record(loaded["path"]),
        "contract_sha256": loaded["sha256"],
        "live_runtime_authorization": _record(runtime["path"]),
        "semantic_retry_count": 0,
        "live_model_call_count": live_model_call_count,
        "live_database_mutation_count": live_database_mutation_count,
        "fixture_turn_count": 0,
        "capacity_admission_count": capacity_admission_count,
        "capacity_denial_count": capacity_denial_count,
        "recovered_without_replay_count": recovered_count,
        "ambiguous_stale_claim_count": 0,
        "usage": {field: int(usage.get(field, 0)) for field in USAGE_FIELDS},
        "wall_elapsed_seconds": round(float(wall_elapsed_seconds), 6),
        "backlog": dict(backlog) if backlog is not None else None,
        "production_mutated": live_database_mutation_count > 0,
        "error_class": type(error).__name__ if error else None,
        "error_sha256": _sha256_bytes(str(error).encode()) if error else None,
    }
    path = root / "run-receipts" / f"{run_id}.json"
    _write_immutable(path, _pretty_json(receipt).encode())
    return receipt


def _live_capacity_wait_checkpoint(
    root: Path,
    *,
    run_id: str,
    loaded: Mapping[str, Any],
    runtime: Mapping[str, Any],
    backlog: Mapping[str, Any],
    denial: LiveCapacityAdmissionDenied,
    admitted_capacity_count: int,
    live_model_call_count: int,
    live_database_mutation_count: int,
    recovered_count: int,
    usage: Mapping[str, int],
    wall_elapsed_seconds: float,
    offline_test_mode: bool,
) -> dict[str, Any]:
    _verify_live_runtime_files_now(loaded=loaded, runtime=runtime)
    binding = denial.binding
    if set(binding) != {"request", "admission"}:
        raise EpisodeContextRunnerError("capacity denial binding shape drifted")
    request_path = _verify_record(
        binding["request"], label="capacity-wait request"
    )
    admission_path = _verify_record(
        binding["admission"], label="capacity-wait admission"
    )
    request = _validate_live_capacity_request(
        _load_json(request_path, label="capacity-wait request"),
        loaded=loaded,
        policy=runtime["capacity_policy"],
        provision_plan_record=runtime["authorization"]["provision_plan"],
        capacity_policy_record=runtime["authorization"]["capacity_policy"],
        runtime_authorization_record=_record(runtime["path"]),
    )
    admission = _validate_live_capacity_admission(
        _load_json(admission_path, label="capacity-wait admission"),
        request=request,
        policy=runtime["capacity_policy"],
        policy_record=runtime["authorization"]["capacity_policy"],
    )
    if admission["state"] != "denied":
        raise EpisodeContextRunnerError(
            "capacity-wait checkpoint lacks a denied admission"
        )
    payload = {
        "schema_version": LIVE_CAPACITY_WAIT_CHECKPOINT_VERSION,
        "state": "waiting",
        "terminal_reason": "fresh_reserve_capacity_not_admitted",
        "nonterminal_capacity_wait": True,
        "run_id": run_id,
        "contract": _record(loaded["path"]),
        "runtime_authorization": _record(runtime["path"]),
        "capacity_denial": {
            "request": _record(request_path),
            "admission": _record(admission_path),
        },
        "capacity_admission_count": admitted_capacity_count,
        "capacity_denial_count": 1,
        "live_model_call_count": live_model_call_count,
        "live_database_mutation_count": live_database_mutation_count,
        "semantic_retry_count": 0,
        "recovered_without_replay_count": recovered_count,
        "usage": {field: int(usage.get(field, 0)) for field in USAGE_FIELDS},
        "wall_elapsed_seconds": round(float(wall_elapsed_seconds), 6),
        "backlog": dict(backlog),
        "test_only": offline_test_mode,
        "promotable": False,
        "production_context_mutated": live_database_mutation_count > 0,
    }
    path = root / "capacity-wait-checkpoints" / f"{run_id}.json"
    _write_immutable(path, _pretty_json(payload).encode())
    return payload


def _collect_live_capacity_artifacts(
    *,
    root: Path,
    loaded: Mapping[str, Any],
    runtime: Mapping[str, Any],
) -> dict[str, Any]:
    staging_root = root / ".capacity-staging"
    if staging_root.is_dir() and any(staging_root.iterdir()):
        raise EpisodeContextRunnerWaiting(
            "live completion has unpublished capacity staging artifacts"
        )
    capacity_counts = Counter()
    records: list[dict[str, Any]] = []
    by_paths: dict[tuple[str, str], dict[str, Any]] = {}
    seen_request_hashes: set[str] = set()
    seen_admission_ids: set[str] = set()
    capacity_root = root / "capacity"
    if capacity_root.is_dir():
        request_paths = sorted(capacity_root.glob("*/request.json"))
        admission_paths = sorted(capacity_root.glob("*/admission.json"))
        if len(request_paths) != len(admission_paths):
            raise EpisodeContextRunnerWaiting(
                "live completion has an incomplete capacity request/admission pair"
            )
        for admission_path in admission_paths:
            request_path = admission_path.with_name("request.json")
            if request_path not in request_paths:
                raise EpisodeContextRunnerWaiting(
                    "live completion capacity admission lacks its exact request"
                )
            bundle_dir = admission_path.parent
            entry_names = {child.name for child in bundle_dir.iterdir()}
            if (
                bundle_dir.is_symlink()
                or request_path.is_symlink()
                or admission_path.is_symlink()
                or not request_path.is_file()
                or not admission_path.is_file()
                or entry_names
                not in (
                    {"request.json", "admission.json"},
                    {
                        "request.json",
                        "admission.json",
                        "abandoned-no-dispatch.json",
                    },
                )
            ):
                raise EpisodeContextRunnerError(
                    "live completion capacity bundle shape drifted"
                )
            request = _validate_live_capacity_request(
                _load_json(request_path, label="completion capacity request"),
                loaded=loaded,
                policy=runtime["capacity_policy"],
                provision_plan_record=runtime["authorization"]["provision_plan"],
                capacity_policy_record=runtime["authorization"]["capacity_policy"],
                runtime_authorization_record=_record(runtime["path"]),
            )
            admission = _validate_live_capacity_admission(
                _load_json(admission_path, label="completion capacity admission"),
                request=request,
                policy=runtime["capacity_policy"],
                policy_record=runtime["authorization"]["capacity_policy"],
            )
            stem = _live_capacity_artifact_stem(request)
            pair_key = (str(request_path.resolve()), str(admission_path.resolve()))
            if (
                request_path.name != "request.json"
                or admission_path.name != "admission.json"
                or request_path.parent != admission_path.parent
                or request_path.parent.name != stem
                or request_path.parent.parent.resolve() != capacity_root.resolve()
                or request["request_sha256"] in seen_request_hashes
                or admission["admission_id"] in seen_admission_ids
                or pair_key in by_paths
            ):
                raise EpisodeContextRunnerError(
                    "live completion capacity artifact identity drifted"
                )
            seen_request_hashes.add(str(request["request_sha256"]))
            seen_admission_ids.add(str(admission["admission_id"]))
            request_record = _record(request_path)
            admission_record = _record(admission_path)
            abandonment_record = None
            if "abandoned-no-dispatch.json" in entry_names:
                abandonment_path = bundle_dir / "abandoned-no-dispatch.json"
                if abandonment_path.is_symlink() or not abandonment_path.is_file():
                    raise EpisodeContextRunnerError(
                        "live completion capacity abandonment shape drifted"
                    )
                abandonment = _validate_capacity_abandonment(
                    _load_json(
                        abandonment_path,
                        label="completion capacity abandonment",
                    ),
                    request_path=request_path,
                    admission_path=admission_path,
                    request=request,
                    admission=admission,
                )
                abandonment_record = _record(abandonment_path)
                capacity_counts["abandoned_no_dispatch"] += 1
            by_paths[pair_key] = {
                "request": request_record,
                "admission": admission_record,
                "state": admission["state"],
                "request_value": request,
                "admission_value": admission,
                "abandonment": abandonment_record,
            }
            capacity_counts[str(admission["state"])] += 1
            records.append(
                {
                    "run_id": request["run_id"],
                    "episode_ordinal": request["episode_ordinal"],
                    "stage": request["admission_stage"],
                    "requested_item": request["requested_item"],
                    "state": admission["state"],
                    "minimum_applicable_remaining_percent": admission[
                        "minimum_applicable_remaining_percent"
                    ],
                    "applicable_capacity_controls_sha256": admission[
                        "applicable_capacity_controls_sha256"
                    ],
                    "request": request_record,
                    "admission": admission_record,
                    "abandonment": abandonment_record,
                }
            )
    return {
        "counts": capacity_counts,
        "records": records,
        "by_paths": by_paths,
    }


def _discard_unpublished_capacity_staging(root: Path) -> int:
    """Remove only recognized, never-published temp artifacts under the writer lock."""

    staging_root = root / ".capacity-staging"
    if not staging_root.exists():
        return 0
    if staging_root.is_symlink() or not staging_root.is_dir():
        raise EpisodeContextRunnerError("capacity staging root shape drifted")
    discarded = 0
    for entry in sorted(staging_root.iterdir()):
        if entry.is_symlink():
            raise EpisodeContextRunnerError("capacity staging entry is a symlink")
        if entry.is_file():
            if not entry.name.startswith("abandonment.") or not entry.name.endswith(
                ".json"
            ):
                raise EpisodeContextRunnerError("unknown capacity staging file")
            entry.unlink()
            discarded += 1
            continue
        if not entry.is_dir():
            raise EpisodeContextRunnerError("unknown capacity staging entry")
        children = list(entry.iterdir())
        if (
            not children
            or {child.name for child in children}
            not in ({"request.json"}, {"request.json", "admission.json"})
            or any(child.is_symlink() or not child.is_file() for child in children)
        ):
            raise EpisodeContextRunnerError(
                "unpublished capacity staging bundle shape drifted"
            )
        for child in children:
            child.unlink()
        entry.rmdir()
        discarded += 1
    _fsync_directory(staging_root)
    return discarded


def _zero_dispatch_attempt_item(
    *,
    root: Path,
    queue: SQLiteEpisodeContextQueue,
    attempt: Mapping[str, Any],
) -> EpisodeContextItem | None:
    if str(attempt.get("status")) != "claimed":
        return None
    job = queue.conn.execute(
        "SELECT * FROM jobs WHERE id = ?", (int(attempt["job_id"]),)
    ).fetchone()
    if job is None or str(job["status"]) != "claimed":
        return None
    payload = loads_json(str(job["payload_json"] or "{}"), {})
    if (
        payload.get("episode_context_attempt_id") != str(attempt["id"])
        or payload.get("app_server_claim_id") != str(attempt["claim_id"])
        or attempt.get("launch_path")
        or attempt.get("sidecar_path")
        or attempt.get("turn_input_path")
        or attempt.get("terminal_path")
        or loads_json(str(attempt.get("usage_json") or "{}"), {})
    ):
        return None
    prompt_path = Path(str(attempt["prompt_path"] or "")).expanduser().resolve()
    if not prompt_path.is_file():
        raise EpisodeContextRunnerError(
            "zero-dispatch capacity recovery lacks its frozen source snapshot"
        )
    source_input = _load_json(
        prompt_path, label="zero-dispatch capacity source snapshot"
    )
    item = queue._item(dict(job), source_input=source_input)
    paths = _turn_paths(root, item)
    if any(
        paths[field].exists()
        for field in ("input", "launch", "sidecar", "output", "terminal")
    ):
        return None
    return item


def _reconcile_live_capacity_ownership(
    *,
    root: Path,
    loaded: Mapping[str, Any],
    runtime: Mapping[str, Any],
    queue: EpisodeContextQueueOperations,
) -> dict[str, int]:
    """Adopt or abandon complete no-turn probe bundles without replay."""

    if not isinstance(queue, SQLiteEpisodeContextQueue):
        raise EpisodeContextRunnerError(
            "live capacity reconciliation requires SQLite attempt authority"
        )
    discarded_staging_count = _discard_unpublished_capacity_staging(root)
    evidence = _collect_live_capacity_artifacts(
        root=root, loaded=loaded, runtime=runtime
    )
    rows = _query_rows(
        queue.conn,
        """
        SELECT * FROM episode_context_run_attempts
        WHERE attempt_kind = 'managed_app_server'
        ORDER BY created_at, id
        """,
    )
    by_id = {str(row["id"]): row for row in rows}
    owned_pairs: dict[tuple[str, str], str] = {}
    for row in rows:
        for stage in ("preclaim", "preturn"):
            request_path = row[f"{stage}_capacity_request_path"]
            admission_path = row[f"{stage}_capacity_admission_path"]
            if bool(request_path) is not bool(admission_path):
                raise EpisodeContextRunnerError(
                    "attempt has a partial capacity ownership pair"
                )
            if not request_path:
                continue
            pair = (
                str(Path(str(request_path)).expanduser().resolve()),
                str(Path(str(admission_path)).expanduser().resolve()),
            )
            if pair in owned_pairs or pair not in evidence["by_paths"]:
                raise EpisodeContextRunnerError(
                    "attempt capacity ownership is duplicate or unexplained"
                )
            owned_pairs[pair] = str(row["id"])

    adopted_attempt_ids: set[str] = set()
    abandoned_count = 0
    ordered_bundles = sorted(
        evidence["by_paths"].items(),
        key=lambda item: (
            0 if item[1]["request_value"]["admission_stage"] == "preclaim" else 1,
            item[0],
        ),
    )
    for pair, bundle in ordered_bundles:
        if pair in owned_pairs or bundle["abandonment"] is not None:
            continue
        request = bundle["request_value"]
        identity = request["requested_item"]
        job_id = int(identity["job_id"])
        episode_id = str(identity["episode_id"])
        matching_rows = [
            row
            for row in rows
            if int(row["job_id"]) == job_id
            and str(row["episode_id"]) == episode_id
        ]
        zero_dispatch = [
            (row, item)
            for row in matching_rows
            if (
                item := _zero_dispatch_attempt_item(
                    root=root, queue=queue, attempt=row
                )
            )
            is not None
        ]
        dispatched_count = sum(
            str(row["status"])
            in {"launched", "in_progress", "db_completed_unterminalized", "completed"}
            or bool(row.get("launch_path"))
            or bool(row.get("sidecar_path"))
            or bool(row.get("terminal_path"))
            for row in matching_rows
        )
        if request["admission_stage"] == "preclaim":
            if len(zero_dispatch) == 1:
                row, item = zero_dispatch[0]
                if row["preclaim_capacity_request_path"]:
                    raise EpisodeContextRunnerError(
                        "orphan preclaim has an already-owned matching attempt"
                    )
                queue.bind_prelaunch_capacity(
                    item,
                    {
                        "preclaim": {
                            "request": bundle["request"],
                            "admission": bundle["admission"],
                        }
                    },
                )
                adopted_attempt_ids.add(str(row["id"]))
                continue
            job = queue.conn.execute(
                "SELECT status, target_id FROM jobs WHERE id = ?", (job_id,)
            ).fetchone()
            if (
                len(zero_dispatch) > 1
                or dispatched_count != 0
                or job is None
                or str(job["target_id"]) != episode_id
                or str(job["status"]) == "claimed"
            ):
                raise EpisodeContextRunnerError(
                    "orphan preclaim capacity ownership is ambiguous"
                )
            reason = (
                "denied_preclaim_probe_completed_before_claim"
                if bundle["state"] == "denied"
                else "orphan_preclaim_probe_completed_before_claim"
            )
            abandonment = _capacity_abandonment_payload(
                request_path=Path(pair[0]),
                admission_path=Path(pair[1]),
                request=request,
                admission=bundle["admission_value"],
                reason=reason,
                ownership_proof={
                    "job_id": job_id,
                    "episode_id": episode_id,
                    "observed_job_status": str(job["status"]),
                    "matching_zero_dispatch_attempt_ids": [],
                    "matching_dispatched_attempt_count": 0,
                },
            )
            _publish_capacity_abandonment(
                bundle_dir=Path(pair[0]).parent,
                payload=abandonment,
            )
            abandoned_count += 1
            continue

        attempt_id = str(identity["attempt_id"])
        row_value = queue.conn.execute(
            "SELECT * FROM episode_context_run_attempts WHERE id = ?",
            (attempt_id,),
        ).fetchone()
        row = dict(row_value) if row_value is not None else None
        item = (
            _zero_dispatch_attempt_item(root=root, queue=queue, attempt=row)
            if row is not None
            else None
        )
        if (
            row is None
            or item is None
            or int(row["job_id"]) != job_id
            or str(row["episode_id"]) != episode_id
            or str(row["claim_id"]) != str(identity["claim_id"])
            or _sha256_bytes(_canonical_json(dict(item.source_input)).encode())
            != str(request["requested_source_input_sha256"])
            or not row["preclaim_capacity_request_path"]
            or not row["preclaim_capacity_admission_path"]
        ):
            raise EpisodeContextRunnerError(
                "orphan preturn capacity cannot prove one zero-dispatch attempt"
            )
        preclaim_request_path = Path(
            str(row["preclaim_capacity_request_path"])
        ).expanduser().resolve()
        preclaim_admission_path = Path(
            str(row["preclaim_capacity_admission_path"])
        ).expanduser().resolve()
        preclaim_request = _load_json(
            preclaim_request_path, label="reconciled preclaim request"
        )
        if (
            preclaim_request.get("run_id") != request["run_id"]
            or preclaim_request.get("episode_ordinal")
            != request["episode_ordinal"]
        ):
            raise EpisodeContextRunnerError(
                "orphan preturn differs from its owned preclaim probe"
            )
        queue.bind_prelaunch_capacity(
            item,
            {
                "preclaim": {
                    "request": _record(preclaim_request_path),
                    "admission": _record(preclaim_admission_path),
                },
                "preturn": {
                    "request": bundle["request"],
                    "admission": bundle["admission"],
                },
            },
        )
        adopted_attempt_ids.add(attempt_id)

    current_rows = _query_rows(
        queue.conn,
        """
        SELECT * FROM episode_context_run_attempts
        WHERE attempt_kind = 'managed_app_server' AND status = 'claimed'
        ORDER BY created_at, id
        """,
    )
    released_count = 0
    for row in current_rows:
        item = _zero_dispatch_attempt_item(root=root, queue=queue, attempt=row)
        if item is None or not row["preclaim_capacity_request_path"]:
            continue
        queue.release_prelaunch_claim(
            item, reason="startup_capacity_bundle_reconciled_no_dispatch"
        )
        released_count += 1
    return {
        "discarded_unpublished_staging_count": discarded_staging_count,
        "adopted_capacity_bundle_count": len(adopted_attempt_ids),
        "abandoned_capacity_bundle_count": abandoned_count,
        "released_zero_dispatch_attempt_count": released_count,
    }


def _collect_live_completion_evidence(
    *,
    root: Path,
    loaded: Mapping[str, Any],
    runtime: Mapping[str, Any],
    queue: EpisodeContextQueueOperations,
) -> dict[str, Any]:
    _verify_live_runtime_files_now(loaded=loaded, runtime=runtime)
    backlog = _normalize_backlog(queue.accounting())
    required_ids = [str(item) for item in runtime["provision_plan"]["required_episode_ids"]]
    if (
        backlog["remaining_episode_context_jobs"] != 0
        or backlog["missing_required_episode_count"] != 0
        or backlog["required_episode_count"] != len(required_ids)
        or backlog["completed_required_episode_count"] != len(required_ids)
        or any(
            backlog["job_status_counts"][state] != 0
            for state in ("pending", "claimed", "failed", "other")
        )
    ):
        raise EpisodeContextRunnerWaiting(
            "live episode-context backlog is not exactly complete"
        )
    connection = getattr(queue, "conn", None)
    if not isinstance(connection, sqlite3.Connection):
        raise EpisodeContextRunnerError(
            "live completion lacks SQLite attempt authority"
        )
    terminals = _terminal_evidence(root, loaded)
    terminal_by_attempt = {str(item["attempt_id"]): item for item in terminals}
    if len(terminal_by_attempt) != len(terminals):
        raise EpisodeContextRunnerError("live completion terminal attempts are not unique")
    rows = _query_rows(
        connection,
        """
        SELECT * FROM episode_context_run_attempts
        WHERE attempt_kind = 'managed_app_server'
          AND episode_id IN (SELECT CAST(value AS TEXT) FROM json_each(?))
        ORDER BY episode_id, attempt_number
        """,
        (_canonical_json(required_ids),),
    )
    completed_rows = [row for row in rows if str(row["status"]) == "completed"]
    nonterminal_rows = [
        row
        for row in rows
        if str(row["status"]) not in {"completed", "released_prelaunch"}
    ]
    completed_by_episode: dict[str, dict[str, Any]] = {}
    for row in completed_rows:
        episode_id = str(row["episode_id"])
        if episode_id in completed_by_episode:
            raise EpisodeContextRunnerError(
                "live completion has multiple completed attempts for one episode"
            )
        completed_by_episode[episode_id] = row
    if (
        sorted(completed_by_episode) != required_ids
        or len(completed_rows) != len(required_ids)
        or nonterminal_rows
        or set(terminal_by_attempt)
        != {str(row["id"]) for row in completed_rows}
    ):
        raise EpisodeContextRunnerWaiting(
            "live completion attempt population is incomplete or ambiguous"
        )
    usage = Counter()
    wall = 0.0
    thread_ids: set[str] = set()
    turn_ids: set[str] = set()
    attempt_index: dict[str, Any] = {}
    context_artifact_index: dict[str, Any] = {}
    for episode_id in required_ids:
        row = completed_by_episode[episode_id]
        attempt_id = str(row["id"])
        terminal = terminal_by_attempt[attempt_id]
        terminal_path = _verify_record(
            terminal["terminal"], label=f"live terminal {episode_id}"
        )
        if str(row["terminal_path"] or "") != str(terminal_path):
            raise EpisodeContextRunnerError(
                "live completion attempt terminal path drifted"
            )
        launch_path = _verify_record(
            terminal["launch"], label=f"live launch {episode_id}"
        )
        input_path = _verify_record(
            terminal["input"], label=f"live input {episode_id}"
        )
        launch = _load_json(launch_path, label=f"live launch {episode_id}")
        _validate_attempt_launch_columns(
            attempt=row, input_path=input_path, launch=launch
        )
        input_value = _load_json(input_path, label=f"live input {episode_id}")
        records, _input_record = _validate_attempt_source_binding(
            attempt=row,
            input_path=input_path,
            input_value=input_value,
            require_bound_turn_input=True,
        )
        artifact_path = Path(str(row["context_artifact_path"])).expanduser().resolve()
        verified_artifact = _verify_managed_context_artifact(
            artifact_path=artifact_path,
            episode_id=episode_id,
            label_pack=LIVE_LABEL_PACK,
            model=LIVE_QUEUE_PAYLOAD_MODEL,
            loaded=loaded,
        )
        terminal_usage = _valid_usage(terminal["usage"])
        if (
            loads_json(str(row["usage_json"] or "{}"), {}) != terminal_usage
            or terminal.get("execution_authority") != "official_live"
            or str(row["semantic_model"]) != LIVE_SEMANTIC_MODEL
            or str(row["reasoning_effort"]) != LIVE_REASONING_EFFORT
            or str(row["queue_payload_model"]) != LIVE_QUEUE_PAYLOAD_MODEL
            or str(row["label_pack"]) != LIVE_LABEL_PACK
            or records.get("terminal") != terminal["terminal"]
        ):
            raise EpisodeContextRunnerError(
                "live completion attempt execution lineage drifted"
            )
        thread_id = str(terminal["thread_id"])
        turn_id = str(terminal["turn_id"])
        if thread_id in thread_ids or turn_id in turn_ids:
            raise EpisodeContextRunnerError(
                "live completion thread or turn identity was reused"
            )
        thread_ids.add(thread_id)
        turn_ids.add(turn_id)
        usage.update(terminal_usage)
        wall += float(terminal["wall_elapsed_seconds"])
        context_artifact_index[episode_id] = _record(artifact_path)
        attempt_index[episode_id] = {
            "attempt_id": attempt_id,
            "context_run_id": str(row["canonical_run_id"]),
            "job_id": int(row["job_id"]),
            "claim_id": str(row["claim_id"]),
            "thread_id": thread_id,
            "turn_id": turn_id,
            "source_input_sha256": str(row["source_input_sha256"]),
            "prompt": records["prompt"],
            "turn_input": records["turn_input"],
            "launch": terminal["launch"],
            "sidecar": terminal["sidecar"],
            "output": terminal["output"],
            "context_artifact": _record(artifact_path),
            "artifact_lineage": _record(verified_artifact["lineage_path"]),
            "terminal": terminal["terminal"],
            "usage": terminal_usage,
            "wall_elapsed_seconds": float(terminal["wall_elapsed_seconds"]),
        }
    capacity_evidence = _collect_live_capacity_artifacts(
        root=root, loaded=loaded, runtime=runtime
    )
    capacity_counts = capacity_evidence["counts"]
    capacity_records = capacity_evidence["records"]
    capacity_by_paths = capacity_evidence["by_paths"]
    released_prelaunch_count = sum(
        str(row["status"]) == "released_prelaunch" for row in rows
    )

    def _capacity_pair_from_binding(
        binding: Mapping[str, Any], *, label: str
    ) -> tuple[str, str]:
        if not isinstance(binding, Mapping) or set(binding) != {
            "request",
            "admission",
        }:
            raise EpisodeContextRunnerError(f"{label} capacity binding drifted")
        request_path = _verify_record(
            binding["request"], label=f"{label} capacity request"
        )
        admission_path = _verify_record(
            binding["admission"], label=f"{label} capacity admission"
        )
        pair = (str(request_path), str(admission_path))
        observed = capacity_by_paths.get(pair)
        if (
            observed is None
            or observed["request"] != binding["request"]
            or observed["admission"] != binding["admission"]
        ):
            raise EpisodeContextRunnerError(
                f"{label} capacity pair was not independently verified"
            )
        return pair

    expected_owned_pairs: set[tuple[str, str]] = set()
    expected_admitted_pairs: set[tuple[str, str]] = set()
    for row in completed_rows:
        terminal = terminal_by_attempt[str(row["id"])]
        binding = terminal.get("capacity_admission")
        if not isinstance(binding, Mapping) or set(binding) != {
            "preclaim",
            "preturn",
        }:
            raise EpisodeContextRunnerError(
                "completed live context attempt lacks both capacity probes"
            )
        for stage in ("preclaim", "preturn"):
            pair = _capacity_pair_from_binding(
                binding[stage], label=f"completed {row['id']} {stage}"
            )
            if capacity_by_paths[pair]["state"] != "admitted":
                raise EpisodeContextRunnerError(
                    "completed live context attempt used denied capacity"
                )
            expected_admitted_pairs.add(pair)
            expected_owned_pairs.add(pair)
    for row in rows:
        if str(row["status"]) != "released_prelaunch":
            continue
        released_pairs: dict[str, tuple[str, str]] = {}
        for stage in ("preclaim", "preturn"):
            request_value = row[f"{stage}_capacity_request_path"]
            admission_value = row[f"{stage}_capacity_admission_path"]
            if bool(request_value) is not bool(admission_value):
                raise EpisodeContextRunnerWaiting(
                    "released prelaunch attempt has a partial capacity pair"
                )
            if not request_value:
                if stage == "preclaim":
                    raise EpisodeContextRunnerWaiting(
                        "released prelaunch attempt lacks admitted preclaim evidence"
                    )
                continue
            request_path = Path(str(request_value)).expanduser().resolve()
            admission_path = Path(str(admission_value)).expanduser().resolve()
            released_pairs[stage] = _capacity_pair_from_binding(
                {
                    "request": _record(request_path),
                    "admission": _record(admission_path),
                },
                label=f"released {row['id']} {stage}",
            )
        records = loads_json(str(row["artifact_records_json"] or "{}"), {})
        expected_capacity_binding = {
            stage: {
                "request": capacity_by_paths[pair]["request"],
                "admission": capacity_by_paths[pair]["admission"],
            }
            for stage, pair in released_pairs.items()
        }
        if (
            not isinstance(records, Mapping)
            or records.get("capacity") != expected_capacity_binding
        ):
            raise EpisodeContextRunnerError(
                "released prelaunch capacity record binding drifted"
            )
        if capacity_by_paths[released_pairs["preclaim"]]["state"] != "admitted":
            raise EpisodeContextRunnerError(
                "released claimed attempt lacks admitted preclaim capacity"
            )
        if "preturn" not in released_pairs and str(row["error"] or "") not in {
            "fresh_preturn_capacity_probe_failed_no_dispatch",
            "stale_prelaunch_claim_released_without_semantic_turn",
            "startup_capacity_bundle_reconciled_no_dispatch",
        }:
            raise EpisodeContextRunnerError(
                "released attempt lacks a proven no-dispatch preturn interruption"
            )
        for pair in released_pairs.values():
            expected_owned_pairs.add(pair)
            if capacity_by_paths[pair]["state"] == "admitted":
                expected_admitted_pairs.add(pair)
    abandonment_pairs = {
        pair
        for pair, evidence in capacity_by_paths.items()
        if evidence["abandonment"] is not None
    }
    if abandonment_pairs & expected_owned_pairs:
        raise EpisodeContextRunnerError(
            "owned capacity pair was also marked abandoned"
        )
    if set(capacity_by_paths) != expected_owned_pairs | abandonment_pairs:
        raise EpisodeContextRunnerWaiting(
            "live completion has unexplained capacity artifacts"
        )
    actual_admitted_pairs = {
        pair
        for pair, evidence in capacity_by_paths.items()
        if evidence["state"] == "admitted"
    }
    if actual_admitted_pairs != expected_admitted_pairs | {
        pair
        for pair in abandonment_pairs
        if capacity_by_paths[pair]["state"] == "admitted"
    }:
        raise EpisodeContextRunnerWaiting(
            "live completion capacity admissions are not exactly attempt-accounted"
        )
    legacy_snapshots = runtime["provision_execution"]["legacy_snapshots"]
    current_legacy = _legacy_snapshot_records(
        connection,
        expected_episode_ids=runtime["provision_plan"]
        ["legacy_completed_episode_ids_to_rerun"],
    )
    if current_legacy != legacy_snapshots:
        raise EpisodeContextRunnerError(
            "live completion legacy snapshot bytes drifted"
        )
    return {
        "backlog": backlog,
        "required_episode_count": len(required_ids),
        "completed_managed_attempt_count": len(completed_rows),
        "released_prelaunch_attempt_count": released_prelaunch_count,
        "attempt_index": attempt_index,
        "attempt_index_sha256": _sha256_bytes(
            _canonical_json(attempt_index).encode()
        ),
        "context_artifact_index": context_artifact_index,
        "context_artifact_index_sha256": _sha256_bytes(
            _canonical_json(context_artifact_index).encode()
        ),
        "legacy_snapshots": legacy_snapshots,
        "legacy_snapshots_sha256": _sha256_bytes(
            _canonical_json(legacy_snapshots).encode()
        ),
        "capacity_records": capacity_records,
        "capacity_records_sha256": _sha256_bytes(
            _canonical_json(capacity_records).encode()
        ),
        "capacity_admission_count": int(capacity_counts["admitted"]),
        "capacity_denial_count": int(capacity_counts["denied"]),
        "capacity_abandonment_count": int(
            capacity_counts["abandoned_no_dispatch"]
        ),
        "usage": {field: int(usage[field]) for field in USAGE_FIELDS},
        "wall_elapsed_seconds": round(wall, 6),
        "unique_thread_count": len(thread_ids),
        "unique_turn_count": len(turn_ids),
        "runtime_artifacts": runtime_artifacts,
        "execution_authority": "official_live",
    }


def build_live_episode_context_completion_receipt(
    *,
    root: Path,
    loaded: Mapping[str, Any],
    runtime: Mapping[str, Any],
    queue: EpisodeContextQueueOperations,
) -> dict[str, Any]:
    evidence = _collect_live_completion_evidence(
        root=root, loaded=loaded, runtime=runtime, queue=queue
    )
    payload = {
        "schema_version": LIVE_COMPLETION_RECEIPT_VERSION,
        "state": "passed",
        "phase": "episode_context",
        "completed_at": now_iso(),
        "database": _queue_main_database_identity(queue),
        "contract": _record(loaded["path"]),
        "runtime_authorization": _record(runtime["path"]),
        "provision_execution_receipt": _record(
            runtime["provision_execution_path"]
        ),
        "required_episode_manifest": evidence["backlog"][
            "required_episode_manifest"
        ],
        **evidence,
        "semantic_retry_count": 0,
        "test_only": False,
        "promotable": True,
        "managed_chatgpt_auth_only": True,
        "production_context_mutated": True,
        "label_queue_mutated": False,
    }
    payload["receipt_sha256"] = _sha256_bytes(_canonical_json(payload).encode())
    return payload


def verify_live_episode_context_completion_receipt(
    receipt_path: Path,
    *,
    contract_path: Path,
    runtime_authorization_path: Path,
    queue: EpisodeContextQueueOperations,
) -> dict[str, Any]:
    loaded = load_episode_context_contract(contract_path)
    runtime = load_live_episode_context_runtime_authorization(
        runtime_authorization_path, loaded=loaded
    )
    path = receipt_path.expanduser().resolve()
    value = _load_json(path, label="live episode-context completion receipt")
    unhashed = dict(value)
    supplied = unhashed.pop("receipt_sha256", None)
    expected = build_live_episode_context_completion_receipt(
        root=path.parent, loaded=loaded, runtime=runtime, queue=queue
    )
    comparable_expected = dict(expected)
    comparable_expected["completed_at"] = value.get("completed_at")
    comparable_expected["receipt_sha256"] = supplied
    if (
        value.get("schema_version") != LIVE_COMPLETION_RECEIPT_VERSION
        or value.get("state") != "passed"
        or value.get("phase") != "episode_context"
        or value.get("database") != runtime["cutover"]["database"]
        or value.get("database") != runtime["provision_plan"]["database"]
        or value.get("database") != runtime["provision_execution"]["database"]
        or value.get("semantic_retry_count") != 0
        or value.get("test_only") is not False
        or value.get("promotable") is not True
        or value.get("managed_chatgpt_auth_only") is not True
        or value.get("production_context_mutated") is not True
        or value.get("label_queue_mutated") is not False
        or not _is_sha256(supplied)
        or supplied != _sha256_bytes(_canonical_json(unhashed).encode())
        or value != comparable_expected
    ):
        raise EpisodeContextRunnerError(
            "live episode-context completion receipt drifted"
        )
    return dict(value)


def _queue_main_database_identity(queue: Any) -> dict[str, Any]:
    connection = getattr(queue, "conn", None)
    if not isinstance(connection, sqlite3.Connection):
        raise EpisodeContextRunnerError("live context queue lacks SQLite authority")
    rows = connection.execute("PRAGMA database_list").fetchall()
    main_path = None
    for row in rows:
        name = row[1] if not isinstance(row, sqlite3.Row) else row["name"]
        if str(name) == "main":
            main_path = row[2] if not isinstance(row, sqlite3.Row) else row["file"]
            break
    if not isinstance(main_path, str) or not main_path:
        raise EpisodeContextRunnerError("live context queue database path is unavailable")
    return _database_identity(Path(main_path))


def _verify_live_client(client: Any, runtime: Mapping[str, Any]) -> None:
    if (
        getattr(client, "account_summary", None)
        != {
            "type": "chatgpt",
            "plan_type": "pro",
            "requires_openai_auth": False,
        }
        or getattr(client, "cli_version", None) != PINNED_CODEX_CLI_VERSION
        or getattr(client, "protocol_schema_sha256", None)
        != runtime["authorization"]["runtime_artifacts"]["protocol_schema"]["sha256"]
        or not isinstance(getattr(client, "app_server_user_agent", None), str)
        or not client.app_server_user_agent
    ):
        raise EpisodeContextRunnerWaiting(
            "official managed ChatGPT Pro app-server preflight failed"
        )


def _verify_live_thread(
    thread: Any,
    *,
    loaded: Mapping[str, Any],
    runtime: Mapping[str, Any],
) -> AppServerThread:
    sources = runtime["instruction_sources"]
    if (
        not isinstance(thread, AppServerThread)
        or not thread.thread_id
        or thread.model != LIVE_SEMANTIC_MODEL
        or thread.cwd != str(PROJECT_ROOT)
        or thread.ephemeral is not True
        or thread.base_instructions_sha256
        != _sha256_file(loaded["prompt_path"])
        or thread.base_instructions_bytes != loaded["prompt_path"].stat().st_size
        or thread.instruction_sources_sha256
        != sources["effective_instruction_sources_sha256"]
        or thread.instruction_sources_count
        != sources["effective_instruction_sources_count"]
    ):
        raise EpisodeContextRunnerWaiting(
            "live episode-context thread isolation lineage drifted"
        )
    return thread


async def run_live_episode_context_queue(
    *,
    contract_path: Path,
    runtime_authorization_path: Path,
    output_root: Path,
    queue: EpisodeContextQueueOperations,
    limit: int = 1,
    execute: bool = False,
    client_factory: Callable[[], Any] | None = None,
    capacity_provider_factory: Callable[
        [Any, Mapping[str, Any], Mapping[str, Any]],
        LiveEpisodeContextCapacityAdmissionProvider,
    ]
    | None = None,
    offline_test_mode: bool = False,
    _writer_lock_held: bool = False,
) -> dict[str, Any]:
    """Run a bounded live lane only under a separately frozen authorization."""

    loaded = load_episode_context_contract(contract_path)
    runtime = load_live_episode_context_runtime_authorization(
        runtime_authorization_path, loaded=loaded
    )
    _verify_live_runtime_files_now(loaded=loaded, runtime=runtime)
    root = output_root.expanduser().resolve()
    run_id = f"ectxlive_{uuid.uuid4().hex}"
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
        raise EpisodeContextRunnerError("live context episode limit must be positive")
    _reject_live_credential_environment()
    if not isinstance(offline_test_mode, bool):
        raise EpisodeContextRunnerError("offline test mode must be boolean")
    if offline_test_mode:
        if client_factory is None or capacity_provider_factory is None:
            raise EpisodeContextRunnerError(
                "offline live-path tests require injected client and capacity provider"
            )
    elif client_factory is not None or capacity_provider_factory is not None:
        raise EpisodeContextRunnerError(
            "live context execution rejects injected client or capacity provider"
        )
    queue_database = _queue_main_database_identity(queue)
    frozen_databases = (
        runtime["cutover"]["database"],
        runtime["provision_plan"]["database"],
        runtime["provision_execution"]["database"],
    )
    if offline_test_mode:
        if queue.fixture_only is not True or queue_database in frozen_databases:
            raise EpisodeContextRunnerError(
                "offline context tests require a distinct fixture database"
            )
    elif queue.fixture_only is not False or any(
        queue_database != database for database in frozen_databases
    ):
        raise EpisodeContextRunnerError("live context queue database identity drifted")
    if not execute:
        return _live_run_receipt(
            root,
            run_id=run_id,
            loaded=loaded,
            runtime=runtime,
            state="waiting",
            terminal_reason="explicit_live_execute_authorization_required",
            backlog=None,
            live_model_call_count=0,
            live_database_mutation_count=0,
            capacity_admission_count=0,
            recovered_count=0,
            usage={},
            wall_elapsed_seconds=0.0,
            offline_test_mode=offline_test_mode,
        )
    if not _writer_lock_held:
        with LiveContextWriterLock(queue_database, phase="run"):
            return await run_live_episode_context_queue(
                contract_path=contract_path,
                runtime_authorization_path=runtime_authorization_path,
                output_root=output_root,
                queue=queue,
                limit=limit,
                execute=True,
                client_factory=client_factory,
                capacity_provider_factory=capacity_provider_factory,
                offline_test_mode=offline_test_mode,
                _writer_lock_held=True,
            )
    queue.bind_execution_contract(loaded)
    required_episode_ids = queue.discover_required_episode_ids()
    if (
        required_episode_ids
        != runtime["provision_plan"]["required_episode_ids"]
    ):
        raise EpisodeContextRunnerError(
            "live context denominator differs from the frozen provision plan"
        )
    required_manifest, required_manifest_record = _freeze_required_manifest(
        root=root, loaded=loaded, episode_ids=required_episode_ids
    )
    queue.bind_required_episode_manifest(
        required_manifest, required_manifest_record
    )
    capacity_reconciliation = _reconcile_live_capacity_ownership(
        root=root,
        loaded=loaded,
        runtime=runtime,
        queue=queue,
    )
    recovered = sum(capacity_reconciliation.values())
    recovered += _recover_db_completed_unterminalized_turns(
        loaded=loaded, root=root, queue=queue
    )
    recovered += queue.recover_stale_unbound_claims(limit=limit)
    stale = queue.stale_claims(limit=limit)
    if stale:
        return _live_run_receipt(
            root,
            run_id=run_id,
            loaded=loaded,
            runtime=runtime,
            state="waiting",
            terminal_reason="ambiguous_stale_claim_preserved_without_replay",
            backlog=_normalize_backlog(queue.accounting()),
            live_model_call_count=0,
            live_database_mutation_count=0,
            capacity_admission_count=0,
            recovered_count=recovered,
            usage={},
            wall_elapsed_seconds=0.0,
            offline_test_mode=offline_test_mode,
        )
    initial_backlog = _normalize_backlog(queue.accounting())
    if initial_backlog["remaining_episode_context_jobs"] == 0:
        if offline_test_mode:
            return _live_run_receipt(
                root,
                run_id=run_id,
                loaded=loaded,
                runtime=runtime,
                state="waiting",
                terminal_reason="offline_test_only_cannot_mint_live_completion",
                backlog=initial_backlog,
                live_model_call_count=0,
                live_database_mutation_count=0,
                capacity_admission_count=0,
                recovered_count=recovered,
                usage={},
                wall_elapsed_seconds=0.0,
                offline_test_mode=True,
            )
        completion_path = root / "live-completion-receipt.json"
        if not completion_path.is_file():
            completion = build_live_episode_context_completion_receipt(
                root=root, loaded=loaded, runtime=runtime, queue=queue
            )
            _write_immutable(completion_path, _pretty_json(completion).encode())
        return verify_live_episode_context_completion_receipt(
            completion_path,
            contract_path=contract_path,
            runtime_authorization_path=runtime_authorization_path,
            queue=queue,
        )
    existing_terminals = _terminal_evidence(root, loaded)
    expected_execution_authority = (
        "offline_test_only" if offline_test_mode else "official_live"
    )
    if any(
        terminal.get("execution_authority") != expected_execution_authority
        for terminal in existing_terminals
    ):
        raise EpisodeContextRunnerError(
            "live context root mixes incompatible execution authority"
        )
    usage = Counter()
    wall = 0.0
    seen_thread_ids = {str(item["thread_id"]) for item in existing_terminals}
    seen_turn_ids = {str(item["turn_id"]) for item in existing_terminals}
    for terminal in existing_terminals:
        for field in USAGE_FIELDS:
            usage[field] += int(terminal["usage"][field])
        wall += float(terminal["wall_elapsed_seconds"])
    call_count = 0
    mutation_count = 0
    capacity_count = 0
    capacity_denial_count = 0
    factory = (
        client_factory
        if offline_test_mode
        else lambda: _managed_episode_context_client(loaded=loaded, runtime=runtime)
    )
    try:
        async with factory() as client:
            _verify_live_client(client, runtime)
            provider = (
                capacity_provider_factory(
                    client,
                    runtime["capacity_policy"],
                    runtime["authorization"]["capacity_policy"],
                )
                if offline_test_mode
                else ManagedEpisodeContextReserveCapacityProvider(
                    client=client,
                    policy=runtime["capacity_policy"],
                    policy_record=runtime["authorization"]["capacity_policy"],
                )
            )
            if getattr(provider, "fixture_only", None) is not False:
                raise EpisodeContextRunnerError(
                    "live context capacity provider is not production-scoped"
                )
            for ordinal in range(1, limit + 1):
                backlog = _normalize_backlog(queue.accounting())
                if backlog["remaining_episode_context_jobs"] == 0:
                    break
                preview = queue.preview(limit=1)
                if len(preview) != 1:
                    raise EpisodeContextRunnerWaiting(
                        "live context backlog has no exactly claimable episode"
                    )
                preview_item = preview[0]
                _validate_preview_item(preview_item)
                preclaim_request = _live_capacity_request_payload(
                    loaded=loaded,
                    run_id=run_id,
                    ordinal=ordinal,
                    admission_stage="preclaim",
                    item=preview_item,
                    backlog=backlog,
                    required_episode_manifest=required_manifest_record,
                    provision_plan_record=runtime["authorization"]["provision_plan"],
                    capacity_policy_record=runtime["authorization"]["capacity_policy"],
                    runtime_authorization_record=_record(runtime["path"]),
                    policy=runtime["capacity_policy"],
                    cumulative_measured_total_tokens=int(usage["total_tokens"]),
                )
                try:
                    _preclaim, preclaim_binding = await _persist_live_capacity_admission(
                        root=root,
                        loaded=loaded,
                        request=preclaim_request,
                        policy=runtime["capacity_policy"],
                        provision_plan_record=runtime["authorization"]["provision_plan"],
                        capacity_policy_record=runtime["authorization"]["capacity_policy"],
                        runtime_authorization_record=_record(runtime["path"]),
                        provider=provider,
                    )
                except LiveCapacityAdmissionDenied:
                    capacity_denial_count += 1
                    raise
                capacity_count += 1
                claimed = queue.claim(limit=1)
                if (
                    len(claimed) != 1
                    or claimed[0].job_id != preview_item.job_id
                    or claimed[0].episode_id != preview_item.episode_id
                ):
                    if len(claimed) == 1:
                        queue.release_prelaunch_claim(
                            claimed[0], reason="capacity_bound_preview_claim_drift"
                        )
                    raise EpisodeContextRunnerWaiting(
                        "live claimed episode differs from capacity admission"
                    )
                item = claimed[0]
                _validate_item(item)
                queue.bind_prelaunch_capacity(
                    item,
                    {"preclaim": preclaim_binding},
                )
                paths = _turn_paths(root, item)
                if any(path.exists() for path in paths.values() if path != paths["root"]):
                    raise EpisodeContextRunnerWaiting(
                        "live episode-context attempt artifacts already exist"
                    )
                input_payload = _item_input(item)
                prompt = _canonical_json(input_payload)
                postclaim_backlog = _normalize_backlog(queue.accounting())
                preturn_request = _live_capacity_request_payload(
                    loaded=loaded,
                    run_id=run_id,
                    ordinal=ordinal,
                    admission_stage="preturn",
                    item=item,
                    backlog=postclaim_backlog,
                    required_episode_manifest=required_manifest_record,
                    provision_plan_record=runtime["authorization"]["provision_plan"],
                    capacity_policy_record=runtime["authorization"]["capacity_policy"],
                    runtime_authorization_record=_record(runtime["path"]),
                    policy=runtime["capacity_policy"],
                    cumulative_measured_total_tokens=int(usage["total_tokens"]),
                )
                try:
                    _preturn, preturn_binding = await _persist_live_capacity_admission(
                        root=root,
                        loaded=loaded,
                        request=preturn_request,
                        policy=runtime["capacity_policy"],
                        provision_plan_record=runtime["authorization"]["provision_plan"],
                        capacity_policy_record=runtime["authorization"]["capacity_policy"],
                        runtime_authorization_record=_record(runtime["path"]),
                        provider=provider,
                    )
                    capacity_count += 1
                    queue.bind_prelaunch_capacity(
                        item,
                        {
                            "preclaim": preclaim_binding,
                            "preturn": preturn_binding,
                        },
                    )
                except LiveCapacityAdmissionDenied as exc:
                    capacity_denial_count += 1
                    queue.bind_prelaunch_capacity(
                        item,
                        {
                            "preclaim": preclaim_binding,
                            "preturn": exc.binding,
                        },
                    )
                    queue.release_prelaunch_claim(
                        item, reason="fresh_preturn_capacity_not_admitted"
                    )
                    raise
                except BaseException:
                    _reconcile_live_capacity_ownership(
                        root=root,
                        loaded=loaded,
                        runtime=runtime,
                        queue=queue,
                    )
                    raise
                base_instructions = loaded["prompt_path"].read_text(encoding="utf-8")
                _verify_live_runtime_files_now(loaded=loaded, runtime=runtime)
                try:
                    queue.reverify_source(item)
                except (EpisodeContextRunnerError, SourceIntegrityError):
                    queue.release_prelaunch_claim(
                        item,
                        reason="source_integrity_drift_before_thread_start",
                    )
                    raise
                thread = _verify_live_thread(
                    await client.start_thread(
                        model=LIVE_SEMANTIC_MODEL,
                        base_instructions=base_instructions,
                        cwd=PROJECT_ROOT,
                        ephemeral=True,
                    ),
                    loaded=loaded,
                    runtime=runtime,
                )
                if thread.thread_id in seen_thread_ids:
                    raise EpisodeContextRunnerWaiting(
                        "live app-server returned a duplicate episode thread id"
                    )
                seen_thread_ids.add(thread.thread_id)
                _verify_live_runtime_files_now(loaded=loaded, runtime=runtime)
                try:
                    queue.reverify_source(item)
                except (EpisodeContextRunnerError, SourceIntegrityError):
                    queue.release_prelaunch_claim(
                        item,
                        reason="source_integrity_drift_before_turn_start",
                    )
                    raise
                _write_immutable(paths["input"], prompt.encode())
                launch = _launch_for_item(
                    loaded=loaded,
                    run_id=run_id,
                    lifecycle_id=f"ectxlife_{uuid.uuid4().hex}",
                    item=item,
                    thread=thread,
                    input_record=_record(paths["input"]),
                    required_episode_manifest=required_manifest_record,
                    capacity_binding={
                        "preclaim": preclaim_binding,
                        "preturn": preturn_binding,
                    },
                    live_runtime_authorization=_record(runtime["path"]),
                    live_execution_mode=expected_execution_authority,
                )
                _write_immutable(paths["launch"], _pretty_json(launch).encode())
                queue.bind_launch(item, paths["launch"])
                call_count += 1
                await client.run_structured_turn(
                    thread=thread,
                    effort=LIVE_REASONING_EFFORT,
                    prompt=prompt,
                    output_schema=loaded["schema"],
                    sidecar_path=paths["sidecar"],
                    output_path=paths["output"],
                    batch_size=1,
                    thread_mode=THREAD_MODE,
                    timeout_seconds=float(loaded["configuration"]["timeout_seconds"]),
                )
                sidecar, output = _validate_completed_turn(
                    sidecar_path=paths["sidecar"],
                    output_path=paths["output"],
                    thread_id=thread.thread_id,
                    model=LIVE_SEMANTIC_MODEL,
                    effort=LIVE_REASONING_EFFORT,
                    prompt=prompt,
                    base_instructions=base_instructions,
                    schema=loaded["schema"],
                    instruction_sources_sha256=runtime["instruction_sources"][
                        "effective_instruction_sources_sha256"
                    ],
                    instruction_sources_count=runtime["instruction_sources"][
                        "effective_instruction_sources_count"
                    ],
                    protocol_schema_sha256=runtime["authorization"][
                        "runtime_artifacts"
                    ]["protocol_schema"]["sha256"],
                    require_official_live_lineage=True,
                )
                if (
                    output["episode_id"] != item.episode_id
                    or sidecar["turn_id"] in seen_turn_ids
                    or int(sidecar["usage"]["total_tokens"])
                    > runtime["capacity_policy"]["maximum_total_tokens_per_episode"]
                ):
                    raise EpisodeContextRunnerWaiting(
                        "live episode-context completed turn contract failed"
                    )
                seen_turn_ids.add(str(sidecar["turn_id"]))
                submission = queue.submit(item, paths["output"])
                mutation_count += int(submission.get("production_mutated") is True)
                terminal = _terminal_for_turn(
                    loaded=loaded,
                    launch_path=paths["launch"],
                    input_path=paths["input"],
                    sidecar_path=paths["sidecar"],
                    output_path=paths["output"],
                    sidecar=sidecar,
                    submission=submission,
                    recovered=False,
                )
                _write_immutable(paths["terminal"], _pretty_json(terminal).encode())
                queue.bind_terminal(item, paths["terminal"])
                for field in USAGE_FIELDS:
                    usage[field] += int(sidecar["usage"][field])
                wall += float(sidecar["wall_elapsed_seconds"])
    except LiveCapacityAdmissionDenied as exc:
        return _live_capacity_wait_checkpoint(
            root,
            run_id=run_id,
            loaded=loaded,
            runtime=runtime,
            backlog=_normalize_backlog(queue.accounting()),
            denial=exc,
            admitted_capacity_count=capacity_count,
            live_model_call_count=call_count,
            live_database_mutation_count=mutation_count,
            recovered_count=recovered,
            usage=usage,
            wall_elapsed_seconds=wall,
            offline_test_mode=offline_test_mode,
        )
    except (
        AppServerError,
        EpisodeContextRunnerError,
        OSError,
        sqlite3.Error,
        ValueError,
    ) as exc:
        return _live_run_receipt(
            root,
            run_id=run_id,
            loaded=loaded,
            runtime=runtime,
            state="waiting",
            terminal_reason="managed_live_context_attempt_waiting_without_retry",
            backlog=_normalize_backlog(queue.accounting()),
            live_model_call_count=call_count,
            live_database_mutation_count=mutation_count,
            capacity_admission_count=capacity_count,
            recovered_count=recovered,
            usage=usage,
            wall_elapsed_seconds=wall,
            capacity_denial_count=capacity_denial_count,
            error=exc,
            offline_test_mode=offline_test_mode,
        )
    backlog = _normalize_backlog(queue.accounting())
    if backlog["remaining_episode_context_jobs"] == 0:
        if offline_test_mode:
            return _live_run_receipt(
                root,
                run_id=run_id,
                loaded=loaded,
                runtime=runtime,
                state="waiting",
                terminal_reason="offline_test_only_cannot_mint_live_completion",
                backlog=backlog,
                live_model_call_count=call_count,
                live_database_mutation_count=mutation_count,
                capacity_admission_count=capacity_count,
                recovered_count=recovered,
                usage=usage,
                wall_elapsed_seconds=wall,
                capacity_denial_count=capacity_denial_count,
                offline_test_mode=True,
            )
        completion_path = root / "live-completion-receipt.json"
        completion = build_live_episode_context_completion_receipt(
            root=root, loaded=loaded, runtime=runtime, queue=queue
        )
        _write_immutable(completion_path, _pretty_json(completion).encode())
        return verify_live_episode_context_completion_receipt(
            completion_path,
            contract_path=contract_path,
            runtime_authorization_path=runtime_authorization_path,
            queue=queue,
        )
    return _live_run_receipt(
        root,
        run_id=run_id,
        loaded=loaded,
        runtime=runtime,
        state="passed",
        terminal_reason="bounded_managed_context_batch_completed_more_work_remains",
        backlog=backlog,
        live_model_call_count=call_count,
        live_database_mutation_count=mutation_count,
        capacity_admission_count=capacity_count,
        recovered_count=recovered,
        usage=usage,
        wall_elapsed_seconds=wall,
        capacity_denial_count=capacity_denial_count,
        offline_test_mode=offline_test_mode,
    )


async def run_episode_context_queue(
    *,
    contract_path: Path,
    output_root: Path,
    queue: EpisodeContextQueueOperations,
    limit: int | None = None,
    dry_run: bool = True,
    fixture_mode: bool = False,
    fixture_client_factory: Callable[[Path], Any] | None = None,
    fixture_capacity_provider: EpisodeContextCapacityAdmissionProvider | None = None,
) -> dict[str, Any]:
    loaded = load_episode_context_contract(contract_path)
    root = output_root.expanduser().resolve()
    run_id = f"ectxrun_{uuid.uuid4().hex}"
    configured_limit = int(loaded["configuration"]["batch_size"])
    bounded_limit = configured_limit if limit is None else min(configured_limit, int(limit))
    if bounded_limit < 1:
        raise EpisodeContextRunnerError("episode-context limit must be positive")
    if (
        not dry_run
        and (
        not fixture_mode
        or queue.fixture_only is not True
        or fixture_client_factory is None
        or getattr(fixture_client_factory, "fixture_only", False) is not True
        )
    ):
        return _run_receipt(
            root,
            run_id=run_id,
            loaded=loaded,
            state="waiting",
            terminal_reason="live_episode_context_execution_not_implemented",
            backlog=None,
            fixture_turn_count=0,
            recovered_count=0,
            ambiguous_count=0,
        )
    _reject_live_credential_environment()

    queue.bind_execution_contract(loaded)
    required_episode_ids = queue.discover_required_episode_ids()
    required_manifest, required_manifest_record = _freeze_required_manifest(
        root=root, loaded=loaded, episode_ids=required_episode_ids
    )
    queue.bind_required_episode_manifest(
        required_manifest, required_manifest_record
    )
    if dry_run:
        preview = queue.preview(limit=bounded_limit)
        backlog = _normalize_backlog(queue.accounting())
        return _run_receipt(
            root,
            run_id=run_id,
            loaded=loaded,
            state="dry_run",
            terminal_reason="fixture_execution_not_requested",
            backlog=backlog,
            fixture_turn_count=0,
            recovered_count=0,
            ambiguous_count=0,
        ) | {"eligible_episode_context_jobs": len(preview)}

    recovered = _recover_db_completed_unterminalized_turns(
        loaded=loaded,
        root=root,
        queue=queue,
    )
    recovered += queue.recover_stale_unbound_claims(limit=bounded_limit)
    stale = queue.stale_claims(limit=bounded_limit)
    ambiguous = 0
    for item in stale:
        _validate_item(item)
        try:
            terminal = _recover_stale_item(
                loaded=loaded, root=root, queue=queue, item=item
            )
        except (EpisodeContextRunnerError, OSError, sqlite3.Error, ValueError):
            terminal = None
        if terminal is None:
            ambiguous += 1
        else:
            recovered += 1
    if ambiguous:
        backlog = _normalize_backlog(queue.accounting())
        return _run_receipt(
            root,
            run_id=run_id,
            loaded=loaded,
            state="waiting",
            terminal_reason="ambiguous_stale_claim_preserved_without_replay",
            backlog=backlog,
            fixture_turn_count=0,
            recovered_count=recovered,
            ambiguous_count=ambiguous,
        )

    backlog = _normalize_backlog(queue.accounting())
    if backlog["remaining_episode_context_jobs"] == 0:
        completion_path = root / "completion-receipt.json"
        try:
            payload = _completion_payload(root=root, loaded=loaded, backlog=backlog)
            _write_immutable(completion_path, _pretty_json(payload).encode())
            verify_episode_context_completion_receipt(
                completion_path,
                expected_contract_sha256=loaded["sha256"],
                expected_evaluation_receipt=loaded["contract"]["lineage"]["evaluation_receipt"],
                expected_holdout_receipt=loaded["contract"]["lineage"]["holdout_receipt"],
            )
            return payload
        except (EpisodeContextRunnerError, OSError, ValueError) as exc:
            return _run_receipt(
                root,
                run_id=run_id,
                loaded=loaded,
                state="waiting",
                terminal_reason="zero_backlog_lacks_exact_managed_attempt_evidence",
                backlog=backlog,
                fixture_turn_count=0,
                recovered_count=recovered,
                ambiguous_count=0,
                error=exc,
            )

    preview = queue.preview(limit=bounded_limit)
    for item in preview:
        _validate_preview_item(item)
    preview_identities = [(item.job_id, item.episode_id) for item in preview]
    if len(preview_identities) != len(set(preview_identities)):
        raise EpisodeContextRunnerWaiting("duplicate episode-context preview identity")
    if not preview:
        return _run_receipt(
            root,
            run_id=run_id,
            loaded=loaded,
            state="waiting",
            terminal_reason="no_claimable_episode_context_jobs_with_nonzero_backlog",
            backlog=backlog,
            fixture_turn_count=0,
            recovered_count=recovered,
            ambiguous_count=0,
        )
    if (
        fixture_capacity_provider is None
        or getattr(fixture_capacity_provider, "fixture_only", False) is not True
    ):
        return _run_receipt(
            root,
            run_id=run_id,
            loaded=loaded,
            state="waiting",
            terminal_reason="capacity_admission_required_before_claim",
            backlog=backlog,
            fixture_turn_count=0,
            recovered_count=recovered,
            ambiguous_count=0,
        )
    capacity_request = _capacity_request_payload(
        loaded=loaded,
        run_id=run_id,
        backlog=backlog,
        required_episode_manifest=required_manifest_record,
        items=preview,
    )
    try:
        _capacity_admission, capacity_binding = _persist_capacity_admission(
            root=root,
            loaded=loaded,
            request=capacity_request,
            provider=fixture_capacity_provider,
        )
    except (EpisodeContextRunnerError, OSError, ValueError, TypeError) as exc:
        return _run_receipt(
            root,
            run_id=run_id,
            loaded=loaded,
            state="waiting",
            terminal_reason="capacity_admission_failed_before_claim",
            backlog=backlog,
            fixture_turn_count=0,
            recovered_count=recovered,
            ambiguous_count=0,
            error=exc,
        )

    items = queue.claim(limit=len(preview))
    if [(item.job_id, item.episode_id) for item in items] != preview_identities:
        return _run_receipt(
            root,
            run_id=run_id,
            loaded=loaded,
            state="waiting",
            terminal_reason="claimed_jobs_differ_from_capacity_admission",
            backlog=_normalize_backlog(queue.accounting()),
            fixture_turn_count=0,
            recovered_count=recovered,
            ambiguous_count=0,
        )
    seen = {field: set() for field in ("job_id", "episode_id", "context_run_id", "claim_id", "attempt_id")}
    for item in items:
        _validate_item(item)
        for field in seen:
            value = getattr(item, field)
            if value in seen[field]:
                raise EpisodeContextRunnerWaiting(f"duplicate episode-context {field}")
            seen[field].add(value)
    base_instructions = loaded["prompt_path"].read_text(encoding="utf-8")
    configuration = loaded["configuration"]
    fixture_turn_count = 0
    seen_thread_ids: set[str] = set()
    seen_turn_ids: set[str] = set()
    try:
        async with fixture_client_factory(loaded["artifact_paths"]["codex_binary"]) as client:
            if getattr(client, "fixture_only", False) is not True:
                raise EpisodeContextRunnerError("fixture client marker is absent")
            account = getattr(client, "account_summary", None)
            if account != {"type": "chatgpt", "plan_type": "pro"}:
                raise EpisodeContextRunnerWaiting("managed ChatGPT Pro auth verification failed")
            for item in items:
                paths = _turn_paths(root, item)
                if any(paths[name].exists() for name in ("input", "launch", "sidecar", "output", "terminal")):
                    raise EpisodeContextRunnerWaiting("episode-context attempt artifacts already exist")
                input_payload = _item_input(item)
                prompt = _canonical_json(input_payload)
                _write_immutable(paths["input"], prompt.encode())
                thread: AppServerThread = await client.start_thread(
                    model=str(configuration["model"]),
                    base_instructions=base_instructions,
                    cwd=Path.cwd(),
                    ephemeral=True,
                )
                if not thread.thread_id or thread.thread_id in seen_thread_ids:
                    raise EpisodeContextRunnerWaiting("app-server returned a duplicate episode thread id")
                seen_thread_ids.add(thread.thread_id)
                lifecycle_id = f"ectxlife_{uuid.uuid4().hex}"
                launch = _launch_for_item(
                    loaded=loaded,
                    run_id=run_id,
                    lifecycle_id=lifecycle_id,
                    item=item,
                    thread=thread,
                    input_record=_record(paths["input"]),
                    required_episode_manifest=required_manifest_record,
                    capacity_binding=capacity_binding,
                )
                _write_immutable(paths["launch"], _pretty_json(launch).encode())
                queue.bind_launch(item, paths["launch"])
                fixture_turn_count += 1
                await client.run_structured_turn(
                    thread=thread,
                    effort=str(configuration["reasoning_effort"]),
                    prompt=prompt,
                    output_schema=loaded["schema"],
                    sidecar_path=paths["sidecar"],
                    output_path=paths["output"],
                    batch_size=1,
                    thread_mode=THREAD_MODE,
                    timeout_seconds=float(configuration["timeout_seconds"]),
                )
                sidecar, output = _validate_completed_turn(
                    sidecar_path=paths["sidecar"],
                    output_path=paths["output"],
                    thread_id=thread.thread_id,
                    model=str(configuration["model"]),
                    effort=str(configuration["reasoning_effort"]),
                    prompt=prompt,
                    base_instructions=base_instructions,
                    schema=loaded["schema"],
                )
                if output.get("episode_id") != item.episode_id:
                    raise EpisodeContextRunnerWaiting("episode-context output episode drifted")
                if sidecar["turn_id"] in seen_turn_ids:
                    raise EpisodeContextRunnerWaiting("app-server returned a duplicate turn id")
                seen_turn_ids.add(sidecar["turn_id"])
                submission = queue.submit(item, paths["output"])
                terminal = _terminal_for_turn(
                    loaded=loaded,
                    launch_path=paths["launch"],
                    input_path=paths["input"],
                    sidecar_path=paths["sidecar"],
                    output_path=paths["output"],
                    sidecar=sidecar,
                    submission=submission,
                    recovered=False,
                )
                _write_immutable(paths["terminal"], _pretty_json(terminal).encode())
                queue.bind_terminal(item, paths["terminal"])
    except (AppServerError, EpisodeContextRunnerError, OSError, sqlite3.Error, ValueError) as exc:
        backlog = _normalize_backlog(queue.accounting())
        return _run_receipt(
            root,
            run_id=run_id,
            loaded=loaded,
            state="waiting",
            terminal_reason="fixture_turn_failure_preserved_without_replay",
            backlog=backlog,
            fixture_turn_count=fixture_turn_count,
            recovered_count=recovered,
            ambiguous_count=0,
            error=exc,
        )

    backlog = _normalize_backlog(queue.accounting())
    if backlog["remaining_episode_context_jobs"] != 0:
        return _run_receipt(
            root,
            run_id=run_id,
            loaded=loaded,
            state="passed",
            terminal_reason="bounded_fixture_batch_completed_more_context_work_remains",
            backlog=backlog,
            fixture_turn_count=fixture_turn_count,
            recovered_count=recovered,
            ambiguous_count=0,
        )
    completion_path = root / "completion-receipt.json"
    try:
        payload = _completion_payload(root=root, loaded=loaded, backlog=backlog)
        _write_immutable(completion_path, _pretty_json(payload).encode())
        verify_episode_context_completion_receipt(
            completion_path,
            expected_contract_sha256=loaded["sha256"],
            expected_evaluation_receipt=loaded["contract"]["lineage"]["evaluation_receipt"],
            expected_holdout_receipt=loaded["contract"]["lineage"]["holdout_receipt"],
        )
        return payload
    except (EpisodeContextRunnerError, OSError, ValueError) as exc:
        return _run_receipt(
            root,
            run_id=run_id,
            loaded=loaded,
            state="waiting",
            terminal_reason="zero_backlog_lacks_exact_managed_attempt_evidence",
            backlog=backlog,
            fixture_turn_count=fixture_turn_count,
            recovered_count=recovered,
            ambiguous_count=0,
            error=exc,
        )


def _open_authorized_live_connection(
    database_path: Path, *, read_only: bool
) -> sqlite3.Connection:
    if read_only:
        return _open_read_only_sqlite(database_path)
    source = database_path.expanduser().resolve()
    connection = sqlite3.connect(str(source), timeout=30.0)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    if int(connection.execute("PRAGMA foreign_keys").fetchone()[0]) != 1:
        connection.close()
        raise EpisodeContextRunnerError(
            "live context SQLite foreign-key enforcement is unavailable"
        )
    return connection


def build_live_context_plan_artifacts(
    *, database_path: Path, output_root: Path
) -> dict[str, Any]:
    """Write only read-only planning evidence; never migrate or provision."""

    root = output_root.expanduser().resolve()
    migration_path = root / "attempt-schema-migration-plan.json"
    provision_path = root / "live-context-provision-plan.json"
    cutover_path = root / "attempt-schema-cutover-before.json"
    migration = build_attempt_schema_migration_plan(database_path)
    _write_immutable(migration_path, _pretty_json(migration).encode())
    provision = build_live_context_provision_plan(database_path)
    _write_immutable(provision_path, _pretty_json(provision).encode())
    cutover = build_attempt_schema_cutover_receipt(
        database_path, migration_plan_path=migration_path
    )
    _write_immutable(cutover_path, _pretty_json(cutover).encode())
    return {
        "state": "planned_read_only",
        "database": _database_identity(database_path),
        "migration_plan": _record(migration_path),
        "cutover_receipt": _record(cutover_path),
        "provision_plan": _record(provision_path),
        "production_mutated": False,
        "semantic_model_call_count": 0,
    }


def verify_live_episode_context_completion_from_database(
    *,
    contract_path: Path,
    runtime_authorization_path: Path,
    database_path: Path,
    output_root: Path,
) -> dict[str, Any]:
    loaded = load_episode_context_contract(contract_path)
    runtime = load_live_episode_context_runtime_authorization(
        runtime_authorization_path, loaded=loaded
    )
    database_identity = _database_identity(database_path)
    if any(
        database_identity != item
        for item in (
            runtime["cutover"]["database"],
            runtime["provision_plan"]["database"],
            runtime["provision_execution"]["database"],
        )
    ):
        raise EpisodeContextRunnerError(
            "live completion database differs from frozen lineage"
        )
    with LiveContextWriterLock(database_identity, phase="verify_completion"):
        connection = _open_authorized_live_connection(database_path, read_only=True)
        try:
            queue = SQLiteEpisodeContextQueue(
                connection,
                lane=LIVE_LANE,
                worker_id="managed-context-completion-verifier",
                label_pack=LIVE_LABEL_PACK,
                model=LIVE_QUEUE_PAYLOAD_MODEL,
                attempt_root=output_root / "managed-context-attempts",
            )
            queue.bind_execution_contract(loaded)
            manifest_record = runtime["provision_execution"][
                "required_episode_manifest"
            ]
            manifest_path = _verify_record(
                manifest_record, label="live provision required manifest"
            )
            manifest = _validate_required_manifest(
                _load_json(manifest_path, label="live required manifest"),
                loaded=loaded,
            )
            queue.bind_required_episode_manifest(manifest, manifest_record)
            return verify_live_episode_context_completion_receipt(
                output_root / "live-completion-receipt.json",
                contract_path=contract_path,
                runtime_authorization_path=runtime_authorization_path,
                queue=queue,
            )
        finally:
            connection.close()


def _build_cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python3 -m research_factory.app_server_episode_context_runner"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    plan = subparsers.add_parser("plan")
    plan.add_argument("--database", type=Path, required=True)
    plan.add_argument("--output-root", type=Path, required=True)

    provision_authorization = subparsers.add_parser("authorize-provision")
    provision_authorization.add_argument("--contract", type=Path, required=True)
    provision_authorization.add_argument(
        "--migration-plan", type=Path, required=True
    )
    provision_authorization.add_argument(
        "--cutover-receipt", type=Path, required=True
    )
    provision_authorization.add_argument(
        "--provision-plan", type=Path, required=True
    )
    provision_authorization.add_argument("--output", type=Path, required=True)
    provision_authorization.add_argument(
        "--operator-authorization-id", required=True
    )
    provision_authorization.add_argument("--authorized-by", required=True)

    migrate = subparsers.add_parser("migrate-provision")
    migrate.add_argument("--database", type=Path, required=True)
    migrate.add_argument("--contract", type=Path, required=True)
    migrate.add_argument("--migration-plan", type=Path, required=True)
    migrate.add_argument("--cutover-receipt", type=Path, required=True)
    migrate.add_argument("--provision-plan", type=Path, required=True)
    migrate.add_argument("--authorization", type=Path, required=True)
    migrate.add_argument("--output-root", type=Path, required=True)
    migrate.add_argument("--execute", action="store_true")

    runtime_authorization = subparsers.add_parser("authorize-runtime")
    runtime_authorization.add_argument("--contract", type=Path, required=True)
    runtime_authorization.add_argument(
        "--cutover-receipt", type=Path, required=True
    )
    runtime_authorization.add_argument(
        "--provision-plan", type=Path, required=True
    )
    runtime_authorization.add_argument(
        "--provision-execution", type=Path, required=True
    )
    runtime_authorization.add_argument(
        "--capacity-policy", type=Path, required=True
    )
    runtime_authorization.add_argument(
        "--context-control-overlay", type=Path, required=True
    )
    runtime_authorization.add_argument(
        "--instruction-source-contract", type=Path, required=True
    )
    runtime_authorization.add_argument("--output", type=Path, required=True)
    runtime_authorization.add_argument(
        "--operator-authorization-id", required=True
    )
    runtime_authorization.add_argument("--authorized-by", required=True)

    run = subparsers.add_parser("run")
    run.add_argument("--database", type=Path, required=True)
    run.add_argument("--contract", type=Path, required=True)
    run.add_argument("--runtime-authorization", type=Path, required=True)
    run.add_argument("--output-root", type=Path, required=True)
    run.add_argument("--limit", type=int, required=True)
    run.add_argument("--execute", action="store_true")

    completion = subparsers.add_parser("verify-completion")
    completion.add_argument("--database", type=Path, required=True)
    completion.add_argument("--contract", type=Path, required=True)
    completion.add_argument("--runtime-authorization", type=Path, required=True)
    completion.add_argument("--output-root", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_cli_parser().parse_args(list(argv) if argv is not None else None)
    _reject_live_credential_environment()
    if args.command == "plan":
        result = build_live_context_plan_artifacts(
            database_path=args.database, output_root=args.output_root
        )
    elif args.command == "authorize-provision":
        result = build_live_context_provision_authorization_artifact(
            contract_path=args.contract,
            migration_plan_path=args.migration_plan,
            cutover_receipt_path=args.cutover_receipt,
            provision_plan_path=args.provision_plan,
            output_path=args.output,
            operator_authorization_id=args.operator_authorization_id,
            authorized_by=args.authorized_by,
        )
    elif args.command == "migrate-provision":
        result = execute_live_context_cutover_and_provision(
            contract_path=args.contract,
            database_path=args.database,
            migration_plan_path=args.migration_plan,
            cutover_receipt_path=args.cutover_receipt,
            provision_plan_path=args.provision_plan,
            authorization_path=args.authorization,
            output_root=args.output_root,
            execute=bool(args.execute),
        )
    elif args.command == "run":
        connection = _open_authorized_live_connection(
            args.database, read_only=not bool(args.execute)
        )
        try:
            queue = SQLiteEpisodeContextQueue(
                connection,
                lane=LIVE_LANE,
                worker_id=f"managed-context-live-{os.getpid()}",
                label_pack=LIVE_LABEL_PACK,
                model=LIVE_QUEUE_PAYLOAD_MODEL,
                attempt_root=args.output_root / "managed-context-attempts",
            )
            result = asyncio.run(
                run_live_episode_context_queue(
                    contract_path=args.contract,
                    runtime_authorization_path=args.runtime_authorization,
                    output_root=args.output_root,
                    queue=queue,
                    limit=args.limit,
                    execute=bool(args.execute),
                )
            )
        finally:
            connection.close()
    elif args.command == "authorize-runtime":
        result = build_live_episode_context_runtime_authorization_artifact(
            contract_path=args.contract,
            cutover_receipt_path=args.cutover_receipt,
            provision_plan_path=args.provision_plan,
            provision_execution_receipt_path=args.provision_execution,
            capacity_policy_path=args.capacity_policy,
            context_control_overlay_path=args.context_control_overlay,
            instruction_source_contract_path=args.instruction_source_contract,
            output_path=args.output,
            operator_authorization_id=args.operator_authorization_id,
            authorized_by=args.authorized_by,
        )
    else:
        result = verify_live_episode_context_completion_from_database(
            contract_path=args.contract,
            runtime_authorization_path=args.runtime_authorization,
            database_path=args.database,
            output_root=args.output_root,
        )
    print(_pretty_json(result), end="")
    return 75 if result.get("nonterminal_capacity_wait") is True else 0


__all__ = [
    "CAPACITY_ADMISSION_CONTRACT",
    "CAPACITY_ADMISSION_VERSION",
    "COMPLETION_RECEIPT_VERSION",
    "CONTRACT_VERSION",
    "FROZEN_CONFIGURATION_VERSION",
    "HOLDOUT_AUTHORIZATION_VERSION",
    "PINNED_CLI_VERSION",
    "SEMANTIC_AUTHORITY_FIELDS",
    "THREAD_MODE",
    "TRANSPORT",
    "EpisodeContextItem",
    "EpisodeContextRunnerError",
    "EpisodeContextRunnerWaiting",
    "LiveContextWriterLock",
    "ManagedEpisodeContextCodexAppServerClient",
    "ManagedEpisodeContextReserveCapacityProvider",
    "SQLiteEpisodeContextQueue",
    "build_attempt_schema_cutover_receipt",
    "build_attempt_schema_migration_plan",
    "build_backlog_snapshot",
    "build_fixture_capacity_admission",
    "build_live_context_plan_artifacts",
    "build_live_context_provision_authorization_artifact",
    "build_live_context_provision_plan",
    "build_live_episode_context_completion_receipt",
    "build_live_episode_context_runtime_authorization_artifact",
    "execute_live_context_cutover_and_provision",
    "load_episode_context_contract",
    "load_live_context_provision_authorization",
    "load_live_episode_context_runtime_authorization",
    "run_episode_context_queue",
    "run_live_episode_context_queue",
    "verify_attempt_schema_cutover_receipt",
    "verify_attempt_schema_migration_plan",
    "verify_episode_context_artifact_for_episode",
    "verify_episode_context_completion_receipt",
    "verify_live_context_provision_execution_receipt",
    "verify_live_context_provision_plan",
    "verify_live_episode_context_completion_from_database",
    "verify_live_episode_context_completion_receipt",
]


if __name__ == "__main__":
    raise SystemExit(main())
