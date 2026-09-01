"""Leased work dispatch for the Signal Desk clean-corpus rebuild.

The dispatcher deliberately separates a stable semantic task from its execution
attempts.  Lease expiry is an infrastructure event and therefore re-leases the
same attempt.  A semantic failure is terminal until an operator explicitly
creates a new attempt through :func:`resurrect_task`.

All mutating operations use ``BEGIN IMMEDIATE`` and lease generations as fencing
tokens.  Callers must not enter these functions with an open transaction.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterator, Mapping, Optional


SCHEMA_VERSION = 1
TASK_STATUSES = ("pending", "running", "succeeded", "terminal_failed")
ATTEMPT_STATUSES = ("pending", "running", "succeeded", "terminal_failed")


class DispatchError(RuntimeError):
    """Base class for dispatch invariant violations."""


class TaskDefinitionConflict(DispatchError):
    """A task key was reused for different semantic work."""


class InvalidTransition(DispatchError):
    """The requested task or attempt transition is not permitted."""


class LostLease(DispatchError):
    """The caller no longer owns the attempt lease."""


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _as_utc(value: Optional[datetime]) -> datetime:
    value = value or _utc_now()
    if value.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)


def _timestamp(value: Optional[datetime] = None) -> str:
    return _as_utc(value).isoformat(timespec="microseconds")


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _payload_hash(payload_json: str) -> str:
    return hashlib.sha256(payload_json.encode("utf-8")).hexdigest()


@contextmanager
def _write_transaction(conn: sqlite3.Connection) -> Iterator[None]:
    if conn.in_transaction:
        raise DispatchError("dispatch mutation requires no active transaction")
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield
    except BaseException:
        conn.rollback()
        raise
    else:
        conn.commit()


def initialize_dispatch_schema(conn: sqlite3.Connection) -> None:
    """Install the versioned dispatcher schema idempotently.

    This intentionally uses its own migration ledger rather than SQLite's global
    ``user_version`` because the production database is shared with other PIF
    subsystems.
    """

    conn.execute("PRAGMA foreign_keys = ON")
    with _write_transaction(conn):
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_desk_rebuild_dispatch_migrations (
              version INTEGER PRIMARY KEY,
              applied_at TEXT NOT NULL
            )
            """
        )
        applied = conn.execute(
            "SELECT 1 FROM signal_desk_rebuild_dispatch_migrations WHERE version = ?",
            (SCHEMA_VERSION,),
        ).fetchone()
        if applied:
            return

        conn.execute(
            """
            CREATE TABLE signal_desk_rebuild_tasks (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              task_key TEXT NOT NULL UNIQUE,
              task_type TEXT NOT NULL,
              payload_json TEXT NOT NULL,
              payload_sha256 TEXT NOT NULL,
              status TEXT NOT NULL CHECK (
                status IN ('pending','running','succeeded','terminal_failed')
              ),
              current_attempt_id INTEGER,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE signal_desk_rebuild_attempts (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              task_id INTEGER NOT NULL
                REFERENCES signal_desk_rebuild_tasks(id) ON DELETE RESTRICT,
              attempt_number INTEGER NOT NULL CHECK (attempt_number >= 1),
              status TEXT NOT NULL CHECK (
                status IN ('pending','running','succeeded','terminal_failed')
              ),
              lease_owner TEXT,
              lease_until TEXT,
              lease_generation INTEGER NOT NULL DEFAULT 0
                CHECK (lease_generation >= 0),
              resurrection_reason TEXT,
              resurrected_by TEXT,
              resurrects_attempt_id INTEGER
                REFERENCES signal_desk_rebuild_attempts(id) ON DELETE RESTRICT,
              semantic_failure_code TEXT,
              semantic_failure_detail TEXT,
              infrastructure_failure_code TEXT,
              infrastructure_failure_detail TEXT,
              output_json TEXT,
              created_at TEXT NOT NULL,
              started_at TEXT,
              completed_at TEXT,
              updated_at TEXT NOT NULL,
              UNIQUE(task_id, attempt_number)
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX signal_desk_rebuild_attempts_dispatch_idx
            ON signal_desk_rebuild_attempts(status, lease_until, id)
            """
        )
        conn.execute(
            """
            CREATE UNIQUE INDEX signal_desk_rebuild_attempts_one_live_idx
            ON signal_desk_rebuild_attempts(task_id)
            WHERE status IN ('pending','running')
            """
        )
        conn.execute(
            "INSERT INTO signal_desk_rebuild_dispatch_migrations(version, applied_at) "
            "VALUES (?, ?)",
            (SCHEMA_VERSION, _timestamp()),
        )


def _row_dict(row: sqlite3.Row) -> Dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def _task_snapshot(conn: sqlite3.Connection, task_id: int) -> Dict[str, Any]:
    row = conn.execute(
        """
        SELECT t.*, a.attempt_number, a.status AS attempt_status,
               a.lease_owner, a.lease_until, a.lease_generation,
               a.resurrects_attempt_id, a.semantic_failure_code
        FROM signal_desk_rebuild_tasks t
        JOIN signal_desk_rebuild_attempts a ON a.id = t.current_attempt_id
        WHERE t.id = ?
        """,
        (task_id,),
    ).fetchone()
    if row is None:
        raise KeyError(task_id)
    result = _row_dict(row)
    result["payload"] = json.loads(result.pop("payload_json"))
    return result


def enqueue_task(
    conn: sqlite3.Connection,
    *,
    task_key: str,
    task_type: str,
    payload: Mapping[str, Any],
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Create semantic work once, or return its existing state.

    Re-enqueueing a terminal task never revives it.  The returned
    ``enqueue_outcome`` makes that condition visible to the caller.
    """

    if not task_key.strip() or not task_type.strip():
        raise ValueError("task_key and task_type must be non-empty")
    payload_json = _canonical_json(dict(payload))
    digest = _payload_hash(payload_json)
    timestamp = _timestamp(now)
    with _write_transaction(conn):
        existing = conn.execute(
            "SELECT * FROM signal_desk_rebuild_tasks WHERE task_key = ?", (task_key,)
        ).fetchone()
        if existing is not None:
            if existing["task_type"] != task_type or existing["payload_sha256"] != digest:
                raise TaskDefinitionConflict(
                    f"task_key {task_key!r} already names different semantic work"
                )
            result = _task_snapshot(conn, int(existing["id"]))
            result["enqueue_outcome"] = (
                "existing_terminal_failed"
                if existing["status"] == "terminal_failed"
                else "existing"
            )
            return result

        cursor = conn.execute(
            """
            INSERT INTO signal_desk_rebuild_tasks
              (task_key, task_type, payload_json, payload_sha256, status,
               created_at, updated_at)
            VALUES (?, ?, ?, ?, 'pending', ?, ?)
            """,
            (task_key, task_type, payload_json, digest, timestamp, timestamp),
        )
        task_id = int(cursor.lastrowid)
        attempt = conn.execute(
            """
            INSERT INTO signal_desk_rebuild_attempts
              (task_id, attempt_number, status, created_at, updated_at)
            VALUES (?, 1, 'pending', ?, ?)
            """,
            (task_id, timestamp, timestamp),
        )
        conn.execute(
            "UPDATE signal_desk_rebuild_tasks SET current_attempt_id = ? WHERE id = ?",
            (int(attempt.lastrowid), task_id),
        )
        result = _task_snapshot(conn, task_id)
        result["enqueue_outcome"] = "created"
        return result


def acquire_lease(
    conn: sqlite3.Connection,
    *,
    lease_owner: str,
    lease_seconds: float,
    now: Optional[datetime] = None,
) -> Optional[Dict[str, Any]]:
    """Lease pending work, or reclaim an expired running attempt atomically."""

    if not lease_owner.strip():
        raise ValueError("lease_owner must be non-empty")
    if lease_seconds <= 0:
        raise ValueError("lease_seconds must be positive")
    instant = _as_utc(now)
    timestamp = _timestamp(instant)
    lease_until = _timestamp(instant + timedelta(seconds=lease_seconds))
    with _write_transaction(conn):
        row = conn.execute(
            """
            SELECT a.id, a.task_id, a.status
            FROM signal_desk_rebuild_attempts a
            JOIN signal_desk_rebuild_tasks t ON t.current_attempt_id = a.id
            WHERE (a.status = 'pending')
               OR (a.status = 'running' AND a.lease_until <= ?)
            ORDER BY CASE a.status WHEN 'running' THEN 0 ELSE 1 END, a.id
            LIMIT 1
            """,
            (timestamp,),
        ).fetchone()
        if row is None:
            return None
        lease_kind = "expired_recovery" if row["status"] == "running" else "initial"
        conn.execute(
            """
            UPDATE signal_desk_rebuild_attempts
            SET status = 'running', lease_owner = ?, lease_until = ?,
                lease_generation = lease_generation + 1,
                started_at = COALESCE(started_at, ?), updated_at = ?
            WHERE id = ?
            """,
            (lease_owner, lease_until, timestamp, timestamp, int(row["id"])),
        )
        conn.execute(
            "UPDATE signal_desk_rebuild_tasks "
            "SET status = 'running', updated_at = ? WHERE id = ?",
            (timestamp, int(row["task_id"])),
        )
        result = _task_snapshot(conn, int(row["task_id"]))
        result["lease_kind"] = lease_kind
        return result


def _assert_lease(
    conn: sqlite3.Connection,
    *,
    attempt_id: int,
    lease_owner: str,
    lease_generation: int,
    now: Optional[datetime],
) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM signal_desk_rebuild_attempts WHERE id = ?", (attempt_id,)
    ).fetchone()
    if row is None:
        raise LostLease(f"attempt {attempt_id} does not exist")
    if (
        row["status"] != "running"
        or row["lease_owner"] != lease_owner
        or int(row["lease_generation"]) != lease_generation
        or row["lease_until"] <= _timestamp(now)
    ):
        raise LostLease(f"lease for attempt {attempt_id} is no longer valid")
    return row


def complete_attempt(
    conn: sqlite3.Connection,
    *,
    attempt_id: int,
    lease_owner: str,
    lease_generation: int,
    output: Mapping[str, Any],
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    timestamp = _timestamp(now)
    with _write_transaction(conn):
        attempt = _assert_lease(
            conn,
            attempt_id=attempt_id,
            lease_owner=lease_owner,
            lease_generation=lease_generation,
            now=now,
        )
        conn.execute(
            """
            UPDATE signal_desk_rebuild_attempts
            SET status = 'succeeded', output_json = ?, lease_owner = NULL,
                lease_until = NULL, completed_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (_canonical_json(dict(output)), timestamp, timestamp, attempt_id),
        )
        conn.execute(
            "UPDATE signal_desk_rebuild_tasks "
            "SET status = 'succeeded', updated_at = ? WHERE id = ?",
            (timestamp, int(attempt["task_id"])),
        )
        return _task_snapshot(conn, int(attempt["task_id"]))


def release_attempt_for_retry(
    conn: sqlite3.Connection,
    *,
    attempt_id: int,
    lease_owner: str,
    lease_generation: int,
    failure_code: str,
    failure_detail: str,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Return infrastructure-failed work to pending on the same attempt.

    Provider throttling, timeouts, and transport outages are not semantic
    verdicts and must not require resurrection or consume a new attempt.
    """

    if not failure_code.strip():
        raise ValueError("failure_code must be non-empty")
    timestamp = _timestamp(now)
    with _write_transaction(conn):
        attempt = _assert_lease(
            conn,
            attempt_id=attempt_id,
            lease_owner=lease_owner,
            lease_generation=lease_generation,
            now=now,
        )
        conn.execute(
            """
            UPDATE signal_desk_rebuild_attempts
            SET status = 'pending', lease_owner = NULL, lease_until = NULL,
                infrastructure_failure_code = ?,
                infrastructure_failure_detail = ?, updated_at = ?
            WHERE id = ?
            """,
            (failure_code, failure_detail, timestamp, attempt_id),
        )
        conn.execute(
            "UPDATE signal_desk_rebuild_tasks "
            "SET status = 'pending', updated_at = ? WHERE id = ?",
            (timestamp, int(attempt["task_id"])),
        )
        return _task_snapshot(conn, int(attempt["task_id"]))


def fail_attempt_semantically(
    conn: sqlite3.Connection,
    *,
    attempt_id: int,
    lease_owner: str,
    lease_generation: int,
    failure_code: str,
    failure_detail: str,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Terminalize semantic work; only explicit resurrection can retry it."""

    if not failure_code.strip():
        raise ValueError("failure_code must be non-empty")
    timestamp = _timestamp(now)
    with _write_transaction(conn):
        attempt = _assert_lease(
            conn,
            attempt_id=attempt_id,
            lease_owner=lease_owner,
            lease_generation=lease_generation,
            now=now,
        )
        conn.execute(
            """
            UPDATE signal_desk_rebuild_attempts
            SET status = 'terminal_failed', semantic_failure_code = ?,
                semantic_failure_detail = ?, lease_owner = NULL,
                lease_until = NULL, completed_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (failure_code, failure_detail, timestamp, timestamp, attempt_id),
        )
        conn.execute(
            "UPDATE signal_desk_rebuild_tasks "
            "SET status = 'terminal_failed', updated_at = ? WHERE id = ?",
            (timestamp, int(attempt["task_id"])),
        )
        return _task_snapshot(conn, int(attempt["task_id"]))


def resurrect_task(
    conn: sqlite3.Connection,
    *,
    task_key: str,
    resurrected_by: str,
    reason: str,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Append a new pending attempt to a terminally failed task lineage."""

    if not resurrected_by.strip() or not reason.strip():
        raise ValueError("resurrected_by and reason must be non-empty")
    timestamp = _timestamp(now)
    with _write_transaction(conn):
        task = conn.execute(
            "SELECT * FROM signal_desk_rebuild_tasks WHERE task_key = ?", (task_key,)
        ).fetchone()
        if task is None:
            raise KeyError(task_key)
        if task["status"] != "terminal_failed":
            raise InvalidTransition(
                f"task {task_key!r} is {task['status']}, not terminal_failed"
            )
        predecessor = conn.execute(
            "SELECT * FROM signal_desk_rebuild_attempts WHERE id = ?",
            (int(task["current_attempt_id"]),),
        ).fetchone()
        if predecessor is None or predecessor["status"] != "terminal_failed":
            raise InvalidTransition("terminal task has no terminal current attempt")
        cursor = conn.execute(
            """
            INSERT INTO signal_desk_rebuild_attempts
              (task_id, attempt_number, status, resurrection_reason,
               resurrected_by, resurrects_attempt_id, created_at, updated_at)
            VALUES (?, ?, 'pending', ?, ?, ?, ?, ?)
            """,
            (
                int(task["id"]),
                int(predecessor["attempt_number"]) + 1,
                reason,
                resurrected_by,
                int(predecessor["id"]),
                timestamp,
                timestamp,
            ),
        )
        conn.execute(
            """
            UPDATE signal_desk_rebuild_tasks
            SET status = 'pending', current_attempt_id = ?, updated_at = ?
            WHERE id = ?
            """,
            (int(cursor.lastrowid), timestamp, int(task["id"])),
        )
        return _task_snapshot(conn, int(task["id"]))


def get_task(conn: sqlite3.Connection, task_key: str) -> Dict[str, Any]:
    row = conn.execute(
        "SELECT id FROM signal_desk_rebuild_tasks WHERE task_key = ?", (task_key,)
    ).fetchone()
    if row is None:
        raise KeyError(task_key)
    return _task_snapshot(conn, int(row["id"]))


def list_attempts(conn: sqlite3.Connection, task_key: str) -> list[Dict[str, Any]]:
    task = conn.execute(
        "SELECT id FROM signal_desk_rebuild_tasks WHERE task_key = ?", (task_key,)
    ).fetchone()
    if task is None:
        raise KeyError(task_key)
    rows = conn.execute(
        "SELECT * FROM signal_desk_rebuild_attempts WHERE task_id = ? "
        "ORDER BY attempt_number",
        (int(task["id"]),),
    ).fetchall()
    return [_row_dict(row) for row in rows]
