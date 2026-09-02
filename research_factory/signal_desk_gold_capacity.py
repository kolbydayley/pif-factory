"""Persistent provider-capacity circuit breaker for the Gold authoring lane.

The Gold authoring workload uses the shared Codex subscription rather than an
isolated API quota.  A provider ``serverOverloaded`` response is therefore a
signal to stop adding load, not a normal per-item retry.  This module stores a
small, cross-process circuit breaker in the Gold dispatch database so a restart
cannot turn an outage into a retry storm.

It intentionally does not fall back to another model: Gold remains the frozen
GPT-5.6-sol reference authoring workload.  Recovery starts with one probe and
only restores multi-call admission after three successful probes.
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator, Mapping

from .util import now_iso, stable_id


SCHEMA_VERSION = "pif_signal_desk_gold_capacity_v1"
MODEL = "gpt-5.6-sol"
GOLD_LANE = "gpt_5_6_sol_gold_authoring"
FRONTIER_LANE = "gpt_5_6_sol_frontier_calibration"
SCORER_LANE = "gpt_5_6_sol_scorer_qualification"
ALLOWED_LANES = frozenset({GOLD_LANE, FRONTIER_LANE, SCORER_LANE})
INITIAL_BACKOFF_SECONDS = 300
MAX_BACKOFF_SECONDS = 21_600
SUCCESSFUL_PROBES_TO_CLOSE = 3
LEASE_SECONDS = 1_800

CAPACITY_ERROR_CODES = frozenset(
    {
        "serverOverloaded",
        "server_overloaded",
        "modelCapacityExceeded",
        "model_capacity_exceeded",
        "capacityExceeded",
        "capacity_exceeded",
        "serviceUnavailable",
    }
)


class GoldCapacityError(RuntimeError):
    pass


def _utc(value: datetime | None = None) -> datetime:
    value = value or datetime.now(timezone.utc)
    if value.tzinfo is None:
        raise ValueError("capacity timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)


def _timestamp(value: datetime | None = None) -> str:
    return _utc(value).isoformat(timespec="microseconds")


@contextmanager
def _transaction(conn: sqlite3.Connection) -> Iterator[None]:
    if conn.in_transaction:
        raise GoldCapacityError("capacity mutation requires no open transaction")
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield
    except BaseException:
        conn.rollback()
        raise
    else:
        conn.commit()


def ensure_gold_capacity_schema(conn: sqlite3.Connection) -> None:
    """Install the cross-run capacity state alongside leased Gold work."""

    with _transaction(conn):
        conn.execute(
            """CREATE TABLE IF NOT EXISTS signal_desk_gold_capacity_state (
                 model TEXT PRIMARY KEY,
                 state TEXT NOT NULL CHECK(state IN ('closed','open','half_open')),
                 consecutive_capacity_failures INTEGER NOT NULL DEFAULT 0,
                 successful_probe_count INTEGER NOT NULL DEFAULT 0,
                 next_probe_at TEXT,
                 last_error_code TEXT,
                 updated_at TEXT NOT NULL
               )"""
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS signal_desk_gold_capacity_leases (
                 admission_id TEXT PRIMARY KEY,
                 model TEXT NOT NULL,
                 lane TEXT NOT NULL DEFAULT 'gpt_5_6_sol_gold_authoring',
                 task_key TEXT NOT NULL,
                 lease_owner TEXT NOT NULL,
                 lease_until TEXT NOT NULL,
                 created_at TEXT NOT NULL,
                 UNIQUE(model, task_key)
               )"""
        )
        columns = {
            str(row[1])
            for row in conn.execute("PRAGMA table_info(signal_desk_gold_capacity_leases)")
        }
        if "lane" not in columns:
            conn.execute(
                "ALTER TABLE signal_desk_gold_capacity_leases "
                "ADD COLUMN lane TEXT NOT NULL DEFAULT 'gpt_5_6_sol_gold_authoring'"
            )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS signal_desk_model_capacity_events (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 occurred_at TEXT NOT NULL,
                 model TEXT NOT NULL,
                 lane TEXT NOT NULL,
                 event_type TEXT NOT NULL,
                 reason TEXT,
                 backend_message TEXT,
                 state TEXT NOT NULL,
                 task_key TEXT,
                 active_admissions INTEGER NOT NULL
               )"""
        )
        event_columns = {
            str(row[1])
            for row in conn.execute("PRAGMA table_info(signal_desk_model_capacity_events)")
        }
        if "backend_message" not in event_columns:
            conn.execute(
                "ALTER TABLE signal_desk_model_capacity_events ADD COLUMN backend_message TEXT"
            )
        conn.execute(
            """CREATE INDEX IF NOT EXISTS signal_desk_model_capacity_events_recent_idx
               ON signal_desk_model_capacity_events(model, occurred_at DESC)"""
        )
        conn.execute(
            """CREATE INDEX IF NOT EXISTS signal_desk_gold_capacity_leases_active_idx
               ON signal_desk_gold_capacity_leases(model, lease_until)"""
        )
        row = conn.execute(
            "SELECT 1 FROM signal_desk_gold_capacity_state WHERE model=?", (MODEL,)
        ).fetchone()
        if row is None:
            conn.execute(
                """INSERT INTO signal_desk_gold_capacity_state
                   (model,state,consecutive_capacity_failures,successful_probe_count,next_probe_at,last_error_code,updated_at)
                   VALUES (?, 'closed', 0, 0, NULL, NULL, ?)""",
                (MODEL, _timestamp()),
            )


def _state(conn: sqlite3.Connection) -> Mapping[str, Any]:
    row = conn.execute(
        "SELECT * FROM signal_desk_gold_capacity_state WHERE model=?", (MODEL,)
    ).fetchone()
    if row is None:
        raise GoldCapacityError("Gold capacity state is missing")
    return {key: row[key] for key in row.keys()} if isinstance(row, sqlite3.Row) else {
        "model": row[0], "state": row[1], "consecutive_capacity_failures": row[2],
        "successful_probe_count": row[3], "next_probe_at": row[4],
        "last_error_code": row[5], "updated_at": row[6],
    }


def _next_probe(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return _utc(parsed)


def _active_count(conn: sqlite3.Connection, *, now: datetime) -> int:
    return int(
        conn.execute(
            "SELECT COUNT(*) FROM signal_desk_gold_capacity_leases WHERE model=? AND lease_until > ?",
            (MODEL, _timestamp(now)),
        ).fetchone()[0]
    )


def _record_event(
    conn: sqlite3.Connection,
    *,
    now: datetime,
    lane: str,
    event_type: str,
    reason: str | None,
    backend_message: str | None = None,
    state: str,
    task_key: str | None,
    active: int,
) -> None:
    """Append sanitized ownership telemetry inside the caller's transaction."""

    conn.execute(
        """INSERT INTO signal_desk_model_capacity_events
           (occurred_at,model,lane,event_type,reason,backend_message,state,task_key,active_admissions)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (
            _timestamp(now),
            MODEL,
            lane,
            event_type,
            reason,
            backend_message,
            state,
            task_key,
            int(active),
        ),
    )


def capacity_status(conn: sqlite3.Connection, *, at: datetime | None = None) -> dict[str, Any]:
    """Read only, sanitized status for receipts and supervisor wait decisions."""

    ensure_gold_capacity_schema(conn)
    now = _utc(at)
    state = _state(conn)
    next_probe = _next_probe(state["next_probe_at"])
    return {
        "schema_version": SCHEMA_VERSION,
        "model": MODEL,
        "state": state["state"],
        "consecutive_capacity_failures": int(state["consecutive_capacity_failures"]),
        "successful_probe_count": int(state["successful_probe_count"]),
        "next_probe_at": state["next_probe_at"],
        "last_error_code": state["last_error_code"],
        "active_admissions": _active_count(conn, now=now),
        "retry_after_seconds": max(0, int((next_probe - now).total_seconds())) if next_probe else 0,
    }


def admit_gold_call(
    conn: sqlite3.Connection,
    *,
    task_key: str,
    lease_owner: str,
    configured_concurrency: int,
    lane: str = GOLD_LANE,
    at: datetime | None = None,
) -> dict[str, Any]:
    """Atomically admit a Gold provider call or return a zero-call wait.

    ``configured_concurrency`` remains the worker-pool contract.  A capacity
    outage temporarily limits *provider* calls to one, preserving the two-to-
    eight worker configuration without allowing a retry stampede.
    """

    if lane not in ALLOWED_LANES:
        raise ValueError("unrecognized GPT-5.6-sol capacity lane")
    if not 1 <= configured_concurrency <= 8:
        raise ValueError("configured Gold concurrency must be 1-8")
    ensure_gold_capacity_schema(conn)
    now = _utc(at)
    with _transaction(conn):
        conn.execute(
            "DELETE FROM signal_desk_gold_capacity_leases WHERE model=? AND lease_until <= ?",
            (MODEL, _timestamp(now)),
        )
        state = _state(conn)
        next_probe = _next_probe(state["next_probe_at"])
        active = _active_count(conn, now=now)
        current_state = str(state["state"])
        if current_state == "open":
            if next_probe is not None and now < next_probe:
                decision = {
                    "allowed": False,
                    "reason": "gold_model_capacity_backoff",
                    "retry_after_seconds": max(1, int((next_probe - now).total_seconds())),
                    "state": current_state,
                }
                _record_event(conn, now=now, lane=lane, event_type="denied", reason=decision["reason"], state=current_state, task_key=task_key, active=active)
                return decision
            if active:
                decision = {
                    "allowed": False,
                    "reason": "gold_model_capacity_probe_in_progress",
                    "retry_after_seconds": LEASE_SECONDS,
                    "state": current_state,
                }
                _record_event(conn, now=now, lane=lane, event_type="denied", reason=decision["reason"], state=current_state, task_key=task_key, active=active)
                return decision
            conn.execute(
                """UPDATE signal_desk_gold_capacity_state
                   SET state='half_open', updated_at=? WHERE model=?""",
                (_timestamp(now), MODEL),
            )
            current_state = "half_open"
        limit = 1 if current_state == "half_open" else configured_concurrency
        if active >= limit:
            decision = {
                "allowed": False,
                "reason": "gold_model_capacity_slots_full",
                "retry_after_seconds": 60,
                "state": current_state,
            }
            _record_event(conn, now=now, lane=lane, event_type="denied", reason=decision["reason"], state=current_state, task_key=task_key, active=active)
            return decision
        admission_id = stable_id(MODEL, task_key, lease_owner, _timestamp(now), prefix="sdgc_")
        conn.execute(
            """INSERT INTO signal_desk_gold_capacity_leases
               (admission_id,model,lane,task_key,lease_owner,lease_until,created_at)
               VALUES (?,?,?,?,?,?,?)""",
            (
                admission_id,
                MODEL,
                lane,
                task_key,
                lease_owner,
                _timestamp(now + timedelta(seconds=LEASE_SECONDS)),
                _timestamp(now),
            ),
        )
        _record_event(conn, now=now, lane=lane, event_type="admitted", reason=None, state=current_state, task_key=task_key, active=active + 1)
        return {
            "allowed": True,
            "admission_id": admission_id,
            "state": current_state,
            "provider_concurrency_limit": limit,
        }


def release_gold_admission(conn: sqlite3.Connection, *, admission_id: str) -> None:
    ensure_gold_capacity_schema(conn)
    with _transaction(conn):
        lease = conn.execute(
            "SELECT lane,task_key FROM signal_desk_gold_capacity_leases WHERE admission_id=?",
            (admission_id,),
        ).fetchone()
        conn.execute(
            "DELETE FROM signal_desk_gold_capacity_leases WHERE admission_id=?", (admission_id,)
        )
        if lease is not None:
            lane, task_key = str(lease[0]), str(lease[1])
            state = str(_state(conn)["state"])
            _record_event(conn, now=_utc(), lane=lane, event_type="released", reason="without_provider_outcome", state=state, task_key=task_key, active=_active_count(conn, now=_utc()))


def _backoff_seconds(consecutive_failures: int) -> int:
    # 5m, 10m, 20m, 40m, 80m, then cap at six hours.  This lets a transient
    # outage recover promptly while making repeated provider rejection quiet.
    exponent = max(0, int(consecutive_failures) - 1)
    return min(MAX_BACKOFF_SECONDS, INITIAL_BACKOFF_SECONDS * (2 ** exponent))


def record_capacity_failure(
    conn: sqlite3.Connection,
    *,
    admission_id: str,
    error_code: str,
    backend_message: str | None = None,
    at: datetime | None = None,
) -> dict[str, Any]:
    """Open the circuit after a provider capacity response and release its slot."""

    if not is_model_capacity_error(error_code=error_code):
        raise GoldCapacityError("only a recognized model-capacity error may open the circuit")
    ensure_gold_capacity_schema(conn)
    now = _utc(at)
    with _transaction(conn):
        lease = conn.execute(
            "SELECT lane,task_key FROM signal_desk_gold_capacity_leases WHERE admission_id=?",
            (admission_id,),
        ).fetchone()
        lane = str(lease[0]) if lease is not None else "unknown"
        task_key = str(lease[1]) if lease is not None else None
        conn.execute(
            "DELETE FROM signal_desk_gold_capacity_leases WHERE admission_id=?", (admission_id,)
        )
        _record_event(
            conn, now=now, lane=lane, event_type="capacity_failure",
            reason=str(error_code), backend_message=backend_message,
            state="open", task_key=task_key,
            active=_active_count(conn, now=now),
        )
        state = _state(conn)
        failures = int(state["consecutive_capacity_failures"]) + 1
        delay = _backoff_seconds(failures)
        next_probe = now + timedelta(seconds=delay)
        conn.execute(
            """UPDATE signal_desk_gold_capacity_state
               SET state='open', consecutive_capacity_failures=?, successful_probe_count=0,
                   next_probe_at=?, last_error_code=?, updated_at=? WHERE model=?""",
            (failures, _timestamp(next_probe), str(error_code), _timestamp(now), MODEL),
        )
        return {
            "state": "open", "consecutive_capacity_failures": failures,
            "backoff_seconds": delay, "next_probe_at": _timestamp(next_probe),
        }


def record_gold_admission_success(
    conn: sqlite3.Connection,
    *,
    admission_id: str,
    at: datetime | None = None,
) -> dict[str, Any]:
    """Release a capacity slot; close only after three half-open successes."""

    ensure_gold_capacity_schema(conn)
    now = _utc(at)
    with _transaction(conn):
        lease = conn.execute(
            "SELECT lane,task_key FROM signal_desk_gold_capacity_leases WHERE admission_id=?",
            (admission_id,),
        ).fetchone()
        lane = str(lease[0]) if lease is not None else "unknown"
        task_key = str(lease[1]) if lease is not None else None
        conn.execute(
            "DELETE FROM signal_desk_gold_capacity_leases WHERE admission_id=?", (admission_id,)
        )
        state = _state(conn)
        if state["state"] != "half_open":
            _record_event(conn, now=now, lane=lane, event_type="success", reason=None, state=str(state["state"]), task_key=task_key, active=_active_count(conn, now=now))
            return {"state": state["state"], "successful_probe_count": int(state["successful_probe_count"])}
        successes = int(state["successful_probe_count"]) + 1
        if successes >= SUCCESSFUL_PROBES_TO_CLOSE:
            conn.execute(
                """UPDATE signal_desk_gold_capacity_state
                   SET state='closed', consecutive_capacity_failures=0, successful_probe_count=0,
                       next_probe_at=NULL, last_error_code=NULL, updated_at=? WHERE model=?""",
                (_timestamp(now), MODEL),
            )
            _record_event(conn, now=now, lane=lane, event_type="success", reason="circuit_closed", state="closed", task_key=task_key, active=_active_count(conn, now=now))
            return {"state": "closed", "successful_probe_count": successes}
        # Re-open with no delay so the next call is a deliberately serialized
        # probe too.  It retains the one-call half-open admission limit.
        conn.execute(
            """UPDATE signal_desk_gold_capacity_state
               SET state='open', successful_probe_count=?, next_probe_at=?, updated_at=? WHERE model=?""",
            (successes, _timestamp(now), _timestamp(now), MODEL),
        )
        _record_event(conn, now=now, lane=lane, event_type="success", reason="probe_succeeded", state="open", task_key=task_key, active=_active_count(conn, now=now))
        return {"state": "open", "successful_probe_count": successes}


def is_model_capacity_error(*, error_code: str | None = None, detail: str | None = None) -> bool:
    """Recognize provider capacity codes without treating semantic errors as retryable."""

    if error_code and str(error_code) in CAPACITY_ERROR_CODES:
        return True
    text = str(detail or "").casefold()
    return (
        "selected model is at capacity" in text
        or "server overloaded" in text
        or "serveroverloaded" in text
        or "model capacity" in text
    )


def capacity_error_from_sidecar(sidecar_path: str | None) -> str | None:
    """Read only the error code from a privacy-safe sidecar, never prompt text."""

    if not sidecar_path:
        return None
    try:
        import json

        payload = json.loads(open(sidecar_path, encoding="utf-8").read())
    except (OSError, ValueError):
        return None
    error = ((payload.get("turn_error") or {}).get("codex_error_info"))
    return str(error) if error is not None else None


def capacity_backend_message_from_sidecar(sidecar_path: str | None) -> str | None:
    """Read the bounded raw backend capacity diagnostic from a sidecar."""

    if not sidecar_path:
        return None
    try:
        import json

        payload = json.loads(open(sidecar_path, encoding="utf-8").read())
    except (OSError, ValueError):
        return None
    error = payload.get("turn_error") or {}
    if not is_model_capacity_error(error_code=error.get("codex_error_info")):
        return None
    message = error.get("backend_message")
    return str(message) if isinstance(message, str) and len(message) <= 500 else None
