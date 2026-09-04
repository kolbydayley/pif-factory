"""Persistent adaptive concurrency for Signal Desk provider lanes.

The controller deliberately records only operational metadata.  It is shared by
independent runners through their existing SQLite state databases, so restarting
a swarm cannot erase a recent overload signal and immediately recreate it.
"""

from __future__ import annotations

import math
import sqlite3
import time
from datetime import datetime, timezone
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator


SCHEMA_VERSION = "pif_signal_desk_adaptive_concurrency_v1"
WINDOW_SECONDS = 600.0
NO_SUCCESS_SECONDS = 900.0
COOLDOWN_SECONDS = 600.0
CALL_WINDOW_SIZE = 100
GOLD_OFF_PEAK_START_UTC = 2
GOLD_OFF_PEAK_END_UTC = 13
GOLD_PEAK_START_UTC = 22
GOLD_PEAK_END_UTC = 2
GOLD_OFF_PEAK_EVALUATION_SECONDS = 120.0


@dataclass(frozen=True)
class LaneBounds:
    minimum: int
    maximum: int
    latency_p95_seconds: float

    def __post_init__(self) -> None:
        if self.minimum < 1 or self.maximum < self.minimum:
            raise ValueError("invalid adaptive concurrency bounds")


GLM_BOUNDS = LaneBounds(1, 23, 90.0)
GOLD_BOUNDS = LaneBounds(2, 8, 900.0)


@contextmanager
def _transaction(conn: sqlite3.Connection) -> Iterator[None]:
    if conn.in_transaction:
        raise RuntimeError("adaptive controller mutation requires no open transaction")
    # The state database is shared with the runner's other writers and with
    # read-only observers (keepalive ticks, monitors).  Without a busy
    # timeout, BEGIN IMMEDIATE raises 'database is locked' on any momentary
    # contention; at runner startup that crashed relaunch attempts on
    # 2026-09-03/04.  Wait briefly instead of failing.
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield
    except BaseException:
        conn.rollback()
        raise
    else:
        conn.commit()


def ensure_adaptive_concurrency_schema(conn: sqlite3.Connection) -> None:
    with _transaction(conn):
        conn.execute(
            """CREATE TABLE IF NOT EXISTS signal_desk_adaptive_concurrency_state (
                 lane TEXT PRIMARY KEY,
                 effective_limit INTEGER NOT NULL,
                 minimum_limit INTEGER NOT NULL,
                 maximum_limit INTEGER NOT NULL,
                 latency_p95_seconds REAL NOT NULL,
                 cooldown_until REAL NOT NULL DEFAULT 0,
                 healthy_windows INTEGER NOT NULL DEFAULT 0,
                 last_success_at REAL,
                 last_evaluated_at REAL NOT NULL DEFAULT 0,
                 last_evaluated_event_id INTEGER NOT NULL DEFAULT 0,
                 last_trip_reason TEXT,
                 updated_at REAL NOT NULL
               )"""
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS signal_desk_adaptive_concurrency_events (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 lane TEXT NOT NULL,
                 occurred_at REAL NOT NULL,
                 outcome TEXT NOT NULL CHECK(outcome IN ('success','rate_limit','timeout','parse_schema','failure')),
                 latency_seconds REAL NOT NULL CHECK(latency_seconds >= 0)
               )"""
        )
        columns = {row[1] for row in conn.execute("PRAGMA table_info(signal_desk_adaptive_concurrency_state)")}
        if "last_evaluated_event_id" not in columns:
            conn.execute(
                "ALTER TABLE signal_desk_adaptive_concurrency_state "
                "ADD COLUMN last_evaluated_event_id INTEGER NOT NULL DEFAULT 0"
            )
        conn.execute(
            """CREATE INDEX IF NOT EXISTS signal_desk_adaptive_concurrency_events_lane_time_idx
               ON signal_desk_adaptive_concurrency_events(lane, occurred_at DESC)"""
        )


def _row(conn: sqlite3.Connection, lane: str) -> sqlite3.Row | tuple[Any, ...] | None:
    return conn.execute(
        "SELECT * FROM signal_desk_adaptive_concurrency_state WHERE lane=?", (lane,)
    ).fetchone()


def _mapping(row: sqlite3.Row | tuple[Any, ...]) -> dict[str, Any]:
    if isinstance(row, sqlite3.Row):
        return dict(row)
    keys = (
        "lane", "effective_limit", "minimum_limit", "maximum_limit", "latency_p95_seconds",
        "cooldown_until", "healthy_windows", "last_success_at", "last_evaluated_at",
        "last_evaluated_event_id", "last_trip_reason", "updated_at",
    )
    return dict(zip(keys, row))


def initialize_lane(
    conn: sqlite3.Connection, *, lane: str, bounds: LaneBounds, initial_limit: int,
    now: float | None = None,
) -> dict[str, Any]:
    """Create or reconcile a lane without erasing its learned lower limit."""

    if not lane:
        raise ValueError("lane is required")
    now = time.time() if now is None else float(now)
    initial_limit = max(bounds.minimum, min(bounds.maximum, int(initial_limit)))
    ensure_adaptive_concurrency_schema(conn)
    with _transaction(conn):
        current = _row(conn, lane)
        if current is None:
            conn.execute(
                """INSERT INTO signal_desk_adaptive_concurrency_state
                   (lane,effective_limit,minimum_limit,maximum_limit,latency_p95_seconds,last_success_at,updated_at)
                   VALUES (?,?,?,?,?,?,?)""",
                (lane, initial_limit, bounds.minimum, bounds.maximum, bounds.latency_p95_seconds, now, now),
            )
        else:
            state = _mapping(current)
            effective = max(bounds.minimum, min(bounds.maximum, int(state["effective_limit"])))
            # A (re)start is a fresh baseline for the no-success rule.  That
            # rule exists to catch a live lane going silent; a lane that was
            # deliberately stopped (a 4h user pause on 2026-09-03) is not an
            # outage, yet its stale last_success_at tripped no_success_15m on
            # resume and throttled the lane into the evening peak freeze.
            # Rate-limit, timeout and p95 rules still react immediately.
            conn.execute(
                """UPDATE signal_desk_adaptive_concurrency_state
                   SET effective_limit=?, minimum_limit=?, maximum_limit=?, latency_p95_seconds=?,
                       last_success_at=?, updated_at=?
                   WHERE lane=?""",
                (effective, bounds.minimum, bounds.maximum, bounds.latency_p95_seconds, now, now, lane),
            )
        return _status_from_state(_mapping(_row(conn, lane)), lane=lane, now=now)


def _recent_events(conn: sqlite3.Connection, lane: str, now: float) -> list[dict[str, Any]]:
    rows = conn.execute(
        """SELECT id,outcome,latency_seconds,occurred_at FROM signal_desk_adaptive_concurrency_events
           WHERE lane=? AND occurred_at >= ? ORDER BY occurred_at DESC,id DESC LIMIT ?""",
        (lane, now - WINDOW_SECONDS, CALL_WINDOW_SIZE),
    ).fetchall()
    return [dict(row) if isinstance(row, sqlite3.Row) else {
        "id": row[0], "outcome": row[1], "latency_seconds": row[2], "occurred_at": row[3]
    } for row in rows]


def _p95(values: list[float]) -> float:
    if not values:
        return 0.0
    values.sort()
    return values[min(len(values) - 1, math.ceil(len(values) * 0.95) - 1)]


def gold_clock_period(now: float) -> str:
    hour = datetime.fromtimestamp(now, timezone.utc).hour
    if GOLD_OFF_PEAK_START_UTC <= hour < GOLD_OFF_PEAK_END_UTC:
        return "off_peak"
    if hour >= GOLD_PEAK_START_UTC or hour < GOLD_PEAK_END_UTC:
        return "peak"
    return "shoulder"


def _trip_reason(
    state: dict[str, Any], events: list[dict[str, Any]], now: float, *, lane: str,
) -> str | None:
    if (
        not (lane == "gold" and gold_clock_period(now) == "peak")
        and now >= float(state["cooldown_until"])
        and state["last_success_at"] is not None
        and now - float(state["last_success_at"]) > NO_SUCCESS_SECONDS
    ):
        return "no_success_15m"
    # Trip idempotency: a trip already charged the events that caused it.
    # Without this watermark every later evaluation (each 120s off-peak)
    # re-tripped on the same events until they aged out of the 600s window,
    # draining a lane from 8 to its minimum on one bad event (2026-09-03).
    last_trip_at = float(state["cooldown_until"]) - COOLDOWN_SECONDS
    events = [event for event in events if float(event["occurred_at"]) > last_trip_at]
    count = len(events)
    if not count:
        return None
    rates = {kind: sum(event["outcome"] == kind for event in events) / count for kind in (
        "rate_limit", "timeout", "parse_schema"
    )}
    if rates["rate_limit"] > 0.02:
        return "rate_limit_over_2pct"
    # The provider's US-evening gpt-5.6-sol saturation is model-pool
    # availability, not account quota or local pressure. During that known
    # peak, hold the last proven limit and react only to an actual capacity
    # response; semantic and transport failures still stop their individual
    # runner, but cannot masquerade as a pool-capacity signal.
    if lane == "gold" and gold_clock_period(now) == "peak":
        return None
    if rates["timeout"] > 0.05:
        return "timeout_over_5pct"
    if rates["parse_schema"] > 0.02:
        return "parse_schema_over_2pct"
    if _p95([float(event["latency_seconds"]) for event in events]) > float(state["latency_p95_seconds"]):
        return "p95_latency_exceeded"
    return None


def _evaluate(conn: sqlite3.Connection, *, lane: str, now: float, force: bool = False) -> dict[str, Any]:
    state = _mapping(_row(conn, lane))
    events = _recent_events(conn, lane, now)
    newest_event_id = max((int(event["id"]) for event in events), default=0)
    interval = (
        GOLD_OFF_PEAK_EVALUATION_SECONDS
        if lane == "gold" and gold_clock_period(now) == "off_peak"
        else WINDOW_SECONDS
    )
    has_new_events = newest_event_id > int(state.get("last_evaluated_event_id") or 0)
    no_success_due = (
        not (lane == "gold" and gold_clock_period(now) == "peak")
        and now >= float(state["cooldown_until"])
        and state["last_success_at"] is not None
        and now - float(state["last_success_at"]) > NO_SUCCESS_SECONDS
        and state.get("last_trip_reason") != "no_success_15m"
    )
    due = no_success_due or (has_new_events and (
        force
        or newest_event_id - int(state.get("last_evaluated_event_id") or 0) >= CALL_WINDOW_SIZE
        or now - float(state["last_evaluated_at"]) >= interval
    ))
    if not due:
        return state
    period = gold_clock_period(now) if lane == "gold" else "default"
    reason = _trip_reason(state, events, now, lane=lane)
    if reason:
        limit = max(int(state["minimum_limit"]), int(state["effective_limit"]) - 2)
        conn.execute(
            """UPDATE signal_desk_adaptive_concurrency_state
               SET effective_limit=?, cooldown_until=?, healthy_windows=0,
                   last_evaluated_at=?, last_evaluated_event_id=?, last_trip_reason=?, updated_at=? WHERE lane=?""",
            (limit, now + COOLDOWN_SECONDS, now, newest_event_id, reason, now, lane),
        )
    elif now >= float(state["cooldown_until"]) and period != "peak":
        healthy = int(state["healthy_windows"]) + 1
        limit = int(state["effective_limit"])
        if healthy >= 3 and limit < int(state["maximum_limit"]):
            limit += 1
            healthy = 0
        conn.execute(
            """UPDATE signal_desk_adaptive_concurrency_state
               SET effective_limit=?, healthy_windows=?, last_evaluated_at=?, last_evaluated_event_id=?, updated_at=? WHERE lane=?""",
            (limit, healthy, now, newest_event_id, now, lane),
        )
    else:
        conn.execute(
            "UPDATE signal_desk_adaptive_concurrency_state SET last_evaluated_at=?, last_evaluated_event_id=?, updated_at=? WHERE lane=?",
            (now, newest_event_id, now, lane),
        )
    conn.execute(
        "DELETE FROM signal_desk_adaptive_concurrency_events WHERE occurred_at < ?", (now - 3600.0,)
    )
    return _mapping(_row(conn, lane))


def record_outcome(
    conn: sqlite3.Connection, *, lane: str, outcome: str, latency_seconds: float,
    now: float | None = None,
) -> dict[str, Any]:
    """Persist a completed call outcome and return the current admission limit."""

    if outcome not in {"success", "rate_limit", "timeout", "parse_schema", "failure"}:
        raise ValueError("unknown adaptive outcome")
    now = time.time() if now is None else float(now)
    ensure_adaptive_concurrency_schema(conn)
    with _transaction(conn):
        if _row(conn, lane) is None:
            raise ValueError(f"adaptive lane is not initialized: {lane}")
        conn.execute(
            """INSERT INTO signal_desk_adaptive_concurrency_events
               (lane,occurred_at,outcome,latency_seconds) VALUES (?,?,?,?)""",
            (lane, now, outcome, max(0.0, float(latency_seconds))),
        )
        if outcome == "success":
            conn.execute(
                "UPDATE signal_desk_adaptive_concurrency_state SET last_success_at=?, updated_at=? WHERE lane=?",
                (now, now, lane),
            )
        state = _evaluate(conn, lane=lane, now=now)
        return _status_from_state(state, lane=lane, now=now)


def lane_status(conn: sqlite3.Connection, *, lane: str, now: float | None = None) -> dict[str, Any]:
    now = time.time() if now is None else float(now)
    ensure_adaptive_concurrency_schema(conn)
    row = _row(conn, lane)
    if row is None:
        raise ValueError(f"adaptive lane is not initialized: {lane}")
    return _status_from_state(_mapping(row), lane=lane, now=now)


def set_effective_limit(
    conn: sqlite3.Connection, *, lane: str, effective_limit: int,
    reason: str, now: float | None = None,
) -> dict[str, Any]:
    """Adjust a live lane's durable ceiling without stopping its workers."""

    if not str(reason).strip():
        raise ValueError("capacity adjustment reason is required")
    now = time.time() if now is None else float(now)
    ensure_adaptive_concurrency_schema(conn)
    with _transaction(conn):
        row = _row(conn, lane)
        if row is None:
            raise ValueError(f"adaptive lane is not initialized: {lane}")
        state = _mapping(row)
        value = int(effective_limit)
        if not int(state["minimum_limit"]) <= value <= int(state["maximum_limit"]):
            raise ValueError("effective limit is outside the lane bounds")
        conn.execute(
            """UPDATE signal_desk_adaptive_concurrency_state
               SET effective_limit=?, healthy_windows=0, last_trip_reason=?, updated_at=?
               WHERE lane=?""",
            (value, f"operator_adjustment:{reason}", now, lane),
        )
        return _status_from_state(_mapping(_row(conn, lane)), lane=lane, now=now)


def admission_limit(conn: sqlite3.Connection, *, lane: str, now: float | None = None) -> dict[str, Any]:
    """Refresh time-based safeguards before admitting another provider call."""

    now = time.time() if now is None else float(now)
    ensure_adaptive_concurrency_schema(conn)
    with _transaction(conn):
        if _row(conn, lane) is None:
            raise ValueError(f"adaptive lane is not initialized: {lane}")
        state = _evaluate(conn, lane=lane, now=now)
        return _status_from_state(state, lane=lane, now=now)


def _status_from_state(state: dict[str, Any], *, lane: str, now: float) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "lane": lane,
        "effective_limit": int(state["effective_limit"]),
        "minimum_limit": int(state["minimum_limit"]),
        "maximum_limit": int(state["maximum_limit"]),
        "cooldown_remaining_seconds": max(0, int(float(state["cooldown_until"]) - now)),
        "healthy_windows": int(state["healthy_windows"]),
        "last_trip_reason": state["last_trip_reason"],
        "clock_period": gold_clock_period(now) if lane == "gold" else "default",
    }
