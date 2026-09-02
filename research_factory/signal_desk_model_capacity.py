"""Sanitized capacity pulse and priority policy for Signal Desk GPT-5.6-sol work.

The provider exposes weekly quota, but not a literal global-capacity percentage.
Consequently the pulse keeps those concepts separate: weekly headroom comes from
the official account read, while provider availability is inferred from the
durable capacity circuit, recent outcomes, admissions, and stale reservations.
No prompt, transcript, output, account identity, or thread identifier is stored.
"""

from __future__ import annotations

import json
import math
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from .util import now_iso


POLICY_VERSION = "pif_signal_desk_model_capacity_policy_v1"
PULSE_VERSION = "pif_signal_desk_model_capacity_pulse_v1"
SNAPSHOT_VERSION = "pif_signal_desk_model_capacity_snapshot_v1"
EXPECTED_MODEL = "gpt-5.6-sol"
EXPECTED_LANES = {
    "interactive_codex": (0, "reserved"),
    "gpt_5_6_sol_gold_authoring": (10, "active"),
    "gpt_5_6_sol_frontier_calibration": (20, "gated"),
    "gpt_5_6_sol_scorer_qualification": (30, "gated"),
    "other_pif_codex_subscription": (50, "paused"),
}


class ModelCapacityError(RuntimeError):
    """The model-capacity policy or pulse inputs are unsafe or incomplete."""


def _utc_timestamp(value: str | None = None) -> float:
    if value is None:
        return time.time()
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ModelCapacityError("capacity timestamp must be timezone-aware")
    return parsed.astimezone(timezone.utc).timestamp()


def load_capacity_policy(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ModelCapacityError("model-capacity policy is unreadable") from exc
    lanes = payload.get("lanes") if isinstance(payload, dict) else None
    health = payload.get("health") if isinstance(payload, dict) else None
    override = payload.get("foreground_override") if isinstance(payload, dict) else None
    if (
        payload.get("schema_version") != POLICY_VERSION
        or payload.get("model") != EXPECTED_MODEL
        or payload.get("active_background_lane") != "gpt_5_6_sol_gold_authoring"
        or not isinstance(lanes, list)
        or not isinstance(health, dict)
        or not isinstance(override, dict)
        or override.get("configured_concurrency") != 8
        or override.get("provider_concurrency_cap") != 8
        or health.get("history_window_seconds") != 600
        or health.get("orphan_after_seconds") != 1800
        or health.get("capacity_error_rate_red") != 0.02
        or health.get("weekly_warning_percent") != 85.0
        or health.get("weekly_kill_percent") != 120.0
    ):
        raise ModelCapacityError("model-capacity policy contract drifted")
    observed = {
        str(row.get("lane")): (row.get("priority"), row.get("mode"))
        for row in lanes
        if isinstance(row, dict)
    }
    if observed != EXPECTED_LANES:
        raise ModelCapacityError("model-capacity lane priorities drifted")
    return payload


def ensure_capacity_pulse_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """CREATE TABLE IF NOT EXISTS signal_desk_model_capacity_snapshots (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             observed_at TEXT NOT NULL,
             lane TEXT NOT NULL,
             model TEXT NOT NULL,
             used_percent REAL NOT NULL,
             resets_at INTEGER NOT NULL,
             window_minutes INTEGER,
             source TEXT NOT NULL
           )"""
    )
    conn.execute(
        """CREATE INDEX IF NOT EXISTS signal_desk_model_capacity_snapshots_recent_idx
           ON signal_desk_model_capacity_snapshots(model, observed_at DESC)"""
    )
    conn.commit()


def record_capacity_snapshot(
    conn: sqlite3.Connection,
    *,
    lane: str,
    snapshot: Mapping[str, Any],
    observed_at: str | None = None,
) -> dict[str, Any]:
    """Persist only numeric quota metadata from an official provider read."""

    used = snapshot.get("used_percent")
    resets_at = snapshot.get("resets_at")
    window = snapshot.get("window_minutes")
    source = snapshot.get("source")
    if (
        isinstance(used, bool)
        or not isinstance(used, (int, float))
        or not math.isfinite(float(used))
        or not 0 <= float(used) <= 100
        or isinstance(resets_at, bool)
        or not isinstance(resets_at, int)
        or resets_at <= 0
        or (window is not None and (isinstance(window, bool) or not isinstance(window, int)))
        or source not in {"app_server_live", "session_log_fallback"}
    ):
        raise ModelCapacityError("provider capacity snapshot is malformed")
    ensure_capacity_pulse_schema(conn)
    record = {
        "schema_version": SNAPSHOT_VERSION,
        "observed_at": observed_at or now_iso(),
        "lane": str(lane),
        "model": EXPECTED_MODEL,
        "used_percent": float(used),
        "resets_at": int(resets_at),
        "window_minutes": window,
        "source": str(source),
    }
    conn.execute(
        """INSERT INTO signal_desk_model_capacity_snapshots
           (observed_at,lane,model,used_percent,resets_at,window_minutes,source)
           VALUES (?,?,?,?,?,?,?)""",
        (
            record["observed_at"], record["lane"], record["model"],
            record["used_percent"], record["resets_at"],
            record["window_minutes"], record["source"],
        ),
    )
    conn.commit()
    return record


def _row(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
    prior = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        value = conn.execute(sql, params).fetchone()
        return dict(value) if value is not None else None
    except sqlite3.OperationalError:
        return None
    finally:
        conn.row_factory = prior


def _rows(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    prior = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        return [dict(row) for row in conn.execute(sql, params).fetchall()]
    except sqlite3.OperationalError:
        return []
    finally:
        conn.row_factory = prior


def build_capacity_pulse(
    conn: sqlite3.Connection,
    *,
    policy: Mapping[str, Any],
    at: str | None = None,
) -> dict[str, Any]:
    """Build an actionable, privacy-safe model health snapshot."""

    if policy.get("schema_version") != POLICY_VERSION:
        raise ModelCapacityError("unvalidated model-capacity policy")
    now_epoch = _utc_timestamp(at)
    history_seconds = int(policy["health"]["history_window_seconds"])
    orphan_after = int(policy["health"]["orphan_after_seconds"])
    recent_after = now_epoch - history_seconds
    capacity = _row(
        conn,
        "SELECT * FROM signal_desk_gold_capacity_state WHERE model=?",
        (EXPECTED_MODEL,),
    ) or {
        "model": EXPECTED_MODEL,
        "state": "unknown",
        "consecutive_capacity_failures": 0,
        "successful_probe_count": 0,
        "next_probe_at": None,
        "last_error_code": None,
    }
    capacity_leases = _rows(
        conn,
        "SELECT lane,COUNT(*) AS count FROM signal_desk_gold_capacity_leases "
        "WHERE model=? AND lease_until>? GROUP BY lane ORDER BY lane",
        (EXPECTED_MODEL, datetime.fromtimestamp(now_epoch, timezone.utc).isoformat()),
    )
    if not capacity_leases:
        # Compatibility with the pre-lane lease schema.
        legacy = _row(
            conn,
            "SELECT COUNT(*) AS count FROM signal_desk_gold_capacity_leases "
            "WHERE model=? AND lease_until>?",
            (EXPECTED_MODEL, datetime.fromtimestamp(now_epoch, timezone.utc).isoformat()),
        )
        if legacy and int(legacy["count"]):
            capacity_leases = [{"lane": "unclassified_legacy", "count": int(legacy["count"])}]
    latest_snapshot = _row(
        conn,
        "SELECT observed_at,lane,used_percent,resets_at,window_minutes,source "
        "FROM signal_desk_model_capacity_snapshots WHERE model=? ORDER BY id DESC LIMIT 1",
        (EXPECTED_MODEL,),
    )
    adaptive = _row(
        conn,
        "SELECT * FROM signal_desk_adaptive_concurrency_state WHERE lane='gold'",
    )
    events = _rows(
        conn,
        "SELECT outcome,latency_seconds,occurred_at FROM signal_desk_adaptive_concurrency_events "
        "WHERE lane='gold' AND occurred_at>=? ORDER BY id",
        (recent_after,),
    )
    outcomes: dict[str, int] = {}
    for event in events:
        key = str(event.get("outcome") or "unknown")
        outcomes[key] = outcomes.get(key, 0) + 1
    total_outcomes = sum(outcomes.values())
    capacity_errors = int(outcomes.get("rate_limit", 0))
    ownership_events = _rows(
        conn,
        "SELECT lane,event_type,reason,backend_message,COUNT(*) AS count "
        "FROM signal_desk_model_capacity_events "
        "WHERE model=? AND occurred_at>=? "
        "GROUP BY lane,event_type,reason,backend_message ORDER BY lane,event_type",
        (
            EXPECTED_MODEL,
            datetime.fromtimestamp(recent_after, timezone.utc).isoformat(),
        ),
    )
    hour_events = _rows(
        conn,
        "SELECT lane,event_type,reason,backend_message,task_key,occurred_at "
        "FROM signal_desk_model_capacity_events "
        "WHERE model=? AND occurred_at>=? ORDER BY id",
        (
            EXPECTED_MODEL,
            datetime.fromtimestamp(now_epoch - 3600, timezone.utc).isoformat(),
        ),
    )
    gold_successes_hour = [
        event for event in hour_events
        if event.get("lane") == "gpt_5_6_sol_gold_authoring"
        and event.get("event_type") == "success"
    ]
    adjudicated_windows_hour = sum(
        ":C:" in str(event.get("task_key") or "") for event in gold_successes_hour
    )
    active_reservations = _rows(
        conn,
        "SELECT id,task_key,created_at,provider_started,reserved_tokens "
        "FROM signal_desk_gold_budget_reservations WHERE status='active' ORDER BY created_at",
    )
    stale = []
    for reservation in active_reservations:
        age = max(0, int(now_epoch - _utc_timestamp(str(reservation["created_at"]))))
        if age >= orphan_after:
            stale.append({
                "reservation_id": reservation["id"],
                "task_key": reservation["task_key"],
                "age_seconds": age,
                "provider_started": bool(reservation["provider_started"]),
                "reserved_tokens": int(reservation["reserved_tokens"]),
            })
    state = str(capacity.get("state") or "unknown")
    error_rate = capacity_errors / total_outcomes if total_outcomes else 0.0
    retry_after = 0
    if capacity.get("next_probe_at"):
        retry_after = max(0, int(_utc_timestamp(str(capacity["next_probe_at"])) - now_epoch))
    if stale or state == "unknown":
        health, recommended, reason = "red", 0, "orphaned_or_unknown_capacity_state"
    elif state == "open" and retry_after > 0:
        health, recommended, reason = "red", 0, "provider_capacity_backoff"
    elif state in {"open", "half_open"}:
        health, recommended, reason = "yellow", 1, "serialized_capacity_probe"
    elif error_rate > float(policy["health"]["capacity_error_rate_red"]):
        health, recommended, reason = "yellow", 1, "recent_provider_capacity_errors"
    else:
        effective = int((adaptive or {}).get("effective_limit") or 2)
        configured = int(policy["foreground_override"]["configured_concurrency"])
        provider_cap = int(policy["foreground_override"]["provider_concurrency_cap"])
        health, recommended, reason = (
            "green", min(effective, configured, provider_cap), "healthy_adaptive_capacity"
        )
    weekly = None
    if latest_snapshot:
        weekly = {
            **latest_snapshot,
            "remaining_percent": max(0.0, 100.0 - float(latest_snapshot["used_percent"])),
            "age_seconds": max(0, int(now_epoch - _utc_timestamp(str(latest_snapshot["observed_at"])))),
        }
    return {
        "schema_version": PULSE_VERSION,
        "measured_at": at or now_iso(),
        "model": EXPECTED_MODEL,
        "health": health,
        "decision": {
            "recommended_provider_concurrency": recommended,
            "reason": reason,
            "active_background_lane": policy["active_background_lane"],
        },
        "weekly_quota": weekly,
        "provider_capacity": {
            "state": state,
            "consecutive_capacity_failures": int(capacity.get("consecutive_capacity_failures") or 0),
            "successful_probe_count": int(capacity.get("successful_probe_count") or 0),
            "last_error_code": capacity.get("last_error_code"),
            "retry_after_seconds": retry_after,
            "active_admissions_by_lane": capacity_leases,
            "recent_window_seconds": history_seconds,
            "recent_outcomes": outcomes,
            "recent_capacity_error_rate": error_rate,
            "recent_events_by_lane": ownership_events,
        },
        "work": {
            "active_reservations": len(active_reservations),
            "stale_provider_reservations": stale,
            "adaptive": adaptive,
        },
        "telemetry": {
            "successful_gold_passes_per_hour": len(gold_successes_hour),
            "adjudicated_windows_per_hour": adjudicated_windows_hour,
            "in_flight_by_lane": capacity_leases,
        },
        "priorities": policy["lanes"],
        "privacy": "counts_capacity_metadata_and_task_keys_only_no_prompt_transcript_output_account_or_thread_ids",
    }


def write_capacity_pulse(path: Path, pulse: Mapping[str, Any]) -> None:
    target = path.expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(dict(pulse), handle, indent=2, sort_keys=True)
        handle.write("\n")
    os.chmod(temporary, 0o600)
    os.replace(temporary, target)
    os.chmod(target, 0o600)


def append_capacity_report(path: Path, pulse: Mapping[str, Any]) -> None:
    """Append a privacy-safe ten-minute operational report."""

    target = path.expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "schema_version": "pif_signal_desk_model_capacity_report_v1",
        "measured_at": pulse.get("measured_at"),
        "health": pulse.get("health"),
        "decision": pulse.get("decision"),
        "weekly_quota": pulse.get("weekly_quota"),
        "provider_capacity": pulse.get("provider_capacity"),
        "telemetry": pulse.get("telemetry"),
    }
    with target.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(report, sort_keys=True, separators=(",", ":")) + "\n")
    os.chmod(target, 0o600)
