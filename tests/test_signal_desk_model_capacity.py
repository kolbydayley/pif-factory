from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from research_factory.signal_desk_model_capacity import (
    ModelCapacityError,
    build_capacity_pulse,
    ensure_capacity_pulse_schema,
    load_capacity_policy,
    record_capacity_snapshot,
    write_capacity_pulse,
)


ROOT = Path(__file__).resolve().parents[1]
POLICY = ROOT / "config/signal_desk_model_capacity_policy.json"


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    ensure_capacity_pulse_schema(conn)
    conn.execute(
        """CREATE TABLE signal_desk_gold_capacity_state (
             model TEXT PRIMARY KEY,state TEXT,consecutive_capacity_failures INTEGER,
             successful_probe_count INTEGER,next_probe_at TEXT,last_error_code TEXT,updated_at TEXT
           )"""
    )
    conn.execute(
        """CREATE TABLE signal_desk_gold_capacity_leases (
             admission_id TEXT PRIMARY KEY,model TEXT,task_key TEXT,lease_owner TEXT,
             lease_until TEXT,created_at TEXT,lane TEXT
           )"""
    )
    conn.execute(
        """CREATE TABLE signal_desk_adaptive_concurrency_state (
             lane TEXT PRIMARY KEY,effective_limit INTEGER,minimum_limit INTEGER,
             maximum_limit INTEGER,latency_p95_seconds REAL,cooldown_until REAL,
             healthy_windows INTEGER,last_success_at REAL,last_evaluated_at REAL,
             last_evaluated_event_id INTEGER,last_trip_reason TEXT,updated_at REAL
           )"""
    )
    conn.execute(
        """CREATE TABLE signal_desk_adaptive_concurrency_events (
             id INTEGER PRIMARY KEY,lane TEXT,occurred_at REAL,outcome TEXT,latency_seconds REAL
           )"""
    )
    conn.execute(
        """CREATE TABLE signal_desk_gold_budget_reservations (
             id TEXT PRIMARY KEY,task_key TEXT,created_at TEXT,provider_started INTEGER,
             reserved_tokens INTEGER,status TEXT
           )"""
    )
    return conn


def test_policy_is_explicit_and_frozen():
    policy = load_capacity_policy(POLICY)
    assert policy["active_background_lane"] == "gpt_5_6_sol_gold_authoring"
    assert policy["foreground_override"] == {
        "configured_concurrency": 4,
        "provider_concurrency_cap": 2,
    }


def test_policy_drift_fails_closed(tmp_path: Path):
    value = json.loads(POLICY.read_text())
    value["lanes"][1]["priority"] = 99
    path = tmp_path / "policy.json"
    path.write_text(json.dumps(value))
    with pytest.raises(ModelCapacityError):
        load_capacity_policy(path)


def test_half_open_pulse_serializes_provider_probe():
    conn = _conn()
    conn.execute(
        "INSERT INTO signal_desk_gold_capacity_state VALUES (?,?,?,?,?,?,?)",
        ("gpt-5.6-sol", "half_open", 1, 2, "2026-09-02T12:00:00+00:00", "serverOverloaded", "2026-09-02T12:00:00+00:00"),
    )
    conn.execute(
        "INSERT INTO signal_desk_adaptive_concurrency_state VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        ("gold", 4, 2, 8, 900.0, 0.0, 0, 0.0, 0.0, 0, None, 0.0),
    )
    pulse = build_capacity_pulse(
        conn,
        policy=load_capacity_policy(POLICY),
        at="2026-09-02T12:01:00+00:00",
    )
    assert pulse["health"] == "yellow"
    assert pulse["decision"]["recommended_provider_concurrency"] == 1
    assert pulse["decision"]["reason"] == "serialized_capacity_probe"


def test_stale_started_reservation_stops_new_capacity():
    conn = _conn()
    conn.execute(
        "INSERT INTO signal_desk_gold_capacity_state VALUES (?,?,?,?,?,?,?)",
        ("gpt-5.6-sol", "closed", 0, 0, None, None, "2026-09-02T12:00:00+00:00"),
    )
    conn.execute(
        "INSERT INTO signal_desk_gold_budget_reservations VALUES (?,?,?,?,?,?)",
        ("r1", "dev:C:w:attempt:1:generation:1", "2026-09-02T11:00:00+00:00", 1, 57000, "active"),
    )
    pulse = build_capacity_pulse(
        conn,
        policy=load_capacity_policy(POLICY),
        at="2026-09-02T12:01:00+00:00",
    )
    assert pulse["health"] == "red"
    assert pulse["decision"]["recommended_provider_concurrency"] == 0
    assert pulse["work"]["stale_provider_reservations"][0]["reservation_id"] == "r1"


def test_official_snapshot_is_sanitized_and_written_0600(tmp_path: Path):
    conn = _conn()
    record = record_capacity_snapshot(
        conn,
        lane="gpt_5_6_sol_gold_authoring",
        snapshot={
            "used_percent": 31.0,
            "resets_at": 1788748260,
            "window_minutes": 10080,
            "source": "app_server_live",
            "email": "must-not-survive@example.com",
        },
        observed_at="2026-09-02T12:00:00+00:00",
    )
    assert "email" not in record
    target = tmp_path / "pulse.json"
    write_capacity_pulse(target, {"schema_version": "test", "weekly": record})
    assert target.stat().st_mode & 0o777 == 0o600
    assert "must-not-survive" not in target.read_text()
