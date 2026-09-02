import sqlite3

import pytest

from research_factory.signal_desk_adaptive_concurrency import (
    GOLD_BOUNDS,
    LaneBounds,
    admission_limit,
    initialize_lane,
    lane_status,
    record_outcome,
)


def _conn():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return conn


def _calls(conn, *, lane, outcome="success", latency=1.0, start=1.0, count=100):
    for offset in range(count):
        state = record_outcome(
            conn, lane=lane, outcome=outcome, latency_seconds=latency,
            now=start + offset / 1000,
        )
    return state


@pytest.mark.parametrize(
    ("outcome", "count", "latencies", "reason"),
    [
        ("rate_limit", 3, None, "rate_limit_over_2pct"),
        ("timeout", 6, None, "timeout_over_5pct"),
        ("parse_schema", 3, None, "parse_schema_over_2pct"),
        ("success", 100, [1.0] * 94 + [91.0] * 6, "p95_latency_exceeded"),
    ],
)
def test_trip_thresholds_reduce_by_two_and_persist(outcome, count, latencies, reason):
    conn = _conn()
    bounds = LaneBounds(1, 12, 90.0)
    initialize_lane(conn, lane="glm-test", bounds=bounds, initial_limit=12, now=0)
    if latencies is not None:
        for index, latency in enumerate(latencies):
            state = record_outcome(
                conn, lane="glm-test", outcome="success", latency_seconds=latency,
                now=1 + index / 1000,
            )
    else:
        _calls(conn, lane="glm-test", start=1, count=100 - count)
        state = _calls(conn, lane="glm-test", outcome=outcome, start=2, count=count)
    assert state["effective_limit"] == 10
    assert state["last_trip_reason"] == reason
    # Reinitializing on another runner preserves the reduced current ceiling.
    assert initialize_lane(conn, lane="glm-test", bounds=bounds, initial_limit=12, now=5)["effective_limit"] == 10


def test_no_success_for_fifteen_minutes_trips_on_next_admission():
    conn = _conn()
    initialize_lane(conn, lane="gold", bounds=GOLD_BOUNDS, initial_limit=4, now=0)
    record_outcome(conn, lane="gold", outcome="failure", latency_seconds=2, now=1)
    state = admission_limit(conn, lane="gold", now=901)
    assert state["effective_limit"] == 2
    assert state["last_trip_reason"] == "no_success_15m"


def test_recovery_waits_for_cooldown_and_three_healthy_windows():
    conn = _conn()
    bounds = LaneBounds(1, 8, 90.0)
    initialize_lane(conn, lane="glm-recover", bounds=bounds, initial_limit=6, now=0)
    _calls(conn, lane="glm-recover", start=1, count=97)
    tripped = _calls(conn, lane="glm-recover", outcome="rate_limit", start=2, count=3)
    assert tripped["effective_limit"] == 4
    # Healthy windows during cooldown do not recover the limit.
    _calls(conn, lane="glm-recover", start=100, count=100)
    assert lane_status(conn, lane="glm-recover", now=100)["effective_limit"] == 4
    for start in (701, 702, 703):
        state = _calls(conn, lane="glm-recover", start=start, count=100)
    assert state["effective_limit"] == 5
    assert state["healthy_windows"] == 0
