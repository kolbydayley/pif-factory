import sqlite3

import pytest

from research_factory.signal_desk_adaptive_concurrency import (
    GOLD_BOUNDS,
    LaneBounds,
    admission_limit,
    gold_clock_period,
    initialize_lane,
    lane_status,
    record_outcome,
    set_effective_limit,
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
    shoulder = 14 * 3600
    initialize_lane(conn, lane="gold", bounds=GOLD_BOUNDS, initial_limit=4, now=shoulder)
    record_outcome(conn, lane="gold", outcome="failure", latency_seconds=2, now=shoulder + 1)
    state = admission_limit(conn, lane="gold", now=shoulder + 901)
    assert state["effective_limit"] == 2
    assert state["last_trip_reason"] == "no_success_15m"


def test_gold_clock_periods_and_peak_holds_without_a_capacity_event():
    assert gold_clock_period(3 * 3600) == "off_peak"
    assert gold_clock_period(14 * 3600) == "shoulder"
    assert gold_clock_period(23 * 3600) == "peak"
    conn = _conn()
    initialize_lane(conn, lane="gold", bounds=GOLD_BOUNDS, initial_limit=4, now=23 * 3600)
    record_outcome(
        conn, lane="gold", outcome="parse_schema", latency_seconds=2,
        now=23 * 3600 + 1,
    )
    state = admission_limit(conn, lane="gold", now=23 * 3600 + 901)
    assert state["effective_limit"] == 4
    assert state["clock_period"] == "peak"


def test_live_limit_adjustment_is_durable_and_does_not_require_a_stop():
    conn = _conn()
    initialize_lane(conn, lane="gold", bounds=GOLD_BOUNDS, initial_limit=2, now=0)
    state = set_effective_limit(
        conn, lane="gold", effective_limit=6, reason="healthy_off_peak", now=1
    )
    assert state["effective_limit"] == 6
    assert state["last_trip_reason"] == "operator_adjustment:healthy_off_peak"
    assert lane_status(conn, lane="gold", now=2)["effective_limit"] == 6


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


def test_trip_is_charged_once_per_event_window_not_on_every_evaluation():
    # One parse_schema event trips the lane once; later evaluations within the
    # 600s window must not re-trip on that same event (it drained 8 -> 2 on
    # 2026-09-03). Only events newer than the last trip count.
    import sqlite3
    from research_factory.signal_desk_adaptive_concurrency import (
        COOLDOWN_SECONDS,
        GOLD_BOUNDS,
        WINDOW_SECONDS,
        ensure_adaptive_concurrency_schema,
        initialize_lane,
        record_outcome,
    )

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    ensure_adaptive_concurrency_schema(conn)
    initialize_lane(conn, lane="gold", bounds=GOLD_BOUNDS, initial_limit=8)
    t = 1_800_000_000.0  # off-peak UTC hour, evaluations every 120s
    for i in range(12):
        record_outcome(conn, lane="gold", outcome="success", latency_seconds=300.0, now=t + i)
    tripped = record_outcome(conn, lane="gold", outcome="parse_schema", latency_seconds=0.0, now=t + 200)
    assert tripped["effective_limit"] == 6
    # Successes keep arriving; each later evaluation sees the same stale event.
    limit = 6
    for k in range(1, 4):
        now = t + 200 + 130 * k  # past the 120s evaluation interval each time, inside the 600s window
        for j in range(3):
            state = record_outcome(conn, lane="gold", outcome="success", latency_seconds=300.0, now=now + j)
        limit = state["effective_limit"]
    assert limit == 6, "re-evaluation must not re-trip on the already-charged event"
    # A genuinely new bad event after the trip may trip again (once cooldown
    # elapsed and the next 120s evaluation is due).
    later = t + 200 + COOLDOWN_SECONDS + 5
    for j in range(12):
        record_outcome(conn, lane="gold", outcome="success", latency_seconds=300.0, now=later + j)
    again = record_outcome(conn, lane="gold", outcome="parse_schema", latency_seconds=0.0, now=later + 130)
    assert again["effective_limit"] == 4


def test_reinitializing_a_lane_baselines_last_success_so_a_pause_is_not_an_outage():
    import sqlite3
    from research_factory.signal_desk_adaptive_concurrency import (
        GOLD_BOUNDS,
        NO_SUCCESS_SECONDS,
        ensure_adaptive_concurrency_schema,
        initialize_lane,
        lane_status,
        record_outcome,
    )

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    ensure_adaptive_concurrency_schema(conn)
    t = 1_800_000_000.0  # off-peak UTC hour
    initialize_lane(conn, lane="gold", bounds=GOLD_BOUNDS, initial_limit=8, now=t)
    record_outcome(conn, lane="gold", outcome="success", latency_seconds=300.0, now=t + 10)
    # Deliberate pause: nothing happens for far longer than NO_SUCCESS_SECONDS.
    resume = t + 10 + 4 * 3600
    # Re-initialising on resume must baseline last_success_at to now...
    initialize_lane(conn, lane="gold", bounds=GOLD_BOUNDS, initial_limit=8, now=resume)
    assert lane_status(conn, lane="gold", now=resume)["effective_limit"] == 8
    # ...so the first post-resume evaluation (a fresh event, past the 120s
    # interval) does not trip no_success_15m on the stale pre-pause timestamp.
    state = record_outcome(conn, lane="gold", outcome="success", latency_seconds=300.0, now=resume + 130)
    assert state["effective_limit"] == 8
    assert state.get("last_trip_reason") != "no_success_15m"
    # A genuine silence AFTER the restart still trips as designed.
    conn.execute("UPDATE signal_desk_adaptive_concurrency_state SET last_evaluated_at=? WHERE lane='gold'", (resume + 130,))
    conn.commit()
    silent = record_outcome(conn, lane="gold", outcome="failure", latency_seconds=1.0, now=resume + 130 + NO_SUCCESS_SECONDS + 200)
    assert silent["effective_limit"] < 8
