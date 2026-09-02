import sqlite3

from research_factory import signal_desk_frontier_runner as runner


def _conn():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return conn


def test_a1_reservations_count_active_work_before_dispatch(tmp_path, monkeypatch):
    conn = _conn()
    monkeypatch.setattr(runner, "subscription_budget_window", lambda: ("2026-09-01", "start"))
    monkeypatch.setattr(
        runner,
        "budget_gate",
        lambda *_args, **_kwargs: {
            "allowed": True,
            "reason": None,
            "tokens_used": 4_920_000,
            "cap_tokens": 5_000_000,
        },
    )
    first = runner.reserve_frontier_call(conn, task_key="a", budget_dir=tmp_path)
    second = runner.reserve_frontier_call(conn, task_key="b", budget_dir=tmp_path)
    assert first["allowed"] is True
    assert second["allowed"] is False
    assert second["reason"] == "daily_reservations_exhaust_cap"


def test_a1_settlement_attributes_the_distinct_non_gold_lane(tmp_path, monkeypatch):
    conn = _conn()
    monkeypatch.setattr(runner, "subscription_budget_window", lambda: ("2026-09-01", "start"))
    monkeypatch.setattr(
        runner,
        "budget_gate",
        lambda *_args, **_kwargs: {
            "allowed": True,
            "reason": None,
            "tokens_used": 0,
            "cap_tokens": 5_000_000,
        },
    )
    reservation = runner.reserve_frontier_call(conn, task_key="a", budget_dir=tmp_path)
    runner.settle_frontier_call(
        conn, reservation_id=reservation["reservation_id"], actual_tokens=12_345
    )
    row = conn.execute(
        "SELECT lane,tokens FROM pif_subscription_budget_ledger"
    ).fetchone()
    assert tuple(row) == (runner.LANE, 12_345)


def test_a1_unstarted_reservation_is_released_for_foreground_preemption(tmp_path, monkeypatch):
    conn = _conn()
    monkeypatch.setattr(runner, "subscription_budget_window", lambda: ("2026-09-01", "start"))
    monkeypatch.setattr(
        runner,
        "budget_gate",
        lambda *_args, **_kwargs: {
            "allowed": True,
            "reason": None,
            "tokens_used": 0,
            "cap_tokens": 5_000_000,
        },
    )
    reservation = runner.reserve_frontier_call(conn, task_key="foreground", budget_dir=tmp_path)
    runner.release_unstarted_frontier_reservation(
        conn, reservation_id=reservation["reservation_id"]
    )
    assert conn.execute(
        "SELECT COUNT(*) FROM signal_desk_frontier_reservations WHERE status='active'"
    ).fetchone()[0] == 0
