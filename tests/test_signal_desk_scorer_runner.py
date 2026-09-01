import sqlite3

from research_factory import signal_desk_scorer_runner as runner


def _conn():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return conn


def test_a2_uses_distinct_lane_and_counts_active_reservations(tmp_path, monkeypatch):
    conn = _conn()
    monkeypatch.setattr(runner, "subscription_budget_window", lambda: ("2026-09-01", "start"))
    monkeypatch.setattr(
        runner,
        "budget_gate",
        lambda *_args, **_kwargs: {
            "allowed": True,
            "reason": None,
            "tokens_used": 4_940_000,
            "cap_tokens": 5_000_000,
        },
    )
    first = runner.reserve_scorer_call(conn, task_key="one", budget_dir=tmp_path)
    second = runner.reserve_scorer_call(conn, task_key="two", budget_dir=tmp_path)
    assert first["allowed"] is True
    assert second["allowed"] is False
    runner.settle_scorer_call(
        conn, reservation_id=first["reservation_id"], actual_tokens=11_000
    )
    row = conn.execute("SELECT lane,tokens FROM pif_subscription_budget_ledger").fetchone()
    assert tuple(row) == (runner.LANE, 11_000)


def test_a2_system_prompt_does_not_let_issue_wording_control_matching():
    assert "Issue-label wording is diagnostic" in runner.SYSTEM_PROMPT
    assert runner.OUTPUT_SCHEMA["properties"]["expected_match"]["type"] == "boolean"
