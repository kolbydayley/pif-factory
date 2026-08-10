"""Daily subscription-token budget ledger (durability plan Phase 1).

Kolby's rulings 2026-08-10: Codex is judge/audit-only; every lane's
subscription usage is metered against a hard 5M-token daily cap enforced
before dispatch, with a KILL file at 120%.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from research_factory import subscription_budget as sb


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    sb.ensure_budget_schema(conn)
    return conn


def test_default_cap_is_the_ruled_five_million() -> None:
    assert sb.DEFAULT_DAILY_CAP_TOKENS == 5_000_000


def test_record_and_sum_by_day() -> None:
    conn = _conn()
    sb.record_usage(
        conn,
        day="2026-08-10",
        provider_lane="codex_subscription",
        lane="labels",
        run_id="run_a",
        tokens=1_000,
        provider_calls=1,
    )
    sb.record_usage(
        conn,
        day="2026-08-10",
        provider_lane="codex_subscription",
        lane="episode_context",
        run_id="run_b",
        tokens=2_500,
        provider_calls=2,
    )
    sb.record_usage(
        conn,
        day="2026-08-09",
        provider_lane="codex_subscription",
        lane="labels",
        run_id="run_c",
        tokens=999,
        provider_calls=1,
    )
    assert sb.tokens_used(conn, day="2026-08-10") == 3_500
    assert sb.tokens_used(conn, day="2026-08-09") == 999


def test_gate_allows_under_cap_and_refuses_at_cap(tmp_path: Path) -> None:
    conn = _conn()
    sb.record_usage(
        conn,
        day="2026-08-10",
        provider_lane="codex_subscription",
        lane="labels",
        run_id="run_a",
        tokens=4_999_999,
        provider_calls=9,
    )
    gate = sb.budget_gate(conn, day="2026-08-10", budget_dir=tmp_path)
    assert gate["allowed"] is True
    assert gate["remaining_tokens"] == 1

    sb.record_usage(
        conn,
        day="2026-08-10",
        provider_lane="codex_subscription",
        lane="labels",
        run_id="run_b",
        tokens=1,
        provider_calls=1,
    )
    gate = sb.budget_gate(conn, day="2026-08-10", budget_dir=tmp_path)
    assert gate["allowed"] is False
    assert gate["reason"] == "daily_cap_reached"


def test_kill_file_engages_at_120_percent_and_blocks(tmp_path: Path) -> None:
    conn = _conn()
    sb.record_usage(
        conn,
        day="2026-08-10",
        provider_lane="codex_subscription",
        lane="labels",
        run_id="run_a",
        tokens=6_000_000,
        provider_calls=10,
    )
    gate = sb.budget_gate(conn, day="2026-08-10", budget_dir=tmp_path)
    assert gate["allowed"] is False
    assert gate["kill_engaged"] is True
    assert (tmp_path / "KILL").exists()

    # The KILL file blocks even a fresh day until a human removes it.
    gate_next = sb.budget_gate(conn, day="2026-08-11", budget_dir=tmp_path)
    assert gate_next["allowed"] is False
    assert gate_next["reason"] == "kill_file_present"


def test_daily_receipt_is_written_and_hashed(tmp_path: Path) -> None:
    conn = _conn()
    sb.record_usage(
        conn,
        day="2026-08-10",
        provider_lane="codex_subscription",
        lane="labels",
        run_id="run_a",
        tokens=1_234,
        provider_calls=3,
    )
    receipt = sb.write_daily_budget_receipt(
        conn, day="2026-08-10", budget_dir=tmp_path
    )
    assert receipt["schema_version"] == "pif_subscription_daily_budget_ledger_v1"
    assert receipt["day"] == "2026-08-10"
    assert receipt["total_tokens"] == 1_234
    assert receipt["cap_tokens"] == sb.DEFAULT_DAILY_CAP_TOKENS
    assert receipt["by_lane"]["labels"]["tokens"] == 1_234
    assert len(receipt["receipt_sha256"]) == 64
    on_disk = tmp_path / "2026-08-10" / "budget-receipt.json"
    assert on_disk.exists()


def test_usage_tokens_from_item_reads_codex_usage_shape() -> None:
    item = {
        "usage": {
            "input_tokens": 100,
            "cached_input_tokens": 40,
            "output_tokens": 25,
        }
    }
    assert sb.usage_tokens_from_item(item) == 125
    assert sb.usage_tokens_from_item({}) == 0
    assert sb.usage_tokens_from_item({"usage": None}) == 0
