"""Quota days freeze the scale-gate streak instead of resetting it (Phase 0.2).

Kolby's ruling 2026-08-10: a day lost to Codex subscription exhaustion is not a
pipeline quality failure. It must not count as a success, and it must not
restart the 7-day promotion clock.
"""

from __future__ import annotations

import json
import sqlite3

from research_factory import daily_cycle as dc
from research_factory.daily_cycle import ensure_daily_schema


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    ensure_daily_schema(conn)
    return conn


def _insert_receipt(
    conn: sqlite3.Connection,
    *,
    run_date: str,
    successful: bool,
    quota_frozen: bool = False,
) -> None:
    receipt = {
        "quota_frozen": quota_frozen,
        "genuinely_successful": successful,
    }
    conn.execute(
        """
        INSERT INTO pif_scale_gate_state_receipts
          (id, daily_run_id, run_date, corpus_release_id, cohort_tier,
           cohort_item_count, next_tier, genuinely_successful,
           consecutive_success_days, promotion_eligible, gate_json,
           receipt_sha256, receipt_json, created_at)
        VALUES (?, ?, ?, 'crel_test', '25', 25, '100', ?, 0, 0, '{}', ?, ?, ?)
        """,
        (
            f"psg_{run_date}",
            f"pdr_{run_date}",
            run_date,
            1 if successful else 0,
            "0" * 64,
            json.dumps(receipt),
            f"{run_date}T12:00:00Z",
        ),
    )


def test_quota_days_are_transparent_to_the_streak() -> None:
    conn = _conn()
    _insert_receipt(conn, run_date="2026-08-05", successful=True)
    _insert_receipt(
        conn, run_date="2026-08-06", successful=False, quota_frozen=True
    )
    _insert_receipt(
        conn, run_date="2026-08-07", successful=False, quota_frozen=True
    )
    streak = dc._consecutive_success_days(
        conn,
        release_id="crel_test",
        tier="25",
        run_date="2026-08-08",
        include_current=True,
    )
    # 08-08 (current) + frozen 08-07/06 skipped + 08-05 = 2 successes.
    assert streak == 2


def test_ordinary_failure_days_still_reset_the_streak() -> None:
    conn = _conn()
    _insert_receipt(conn, run_date="2026-08-05", successful=True)
    _insert_receipt(conn, run_date="2026-08-06", successful=False)
    streak = dc._consecutive_success_days(
        conn,
        release_id="crel_test",
        tier="25",
        run_date="2026-08-07",
        include_current=True,
    )
    assert streak == 1


def test_missing_days_still_reset_the_streak() -> None:
    conn = _conn()
    _insert_receipt(conn, run_date="2026-08-05", successful=True)
    # No receipt at all for 08-06: an unexplained gap is not frozen.
    streak = dc._consecutive_success_days(
        conn,
        release_id="crel_test",
        tier="25",
        run_date="2026-08-07",
        include_current=True,
    )
    assert streak == 1


def test_all_frozen_history_yields_only_the_current_day() -> None:
    conn = _conn()
    _insert_receipt(
        conn, run_date="2026-08-06", successful=False, quota_frozen=True
    )
    streak = dc._consecutive_success_days(
        conn,
        release_id="crel_test",
        tier="25",
        run_date="2026-08-07",
        include_current=True,
    )
    assert streak == 1


def test_quota_skip_is_a_named_blocker_not_a_generic_defer() -> None:
    truth = dc._assess_required_stage_truth(
        [
            {
                "stage_name": "bounded_baseline_extraction",
                "status": "skipped",
                "result": {
                    "provider_quota_exhausted": True,
                    "usage_limit_retry_at": "Aug 8th, 2026 1:30 AM",
                    "work_due": True,
                    "healthy_no_work": False,
                },
            }
        ]
    )
    reasons = [blocker["reason"] for blocker in truth["blockers"]]
    assert reasons == ["provider_quota_exhausted"]
