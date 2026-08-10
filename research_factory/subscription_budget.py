"""Daily subscription-token budget ledger.

The July-August 2026 production wave consumed ~1.03B Codex subscription tokens
in six days without tripping any gate, because the cost gate only detects
metered API billing: subscription spend read as $0. This module makes
subscription tokens a first-class budgeted resource.

Kolby's rulings (2026-08-10):

- Codex is judge/audit-only; the daily cap is **5,000,000 subscription
  tokens** across all lanes (episode_context, labels, audits, backfills).
- The cap is enforced **before dispatch** at the executor choke point.
- At >=120% of the cap a ``KILL`` file engages and every further dispatch is
  refused, on any day, until a human removes the file.

The ledger is append-only rows in ``pif_subscription_budget_ledger``; the
daily receipt (``pif_subscription_daily_budget_ledger_v1``) generalizes the
per-campaign ``pif_instrumented_backfill_budget_ledger_v1`` shape to one
receipt per calendar day under ``work/pif-ops/budget/<day>/``.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Mapping

from .paths import root
from .util import dumps_json, now_iso, sha256_text, stable_id

__all__ = [
    "DEFAULT_DAILY_CAP_TOKENS",
    "KILL_MULTIPLIER",
    "ensure_budget_schema",
    "record_usage",
    "tokens_used",
    "budget_gate",
    "write_daily_budget_receipt",
    "usage_tokens_from_item",
    "default_budget_dir",
]


# Kolby's ruling 2026-08-10: hard daily ceiling for subscription tokens.
DEFAULT_DAILY_CAP_TOKENS = 5_000_000
# At 120% of the cap the KILL file engages: something is bypassing the
# before-dispatch gate (a crash loop, a parallel driver) and everything stops.
KILL_MULTIPLIER = 1.2


def default_budget_dir() -> Path:
    return root() / "work" / "pif-ops" / "budget"


def ensure_budget_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pif_subscription_budget_ledger (
          id TEXT PRIMARY KEY,
          day TEXT NOT NULL,
          provider_lane TEXT NOT NULL,
          lane TEXT NOT NULL,
          run_id TEXT NOT NULL,
          tokens INTEGER NOT NULL CHECK(tokens >= 0),
          provider_calls INTEGER NOT NULL CHECK(provider_calls >= 0),
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_subscription_budget_day
          ON pif_subscription_budget_ledger(day)
        """
    )


def record_usage(
    conn: sqlite3.Connection,
    *,
    day: str,
    provider_lane: str,
    lane: str,
    run_id: str,
    tokens: int,
    provider_calls: int,
) -> str:
    """Append one usage row; returns the row id. Rows are never mutated."""

    ensure_budget_schema(conn)
    created_at = now_iso()
    row_id = stable_id(
        day, provider_lane, lane, run_id, created_at, prefix="psb_"
    )
    conn.execute(
        """
        INSERT INTO pif_subscription_budget_ledger
          (id, day, provider_lane, lane, run_id, tokens, provider_calls,
           created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row_id,
            day,
            provider_lane,
            lane,
            run_id,
            max(0, int(tokens or 0)),
            max(0, int(provider_calls or 0)),
            created_at,
        ),
    )
    return row_id


def tokens_used(conn: sqlite3.Connection, *, day: str) -> int:
    ensure_budget_schema(conn)
    row = conn.execute(
        "SELECT COALESCE(SUM(tokens), 0) AS total "
        "FROM pif_subscription_budget_ledger WHERE day = ?",
        (day,),
    ).fetchone()
    if row is None:
        return 0
    try:
        return int(row["total"])
    except (TypeError, KeyError, IndexError):
        return int(row[0])


def budget_gate(
    conn: sqlite3.Connection,
    *,
    day: str,
    cap_tokens: int = DEFAULT_DAILY_CAP_TOKENS,
    budget_dir: Path | None = None,
) -> dict[str, Any]:
    """Decide whether dispatch is allowed right now. Fail closed.

    The KILL file blocks every day, not just the day that engaged it: an
    engaged kill means the before-dispatch gate was bypassed, and only a human
    removing the file may re-open dispatch.
    """

    directory = budget_dir if budget_dir is not None else default_budget_dir()
    directory.mkdir(parents=True, exist_ok=True)
    kill_path = directory / "KILL"
    used = tokens_used(conn, day=day)
    kill_engaged = used >= int(cap_tokens * KILL_MULTIPLIER)
    if kill_engaged and not kill_path.exists():
        kill_path.write_text(
            dumps_json(
                {
                    "engaged_at": now_iso(),
                    "day": day,
                    "tokens_used": used,
                    "cap_tokens": cap_tokens,
                    "reason": "usage_reached_120_percent_of_daily_cap",
                }
            )
            + "\n",
            encoding="utf-8",
        )
    if kill_path.exists():
        allowed = False
        reason = "kill_file_present"
    elif used >= cap_tokens:
        allowed = False
        reason = "daily_cap_reached"
    else:
        allowed = True
        reason = None
    return {
        "day": day,
        "cap_tokens": int(cap_tokens),
        "tokens_used": used,
        "remaining_tokens": max(0, int(cap_tokens) - used),
        "allowed": allowed,
        "reason": reason,
        "kill_engaged": kill_engaged or kill_path.exists(),
        "kill_path": str(kill_path),
    }


def usage_tokens_from_item(item: Mapping[str, Any]) -> int:
    """Billable tokens for one executor item (input + output, uncached and
    cached alike — the subscription meters them all)."""

    usage = item.get("usage")
    if not isinstance(usage, Mapping):
        return 0
    total = int(usage.get("total_tokens") or 0)
    if total:
        return total
    return int(usage.get("input_tokens") or 0) + int(
        usage.get("output_tokens") or 0
    )


def write_daily_budget_receipt(
    conn: sqlite3.Connection,
    *,
    day: str,
    cap_tokens: int = DEFAULT_DAILY_CAP_TOKENS,
    budget_dir: Path | None = None,
) -> dict[str, Any]:
    ensure_budget_schema(conn)
    directory = budget_dir if budget_dir is not None else default_budget_dir()
    by_lane: dict[str, dict[str, int]] = {}
    total_tokens = 0
    total_calls = 0
    for row in conn.execute(
        """
        SELECT lane, provider_lane, SUM(tokens) AS tokens,
               SUM(provider_calls) AS calls
        FROM pif_subscription_budget_ledger
        WHERE day = ?
        GROUP BY lane, provider_lane
        ORDER BY lane, provider_lane
        """,
        (day,),
    ):
        lane_key = str(row["lane"])
        entry = by_lane.setdefault(
            lane_key,
            {"tokens": 0, "provider_calls": 0},
        )
        entry["tokens"] += int(row["tokens"] or 0)
        entry["provider_calls"] += int(row["calls"] or 0)
        total_tokens += int(row["tokens"] or 0)
        total_calls += int(row["calls"] or 0)
    base = {
        "schema_version": "pif_subscription_daily_budget_ledger_v1",
        "day": day,
        "cap_tokens": int(cap_tokens),
        "total_tokens": total_tokens,
        "total_provider_calls": total_calls,
        "cap_utilization": (
            round(total_tokens / cap_tokens, 6) if cap_tokens else None
        ),
        "by_lane": by_lane,
        "created_at": now_iso(),
    }
    receipt = {**base, "receipt_sha256": sha256_text(dumps_json(base))}
    out_dir = directory / day
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "budget-receipt.json").write_text(
        dumps_json(receipt) + "\n", encoding="utf-8"
    )
    return receipt
