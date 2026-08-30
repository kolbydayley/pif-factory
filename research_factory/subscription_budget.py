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
  refused for that budget day. A valid prior-day receipt is archived
  automatically after rollover; malformed, same-day, and future-dated
  receipts remain fail-closed.

The ledger is append-only rows in ``pif_subscription_budget_ledger``; the
daily receipt (``pif_subscription_daily_budget_ledger_v1``) generalizes the
per-campaign ``pif_instrumented_backfill_budget_ledger_v1`` shape to one
receipt per calendar day under ``work/pif-ops/budget/<day>/``.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Mapping

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
MAX_AUTHORIZED_DAILY_CAP_TOKENS = 50_000_000
# At 120% of the cap the KILL file engages: something is bypassing the
# before-dispatch gate (a crash loop, a parallel driver) and everything stops.
KILL_MULTIPLIER = 1.2


def _authorized_cap_override(directory: Path, *, day: str) -> tuple[int, str] | None:
    """Return a bounded, owner-authorized cap for exactly one budget day."""

    path = directory / "AUTHORIZED_CAP.json"
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        cap = payload["cap_tokens"]
        if (
            payload.get("day") != day
            or payload.get("authorized_by") != "kolby"
            or not isinstance(cap, int)
            or isinstance(cap, bool)
            or cap < DEFAULT_DAILY_CAP_TOKENS
            or cap > MAX_AUTHORIZED_DAILY_CAP_TOKENS
        ):
            return None
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
        return None
    return cap, str(path)


def _retire_prior_day_kill(kill_path: Path, *, day: str) -> Path | None:
    """Archive a valid prior-day kill receipt; otherwise fail closed."""

    if not kill_path.exists():
        return None
    try:
        payload = json.loads(kill_path.read_text(encoding="utf-8"))
        kill_day_text = payload["day"]
        if not isinstance(kill_day_text, str):
            return None
        kill_day = date.fromisoformat(kill_day_text)
        current_day = date.fromisoformat(day)
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
        return None
    if kill_day >= current_day:
        return None

    archive_dir = kill_path.parent / kill_day_text
    archive_dir.mkdir(parents=True, exist_ok=True)
    digest = sha256_text(dumps_json(payload))[:12]
    archive_path = archive_dir / f"KILL-retired-{digest}.json"
    try:
        kill_path.replace(archive_path)
    except FileNotFoundError:
        # Another executor retired the same shared gate first.
        return None
    return archive_path


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


def _connection_database_path(conn: sqlite3.Connection) -> Path | None:
    for row in conn.execute("PRAGMA database_list"):
        name = row[1]
        filename = row[2]
        if name == "main" and filename:
            return Path(filename).resolve()
    return None


def _tokens_used_from_readonly_database(path: Path, *, day: str) -> int:
    if not path.exists():
        return 0
    other = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    try:
        table = other.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' "
            "AND name='pif_subscription_budget_ledger'"
        ).fetchone()
        if table is None:
            return 0
        row = other.execute(
            "SELECT COALESCE(SUM(tokens), 0) "
            "FROM pif_subscription_budget_ledger WHERE day = ?",
            (day,),
        ).fetchone()
        return int(row[0] if row is not None else 0)
    finally:
        other.close()


def budget_gate(
    conn: sqlite3.Connection,
    *,
    day: str,
    cap_tokens: int | None = None,
    budget_dir: Path | None = None,
    additional_budget_db_paths: Iterable[Path] | None = None,
) -> dict[str, Any]:
    """Decide whether dispatch is allowed right now. Fail closed.

    A valid prior-day KILL receipt is archived on rollover. Same-day,
    malformed, and future-dated receipts continue to block dispatch.
    """

    directory = budget_dir if budget_dir is not None else default_budget_dir()
    directory.mkdir(parents=True, exist_ok=True)
    resolved_cap = (
        DEFAULT_DAILY_CAP_TOKENS if cap_tokens is None else int(cap_tokens)
    )
    cap_source = "default" if cap_tokens is None else "argument"
    authorized_cap_path = None
    if cap_tokens is None:
        authorized = _authorized_cap_override(directory, day=day)
        if authorized is not None:
            resolved_cap, authorized_cap_path = authorized
            cap_source = "owner_authorized_day_override"
    kill_path = directory / "KILL"
    retired_kill_path = _retire_prior_day_kill(kill_path, day=day)
    current_used = tokens_used(conn, day=day)
    current_path = _connection_database_path(conn)
    additional_used = 0
    counted_paths: list[str] = []
    for candidate in additional_budget_db_paths or ():
        resolved = Path(candidate).resolve()
        if resolved == current_path or str(resolved) in counted_paths:
            continue
        additional_used += _tokens_used_from_readonly_database(resolved, day=day)
        counted_paths.append(str(resolved))
    used = current_used + additional_used
    kill_engaged = used >= int(resolved_cap * KILL_MULTIPLIER)
    if kill_engaged and not kill_path.exists():
        kill_path.write_text(
            dumps_json(
                {
                    "engaged_at": now_iso(),
                    "day": day,
                    "tokens_used": used,
                    "cap_tokens": resolved_cap,
                    "reason": "usage_reached_120_percent_of_daily_cap",
                }
            )
            + "\n",
            encoding="utf-8",
        )
    if kill_path.exists():
        allowed = False
        reason = "kill_file_present"
    elif used >= resolved_cap:
        allowed = False
        reason = "daily_cap_reached"
    else:
        allowed = True
        reason = None
    return {
        "day": day,
        "cap_tokens": resolved_cap,
        "cap_source": cap_source,
        "authorized_cap_path": authorized_cap_path,
        "tokens_used": used,
        "tokens_used_current_database": current_used,
        "tokens_used_additional_databases": additional_used,
        "additional_budget_db_paths": counted_paths,
        "remaining_tokens": max(0, resolved_cap - used),
        "allowed": allowed,
        "reason": reason,
        "kill_engaged": kill_engaged or kill_path.exists(),
        "kill_path": str(kill_path),
        "retired_kill_path": (
            str(retired_kill_path) if retired_kill_path is not None else None
        ),
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
