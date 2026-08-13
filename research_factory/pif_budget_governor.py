"""Codex weekly budget governor for PIF cheap-lane supervision.

Attributes PIF's Codex consumption as percentage-points of the live weekly
rate-limit window (before/after ``used_percent`` deltas) and enforces a hard
cap (default 30 points) with per-day pacing. Fail-closed: a missing or
malformed snapshot yields ``allowed=False``.

Spec: docs/superpowers/specs/2026-08-13-glm-primary-labeling-budget-governor-design.md
"""
from __future__ import annotations

import glob
import json
import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional

DAY_SECONDS = 86400
DEFAULT_CAP_POINTS = 30.0
BORROW_FACTOR = 2.0


def _find_rate_limits(obj: Any) -> Optional[Dict[str, Any]]:
    if isinstance(obj, dict):
        if "rate_limits" in obj and isinstance(obj["rate_limits"], dict):
            return obj["rate_limits"]
        for value in obj.values():
            found = _find_rate_limits(value)
            if found:
                return found
    if isinstance(obj, list):
        for value in obj:
            found = _find_rate_limits(value)
            if found:
                return found
    return None


def read_weekly_snapshot(session_root: Path) -> Optional[Dict[str, Any]]:
    """Newest weekly rate-limit snapshot from Codex session rollout logs."""
    pattern = str(Path(session_root) / "**" / "rollout-*.jsonl")
    files = sorted(glob.glob(pattern, recursive=True), key=os.path.getmtime, reverse=True)
    for path in files[:30]:
        best_line = None
        try:
            with open(path, errors="replace") as fh:
                for line in fh:
                    if '"rate_limit' in line:
                        best_line = line
        except OSError:
            continue
        if not best_line:
            continue
        try:
            limits = _find_rate_limits(json.loads(best_line))
        except json.JSONDecodeError:
            continue
        if not limits:
            continue
        primary = limits.get("primary")
        if not isinstance(primary, dict):
            continue
        used = primary.get("used_percent")
        resets = primary.get("resets_at")
        if not isinstance(used, (int, float)) or isinstance(used, bool):
            continue
        if not isinstance(resets, int) or isinstance(resets, bool):
            continue
        return {
            "used_percent": float(used),
            "resets_at": resets,
            "window_minutes": primary.get("window_minutes"),
            "source_file": os.path.basename(path),
        }
    return None


class WeeklyLedger:
    """Append-only ledger of PIF-attributed weekly percentage points."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS weekly_points (
                       id INTEGER PRIMARY KEY,
                       run_id TEXT NOT NULL,
                       resets_at INTEGER NOT NULL,
                       points REAL NOT NULL,
                       before_percent REAL NOT NULL,
                       after_percent REAL NOT NULL,
                       created_at TEXT NOT NULL DEFAULT (datetime('now'))
                   )""")

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def record(self, before: Dict[str, Any], after: Dict[str, Any], *, run_id: str) -> float:
        """Attribute after-before delta to the after snapshot's window.

        A window rollover mid-batch (before.resets_at != after.resets_at, or a
        negative delta) clamps to the after-window's used_percent: everything
        visible in the new window is conservatively attributed to us.
        """
        if before.get("resets_at") == after.get("resets_at"):
            points = max(0.0, float(after["used_percent"]) - float(before["used_percent"]))
        else:
            points = max(0.0, float(after["used_percent"]))
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO weekly_points (run_id, resets_at, points, before_percent, after_percent)"
                " VALUES (?, ?, ?, ?, ?)",
                (run_id, int(after["resets_at"]), points,
                 float(before["used_percent"]), float(after["used_percent"])))
        return points

    def points_used(self, resets_at: int) -> float:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COALESCE(SUM(points), 0.0) FROM weekly_points WHERE resets_at = ?",
                (int(resets_at),)).fetchone()
        return float(row[0])


def allowance(snapshot: Optional[Dict[str, Any]], ledger: WeeklyLedger, *,
              cap_points: float = DEFAULT_CAP_POINTS, now: int) -> Dict[str, Any]:
    """Remaining PIF allowance in the current weekly window, fail-closed."""
    if not snapshot or "resets_at" not in snapshot:
        return {"allowed": False, "reason": "no_weekly_snapshot",
                "points_remaining": 0.0, "points_today": 0.0}
    resets_at = int(snapshot["resets_at"])
    used = ledger.points_used(resets_at)
    remaining = max(0.0, float(cap_points) - used)
    if remaining <= 0.0:
        return {"allowed": False, "reason": "weekly_cap_reached",
                "points_remaining": 0.0, "points_today": 0.0,
                "points_used": used, "resets_at": resets_at}
    days_left = max(1.0, (resets_at - now) / DAY_SECONDS)
    per_day = remaining / days_left
    points_today = min(remaining, per_day * BORROW_FACTOR)
    return {"allowed": True, "reason": None,
            "points_remaining": remaining, "points_today": points_today,
            "points_used": used, "resets_at": resets_at,
            "days_left": days_left}
