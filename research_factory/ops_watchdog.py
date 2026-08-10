"""Deterministic ops watchdog for the durable PIF system (Phase 4).

Checks, all filesystem-only and side-effect-free except the optional report:

- ``lane_dead``: newest headless log is older than the threshold (or absent) —
  the Aug 6-8 outage ran three days before anyone noticed;
- ``budget_kill_engaged``: the subscription-budget KILL file exists;
- ``agreement_freeze_engaged``: any provider-agreement FREEZE_<stage> exists;
- ``missing_daily_receipt``: yesterday has no daily receipt directory — the
  scale-gate streak is about to reset for a *scheduling* reason, which is
  exactly the failure the same-day rerun window exists for.

The watchdog observes and reports; it never remediates, never dispatches, and
never mutates state beyond its own report file.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any

from .paths import root
from .util import now_iso, sha256_text

__all__ = ["run_watchdog", "LANE_DEAD_HOURS"]

# The daily cadence is one cycle per day; 30h tolerates jitter without
# letting a dead lane run a second full day unnoticed.
LANE_DEAD_HOURS = 30.0


def _yesterday() -> str:
    return (dt.date.today() - dt.timedelta(days=1)).isoformat()


def _newest_mtime(directory: Path) -> float | None:
    if not directory.is_dir():
        return None
    newest: float | None = None
    for entry in directory.iterdir():
        if not entry.is_file():
            continue
        stamp = entry.stat().st_mtime
        if newest is None or stamp > newest:
            newest = stamp
    return newest


def run_watchdog(
    *,
    project_root: Path | None = None,
    lane_dead_hours: float = LANE_DEAD_HOURS,
    write_report: bool = False,
) -> dict[str, Any]:
    base = project_root if project_root is not None else root()
    alerts: list[dict[str, Any]] = []
    now = dt.datetime.now().timestamp()

    newest = _newest_mtime(base / "runs" / "headless_logs")
    if newest is None:
        alerts.append(
            {
                "kind": "lane_dead",
                "detail": "no headless logs exist at all",
            }
        )
    else:
        age_hours = (now - newest) / 3600.0
        if age_hours > lane_dead_hours:
            alerts.append(
                {
                    "kind": "lane_dead",
                    "detail": (
                        f"newest headless log is {age_hours:.1f}h old "
                        f"(threshold {lane_dead_hours:.0f}h)"
                    ),
                }
            )

    kill_path = base / "work" / "pif-ops" / "budget" / "KILL"
    if kill_path.exists():
        alerts.append(
            {
                "kind": "budget_kill_engaged",
                "detail": str(kill_path),
            }
        )

    agreement_dir = base / "work" / "pif-ops" / "agreement"
    if agreement_dir.is_dir():
        for freeze in sorted(agreement_dir.glob("FREEZE_*")):
            alerts.append(
                {
                    "kind": "agreement_freeze_engaged",
                    "detail": str(freeze),
                }
            )

    yesterday = _yesterday()
    receipt_dir = base / "work" / "pif-ops" / "daily" / yesterday
    if not receipt_dir.is_dir():
        alerts.append(
            {
                "kind": "missing_daily_receipt",
                "detail": (
                    f"no daily receipt directory for {yesterday}; the scale "
                    "streak resets unless the same-day rerun window is used"
                ),
            }
        )

    day = now_iso()[:10]
    base_report = {
        "schema_version": "pif_ops_watchdog_report_v1",
        "day": day,
        "created_at": now_iso(),
        "project_root": str(base),
        "lane_dead_threshold_hours": float(lane_dead_hours),
        "alerts": alerts,
        "ok": not alerts,
    }
    report = {
        **base_report,
        "report_sha256": sha256_text(
            json.dumps(base_report, sort_keys=True)
        ),
    }
    if write_report:
        out_dir = base / "work" / "pif-ops" / "watchdog"
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / f"{day}.json").write_text(
            json.dumps(report, indent=1, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    return report


if __name__ == "__main__":
    import sys

    result = run_watchdog(write_report=True)
    print(json.dumps(result, indent=1, sort_keys=True))
    sys.exit(0 if result["ok"] else 1)
