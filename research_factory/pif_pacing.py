"""Deadline pacing for the cheap-lane bulk fleet.

Kolby's ruling 2026-08-25: pace every lane to land at ~95% of its quota
window RIGHT BEFORE the window resets, instead of idling at fixed tier caps.
Each roll re-reads the live usage signal and re-derives today's count, so the
pace self-corrects as consumption drifts.

Per-lane signal and deadline:
- glm-zai: live z.ai quota API (weekly TOKENS_LIMIT window percentage +
  nextResetTime). calls-per-percent is measured from trailing receipts.
- grok:    trailing receipt calls vs the measured 650-call weekly budget;
  window anchored Wednesdays 08:48 ET (observed SuperGrok reset).
- codex:   the budget governor's weekly snapshot vs the ARTIFICIAL cap Kolby
  set (lane_profiles draft_budget_cap_points); calls-per-point measured
  55-60 on 2026-08-14 -- 55 is the conservative planning number so the paced
  count never overshoots the cap mid-run.
- glm (OpenCode Go): no programmatic quota visibility -- returns None and
  the tier-ladder cap stays authoritative.

Fail-closed: any missing/garbled signal returns None and the caller falls
back to the tier cap. A paced count of 0 means "window budget spent: skip
today, do not red-tick".
"""
from __future__ import annotations

import datetime as dt
import json
import time
from pathlib import Path
from typing import Any, Dict, Optional

from .lane_profiles import LANE_PROFILES
from .pif_budget_governor import WeeklyLedger, allowance, read_weekly_snapshot

PIF_ROOT = Path.home() / "pif-factory"
SHADOW_ROOT = PIF_ROOT / "work" / "bulk-drafts"
CODEX_SESSIONS = Path.home() / ".codex" / "sessions"
DRAFT_LEDGER_DB = SHADOW_ROOT / "codex_draft_ledger.sqlite"

TARGET_FRACTION = 0.95        # land at 95% of the window, not 100%
CODEX_CALLS_PER_POINT = 55.0  # measured 2026-08-14 (55-60); low = conservative
GROK_WEEKLY_BUDGET = 650      # measured 402-exhaustion 2026-08-18
# Hard per-day ceilings: a pacing bug must never be able to ask a lane for a
# genuinely unbounded day. Chosen ~2x the max plausible paced day.
HARD_DAY_MAX = {"glm-zai": 3200, "grok": 650, "codex": 480}
SECONDS_PER_DAY = 86400.0


def _receipt_calls_since(lane: str, since: float) -> int:
    """Sum calls_made across this lane's bulk receipts newer than ``since``."""
    total = 0
    prefix = f"receipt-bulk-{lane}-"
    for path in SHADOW_ROOT.glob(f"{prefix}*.json"):
        suffix = path.name[len(prefix):]
        if not (suffix and suffix[0].isdigit()):
            continue  # keep glm-* from swallowing glm-zai-*
        try:
            if path.stat().st_mtime >= since:
                total += json.loads(path.read_text()).get("calls_made") or 0
        except (OSError, json.JSONDecodeError):
            continue
    return total


def _clamp_count(lane: str, raw: float) -> int:
    return int(max(0, min(HARD_DAY_MAX[lane], round(raw))))


def pace_glm_zai(now: Optional[float] = None,
                 quota: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    """Deadline-pace the z.ai lane against its weekly TOKENS_LIMIT window."""
    now = now or time.time()
    if quota is None:
        quota = _zai_weekly_window()
    if not quota:
        return None
    pct = quota.get("percentage")
    reset_ms = quota.get("nextResetTime")
    if not isinstance(pct, (int, float)) or isinstance(pct, bool):
        return None
    pct = float(pct)
    if isinstance(reset_ms, (int, float)) and reset_ms / 1000.0 > now:
        days_left = min(7.0, max(0.25, (reset_ms / 1000.0 - now) / SECONDS_PER_DAY))
        deadline_source = "api_reset_time"
    else:
        # Stale/absent reset: assume uniform burn over a 7-day window.
        days_left = max(0.5, 7.0 * (1.0 - pct / 100.0))
        deadline_source = "uniform_burn_estimate"
    calls_7d = _receipt_calls_since("glm-zai", now - 7 * SECONDS_PER_DAY)
    if calls_7d <= 0 or pct <= 0:
        # No consumption history to calibrate against: keep a known-safe day.
        return {"lane": "glm-zai", "count": 1600, "reason": "uncalibrated_default",
                "window_percent": pct, "days_left": round(days_left, 2)}
    calls_per_percent = calls_7d / max(pct, 1.0)
    percent_remaining = max(0.0, TARGET_FRACTION * 100.0 - pct)
    raw = (percent_remaining / days_left) * calls_per_percent
    return {"lane": "glm-zai", "count": _clamp_count("glm-zai", raw),
            "reason": "deadline_pacing", "window_percent": pct,
            "days_left": round(days_left, 2),
            "calls_per_percent": round(calls_per_percent, 1),
            "percent_remaining": round(percent_remaining, 1),
            "deadline_source": deadline_source}


def _zai_weekly_window() -> Optional[Dict[str, Any]]:
    """The weekly (longest-period) TOKENS_LIMIT entry from the z.ai quota API."""
    try:
        auth = json.loads((Path.home() / ".local" / "share" / "opencode"
                           / "auth.json").read_text())
        key = auth.get("zai-coding-plan", {}).get("key")
        if not key:
            return None
        import urllib.request
        req = urllib.request.Request(
            "https://api.z.ai/api/monitor/usage/quota/limit",
            headers={"Authorization": f"Bearer {key}"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
        tokens = [l for l in (data.get("data") or {}).get("limits") or []
                  if l.get("type") == "TOKENS_LIMIT"]
        if not tokens:
            return None
        # The weekly window is the one with the furthest reset.
        return max(tokens, key=lambda l: l.get("nextResetTime") or 0)
    except Exception:  # noqa: BLE001 - pacing must never break the roll
        return None


def _grok_window_start(now: float) -> float:
    """Most recent Wednesday 08:48 ET (12:48 UTC during EDT)."""
    anchor = dt.datetime.fromtimestamp(now, dt.timezone.utc).replace(
        hour=12, minute=48, second=0, microsecond=0)
    days_back = (anchor.weekday() - 2) % 7  # Wednesday = 2
    start = anchor - dt.timedelta(days=days_back)
    if start.timestamp() > now:
        start -= dt.timedelta(days=7)
    return start.timestamp()


def pace_grok(now: Optional[float] = None) -> Dict[str, Any]:
    now = now or time.time()
    window_start = _grok_window_start(now)
    reset_at = window_start + 7 * SECONDS_PER_DAY
    used = _receipt_calls_since("grok", window_start)
    remaining = max(0.0, TARGET_FRACTION * GROK_WEEKLY_BUDGET - used)
    days_left = max(0.25, (reset_at - now) / SECONDS_PER_DAY)
    raw = min(remaining, remaining / days_left * 1.25)  # mild front-load
    return {"lane": "grok", "count": _clamp_count("grok", raw),
            "reason": "deadline_pacing", "window_calls_used": used,
            "budget": GROK_WEEKLY_BUDGET, "days_left": round(days_left, 2),
            "resets_at": int(reset_at)}


def pace_codex(now: Optional[float] = None,
               snapshot: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    """Pace the codex quality lane to Kolby's artificial weekly point cap."""
    now = now or time.time()
    snapshot = snapshot or read_weekly_snapshot(CODEX_SESSIONS)
    if not snapshot:
        return None
    cap = LANE_PROFILES["codex"]["draft_budget_cap_points"]
    budget = allowance(snapshot, WeeklyLedger(DRAFT_LEDGER_DB),
                       cap_points=cap, now=int(now))
    if not budget.get("allowed"):
        return {"lane": "codex", "count": 0, "reason": budget.get("reason"),
                "points_used": budget.get("points_used"), "cap_points": cap}
    # points_today already deadline-paces (remaining / days-to-reset, borrow
    # factor 2); convert points to calls conservatively.
    raw = budget["points_today"] * CODEX_CALLS_PER_POINT
    return {"lane": "codex", "count": _clamp_count("codex", raw),
            "reason": "deadline_pacing",
            "points_remaining": round(budget["points_remaining"], 2),
            "points_today": round(budget["points_today"], 2),
            "cap_points": cap, "days_left": round(budget["days_left"], 2)}


def paced_count(lane: str, now: Optional[float] = None) -> Optional[Dict[str, Any]]:
    """Today's paced count for a lane, or None when the tier cap should rule."""
    if lane == "glm-zai":
        return pace_glm_zai(now)
    if lane == "grok":
        return pace_grok(now)
    if lane == "codex":
        return pace_codex(now)
    return None  # glm/opencode-go: no quota visibility, ladder cap rules


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Report today's paced counts.")
    parser.add_argument("--lanes", default="glm,glm-zai,grok,codex")
    args = parser.parse_args()
    out = {}
    for lane in [l.strip() for l in args.lanes.split(",") if l.strip()]:
        out[lane] = paced_count(lane) or {"lane": lane, "count": None,
                                          "reason": "tier_cap_rules"}
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
