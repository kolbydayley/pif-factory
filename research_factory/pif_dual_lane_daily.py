"""Daily dual-lane bulk roll: run every unfrozen cheap lane at its tier cap,
evaluate the receipt, and record the tier tick.

Idempotent per calendar day: a lane already recorded in today's roll receipt
is skipped, so re-runs never draft past the daily cap. Lanes run sequentially
(the bulk runner holds a global single-writer lock on the shadow DB).

Usage:
    python3 -m research_factory.pif_dual_lane_daily [--date YYYY-MM-DD] [--lanes glm,grok]

Exit code 1 if any lane's day was red (scheduler-visible).
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Optional

from .lane_profiles import LANE_PROFILES
from .pif_lane_promotion import tier_state_path, tier_tick

PIF_ROOT = Path.home() / "pif-factory"
SHADOW_ROOT = PIF_ROOT / "work" / "bulk-drafts"
ROLL_ROOT = PIF_ROOT / "work" / "pif-ops" / "bulk-daily"

# Green thresholds for a tier day. Deliberately looser than qualification
# gates: they detect an unhealthy day (provider outage, quality regression),
# not re-litigate qualification. GLM's accepted tier-1 debut (88/100 drafted,
# audit pass 0.923) clears them with margin.
GREEN_MIN_DRAFT_FRACTION = 0.8
GREEN_MIN_AUDIT_PASS = 0.8

# Consumption proxy. Neither SuperGrok nor OpenCode Go exposes a remaining-
# quota API, so lane call counts stand in for quota pressure. The grok weekly
# call budget derives from the observed dashboard reading on 2026-08-14:
# ~580 CLI calls had consumed 54% of the weekly SuperGrok Lite window
# (window resets Wednesdays ~08:48 ET) => capacity ~1,075 calls/week. 1000 is
# the conservative planning number; re-derive it if the plan tier changes.
# 1000 was the 2026-08-14 dashboard-derived estimate; 2026-08-18 the pool
# 402'd ("usage balance exhausted") at ~657 successful calls (757 attempted),
# so 650 is the measured planning number.
WEEKLY_CALL_BUDGETS = {"grok": 650}
QUOTA_WARN_FRACTION = 0.8


def consumption_summary(lane: str, day_receipt: Optional[Dict[str, Any]],
                        now: Optional[float] = None) -> Dict[str, Any]:
    """Per-lane consumption proxy: today's calls + failure taxonomy, the
    trailing-7-day call total from stored receipts, and a weekly projection
    against the lane's call budget (where one is known)."""
    import time as _time
    now = now or _time.time()
    week_ago = now - 7 * 86400
    calls_7d = 0
    for path in SHADOW_ROOT.glob(f"receipt-bulk-{lane}-*.json"):
        # run-ids are timestamps: the char after the lane prefix must be a
        # digit, else "glm-*" would also swallow "glm-zai-*" receipts.
        suffix = path.name[len(f"receipt-bulk-{lane}-"):]
        if not (suffix and suffix[0].isdigit()):
            continue
        if path.stat().st_mtime >= week_ago:
            try:
                calls_7d += json.loads(path.read_text()).get("calls_made") or 0
            except (json.JSONDecodeError, OSError):
                continue
    summary: Dict[str, Any] = {
        "calls_today": (day_receipt or {}).get("calls_made"),
        "failures_today": (day_receipt or {}).get("failure_counts") or {},
        "calls_7d": calls_7d,
    }
    budget = WEEKLY_CALL_BUDGETS.get(lane)
    if budget:
        summary["weekly_call_budget"] = budget
        summary["budget_used_fraction"] = round(calls_7d / budget, 3)
        if calls_7d >= QUOTA_WARN_FRACTION * budget:
            summary["quota_warning"] = (
                f"{lane} trailing-7d calls {calls_7d} >= "
                f"{QUOTA_WARN_FRACTION:.0%} of weekly budget {budget}")
    if lane in ("glm", "glm-zai"):
        zai = zai_quota_snapshot()
        if zai is not None:
            summary["zai_plan_percentages"] = zai
    return summary


def zai_quota_snapshot() -> Optional[Dict[str, Any]]:
    """Best-effort live read of the z.ai coding-plan quota API — the one GLM
    route with programmatic visibility. Returns window percentages, or None
    when the key is absent or the API unreachable. Never raises."""
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
        limits = (data.get("data") or {}).get("limits") or []
        return {f"{l.get('type')}_{l.get('unit')}_{l.get('number')}":
                l.get("percentage") for l in limits}
    except Exception:  # noqa: BLE001 - monitoring must never break the roll
        return None


def evaluate_receipt(receipt: Optional[Dict[str, Any]], cap: int, *,
                     require_audit: bool = True) -> Dict[str, Any]:
    """Pure green/red decision for one lane-day from its bulk-run receipt.

    ``require_audit=False`` is for the codex quality lane, which skips audit
    sampling (the reference model auditing itself is circular).
    """
    if receipt is None:
        return {"green": False, "reason": "no_receipt"}
    if receipt.get("aborted"):
        return {"green": False, "reason": f"aborted:{receipt['aborted']}"}
    drafted = receipt.get("drafted") or 0
    if drafted < GREEN_MIN_DRAFT_FRACTION * cap:
        return {"green": False, "reason": f"drafted_{drafted}_below_{GREEN_MIN_DRAFT_FRACTION:.0%}_of_{cap}"}
    if require_audit:
        audit_pass = receipt.get("audit_pass_rate")
        if audit_pass is None:
            return {"green": False, "reason": "no_audit_sample"}
        if audit_pass < GREEN_MIN_AUDIT_PASS:
            return {"green": False, "reason": f"audit_pass_{audit_pass}_below_{GREEN_MIN_AUDIT_PASS}"}
    return {"green": True, "reason": None}


def newest_receipt(lane: str, not_before: float) -> Optional[Dict[str, Any]]:
    prefix = f"receipt-bulk-{lane}-"
    paths = sorted((p for p in SHADOW_ROOT.glob(f"{prefix}*.json")
                    if p.name[len(prefix):][:1].isdigit()),
                   key=lambda p: p.stat().st_mtime, reverse=True)
    for path in paths:
        if path.stat().st_mtime >= not_before:
            return json.loads(path.read_text())
    return None


def _provider_quota_exhausted(receipt: Dict[str, Any]) -> bool:
    """True when a run drafted nothing and every call failed provider-side."""
    calls = receipt.get("calls_made") or 0
    failures = receipt.get("failure_counts") or {}
    return (calls > 0 and not receipt.get("drafted")
            and failures.get("provider", 0) >= calls)


def _runner_lock_held(stdout: Optional[str]) -> bool:
    """True when the bulk runner aborted because another run holds the
    single-writer lock (it prints one JSON line and exits 0, no receipt)."""
    for line in (stdout or "").splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            if json.loads(line).get("aborted") == "runner_lock_held":
                return True
        except json.JSONDecodeError:
            continue
    return False


def roll_lane(lane: str, date: str) -> Dict[str, Any]:
    state_path = tier_state_path(lane)
    if not state_path.exists():
        return {"lane": lane, "skipped": "no_tier_state"}
    state = json.loads(state_path.read_text())
    if state.get("frozen"):
        return {"lane": lane, "skipped": "frozen"}
    cap = state.get("daily_cap")
    count = cap if cap else 400  # uncapped tier still rolls in bounded chunks
    started = dt.datetime.now().timestamp()
    try:
        proc = subprocess.run(
            [sys.executable, "-B", "-m", "research_factory.pif_bulk_draft_runner",
             "--lane", lane, "--count", str(count), "--audit-rate", "0.12"],
            capture_output=True, text=True, cwd=str(PIF_ROOT), timeout=10 * 3600)
    except subprocess.TimeoutExpired:
        # Per-segment drafts are already stored; record a red day, never crash
        # the roller (a crash skips the tier tick and the day receipt).
        tick = tier_tick(lane, date, green=False)
        return {"lane": lane, "cap": cap, "green": False,
                "reason": "runner_timeout_10h",
                "consumption": consumption_summary(lane, None),
                "tier_after": tick}
    if _runner_lock_held(proc.stdout):
        # Another bulk run (manual or a second roll entry point) holds the
        # single-writer lock. That is contention, not a lane failure: record
        # a retryable skip and DO NOT tier-tick — a red tick here would let a
        # scheduler catch-up run zero a streak while a legitimate run is
        # mid-flight (observed 2026-08-17).
        return {"lane": lane, "skipped": "runner_lock_held"}
    receipt = newest_receipt(lane, not_before=started)
    if receipt and _provider_quota_exhausted(receipt):
        # Every call failed provider-side with nothing drafted: the weekly
        # pool is spent (grok 402s at ~650 successful calls/week, measured
        # 2026-08-18) or the provider is down. Neither is a lane-quality
        # regression, so skip without a tier tick; the lane resumes on the
        # provider's reset.
        return {"lane": lane, "skipped": "provider_quota_exhausted_or_outage",
                "consumption": consumption_summary(lane, receipt)}
    verdict = evaluate_receipt(
        receipt, count,
        require_audit=LANE_PROFILES.get(lane, {}).get("audit", True))
    if proc.returncode != 0 and verdict["green"]:
        verdict = {"green": False, "reason": f"runner_exit_{proc.returncode}"}
    tick = tier_tick(lane, date, green=verdict["green"])
    return {"lane": lane, "cap": cap, "green": verdict["green"],
            "reason": verdict["reason"],
            "run_id": (receipt or {}).get("run_id"),
            "drafted": (receipt or {}).get("drafted"),
            "audit_pass_rate": (receipt or {}).get("audit_pass_rate"),
            "throughput_per_hour": (receipt or {}).get("throughput_per_hour"),
            "consumption": consumption_summary(lane, receipt),
            "tier_after": tick}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=dt.date.today().isoformat())
    parser.add_argument("--lanes", default="glm,glm-zai,grok,codex")
    args = parser.parse_args()

    ROLL_ROOT.mkdir(parents=True, exist_ok=True)
    day_path = ROLL_ROOT / f"{args.date}.json"
    day: Dict[str, Any] = json.loads(day_path.read_text()) if day_path.exists() else {
        "date": args.date, "lanes": {}}

    for lane in [l.strip() for l in args.lanes.split(",") if l.strip()]:
        if lane in day["lanes"] and not day["lanes"][lane].get("skipped"):
            print(f"[{lane}] already rolled on {args.date}; skipping", flush=True)
            continue
        print(f"[{lane}] rolling...", flush=True)
        result = roll_lane(lane, args.date)
        day["lanes"][lane] = result
        day_path.write_text(json.dumps(day, indent=1))
        print(json.dumps(result, indent=1), flush=True)

    red = [l for l, r in day["lanes"].items() if not r.get("skipped") and not r.get("green")]
    print(json.dumps({"date": args.date, "red_lanes": red}, indent=1))
    raise SystemExit(1 if red else 0)


if __name__ == "__main__":
    main()
