"""Cheap-lane promotion, tier ladder, and auto-freeze.

Promotion flips ``config/provider_policy.json`` (label_segment -> winning
lane) ONLY when a qualification report shows every gate green, commits the
one-line policy edit referencing the report, initializes tier state, and
queues a Telegram notice via ``codex-ops notify``. Freeze reverts to Codex
bounded mode; un-freezing requires a fresh passed qualification, never a
timer.

Tier ladder: 100 -> 400 -> 1600 -> uncapped (None), promoted on 7-day
calendar-continuous green streaks; a missing day resets the streak.

Usage:
    python3 -m research_factory.pif_lane_promotion promote --report <report.json> --lane glm
    python3 -m research_factory.pif_lane_promotion freeze --lane glm --reason "audit breach"
    python3 -m research_factory.pif_lane_promotion tier-tick --lane glm --date 2026-08-14 --green
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
from pathlib import Path
from typing import Any, Dict, Optional

PIF_ROOT = Path.home() / "pif-factory"
POLICY_PATH = PIF_ROOT / "config" / "provider_policy.json"
TIER_STATE_PATH = PIF_ROOT / "work" / "bulk-drafts" / "tier_state.json"


def tier_state_path(lane: str) -> Path:
    """Per-lane tier state. glm keeps the legacy single-file path (live state
    predates dual lanes); every other lane gets its own file."""
    if lane == "glm":
        return TIER_STATE_PATH
    return TIER_STATE_PATH.with_name(f"tier_state_{lane}.json")

TIERS = [100, 400, 1600, None]  # None = uncapped
STREAK_DAYS_REQUIRED = 7

LANE_PROVIDERS = {
    "glm": {"provider": "glm_opencode", "model": "opencode-go/glm-5.2"},
    "grok": {"provider": "grok_cli", "model": "grok-4.6"},
}


def _load(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text())


def _save(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2) + "\n")


def _git_commit(paths, message: str) -> None:
    subprocess.run(["git", "add", *[str(p) for p in paths]], cwd=str(PIF_ROOT), check=True)
    subprocess.run(["git", "commit", "-m", message], cwd=str(PIF_ROOT), check=True,
                   capture_output=True, text=True)


def _notify(title: str, body: str) -> None:
    try:
        subprocess.run(["codex-ops", "notify", "--title", title, "--body", body],
                       check=False, capture_output=True, text=True, timeout=60)
    except (OSError, subprocess.TimeoutExpired):
        pass  # notification is best-effort; the receipt/commit is the record


def gates_green(report: Dict[str, Any], lane: str) -> bool:
    lane_data = (report.get("lanes") or {}).get(lane) or report.get(lane) or {}
    return bool(lane_data.get("gates_passed"))


def promote(report_path: Path, lane: str) -> Dict[str, Any]:
    report = _load(report_path)
    if not gates_green(report, lane):
        raise SystemExit(f"refusing promotion: lane '{lane}' gates not green in {report_path}")
    if lane not in LANE_PROVIDERS:
        raise SystemExit(f"unknown lane '{lane}'")
    policy = _load(POLICY_PATH)
    stage = policy["stages"]["label_segment"]
    previous = {"provider": stage["provider"], "model": stage["model"]}
    if lane == "grok" and "grok_cli" not in policy["providers"]:
        policy["providers"]["grok_cli"] = {
            "transport": "grok_build_cli_headless",
            "billing": "supergrok_subscription",
        }
    stage["provider"] = LANE_PROVIDERS[lane]["provider"]
    stage["model"] = LANE_PROVIDERS[lane]["model"]
    stage["frozen_fallback"] = previous
    stage["promotion_report"] = str(report_path.relative_to(PIF_ROOT)) \
        if str(report_path).startswith(str(PIF_ROOT)) else str(report_path)
    _save(POLICY_PATH, policy)
    tier_state = {
        "lane": lane, "tier_index": 0, "daily_cap": TIERS[0],
        "streak_days": 0, "last_green_date": None, "frozen": False,
        "promoted_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "promotion_report": stage["promotion_report"],
    }
    _save(tier_state_path(lane), tier_state)
    _git_commit([POLICY_PATH],
                f"feat(policy): promote {lane} to label_segment bulk lane\n\n"
                f"Qualification report: {stage['promotion_report']}\n\n"
                f"Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>")
    _notify(f"PIF: {lane} promoted to bulk labeling",
            f"Gates green in {stage['promotion_report']}; tier 100/day, Codex audit-only.")
    return {"promoted": lane, "previous": previous, "tier_state": tier_state}


def freeze(lane: str, reason: str) -> Dict[str, Any]:
    policy = _load(POLICY_PATH)
    stage = policy["stages"]["label_segment"]
    fallback = stage.get("frozen_fallback") or {"provider": "codex", "model": "gpt-5.5"}
    stage["provider"] = fallback["provider"]
    stage["model"] = fallback["model"]
    stage["frozen_reason"] = reason
    _save(POLICY_PATH, policy)
    lane_tier_path = tier_state_path(lane)
    if lane_tier_path.exists():
        tier_state = _load(lane_tier_path)
        tier_state["frozen"] = True
        tier_state["freeze_reason"] = reason
        _save(lane_tier_path, tier_state)
    _git_commit([POLICY_PATH],
                f"fix(policy): auto-freeze {lane} bulk lane -> Codex bounded\n\n"
                f"Reason: {reason}\n\n"
                f"Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>")
    _notify(f"PIF: {lane} lane FROZEN", f"Reverted to Codex bounded 25/day. Reason: {reason}")
    return {"frozen": lane, "reason": reason}


def init_tier(lane: str, report_path: Path) -> Dict[str, Any]:
    """Initialize tier state for a lane WITHOUT touching provider policy.

    This is the shadow second-lane path: the lane drafts the backlog at the
    tier ladder's caps while the label_segment primary stays as-is. Requires a
    qualification report with green gates, same bar as promote().
    """
    report = _load(report_path)
    if not gates_green(report, lane):
        raise SystemExit(f"refusing tier init: lane '{lane}' gates not green in {report_path}")
    path = tier_state_path(lane)
    if path.exists() and not _load(path).get("frozen"):
        raise SystemExit(f"tier state for '{lane}' already exists at {path}")
    rel_report = (str(report_path.relative_to(PIF_ROOT))
                  if str(report_path).startswith(str(PIF_ROOT)) else str(report_path))
    tier_state = {
        "lane": lane, "tier_index": 0, "daily_cap": TIERS[0],
        "streak_days": 0, "last_green_date": None, "frozen": False,
        "promoted_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "promotion_report": rel_report,
        "shadow_lane": True,
    }
    _save(path, tier_state)
    _notify(f"PIF: {lane} shadow bulk lane initialized",
            f"Gates green in {rel_report}; tier 100/day, Codex audit-only.")
    return tier_state


def tier_tick(lane: str, date: str, green: bool) -> Dict[str, Any]:
    """Record one day's outcome; promote tier on a 7-day continuous streak."""
    tier_state = _load(tier_state_path(lane))
    if tier_state.get("lane") != lane:
        raise SystemExit(f"tier state belongs to '{tier_state.get('lane')}', not '{lane}'")
    if tier_state.get("frozen"):
        raise SystemExit("lane is frozen; tier ticks require re-qualification first")
    today = dt.date.fromisoformat(date)
    last = tier_state.get("last_green_date")
    if not green:
        tier_state["streak_days"] = 0
    else:
        contiguous = last is not None and dt.date.fromisoformat(last) == today - dt.timedelta(days=1)
        tier_state["streak_days"] = (tier_state["streak_days"] + 1) if contiguous else 1
        tier_state["last_green_date"] = date
        if (tier_state.get("ladder", True)
                and tier_state["streak_days"] >= STREAK_DAYS_REQUIRED
                and tier_state["tier_index"] < len(TIERS) - 1):
            tier_state["tier_index"] += 1
            tier_state["daily_cap"] = TIERS[tier_state["tier_index"]]
            tier_state["streak_days"] = 0
            _notify(f"PIF: {lane} tier promoted",
                    f"New daily cap: {tier_state['daily_cap'] or 'uncapped'}")
    _save(tier_state_path(lane), tier_state)
    return tier_state


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("promote")
    p.add_argument("--report", type=Path, required=True)
    p.add_argument("--lane", required=True)
    f = sub.add_parser("freeze")
    f.add_argument("--lane", required=True)
    f.add_argument("--reason", required=True)
    i = sub.add_parser("init-tier")
    i.add_argument("--report", type=Path, required=True)
    i.add_argument("--lane", required=True)
    t = sub.add_parser("tier-tick")
    t.add_argument("--lane", required=True)
    t.add_argument("--date", required=True)
    t.add_argument("--green", action="store_true")
    args = parser.parse_args()
    if args.command == "promote":
        print(json.dumps(promote(args.report, args.lane), indent=1))
    elif args.command == "freeze":
        print(json.dumps(freeze(args.lane, args.reason), indent=1))
    elif args.command == "init-tier":
        print(json.dumps(init_tier(args.lane, args.report), indent=1))
    else:
        print(json.dumps(tier_tick(args.lane, args.date, args.green), indent=1))


if __name__ == "__main__":
    main()
