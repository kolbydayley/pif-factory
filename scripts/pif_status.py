#!/usr/bin/env python3
"""One-command PIF health scoreboard. Read-only; safe to run anytime.

Usage: python3 scripts/pif_status.py [--json]
"""
import argparse
import datetime as dt
import json
import sqlite3
import sys
from pathlib import Path

PIF_ROOT = Path.home() / "pif-factory"
sys.path.insert(0, str(PIF_ROOT))

from research_factory.pif_budget_governor import read_weekly_snapshot  # noqa: E402
from research_factory.pif_dual_lane_daily import (  # noqa: E402
    WEEKLY_CALL_BUDGETS, consumption_summary, zai_quota_snapshot)

CANON = PIF_ROOT / "data" / "factory.sqlite"
SHADOW = PIF_ROOT / "work" / "bulk-drafts" / "drafts.sqlite"
LANES = ("glm-zai", "glm", "grok", "codex")


def collect() -> dict:
    canon = sqlite3.connect(f"file:{CANON}?mode=ro", uri=True)
    shadow = sqlite3.connect(f"file:{SHADOW}?mode=ro", uri=True)
    today = dt.date.today().isoformat()

    labels_by_pack = dict(canon.execute(
        "SELECT label_pack, COUNT(*) FROM labels GROUP BY label_pack"))
    total_segments = canon.execute("SELECT COUNT(*) FROM segments").fetchone()[0]
    labeled_segments = canon.execute(
        "SELECT COUNT(DISTINCT segment_id) FROM labels").fetchone()[0]

    gate = canon.execute(
        "SELECT run_date, json_extract(receipt_json,'$.genuinely_successful'),"
        "       json_extract(receipt_json,'$.consecutive_success_days'),"
        "       json_extract(receipt_json,'$.cohort_tier'),"
        "       json_extract(receipt_json,'$.promotion_eligible')"
        " FROM pif_scale_gate_state_receipts"
        " ORDER BY created_at DESC LIMIT 1").fetchone()

    drafts_today = dict(shadow.execute(
        "SELECT lane, COUNT(*) FROM draft_labels WHERE date(created_at)=?"
        " GROUP BY lane", (today,)))
    drafts_7d = shadow.execute(
        "SELECT COUNT(*) FROM draft_labels"
        " WHERE created_at >= datetime('now','-7 days')").fetchone()[0]
    unpromoted = shadow.execute(
        "SELECT COUNT(*) FROM draft_labels WHERE promoted_at IS NULL"
    ).fetchone()[0]
    audit_7d = shadow.execute(
        "SELECT ROUND(AVG(CASE WHEN json_extract(audit_json,'$.verdict')='pass'"
        " THEN 1.0 ELSE 0 END),3), COUNT(*) FROM draft_labels"
        " WHERE audit_json IS NOT NULL"
        "   AND created_at >= datetime('now','-7 days')").fetchone()

    tiers = {}
    for lane in LANES:
        suffix = "" if lane == "glm" else f"_{lane}"
        path = PIF_ROOT / "work" / "bulk-drafts" / f"tier_state{suffix}.json"
        if path.exists():
            s = json.loads(path.read_text())
            tiers[lane] = {k: s.get(k) for k in
                           ("daily_cap", "streak_days", "last_green_date",
                            "frozen")}

    backlog = total_segments - labeled_segments
    rate = drafts_7d / 7 if drafts_7d else 0
    codex = read_weekly_snapshot(Path.home() / ".codex" / "sessions") or {}
    grok = consumption_summary("grok", None)

    canon.close()
    shadow.close()
    return {
        "as_of": dt.datetime.now().isoformat(timespec="seconds"),
        "production_gate": {
            "last_run": gate[0] if gate else None,
            "green": bool(gate[1]) if gate else None,
            "streak_days": gate[2] if gate else None,
            "cohort_tier": gate[3] if gate else None,
            "promotion_eligible": bool(gate[4]) if gate else None,
        },
        "canonical_labels": {
            "total": sum(labels_by_pack.values()),
            "by_pack": labels_by_pack,
        },
        "funnel": {
            "segments_total": total_segments,
            "segments_labeled": labeled_segments,
            "backlog_unlabeled": backlog,
            "eta_days_at_7d_rate": round(backlog / rate, 1) if rate else None,
        },
        "drafting": {
            "today_by_lane": drafts_today,
            "today_total": sum(drafts_today.values()),
            "trailing_7d": drafts_7d,
            "audit_pass_7d": audit_7d[0],
            "audit_sample_7d": audit_7d[1],
            "shadow_awaiting_promotion": unpromoted,
        },
        "tiers": tiers,
        "quota": {
            "zai_percent_used": zai_quota_snapshot(),
            "grok_calls_7d": grok.get("calls_7d"),
            "grok_weekly_budget": WEEKLY_CALL_BUDGETS.get("grok"),
            "codex_weekly_used_percent": codex.get("used_percent"),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    status = collect()
    if args.json:
        print(json.dumps(status, indent=1))
        return
    g = status["production_gate"]
    f = status["funnel"]
    d = status["drafting"]
    q = status["quota"]
    print(f"PIF status @ {status['as_of']}")
    print(f"  production gate : {'GREEN' if g['green'] else 'RED'} "
          f"streak {g['streak_days']}d, tier {g['cohort_tier']}"
          f"{' (promotion eligible)' if g['promotion_eligible'] else ''}")
    print(f"  canonical labels: {status['canonical_labels']['total']:,} "
          f"({json.dumps(status['canonical_labels']['by_pack'])})")
    print(f"  backlog         : {f['backlog_unlabeled']:,} unlabeled of "
          f"{f['segments_total']:,} segments"
          f" — ETA {f['eta_days_at_7d_rate']} days at current pace")
    print(f"  drafts today    : {d['today_total']:,} {d['today_by_lane']}")
    print(f"  trailing 7d     : {d['trailing_7d']:,} drafts, audit pass "
          f"{d['audit_pass_7d']} (n={d['audit_sample_7d']}), "
          f"{d['shadow_awaiting_promotion']:,} awaiting promotion")
    for lane, t in status["tiers"].items():
        print(f"  lane {lane:8s}   : cap {t['daily_cap']}, streak "
              f"{t['streak_days']}d, last green {t['last_green_date']}"
              f"{' FROZEN' if t.get('frozen') else ''}")
    print(f"  quota           : zai {q['zai_percent_used']}, grok "
          f"{q['grok_calls_7d']}/{q['grok_weekly_budget']} calls, codex week "
          f"{q['codex_weekly_used_percent']}%")


if __name__ == "__main__":
    main()
