#!/usr/bin/env python3
"""Golden-dataset backtest for the Signal Desk detectors.

North star (Kolby 2026-08-27): check the system against real-world trend
data — when our detectors fire, does an external ground-truth series move
the same way, and how much do we lead or lag it?

Ground truth: Wikipedia Pageviews API (free, keyless, daily, per-article,
subject-agnostic). Topic->article keyword mappings and hand-curated events
live in config/backtest_truth.json — the ONLY place domain terms appear;
this module is subject-blind.

Usage:
  python3 scripts/pif_backtest.py --start 2026-01-05 --end 2026-08-01
  python3 scripts/pif_backtest.py ... --stride 2      # replay every 2 weeks
  python3 scripts/pif_backtest.py ... --offline       # cache only, no fetch
  python3 scripts/pif_backtest.py ... --sweep         # threshold grid search

Outputs docs/signal-desk/backtest-report.md and
work/pif-ops/backtest/scores.json. Deterministic given a warm cache.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sqlite3
import statistics
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

PIF_ROOT = Path.home() / "pif-factory"
sys.path.insert(0, str(PIF_ROOT))

CONFIG_PATH = PIF_ROOT / "config" / "backtest_truth.json"
CACHE_DIR = PIF_ROOT / "work" / "pif-ops" / "backtest" / "cache"
SCORES_PATH = PIF_ROOT / "work" / "pif-ops" / "backtest" / "scores.json"
REPORT_PATH = PIF_ROOT / "docs" / "signal-desk" / "backtest-report.md"
DB_PATH = PIF_ROOT / "data" / "factory.sqlite"

WIKI_API = ("https://wikimedia.org/api/rest_v1/metrics/pageviews/"
            "per-article/en.wikipedia/all-access/user/{article}/daily/"
            "{start}/{end}")
USER_AGENT = "pif-signal-desk-backtest/1.0 (local research; contact: owner)"


# --------------------------------------------------------------------------
# Pure scoring functions (unit-tested; no I/O)
# --------------------------------------------------------------------------

def cusum_changepoints(series: Sequence[float], k: float = 0.5,
                       h: float = 4.0) -> List[int]:
    """Upward CUSUM changepoints. Baseline stats come from the first third
    of the series (>= 8 points); returns indices where the standardized
    cumulative excursion crosses h."""
    n = len(series)
    base_n = max(8, n // 3)
    if n < base_n + 2:
        return []
    base = list(series[:base_n])
    mu = statistics.fmean(base)
    sigma = statistics.pstdev(base)
    sigma = max(sigma, 0.05 * abs(mu), 1e-9)
    cps: List[int] = []
    s = 0.0
    for i in range(base_n, n):
        z = (series[i] - mu) / sigma
        s = max(0.0, s + z - k)
        if s > h:
            cps.append(i)
            s = 0.0
    return cps


def external_rise(series: Sequence[float], w: int,
                  baseline_span: Tuple[int, int] = (-10, -2),
                  window_span: Tuple[int, int] = (-2, 5),
                  lift: float = 0.25) -> bool:
    """Did the truth series rise >= lift vs its trailing baseline around
    firing week w?"""
    n = len(series)
    b_lo, b_hi = w + baseline_span[0], w + baseline_span[1]
    w_lo, w_hi = max(0, w + window_span[0]), min(n, w + window_span[1])
    # Need a full baseline, but accept a truncated post-window (>= 2
    # weeks) so firings near the end of the timeline still score.
    if b_lo < 0 or b_hi <= b_lo or w_hi - w_lo < 2:
        return False
    baseline = statistics.fmean(series[b_lo:b_hi])
    if baseline <= 0:
        return False
    peak = max(series[w_lo:w_hi])
    return peak >= (1.0 + lift) * baseline


def _pearson(a: Sequence[float], b: Sequence[float]) -> float:
    n = len(a)
    if n < 3:
        return 0.0
    ma, mb = statistics.fmean(a), statistics.fmean(b)
    va = sum((x - ma) ** 2 for x in a)
    vb = sum((x - mb) ** 2 for x in b)
    if va <= 0 or vb <= 0:
        return 0.0
    cov = sum((x - ma) * (y - mb) for x, y in zip(a, b))
    return cov / (va ** 0.5 * vb ** 0.5)


def best_lag_correlation(ours: Sequence[float], truth: Sequence[float],
                         max_lag: int = 8) -> Tuple[int, float]:
    """Best (lag, pearson r): positive lag means OUR series leads truth by
    that many weeks. Ties prefer the smallest |lag|."""
    candidates: List[Tuple[float, int]] = []
    n = min(len(ours), len(truth))
    for lag in range(-max_lag, max_lag + 1):
        pairs = [(ours[i], truth[i + lag])
                 for i in range(n) if 0 <= i + lag < n]
        if len(pairs) < 3:
            continue
        r = _pearson([p[0] for p in pairs], [p[1] for p in pairs])
        candidates.append((r, lag))
    if not candidates:
        return (0, 0.0)
    best_r = max(r for r, _ in candidates)
    best = min((lag for r, lag in candidates if r == best_r), key=abs)
    return (best, best_r)


def score_firings(firings: List[Dict],
                  truths: Dict[str, Sequence[float]]) -> Dict:
    """Per-tier precision and median lead time of detector firings against
    the mapped ground-truth series."""
    by_tier: Dict[str, Dict] = {}
    skipped = 0
    tier_hits: Dict[str, List[bool]] = {}
    tier_leads: Dict[str, List[int]] = {}
    for f in firings:
        truth = truths.get(f["topic"])
        if truth is None:
            skipped += 1
            continue
        tier = f.get("tier", "weak")
        confirmed = external_rise(truth, f["week_idx"])
        tier_hits.setdefault(tier, []).append(confirmed)
        if confirmed:
            cps = cusum_changepoints(truth)
            if cps:
                lead = min((cp - f["week_idx"] for cp in cps), key=abs)
                tier_leads.setdefault(tier, []).append(lead)
    for tier, hits in tier_hits.items():
        leads = tier_leads.get(tier, [])
        by_tier[tier] = {
            "n": len(hits),
            "precision": round(sum(hits) / len(hits), 3),
            "median_lead_weeks": (statistics.median(leads)
                                  if leads else None),
        }
    return {"by_tier": by_tier, "skipped_unmapped": skipped}


def event_recall(events: List[Dict], firings: List[Dict],
                 tolerance: int = 3) -> Dict:
    """Fraction of curated ground-truth events any detector caught within
    +/- tolerance weeks."""
    hit = 0
    for ev in events:
        topics = set(ev.get("topics", []))
        if any(f["topic"] in topics
               and abs(f["week_idx"] - ev["week_idx"]) <= tolerance
               for f in firings):
            hit += 1
    total = len(events)
    return {"hit": hit, "total": total,
            "recall": round(hit / total, 3) if total else None}


# --------------------------------------------------------------------------
# I/O shell: truth fetching, detector replay, report
# --------------------------------------------------------------------------

def load_config(path: Path = CONFIG_PATH) -> Dict:
    return json.loads(path.read_text())


def _weeks_between(start: dt.date, end: dt.date) -> List[dt.date]:
    """Mondays from the Monday of `start` through `end`."""
    monday = start - dt.timedelta(days=start.weekday())
    out = []
    while monday <= end:
        out.append(monday)
        monday += dt.timedelta(weeks=1)
    return out


def _fetch_article_daily(article: str, start: dt.date, end: dt.date,
                         offline: bool = False) -> Dict[str, int]:
    """Daily pageviews for one article, cached per (article, range)."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    key = f"{article.replace('/', '_')}_{start:%Y%m%d}_{end:%Y%m%d}.json"
    cache = CACHE_DIR / key
    if cache.exists():
        return json.loads(cache.read_text())
    if offline:
        return {}
    url = WIKI_API.format(article=urllib.request.quote(article, safe=""),
                          start=f"{start:%Y%m%d}", end=f"{end:%Y%m%d}")
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    payload = None
    for attempt in range(4):
        time.sleep(1.0 + attempt * 4.0)  # be polite; back off on 429
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                payload = json.load(resp)
            break
        except urllib.error.HTTPError as exc:
            if exc.code == 429 and attempt < 3:
                continue
            print(f"  fetch failed for {article}: {exc}", file=sys.stderr)
            return {}
        except urllib.error.URLError as exc:
            print(f"  fetch failed for {article}: {exc}", file=sys.stderr)
            return {}
    if payload is None:
        return {}
    daily = {item["timestamp"][:8]: item["views"]
             for item in payload.get("items", [])}
    cache.write_text(json.dumps(daily))
    return daily


def fetch_truth(cfg: Dict, start: dt.date, end: dt.date,
                offline: bool = False) -> Dict[str, List[float]]:
    """Weekly truth series per mapped topic (summed over its articles)."""
    weeks = _weeks_between(start, end)
    idx = {w: i for i, w in enumerate(weeks)}
    truths: Dict[str, List[float]] = {}
    for m in cfg.get("mappings", []):
        weekly = [0.0] * len(weeks)
        got = False
        for article in m.get("wikipedia", []):
            daily = _fetch_article_daily(article, start, end, offline)
            got = got or bool(daily)
            for ts, views in daily.items():
                day = dt.date(int(ts[:4]), int(ts[4:6]), int(ts[6:8]))
                monday = day - dt.timedelta(days=day.weekday())
                if monday in idx:
                    weekly[idx[monday]] += views
        if got:
            truths[m["topic"]] = weekly
    return truths


def replay_detectors(db_path: Path, as_of_dates: List[dt.date],
                     timeline: List[dt.date]) -> List[Dict]:
    """As-of replay: run collect(now=D) at each date, emit firings with a
    global week index on the shared timeline."""
    from research_factory.pif_discourse_aggregates import collect
    t_idx = {w: i for i, w in enumerate(timeline)}
    firings: List[Dict] = []
    seen = set()
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        for as_of in as_of_dates:
            monday = as_of - dt.timedelta(days=as_of.weekday())
            if monday not in t_idx:
                continue
            payload = collect(conn, now=as_of)
            for family, entries in payload["detectors"].items():
                for e in entries:
                    dedupe = (family, e["topic"], monday)
                    if dedupe in seen:
                        continue
                    seen.add(dedupe)
                    firings.append({"family": family, "topic": e["topic"],
                                    "tier": e.get("tier", "weak"),
                                    "p_value": e.get("p_value"),
                                    "week_idx": t_idx[monday],
                                    "as_of": as_of.isoformat()})
            print(f"  {as_of}: "
                  f"{sum(len(v) for v in payload['detectors'].values())} "
                  f"firings", file=sys.stderr)
    finally:
        conn.close()
    return firings


def _events_with_week_idx(cfg: Dict, timeline: List[dt.date]) -> List[Dict]:
    t_idx = {w: i for i, w in enumerate(timeline)}
    out = []
    for ev in cfg.get("events", []):
        day = dt.date.fromisoformat(ev["date"])
        monday = day - dt.timedelta(days=day.weekday())
        if monday in t_idx:
            out.append({**ev, "week_idx": t_idx[monday]})
    return out


def write_report(scores: Dict, corr: Dict, recall: Dict, meta: Dict) -> None:
    lines = [
        "# Signal Desk backtest report",
        "",
        f"Generated {meta['generated_at']} · window {meta['start']} -> "
        f"{meta['end']} · {meta['n_replays']} as-of replays · "
        f"{meta['n_firings']} distinct firings · "
        f"{meta['n_truth_topics']} mapped truth topics "
        f"(Wikipedia pageviews).",
        "",
        "## Emerging-detector precision vs external ground truth (by tier)",
        "",
        "Only `emerging` claims an external attention rise; shifting/"
        "contested/fading are scored via event recall, not this table.",
        "",
        "| tier | firings scored | precision | median lead (weeks) |",
        "|---|---|---|---|",
    ]
    for tier in ("strong", "moderate", "weak"):
        row = scores["by_tier"].get(tier)
        if row:
            lead = row["median_lead_weeks"]
            lines.append(f"| {tier} | {row['n']} | {row['precision']} | "
                         f"{lead if lead is not None else '—'} |")
    lines += [
        "",
        f"Unmapped-topic firings skipped: {scores['skipped_unmapped']} "
        "(extend config/backtest_truth.json mappings to cover them).",
        "",
        "## Event recall",
        "",
        f"{recall['hit']}/{recall['total']} curated events caught within "
        f"±3 weeks (recall {recall['recall']})."
        if recall["total"] else "No curated events in window.",
        "",
        "## Lead/lag correlation (share_smooth vs truth)",
        "",
        "| topic | best lag (weeks, + = we lead) | r |",
        "|---|---|---|",
    ]
    for topic, (lag, r) in sorted(corr.items()):
        lines.append(f"| {topic} | {lag:+d} | {r:.2f} |")
    lines += [
        "",
        "## Reading it",
        "",
        "- Precision: share of firings where the truth series rose >=25% "
        "vs its trailing 8-week baseline within [-2,+4] weeks.",
        "- Lead: distance from our firing to the truth series' CUSUM "
        "changepoint (negative = we were early).",
        "- The tuning loop: strong-tier precision must beat weak-tier. "
        "If it doesn't, adjust effect floors in "
        "research_factory/discourse_stats.py (see --sweep).",
    ]
    REPORT_PATH.write_text("\n".join(lines) + "\n")
    SCORES_PATH.parent.mkdir(parents=True, exist_ok=True)
    SCORES_PATH.write_text(json.dumps(
        {"meta": meta, "scores": scores, "event_recall": recall,
         "correlations": {k: {"lag": v[0], "r": round(v[1], 3)}
                          for k, v in corr.items()}}, indent=2))


FIRINGS_CACHE = PIF_ROOT / "work" / "pif-ops" / "backtest" / "firings.json"


def run(start: dt.date, end: dt.date, stride: int, offline: bool,
        sweep: bool, reuse_firings: bool = False) -> int:
    cfg = load_config()
    timeline = _weeks_between(start, end)
    print(f"timeline: {len(timeline)} weeks", file=sys.stderr)
    truths = fetch_truth(cfg, start, end, offline=offline)
    print(f"truth series: {len(truths)} topics", file=sys.stderr)
    as_of = [d + dt.timedelta(days=6) for d in timeline[::stride]]
    cache_key = {"start": start.isoformat(), "end": end.isoformat(),
                 "stride": stride}
    if reuse_firings and FIRINGS_CACHE.exists():
        cached = json.loads(FIRINGS_CACHE.read_text())
        if cached.get("key") == cache_key:
            firings = cached["firings"]
            print(f"reusing {len(firings)} cached firings", file=sys.stderr)
        else:
            firings = replay_detectors(DB_PATH, as_of, timeline)
    else:
        firings = replay_detectors(DB_PATH, as_of, timeline)
    FIRINGS_CACHE.parent.mkdir(parents=True, exist_ok=True)
    FIRINGS_CACHE.write_text(json.dumps(
        {"key": cache_key, "firings": firings}))
    events = _events_with_week_idx(cfg, timeline)

    def _score_with_current_constants():
        # Only "emerging" claims an external attention RISE; a stance shift
        # or contested split need not move public attention at all, and
        # fading claims the opposite. Score each family against the claim
        # it actually makes; shifting/contested get event-recall only.
        s = score_firings(
            [f for f in firings if f["family"] == "emerging"], truths)
        r = event_recall(events, firings)
        return s, r

    if sweep:
        from research_factory import discourse_stats as ds_mod
        print("sweep over emerging rate-ratio floor:", file=sys.stderr)
        for floor in (2.0, 2.5, 3.0, 4.0, 5.0):
            ds_mod.EMERGING_RATE_RATIO_FLOOR = floor
            re_firings = replay_detectors(DB_PATH, as_of, timeline)
            s = score_firings(re_firings, truths)
            strong = s["by_tier"].get("strong", {})
            print(f"  floor={floor}: strong n={strong.get('n', 0)} "
                  f"precision={strong.get('precision')}")
        return 0

    scores, recall = _score_with_current_constants()
    corr = {}
    # correlate detector-agnostic topic share series where mapped
    from research_factory.pif_discourse_aggregates import collect
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        payload = collect(conn, now=end)
    finally:
        conn.close()
    t_periods = payload["months"]
    for topic, truth in truths.items():
        info = payload["topics"].get(topic)
        if not info:
            continue
        ours = [s.get("share_smooth", 0.0) for s in info["series"]]
        # align: our series covers the last len(t_periods) buckets of timeline
        tail = truth[-len(t_periods):]
        if len(tail) >= 8:
            corr[topic] = best_lag_correlation(ours[-len(tail):], tail)
    meta = {"generated_at": dt.datetime.now().isoformat(timespec="seconds"),
            "start": start.isoformat(), "end": end.isoformat(),
            "n_replays": len(as_of), "n_firings": len(firings),
            "n_truth_topics": len(truths),
            "stride_weeks": stride}
    write_report(scores, corr, recall, meta)
    print(f"report: {REPORT_PATH}")
    print(json.dumps(scores, indent=2))
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=dt.date.fromisoformat, required=True)
    ap.add_argument("--end", type=dt.date.fromisoformat, required=True)
    ap.add_argument("--stride", type=int, default=1,
                    help="replay every Nth week (replays are ~30-45s each)")
    ap.add_argument("--offline", action="store_true",
                    help="use only cached truth data, never fetch")
    ap.add_argument("--sweep", action="store_true",
                    help="grid-search detector effect floors")
    ap.add_argument("--reuse-firings", action="store_true",
                    help="reuse cached replay firings for the same window")
    args = ap.parse_args()
    return run(args.start, args.end, args.stride, args.offline, args.sweep,
               reuse_firings=args.reuse_firings)


if __name__ == "__main__":
    raise SystemExit(main())
