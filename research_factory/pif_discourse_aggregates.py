"""Discourse aggregates for the technical-podcast dashboard. Read-only.

Produces one JSON blob (work/pif-ops/dashboard/data.json) with:
- corpus coverage timeline (per-source episode counts by month — so trend
  reads stay honest when the source set changes)
- open-vocabulary topic series (weekly volume / stance mix / intensity),
  taxonomy grown from the data, never configured
- the people board: named speakers' recent positions, stance MOVES,
  against-the-field disagreements, and authority scores
- subject-blind trend detectors: emerging / shifting / contested / fading

Discourse time is episode published_at (not label time). Stances collapse to
positive / negative / neutral groups for cross-source comparability:
supportive, promotional, bullish -> positive; skeptical, warning,
bearish -> negative; everything else neutral.

Usage: python3 -m research_factory.pif_discourse_aggregates [--out PATH]
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import re
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

PIF_ROOT = Path.home() / "pif-factory"
CANONICAL_DB = PIF_ROOT / "data" / "factory.sqlite"
OUT_DEFAULT = PIF_ROOT / "work" / "pif-ops" / "dashboard" / "data.json"

RECENT_WEEKS = 26
PULSE_WEEKS = 4          # "now" window for detectors
BASELINE_WEEKS = 20      # trailing baseline behind the pulse window
TOP_TOPICS = 90
TOP_PEOPLE = 120

POSITIVE = {"supportive", "promotional", "bullish"}
NEGATIVE = {"skeptical", "warning", "bearish"}


def stance_group(stance: Optional[str]) -> str:
    s = (stance or "").lower()
    if s in POSITIVE:
        return "positive"
    if s in NEGATIVE:
        return "negative"
    return "neutral"


def norm_topic(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    t = re.sub(r"[\s_/-]+", " ", str(name).lower()).strip()
    t = re.sub(r"[^a-z0-9 .+#]", "", t)
    return t or None


def week_of(published_at: Optional[str]) -> Optional[str]:
    if not published_at:
        return None
    try:
        d = dt.date.fromisoformat(published_at[:10])
    except ValueError:
        return None
    iso = d.isocalendar()
    return f"{iso[0]}-W{iso[1]:02d}"


def _week_index(weeks: List[str]) -> Dict[str, int]:
    return {w: i for i, w in enumerate(weeks)}


def recent_weeks_list(now: Optional[dt.date] = None) -> List[str]:
    today = now or dt.date.today()
    out = []
    for i in range(RECENT_WEEKS - 1, -1, -1):
        d = today - dt.timedelta(weeks=i)
        iso = d.isocalendar()
        out.append(f"{iso[0]}-W{iso[1]:02d}")
    return out


def data_frontier(conn: sqlite3.Connection) -> Optional[dt.date]:
    """Latest week with meaningful labeled volume. Anchoring the pulse here
    (instead of today) keeps detectors honest when ingestion lags: an
    acquisition freeze must read as a coverage gap, not as every topic
    'fading'."""
    row = conn.execute(
        """
        SELECT substr(e.published_at, 1, 10) AS d, COUNT(*) AS n
        FROM labels l
        JOIN segments s ON s.id = l.segment_id
        JOIN episodes e ON e.id = s.episode_id
        GROUP BY substr(e.published_at, 1, 7)
        HAVING n >= 50
        ORDER BY d DESC LIMIT 1
        """).fetchone()
    if not row or not row["d"]:
        return None
    try:
        return dt.date.fromisoformat(row["d"])
    except ValueError:
        return None


def collect(conn: sqlite3.Connection, now: Optional[dt.date] = None) -> Dict[str, Any]:
    conn.row_factory = sqlite3.Row
    if now is None:
        now = data_frontier(conn)
    weeks = recent_weeks_list(now)
    widx = _week_index(weeks)
    cutoff = (now or dt.date.today()) - dt.timedelta(weeks=RECENT_WEEKS)
    cutoff_iso = cutoff.isoformat()

    # ---- corpus coverage timeline (per source, monthly, last 3 years)
    coverage = defaultdict(lambda: defaultdict(int))
    for r in conn.execute(
            "SELECT source_id, substr(published_at, 1, 7) AS month, COUNT(*) n"
            " FROM episodes WHERE published_at >= date('now', '-36 months')"
            " GROUP BY source_id, month"):
        coverage[r["source_id"]][r["month"]] = r["n"]

    # ---- topic series from labels (all packs; open vocabulary)
    topic_weekly: Dict[str, Dict[int, Dict[str, Any]]] = defaultdict(dict)
    topic_total: Counter = Counter()
    # Breadth = the anti-noise gate. A topic mentioned 11 times inside ONE
    # episode is that episode's turn of phrase, not a discourse trend
    # (audited 2026-08-26: nearly every ungated signal was single-episode).
    breadth: Dict[str, Dict[str, set]] = defaultdict(
        lambda: {"pulse_eps": set(), "pulse_shows": set(),
                 "base_eps": set(), "base_shows": set()})

    def note_breadth(topic: str, wk_i: int, episode_id: str, show: str) -> None:
        zone = "pulse" if wk_i >= RECENT_WEEKS - PULSE_WEEKS else "base"
        breadth[topic][f"{zone}_eps"].add(episode_id)
        breadth[topic][f"{zone}_shows"].add(show)
    rows = conn.execute(
        """
        SELECT t.value AS topic_json, e.published_at,
               e.id AS episode_id, e.source_id
        FROM labels l
        JOIN segments s ON s.id = l.segment_id
        JOIN episodes e ON e.id = s.episode_id,
        json_each(l.output_json, '$.topics') t
        WHERE e.published_at >= ?
        """, (cutoff_iso,))
    for r in rows:
        try:
            item = json.loads(r["topic_json"])
        except (json.JSONDecodeError, TypeError):
            continue
        topic = norm_topic(item.get("topic"))
        wk = week_of(r["published_at"])
        if not topic or wk not in widx:
            continue
        cell = topic_weekly[topic].setdefault(
            widx[wk], {"vol": 0, "positive": 0, "negative": 0, "neutral": 0,
                       "intensity_sum": 0.0})
        cell["vol"] += 1
        cell[stance_group(item.get("stance"))] += 1
        try:
            cell["intensity_sum"] += float(item.get("intensity") or 0)
        except (TypeError, ValueError):
            pass
        topic_total[topic] += 1
        note_breadth(topic, widx[wk], r["episode_id"], r["source_id"])

    # ---- concept-level positions also feed the topic series (deeper history)
    pos_rows = conn.execute(
        """
        SELECT ap.actor_name, ap.actor_type, ap.concept_name, ap.stance,
               ap.claim_type, ap.evidence_json, ap.segment_id,
               s.episode_id, e.published_at, e.source_id,
               e.title AS episode_title
        FROM actor_positions ap
        JOIN segments s ON s.id = ap.segment_id
        JOIN episodes e ON e.id = s.episode_id
        WHERE e.published_at >= ?
        """, (cutoff_iso,)).fetchall()
    people_positions: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    concept_field: Dict[str, Counter] = defaultdict(Counter)
    for r in pos_rows:
        topic = norm_topic(r["concept_name"])
        wk = week_of(r["published_at"])
        if topic and wk in widx:
            cell = topic_weekly[topic].setdefault(
                widx[wk], {"vol": 0, "positive": 0, "negative": 0,
                           "neutral": 0, "intensity_sum": 0.0})
            cell["vol"] += 1
            cell[stance_group(r["stance"])] += 1
            topic_total[topic] += 1
            note_breadth(topic, widx[wk], r["episode_id"], r["source_id"])
        name = (r["actor_name"] or "").strip()
        if not name or r["actor_type"] not in ("guest", "host", "person"):
            continue
        try:
            evidence = json.loads(r["evidence_json"]).get("evidence")
        except (json.JSONDecodeError, TypeError):
            evidence = None
        entry = {
            "topic": topic, "stance": r["stance"],
            "group": stance_group(r["stance"]),
            "claim_type": r["claim_type"], "week": wk,
            "date": (r["published_at"] or "")[:10],
            "show": r["source_id"], "episode": r["episode_title"],
            "evidence": (evidence or "")[:300] or None,
            "role": r["actor_type"],
        }
        people_positions[name].append(entry)
        if topic and wk in widx and widx[wk] >= RECENT_WEEKS - PULSE_WEEKS:
            concept_field[topic][entry["group"]] += 1

    # ---- authority scores (latest accepted per person name)
    authority: Dict[str, float] = {}
    for r in conn.execute(
            """
            SELECT cp.display_name AS name, MAX(eas.score) AS score
            FROM expert_authority_scores eas
            JOIN canonical_people cp ON cp.id = eas.canonical_person_id
            WHERE eas.status != 'retracted'
            GROUP BY cp.display_name
            """):
        if r["name"]:
            authority[r["name"].strip()] = round(float(r["score"]), 4)

    # ---- people board
    people = []
    for name, entries in people_positions.items():
        entries.sort(key=lambda x: x["date"])
        moves = []
        last_by_topic: Dict[str, Dict[str, Any]] = {}
        for e in entries:
            if not e["topic"] or e["group"] == "neutral":
                continue
            prev = last_by_topic.get(e["topic"])
            if prev and prev["group"] != e["group"]:
                moves.append({"topic": e["topic"], "from": prev["group"],
                              "to": e["group"], "from_date": prev["date"],
                              "to_date": e["date"],
                              "from_evidence": prev["evidence"],
                              "to_evidence": e["evidence"],
                              "show": e["show"]})
            last_by_topic[e["topic"]] = e
        against = []
        for topic, e in last_by_topic.items():
            field = concept_field.get(topic)
            if not field or sum(field.values()) < 5:
                continue
            majority, majority_n = field.most_common(1)[0]
            share = majority_n / sum(field.values())
            if share >= 0.6 and e["group"] != majority and \
                    e["group"] != "neutral":
                against.append({"topic": topic, "stance": e["group"],
                                "field_majority": majority,
                                "majority_share": round(share, 2),
                                "evidence": e["evidence"],
                                "date": e["date"]})
        recent = [e for e in entries
                  if e["week"] in widx
                  and widx[e["week"]] >= RECENT_WEEKS - PULSE_WEEKS * 2]
        people.append({
            "name": name,
            "roles": sorted({e["role"] for e in entries}),
            "shows": sorted({e["show"] for e in entries if e["show"]}),
            "n_positions": len(entries),
            "n_recent": len(recent),
            "recent": recent[-12:],
            "moves": moves[-8:],
            "against_field": against[:6],
            "authority": authority.get(name),
            "last_seen": entries[-1]["date"] if entries else None,
        })
    people.sort(key=lambda p: (len(p["moves"]) > 0, p["n_recent"],
                               p["authority"] or 0), reverse=True)
    people = people[:TOP_PEOPLE]

    # ---- topic table + detectors
    topics_out = {}
    detectors = {"emerging": [], "shifting": [], "contested": [], "fading": []}
    pulse_lo = RECENT_WEEKS - PULSE_WEEKS
    for topic, _n in topic_total.most_common(TOP_TOPICS * 3):
        cells = topic_weekly[topic]
        series = []
        for i in range(RECENT_WEEKS):
            c = cells.get(i)
            series.append({
                "week": weeks[i], "vol": c["vol"] if c else 0,
                "pos": c["positive"] if c else 0,
                "neg": c["negative"] if c else 0,
                "neu": c["neutral"] if c else 0,
                "intensity": round(c["intensity_sum"] / c["vol"], 2)
                if c and c["vol"] else 0,
            })
        pulse = series[pulse_lo:]
        base = series[:pulse_lo]
        pulse_vol = sum(s["vol"] for s in pulse)
        base_rate = sum(s["vol"] for s in base) / max(1, BASELINE_WEEKS)
        pulse_rate = pulse_vol / PULSE_WEEKS
        info = {"series": series, "total": topic_total[topic],
                "pulse_vol": pulse_vol,
                "pulse_rate": round(pulse_rate, 2),
                "base_rate": round(base_rate, 2)}
        if len(topics_out) < TOP_TOPICS or pulse_vol > 0:
            topics_out[topic] = info

        def stance_dist(rows_):
            tot = sum(r["pos"] + r["neg"] + r["neu"] for r in rows_)
            if tot == 0:
                return None
            return [sum(r["pos"] for r in rows_) / tot,
                    sum(r["neg"] for r in rows_) / tot,
                    sum(r["neu"] for r in rows_) / tot]

        br = breadth.get(topic, {})
        p_eps = len(br.get("pulse_eps", ()))
        p_shows = len(br.get("pulse_shows", ()))
        b_eps = len(br.get("base_eps", ()))
        info["pulse_episodes"] = p_eps
        info["pulse_shows"] = p_shows
        if pulse_vol >= 8 and base_rate < 0.5 and p_eps >= 3 and p_shows >= 2:
            detectors["emerging"].append(
                {"topic": topic, "pulse_vol": pulse_vol,
                 "base_rate": round(base_rate, 2),
                 "episodes": p_eps, "shows": p_shows})
        dp, db = stance_dist(pulse), stance_dist(base)
        if dp and db and sum(s["vol"] for s in base) >= 10 and pulse_vol >= 8:
            div = sum(abs(a - b) for a, b in zip(dp, db)) / 2
            if div >= 0.3 and p_eps >= 3 and p_shows >= 2 and b_eps >= 3:
                detectors["shifting"].append(
                    {"topic": topic, "divergence": round(div, 2),
                     "now": [round(x, 2) for x in dp],
                     "before": [round(x, 2) for x in db],
                     "episodes": p_eps, "shows": p_shows})
        field = concept_field.get(topic)
        if field and sum(field.values()) >= 8:
            p = field.get("positive", 0)
            n = field.get("negative", 0)
            if p and n and min(p, n) / max(p, n) >= 0.5 \
                    and p_eps >= 3 and p_shows >= 2:
                detectors["contested"].append(
                    {"topic": topic, "positive": p, "negative": n,
                     "neutral": field.get("neutral", 0),
                     "episodes": p_eps, "shows": p_shows})
        peak = max((s["vol"] for s in base), default=0)
        if peak >= 8 and pulse_rate <= 0.25 * peak and pulse_vol <= 2 \
                and b_eps >= 5 and len(br.get("base_shows", ())) >= 2:
            detectors["fading"].append(
                {"topic": topic, "peak_week_vol": peak,
                 "pulse_vol": pulse_vol, "base_episodes": b_eps})
    for key in detectors:
        detectors[key].sort(key=lambda x: (x.get("shows", 0),
                                           x.get("episodes",
                                                 x.get("base_episodes", 0))),
                            reverse=True)
        detectors[key] = detectors[key][:15]

    counts = conn.execute(
        "SELECT (SELECT COUNT(*) FROM episodes) ep,"
        " (SELECT COUNT(*) FROM labels) lab,"
        " (SELECT COUNT(DISTINCT source_id) FROM episodes) shows").fetchone()
    return {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "data_through": (now or dt.date.today()).isoformat(),
        "window_weeks": RECENT_WEEKS,
        "pulse_weeks": PULSE_WEEKS,
        "weeks": weeks,
        "corpus": {"episodes": counts["ep"], "labels": counts["lab"],
                   "shows": counts["shows"],
                   "coverage": {k: dict(v) for k, v in coverage.items()}},
        "topics": topics_out,
        "people": people,
        "detectors": detectors,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=OUT_DEFAULT)
    args = parser.parse_args()
    conn = sqlite3.connect(f"file:{CANONICAL_DB}?mode=ro", uri=True)
    data = collect(conn)
    conn.close()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(data))
    print(json.dumps({
        "out": str(args.out),
        "bytes": args.out.stat().st_size,
        "topics": len(data["topics"]),
        "people": len(data["people"]),
        "detectors": {k: len(v) for k, v in data["detectors"].items()},
    }, indent=1))


if __name__ == "__main__":
    main()
