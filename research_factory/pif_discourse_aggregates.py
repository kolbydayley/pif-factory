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
import bisect
import datetime as dt
import json
import math
import re
import sqlite3
import statistics

from research_factory import discourse_stats as ds
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


def short_excerpt(value: Optional[str], limit: int = 240) -> Optional[str]:
    """Return a compact excerpt without ending on a cut-off word."""
    cleaned = re.sub(r"\s*Speaker \d+:\s*", " ", value or "")
    cleaned = " ".join(cleaned.split())
    if not cleaned:
        return None
    if len(cleaned) <= limit:
        return cleaned
    clipped = cleaned[:limit - 1].rsplit(" ", 1)[0]
    return f"{clipped}…"


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


JUNK_TOPICS = {"other", "misc", "miscellaneous", "unknown", "none", "n a",
               "general"}
_STOP = {"the", "of", "and", "vs", "a", "an", "in", "on", "for", "to", "as"}


def _stem(tok: str) -> str:
    for suf in ("ical", "ally", "ing", "ity", "ic", "s"):
        if len(tok) > 4 and tok.endswith(suf):
            return tok[: -len(suf)]
    return tok


def _topic_tokens(topic: str) -> frozenset:
    return frozenset(_stem(w) for w in topic.split() if w not in _STOP)


def build_topic_canon(counts: Counter) -> Dict[str, str]:
    """Cluster near-duplicate open-vocabulary topics (quality audit item 3).

    Merge rule: stemmed-token containment or Jaccard >= 0.6; the
    highest-volume member names the cluster. BOUNDED: only the top
    MAX_HEADS topics may found clusters and everything else matches
    against those heads — the all-pairs version is quadratic over the
    10k+ raw vocabulary and took minutes (measured 2026-08-26).
    """
    MAX_HEADS = 400
    names = [n for n, _ in counts.most_common() if n not in JUNK_TOPICS]
    toks = {n: _topic_tokens(n) for n in names}
    heads: list = []
    canon: Dict[str, str] = {}
    for name in names:
        a = toks[name]
        merged = None
        if a:
            for head in heads:
                b = toks[head]
                if b and (a <= b or b <= a
                          or len(a & b) / len(a | b) >= 0.6):
                    merged = head
                    break
        if merged is None and len(heads) < MAX_HEADS:
            heads.append(name)
        canon[name] = merged or name
    return canon


CTX_RAW_SPAN = 500     # transcript chars read on each side of the quote
CTX_DISPLAY_CAP = 300  # public display cap per side (excerpt policy)


def _clean_context(raw: str, keep_end: bool) -> Optional[str]:
    """Clean a transcript window like short_excerpt, capped per side.
    keep_end keeps the tail (for text BEFORE the quote)."""
    cleaned = re.sub(r"\s*Speaker \d+:\s*", " ", raw or "")
    cleaned = re.sub(r">>\s*\[?[a-z ]*\]?", " ", cleaned)
    cleaned = " ".join(cleaned.split())
    if not cleaned:
        return None
    if len(cleaned) <= CTX_DISPLAY_CAP:
        return cleaned
    if keep_end:
        clipped = cleaned[-CTX_DISPLAY_CAP:]
        sp = clipped.find(" ")
        return clipped[sp + 1:] if 0 <= sp < 40 else clipped
    clipped = cleaned[:CTX_DISPLAY_CAP]
    sp = clipped.rfind(" ")
    return clipped[:sp] if sp > CTX_DISPLAY_CAP - 40 else clipped


def attach_context(entries: List[Dict[str, Any]],
                   ctx_map: Dict[str, tuple],
                   cache: Dict[str, Optional[str]]) -> None:
    """Attach bounded surrounding-transcript windows to evidence entries.
    Reads segment text lazily (only for entries that survived into the
    payload) and never exposes the file path."""
    for e in entries:
        ctx = ctx_map.get(e.get("id"))
        if not ctx or "context_before" in e:
            continue
        path, start, end = ctx
        if path not in cache:
            try:
                cache[path] = Path(path).read_text(errors="replace")
            except OSError:
                cache[path] = None
        text = cache[path]
        if text is None or not (0 <= start <= end <= len(text)):
            continue
        before = _clean_context(text[max(0, start - CTX_RAW_SPAN):start],
                                keep_end=True)
        after = _clean_context(text[end:end + CTX_RAW_SPAN], keep_end=False)
        if before:
            e["context_before"] = before
        if after:
            e["context_after"] = after


def find_inflections(series: List[Dict[str, Any]],
                     week_totals: List[int]) -> List[Dict[str, Any]]:
    """Weeks where a topic's discourse crossed a significance boundary.

    Slides the 4-week pulse window across the frame and records the FIRST
    week each kind of change (surge / fade / stance shift) becomes
    significant — the renderer draws these as inflection markers, which is
    the literal answer to "show me how the narratives are shifting"
    (Kolby 2026-08-27). Same tests and floors as the live detectors.
    """
    out: List[Dict[str, Any]] = []
    prev_kinds: set = set()
    for o in range(PULSE_WEEKS * 2, len(series) + 1):
        pulse, base = series[o - PULSE_WEEKS:o], series[:o - PULSE_WEEKS]
        pv = sum(s["vol"] for s in pulse)
        bv = sum(s["vol"] for s in base)
        pe = sum(week_totals[o - PULSE_WEEKS:o])
        be = sum(week_totals[:o - PULSE_WEEKS])
        kinds: Dict[str, str] = {}
        if pe > 0 and be > 0 and pv + bv >= 8:
            rt = ds.poisson_rate_test(pv, pe, bv, be)
            if rt["p_value"] < ds.ALPHA_MODERATE \
                    and rt["rate_ratio"] >= ds.EMERGING_RATE_RATIO_FLOOR:
                kinds["surge"] = ("strong" if rt["p_value"] < ds.ALPHA_STRONG
                                  else "moderate")
            ft = ds.fading_test(pv, pe, bv, be)
            if bv >= 8 and ft["p_value"] < ds.ALPHA_MODERATE \
                    and ft["rate_ratio"] <= ds.FADING_RATE_RATIO_CEIL:
                kinds["fade"] = ("strong" if ft["p_value"] < ds.ALPHA_STRONG
                                 else "moderate")
        if pv >= 8 and bv >= 10:
            sh = ds.stance_shift_test(
                [sum(s["pos"] for s in pulse), sum(s["neg"] for s in pulse),
                 sum(s["neu"] for s in pulse)],
                [sum(s["pos"] for s in base), sum(s["neg"] for s in base),
                 sum(s["neu"] for s in base)],
                n_permutations=300, n_bootstrap=0)
            if sh["p_value"] < ds.ALPHA_MODERATE \
                    and sh["tv_distance"] >= ds.SHIFT_TV_FLOOR:
                kinds["shift"] = ("strong" if sh["p_value"] < ds.ALPHA_STRONG
                                  else "moderate")
        for kind, tier in kinds.items():
            if kind not in prev_kinds:  # record the crossing, not the run
                out.append({"week": series[o - 1]["week"], "kind": kind,
                            "tier": tier})
        prev_kinds = set(kinds)
    return out[-6:]


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
    episode_topics: Dict[str, set] = defaultdict(set)
    episode_shows: Dict[str, str] = {}
    topic_evidence_raw: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
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
    # ---- pre-pass: raw topic frequencies -> canonical cluster map
    raw_counts: Counter = Counter()
    for r in conn.execute(
            """SELECT t.value AS tj FROM labels l
               JOIN segments s ON s.id = l.segment_id
               JOIN episodes e ON e.id = s.episode_id,
               json_each(l.output_json, '$.topics') t
               WHERE e.published_at >= ?""", (cutoff_iso,)):
        try:
            nt = norm_topic(json.loads(r["tj"]).get("topic"))
        except (json.JSONDecodeError, TypeError):
            continue
        if nt:
            raw_counts[nt] += 1
    for r in conn.execute(
            """SELECT ap.concept_name AS c FROM actor_positions ap
               JOIN segments s ON s.id = ap.segment_id
               JOIN episodes e ON e.id = s.episode_id
               WHERE e.published_at >= ?""", (cutoff_iso,)):
        nt = norm_topic(r["c"])
        if nt:
            raw_counts[nt] += 1
    topic_canon = build_topic_canon(raw_counts)

    def canon(topic: Optional[str]) -> Optional[str]:
        if not topic or topic in JUNK_TOPICS:
            return None
        return topic_canon.get(topic, topic)

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
        topic = canon(norm_topic(item.get("topic")))
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
        episode_topics[r["episode_id"]].add(topic)
        episode_shows[r["episode_id"]] = r["source_id"]

    # ---- concept-level positions also feed the topic series (deeper history)
    pos_rows = conn.execute(
        """
        SELECT ap.id, ap.actor_name, ap.actor_type, ap.concept_name, ap.stance,
               ap.claim_type, ap.confidence, ap.evidence_json, ap.segment_id,
               s.episode_id, s.text_path, e.published_at, e.source_id,
               e.title AS episode_title, e.url AS episode_url,
               e.audio_url AS episode_audio_url
        FROM actor_positions ap
        JOIN segments s ON s.id = ap.segment_id
        JOIN episodes e ON e.id = s.episode_id
        WHERE e.published_at >= ?
        """, (cutoff_iso,)).fetchall()
    people_positions: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    concept_field: Dict[str, Counter] = defaultdict(Counter)
    ctx_map: Dict[str, tuple] = {}
    for r in pos_rows:
        topic = canon(norm_topic(r["concept_name"]))
        wk = week_of(r["published_at"])
        if topic and wk in widx:
            cell = topic_weekly[topic].setdefault(
                widx[wk], {"vol": 0, "positive": 0, "negative": 0,
                           "neutral": 0, "intensity_sum": 0.0})
            cell["vol"] += 1
            cell[stance_group(r["stance"])] += 1
            topic_total[topic] += 1
            note_breadth(topic, widx[wk], r["episode_id"], r["source_id"])
            episode_topics[r["episode_id"]].add(topic)
            episode_shows[r["episode_id"]] = r["source_id"]
        name = (r["actor_name"] or "").strip()
        try:
            ej = json.loads(r["evidence_json"])
            evidence = ej.get("evidence")
            # Segment-relative offsets let the evidence page show the
            # surrounding transcript window (Kolby 2026-08-27: "read
            # context" must give where / what was said / what surrounded
            # it). Kept in a side map so filesystem paths never enter the
            # public payload.
            if r["text_path"] is not None and \
                    isinstance(ej.get("start"), int) and \
                    isinstance(ej.get("end"), int):
                ctx_map[r["id"]] = (r["text_path"], ej["start"], ej["end"])
        except (json.JSONDecodeError, TypeError):
            evidence = None
        entry = {
            "id": r["id"],
            "topic": topic, "stance": r["stance"],
            "group": stance_group(r["stance"]),
            "claim_type": r["claim_type"], "week": wk,
            "date": (r["published_at"] or "")[:10],
            "show": r["source_id"], "episode": r["episode_title"],
            "episode_id": r["episode_id"],
            "source_url": r["episode_url"] or r["episode_audio_url"],
            "confidence": round(float(r["confidence"] or 0), 2),
            "evidence": short_excerpt(evidence),
            "role": r["actor_type"],
        }
        if topic and entry["evidence"]:
            display_name = name
            if not display_name or display_name.lower() in {
                    "unknown", "none", "n/a"}:
                display_name = "Unattributed voice"
            topic_evidence_raw[topic].append(
                {**entry, "person": display_name})
        if not name or r["actor_type"] not in ("guest", "host", "person"):
            continue
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
        topic_counts: Dict[str, Counter] = defaultdict(Counter)
        for entry in entries:
            if entry["topic"]:
                topic_counts[entry["topic"]][entry["group"]] += 1
        moves = []
        last_by_topic: Dict[str, Dict[str, Any]] = {}
        for e in entries:
            if not e["topic"] or e["group"] == "neutral":
                continue
            prev = last_by_topic.get(e["topic"])
            days_apart = 0
            if prev:
                try:
                    days_apart = abs((dt.date.fromisoformat(e["date"])
                                      - dt.date.fromisoformat(prev["date"])
                                      ).days)
                except ValueError:
                    days_apart = 0
            if (prev and prev["group"] != e["group"]
                    and prev["episode_id"] != e["episode_id"]
                    and days_apart >= 14):
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
        evidence = []
        seen_evidence = set()
        for entry in reversed(entries):
            key = (entry["episode_id"], entry["evidence"])
            if not entry["evidence"] or key in seen_evidence:
                continue
            seen_evidence.add(key)
            evidence.append(entry)
            if len(evidence) >= 18:
                break
        top_topics = []
        for topic, counts_by_group in sorted(
                topic_counts.items(), key=lambda item: sum(item[1].values()),
                reverse=True)[:8]:
            top_topics.append({
                "topic": topic,
                "count": sum(counts_by_group.values()),
                "positive": counts_by_group.get("positive", 0),
                "negative": counts_by_group.get("negative", 0),
                "neutral": counts_by_group.get("neutral", 0),
            })
        people.append({
            "n_episodes": len({e["episode_id"] for e in entries}),
            "n_recent_episodes": len({e["episode_id"] for e in recent}),
            "name": name,
            "roles": sorted({e["role"] for e in entries}),
            "shows": sorted({e["show"] for e in entries if e["show"]}),
            "n_positions": len(entries),
            "n_recent": len(recent),
            "recent": recent[-12:],
            "evidence": evidence,
            "top_topics": top_topics,
            "moves": moves[-8:],
            "against_field": against[:6],
            "authority": authority.get(name),
            "last_seen": entries[-1]["date"] if entries else None,
        })
    # Rank by breadth of presence (audit item 2): a one-interview guest
    # with 500 positions is not a tracked voice.
    people = [p for p in people if p["n_episodes"] >= 2]
    people.sort(key=lambda p: (p["n_episodes"],
                               len(p["moves"]) > 0,
                               p["authority"] or 0,
                               p["n_recent_episodes"]), reverse=True)
    people = people[:TOP_PEOPLE]

    # ---- network trust: PageRank over the co-appearance graph.
    # Early-Google style (Kolby 2026-08-27): trust grows with the number of
    # relationships AND the trust of the people on the other end. Edge
    # weight = shared episodes; subject-blind by construction.
    ep_people: Dict[str, set] = defaultdict(set)
    for p in people:
        for e in people_positions[p["name"]]:
            ep_people[e["episode_id"]].add(p["name"])
    co_edges: Dict[str, Dict[str, float]] = defaultdict(dict)
    for names in ep_people.values():
        for a in names:
            for b in names:
                if a != b:
                    co_edges[a][b] = co_edges[a].get(b, 0.0) + 1.0
    net_rank = ds.pagerank(dict(co_edges))
    if net_rank:
        top_rank = max(net_rank.values())
        ranked_scores = sorted(net_rank.values())
        for p in people:
            r = net_rank.get(p["name"])
            if r is None:
                p["trust"] = {"score": 0.0, "tier": "peripheral",
                              "n_links": 0,
                              "basis": "co-appearance pagerank (d=0.85)"}
                continue
            pct = bisect.bisect_left(ranked_scores, r) / len(ranked_scores)
            tier = ("hub" if pct >= 0.75 else
                    "connected" if pct >= 0.25 else "peripheral")
            p["trust"] = {"score": round(r / top_rank * 100, 1),
                          "tier": tier,
                          "n_links": len(co_edges.get(p["name"], {})),
                          "basis": "co-appearance pagerank (d=0.85)"}
    else:
        for p in people:
            p["trust"] = {"score": 0.0, "tier": "peripheral", "n_links": 0,
                          "basis": "co-appearance pagerank (d=0.85)"}

    week_totals = [0] * RECENT_WEEKS
    for cells in topic_weekly.values():
        for i, c in cells.items():
            week_totals[i] += c["vol"]

    # ---- topic table + detectors
    _ctx_cache: Dict[str, Optional[str]] = {}
    for p in people:
        attach_context(p["evidence"], ctx_map, _ctx_cache)
    topics_out = {}
    detectors = {"emerging": [], "shifting": [], "contested": [], "fading": []}
    pulse_lo = RECENT_WEEKS - PULSE_WEEKS
    # Exposure = total labeled mentions per window. Rate tests condition on
    # it, so a week where the corpus simply covered 3x the ground does not
    # read as a topic surge (Kolby 2026-08-27: the spikes were coverage
    # artifacts, not trends).
    pulse_exposure = sum(week_totals[pulse_lo:])
    base_exposure = sum(week_totals[:pulse_lo])
    # The fixed low_sample cutoff of 150 never fired on live data (weekly
    # totals 544-4634 measured 2026-08-27); derive it from the corpus.
    week_median = statistics.median(week_totals) if week_totals else 0
    low_sample_floor = 0.25 * week_median
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
        # Share-of-discourse per week: raw counts inherit the corpus's 10x
        # weekly coverage swings (81..923 labeled mentions/wk measured
        # 2026-08-26), which makes evergreen topics look spiky. share is the
        # honest trend line; low_sample flags weeks too thin to trust.
        for i, s in enumerate(series):
            wk_total = week_totals[i]
            s["share"] = round(s["vol"] / wk_total, 4) if wk_total else 0
            s["week_total"] = wk_total
            s["low_sample"] = bool(wk_total < low_sample_floor)
            s["thin"] = s["vol"] < 3
            if s["vol"]:
                s["pos_ci"] = [round(x, 3)
                               for x in ds.wilson_interval(s["pos"], s["vol"])]
                s["neg_ci"] = [round(x, 3)
                               for x in ds.wilson_interval(s["neg"], s["vol"])]
        for i, s in enumerate(series):
            lo, hi = max(0, i - 1), min(len(series), i + 2)
            window = [series[j]["share"] for j in range(lo, hi)]
            s["share_smooth"] = round(sum(window) / len(window), 4)
        pulse = series[pulse_lo:]
        base = series[:pulse_lo]
        pulse_vol = sum(s["vol"] for s in pulse)
        base_vol = sum(s["vol"] for s in base)
        base_rate = base_vol / max(1, BASELINE_WEEKS)
        pulse_rate = pulse_vol / PULSE_WEEKS
        info = {"series": series, "total": topic_total[topic],
                "pulse_vol": pulse_vol,
                "pulse_rate": round(pulse_rate, 2),
                "base_rate": round(base_rate, 2)}
        if len(topics_out) < TOP_TOPICS or pulse_vol > 0:
            info["inflections"] = find_inflections(series, week_totals)
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
            rt = ds.poisson_rate_test(pulse_vol, pulse_exposure,
                                      base_vol, base_exposure)
            effect_ok = rt["rate_ratio"] >= ds.EMERGING_RATE_RATIO_FLOOR
            detectors["emerging"].append(
                {"topic": topic, "pulse_vol": pulse_vol,
                 "base_rate": round(base_rate, 2),
                 "episodes": p_eps, "shows": p_shows,
                 "p_value": round(rt["p_value"], 5),
                 "rate_ratio": round(rt["rate_ratio"], 2),
                 "rate_ratio_ci": [round(x, 2) for x in rt["rate_ratio_ci"]],
                 "effect": round(rt["rate_ratio"], 2),
                 "tier": ds.confidence_tier(rt["p_value"], effect_ok,
                                            p_shows)})
        dp, db = stance_dist(pulse), stance_dist(base)
        if dp and db and base_vol >= 10 and pulse_vol >= 8:
            div = sum(abs(a - b) for a, b in zip(dp, db)) / 2
            if div >= 0.3 and p_eps >= 3 and p_shows >= 2 and b_eps >= 3:
                sh = ds.stance_shift_test(
                    [sum(s["pos"] for s in pulse),
                     sum(s["neg"] for s in pulse),
                     sum(s["neu"] for s in pulse)],
                    [sum(s["pos"] for s in base),
                     sum(s["neg"] for s in base),
                     sum(s["neu"] for s in base)])
                effect_ok = div >= ds.SHIFT_TV_FLOOR
                detectors["shifting"].append(
                    {"topic": topic, "divergence": round(div, 2),
                     "now": [round(x, 2) for x in dp],
                     "before": [round(x, 2) for x in db],
                     "episodes": p_eps, "shows": p_shows,
                     "p_value": round(sh["p_value"], 5),
                     "tv_ci": list(sh["tv_ci"]),
                     "effect": round(div, 2),
                     "tier": ds.confidence_tier(sh["p_value"], effect_ok,
                                                p_shows)})
        field = concept_field.get(topic)
        if field and sum(field.values()) >= 8:
            p = field.get("positive", 0)
            n = field.get("negative", 0)
            if p and n and min(p, n) / max(p, n) >= 0.5 \
                    and p_eps >= 3 and p_shows >= 2:
                ct = ds.contested_test(p, n, field.get("neutral", 0))
                effect_ok = ct["balance"] >= ds.CONTESTED_BALANCE_FLOOR
                detectors["contested"].append(
                    {"topic": topic, "positive": p, "negative": n,
                     "neutral": field.get("neutral", 0),
                     "episodes": p_eps, "shows": p_shows,
                     "p_value": round(ct["p_value"], 5),
                     "pos_share_ci": [round(x, 3)
                                      for x in ct["pos_share_ci"]],
                     "effect": round(ct["balance"], 2),
                     "tier": ds.confidence_tier(ct["p_value"], effect_ok,
                                                p_shows)})
        peak = max((s["vol"] for s in base), default=0)
        b_shows = len(br.get("base_shows", ()))
        if peak >= 8 and pulse_rate <= 0.25 * peak and pulse_vol <= 2 \
                and b_eps >= 5 and b_shows >= 2:
            ft = ds.fading_test(pulse_vol, pulse_exposure,
                                base_vol, base_exposure)
            effect_ok = ft["rate_ratio"] <= ds.FADING_RATE_RATIO_CEIL
            detectors["fading"].append(
                {"topic": topic, "peak_week_vol": peak,
                 "pulse_vol": pulse_vol, "base_episodes": b_eps,
                 "shows": b_shows,
                 "p_value": round(ft["p_value"], 5),
                 "rate_ratio": round(ft["rate_ratio"], 3),
                 "effect": round(1.0 / max(ft["rate_ratio"], 1e-6), 2),
                 "tier": ds.confidence_tier(ft["p_value"], effect_ok,
                                            b_shows)})
    # One test ran per candidate topic per family; without FDR control a
    # weekly crop of false "strong" firings is guaranteed. BH demotes
    # strong hits that don't survive the family-wise gate.
    _tier_rank = {"strong": 2, "moderate": 1, "weak": 0}
    for key in detectors:
        entries = detectors[key]
        survives = ds.benjamini_hochberg([e["p_value"] for e in entries])
        for e, ok in zip(entries, survives):
            if e["tier"] == "strong" and not ok:
                e["tier"] = "moderate"
        entries.sort(key=lambda x: (_tier_rank[x["tier"]],
                                    x.get("effect", 0),
                                    x.get("shows", 0)),
                     reverse=True)
        detectors[key] = entries[:15]

    # ---- progressive-disclosure research layers
    related_counts: Dict[str, Counter] = defaultdict(Counter)
    related_shows: Dict[str, Dict[str, set]] = defaultdict(
        lambda: defaultdict(set))
    for episode_id, names in episode_topics.items():
        show = episode_shows.get(episode_id)
        for topic in names:
            for other in names:
                if other == topic:
                    continue
                related_counts[topic][other] += 1
                if show:
                    related_shows[topic][other].add(show)

    for topic, info in topics_out.items():
        related = []
        for other, episode_count in related_counts.get(
                topic, Counter()).most_common(20):
            if other not in topics_out or other == "other":
                continue
            related.append({
                "topic": other,
                "shared_episodes": episode_count,
                "shared_shows": len(related_shows[topic][other]),
            })
        related.sort(
            key=lambda item: (item["shared_shows"], item["shared_episodes"]),
            reverse=True,
        )
        info["related"] = related[:8]

        evidence = []
        seen_evidence = set()
        for entry in topic_evidence_raw.get(topic, []):
            key = (entry["person"].lower(), entry["episode_id"],
                   entry["evidence"].lower())
            if key in seen_evidence:
                continue
            seen_evidence.add(key)
            evidence.append({
                **entry,
                "authority": authority.get(entry["person"]),
                "authority_scored": entry["person"] in authority,
            })
        evidence.sort(
            key=lambda item: (
                item["authority_scored"], item["authority"] or 0,
                item["confidence"], item["date"],
            ),
            reverse=True,
        )
        info["evidence"] = evidence[:16]
        attach_context(info["evidence"], ctx_map, _ctx_cache)
        stance_counts = Counter(item["group"] for item in evidence)
        info["evidence_stances"] = {
            "positive": stance_counts.get("positive", 0),
            "negative": stance_counts.get("negative", 0),
            "neutral": stance_counts.get("neutral", 0),
        }

    counts = conn.execute(
        "SELECT (SELECT COUNT(*) FROM episodes) ep,"
        " (SELECT COUNT(*) FROM labels) lab,"
        " (SELECT COUNT(DISTINCT source_id) FROM episodes) shows,"
        " (SELECT MAX(published_at) FROM episodes) latest").fetchone()
    return {
        "schema_version": "signal_desk_v4",
        "latest_episode": (counts["latest"] or "")[:10] or None,
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "data_through": (now or dt.date.today()).isoformat(),
        "window_weeks": RECENT_WEEKS,
        "pulse_weeks": PULSE_WEEKS,
        "weeks": weeks,
        "week_totals": week_totals,
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
    # Rotate the prior build so the renderer can show a "since last build"
    # narrative diff. Same-day re-runs overwrite data.json but keep the
    # older prev, which is the intent: diff against the previous session.
    prev_path = args.out.with_name("data_prev.json")
    if args.out.exists():
        try:
            old = json.loads(args.out.read_text())
            if old.get("generated_at", "")[:10] != \
                    data["generated_at"][:10] or not prev_path.exists():
                prev_path.write_text(json.dumps(old))
        except (json.JSONDecodeError, OSError):
            pass
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
