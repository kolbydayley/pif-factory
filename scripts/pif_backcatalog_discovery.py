#!/usr/bin/env python3
"""Back-catalog transcript DISCOVERY (no fetching, no job enqueues).

Division of labor agreed with the acquisition session ("Podcast feeds",
2026-08-29): this script only LOCATES transcripts for episodes that never
entered acquisition tracking, writing status rows (and, for YouTube
matches, episodes.verified_transcript_url) that the fetch session's
runners pick up automatically. It never enqueues fetch jobs (db.enqueue_job
dedupes into terminally-failed rows — known trap) and never touches the
throttled caption endpoint.

Phases:
  A (--phase feed):    episodes with a feed/verified transcript URL but no
                       acquisition status row -> record_transcript_source_found.
  B (--phase youtube): fuzzy-match untracked episodes to the cached YouTube
                       channel listings (work/pif-ops/youtube-discovery/) and
                       stamp verified_transcript_url + youtube_caption_found.
                       Uses ONLY existing caches unless --refresh-listings.

Dry-run by default; --apply writes. Disabled sources in config/sources.yaml
are always skipped (wave gating by design).
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from pathlib import Path

PIF_ROOT = Path.home() / "pif-factory"
sys.path.insert(0, str(PIF_ROOT))
from research_factory.ingest import (  # noqa: E402
    record_transcript_source_found, source_kind_for_transcript_url)

DB = PIF_ROOT / "data" / "factory.sqlite"
CACHE_DIR = PIF_ROOT / "work" / "pif-ops" / "youtube-discovery"
WORKER = "backcatalog-discovery"


def disabled_sources() -> set:
    """Source ids with enabled:false. Entries key on `- name:`; the id is
    the slug of the name (matching research_factory.ingest._source_id)."""
    from research_factory.util import slugify
    out, current = set(), None
    for line in (PIF_ROOT / "config" / "sources.yaml").read_text().splitlines():
        m = re.match(r"\s*-\s*name:\s*[\"']?(.+?)[\"']?\s*$", line)
        if m:
            current = slugify(m.group(1))
        m_id = re.match(r"\s*id:\s*([\w-]+)", line)
        if m_id:
            current = m_id.group(1)
        if re.search(r"enabled:\s*false", line) and current:
            out.add(current)
    return out


def norm_tokens(title: str) -> set:
    stop = {"the", "and", "with", "for", "from", "this", "that", "podcast",
            "episode", "show"}
    return {t for t in re.findall(r"[a-z0-9]+", (title or "").lower())
            if len(t) > 2 and t not in stop}


def jaccard(a: set, b: set) -> float:
    return len(a & b) / len(a | b) if a and b else 0.0


def untracked(conn, skip: set):
    rows = conn.execute(
        """
        SELECT e.id, e.source_id, e.title, e.published_at,
               COALESCE(e.verified_transcript_url, e.feed_transcript_url,
                        e.transcript_url) AS url
        FROM episodes e
        LEFT JOIN transcript_acquisition_status t ON t.episode_id = e.id
        LEFT JOIN transcripts tr ON tr.episode_id = e.id
             AND tr.status = 'ready'
        WHERE t.episode_id IS NULL AND tr.id IS NULL
        """).fetchall()
    return [r for r in rows if r["source_id"] not in skip]


def phase_feed(conn, apply: bool) -> dict:
    skip = disabled_sources()
    rows = [r for r in untracked(conn, skip) if r["url"]]
    kinds = {}
    for r in rows:
        kind = source_kind_for_transcript_url(r["url"])
        kinds[kind] = kinds.get(kind, 0) + 1
        if apply:
            record_transcript_source_found(
                conn, episode_id=r["id"], source_kind=kind,
                result_url=r["url"], worker_id=WORKER)
    if apply:
        conn.commit()
    return {"phase": "feed", "candidates": len(rows), "by_kind": kinds,
            "applied": apply, "skipped_sources": sorted(skip)}


def phase_youtube(conn, apply: bool, min_score: float = 0.5) -> dict:
    skip = disabled_sources()
    listings = {}
    for path in CACHE_DIR.glob("*.json"):
        try:
            listings[path.stem] = json.loads(path.read_text())
        except json.JSONDecodeError:
            continue
    rows = [r for r in untracked(conn, skip) if not r["url"]
            and r["source_id"] in listings]
    matched, ambiguous = 0, 0
    per_source = {}
    for r in rows:
        etoks = norm_tokens(r["title"])
        best, second = (0.0, None), 0.0
        for video in listings[r["source_id"]] or []:
            vt = video.get("title") if isinstance(video, dict) else None
            vid = video.get("id") if isinstance(video, dict) else None
            if not vt or not vid:
                continue
            s = jaccard(etoks, norm_tokens(vt))
            if s > best[0]:
                second = best[0]
                best = (s, vid)
            elif s > second:
                second = s
        if best[1] and best[0] >= min_score and best[0] - second >= 0.15:
            matched += 1
            per_source[r["source_id"]] = per_source.get(r["source_id"], 0) + 1
            if apply:
                url = f"https://www.youtube.com/watch?v={best[1]}"
                conn.execute(
                    "UPDATE episodes SET verified_transcript_url = ?"
                    " WHERE id = ? AND verified_transcript_url IS NULL",
                    (url, r["id"]))
                record_transcript_source_found(
                    conn, episode_id=r["id"], source_kind="youtube_captions",
                    result_url=url, worker_id=WORKER)
        elif best[0] >= min_score:
            ambiguous += 1
    if apply:
        conn.commit()
    return {"phase": "youtube", "untracked_with_cached_channel": len(rows),
            "matched": matched, "ambiguous_skipped": ambiguous,
            "per_source": per_source, "cached_channels": sorted(listings),
            "applied": apply}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", choices=["feed", "youtube"], required=True)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--min-score", type=float, default=0.5)
    args = ap.parse_args()
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    if args.phase == "feed":
        out = phase_feed(conn, args.apply)
    else:
        out = phase_youtube(conn, args.apply, args.min_score)
    conn.close()
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
