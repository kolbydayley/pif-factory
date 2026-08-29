#!/usr/bin/env python3
"""Index NPR-era How I Built This official transcripts (discovery only).

NPR hosts official transcripts at npr.org/transcripts/<storyId> for the
2016-2022 era of the show. This walks the series archive month by month,
extracts story ids + titles, containment-matches to corpus episodes, and
registers official_show_transcript sources. Fetching stays with the
acquisition session. Zero enqueues.

Usage: python3 scripts/pif_npr_hibt_index.py [--apply]
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import sqlite3
import sys
import time
import urllib.request
from pathlib import Path

PIF_ROOT = Path.home() / "pif-factory"
sys.path.insert(0, str(PIF_ROOT))
from research_factory.ingest import record_transcript_source_found  # noqa: E402

DB = PIF_ROOT / "data" / "factory.sqlite"
CACHE = PIF_ROOT / "work" / "pif-ops" / "official-transcript-index" / "npr-hibt.json"
ARCHIVE = "https://www.npr.org/series/490248027/how-i-built-this/archive?date={d}"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
SOURCE_ID = "how-i-built-this"


def norm_tokens(title: str) -> set:
    stop = {"the", "and", "with", "for", "from", "this", "that", "how",
            "built", "live", "npr"}
    return {t for t in re.findall(r"[a-z0-9]+", (title or "").lower())
            if len(t) > 2 and t not in stop}


def crawl() -> dict:
    entries = json.loads(CACHE.read_text()) if CACHE.exists() else {}
    d = dt.date(2016, 10, 1)
    while d <= dt.date(2023, 3, 1):
        url = ARCHIVE.format(d=d.strftime("%m-%d-%Y"))
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            body = urllib.request.urlopen(req, timeout=45).read().decode(
                "utf-8", "replace")
            found = 0
            for _u, sid, title in re.findall(
                    r'href="(https://www\.npr\.org/2\d{3}/\d{2}/\d{2}/(\d+)/'
                    r'[^"]+)"[^>]*>([^<]{8,140})<', body):
                title = title.strip()
                if sid not in entries and title:
                    entries[sid] = title
                    found += 1
            print(json.dumps({"date": d.isoformat(), "new": found,
                              "total": len(entries)}), flush=True)
        except Exception as exc:  # noqa: BLE001
            print(json.dumps({"date": d.isoformat(),
                              "error": str(exc)[:100]}), flush=True)
        CACHE.parent.mkdir(parents=True, exist_ok=True)
        CACHE.write_text(json.dumps(entries))
        d = (d.replace(day=1) + dt.timedelta(days=40)).replace(day=1)
        time.sleep(3)
    return entries


def match_and_register(conn, entries: dict, apply: bool) -> dict:
    rows = conn.execute(
        """SELECT e.id, e.title FROM episodes e
           LEFT JOIN transcripts t ON t.episode_id = e.id
           LEFT JOIN transcript_acquisition_status s ON s.episode_id = e.id
           WHERE e.source_id = ? AND t.id IS NULL
             AND (s.status IS NULL OR s.status NOT LIKE '%found')""",
        (SOURCE_ID,)).fetchall()
    vtoks = [(sid, norm_tokens(title)) for sid, title in entries.items()]
    matched, ambiguous = 0, 0
    for r in rows:
        et = norm_tokens(r["title"])
        best, second = (0.0, None), 0.0
        for sid, vt in vtoks:
            s = len(et & vt) / len(et) if et else 0.0
            if s > best[0]:
                second = best[0]
                best = (s, sid)
            elif s > second:
                second = s
        if best[1] and best[0] >= 0.8 and best[0] - second >= 0.1:
            matched += 1
            if apply:
                url = f"https://www.npr.org/transcripts/{best[1]}"
                conn.execute(
                    "UPDATE episodes SET verified_transcript_url = ?"
                    " WHERE id = ? AND verified_transcript_url IS NULL",
                    (url, r["id"]))
                record_transcript_source_found(
                    conn, episode_id=r["id"],
                    source_kind="official_show_transcript",
                    result_url=url, worker_id="npr-hibt-index")
        elif best[0] >= 0.8:
            ambiguous += 1
    if apply:
        conn.commit()
    return {"corpus_candidates": len(rows), "archive_entries": len(entries),
            "matched": matched, "ambiguous_skipped": ambiguous,
            "applied": apply}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    entries = crawl()
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    print(json.dumps(match_and_register(conn, entries, args.apply)))
    conn.close()


if __name__ == "__main__":
    main()
