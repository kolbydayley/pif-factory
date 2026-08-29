#!/usr/bin/env python3
"""Index developertea.com's creator-published transcripts (discovery only).

Web dig 2026-08-29 (Kolby: "dig harder for transcripts on the web") found
every developertea.com episode page carries a full creator-published
Whisper transcript — 1,239 corpus episodes with no other text route.
This crawls the paginated episode index (66 pages, polite pace), fuzzy-
matches page titles to corpus episodes, and registers each match as an
official_show_transcript source (record_transcript_source_found +
verified_transcript_url). Fetching stays with the acquisition session's
lanes, per the standing division. Zero job enqueues.

Usage: python3 scripts/pif_devtea_transcript_index.py [--apply]
       [--pages N] [--sleep S]
"""
from __future__ import annotations

import argparse
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
CACHE = PIF_ROOT / "work" / "pif-ops" / "official-transcript-index"
BASE = "https://developertea.com"
SOURCE_ID = "developer-tea"


def norm_tokens(title: str) -> set:
    stop = {"the", "and", "with", "for", "from", "this", "that"}
    return {t for t in re.findall(r"[a-z0-9]+", (title or "").lower())
            if len(t) > 2 and t not in stop}


def jaccard(a: set, b: set) -> float:
    return len(a & b) / len(a | b) if a and b else 0.0


def fetch(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "pif-index/1"})
    with urllib.request.urlopen(req, timeout=45) as resp:
        return resp.read().decode("utf-8", "replace")


def crawl_index(pages: int, sleep_s: int) -> list:
    CACHE.mkdir(parents=True, exist_ok=True)
    cache_file = CACHE / "developer-tea.json"
    entries = json.loads(cache_file.read_text()) if cache_file.exists() else []
    seen = {e["url"] for e in entries}
    pattern = re.compile(
        r'href="(/episodes/[0-9a-f-]{36})"[^>]*>(.*?)</a>', re.S)
    for p in range(1, pages + 1):
        try:
            body = fetch(f"{BASE}/episodes?p={p}")
        except Exception as exc:  # noqa: BLE001
            print(json.dumps({"page": p, "error": str(exc)[:120]}), flush=True)
            time.sleep(sleep_s)
            continue
        found = 0
        for path, raw_title in pattern.findall(body):
            url = BASE + path
            if url in seen:
                continue
            title = re.sub(r"<[^>]+>|\s+", " ", raw_title).strip()
            if not title:
                continue
            entries.append({"url": url, "title": title})
            seen.add(url)
            found += 1
        print(json.dumps({"page": p, "new": found, "total": len(entries)}),
              flush=True)
        cache_file.write_text(json.dumps(entries))
        if found == 0 and p > 2:
            pass  # keep going; old pages may still be uncached
        time.sleep(sleep_s)
    return entries


def match_and_register(conn, entries: list, apply: bool) -> dict:
    rows = conn.execute(
        """
        SELECT e.id, e.title FROM episodes e
        LEFT JOIN transcripts t ON t.episode_id = e.id
        WHERE e.source_id = ? AND t.id IS NULL
        """, (SOURCE_ID,)).fetchall()
    etoks = [(r[0], r[1], norm_tokens(r[1])) for r in rows]
    vtoks = [(e["url"], norm_tokens(e["title"])) for e in entries]
    matched, ambiguous = 0, 0
    for ep_id, _title, et in etoks:
        best, second = (0.0, None), 0.0
        for url, vt in vtoks:
            # Web anchors append the episode DESCRIPTION to the title, so
            # Jaccard dilutes; score by episode-side coverage instead.
            s = len(et & vt) / len(et) if et else 0.0
            if s > best[0]:
                second = best[0]
                best = (s, url)
            elif s > second:
                second = s
        if best[1] and best[0] >= 0.85 and best[0] - second >= 0.1:
            matched += 1
            if apply:
                conn.execute(
                    "UPDATE episodes SET verified_transcript_url = ?"
                    " WHERE id = ? AND verified_transcript_url IS NULL",
                    (best[1], ep_id))
                record_transcript_source_found(
                    conn, episode_id=ep_id,
                    source_kind="official_show_transcript",
                    result_url=best[1], worker_id="devtea-index")
        elif best[0] >= 0.85:
            ambiguous += 1
    if apply:
        conn.commit()
    return {"corpus_episodes_unmatched_before": len(rows),
            "index_entries": len(entries), "matched": matched,
            "ambiguous_skipped": ambiguous, "applied": apply}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--pages", type=int, default=66)
    ap.add_argument("--sleep", type=int, default=4)
    args = ap.parse_args()
    entries = crawl_index(args.pages, args.sleep)
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    print(json.dumps(match_and_register(conn, entries, args.apply)))
    conn.close()


if __name__ == "__main__":
    main()
