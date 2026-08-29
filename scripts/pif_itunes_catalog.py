#!/usr/bin/env python3
"""Pull episode catalogs from Apple's keyless iTunes lookup API.

Fallback for PodcastIndex (signup unavailable 2026-08-29). Same output
contract as pif_podcastindex_catalog.py so the acquisition session's
inserter consumes either file identically. Caveats: Apple caps episode
lookups at 300 per show (recovers the mid-size gaps; the 1,000+ catalogs
still need per-show archive indexers), and Apple guids may differ from
RSS guids (inserter's title-fallback dedupe handles it — the Dwarkesh
lesson). index_total_episodes comes from Apple's trackCount so coverage
reporting stays honest about what the cap left behind.

Usage: python3 scripts/pif_itunes_catalog.py --truncated
       python3 scripts/pif_itunes_catalog.py --source marketplace-tech
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import sqlite3
import time
import urllib.parse
import urllib.request
from pathlib import Path

PIF_ROOT = Path.home() / "pif-factory"
DB = PIF_ROOT / "data" / "factory.sqlite"
OUT_DIR = PIF_ROOT / "work" / "pif-ops" / "catalog-index"
TRUNCATED = ["marketplace-tech", "security-now", "software-engineering-daily",
             "risky-business", "microsoft-research-podcast", "me-myself-and-ai",
             "the-gradient", "acquired", "the-knowledge-project",
             "kubernetes-podcast-from-google", "nvidia-ai-podcast",
             "last-week-in-ai"]


def _get(url: str) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "pif-catalog/1"})
    with urllib.request.urlopen(req, timeout=45) as resp:
        return json.loads(resp.read().decode())


def norm(s: str) -> set:
    return {t for t in re.findall(r"[a-z0-9]+", (s or "").lower())
            if len(t) > 2}


def find_itunes_id(name: str, feed_url: str) -> tuple:
    q = urllib.parse.urlencode({"term": name, "media": "podcast", "limit": 10})
    results = _get(f"https://itunes.apple.com/search?{q}").get("results", [])
    for r in results:  # exact feed match wins
        if (r.get("feedUrl") or "").rstrip("/") == feed_url.rstrip("/"):
            return r.get("collectionId"), r.get("trackCount"), "feed_match"
    nt = norm(name)
    for r in results:  # else strong title match
        if norm(r.get("collectionName", "")) == nt:
            return r.get("collectionId"), r.get("trackCount"), "title_match"
    return None, None, "no_match"


def pull_source(conn, source_id: str) -> dict:
    row = conn.execute(
        "SELECT name, rss_url FROM sources WHERE id = ?", (source_id,)
    ).fetchone()
    if not row:
        return {"source_id": source_id, "error": "unknown_source"}
    name, feed_url = row
    cid, track_count, how = find_itunes_id(name, feed_url)
    if not cid:
        return {"source_id": source_id, "error": "itunes_no_match"}
    time.sleep(2)
    q = urllib.parse.urlencode({"id": cid, "entity": "podcastEpisode",
                                "limit": 300})
    items = _get(f"https://itunes.apple.com/lookup?{q}").get("results", [])
    episodes = []
    for e in items:
        if e.get("wrapperType") != "podcastEpisode":
            continue
        pub = e.get("releaseDate")
        episodes.append({
            "guid": e.get("episodeGuid"),
            "title": e.get("trackName"),
            "published_at": pub,
            "audio_url": e.get("episodeUrl"),
            "duration_seconds": int(e["trackTimeMillis"] / 1000)
            if e.get("trackTimeMillis") else None,
            "description": (e.get("description") or "")[:1000],
            "url": e.get("trackViewUrl"),
        })
    out = {"source_id": source_id, "feed_url": feed_url,
           "index_total_episodes": track_count,
           "catalog_source": f"itunes:{how}",
           "pulled_at": dt.datetime.now().isoformat(timespec="seconds"),
           "episodes": episodes}
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / f"{source_id}.json").write_text(json.dumps(out))
    return {"source_id": source_id, "pulled": len(episodes),
            "apple_total": track_count, "match": how}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", action="append")
    ap.add_argument("--truncated", action="store_true")
    args = ap.parse_args()
    targets = args.source or (TRUNCATED if args.truncated else [])
    conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    for s in targets:
        print(json.dumps(pull_source(conn, s)), flush=True)
        time.sleep(3)
    conn.close()


if __name__ == "__main__":
    main()
