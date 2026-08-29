#!/usr/bin/env python3
"""Pull complete episode catalogs from PodcastIndex (discovery only).

Division agreed 2026-08-29: this side pulls catalogs; the acquisition
session owns insertion (scripts/pif_catalog_backfill.py --podcastindex-file)
with guid-first dedupe. Output contract per show:

  work/pif-ops/catalog-index/<source_id>.json
  {"source_id", "feed_url", "index_total_episodes",  # for coverage reports
   "pulled_at", "episodes": [{guid, title, published_at, audio_url,
                              duration_seconds, description, url}]}

Auth: PODCASTINDEX_KEY and PODCASTINDEX_SECRET env vars (free tier).
Paces one request/2s; safe to re-run.

ToS handling rules (review 2026-08-29, in effect):
- PI responses are transient discovery pointers, NOT a source of record.
  Cached JSONs must be deleted after the insertion pass (--purge, or by
  hand); episode rows are independently re-verifiable from the shows' own
  public feeds, which remain the source of record.
- Credentials are env-only, never written to any file.
- Anything user-facing leaning on PI data carries the attribution already
  in docs/signal-desk/README.md. Never expose the API through anything
  we build.

Usage: python3 scripts/pif_podcastindex_catalog.py --source marketplace-tech
       python3 scripts/pif_podcastindex_catalog.py --truncated  # all gap shows
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import sqlite3
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

PIF_ROOT = Path.home() / "pif-factory"
DB = PIF_ROOT / "data" / "factory.sqlite"
OUT_DIR = PIF_ROOT / "work" / "pif-ops" / "catalog-index"
API = "https://api.podcastindex.org/api/1.0"
# Shows with confirmed feed-truncation gaps (audit 2026-08-29).
TRUNCATED = ["marketplace-tech", "security-now", "software-engineering-daily",
             "risky-business", "microsoft-research-podcast", "me-myself-and-ai",
             "the-gradient", "acquired", "the-knowledge-project",
             "kubernetes-podcast-from-google", "nvidia-ai-podcast",
             "last-week-in-ai"]


def _headers() -> dict:
    key = os.environ.get("PODCASTINDEX_KEY", "")
    secret = os.environ.get("PODCASTINDEX_SECRET", "")
    if not key or not secret:
        raise SystemExit("Set PODCASTINDEX_KEY and PODCASTINDEX_SECRET "
                         "(free at podcastindex.org)")
    ts = str(int(time.time()))
    return {"User-Agent": "pif-catalog/1", "X-Auth-Key": key,
            "X-Auth-Date": ts,
            "Authorization": hashlib.sha1(
                (key + secret + ts).encode()).hexdigest()}


def _get(path: str, params: dict) -> dict:
    url = f"{API}{path}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers=_headers())
    with urllib.request.urlopen(req, timeout=45) as resp:
        return json.loads(resp.read().decode())


def pull_source(conn, source_id: str) -> dict:
    row = conn.execute("SELECT rss_url FROM sources WHERE id = ?",
                       (source_id,)).fetchone()
    if not row:
        return {"source_id": source_id, "error": "unknown_source"}
    feed_url = row[0]
    # PI indexes canonical feeds, not tracking wrappers (same lesson as
    # Wayback) — unwrap and try candidates; 400 = "not found", try next.
    import importlib.util as _ilu
    spec = _ilu.spec_from_file_location(
        "wb", str(PIF_ROOT / "scripts" / "pif_wayback_catalog.py"))
    wb = _ilu.module_from_spec(spec)
    spec.loader.exec_module(wb)
    feed_info, feed_id = {}, None
    import urllib.error
    for candidate in wb.canonical_feed_urls(feed_url):
        try:
            feed = _get("/podcasts/byfeedurl", {"url": candidate})
        except urllib.error.HTTPError as exc:
            if exc.code == 400:
                continue
            raise
        feed_info = feed.get("feed") or {}
        feed_id = feed_info.get("id")
        if feed_id:
            feed_url = candidate
            break
        time.sleep(2)
    if not feed_id:
        return {"source_id": source_id, "error": "feed_not_in_index"}
    time.sleep(2)
    episodes, seen = [], set()
    # episodes/byfeedid supports max=1000; page back with 'since' walking.
    before = None
    for _page in range(30):
        params = {"id": feed_id, "max": 1000}
        if before:
            params["before"] = before
        eps = _get("/episodes/byfeedid", params).get("items") or []
        new = 0
        for e in eps:
            gid = e.get("guid") or str(e.get("id"))
            if gid in seen:
                continue
            seen.add(gid)
            new += 1
            episodes.append({
                "guid": e.get("guid"),
                "title": e.get("title"),
                "published_at": dt.datetime.utcfromtimestamp(
                    e["datePublished"]).isoformat() + "+00:00"
                if e.get("datePublished") else None,
                "audio_url": e.get("enclosureUrl"),
                "duration_seconds": e.get("duration"),
                "description": (e.get("description") or "")[:1000],
                "url": e.get("link"),
            })
        if new == 0 or len(eps) < 1000:
            break
        before = min(e.get("datePublished", 0) for e in eps)
        time.sleep(2)
    out = {"source_id": source_id, "feed_url": feed_url,
           "index_total_episodes": feed_info.get("episodeCount"),
           "pulled_at": dt.datetime.now().isoformat(timespec="seconds"),
           "episodes": episodes}
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / f"{source_id}.json").write_text(json.dumps(out))
    return {"source_id": source_id, "pulled": len(episodes),
            "index_total": feed_info.get("episodeCount")}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", action="append")
    ap.add_argument("--truncated", action="store_true")
    ap.add_argument("--purge", action="store_true",
                    help="Delete this run's cached catalog JSONs (call after"
                         " the insertion pass; ToS retention rule).")
    args = ap.parse_args()
    if args.purge:
        removed = 0
        for f in OUT_DIR.glob("*.json"):
            f.unlink()
            removed += 1
        print(json.dumps({"purged": removed}))
        return
    targets = args.source or (TRUNCATED if args.truncated else [])
    if not targets:
        raise SystemExit("--source or --truncated required")
    conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    for s in targets:
        print(json.dumps(pull_source(conn, s)), flush=True)
        time.sleep(2)
    conn.close()


if __name__ == "__main__":
    main()
