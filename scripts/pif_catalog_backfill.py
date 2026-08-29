#!/usr/bin/env python3
"""Backfill full episode catalogs for shows whose RSS feeds truncate.

Kolby 2026-08-28: "make sure ALL episodes of ALL shows are included."
RSS backfill covers feed-complete shows; this tool covers the truncated
ones (publisher serves only recent items) from two supplementary sources:

- --apple-id: iTunes lookup (up to 300 episodes, guid matches RSS so the
  (source_id, guid) upsert dedupes naturally). Works when Apple has the
  full catalog (e.g. Dwarkesh).
- --youtube-channel: full channel listing via yt-dlp; episodes insert
  with guid yt:<video_id>, the watch URL as verified youtube_captions
  transcript source (feeds the caption lane), and title-dedupe against
  existing rows so RSS-era episodes are never duplicated.

Writes go through research_factory.ingest.upsert_episode (the same
function the RSS path uses). Dry-run by default; --apply inserts.
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import subprocess
import sys
import urllib.request
from pathlib import Path

PIF_ROOT = Path.home() / "pif-factory"
sys.path.insert(0, str(PIF_ROOT))

from research_factory.ingest import upsert_episode  # noqa: E402
from research_factory.util import stable_id  # noqa: E402

YTDLP = "/opt/homebrew/bin/yt-dlp"
MIN_SECONDS = 600  # anything shorter is a clip/short, not an episode


def norm_title(t: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", " ", (t or "").lower())).strip()


def existing_titles(conn, source_id: str) -> set[str]:
    return {norm_title(r[0]) for r in conn.execute(
        "SELECT title FROM episodes WHERE source_id = ?", (source_id,))}


def plan_youtube_inserts(source_id: str, existing: set[str],
                         candidates: list[dict]) -> list[dict]:
    out = []
    for c in candidates:
        if not c.get("upload_date") or not c.get("id"):
            continue
        if (c.get("duration") or 0) < MIN_SECONDS:
            continue
        if norm_title(c.get("title", "")) in existing:
            continue
        d = c["upload_date"]
        published = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
        url = f"https://www.youtube.com/watch?v={c['id']}"
        guid = f"yt:{c['id']}"
        out.append({
            "id": stable_id(source_id, guid, prefix="ep_"),
            "source_id": source_id, "guid": guid,
            "title": c["title"], "description": None,
            "url": url, "audio_url": None,
            "published_at": published,
            "duration_seconds": c.get("duration"),
            "feed_transcript_url": None, "feed_transcript_type": None,
            "verified_transcript_url": url,
            "verified_transcript_type": "text/plain",
            "verified_transcript_source_kind": "youtube_captions",
            "transcript_url": url, "transcript_type": "text/plain",
        })
    return out


def youtube_candidates(channel_url: str) -> list[dict]:
    r = subprocess.run(
        [YTDLP, "-J", "--ignore-errors", channel_url],
        capture_output=True, text=True, timeout=3600)
    if not r.stdout.strip():
        raise RuntimeError(f"yt-dlp returned nothing: {r.stderr[-200:]}")
    data = json.loads(r.stdout)
    entries = data.get("entries") or []
    # channel root returns tabs; find the videos playlist
    flat = []
    for e in entries:
        if e and e.get("entries"):
            flat.extend(x for x in e["entries"] if x)
        elif e:
            flat.append(e)
    return [{"id": e.get("id"), "title": e.get("title"),
             "upload_date": e.get("upload_date"),
             "duration": e.get("duration")} for e in flat]


def apple_episodes(source_id: str, apple_id: int) -> list[dict]:
    u = (f"https://itunes.apple.com/lookup?id={apple_id}"
         "&entity=podcastEpisode&limit=300")
    j = json.load(urllib.request.urlopen(u, timeout=60))
    out = []
    for e in j.get("results", []):
        if e.get("kind") != "podcast-episode" or not e.get("episodeGuid"):
            continue
        out.append({
            "id": stable_id(source_id, e["episodeGuid"], prefix="ep_"),
            "source_id": source_id, "guid": e["episodeGuid"],
            "title": e.get("trackName") or "?",
            "description": (e.get("description") or "")[:2000] or None,
            "url": e.get("trackViewUrl"),
            "audio_url": e.get("episodeUrl"),
            "published_at": (e.get("releaseDate") or "")[:10] or None,
            "duration_seconds": int(e["trackTimeMillis"] / 1000)
            if e.get("trackTimeMillis") else None,
            "feed_transcript_url": None, "feed_transcript_type": None,
            "verified_transcript_url": None,
            "verified_transcript_type": None,
            "verified_transcript_source_kind": None,
            "transcript_url": None, "transcript_type": None,
        })
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True)
    ap.add_argument("--apple-id", type=int)
    ap.add_argument("--youtube-channel")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    conn = sqlite3.connect(PIF_ROOT / "data" / "factory.sqlite")
    conn.row_factory = sqlite3.Row
    if args.apple_id:
        eps = apple_episodes(args.source, args.apple_id)
        have_guid = {r[0] for r in conn.execute(
            "SELECT guid FROM episodes WHERE source_id=?", (args.source,))}
        have_title = existing_titles(conn, args.source)
        eps = [e for e in eps if e["guid"] not in have_guid
               and norm_title(e["title"]) not in have_title]
    elif args.youtube_channel:
        cands = youtube_candidates(args.youtube_channel)
        eps = plan_youtube_inserts(args.source,
                                   existing_titles(conn, args.source), cands)
    else:
        ap.error("pass --apple-id or --youtube-channel")
    print(f"{args.source}: {len(eps)} new episodes to insert")
    for e in eps[:5]:
        print(f"  {e['published_at']} {e['title'][:70]!r}")
    if args.apply:
        for e in eps:
            upsert_episode(conn, e)
        conn.commit()
        print(f"inserted {len(eps)}")
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
