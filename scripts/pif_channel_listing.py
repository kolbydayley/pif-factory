#!/usr/bin/env python3
"""Expand/deepen YouTube channel listings for back-catalog discovery.

Round-2 division (agreed with the acquisition session 2026-08-29): listing
expansion is this side's job; caches go to work/pif-ops/youtube-discovery/
in the same [{id,title}] format (plus a duration field on new entries).

Safety rails from the peer's field lessons:
- SANITY CHECK before trusting a channel: the listing must title-match
  >=25% of a 40-episode sample of the show's catalog, else we write
  <slug>.skip-note instead of a cache (the hard-fork clips pattern).
- Duration floor 1500s: shorter uploads are segment clips, not episodes.
- Modest pace: one yt-dlp metadata call per channel, sleep between.

Usage: python3 scripts/pif_channel_listing.py --slug syntax --url https://...
       python3 scripts/pif_channel_listing.py --batch batch.json
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import subprocess
import time
from pathlib import Path

PIF_ROOT = Path.home() / "pif-factory"
DB = PIF_ROOT / "data" / "factory.sqlite"
CACHE_DIR = PIF_ROOT / "work" / "pif-ops" / "youtube-discovery"
MIN_DURATION_S = 1500
SANITY_SAMPLE = 40
SANITY_MIN_MATCH = 0.25


def norm_tokens(title: str) -> set:
    stop = {"the", "and", "with", "for", "from", "this", "that", "podcast",
            "episode", "show"}
    return {t for t in re.findall(r"[a-z0-9]+", (title or "").lower())
            if len(t) > 2 and t not in stop}


def jaccard(a: set, b: set) -> float:
    return len(a & b) / len(a | b) if a and b else 0.0


def list_channel(url: str, depth: int) -> list:
    proc = subprocess.run(
        ["yt-dlp", "--flat-playlist", "--playlist-end", str(depth), "-J", url],
        capture_output=True, text=True, timeout=600)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr[-300:])
    data = json.loads(proc.stdout)
    out = []
    for e in data.get("entries") or []:
        if not e or not e.get("id") or not e.get("title"):
            continue
        dur = e.get("duration")
        if dur is not None and dur < MIN_DURATION_S:
            continue  # clip, not an episode
        out.append({"id": e["id"], "title": e["title"],
                    "duration": dur})
    return out


def sanity(conn, slug: str, listing: list) -> float:
    eps = conn.execute(
        "SELECT title FROM episodes WHERE source_id = ?"
        " ORDER BY published_at DESC LIMIT ?", (slug, SANITY_SAMPLE)).fetchall()
    if not eps:
        return 0.0
    vtoks = [norm_tokens(v["title"]) for v in listing]
    hits = 0
    for (title,) in eps:
        et = norm_tokens(title)
        if any(jaccard(et, vt) >= 0.5 for vt in vtoks):
            hits += 1
    return hits / len(eps)


def run_one(conn, slug: str, url: str, depth: int) -> dict:
    try:
        listing = list_channel(url, depth)
    except Exception as exc:  # noqa: BLE001
        return {"slug": slug, "error": str(exc)[:160]}
    score = sanity(conn, slug, listing)
    if score < SANITY_MIN_MATCH:
        note = CACHE_DIR / f"{slug}.skip-note"
        note.write_text(
            f"channel {url} failed sanity: only {score:.0%} of a "
            f"{SANITY_SAMPLE}-episode sample title-matched (floor "
            f"{SANITY_MIN_MATCH:.0%}). Likely clips/re-titles/wrong channel "
            f"(hard-fork pattern). {time.strftime('%F %T')}\n")
        return {"slug": slug, "videos": len(listing),
                "sanity": round(score, 2), "verdict": "SKIP-NOTE written"}
    (CACHE_DIR / f"{slug}.json").write_text(json.dumps(listing))
    return {"slug": slug, "videos": len(listing),
            "sanity": round(score, 2), "verdict": "cached"}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--slug")
    ap.add_argument("--url")
    ap.add_argument("--batch", type=Path,
                    help="JSON file: [{slug, url}, ...]")
    ap.add_argument("--depth", type=int, default=2500)
    ap.add_argument("--sleep", type=int, default=45)
    args = ap.parse_args()
    conn = sqlite3.connect(DB)
    targets = (json.loads(args.batch.read_text()) if args.batch
               else [{"slug": args.slug, "url": args.url}])
    for i, t in enumerate(targets):
        print(json.dumps(run_one(conn, t["slug"], t["url"], args.depth)),
              flush=True)
        if i < len(targets) - 1:
            time.sleep(args.sleep)


if __name__ == "__main__":
    main()
