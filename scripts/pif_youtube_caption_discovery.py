#!/usr/bin/env python3
"""Discover YouTube caption sources for episodes with no transcript.

Kolby authorized the YouTube captions lane 2026-08-27 ("get transcripts by
any means necessary; YouTube is the backstop"). This worker fills the
discovery gap the factory left manual: it lists a show's own YouTube
channel (creator-published videos only), fuzzy-matches video titles to
untranscribed episodes, and attaches matches through the official
`attach-transcript` CLI (source_kind=youtube_captions), which enqueues the
caption fetch handled by research_factory.ingest via youtube-transcript-api.

Dry-run by default; --apply performs the attaches. Channel listings are
cached under work/pif-ops/youtube-discovery/.

Usage:
  python3 scripts/pif_youtube_caption_discovery.py --source no-priors
  python3 scripts/pif_youtube_caption_discovery.py --all --apply --per-show 40
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import subprocess
import sys
from pathlib import Path

PIF_ROOT = Path.home() / "pif-factory"
DB = PIF_ROOT / "data" / "factory.sqlite"
CACHE_DIR = PIF_ROOT / "work" / "pif-ops" / "youtube-discovery"

# Creator-owned channels only (the factory's transcript policy allows
# creator-published material; these are the shows' own channels).
CHANNELS = {
    "no-priors": "https://www.youtube.com/@NoPriorsPodcast/videos",
    "waveform": "https://www.youtube.com/@Waveform/videos",
    "data-skeptic": "https://www.youtube.com/@DataSkeptic/videos",
    "the-real-python-podcast": "https://www.youtube.com/@realpython/videos",
    "hard-fork": "https://www.youtube.com/@hardfork/videos",
    "the-ezra-klein-show": "https://www.youtube.com/@EzraKleinShow/videos",
    "risky-business": "https://www.youtube.com/@riskybusinesspod/videos",
    "animal-spirits": "https://www.youtube.com/@TheCompoundNews/videos",
    "all-in": "https://www.youtube.com/@allin/videos",
    "the-twiml-ai-podcast": "https://www.youtube.com/@TWIMLAI/videos",
    "big-technology-podcast": "https://www.youtube.com/@BigTechnologyPodcast/videos",
    "eye-on-ai": "https://www.youtube.com/@eyeonai3425/videos",
}

_STOP = {"the", "a", "an", "and", "or", "of", "in", "on", "with", "for",
         "to", "is", "are", "how", "why", "what", "ep", "episode", "podcast"}


def norm_tokens(title: str) -> frozenset:
    t = re.sub(r"[^a-z0-9 ]", " ", (title or "").lower())
    return frozenset(w for w in t.split() if len(w) > 2 and w not in _STOP)


def list_channel(source_id: str, url: str, limit: int,
                 refresh: bool = False) -> list[dict]:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache = CACHE_DIR / f"{source_id}.json"
    if cache.exists() and not refresh:
        return json.loads(cache.read_text())
    cmd = [sys.executable, "-m", "yt_dlp", "--flat-playlist", "-J",
           "--playlist-end", str(limit), url]
    out = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    if out.returncode != 0:
        print(f"  yt-dlp failed for {source_id}: "
              f"{out.stderr.strip().splitlines()[-1][:160] if out.stderr else '?'}",
              file=sys.stderr)
        return []
    data = json.loads(out.stdout)
    entries = [{"id": e["id"], "title": e.get("title") or ""}
               for e in (data.get("entries") or []) if e and e.get("id")]
    cache.write_text(json.dumps(entries))
    return entries


def untranscribed_episodes(conn, source_id: str, since: str,
                           limit: int) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT e.id, e.title, e.published_at FROM episodes e
        WHERE e.source_id = ? AND e.published_at >= ?
          AND e.id NOT IN (SELECT episode_id FROM transcripts)
          AND (e.verified_transcript_url IS NULL)
        ORDER BY e.published_at DESC LIMIT ?
        """, (source_id, since, limit)).fetchall()


def match(episodes, videos) -> list[dict]:
    vid_tokens = [(v, norm_tokens(v["title"])) for v in videos]
    out = []
    used_videos: set[str] = set()
    for ep in episodes:
        etok = norm_tokens(ep["title"])
        if len(etok) < 3:
            continue  # too generic to match safely
        best, best_score = None, 0.0
        for v, vtok in vid_tokens:
            if v["id"] in used_videos or not vtok:
                continue
            inter = len(etok & vtok)
            containment = inter / len(etok)
            jaccard = inter / len(etok | vtok)
            score = max(containment, jaccard)
            if score > best_score:
                best, best_score = v, score
        # Strict floor: a wrong transcript poisons the corpus quietly.
        if best and best_score >= 0.75:
            used_videos.add(best["id"])
            out.append({"episode_id": ep["id"],
                        "episode_title": ep["title"],
                        "published_at": (ep["published_at"] or "")[:10],
                        "video_id": best["id"],
                        "video_title": best["title"],
                        "score": round(best_score, 2)})
    return out


def attach(matches: list[dict]) -> int:
    ok = 0
    for m in matches:
        url = f"https://www.youtube.com/watch?v={m['video_id']}"
        r = subprocess.run(
            [sys.executable, "-m", "research_factory", "attach-transcript",
             "--episode-id", m["episode_id"], "--transcript-url", url,
             "--transcript-type", "text/plain",
             "--source-kind", "youtube_captions"],
            capture_output=True, text=True, cwd=PIF_ROOT)
        if r.returncode == 0:
            ok += 1
        else:
            print(f"  attach failed {m['episode_id']}: "
                  f"{(r.stderr or r.stdout).strip()[:160]}", file=sys.stderr)
    return ok


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", action="append",
                    help="source id; repeatable. Default with --all: every "
                         "mapped channel")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--since", default="2025-06-01")
    ap.add_argument("--per-show", type=int, default=40)
    ap.add_argument("--channel-depth", type=int, default=200,
                    help="how many recent channel videos to list")
    ap.add_argument("--refresh", action="store_true",
                    help="ignore the channel-listing cache")
    ap.add_argument("--apply", action="store_true",
                    help="attach matches (default: dry-run report)")
    args = ap.parse_args()
    targets = args.source or (list(CHANNELS) if args.all else [])
    if not targets:
        ap.error("pass --source <id> or --all")
    conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    total_matched = total_attached = 0
    for sid in targets:
        url = CHANNELS.get(sid)
        if not url:
            print(f"{sid}: no channel mapped; skipping", file=sys.stderr)
            continue
        eps = untranscribed_episodes(conn, sid, args.since, args.per_show)
        if not eps:
            print(f"{sid}: no untranscribed episodes since {args.since}")
            continue
        videos = list_channel(sid, url, args.channel_depth, args.refresh)
        matches = match(eps, videos)
        total_matched += len(matches)
        print(f"{sid}: {len(eps)} untranscribed, {len(videos)} channel "
              f"videos, {len(matches)} matches")
        for m in matches[:5]:
            print(f"    {m['score']:.2f} {m['published_at']} "
                  f"{m['episode_title'][:48]!r} -> {m['video_title'][:48]!r}")
        if args.apply and matches:
            n = attach(matches)
            total_attached += n
            print(f"    attached {n}/{len(matches)}")
    print(json.dumps({"matched": total_matched,
                      "attached": total_attached,
                      "apply": args.apply}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
