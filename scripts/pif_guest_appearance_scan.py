#!/usr/bin/env python3
"""Guest-appearance discovery: where do tracked-interest people appear?

Kolby 2026-08-28: expand the upper funnel by following people (Dario
Amodei, Sam Altman, Elon Musk, ...) to shows the corpus doesn't track.
Searches YouTube via yt-dlp (no API key), groups hits by channel, and
reports channels absent from config/sources.yaml — a curation aid, never
an auto-adder.

Usage: python3 scripts/pif_guest_appearance_scan.py "Dario Amodei" "Sam Altman"
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

PIF_ROOT = Path.home() / "pif-factory"
YTDLP = "/opt/homebrew/bin/yt-dlp"
PER_PERSON = 30


def tracked_names() -> set[str]:
    import re
    text = (PIF_ROOT / "config" / "sources.yaml").read_text()
    return {m.lower() for m in re.findall(r"- name: (.+)", text)}


def scan(person: str) -> list[dict]:
    q = f"ytsearch{PER_PERSON}:\"{person}\" interview podcast"
    r = subprocess.run([YTDLP, "--flat-playlist", "-J", q],
                       capture_output=True, text=True, timeout=300)
    if r.returncode != 0:
        print(f"search failed for {person}: {r.stderr[-160:]}",
              file=sys.stderr)
        return []
    data = json.loads(r.stdout)
    return [{"title": e.get("title") or "", "channel": e.get("channel")
             or e.get("uploader") or "?", "id": e.get("id"),
             "duration": e.get("duration")}
            for e in (data.get("entries") or []) if e]


def main() -> int:
    people = sys.argv[1:] or ["Dario Amodei", "Sam Altman", "Elon Musk"]
    tracked = tracked_names()
    report: dict[str, dict] = {}
    for person in people:
        for hit in scan(person):
            # long-form only: an interview is >20 min, clips are noise
            if not hit["duration"] or hit["duration"] < 1200:
                continue
            ch = hit["channel"]
            key = ch.lower()
            entry = report.setdefault(ch, {"tracked": any(
                key in t or t in key for t in tracked), "hits": []})
            entry["hits"].append({"person": person,
                                  "title": hit["title"][:90],
                                  "video": hit["id"]})
    untracked = {ch: v for ch, v in report.items() if not v["tracked"]}
    print(json.dumps({"untracked_channels": untracked,
                      "tracked_channel_count":
                          len(report) - len(untracked)}, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
