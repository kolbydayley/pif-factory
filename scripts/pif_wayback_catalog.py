#!/usr/bin/env python3
"""Reconstruct deep episode catalogs from Wayback Machine feed snapshots.

Feeds are rolling windows (newest ~100-300); Apple mirrors the same
window. But web.archive.org has been snapshotting these RSS URLs for
years — union the items across historical snapshots and the full catalog
reassembles. Keyless, universal, polite.

Same output contract as the other catalog pullers (the acquisition
session's inserter consumes it identically); results MERGE into any
existing catalog-index file rather than clobbering it.

Usage: python3 scripts/pif_wayback_catalog.py --source security-now
       python3 scripts/pif_wayback_catalog.py --deep   # the big-gap shows
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
import xml.etree.ElementTree as ET
from pathlib import Path

PIF_ROOT = Path.home() / "pif-factory"
DB = PIF_ROOT / "data" / "factory.sqlite"
OUT_DIR = PIF_ROOT / "work" / "pif-ops" / "catalog-index"
CDX = "https://web.archive.org/cdx/search/cdx"
DEEP = ["marketplace-tech", "security-now", "software-engineering-daily",
        "risky-business", "acquired", "the-knowledge-project",
        "kubernetes-podcast-from-google", "nvidia-ai-podcast",
        "last-week-in-ai", "microsoft-research-podcast"]
MAX_SNAPSHOTS = 40  # ~quarterly over a decade


def _get(url: str, timeout: int = 60) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "pif-catalog/1"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def snapshots_for(feed_url: str) -> list:
    q = urllib.parse.urlencode({
        "url": feed_url, "output": "json", "fl": "timestamp,statuscode",
        "filter": "statuscode:200", "collapse": "timestamp:6"})  # monthly
    rows = json.loads(_get(f"{CDX}?{q}").decode() or "[]")
    stamps = [r[0] for r in rows[1:]]  # drop header
    if len(stamps) > MAX_SNAPSHOTS:  # thin to ~quarterly, keep ends
        step = len(stamps) / MAX_SNAPSHOTS
        stamps = [stamps[int(i * step)] for i in range(MAX_SNAPSHOTS)]
    return stamps


def parse_feed(body: bytes) -> list:
    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        return []
    out = []
    for item in root.iter("item"):
        def txt(tag):
            el = item.find(tag)
            return el.text.strip() if el is not None and el.text else None
        guid = txt("guid")
        title = txt("title")
        pub = txt("pubDate")
        enc = item.find("enclosure")
        dur_el = item.find("{http://www.itunes.com/dtds/podcast-1.0.dtd}duration")
        dur = None
        if dur_el is not None and dur_el.text:
            parts = [int(p) for p in re.findall(r"\d+", dur_el.text)]
            if len(parts) == 1:
                dur = parts[0]
            elif len(parts) == 2:
                dur = parts[0] * 60 + parts[1]
            elif len(parts) >= 3:
                dur = parts[0] * 3600 + parts[1] * 60 + parts[2]
        pub_iso = None
        if pub:
            try:
                import email.utils
                pub_iso = email.utils.parsedate_to_datetime(pub).isoformat()
            except Exception:  # noqa: BLE001
                pass
        if title:
            out.append({"guid": guid, "title": title, "published_at": pub_iso,
                        "audio_url": enc.get("url") if enc is not None else None,
                        "duration_seconds": dur, "description": None,
                        "url": txt("link")})
    return out


def pull_source(conn, source_id: str, sleep_s: int) -> dict:
    row = conn.execute("SELECT rss_url FROM sources WHERE id = ?",
                       (source_id,)).fetchone()
    if not row:
        return {"source_id": source_id, "error": "unknown_source"}
    feed_url = row[0]
    try:
        stamps = snapshots_for(feed_url)
    except Exception as exc:  # noqa: BLE001
        return {"source_id": source_id, "error": f"cdx: {exc}"[:120]}
    merged: dict = {}
    path = OUT_DIR / f"{source_id}.json"
    existing = json.loads(path.read_text()) if path.exists() else None
    if existing:
        for e in existing.get("episodes", []):
            merged[e.get("guid") or e.get("title")] = e
    ok_snaps = 0
    for ts in stamps:
        try:
            body = _get(f"https://web.archive.org/web/{ts}id_/{feed_url}")
            items = parse_feed(body)
            if items:
                ok_snaps += 1
            for e in items:
                merged.setdefault(e.get("guid") or e.get("title"), e)
        except Exception:  # noqa: BLE001
            pass
        time.sleep(sleep_s)
        print(json.dumps({"source": source_id, "snapshot": ts,
                          "catalog": len(merged)}), flush=True)
    out = {"source_id": source_id, "feed_url": feed_url,
           "index_total_episodes": None,
           "catalog_source": (existing or {}).get("catalog_source",
                                                  "") + "+wayback",
           "pulled_at": dt.datetime.now().isoformat(timespec="seconds"),
           "episodes": list(merged.values())}
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out))
    return {"source_id": source_id, "snapshots_used": ok_snaps,
            "catalog_episodes": len(merged)}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", action="append")
    ap.add_argument("--deep", action="store_true")
    ap.add_argument("--sleep", type=int, default=6)
    args = ap.parse_args()
    targets = args.source or (DEEP if args.deep else [])
    conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    for s in targets:
        print(json.dumps(pull_source(conn, s, args.sleep)), flush=True)
    conn.close()


if __name__ == "__main__":
    main()
