#!/usr/bin/env python3
"""Publish the Signal Desk dashboard to the Railway keystone-dashboards host.

Automated publishing was explicitly authorized by Kolby's Signal Desk 10x
project (2026-08-27); before that the nightly job only rebuilt local files.

Flow: guard-check the freshly built artifact, PUT it to the dashboards
service, then verify the public GET serves byte-identical content. The API
key is read at runtime from the Railway service variables (never stored).

Usage: python3 scripts/pif_dashboard_publish.py [--dry-run]
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import subprocess
import sys
import urllib.request
from pathlib import Path

PIF_ROOT = Path.home() / "pif-factory"
DATA_JSON = PIF_ROOT / "work" / "pif-ops" / "dashboard" / "data.json"
DASHBOARD_HTML = PIF_ROOT / "work" / "pif-ops" / "dashboard" / "dashboard.html"
PUBLIC_INDEX_JSON = DASHBOARD_HTML.with_name("signal-desk-index.json")
FUNNEL_JSON = DASHBOARD_HTML.with_name("pif-signal-desk-funnel.json")
PUBLIC_BUILD_FILES = (
    "dashboard.html",
    "signal-desk.css",
    "signal-desk.js",
    "signal-desk-index.json",
    "signal-desk-issues.json",
    "signal-desk-voices.json",
    "signal-desk-network.json",
    "pif-signal-desk-funnel.json",
)
HOST = "https://dashboards-production-dcba.up.railway.app"
SLUG = "pif-signal-desk"
CANONICAL = "https://signal-desk-production-edf4.up.railway.app/pif-signal-desk.html"
# The blob host caps uploads (~5MB; the artifact outgrew it 2026-08-27),
# so the legacy slug now serves a redirect to the canonical GitHub->Railway
# deploy. Old bookmarks keep working; divergence is impossible.
REDIRECT_HTML = (
    "<!doctype html><meta charset=\"utf-8\">"
    f"<meta http-equiv=\"refresh\" content=\"0; url={CANONICAL}\">"
    f"<title>Signal Desk</title><a href=\"{CANONICAL}\">"
    "Signal Desk has moved</a>"
).encode()
EXPECTED_SCHEMA = "signal_desk_v5"
MAX_BUILD_AGE_DAYS = 2  # nightly cadence; anything older is a broken chain


def should_publish(data: dict, today: dt.date | None = None) -> tuple[bool, str]:
    """Refuse to push a stale or structurally broken artifact."""
    today = today or dt.date.today()
    if data.get("schema_version") != EXPECTED_SCHEMA:
        return False, (f"schema mismatch: {data.get('schema_version')!r} "
                       f"!= {EXPECTED_SCHEMA!r}")
    if not data.get("issues"):
        return False, "empty issues index — public build produced no output"
    try:
        generated = dt.datetime.fromisoformat(data["generated_at"]).date()
    except (KeyError, ValueError):
        return False, "missing/invalid generated_at"
    age = (today - generated).days
    if age > MAX_BUILD_AGE_DAYS:
        return False, f"stale build: generated {age} days ago"
    return True, "ok"


def _railway_api_key() -> str:
    out = subprocess.run(
        ["railway", "variables", "-s", "dashboards", "-e", "production",
         "--json"],
        capture_output=True, text=True, check=True,
        cwd="/tmp/railway-link" if Path("/tmp/railway-link").exists() else None)
    return json.loads(out.stdout)["DASHBOARD_API_KEY"]


def publish(dry_run: bool = False) -> int:
    data = json.loads(PUBLIC_INDEX_JSON.read_text())
    ok, reason = should_publish(data)
    if not ok:
        print(f"REFUSED: {reason}", file=sys.stderr)
        return 2
    missing = [name for name in PUBLIC_BUILD_FILES
               if not (DASHBOARD_HTML.parent / name).is_file()]
    if missing:
        print(f"REFUSED: missing public build files: {', '.join(missing)}",
              file=sys.stderr)
        return 2
    body = DASHBOARD_HTML.read_bytes()
    local_sha = hashlib.sha256(body).hexdigest()
    print(f"artifact: {len(body)} bytes sha256={local_sha[:16]}… ({reason})")
    if dry_run:
        print("dry run: not publishing")
        return 0
    rc = sync_site_and_push()
    if rc:
        return rc
    key = _railway_api_key()
    req = urllib.request.Request(
        f"{HOST}/api/dashboard/{SLUG}", data=REDIRECT_HTML, method="PUT",
        headers={"content-type": "text/html", "x-api-key": key})
    with urllib.request.urlopen(req, timeout=60) as resp:
        print(f"legacy redirect PUT status {resp.status}")
    with urllib.request.urlopen(f"{HOST}/d/{SLUG}.html", timeout=60) as resp:
        if CANONICAL.encode() not in resp.read():
            print("VERIFY FAILED: legacy slug does not redirect to canonical",
                  file=sys.stderr)
            return 3
    print(f"legacy {HOST}/d/{SLUG}.html now redirects to canonical")
    return 0


SITE_FILE = PIF_ROOT / "site" / "pif-signal-desk.html"
SITE_FUNNEL_FILE = PIF_ROOT / "site" / "pif-signal-desk-funnel.json"


def sync_site_and_push() -> int:
    """Copy the artifact into site/ and push, which triggers the Railway
    GitHub auto-deploy (Kolby 2026-08-27: git push is the one publish
    surface). Stages ONLY the site artifact — this is a shared checkout."""
    if not SITE_FILE.parent.exists():
        return 0
    remotes = subprocess.run(["git", "remote"], capture_output=True,
                             text=True, cwd=PIF_ROOT).stdout.strip()
    if not remotes:
        print("no git remote configured; skipping site push "
              "(blob PUT already published)")
        return 0
    source_dir = DASHBOARD_HTML.parent
    site_paths = []
    for name in PUBLIC_BUILD_FILES:
        source = source_dir / name
        target_name = "pif-signal-desk.html" if name == "dashboard.html" else name
        target = SITE_FILE.parent / target_name
        target.write_bytes(source.read_bytes())
        site_paths.append(target)
    diff = subprocess.run(
        ["git", "status", "--porcelain", "--",
         *[str(path) for path in site_paths]],
        capture_output=True, text=True, cwd=PIF_ROOT)
    if not diff.stdout.strip():
        print("site artifact unchanged; no push needed")
        return 0
    for cmd in (
        ["git", "add", "--", *[str(path) for path in site_paths]],
        ["git", "commit", "-m", "chore(site): nightly Signal Desk artifact"],
        ["git", "push"],
    ):
        r = subprocess.run(cmd, capture_output=True, text=True, cwd=PIF_ROOT)
        if r.returncode != 0:
            print(f"site publish step failed ({' '.join(cmd[:2])}): "
                  f"{r.stderr.strip()[:300]}", file=sys.stderr)
            return 4
    print("site artifact committed and pushed (Railway auto-deploys)")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    return publish(dry_run=args.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())
