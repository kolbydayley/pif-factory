#!/usr/bin/env python3
"""Build the split, trust-aware Signal Desk V5 static application."""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

PIF_ROOT = Path.home() / "pif-factory"
if str(PIF_ROOT) not in sys.path:
    sys.path.insert(0, str(PIF_ROOT))

from research_factory.signal_desk_intelligence import build_payloads


DATA = PIF_ROOT / "work" / "pif-ops" / "dashboard" / "data.json"
OUT_DIR = DATA.parent
OUT = OUT_DIR / "dashboard.html"
ASSET_ROOT = PIF_ROOT / "scripts" / "signal_desk_assets"
FUNNEL_OUT = OUT_DIR / "pif-signal-desk-funnel.json"

PAYLOAD_FILES = {
    "index": "signal-desk-index.json",
    "issues": "signal-desk-issues.json",
    "voices": "signal-desk-voices.json",
    "network": "signal-desk-network.json",
    "coverage": "pif-signal-desk-funnel.json",
}

TEMPLATE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
  <meta name="theme-color" content="#faf8f3">
  <meta name="description" content="Evidence-grounded intelligence about changes in technology discourse.">
  <title>Signal Desk</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,500;9..144,700;9..144,900&family=IBM+Plex+Sans:wght@400;500;600&family=IBM+Plex+Mono:wght@400;500&display=swap" rel="stylesheet">
  <link rel="stylesheet" href="./signal-desk.css">
</head>
<body>
  <div class="shell">
    <header class="topbar">
      <button class="brand" data-route="briefing" aria-label="Open Signal Desk briefing">
        <strong>Signal Desk</strong><span>strategic intelligence</span>
      </button>
      <nav class="desktop-nav" aria-label="Primary">
        <button class="nav-link" data-route="briefing" data-nav="briefing">Briefing</button>
        <button class="nav-link" data-route="issues" data-nav="issues">Issues</button>
        <button class="nav-link" data-route="voices" data-nav="voices">Voices</button>
        <button class="nav-link" data-route="ask" data-nav="ask">Ask</button>
      </nav>
      <button class="coverage-link" data-route="coverage" data-nav="coverage">Coverage &amp; Trust</button>
    </header>
    <main id="app" tabindex="-1" aria-live="polite">
      <section class="view loading-state"><div class="eyebrow">Loading</div><h1>Building the briefing…</h1></section>
    </main>
    <nav class="bottom-nav" aria-label="Mobile primary">
      <button class="nav-link" data-route="briefing" data-nav="briefing">Briefing</button>
      <button class="nav-link" data-route="issues" data-nav="issues">Issues</button>
      <button class="nav-link" data-route="voices" data-nav="voices">Voices</button>
      <button class="nav-link" data-route="ask" data-nav="ask">Ask</button>
    </nav>
  </div>
  <script defer src="./signal-desk.js"></script>
</body>
</html>
"""


def _last_share(topic_info):
    smooth = [s.get("share_smooth") for s in topic_info.get("series", [])
              if s.get("vol")]
    return smooth[-1] if smooth else None


def compute_diff(cur, prev):
    """Return a compact change log against the prior aggregate build."""
    if not prev:
        return None
    diff = {"since": prev.get("generated_at", "")[:10],
            "detectors": [], "movers": [], "new_people": []}
    for family, entries in cur.get("detectors", {}).items():
        cur_map = {e["topic"]: e for e in entries}
        prev_map = {e["topic"]: e
                    for e in prev.get("detectors", {}).get(family, [])}
        for topic, entry in cur_map.items():
            if topic not in prev_map:
                diff["detectors"].append(
                    {"topic": topic, "family": family, "change": "new",
                     "tier": entry.get("tier")})
            elif entry.get("tier") != prev_map[topic].get("tier"):
                diff["detectors"].append(
                    {"topic": topic, "family": family, "change": "tier",
                     "from": prev_map[topic].get("tier"),
                     "to": entry.get("tier")})
        for topic in prev_map:
            if topic not in cur_map:
                diff["detectors"].append(
                    {"topic": topic, "family": family, "change": "gone"})
    movers = []
    for topic, info in cur.get("topics", {}).items():
        previous = prev.get("topics", {}).get(topic)
        if not previous:
            continue
        current_share, previous_share = _last_share(info), _last_share(previous)
        if current_share is None or previous_share is None:
            continue
        if abs(current_share - previous_share) >= 0.005:
            movers.append({"topic": topic,
                           "delta": round(current_share - previous_share, 4)})
    movers.sort(key=lambda item: -abs(item["delta"]))
    diff["movers"] = movers[:6]
    old_names = {p.get("name") for p in prev.get("people", [])}
    diff["new_people"] = [p["name"] for p in cur.get("people", [])
                          if p.get("name") not in old_names][:8]
    return diff


def _read_previous() -> dict | None:
    path = DATA.with_name("data_prev.json")
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None


def main() -> None:
    data = json.loads(DATA.read_text())
    payloads = build_payloads(data, compute_diff(data, _read_previous()))
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    OUT.write_text(TEMPLATE)
    for name in ("signal-desk.css", "signal-desk.js"):
        shutil.copyfile(ASSET_ROOT / name, OUT_DIR / name)
    sizes = {}
    for key, filename in PAYLOAD_FILES.items():
        path = OUT_DIR / filename
        path.write_text(json.dumps(payloads[key], separators=(",", ":")))
        sizes[filename] = path.stat().st_size
    print(json.dumps({
        "schema_version": payloads["index"]["schema_version"],
        "out": str(OUT), "html_bytes": OUT.stat().st_size,
        "payload_bytes": sizes,
        "issues": len(payloads["issues"]["issues"]),
        "voices": len(payloads["voices"]["voices"]),
        "decision_grade": sum(
            1 for item in payloads["index"]["briefing"]
            if item["brief"]["decision_grade"]),
    }, indent=2))


if __name__ == "__main__":
    main()
