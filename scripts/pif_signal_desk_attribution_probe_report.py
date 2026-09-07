#!/usr/bin/env python3
"""Build a complete, source-revalidated attribution diagnostic report."""
import json
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.pif_signal_desk_attribution_probe_review import prepare, OUT
from scripts.pif_signal_desk_gold_merge_provenance import immutable_json
from research_factory.signal_desk_attribution_probe_report import summarize


def main():
    packets = prepare()  # Revalidates original source and complete SOL results.
    reviews = {p["review_packet_sha256"]: json.loads((OUT / f"{p['review_packet_sha256']}.review.json").read_text()) for p in packets}
    report = summarize(packets, reviews)
    immutable_json(OUT / "diagnostic-report.json", report)
    print(json.dumps({"totals": report["totals"], "qualified": False}), flush=True)


if __name__ == "__main__": main()
