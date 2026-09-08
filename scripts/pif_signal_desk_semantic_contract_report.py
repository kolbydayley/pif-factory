#!/usr/bin/env python3
"""Read-only by default; --write saves an immutable diagnostic report."""
import argparse
import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.pif_signal_desk_semantic_contract_review import OUT, validate
from scripts.pif_signal_desk_gold_merge_provenance import immutable_json
from research_factory.signal_desk_semantic_contract_report import summarize
from research_factory.signal_desk_rubric_reference_packets import digest


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--write", action="store_true"); args = parser.parse_args()
    report = summarize(OUT, validate)
    if args.write:
        immutable_json(OUT / "reports" / f"{digest(report)}.json", report)
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__": main()
