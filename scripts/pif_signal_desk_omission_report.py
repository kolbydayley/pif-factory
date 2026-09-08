#!/usr/bin/env python3
"""Produce a validated diagnostic snapshot without starting any provider calls."""
import argparse
import json
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.pif_signal_desk_omission_review import OUT
from scripts.pif_signal_desk_gold_merge_provenance import immutable_json
from research_factory.signal_desk_omission_report import summarize
from research_factory.signal_desk_rubric_reference_packets import digest


def main():
    p = argparse.ArgumentParser(); p.add_argument("--partial", action="store_true")
    p.add_argument("--recovered", action="store_true"); args = p.parse_args()
    plan = json.loads((OUT / "plan.json").read_text())
    packets = {sha: json.loads((OUT / f"{sha}.packet.json").read_text()) for sha in plan["packets"]}
    reviews = {sha: json.loads((OUT / f"{sha}.review.json").read_text()) for sha in plan["packets"] if (OUT / f"{sha}.review.json").exists()}
    first_pass = len(reviews); recovery_receipts = {}
    if args.recovered:
        from scripts.pif_signal_desk_omission_recovery import prepare, collect
        originals, selected, recovery_plan = prepare()
        recovered, recovery_receipts, _ = collect(originals, selected, recovery_plan)
        if set(recovered) & set(reviews): raise ValueError("recovery overwrites a first-pass result")
        reviews.update(recovered)
    result = summarize(plan, packets, reviews, allow_partial=args.partial)
    result["first_pass_validated_packets"] = first_pass
    result["reconciled_packets"] = len(recovery_receipts)
    result["recovery_receipt_inventory"] = {sha: digest(r) for sha, r in recovery_receipts.items()}
    immutable_json(OUT / "reports" / f"{digest(result)}.json", result)
    if result["complete_diagnostic"]:
        immutable_json(OUT / ("reconciled-diagnostic-report.json" if args.recovered else "diagnostic-report.json"), result)
    print(json.dumps({k: result[k] for k in ("windows", "expected_cases", "expected_packets", "reviewed_packets", "complete_diagnostic", "gold_accepted")}))


if __name__ == "__main__": main()
