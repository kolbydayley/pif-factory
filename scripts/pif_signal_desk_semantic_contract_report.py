#!/usr/bin/env python3
"""Read-only by default; --write saves an immutable diagnostic report."""
import argparse
from collections import Counter
import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.pif_signal_desk_semantic_contract_review import OUT, validate
from scripts.pif_signal_desk_gold_merge_provenance import immutable_json
from research_factory.signal_desk_semantic_contract_report import summarize
from research_factory.signal_desk_rubric_reference_packets import digest


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--write", action="store_true"); parser.add_argument("--recovered", action="store_true"); args = parser.parse_args()
    report = summarize(OUT, validate)
    if args.recovered:
        from scripts.pif_signal_desk_semantic_contract_recovery import collect
        outputs, receipts, pending = collect()
        report["first_pass"] = {k: report[k] for k in ("verdicts", "validated_cases", "complete")}
        report["recovery_receipts"] = {sha: digest(r) for sha, r in receipts.items()}
        report["unresolved_recovery_packets"] = pending
        report["first_pass_failure_preserved"] = True
        for row in report["packets"]:
            sha = row["packet_sha256"]
            if sha not in outputs: continue
            if row["state"] != "held_contract_failure": raise ValueError("recovery replaced nonfailed original")
            value = outputs[sha]
            row["state"] = "validated_recovered_diagnostic"
            row["verdicts"] = dict(Counter(d["verdict"] for d in value["decisions"]))
            for decision in value["decisions"]:
                if decision["verdict"] != "supported" or decision["proposed_rule_change"].strip():
                    report["action_references"].append({"packet_sha256": sha, "case_id": decision["case_id"],
                        "family_id": row["family_id"], "window_id": row["window_id"], "verdict": decision["verdict"],
                        "has_rule_proposal": bool(decision["proposed_rule_change"].strip()), "recovered": True})
        counts = Counter()
        for row in report["packets"]: counts.update(row.get("verdicts", {}))
        report["verdicts"] = dict(counts); report["validated_cases"] = sum(counts.values())
        report["complete"] = all(r["state"] in {"validated_diagnostic", "validated_recovered_diagnostic"} for r in report["packets"])
    if args.write:
        immutable_json(OUT / "reports" / f"{digest(report)}.json", report)
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__": main()
