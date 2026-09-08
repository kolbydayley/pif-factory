#!/usr/bin/env python3
"""Freeze and review every old/current candidate plus cross-role disagreements."""
import argparse
import asyncio
import fcntl
import json
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.pif_signal_desk_full_event_review import prepare as validate_chain, QUAL, BASE, OUT as FINAL
from scripts.pif_signal_desk_rubric_reference_review import QUAL as OLD
from scripts.pif_signal_desk_attribution_probe_review import execute as metered_review
from scripts.pif_signal_desk_gold_merge_provenance import immutable_json
from scripts.pif_signal_desk_full_event_report import summarize
from research_factory.signal_desk_rubric_reference_packets import digest
from research_factory.signal_desk_omission_review import packets, SYSTEM, schema, validate

OUT = QUAL / "source-disagreement-review-v1"


def prepare():
    import tiktoken
    enc = tiktoken.get_encoding("o200k_base")
    final_packets = validate_chain()
    final_reviews = {p["packet_sha256"]: json.loads((FINAL / f"{p['packet_sha256']}.review.json").read_text()) for p in final_packets}
    frozen = json.loads((QUAL / "plan.json").read_text())
    contrast = json.loads((BASE / "plan.json").read_text())
    authored = {}; structures = {}; sources = {}
    for wid, sha in frozen["source_packets"].items():
        sources[wid] = json.loads((BASE / f"{sha}.packet.json").read_text())
        structures[wid] = sources[wid]["transcript_structure"]
        authored[wid] = {}
        for role in ("A", "B", "C", "AUDIT"):
            directory = QUAL / "calls" / wid / role
            p = json.loads((directory / "packet.json").read_text())
            authored[wid][role] = json.loads((directory / f"{p['packet_sha256']}.result.json").read_text())
    report = summarize(final_packets, final_reviews, authored, structures)
    selected = []; ledger = {}
    for wid in sorted(authored):
        old = json.loads((OLD / "results" / "C" / f"{wid}.json").read_text())
        if digest(old) != contrast["candidate_inventory"][wid]: raise ValueError("legacy candidate bytes changed")
        cases = []
        def add(kind, origin, event, **extra):
            value = {"kind": kind, "origin": origin, "event": event, **extra}
            value["case_id"] = digest([wid, value])
            cases.append(value)
        for event in old["events"]: add("candidate", "legacy_C", event)
        for event in authored[wid]["C"]["events"]: add("candidate", "current_C", event)
        roles = {}
        for row in report["comparisons"]:
            if row["window_id"] != wid: continue
            required = set(row["unmatched_prediction"]) | {d["prediction_event_id"] for d in row["differences"]}
            for event in authored[wid][row["role"]]["events"]:
                if event["event_id"] in required: add("candidate", row["role"], event)
            roles[row["role"]] = {"total": row["prediction_count"], "reviewed": len(required),
                "matched_field_agreement_not_independent_acceptance": row["prediction_count"] - len(required)}
        for p in final_packets:
            if p["window_id"] != wid: continue
            originals = {e["event_id"]: e for e in p["candidates"]}
            for decision in final_reviews[p["packet_sha256"]]["decisions"]:
                if decision["verdict"] != "supported":
                    add("correction", "final_review", originals[decision["event"]["event_id"]],
                        proposed_event=decision["event"], prior_rationale=decision["rationale"],
                        prior_verdict=decision["verdict"])
        if not cases:
            add("empty_source", "all_empty", None)
        provenance = {"legacy_C_sha256": digest(old), "authored_sha256": digest(authored[wid]),
            "final_review_inventory_sha256": digest(report["review_inventory"]), "original_source_packet_sha256": sources[wid]["packet_sha256"]}
        selected.extend(packets(window_id=wid, source=sources[wid]["transcript_window"], structure=structures[wid],
            cases=cases, current=authored[wid]["C"]["events"], provenance=provenance, token_count=lambda s: len(enc.encode(s))))
        ledger[wid] = {"legacy_C": len(old["events"]), "current_C": len(authored[wid]["C"]["events"]),
            "roles": roles, "cases": len(cases), "case_ids": [c["case_id"] for c in cases]}
    if len(ledger) != 16 or sum(r["legacy_C"] for r in ledger.values()) != 477 or sum(r["current_C"] for r in ledger.values()) != 198:
        raise ValueError("frozen diagnostic population changed")
    OUT.mkdir(parents=True, exist_ok=True, mode=0o700)
    for p in selected: immutable_json(OUT / f"{p['packet_sha256']}.packet.json", p)
    immutable_json(OUT / "plan.json", {"packets": [p["packet_sha256"] for p in selected], "ledger": ledger,
        "system": SYSTEM, "system_sha256": digest(SYSTEM), "schema_sha256": digest(schema()),
        "model": "gpt-5.5", "effort": "high", "concurrency": 1, "max_candidates": 25, "max_input_tokens": 12000,
        "gold_accepted": False, "gate_eligible": False,
        "sampling": "Complete sixteen development windows; all legacy C and current C, all unmatched or field-disagreeing independent events, all final proposals. Purposive diagnostic, not a prevalence estimate."})
    return selected


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--execute", action="store_true"); args = parser.parse_args()
    QUAL.mkdir(parents=True, exist_ok=True)
    with (QUAL / "source-disagreement-review.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        selected = prepare()
        print(json.dumps({"windows": len({p['window_id'] for p in selected}), "packets": len(selected),
            "cases": sum(len(p["cases"]) for p in selected), "gold_accepted": False}), flush=True)
        if args.execute:
            raise SystemExit(asyncio.run(metered_review(selected, output_root=OUT,
                task_prefix="full-event-v3-source-disagreement-v1", system=SYSTEM,
                schema_for_packet=lambda p: schema(), validator=validate,
                packet_id=lambda p: p["packet_sha256"], verdict_rows=lambda v: v["decisions"])))


if __name__ == "__main__": main()
