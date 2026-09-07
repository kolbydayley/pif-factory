#!/usr/bin/env python3
"""Freeze isolated all-role qualification inputs; no provider dispatch here."""
import json
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.pif_signal_desk_attribution_probe_run import BASE, freeze
from scripts.pif_signal_desk_gold_merge_provenance import immutable_json
from research_factory.signal_desk_full_event_prompts import packet, receipt
from research_factory.signal_desk_full_event_experiment import schema
from research_factory.signal_desk_rubric_reference_packets import digest
OUT = BASE.parent / "full-event-attribution-qualification-v3"


def prepare():
    freeze()  # Checks original development-only source selection and byte hashes.
    old_plan = json.loads((BASE / "plan.json").read_text())
    if len(old_plan["packet_digests"]) != 16: raise ValueError("complete development set required")
    sources = []
    for sha in old_plan["packet_digests"]:
        source = json.loads((BASE / f"{sha}.packet.json").read_text())
        if digest({k: v for k, v in source.items() if k != "packet_sha256"}) != sha:
            raise ValueError("source packet digest mismatch")
        sources.append(source)
    if len({p["window_id"] for p in sources}) != 16: raise ValueError("duplicate development windows")
    packets = [packet(source, role) for source in sources for role in ("A", "B", "AUDIT")]
    plan = {"contract": receipt(), "window_ids": [s["window_id"] for s in sources],
        "source_packets": {s["window_id"]: s["packet_sha256"] for s in sources},
        "independent_packets": [p["packet_sha256"] for p in packets],
        "sol_calls": {"A": 16, "B": 16, "C": 16, "AUDIT": 16},
        "final_review": "GPT-5.5 approval-only packets, <=25 candidates and <=12k input tokens",
        "model": "gpt-5.6-sol", "reasoning": "medium", "concurrency": 2,
        "gold_accepted": False, "qualified": False, "sealed_items_opened": False,
        "execution_ready": False, "required_before_execution": ["schema-aware metered runner and recovery tests", "final-review envelope sharing common semantics"],
        "denominators": "all 16 original windows; no recall/benchmark gate claims from qualification",
        "comparison": "schema-and-semantics experiment, not a comparable v2 tournament round"}
    OUT.mkdir(parents=True, exist_ok=True, mode=0o700)
    for value in packets: immutable_json(OUT / "packets" / f"{value['packet_sha256']}.json", value)
    immutable_json(OUT / "schema.json", schema())
    immutable_json(OUT / "plan.json", plan)
    return plan


if __name__ == "__main__":
    plan = prepare()
    print(json.dumps({"windows": len(plan["window_ids"]), "provider_calls_started": 0, "execution_ready": False}))
