#!/usr/bin/env python3
"""Complete-source final review, requiring all full-event qualification roles."""
import argparse
import asyncio
import fcntl
import json
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.pif_signal_desk_full_event_qualification import prepare as freeze, OUT as QUAL, BASE
from scripts.pif_signal_desk_attribution_probe_review import execute as metered_review
from scripts.pif_signal_desk_gold_merge_provenance import immutable_json
from research_factory.signal_desk_full_event_prompts import packet
from research_factory.signal_desk_full_event_experiment import validate
from research_factory.signal_desk_full_event_review import packets as review_packets, SYSTEM, schema, validate_review
from research_factory.signal_desk_rubric_reference_packets import digest
OUT = QUAL / "final-review"


def prepare():
    import tiktoken
    enc = tiktoken.get_encoding("o200k_base")
    plan = freeze(); selected = []; inventory = {}
    execution = json.loads((QUAL / "execution-contract.json").read_text())
    if execution["final_system_sha256"] != digest(SYSTEM) or execution["final_schema_sha256"] != digest(schema()):
        raise ValueError("final-review contract differs from executed qualification")
    if execution["frozen_plan_sha256"] != digest(plan): raise ValueError("executed qualification plan changed")
    for wid in plan["window_ids"]:
        sha = plan["source_packets"][wid]
        source = json.loads((BASE / f"{sha}.packet.json").read_text())
        authored = {}; inventory[wid] = {}
        for role in ("A", "B", "C", "AUDIT"):
            expected = packet(source, role, author_a=authored.get("A") if role == "C" else None,
                author_b=authored.get("B") if role == "C" else None)
            path = QUAL / "calls" / wid / role
            if json.loads((path / "packet.json").read_text()) != expected:
                raise ValueError("source/author/role packet provenance mismatch")
            authored[role] = validate(json.loads((path / f"{expected['packet_sha256']}.result.json").read_text()),
                source=source["transcript_window"], window_id=wid)
            inventory[wid][role] = digest(authored[role])
        selected.extend(review_packets(authored["C"], source=source["transcript_window"], window_id=wid,
            token_count=lambda s: len(enc.encode(s))))
    # Nothing is frozen or dispatched until all sixteen A/B/C/AUDIT chains validate.
    OUT.mkdir(parents=True, exist_ok=True, mode=0o700)
    for p in selected: immutable_json(OUT / f"{p['packet_sha256']}.packet.json", p)
    immutable_json(OUT / "plan.json", {"packets": [p["packet_sha256"] for p in selected],
        "inventory": inventory, "system_sha256": digest(SYSTEM), "schema_sha256": digest(schema()),
        "model": "gpt-5.5", "effort": "high", "concurrency": 1,
        "qualified": False, "gold_accepted": False})
    return selected


async def execute(packets):
    return await metered_review(packets, output_root=OUT, task_prefix="full-event-qualification-v3-final",
        system=SYSTEM, schema_for_packet=lambda p: schema(), packet_id=lambda p: p["packet_sha256"],
        validator=lambda v, p: validate_review(v, source=p["transcript_window"], window_id=p["window_id"], candidates=p["candidates"]),
        verdict_rows=lambda v: v["decisions"])


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--execute", action="store_true"); args = parser.parse_args()
    QUAL.mkdir(parents=True, exist_ok=True)
    with (QUAL / "final-review.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        packets = prepare(); print(json.dumps({"packets": len(packets), "gold_accepted": False}), flush=True)
        if args.execute: raise SystemExit(asyncio.run(execute(packets)))


if __name__ == "__main__": main()
