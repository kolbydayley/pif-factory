#!/usr/bin/env python3
"""Independent GPT-5.5 review after complete unchanged sixteen-window authoring."""
import argparse
import asyncio
import fcntl
import json
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]; sys.path.insert(0, str(ROOT))
from scripts.pif_signal_desk_full_event_v4_run import prepare as author_plan, OUT as AUTHOR, BASE
from scripts.pif_signal_desk_attribution_probe_review import execute
from scripts.pif_signal_desk_gold_merge_provenance import immutable_json
from research_factory import signal_desk_full_event_v4_review as review
from research_factory.signal_desk_full_event_v4_prompts import packet
from research_factory.signal_desk_full_event_v4 import validate
from research_factory.signal_desk_rubric_reference_packets import digest
OUT = AUTHOR / "final-review"


def prepare():
    import tiktoken
    enc = tiktoken.get_encoding("o200k_base"); plan = author_plan(); packets = []; inventory = {}
    for wid in plan["window_ids"]:
        source = json.loads((BASE / f"{plan['source_packets'][wid]}.packet.json").read_text()); outputs = {}
        for role in ("A", "B", "C", "AUDIT"):
            p = packet(source, role, author_a=outputs.get("A") if role == "C" else None, author_b=outputs.get("B") if role == "C" else None)
            directory = AUTHOR / "calls" / wid / role
            if json.loads((directory / "packet.json").read_text()) != p: raise ValueError("author packet lineage changed")
            outputs[role] = validate(json.loads((directory / f"{p['packet_sha256']}.result.json").read_text()), source=source["transcript_window"], window_id=wid)
            sidecar = json.loads((directory / f"{p['packet_sha256']}.sidecar.json").read_text())
            raw = json.loads((directory / f"{p['packet_sha256']}.output.json").read_text())
            if sidecar.get("state") != "completed" or sidecar.get("error_class") or raw != outputs[role]:
                raise ValueError("author output requires explicit recovery/provenance reconciliation")
        inventory[wid] = {r: digest(v) for r, v in outputs.items()}
        packets.extend(review.packets(outputs["C"], source=source["transcript_window"], window_id=wid, token_count=lambda text: len(enc.encode(text))))
    OUT.mkdir(parents=True, exist_ok=True, mode=0o700)
    for p in packets: immutable_json(OUT / f"{p['packet_sha256']}.packet.json", p)
    immutable_json(OUT / "plan.json", {"packets": [p["packet_sha256"] for p in packets], "author_inventory": inventory,
        "author_plan_sha256": digest(plan), "review_contract": review.receipt(), "model": "gpt-5.5", "effort": "high",
        "concurrency": 1, "gold_accepted": False, "qualified": False})
    return packets


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--execute", action="store_true"); args = parser.parse_args()
    AUTHOR.mkdir(parents=True, exist_ok=True)
    with (AUTHOR / "runner.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        packets = prepare()
        print(json.dumps({"packets": len(packets), "records": sum(len(p["candidates"]) for p in packets), "gold_accepted": False}), flush=True)
        if args.execute:
            raise SystemExit(asyncio.run(execute(packets, output_root=OUT, task_prefix="full-event-v4-final-review", system=review.SYSTEM,
                schema_for_packet=lambda p: review.schema(), validator=review.validate_review,
                packet_id=lambda p: p["packet_sha256"], verdict_rows=lambda v: v["decisions"])))


if __name__ == "__main__": main()
