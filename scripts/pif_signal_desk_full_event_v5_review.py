#!/usr/bin/env python3
"""Review only after all original sixteen sources have validated A/B/C/AUDIT."""
import argparse
import asyncio
import fcntl
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from scripts import pif_signal_desk_full_event_v5_run as run
from scripts.pif_signal_desk_attribution_probe_review import execute
from scripts.pif_signal_desk_gold_merge_provenance import immutable_json
from research_factory.signal_desk_rubric_reference_packets import digest
from research_factory.signal_desk_full_event_v5_offset_recovery import load_call
OUT=run.OUT/"final-review"


def prepare():
    import tiktoken
    plan=run.prepare();enc=tiktoken.get_encoding("o200k_base");packets=[];inventory={}
    for wid in plan["window_ids"]:
        source=json.loads((run.BASE/f"{plan['source_packets'][wid]}.packet.json").read_text());outputs={};provenance={}
        for role in ("A","B","C","AUDIT"):
            p=run.author.packet(source,role,author_a=outputs.get("A") if role=="C" else None,author_b=outputs.get("B") if role=="C" else None)
            d=run.OUT/"calls"/wid/role;sha=p["packet_sha256"]
            if json.loads((d/"packet.json").read_text())!=p:raise ValueError("author packet changed")
            saved,provenance[role]=load_call(d,p)
            sidecar=json.loads((d/f"{sha}.sidecar.json").read_text())
            if sidecar.get("state")!="completed" or sidecar.get("error_class"):
                raise ValueError("author requires explicit source/provenance recovery")
            outputs[role]=run.contract.validate(saved,source=source["transcript_window"],window_id=wid)
        inventory[wid]={"outputs":{r:digest(v) for r,v in outputs.items()},"provenance":provenance}
        packets.extend(run.review.packets(outputs["C"],source=source["transcript_window"],window_id=wid,token_count=lambda text:len(enc.encode(text))))
    OUT.mkdir(parents=True,exist_ok=True,mode=0o700)
    for p in packets:immutable_json(OUT/f"{p['packet_sha256']}.packet.json",p)
    immutable_json(OUT/"plan.json",{"packets":[p["packet_sha256"] for p in packets],"author_inventory":inventory,
        "author_plan_sha256":digest(plan),"review_contract":run.review.receipt(),"model":"gpt-5.5","effort":"high","gold_accepted":False})
    return packets


def main():
    parser=argparse.ArgumentParser();parser.add_argument("--execute",action="store_true");args=parser.parse_args()
    with (run.OUT/"runner.lock").open("a") as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);packets=prepare()
        print(json.dumps({"packets":len(packets),"gold_accepted":False}),flush=True)
        if args.execute:
            raise SystemExit(asyncio.run(execute(packets,output_root=OUT,task_prefix="full-event-v5-context-final-review",system=run.review.SYSTEM,
                schema_for_packet=run.review.schema,validator=run.review.validate_review,
                packet_id=lambda p:p["packet_sha256"],verdict_rows=lambda v:v["decisions"])))


if __name__=="__main__":main()
