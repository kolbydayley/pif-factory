#!/usr/bin/env python3
"""Fresh unchanged sixteen-source qualification of v5 context representation."""
import argparse
import asyncio
import fcntl
import json
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]; sys.path.insert(0, str(ROOT))
from scripts import pif_signal_desk_full_event_v4_run as parent
from scripts.pif_signal_desk_gold_merge_provenance import immutable_json
from research_factory import signal_desk_full_event_v5 as contract
from research_factory import signal_desk_full_event_v5_prompts as author
from research_factory import signal_desk_full_event_v5_review as review
from research_factory.signal_desk_provider_schema import lower
from research_factory.signal_desk_rubric_reference_packets import digest

BASE = parent.BASE
OUT = BASE.parent / "full-event-semantic-qualification-v5"


def prepare():
    import tiktoken
    old = parent.prepare(); enc = tiktoken.get_encoding("o200k_base")
    if len(old["window_ids"]) != 16 or len(set(old["window_ids"])) != 16: raise ValueError("full original population required")
    sources = [json.loads((BASE / f"{old['source_packets'][wid]}.packet.json").read_text()) for wid in old["window_ids"]]
    ps = [author.packet(s,role) for s in sources for role in ("A","B","AUDIT")]
    sizes = {p["packet_sha256"]: len(enc.encode(author.prompts()[p["role"]] + json.dumps(p,ensure_ascii=False) + json.dumps(contract.schema()))) for p in ps}
    empty_sizes = {}
    for s in sources:
        empty = {"schema_version":contract.VERSION,"window_id":s["window_id"],"window_disposition":"no_records","events":[],"voice_bindings":[]}
        r = review.packets(empty,source=s["transcript_window"],window_id=s["window_id"],token_count=lambda text:len(enc.encode(text)))[0]
        empty_sizes[s["window_id"]] = len(enc.encode(review.SYSTEM+json.dumps(r,ensure_ascii=False)+json.dumps(review.schema(r))))+1500
    provider, lowering = lower(contract.schema())
    plan = {"contract":author.receipt(),"final_review":review.receipt(),"predecessor_plan_sha256":digest(old),
        "window_ids":old["window_ids"],"source_packets":old["source_packets"],"source_plan_sha256":old["source_plan_sha256"],
        "provider_schema":lowering,"independent_packets":[p["packet_sha256"] for p in ps],
        "input_token_counts":sizes,"empty_review_baseline_tokens":empty_sizes,
        "sol_calls":{"A":16,"B":16,"C":16,"AUDIT":16},"model":"gpt-5.6-sol","reasoning":"medium","max_concurrency":2,
        "scope":"Full original sixteen development sources; fresh all-role v5 qualification, not benchmark acceptance.",
        "gold_accepted":False,"qualified":False,"sealed_items_opened":False}
    OUT.mkdir(parents=True,exist_ok=True,mode=0o700)
    for p in ps: immutable_json(OUT/"packets"/f"{p['packet_sha256']}.json",p)
    immutable_json(OUT/"plan.json",plan);immutable_json(OUT/"schema.json",contract.schema())
    immutable_json(OUT/"provider-schema.json",provider)
    return plan


def main():
    parser=argparse.ArgumentParser();parser.add_argument("--execute",action="store_true");args=parser.parse_args()
    plan=prepare()
    print(json.dumps({"windows":len(plan["window_ids"]),"calls":64,"max_author_input_tokens":max(plan["input_token_counts"].values()),
        "max_empty_review_tokens":max(plan["empty_review_baseline_tokens"].values()),"gold_accepted":False}),flush=True)
    # Share the predecessor's lock as well, so old/new author or review workers
    # cannot overlap accidentally during the schema migration.
    with (parent.OUT/"runner.lock").open("a") as previous_lock, (OUT/"runner.lock").open("a") as lock:
        for handle in (previous_lock,lock):fcntl.flock(handle,fcntl.LOCK_EX|fcntl.LOCK_NB)
        if args.execute:
            immutable_json(OUT/"execution-contract.json",{"plan_sha256":digest(plan),"review":review.receipt(),"gold_accepted":False})
            raise SystemExit(asyncio.run(parent.execute(plan,output_root=OUT,packet_builder=author.packet,prompt_factory=author.prompts,
                review_contract=review,output_validator=contract.validate,output_schema=contract.schema,task_prefix="full-event-v5-context-qualification")))


if __name__=="__main__":main()
