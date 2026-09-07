#!/usr/bin/env python3
"""Metered final-authority reference review for the complete qualification set."""
import argparse
import asyncio
from collections import Counter
import fcntl
import json
import sqlite3
import sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from scripts.pif_signal_desk_gold_review_corrected import R
from scripts.pif_signal_desk_gold_merge_provenance import immutable_json
from research_factory.signal_desk_rubric_reference_packets import (
    build_reference_packets,REFERENCE_SYSTEM,reference_schema,validate_reference,digest)
from research_factory.codex_app_server import CodexAppServerClient
from research_factory.signal_desk_gold_wider_speaker import reserve_call,settle_call
from research_factory.signal_desk_rebuild_approval import ApprovalBudgetConfig

QUAL=R/"shared-rubric-qualification-v1"
OUT=QUAL/"reference-review"


def prepare():
    import tiktoken
    enc=tiktoken.get_encoding("o200k_base")
    plan=json.loads((QUAL/"plan.json").read_text())
    result=build_reference_packets(plan=plan,manifest=json.loads((R/"merged-manifest.json").read_text()),
        result_root=QUAL/"results",project_root=ROOT,token_count=lambda s:len(enc.encode(s)))
    # Packet builder validates every A/B/C output before anything is written.
    OUT.mkdir(parents=True,exist_ok=True,mode=0o700)
    for packet in result["packets"]:immutable_json(OUT/f"{packet['packet_sha256']}.packet.json",packet)
    receipt={"packet_digests":[p["packet_sha256"] for p in result["packets"]],"source_inventory":result["inventory"],
        "rubric_sha256":plan["rubric"]["sha256"],"system_sha256":digest(REFERENCE_SYSTEM),
        "model":"gpt-5.5","effort":"high","concurrency":1,
        "batching":{"max_candidates":25,"max_input_tokens":12000,"schema_token_allowance":1500,"truncation_allowed":False},
        "candidate_events":sum(len(p["candidate_events"]) for p in result["packets"]),
        "empty_windows":sum(p["empty_window_review"] for p in result["packets"]),
        "gold_accepted":False,"rubric_qualified":False}
    immutable_json(OUT/"plan.json",receipt)
    return result["packets"]


async def execute(packets):
    db=sqlite3.connect(ROOT/"data/factory.sqlite",timeout=30)
    budget=ApprovalBudgetConfig(campaign_id="signal-desk-clean-corpus-2026-08-31",
        grant_path=ROOT/"config/signal_desk_rebuild_budget_grant.json",budget_dir=ROOT/"work/pif-ops/budget")
    try:
        async with CodexAppServerClient(command=["codex","app-server","--stdio","--strict-config"],expected_cli_version="0.147.0") as client:
            for packet in packets:
                sha=packet["packet_sha256"];target=OUT/f"{sha}.reference.json"
                if target.exists():
                    validate_reference(json.loads(target.read_text()),packet);continue
                if (OUT/f"{sha}.pending.json").exists():continue
                key="shared-rubric-reference-v1:"+sha
                reservation=reserve_call(db,task_key=key,budget=budget)
                if reservation.get("existing"):raise RuntimeError("existing paid attempt needs sidecar recovery; no blind redispatch")
                usage=None
                try:
                    result=await client.run_ephemeral_structured_turn(model="gpt-5.5",effort="high",base_instructions=REFERENCE_SYSTEM,
                        prompt=json.dumps(packet,ensure_ascii=False),output_schema=reference_schema(packet),cwd=ROOT,
                        sidecar_path=OUT/f"{sha}.sidecar.json",output_path=OUT/f"{sha}.output.json",timeout_seconds=900)
                    usage=result.usage.total_tokens if result.usage else None
                    if not result.status_ok or result.output is None:raise RuntimeError(result.error_class or result.status)
                    try:validate_reference(result.output,packet)
                    except ValueError as exc:
                        immutable_json(OUT/f"{sha}.pending.json",{"packet_sha256":sha,"reason":str(exc),"gold_accepted":False});continue
                    immutable_json(target,result.output)
                finally:settle_call(db,reservation_id=reservation["reservation_id"],task_key=key,actual_tokens=usage)
    finally:db.close()
    counts=Counter();relevance=Counter();pending=[];empty=Counter()
    for packet in packets:
        sha=packet["packet_sha256"];path=OUT/f"{sha}.reference.json"
        if not path.exists():pending.append(sha);continue
        value=validate_reference(json.loads(path.read_text()),packet)
        counts.update(r["verdict"] for r in value["decisions"])
        relevance.update(r["strategic_relevance"] for r in value["decisions"])
        if packet["empty_window_review"]:empty[value["empty_window_verdict"]]+=1
    receipt={"all_calls_validated":not pending,"verdicts":dict(counts),"relevance":dict(relevance),"empty_window_verdicts":dict(empty),
        "pending_packets":pending,"gold_accepted":False,"rubric_qualified":False,
        "next":"resolve source-bound reference corrections, then measure independent audit and cross-role consistency; no automatic promotion"}
    immutable_json(OUT/"receipt.json",receipt);print(json.dumps(receipt),flush=True)
    return 0 if not pending else 2


def main():
    p=argparse.ArgumentParser();p.add_argument("--execute",action="store_true");args=p.parse_args()
    # Per-stage OS lock is released on process exit; a duplicate cannot dispatch.
    QUAL.mkdir(parents=True,exist_ok=True)
    with (QUAL/"reference-review.lock").open("a") as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        packets=prepare();print(json.dumps({"packets":len(packets),"gold_accepted":False}),flush=True)
        if args.execute:raise SystemExit(asyncio.run(execute(packets)))


if __name__=="__main__":main()
