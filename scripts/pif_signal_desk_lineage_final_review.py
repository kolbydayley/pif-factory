#!/usr/bin/env python3
"""Full population preflight before independent canonical and lineage review."""
import argparse
import asyncio
import fcntl
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from scripts import pif_signal_desk_lineage_qualification as run
from scripts.pif_signal_desk_lineage_status import inventory
from scripts.pif_signal_desk_attribution_probe_review import execute
from scripts.pif_signal_desk_gold_merge_provenance import immutable_json
from research_factory import signal_desk_lineage_review as ledger
from research_factory.signal_desk_rubric_reference_packets import digest

OUT=run.OUT/'final-review'


def prepare():
    import tiktoken
    plan=run.prepare();rows,complete=inventory(plan,require_complete=True)
    enc=tiktoken.get_encoding('o200k_base');count=lambda text:len(enc.encode(text))
    canonical=[];lineage=[];proofs={}
    for wid in plan['window_ids']:
        entry=complete[wid];outputs=entry['outputs'];source=entry['source'];p=run.packet(source,'C',outputs)
        canonical.extend(run.previous.review.packets(outputs['C']['records'],source=source['transcript_window'],window_id=wid,token_count=count))
        lineage.extend(ledger.packets(outputs['C'],p,token_count=count))
        proofs[wid]={'outputs':{r:digest(v) for r,v in outputs.items()},'provenance':entry['provenance']}
    # Freeze nothing until ALL 64 inputs and all untruncated review packets pass.
    for name,packets in (('canonical',canonical),('lineage',lineage)):
        for p in packets:immutable_json(OUT/name/f"{p['packet_sha256']}.packet.json",p)
    immutable_json(OUT/'plan.json',{'source_plan_sha256':digest(plan),'windows':16,'role_outputs':64,
        'canonical_packets':[p['packet_sha256'] for p in canonical],'lineage_packets':[p['packet_sha256'] for p in lineage],
        'canonical_contract':run.previous.review.receipt(),'lineage_contract':ledger.receipt(),'inventory':proofs,
        'model':'gpt-5.5','effort':'high','max_concurrency':1,'gold_accepted':False,'qualified':False})
    return canonical,lineage


async def run_review(canonical,lineage):
    code=await execute(canonical,output_root=OUT/'canonical',task_prefix='lineage-qualification-canonical-final-v1',
        system=run.previous.review.SYSTEM,schema_for_packet=run.previous.review.schema,validator=run.previous.review.validate_review,
        packet_id=lambda p:p['packet_sha256'],verdict_rows=lambda v:v['decisions'])
    if code:return code
    return await execute(lineage,output_root=OUT/'lineage',task_prefix='lineage-qualification-dispositions-final-v1',
        system=ledger.SYSTEM,schema_for_packet=ledger.schema,validator=ledger.validate,
        packet_id=lambda p:p['packet_sha256'],verdict_rows=lambda v:v['decisions'])


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--execute',action='store_true');args=parser.parse_args()
    with (run.previous.parent.OUT/'runner.lock').open('a') as v4lock,(run.previous.OUT/'runner.lock').open('a') as v5lock:
        for lock in (v4lock,v5lock):fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        canonical,lineage=prepare()
        print(json.dumps({'canonical_packets':len(canonical),'lineage_packets':len(lineage),'gold_accepted':False}),flush=True)
        if args.execute:raise SystemExit(asyncio.run(run_review(canonical,lineage)))


if __name__=='__main__':main()
