"""Independent review of all Stoica C records and every preserved lineage item."""
import argparse
import asyncio
import fcntl
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from scripts import pif_signal_desk_lineage_qualification as run
from scripts.pif_signal_desk_attribution_probe_review import execute
from research_factory.signal_desk_stoica_c_proposal import prepare as proposal
from research_factory.signal_desk_full_event_v4_review import packets
from research_factory import signal_desk_lineage_review as ledger
from research_factory.signal_desk_rubric_reference_packets import digest
from research_factory.signal_desk_actual_review_receipt import verify
OUT=run.OUT/'stoica-c-explicit-proposal-review-v1'
SYSTEM=run.previous.review.SYSTEM+'''\nReview every assigned C record independently against the complete source.
Opening speakers remain unknown; do not inherit Ion's name from later labels.
Check Sonya's summary versus an independent forecast, the open-source advocacy
antecedent, conditional US/California warning, scarcity caveat, and Nvidia's
uncertain market-share threshold. Check every field and omitted consequential
content. Earlier A/B approvals do not approve C records or its dispositions.
'''



def prepare(*,write=True):
    import tiktoken
    fixed,proof,p=proposal();enc=tiktoken.get_encoding('o200k_base');count=lambda s:len(enc.encode(s))
    records=packets(fixed['records'],source=p['transcript_window'],window_id=p['window_id'],token_count=count,
        system=SYSTEM,schema_for_packet=run.previous.review.schema,output_validator=run.previous.contract.validate)
    lineage=ledger.packets(fixed,p,token_count=count)
    if ([e['event_id'] for packet in records for e in packet['candidates']]!=[e['event_id'] for e in fixed['records']['events']]
        or sum(len(packet['candidates']) for packet in lineage)!=27):raise ValueError('C review population changed')
    plan=dict(record_packets=[x['packet_sha256'] for x in records],ledger_packets=[x['packet_sha256'] for x in lineage],
        records=15,input_dispositions=27,additions=0,proposal_sha256=digest(fixed),provenance_sha256=digest(proof),
        qualified=False,gold_accepted=False)
    if write:
        run.immutable_json(OUT/'proposal.json',fixed);run.immutable_json(OUT/'provenance.json',proof)
        for kind,ps in [('records',records),('ledger',lineage)]:
            for packet in ps:run.immutable_json(OUT/kind/f"{packet['packet_sha256']}.packet.json",packet)
        run.immutable_json(OUT/'plan.json',plan)
    return records,lineage,plan


async def run_reviews(records,lineage):
    for kind,ps,system,provider in [('records',records,SYSTEM,run.previous.review),('ledger',lineage,ledger.SYSTEM,ledger)]:
        code=await execute(ps,output_root=OUT/kind,task_prefix='stoica-c-'+kind+'-review-v1',system=system,
            schema_for_packet=provider.schema,validator=provider.validate_review if kind=='records' else provider.validate,
            packet_id=lambda p:p['packet_sha256'],verdict_rows=lambda v:v['decisions'])
        if code:return code
    return 0


def verified_reviews():
    records,lineage,plan=prepare(write=False)
    if json.loads((OUT/'plan.json').read_text())!=plan:raise ValueError('C review plan changed')
    values=[];proofs=[]
    for kind,ps,system,validator in [('records',records,SYSTEM,run.previous.review.validate_review),('ledger',lineage,ledger.SYSTEM,ledger.validate)]:
        for p in ps:
            v,proof=verify(OUT/kind,p,system=system,validator=validator)
            values.append(dict(kind=kind,packet_sha256=p['packet_sha256'],review=v));proofs.append(proof)
    return values,proofs


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--execute',action='store_true');args=parser.parse_args()
    with (run.previous.OUT/'runner.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);records,lineage,_=prepare()
        if args.execute:raise SystemExit(asyncio.run(run_reviews(records,lineage)))

