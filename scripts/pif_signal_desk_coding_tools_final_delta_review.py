#!/usr/bin/env python3
"""One-record evidence-completeness correction with unchanged prior approvals."""
import argparse
import asyncio
import fcntl
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from scripts import pif_signal_desk_coding_tools_delta_review as parent
from scripts.pif_signal_desk_attribution_probe_review import execute
from research_factory.signal_desk_full_event_v4_repair import propose
from research_factory.signal_desk_repair_review_proof import verify
from research_factory.signal_desk_rubric_reference_packets import digest

OUT=parent.OUT.parent/'coding-tools-explicit-repair-review-v3'
SYSTEM=parent.SYSTEM+"""\nFINAL EVIDENCE DELTA: only e10 is assigned. Its claim and
quarantine are unchanged. Add exact antecedent context2844:3018 for acquisition
thesis and extend main evidence to3384 for the exact-positioning caveat. Inspect
the complete record independently; require every substantive component to have
adequate source support. Do not treat a complete excerpt as proof of claim truth.
"""


def prepare(*,write=True):
    import tiktoken
    previous,baseline,prior=parent.prepare(write=False);decisions,receipts=verify(parent,previous)
    eid=baseline['events'][9]['event_id']
    if {d['event_id'] for d in decisions if d['verdict']!='supported'}!={eid}:raise ValueError('unexpected prior unresolved population')
    s=previous[0]['transcript_window'];e=baseline['events'][9]
    if not s[2844:3018].startswith("I've also said") or not s[3018:3384].endswith('What the, what the exact positioning of this is.'):
        raise ValueError('inspected context changed')
    changes=[{'path':['events',9,'context_evidence'],'before':e['context_evidence'],
        'after':[{'text':s[2844:3018],'start':2844,'end':3018,'purpose':'antecedent'}],
        'reason':'Exact adjacent acquisition reference required by independent reviewer; no inferred acquirer identity.'},
        {'path':['events',9,'evidence_text'],'before':e['evidence_text'],'after':s[3018:3384],
         'reason':'Include exact-positioning caveat referenced in unchanged claim.'},
        {'path':['events',9,'evidence_end'],'before':e['evidence_end'],'after':3384,'reason':'Exact end of the supplied caveat sentence.'}]
    fixed,proof=propose(baseline,source=s,window_id=parent.parent.diagnosis.WID,expected_original_sha256=digest(baseline),
        replacements=changes,output_validator=parent.parent.diagnosis.run.previous.contract.validate)
    proof['parent_delta_proof_sha256']=digest(prior);proof['parent_delta_reviews']=receipts
    enc=tiktoken.get_encoding('o200k_base');review=parent.parent.diagnosis.run.previous.review
    full=parent.parent.packets(fixed,source=s,window_id=parent.parent.diagnosis.WID,token_count=lambda t:len(enc.encode(t))+600,
        system=SYSTEM,schema_for_packet=review.schema,output_validator=parent.parent.diagnosis.run.previous.contract.validate)
    p=next(p for p in full if any(e['event_id']==eid for e in p['candidates']))
    p.pop('packet_sha256');p['candidates']=[fixed['events'][9]];p['voice_bindings']=[b for b in p['voice_bindings'] if b['voice_binding_id']==e['voice_binding_id']]
    p['proposal_sha256']=digest(fixed);p['repair_proof_sha256']=digest(proof);p['schema_sha256']=digest(review.schema(p));p['packet_sha256']=digest(p)
    if len(enc.encode(SYSTEM+json.dumps(p,ensure_ascii=False)+json.dumps(review.schema(p))))+1500>12000:raise ValueError('packet budget exceeded')
    if write:
        OUT.mkdir(parents=True,exist_ok=True,mode=0o700)
        for name,v in [('proposal',fixed),('provenance',proof),(p['packet_sha256']+'.packet',p)]:parent.parent.diagnosis.run.immutable_json(OUT/f'{name}.json',v)
        parent.parent.diagnosis.run.immutable_json(OUT/'plan.json',{'packets':[p['packet_sha256']],'records':15,'reviewed_delta_records':1,
            'proposal_sha256':digest(fixed),'provenance_sha256':digest(proof),'applied':False,'gold_accepted':False})
    return [p],fixed,proof


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--execute',action='store_true');args=parser.parse_args()
    with (parent.parent.diagnosis.run.previous.OUT/'runner.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);ps,_,_=prepare();print(json.dumps({'packets':1,'delta_records':1,'records':15}),flush=True)
        if args.execute:
            review=parent.parent.diagnosis.run.previous.review
            raise SystemExit(asyncio.run(execute(ps,output_root=OUT,task_prefix='coding-tools-explicit-repair-review-v3',system=SYSTEM,
                schema_for_packet=review.schema,validator=review.validate_review,packet_id=lambda p:p['packet_sha256'],verdict_rows=lambda v:v['decisions'])))


if __name__=='__main__':main()
