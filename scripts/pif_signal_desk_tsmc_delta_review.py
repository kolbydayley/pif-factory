"""One causal-wording correction; retain the other thirteen actual approvals."""
import argparse
import asyncio
import fcntl
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from scripts import pif_signal_desk_tsmc_repair_review as parent
from scripts.pif_signal_desk_attribution_probe_review import execute
from research_factory.signal_desk_tsmc_recovery import review_proof
from research_factory.signal_desk_full_event_v4_repair import propose
from research_factory.signal_desk_rubric_reference_packets import digest
OUT=parent.OUT.parent/'tsmc-explicit-proposal-review-v2'
SYSTEM=parent.run.previous.review.SYSTEM+'''\nReview the assigned corrected record independently against the full source.
Its claim now describes Ben's positive interpretation of the relationship,
settlement and subsequent business without asserting that the relationship caused
the settlement. Check every field and the exact source; previous review is not
automatic approval. Do not attribute Ben's interpretation to Morris.
'''


def prepare(*,write=True):
    import tiktoken
    baseline,prior,p=parent.proposal();ds,proofs=review_proof(parent)
    eid=baseline['events'][10]['event_id']
    if len(ds)!=14 or {d['event_id'] for d in ds if d['verdict']!='supported'}!={eid}:
        raise ValueError('unexpected unresolved TSMC population')
    changes=[{'path':['events',10,'claim_text'],'before':baseline['events'][10]['claim_text'],
        'after':'Ben describes the settlement as successful conflict resolution: TSMC and NVIDIA had a long partnership and close personal relationship, reached a settlement exceeding $100 million, and subsequently did many billions of dollars of business together.',
        'reason':'Preserve the speaker interpretation without asserting an unsupported causal mechanism.'}]
    fixed,proof=propose(baseline,source=p['transcript_window'],window_id=p['window_id'],expected_original_sha256=digest(baseline),
        replacements=changes,output_validator=parent.run.previous.contract.validate)
    proof.update(parent_provenance_sha256=digest(prior),parent_reviews=proofs)
    enc=tiktoken.get_encoding('o200k_base');review=parent.run.previous.review
    full=parent.packets(fixed,source=p['transcript_window'],window_id=p['window_id'],token_count=lambda s:len(enc.encode(s))+600,
        system=SYSTEM,schema_for_packet=review.schema,output_validator=parent.run.previous.contract.validate)
    packet=next(row for row in full if any(e['event_id']==eid for e in row['candidates']))
    packet.pop('packet_sha256');packet['candidates']=[fixed['events'][10]]
    packet['voice_bindings']=[b for b in packet['voice_bindings'] if b['voice_binding_id']==fixed['events'][10]['voice_binding_id']]
    packet.update(proposal_sha256=digest(fixed),repair_proof_sha256=digest(proof))
    packet['schema_sha256']=digest(review.schema(packet));packet['packet_sha256']=digest(packet)
    if len(enc.encode(SYSTEM+json.dumps(packet,ensure_ascii=False)+json.dumps(review.schema(packet))))+1500>12000:
        raise ValueError('packet budget exceeded')
    if write:
        OUT.mkdir(parents=True,exist_ok=True,mode=0o700)
        for name,value in [('proposal',fixed),('provenance',proof),(packet['packet_sha256']+'.packet',packet)]:
            parent.run.immutable_json(OUT/f'{name}.json',value)
        parent.run.immutable_json(OUT/'plan.json',{'packets':[packet['packet_sha256']],'records':14,'reviewed_delta_records':1,
            'proposal_sha256':digest(fixed),'provenance_sha256':digest(proof),'applied':False,'gold_accepted':False})
    return [packet],fixed,proof


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--execute',action='store_true');args=parser.parse_args()
    with (parent.run.previous.OUT/'runner.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);ps,_,_=prepare()
        if args.execute:
            review=parent.run.previous.review
            raise SystemExit(asyncio.run(execute(ps,output_root=OUT,task_prefix='tsmc-explicit-proposal-review-v2',system=SYSTEM,
                schema_for_packet=review.schema,validator=review.validate_review,packet_id=lambda p:p['packet_sha256'],verdict_rows=lambda v:v['decisions'])))
