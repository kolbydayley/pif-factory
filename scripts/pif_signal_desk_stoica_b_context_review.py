"""Explicit one-record antecedent repair; no approval transfer for changed record."""
import argparse
import asyncio
import fcntl
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from scripts import pif_signal_desk_stoica_b_review as parent
from scripts.pif_signal_desk_attribution_probe_review import execute
from research_factory.signal_desk_repair_review_proof import verify
from research_factory.signal_desk_full_event_v4_repair import propose
from research_factory.signal_desk_rubric_reference_packets import digest
OUT=parent.OUT.parent/'stoica-b-context-review-v2'
EVENT='sdw_ff2331e598d948e07cc9_e8'
SYSTEM=parent.SYSTEM+'\nCheck the restored antecedent for the open-source preference and conditional competitiveness claim. Review every field independently.\n'


def proposal():
    original,prior,p=parent.proposal();ds,proofs=verify(parent,parent.prepare(write=False))
    if len(ds)!=12 or {d['event_id'] for d in ds if d['verdict']!='supported'}!={EVENT}:
        raise ValueError('unexpected Stoica B correction population')
    text='And that, when it comes to AI today, that kind of partnership is broken. It’s like industries, every company is doing this research in silos, right? You know, they don’t talk to each other as much. Academia, like you said, doesn’t have resources, and the government doesn’t invest as much.'
    source=p['transcript_window']
    if source.count(text)!=1:raise ValueError('antecedent occurrence changed')
    start=source.index(text)
    changes=[dict(path=['events',7,'context_evidence'],before=original['events'][7]['context_evidence'],
        after=[dict(text=text,start=start,end=start+len(text),purpose='antecedent')],
        reason='Resolve the stated concern and conditional warning to the broken research partnership in the preceding passage.')]
    fixed,proof=propose(original,source=source,window_id=p['window_id'],expected_original_sha256=digest(original),
        replacements=changes,output_validator=parent.run.previous.contract.validate)
    proof.update(parent_provenance_sha256=digest(prior),parent_reviews=proofs)
    return fixed,proof,p


def prepare(*,write=True):
    import tiktoken
    fixed,proof,p=proposal();enc=tiktoken.get_encoding('o200k_base');review=parent.run.previous.review
    whole=parent.packets(fixed,source=p['transcript_window'],window_id=p['window_id'],token_count=lambda s:len(enc.encode(s)),
        system=SYSTEM,schema_for_packet=review.schema,output_validator=parent.run.previous.contract.validate)
    row=next(q for q in whole if any(e['event_id']==EVENT for e in q['candidates']))
    q=dict(row);q.pop('packet_sha256');q['candidates']=[e for e in row['candidates'] if e['event_id']==EVENT]
    q.update(proposal_sha256=digest(fixed),repair_proof_sha256=digest(proof))
    q['schema_sha256']=digest(review.schema(q));q['packet_sha256']=digest(q)
    if write:
        for name,value in [('proposal',fixed),('provenance',proof)]:parent.run.immutable_json(OUT/f'{name}.json',value)
        parent.run.immutable_json(OUT/f"{q['packet_sha256']}.packet.json",q)
        parent.run.immutable_json(OUT/'plan.json',dict(packets=[q['packet_sha256']],records=12,reviewed_delta_records=1,
            proposal_sha256=digest(fixed),provenance_sha256=digest(proof),applied=False,gold_accepted=False))
    return [q]


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--execute',action='store_true');args=parser.parse_args()
    with (parent.run.previous.OUT/'runner.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);ps=prepare()
        if args.execute:
            review=parent.run.previous.review
            raise SystemExit(asyncio.run(execute(ps,output_root=OUT,task_prefix='stoica-b-context-v2',system=SYSTEM,
                schema_for_packet=review.schema,validator=review.validate_review,packet_id=lambda p:p['packet_sha256'],verdict_rows=lambda v:v['decisions'])))
