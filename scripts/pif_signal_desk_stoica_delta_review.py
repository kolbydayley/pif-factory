"""Two source-inspected attitude corrections, requiring fresh independent review."""
import argparse
import asyncio
import fcntl
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from scripts import pif_signal_desk_stoica_review as parent
from scripts.pif_signal_desk_attribution_probe_review import execute
from research_factory.signal_desk_repair_review_proof import verify
from research_factory.signal_desk_full_event_v4_repair import propose
from research_factory.signal_desk_rubric_reference_packets import digest
OUT=parent.OUT.parent/'stoica-explicit-proposal-review-v2'
IDS={'sdw_ff2331e598d948e07cc9_e01','sdw_ff2331e598d948e07cc9_e02'}
SYSTEM=parent.SYSTEM+'''\nReview the two corrected opening records independently against the entire source.
Check the negative evaluation of current application-development difficulty and
the positive evaluation of human-assistant application, separating target from
evaluative wording. Keep the unlabeled voice unresolved. Check all fields, not
only the attitude changes. No earlier decision approves these changed records.
'''


def proposal():
    baseline,prior,p=parent.proposal();ds,proofs=verify(parent,parent.prepare(write=False))
    if len(ds)!=15 or {d['event_id'] for d in ds if d['verdict']!='supported'}!=IDS:
        raise ValueError('unexpected unresolved Stoica population')
    source=p['transcript_window'];changes=[]
    def span(text,start=None):
        if start is None:
            if source.count(text)!=1:raise ValueError('ambiguous correction span')
            start=source.index(text)
        if source[start:start+len(text)]!=text:raise ValueError('source occurrence changed')
        return dict(text=text,start=start,end=start+len(text))
    def change(path,after,reason):
        before=baseline
        for key in path:before=before[key]
        changes.append(dict(path=path,before=before,after=after,reason=reason))
    change(['events',0,'attitude','attitude'],'negative','Explicit evaluation of current application-development difficulty.')
    change(['events',0,'attitude','target'],span('develop all this application'),'Identify the evaluated activity without borrowing an unproven speaker.')
    change(['events',0,'attitude','evaluation_evidence'],[span('Now it’s extremely hard.')],'Exact negative evaluation, unchanged source text.')
    change(['events',0,'attitude','rationale'],'The unnamed speaker negatively evaluates current application-development difficulty as extremely hard.','Align interpretation with the explicit source evaluation.')
    change(['events',1,'attitude','target'],span('application',1242),'Separate the application target from the praise word fantastic.')
    fixed,proof=propose(baseline,source=source,window_id=p['window_id'],expected_original_sha256=digest(baseline),
        replacements=changes,output_validator=parent.run.previous.contract.validate)
    proof.update(parent_provenance_sha256=digest(prior),parent_reviews=proofs)
    return fixed,proof,p


def prepare(*,write=True):
    import tiktoken
    fixed,proof,p=proposal();enc=tiktoken.get_encoding('o200k_base');review=parent.run.previous.review
    all_packets=parent.packets(fixed,source=p['transcript_window'],window_id=p['window_id'],
        token_count=lambda s:len(enc.encode(s)),system=SYSTEM,schema_for_packet=review.schema,
        output_validator=parent.run.previous.contract.validate)
    ps=[]
    for original in all_packets:
        candidates=[e for e in original['candidates'] if e['event_id'] in IDS]
        if not candidates:continue
        packet=dict(original);packet.pop('packet_sha256');packet['candidates']=candidates
        packet.update(proposal_sha256=digest(fixed),repair_proof_sha256=digest(proof))
        packet['schema_sha256']=digest(review.schema(packet));packet['packet_sha256']=digest(packet)
        if len(enc.encode(SYSTEM+json.dumps(packet,ensure_ascii=False)+json.dumps(review.schema(packet))))+1500>12000:
            raise ValueError('delta packet exceeds budget')
        ps.append(packet)
    if [e['event_id'] for packet in ps for e in packet['candidates']]!=sorted(IDS):raise ValueError('delta population changed')
    if write:
        OUT.mkdir(parents=True,exist_ok=True,mode=0o700)
        for name,value in [('proposal',fixed),('provenance',proof)]:parent.run.immutable_json(OUT/f'{name}.json',value)
        for packet in ps:parent.run.immutable_json(OUT/f"{packet['packet_sha256']}.packet.json",packet)
        parent.run.immutable_json(OUT/'plan.json',dict(packets=[p['packet_sha256'] for p in ps],records=15,
            reviewed_delta_records=2,proposal_sha256=digest(fixed),provenance_sha256=digest(proof),applied=False,gold_accepted=False))
    return ps


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--execute',action='store_true');args=parser.parse_args()
    with (parent.run.previous.OUT/'runner.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);ps=prepare()
        if args.execute:
            review=parent.run.previous.review
            raise SystemExit(asyncio.run(execute(ps,output_root=OUT,task_prefix='stoica-explicit-proposal-review-v2',system=SYSTEM,
                schema_for_packet=review.schema,validator=review.validate_review,packet_id=lambda p:p['packet_sha256'],verdict_rows=lambda v:v['decisions'])))
