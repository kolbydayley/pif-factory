"""Four audit-review corrections; never reuse approval for changed records."""
import argparse
import asyncio
import fcntl
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from scripts import pif_signal_desk_ai_governance_audit_review as parent
from scripts.pif_signal_desk_attribution_probe_review import execute
from research_factory.signal_desk_repair_review_proof import verify
from research_factory.signal_desk_full_event_v4_repair import propose
from research_factory.signal_desk_rubric_reference_packets import digest
OUT=parent.OUT.parent/'ai-governance-audit-explicit-proposal-review-v2'
IDS={f'sdw_3e13b01692fa8508d0cd_e{i:02d}' for i in (2,6,10,13)}
SYSTEM=parent.SYSTEM+'''\nIndependently review the four corrected records against full source.
Distinguish assertion of a negative proposition from denying that proposition;
an imagined embedded quotation from observed reported speech; an evaluative
target from its evaluation; and a conditional forecast from expressed doubt.
In e06 the embedded objection is anticipated, not observed: the proposed forecast
speech act and quoted_statement with null owner describe the narrator's forecast
of an imagined quotation, not an actual quote from an identified opponent.
Check that this representation is faithful under the frozen contract. Do not
approve merely because the enum is valid. All other fields also need review.
'''


def proposal():
    baseline,prior,p=parent.proposal();ds,proofs=verify(parent,parent.prepare(write=False))
    if len(ds)!=15 or {d['event_id'] for d in ds if d['verdict']!='supported'}!=IDS:
        raise ValueError('unexpected audit correction population')
    changes=[]
    def change(path,after,reason):
        before=baseline
        for key in path:before=before[key]
        changes.append(dict(path=path,before=before,after=after,reason=reason))
    change(['events',1,'attitude','proposition_status'],'asserted','The complete candidate asserts the narrator’s rejection of government-control improvement.')
    change(['events',1,'attitude','rationale'],'The narrator asserts a negative assessment of transferring authority to government; the candidate proposition itself is asserted.','Separate negative content from denying the candidate proposition.')
    change(['events',5,'speech_act'],'forecast','The narrator anticipates an objection rather than reporting an observed speaker position.')
    change(['events',5,'attribution','relation'],'quoted_statement','The source supplies an imagined embedded quotation with null owner and anticipated actuality, not observed reported speech.')
    text='that entity be a private company';source=p['transcript_window']
    if source.count(text)!=1:raise ValueError('target occurrence ambiguous')
    start=source.index(text)
    change(['events',9,'attitude','target'],dict(text=text,start=start,end=start+len(text)),'Separate private-company status from the negative evaluation.')
    change(['events',12,'attitude','epistemic'],'certain','Even if specifies the condition; no doubt is expressed in the conditional forecast.')
    change(['events',12,'attitude','modality_evidence'],[],'Remove scenario condition wrongly treated as epistemic hedge; the claim still preserves the condition.')
    change(['events',12,'attitude','rationale'],'The narrator assertively forecasts the conditional outcome; the adverse-use description alone is not explicit evaluative wording.','Keep conditional scope distinct from expressed uncertainty.')
    fixed,proof=propose(baseline,source=source,window_id=p['window_id'],expected_original_sha256=digest(baseline),
        replacements=changes,output_validator=parent.run.previous.contract.validate)
    proof.update(parent_provenance_sha256=digest(prior),parent_reviews=proofs)
    return fixed,proof,p


def prepare(*,write=True):
    import tiktoken
    fixed,proof,p=proposal();enc=tiktoken.get_encoding('o200k_base');review=parent.run.previous.review
    whole=parent.packets(fixed,source=p['transcript_window'],window_id=p['window_id'],token_count=lambda s:len(enc.encode(s)),
        system=SYSTEM,schema_for_packet=review.schema,output_validator=parent.run.previous.contract.validate)
    ps=[]
    for row in whole:
        candidates=[e for e in row['candidates'] if e['event_id'] in IDS]
        if not candidates:continue
        q=dict(row);q.pop('packet_sha256');q['candidates']=candidates
        q.update(proposal_sha256=digest(fixed),repair_proof_sha256=digest(proof))
        q['schema_sha256']=digest(review.schema(q));q['packet_sha256']=digest(q)
        if len(enc.encode(SYSTEM+json.dumps(q,ensure_ascii=False)+json.dumps(review.schema(q))))+1500>12000:raise ValueError('packet budget exceeded')
        ps.append(q)
    if [e['event_id'] for q in ps for e in q['candidates']]!=sorted(IDS):raise ValueError('delta population changed')
    if write:
        for name,value in [('proposal',fixed),('provenance',proof)]:parent.run.immutable_json(OUT/f'{name}.json',value)
        for q in ps:parent.run.immutable_json(OUT/f"{q['packet_sha256']}.packet.json",q)
        parent.run.immutable_json(OUT/'plan.json',dict(packets=[q['packet_sha256'] for q in ps],records=15,
            reviewed_delta_records=4,proposal_sha256=digest(fixed),provenance_sha256=digest(proof),applied=False,gold_accepted=False))
    return ps


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--execute',action='store_true');args=parser.parse_args()
    with (parent.run.previous.OUT/'runner.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);ps=prepare()
        if args.execute:
            review=parent.run.previous.review
            raise SystemExit(asyncio.run(execute(ps,output_root=OUT,task_prefix='ai-governance-audit-delta-review-v2',system=SYSTEM,
                schema_for_packet=review.schema,validator=review.validate_review,packet_id=lambda p:p['packet_sha256'],verdict_rows=lambda v:v['decisions'])))
