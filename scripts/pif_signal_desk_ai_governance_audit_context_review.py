"""Second explicit audit delta: conditional assertion and evaluative context."""
import argparse
import asyncio
import fcntl
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from scripts import pif_signal_desk_ai_governance_audit_delta_review as previous
from scripts.pif_signal_desk_attribution_probe_review import execute
from research_factory.signal_desk_repair_review_proof import verify
from research_factory.signal_desk_full_event_v4_repair import propose
from research_factory.signal_desk_rubric_reference_packets import digest
parent=previous.parent
OUT=previous.OUT.parent/'ai-governance-audit-context-review-v3'
IDS={f'sdw_3e13b01692fa8508d0cd_e{i:02d}' for i in (10,13)}
SYSTEM=previous.SYSTEM+'''\nCheck the two revised records independently. A conditional normative position
can itself be asserted without removing its condition. For the diffusion forecast,
check the exact antecedent of such uses and whether surrounding evaluative wording
supports negative attitude toward government mass surveillance. Do not infer a
named narrator. Review every field; prior corrections are not automatic approval.
'''


def proposal():
    original,prior,p=previous.proposal()
    decisions,proofs=verify(previous,previous.prepare(write=False))
    if len(decisions)!=4 or {d['event_id'] for d in decisions if d['verdict']!='supported'}!=IDS:
        raise ValueError('unexpected second-delta population')
    source=p['transcript_window'];changes=[]
    def span(text):
        if source.count(text)!=1:raise ValueError('source context ambiguous')
        start=source.index(text);return dict(text=text,start=start,end=start+len(text))
    def change(path,after,reason):
        before=original
        for key in path:before=before[key]
        changes.append(dict(path=path,before=before,after=after,reason=reason))
    change(['events',9,'attitude','proposition_status'],'asserted','The narrator asserts the complete conditional judgment, not an unconditional forecast.')
    change(['events',9,'attitude','epistemic'],'certain','Scenario conditions are not doubt about the conditional conclusion.')
    change(['events',9,'attitude','modality_evidence'],[],'Retain conditions in the claim rather than tagging them as epistemic doubt.')
    change(['events',9,'attitude','rationale'],'The narrator asserts that private ownership would be unacceptable under the stated future condition; the condition remains part of the claim.','Preserve conditional scope explicitly.')
    antecedent=span('structurally AI favors authoritarian applications, mass surveillance being one among many.')
    evaluation=span('it is unacceptable for the government to use AI to enforce mass surveillance and censorship and control')
    change(['events',12,'context_evidence'],[dict(antecedent,purpose='antecedent')],'Resolve such uses from the immediately preceding source sentence.')
    change(['events',12,'attitude','attitude'],'negative','Surrounding source explicitly rejects government mass surveillance.')
    change(['events',12,'attitude','evaluation_evidence'],[antecedent,evaluation],'Use exact surrounding evaluation, not inferred sentiment from the forecast alone.')
    change(['events',12,'attitude','rationale'],'The surrounding text calls these authoritarian applications and explicitly says government use of AI for mass surveillance, censorship and control is unacceptable.','Bind negative assessment to supplied source context.')
    fixed,proof=propose(original,source=source,window_id=p['window_id'],expected_original_sha256=digest(original),
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
        q['schema_sha256']=digest(review.schema(q));q['packet_sha256']=digest(q);ps.append(q)
    if [e['event_id'] for q in ps for e in q['candidates']]!=sorted(IDS):raise ValueError('context-delta population changed')
    if write:
        for name,value in [('proposal',fixed),('provenance',proof)]:parent.run.immutable_json(OUT/f'{name}.json',value)
        for q in ps:parent.run.immutable_json(OUT/f"{q['packet_sha256']}.packet.json",q)
        parent.run.immutable_json(OUT/'plan.json',dict(packets=[q['packet_sha256'] for q in ps],records=15,
            reviewed_delta_records=2,proposal_sha256=digest(fixed),provenance_sha256=digest(proof),applied=False,gold_accepted=False))
    return ps


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--execute',action='store_true');args=parser.parse_args()
    with (parent.run.previous.OUT/'runner.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);ps=prepare()
        if args.execute:
            review=parent.run.previous.review
            raise SystemExit(asyncio.run(execute(ps,output_root=OUT,task_prefix='ai-governance-audit-context-v3',system=SYSTEM,
                schema_for_packet=review.schema,validator=review.validate_review,packet_id=lambda p:p['packet_sha256'],verdict_rows=lambda v:v['decisions'])))
