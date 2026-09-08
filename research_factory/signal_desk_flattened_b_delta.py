"""Six independently identified scope, target and antecedent corrections."""
from .signal_desk_full_event_v4_repair import propose
from .signal_desk_repair_review_proof import verify
from .signal_desk_rubric_reference_packets import digest


def prepare():
    from scripts import pif_signal_desk_flattened_b_review as parent
    raw,prior,p=parent.proposal();rows,proofs=verify(parent,parent.prepare(write=False))
    ids={raw['events'][i-1]['event_id'] for i in (2,4,8,9,13,17)}
    if len(rows)!=19 or {r['event_id'] for r in rows if r['verdict']!='supported'}!=ids:
        raise ValueError('flattened B correction population changed')
    s=p['transcript_window'];changes=[]
    def change(n,path,after,reason):
        path=['events',n-1]+path;before=raw
        for key in path:before=before[key]
        changes.append(dict(path=path,before=before,after=after,reason=reason))
    def span(text,start):
        if s[start:start+len(text)]!=text:raise ValueError('source changed')
        return dict(text=text,start=start,end=start+len(text))
    for n in (2,4):
        e=raw['events'][n-1];start=s.index('our AI',e['evidence_start'],e['evidence_end'])
        change(n,['attitude','target'],span('our AI',start),'Praise concerns our AI and its impact, not barriers or human lives themselves.')
    change(2,['claim_text'],'The speaker says they are happy that our AI has broken down communication barriers between individual people and entire nations.','Preserve the source-specific our AI scope.')
    change(4,['claim_text'],'The speaker claims our AI makes human lives longer, healthier and easier through thousands of applications in medicine, drug design and sustainable development.','Do not generalize our AI to all AI.')
    target="way of indexing the world's existing human generated knowledge"
    evaluation='clever '+target
    change(8,['attitude','attitude'],'positive','The explicit clever evaluation is not neutral.')
    change(8,['attitude','target'],span(target,s.index(target)),'Ground the positively evaluated indexing capability.')
    change(8,['attitude','evaluation_evidence'],[span(evaluation,s.index(evaluation))],'Retain the exact clever evaluation.')
    change(8,['attitude','rationale'],'The speaker positively evaluates the described knowledge-indexing approach as clever, without asserting that LLMs reach AGI.','Preserve the evaluation and its limits.')
    change(9,['attitude','target'],span("That's",2774),'The adequacy evaluation concerns the preceding LLM capability, not jobs.')
    e=raw['events'][7];context=dict(text=e['evidence_text'],start=e['evidence_start'],end=e['evidence_end'],purpose='antecedent')
    change(9,['context_evidence'],[context],'Resolve Thats through the preceding LLM characterization.')
    change(9,['attitude','rationale'],'The LLM capability just described is positively evaluated as adequate to facilitate desktop work; the jobs are not themselves evaluated.','Align the rationale with the resolved target.')
    change(13,['attitude','target'],span('the real world',4081),'Use the local evaluated occurrence rather than an earlier repetition.')
    change(13,['attitude','status'],'proposed','The target is directly grounded inside the evidence.')
    change(13,['attitude','rationale'],'The source evaluates the real world as really challenging while separately hedging whether the project was ahead of its time.','Remove the incorrect claim that the local target was unavailable.')
    text='when we now look at the predictive world model of the the controller interacting with an environment as, as discussed earlier,'
    context=span(text,5458);context['purpose']='antecedent'
    change(17,['context_evidence'],[context],'Resolve it to the controller predictive world model.')
    fixed,proof=propose(raw,source=s,window_id=p['window_id'],expected_original_sha256=digest(raw),
        replacements=changes,output_validator=parent.run.previous.contract.validate)
    proof.update(parent_provenance_sha256=digest(prior),parent_reviews=proofs)
    return fixed,proof,p
