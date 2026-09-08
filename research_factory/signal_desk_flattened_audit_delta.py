"""Six full-source corrections required by the independent flattened AUDIT review."""
from .signal_desk_full_event_v4_repair import propose
from .signal_desk_repair_review_proof import verify
from .signal_desk_rubric_reference_packets import digest


def prepare():
    from scripts import pif_signal_desk_flattened_audit_review as parent
    raw,prior,p=parent.proposal();rows,proofs=verify(parent,parent.prepare(write=False))
    ids={raw['events'][i-1]['event_id'] for i in (2,4,7,9,15,17)}
    if len(rows)!=17 or {r['event_id'] for r in rows if r['verdict']!='supported'}!=ids:
        raise ValueError('flattened AUDIT correction population changed')
    s=p['transcript_window'];changes=[]
    def change(n,path,after,reason):
        path=['events',n-1]+path;before=raw
        for key in path:before=before[key]
        changes.append(dict(path=path,before=before,after=after,reason=reason))
    def span(text,purpose=None):
        if s.count(text)!=1:raise ValueError('ambiguous source span')
        a=s.index(text);v=dict(text=text,start=a,end=a+len(text))
        if purpose:v['purpose']=purpose
        return v
    change(2,['context_evidence'],[span('I remember when I went to China 15 years ago, and I still had to\nshow the taxi driver a picture of the hotel where I went to go.','antecedent')],'Resolve he to the explicitly mentioned taxi driver without identifying the interview speaker.')
    change(4,['speech_act'],'assertion','The current speaker reports a conversation without attributing an expressed strategic position to Will.')
    entities=raw['events'][6]['attribution']['mentioned_entities']
    if len(entities)!=1 or entities[0]['surface_name']!='Google Scholar':raise ValueError('product entity changed')
    change(7,['attribution','mentioned_entities'],[],'Google Scholar is a search product, not an organization; its name remains in the claim and source.')
    if s[2774:2780]!="That's":raise ValueError('local adequacy target changed')
    change(9,['attitude','target'],dict(text="That's",start=2774,end=2780),'The adequacy evaluation targets the preceding LLM capability through the explicit pronoun, not the jobs it facilitates.')
    change(9,['attitude','rationale'],'The preceding LLM knowledge-indexing capability is evaluated as adequate for the described desktop tasks; the tasks themselves are not praised. The supplied antecedent resolves Thats.','Align the evaluation with its source-resolved target.')
    context=span('and then it solves it and then distills it down into the automatizer','supporting_context')
    change(15,['context_evidence'],[context],'Include the explicit transfer destination supporting into the automatizer; preserve the original excerpt.')
    evidence=dict(context);evidence.pop('purpose')
    change(15,['position','source_evidence'],raw['events'][14]['position']['source_evidence']+[evidence],'Ground the complete claimed transfer, not merely compression and absorption.')
    change(17,['context_evidence'],[span('So what is that predictive coding. You just try to predict. If you can predict it, then you have to store it extra in some way.','antecedent')],'Resolve it to predictive coding while preserving the awkward source wording rather than silently repairing it.')
    fixed,proof=propose(raw,source=s,window_id=p['window_id'],expected_original_sha256=digest(raw),replacements=changes,output_validator=parent.run.previous.contract.validate)
    proof.update(parent_provenance_sha256=digest(prior),parent_reviews=proofs)
    return fixed,proof,p
