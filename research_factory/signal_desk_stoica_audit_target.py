"""Final explicit target refinement required by the actual delta review."""
from .signal_desk_full_event_v4_repair import propose
from .signal_desk_repair_review_proof import verify
from .signal_desk_rubric_reference_packets import digest


def prepare():
    from scripts import pif_signal_desk_stoica_audit_delta_review as parent
    raw,prior,p=parent.proposal();rows,proofs=verify(parent,parent.prepare(write=False))
    if len(rows)!=5 or {r['event_id'] for r in rows if r['verdict']!='supported'}!={'evt_003'}:
        raise ValueError('actual target correction review changed')
    s=p['transcript_window']
    def span(text):
        if s.count(text)!=1:raise ValueError('ambiguous source target')
        a=s.index(text);return dict(text=text,start=a,end=a+len(text))
    component=raw['events'][2]['attitude']['target_components'][1]
    target=span('this system where it’s the human in the loop')
    evidence=span('you are still limited to having this system where it’s the human in the loop, and the human is a bottleneck, will become the bottleneck.')
    changes=[dict(path=['events',2,'attitude','target_components',1,key],before=component[key],after=after,
        reason='The negative evaluation concerns continued dependence on the human-in-the-loop system, not people themselves; preserve the full limitation clause.')
        for key,after in [('target',target),('evaluation_evidence',[evidence])]]
    fixed,proof=propose(raw,source=s,window_id=p['window_id'],expected_original_sha256=digest(raw),replacements=changes,output_validator=parent.run.previous.contract.validate)
    proof.update(parent_provenance_sha256=digest(prior),parent_reviews=proofs)
    return fixed,proof,p
