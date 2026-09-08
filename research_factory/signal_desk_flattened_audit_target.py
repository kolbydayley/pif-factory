"""Resolve the evaluated pronoun to its exact antecedent capability."""
from .signal_desk_full_event_v4_repair import propose
from .signal_desk_repair_review_proof import verify
from .signal_desk_rubric_reference_packets import digest


def prepare():
    from scripts import pif_signal_desk_flattened_audit_delta_review as parent
    raw,prior,p=parent.proposal();rows,proofs=verify(parent,parent.prepare(write=False))
    if len(rows)!=6 or {r['event_id'] for r in rows if r['verdict']!='supported'}!={'evt_09'}:
        raise ValueError('actual antecedent target review changed')
    s=p['transcript_window'];text="way of indexing the world's existing human generated knowledge"
    if s.count(text)!=1:raise ValueError('antecedent capability changed')
    start=s.index(text);target=dict(text=text,start=start,end=start+len(text))
    changes=[dict(path=['events',8,'attitude','target'],before=raw['events'][8]['attitude']['target'],after=target,
        reason='Resolve the adequacy evaluation to the exact knowledge-indexing capability in supplied antecedent context, not the pronoun itself.')]
    fixed,proof=propose(raw,source=s,window_id=p['window_id'],expected_original_sha256=digest(raw),replacements=changes,output_validator=parent.run.previous.contract.validate)
    proof.update(parent_provenance_sha256=digest(prior),parent_reviews=proofs)
    return fixed,proof,p
