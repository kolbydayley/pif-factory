"""Preserve the last ambiguous ASR phrase without asserting a grammatical repair."""
from .signal_desk_repair_review_proof import verify
from .signal_desk_full_event_v4_repair import propose
from .signal_desk_rubric_reference_packets import digest


def prepare():
    from scripts import pif_signal_desk_flattened_b_delta_review as parent
    raw,prior,p=parent.proposal();rows,proofs=verify(parent,parent.prepare(write=False))
    identity=raw['events'][3]['event_id']
    if len(rows)!=6 or {r['event_id'] for r in rows if r['verdict']!='supported'}!={identity}:
        raise ValueError('surface correction population changed')
    text='The speaker claims our AI makes human lives longer, healthier and easier, describing "thousands of applications and medicine and drug design, sustainable development."'
    fixed,proof=propose(raw,source=p['transcript_window'],window_id=p['window_id'],expected_original_sha256=digest(raw),
        replacements=[dict(path=['events',3,'claim_text'],before=raw['events'][3]['claim_text'],after=text,
            reason='Quote the ambiguous source list rather than silently replacing and medicine with in medicine.')],
        output_validator=parent.run.previous.contract.validate)
    proof.update(parent_provenance_sha256=digest(prior),parent_reviews=proofs)
    return fixed,proof,p
