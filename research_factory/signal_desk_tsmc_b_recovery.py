"""TSMC B requires its own complete actual review, never author A approval."""
import json
from .signal_desk_rubric_reference_packets import digest
from .signal_desk_repair_review_proof import verify


def recover(original,packet):
    from scripts import pif_signal_desk_tsmc_b_review as review
    fixed,provenance,expected=review.proposal()
    if packet!=expected or digest(original)!=provenance['original_sha256']:
        raise ValueError('TSMC B original changed')
    if json.loads((review.OUT/'proposal.json').read_text())!=fixed or json.loads((review.OUT/'provenance.json').read_text())!=provenance:
        raise ValueError('TSMC B saved proposal changed')
    decisions,proofs=verify(review,review.prepare(write=False))
    ids={e['event_id'] for e in fixed['events']}
    if len(ids)!=13 or len(decisions)!=13 or {d['event_id'] for d in decisions}!=ids:
        raise ValueError('TSMC B approval population changed')
    if any(d['verdict']!='supported' for d in decisions):
        raise ValueError('TSMC B not independently supported')
    return fixed,{'repair':provenance,'reviews':proofs,'gold_accepted':False,'qualified':False}
