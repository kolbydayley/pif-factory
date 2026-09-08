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
        from scripts import pif_signal_desk_tsmc_b_delta_review as delta
        if {d['event_id'] for d in decisions if d['verdict']!='supported'} != delta.IDS:
            raise ValueError('TSMC B not independently supported')
        revised, delta_provenance, delta_packet = delta.proposal()
        delta_decisions, delta_proofs = verify(delta, delta.prepare(write=False))
        if (len(delta_decisions)!=3 or {d['event_id'] for d in delta_decisions}!=delta.IDS
            or any(d['verdict']!='supported' for d in delta_decisions)):
            raise ValueError('TSMC B delta not independently supported')
        plan=json.loads((delta.OUT/'plan.json').read_text())
        if (delta_packet!=packet or plan['proposal_sha256']!=digest(revised)
            or plan['provenance_sha256']!=digest(delta_provenance)
            or json.loads((delta.OUT/'proposal.json').read_text())!=revised
            or json.loads((delta.OUT/'provenance.json').read_text())!=delta_provenance):
            raise ValueError('TSMC B saved delta changed')
        assert_unchanged_approvals(fixed,revised,delta.IDS)
        return revised,{'repair':provenance,'reviews':proofs,'delta':delta_provenance,
            'delta_reviews':delta_proofs,'gold_accepted':False,'qualified':False}
    return fixed,{'repair':provenance,'reviews':proofs,'gold_accepted':False,'qualified':False}


def assert_unchanged_approvals(original,revised,changed_ids):
    from copy import deepcopy
    restored=deepcopy(revised)
    if [e['event_id'] for e in original['events']] != [e['event_id'] for e in restored['events']]:
        raise ValueError('TSMC B delta population changed')
    for i,e in enumerate(original['events']):
        if e['event_id'] in changed_ids:
            restored['events'][i]=e
    if restored!=original:
        raise ValueError('TSMC B previously approved content changed')
