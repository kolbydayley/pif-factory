"""Fail-closed approval proof for the explicit TSMC correction."""
import json
from .signal_desk_rubric_reference_packets import digest
from .signal_desk_repair_review_proof import verify


def review_proof(review):
    from scripts import pif_signal_desk_tsmc_capacity_continuation as continuation
    if (continuation.OUT/'plan.json').exists():
        from .signal_desk_review_capacity_continuation import combined
        # A started continuation cannot silently fall back to the incomplete
        # original attempt or overwrite its failed provider receipt.
        return combined(continuation)
    return verify(review, review.prepare(write=False))


def recover(original, packet):
    from scripts import pif_signal_desk_tsmc_repair_review as review
    fixed, provenance, expected_packet = review.proposal()
    if packet != expected_packet or digest(original) != provenance['original_sha256']:
        raise ValueError('TSMC original source/request changed')
    if json.loads((review.OUT/'proposal.json').read_text()) != fixed or json.loads((review.OUT/'provenance.json').read_text()) != provenance:
        raise ValueError('TSMC saved proposal changed')
    decisions, proofs = review_proof(review)
    expected = {e['event_id'] for e in fixed['events']}
    if len(decisions) != 14 or len(expected) != 14 or {d['event_id'] for d in decisions} != expected:
        raise ValueError('incomplete TSMC review population')
    if any(d['verdict'] != 'supported' for d in decisions):
        from scripts import pif_signal_desk_tsmc_delta_review as delta
        eid = fixed['events'][10]['event_id']
        if {d['event_id'] for d in decisions if d['verdict'] != 'supported'} != {eid}:
            raise ValueError('TSMC correction not independently supported')
        packets, revised, delta_provenance = delta.prepare(write=False)
        delta_decisions, delta_proofs = verify(delta, packets)
        if len(delta_decisions) != 1 or delta_decisions[0]['event_id'] != eid or delta_decisions[0]['verdict'] != 'supported':
            raise ValueError('TSMC delta not independently supported')
        plan = json.loads((delta.OUT/'plan.json').read_text())
        if (plan['proposal_sha256'] != digest(revised) or plan['provenance_sha256'] != digest(delta_provenance)
            or json.loads((delta.OUT/'proposal.json').read_text()) != revised
            or json.loads((delta.OUT/'provenance.json').read_text()) != delta_provenance):
            raise ValueError('TSMC saved delta changed')
        # Only the reviewed claim text can change. All thirteen approvals bind to
        # byte-identical records and the same full source and voice pool.
        from copy import deepcopy
        unchanged = deepcopy(revised)
        if len(unchanged['events']) != 14 or unchanged['events'][10]['event_id'] != eid:
            raise ValueError('TSMC delta population changed')
        unchanged['events'][10]['claim_text'] = fixed['events'][10]['claim_text']
        if unchanged != fixed:
            raise ValueError('TSMC unreviewed delta mutation')
        return revised, {'repair': provenance, 'reviews': proofs, 'delta': delta_provenance,
                         'delta_reviews': delta_proofs, 'gold_accepted': False, 'qualified': False}
    return fixed, {'repair': provenance, 'reviews': proofs, 'gold_accepted': False, 'qualified': False}
