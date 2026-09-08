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
        raise ValueError('TSMC correction not independently supported')
    return fixed, {'repair': provenance, 'reviews': proofs, 'gold_accepted': False, 'qualified': False}
