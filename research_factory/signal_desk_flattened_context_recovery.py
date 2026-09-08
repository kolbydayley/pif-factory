"""Combine exact original, first-delta, and context-delta approvals without transfer."""
from copy import deepcopy
import json
from .signal_desk_rubric_reference_packets import digest
from .signal_desk_repair_review_proof import verify
from .signal_desk_review_capacity_continuation import combined
from .signal_desk_flattened_recovery import unchanged_outside_delta


def unchanged_except_context_targets(baseline, fixed):
    restored = deepcopy(fixed)
    if [e['event_id'] for e in restored['events']] != [e['event_id'] for e in baseline['events']]:
        raise ValueError('flattened context population changed')
    restored['events'][8]['context_evidence'] = baseline['events'][8]['context_evidence']
    for key in ('claim_text', 'attitude'):
        restored['events'][13][key] = baseline['events'][13][key]
    if restored != baseline:
        raise ValueError('unreviewed context-delta content changed')


def recover(original, packet):
    from scripts import pif_signal_desk_flattened_interview_review as review
    from scripts import pif_signal_desk_flattened_capacity_continuation as continuation
    from scripts import pif_signal_desk_flattened_delta_review as first
    from scripts import pif_signal_desk_flattened_context_review as last
    baseline, prior, p = review.proposal()
    if p != packet or digest(original) != prior['original_sha256']:
        raise ValueError('flattened original changed')
    rows, proof = combined(continuation)
    first_ids = {baseline['events'][n-1]['event_id'] for n in first.NUMBERS}
    if len(rows) != 27 or {r['event_id'] for r in rows if r['verdict'] != 'supported'} != first_ids:
        raise ValueError('original flattened approval coverage changed')
    middle, mp, q = first.proposal()
    mids, midproof = verify(first, first.prepare(write=False))
    last_ids = {baseline['events'][n-1]['event_id'] for n in last.NUMBERS}
    if len(mids) != 11 or {d['event_id'] for d in mids} != first_ids or {d['event_id'] for d in mids if d['verdict'] != 'supported'} != last_ids:
        raise ValueError('first flattened delta approval coverage changed')
    fixed, fp, r = last.proposal()
    finals, finalproof = verify(last, last.prepare(write=False))
    if len(finals) != 2 or {d['event_id'] for d in finals} != last_ids or any(d['verdict'] != 'supported' for d in finals):
        raise ValueError('flattened context corrections not independently supported')
    if q != packet or r != packet:
        raise ValueError('flattened context source changed')
    for module, value, provenance in ((review, baseline, prior), (first, middle, mp), (last, fixed, fp)):
        if json.loads((module.OUT/'proposal.json').read_text()) != value or json.loads((module.OUT/'provenance.json').read_text()) != provenance:
            raise ValueError('flattened context saved proposal changed')
        plan = json.loads((module.OUT/'plan.json').read_text())
        if plan['proposal_sha256'] != digest(value) or plan['provenance_sha256'] != digest(provenance):
            raise ValueError('flattened context plan changed')
    unchanged_outside_delta(baseline, middle, first.NUMBERS)
    unchanged_except_context_targets(middle, fixed)
    return fixed, dict(repair=prior, reviews=proof, first_delta=mp, first_reviews=midproof,
        context_delta=fp, context_reviews=finalproof, approved_records=27, qualified=False, gold_accepted=False)
