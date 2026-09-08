"""Require all fifteen luxury records to have exact independent approval coverage."""
from copy import deepcopy
import json
from .signal_desk_rubric_reference_packets import digest
from .signal_desk_repair_review_proof import verify


def verified_delta():
    from scripts import pif_signal_desk_luxury_capacity_continuation as continuation
    from .signal_desk_review_capacity_continuation import combined
    return combined(continuation)


def unchanged_outside_delta(baseline, fixed, numbers):
    if len(fixed['events']) != 15 or [e['event_id'] for e in fixed['events']] != [e['event_id'] for e in baseline['events']]:
        raise ValueError('luxury population changed')
    restored = deepcopy(fixed)
    for n in numbers:
        before, after = baseline['events'][n-1], fixed['events'][n-1]
        for key in ('attribution', 'attribution_confidence', 'voice_binding_id', 'publishability_state', 'evidence_text', 'evidence_start', 'evidence_end'):
            if before[key] != after[key]:
                raise ValueError('unreviewed luxury evidence or attribution change')
        restored['events'][n-1] = baseline['events'][n-1]
    if restored != baseline:
        raise ValueError('unreviewed luxury content changed')


def recover(original, packet):
    from scripts import pif_signal_desk_luxury_review as review
    from scripts import pif_signal_desk_luxury_delta_review as delta
    baseline, prior, p = review.proposal()
    if packet != p or digest(original) != prior['original_sha256']:
        raise ValueError('luxury original changed')
    rows, proofs = verify(review, review.prepare(write=False))
    ids = {e['event_id'] for e in baseline['events']}
    changed = {baseline['events'][n-1]['event_id'] for n in delta.NUMBERS}
    if len(rows) != 15 or {r['event_id'] for r in rows} != ids or {r['event_id'] for r in rows if r['verdict'] != 'supported'} != changed:
        raise ValueError('luxury prior review coverage changed')
    fixed, dp, q = delta.proposal()
    ds, delta_proofs = verified_delta()
    if len(ds) != 4 or {d['event_id'] for d in ds} != changed or any(d['verdict'] != 'supported' for d in ds):
        raise ValueError('luxury corrections not independently supported')
    if q != packet:
        raise ValueError('luxury delta source changed')
    for module, value, proof in ((review, baseline, prior), (delta, fixed, dp)):
        if json.loads((module.OUT/'proposal.json').read_text()) != value or json.loads((module.OUT/'provenance.json').read_text()) != proof:
            raise ValueError('luxury saved proposal changed')
        plan = json.loads((module.OUT/'plan.json').read_text())
        if plan['proposal_sha256'] != digest(value) or plan['provenance_sha256'] != digest(proof):
            raise ValueError('luxury proposal plan changed')
    unchanged_outside_delta(baseline, fixed, delta.NUMBERS)
    return fixed, dict(repair=prior, reviews=proofs, delta=dp, delta_reviews=delta_proofs,
        approved_records=15, qualified=False, gold_accepted=False)
