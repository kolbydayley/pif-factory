"""Fail closed until all 27 flattened records have content-specific review coverage."""
from copy import deepcopy
import json
from .signal_desk_rubric_reference_packets import digest
from .signal_desk_repair_review_proof import verify
from .signal_desk_review_capacity_continuation import combined


def unchanged_outside_delta(baseline, fixed, numbers):
    if len(fixed['events']) != 27 or [e['event_id'] for e in fixed['events']] != [e['event_id'] for e in baseline['events']]:
        raise ValueError('flattened population changed')
    restored = deepcopy(fixed)
    for n in numbers:
        before, after = baseline['events'][n-1], fixed['events'][n-1]
        for key in ('voice_binding_id', 'attribution_confidence', 'publishability_state', 'evidence_text', 'evidence_start', 'evidence_end'):
            if before[key] != after[key]:
                raise ValueError('unreviewed source or attribution change')
        for key in ('transcript_voice', 'proposition_owner', 'relation'):
            if before['attribution'][key] != after['attribution'][key]:
                raise ValueError('unreviewed speaker assignment')
        restored['events'][n-1] = baseline['events'][n-1]
    if restored != baseline:
        raise ValueError('unreviewed flattened content changed')


def recover(original, packet):
    from scripts import pif_signal_desk_flattened_interview_review as review
    from scripts import pif_signal_desk_flattened_capacity_continuation as continuation
    from scripts import pif_signal_desk_flattened_delta_review as delta
    baseline, prior, p = review.proposal()
    if p != packet or digest(original) != prior['original_sha256']:
        raise ValueError('flattened original changed')
    rows, proofs = combined(continuation)
    expected = {baseline['events'][n-1]['event_id'] for n in delta.NUMBERS}
    if len(rows) != 27 or {r['event_id'] for r in rows if r['verdict'] != 'supported'} != expected:
        raise ValueError('flattened prior coverage changed')
    fixed, dp, q = delta.proposal()
    ps = delta.prepare(write=False)
    ds, delta_proofs = verify(delta, ps)
    if len(ds) != 11 or {d['event_id'] for d in ds} != expected or any(d['verdict'] != 'supported' for d in ds):
        raise ValueError('flattened corrections not independently supported')
    if q != packet:
        raise ValueError('flattened source packet changed')
    for module, value, proof in ((review, baseline, prior), (delta, fixed, dp)):
        if json.loads((module.OUT/'proposal.json').read_text()) != value or json.loads((module.OUT/'provenance.json').read_text()) != proof:
            raise ValueError('flattened saved proposal changed')
        plan = json.loads((module.OUT/'plan.json').read_text())
        if plan['proposal_sha256'] != digest(value) or plan['provenance_sha256'] != digest(proof):
            raise ValueError('flattened proposal plan changed')
    unchanged_outside_delta(baseline, fixed, delta.NUMBERS)
    return fixed, dict(repair=prior, reviews=proofs, delta=dp, delta_reviews=delta_proofs,
        approved_records=27, qualified=False, gold_accepted=False)
