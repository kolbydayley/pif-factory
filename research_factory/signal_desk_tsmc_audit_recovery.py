"""Apply only the exact, fully reviewed TSMC audit proposal and metadata delta."""
from copy import deepcopy
import json
from .signal_desk_rubric_reference_packets import digest
from .signal_desk_repair_review_proof import verify


def unchanged_except_metadata(baseline, fixed):
    restored = deepcopy(fixed)
    if [e['event_id'] for e in restored['events']] != [e['event_id'] for e in baseline['events']]:
        raise ValueError('audit population changed')
    for key in ('issue_aliases', 'attitude'):
        restored['events'][5][key] = baseline['events'][5][key]
    if restored != baseline:
        raise ValueError('unreviewed audit content changed')


def recover(original, packet):
    from scripts import pif_signal_desk_tsmc_audit_review as review
    from scripts import pif_signal_desk_tsmc_audit_delta_review as delta
    baseline, prior, p = review.proposal()
    if packet != p or digest(original) != prior['original_sha256']:
        raise ValueError('TSMC audit original changed')
    rows, proofs = verify(review, review.prepare(write=False))
    ids = {e['event_id'] for e in baseline['events']}
    if len(rows) != 15 or {r['event_id'] for r in rows} != ids or {r['event_id'] for r in rows if r['verdict'] != 'supported'} != {delta.EVENT}:
        raise ValueError('audit review coverage changed')
    fixed, dp, q = delta.proposal()
    ds, delta_proofs = verify(delta, delta.prepare(write=False))
    if len(ds) != 1 or ds[0]['event_id'] != delta.EVENT or ds[0]['verdict'] != 'supported':
        raise ValueError('audit correction not independently supported')
    if packet != q:
        raise ValueError('audit delta source changed')
    for module, value, proof in ((review, baseline, prior), (delta, fixed, dp)):
        if json.loads((module.OUT/'proposal.json').read_text()) != value or json.loads((module.OUT/'provenance.json').read_text()) != proof:
            raise ValueError('audit saved proposal changed')
        plan = json.loads((module.OUT/'plan.json').read_text())
        if plan['proposal_sha256'] != digest(value) or plan['provenance_sha256'] != digest(proof):
            raise ValueError('audit proposal plan changed')
    unchanged_except_metadata(baseline, fixed)
    return fixed, dict(repair=prior, reviews=proofs, delta=dp, delta_reviews=delta_proofs,
        approved_records=15, qualified=False, gold_accepted=False)
