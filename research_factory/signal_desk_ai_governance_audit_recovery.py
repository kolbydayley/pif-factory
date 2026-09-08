"""Require actual full-audit approval plus approval of every changed record."""
import json
from copy import deepcopy
from .signal_desk_rubric_reference_packets import digest
from .signal_desk_repair_review_proof import verify


def recover(original,packet):
    from scripts import pif_signal_desk_ai_governance_audit_review as review
    from scripts import pif_signal_desk_ai_governance_audit_delta_review as delta
    baseline,provenance,p=review.proposal()
    if packet!=p or digest(original)!=provenance['original_sha256']:raise ValueError('audit original changed')
    if (json.loads((review.OUT/'proposal.json').read_text())!=baseline
        or json.loads((review.OUT/'provenance.json').read_text())!=provenance):raise ValueError('audit saved proposal changed')
    rows,proofs=verify(review,review.prepare(write=False));ids={e['event_id'] for e in baseline['events']}
    if (len(ids)!=15 or len(rows)!=15 or {r['event_id'] for r in rows}!=ids
        or {r['event_id'] for r in rows if r['verdict']!='supported'}!=delta.IDS):raise ValueError('audit review population changed')
    fixed,delta_proof,q=delta.proposal();ds,dps=verify(delta,delta.prepare(write=False))
    if len(ds)!=4 or {d['event_id'] for d in ds}!=delta.IDS or any(d['verdict']!='supported' for d in ds):
        raise ValueError('audit delta not independently supported')
    plan=json.loads((delta.OUT/'plan.json').read_text())
    if (q!=packet or plan['proposal_sha256']!=digest(fixed) or plan['provenance_sha256']!=digest(delta_proof)
        or json.loads((delta.OUT/'proposal.json').read_text())!=fixed
        or json.loads((delta.OUT/'provenance.json').read_text())!=delta_proof):raise ValueError('audit saved delta changed')
    unchanged_approvals(baseline,fixed,delta.IDS)
    return fixed,dict(repair=provenance,reviews=proofs,delta=delta_proof,delta_reviews=dps,qualified=False,gold_accepted=False)


def unchanged_approvals(baseline,fixed,changed):
    restored=deepcopy(fixed)
    if [e['event_id'] for e in restored['events']]!=[e['event_id'] for e in baseline['events']]:
        raise ValueError('audit delta population changed')
    for i,e in enumerate(baseline['events']):
        if e['event_id'] in changed:restored['events'][i]=e
    if restored!=baseline:raise ValueError('audit unreviewed content changed')
