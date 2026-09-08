"""Require full B approval plus the corrected antecedent's actual review."""
import json
from copy import deepcopy
from .signal_desk_rubric_reference_packets import digest
from .signal_desk_repair_review_proof import verify


def unchanged_except_context(baseline,fixed):
    restored=deepcopy(fixed)
    if [e['event_id'] for e in restored['events']]!=[e['event_id'] for e in baseline['events']]:
        raise ValueError('B population changed')
    restored['events'][7]['context_evidence']=baseline['events'][7]['context_evidence']
    if restored!=baseline:raise ValueError('unreviewed B content changed')


def recover(original,packet):
    from scripts import pif_signal_desk_stoica_b_review as review
    from scripts import pif_signal_desk_stoica_b_context_review as delta
    baseline,provenance,p=review.proposal()
    if packet!=p or digest(original)!=provenance['original_sha256']:raise ValueError('B original changed')
    rows,proofs=verify(review,review.prepare(write=False))
    ids={e['event_id'] for e in baseline['events']}
    if len(ids)!=12 or len(rows)!=12 or {r['event_id'] for r in rows}!=ids or {r['event_id'] for r in rows if r['verdict']!='supported'}!={delta.EVENT}:
        raise ValueError('B original review coverage changed')
    fixed,dp,q=delta.proposal();ds,dproofs=verify(delta,delta.prepare(write=False))
    if len(ds)!=1 or ds[0]['event_id']!=delta.EVENT or ds[0]['verdict']!='supported':
        raise ValueError('B context correction not independently supported')
    if q!=packet:raise ValueError('B delta packet changed')
    for module,value,proof in [(review,baseline,provenance),(delta,fixed,dp)]:
        if (json.loads((module.OUT/'proposal.json').read_text())!=value or
            json.loads((module.OUT/'provenance.json').read_text())!=proof):
            raise ValueError('B saved proposal changed')
        plan=json.loads((module.OUT/'plan.json').read_text())
        if plan['proposal_sha256']!=digest(value) or plan['provenance_sha256']!=digest(proof):
            raise ValueError('B proposal plan changed')
    unchanged_except_context(baseline,fixed)
    return fixed,dict(repair=provenance,reviews=proofs,delta=dp,delta_reviews=dproofs,
                     approved_records=12,qualified=False,gold_accepted=False)
