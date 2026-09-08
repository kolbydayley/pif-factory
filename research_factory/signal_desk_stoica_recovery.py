"""Apply only the source-bound Stoica proposal with complete actual review proof."""
import json
from copy import deepcopy
from .signal_desk_rubric_reference_packets import digest
from .signal_desk_repair_review_proof import verify


def recover(original,packet):
    from scripts import pif_signal_desk_stoica_review as review
    from scripts import pif_signal_desk_stoica_delta_review as delta
    baseline,provenance,expected=review.proposal()
    if expected!=packet or digest(original)!=provenance['original_sha256']:
        raise ValueError('Stoica original source changed')
    if (json.loads((review.OUT/'proposal.json').read_text())!=baseline
        or json.loads((review.OUT/'provenance.json').read_text())!=provenance):
        raise ValueError('Stoica saved proposal changed')
    decisions,proofs=verify(review,review.prepare(write=False))
    ids={e['event_id'] for e in baseline['events']}
    if (len(ids)!=15 or len(decisions)!=15 or {d['event_id'] for d in decisions}!=ids
        or {d['event_id'] for d in decisions if d['verdict']!='supported'}!=delta.IDS):
        raise ValueError('Stoica original review population changed')
    fixed,delta_provenance,p=delta.proposal()
    ds,delta_proofs=verify(delta,delta.prepare(write=False))
    if len(ds)!=2 or {d['event_id'] for d in ds}!=delta.IDS or any(d['verdict']!='supported' for d in ds):
        raise ValueError('Stoica delta not independently supported')
    plan=json.loads((delta.OUT/'plan.json').read_text())
    if (p!=packet or plan['proposal_sha256']!=digest(fixed) or plan['provenance_sha256']!=digest(delta_provenance)
        or json.loads((delta.OUT/'proposal.json').read_text())!=fixed
        or json.loads((delta.OUT/'provenance.json').read_text())!=delta_provenance):
        raise ValueError('Stoica saved delta changed')
    assert_attitude_only_delta(baseline,fixed)
    return fixed,dict(repair=provenance,reviews=proofs,delta=delta_provenance,delta_reviews=delta_proofs,
                     qualified=False,gold_accepted=False)


def assert_attitude_only_delta(baseline,fixed):
    restored=deepcopy(fixed)
    if len(restored['events'])!=15:raise ValueError('Stoica population changed')
    for i in (0,1):restored['events'][i]['attitude']=baseline['events'][i]['attitude']
    if restored!=baseline:raise ValueError('Stoica unreviewed content changed')
