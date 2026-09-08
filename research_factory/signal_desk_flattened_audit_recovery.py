"""Apply only the exact, fully reviewed flattened AUDIT proposal and metadata delta."""
from copy import deepcopy
import json
from .signal_desk_rubric_reference_packets import digest
from .signal_desk_repair_review_proof import verify


def unchanged_except_metadata(baseline, fixed):
    restored = deepcopy(fixed)
    if [e['event_id'] for e in restored['events']] != [e['event_id'] for e in baseline['events']]:
        raise ValueError('audit population changed')
    for n in (2,4,7,9,15,17):
        before,after=baseline['events'][n-1],fixed['events'][n-1]
        for key in ('voice_binding_id','evidence_text','evidence_start','evidence_end','publishability_state'):
            if before[key]!=after[key]:raise ValueError('unreviewed evidence or attribution change')
        attribution=deepcopy(after['attribution'])
        if n==7:attribution['mentioned_entities']=before['attribution']['mentioned_entities']
        if attribution!=before['attribution']:raise ValueError('unreviewed attribution changed')
        if n==7 and after['attribution']['mentioned_entities']!=[]:raise ValueError('unexpected product entity replacement')
        restored['events'][n-1]=baseline['events'][n-1]
    if restored != baseline:
        raise ValueError('unreviewed audit content changed')


def recover(original, packet):
    from scripts import pif_signal_desk_flattened_audit_review as review
    from scripts import pif_signal_desk_flattened_audit_delta_review as delta
    from scripts import pif_signal_desk_flattened_audit_target_review as final_review
    baseline, prior, p = review.proposal()
    if packet != p or digest(original) != prior['original_sha256']:
        raise ValueError('flattened AUDIT original changed')
    rows, proofs = verify(review, review.prepare(write=False))
    ids = {e['event_id'] for e in baseline['events']}
    changed={baseline['events'][n-1]['event_id'] for n in (2,4,7,9,15,17)}
    if len(rows) != 17 or {r['event_id'] for r in rows} != ids or {r['event_id'] for r in rows if r['verdict'] != 'supported'} != changed:
        raise ValueError('audit review coverage changed')
    fixed, dp, q = delta.proposal()
    ds, delta_proofs = verify(delta, delta.prepare(write=False))
    final_id=baseline['events'][8]['event_id']
    if len(ds) != 6 or {d['event_id'] for d in ds} != changed or {d['event_id'] for d in ds if d['verdict'] != 'supported'} != {final_id}:
        raise ValueError('audit correction not independently supported')
    final,fp,t=final_review.proposal()
    fs,final_proofs=verify(final_review,final_review.prepare(write=False))
    if len(fs)!=1 or fs[0]['event_id']!=final_id or fs[0]['verdict']!='supported':
        raise ValueError('surface correction not independently supported')
    restored=deepcopy(final)
    restored['events'][8]['attitude']['target']=fixed['events'][8]['attitude']['target']
    if restored!=fixed or t!=packet:
        raise ValueError('unreviewed final surface content changed')
    if packet != q:
        raise ValueError('audit delta source changed')
    for module, value, proof in ((review, baseline, prior), (delta, fixed, dp), (final_review, final, fp)):
        if json.loads((module.OUT/'proposal.json').read_text()) != value or json.loads((module.OUT/'provenance.json').read_text()) != proof:
            raise ValueError('audit saved proposal changed')
        plan = json.loads((module.OUT/'plan.json').read_text())
        if plan['proposal_sha256'] != digest(value) or plan['provenance_sha256'] != digest(proof):
            raise ValueError('audit proposal plan changed')
    unchanged_except_metadata(baseline, fixed)
    return final, dict(repair=prior, reviews=proofs, delta=dp, delta_reviews=delta_proofs,
        surface_delta=fp, surface_reviews=final_proofs,
        approved_records=17, qualified=False, gold_accepted=False)
