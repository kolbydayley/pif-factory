"""Fail-closed recovery of an exactly proposed and independently approved repair."""
import hashlib
import json
from .signal_desk_rubric_reference_packets import digest


def recover(original,packet,*,review_root=None):
    from scripts import pif_signal_desk_coding_tools_repair_review as module
    if packet['window_id']!=module.diagnosis.WID or packet['role']!='A' or packet['packet_sha256']!=module.diagnosis.PACKET:
        raise ValueError('wrong source or role')
    ps,fixed,proof=module.prepare(write=False);root=module.OUT if review_root is None else review_root
    if digest(original)!=proof['original_sha256']:raise ValueError('original response changed')
    plan=json.loads((root/'plan.json').read_text())
    if plan['packets']!=[p['packet_sha256'] for p in ps] or plan['proposal_sha256']!=digest(fixed) or plan['provenance_sha256']!=digest(proof):
        raise ValueError('review plan changed')
    if json.loads((root/'proposal.json').read_text())!=fixed or json.loads((root/'provenance.json').read_text())!=proof:
        raise ValueError('saved proposal changed')
    h=lambda s:hashlib.sha256(s.encode()).hexdigest();reviews=[];ids=[]
    for p in ps:
        sha=p['packet_sha256']
        if p['transcript_window']!=packet['transcript_window'] or json.loads((root/f'{sha}.packet.json').read_text())!=p:
            raise ValueError('review source or packet changed')
        side=json.loads((root/f'{sha}.sidecar.json').read_text())
        if side.get('state')!='completed' or side.get('error_class') or side.get('model')!='gpt-5.5' or side.get('effort')!='high':
            raise ValueError('approval provider mismatch')
        if side.get('base_instructions_sha256')!=h(module.SYSTEM) or side.get('prompt_sha256')!=h(json.dumps(p,ensure_ascii=False)):
            raise ValueError('approval request mismatch')
        value=json.loads((root/f'{sha}.review.json').read_text())
        if value!=json.loads((root/f'{sha}.output.json').read_text()):raise ValueError('approval output changed')
        module.diagnosis.run.previous.review.validate_review(value,p)
        if any(d['verdict']!='supported' for d in value['decisions']):raise ValueError('repair not independently supported')
        ids.extend(d['event_id'] for d in value['decisions'])
        reviews.append({'packet_sha256':sha,'review_sha256':digest(value),'sidecar_sha256':digest(side)})
    expected=[e['event_id'] for e in fixed['events']]
    if len(ids)!=len(expected) or set(ids)!=set(expected):raise ValueError('incomplete approval population')
    return fixed,{'repair':'coding-tools-reviewed-explicit-v1','original_sha256':digest(original),'repaired_sha256':digest(fixed),
        'proposal_proof_sha256':digest(proof),'reviews':reviews,'records_before':15,'records_after':15,
        'original_failure_preserved':True,'qualified':False,'gold_accepted':False}
