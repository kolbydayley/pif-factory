"""Read-only actual-provider verification for explicitly reconstructed repair packets."""
import hashlib
import json
from .signal_desk_rubric_reference_packets import digest
from . import signal_desk_full_event_v5_review as review


def verify(module,packets):
    root=module.OUT;plan=json.loads((root/'plan.json').read_text())
    if plan['packets']!=[p['packet_sha256'] for p in packets]:raise ValueError('review plan changed')
    decisions=[];proof=[];h=lambda s:hashlib.sha256(s.encode()).hexdigest()
    for p in packets:
        sha=p['packet_sha256'];s=json.loads((root/f'{sha}.sidecar.json').read_text())
        if s.get('state')!='completed' or s.get('error_class') or s.get('model')!='gpt-5.5' or s.get('effort')!='high':raise ValueError('review provider mismatch')
        if s.get('base_instructions_sha256')!=h(module.SYSTEM) or s.get('prompt_sha256')!=h(json.dumps(p,ensure_ascii=False)):raise ValueError('review request mismatch')
        if json.loads((root/f'{sha}.packet.json').read_text())!=p:raise ValueError('saved packet changed')
        v=json.loads((root/f'{sha}.review.json').read_text())
        if json.loads((root/f'{sha}.output.json').read_text())!=v:raise ValueError('review output changed')
        review.validate_review(v,p);decisions.extend(v['decisions'])
        proof.append({'packet_sha256':sha,'review_sha256':digest(v),'sidecar_sha256':digest(s)})
    return decisions,proof
