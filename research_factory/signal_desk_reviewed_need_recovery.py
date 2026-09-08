"""Fail-closed proof loader for two explicitly inspected, reviewed proposals."""
import hashlib
import json
from .signal_desk_rubric_reference_packets import digest


def case_module(case):
    if case=='hidden-brain':
        from scripts import pif_signal_desk_hidden_brain_repair_review as module
    elif case=='marketplace':
        from scripts import pif_signal_desk_marketplace_lineage_repair_review as module
    else:raise ValueError('unknown reviewed repair case')
    return module


def recover(case,original,packet,*,review_root=None):
    module=case_module(case);root=module.OUT if review_root is None else review_root
    expected_role='A' if case=='hidden-brain' else 'C'
    if packet['window_id']!=module.WID or packet['role']!=expected_role:raise ValueError('wrong source or role')
    ps,fixed,proposal_proof=module.prepare(write=False)
    if len(ps)!=1:raise ValueError('unexpected review population')
    request=ps[0];sha=request['packet_sha256'];plan=json.loads((root/'plan.json').read_text())
    if plan['packets']!=[sha] or plan['source_packet_sha256']!=packet['packet_sha256'] or request['transcript_window']!=packet['transcript_window']:
        raise ValueError('review source or plan changed')
    original_key='original_sha256' if case=='hidden-brain' else 'original_envelope_sha256'
    if digest(original)!=proposal_proof[original_key]:raise ValueError('original response changed')
    if json.loads((root/'proposal.json').read_text())!=fixed or json.loads((root/'provenance.json').read_text())!=proposal_proof:
        raise ValueError('saved proposal changed')
    if json.loads((root/f'{sha}.packet.json').read_text())!=request:raise ValueError('saved review packet changed')
    value=json.loads((root/f'{sha}.review.json').read_text());raw=json.loads((root/f'{sha}.output.json').read_text())
    side=json.loads((root/f'{sha}.sidecar.json').read_text());h=lambda s:hashlib.sha256(s.encode()).hexdigest()
    if (raw!=value or side.get('state')!='completed' or side.get('error_class') or side.get('model')!='gpt-5.5' or side.get('effort')!='high' or
        side.get('base_instructions_sha256')!=h(module.SYSTEM) or side.get('prompt_sha256')!=h(json.dumps(request,ensure_ascii=False))):
        raise ValueError('independent approval provenance mismatch')
    from . import signal_desk_full_event_v5_review as review
    review.validate_review(value,request)
    decisions=value['decisions']
    if len(decisions)!=1 or decisions[0]['event_id']!=module.EID or decisions[0]['verdict']!='supported':
        raise ValueError('repair not independently supported')
    records=fixed['events'] if case=='hidden-brain' else fixed['records']['events']
    return fixed,{'repair':'reviewed-source-need-v1','case':case,'original_sha256':digest(original),'repaired_sha256':digest(fixed),
        'proposal_proof_sha256':digest(proposal_proof),'review_packet_sha256':sha,'review_sha256':digest(value),'review_sidecar_sha256':digest(side),
        'records_before':11,'records_after':len(records),'input_dispositions':len(fixed.get('input_dispositions',[])),
        'original_failure_preserved':True,'qualified':False,'gold_accepted':False}
