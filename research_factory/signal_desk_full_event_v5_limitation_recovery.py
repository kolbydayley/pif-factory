"""Recompute a narrowly approved quarantine repair from immutable evidence."""
import hashlib
import json
from .signal_desk_full_event_v4_repair import propose
from .signal_desk_full_event_v5 import validate
from .signal_desk_rubric_reference_packets import digest


def recover(original, packet, root):
    from scripts.pif_signal_desk_full_event_v5_limitation_review import SYSTEM,WID,EID,prepare
    from . import signal_desk_full_event_v5_review as review
    if packet['window_id']!=WID or packet['role']!='B' or len(original['events'])!=9:
        raise ValueError('not the independently inspected B record')
    plan=json.loads((root/'plan.json').read_text());receipt=json.loads((root/'provenance.json').read_text())
    if plan['original_B_packet_sha256']!=packet['packet_sha256'] or plan['provenance_sha256']!=digest(receipt):
        raise ValueError('review plan/source lineage changed')
    i=next(i for i,e in enumerate(original['events']) if e['event_id']==EID)
    expected=[(['events',i,'evidence_role','role'],'substantive_claim','research_limitation'),
        (['events',i,'publishability_state'],'candidate','quarantined')]
    if [(r['path'],r['before'],r['after']) for r in receipt['replacements']]!=expected:
        raise ValueError('unapproved semantic changes')
    fixed,recomputed=propose(original,source=packet['transcript_window'],window_id=WID,
        expected_original_sha256=receipt['original_sha256'],replacements=receipt['replacements'],output_validator=validate)
    if recomputed!=receipt or json.loads((root/'proposal.json').read_text())!=fixed:
        raise ValueError('proposal changed')
    if len(plan['packets'])!=1:raise ValueError('changed review population')
    sha=plan['packets'][0];p=json.loads((root/f'{sha}.packet.json').read_text())
    if digest({k:v for k,v in p.items() if k!='packet_sha256'})!=sha or p['repair_provenance']!=receipt or p['transcript_window']!=packet['transcript_window']:
        raise ValueError('review packet changed')
    if p['candidates']!=[fixed['events'][i]] or p['candidate_population']!=9:
        raise ValueError('review candidate mismatch')
    if p['system_sha256']!=digest(SYSTEM) or p['schema_sha256']!=digest(review.schema(p)):
        raise ValueError('review contract changed')
    result=json.loads((root/f'{sha}.review.json').read_text());raw=json.loads((root/f'{sha}.output.json').read_text())
    s=json.loads((root/f'{sha}.sidecar.json').read_text())
    h=lambda text:hashlib.sha256(text.encode()).hexdigest()
    if raw!=result or s.get('state')!='completed' or s.get('error_class') or s.get('model')!='gpt-5.5' or s.get('effort')!='high':
        raise ValueError('independent approval did not complete')
    # Frozen JSON files sort keys; provider prompts used construction order.
    # Rebuild the exact request read-only, verify semantic equality, then hash
    # its serialized bytes. Never drop this check merely for map ordering.
    rebuilt=prepare(write=False)[0]
    if rebuilt!=p:raise ValueError('rebuilt review request changed')
    if s['base_instructions_sha256']!=h(SYSTEM) or s['prompt_sha256']!=h(json.dumps(rebuilt,ensure_ascii=False)):
        raise ValueError('approval sidecar contract mismatch')
    review.validate_review(result,p)
    if len(result['decisions'])!=1 or result['decisions'][0]['event_id']!=EID or result['decisions'][0]['verdict']!='supported':
        raise ValueError('quarantine repair not independently supported')
    proof={'repair':'v5-reviewed-limitation-quarantine-v1','proposal_receipt_sha256':digest(receipt),
        'original_sha256':digest(original),'repaired_sha256':digest(fixed),'review_packet_sha256':sha,
        'review_sha256':digest(result),'review_sidecar_sha256':digest(s),'original_failure_preserved':True,
        'records_before':9,'records_after':9,'gold_accepted':False,'qualified':False}
    return fixed,proof
