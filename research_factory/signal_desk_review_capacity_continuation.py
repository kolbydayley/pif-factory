"""Preserve successful review prefixes; retry only verified capacity failures."""
from datetime import datetime
import hashlib
import json
from .signal_desk_rubric_reference_packets import digest
from .signal_desk_repair_review_proof import verify


def prepare(parent, *, failed_sha, cooldown_seconds, output_root, write=True):
    ps=parent.prepare(write=False)
    if cooldown_seconds<300:raise ValueError('capacity cooldown below minimum')
    saved=json.loads((parent.OUT/'plan.json').read_text())
    if saved['packets']!=[p['packet_sha256'] for p in ps]:raise ValueError('parent plan changed')
    preserved=[];remaining=[];failed=False;failure=None
    h=lambda text:hashlib.sha256(text.encode()).hexdigest()
    for p in ps:
        sha=p['packet_sha256'];sp=parent.OUT/f'{sha}.sidecar.json'
        if json.loads((parent.OUT/f'{sha}.packet.json').read_text())!=p:raise ValueError('original packet changed')
        if failed:
            if any((parent.OUT/f'{sha}.{suffix}.json').exists() for suffix in ('sidecar','output','review','pending')):
                raise ValueError('later packet already attempted; inspect before continuation')
            remaining.append(p);continue
        s=json.loads(sp.read_text())
        if s.get('model')!='gpt-5.5' or s.get('effort')!='high' or s.get('base_instructions_sha256')!=h(parent.SYSTEM) or s.get('prompt_sha256')!=h(json.dumps(p,ensure_ascii=False)):
            raise ValueError('original provider/request mismatch')
        if sha==failed_sha:
            if s.get('state')!='failed' or s.get('turn_error',{}).get('codex_error_info')!='serverOverloaded' or s.get('turn_error',{}).get('backend_message')!='Selected model is at capacity. Please try a different model.' or s.get('output_sha256') is not None:
                raise ValueError('not a no-output capacity failure')
            if any((parent.OUT/f'{sha}.{suffix}.json').exists() for suffix in ('output','review','pending')):
                raise ValueError('failed review has output; no blind retry')
            failure=s;failed=True;remaining.append(p);continue
        if s.get('state')!='completed' or s.get('error_class'):raise ValueError('preceding review incomplete')
        v=json.loads((parent.OUT/f'{sha}.review.json').read_text())
        if v!=json.loads((parent.OUT/f'{sha}.output.json').read_text()):raise ValueError('preceding output changed')
        parent.run.previous.review.validate_review(v,p)
        preserved.append({'packet_sha256':sha,'sidecar_sha256':digest(s),'review_sha256':digest(v)})
    if not failed:raise ValueError('failed packet outside original population')
    receipt={'packets':[p['packet_sha256'] for p in remaining], 'parent_packets':saved['packets'],
        'preserved_reviews':preserved,'original_capacity_failure_sha256':digest(failure),
        'not_before_epoch':datetime.fromisoformat(failure['finished_at']).timestamp()+cooldown_seconds,
        'cooldown_seconds':cooldown_seconds,'same_semantic_sample':True,'original_charge_preserved':True,
        'gold_accepted':False,'applied':False}
    if write:
        output_root.mkdir(parents=True,exist_ok=True,mode=0o700)
        parent.run.immutable_json(output_root/'plan.json',receipt)
        for p in remaining:parent.run.immutable_json(output_root/f"{p['packet_sha256']}.packet.json",p)
    return remaining,receipt


def combined(module):
    ps,lineage=module.prepare(write=False)
    if json.loads((module.OUT/'plan.json').read_text())!=lineage:raise ValueError('continuation lineage changed')
    decisions,proof=verify(module,ps)
    rows=[]
    for prior in lineage['preserved_reviews']:
        v=json.loads((module.parent.OUT/f"{prior['packet_sha256']}.review.json").read_text())
        if digest(v)!=prior['review_sha256']:raise ValueError('preserved review changed')
        rows.extend(v['decisions'])
    rows.extend(decisions)
    expected=[e['event_id'] for p in module.parent.prepare(write=False) for e in p['candidates']]
    if len(rows)!=len(expected) or len({d['event_id'] for d in rows})!=len(expected) or {d['event_id'] for d in rows}!=set(expected):
        raise ValueError('combined review population changed')
    return rows,{'preserved_reviews':lineage['preserved_reviews'],'continuation_reviews':proof,
        'original_capacity_failure_sha256':lineage['original_capacity_failure_sha256'],'gold_accepted':False}
