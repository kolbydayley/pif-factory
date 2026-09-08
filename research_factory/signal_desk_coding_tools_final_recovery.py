"""Combine exact unchanged and delta approvals; never drop a failed record."""
import json
from .signal_desk_coding_tools_recovery import inspect_reviews
from .signal_desk_repair_review_proof import verify
from .signal_desk_rubric_reference_packets import digest


def recover(original,packet):
    from scripts import pif_signal_desk_coding_tools_delta_review as v2
    from scripts import pif_signal_desk_coding_tools_final_delta_review as v3
    value,first_proof,decisions=inspect_reviews(original,packet)
    current={e['event_id']:e for e in value['events']};approved={d['event_id']:d['verdict'] for d in decisions}
    chain=[first_proof]
    for module,expected_count in [(v2,4),(v3,1)]:
        ps,fixed,proof=module.prepare(write=False);ds,receipts=verify(module,ps)
        plan=json.loads((module.OUT/'plan.json').read_text())
        if plan['proposal_sha256']!=digest(fixed) or plan['provenance_sha256']!=digest(proof):raise ValueError('delta plan changed')
        if json.loads((module.OUT/'proposal.json').read_text())!=fixed or json.loads((module.OUT/'provenance.json').read_text())!=proof:
            raise ValueError('saved delta changed')
        assigned={d['event_id'] for d in ds};next_rows={e['event_id']:e for e in fixed['events']}
        if len(ds)!=expected_count or len(assigned)!=expected_count or set(next_rows)!=set(current):raise ValueError('delta population changed')
        if fixed['voice_bindings']!=value['voice_bindings']:raise ValueError('delta voice bindings changed')
        for eid,event in next_rows.items():
            if eid not in assigned and (event!=current[eid] or approved[eid]!='supported'):
                raise ValueError('unchanged approval cannot be reused')
        approved.update({d['event_id']:d['verdict'] for d in ds});current=next_rows;value=fixed
        chain.append({'proposal_sha256':digest(fixed),'proof_sha256':digest(proof),'reviews':receipts})
    if len(current)!=15 or any(v!='supported' for v in approved.values()):raise ValueError('final repair not fully supported')
    return value,{'repair':'coding-tools-reviewed-explicit-v3','original_sha256':digest(original),'repaired_sha256':digest(value),
        'approval_chain':chain,'records_before':15,'records_after':15,'original_failure_preserved':True,'qualified':False,'gold_accepted':False}
