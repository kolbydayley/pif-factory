"""Require actual approvals across all three audit proposal generations."""
import json
from .signal_desk_rubric_reference_packets import digest
from .signal_desk_repair_review_proof import verify
from .signal_desk_ai_governance_audit_recovery import unchanged_approvals


def recover(original, packet):
    from scripts import pif_signal_desk_ai_governance_audit_review as first
    from scripts import pif_signal_desk_ai_governance_audit_delta_review as second
    from scripts import pif_signal_desk_ai_governance_audit_context_review as third
    proofs=[];previous=None;approved=set()
    stages=[(first,second.IDS),(second,third.IDS),(third,set())]
    for module,remaining in stages:
        fixed,provenance,p=module.proposal()
        if p!=packet:raise ValueError('audit packet changed')
        if module is first and digest(original)!=provenance['original_sha256']:
            raise ValueError('audit original changed')
        if previous is not None:unchanged_approvals(previous,fixed,module.IDS)
        for name,value in [('proposal',fixed),('provenance',provenance)]:
            if json.loads((module.OUT/f'{name}.json').read_text())!=value:
                raise ValueError('audit saved proposal/provenance changed')
        packets=module.prepare(write=False);rows,receipts=verify(module,packets)
        expected={e['event_id'] for q in packets for e in q['candidates']}
        if len(rows)!=len(expected) or {r['event_id'] for r in rows}!=expected:
            raise ValueError('audit review coverage changed')
        if {r['event_id'] for r in rows if r['verdict']!='supported'}!=remaining:
            raise ValueError('audit context delta not independently supported')
        approved.difference_update(expected)
        approved.update(r['event_id'] for r in rows if r['verdict']=='supported')
        proofs.append(dict(provenance=provenance,reviews=receipts))
        previous=fixed
    if approved!={e['event_id'] for e in fixed['events']} or len(approved)!=15:
        raise ValueError('full audit approval coverage incomplete')
    return fixed,dict(generations=proofs,approved_records=15,qualified=False,gold_accepted=False)
