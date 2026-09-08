"""Require exact full record approval and wholly fresh corrected C ledger review."""
import json
from .signal_desk_rubric_reference_packets import digest
from .signal_desk_actual_review_receipt import verify
from .signal_desk_tsmc_c_recovery import select_supported


def recover(original,packet):
    from scripts import pif_signal_desk_stoica_c_review as first
    from scripts import pif_signal_desk_stoica_c_delta_review as second
    baseline,bp,p=first.proposal();fixed,fp,q=second.proposal()
    if packet!=p or packet!=q or digest(original)!=bp['original_envelope_sha256']:
        raise ValueError('Stoica source or original changed')
    for module,value,proof in ((first,baseline,bp),(second,fixed,fp)):
        if json.loads((module.OUT/'proposal.json').read_text())!=value or json.loads((module.OUT/'provenance.json').read_text())!=proof:
            raise ValueError('Stoica saved proposal changed')
        if json.loads((module.OUT/'plan.json').read_text())!=module.prepare(write=False)[2]:
            raise ValueError('Stoica review plan changed')
    rr=[];lr=[]
    def collect(kind,ps,module,system,validator):
        key='event_id' if kind=='records' else 'decision_id'
        for p in ps:
            value,proof=verify(module.OUT/kind,p,system=system,validator=validator)
            decisions={d[key]:d for d in value['decisions']}
            for candidate in p['candidates']:
                (rr if kind=='records' else lr).append((candidate,decisions[candidate[key]],proof))
    records,_,_=first.prepare(write=False)
    collect('records',records,first,first.SYSTEM,first.run.previous.review.validate_review)
    records,ledger,_=second.prepare(write=False)
    collect('records',records,second,second.SYSTEM,first.run.previous.review.validate_review)
    collect('ledger',ledger,second,second.LEDGER_SYSTEM,first.ledger.validate)
    targets={e['event_id']:e for e in fixed['records']['events']}
    decisions={d['decision_id']:d for p in ledger for d in p['candidates']}
    if len(targets)!=15 or len(decisions)!=27:raise ValueError('Stoica population changed')
    record_cover=select_supported(targets,rr,key='event_id')
    ledger_cover=select_supported(decisions,lr,key='decision_id')
    first.run.validate(fixed,packet)
    return fixed,dict(repair=bp,delta=fp,record_approvals=record_cover,lineage_approvals=ledger_cover,
        approved_records=15,approved_lineage_items=27,qualified=False,gold_accepted=False)
