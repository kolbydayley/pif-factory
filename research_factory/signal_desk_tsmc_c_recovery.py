"""Require a complete exact-content approval cover for C records and lineage."""
import json
from .signal_desk_rubric_reference_packets import digest
from .signal_desk_actual_review_receipt import verify


def select_supported(targets, reviewed, *, key):
    selected={}
    for candidate,decision,receipt in reviewed:
        identity=candidate[key]
        if identity in targets and candidate==targets[identity] and decision['verdict']=='supported':
            selected[identity]=receipt
    if set(selected)!=set(targets):
        raise ValueError('incomplete exact-content approval coverage')
    return selected


def recover(original, packet):
    from scripts import pif_signal_desk_tsmc_c_review as first
    from scripts import pif_signal_desk_tsmc_c_delta_review as second
    from scripts import pif_signal_desk_tsmc_c_ledger_retry as third
    fixed,provenance,p=second.proposal()
    baseline,baseproof,_=first.proposal()
    if packet!=p or digest(original)!=baseproof['original_envelope_sha256']:
        raise ValueError('C original packet/output changed')
    for module,value,proof in [(first,baseline,baseproof),(second,fixed,provenance)]:
        if (json.loads((module.OUT/'proposal.json').read_text())!=value or
            json.loads((module.OUT/'provenance.json').read_text())!=proof):
            raise ValueError('C saved proposal/provenance changed')
    r,l,_=first.prepare(write=False);delta,_=second.prepare(write=False)
    reviewed={'records':[],'ledger':[]}
    def collect(kind,ps,directory,system,validator,exclude):
        for q in ps:
            # Historical invalid packets remain failures; superseding reviews
            # must cover their exact candidate identities/content below.
            if q['packet_sha256'] in exclude:continue
            value,receipt=verify(directory,q,system=system,validator=validator)
            decisions={d['event_id' if kind=='records' else 'decision_id']:d for d in value['decisions']}
            for candidate in q['candidates']:
                identity=candidate['event_id' if kind=='records' else 'decision_id']
                reviewed[kind].append((candidate,decisions[identity],receipt))
    collect('records',r,first.OUT/'records',first.SYSTEM,first.run.previous.review.validate_review,set())
    collect('ledger',l,first.OUT/'ledger',first.ledger.SYSTEM,first.ledger.validate,
            {'71f9777c7df0599c420506e563a4a938318783f7dda8b7f35dcb4604dd2bc79e',
             'bb397f792ce1cd164443c804a2ad29f4d837c8e9c89d95fee0b06cfaba3551da'})
    collect('records',delta['records'],second.OUT/'records',second.RECORD_SYSTEM,
            first.run.previous.review.validate_review,set())
    collect('ledger',delta['ledger'],second.OUT/'ledger',second.LEDGER_SYSTEM,first.ledger.validate,{third.FAILED})
    collect('ledger',third.prepare(write=False),third.OUT,third.SYSTEM,first.ledger.validate,set())
    import tiktoken
    enc=tiktoken.get_encoding('o200k_base')
    final_ledger=first.ledger.packets(fixed,p,token_count=lambda s:len(enc.encode(s)))
    records={e['event_id']:e for e in fixed['records']['events']}
    lineage={c['decision_id']:c for q in final_ledger for c in q['candidates']}
    if len(records)!=15 or len(lineage)!=29:raise ValueError('C full population changed')
    record_cover=select_supported(records,reviewed['records'],key='event_id')
    ledger_cover=select_supported(lineage,reviewed['ledger'],key='decision_id')
    first.run.validate(fixed,p)
    return fixed,dict(repair=provenance,record_approvals=record_cover,lineage_approvals=ledger_cover,
                      approved_records=15,approved_lineage_items=29,qualified=False,gold_accepted=False)
