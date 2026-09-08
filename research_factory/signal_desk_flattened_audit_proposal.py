"""Explicit flattened AUDIT proposal; preserve all records and unknown voices."""
import json
from .signal_desk_rubric_reference_packets import digest
from .signal_desk_full_event_v4_repair import propose


def prepare():
    from scripts import pif_signal_desk_lineage_qualification as run
    wid='sdw_f367e118b794f35d05f9';d=run.OUT/'calls'/wid/'AUDIT'
    plan=json.loads((run.OUT/'plan.json').read_text())
    source=json.loads((run.previous.BASE/f"{plan['source_packets'][wid]}.packet.json").read_text())
    p=run.packet(source,'AUDIT',{});run.verify_provider(d,p)
    raw=json.loads((d/f"{p['packet_sha256']}.output.json").read_text())
    if p['packet_sha256']!='7f09b270440fb1ff94fae222430e13f96e4005e7f01647178dba79d15eda64be' or digest(raw)!='22020ef217ca09a8a4d27349ef98189daea2a81fd0db1fab0fe2dd8eb2a17b10':
        raise ValueError('flattened AUDIT request changed')
    changes=[]
    for i in [0,1,2,4,5,6,7,8,9,10,11,12,13,14,15,16]:
        e=raw['events'][i]
        if e['attribution']['transcript_voice'] is not None:
            raise ValueError('unknown voice changed')
        changes.append(dict(path=['events',i,'evidence_role','needs'],before=e['evidence_role']['needs'],after='none',
            reason='The supplied text states a readable claim or example. Missing speaker identity or independent external verification does not make the proposition unrecoverable. Preserve all attribution and publication uncertainty.'))
    for path,start,before in [(['events',8,'attitude','evaluation_evidence',0],2774,2805),
                              (['voice_bindings',0,'source_kind_evidence',0],2454,2500)]:
        span=raw
        for key in path:span=span[key]
        end=start+len(span['text'])
        if span['start']!=start or p['transcript_window'].count(span['text'])!=1 or p['transcript_window'][start:end]!=span['text']:
            raise ValueError('inspected exact span changed')
        changes.append(dict(path=path+['end'],before=before,after=end,
            reason='Correct only the end offset of the unique verbatim source substring; preserve the quoted text.'))
    for i,start,old_start,old_end in [(8,2575,2578,2773),(15,5437,5444,5584)]:
        span=raw['events'][i]['context_evidence'][0]
        end=start+len(span['text'])
        if p['transcript_window'].count(span['text'])!=1 or p['transcript_window'][start:end]!=span['text']:
            raise ValueError('inspected antecedent changed')
        for key,before,after in [('start',old_start,start),('end',old_end,end)]:
            if before!=after:
                changes.append(dict(path=['events',i,'context_evidence',0,key],before=before,after=after,
                    reason='Bind the unchanged antecedent quotation to its inspected unique source location.'))
    fixed,proof=propose(raw,source=p['transcript_window'],window_id=wid,expected_original_sha256=digest(raw),
        replacements=changes,output_validator=run.previous.contract.validate)
    if len(fixed['events'])!=17:raise ValueError('AUDIT population changed')
    proof.update(source_packet_sha256=p['packet_sha256'],independent_full_record_review_required=True)
    return fixed,proof,p
