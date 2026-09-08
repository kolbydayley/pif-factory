"""Preserve unrecoverable context and unknown voices in the flattened interview."""
import json
from .signal_desk_rubric_reference_packets import digest
from .signal_desk_full_event_v4_repair import propose


def prepare():
    from scripts import pif_signal_desk_lineage_qualification as run
    wid='sdw_f367e118b794f35d05f9';d=run.OUT/'calls'/wid/'A'
    plan=json.loads((run.OUT/'plan.json').read_text())
    source=json.loads((run.previous.BASE/f"{plan['source_packets'][wid]}.packet.json").read_text())
    p=run.packet(source,'A',{});run.verify_provider(d,p)
    raw=json.loads((d/f"{p['packet_sha256']}.output.json").read_text());s=p['transcript_window']
    if p['packet_sha256']!='c1abaf818d04f1e880c6033c15e0d2717d24e3493e3cf8160989e1d1f91edfbd' or digest(raw)!='6f9445939f9c153d3e07882f2262d5874daa07abe4d0319c69b2cb7568a58d00':
        raise ValueError('flattened interview original changed')
    changes=[]
    def change(path,after,reason):
        before=raw
        for key in path:before=before[key]
        if before!=after:changes.append(dict(path=path,before=before,after=after,reason=reason))
    for i in (1,2,3,5,6,7,9,10,11,12,13,15,16,17,19,20,21,22,23,24,26):
        if raw['events'][i]['attribution']['transcript_voice'] is not None:
            raise ValueError('unlabeled voice changed')
        change(['events',i,'evidence_role','needs'],'none','Readable source proposition; speaker remains null and uncertain. No outside identity inferred.')
    # The predictive-coding storage sentence is internally questionable. Keep
    # its verbatim wording, but do not publish it as a reliable mechanism.
    change(['events',25,'evidence_role','role'],'research_limitation','Potentially garbled storage explanation requires audio/source clarification.')
    change(['events',25,'evidence_role','decision'],'boundary','Mark unresolved source meaning as a limitation, not a publishable claim.')
    change(['events',25,'publishability_state'],'quarantined','Preserve the questionable text without turning it into accepted technical evidence.')
    offsets=[
        (['events',1,'attitude','target'],396),
        (['events',4,'attribution','proposition_owner','binding_span'],1018),
        (['events',4,'position','source_evidence',0],1053),
        (['events',15,'attitude','target_components',0,'target'],3474),
        (['events',21,'attitude','modality_evidence',0],4810),
        (['events',23,'attitude','modality_evidence',0],5109),
        (['events',25,'attitude','target'],5739),
        (['events',25,'attitude','modality_evidence',0],5783),
    ]
    for path,start in offsets:
        span=raw
        for key in path:span=span[key]
        end=start+len(span['text'])
        if s[start:end]!=span['text']:raise ValueError('inspected source occurrence changed')
        change(path+['start'],start,'Correct numeric offset to inspected verbatim occurrence; text unchanged.')
        change(path+['end'],end,'Correct numeric offset to inspected verbatim occurrence; text unchanged.')
    fixed,proof=propose(raw,source=s,window_id=wid,expected_original_sha256=digest(raw),
                        replacements=changes,output_validator=run.previous.contract.validate)
    if len(fixed['events'])!=27:raise ValueError('flattened population changed')
    proof.update(source_packet_sha256=p['packet_sha256'],independent_full_record_review_required=True,
                 unresolved_source_events=[raw['events'][i]['event_id'] for i in (4,25)])
    return fixed,proof,p
