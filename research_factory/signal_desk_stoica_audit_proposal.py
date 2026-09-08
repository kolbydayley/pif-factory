"""Source-bound AUDIT proposal; previous role approvals do not transfer."""
import json
from .signal_desk_rubric_reference_packets import digest
from .signal_desk_full_event_v4_repair import propose


def prepare():
    from scripts import pif_signal_desk_lineage_qualification as run
    wid='sdw_ff2331e598d948e07cc9';d=run.OUT/'calls'/wid/'AUDIT'
    plan=json.loads((run.OUT/'plan.json').read_text())
    source=json.loads((run.previous.BASE/f"{plan['source_packets'][wid]}.packet.json").read_text())
    p=run.packet(source,'AUDIT',{});run.verify_provider(d,p)
    raw=json.loads((d/f"{p['packet_sha256']}.output.json").read_text())
    if (p['packet_sha256']!='57731b8cbb2ceb1b5b1296c4dd93346d6d6c1a2702f0147785ce5f8058644c17'
        or digest(raw)!='6dd0f5b7d525f36d84fc3fd0b46260476895f192dd5059430d25b1dc90e9c1b7'):
        raise ValueError('Stoica AUDIT original changed')
    changes=[]
    for i in range(4):
        e=raw['events'][i]
        if e['attribution']['transcript_voice'] is not None or e['publishability_state']!='uncertain':
            raise ValueError('opening voice/uncertainty changed')
        changes.append(dict(path=['events',i,'evidence_role','needs'],before='audio_or_source',after='none',
            reason='Readable substantive proposition with unresolved speaker, not missing claim meaning. Preserve null attribution and uncertain publication; do not infer the opening speaker from later labels.'))
    span=raw['events'][0]['position']['source_evidence'][0]
    end=319+len(span['text'])
    if p['transcript_window'].count(span['text'])!=1 or p['transcript_window'][319:end]!=span['text']:
        raise ValueError('opening infrastructure evidence changed')
    for key,before,after in [('start',328,319),('end',421,end)]:
        changes.append(dict(path=['events',0,'position','source_evidence',0,key],before=before,after=after,
            reason='Inspected unique verbatim infrastructure statement; correct numeric offsets only, preserving evidence text.'))
    fixed,proof=propose(raw,source=p['transcript_window'],window_id=wid,expected_original_sha256=digest(raw),
        replacements=changes,output_validator=run.previous.contract.validate)
    if len(fixed['events'])!=13:raise ValueError('AUDIT population changed')
    proof.update(source_packet_sha256=p['packet_sha256'],independent_full_record_review_required=True)
    return fixed,proof,p
