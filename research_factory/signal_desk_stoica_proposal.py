"""Explicit source-bound Stoica interview proposal, not accepted evidence."""
import json
from .signal_desk_rubric_reference_packets import digest
from .signal_desk_full_event_v4_repair import propose


def prepare():
    from scripts import pif_signal_desk_lineage_qualification as run
    wid='sdw_ff2331e598d948e07cc9';d=run.OUT/'calls'/wid/'A'
    plan=json.loads((run.OUT/'plan.json').read_text())
    source=json.loads((run.previous.BASE/f"{plan['source_packets'][wid]}.packet.json").read_text())
    p=run.packet(source,'A',{});run.verify_provider(d,p)
    raw=json.loads((d/f"{p['packet_sha256']}.output.json").read_text())
    if p['packet_sha256']!='34245f604f2b2daae78a2270a0610618bcb98a427dde60bbabaa28a83d00d1c1' or digest(raw)!='a6046a484073f7e1cc26bbd1276a84e5af091b3c8a2571ea01ccf997697451ad':
        raise ValueError('Stoica original source response changed')
    changes=[]
    def change(path,after,reason):
        before=raw
        for key in path:before=before[key]
        if before!=after:changes.append({'path':path,'before':before,'after':after,'reason':reason})
    for i in range(4):
        change(['events',i,'evidence_role','needs'],'none','Readable statement; speaker remains unresolved, not backfilled from later Ion label.')
        change(['events',i,'evidence_role','scope'],'not_applicable','Unknown opening speaker has no source-established firsthand basis.')
    for i in (7,8,9,10,11,13,14):
        change(['events',i,'evidence_role','scope'],'attributed_view','Opinion, recommendation, reported comparison or forecast is attributed to Ion, not established as firsthand observation.')
    for i,start in ((8,3855),(9,3981)):
        e=raw['events'][i];end=start+len(e['evidence_text'])
        if p['transcript_window'][start:end]!=e['evidence_text']:raise ValueError('inspected main quote changed')
        change(['events',i,'evidence_start'],start,'Exact unique unchanged quote start.')
        change(['events',i,'evidence_end'],end,'Exact unique unchanged quote end.')
        span=e['position']['source_evidence'][0]
        if span['text']!=e['evidence_text']:raise ValueError('position quote changed')
        change(['events',i,'position','source_evidence',0,'start'],start,'Same exact unique quote.')
        change(['events',i,'position','source_evidence',0,'end'],end,'Same exact unique quote.')
    span=raw['events'][11]['attitude']['target'];start=4764;end=start+len(span['text'])
    if p['transcript_window'][start:end]!=span['text']:raise ValueError('inspected target changed')
    change(['events',11,'attitude','target','start'],start,'Source-inspected new-model-architectures target.')
    change(['events',11,'attitude','target','end'],end,'Exact unchanged target end.')
    fixed,proof=propose(raw,source=p['transcript_window'],window_id=wid,expected_original_sha256=digest(raw),
        replacements=changes,output_validator=run.previous.contract.validate)
    if len(fixed['events'])!=15 or fixed['voice_bindings']!=raw['voice_bindings']:raise ValueError('population or voices changed')
    proof.update(source_packet_sha256=p['packet_sha256'],independent_full_source_review_required=True)
    return fixed,proof,p
