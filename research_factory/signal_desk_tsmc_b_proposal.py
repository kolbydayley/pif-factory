"""Source-inspected B proposal; must receive its own independent approval."""
import json
from .signal_desk_rubric_reference_packets import digest
from .signal_desk_full_event_v4_repair import propose


def prepare():
    from scripts import pif_signal_desk_lineage_qualification as run
    wid='sdw_4acaaec65ea71658126b';d=run.OUT/'calls'/wid/'B'
    plan=json.loads((run.OUT/'plan.json').read_text())
    source=json.loads((run.previous.BASE/f"{plan['source_packets'][wid]}.packet.json").read_text())
    p=run.packet(source,'B',{});run.verify_provider(d,p)
    raw=json.loads((d/f"{p['packet_sha256']}.output.json").read_text())
    if p['packet_sha256']!='a0f799f287c2b2f922173cca00894dee190d38c729c542f19faac1bae4cb8438' or digest(raw)!='bd67078f3dea1f71439fa26b4b858b66851244542a6e2c310f8847ebca62c8fe':
        raise ValueError('TSMC B source response changed')
    changes=[]
    def change(path,after,reason):
        before=raw
        for key in path:before=before[key]
        changes.append({'path':path,'before':before,'after':after,'reason':reason})
    for i in range(3):
        change(['events',i,'evidence_role','needs'],'none','Readable content; unknown opening voice remains unresolved and uncertain.')
        change(['events',i,'evidence_role','scope'],'not_applicable','No source-backed affiliation establishes the unnamed speaker as an interested party.')
    change(['events',3,'evidence_role','scope'],'attributed_view','Morris reports an executive role; the excerpt does not establish firsthand observation.')
    change(['events',8,'context_evidence',0,'purpose'],'antecedent','David states a concern about arbitration; this is not an interrogative question.')
    change(['events',10,'claim_text'],'Ben describes the dispute resolution as successful: the companies had a longstanding personal partnership, reached a settlement exceeding $100 million, and subsequently did many billions of dollars of business together.',
           'Describe the linked events without asserting that the relationship caused the settlement.')
    change(['events',10,'evidence_role','rationale'],'Ben offers a positive interpretation linking the partnership, settlement and later commercial activity; this does not establish a causal mechanism.',
           'Align rationale with the bounded source-supported interpretation.')
    for parts,start in [(['events',2,'attitude','target'],581),(['events',2,'attitude','evaluation_evidence',0],596),(['events',12,'attitude','evaluation_evidence',1],5966)]:
        span=raw
        for key in parts:span=span[key]
        if p['transcript_window'][start:start+len(span['text'])]!=span['text']:raise ValueError('inspected occurrence changed')
        for field,after in [('start',start),('end',start+len(span['text']))]:
            if span[field]!=after:change(parts+[field],after,'Exact inspected occurrence; quoted text unchanged.')
    fixed,proof=propose(raw,source=p['transcript_window'],window_id=wid,expected_original_sha256=digest(raw),
        replacements=changes,output_validator=run.previous.contract.validate)
    if len(fixed['events'])!=13 or fixed['voice_bindings']!=raw['voice_bindings']:raise ValueError('B population or voices changed')
    proof.update(source_packet_sha256=p['packet_sha256'],independent_b_review_required=True)
    return fixed,proof,p
