"""Full-source audit proposal; no change is independently approved yet."""
import json
from .signal_desk_september8_offsets import PENDING_OFFSETS
from .signal_desk_rubric_reference_packets import digest
from .signal_desk_full_event_v4_repair import propose


def prepare():
    from scripts import pif_signal_desk_lineage_qualification as run
    spec=PENDING_OFFSETS['ai-governance-audit'];wid=spec['window'];d=run.OUT/'calls'/wid/'AUDIT'
    plan=json.loads((run.OUT/'plan.json').read_text())
    source=json.loads((run.previous.BASE/f"{plan['source_packets'][wid]}.packet.json").read_text())
    p=run.packet(source,'AUDIT',{});run.verify_provider(d,p)
    raw=json.loads((d/f"{p['packet_sha256']}.output.json").read_text())
    if p['packet_sha256']!=spec['packet'] or digest(raw)!=spec['raw'] or digest(p['transcript_window'])!=spec['source']:
        raise ValueError('audit source response changed')
    changes=[]
    def change(path,after,reason):
        before=raw
        for key in path:before=before[key]
        if before!=after:changes.append(dict(path=path,before=before,after=after,reason=reason))
    for index,start in spec['main_spans']:
        e=raw['events'][index];end=start+len(e['evidence_text'])
        if p['transcript_window'][start:end]!=e['evidence_text']:raise ValueError('main quote changed')
        change(['events',index,'evidence_start'],start,'Exact inspected main quote, numeric only.')
        change(['events',index,'evidence_end'],end,'Exact inspected main quote, numeric only.')
    for index,parts,start in spec['spans']:
        span=raw['events'][index]
        for key in parts:span=span[key]
        end=start+len(span['text'])
        if p['transcript_window'][start:end]!=span['text']:raise ValueError('nested occurrence changed')
        change(['events',index]+parts+['start'],start,'Exact individually inspected occurrence, unchanged text.')
        change(['events',index]+parts+['end'],end,'Exact individually inspected occurrence, unchanged text.')
    for index in range(1,15):
        change(['events',index,'evidence_role','needs'],'none','Content is readable; unresolved narrator remains null and uncertain, not a missing-source claim.')
        if index!=14:
            change(['events',index,'evidence_role','scope'],'not_applicable','Unknown narrator assessment, analogy or forecast does not establish firsthand factual knowledge or a named attributed viewpoint.')
    change(['events',5,'evidence_role','role'],'supporting_context','An anticipated objection is context for the response, not an observed opposing factual claim.')
    change(['events',5,'evidence_role','context_for'],[raw['events'][6]['event_id']],'Link anticipated objection to the subsequent specific-end-use regulatory response.')
    change(['events',5,'evidence_role','context_parent_status'],'linked','Retain explicit supporting-context linkage.')
    fixed,proof=propose(raw,source=p['transcript_window'],window_id=wid,expected_original_sha256=spec['raw'],
        replacements=changes,output_validator=run.previous.contract.validate)
    if len(fixed['events'])!=15 or fixed['voice_bindings']!=raw['voice_bindings']:raise ValueError('population/voice changed')
    proof.update(source_packet_sha256=p['packet_sha256'],independent_full_source_review_required=True,
                 no_author_answers_supplied=True)
    return fixed,proof,p
