"""One inspected development proposal; never a generic normalizer or approval."""
from .signal_desk_full_event_v4_repair import propose
from .signal_desk_rubric_reference_packets import digest


def prepare():
    import json
    from scripts import pif_signal_desk_coding_tools_held_review as review
    status=review.status()
    if not status['all_reviews_verified']:raise ValueError('full independent diagnosis missing')
    d=review.run.OUT/'calls'/review.WID/'A'
    plan=json.loads((review.run.OUT/'plan.json').read_text())
    source_packet=json.loads((review.run.previous.BASE/f"{plan['source_packets'][review.WID]}.packet.json").read_text())
    p=review.run.packet(source_packet,'A',{});review.run.verify_provider(d,p)
    original=json.loads((d/f'{review.PACKET}.output.json').read_text());source=p['transcript_window']
    review.inspected_invalid_input(original,source=source,window_id=review.WID)
    changes=[]
    def get(path):
        v=original
        for key in path:v=v[key]
        return v
    def change(path,after,reason):
        before=get(path)
        if before!=after:changes.append({'path':path,'before':before,'after':after,'reason':reason})
    # Each start is explicitly selected from the inspected full source. Repeated
    # Anthropic/obviously refer to these exact contextual occurrences, not first hits.
    spans=[(0,['attitude','target'],217),(1,['position','source_evidence',0],973),
        (1,['attitude','target'],933),(1,['context_evidence',0],817),
        (4,['attitude','evaluation_evidence',0],1603),(6,['position','source_evidence',0],2216),
        (7,['attitude','evaluation_evidence',0],2701),(7,['context_evidence',0],2382),
        (9,['position','source_evidence',0],3096),(9,['position','source_evidence',1],3258),
        (9,['attitude','target'],3123),(9,['attitude','modality_evidence',1],3157),
        (12,['position','source_evidence',0],4335),(12,['attitude','modality_evidence',0],4340),
        (12,['attitude','target_components',0,'evaluation_evidence',0],4350),
        (13,['position','source_evidence',0],4667),(13,['context_evidence',0],4485),
        (14,['position','source_evidence',1],5077)]
    for i,tail,start in spans:
        path=['events',i]+tail;span=get(path);end=start+len(span['text'])
        if source[start:end]!=span['text']:raise ValueError('inspected exact span changed')
        for field,value in [('start',start),('end',end)]:
            change(path+[field],value,'Explicit source-inspected numeric correction; same quoted text, speaker and contextual occurrence.')
    change(['events',6,'claim_text'],original['events'][6]['claim_text'].replace('an IDE-only form factor','the transcribed “ID form factor”'),
           'Independent diagnosis requires preserving supplied ASR wording, not external normalization.')
    change(['events',7,'attitude','target'],None,'Garbled forecast object does not establish a reliable evaluative target.')
    change(['events',7,'attitude','attitude'],'indeterminate','Independent reviewer found no justified evaluative polarity; retain certain epistemic strength.')
    change(['events',7,'claim_text'],"In the AI coding-tool competition discussion, Swyx emphatically forecast greater intensity during the year, with the object transcribed as 'the worst' and unresolved.",
           'Retain contextual forecast while explicitly flagging the damaged ASR object.')
    change(['events',9,'claim_text'],"Shawn said his acquisition thesis had died, reporting that Cursor was 'rumored to be Anthropic speakers, customer now, something like that' and discussing alleged limits with Anthropic, while saying he did not know how true the claims or exact positioning were.",
           'Remove unsupported after chronology; preserve supplied garbled surface, rumor and uncertainty without external repair.')
    for i in (0,1,11,13,14):
        change(['events',i,'evidence_role','needs'],'none',
               'Proposed interpretation: claim is intelligible from supplied context; retain uncertainty and qualifiers. Independent semantic approval required.')
    for i in (8,9):
        change(['events',i,'evidence_role','role'],'research_limitation',
               'Proposed recovery disposition: unresolved acquisition/relationship wording requires source or audio; preserve candidate and do not invent names.')
        change(['events',i,'publishability_state'],'quarantined',
               'Keep unresolved recovery candidate in the denominator, excluded from publication pending source repair. Not accepted gold.')
    for i in (4,5,6,7):
        for owner in ('transcript_voice','proposition_owner'):
            path=['events',i,'attribution',owner,'binding_span']
            if get(path)!={'text':'Swyx:','start':1582,'end':1587} or source[1310:1315]!='Swyx:':
                raise ValueError('inspected repeated speaker label changed')
            change(path,{'text':'Swyx:','start':1310,'end':1315},
                   'Align attribution to its existing same-label voice-binding anchor within the unchanged corridor; no name inference or corridor expansion. Explicit independent review required.')
    fixed,proof=propose(original,source=source,window_id=review.WID,expected_original_sha256=review.RAW,
                        replacements=changes,output_validator=review.run.previous.contract.validate)
    proof['independent_diagnosis_sha256']=digest(status)
    return p,fixed,proof
