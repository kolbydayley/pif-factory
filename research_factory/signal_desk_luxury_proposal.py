"""Unapproved source-bound luxury interview repair; no invented currency or voice."""
import json
from .signal_desk_rubric_reference_packets import digest
from .signal_desk_full_event_v4_repair import propose


def prepare():
    from scripts import pif_signal_desk_lineage_qualification as run
    wid='sdw_36c333137a506679078a';d=run.OUT/'calls'/wid/'A'
    plan=json.loads((run.OUT/'plan.json').read_text())
    source=json.loads((run.previous.BASE/f"{plan['source_packets'][wid]}.packet.json").read_text())
    p=run.packet(source,'A',{});run.verify_provider(d,p)
    raw=json.loads((d/f"{p['packet_sha256']}.output.json").read_text());s=p['transcript_window']
    if p['packet_sha256']!='295b3b15133f74138eeb4cbe48ed008ec27ee086b07f38acb4226b3047ee6bbe' or digest(raw)!='ea2bbe02c4b12d06b1c2efbe385b9e2757968b533d3b4e9db75ad1088106195c':
        raise ValueError('luxury original changed')
    changes=[]
    def change(path,after,reason):
        before=raw
        for key in path:before=before[key]
        if before!=after:changes.append(dict(path=path,before=before,after=after,reason=reason))
    for i,e in enumerate(raw['events']):
        if e['attribution']['transcript_voice'] is not None:raise ValueError('flattened speaker changed')
        change(['events',i,'publishability_state'],'quarantined' if i==12 else 'uncertain',
               'Unidentified source voices remain explicitly uncertain; garbled mechanism remains quarantined.')
        if i!=12:
            change(['events',i,'evidence_role','needs'],'none','Readable proposition despite unknown voice; do not infer an identity.')
    change(['events',8,'claim_text'],"Scale improves luxury-brand margins: Gucci's margins were higher at greater size, reaching 40% in one year and operating around 35%–40% when revenue was roughly 8 billion–10 billion; the source does not specify a currency.",
           'Remove unsupported dollar notation while preserving the source numerical range.')
    change(['events',10,'attribution','proposition_owner'],None,'Generic consumers are not an identified person or securely bound individual speaker.')
    change(['events',12,'evidence_role','role'],'research_limitation','The word rues in the decline comparison is potentially garbled; do not silently replace it with a technical term.')
    change(['events',12,'evidence_role','decision'],'boundary','Retain the source ambiguity as an explicit recovery item.')
    change(['events',12,'claim_text'],"The narrator describes reverse operating gearing with a retained cost base and brand investment, then says 'when rues decline 25% profits are down 50%'; the unclear word prevents treating this as a clean quantified revenue-to-profit relationship.",
           'Preserve literal uncertain wording and explain the limitation instead of normalizing it.')
    change(['events',12,'evidence_role','rationale'],'The source describes reverse operating gearing but the key decline term is transcribed as rues. Audio or source clarification is required before publishing the quantified relationship.',
           'Make the specific missing evidence explicit.')
    for i,start,end in [(3,1398,1410),(12,4667,4703)]:
        path=['events',i,'attitude','modality_evidence',0];span=raw['events'][i]['attitude']['modality_evidence'][0]
        if s[start:end]!=span['text']:raise ValueError('inspected offset changed')
        change(path+['start'],start,'Numeric-only correction to inspected exact source quote.')
        change(path+['end'],end,'Numeric-only correction to inspected exact source quote.')
    fixed,proof=propose(raw,source=s,window_id=wid,expected_original_sha256=digest(raw),
                        replacements=changes,output_validator=run.previous.contract.validate)
    proof.update(source_packet_sha256=p['packet_sha256'],independent_full_record_review_required=True)
    return fixed,proof,p
