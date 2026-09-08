"""Explicit source-grounded flattened B corrections, never inferred speakers."""
import json
from .signal_desk_rubric_reference_packets import digest
from .signal_desk_full_event_v4_repair import propose


def prepare():
    from scripts import pif_signal_desk_lineage_qualification as run
    wid='sdw_f367e118b794f35d05f9';d=run.OUT/'calls'/wid/'B'
    plan=json.loads((run.OUT/'plan.json').read_text())
    source=json.loads((run.previous.BASE/f"{plan['source_packets'][wid]}.packet.json").read_text())
    p=run.packet(source,'B',{});run.verify_provider(d,p)
    raw=json.loads((d/f"{p['packet_sha256']}.output.json").read_text())
    if p['packet_sha256']!='4943cc28e196b3a9bd70e9ad6332ebe5859ee25be1c853216f7267fa5581e838' or digest(raw)!='c8fd86558fe36261c50ca2214330e8e4c24e286ad2cade3983783946e2d911bd':
        raise ValueError('flattened B request changed')
    changes=[]
    def change(path,after,reason):
        before=raw
        for key in path:before=before[key]
        changes.append(dict(path=path,before=before,after=after,reason=reason))
    change(['events',5,'attitude','evaluation_evidence',0,'start'],2148,'Exclude the preceding newline while preserving the exact quotation.')
    s=p['transcript_window'];text='these techniques'
    start=s.index(text,2148,2299)
    change(['events',5,'attitude','target'],dict(text=text,start=start,end=start+len(text)),
        'The source explicitly evaluates the medical impact of these techniques, not the literature search product.')
    change(['events',5,'attitude','status'],'proposed','The evaluated techniques have an exact source target.')
    change(['events',5,'attitude','rationale'],'The speaker positively evaluates the medical impact of these techniques; the literature examples support that assessment without proving patient outcomes.',
        'Replace the unsupported assertion that no compact source target exists.')
    change(['events',5,'attribution','mentioned_entities'],[], 'Google Scholar is a search product, not an organization; retain its name in the evidence and claim.')
    change(['events',10,'claim_text'],'The speaker says that, at the moment, the only AI that works well is behind the screen, is good for "desktop to desktop workers," and is not really for people working in the physical world.',
        'Preserve the source only boundary and awkward desktop phrase rather than weakening or silently normalizing it.')
    change(['events',12,'claim_text'],'The speaker says their group founded NNAISENSE, described as an "eye company for the physical world," using the repeated date wording "in 2014 and 2014," and suggests it may have been ahead of its time because the real world is extremely challenging.',
        'Preserve ASR surface uncertainty instead of silently correcting eye company to AI company.')
    fixed,proof=propose(raw,source=s,window_id=wid,expected_original_sha256=digest(raw),
        replacements=changes,output_validator=run.previous.contract.validate)
    return fixed,proof,p
