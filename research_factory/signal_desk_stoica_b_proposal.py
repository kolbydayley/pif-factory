"""Explicit source-need proposal for B; A approvals do not transfer."""
import json
from .signal_desk_rubric_reference_packets import digest
from .signal_desk_full_event_v4_repair import propose


def prepare():
    from scripts import pif_signal_desk_lineage_qualification as run
    wid='sdw_ff2331e598d948e07cc9';d=run.OUT/'calls'/wid/'B'
    plan=json.loads((run.OUT/'plan.json').read_text())
    source=json.loads((run.previous.BASE/f"{plan['source_packets'][wid]}.packet.json").read_text())
    p=run.packet(source,'B',{});run.verify_provider(d,p)
    raw=json.loads((d/f"{p['packet_sha256']}.output.json").read_text())
    if (p['packet_sha256']!='e3297d6ad2771a866060114bd0456ea38e2b8af79d0bc6d9aa41db30d6e9b863'
        or digest(raw)!='ace3e3de973153f48840e38c7630a54b73b87701ff8df0554daf0a9ce0271d93'):
        raise ValueError('Stoica B original changed')
    changes=[]
    for i in range(3):
        e=raw['events'][i]
        if e['attribution']['transcript_voice'] is not None or e['publishability_state']!='uncertain':
            raise ValueError('opening voice/uncertainty changed')
        changes.append(dict(path=['events',i,'evidence_role','needs'],before=e['evidence_role']['needs'],after='none',
            reason='The source expresses a readable proposition; unknown speaker stays null and publication remains uncertain. No missing text is needed to recover the claim meaning.'))
    span=raw['events'][5]['position']['source_evidence'][0]
    text=span['text'];start=3110;end=start+len(text)
    if span['start']!=start or span['end']!=3334 or p['transcript_window'].count(text)!=1 or p['transcript_window'][start:end]!=text:
        raise ValueError('historical partnership evidence changed')
    changes.append(dict(path=['events',5,'position','source_evidence',0,'end'],before=3334,after=end,
                        reason='Inspected unique verbatim source span; correct only the end offset from 3334 to 3341.'))
    fixed,proof=propose(raw,source=p['transcript_window'],window_id=wid,expected_original_sha256=digest(raw),
                        replacements=changes,output_validator=run.previous.contract.validate)
    if len(fixed['events'])!=12:raise ValueError('B population changed')
    proof.update(source_packet_sha256=p['packet_sha256'],independent_full_record_review_required=True)
    return fixed,proof,p
