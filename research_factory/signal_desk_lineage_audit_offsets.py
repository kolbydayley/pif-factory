"""Nineteen inspected numeric corrections for one immutable development audit."""
from .signal_desk_rubric_reference_packets import digest
from .signal_desk_full_event_v4_repair import propose
from .signal_desk_full_event_v5 import validate

WID='sdw_c38394ac01937fde7f33'
RAW='59f2462475a864c6743ad123b75e9b842ac1b573fde1471ddd4666a0d2f47a43'
PACKET='3008e54af99225636a0bda589056078c4330beb688c6747433496916db16103a'
SOURCE='733c4e5c8061e3140e0bf2f93924e8571323ebe623d8155ea75e0eb12e558c65'
# Repeated phrases are explicitly anchored to their source-inspected occurrence,
# never selected by first-match or a generic nearest-match repair algorithm.
SPANS=[
    (0,['position','source_evidence',1],297,520,297),
    (1,['position','source_evidence',0],823,871,823),
    (1,['attitude','evaluation_evidence',0],826,859,826),
    (3,['position','source_evidence',0],2595,2695,2598),
    (3,['attitude','target'],2555,2579,2546),
    (4,['position','source_evidence',1],3246,3382,3246),
    (4,['attitude','target'],3141,3211,3138),
    (5,['position','source_evidence',0],4376,4533,4375),
    (5,['position','source_evidence',1],4580,4652,4581),
    (5,['attitude','target'],4449,4458,4441),
    (5,['attitude','evaluation_evidence',0],4413,4469,4409),
    (5,['attitude','evaluation_evidence',1],4781,4831,4777),
    (5,['attitude','modality_evidence',0],4376,4383,4375),
    (5,['attitude','modality_evidence',2],4580,4588,4581),
    (5,['attitude','modality_evidence',3],4725,4733,4714),
    (7,['position','source_evidence',0],4059,4104,4059),
    (7,['attitude','evaluation_evidence',0],4065,4103,4065),
    (8,['attitude','target'],5161,5166,5156),
    (8,['attitude','evaluation_evidence',1],5245,5261,5251),
]


def recover(original,packet):
    source=packet['transcript_window']
    if (packet['window_id']!=WID or packet['role']!='AUDIT' or packet['packet_sha256']!=PACKET or
        digest({k:v for k,v in packet.items() if k!='packet_sha256'})!=PACKET or digest(original)!=RAW or digest(source)!=SOURCE):
        raise ValueError('not the inspected audit/source/request')
    if len(original['events'])!=9:raise ValueError('changed audit population')
    changes=[]
    for index,parts,start,end,new_start in SPANS:
        path=['events',index]+parts;span=original
        for key in path:span=span[key]
        if set(span)!={'text','start','end'} or span['start']!=start or span['end']!=end:raise ValueError('inspected span changed')
        new_end=new_start+len(span['text'])
        if source[new_start:new_end]!=span['text']:raise ValueError('inspected source occurrence changed')
        for field,before,after in [('start',start,new_start),('end',end,new_end)]:
            if before!=after:changes.append({'path':path+[field],'before':before,'after':after,
                'reason':'Source-inspected exact occurrence; numeric offset only, no change to quoted text, speaker or claim.'})
    fixed,proof=propose(original,source=source,window_id=WID,expected_original_sha256=RAW,replacements=changes,output_validator=validate)
    proof.update(repair='lineage-inspected-changelog-audit-offsets-v1',packet_sha256=PACKET,source_sha256=SOURCE,
                 inspected_spans=19,semantic_fields_changed=False,original_failure_preserved=True)
    return fixed,proof
