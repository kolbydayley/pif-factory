"""Only source-inspected, hash-registered nested-offset cases are recoverable."""
from .signal_desk_full_event_v4_repair import propose
from .signal_desk_full_event_v5 import validate
from .signal_desk_rubric_reference_packets import digest

REGISTRY={
    '685858df0a7a9f4a4186803db8718b389300a408d26d04f58931d0ec49f68c30':{
        'window_id':'sdw_c38394ac01937fde7f33','role':'B','records':8,'spans':6,'max_displacement':8,
        'packet_sha256':'5d9f431ad2eafa165b663d045992940c00b14de67d5484ac50110fb96a323665',
        'source_sha256':'733c4e5c8061e3140e0bf2f93924e8571323ebe623d8155ea75e0eb12e558c65'},
    '3dac56cb685ca2fbd4e931fa4b4df6e6af21f9d7e78501b6fa224eb5f0200f34':{
        'window_id':'sdw_777d46db3fa4c592b71e','role':'C','records':20,'spans':2,'max_displacement':6,
        'packet_sha256':'9d3d8e3a5692c27bbbc3dbf2226fe127435afc80ccf7a432cc5d42f2a55e35dd',
        'source_sha256':'947457b010e805557930b6a88bbfbbbbea7c06a8e9cee27349cc10e5e44138c7'},
}


def recover(original,packet):
    key=digest(original);entry=REGISTRY.get(key);source=packet['transcript_window']
    if entry is None or any(packet.get(k)!=entry[k] for k in ['window_id','role','packet_sha256']) or digest(source)!=entry['source_sha256'] or len(original['events'])!=entry['records']:
        raise ValueError('not a registered source-inspected offset case')
    changes=[];spans=0
    def walk(value,path):
        nonlocal spans
        if isinstance(value,dict):
            if {'text','start','end'}<=set(value):
                text,start,end=value['text'],value['start'],value['end']
                if not isinstance(text,str) or type(start)is not int or type(end)is not int:raise ValueError('not a textual integer-offset span')
                if not (0<=start<end<=len(source) and source[start:end]==text):
                    first=source.find(text)
                    if not text or first<0 or source.find(text,first+1)>=0:raise ValueError('missing or ambiguous exact source text')
                    last=first+len(text)
                    if max(abs(first-start),abs(last-end))>entry['max_displacement']:raise ValueError('outside inspected offset bounds')
                    spans+=1
                    for field,before,after in [('start',start,first),('end',end,last)]:
                        if before!=after:changes.append({'path':path+[field],'before':before,'after':after,
                            'reason':'Hash-registered, source-inspected unique exact text; numeric offsets only, all words and semantic fields preserved.'})
            for name,item in value.items():walk(item,path+[name])
        elif isinstance(value,list):
            for i,item in enumerate(value):walk(item,path+[i])
    walk(original,[])
    if spans!=entry['spans']:raise ValueError('inspected error population changed')
    fixed,receipt=propose(original,source=source,window_id=packet['window_id'],expected_original_sha256=key,
        replacements=changes,output_validator=validate)
    receipt.update(repair='v5-hash-registered-nested-offsets-v1',registry_entry_sha256=digest(entry),
        repaired_span_count=spans,semantic_fields_changed=False,original_failure_preserved=True)
    return fixed,receipt
