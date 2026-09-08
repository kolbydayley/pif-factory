"""Explicit inspected numeric offsets; preserves text, identities and failures."""
from .signal_desk_rubric_reference_packets import digest
from .signal_desk_full_event_v4_repair import propose
from .signal_desk_full_event_v5 import validate

CASES = {
 'ai-governance-audit': {
  'window':'sdw_3e13b01692fa8508d0cd','role':'AUDIT','records':15,'failure':'inexact record evidence',
  'packet':'d33707a17d59b205a590ffccfd95f08c10d854774528a20d30010e18f3a5c794',
  'raw':'daefd60bfc2c56d39682a4d167eeaab10b3a40d47c3f0e8e1b31610cd293c9f9',
  'source':'ff1118462633db2182c44578f897008db840ddce2aa1f6fec51824fd5ab37374',
  'main_spans':[(8,3354),(11,4266),(12,4464)],
  'spans':[(0,['position','source_evidence',0],216),(0,['attitude','target'],301),
           (1,['position','source_evidence',0],666),(1,['attitude','evaluation_evidence',0],666),
           (3,['position','source_evidence',0],1442),(8,['position','source_evidence',0],3354),
           (8,['attitude','target'],3354),(9,['position','source_evidence',0],3846),
           (11,['position','source_evidence',0],4266),(11,['position','source_evidence',1],4373),
           (11,['attitude','target'],4266),(11,['attitude','evaluation_evidence',0],4303),
           (12,['position','source_evidence',0],4584),(12,['position','source_evidence',1],4701),
           (12,['attitude','target'],4787),(12,['attitude','modality_evidence',0],4464),
           (14,['position','source_evidence',0],5305),(14,['position','source_evidence',1],5373),
           (14,['attitude','modality_evidence',1],5307)]},
 'ai-governance-c': {
  'window':'sdw_3e13b01692fa8508d0cd','role':'C','records':17,'failure':'inexact evidence',
  'packet':'aa90ed4187cdb1b78d8eabc60c78a1bd4695c69e78829ae4b1f15af3eff5dfb3',
  'raw':'2605323423ebb9ea1cb90b5270e8f8ee92c1930df3f93db4e979e40c8f8bfc95',
  'source':'ff1118462633db2182c44578f897008db840ddce2aa1f6fec51824fd5ab37374',
  'spans':[(3,['position','source_evidence',1],920),(7,['position','source_evidence',1],2801)]},
 'ai-governance-b': {
  'window':'sdw_3e13b01692fa8508d0cd','role':'B','records':14,'failure':'inexact evidence',
  'packet':'c20176a2fb40f144dba7c9fe9c2f4692bc212b0138ccefd2de5fe1b21f9598ca',
  'raw':'c37ceee41e26ff0387c1f432406bec2d891e0a6e6d9685f7a82f223f5a690f5f',
  'source':'ff1118462633db2182c44578f897008db840ddce2aa1f6fec51824fd5ab37374',
  'spans':[(0,['position','source_evidence',0],216),
           (1,['position','source_evidence',0],666),
           (1,['attitude','evaluation_evidence',0],666),
           (1,['attitude','modality_evidence',0],666),
           (7,['position','source_evidence',0],2938),
           (7,['attitude','evaluation_evidence',0],3004),
           (9,['position','source_evidence',0],3602),
           (9,['attitude','modality_evidence',0],3602),
           (9,['attitude','modality_evidence',1],3647),
           (11,['attitude','evaluation_evidence',0],4396),
           (13,['position','source_evidence',0],5212),
           (13,['position','source_evidence',1],5305),
           (13,['attitude','modality_evidence',0],5305)]},
 'hidden-brain-audit': {
  'window': 'sdw_99a1771e94fa2b923f9e', 'role': 'AUDIT', 'records': 9,
  'packet': 'b1953d846df1189168eec4c5e64c12fb9e0245ff861d57f7f746f232c9e367ab',
  'raw': '91af180837f7543db29c633355e6adbbb1e11d5cf0ac1cdfa30b012aa996cc21',
  'source': 'ec5cd4d05c36bc6d04c26a45cac88723eda08b45e4ec56b193f948f683760f70',
  'spans': [(3,['attitude','modality_evidence',0],2652), (3,['attitude','modality_evidence',1],2716)]},
 'ai-governance-a': {
  'window': 'sdw_3e13b01692fa8508d0cd', 'role': 'A', 'records': 15,
  'packet': 'ecf319aac9a384c04723ba67eda8f59f164d9c7f8814e2db23f7d99f6ef71455',
  'raw': '4771023be1a8d1eba1468feca6610a3b7beb6249fde0e056b247def06a63f9dd',
  'source': 'ff1118462633db2182c44578f897008db840ddce2aa1f6fec51824fd5ab37374',
  'spans': [(5,['attitude','evaluation_evidence',1],2836),
            (6,['attitude','evaluation_evidence',0],2979),
            (7,['attitude','target'],3354),
            (7,['attitude','evaluation_evidence',0],3354),
            (8,['attitude','evaluation_evidence',0],3861),
            (8,['attitude','modality_evidence',0],3602),
            (8,['attitude','modality_evidence',1],3647),
            (8,['attitude','modality_evidence',2],3749),
            (10,['attitude','target_components',0,'evaluation_evidence',0],4266),
            (11,['attitude','evaluation_evidence',0],4915),
            (14,['attitude','modality_evidence',0],5883)]}}


# These exact positions are inspected, but other semantic validation defects
# remain. Never expose this audit as an applicable offset-only repair case.
PENDING_OFFSETS = {'ai-governance-audit': CASES.pop('ai-governance-audit')}


def recover(case, original, packet):
    spec=CASES[case];source=packet['transcript_window']
    records=original['records'] if spec['role']=='C' else original
    if (packet['window_id']!=spec['window'] or packet['role']!=spec['role'] or
        packet['packet_sha256']!=spec['packet'] or digest({k:v for k,v in packet.items() if k!='packet_sha256'})!=spec['packet'] or
        digest(original)!=spec['raw'] or digest(source)!=spec['source'] or len(records['events'])!=spec['records']):
        raise ValueError('not the inspected original source response')
    changes=[]
    for index,start in spec.get('main_spans',[]):
        event=records['events'][index];end=start+len(event['evidence_text'])
        if source[start:end]!=event['evidence_text']:raise ValueError('inspected main occurrence changed')
        for field,after in [('evidence_start',start),('evidence_end',end)]:
            if event[field]!=after:
                changes.append({'path':(['records'] if spec['role']=='C' else [])+['events',index,field],
                    'before':event[field],'after':after,'reason':'Exact individually inspected main quote; numeric offsets only.'})
    for index,parts,start in spec['spans']:
        path=(['records'] if spec['role']=='C' else [])+['events',index]+parts;span=original
        for key in path:span=span[key]
        if set(span)!={'text','start','end'}:raise ValueError('span shape changed')
        end=start+len(span['text'])
        if source[start:end]!=span['text']:raise ValueError('inspected occurrence changed')
        for field,after in [('start',start),('end',end)]:
            if span[field]!=after:changes.append({'path':path+[field],'before':span[field],'after':after,
                'reason':'Individually source-inspected occurrence; numeric offset only. No change to quoted text, identity, claim or uncertainty.'})
    if spec['role']=='C':
        from copy import deepcopy
        from .signal_desk_adjudication_lineage import validate as validate_lineage
        record_changes=[{**c,'path':c['path'][1:]} for c in changes]
        repaired,proof=propose(records,source=source,window_id=spec['window'],expected_original_sha256=digest(records),
            replacements=record_changes,output_validator=validate)
        fixed=deepcopy(original);fixed['records']=repaired
        validate_lineage(fixed,source=source,window_id=spec['window'],author_a=packet['author_a'],author_b=packet['author_b'])
        proof.update(original_envelope_sha256=digest(original),proposed_envelope_sha256=digest(fixed),
                     replacements=changes,lineage_unchanged=True)
    else:
        fixed,proof=propose(original,source=source,window_id=spec['window'],expected_original_sha256=spec['raw'],
            replacements=changes,output_validator=validate)
    proof.update(repair='september8-inspected-offsets-v1',case=case,packet_sha256=spec['packet'],
        semantic_fields_changed=False,original_failure_preserved=True,
        inspected_spans=len(spec['spans'])+len(spec.get('main_spans',[])))
    return fixed,proof
