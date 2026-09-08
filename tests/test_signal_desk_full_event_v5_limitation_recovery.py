from copy import deepcopy
import hashlib
import json
import pytest
from research_factory.signal_desk_full_event_v4_repair import propose
from research_factory.signal_desk_full_event_v5 import validate
from research_factory.signal_desk_full_event_v5_limitation_recovery import recover
from research_factory import signal_desk_full_event_v5_review as review
from research_factory.signal_desk_rubric_reference_packets import digest
from scripts import pif_signal_desk_full_event_v5_limitation_review as script
from test_signal_desk_full_event_v5 import value,SOURCE


def setup(tmp_path,monkeypatch):
    v=value();v['events']=[dict(deepcopy(v['events'][0]),event_id=f'c{i}') for i in range(9)]
    e=v['events'][0];e['evidence_role']['needs']='wider_context'
    changes=[{'path':['events',0,'evidence_role','role'],'before':'substantive_claim','after':'research_limitation','reason':'Needs source recovery.'},
        {'path':['events',0,'publishability_state'],'before':'candidate','after':'quarantined','reason':'Hold without dropping.'}]
    fixed,r=propose(v,source=SOURCE,window_id='dev',expected_original_sha256=digest(v),replacements=changes,output_validator=validate)
    p={'transcript_window':SOURCE,'repair_provenance':r,'candidates':[fixed['events'][0]],'candidate_population':9,'system_sha256':digest(script.SYSTEM)}
    p['schema_sha256']=digest(review.schema(p));p['packet_sha256']=digest(p);sha=p['packet_sha256']
    result={'decisions':[{'event_id':'c0','verdict':'supported','correction_proposal':'','rationale':'Limited source.','source_quotes':['Alex:']}],
        'empty_window_verdict':'not_applicable','empty_window_rationale':'','coverage_notes':''}
    h=lambda s:hashlib.sha256(s.encode()).hexdigest()
    sidecar={'state':'completed','model':'gpt-5.5','effort':'high','error_class':None,
        'base_instructions_sha256':h(script.SYSTEM),'prompt_sha256':h(json.dumps(p,ensure_ascii=False))}
    plan={'original_B_packet_sha256':'original','provenance_sha256':digest(r),'packets':[sha]}
    for name,data in [('plan.json',plan),('provenance.json',r),('proposal.json',fixed),(f'{sha}.packet.json',p),
        (f'{sha}.review.json',result),(f'{sha}.output.json',result),(f'{sha}.sidecar.json',sidecar)]:
        (tmp_path/name).write_text(json.dumps(data,sort_keys=True))
    monkeypatch.setattr(script,'WID','dev');monkeypatch.setattr(script,'EID','c0')
    monkeypatch.setattr(script,'prepare',lambda **kwargs:[p])
    original_packet={'packet_sha256':'original','role':'B','window_id':'dev','transcript_window':SOURCE}
    return v,original_packet,sha


def test_reviewed_projection_keeps_denominator_and_is_not_gold(tmp_path,monkeypatch):
    v,p,sha=setup(tmp_path,monkeypatch);fixed,r=recover(v,p,tmp_path)
    assert len(fixed['events'])==len(v['events'])==9 and not r['gold_accepted']
    assert fixed['events'][0]['publishability_state']=='quarantined'
    assert v['events'][0]['publishability_state']=='candidate'


@pytest.mark.parametrize('tamper',['model','prompt','unsupported','source','proposal'])
def test_unverified_approval_cannot_apply(tmp_path,monkeypatch,tamper):
    v,p,sha=setup(tmp_path,monkeypatch)
    if tamper in ['model','prompt']:
        f=tmp_path/f'{sha}.sidecar.json';s=json.loads(f.read_text());s['model' if tamper=='model' else 'prompt_sha256']='wrong';f.write_text(json.dumps(s))
    elif tamper=='unsupported':
        for suffix in ['review','output']:
            f=tmp_path/f'{sha}.{suffix}.json';s=json.loads(f.read_text());s['decisions'][0]['verdict']='unresolved';f.write_text(json.dumps(s))
    elif tamper=='source':p['transcript_window']=SOURCE+' changed'
    else:(tmp_path/'proposal.json').write_text('{}')
    with pytest.raises(ValueError):recover(v,p,tmp_path)
