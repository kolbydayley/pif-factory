import asyncio
import hashlib
import json
import pytest
from scripts import pif_signal_desk_question_qualification as run
from test_signal_desk_full_event_v5 import value,SOURCE


def setup(tmp_path,monkeypatch):
    base=tmp_path/'sources';old=tmp_path/'old';out=tmp_path/'new'
    base.mkdir();old.mkdir();out.mkdir()
    monkeypatch.setattr(run.previous.previous,'BASE',base);monkeypatch.setattr(run.previous,'OUT',old);monkeypatch.setattr(run,'OUT',out)
    ids=[f'w{i}' for i in range(16)];mapping={}
    for wid in ids:
        source={'window_id':wid,'transcript_window':SOURCE,'transcript_structure':'speaker_turn'};source['packet_sha256']=run.digest(source)
        mapping[wid]=source['packet_sha256'];run.immutable_json(base/f"{source['packet_sha256']}.packet.json",source)
    run.immutable_json(old/'plan.json',{'window_ids':ids,'source_packets':mapping})
    return run.prepare(),out


def response(p):
    v=value();v.update(window_id=p['window_id'],schema_version=run.authors.contract.VERSION)
    if p['role']!='C':return v
    eid=v['events'][0]['event_id']
    return {'schema_version':run.lineage.VERSION,'records':v,'additions':[],
            'input_dispositions':[{'author':a,'input_event_id':eid,'action':'merged','output_event_ids':[eid],
                                  'reason':'Equivalent source proposition.'} for a in ('A','B')]}


def save(directory,p,v,model='gpt-5.6-sol'):
    sha=p['packet_sha256'];h=lambda s:hashlib.sha256(s.encode()).hexdigest()
    run.immutable_json(directory/'packet.json',p)
    for suffix in ('output','result'):run.immutable_json(directory/f'{sha}.{suffix}.json',v)
    run.immutable_json(directory/f'{sha}.sidecar.json',{'state':'completed','error_class':None,'model':model,'effort':'medium',
        'base_instructions_sha256':h(run.system(p['role'])),'prompt_sha256':h(json.dumps(p,ensure_ascii=False))})


def test_all_64_fresh_roles_then_no_duplicate_calls(tmp_path,monkeypatch):
    plan,out=setup(tmp_path,monkeypatch);seen=[]
    async def fake(ps,**kwargs):
        p=ps[0];seen.append((p['window_id'],p['role']))
        save(kwargs['output_root'],p,kwargs['validator'](response(p),p));return 0
    monkeypatch.setattr(run,'metered_execute',fake)
    assert asyncio.run(run.execute(plan))==0 and len(seen)==64
    seen.clear();assert asyncio.run(run.execute(plan))==0 and not seen
    result=json.loads(next((out/'runs').glob('*.json')).read_text())
    assert len(result['windows'])==16 and not result['qualified'] and not result['gold_accepted']


def test_hold_blocks_paid_work_without_shrinking_population(tmp_path,monkeypatch):
    plan,out=setup(tmp_path,monkeypatch);run.immutable_json(out/'ADMISSION-HOLD.json',{'reason':'test'})
    async def forbidden(*args,**kwargs):raise AssertionError('paid call on hold')
    monkeypatch.setattr(run,'metered_execute',forbidden)
    assert asyncio.run(run.execute(plan))==2
    result=json.loads(next((out/'runs').glob('*.json')).read_text());assert len(result['windows'])==16


def test_wrong_provider_and_changed_source_fail_closed(tmp_path,monkeypatch):
    plan,out=setup(tmp_path,monkeypatch)
    p=run.packet(run.source_for(plan,'w0'),'A',{});save(out/'test',p,response(p),model='wrong')
    with pytest.raises(ValueError,match='provenance'):run.verified_call(out/'test',p)
    plan['source_packets']['w0']=plan['source_packets']['w1']
    with pytest.raises(ValueError,match='frozen source changed'):run.source_for(plan,'w0')


def test_changed_plan_cannot_dispatch(tmp_path,monkeypatch):
    plan,_=setup(tmp_path,monkeypatch);plan['max_concurrency']=8
    with pytest.raises(ValueError,match='frozen plan changed'):asyncio.run(run.execute(plan))


def test_existing_invalid_response_is_held_not_redispatched(tmp_path,monkeypatch):
    plan,out=setup(tmp_path,monkeypatch);seen=[]
    p=run.packet(run.source_for(plan,'w0'),'A',{});bad=response(p);bad['events'][0]['evidence_start']=999999
    directory=out/'calls'/'w0'/'A';save(directory,p,bad)
    (directory/f"{p['packet_sha256']}.result.json").unlink()
    async def fake(ps,**kwargs):
        q=ps[0];seen.append((q['window_id'],q['role']));save(kwargs['output_root'],q,response(q));return 0
    monkeypatch.setattr(run,'metered_execute',fake)
    assert asyncio.run(run.execute(plan))==2
    assert len(seen)==60 and not any(w=='w0' for w,r in seen)
    result=json.loads(next((out/'runs').glob('*.json')).read_text())
    assert len(result['windows'])==16 and result['windows'][0]['state']=='held_existing_response'
    assert json.loads((directory/f"{p['packet_sha256']}.output.json").read_text())==bad
