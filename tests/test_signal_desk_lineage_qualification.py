import asyncio
import hashlib
import json
import pytest
from scripts import pif_signal_desk_lineage_qualification as run
from test_signal_desk_full_event_v5 import value,SOURCE


def setup(tmp_path,monkeypatch):
    base=tmp_path/'sources';old=tmp_path/'old';out=tmp_path/'new'
    base.mkdir();old.mkdir();out.mkdir()
    monkeypatch.setattr(run.previous,'BASE',base);monkeypatch.setattr(run.previous,'OUT',old);monkeypatch.setattr(run,'OUT',out)
    ids=[f'w{i}' for i in range(16)];mapping={}
    for wid in ids:
        p={'window_id':wid,'transcript_window':SOURCE,'transcript_structure':'speaker_turn'};p['packet_sha256']=run.digest(p)
        mapping[wid]=p['packet_sha256'];run.immutable_json(base/f"{p['packet_sha256']}.packet.json",p)
    return {'window_ids':ids,'source_packets':mapping},out,old


def response(p):
    v=value();v['window_id']=p['window_id']
    if p['role']!='C':return v
    eid=v['events'][0]['event_id']
    return {'schema_version':run.lineage.VERSION,'records':v,'additions':[],
        'input_dispositions':[{'author':a,'input_event_id':eid,'action':'merged','output_event_ids':[eid],
                              'reason':'Equivalent proposition.'} for a in ('A','B')]}


def save(directory,p,v,*,model='gpt-5.6-sol'):
    sha=p['packet_sha256'];h=lambda s:hashlib.sha256(s.encode()).hexdigest()
    run.immutable_json(directory/'packet.json',p)
    for suffix in ('output','result'):run.immutable_json(directory/f'{sha}.{suffix}.json',v)
    run.immutable_json(directory/f'{sha}.sidecar.json',{'state':'completed','error_class':None,'model':model,'effort':'medium',
        'base_instructions_sha256':h(run.system(p['role'])),'prompt_sha256':h(json.dumps(p,ensure_ascii=False))})


@pytest.mark.parametrize('held',[False,True])
def test_full_population_uses_new_c_and_honest_predecessor_holds(tmp_path,monkeypatch,held):
    plan,out,old=setup(tmp_path,monkeypatch);seen=[]
    if held:run.immutable_json(old/'calls'/'w0'/'A'/'packet.json',{'failed_original':True})
    async def fake(ps,**kwargs):
        p=ps[0];seen.append((p['window_id'],p['role']))
        assert kwargs['system_for_packet'](p)==run.system(p['role'])
        v=kwargs['validator'](response(p),p);save(kwargs['output_root'],p,v);return 0
    monkeypatch.setattr(run,'metered_execute',fake)
    code=asyncio.run(run.execute(plan))
    assert code==(2 if held else 0) and len(seen)==(60 if held else 64)
    receipt=json.loads(next((out/'runs').glob('*.json')).read_text())
    assert len(receipt['windows'])==16 and not receipt['gold_accepted']
    if held:assert receipt['windows'][0]['state']=='held_predecessor'
    # Resume revalidates outputs without reissuing paid calls.
    seen.clear();asyncio.run(run.execute(plan));assert seen==[]


def test_reuse_requires_actual_model_and_never_imports_c(tmp_path,monkeypatch):
    plan,out,old=setup(tmp_path,monkeypatch)
    s=json.loads((run.previous.BASE/f"{plan['source_packets']['w0']}.packet.json").read_text())
    p=run.packet(s,'A',{});save(old/'w0'/'A',p,response(p),model='wrong')
    with pytest.raises(ValueError,match='provenance'):run.verified_call(old/'w0'/'A',p,imported=True)
    a=response(p);c=run.packet(s,'C',{'A':a,'B':a});save(out/'C',c,response(c))
    with pytest.raises(ValueError,match='old C'):run.verified_call(out/'C',c,imported=True)


def test_quality_hold_makes_zero_new_provider_calls(tmp_path,monkeypatch):
    plan,out,old=setup(tmp_path,monkeypatch);run.immutable_json(out/'ADMISSION-HOLD.json',{'reason':'review'})
    async def forbidden(*args,**kwargs):raise AssertionError('provider called during hold')
    monkeypatch.setattr(run,'metered_execute',forbidden)
    assert asyncio.run(run.execute(plan))==2
    receipt=json.loads(next((out/'runs').glob('*.json')).read_text())
    assert len(receipt['windows'])==16 and all(r['state']=='checkpointed' for r in receipt['windows'])
