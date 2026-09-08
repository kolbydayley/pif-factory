import hashlib
import json
import pytest
from scripts import pif_signal_desk_full_event_v5_review as final
from research_factory.signal_desk_rubric_reference_packets import digest
from test_signal_desk_full_event_v5 import value,SOURCE


def setup(tmp_path,monkeypatch):
    base=tmp_path/'base';out=tmp_path/'out';base.mkdir();out.mkdir()
    monkeypatch.setattr(final.run,'BASE',base);monkeypatch.setattr(final.run,'OUT',out);monkeypatch.setattr(final,'OUT',out/'final-review')
    plan={'window_ids':[f'dev{i}' for i in range(16)],'source_packets':{}}
    paths={};h=lambda text:hashlib.sha256(text.encode()).hexdigest()
    for wid in plan['window_ids']:
        s={'window_id':wid,'transcript_window':SOURCE,'transcript_structure':'speaker_turn'};s['packet_sha256']=digest(s)
        plan['source_packets'][wid]=s['packet_sha256'];(base/f"{s['packet_sha256']}.packet.json").write_text(json.dumps(s,sort_keys=True))
        # Reload serialized sources/results to match the actual request builder.
        s=json.loads((base/f"{s['packet_sha256']}.packet.json").read_text());outputs={}
        for role in ('A','B','C','AUDIT'):
            p=final.run.author.packet(s,role,author_a=outputs.get('A') if role=='C' else None,author_b=outputs.get('B') if role=='C' else None)
            d=out/'calls'/wid/role;d.mkdir(parents=True);sha=p['packet_sha256'];paths[(wid,role)]=(d,sha)
            (d/'packet.json').write_text(json.dumps(p,sort_keys=True))
            v=value();v['window_id']=wid
            for suffix in ('output','result'):(d/f'{sha}.{suffix}.json').write_text(json.dumps(v,sort_keys=True))
            outputs[role]=json.loads((d/f'{sha}.result.json').read_text())
            (d/f'{sha}.sidecar.json').write_text(json.dumps({'state':'completed','error_class':None,'model':'gpt-5.6-sol','effort':'medium',
                'base_instructions_sha256':h(final.run.author.prompts()[role]),'prompt_sha256':h(json.dumps(p,ensure_ascii=False))}))
    monkeypatch.setattr(final.run,'prepare',lambda:plan)
    return paths


def test_complete_sources_required_and_inventory_retains_all_roles(tmp_path,monkeypatch):
    setup(tmp_path,monkeypatch);packets=final.prepare()
    assert len(packets)==16
    plan=json.loads((final.OUT/'plan.json').read_text())
    assert len(plan['author_inventory'])==16
    for row in plan['author_inventory'].values():assert set(row['outputs'])==set(row['provenance'])=={'A','B','C','AUDIT'}
    assert not plan['gold_accepted']


@pytest.mark.parametrize('failure',['missing_audit','wrong_model','changed_prompt'])
def test_partial_or_unbound_authors_cannot_start_final_review(tmp_path,monkeypatch,failure):
    paths=setup(tmp_path,monkeypatch);d,sha=paths[('dev15','AUDIT')]
    if failure=='missing_audit':(d/f'{sha}.result.json').unlink()
    else:
        f=d/f'{sha}.sidecar.json';s=json.loads(f.read_text());s['model' if failure=='wrong_model' else 'prompt_sha256']='wrong';f.write_text(json.dumps(s))
    with pytest.raises((ValueError,FileNotFoundError)):final.prepare()
    assert not final.OUT.exists()
