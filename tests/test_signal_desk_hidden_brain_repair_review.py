import json
from copy import deepcopy
import pytest
from scripts import pif_signal_desk_hidden_brain_repair_review as repair


@pytest.fixture
def inspected(tmp_path,monkeypatch):
    original_root=repair.run.OUT;directory=original_root/'calls'/repair.WID/'A'
    if not (directory/'packet.json').exists():pytest.skip('private development fixture not installed')
    packet=json.loads((directory/'packet.json').read_text());sha=packet['packet_sha256']
    root=tmp_path/'v5';target=root/'calls'/repair.WID/'A';target.mkdir(parents=True)
    for name in ['packet.json',sha+'.output.json',sha+'.sidecar.json']:
        (target/name).write_bytes((directory/name).read_bytes())
    (root/'plan.json').write_bytes((original_root/'plan.json').read_bytes())
    monkeypatch.setattr(repair.run,'OUT',root);monkeypatch.setattr(repair,'OUT',tmp_path/'review')
    return packet,json.loads((target/(sha+'.output.json')).read_text()),target


def test_inspected_proposal_preserves_population_claims_and_uncertainty(inspected):
    p,original,target=inspected;ps,proposed,proof=repair.prepare(write=False)
    assert not repair.OUT.exists() and len(proposed['events'])==len(original['events'])==11
    assert [e['claim_text'] for e in proposed['events']]==[e['claim_text'] for e in original['events']]
    assert [e['event_id'] for e in proposed['events']]==[e['event_id'] for e in original['events']]
    assert proposed['events'][1]['publishability_state']=='uncertain'
    assert proposed['events'][1]['evidence_role']['needs']=='none'
    for before,after in zip(original['voice_bindings'],proposed['voice_bindings']):
        assert before['voice_surface']==after['voice_surface'] and before['corridor']==after['corridor']
    assert [e['event_id'] for e in ps[0]['candidates']]==[repair.EID]
    repair.run.contract.validate(proposed,source=p['transcript_window'],window_id=repair.WID)


@pytest.mark.parametrize('tamper',['raw','sidecar'])
def test_only_inspected_original_and_verified_author_can_be_repaired(inspected,tamper):
    p,raw,target=inspected;sha=p['packet_sha256']
    path=target/(sha+('.output.json' if tamper=='raw' else '.sidecar.json'))
    value=json.loads(path.read_text())
    if tamper=='raw':value['events'][0]['claim_text']='Changed'
    else:value['model']='wrong'
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError):repair.prepare(write=False)
