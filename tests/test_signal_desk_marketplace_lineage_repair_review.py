import json
import pytest
from scripts import pif_signal_desk_marketplace_lineage_repair_review as repair


@pytest.fixture
def inspected(tmp_path,monkeypatch):
    root=repair.run.OUT;original=root/'calls'/repair.WID/'C'
    if not (original/'packet.json').exists():pytest.skip('private development fixture not installed')
    p=json.loads((original/'packet.json').read_text());sha=p['packet_sha256']
    target=tmp_path/'calls'/repair.WID/'C';target.mkdir(parents=True)
    for name in ['packet.json',sha+'.output.json',sha+'.sidecar.json']:(target/name).write_bytes((original/name).read_bytes())
    (tmp_path/'plan.json').write_bytes((root/'plan.json').read_bytes())
    monkeypatch.setattr(repair.run,'OUT',tmp_path);monkeypatch.setattr(repair,'OUT',tmp_path/'review')
    return p,json.loads((target/(sha+'.output.json')).read_text()),target


def test_complete_envelope_preserved_except_one_explicit_field(inspected):
    p,raw,target=inspected;ps,proposed,proof=repair.prepare(write=False)
    assert not repair.OUT.exists() and len(proposed['records']['events'])==11 and len(proposed['input_dispositions'])==20
    assert proof['lineage_unchanged'] and proposed['input_dispositions']==raw['input_dispositions']
    e=next(e for e in proposed['records']['events'] if e['event_id']==repair.EID)
    assert e['publishability_state']=='uncertain' and e['evidence_role']['decision']=='boundary'
    e['evidence_role']['needs']='wider_context'
    assert proposed==raw
    assert [e['event_id'] for e in ps[0]['candidates']]==[repair.EID]


@pytest.mark.parametrize('tamper',['raw','request'])
def test_tampered_response_or_request_is_not_repaired(inspected,tamper):
    p,raw,target=inspected;sha=p['packet_sha256']
    path=target/(sha+('.output.json' if tamper=='raw' else '.sidecar.json'));v=json.loads(path.read_text())
    if tamper=='raw':v['input_dispositions'].pop()
    else:v['prompt_sha256']='wrong'
    path.write_text(json.dumps(v))
    with pytest.raises(ValueError):repair.prepare(write=False)
