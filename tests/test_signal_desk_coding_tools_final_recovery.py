import json
import pytest
from research_factory import signal_desk_coding_tools_final_recovery as r
from scripts import pif_signal_desk_coding_tools_final_delta_review as final


def inputs():
    if not (final.OUT/'plan.json').exists():pytest.skip('private fixture absent')
    source=final.parent.parent.diagnosis;d=source.run.OUT/'calls'/source.WID/'A'
    return json.loads((d/f'{source.PACKET}.output.json').read_text()),json.loads((d/'packet.json').read_text())


def test_actual_approval_chain_preserves_all_records():
    raw,p=inputs();v,proof=r.recover(raw,p)
    assert len(proof['approval_chain'])==3
    assert [e['event_id'] for e in v['events']]==[e['event_id'] for e in raw['events']]
    assert len(v['events'])==15 and not proof['gold_accepted']
    assert sum(e['publishability_state']=='quarantined' for e in v['events'])==2


def test_last_unapproved_delta_cannot_reuse_earlier_approval(monkeypatch):
    raw,p=inputs();verify=r.verify
    def changed(module,ps):
        ds,proof=verify(module,ps)
        if module is final:ds[0]['verdict']='needs_correction'
        return ds,proof
    monkeypatch.setattr(r,'verify',changed)
    with pytest.raises(ValueError,match='not fully supported'):r.recover(raw,p)
