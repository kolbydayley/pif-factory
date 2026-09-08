import copy
import json
import pytest
from scripts import pif_signal_desk_hidden_brain_b_repair_review as repair


def test_only_inspected_need_changes_with_full_source_and_denominator():
    if not (repair.run.OUT/'plan.json').exists():pytest.skip('private development fixture absent')
    ps,fixed,proof=repair.prepare(write=False)
    d=repair.run.OUT/'calls'/repair.WID/'B';p=json.loads((d/'packet.json').read_text())
    raw=json.loads((d/f"{p['packet_sha256']}.output.json").read_text())
    expected=copy.deepcopy(raw)
    next(e for e in expected['events'] if e['event_id']==repair.EID)['evidence_role']['needs']='none'
    assert fixed==expected and len(fixed['events'])==9
    assert ps[0]['transcript_window']==p['transcript_window']
    assert [e['event_id'] for e in ps[0]['candidates']]==[repair.EID]
    assert proof['original_sha256']==repair.digest(raw)


def test_review_requires_original_provider_provenance(monkeypatch):
    if not (repair.run.OUT/'plan.json').exists():pytest.skip('private development fixture absent')
    def reject(*args):raise ValueError('provider provenance mismatch')
    monkeypatch.setattr(repair.run,'verify_provider',reject)
    with pytest.raises(ValueError,match='provider provenance'):repair.prepare(write=False)
