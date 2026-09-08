import copy
import pytest
from scripts import pif_signal_desk_stoica_delta_review as delta


def test_only_two_attitudes_change_and_voices_remain_unresolved():
    baseline,_,_=delta.parent.proposal()
    fixed,proof,p=delta.proposal()
    restored=copy.deepcopy(fixed)
    for i in (0,1):
        restored['events'][i]['attitude']=baseline['events'][i]['attitude']
        assert fixed['events'][i]['attribution']['transcript_voice'] is None
        assert fixed['events'][i]['publishability_state']=='uncertain'
    assert restored==baseline
    assert len(fixed['events'])==15
    assert fixed['events'][0]['attitude']['attitude']=='negative'
    target=fixed['events'][1]['attitude']['target']
    assert target['text']=='application'
    assert p['transcript_window'][target['start']:target['end']]==target['text']
    assert len(proof['replacements'])==5
    ps=delta.prepare(write=False)
    assert len(ps)==1
    assert {e['event_id'] for e in ps[0]['candidates']}==delta.IDS


def test_changed_review_population_blocks(monkeypatch):
    real=delta.verify
    def changed(*args):
        rows,proofs=real(*args)
        rows=copy.deepcopy(rows);rows[-1]['verdict']='unresolved'
        return rows,proofs
    monkeypatch.setattr(delta,'verify',changed)
    with pytest.raises(ValueError,match='unexpected unresolved'):
        delta.proposal()
