import asyncio
import pytest
from scripts import pif_signal_desk_question_final_review as final
from test_signal_desk_question_qualification import setup, response, save


def test_incomplete_inventory_freezes_nothing(tmp_path,monkeypatch):
    plan,out=setup(tmp_path,monkeypatch);monkeypatch.setattr(final,'OUT',out/'final-review')
    with pytest.raises(FileNotFoundError):final.prepare()
    assert not final.OUT.exists()


def test_all_four_roles_and_every_lineage_input_are_reviewed(tmp_path,monkeypatch):
    plan,out=setup(tmp_path,monkeypatch);monkeypatch.setattr(final,'OUT',out/'final-review')
    async def fake(ps,**kwargs):
        p=ps[0];save(kwargs['output_root'],p,response(p));return 0
    monkeypatch.setattr(final.run,'metered_execute',fake)
    assert asyncio.run(final.run.execute(plan))==0
    records,ledger,receipt=final.prepare()
    assert len(records)==64
    assert {(p['window_id'],p['author_role']) for p in records}=={(wid,r) for wid in plan['window_ids'] for r in final.run.ROLES}
    assert sum(len(p['candidates']) for p in ledger)==32
    assert len(receipt['inventory'])==16 and not receipt['gold_accepted']
    assert all(p['transcript_window'] for p in records+ledger)
