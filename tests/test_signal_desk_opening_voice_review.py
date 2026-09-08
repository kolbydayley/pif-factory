import pytest
from scripts import pif_signal_desk_opening_voice_review as r


def test_all_original_records_and_sources_are_retained():
    if not (r.run.OUT/'plan.json').exists():pytest.skip('private development fixture absent')
    ps=r.prepare(write=False)
    assert {p['window_id'] for p in ps}==set(r.CASES)
    for wid,(_,raw_hash,count) in r.CASES.items():
        rows=[p for p in ps if p['window_id']==wid]
        assert sum(len(p['candidates']) for p in rows)==count
        assert len({p['transcript_window'] for p in rows})==1
        assert all(p['original_output_sha256']==raw_hash and not p['gold_accepted'] for p in rows)
        ids=[e['event_id'] for p in rows for e in p['candidates']]
        assert len(set(ids))==count


def test_original_provider_must_verify(monkeypatch):
    if not (r.run.OUT/'plan.json').exists():pytest.skip('private development fixture absent')
    def reject(*args):raise ValueError('provider mismatch')
    monkeypatch.setattr(r.run,'verify_provider',reject)
    with pytest.raises(ValueError,match='provider'):r.prepare(write=False)
