import pytest
from scripts import pif_signal_desk_coding_tools_b_review as review


def test_complete_source_and_all_fourteen_records():
    ps = review.prepare(write=False)
    ids = [e['event_id'] for p in ps for e in p['candidates']]
    assert len(ids) == len(set(ids)) == 14
    assert len({p['transcript_window'] for p in ps}) == 1
    assert all(p['original_output_sha256'] == review.RAW for p in ps)
    assert all(p['gold_accepted'] is False for p in ps)


def test_provider_required(monkeypatch):
    def reject(*args): raise ValueError('provider mismatch')
    monkeypatch.setattr(review.run, 'verify_provider', reject)
    with pytest.raises(ValueError, match='provider mismatch'): review.prepare(write=False)
