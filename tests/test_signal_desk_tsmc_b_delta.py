import copy
import pytest
from scripts import pif_signal_desk_tsmc_b_delta_review as delta


def test_delta_preserves_population_and_approved_records():
    baseline, _, source = delta.parent.proposal()
    fixed, proof, packet = delta.proposal()
    assert source == packet
    assert fixed['voice_bindings'] == baseline['voice_bindings']
    assert len(fixed['events']) == 13
    for old, new in zip(baseline['events'], fixed['events']):
        if old['event_id'] not in delta.IDS:
            assert old == new
    assert fixed['events'][3]['evidence_role']['scope'] == 'not_applicable'
    span = fixed['events'][3]['context_evidence'][-1]
    assert packet['transcript_window'][span['start']:span['end']] == span['text']
    assert 'node' not in fixed['events'][5]['claim_text']
    assert fixed['events'][5]['evidence_text'] == baseline['events'][5]['evidence_text']
    ps = delta.prepare(write=False)
    assert [e['event_id'] for p in ps for e in p['candidates']] == ['evt_04', 'evt_06', 'evt_11']
    assert proof['parent_reviews']


def test_unexpected_review_population_blocks(monkeypatch):
    original = delta.verify
    def altered(*args):
        decisions, proofs = original(*args)
        decisions = copy.deepcopy(decisions)
        decisions[0]['verdict'] = 'needs_correction'
        return decisions, proofs
    monkeypatch.setattr(delta, 'verify', altered)
    with pytest.raises(ValueError, match='unexpected unresolved'):
        delta.proposal()
