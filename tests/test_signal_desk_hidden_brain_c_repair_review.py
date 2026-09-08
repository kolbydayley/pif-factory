import json
from unittest.mock import patch

import pytest
from scripts import pif_signal_desk_hidden_brain_c_repair_review as review


def test_explicit_proposal_preserves_lineage_and_population():
    packets, fixed, proof = review.prepare(write=False)
    directory = review.run.OUT / 'calls' / review.WID / 'C'
    original = json.loads(next(directory.glob('*.output.json')).read_text())
    assert fixed['input_dispositions'] == original['input_dispositions']
    assert fixed['additions'] == original['additions']
    assert fixed['records']['voice_bindings'] == original['records']['voice_bindings']
    assert len(fixed['records']['events']) == 12
    assert len(proof['replacements']) == 3
    for index in range(12):
        if index not in (1, 10):
            assert fixed['records']['events'][index] == original['records']['events'][index]
    assert {e['event_id'] for p in packets for e in p['candidates']} == {
        review.WID + '_c2', review.WID + '_c11'}
    assert all(p['transcript_window'] for p in packets)
    assert proof['gold_accepted'] is False


def test_actual_provider_proof_required():
    with patch.object(review.run, 'verify_provider', side_effect=ValueError('unverified provider')):
        with pytest.raises(ValueError, match='unverified provider'):
            review.prepare(write=False)
