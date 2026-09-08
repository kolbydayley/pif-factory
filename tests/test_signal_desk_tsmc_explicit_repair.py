import json
from unittest.mock import patch
import pytest
from research_factory import signal_desk_tsmc_explicit_repair as repair
from scripts import pif_signal_desk_opening_voice_review as review


def test_preserves_all_records_quotes_and_unknown_speakers():
    value, proof, packet = repair.prepare()
    directory=review.run.OUT/'calls'/packet['window_id']/'A'
    raw=json.loads(next(directory.glob('*.output.json')).read_text())
    assert len(value['events']) == 14
    assert value['voice_bindings'] == raw['voice_bindings']
    for a,b in zip(raw['events'],value['events']):
        for key in ('event_id','claim_text','evidence_text','attribution','publishability_state'):
            assert a[key] == b[key]
    assert all(e['attribution']['transcript_voice'] is None for e in value['events'][:3])
    assert all(e['publishability_state']=='quarantined' for e in value['events'][-2:])
    assert proof['gold_accepted'] is False
    assert len(proof['replacements']) == 26


def test_diagnosis_provenance_required():
    with patch.object(repair,'verify',side_effect=ValueError('unverified diagnosis')):
        with pytest.raises(ValueError,match='unverified diagnosis'): repair.prepare()
