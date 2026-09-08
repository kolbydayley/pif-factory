import json
from scripts import pif_signal_desk_flattened_b_review as review


def test_full_population_voice_and_source_preserved():
    fixed,proof,p=review.proposal()
    d=review.run.OUT/'calls'/p['window_id']/'B'
    raw=json.loads((d/f"{p['packet_sha256']}.output.json").read_text())
    assert fixed['voice_bindings']==raw['voice_bindings']
    for before,after in zip(raw['events'],fixed['events']):
        for key in ('event_id','voice_binding_id','evidence_text','evidence_start','evidence_end','publishability_state'):
            assert before[key]==after[key]
        assert after['attribution']['transcript_voice'] is None
        assert after['attribution']['proposition_owner'] is None
    assert fixed['events'][17]==raw['events'][17]
    assert proof['gold_accepted'] is False
    ps=review.prepare(write=False)
    assert sum(len(q['candidates']) for q in ps)==19
    assert all(q['transcript_window']==p['transcript_window'] for q in ps)
