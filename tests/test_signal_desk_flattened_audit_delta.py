from scripts import pif_signal_desk_flattened_audit_review as parent
from scripts import pif_signal_desk_flattened_audit_delta_review as review


def test_six_corrections_preserve_quotes_voices_and_other_records():
    fixed,proof,p=review.proposal();raw,_,_=parent.proposal()
    changed={'evt_02','evt_04','evt_07','evt_09','evt_15','evt_17'}
    assert len(fixed['events'])==17
    assert fixed['voice_bindings']==raw['voice_bindings']
    for before,after in zip(raw['events'],fixed['events']):
        if before['event_id'] not in changed:assert before==after
        for key in ('event_id','evidence_text','evidence_start','evidence_end','publishability_state'):
            assert before[key]==after[key]
        assert before['attribution']['transcript_voice']==after['attribution']['transcript_voice']
    assert not proof['gold_accepted']


def test_six_full_records_and_source_in_review():
    ps=review.prepare(write=False);_,_,p=review.proposal()
    assert [e['event_id'] for q in ps for e in q['candidates']]==['evt_02','evt_04','evt_07','evt_09','evt_15','evt_17']
    assert all(q['transcript_window']==p['transcript_window'] for q in ps)
