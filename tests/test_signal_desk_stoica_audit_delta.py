from scripts import pif_signal_desk_stoica_audit_review as parent
from scripts import pif_signal_desk_stoica_audit_delta_review as review


def test_exact_five_review_corrections_preserve_population_and_evidence():
    fixed,proof,p=review.proposal();raw,_,_=parent.proposal()
    changed={'evt_003','evt_005','evt_007','evt_008','evt_009'}
    assert len(fixed['events'])==13
    assert fixed['voice_bindings']==raw['voice_bindings']
    for before,after in zip(raw['events'],fixed['events']):
        if before['event_id'] not in changed:assert before==after
        for key in ('event_id','evidence_text','evidence_start','evidence_end','attribution','publishability_state'):
            assert before[key]==after[key]
    assert not proof['gold_accepted']


def test_delta_full_source_and_five_records():
    ps=review.prepare(write=False);_,_,p=review.proposal()
    assert [e['event_id'] for q in ps for e in q['candidates']]==['evt_003','evt_005','evt_007','evt_008','evt_009']
    assert all(q['transcript_window']==p['transcript_window'] for q in ps)
