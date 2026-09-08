from copy import deepcopy
from scripts import pif_signal_desk_stoica_audit_delta_review as parent
from scripts import pif_signal_desk_stoica_audit_target_review as review


def test_only_the_reviewed_component_changes():
    fixed,proof,p=review.proposal();raw,_,_=parent.proposal()
    restored=deepcopy(fixed)
    restored['events'][2]['attitude']['target_components'][1]=raw['events'][2]['attitude']['target_components'][1]
    assert restored==raw
    assert len(fixed['events'])==13
    assert not proof['gold_accepted']


def test_full_record_and_source_in_final_target_review():
    ps=review.prepare(write=False);_,_,p=review.proposal()
    assert [e['event_id'] for q in ps for e in q['candidates']]==['evt_003']
    assert all(q['transcript_window']==p['transcript_window'] for q in ps)
