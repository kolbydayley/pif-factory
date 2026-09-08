from copy import deepcopy
from scripts import pif_signal_desk_flattened_audit_delta_review as parent
from scripts import pif_signal_desk_flattened_audit_target_review as review


def test_only_target_changes_and_source_complete():
    fixed,proof,p=review.proposal();raw,_,_=parent.proposal()
    restored=deepcopy(fixed);restored['events'][8]['attitude']['target']=raw['events'][8]['attitude']['target']
    assert restored==raw
    assert len(fixed['events'])==17 and not proof['gold_accepted']
    ps=review.prepare(write=False)
    assert [e['event_id'] for q in ps for e in q['candidates']]==['evt_09']
    assert all(q['transcript_window']==p['transcript_window'] for q in ps)
