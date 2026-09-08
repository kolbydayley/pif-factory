import json
from research_factory.signal_desk_stoica_audit_proposal import prepare
from scripts import pif_signal_desk_stoica_audit_review as review


def test_exact_four_needs_changes_preserve_unknown_voices_and_all_other_fields():
    fixed,proof,p=prepare();d=review.run.OUT/'calls'/p['window_id']/'AUDIT'
    raw=json.loads((d/f"{p['packet_sha256']}.output.json").read_text())
    for i in range(4):
        assert fixed['events'][i]['attribution']['transcript_voice'] is None
        assert fixed['events'][i]['publishability_state']=='uncertain'
        raw['events'][i]['evidence_role']['needs']='none'
    span=raw['events'][0]['position']['source_evidence'][0]
    span['start']=319;span['end']=319+len(span['text'])
    assert fixed==raw
    assert not proof['gold_accepted']


def test_complete_source_and_thirteen_records_require_independent_review():
    ps=review.prepare(write=False);fixed,_,p=prepare()
    assert [e['event_id'] for q in ps for e in q['candidates']]==[e['event_id'] for e in fixed['events']]
    assert len(fixed['events'])==13
    assert all(q['transcript_window']==p['transcript_window'] for q in ps)
