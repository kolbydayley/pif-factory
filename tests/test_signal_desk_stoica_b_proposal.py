import json
from research_factory.signal_desk_stoica_b_proposal import prepare
from scripts import pif_signal_desk_stoica_b_review as review


def test_only_explicit_needs_change_no_speaker_invention():
    fixed,proof,p=prepare();d=review.run.OUT/'calls'/p['window_id']/'B'
    raw=json.loads((d/f"{p['packet_sha256']}.output.json").read_text())
    for i in range(3):
        assert fixed['events'][i]['attribution']['transcript_voice'] is None
        assert fixed['events'][i]['publishability_state']=='uncertain'
        raw['events'][i]['evidence_role']['needs']='none'
    span=raw['events'][5]['position']['source_evidence'][0]
    span['end']=span['start']+len(span['text'])
    assert fixed==raw
    assert not proof['gold_accepted']


def test_full_twelve_records_and_source_in_review():
    ps=review.prepare(write=False);fixed,_,p=prepare()
    assert [e['event_id'] for q in ps for e in q['candidates']]==[e['event_id'] for e in fixed['events']]
    assert len(fixed['events'])==12
    assert all(q['transcript_window']==p['transcript_window'] for q in ps)
