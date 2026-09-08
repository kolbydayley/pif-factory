import json
from research_factory.signal_desk_flattened_interview_proposal import prepare
from scripts import pif_signal_desk_lineage_qualification as run


def test_unknown_speakers_and_genuine_source_limits_preserved():
    fixed,proof,p=prepare();d=run.OUT/'calls'/p['window_id']/'A'
    raw=json.loads((d/f"{p['packet_sha256']}.output.json").read_text())
    assert len(fixed['events'])==27
    assert fixed['voice_bindings']==raw['voice_bindings']
    for old,new in zip(raw['events'],fixed['events']):
        assert new['claim_text']==old['claim_text']
        assert new['evidence_text']==old['evidence_text']
        assert new['attribution']['transcript_voice']==old['attribution']['transcript_voice']
    for i in (4,25):
        assert fixed['events'][i]['evidence_role']['role']=='research_limitation'
        assert fixed['events'][i]['evidence_role']['needs']=='audio_or_source'
        assert fixed['events'][i]['publishability_state']=='quarantined'
    assert fixed['events'][25]['evidence_text']==raw['events'][25]['evidence_text']
    assert not proof['gold_accepted']


def test_all_twenty_seven_records_receive_full_source_review():
    from scripts import pif_signal_desk_flattened_interview_review as review
    ps=review.prepare(write=False);fixed,_,p=prepare()
    assert [e['event_id'] for q in ps for e in q['candidates']]==[e['event_id'] for e in fixed['events']]
    assert all(q['transcript_window']==p['transcript_window'] for q in ps)
