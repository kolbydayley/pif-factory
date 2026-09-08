from scripts import pif_signal_desk_ai_governance_audit_delta_review as delta


def test_only_assigned_four_records_change():
    baseline,_,_=delta.parent.proposal();fixed,proof,p=delta.proposal()
    assert fixed['voice_bindings']==baseline['voice_bindings']
    assert len(fixed['events'])==15
    for old,new in zip(baseline['events'],fixed['events']):
        if old['event_id'] not in delta.IDS:assert old==new
        assert old['claim_text']==new['claim_text']
        assert old['evidence_text']==new['evidence_text']
        assert old['publishability_state']==new['publishability_state']
        assert new['attribution']['transcript_voice']==old['attribution']['transcript_voice']
    imagined=fixed['events'][5]
    assert imagined['position']['position_status']=='anticipated_position'
    assert imagined['attribution']['proposition_owner'] is None
    assert imagined['speech_act']=='forecast'
    target=fixed['events'][9]['attitude']['target']
    assert p['transcript_window'][target['start']:target['end']]==target['text']
    assert sum(len(q['candidates']) for q in delta.prepare(write=False))==4
    assert not proof['gold_accepted']
