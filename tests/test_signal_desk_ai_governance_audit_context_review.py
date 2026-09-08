from scripts import pif_signal_desk_ai_governance_audit_context_review as review


def test_conditional_scope_and_unknown_voice_preserved_with_exact_context():
    old,_,_=review.previous.proposal()
    fixed,proof,p=review.proposal()
    assert len(fixed['events'])==15
    assert fixed['voice_bindings']==old['voice_bindings']
    for i,event in enumerate(fixed['events']):
        assert event['claim_text']==old['events'][i]['claim_text']
        assert event['attribution']==old['events'][i]['attribution']
        assert event['evidence_text']==old['events'][i]['evidence_text']
        if i not in (9,12):assert event==old['events'][i]
    assert fixed['events'][9]['claim_text'].startswith('If only one entity')
    assert fixed['events'][9]['attitude']['proposition_status']=='asserted'
    for span in fixed['events'][12]['context_evidence']+fixed['events'][12]['attitude']['evaluation_evidence']:
        assert p['transcript_window'][span['start']:span['end']]==span['text']
    assert not proof['gold_accepted']


def test_review_covers_both_changed_records_with_full_source():
    ps=review.prepare(write=False)
    assert {e['event_id'] for p in ps for e in p['candidates']}==review.IDS
    _,_,source_packet=review.proposal()
    assert all(p['transcript_window']==source_packet['transcript_window'] for p in ps)
