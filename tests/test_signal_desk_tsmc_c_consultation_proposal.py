from research_factory.signal_desk_tsmc_c_consultation_proposal import prepare
from research_factory.signal_desk_tsmc_c_proposal import prepare as baseline


def test_consultation_restored_without_changing_other_records_or_voices():
    old, _, _ = baseline()
    new, proof, packet = prepare()
    assert len(new['records']['events']) == 15
    assert new['records']['voice_bindings'] == old['records']['voice_bindings']
    for i, event in enumerate(new['records']['events']):
        if i != 7:
            assert event == old['records']['events'][i]
    event = new['records']['events'][7]
    assert 'consulted salespeople' in event['claim_text']
    assert event['evidence_text'].startswith('I called the salespeople')
    assert packet['transcript_window'][event['evidence_start']:event['evidence_end']] == event['evidence_text']
    assert event['attribution'] == old['records']['events'][7]['attribution']
    assert new['additions'] == old['additions']
    for i, row in enumerate(new['input_dispositions']):
        if i != 12:
            assert row == old['input_dispositions'][i]
    assert proof['independent_record_and_affected_lineage_review_required']
    assert not proof['gold_accepted'] and not proof['applied']
