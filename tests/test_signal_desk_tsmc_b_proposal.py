from research_factory.signal_desk_tsmc_b_proposal import prepare


def test_b_proposal_preserves_unresolved_voices_and_sponsor_quarantine():
    fixed,proof,p=prepare()
    assert len(fixed['events'])==13 and proof['independent_b_review_required']
    for e in fixed['events'][:3]:
        assert e['evidence_role']['needs']=='none'
        assert e['evidence_role']['scope']=='not_applicable'
        assert e['publishability_state']=='uncertain'
        assert e['attribution']['transcript_voice'] is None
    assert all(e['publishability_state']=='quarantined' for e in fixed['events'][-2:])
    assert 'enabled' not in fixed['events'][10]['claim_text']
    assert proof['gold_accepted'] is False
