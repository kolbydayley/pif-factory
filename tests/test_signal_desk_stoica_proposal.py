from research_factory.signal_desk_stoica_proposal import prepare


def test_full_population_and_unknown_opening_preserved():
    value,proof,p=prepare()
    assert len(value['events'])==15 and not proof['gold_accepted']
    for e in value['events'][:4]:
        assert e['attribution']['transcript_voice'] is None
        assert e['publishability_state']=='uncertain'
        assert e['evidence_role']['needs']=='none'
    assert value['events'][-1]['context_evidence'][0]['purpose']=='question'
    assert value['events'][-1]['evidence_role']['scope']=='attributed_view'
    assert all(c['path'][-1] not in {'claim_text','evidence_text'} for c in proof['replacements'])
