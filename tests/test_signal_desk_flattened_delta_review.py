from scripts import pif_signal_desk_flattened_delta_review as delta


def test_exact_eleven_changes_preserve_population_and_unknown_voices():
    original, _, packet = delta.parent.proposal()
    fixed, proof, p = delta.proposal()
    assert p == packet
    assert len(fixed['events']) == 27
    assert fixed['voice_bindings'] == original['voice_bindings']
    changed = []
    for n, (before, after) in enumerate(zip(original['events'], fixed['events']), 1):
        assert before['event_id'] == after['event_id']
        for key in ('voice_binding_id', 'attribution_confidence', 'publishability_state', 'evidence_text', 'evidence_start', 'evidence_end'):
            assert before[key] == after[key]
        assert before['attribution']['transcript_voice'] == after['attribution']['transcript_voice']
        if before != after:
            changed.append(n)
    assert tuple(changed) == delta.NUMBERS
    assert fixed['events'][25]['publishability_state'] == 'quarantined'
    assert not proof['gold_accepted']


def test_review_retains_full_source_and_exact_delta_population():
    ps = delta.prepare(write=False)
    fixed, _, p = delta.proposal()
    expected = [fixed['events'][n-1]['event_id'] for n in delta.NUMBERS]
    assert [e['event_id'] for q in ps for e in q['candidates']] == expected
    for q in ps:
        assert p['transcript_window'] == q['transcript_window']
        assert q['candidate_population'] == 27
