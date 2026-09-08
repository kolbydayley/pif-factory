from scripts import pif_signal_desk_luxury_delta_review as delta


def test_four_corrections_preserve_source_and_unknown_voices():
    original, _, _ = delta.parent.proposal()
    fixed, proof, p = delta.proposal()
    assert len(fixed['events']) == 15
    assert fixed['voice_bindings'] == original['voice_bindings']
    assert tuple(n for n, (a, b) in enumerate(zip(original['events'], fixed['events']), 1) if a != b) == delta.NUMBERS
    for a, b in zip(original['events'], fixed['events']):
        for key in ('event_id', 'attribution', 'voice_binding_id', 'evidence_text', 'evidence_start', 'evidence_end', 'publishability_state'):
            assert a[key] == b[key]
    assert fixed['events'][12]['publishability_state'] == 'quarantined'
    ps = delta.prepare(write=False)
    assert sum(len(q['candidates']) for q in ps) == 4
    assert all(q['transcript_window'] == p['transcript_window'] and q['candidate_population'] == 15 for q in ps)
    assert not proof['gold_accepted']
