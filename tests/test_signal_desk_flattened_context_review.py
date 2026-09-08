from scripts import pif_signal_desk_flattened_context_review as delta


def test_exact_two_context_corrections_keep_twenty_seven_records():
    original, _, _ = delta.parent.proposal()
    fixed, proof, p = delta.proposal()
    assert tuple(n for n, (a,b) in enumerate(zip(original['events'],fixed['events']),1) if a != b) == delta.NUMBERS
    assert len(fixed['events']) == 27
    assert original['voice_bindings'] == fixed['voice_bindings']
    for before, after in zip(original['events'], fixed['events']):
        for key in ('event_id', 'attribution', 'voice_binding_id', 'evidence_text', 'evidence_start', 'evidence_end', 'publishability_state'):
            assert before[key] == after[key]
    assert fixed['events'][13]['attitude']['target']['text'] == 'AI'
    assert fixed['events'][13]['attitude']['attitude'] == 'mixed'
    ps = delta.prepare(write=False)
    assert sum(len(q['candidates']) for q in ps) == 2
    assert all(q['transcript_window'] == p['transcript_window'] and q['candidate_population'] == 27 for q in ps)
    assert not proof['gold_accepted']
