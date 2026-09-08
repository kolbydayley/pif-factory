from scripts import pif_signal_desk_tsmc_audit_delta_review as delta


def test_only_reviewed_metadata_changes():
    original, _, p = delta.parent.proposal()
    fixed, proof, q = delta.proposal()
    assert p == q
    assert fixed['voice_bindings'] == original['voice_bindings']
    assert len(fixed['events']) == 15
    assert [i for i, pair in enumerate(zip(original['events'], fixed['events'])) if pair[0] != pair[1]] == [5]
    for key in ('claim_text', 'attribution', 'evidence_text', 'evidence_start', 'evidence_end', 'publishability_state'):
        assert fixed['events'][5][key] == original['events'][5][key]
    ps = delta.prepare(write=False)
    assert len(ps) == 1 and len(ps[0]['candidates']) == 1
    assert ps[0]['transcript_window'] == p['transcript_window']
    assert ps[0]['candidate_population'] == 15
    assert not proof['gold_accepted']
