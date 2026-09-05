from research_factory.signal_desk_attribution_gate import audit_events


def event(**changes):
    value = {
        "speech_act": "assertion", "attribution_type": "direct_speech",
        "speaker_id": "host", "attribution_confidence": 0.9,
        "mentioned_person_ids": [],
    }
    value.update(changes)
    return value


def row(structure, text, *events, speaker_map=None):
    return {"metadata": {"window_id": "w1", "show_id": "show", "transcript_structure": structure,
                          "speaker_map": speaker_map or []}, "text": text, "events": list(events)}


def test_speaker_turn_requires_supported_speaker():
    receipt = audit_events(windows=[row("speaker_turn", "Host: AI matters", event(speaker_id=None))])
    assert receipt["passed"] is False
    assert receipt["totals"]["missing_speaker_assignment"] == 1


def test_speaker_turn_rejects_unsupported_fabricated_identity():
    receipt = audit_events(windows=[row("speaker_turn", "Host: AI matters", event(speaker_id="spk_guest"))])
    assert receipt["passed"] is False
    assert receipt["totals"]["fabricated_or_unsupported_attribution"] == 1


def test_flattened_allows_indeterminable_without_identity():
    receipt = audit_events(windows=[row("flattened", "AI matters", event(speaker_id=None, attribution_type="unresolved_speaker"))])
    assert receipt["passed"] is True
    assert receipt["totals"]["indeterminable_claims"] == 1


def test_flattened_requires_assignment_when_speaker_map_identity_is_present():
    receipt = audit_events(windows=[row("flattened", "Mustafa says AI matters", event(speaker_id=None),
                                       speaker_map=[{"speaker_id": "spk_host", "name": "Mustafa"}])])
    assert receipt["passed"] is False
    assert receipt["totals"]["missing_supported_speaker"] == 1


def test_third_party_cannot_be_presented_as_own_statement():
    receipt = audit_events(windows=[row("speaker_turn", "Host: Dario believes this", event(
        attribution_type="third_party_mention", speaker_id="dario", mentioned_person_ids=["dario"]))])
    assert receipt["passed"] is False
    assert receipt["totals"]["third_party_presented_as_own"] == 1


def test_receipt_is_sanitized_and_by_show_is_present():
    receipt = audit_events(windows=[row("speaker_turn", "Host: AI matters", event(speaker_id="host"),
                                       speaker_map=[{"speaker_id": "host", "name": "Host"}])])
    assert receipt["passed"] is True
    assert receipt["receipt_exposes_transcript_text"] is False
    assert "show" in receipt["by_show"]
