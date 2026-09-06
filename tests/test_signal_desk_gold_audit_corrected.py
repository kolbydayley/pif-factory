import pytest
from scripts.pif_signal_desk_gold_audit_corrected import retain_quarantined_candidates


def test_quarantines_stay_in_audit_denominator_without_guessing_speakers():
    raw = {"events": [{"event_id": "a", "speaker_id": None}, {"event_id": "b", "speaker_id": None}], "window_disposition": "has_consequential_claims"}
    projected = {"events": [{"event_id": "a", "speaker_id": "supported"}]}
    result = retain_quarantined_candidates(projected, raw, 1)
    assert len(result["events"]) == 2
    assert result["events"][1]["speaker_id"] is None
    assert result["events"][0]["speaker_id"] == "supported"
    with pytest.raises(ValueError):
        retain_quarantined_candidates(projected, raw, 0)


def test_unexpected_claim_cannot_enter_audit():
    with pytest.raises(ValueError):
        retain_quarantined_candidates({"events": [{"event_id": "invented"}]}, {"events": [], "window_disposition": "empty"}, 0)
