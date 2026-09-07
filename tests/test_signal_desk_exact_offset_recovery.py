import pytest
from research_factory.signal_desk_exact_offset_recovery import recover
from research_factory.signal_desk_full_event_experiment import VERSION


def fixture():
    event = {"event_id": "e", "claim_text": "Demand grew.", "speech_act": "assertion", "evidence_text": "Demand grew.",
        "evidence_start": 0, "evidence_end": 11, "attribution_confidence": .5,
        "attribution": {"transcript_voice": None, "proposition_owner": None, "relation": "own_statement", "mentioned_entities": []},
        "issue_label": "Demand", "issue_aliases": [], "stance": "neutral", "publishability_state": "candidate"}
    return {"schema_version": VERSION, "window_id": "w", "window_disposition": "claims_found", "events": [event]}


def test_unique_offset_repair_preserves_original_and_semantics():
    original = fixture(); fixed, receipt = recover(original, source="Demand grew.", window_id="w")
    assert original["events"][0]["evidence_end"] == 11
    assert fixed["events"][0]["evidence_end"] == 12
    assert not receipt["semantic_fields_changed"] and not receipt["gold_accepted"]


@pytest.mark.parametrize("source", ["Demand shrank.", "Demand grew. Demand grew.", "Something far away. Demand grew."])
def test_nonunique_missing_or_far_span_not_repaired(source):
    with pytest.raises(ValueError): recover(fixture(), source=source, window_id="w")
