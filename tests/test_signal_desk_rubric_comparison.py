import pytest
from research_factory.signal_desk_rubric_comparison import compare_role


def event(speaker):
    return {"event_id":"e", "claim_text":"Demand for chips increased", "evidence_text":"Demand for chips increased",
        "evidence_start":0,"evidence_end":26,"speaker_id":speaker,"attribution_type":"direct_speech",
        "stance":"neutral","issue_label":"chips"}


def test_field_difference_is_review_candidate_not_confirmed_error():
    out=compare_role({"window_id":"w","events":[event("Alice")]},
        {"window_id":"w","events":[event("Bob")]},role="A",transcript_structure="speaker_turn")
    assert out["matched_events"]==1
    assert out["field_denominators"]["speaker"]==1
    assert out["field_agreement_counts"]["speaker"]==0
    assert out["review_candidates"][0]["confirmed_error"] is False
    assert not out["gold_accepted"] and not out["gate_eligible"]


def test_missing_extraction_is_preserved_not_declared_false_gold():
    out=compare_role({"window_id":"w","events":[event("Alice")]},
        {"window_id":"w","events":[]},role="AUDIT",transcript_structure="speaker_turn")
    assert out["unmatched_reference_ids"]==["e"]
    assert out["source_adjudication_required"] and out["reference_events"]==1


def test_correct_empty_keeps_explicit_denominator_state():
    out=compare_role({"window_id":"w","events":[]},{"window_id":"w","events":[]},role="B",transcript_structure="flattened")
    assert out["both_empty"] and not out["gate_eligible"]


def test_cross_window_rejected():
    with pytest.raises(ValueError):
        compare_role({"window_id":"w","events":[]},{"window_id":"x","events":[]},role="A",transcript_structure="flattened")
