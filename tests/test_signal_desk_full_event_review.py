import pytest
from research_factory.signal_desk_full_event_review import SYSTEM, validate_review
from research_factory.signal_desk_full_event_prompts import COMMON


def test_final_reviewer_shares_semantics():
    assert COMMON in SYSTEM


def test_empty_requires_explicit_semantic_verdict():
    result = {"decisions": [], "empty_window_verdict": "supported_empty", "empty_window_rationale": "Only greeting."}
    assert validate_review(result, source="Hello.", window_id="w", candidates=[]) == result
    result["empty_window_verdict"] = "not_applicable"
    with pytest.raises(ValueError, match="substantive"): validate_review(result, source="Hello.", window_id="w", candidates=[])


def test_blank_empty_rationale_rejected():
    with pytest.raises(ValueError, match="substantive"):
        validate_review({"decisions": [], "empty_window_verdict": "unusable", "empty_window_rationale": " "}, source="", window_id="w", candidates=[])
