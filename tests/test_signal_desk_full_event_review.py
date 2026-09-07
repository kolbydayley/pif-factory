import pytest
from research_factory.signal_desk_full_event_review import SYSTEM, validate_review
from research_factory.signal_desk_full_event_prompts import COMMON
from research_factory.signal_desk_full_event_experiment import VERSION
from research_factory.signal_desk_full_event_review import packets
from copy import deepcopy


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


def candidate():
    return {"event_id": "e", "claim_text": "Demand grew.", "speech_act": "assertion", "evidence_text": "Demand grew.",
        "evidence_start": 0, "evidence_end": 12, "attribution_confidence": .5,
        "attribution": {"transcript_voice": None, "proposition_owner": None, "relation": "own_statement", "mentioned_entities": []},
        "issue_label": "Demand", "issue_aliases": [], "stance": "neutral", "publishability_state": "candidate"}


@pytest.mark.parametrize("mode", ["missing", "duplicate", "rewrite", "empty_correction"])
def test_nonempty_review_guards(mode):
    event = candidate()
    row = {"verdict": "supported", "event": deepcopy(event), "rationale": "Exact source."}
    value = {"decisions": [row], "empty_window_verdict": "not_applicable", "empty_window_rationale": ""}
    if mode == "missing": value["decisions"] = []
    if mode == "duplicate": value["decisions"].append(deepcopy(row))
    if mode == "rewrite": row["event"]["stance"] = "supportive"
    if mode == "empty_correction": row["verdict"] = "corrected"
    with pytest.raises(ValueError): validate_review(value, source="Demand grew.", window_id="w", candidates=[event])


def test_batch_limits_preserve_every_candidate_and_full_source():
    events = [{**candidate(), "event_id": str(i)} for i in range(51)]
    output = {"schema_version": VERSION, "window_id": "w", "window_disposition": "claims_found", "events": events}
    result = packets(output, source="Demand grew.", window_id="w", token_count=lambda s: 10)
    assert [len(p["candidates"]) for p in result] == [25, 25, 1]
    assert all(p["transcript_window"] == "Demand grew." and p["candidate_population"] == 51 for p in result)
    assert [e for p in result for e in p["candidates"]] == events
    with pytest.raises(ValueError, match="token limit"):
        packets(output, source="Demand grew.", window_id="w", token_count=lambda s: 12000)
