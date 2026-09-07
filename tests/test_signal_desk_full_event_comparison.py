from copy import deepcopy
import pytest
from research_factory.signal_desk_full_event_experiment import VERSION
from research_factory.signal_desk_full_event_comparison import compare


def fixture():
    source = "Alex: Demand grew."
    a = {"surface_name": "Alex", "kind": "person", "binding_span": {"text": "Alex", "start": 0, "end": 4}}
    event = {"event_id": "e", "claim_text": "Demand grew.", "speech_act": "assertion", "evidence_text": "Demand grew.",
        "evidence_start": 6, "evidence_end": len(source), "attribution_confidence": .5,
        "attribution": {"transcript_voice": a, "proposition_owner": deepcopy(a), "relation": "own_statement", "mentioned_entities": []},
        "issue_label": "Demand", "issue_aliases": [], "stance": "neutral", "publishability_state": "candidate"}
    value = {"schema_version": VERSION, "window_id": "w", "window_disposition": "claims_found", "events": [event]}
    return source, value


def test_wrong_field_still_matches_claim_and_enters_denominator():
    source, gold = fixture(); pred = deepcopy(gold)
    pred["events"][0]["stance"] = "supportive"
    pred["events"][0]["attribution"]["transcript_voice"] = None
    pred["events"][0]["attribution"]["proposition_owner"] = None
    result = compare(gold, pred, source=source, structure="speaker_turn", role="AUDIT")
    assert result["matched"] == result["denominators"]["transcript_voice"] == 1
    assert result["agreement"]["transcript_voice"] == result["agreement"]["stance"] == 0
    assert not result["differences"][0]["confirmed_error"]


def test_different_valid_binding_span_is_not_identity_error():
    source, gold = fixture(); pred = deepcopy(gold)
    for field in ("transcript_voice", "proposition_owner"):
        pred["events"][0]["attribution"][field]["binding_span"] = {"text": "Alex:", "start": 0, "end": 5}
    result = compare(gold, pred, source=source, structure="speaker_turn", role="A")
    assert result["agreement"]["transcript_voice"] == 1 and not result["differences"]


def test_empty_prediction_keeps_missed_reference():
    source, gold = fixture(); pred = deepcopy(gold); pred["events"] = []; pred["window_disposition"] = "no_consequential_claims"
    result = compare(gold, pred, source=source, structure="speaker_turn", role="B")
    assert result["unmatched_reference"] == ["e"] and not result["gate_eligible"]


def test_cross_window_comparison_rejected():
    source, gold = fixture(); pred = deepcopy(gold); pred["window_id"] = "different"
    with pytest.raises(ValueError): compare(gold, pred, source=source, structure="speaker_turn", role="B")
