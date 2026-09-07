from copy import deepcopy
import pytest
from research_factory.signal_desk_full_event_experiment import VERSION, schema, validate, receipt
from research_factory.signal_desk_rebuild_contracts import event_schema


def fixture():
    source = "Alex says demand grew."
    owner = {"surface_name": "Alex", "kind": "person", "binding_span": {"text": "Alex says", "start": 0, "end": 9}}
    event = {"event_id": "e", "claim_text": "Demand grew.", "speech_act": "assertion",
        "evidence_text": "demand grew.", "evidence_start": 10, "evidence_end": 22,
        "attribution": {"transcript_voice": None, "proposition_owner": owner, "relation": "reported_statement", "mentioned_entities": []},
        "attribution_confidence": .8, "issue_label": "Demand", "issue_aliases": [], "stance": "neutral", "publishability_state": "candidate"}
    # Source length/offsets are intentionally derived in fixture, never repaired in validator.
    event["evidence_start"] = source.index("demand"); event["evidence_end"] = len(source)
    return source, {"schema_version": VERSION, "window_id": "w", "window_disposition": "claims_found", "events": [event]}


def test_known_owner_unknown_narrator_valid_in_full_event():
    source, value = fixture(); assert validate(value, source=source, window_id="w") == value


def test_frozen_contract_untouched():
    original = deepcopy(event_schema()); schema()
    assert event_schema() == original
    assert "speaker_id" in original["properties"]["events"]["items"]["required"]
    assert not receipt()["qualified"]


@pytest.mark.parametrize("field,value", [("publishability_state", "accepted"), ("evidence_start", True), ("stance", "positive")])
def test_invalid_or_self_approved_rejected(field, value):
    source, output = fixture(); output["events"][0][field] = value
    with pytest.raises(ValueError): validate(output, source=source, window_id="w")


def test_missing_or_fabricated_identity_span_rejected():
    source, output = fixture(); output["events"][0]["attribution"]["proposition_owner"]["binding_span"]["text"] = "Blake says"
    with pytest.raises(ValueError): validate(output, source=source, window_id="w")


def test_empty_window_preserved():
    source, output = fixture(); output["events"] = []; output["window_disposition"] = "no_consequential_claims"
    assert validate(output, source=source, window_id="w") == output
