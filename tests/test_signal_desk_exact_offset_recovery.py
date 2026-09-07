import pytest
from research_factory.signal_desk_exact_offset_recovery import recover
from research_factory.signal_desk_exact_offset_recovery import load_call
from research_factory.signal_desk_exact_offset_recovery import recover_binding
import json
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


def test_recovery_provenance_must_reproduce_result(tmp_path):
    original = fixture(); fixed, receipt = recover(original, source="Demand grew.", window_id="w")
    packet = {"packet_sha256": "p", "transcript_window": "Demand grew.", "window_id": "w"}
    (tmp_path / "p.output.json").write_text(json.dumps(original))
    (tmp_path / "p.result.json").write_text(json.dumps(fixed))
    (tmp_path / "offset-recovery.json").write_text(json.dumps(receipt))
    result, provenance = load_call(tmp_path, packet)
    assert result == fixed and provenance["offset_recovery"]
    fixed["events"][0]["stance"] = "supportive"
    (tmp_path / "p.result.json").write_text(json.dumps(fixed))
    with pytest.raises(ValueError, match="unverified"): load_call(tmp_path, packet)


def binding_fixture():
    value = fixture(); event = value["events"][0]; event["evidence_end"] = 12
    event["attribution"]["relation"] = "reported_statement"
    event["attribution"]["proposition_owner"] = {"surface_name": "Redfin", "kind": "organization",
        "binding_span": {"text": "data from Redfin that claims", "start": 13, "end": 40}}
    return value


def test_binding_projection_preserves_name_text_and_claim(tmp_path):
    value = binding_fixture(); source = "Demand grew. data from Redfin that claims"
    fixed, receipt = recover_binding(value, source=source, window_id="w")
    assert receipt["changes"] == [{"event_id": "e", "role": "proposition_owner", "before": [13, 40], "after": [13, 41]}]
    assert value["events"][0]["attribution"]["proposition_owner"]["binding_span"]["end"] == 40
    packet = {"packet_sha256": "p", "transcript_window": source, "window_id": "w"}
    for filename, data in (("p.output.json", value), ("p.result.json", fixed), ("offset-recovery.json", receipt)):
        (tmp_path / filename).write_text(json.dumps(data))
    assert load_call(tmp_path, packet)[1]["offset_recovery"]
    fixed["events"][0]["attribution"]["proposition_owner"]["surface_name"] = "Other"
    (tmp_path / "p.result.json").write_text(json.dumps(fixed))
    with pytest.raises(ValueError, match="unverified"): load_call(tmp_path, packet)


@pytest.mark.parametrize("source", ["Demand grew. Redfin", "Demand grew. data from Redfin that claims data from Redfin that claims", "Demand grew. many extra words data from Redfin that claims"])
def test_binding_missing_ambiguous_or_far_rejected(source):
    with pytest.raises(ValueError): recover_binding(binding_fixture(), source=source, window_id="w")
