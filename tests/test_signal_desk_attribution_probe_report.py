from copy import deepcopy
import pytest
from research_factory.signal_desk_attribution_probe_review import build_packet
from research_factory.signal_desk_attribution_probe_report import summarize


def fixture():
    p = {"window_id": "w", "transcript_structure": "flattened", "candidate_event_count": 10,
        "anchors": [{"event_id": "e"}], "transcript_window": "An unnamed voice."}
    d = {"event_id": "e", "claim_status": "supported", "stance": "neutral", "source_rationale": "No name.",
        "attribution": {"transcript_voice": None, "proposition_owner": None, "relation": "own_statement", "mentioned_entities": []}}
    packet = build_packet(p, {"decisions": [d]})
    review = {"reviews": [{"verdict": "supported", "decision": deepcopy(d), "review_rationale": "Source supports abstention."}]}
    return packet, review


def test_denominators_include_unknown_and_unselected_candidates():
    p, r = fixture(); value = summarize([p], {p["review_packet_sha256"]: r})
    assert value["totals"]["candidate_events"] == 10
    assert value["totals"]["anchors"] == value["totals"]["review_unknown_voice"] == 1
    assert not value["gold_accepted"] and not value["gate_eligible"]


def test_missing_review_is_not_silently_dropped():
    p, r = fixture()
    with pytest.raises(ValueError, match="missing"): summarize([p], {})


def test_correction_remains_unresolved_proposal():
    p, r = fixture(); r["reviews"][0]["verdict"] = "corrected"; r["reviews"][0]["decision"]["stance"] = "unknown"
    result = summarize([p], {p["review_packet_sha256"]: r})
    assert result["unresolved_review_proposals"][0]["changed_fields"] == ["stance"]
    assert result["unresolved_review_proposals"][0]["source_adjudication_required"]


def test_modified_proposal_provenance_rejected():
    p, r = fixture(); p["proposed_labels"]["decisions"][0]["stance"] = "unknown"
    with pytest.raises(ValueError, match="provenance"): summarize([p], {p["review_packet_sha256"]: r})
