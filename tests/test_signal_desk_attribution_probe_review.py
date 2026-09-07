from copy import deepcopy
import pytest
from research_factory.signal_desk_attribution_probe_review import validate_review


def fixture():
    packet = {"anchors": [{"event_id": "e"}], "transcript_window": "Someone says demand grew."}
    d = {"event_id": "e", "claim_status": "supported", "stance": "neutral", "source_rationale": "Unlabelled voice.",
        "attribution": {"transcript_voice": None, "proposition_owner": None, "relation": "own_statement", "mentioned_entities": []}}
    proposed = {"decisions": [d]}
    review = {"reviews": [{"verdict": "supported", "decision": deepcopy(d), "review_rationale": "No named narrator in source."}]}
    return packet, proposed, review


def test_correct_unknown_voice_can_be_supported():
    packet, p, r = fixture()
    assert validate_review(r, packet, p) == r


def test_supported_cannot_hide_stance_change():
    packet, p, r = fixture(); r["reviews"][0]["decision"]["stance"] = "supportive"
    with pytest.raises(ValueError, match="silently rewrite"): validate_review(r, packet, p)


def test_rationale_rewording_is_not_a_substantive_correction():
    packet, p, r = fixture(); row = r["reviews"][0]; row["verdict"] = "corrected"; row["decision"]["source_rationale"] = "Same meaning."
    with pytest.raises(ValueError, match="substantive"): validate_review(r, packet, p)


def test_missing_event_cannot_pass_review():
    packet, p, r = fixture(); r["reviews"] = []
    with pytest.raises(ValueError, match="incomplete"): validate_review(r, packet, p)


def test_unresolved_remains_explicit():
    packet, p, r = fixture(); r["reviews"][0]["verdict"] = "unresolved"
    assert validate_review(r, packet, p)["reviews"][0]["verdict"] == "unresolved"
