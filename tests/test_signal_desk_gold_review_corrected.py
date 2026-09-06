from scripts.pif_signal_desk_gold_review_corrected import canonical_fields


def test_internal_role_maps_to_real_attribution_field_not_occupation():
    case = {"fields": ["speaker_role", "mentioned_people", "speaker"],
            "gold": {"speaker_role": None, "attribution_type": "third_party_mention",
                     "mentioned_person_ids": ["Other"], "mentioned_people": None},
            "audit": {"speaker_role": None, "attribution_type": "direct_speech"}}
    result = canonical_fields(case)
    assert result["fields"] == ["attribution_type", "mentioned_person_ids", "speaker_id"]
    assert "speaker_role" not in result["gold"]
    assert result["gold"]["attribution_type"] != result["audit"]["attribution_type"]
    assert result["gold"]["mentioned_person_ids"] == ["Other"]
    assert "speaker_role" in case["gold"]  # input remains immutable


def test_unmatched_candidate_remains_absent():
    result = canonical_fields({"fields": ["event_presence"], "gold": {}, "audit": None})
    assert result["audit"] is None
