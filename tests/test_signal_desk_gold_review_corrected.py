from scripts.pif_signal_desk_gold_review_corrected import canonical_fields, batch_packets, packet_cases
from scripts.pif_signal_desk_gold_review_corrected import validate_packet_decisions
import pytest


def test_malformed_case_id_never_counts_as_reviewed():
    packet = {"cases": [{"case_id": "expected", "audit": None}]}
    with pytest.raises(ValueError):
        validate_packet_decisions({"model": "gpt-5.5", "decisions": [{"case_id": "typo", "decision": "gold_supported", "rationale": "Grounded"}]}, packet)


def test_nonexistent_audit_cannot_be_supported():
    packet = {"cases": [{"case_id": "expected", "audit": None}]}
    with pytest.raises(ValueError):
        validate_packet_decisions({"model": "gpt-5.5", "decisions": [{"case_id": "expected", "decision": "audit_supported", "rationale": "Grounded"}]}, packet)


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


def test_batches_preserve_exact_cases_and_never_mix_source_context():
    packets = [{"case": {"case_id": str(i)}, "transcript_window": "same source", "packet_sha256": str(i)} for i in range(9)]
    packets.append({"case": {"case_id": "other"}, "transcript_window": "different source", "packet_sha256": "other"})
    batches = batch_packets(packets)
    assert [len(packet_cases(p)) for p in batches] == [4, 4, 1, 1]
    assert [c["case_id"] for p in batches for c in packet_cases(p)] == [str(i) for i in range(9)] + ["other"]
    assert all(len(p["packet_sha256"]) == 64 for p in batches)
