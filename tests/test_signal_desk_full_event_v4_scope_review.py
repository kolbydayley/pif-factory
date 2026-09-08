import pytest
from research_factory import signal_desk_full_event_v4_scope_review as review
from research_factory import signal_desk_full_event_v4_review as parent
from research_factory.signal_desk_rubric_reference_packets import digest
from test_signal_desk_full_event_v4 import fixture, SOURCE


def test_packet_schema_only_assigns_actual_candidates():
    p = review.packets(fixture(), source=SOURCE, window_id="dev", token_count=lambda s: 1)[0]
    spec = review.schema(p)
    assert spec["properties"]["decisions"]["items"]["properties"]["event_id"]["enum"] == ["c1"]
    assert p["system_sha256"] == digest(review.SYSTEM)
    assert p["schema_sha256"] == digest(spec)
    v = {"decisions": [{"event_id": "context_only", "verdict": "supported", "correction_proposal": "", "rationale": "x", "source_quotes": ["Alex:"]}],
        "empty_window_verdict": "not_applicable", "empty_window_rationale": "", "coverage_notes": ""}
    with pytest.raises(ValueError): review.validate_review(v, p)


def test_old_packet_defaults_still_bind_original_system_schema():
    p = parent.packets(fixture(), source=SOURCE, window_id="dev", token_count=lambda s: 1)[0]
    assert p["system_sha256"] == digest(parent.SYSTEM)
    assert p["schema_sha256"] == digest(parent.schema())


def test_empty_assignment_uses_zero_decisions_not_invalid_empty_enum():
    v = fixture(); v.update(events=[], voice_bindings=[], window_disposition="no_records")
    p = review.packets(v, source=SOURCE, window_id="dev", token_count=lambda s: 1)[0]
    spec = review.schema(p)["properties"]["decisions"]
    assert spec["maxItems"] == 0 and "enum" not in spec["items"]["properties"]["event_id"]
