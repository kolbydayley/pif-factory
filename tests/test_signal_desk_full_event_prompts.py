import pytest
from research_factory.signal_desk_full_event_prompts import COMMON, packet, prompts, receipt
from research_factory.signal_desk_full_event_experiment import VERSION


def source():
    return {"window_id": "w", "transcript_window": "Hello.", "transcript_structure": "paragraph",
        "packet_sha256": "source", "anchors": [{"prior_answer": "DO NOT LEAK"}]}


def empty():
    return {"schema_version": VERSION, "window_id": "w", "window_disposition": "no_consequential_claims", "events": []}


def test_every_role_has_identical_common_semantics():
    assert set(prompts()) == {"A", "B", "C", "AUDIT", "FINAL"}
    assert all(p.endswith(COMMON) for p in prompts().values())
    assert not receipt()["qualified"]


@pytest.mark.parametrize("role", ["A", "B", "AUDIT"])
def test_independent_roles_withhold_prior_anchors_and_answers(role):
    assert "DO NOT LEAK" not in str(packet(source(), role))
    with pytest.raises(ValueError, match="independent"):
        packet(source(), role, author_a=empty())


def test_c_requires_two_source_validated_authors():
    with pytest.raises(ValueError, match="both"): packet(source(), "C", author_a=empty())
    value = packet(source(), "C", author_a=empty(), author_b=empty())
    assert "author_a" in value and "author_b" in value
    bad = empty(); bad["window_id"] = "another"
    with pytest.raises(ValueError, match="identity"): packet(source(), "C", author_a=empty(), author_b=bad)
