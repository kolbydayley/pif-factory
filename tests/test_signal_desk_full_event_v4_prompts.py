import pytest
from research_factory import signal_desk_full_event_v4_prompts as prompts
from research_factory.signal_desk_rubric_reference_packets import digest
from test_signal_desk_full_event_v4 import fixture, SOURCE


def source():
    p = {"window_id": "dev", "transcript_window": SOURCE, "transcript_structure": "speaker_turn",
        "old_answers": "must never reach an independent author"}
    p["packet_sha256"] = digest(p)
    return p


@pytest.mark.parametrize("role", ["A", "B", "AUDIT"])
def test_independent_roles_no_prior_answers(role):
    p = prompts.packet(source(), role)
    assert "old_answers" not in p and "author_a" not in p and "author_b" not in p
    with pytest.raises(ValueError): prompts.packet(source(), role, author_a=fixture())


def test_adjudicator_requires_valid_complete_authors():
    with pytest.raises(ValueError): prompts.packet(source(), "C")
    p = prompts.packet(source(), "C", author_a=fixture(), author_b=fixture())
    assert p["author_a"]["events"] and p["author_b"]["voice_bindings"]


def test_all_roles_share_same_semantics():
    assert all(p.endswith(prompts.COMMON) for p in prompts.prompts().values())
    assert "Never infer from metadata, alternation, self-introduction elsewhere" not in prompts.COMMON
    assert "Stance describes an explicit attitude toward the event's complete proposition" not in prompts.COMMON
    assert not prompts.receipt()["qualified"]


def test_source_hash_mismatch_fails():
    p = source(); p["transcript_window"] += "changed"
    with pytest.raises(ValueError): prompts.packet(p, "A")
