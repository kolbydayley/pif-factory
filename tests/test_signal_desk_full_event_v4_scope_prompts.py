import pytest
from research_factory import signal_desk_full_event_v4_scope_prompts as variant
from research_factory import signal_desk_full_event_v4_prompts as parent
from research_factory.signal_desk_rubric_reference_packets import digest
from test_signal_desk_full_event_v4 import SOURCE, fixture


def source():
    p = {"window_id": "dev", "transcript_window": SOURCE, "transcript_structure": "speaker_turn"}
    p["packet_sha256"] = digest(p)
    return p


def test_one_explicit_replacement_in_every_role_and_no_gate_change():
    assert variant.COMMON.replace(variant.NEW, variant.OLD) == parent.COMMON
    assert variant.receipt()["schema_sha256"] == parent.receipt()["schema_sha256"]
    for r, text in variant.prompts().items():
        assert variant.NEW in text and variant.OLD not in text
        assert digest(text) != digest(parent.prompts()[r])
    assert not variant.receipt()["qualified"]


@pytest.mark.parametrize("role", ["A", "B", "AUDIT"])
def test_independent_packet_has_no_peer_labels(role):
    p = variant.packet(source(), role)
    assert "author_a" not in p and "author_b" not in p
    assert p["transcript_window"] == SOURCE
    assert p["packet_sha256"] == digest({k:v for k,v in p.items() if k != "packet_sha256"})
    with pytest.raises(ValueError): variant.packet(source(), role, author_a=fixture())


def test_c_validates_both_authors_before_composition():
    with pytest.raises(ValueError): variant.packet(source(), "C")
    p = variant.packet(source(), "C", author_a=fixture(), author_b=fixture())
    assert p["author_a"] == p["author_b"] == fixture()
