import pytest
from scripts.pif_signal_desk_gold_context_survey import source_sections, grounded_index
from scripts.pif_signal_desk_gold_context_survey import source_units, range_index, range_schema


def test_range_selection_copies_exact_source_including_whitespace_and_cross_unit_cues():
    text = "I\nmean I don't think we should"
    units = source_units(text, 100, 7)
    packet = {"source_units": units, "packet_sha256": "p", "transcript_sha256": "s"}
    assert "".join(u["text"] for u in units) == text
    output = {"model": "gpt-5.5", "findings": [{"first_unit": units[0]["id"], "last_unit": units[-1]["id"]}], "limitations": ""}
    finding = range_index(output, packet)["findings"][0]
    assert finding["quote"] == text
    assert finding["source_locations"] == [[100, 100 + len(text)]]
    assert range_schema(packet)["properties"]["findings"]["items"]["properties"]["first_unit"]["enum"] == [u["id"] for u in units]


@pytest.mark.parametrize("first,last", [("u001", "u000"), ("invented", "u001")])
def test_reversed_or_unknown_range_is_rejected(first, last):
    with pytest.raises(ValueError):
        range_index({"model": "gpt-5.5", "findings": [{"first_unit": first, "last_unit": last}], "limitations": ""},
                    {"source_units": source_units("abcdefgh", 0, 4)})


def test_sections_cover_every_character_with_overlap_and_no_tail_loss():
    text = "abcdefghijklmnopqrstuvwxyz" * 10
    sections = list(source_sections(text, 40, 7))
    assert sections[0][0] == 0 and sections[-1][1] == len(text)
    for i, (start, end, part) in enumerate(sections):
        assert part == text[start:end]
        if i:
            assert start == sections[i-1][1] - 7


def test_exact_quotes_bind_all_locations_without_guessing_unique_offset():
    packet = {"source_text": "Ben: hi Ben: hi", "start_char": 100, "packet_sha256": "p", "transcript_sha256": "s"}
    output = {"model": "gpt-5.5", "findings": [{"quote": "Ben: hi"}], "limitations": "not a final assignment"}
    index = grounded_index(output, packet)
    assert index["findings"][0]["source_locations"] == [[100, 107], [108, 115]]
    assert index["findings"][0]["location_ambiguous"]
    assert not index["gold_accepted"]


def test_fabricated_or_normalized_quote_fails_closed():
    with pytest.raises(ValueError):
        grounded_index({"model": "gpt-5.5", "findings": [{"quote": "Ben: hi"}], "limitations": ""},
            {"source_text": "Ben:  hi", "start_char": 0})
