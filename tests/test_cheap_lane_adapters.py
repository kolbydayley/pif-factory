import json

import pytest

from research_factory.cheap_lane_adapters import (
    LABEL_SCHEMA,
    extract_json_lenient,
    unwrap_grok_response,
    validate_label,
)


def _label(claims=None, topics=None):
    return {
        "claims": claims or [],
        "entities": {"people": [], "organizations": [], "products": []},
        "topics": topics or [],
        "summary": "s",
        "needs_review": False,
        "overall_confidence": 0.9,
    }


def test_unwrap_grok_text_envelope():
    inner = _label()
    raw = json.dumps({"text": json.dumps(inner)})
    assert unwrap_grok_response(raw) == inner


def test_unwrap_grok_bare_json():
    inner = _label()
    assert unwrap_grok_response(json.dumps(inner)) == inner


def test_extract_json_lenient_fenced():
    inner = _label()
    text = "Here you go:\n```json\n" + json.dumps(inner) + "\n```\nthanks"
    assert extract_json_lenient(text) == inner


def test_extract_json_lenient_prose_wrapped():
    inner = _label()
    text = "Sure!\n" + json.dumps(inner) + "\nAnything else?"
    assert extract_json_lenient(text) == inner


def test_extract_json_lenient_raises_without_json():
    with pytest.raises(ValueError):
        extract_json_lenient("no json here at all")


def test_validate_drops_ungrounded_event_keeps_grounded():
    seg = "the quick brown fox jumps over the lazy dog"
    label = _label(claims=[
        {"claim_text": "a", "claim_type": "assessment", "evidence": "quick brown fox", "confidence": 0.9},
        {"claim_text": "b", "claim_type": "assessment", "evidence": "not in transcript", "confidence": 0.9},
    ])
    result = validate_label(label, seg)
    assert result["schema_ok"] is True
    assert len(result["label"]["claims"]) == 1
    assert result["dropped"] == 1
    assert result["label"]["claims"][0]["claim_text"] == "a"


def test_validate_schema_rejects_missing_keys():
    result = validate_label({"claims": []}, "text")
    assert result["schema_ok"] is False


def test_validate_schema_rejects_bad_claim_type():
    seg = "alpha beta"
    label = _label(claims=[{"claim_text": "a", "claim_type": "vibes", "evidence": "alpha", "confidence": 0.5}])
    result = validate_label(label, seg)
    assert result["schema_ok"] is False


def test_label_schema_has_required_top_level_keys():
    assert set(LABEL_SCHEMA["required"]) == {
        "claims", "entities", "topics", "summary", "needs_review", "overall_confidence"}


def test_ground_span_exact_passthrough():
    from research_factory.cheap_lane_adapters import ground_span
    seg = "alpha beta gamma"
    assert ground_span("beta gamma", seg) == "beta gamma"


def test_ground_span_recovers_across_speaker_tags():
    from research_factory.cheap_lane_adapters import ground_span
    seg = "as this sort\nSpeaker 3: of unilateral thing.\nSpeaker 4: Almost so."
    got = ground_span("as this sort of unilateral thing.", seg)
    assert got == "as this sort\nSpeaker 3: of unilateral thing."
    assert got in seg


def test_ground_span_recovers_collapsed_whitespace():
    from research_factory.cheap_lane_adapters import ground_span
    seg = "hello   world\n  again"
    assert ground_span("hello world again", seg) == "hello   world\n  again"


def test_ground_span_none_for_paraphrase():
    from research_factory.cheap_lane_adapters import ground_span
    assert ground_span("completely different words", "the actual transcript text") is None


def test_validate_rewrites_evidence_to_exact_substring():
    seg = "intro\nSpeaker 1: the product is\nSpeaker 1: really fast today"
    label = _label(claims=[{"claim_text": "a", "claim_type": "assessment",
                            "evidence": "the product is really fast", "confidence": 0.9}])
    result = validate_label(label, seg)
    assert result["schema_ok"] is True
    assert len(result["label"]["claims"]) == 1
    assert result["label"]["claims"][0]["evidence"] in seg
    assert result["recovered"] == 1


def test_window_text_short_segment_single_window():
    from research_factory.cheap_lane_adapters import window_text
    assert window_text("short segment", max_chars=6000) == ["short segment"]


def test_window_text_splits_on_line_boundaries_with_overlap():
    from research_factory.cheap_lane_adapters import window_text
    lines = [f"Speaker 1: line {i} of the conversation about topic {i}" for i in range(300)]
    text = "\n".join(lines)
    windows = window_text(text, max_chars=4000, overlap_chars=400)
    assert all(len(w) <= 4000 for w in windows)
    assert len(windows) >= 3
    # every window is a contiguous substring of the original
    assert all(w in text for w in windows)
    # consecutive windows overlap
    for a, b in zip(windows, windows[1:]):
        assert a[-100:] in b or b[:100] in a


def test_merge_window_labels_dedupes_by_evidence():
    from research_factory.cheap_lane_adapters import merge_window_labels
    l1 = {"claims": [{"claim_text": "a", "claim_type": "assessment", "evidence": "shared evidence span", "confidence": 0.9}],
          "entities": {"people": ["Ann"], "organizations": [], "products": []},
          "topics": [], "summary": "first", "needs_review": False, "overall_confidence": 0.9}
    l2 = {"claims": [{"claim_text": "a2", "claim_type": "assessment", "evidence": "shared evidence span", "confidence": 0.8},
                     {"claim_text": "b", "claim_type": "prediction", "evidence": "unique span", "confidence": 0.7}],
          "entities": {"people": ["Ann", "Bob"], "organizations": [], "products": []},
          "topics": [], "summary": "second", "needs_review": True, "overall_confidence": 0.7}
    merged = merge_window_labels([l1, l2])
    assert len(merged["claims"]) == 2  # dupe evidence collapsed
    assert sorted(merged["entities"]["people"]) == ["Ann", "Bob"]
    assert merged["needs_review"] is True
