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
