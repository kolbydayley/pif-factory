import json

import pytest

from research_factory.signal_desk_gold_disagreement_apply import (
    _apply_correction,
    _parse_correction,
)


def _event():
    return {
        "claim_text": "A claim",
        "evidence_text": "A claim",
        "evidence_start": 2,
        "evidence_end": 9,
        "speaker_id": None,
        "quoted_person_id": None,
        "mentioned_person_ids": [],
        "attribution_type": "unresolved_speaker",
        "stance": "neutral",
    }


def test_correction_maps_adjudicator_field_names_and_recomputes_end():
    event = _apply_correction(
        _event(),
        json.dumps({"speaker_id": "s1", "speaker_role": "direct_speech",
                    "mentioned_people": ["p1"], "evidence_text": "A longer claim",
                    "evidence_start": 4}),
    )
    assert event["speaker_id"] == "s1"
    assert event["attribution_type"] == "direct_speech"
    assert event["mentioned_person_ids"] == ["p1"]
    assert event["evidence_end"] == 18


def test_event_presence_absent_is_rejected_for_an_accepted_event():
    with pytest.raises(ValueError, match="absent"):
        _apply_correction(_event(), json.dumps({"event_presence": "absent"}))


def test_correction_rejects_unknown_fields():
    with pytest.raises(ValueError, match="unsupported"):
        _parse_correction(json.dumps({"transcript_text": "secret"}))
