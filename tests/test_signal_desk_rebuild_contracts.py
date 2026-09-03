from __future__ import annotations

import pytest

from research_factory.signal_desk_rebuild_contracts import (
    EvidenceContractError,
    SCHEMA_VERSION,
    contract_sha256,
    eligible_for_person_says,
    event_schema,
    validate_output,
)


TEXT = "Ajeya said AI may displace some work, but the timing is uncertain."


def _event(**changes):
    evidence = "AI may displace some work"
    start = TEXT.index(evidence)
    event = {
        "event_id": "e1",
        "claim_text": "AI may displace some work.",
        "speech_act": "forecast",
        "evidence_text": evidence,
        "evidence_start": start,
        "evidence_end": start + len(evidence),
        "speaker_id": "person_ajeya",
        "quoted_person_id": None,
        "mentioned_person_ids": [],
        "attribution_type": "direct_speech",
        "attribution_confidence": 0.99,
        "issue_label": "AI impact on employment",
        "issue_aliases": ["AI jobs"],
        "stance": "warning",
        "publishability_state": "candidate",
    }
    event.update(changes)
    return event


def _output(event=None):
    return {
        "schema_version": SCHEMA_VERSION,
        "window_id": "w1",
        "window_disposition": "claims_found",
        "events": [event or _event()],
    }


def test_gold_and_glm_share_hash_stable_schema_and_exact_grounding():
    assert event_schema()["title"] == SCHEMA_VERSION
    assert len(contract_sha256()) == 64
    assert validate_output(_output(), transcript_window=TEXT, expected_window_id="w1")
    with pytest.raises(EvidenceContractError, match="not exact"):
        validate_output(
            _output(_event(evidence_text="AI definitely displaces work")),
            transcript_window=TEXT,
            expected_window_id="w1",
        )


def test_workhorse_cannot_self_approve_and_anonymous_cannot_be_public():
    with pytest.raises(EvidenceContractError, match="self-approve"):
        validate_output(
            _output(_event(publishability_state="accepted")),
            transcript_window=TEXT,
            expected_window_id="w1",
        )
    with pytest.raises(EvidenceContractError, match="cannot be public"):
        validate_output(
            _output(
                _event(
                    speaker_id=None,
                    attribution_type="unresolved_speaker",
                    publishability_state="accepted",
                )
            ),
            transcript_window=TEXT,
            expected_window_id="w1",
            allow_accepted=True,
        )


def test_mentions_never_become_what_person_says():
    mention = _event(
        speaker_id="person_host",
        mentioned_person_ids=["person_dario"],
        attribution_type="third_party_mention",
        publishability_state="accepted",
    )
    assert eligible_for_person_says(mention, "person_dario") is False
    direct = _event(publishability_state="accepted")
    assert eligible_for_person_says(direct, "person_ajeya") is True


def test_quoted_person_requires_separate_role():
    with pytest.raises(EvidenceContractError, match="quoted_person_id"):
        validate_output(
            _output(_event(attribution_type="quoted_speech")),
            transcript_window=TEXT,
            expected_window_id="w1",
        )


def test_chrome_and_questions_are_rejected():
    bad_text = "Subscribe to hear the rest"
    bad = _event(
        evidence_text=bad_text,
        evidence_start=0,
        evidence_end=len(bad_text),
    )
    with pytest.raises(EvidenceContractError, match="chrome"):
        validate_output(_output(bad), transcript_window=bad_text, expected_window_id="w1")
    with pytest.raises(EvidenceContractError, match="speech acts"):
        validate_output(
            _output(_event(speech_act="question")),
            transcript_window=TEXT,
            expected_window_id="w1",
        )


def test_substantive_discussion_of_subscriptions_is_not_chrome():
    text = "An RSS feed lets people subscribe and learn when new episodes are released."
    event = _event(evidence_text=text, evidence_start=0, evidence_end=len(text))
    assert validate_output(
        _output(event), transcript_window=text, expected_window_id="w1"
    )["events"][0]["evidence_text"] == text


@pytest.mark.parametrize("speech", [
    # Four real validation excerpts the bare-phrase regex quarantined on 2026-09-03.
    "Some businesses too often trick consumers into paying for subscriptions they no longer want or didn't sign up for in the",
    "I would go and sign up to be the fry cook at\na Denny's that doesn't close.",
    "Right now there is a spring fling race by the\nlarge AI labs to get students to sign up.",
    "the privacy policy makes no mention of the new tracking or maintainer",
    # Same class: talking ABOUT the thing is evidence.
    "we read the cookie policy and it says nothing about third parties",
    "all episodes of that season were recorded in one week",
])
def test_speech_that_discusses_signup_or_policies_is_evidence_not_chrome(speech):
    event = _event(evidence_text=speech, evidence_start=0, evidence_end=len(speech))
    assert validate_output(_output(event), transcript_window=speech, expected_window_id="w1")


@pytest.mark.parametrize("chrome", [
    # Real page chrome / CTA forms seen in the benchmark windows.
    "Email (required) Sign Up By submitting your email, you agree to",
    "Login / Sign Up close Close Search",
    "Sign Up On Substack",
    "sign up for our weekly summary check out",
    "sign up for the newsletter",
    "Sign up now to get every episode",
    "sign up at pythonbytes.fm/foundershub",
    "Privacy Policy | Terms of Use",
    "cookie policy and terms",
    "See all episodes",
    "this episode is sponsored by our friends",
    "brought to you by Squarespace",
    "Subscribe to hear the rest",
    "skip to content",
])
def test_boilerplate_forms_are_still_rejected_as_chrome(chrome):
    event = _event(evidence_text=chrome, evidence_start=0, evidence_end=len(chrome))
    with pytest.raises(EvidenceContractError, match="chrome"):
        validate_output(_output(event), transcript_window=chrome, expected_window_id="w1")
