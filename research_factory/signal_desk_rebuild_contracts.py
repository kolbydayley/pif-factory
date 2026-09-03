"""Shared gold/GLM/GPT-5.5 evidence contract for the clean rebuild."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Mapping, Sequence


SCHEMA_VERSION = "pif_signal_desk_clean_event_v2"
ATTRIBUTION_TYPES = (
    "direct_speech",
    "quoted_speech",
    "reported_paraphrase",
    "third_party_mention",
    "unresolved_speaker",
)
SPEECH_ACTS = (
    "assertion",
    "forecast",
    "explanation",
    "recommendation",
    "commitment",
    "disagreement",
    "reported_position",
)
PUBLISHABILITY_STATES = ("candidate", "accepted", "uncertain", "quarantined")
RELATIONSHIP_TYPES = (
    "drives",
    "constrains",
    "competes_with",
    "evidence_for",
    "evidence_against",
)
# Page chrome, ad reads and navigation boilerplate that must never be cited as
# evidence.  Every alternative is anchored to a boilerplate FORM: a call-to-
# action tail, title-case button text, or a nav/legal phrase.  Bare noun
# phrases are deliberately absent - on 2026-09-03 the bare alternatives
# ``sign up`` and ``privacy policy`` rejected genuine speech ("subscriptions
# they didn't sign up for", "get students to sign up", "the privacy policy
# makes no mention of the new tracking") and quarantined four validation
# windows.  Speech that merely DISCUSSES signing up or a privacy policy is
# evidence; a button or footer that SAYS it is chrome.
#
# MONOTONIC CONTRACT: this pattern must only ever ACCEPT more than its
# predecessor, never reject more.  The runner re-validates every previously
# accepted output at phase start, so any new rejection crashes the campaign
# at startup (it did, on 2026-09-03, when a "terms of service" form was added
# here: seven accepted excerpts of genuine speech began failing).  Every
# alternative below is either byte-identical to the previous pattern or a
# strict subset of the bare phrase it replaces; the tests assert this against
# the old pattern and against the accepted corpus on disk.
_CHROME_RE = re.compile(
    r"\b(?:subscribe\s+(?:now|today|for\b|to\s+(?:hear|listen|watch|read|support))|"
    # was: sign up  (bare) -> call-to-action tails or title-case button text only
    r"sign up (?:now|today|free|here|at \S+\.\S+|on substack|"
    r"for (?:free|our\b|the (?:newsletter|podcast|show)|(?:our |the )?"
    r"(?:newsletter|e-?mails?|updates|alerts|summary|notifications)))|"
    r"(?-i:Sign Up)(?= ?(?:$|[|·•/]|By submitting|On Substack|Log ?in|close\b))|"
    # was: cookie policy / privacy policy (bare) -> title case, or followed by nav/legal glue
    r"(?-i:Cookie Policy)|cookie policy(?= ?(?:[|·•]|and terms|terms\b))|"
    r"(?-i:Privacy Policy)|privacy policy(?= ?(?:[|·•]|and terms|terms\b))|"
    # was: all episodes (bare) -> title case, or a browse verb
    r"(?-i:All Episodes)|(?:see|view|browse) all episodes|"
    r"this (?:episode|show) is sponsored by|"
    r"brought to you by|navigation menu|skip to content)\b",
    re.I,
)

# The predecessor pattern, kept verbatim so the monotonicity test can prove
# that nothing the old rule accepted is rejected by the new one.
_CHROME_RE_PREVIOUS = re.compile(
    r"\b(?:subscribe\s+(?:now|today|for\b|to\s+(?:hear|listen|watch|read|support))|"
    r"sign up|cookie policy|privacy policy|all episodes|this (?:episode|show) is sponsored by|"
    r"brought to you by|navigation menu|skip to content)\b",
    re.I,
)


class EvidenceContractError(ValueError):
    pass


def event_schema() -> dict[str, Any]:
    """Return the exact structured-output schema shared by gold and GLM."""

    event = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "event_id",
            "claim_text",
            "speech_act",
            "evidence_text",
            "evidence_start",
            "evidence_end",
            "speaker_id",
            "quoted_person_id",
            "mentioned_person_ids",
            "attribution_type",
            "attribution_confidence",
            "issue_label",
            "issue_aliases",
            "stance",
            "publishability_state",
        ],
        "properties": {
            "event_id": {"type": "string", "minLength": 1},
            "claim_text": {"type": "string", "minLength": 1},
            "speech_act": {"type": "string", "enum": list(SPEECH_ACTS)},
            "evidence_text": {"type": "string", "minLength": 1},
            "evidence_start": {"type": "integer", "minimum": 0},
            "evidence_end": {"type": "integer", "minimum": 1},
            "speaker_id": {"type": ["string", "null"]},
            "quoted_person_id": {"type": ["string", "null"]},
            "mentioned_person_ids": {
                "type": "array",
                "items": {"type": "string", "minLength": 1},
            },
            "attribution_type": {"type": "string", "enum": list(ATTRIBUTION_TYPES)},
            "attribution_confidence": {"type": "number", "minimum": 0, "maximum": 1},
            "issue_label": {"type": "string", "minLength": 1},
            "issue_aliases": {
                "type": "array",
                "items": {"type": "string", "minLength": 1},
            },
            "stance": {
                "type": "string",
                "enum": ["supportive", "skeptical", "neutral", "mixed", "warning", "unknown"]
            },
            "publishability_state": {
                "type": "string",
                "enum": list(PUBLISHABILITY_STATES),
            },
        },
    }
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": SCHEMA_VERSION,
        "type": "object",
        "additionalProperties": False,
        "required": ["schema_version", "window_id", "window_disposition", "events"],
        "properties": {
            "schema_version": {"type": "string", "const": SCHEMA_VERSION},
            "window_id": {"type": "string", "minLength": 1},
            "window_disposition": {
                "type": "string",
                "enum": ["claims_found", "no_consequential_claims", "unusable_input"]
            },
            "events": {"type": "array", "items": event},
        },
    }


def contract_sha256() -> str:
    payload = json.dumps(event_schema(), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()


def evidence_dedupe_key(
    *, source_id: str, evidence_start: int, evidence_end: int, claim_text: str
) -> str:
    normalized_claim = " ".join(claim_text.casefold().split())
    body = f"{source_id}\0{evidence_start}\0{evidence_end}\0{normalized_claim}"
    return hashlib.sha256(body.encode()).hexdigest()


def validate_event(
    event: Mapping[str, Any],
    *,
    transcript_window: str,
    allow_accepted: bool = False,
) -> dict[str, Any]:
    """Validate grounding and role separation beyond JSON Schema shape."""

    required = set(event_schema()["properties"]["events"]["items"]["required"])
    missing = sorted(required - event.keys())
    if missing:
        raise EvidenceContractError(f"event missing fields: {', '.join(missing)}")
    try:
        start, end = int(event["evidence_start"]), int(event["evidence_end"])
    except (TypeError, ValueError) as exc:
        raise EvidenceContractError("evidence offsets must be integers") from exc
    evidence = str(event["evidence_text"])
    if start < 0 or end <= start or end > len(transcript_window):
        raise EvidenceContractError("evidence offsets are outside the transcript window")
    if transcript_window[start:end] != evidence:
        raise EvidenceContractError("evidence text is not exact at the declared offsets")
    if _CHROME_RE.search(evidence):
        raise EvidenceContractError("evidence contains chrome, advertising, or navigation text")
    attribution = str(event["attribution_type"])
    if attribution not in ATTRIBUTION_TYPES:
        raise EvidenceContractError("unknown attribution type")
    speaker = event.get("speaker_id")
    quoted = event.get("quoted_person_id")
    mentioned = event.get("mentioned_person_ids")
    if not isinstance(mentioned, Sequence) or isinstance(mentioned, (str, bytes)):
        raise EvidenceContractError("mentioned_person_ids must be an array")
    if len(list(mentioned)) != len(set(str(item) for item in mentioned)):
        raise EvidenceContractError("mentioned_person_ids must be unique")
    if attribution in {"direct_speech", "quoted_speech", "reported_paraphrase"} and not speaker:
        raise EvidenceContractError("speech attribution requires the actual speaker")
    if attribution == "quoted_speech" and not quoted:
        raise EvidenceContractError("quoted speech requires quoted_person_id")
    if attribution == "third_party_mention" and not mentioned:
        raise EvidenceContractError("third-party mention requires mentioned_person_ids")
    if attribution == "unresolved_speaker" and speaker is not None:
        raise EvidenceContractError("unresolved speaker cannot carry a guessed speaker_id")
    state = str(event["publishability_state"])
    if state not in PUBLISHABILITY_STATES:
        raise EvidenceContractError("unknown publishability state")
    if state == "accepted" and not allow_accepted:
        raise EvidenceContractError("workhorse/gold packets cannot self-approve publication")
    if state == "accepted" and attribution == "unresolved_speaker":
        raise EvidenceContractError("unresolved-speaker evidence cannot be public")
    confidence = float(event["attribution_confidence"])
    if not 0.0 <= confidence <= 1.0:
        raise EvidenceContractError("attribution confidence must be in [0, 1]")
    if event["speech_act"] not in SPEECH_ACTS:
        raise EvidenceContractError("questions and unsupported speech acts are not claims")
    aliases = event.get("issue_aliases")
    if not isinstance(aliases, Sequence) or isinstance(aliases, (str, bytes)):
        raise EvidenceContractError("issue_aliases must be an array")
    if len(list(aliases)) != len(set(str(item) for item in aliases)):
        raise EvidenceContractError("issue_aliases must be unique")
    result = dict(event)
    result["evidence_start"] = start
    result["evidence_end"] = end
    return result


def validate_output(
    output: Mapping[str, Any],
    *,
    transcript_window: str,
    expected_window_id: str,
    allow_accepted: bool = False,
) -> dict[str, Any]:
    if output.get("schema_version") != SCHEMA_VERSION:
        raise EvidenceContractError("schema version mismatch")
    if output.get("window_id") != expected_window_id:
        raise EvidenceContractError("window identity mismatch")
    events = output.get("events")
    if not isinstance(events, list):
        raise EvidenceContractError("events must be an array")
    disposition = output.get("window_disposition")
    if disposition == "claims_found" and not events:
        raise EvidenceContractError("claims_found requires at least one event")
    if disposition != "claims_found" and events:
        raise EvidenceContractError("non-claim disposition cannot contain events")
    validated = [
        validate_event(event, transcript_window=transcript_window, allow_accepted=allow_accepted)
        for event in events
    ]
    event_ids = [str(event["event_id"]) for event in validated]
    if len(event_ids) != len(set(event_ids)):
        raise EvidenceContractError("event_id values must be unique within a window")
    return {**dict(output), "events": validated}


def eligible_for_person_says(event: Mapping[str, Any], person_id: str) -> bool:
    """Prevent third-party mentions from becoming a person's own claims."""

    attribution = event.get("attribution_type")
    return bool(
        event.get("publishability_state") == "accepted"
        and (
            (attribution in {"direct_speech", "reported_paraphrase"} and event.get("speaker_id") == person_id)
            or (attribution == "quoted_speech" and event.get("quoted_person_id") == person_id)
        )
    )
