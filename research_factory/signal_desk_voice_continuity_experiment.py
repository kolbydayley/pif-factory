"""Source-bound voice corridor proposals. Never an attribution approval gate."""
import hashlib
import json

FAMILY_ID = "signal-desk-voice-continuity-v1"
RULES = """Resolve the person uttering the supplied candidate, not its proposition
owner. Preserve every candidate ID. For labeled turns use the explicit label and
the precise turn range. For an unlabelled monologue, a self-identification may
support an earlier or later passage ONLY when supplied source demonstrates one
continuous voice between that passage and the identification. Give the complete
corridor and exact identity anchor, with no hidden gaps. A host name in metadata,
someone else's introduction, alternation, a mentioned name, apparent writing
style, or the absence of printed labels does NOT establish this continuity.
Interview answers, inserted recordings, guest turns and ambiguous transitions
break a corridor. Stop at the boundary; if the candidate crosses it or continuity
is unresolved, leave the voice null. Do not turn a host's closing identification
into a blanket identity for an entire episode. Flat captions can contain several
voices even if their labels are missing.
A narrator reading a quotation remains the transcript voice; the quoted person's
proposition ownership stays separate. An actual inserted guest recording changes
the voice. If those possibilities cannot be distinguished, leave voice null.
Return exclusions for known inserted-voice ranges within any proposed corridor.
A candidate or identity anchor may not overlap exclusions, and continuity may
not bridge them. Mark indeterminate with a reason when the supplied source is
insufficient. Preserve ASR surface names; external corrected names need separately
verified aliases. These are proposals: a second independent source adjudicator
must confirm identity AND continuity before any assignment can become accepted.
Exact offsets alone do not prove that a corridor has one voice.
"""


def schema():
    span = {"type": "object", "additionalProperties": False,
        "required": ["text", "start", "end"], "properties": {
            "text": {"type": "string", "minLength": 1},
            "start": {"type": "integer", "minimum": 0},
            "end": {"type": "integer", "minimum": 1}}}
    properties = {
        "candidate_id": {"type": "string", "minLength": 1},
        "voice_surface": {"type": ["string", "null"]},
        "basis": {"type": "string", "enum": ["explicit_label", "self_identification_continuity", "indeterminate"]},
        "identity_anchor": {"anyOf": [span, {"type": "null"}]},
        "corridor": {"anyOf": [span, {"type": "null"}]},
        "excluded_voice_ranges": {"type": "array", "items": span},
        "rationale": {"type": "string", "minLength": 1}}
    return {"type": "object", "additionalProperties": False,
        "required": ["source_sha256", "window_id", "decisions"], "properties": {
            "source_sha256": {"type": "string"}, "window_id": {"type": "string"},
            "decisions": {"type": "array", "items": {"type": "object",
                "additionalProperties": False, "required": list(properties), "properties": properties}}}}


def _span(span, source):
    if not isinstance(span, dict) or set(span) != {"text", "start", "end"}:
        raise ValueError("invalid span")
    if type(span["start"]) is not int or type(span["end"]) is not int:
        raise ValueError("noninteger offsets")
    if not isinstance(span["text"], str) or not span["text"].strip() or not 0 <= span["start"] < span["end"] <= len(source) or source[span["start"]:span["end"]] != span["text"]:
        raise ValueError("inexact span")


def validate(value, *, source, window_id, candidates):
    """candidates maps IDs to immutable evidence spans from the input packet."""
    if not isinstance(source, str) or not isinstance(window_id, str) or not window_id.strip() or not isinstance(candidates, dict):
        raise ValueError("invalid inputs")
    for key, span in candidates.items():
        if not isinstance(key, str) or not key.strip():
            raise ValueError("invalid candidate id")
        _span(span, source)
    if not isinstance(value, dict) or set(value) != {"source_sha256", "window_id", "decisions"}:
        raise ValueError("invalid envelope")
    if value["source_sha256"] != hashlib.sha256(source.encode()).hexdigest() or value["window_id"] != window_id:
        raise ValueError("source lineage mismatch")
    if not isinstance(value["decisions"], list):
        raise ValueError("invalid decisions")
    fields = set(schema()["properties"]["decisions"]["items"]["required"])
    seen = set()
    for row in value["decisions"]:
        if not isinstance(row, dict) or set(row) != fields:
            raise ValueError("invalid fields")
        key = row["candidate_id"]
        if not isinstance(key, str) or key not in candidates or key in seen:
            raise ValueError("changed population")
        seen.add(key)
        if not isinstance(row["rationale"], str) or not row["rationale"].strip():
            raise ValueError("missing rationale")
        if row["basis"] not in ("explicit_label", "self_identification_continuity", "indeterminate"):
            raise ValueError("invalid basis")
        if not isinstance(row["excluded_voice_ranges"], list):
            raise ValueError("invalid exclusions")
        for excluded in row["excluded_voice_ranges"]:
            _span(excluded, source)
        if row["basis"] == "indeterminate":
            if any(row[field] is not None for field in ("voice_surface", "identity_anchor", "corridor")):
                raise ValueError("indeterminate cannot assign voice")
            continue
        if not isinstance(row["voice_surface"], str) or not row["voice_surface"].strip():
            raise ValueError("missing voice")
        anchor, corridor = row["identity_anchor"], row["corridor"]
        _span(anchor, source)
        _span(corridor, source)
        if " ".join(row["voice_surface"].casefold().split()) not in " ".join(anchor["text"].casefold().split()):
            raise ValueError("voice absent from anchor")
        for span in (anchor, candidates[key]):
            if not corridor["start"] <= span["start"] < span["end"] <= corridor["end"]:
                raise ValueError("corridor must cover anchor and candidate")
        for excluded in row["excluded_voice_ranges"]:
            if max(corridor["start"], excluded["start"]) < min(corridor["end"], excluded["end"]):
                raise ValueError("corridor crosses known voice boundary")
    if seen != set(candidates):
        raise ValueError("missing candidate")
    return value


def receipt():
    return {"family_id": FAMILY_ID,
        "sha256": hashlib.sha256(json.dumps({"rules": RULES, "schema": schema()}, sort_keys=True).encode()).hexdigest(),
        "changed_dimension": "scoped_transcript_voice_continuity_only",
        "qualified": False, "production_enabled": False,
        "independent_identity_and_continuity_review_required": True}
