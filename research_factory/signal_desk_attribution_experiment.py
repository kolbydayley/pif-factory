"""Unqualified attribution schema family; never imports into frozen v2 gold.

Byte grounding is necessary, not proof that an identity binding is semantically
valid. Every binding still requires independent source adjudication.
"""
import hashlib
import json

FAMILY_ID = "signal-desk-attribution-schema-v3-experiment"
RULES = """Separate the transcript voice from the owner of the MAIN proposition.
transcript_voice is the person uttering the transcript passage; leave null when
unlabelled. Never infer from metadata, alternation, self-introduction elsewhere,
or a name merely occurring in text. Identity spans must support the binding,
not merely contain the name. quoted_statement records an embedded quotation's
explicit owner independently of the narrator. reported_statement records an
explicit report of someone else's position; it is NOT a direct quote. An
unknown or unnamed owner remains null. own_statement has the same owner as the
transcript voice, including null. A narrator's own decision explained by others'
advice is still own_statement when the decision is the main proposition.
Mentions never become voice/owner bindings by name presence alone. Organizations
can own reports but never become people. Reported statements belong under
'Reported views', separate from 'Direct statements and quotations'.
Stance describes an explicit attitude toward the event's complete proposition.
Assertion alone, including a negative assertion, is neutral. Recommendation or
endorsement is supportive only when that evaluative act is expressed. Doubt is
skeptical; explicit caution is warning. Criticism is not automatically doubt.
Mixed requires multiple attitudes toward the same proposition. Context may
qualify a proposition but unrelated positive/negative words do not set stance.
This experiment is unqualified; schema validation never authorizes publication.
"""


def schema():
    span = {"type": "object", "additionalProperties": False,
        "required": ["text", "start", "end"], "properties": {
            "text": {"type": "string", "minLength": 1},
            "start": {"type": "integer", "minimum": 0},
            "end": {"type": "integer", "minimum": 1}}}
    identity = {"type": ["object", "null"], "additionalProperties": False,
        "required": ["surface_name", "kind", "binding_span"], "properties": {
            "surface_name": {"type": "string", "minLength": 1},
            "kind": {"type": "string", "enum": ["person", "organization"]},
            "binding_span": span}}
    return {"type": "object", "additionalProperties": False,
        "required": ["transcript_voice", "proposition_owner", "relation", "mentioned_entities"],
        "properties": {"transcript_voice": identity, "proposition_owner": identity,
            "relation": {"type": "string", "enum": ["own_statement", "quoted_statement", "reported_statement"]},
            "mentioned_entities": {"type": "array", "items": {
                "type": "object", "additionalProperties": False,
                "required": ["surface_name", "kind"], "properties": {
                    "surface_name": {"type": "string", "minLength": 1},
                    "kind": {"type": "string", "enum": ["person", "organization"]}}}}}}


def normalize(value):
    return " ".join(value.casefold().split())


def validate(attribution, *, source):
    if not isinstance(attribution, dict) or set(attribution) != {"transcript_voice", "proposition_owner", "relation", "mentioned_entities"}:
        raise ValueError("invalid attribution envelope")
    if attribution["relation"] not in {"own_statement", "quoted_statement", "reported_statement"}:
        raise ValueError("unknown attribution relation")
    mentions = attribution["mentioned_entities"]
    if not isinstance(mentions, list):
        raise ValueError("mentions must be an array")
    for entity in mentions:
        if not isinstance(entity, dict) or set(entity) != {"surface_name", "kind"}:
            raise ValueError("invalid mention")
        if not isinstance(entity["surface_name"], str) or not entity["surface_name"].strip() or entity["kind"] not in {"person", "organization"}:
            raise ValueError("invalid mention name or kind")
    for field in ("transcript_voice", "proposition_owner"):
        identity = attribution[field]
        if identity is None:
            continue
        if not isinstance(identity, dict) or set(identity) != {"surface_name", "kind", "binding_span"}:
            raise ValueError("invalid identity envelope")
        if not isinstance(identity["surface_name"], str) or not identity["surface_name"].strip() or identity["kind"] not in {"person", "organization"}:
            raise ValueError("invalid identity name or kind")
        span = identity["binding_span"]
        if not isinstance(span, dict) or set(span) != {"text", "start", "end"} or not isinstance(span["text"], str) or not span["text"]:
            raise ValueError("invalid identity span")
        if type(span["start"]) is not int or type(span["end"]) is not int:
            raise ValueError("identity span offsets must be integers")
        if not 0 <= span["start"] < span["end"] <= len(source) or source[span["start"]:span["end"]] != span["text"]:
            raise ValueError("identity binding span is not exact source")
        if normalize(identity["surface_name"]) not in normalize(span["text"]):
            raise ValueError("surface identity absent from binding span; aliases need separate provenance")
    voice = attribution["transcript_voice"]
    if voice and voice["kind"] != "person":
        raise ValueError("transcript voice must be a person, not a cited organization")
    if attribution["relation"] == "own_statement" and voice != attribution["proposition_owner"]:
        raise ValueError("own statement cannot assign a different proposition owner")
    return attribution


def proposed_person_section(attribution, surface_name):
    """Research routing proposal only; does not approve or publish an event."""
    owner = attribution["proposition_owner"]
    if owner and owner["kind"] == "person" and normalize(owner["surface_name"]) == normalize(surface_name):
        return "reported_views" if attribution["relation"] == "reported_statement" else "direct_statements_and_quotations"
    if any(e["kind"] == "person" and normalize(e["surface_name"]) == normalize(surface_name)
           for e in attribution["mentioned_entities"]):
        return "mentions"
    return None


def receipt():
    encoded = json.dumps({"rules": RULES, "schema": schema()}, sort_keys=True, separators=(",", ":"))
    return {"family_id": FAMILY_ID, "sha256": hashlib.sha256(encoded.encode()).hexdigest(),
        "changed_dimension": "attribution_schema_and_explicit_semantics",
        "qualified": False, "production_enabled": False,
        "source_adjudication_required": True, "frozen_v2_compatible": False}
