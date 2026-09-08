"""Isolated utility-role experiment. Never approves or mutates frozen gold."""
import hashlib
import json

FAMILY_ID = "signal-desk-evidence-role-v1"
ROLES = ("substantive_claim", "supporting_context", "voice_source_metadata",
         "promotion_housekeeping", "research_limitation")
RULES = """Classify research usefulness independently of factual fidelity and attribution.
Assign every supplied candidate exactly once; never omit an inconvenient candidate.
substantive_claim: a specific view, mechanism, attributed development, comparison,
forecast, constraint or decision criterion. Concrete firsthand workflows and
willingness-to-pay can qualify, but are anecdotes, not prevalence estimates.
supporting_context: examples, caveats, historical baselines and definitions linked
to a substantive candidate. Preserve counterbalancing caveats; do not atomize
each example into another independent claim or source. Use only supplied IDs.
voice_source_metadata: expertise, roles, research focus and bibliographic pointers;
useful for credibility or navigation, not independently an industry change.
promotion_housekeeping: schedules, invitations, sponsor pitches, generic show
mission/feedback promises, greetings and filler. Do not discard a mixed window.
A concrete product capability from its founder may be an interested-party claim;
founder identity alone does not make it promotion. Mark interested-party scope.
research_limitation: source too clipped, garbled or unresolved to support a useful
claim. Specify wider_context or audio_or_source as needed; never invent a name.
If evidence is adequate but usefulness is debatable, use boundary and explain why;
do not disguise usefulness disagreement as corrupt source or factual error.
Narrative examples can illuminate mechanisms without establishing population facts.
Host opinions are their own; question premises are not the guest's statements.
Reported hearsay must retain no-firsthand-use caveats and cannot create new sources.
Bare maxims or vague change language do not lead briefs. Preserve them as context
only when they qualify an identifiable substantive argument.
Output is an experimental proposal only. Never emit accepted or qualified status.
"""


def schema():
    item = {"type": "object", "additionalProperties": False, "properties": {
        "candidate_id": {"type": "string", "minLength": 1},
        "role": {"type": "string", "enum": list(ROLES)},
        "decision": {"type": "string", "enum": ["proposed", "boundary"]},
        "context_for": {"type": "array", "items": {"type": "string"}, "uniqueItems": True},
        "scope": {"type": "string", "enum": ["firsthand_account", "attributed_view",
            "reported_hearsay", "interested_party", "not_applicable"]},
        "needs": {"type": "string", "enum": ["none", "wider_context", "audio_or_source"]},
        "rationale": {"type": "string", "minLength": 1}}}
    item["required"] = list(item["properties"])
    return {"type": "object", "additionalProperties": False,
        "required": ["decisions"], "properties": {
            "decisions": {"type": "array", "items": item}}}


def validate(value, candidate_ids):
    """Contract/lineage validation only, not semantic approval."""
    if not isinstance(value, dict) or set(value) != {"decisions"} or not isinstance(value["decisions"], list):
        raise ValueError("invalid role envelope")
    properties = schema()["properties"]["decisions"]["items"]["properties"]
    for row in value["decisions"]:
        if not isinstance(row, dict) or set(row) != set(properties):
            raise ValueError("invalid role decision")
        for key, spec in properties.items():
            item = row[key]
            if spec["type"] == "string":
                if not isinstance(item, str) or not item.strip():
                    raise ValueError("invalid string field")
                if "enum" in spec and item not in spec["enum"]:
                    raise ValueError("unknown enum value")
            elif not isinstance(item, list) or any(not isinstance(x, str) or not x.strip() for x in item) or len(item) != len(set(item)):
                raise ValueError("invalid unique ID array")
    if any(not isinstance(x, str) or not x.strip() for x in candidate_ids):
        raise ValueError("invalid candidate ID")
    expected = set(candidate_ids)
    if len(expected) != len(candidate_ids):
        raise ValueError("duplicate input IDs")
    rows = value["decisions"]
    by_id = {row["candidate_id"]: row for row in rows}
    if len(by_id) != len(rows) or set(by_id) != expected:
        raise ValueError("candidate population changed")
    for row in rows:
        if not row["rationale"].strip():
            raise ValueError("empty rationale")
        links = row["context_for"]
        if row["role"] == "supporting_context":
            if not links:
                raise ValueError("context requires a substantive parent")
            for parent in links:
                if parent not in by_id or parent == row["candidate_id"]:
                    raise ValueError("unknown or self context link")
                if by_id[parent]["role"] != "substantive_claim":
                    raise ValueError("context parent must be substantive")
        elif links:
            raise ValueError("only context has parent links")
        if (row["role"] == "research_limitation") != (row["needs"] != "none"):
            raise ValueError("source limitation must specify recovery evidence")
    return value


def receipt():
    payload = {"rules": RULES, "schema": schema()}
    return {"family_id": FAMILY_ID,
        "sha256": hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest(),
        "changed_dimension": "research_usefulness_roles_only",
        "qualified": False, "production_enabled": False,
        "independent_source_review_required": True,
        "preserves_input_population": True}
