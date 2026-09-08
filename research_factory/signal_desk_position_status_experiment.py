"""Experimental actual/anticipated position distinction; not a truth verdict."""
import hashlib
import json

FAMILY_ID = "signal-desk-position-status-v1"
RULES = """Classify the provenance of each candidate proposition, not its truth.
Preserve every supplied candidate ID exactly once. Use actual_position when the
source presents the position as actually expressed, including the current
speaker's own position. This does NOT establish the proposition as true.
Use anticipated_position for a predicted, imagined or hypothetical objection or
quotation, even if the narrator invents an exact quote or names a likely speaker.
Use indeterminate when context does not settle whether anyone expressed it.
Explicitly quoted actual speech and reports of actual views can qualify as actual
positions; they still require their separate attribution/reliability checks.
An allegation presented as actually made is an actual position, NOT an established
fact. 'Reportedly' alone does not establish a distinct proposition owner.
Do not turn 'people will say' into an observed consensus, a second credible voice,
or evidence that opponents actually hold that view. Preserve the hypothetical
argument for research without giving it observed-source breadth.
Supply exact source evidence supporting this classification. Missing context must
remain indeterminate; never infer actuality from external familiarity.
Outputs are unqualified proposals. No output approves an event or a named speaker.
"""


def schema():
    span = {"type": "object", "additionalProperties": False,
        "required": ["text", "start", "end"], "properties": {
            "text": {"type": "string", "minLength": 1},
            "start": {"type": "integer", "minimum": 0},
            "end": {"type": "integer", "minimum": 1}}}
    properties = {
        "candidate_id": {"type": "string", "minLength": 1},
        "position_status": {"type": "string", "enum": ["actual_position", "anticipated_position", "indeterminate"]},
        "source_evidence": {"type": "array", "items": span, "minItems": 1},
        "rationale": {"type": "string", "minLength": 1}}
    return {"type": "object", "additionalProperties": False,
        "required": ["decisions"], "properties": {"decisions": {"type": "array",
            "items": {"type": "object", "additionalProperties": False,
                "required": list(properties), "properties": properties}}}}


def validate(value, *, source, candidate_ids):
    if not isinstance(source, str) or any(not isinstance(x, str) or not x.strip() for x in candidate_ids) or len(set(candidate_ids)) != len(candidate_ids):
        raise ValueError("invalid inputs")
    if not isinstance(value, dict) or set(value) != {"decisions"} or not isinstance(value["decisions"], list):
        raise ValueError("invalid envelope")
    seen = set()
    for row in value["decisions"]:
        if not isinstance(row, dict) or set(row) != {"candidate_id", "position_status", "source_evidence", "rationale"}:
            raise ValueError("invalid decision fields")
        for key in ("candidate_id", "position_status", "rationale"):
            if not isinstance(row[key], str) or not row[key].strip():
                raise ValueError("invalid string")
        if row["candidate_id"] not in candidate_ids or row["candidate_id"] in seen:
            raise ValueError("changed candidate population")
        seen.add(row["candidate_id"])
        if row["position_status"] not in {"actual_position", "anticipated_position", "indeterminate"}:
            raise ValueError("invalid position status")
        if not isinstance(row["source_evidence"], list) or not row["source_evidence"]:
            raise ValueError("missing source evidence")
        for span in row["source_evidence"]:
            if not isinstance(span, dict) or set(span) != {"text", "start", "end"}:
                raise ValueError("invalid evidence span")
            if type(span["start"]) is not int or type(span["end"]) is not int:
                raise ValueError("noninteger offsets")
            if not isinstance(span["text"], str) or not span["text"].strip() or not 0 <= span["start"] < span["end"] <= len(source) or source[span["start"]:span["end"]] != span["text"]:
                raise ValueError("inexact evidence")
    if seen != set(candidate_ids):
        raise ValueError("missing candidate")
    return value


def proposed_observed_position_eligible(row):
    """Necessary condition only. Separate attribution and approval still required."""
    return row.get("position_status") == "actual_position"


def receipt():
    return {"family_id": FAMILY_ID,
        "sha256": hashlib.sha256(json.dumps({"rules": RULES, "schema": schema()}, sort_keys=True).encode()).hexdigest(),
        "changed_dimension": "actual_versus_anticipated_positions_only",
        "qualified": False, "production_enabled": False,
        "semantic_source_review_required": True}
