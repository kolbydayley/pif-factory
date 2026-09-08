"""Target-specific attitude experiment; independent of frozen stance labels."""
import hashlib
import json

FAMILY_ID = "signal-desk-target-attitude-v1"
RULES = """Separate endorsement of a proposition from attitude toward its target.
Return one decision for EVERY supplied candidate, retaining its ID.
proposition_status describes the speaker's treatment of the complete proposition:
asserted, denied, questioned, hypothetical, or indeterminate. Reported/quoted
positions are evaluated for their proposition owner, not silently for the narrator.
attitude describes evaluation of an explicit target: positive, negative, mixed,
neutral, or indeterminate. Negative criticism is not the same as epistemic doubt.
Name the target using an exact source span, not an invented inferred entity.
For negative/positive/mixed attitude, include exact source evidence of evaluation.
Opposition to factory farming is negative toward factory farming even though the
speaker asserts that opposition. Calling a company worst is negative toward that
company, not supportive merely because the sentence is confidently asserted.
Criticism of judgment size targets the judgments, not automatically the defendant.
Praise followed by a factual limitation is not automatically mixed. Mixed requires
positive and negative evaluations of the SAME target. Different targets should not
be collapsed into one polarity. If one candidate cannot represent the distinctions,
mark split_required and explain; do not manufacture new candidate IDs.
A neutral factual statement may mention adverse events without evaluating them.
Rhetorical critical comparisons can convey negative attitude; distinguish that
from skepticism about the underlying proposition's truth. Explicit uncertainty
belongs in epistemic: certain, hedged, or indeterminate. This is expressed modality,
not the model's confidence and not a factual truth judgment.
If target, stance or ownership cannot be resolved from the supplied source and
candidate, use indeterminate and needs_review. Never fabricate a speaker or target.
All outputs are experimental proposals; no acceptance or production authorization.
"""


def schema():
    span = {"type": "object", "additionalProperties": False,
        "required": ["text", "start", "end"], "properties": {
            "text": {"type": "string", "minLength": 1},
            "start": {"type": "integer", "minimum": 0},
            "end": {"type": "integer", "minimum": 1}}}
    properties = {
        "candidate_id": {"type": "string", "minLength": 1},
        "proposition_status": {"type": "string", "enum": ["asserted", "denied", "questioned", "hypothetical", "indeterminate"]},
        "epistemic": {"type": "string", "enum": ["certain", "hedged", "indeterminate"]},
        "attitude": {"type": "string", "enum": ["positive", "negative", "mixed", "neutral", "indeterminate"]},
        "target": {"anyOf": [span, {"type": "null"}]},
        "evaluation_evidence": {"type": "array", "items": span},
        "status": {"type": "string", "enum": ["proposed", "needs_review", "split_required"]},
        "rationale": {"type": "string", "minLength": 1}}
    return {"type": "object", "additionalProperties": False,
        "required": ["decisions"], "properties": {"decisions": {"type": "array",
            "items": {"type": "object", "additionalProperties": False,
                "required": list(properties), "properties": properties}}}}


def _span(value, source):
    if not isinstance(value, dict) or set(value) != {"text", "start", "end"}:
        raise ValueError("invalid span")
    if type(value["start"]) is not int or type(value["end"]) is not int:
        raise ValueError("noninteger offsets")
    if not isinstance(value["text"], str) or not value["text"].strip():
        raise ValueError("empty span")
    if not 0 <= value["start"] < value["end"] <= len(source) or source[value["start"]:value["end"]] != value["text"]:
        raise ValueError("span not exact source")


def validate(value, *, source, candidate_ids):
    """Validate shape, population and grounding, never the model's semantics."""
    if not isinstance(value, dict) or set(value) != {"decisions"} or not isinstance(value["decisions"], list):
        raise ValueError("invalid envelope")
    if not isinstance(source, str) or any(not isinstance(x, str) or not x.strip() for x in candidate_ids) or len(set(candidate_ids)) != len(candidate_ids):
        raise ValueError("invalid inputs")
    properties = schema()["properties"]["decisions"]["items"]["properties"]
    seen = set()
    for row in value["decisions"]:
        if not isinstance(row, dict) or set(row) != set(properties):
            raise ValueError("invalid decision fields")
        for key, spec in properties.items():
            if spec.get("type") == "string":
                if not isinstance(row[key], str) or not row[key].strip() or ("enum" in spec and row[key] not in spec["enum"]):
                    raise ValueError("invalid string or enum")
        key = row["candidate_id"]
        if key not in candidate_ids or key in seen:
            raise ValueError("changed candidate population")
        seen.add(key)
        if row["target"] is not None:
            _span(row["target"], source)
        if not isinstance(row["evaluation_evidence"], list):
            raise ValueError("invalid evidence array")
        for evidence in row["evaluation_evidence"]:
            _span(evidence, source)
        if row["attitude"] in {"positive", "negative", "mixed"} and (row["target"] is None or not row["evaluation_evidence"]):
            raise ValueError("evaluative attitude requires grounded target and evidence")
        if "indeterminate" in (row["attitude"], row["epistemic"], row["proposition_status"]) and row["status"] == "proposed":
            raise ValueError("indeterminacy requires review")
    if seen != set(candidate_ids):
        raise ValueError("missing candidate")
    return value


def receipt():
    return {"family_id": FAMILY_ID,
        "sha256": hashlib.sha256(json.dumps({"rules": RULES, "schema": schema()}, sort_keys=True).encode()).hexdigest(),
        "changed_dimension": "target_attitude_and_expressed_modality_only",
        "qualified": False, "production_enabled": False,
        "semantic_source_review_required": True}
