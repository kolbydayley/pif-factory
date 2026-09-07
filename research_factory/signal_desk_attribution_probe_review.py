"""Source-grounded final-authority review contract for attribution diagnostics."""
from .signal_desk_attribution_contrasts import SYSTEM, response_schema, validate_response
from .signal_desk_rubric_reference_packets import digest

REVIEW_SYSTEM = """You are GPT-5.5 reviewing experimental attribution labels.
Inspect each original claim anchor, its complete source window, and the proposed
SOL decision. The proposal is not authority. For each event return supported,
corrected, or unresolved; a complete proposed decision; and a source-grounded
review rationale. supported must preserve the original decision exactly.
corrected must change a substantive field, not just reword a rationale.
unresolved is mandatory when source cannot settle the identity or claim; do
not invent names. A null narrator can be the CORRECT resolved answer, not a
reason to invent one. Unsupported claims can also be correctly classified.
Corrected decisions are review proposals, not automatic approved gold. This is
a bounded schema experiment, not an extraction recall or corpus reliability gate.
The following independent labeling instructions and contract also govern you:
""" + SYSTEM


def schema(packet):
    decision_schema = response_schema(packet)["properties"]["decisions"]["items"]
    return {"type": "object", "additionalProperties": False, "required": ["reviews"], "properties": {
        "reviews": {"type": "array", "minItems": len(packet["anchors"]), "maxItems": len(packet["anchors"]),
            "items": {"type": "object", "additionalProperties": False,
                "required": ["verdict", "decision", "review_rationale"], "properties": {
                    "verdict": {"type": "string", "enum": ["supported", "corrected", "unresolved"]},
                    "decision": decision_schema, "review_rationale": {"type": "string", "minLength": 1}}}}}}


def validate_review(value, packet, proposed):
    validate_response(proposed, packet)
    if not isinstance(value, dict) or set(value) != {"reviews"} or not isinstance(value["reviews"], list):
        raise ValueError("invalid attribution review envelope")
    originals = {d["event_id"]: d for d in proposed["decisions"]}
    decisions = []
    for row in value["reviews"]:
        if not isinstance(row, dict) or set(row) != {"verdict", "decision", "review_rationale"}:
            raise ValueError("invalid attribution review row")
        if row["verdict"] not in {"supported", "corrected", "unresolved"} or not isinstance(row["review_rationale"], str) or not row["review_rationale"].strip():
            raise ValueError("invalid review verdict or rationale")
        decision = row["decision"]
        if not isinstance(decision, dict) or not isinstance(decision.get("event_id"), str):
            raise ValueError("invalid review decision")
        original = originals.get(decision["event_id"])
        if original is None: raise ValueError("review event outside original anchors")
        if row["verdict"] == "supported" and decision != original:
            raise ValueError("supported review cannot silently rewrite labels")
        meaningful = lambda d: {k: v for k, v in d.items() if k != "source_rationale"}
        if row["verdict"] == "corrected" and meaningful(decision) == meaningful(original):
            raise ValueError("correction must change substantive label")
        decisions.append(decision)
    validate_response({"decisions": decisions}, packet)
    return value


def build_packet(packet, proposed):
    validate_response(proposed, packet)
    result = {"original_packet": packet, "proposed_labels": proposed,
        "proposal_sha256": digest(proposed), "review_system_sha256": digest(REVIEW_SYSTEM),
        "qualified": False, "gold_accepted": False}
    result["review_packet_sha256"] = digest(result)
    return result
