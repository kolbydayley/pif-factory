"""Shared scope prompt plus separately versioned packet-ID review envelope."""
from . import signal_desk_full_event_v4_review as parent
from . import signal_desk_full_event_v4_scope_prompts as author
from .signal_desk_rubric_reference_packets import digest

SYSTEM = parent.SYSTEM.replace(author.parent.COMMON, author.COMMON) + """
PACKET ASSIGNMENT: candidates is the complete list of records YOU must review in
this packet. Return only those event_ids, exactly once each. record_index is a
read-only context index across all packets; an indexed ID absent from candidates
is assigned elsewhere, NOT missing evidence, NOT an omission, and NOT your output
decision. candidate_population is the whole-source count, NOT this packet's count.
Do not recommend restoring other packets' records into this packet. Evaluate
coverage_notes only for substantive source omissions from the entire record_index,
not for normal packet boundaries. Do not infer that indexed candidate text is true.
"""


def schema(packet):
    result = parent.schema()
    ids = [e["event_id"] for e in packet["candidates"]]
    decisions = result["properties"]["decisions"]
    decisions["minItems"] = len(ids); decisions["maxItems"] = len(ids)
    if ids:
        decisions["items"]["properties"]["event_id"]["enum"] = ids
    return result


def packets(output, *, source, window_id, token_count):
    return parent.packets(output, source=source, window_id=window_id, token_count=token_count,
        system=SYSTEM, schema_for_packet=schema)


def validate_review(value, packet):
    parent._shape(value, schema(packet))
    return parent.validate_review(value, packet)


def receipt():
    return {"family": "full-event-v4-explicit-packet-review-v1", "author_prompt": author.receipt(),
        "system_sha256": digest(SYSTEM), "parent": parent.receipt(),
        "changed_dimension": "explicit_packet_assignment_and_dynamic_id_envelope_only",
        "semantic_acceptance_gate_changed": False, "qualified": False, "gold_accepted": False}
