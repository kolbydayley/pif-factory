"""Unqualified one-change prompt variant: make existing voice-span scope explicit."""
from . import signal_desk_full_event_v4_prompts as parent
from . import signal_desk_full_event_v4 as contract
from .signal_desk_rubric_reference_packets import digest

FAMILY = "full-event-v4-voice-evidence-scope-v1"
OLD = "In a labeled turn its explicit label/turn\nboundary may support continuity."
NEW = """In a labeled turn its own explicit speaker label may support continuity.
Every continuity_evidence span must lie wholly INSIDE the proposed corridor.
The next speaker's label marks where this corridor ends; it is NOT evidence
inside this voice and must NOT appear in continuity_evidence. Explain that
boundary in rationale, without expanding the corridor across it. A corridor
ending at the next label's start excludes that label (half-open intervals).
Likewise, an event assigned to this voice must have its entire evidence excerpt
inside that voice's corridor. Do not include an interviewer's question in an
answerer's spoken excerpt. Preserve the question's meaning using the supplied
full-source context and a useful linked context record where appropriate. If
single-voice evidence cannot preserve the proposition faithfully, retain the
limitation rather than inventing a wider single-speaker corridor or dropping
the qualifier. These instructions clarify existing checks, not new labels."""
if parent.COMMON.count(OLD) != 1:
    raise RuntimeError("parent prompt changed; rebase the explicit variant")
COMMON = parent.COMMON.replace(OLD, NEW)


def prompts():
    return {role: text + "\n" + COMMON for role, text in parent.ROLE_TEXT.items()}


def packet(source_packet, role, *, author_a=None, author_b=None):
    p = parent.packet(source_packet, role, author_a=author_a, author_b=author_b)
    p.pop("packet_sha256")
    p["system_sha256"] = digest(prompts()[role])
    p["prompt_family"] = FAMILY
    p["packet_sha256"] = digest(p)
    return p


def receipt():
    return {"family": FAMILY, "parent": parent.receipt(),
        "changed_dimension": "explicit_existing_voice_evidence_scope_only",
        "replacement": {"before": OLD, "after": NEW}, "common_rules_sha256": digest(COMMON),
        "role_hashes": {r: digest(s) for r,s in prompts().items()},
        "schema_sha256": digest(contract.schema()), "qualified": False,
        "validator_changed": False, "offset_rules_changed": False,
        "requires_fresh_all_role_qualification": True}
