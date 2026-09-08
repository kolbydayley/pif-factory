"""One shared v5 semantic contract for independent authors and approval review."""
from . import signal_desk_full_event_v4_scope_prompts as scope
from . import signal_desk_full_event_v5 as contract
from .signal_desk_rubric_reference_packets import digest

COMMON = scope.COMMON.replace("full-event v4", "full-event v5") + "\n" + contract.CONTEXT_RULES


def prompts():
    return {role: text + "\n" + COMMON for role, text in scope.parent.ROLE_TEXT.items()}


def packet(source_packet, role, *, author_a=None, author_b=None):
    if role not in {"A", "B", "C", "AUDIT"}: raise ValueError("invalid role")
    if role != "C" and (author_a is not None or author_b is not None): raise ValueError("independent roles cannot see answers")
    sha = source_packet["packet_sha256"]
    if digest({k:v for k,v in source_packet.items() if k != "packet_sha256"}) != sha: raise ValueError("source lineage changed")
    source = source_packet["transcript_window"]; wid = source_packet["window_id"]
    p = {"window_id": wid, "transcript_window": source, "transcript_structure": source_packet["transcript_structure"],
        "schema_version": contract.VERSION, "role": role, "original_source_packet_sha256": sha,
        "system_sha256": digest(prompts()[role]), "schema_sha256": digest(contract.schema())}
    if role == "C":
        if author_a is None or author_b is None: raise ValueError("both independent authors required")
        p["author_a"] = contract.validate(author_a, source=source, window_id=wid)
        p["author_b"] = contract.validate(author_b, source=source, window_id=wid)
    p["packet_sha256"] = digest(p)
    return p


def receipt():
    return {"family": "full-event-v5-context-all-role-qualification", "contract": contract.receipt(),
        "scope_prompt_family": scope.receipt(), "common_rules_sha256": digest(COMMON),
        "role_hashes": {r:digest(s) for r,s in prompts().items()}, "schema_sha256": digest(contract.schema()),
        "qualified": False, "gold_accepted": False, "comparable_to_v4_without_recalibration": False}
