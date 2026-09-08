"""Full-source, packet-assigned review of distinct evidence and context."""
from . import signal_desk_full_event_v4_review as base
from . import signal_desk_full_event_v4_scope_review as scope
from . import signal_desk_full_event_v5_prompts as author
from .signal_desk_rubric_reference_packets import digest

SYSTEM = scope.SYSTEM.replace(author.scope.COMMON, author.COMMON)
schema = scope.schema
validate_review = scope.validate_review


def packets(output, *, source, window_id, token_count):
    return base.packets(output, source=source, window_id=window_id, token_count=token_count,
        system=SYSTEM, schema_for_packet=schema, output_validator=author.contract.validate)


def receipt():
    return {"family": "full-event-v5-context-final-review", "system_sha256": digest(SYSTEM),
        "author_prompt": author.receipt(), "packet_envelope": scope.receipt(),
        "qualified": False, "gold_accepted": False, "requires_source_reconciliation": True}
