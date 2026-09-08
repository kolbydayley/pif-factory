"""Separate source context from attributed spoken evidence; experimental only."""
from copy import deepcopy
from . import signal_desk_full_event_v4 as parent
from .signal_desk_voice_continuity_experiment import _span
from .signal_desk_rubric_reference_packets import digest

VERSION = "pif_signal_desk_semantic_records_v5"
CONTEXT_RULES = """CONTEXT EVIDENCE: context_evidence contains exact source spans needed
to understand the record's spoken excerpt: a preceding question, an antecedent,
a consequential qualifier, or other supporting context. Give each a purpose.
These spans can contain OTHER speakers and do not inherit transcript_voice or
proposition_owner. Never display them as that person's quotation or count them
as additional claims or independent sources. evidence_text remains the actual
excerpt attributable to its own voice; context_evidence supplies meaning without
widening that voice's corridor. A short answer may support a contextualized claim
ONLY when the necessary question/antecedent is supplied here and the answer
really affirms it. Do not turn a leading question into an affirmed answer or a
new view. Include consequential qualifying context, not just helpful premises.
Keep context_evidence empty when evidence_text is sufficient. Do not duplicate
the spoken excerpt there or copy the entire window indiscriminately. The full
window remains available for independent review. Context cannot establish an
unknown speaker's identity, resolve missing audio, or justify unsupported claims.
Review meaning using evidence_text together with explicitly cited context; do
not demand that another person's question be merged into spoken evidence.
"""


def schema():
    result = deepcopy(parent.schema())
    result["title"] = VERSION; result["properties"]["schema_version"]["const"] = VERSION
    item = result["properties"]["events"]["items"]
    item["properties"]["context_evidence"] = {"type": "array", "items": {
        "type": "object", "additionalProperties": False, "required": ["text", "start", "end", "purpose"],
        "properties": {"text": {"type": "string", "minLength": 1}, "start": {"type": "integer", "minimum": 0},
            "end": {"type": "integer", "minimum": 1}, "purpose": {"type": "string", "enum": ["question", "antecedent", "qualifier", "supporting_context"]}}}}
    item["required"].append("context_evidence")
    return result


def validate(value, *, source, window_id):
    if not isinstance(value, dict) or value.get("schema_version") != VERSION:
        raise ValueError("wrong v5 version")
    projected = deepcopy(value); projected["schema_version"] = parent.VERSION
    if not isinstance(projected.get("events"), list): raise ValueError("invalid events")
    for event in projected["events"]:
        contexts = event.pop("context_evidence", None)
        if not isinstance(contexts, list): raise ValueError("explicit context array required")
        seen = set()
        for context in contexts:
            if not isinstance(context, dict) or set(context) != {"text", "start", "end", "purpose"}:
                raise ValueError("invalid context fields")
            if context["purpose"] not in {"question", "antecedent", "qualifier", "supporting_context"}:
                raise ValueError("invalid context purpose")
            _span({k:v for k,v in context.items() if k != "purpose"}, source)
            key = (context["start"], context["end"])
            if key in seen or key == (event["evidence_start"], event["evidence_end"]):
                raise ValueError("duplicated context or spoken evidence")
            seen.add(key)
    # Context adds no voice authority. ALL existing attribution/voice/role gates
    # run unchanged on the spoken excerpt, never on combined context text.
    parent.validate(projected, source=source, window_id=window_id)
    return value


def receipt():
    return {"family": "full-event-v5-separate-source-context", "parent": parent.receipt(),
        "schema_sha256": digest(schema()), "context_rules_sha256": digest(CONTEXT_RULES),
        "changed_dimension": "explicit_unattributed_supporting_context_representation_only",
        "voice_gate_changed": False, "context_adds_claims_or_sources": False,
        "requires_fresh_all_role_qualification": True, "qualified": False, "gold_accepted": False}
