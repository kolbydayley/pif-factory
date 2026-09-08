"""Complete-record diagnostic review; corrections require explicit reconciliation."""
import json
from .signal_desk_full_event_v4 import validate, receipt as contract_receipt
from .signal_desk_full_event_v4_prompts import prompts
from .signal_desk_full_event_experiment import _shape
from .signal_desk_rubric_reference_packets import digest

SYSTEM = prompts()["FINAL"] + """
REVIEW ENVELOPE overrides the extraction representation: do not return events or
voice_bindings. Return exactly one decision for every supplied candidate event_id.
Use supported, needs_correction, or unresolved. Check every nested dimension and
the referenced binding against the complete source. record_index preserves parent
context outside this packet; its records are unaccepted candidates, not evidence.
In correction_proposal specify the exact field(s), current interpretation and
source-supported replacement. This is a proposal, never an applied correction.
supported requires an empty correction_proposal; needs_correction requires a
nonempty one. unresolved requires an explicit missing-context explanation in
rationale and no invented replacement. Provide exact transcript source_quotes for
every nonempty decision, without adding quote characters or rewriting fillers.
An empty packet has no decisions; give supported_empty, missed_records or unusable
and substantive empty_window_rationale. Missing useful records can be identified
in coverage_notes for any packet, without inventing new event IDs or changing the
supplied population. Do not confuse article/context records with spoken quotes.
Neither valid syntax nor supported review diagnoses make these accepted gold.
"""


def schema():
    return {"type": "object", "additionalProperties": False,
        "required": ["decisions", "empty_window_verdict", "empty_window_rationale", "coverage_notes"], "properties": {
            "empty_window_verdict": {"type": "string", "enum": ["not_applicable", "supported_empty", "missed_records", "unusable"]},
            "empty_window_rationale": {"type": "string"}, "coverage_notes": {"type": "string"},
            "decisions": {"type": "array", "items": {"type": "object", "additionalProperties": False,
                "required": ["event_id", "verdict", "correction_proposal", "rationale", "source_quotes"], "properties": {
                    "event_id": {"type": "string", "minLength": 1},
                    "verdict": {"type": "string", "enum": ["supported", "needs_correction", "unresolved"]},
                    "correction_proposal": {"type": "string"}, "rationale": {"type": "string", "minLength": 1},
                    "source_quotes": {"type": "array", "items": {"type": "string", "minLength": 1}, "minItems": 1}}}}}}


def validate_review(value, packet):
    _shape(value, schema())
    candidates = packet["candidates"]; ids = {e["event_id"] for e in candidates}
    if len(ids) != len(candidates): raise ValueError("duplicate input candidates")
    if candidates and value["empty_window_verdict"] != "not_applicable": raise ValueError("nonempty marked empty")
    if not candidates and (value["empty_window_verdict"] == "not_applicable" or not value["empty_window_rationale"].strip()):
        raise ValueError("empty source requires an explicit judgment")
    seen = set()
    for row in value["decisions"]:
        if row["event_id"] not in ids or row["event_id"] in seen: raise ValueError("changed review population")
        seen.add(row["event_id"])
        if not row["rationale"].strip(): raise ValueError("empty rationale")
        if (row["verdict"] == "needs_correction") != bool(row["correction_proposal"].strip()):
            raise ValueError("correction verb/proposal mismatch")
        if not row["source_quotes"] or any(not q.strip() or q not in packet["transcript_window"] for q in row["source_quotes"]):
            raise ValueError("review quotes must be exact source")
    if seen != ids: raise ValueError("missing review decisions")
    return value


def packets(output, *, source, window_id, token_count, system=None, schema_for_packet=None, output_validator=validate):
    selected_system = SYSTEM if system is None else system
    selected_schema = (lambda p: schema()) if schema_for_packet is None else schema_for_packet
    output_validator(output, source=source, window_id=window_id)
    # Full source always retained; compact whole-population index exposes context
    # links without multiplying every detailed record in every review packet.
    index = [{"event_id": e["event_id"], "claim_text": e["claim_text"],
        "role": e["evidence_role"]["role"], "context_for": e["evidence_role"]["context_for"]} for e in output["events"]]
    def build(events):
        refs = {e["voice_binding_id"] for e in events}
        p = {"window_id": window_id, "transcript_window": source, "source_sha256": digest(source),
            "candidates": events, "voice_bindings": [b for b in output["voice_bindings"] if b["voice_binding_id"] in refs],
            "record_index": index, "candidate_population": len(output["events"]), "original_output_sha256": digest(output),
            "system_sha256": digest(selected_system), "gold_accepted": False}
        p["schema_sha256"] = digest(selected_schema(p))
        p["packet_sha256"] = digest(p)
        return p
    def fits(p):
        return len(p["candidates"]) <= 25 and token_count(selected_system + json.dumps(p, ensure_ascii=False) + json.dumps(selected_schema(p))) + 1500 <= 12000
    result = []; group = []
    for event in output["events"]:
        if not fits(build(group + [event])):
            if not group: raise ValueError("full source and one record exceed review budget")
            result.append(build(group)); group = []
        group.append(event)
        if not fits(build(group)): raise ValueError("full source and one record exceed review budget")
    if group or not output["events"]:
        p = build(group)
        if not fits(p): raise ValueError("empty full source exceeds review budget")
        result.append(p)
    return result


def receipt():
    return {"family": "full-event-v4-independent-final-review", "system_sha256": digest(SYSTEM),
        "schema_sha256": digest(schema()), "extraction_contract": contract_receipt(),
        "qualified": False, "gold_accepted": False, "corrections_require_explicit_reconciliation": True}
