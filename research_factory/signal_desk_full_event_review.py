"""Final-review envelope for full-event candidate v3; no automatic promotion."""
from .signal_desk_full_event_prompts import prompts
from .signal_desk_full_event_experiment import schema as extraction_schema, validate, VERSION
from .signal_desk_rubric_reference_packets import digest
import json

SYSTEM = prompts()["FINAL"] + """
For every supplied candidate ID return supported, corrected, or unresolved with
a complete event and source-grounded rationale. Supported must preserve the
candidate exactly. Corrected must change a substantive field, retaining event_id.
Unresolved retains the candidate with the uncertainty explained in the rationale.
Do not invent a replacement to conceal an unusable claim. Corrected events are
proposals requiring reconciliation, not approved gold. Empty packets have no
candidate decisions and require an explicit empty_window_verdict; a supported
empty means no consequential claim was missed, not that an API call succeeded.
"""


def schema():
    event = extraction_schema()["properties"]["events"]["items"]
    return {"type": "object", "additionalProperties": False,
        "required": ["decisions", "empty_window_verdict", "empty_window_rationale"], "properties": {
            "empty_window_verdict": {"type": "string", "enum": ["not_applicable", "supported_empty", "missed_claims", "unusable"]},
            "empty_window_rationale": {"type": "string"},
            "decisions": {"type": "array", "items": {"type": "object", "additionalProperties": False,
                "required": ["verdict", "event", "rationale"], "properties": {
                    "verdict": {"type": "string", "enum": ["supported", "corrected", "unresolved"]},
                    "event": event, "rationale": {"type": "string", "minLength": 1}}}}}}


def validate_review(value, *, source, window_id, candidates):
    from .signal_desk_full_event_experiment import _shape
    _shape(value, schema())
    original = {e["event_id"]: e for e in candidates}
    if len(original) != len(candidates): raise ValueError("duplicate original candidate")
    if candidates and value["empty_window_verdict"] != "not_applicable": raise ValueError("nonempty packet marked empty")
    if not candidates and (value["empty_window_verdict"] == "not_applicable" or not value["empty_window_rationale"].strip()):
        raise ValueError("empty window requires substantive verdict")
    events = []
    for row in value["decisions"]:
        event = row["event"]; prior = original.get(event["event_id"])
        if prior is None: raise ValueError("unexpected candidate")
        if row["verdict"] in {"supported", "unresolved"} and event != prior: raise ValueError("silent candidate rewrite")
        if row["verdict"] == "corrected" and event == prior: raise ValueError("empty correction")
        events.append(event)
    if {e["event_id"] for e in events} != set(original): raise ValueError("missing candidate decision")
    validate({"schema_version": VERSION, "window_id": window_id, "window_disposition": "claims_found" if events else "no_consequential_claims", "events": events},
        source=source, window_id=window_id)
    return value


def packets(output, *, source, window_id, token_count):
    """Preserve full context and every candidate while enforcing approval limits."""
    validate(output, source=source, window_id=window_id)
    def build(events):
        value = {"window_id": window_id, "transcript_window": source, "candidates": events,
            "original_output_sha256": digest(output), "candidate_population": len(output["events"]),
            "system_sha256": digest(SYSTEM), "schema_sha256": digest(schema()), "gold_accepted": False}
        value["packet_sha256"] = digest(value)
        return value
    def fits(value):
        return len(value["candidates"]) <= 25 and token_count(SYSTEM + json.dumps(value, ensure_ascii=False) + json.dumps(schema())) + 1500 <= 12000
    result = []; current = []
    for event in output["events"]:
        trial = build(current + [event])
        if not fits(trial):
            if not current: raise ValueError("single candidate and complete source exceed approval token limit")
            result.append(build(current)); current = [event]
            if not fits(build(current)): raise ValueError("single candidate and complete source exceed approval token limit")
        else: current.append(event)
    if current or not output["events"]:
        value = build(current)
        if not fits(value): raise ValueError("complete source exceeds approval token limit")
        result.append(value)
    return result
