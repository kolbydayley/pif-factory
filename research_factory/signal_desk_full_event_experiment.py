"""Isolated full-event attribution candidate; never a frozen-v2 migration."""
from copy import deepcopy
from .signal_desk_rebuild_contracts import event_schema as v2_schema, _CHROME_RE
from .signal_desk_attribution_experiment import schema as attribution_schema, validate as validate_attribution, RULES
from .signal_desk_rubric_reference_packets import digest

VERSION = "pif_signal_desk_full_event_attribution_candidate_v3"
REMOVED = {"speaker_id", "quoted_person_id", "mentioned_person_ids", "attribution_type"}


def schema():
    result = deepcopy(v2_schema())
    result["title"] = VERSION
    result["properties"]["schema_version"]["const"] = VERSION
    event = result["properties"]["events"]["items"]
    event["required"] = [k for k in event["required"] if k not in REMOVED] + ["attribution"]
    for key in REMOVED: del event["properties"][key]
    event["properties"]["attribution"] = attribution_schema()
    event["properties"]["publishability_state"]["enum"] = ["candidate", "uncertain", "quarantined"]
    return result


def _shape(value, spec):
    kind = spec.get("type")
    kinds = kind if isinstance(kind, list) else [kind]
    matches = {"object": isinstance(value, dict), "array": isinstance(value, list),
        "string": isinstance(value, str), "integer": type(value) is int,
        "number": type(value) in (int, float), "null": value is None}
    if not any(matches.get(k, False) for k in kinds): raise ValueError("schema type mismatch")
    if value is None: return
    if "const" in spec and value != spec["const"]: raise ValueError("schema constant mismatch")
    if "enum" in spec and value not in spec["enum"]: raise ValueError("schema enum mismatch")
    if isinstance(value, dict):
        if set(value) != set(spec["required"]): raise ValueError("schema fields mismatch")
        for key, item in value.items(): _shape(item, spec["properties"][key])
    elif isinstance(value, list):
        for item in value: _shape(item, spec["items"])
    elif isinstance(value, str):
        if len(value) < spec.get("minLength", 0): raise ValueError("empty required text")
    elif type(value) in (int, float):
        if not spec.get("minimum", float("-inf")) <= value <= spec.get("maximum", float("inf")):
            raise ValueError("schema numeric range mismatch")


def validate(value, *, source, window_id):
    _shape(value, schema())
    if value["window_id"] != window_id: raise ValueError("window identity mismatch")
    events = value["events"]
    if (value["window_disposition"] == "claims_found") != bool(events):
        raise ValueError("window disposition contradicts events")
    seen = set()
    for event in events:
        if event["event_id"] in seen: raise ValueError("duplicate event identity")
        seen.add(event["event_id"])
        start, end = event["evidence_start"], event["evidence_end"]
        if not 0 <= start < end <= len(source) or source[start:end] != event["evidence_text"]:
            raise ValueError("evidence not exact at source offsets")
        if _CHROME_RE.search(event["evidence_text"]): raise ValueError("chrome evidence forbidden")
        if len(event["issue_aliases"]) != len(set(event["issue_aliases"])):
            raise ValueError("duplicate issue aliases")
        validate_attribution(event["attribution"], source=source)
    return value


def receipt():
    return {"schema_version": VERSION, "schema_sha256": digest(schema()),
        "attribution_rules_sha256": digest(RULES), "qualified": False,
        "production_enabled": False, "frozen_v2_modified": False,
        "required_next": "all-role prompt and extraction qualification before any gold migration"}
