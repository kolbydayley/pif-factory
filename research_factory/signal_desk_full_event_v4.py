"""Unqualified composed semantic records. Frozen v3 scores are not comparable."""
from copy import deepcopy
import hashlib
from . import signal_desk_full_event_experiment as v3
from . import signal_desk_semantic_join as join
from .signal_desk_rubric_reference_packets import digest

VERSION = "pif_signal_desk_semantic_records_v4"
DIMENSIONS = {"evidence_role": join.role, "position": join.position,
    "voice": join.voice, "attitude": join.attitude}


def schema():
    result = deepcopy(v3.schema())
    result["title"] = VERSION
    result["properties"]["schema_version"]["const"] = VERSION
    result["properties"]["window_disposition"]["enum"] = ["records_found", "no_records"]
    item = result["properties"]["events"]["items"]
    del item["properties"]["stance"]
    for key, module in DIMENSIONS.items():
        detail = deepcopy(module.schema()["properties"]["decisions"]["items"])
        detail["properties"].pop("candidate_id")
        detail["required"] = [k for k in detail["required"] if k != "candidate_id"]
        item["properties"][key] = detail
    item["required"] = list(item["properties"])
    return result


def semantic_bundle(value, source):
    """Structural conversion only; never modifies output or assigns labels."""
    bundle = {"attribution": {}}
    bundle.update({key: {"decisions": []} for key in DIMENSIONS})
    bundle["voice"].update(source_sha256=hashlib.sha256(source.encode()).hexdigest(), window_id=value["window_id"])
    candidates = {}
    for event in value["events"]:
        cid = event["event_id"]
        if cid in candidates: raise ValueError("duplicate record identity")
        candidates[cid] = {"text": event["evidence_text"], "start": event["evidence_start"], "end": event["evidence_end"]}
        bundle["attribution"][cid] = event["attribution"]
        for key in DIMENSIONS:
            if not isinstance(event[key], dict) or "candidate_id" in event[key]: raise ValueError("nested dimension must use enclosing event ID")
            bundle[key]["decisions"].append(dict(event[key], candidate_id=cid))
    return bundle, candidates


def validate(value, *, source, window_id):
    top = schema()
    if not isinstance(value, dict) or set(value) != set(top["required"]): raise ValueError("invalid v4 envelope")
    if value["schema_version"] != VERSION or value["window_id"] != window_id: raise ValueError("wrong version or window")
    if not isinstance(value["events"], list) or value["window_disposition"] not in {"records_found", "no_records"}:
        raise ValueError("invalid record list/disposition")
    if (value["window_disposition"] == "records_found") != bool(value["events"]): raise ValueError("disposition mismatch")
    fields = set(top["properties"]["events"]["items"]["required"])
    base_spec = v3.schema()["properties"]["events"]["items"]
    for event in value["events"]:
        if not isinstance(event, dict) or set(event) != fields: raise ValueError("invalid record fields")
        # Reuse only the frozen common-field SHAPE checks. The neutral placeholder
        # is never emitted or scored; v4 replaces ambiguous stance completely.
        projection = {k: deepcopy(v) for k, v in event.items() if k not in DIMENSIONS}
        projection["stance"] = "neutral"
        v3._shape(projection, base_spec)
        start, end = event["evidence_start"], event["evidence_end"]
        if not 0 <= start < end <= len(source) or source[start:end] != event["evidence_text"]:
            raise ValueError("inexact record evidence")
        if len(event["issue_aliases"]) != len(set(event["issue_aliases"])): raise ValueError("duplicate aliases")
        role = event["evidence_role"]
        if role.get("role") in {"promotion_housekeeping", "research_limitation"} and event["publishability_state"] != "quarantined":
            raise ValueError("excluded evidence must remain quarantined")
        if role.get("context_parent_status") == "missing" and event["publishability_state"] == "candidate":
            raise ValueError("missing-parent context remains uncertain or quarantined")
    bundle, candidates = semantic_bundle(value, source)
    join.validate(bundle, source=source, window_id=window_id, candidates=candidates)
    return value


def counts(value, *, source, window_id):
    validate(value, source=source, window_id=window_id)
    return {"records": len(value["events"]),
        "substantive_candidates": sum(e["evidence_role"]["role"] == "substantive_claim" for e in value["events"]),
        "supporting_context": sum(e["evidence_role"]["role"] == "supporting_context" for e in value["events"]),
        "quarantined": sum(e["publishability_state"] == "quarantined" for e in value["events"]),
        "accepted_claims": 0, "counted_source_breadth": 0}


def receipt():
    return {"schema_version": VERSION, "schema_sha256": digest(schema()), "semantic_join": join.receipt(),
        "qualified": False, "production_enabled": False, "frozen_v3_modified": False,
        "requires_fresh_all_role_qualification": True, "comparable_to_frozen_v3_scores": False}
