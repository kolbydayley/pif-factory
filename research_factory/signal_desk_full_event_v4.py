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
        if key == "voice":
            detail["properties"]["voice_binding_id"] = {"type": "string", "minLength": 1}
            detail["required"].append("voice_binding_id")
            result["properties"]["voice_bindings"] = {"type": "array", "items": detail}
            result["required"].append("voice_bindings")
            item["properties"]["voice_binding_id"] = {"type": "string", "minLength": 1}
        else:
            item["properties"][key] = detail
    item["required"] = list(item["properties"])
    return result


def semantic_bundle(value, source):
    """Structural conversion only; never modifies output or assigns labels."""
    bundle = {"attribution": {}}
    bundle.update({key: {"decisions": []} for key in DIMENSIONS})
    bundle["voice"].update(source_sha256=hashlib.sha256(source.encode()).hexdigest(), window_id=value["window_id"])
    bindings = {r["voice_binding_id"]: {k: v for k, v in r.items() if k != "voice_binding_id"} for r in value["voice_bindings"]}
    candidates = {}
    for event in value["events"]:
        cid = event["event_id"]
        if cid in candidates: raise ValueError("duplicate record identity")
        candidates[cid] = {"text": event["evidence_text"], "start": event["evidence_start"], "end": event["evidence_end"]}
        bundle["attribution"][cid] = event["attribution"]
        for key in DIMENSIONS:
            detail = bindings[event["voice_binding_id"]] if key == "voice" else event[key]
            if not isinstance(detail, dict) or "candidate_id" in detail: raise ValueError("nested dimension must use enclosing event ID")
            bundle[key]["decisions"].append(dict(detail, candidate_id=cid))
    return bundle, candidates


def validate(value, *, source, window_id):
    top = schema()
    if not isinstance(value, dict) or set(value) != set(top["required"]): raise ValueError("invalid v4 envelope")
    if value["schema_version"] != VERSION or value["window_id"] != window_id: raise ValueError("wrong version or window")
    if not isinstance(value["events"], list) or value["window_disposition"] not in {"records_found", "no_records"}:
        raise ValueError("invalid record list/disposition")
    if (value["window_disposition"] == "records_found") != bool(value["events"]): raise ValueError("disposition mismatch")
    if not isinstance(value["voice_bindings"], list): raise ValueError("invalid binding pool")
    binding_fields = set(top["properties"]["voice_bindings"]["items"]["required"])
    binding_ids = set(); binding_contents = set()
    for binding in value["voice_bindings"]:
        if not isinstance(binding, dict) or set(binding) != binding_fields: raise ValueError("invalid binding fields")
        bid = binding["voice_binding_id"]
        if not isinstance(bid, str) or not bid.strip() or bid in binding_ids: raise ValueError("invalid/duplicate binding ID")
        content = digest({k: v for k, v in binding.items() if k != "voice_binding_id"})
        if content in binding_contents: raise ValueError("duplicate equivalent binding; reuse its ID")
        binding_ids.add(bid); binding_contents.add(content)
    referenced = set()
    fields = set(top["properties"]["events"]["items"]["required"])
    base_spec = v3.schema()["properties"]["events"]["items"]
    for event in value["events"]:
        if not isinstance(event, dict) or set(event) != fields: raise ValueError("invalid record fields")
        # Reuse only the frozen common-field SHAPE checks. The neutral placeholder
        # is never emitted or scored; v4 replaces ambiguous stance completely.
        bid = event["voice_binding_id"]
        if not isinstance(bid, str) or bid not in binding_ids: raise ValueError("unknown voice binding reference")
        referenced.add(bid)
        projection = {k: deepcopy(v) for k, v in event.items() if k not in DIMENSIONS and k != "voice_binding_id"}
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
    if referenced != binding_ids: raise ValueError("unreferenced voice binding")
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
        "voice_representation": "deduplicated_source_bound_pool_with_per_record_coverage_checks",
        "requires_fresh_all_role_qualification": True, "comparable_to_frozen_v3_scores": False}
