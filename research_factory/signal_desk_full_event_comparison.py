"""Diagnostic v3 cross-role matching, not a qualified scorer or error verdict."""
from collections import Counter
from .signal_desk_rebuild_scorer import match_events, scorer_sha256
from .signal_desk_full_event_experiment import validate
from .signal_desk_attribution_experiment import normalize


def identity(value):
    # Two valid binding spans can identify the same source-surface person.
    return None if value is None else (value["kind"], normalize(value["surface_name"]))


def fields(event):
    a = event["attribution"]
    return {"transcript_voice": identity(a["transcript_voice"]),
        "proposition_owner": identity(a["proposition_owner"]), "relation": a["relation"],
        "mentions": sorted((m["kind"], normalize(m["surface_name"])) for m in a["mentioned_entities"]),
        "stance": event["stance"], "issue_surface": normalize(event["issue_label"]), "speech_act": event["speech_act"]}


def compare(reference, prediction, *, source, structure, role):
    wid = reference["window_id"]
    validate(reference, source=source, window_id=wid)
    validate(prediction, source=source, window_id=wid)
    # Existing scorer supplies only evidence/claim-identity matching. Its v2
    # speaker metrics are deliberately NOT consumed for this distinct schema.
    matched = match_events(reference["events"], prediction["events"], transcript_structure=structure)
    agreed = Counter(); denominators = Counter(); differences = []
    used_ref = set(); used_pred = set()
    for pair in matched["matches"]:
        gi, pi = pair["gold_index"], pair["predicted_index"]
        used_ref.add(gi); used_pred.add(pi)
        g, p = reference["events"][gi], prediction["events"][pi]
        gf, pf = fields(g), fields(p); changed = []
        for field in gf:
            denominators[field] += 1; agreed[field] += int(gf[field] == pf[field])
            if gf[field] != pf[field]: changed.append(field)
        if changed:
            differences.append({"reference_event_id": g["event_id"], "prediction_event_id": p["event_id"],
                "fields": changed, "confirmed_error": False})
    return {"window_id": wid, "role": role, "structure": structure,
        "matching_scorer_sha256": scorer_sha256(), "comparison_schema": "full-event-v3-diagnostic-v1",
        "reference_count": len(reference["events"]), "prediction_count": len(prediction["events"]),
        "matched": len(used_ref), "both_empty": not reference["events"] and not prediction["events"],
        "agreement": dict(agreed), "denominators": dict(denominators), "differences": differences,
        "unmatched_reference": [e["event_id"] for i, e in enumerate(reference["events"]) if i not in used_ref],
        "unmatched_prediction": [e["event_id"] for i, e in enumerate(prediction["events"]) if i not in used_pred],
        "gate_eligible": False, "gold_accepted": False, "source_adjudication_required": True}
