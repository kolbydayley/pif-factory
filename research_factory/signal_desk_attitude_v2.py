"""Grounded modality and multi-target detail; one output per original candidate."""
from copy import deepcopy
import hashlib
import json
from . import signal_desk_attitude_experiment as v1

FAMILY_ID = "signal-desk-target-attitude-v2"
RULES = v1.RULES + """
V2: modality_evidence contains exact spans supporting expressed hedges or certainty.
Hedged epistemic requires a source cue in this array; ordinary assertions may have
an empty array rather than fabricated certainty language. The adjective 'worst'
is a negative evaluation, not a hedge. Subjective/qualified 'I feel ... a bit' must
remain bounded rather than become a measured causal finding.
For split_required multi-target candidates, set scalar target=null and scalar
attitude=indeterminate, and supply at least two target_components, each with its
own exact target, attitude and evaluation_evidence. Components are distinctions
within the SAME candidate, not newly accepted claims, speakers or evidence counts.
Do not duplicate a component or split synonymous target names to inflate density.
For other statuses target_components must be empty. One coherent positive and
negative evaluation of the same target can use mixed; unrelated targets cannot.
Ownership and position actuality are supplied by separate attribution/position
records joined by candidate ID. Do not infer missing owners or make a narrator
endorse quoted content. Those records also remain subject to independent review.
All offsets are zero-based Unicode code-point, half-open [start,end) into source;
end > start and source[start:end] equals the exact text. Validation is not truth
or semantic approval. Keep every candidate ID exactly once.
"""


def schema():
    result = deepcopy(v1.schema()); item = result["properties"]["decisions"]["items"]
    span = deepcopy(item["properties"]["target"]["anyOf"][0])
    component = {"type": "object", "additionalProperties": False,
        "required": ["target", "attitude", "evaluation_evidence"], "properties": {
            "target": span, "attitude": {"type": "string", "enum": ["positive", "negative", "mixed", "neutral", "indeterminate"]},
            "evaluation_evidence": {"type": "array", "items": deepcopy(span), "minItems": 1}}}
    item["properties"].update(modality_evidence={"type": "array", "items": deepcopy(span)},
        target_components={"type": "array", "items": component})
    item["required"] = list(item["properties"])
    return result


def validate(value, *, source, candidate_ids):
    if not isinstance(value, dict) or set(value) != {"decisions"} or not isinstance(value["decisions"], list):
        raise ValueError("invalid attitude v2 envelope")
    reduced = deepcopy(value); fields = set(schema()["properties"]["decisions"]["items"]["required"])
    for row, old in zip(value["decisions"], reduced["decisions"]):
        if not isinstance(row, dict) or set(row) != fields: raise ValueError("invalid v2 fields")
        if not isinstance(row["modality_evidence"], list): raise ValueError("invalid modality evidence")
        for span in row["modality_evidence"]: v1._span(span, source)
        if row["epistemic"] == "hedged" and not row["modality_evidence"]: raise ValueError("hedge needs evidence")
        components = row["target_components"]
        if not isinstance(components, list): raise ValueError("invalid components")
        if row["status"] == "split_required":
            if len(components) < 2 or row["target"] is not None or row["attitude"] != "indeterminate":
                raise ValueError("split must preserve distinct components without collapsed scalar attitude")
        elif components: raise ValueError("components only for split candidates")
        seen = set()
        for component in components:
            if not isinstance(component, dict) or set(component) != {"target", "attitude", "evaluation_evidence"}: raise ValueError("invalid component")
            v1._span(component["target"], source)
            if component["attitude"] not in ("positive", "negative", "mixed", "neutral", "indeterminate"): raise ValueError("invalid component attitude")
            evidence = component["evaluation_evidence"]
            if not isinstance(evidence, list) or not evidence: raise ValueError("component needs evidence")
            for span in evidence: v1._span(span, source)
            identity = json.dumps(component, sort_keys=True)
            if identity in seen: raise ValueError("duplicate component")
            seen.add(identity)
        old.pop("modality_evidence"); old.pop("target_components")
    v1.validate(reduced, source=source, candidate_ids=candidate_ids)
    return value


def receipt():
    return {"family_id": FAMILY_ID, "parent": v1.receipt(),
        "sha256": hashlib.sha256(json.dumps({"rules": RULES, "schema": schema()}, sort_keys=True).encode()).hexdigest(),
        "changed_dimension": "grounded_modality_and_target_components",
        "qualified": False, "production_enabled": False, "semantic_source_review_required": True}
