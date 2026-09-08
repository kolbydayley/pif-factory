"""Reviewed voice proposal revision; no mutation of frozen v1 or gold."""
from copy import deepcopy
import hashlib
import json
from . import signal_desk_voice_continuity_experiment as v1

FAMILY_ID = "signal-desk-voice-continuity-v2"
RULES = v1.RULES + """
V2 CLARIFICATIONS: source_kind is spoken_transcript, article_context, or
indeterminate. Article introduction/summary prose outside an explicitly marked
edited transcript is not an unidentified spoken voice. Mark article_context,
leave voice_surface/identity_anchor/corridor null and basis indeterminate; cite
source_kind_evidence showing the prose/transcript boundary. This classification
does not reject potentially useful article evidence or assign its author.
If source kind itself is unresolved, use indeterminate, not a guessed voice.
source_kind_evidence must use exact source spans when kind is known.
continuity_evidence is separate from identity_anchor. For a proposed voice,
provide exact source evidence supporting the extent of the turn or narration,
not merely the existence of the name. In a labeled turn its explicit label/turn
boundary may support continuity. In a self-identification corridor, repeating
only the identity anchor is insufficient; the evidence must independently
support uninterrupted narration across the claimed range. Lack of labels alone
is not such evidence. If audio or wider context is needed but absent, leave null.
Any basis indeterminate requires null voice_surface, identity_anchor and corridor
and an empty continuity_evidence array. Explain the missing evidence in rationale.
Preserve shortened explicit speaker labels such as Brunson unless a separate
in-source alias is verified; do not invent a full name or use external metadata.
All span offsets are zero-based Unicode code-point indices in supplied source,
half-open [start,end); require end > start and source[start:end] == text.
These are structural requirements plus semantic instructions. A second independent
source review must still establish that evidence truly supports the identity and
continuity; validation is not that review. Do not add unindexed candidates.
"""


def schema():
    result = deepcopy(v1.schema())
    item = result["properties"]["decisions"]["items"]
    span = deepcopy(item["properties"]["identity_anchor"]["anyOf"][0])
    item["properties"].update({
        "source_kind": {"type": "string", "enum": ["spoken_transcript", "article_context", "indeterminate"]},
        "source_kind_evidence": {"type": "array", "items": span},
        "continuity_evidence": {"type": "array", "items": deepcopy(span)}})
    item["required"] = list(item["properties"])
    return result


def validate(value, *, source, window_id, candidates):
    if not isinstance(value, dict) or set(value) != {"source_sha256", "window_id", "decisions"} or not isinstance(value["decisions"], list):
        raise ValueError("invalid envelope")
    reduced = deepcopy(value)
    extra = {"source_kind", "source_kind_evidence", "continuity_evidence"}
    expected = set(schema()["properties"]["decisions"]["items"]["required"])
    for row, old in zip(value["decisions"], reduced["decisions"]):
        if not isinstance(row, dict) or set(row) != expected:
            raise ValueError("invalid v2 voice fields")
        if row["source_kind"] not in ("spoken_transcript", "article_context", "indeterminate"):
            raise ValueError("invalid source kind")
        for key in ("source_kind_evidence", "continuity_evidence"):
            if not isinstance(row[key], list): raise ValueError("evidence must be array")
            for evidence in row[key]: v1._span(evidence, source)
        if row["source_kind"] != "indeterminate" and not row["source_kind_evidence"]:
            raise ValueError("known source kind requires evidence")
        if row["source_kind"] != "spoken_transcript" and row["basis"] != "indeterminate":
            raise ValueError("nontranscript or unknown source cannot assign voice")
        if row["basis"] == "indeterminate":
            if row["continuity_evidence"]: raise ValueError("unresolved continuity cannot claim supporting evidence")
        else:
            if not row["continuity_evidence"]: raise ValueError("voice needs independent continuity evidence")
            if row["basis"] == "self_identification_continuity" and all(e == row["identity_anchor"] for e in row["continuity_evidence"]):
                raise ValueError("self-identification alone is not continuity evidence")
        for key in extra: old.pop(key)
    v1.validate(reduced, source=source, window_id=window_id, candidates=candidates)
    for row in value["decisions"]:
        if row["basis"] == "indeterminate": continue
        corridor = row["corridor"]
        if any(not corridor["start"] <= e["start"] < e["end"] <= corridor["end"] for e in row["continuity_evidence"]):
            raise ValueError("continuity evidence outside proposed corridor")
    return value


def receipt():
    return {"family_id": FAMILY_ID, "parent": v1.receipt(),
        "sha256": hashlib.sha256(json.dumps({"rules": RULES, "schema": schema()}, sort_keys=True).encode()).hexdigest(),
        "changed_dimension": "source_kind_and_explicit_voice_continuity_evidence",
        "qualified": False, "production_enabled": False,
        "independent_identity_and_continuity_review_required": True}
