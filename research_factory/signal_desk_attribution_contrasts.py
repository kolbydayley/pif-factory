"""Fixed attribution probes; diagnostic only, not an extraction benchmark."""
import json
from pathlib import Path
from .signal_desk_attribution_experiment import receipt, RULES, schema
from .signal_desk_rubric_reference_packets import digest
from .signal_desk_rebuild_contracts import validate_output
from .signal_desk_gold_audit import _load_frozen_window_text

ANCHOR_FIELDS = ("event_id", "claim_text", "evidence_text", "evidence_start", "evidence_end")
SYSTEM = """Independently evaluate each fixed claim anchor against the FULL supplied
development transcript window using the experimental attribution contract.
You are not extracting new events or repairing the source. The anchors are
candidates, not facts: classify unsupported or ambiguous propositions explicitly.
Return an attribution block and stance for each exact event ID, with a concise
source rationale. Never infer a missing narrator from an embedded quotation owner.
Identity binding spans use offsets into this exact supplied source. An explicit
quoted owner can be named even when the transcript voice is unknown. Do not use
outside knowledge to repair ASR spellings. Assign person versus organization.
No preceding model answers or expected labels are supplied. This is not gold
acceptance and does not measure recall, omissions, or corpus reliability.
""" + RULES


def response_schema(packet):
    return {"type": "object", "additionalProperties": False, "required": ["decisions"], "properties": {
        "decisions": {"type": "array", "minItems": len(packet["anchors"]), "maxItems": len(packet["anchors"]),
            "items": {"type": "object", "additionalProperties": False,
                "required": ["event_id", "claim_status", "attribution", "stance", "source_rationale"], "properties": {
                    "event_id": {"type": "string", "enum": [a["event_id"] for a in packet["anchors"]] or ["no_anchors"]},
                    "claim_status": {"type": "string", "enum": ["supported", "unsupported", "ambiguous"]},
                    "attribution": schema(),
                    "stance": {"type": "string", "enum": ["neutral", "supportive", "skeptical", "warning", "mixed", "unknown"]},
                    "source_rationale": {"type": "string", "minLength": 1}}}}}}


def build(*, qualification_plan, manifest, result_root, project_root):
    ids = qualification_plan["window_ids"]
    if len(ids) != 16 or len(set(ids)) != 16 or qualification_plan["manifest_sha256"] != manifest["manifest_sha256"]:
        raise ValueError("contrast scope must retain frozen 16-window development population")
    rows = {r["window_id"]: r for r in manifest["windows"] if r["split"] == "development"}
    if not set(ids) <= set(rows):
        raise ValueError("protected or missing contrast source")
    packets = []; inventory = {}
    for wid in ids:
        row = rows[wid]; text = _load_frozen_window_text(row, project_root=project_root)
        value = json.loads((Path(result_root) / "C" / f"{wid}.json").read_text())
        validate_output(value, transcript_window=text, expected_window_id=wid)
        inventory[wid] = digest(value)
        # Selection uses event identity only, never prior reviewer/attribution labels.
        selected = sorted(value["events"], key=lambda e: digest(["attribution-contrast-v1", wid, e["event_id"]]))[:3]
        packet = {"window_id": wid, "show_id": row["show_id"], "transcript_window": text,
            "transcript_structure": row["transcript_structure"], "alignment": row.get("alignment"),
            "source_sha256": row["text_sha256"], "manifest_sha256": manifest["manifest_sha256"],
            "anchors": [{k: e[k] for k in ANCHOR_FIELDS} for e in selected],
            "candidate_event_count": len(value["events"]), "empty_anchor_window": not selected,
            "contract_sha256": receipt()["sha256"], "system_sha256": digest(SYSTEM)}
        packet["packet_sha256"] = digest(packet); packets.append(packet)
    return {"packets": packets, "candidate_inventory": inventory, "contract": receipt(),
        "selection": "up to three event-ID-hash anchors per original window; all windows retained",
        "selection_uses_candidate_claims": True, "selection_uses_attribution_or_judge_labels": False,
        "classification_only": True, "representative_corpus_estimate": False,
        "recall_gate_eligible": False, "gold_accepted": False}
