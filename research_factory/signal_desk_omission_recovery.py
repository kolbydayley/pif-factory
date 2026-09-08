"""Explicit, source-bound repair of failed diagnostic envelopes, not gold."""
from copy import deepcopy
import json
from .signal_desk_omission_review import SYSTEM as ORIGINAL_SYSTEM, schema, validate
from .signal_desk_rubric_reference_packets import digest

SYSTEM = ORIGINAL_SYSTEM + """
RECOVERY CONTRACT: prior_invalid_decisions are FAILED outputs, not an answer key.
Return exactly one decision per supplied case_id, no new case IDs or suggestions.
For kind=candidate or empty_source, correction MUST equal not_applicable, even
when verdict=material_error. Only kind=correction judges a proposed correction.
Copy source_quotes byte-for-byte from transcript_window, including fillers,
repetitions, punctuation, and case. Do not clean up a quoted sentence. Choose a
longer contiguous quotation if a short phrase occurs more than once. Check each
quote against the source before submitting. Reconsider the source reasoning;
do not mechanically repair strings to preserve an incorrect diagnosis.
"""


def partition(packet, raw):
    """Retain exact valid rows; isolate every missing/duplicate/invalid row."""
    if not isinstance(raw, dict) or set(raw) != {"decisions"} or not isinstance(raw["decisions"], list):
        raise ValueError("malformed raw envelope requires explicit investigation")
    expected = {c["case_id"]: c for c in packet["cases"]}
    if len(expected) != len(packet["cases"]): raise ValueError("duplicate original cases")
    if any(not isinstance(r, dict) for r in raw["decisions"]): raise ValueError("nonobject raw decision")
    retained = []; failures = []
    for cid, case in expected.items():
        rows = [r for r in raw["decisions"] if r.get("case_id") == cid]
        try:
            validate({"decisions": rows}, dict(packet, cases=[case]))
        except ValueError as exc:
            failures.append({"case_id": cid, "reason": str(exc), "prior_invalid_decisions": deepcopy(rows)})
        else: retained.append(deepcopy(rows[0]))
    extras = [deepcopy(r) for r in raw["decisions"] if r.get("case_id") not in expected]
    return {"original_packet_sha256": digest(packet), "original_output_sha256": digest(raw),
        "retained": retained, "failures": failures, "out_of_scope_suggestions": extras,
        "expected_cases": len(expected), "gold_accepted": False}


def repair_packets(packet, raw, *, token_count):
    parts = partition(packet, raw)
    originals = {c["case_id"]: c for c in packet["cases"]}
    cases = [dict(deepcopy(originals[f["case_id"]]), validation_failure=f["reason"],
        prior_invalid_decisions=f["prior_invalid_decisions"]) for f in parts["failures"]]
    def build(rows):
        result = {k: deepcopy(packet[k]) for k in ("window_id", "transcript_window", "transcript_structure", "current_C_index", "source_sha256")}
        result.update({"cases": rows, "parent_packet_sha256": packet["packet_sha256"],
            "original_output_sha256": digest(raw), "partition_sha256": digest(parts),
            "system_sha256": digest(SYSTEM), "schema_sha256": digest(schema()),
            "gold_accepted": False, "gate_eligible": False})
        result["packet_sha256"] = digest(result)
        return result
    def fits(p):
        return len(p["cases"]) <= 25 and token_count(SYSTEM + json.dumps(p, ensure_ascii=False) + json.dumps(schema())) + 1500 <= 12000
    packets = []; rows = []
    for case in cases:
        if not fits(build(rows + [case])):
            if not rows: raise ValueError("source and failed case exceed repair packet limit")
            packets.append(build(rows)); rows = []
        rows.append(case)
        if not fits(build(rows)): raise ValueError("source and failed case exceed repair packet limit")
    if rows: packets.append(build(rows))
    return parts, packets


def reconcile(packet, raw, repairs):
    """Assemble only a complete, provenance-bound set; never discard extras."""
    parts = partition(packet, raw)
    required = {r["case_id"] for r in parts["failures"]}; repaired = {}; provenance = {}
    for p, response in repairs:
        if p["parent_packet_sha256"] != packet["packet_sha256"] or p["original_output_sha256"] != digest(raw) or p["partition_sha256"] != digest(parts):
            raise ValueError("repair lineage mismatch")
        if p["transcript_window"] != packet["transcript_window"] or p["current_C_index"] != packet["current_C_index"]:
            raise ValueError("repair context changed")
        sha = p["packet_sha256"]
        if digest({k: v for k, v in p.items() if k != "packet_sha256"}) != sha or p["system_sha256"] != digest(SYSTEM) or p["schema_sha256"] != digest(schema()):
            raise ValueError("repair contract changed")
        validate(response, p)
        originals = {c["case_id"]: c for c in packet["cases"]}
        for case in p["cases"]:
            stripped = {k: v for k, v in case.items() if k not in {"validation_failure", "prior_invalid_decisions"}}
            if originals.get(case["case_id"]) != stripped: raise ValueError("repair rewrote candidate")
        for row in response["decisions"]:
            cid = row["case_id"]
            if cid not in required or cid in repaired: raise ValueError("duplicate or out of scope repair")
            repaired[cid] = row
        provenance[sha] = digest(response)
    if set(repaired) != required: raise ValueError("missing repairs")
    rows = {r["case_id"]: r for r in parts["retained"]}; rows.update(repaired)
    output = {"decisions": [rows[c["case_id"]] for c in packet["cases"]]}
    validate(output, packet)
    receipt = {"partition": parts, "repair_inventory": provenance, "reconciled_sha256": digest(output),
        "first_pass_failure_preserved": True, "gold_accepted": False, "gate_eligible": False}
    return output, receipt
