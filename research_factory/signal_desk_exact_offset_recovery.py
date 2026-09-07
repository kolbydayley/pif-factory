"""Auditable offset-only projection; no excerpt or semantic field may change."""
from copy import deepcopy
import json
from .signal_desk_full_event_experiment import validate
from .signal_desk_rubric_reference_packets import digest


def recover(value, *, source, window_id):
    result = deepcopy(value); changes = []
    for event in result["events"]:
        text = event["evidence_text"]
        start, end = event["evidence_start"], event["evidence_end"]
        if type(start) is not int or type(end) is not int: raise ValueError("noninteger offsets require separate recovery")
        if 0 <= start < end <= len(source) and source[start:end] == text: continue
        first = source.find(text)
        if not text or first < 0 or source.find(text, first + 1) >= 0:
            raise ValueError("excerpt missing or ambiguous; offset-only repair prohibited")
        fixed_end = first + len(text)
        if abs(start - first) > 3 or abs(end - fixed_end) > 3:
            raise ValueError("offset discrepancy exceeds bounded mechanical repair")
        event["evidence_start"], event["evidence_end"] = first, fixed_end
        changes.append({"event_id": event["event_id"], "before": [start, end], "after": [first, fixed_end]})
    if not changes: raise ValueError("no offset repair needed")
    validate(result, source=source, window_id=window_id)
    return result, {"repair": "unique-exact-offset-only-v1", "original_sha256": digest(value),
        "repaired_sha256": digest(result), "source_sha256": digest(source), "changes": changes,
        "semantic_fields_changed": False, "gold_accepted": False, "original_failure_preserved": True}


def load_call(path, packet):
    """Require result equality to the raw model output or reproducible projection."""
    sha = packet["packet_sha256"]
    raw = json.loads((path / f"{sha}.output.json").read_text())
    result = json.loads((path / f"{sha}.result.json").read_text())
    provenance = {"raw_sha256": digest(raw), "result_sha256": digest(result), "offset_recovery": False}
    if raw != result:
        expected, receipt = recover(raw, source=packet["transcript_window"], window_id=packet["window_id"])
        saved = json.loads((path / "offset-recovery.json").read_text())
        if saved != receipt or expected != result: raise ValueError("unverified output projection")
        provenance.update(offset_recovery=True, recovery_receipt_sha256=digest(receipt))
    validate(result, source=packet["transcript_window"], window_id=packet["window_id"])
    return result, provenance
