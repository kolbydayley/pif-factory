"""Explicit, source-bound repair proposals; no silent normalization or approval."""
from copy import deepcopy
from .signal_desk_full_event_v4 import validate
from .signal_desk_rubric_reference_packets import digest


def propose(original, *, source, window_id, expected_original_sha256, replacements, output_validator=validate):
    if digest(original) != expected_original_sha256:
        raise ValueError("original response changed")
    value = deepcopy(original)
    if not replacements:
        raise ValueError("explicit changes required")
    for change in replacements:
        if set(change) != {"path", "before", "after", "reason"} or not change["reason"].strip():
            raise ValueError("invalid explicit change")
        path = change["path"]
        if not isinstance(path, list) or not path:
            raise ValueError("missing field path")
        target = value
        for part in path[:-1]:
            target = target[part]
        if target[path[-1]] != change["before"]:
            raise ValueError("expected field changed")
        target[path[-1]] = deepcopy(change["after"])
    if [e["event_id"] for e in original["events"]] != [e["event_id"] for e in value["events"]]:
        raise ValueError("repair cannot hide or rename records")
    if [b["voice_binding_id"] for b in original["voice_bindings"]] != [b["voice_binding_id"] for b in value["voice_bindings"]]:
        raise ValueError("binding population changed")
    output_validator(value, source=source, window_id=window_id)
    receipt = {"original_sha256": digest(original), "proposed_sha256": digest(value),
        "source_sha256": digest(source), "window_id": window_id, "replacements": deepcopy(replacements),
        "records_before": len(original["events"]), "records_after": len(value["events"]),
        "structurally_valid": True, "independent_review_required": True,
        "gold_accepted": False, "qualified": False}
    return value, receipt
