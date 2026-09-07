"""Resolve explicitly bound format retries without erasing first-pass failures."""
import json
from .signal_desk_rubric_reference_packets import digest, validate_reference


def load_reference(root, packet, *, expected_retry_system_sha256):
    sha = packet["packet_sha256"]
    original = root / f"{sha}.reference.json"
    if original.exists():
        value = validate_reference(json.loads(original.read_text()), packet)
        return value, {original.name: digest(value)}, {"retried": False, "judgment_changes": []}
    retry = root / "format-retry-v1"
    plan = json.loads((retry / "plan.json").read_text())
    prior = json.loads((root / "receipt.json").read_text())
    bindings = plan["bindings"]
    originals = [b["original_packet_sha256"] for b in bindings]
    if len(originals) != len(set(originals)) or set(originals) != set(prior["pending_packets"]):
        raise ValueError("retry inventory differs from first-pass failures")
    if sha not in originals or plan["system_sha256"] != expected_retry_system_sha256:
        raise ValueError("unbound retry or changed format contract")
    binding = next(b for b in bindings if b["original_packet_sha256"] == sha)
    retry_sha = binding["retry_packet_sha256"]
    retried_packet = json.loads((retry / f"{retry_sha}.packet.json").read_text())
    unsigned = {k: v for k, v in retried_packet.items() if k != "packet_sha256"}
    if digest(unsigned) != retry_sha or retried_packet["packet_sha256"] != retry_sha:
        raise ValueError("retry packet digest mismatch")
    expected = dict(packet); expected.pop("packet_sha256")
    expected["system_sha256"] = expected_retry_system_sha256
    if unsigned != expected:
        raise ValueError("retry changed source or candidates")
    raw = json.loads((root / f"{sha}.output.json").read_text())
    if digest(raw) != binding["original_output_sha256"]:
        raise ValueError("original rejected response changed")
    value = validate_reference(json.loads((retry / f"{retry_sha}.reference.json").read_text()), retried_packet)
    old = {d["event_id"]: d for d in raw["decisions"]}
    changes = []
    for decision in value["decisions"]:
        before = old.get(decision["event_id"])
        if before != decision:
            changes.append({"event_id": decision["event_id"], "original_decision": before,
                "retry_decision": decision, "resolved": False})
    if raw.get("empty_window_verdict") != value["empty_window_verdict"]:
        changes.append({"event_id": None, "original_empty_verdict": raw.get("empty_window_verdict"),
            "retry_empty_verdict": value["empty_window_verdict"], "resolved": False})
    return value, {f"{sha}.output.json": digest(raw),
        f"format-retry-v1/{retry_sha}.reference.json": digest(value)}, {
        "retried": True, "original_packet_sha256": sha, "retry_packet_sha256": retry_sha,
        "first_pass_failed": True, "judgment_changes": changes}
