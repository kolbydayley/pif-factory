import json
import pytest
from research_factory.signal_desk_reference_recovery import load_reference
from research_factory.signal_desk_rubric_reference_packets import digest


def fixture(tmp_path):
    packet = {"window_id": "w", "transcript_window": "original", "candidate_events": [],
              "system_sha256": "old", "packet_sha256": "original"}
    changed = {k: v for k, v in packet.items() if k != "packet_sha256"}
    changed["system_sha256"] = "new"; sha = digest(changed); changed["packet_sha256"] = sha
    raw = {"model": "gpt-5.5", "decisions": [], "empty_window_verdict": "unresolved", "empty_window_rationale": "unclear"}
    response = dict(raw, empty_window_verdict="supported_empty", empty_window_rationale="no events")
    target = tmp_path / "format-retry-v1"; target.mkdir()
    for path, value in [(tmp_path / "receipt.json", {"pending_packets": ["original"]}),
        (tmp_path / "original.output.json", raw), (target / f"{sha}.packet.json", changed),
        (target / f"{sha}.reference.json", response),
        (target / "plan.json", {"system_sha256": "new", "bindings": [{"original_packet_sha256": "original",
            "retry_packet_sha256": sha, "original_output_sha256": digest(raw)}]})]:
        path.write_text(json.dumps(value))
    return packet, target, sha


def test_recovery_preserves_failed_provenance_and_changed_judgment(tmp_path):
    packet, _, _ = fixture(tmp_path)
    value, inventory, recovery = load_reference(tmp_path, packet, expected_retry_system_sha256="new")
    assert len(inventory) == 2 and recovery["first_pass_failed"]
    assert recovery["judgment_changes"][0]["resolved"] is False
    assert value["empty_window_verdict"] == "supported_empty"


def test_source_mutation_rejected_even_with_recomputed_hash(tmp_path):
    packet, target, sha = fixture(tmp_path)
    packet["transcript_window"] = "different"
    with pytest.raises(ValueError, match="changed source"):
        load_reference(tmp_path, packet, expected_retry_system_sha256="new")


def test_rejected_output_mutation_rejected(tmp_path):
    packet, _, _ = fixture(tmp_path)
    (tmp_path / "original.output.json").write_text("{}")
    with pytest.raises(ValueError, match="rejected response changed"):
        load_reference(tmp_path, packet, expected_retry_system_sha256="new")


def test_wrong_contract_rejected(tmp_path):
    packet, _, _ = fixture(tmp_path)
    with pytest.raises(ValueError, match="changed format contract"):
        load_reference(tmp_path, packet, expected_retry_system_sha256="other")
