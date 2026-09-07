import json
import pytest
from scripts import pif_signal_desk_rubric_reference_format_retry as retry


def setup(tmp_path, monkeypatch, reason="reference correction exceeds field scope"):
    monkeypatch.setattr(retry.runner, "OUT", tmp_path)
    packet = {"packet_sha256": "bad", "system_sha256": "old", "candidate_events": [],
              "transcript_window": "frozen source", "window_id": "w"}
    monkeypatch.setattr(retry.runner, "prepare", lambda: [packet, {"packet_sha256": "good"}])
    (tmp_path / "receipt.json").write_text(json.dumps({"pending_packets": ["bad"]}))
    (tmp_path / "bad.pending.json").write_text(json.dumps({"reason": reason}))
    (tmp_path / "bad.output.json").write_text('{"original":true}')
    return packet


def test_retry_only_pending_preserves_source_and_failed_receipts(tmp_path, monkeypatch):
    packet = setup(tmp_path, monkeypatch)
    out, _, packets = retry.prepare()
    plan = json.loads((out / "plan.json").read_text())
    assert len(packets) == 1
    assert packets[0]["transcript_window"] == packet["transcript_window"]
    assert packets[0]["candidate_events"] == packet["candidate_events"]
    assert packets[0]["packet_sha256"] != "bad"
    assert plan["first_pass_total_packets"] == 2
    assert plan["first_pass_invalid_packets"] == 1
    assert plan["retry_results_do_not_erase_first_pass_failures"]
    assert (tmp_path / "bad.output.json").read_text() == '{"original":true}'
    assert not plan["gold_accepted"] and not plan["rubric_qualified"]


def test_unknown_failure_not_blindly_retried(tmp_path, monkeypatch):
    setup(tmp_path, monkeypatch, reason="cross-window mismatch")
    with pytest.raises(ValueError, match="failure family"):
        retry.prepare()
    assert not (tmp_path / "format-retry-v1/plan.json").exists()
