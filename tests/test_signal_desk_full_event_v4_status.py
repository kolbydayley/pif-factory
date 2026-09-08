import json
import pytest
from scripts.pif_signal_desk_full_event_v4_status import summarize
from research_factory.signal_desk_full_event_v4_prompts import packet, receipt
from research_factory.signal_desk_rubric_reference_packets import digest
from test_signal_desk_full_event_v4 import fixture, SOURCE


def setup(tmp_path, value=None):
    base = tmp_path / "base"; root = tmp_path / "out"
    base.mkdir(); root.mkdir()
    source = {"window_id": "dev", "transcript_window": SOURCE, "transcript_structure": "speaker_turn"}
    sha = digest(source); source["packet_sha256"] = sha
    (base / f"{sha}.packet.json").write_text(json.dumps(source))
    (root / "plan.json").write_text(json.dumps({"window_ids": ["dev"], "source_packets": {"dev": sha}, "contract": receipt()}))
    p = packet(source, "A"); directory = root / "calls" / "dev" / "A"; directory.mkdir(parents=True)
    (directory / "packet.json").write_text(json.dumps(p))
    prefix = directory / p["packet_sha256"]
    value = fixture() if value is None else value
    for suffix in ("output", "result"):
        prefix.with_suffix(f".{suffix}.json").write_text(json.dumps(value))
    prefix.with_suffix(".sidecar.json").write_text(json.dumps({"state": "completed", "usage": {"total_tokens": 123}}))
    return root, base, prefix


def test_reports_authored_not_accepted_and_role_specific_voice(tmp_path):
    root, base, _ = setup(tmp_path)
    s = summarize(root, base)
    assert s["states"] == {"authored_not_accepted": 1, "not_started": 3}
    assert s["by_role"]["A"]["spoken_named_voice"] == 1
    assert s["measured_usage_tokens"] == 123
    assert not s["accepted_gold"] and not s["qualified"]


def test_invalid_offsets_held_not_counted(tmp_path):
    v = fixture(); v["events"][0]["evidence_start"] += 1
    root, base, _ = setup(tmp_path, v)
    s = summarize(root, base)
    assert s["states"]["held_contract_failure"] == 1
    assert not s["by_role"] and s["problems"]


def test_raw_result_mismatch_rejected(tmp_path):
    root, base, prefix = setup(tmp_path)
    prefix.with_suffix(".result.json").write_text("{}")
    with pytest.raises(ValueError, match="saved result differs"):
        summarize(root, base)


def test_empty_window_preserved(tmp_path):
    v = fixture(); v.update(events=[], voice_bindings=[], window_disposition="no_records")
    root, base, _ = setup(tmp_path, v)
    s = summarize(root, base)
    assert s["by_role"]["A"]["empty_windows"] == 1
    assert s["by_role"]["A"]["records"] == 0
