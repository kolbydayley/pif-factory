import asyncio
import json
from scripts import pif_signal_desk_full_event_v4_run as runner
from research_factory.signal_desk_rubric_reference_packets import digest
from test_signal_desk_full_event_v4 import fixture, SOURCE


def setup(tmp_path, monkeypatch):
    base = tmp_path / "base"; out = tmp_path / "out"; base.mkdir(); out.mkdir()
    monkeypatch.setattr(runner, "BASE", base); monkeypatch.setattr(runner, "OUT", out)
    s = {"window_id": "dev", "transcript_window": SOURCE, "transcript_structure": "speaker_turn"}; s["packet_sha256"] = digest(s)
    (base / f"{s['packet_sha256']}.packet.json").write_text(json.dumps(s))
    return {"window_ids": ["dev"], "source_packets": {"dev": s["packet_sha256"]}}, out


def test_pipeline_success_keeps_audit_independent(tmp_path, monkeypatch):
    plan, out = setup(tmp_path, monkeypatch); roles = []
    async def fake(packets, **kwargs):
        p = packets[0]; roles.append(p["role"])
        if p["role"] == "C": assert "author_a" in p and "author_b" in p
        else: assert "author_a" not in p and "author_b" not in p
        value = kwargs["validator"](fixture(), p)
        runner.immutable_json(kwargs["output_root"] / f"{p['packet_sha256']}.result.json", value)
        return 0
    monkeypatch.setattr(runner, "metered_execute", fake)
    assert asyncio.run(runner.execute(plan)) == 0
    assert roles == ["A", "B", "C", "AUDIT"]
    assert list((out / "final-packets").glob('*.json'))


def test_failure_checkpoints_without_followon_calls(tmp_path, monkeypatch):
    plan, out = setup(tmp_path, monkeypatch); calls = []
    async def fake(packets, **kwargs): calls.append(packets[0]["role"]); return 2
    monkeypatch.setattr(runner, "metered_execute", fake)
    assert asyncio.run(runner.execute(plan)) == 2
    assert calls == ["A"]
    result = json.loads(next((out / "runs").glob('*.json')).read_text())
    assert not result["complete"] and not result["gold_accepted"]
