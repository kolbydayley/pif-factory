import asyncio
import json
from types import SimpleNamespace
import pytest
from scripts import pif_signal_desk_attribution_probe_review as runner
from research_factory.signal_desk_attribution_probe_review import build_packet


def setup(tmp_path, monkeypatch, *, existing=False, invalid=False, denied=False, missing_usage=False):
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    monkeypatch.setattr(runner, "OUT", tmp_path / "out")
    (tmp_path / "data").mkdir(); runner.OUT.mkdir()
    calls = []
    def reserve(*args, **kwargs):
        if denied: raise RuntimeError("budget denied")
        return {"reservation_id": "r", "existing": existing}
    monkeypatch.setattr(runner, "reserve_call", reserve)
    monkeypatch.setattr(runner, "settle_call", lambda *a, **kw: calls.append(("settle", kw)))
    class Client:
        def __init__(self, **kw): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        async def run_ephemeral_structured_turn(self, **kw):
            calls.append(("provider", kw))
            return SimpleNamespace(usage=None if missing_usage else SimpleNamespace(total_tokens=100),
                status_ok=True, output={"bad": True} if invalid else {"reviews": []})
    monkeypatch.setattr(runner, "CodexAppServerClient", Client)
    packet = build_packet({"anchors": [], "transcript_window": "source"}, {"decisions": []})
    return [packet], calls


def test_success_metered_and_idempotent(tmp_path, monkeypatch):
    packets, calls = setup(tmp_path, monkeypatch)
    assert asyncio.run(runner.execute(packets)) == 0
    assert asyncio.run(runner.execute(packets)) == 0
    assert sum(n == "provider" for n, _ in calls) == 1
    assert any(n == "settle" and k["actual_tokens"] == 100 for n, k in calls)
    assert not json.loads((runner.OUT / "receipt.json").read_text())["gold_accepted"]


@pytest.mark.parametrize("mode", ["existing", "denied"])
def test_budget_or_prior_paid_work_prevents_call(tmp_path, monkeypatch, mode):
    packets, calls = setup(tmp_path, monkeypatch, **{mode: True})
    with pytest.raises(RuntimeError): asyncio.run(runner.execute(packets))
    assert not calls


def test_invalid_preserved_not_retried(tmp_path, monkeypatch):
    packets, calls = setup(tmp_path, monkeypatch, invalid=True)
    assert asyncio.run(runner.execute(packets)) == 2
    assert asyncio.run(runner.execute(packets)) == 2
    assert sum(n == "provider" for n, _ in calls) == 1
    assert list(runner.OUT.glob("*.pending.json"))


def test_missing_usage_keeps_conservative_settlement(tmp_path, monkeypatch):
    packets, calls = setup(tmp_path, monkeypatch, missing_usage=True)
    asyncio.run(runner.execute(packets))
    assert any(n == "settle" and k["actual_tokens"] is None for n, k in calls)


def prepare_fixture(tmp_path, monkeypatch):
    from research_factory.signal_desk_rubric_reference_packets import digest
    monkeypatch.setattr(runner, "BASE", tmp_path / "base")
    monkeypatch.setattr(runner, "SOL", tmp_path / "sol")
    monkeypatch.setattr(runner, "OUT", tmp_path / "review")
    monkeypatch.setattr(runner, "freeze", lambda: None)
    runner.BASE.mkdir(); runner.SOL.mkdir()
    shas = []
    for i in range(16):
        packet = {"anchors": [], "transcript_window": "original source " + str(i)}
        sha = digest(packet); packet["packet_sha256"] = sha; shas.append(sha)
        (runner.BASE / f"{sha}.packet.json").write_text(json.dumps(packet))
        (runner.SOL / f"{sha}.result.json").write_text(json.dumps({"decisions": []}))
    (runner.BASE / "plan.json").write_text(json.dumps({"packet_digests": shas}))
    return shas


def test_prepare_complete_set_and_freezes_proposals(tmp_path, monkeypatch):
    prepare_fixture(tmp_path, monkeypatch)
    packets = runner.prepare()
    assert len(packets) == 16
    assert len(list(runner.OUT.glob("*.packet.json"))) == 16
    assert not json.loads((runner.OUT / "plan.json").read_text())["qualified"]


def test_prepare_missing_result_freezes_nothing(tmp_path, monkeypatch):
    shas = prepare_fixture(tmp_path, monkeypatch)
    (runner.SOL / f"{shas[-1]}.result.json").unlink()
    with pytest.raises(FileNotFoundError): runner.prepare()
    assert not runner.OUT.exists()


def test_prepare_changed_source_fails_closed(tmp_path, monkeypatch):
    shas = prepare_fixture(tmp_path, monkeypatch)
    path = runner.BASE / f"{shas[-1]}.packet.json"
    packet = json.loads(path.read_text()); packet["transcript_window"] = "changed"
    path.write_text(json.dumps(packet))
    with pytest.raises(ValueError, match="digest mismatch"): runner.prepare()
    assert not runner.OUT.exists()
