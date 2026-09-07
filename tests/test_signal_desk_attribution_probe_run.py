import asyncio
import json
import sqlite3
from types import SimpleNamespace
import pytest
from scripts import pif_signal_desk_attribution_probe_run as runner


def setup(tmp_path, monkeypatch, *, denied=False, output=None, missing_usage=False, capacity=False):
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    monkeypatch.setattr(runner, "OUT", tmp_path / "out")
    (tmp_path / "data").mkdir()
    db = sqlite3.connect(tmp_path / "data/factory.sqlite")
    db.execute("CREATE TABLE signal_desk_gold_budget_reservations (id TEXT,task_key TEXT,provider_started INTEGER)")
    db.close(); calls = []
    for name in ("ensure_gold_budget_schema", "initialize_lane", "mark_provider_started", "release_unstarted_reservation",
                 "release_gold_admission", "record_gold_admission_success", "record_outcome", "settle_gold_call", "record_capacity_failure"):
        monkeypatch.setattr(runner, name, lambda *a, _name=name, **kw: calls.append((_name, kw)))
    monkeypatch.setattr(runner, "admission_limit", lambda *a, **kw: {"effective_limit": 2})
    monkeypatch.setattr(runner, "admit_gold_call", lambda *a, **kw: {"allowed": True, "admission_id": "a"})
    monkeypatch.setattr(runner, "reserve_gold_call", lambda *a, **kw: {"allowed": not denied, "reservation_id": "r", "reason": "test_denial"})
    class Client:
        def __init__(self, **kw): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        async def read_weekly_rate_limit(self): return {"test": True}
        async def run_ephemeral_structured_turn(self, **kw):
            calls.append(("provider", kw))
            if capacity:
                kw["sidecar_path"].write_text(json.dumps({"turn_error": {"codex_error_info": "serverOverloaded", "message": "Selected model is at capacity"}}))
                raise RuntimeError("capacity")
            return SimpleNamespace(usage=None if missing_usage else SimpleNamespace(total_tokens=100), status_ok=True,
                output={"decisions": []} if output is None else output, error_class=None, status="completed")
    monkeypatch.setattr(runner, "CodexAppServerClient", Client)
    packets = [{"packet_sha256": "test", "anchors": [], "transcript_window": "source"}]
    return packets, calls


def test_success_is_metered_and_not_accepted(tmp_path, monkeypatch):
    packets, calls = setup(tmp_path, monkeypatch)
    assert asyncio.run(runner.execute(packets)) == 0
    assert sum(n == "provider" for n, _ in calls) == 1
    assert any(n == "settle_gold_call" and kw["actual_tokens"] == 100 for n, kw in calls)
    receipt = json.loads((runner.OUT / "receipt.json").read_text())
    assert receipt["complete"] and not receipt["gold_accepted"] and not receipt["qualified"]
    assert asyncio.run(runner.execute(packets)) == 0
    assert sum(n == "provider" for n, _ in calls) == 1


def test_budget_denial_makes_no_provider_call(tmp_path, monkeypatch):
    packets, calls = setup(tmp_path, monkeypatch, denied=True)
    with pytest.raises(RuntimeError, match="budget denied"):
        asyncio.run(runner.execute(packets))
    assert not any(n == "provider" for n, _ in calls)
    db = sqlite3.connect(runner.OUT / "dispatch.sqlite")
    assert db.execute("SELECT status FROM signal_desk_rebuild_tasks").fetchone()[0] == "pending"


def test_invalid_output_terminal_not_silent_retry(tmp_path, monkeypatch):
    packets, calls = setup(tmp_path, monkeypatch, output={"bad": True})
    assert asyncio.run(runner.execute(packets)) == 2
    assert not (runner.OUT / "test.result.json").exists()
    assert asyncio.run(runner.execute(packets)) == 2
    assert sum(n == "provider" for n, _ in calls) == 1


def test_missing_usage_not_settled_as_zero(tmp_path, monkeypatch):
    packets, calls = setup(tmp_path, monkeypatch, missing_usage=True)
    with pytest.raises(RuntimeError, match="usage missing"):
        asyncio.run(runner.execute(packets))
    assert not any(n == "settle_gold_call" for n, _ in calls)


def test_previous_paid_work_blocks_duplicate(tmp_path, monkeypatch):
    packets, calls = setup(tmp_path, monkeypatch)
    db = sqlite3.connect(tmp_path / "data/factory.sqlite")
    db.execute("INSERT INTO signal_desk_gold_budget_reservations VALUES ('old','attribution-probe-sol-v1:test:1:1',1)"); db.commit(); db.close()
    with pytest.raises(RuntimeError, match="sidecar recovery"):
        asyncio.run(runner.execute(packets))
    assert not any(n == "provider" for n, _ in calls)


def test_capacity_failure_opens_shared_circuit(tmp_path, monkeypatch):
    packets, calls = setup(tmp_path, monkeypatch, capacity=True)
    with pytest.raises(RuntimeError, match="capacity"):
        asyncio.run(runner.execute(packets))
    assert any(n == "record_capacity_failure" for n, _ in calls)
    assert any(n == "record_outcome" and kw["outcome"] == "rate_limit" for n, kw in calls)
