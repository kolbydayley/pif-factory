import asyncio
import json
from scripts import pif_signal_desk_full_event_run as runner
from research_factory.signal_desk_full_event_experiment import VERSION


def setup(tmp_path, monkeypatch, fail=False):
    monkeypatch.setattr(runner, "BASE", tmp_path / "base"); runner.BASE.mkdir()
    monkeypatch.setattr(runner, "OUT", tmp_path / "out")
    calls = []; active = 0; maximum = 0
    plan = {"window_ids": ["one", "two", "three"], "source_packets": {w: w for w in ("one", "two", "three")}}
    for wid in plan["window_ids"]:
        (runner.BASE / f"{wid}.packet.json").write_text(json.dumps({"window_id": wid,
            "transcript_window": "Hello.", "transcript_structure": "paragraph", "packet_sha256": wid}))
    async def execute(packets, **kw):
        nonlocal active, maximum
        p = packets[0]; calls.append(p); active += 1; maximum = max(maximum, active)
        await asyncio.sleep(.001)
        active -= 1
        if fail and p["window_id"] == "one": return 2
        output = {"schema_version": VERSION, "window_id": p["window_id"], "window_disposition": "no_consequential_claims", "events": []}
        kw["validator"](output, p)
        (kw["output_root"] / f"{p['packet_sha256']}.result.json").write_text(json.dumps(output))
        return 0
    monkeypatch.setattr(runner, "metered_execute", execute)
    return plan, calls, lambda: (active, maximum)


def test_window_dependencies_blindness_and_two_worker_limit(tmp_path, monkeypatch):
    plan, calls, health = setup(tmp_path, monkeypatch)
    assert asyncio.run(runner.execute(plan)) == 0
    assert len(calls) == 12 and health() == (0, 2)
    for wid in plan["window_ids"]:
        assert [p["role"] for p in calls if p["window_id"] == wid] == ["A", "B", "C", "AUDIT"]
    assert all(("author_a" in p) == (p["role"] == "C") for p in calls)
    receipt = json.loads(next((runner.OUT / "runs").glob("*.json")).read_text())
    assert receipt["complete"] and not receipt["gold_accepted"]


def test_failure_drains_peer_and_stops_new_admissions(tmp_path, monkeypatch):
    plan, calls, health = setup(tmp_path, monkeypatch, fail=True)
    assert asyncio.run(runner.execute(plan)) == 2
    assert len(calls) == 2 and health() == (0, 2)
    assert all(p["role"] == "A" for p in calls)
    assert not any(p["window_id"] == "three" for p in calls)
