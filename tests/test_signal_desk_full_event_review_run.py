import json
import pytest
from scripts import pif_signal_desk_full_event_review as runner
from research_factory.signal_desk_full_event_experiment import VERSION
from research_factory.signal_desk_full_event_prompts import packet
from research_factory.signal_desk_rubric_reference_packets import digest


def setup(tmp_path, monkeypatch):
    for name in ("QUAL", "BASE", "OUT"): monkeypatch.setattr(runner, name, tmp_path / name.lower())
    runner.QUAL.mkdir(); runner.BASE.mkdir()
    ids = [str(i) for i in range(16)]
    plan = {"window_ids": ids, "source_packets": {wid: wid for wid in ids}}
    monkeypatch.setattr(runner, "freeze", lambda: plan)
    for wid in ids:
        source = {"window_id": wid, "transcript_window": "Hello.", "transcript_structure": "paragraph", "packet_sha256": wid}
        (runner.BASE / f"{wid}.packet.json").write_text(json.dumps(source))
        authors = {}
        for role in ("A", "B", "C", "AUDIT"):
            p = packet(source, role, author_a=authors.get("A") if role == "C" else None,
                author_b=authors.get("B") if role == "C" else None)
            path = runner.QUAL / "calls" / wid / role; path.mkdir(parents=True)
            (path / "packet.json").write_text(json.dumps(p))
            output = {"schema_version": VERSION, "window_id": wid, "window_disposition": "no_consequential_claims", "events": []}
            (path / f"{p['packet_sha256']}.result.json").write_text(json.dumps(output))
            authors[role] = output
    execution = {"final_system_sha256": digest(runner.SYSTEM), "final_schema_sha256": digest(runner.schema()), "frozen_plan_sha256": digest(plan)}
    (runner.QUAL / "execution-contract.json").write_text(json.dumps(execution))


def test_requires_all_roles_and_retains_empty_windows(tmp_path, monkeypatch):
    setup(tmp_path, monkeypatch)
    result = runner.prepare()
    assert len(result) == 16 and all(p["candidates"] == [] for p in result)
    assert len(json.loads((runner.OUT / "plan.json").read_text())["inventory"]) == 16


def test_missing_last_audit_prevents_any_review_freeze(tmp_path, monkeypatch):
    setup(tmp_path, monkeypatch)
    next((runner.QUAL / "calls/15/AUDIT").glob("*.result.json")).unlink()
    with pytest.raises(FileNotFoundError): runner.prepare()
    assert not runner.OUT.exists()


def test_changed_author_binding_prevents_review(tmp_path, monkeypatch):
    setup(tmp_path, monkeypatch)
    path = runner.QUAL / "calls/15/C/packet.json"
    p = json.loads(path.read_text()); p["author_a"]["window_id"] = "wrong"
    path.write_text(json.dumps(p))
    with pytest.raises(ValueError, match="provenance"): runner.prepare()
    assert not runner.OUT.exists()
