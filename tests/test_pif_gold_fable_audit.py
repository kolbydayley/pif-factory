import importlib.util
import json
from pathlib import Path

import pytest

_spec = importlib.util.spec_from_file_location(
    "pif_gold_fable_audit", Path(__file__).resolve().parents[1] / "scripts/pif_gold_fable_audit.py"
)
audit = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(audit)


def test_resolve_window_unique_event_id_wins_regardless_of_position():
    batch = [{"event_id": "a", "window_id": "w1"}, {"event_id": "b", "window_id": "w2"}]
    assert audit._resolve_window(batch, "b", line_no=0)["window_id"] == "w2"


def test_resolve_window_duplicate_event_id_resolves_by_line_position():
    # the same event_id string legitimately appears in two different windows in one batch
    batch = [
        {"event_id": "e11", "window_id": "wA"},
        {"event_id": "x", "window_id": "wB"},
        {"event_id": "e11", "window_id": "wC"},
    ]
    assert audit._resolve_window(batch, "e11", line_no=0)["window_id"] == "wA"
    assert audit._resolve_window(batch, "e11", line_no=2)["window_id"] == "wC"


def test_resolve_window_unknown_event_id_is_none():
    assert audit._resolve_window([{"event_id": "a", "window_id": "w"}], "zzz", line_no=0) is None


def test_ingest_migrates_v1_event_id_pk_to_composite_key(tmp_path, monkeypatch):
    db = tmp_path / "audit.sqlite"
    monkeypatch.setattr(audit, "AUDIT_DB", db)
    import sqlite3
    conn = sqlite3.connect(db)
    conn.execute("""CREATE TABLE gold_claim_machine_flags (event_id TEXT PRIMARY KEY, window_id TEXT NOT NULL,
        split TEXT NOT NULL, verdict TEXT NOT NULL, reason TEXT NOT NULL DEFAULT '',
        model TEXT NOT NULL DEFAULT 'fable', updated_at TEXT NOT NULL)""")
    conn.execute("INSERT INTO gold_claim_machine_flags VALUES ('e1','w1','development','legit','','fable','t')")
    conn.commit(); conn.close()
    conn = audit._machine_conn()
    pk = [r[1] for r in conn.execute("PRAGMA table_info(gold_claim_machine_flags)") if r[5]]
    assert sorted(pk) == ["event_id", "window_id"]
    assert conn.execute("SELECT COUNT(*) FROM gold_claim_machine_flags").fetchone()[0] == 1
    # the same event_id may now live in a second window
    conn.execute("INSERT INTO gold_claim_machine_flags VALUES ('e1','w2','development','not_legit','','opus','t')")
    assert conn.execute("SELECT COUNT(*) FROM gold_claim_machine_flags").fetchone()[0] == 2


def test_export_refuses_exclusions_absent_from_gold(tmp_path, monkeypatch):
    db = tmp_path / "audit.sqlite"
    gold = tmp_path / "gold"
    (gold / "results" / "development" / "C").mkdir(parents=True)
    (gold / "results" / "development" / "C" / "w1.json").write_text(json.dumps(
        {"window_id": "w1", "events": [{"event_id": "w1_e1"}]}
    ))
    monkeypatch.setattr(audit, "AUDIT_DB", db)
    monkeypatch.setattr(audit, "GOLD_ROOT", gold)
    conn = audit._machine_conn()
    conn.execute("INSERT INTO gold_claim_machine_flags VALUES ('w1_e9','w1','development','not_legit','ghost','opus','t')")
    conn.commit(); conn.close()
    with pytest.raises(SystemExit, match="do not exist in gold"):
        audit.cmd_export_exclusions(audit.argparse.Namespace(output=str(tmp_path / "ledger.json")))
    assert not (tmp_path / "ledger.json").exists()


def test_export_writes_verifiable_ledger(tmp_path, monkeypatch):
    db = tmp_path / "audit.sqlite"
    gold = tmp_path / "gold"
    (gold / "results" / "development" / "C").mkdir(parents=True)
    (gold / "results" / "development" / "C" / "w1.json").write_text(json.dumps(
        {"window_id": "w1", "events": [{"event_id": "w1_e1"}, {"event_id": "w1_e2"}]}
    ))
    monkeypatch.setattr(audit, "AUDIT_DB", db)
    monkeypatch.setattr(audit, "GOLD_ROOT", gold)
    conn = audit._machine_conn()
    conn.execute("INSERT INTO gold_claim_machine_flags VALUES ('w1_e1','w1','development','not_legit','chrome','opus','t')")
    conn.execute("INSERT INTO gold_claim_machine_flags VALUES ('w1_e2','w1','development','unsure','thin','opus','t')")
    conn.commit(); conn.close()
    out = tmp_path / "ledger.json"
    audit.cmd_export_exclusions(audit.argparse.Namespace(output=str(out)))
    from research_factory.gold_exclusions import load_exclusions
    assert load_exclusions(out, required=True) == frozenset({("w1", "w1_e1")})  # unsure not excluded
