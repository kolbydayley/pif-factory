import json
import sqlite3
from pathlib import Path

import pytest

from research_factory.gold_exclusions import (
    EXCLUSION_SCHEMA,
    build_exclusion_ledger,
    default_exclusion_path,
    load_exclusions,
    load_gold_windows,
)


def _window(window_id: str, ids: list[str]) -> dict:
    return {
        "schema_version": "x", "window_id": window_id, "window_disposition": "ok",
        "events": [{"event_id": i, "claim_text": f"claim {i}"} for i in ids],
    }


def _write_gold(root: Path, windows: list[dict]) -> None:
    root.mkdir(parents=True)
    for w in windows:
        (root / f"{w['window_id']}.json").write_text(json.dumps(w))


def _audit_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("""CREATE TABLE gold_claim_flags (event_id TEXT PRIMARY KEY, window_id TEXT, split TEXT,
        verdict TEXT, note TEXT DEFAULT '', updated_at TEXT)""")
    conn.execute("""CREATE TABLE gold_claim_machine_flags (event_id TEXT PRIMARY KEY, window_id TEXT, split TEXT,
        verdict TEXT, reason TEXT DEFAULT '', model TEXT DEFAULT 'fable', updated_at TEXT)""")
    return conn


def test_ledger_unions_machine_not_legit_and_manual_illegitimate_only(tmp_path):
    conn = _audit_db(tmp_path / "audit.sqlite")
    conn.executemany("INSERT INTO gold_claim_machine_flags VALUES (?,?,?,?,?,?,?)", [
        ("w1_e1", "w1", "development", "not_legit", "chrome", "fable", "t"),
        ("w1_e2", "w1", "development", "unsure", "thin", "fable", "t"),      # unsure NOT excluded
        ("w1_e3", "w1", "development", "legit", "", "fable", "t"),
    ])
    conn.executemany("INSERT INTO gold_claim_flags VALUES (?,?,?,?,?,?)", [
        ("w2_e1", "w2", "validation", "illegitimate", "bad", "t"),
        ("w2_e2", "w2", "validation", "legitimate", "", "t"),
        ("w1_e1", "w1", "development", "illegitimate", "manual too", "t"),  # same claim from both sources
    ])
    conn.commit()
    ledger = build_exclusion_ledger(conn, now="2026-09-05T00:00:00+00:00")
    assert ledger["schema_version"] == EXCLUSION_SCHEMA
    keys = {(e["window_id"], e["event_id"]) for e in ledger["entries"]}
    assert keys == {("w1", "w1_e1"), ("w2", "w2_e1")}
    both = next(e for e in ledger["entries"] if e["event_id"] == "w1_e1")
    assert sorted(both["sources"]) == ["machine_not_legit", "manual_illegitimate"]
    assert ledger["counts"] == {"total": 2, "machine_not_legit": 1, "manual_illegitimate": 2}
    assert len(ledger["ledger_sha256"]) == 64


def test_ledger_sha_is_deterministic_over_entries_not_timestamp(tmp_path):
    conn = _audit_db(tmp_path / "audit.sqlite")
    conn.execute("INSERT INTO gold_claim_machine_flags VALUES ('w_e1','w','development','not_legit','r','fable','t')")
    conn.commit()
    a = build_exclusion_ledger(conn, now="2026-01-01T00:00:00+00:00")
    b = build_exclusion_ledger(conn, now="2026-12-31T00:00:00+00:00")
    assert a["ledger_sha256"] == b["ledger_sha256"]


def test_load_gold_windows_filters_by_window_and_event_id_without_touching_disk(tmp_path):
    gold = tmp_path / "C"
    # the same event_id string legitimately appears in two different windows
    _write_gold(gold, [_window("wA", ["e1", "e2", "e3"]), _window("wB", ["e1", "e2"])])
    before = {p.name: p.read_bytes() for p in gold.glob("*.json")}
    windows = load_gold_windows(gold, exclusions=frozenset({("wA", "e2")}))
    assert [e["event_id"] for e in windows["wA"]["events"]] == ["e1", "e3"]
    assert [e["event_id"] for e in windows["wB"]["events"]] == ["e1", "e2"]  # wB's e2 untouched
    assert windows["wA"]["excluded_event_ids"] == ["e2"]
    assert windows["wA"]["authored_event_count"] == 3
    assert windows["wB"]["excluded_event_ids"] == []
    assert {p.name: p.read_bytes() for p in gold.glob("*.json")} == before  # sealed files unchanged


def test_load_exclusions_missing_file_is_empty_unless_required(tmp_path):
    missing = tmp_path / "nope.json"
    assert load_exclusions(missing) == frozenset()
    with pytest.raises(FileNotFoundError):
        load_exclusions(missing, required=True)


def test_load_exclusions_round_trips_ledger_and_rejects_hash_drift(tmp_path):
    conn = _audit_db(tmp_path / "audit.sqlite")
    conn.execute("INSERT INTO gold_claim_machine_flags VALUES ('w_e1','w','development','not_legit','r','fable','t')")
    conn.commit()
    ledger = build_exclusion_ledger(conn, now="t")
    path = tmp_path / "ledger.json"
    path.write_text(json.dumps(ledger))
    assert load_exclusions(path) == frozenset({("w", "w_e1")})
    ledger["entries"].append({"window_id": "w", "event_id": "w_e9", "split": "development", "sources": ["x"], "reason": ""})
    path.write_text(json.dumps(ledger))  # tampered entries, stale sha
    with pytest.raises(ValueError):
        load_exclusions(path)


def test_default_exclusion_path_sits_in_campaign_artifacts():
    root = Path("/x/gold-authoring-v2/results/development/C")
    assert default_exclusion_path(root) == Path("/x/gold-authoring-v2/artifacts/gold-claim-exclusions.json")


def test_ledger_tolerates_missing_manual_table(tmp_path):
    conn = sqlite3.connect(tmp_path / "fresh.sqlite")
    conn.execute("""CREATE TABLE gold_claim_machine_flags (event_id TEXT, window_id TEXT, split TEXT,
        verdict TEXT, reason TEXT DEFAULT '', model TEXT DEFAULT 'opus', updated_at TEXT)""")
    conn.execute("INSERT INTO gold_claim_machine_flags VALUES ('w_e1','w','development','not_legit','r','opus','t')")
    conn.commit()
    ledger = build_exclusion_ledger(conn, now="t")
    assert ledger["counts"] == {"total": 1, "machine_not_legit": 1, "manual_illegitimate": 0}
