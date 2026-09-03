import json
from pathlib import Path

import pytest

from research_factory.gold_audit import (
    AUDITABLE_SPLITS,
    build_context,
    clear_flag,
    connect_audit_db,
    flag_summary,
    illegitimate_flags,
    load_claims,
    set_flag,
)


def test_flag_upsert_replaces_and_summarizes():
    conn = connect_audit_db(Path(":memory:"))
    set_flag(conn, event_id="w1_e01", window_id="w1", split="validation", verdict="illegitimate",
             note="hallucinated", now="2026-09-03T12:00:00Z")
    # Re-flagging the same event replaces the verdict, does not duplicate.
    row = set_flag(conn, event_id="w1_e01", window_id="w1", split="validation", verdict="legitimate",
                   note="", now="2026-09-03T12:05:00Z")
    assert row["verdict"] == "legitimate"
    set_flag(conn, event_id="w1_e02", window_id="w1", split="validation", verdict="illegitimate",
             note="wrong speaker", now="2026-09-03T12:06:00Z")
    assert flag_summary(conn) == {"legitimate": 1, "illegitimate": 1, "unsure": 0}
    bad = illegitimate_flags(conn)
    assert [b["event_id"] for b in bad] == ["w1_e02"]
    assert bad[0]["note"] == "wrong speaker"
    clear_flag(conn, event_id="w1_e01")
    assert flag_summary(conn)["legitimate"] == 0


def test_set_flag_rejects_unknown_verdict():
    conn = connect_audit_db(Path(":memory:"))
    with pytest.raises(ValueError):
        set_flag(conn, event_id="e", window_id="w", split="validation", verdict="bogus", now="t")


def test_build_context_rebases_evidence_offsets_onto_trimmed_slice():
    window = "A" * 1000 + "THE EVIDENCE" + "B" * 1000
    ctx = build_context(window, 1000, 1012, pad=50)
    before, evidence, after = ctx.segments()
    assert evidence == "THE EVIDENCE"
    assert before.startswith("…") and after.endswith("…")  # trimmed on both sides
    assert before.endswith("A" * 50)


def test_build_context_is_robust_to_invalid_offsets():
    ctx = build_context("short context", 500, 999)
    before, evidence, after = ctx.segments()
    assert (before, evidence, after) == ("short context", "", "")


def test_load_claims_reads_c_outputs_attaches_context_and_flag_and_excludes_holdout(tmp_path):
    gold_root = tmp_path / "gold"
    window_text = "intro text. Manufacturing chips is hard. outro text."
    ev = "Manufacturing chips is hard"
    s = window_text.index(ev)
    for split in ("development", "validation", "sealed_holdout"):
        d = gold_root / "sealed-gold-results" / split / "C"
        d.mkdir(parents=True)
        (d / "w1.json").write_text(json.dumps({
            "window_id": "w1",
            "window_disposition": "claims_found",
            "events": [{
                "event_id": "w1_e01", "claim_text": "Chips are hard to make.",
                "evidence_text": ev, "evidence_start": s, "evidence_end": s + len(ev),
                "speech_act": "explanation", "stance": "warning", "issue_label": "chips",
                "publishability_state": "candidate",
            }],
        }))
    conn = connect_audit_db(Path(":memory:"))
    set_flag(conn, event_id="w1_e01", window_id="w1", split="validation", verdict="illegitimate",
             note="n", now="t")
    from research_factory.gold_audit import all_flags
    # Only development + validation are auditable; sealed_holdout stays sealed.
    claims = load_claims(gold_root, window_texts={"w1": window_text}, flags=all_flags(conn))
    assert {c["split"] for c in claims} == set(AUDITABLE_SPLITS)
    dev = next(c for c in claims if c["split"] == "development")
    assert dev["context_evidence"] == ev
    assert dev["context_before"].endswith("intro text. ")
    val = next(c for c in claims if c["split"] == "validation")
    assert val["verdict"] == "illegitimate" and val["note"] == "n"
    # A window with no reconstructable text still yields the claim (empty context).
    claims_no_text = load_claims(gold_root, window_texts={}, splits=("development",))
    assert claims_no_text[0]["context_evidence"] == ""
    assert claims_no_text[0]["claim_text"] == "Chips are hard to make."
