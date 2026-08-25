import json
import sqlite3

import pytest

from research_factory.pif_canonical_promotion import (
    PACK, _evidence_grounded, _schema_ok, promote, select_candidates,
    _ensure_promoted_column)

SEG_TEXT = "Speaker 1: The model shipped early. Speaker 2: Costs fell fast."


def good_label(evidence="The model shipped early."):
    return {
        "claims": [{"claim_text": "The model shipped early.",
                    "claim_type": "factual_assertion",
                    "evidence": evidence, "confidence": 0.9}],
        "entities": {"people": [], "organizations": [], "products": []},
        "topics": [],
        "summary": "Shipping and cost discussion.",
        "needs_review": False,
        "overall_confidence": 0.9,
    }


@pytest.fixture
def dbs(tmp_path):
    (tmp_path / "segs").mkdir()
    canon_path = tmp_path / "factory.sqlite"
    canon = sqlite3.connect(canon_path)
    canon.execute("CREATE TABLE segments (id TEXT PRIMARY KEY,"
                  " text_path TEXT NOT NULL)")
    canon.execute(
        "CREATE TABLE labels (id TEXT PRIMARY KEY, segment_id TEXT NOT NULL,"
        " label_pack TEXT NOT NULL, label_pack_version TEXT NOT NULL,"
        " model TEXT NOT NULL, status TEXT NOT NULL,"
        " output_json TEXT NOT NULL, evidence_start INTEGER,"
        " evidence_end INTEGER, confidence REAL,"
        " needs_review INTEGER NOT NULL DEFAULT 0, worker_id TEXT,"
        " prompt_path TEXT, output_path TEXT, created_at TEXT NOT NULL,"
        " UNIQUE(segment_id, label_pack, label_pack_version, model))")
    shadow_path = tmp_path / "drafts.sqlite"
    shadow = sqlite3.connect(shadow_path)
    shadow.execute(
        "CREATE TABLE draft_labels (segment_id TEXT NOT NULL,"
        " lane TEXT NOT NULL, label_json TEXT NOT NULL,"
        " validation_json TEXT NOT NULL, audit_json TEXT,"
        " created_at TEXT NOT NULL DEFAULT (datetime('now')),"
        " PRIMARY KEY (segment_id, lane))")
    canon.commit()
    shadow.commit()
    return {"tmp": tmp_path, "canon_path": canon_path, "canon": canon,
            "shadow_path": shadow_path, "shadow": shadow}


def add_segment(dbs_, seg_id, text=SEG_TEXT):
    rel = f"segs/{seg_id}.txt"
    (dbs_["tmp"] / rel).write_text(text)
    dbs_["canon"].execute("INSERT INTO segments VALUES (?,?)", (seg_id, rel))
    dbs_["canon"].commit()


def add_draft(dbs_, seg_id, lane, label, *, audit=None,
              created="2026-08-20 10:00:00"):
    dbs_["shadow"].execute(
        "INSERT INTO draft_labels (segment_id, lane, label_json,"
        " validation_json, audit_json, created_at) VALUES (?,?,?,?,?,?)",
        (seg_id, lane, json.dumps(label),
         json.dumps({"dropped": 0}),
         json.dumps(audit) if audit else None, created))
    dbs_["shadow"].commit()


def run(dbs_, execute, **kw):
    return promote(execute=execute,
                   lanes=["codex", "glm-zai", "glm", "grok"],
                   limit=kw.pop("limit", None),
                   canonical_db=dbs_["canon_path"],
                   shadow_db=dbs_["shadow_path"], pif_root=dbs_["tmp"])


def test_schema_and_grounding_helpers():
    assert _schema_ok(good_label())
    assert not _schema_ok({**good_label(), "extra_key": 1})
    bad_type = good_label()
    bad_type["claims"][0]["claim_type"] = "vibe"
    assert not _schema_ok(bad_type)
    assert _evidence_grounded(good_label(), SEG_TEXT)
    assert not _evidence_grounded(good_label("not in segment"), SEG_TEXT)


def test_dry_run_counts_without_writing(dbs):
    add_segment(dbs, "seg_a")
    add_draft(dbs, "seg_a", "glm", good_label())
    receipt = run(dbs, execute=False)
    assert receipt["promoted"] == 1 and receipt["mode"] == "dry_run"
    assert dbs["canon"].execute("SELECT COUNT(*) FROM labels").fetchone()[0] == 0
    assert dbs["shadow"].execute(
        "SELECT COUNT(*) FROM draft_labels WHERE promoted_at IS NOT NULL"
    ).fetchone()[0] == 0


def test_execute_inserts_and_is_idempotent(dbs):
    add_segment(dbs, "seg_a")
    add_draft(dbs, "seg_a", "glm", good_label())
    r1 = run(dbs, execute=True)
    assert r1["promoted"] == 1
    row = dbs["canon"].execute(
        "SELECT label_pack, model, status, needs_review, worker_id"
        " FROM labels").fetchone()
    assert row == (PACK, "opencode-go/glm-5.2", "ready", 0, "bulk:glm")
    r2 = run(dbs, execute=True)  # promoted_at now set -> nothing to do
    assert r2["promoted"] == 0 and r2["candidates"] == 0


def test_best_draft_per_segment_prefers_audited_then_lane(dbs):
    add_segment(dbs, "seg_a")
    add_draft(dbs, "seg_a", "codex", good_label())
    add_draft(dbs, "seg_a", "grok", good_label(),
              audit={"verdict": "pass", "support": 1.0})
    receipt = run(dbs, execute=True)
    assert receipt["promoted"] == 1
    assert dbs["canon"].execute(
        "SELECT worker_id FROM labels").fetchone()[0] == "bulk:grok"


def test_ungrounded_and_invalid_drafts_skip(dbs):
    add_segment(dbs, "seg_a")
    add_segment(dbs, "seg_b")
    add_draft(dbs, "seg_a", "glm", good_label("evidence not in text"))
    bad = good_label()
    bad["claims"][0]["claim_type"] = "vibe"
    add_draft(dbs, "seg_b", "glm", bad)
    receipt = run(dbs, execute=True)
    assert receipt["promoted"] == 0
    assert receipt["skips"] == {"evidence_not_grounded": 1,
                                "schema_invalid": 1}


def test_missing_segment_skips(dbs):
    add_draft(dbs, "seg_ghost", "glm", good_label())
    receipt = run(dbs, execute=True)
    assert receipt["skips"] == {"segment_missing": 1}


def test_limit_bounds_batch(dbs):
    for i in range(5):
        add_segment(dbs, f"seg_{i}")
        add_draft(dbs, f"seg_{i}", "glm", good_label())
    receipt = run(dbs, execute=True, limit=2)
    assert receipt["promoted"] == 2
    assert dbs["canon"].execute("SELECT COUNT(*) FROM labels").fetchone()[0] == 2


def test_promoted_column_added_once(dbs):
    _ensure_promoted_column(dbs["shadow"])
    _ensure_promoted_column(dbs["shadow"])  # idempotent
    cols = {r[1] for r in dbs["shadow"].execute(
        "PRAGMA table_info(draft_labels)")}
    assert "promoted_at" in cols


def test_select_candidates_only_unpromoted(dbs):
    add_segment(dbs, "seg_a")
    add_draft(dbs, "seg_a", "glm", good_label())
    _ensure_promoted_column(dbs["shadow"])
    assert len(select_candidates(dbs["shadow"], ["glm"], None)) == 1
    dbs["shadow"].execute(
        "UPDATE draft_labels SET promoted_at='2026-08-25T00:00:00'")
    dbs["shadow"].commit()
    assert select_candidates(dbs["shadow"], ["glm"], None) == []
