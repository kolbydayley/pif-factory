"""needs_review drain per Kolby's approved ruling (2026-08-10).

Advisory families reclassify deterministically to advisory_acknowledged;
bootstrap labels to superseded_bootstrap; real recall-loss families stay in
the actionable backlog for the bounded sampled drain. Resolutions are
append-only side-table rows: the canonical labels rows are never mutated.
"""

from __future__ import annotations

import json
import sqlite3

from research_factory import review_drain as rd


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE labels (
          id TEXT PRIMARY KEY,
          segment_id TEXT NOT NULL,
          label_pack TEXT NOT NULL,
          model TEXT NOT NULL,
          status TEXT NOT NULL,
          output_json TEXT NOT NULL,
          needs_review INTEGER NOT NULL DEFAULT 0,
          created_at TEXT NOT NULL
        )
        """
    )
    rd.ensure_review_resolution_schema(conn)
    return conn


def _label(conn, label_id: str, reason: str | None, *, needs_review: int = 1) -> None:
    output = {"review_reason": reason} if reason is not None else {}
    conn.execute(
        "INSERT INTO labels (id, segment_id, label_pack, model, status,"
        " output_json, needs_review, created_at)"
        " VALUES (?, 'seg', 'ai_discourse_v3_1', 'gpt-5.5', 'completed', ?, ?,"
        " '2026-08-01T00:00:00Z')",
        (label_id, json.dumps(output), needs_review),
    )


def test_classification_matches_the_approved_families() -> None:
    assert rd.classify_review_reason(
        "Speaker attribution is inferred from episode context because the "
        "captions lack explicit turns."
    ) == "advisory"
    assert rd.classify_review_reason(
        "The caption transcript contains ASR variants of product names."
    ) == "advisory"
    assert rd.classify_review_reason(
        "the segment overlaps adjacent content"
    ) == "advisory"
    assert rd.classify_review_reason("high_impact_signal") == "advisory"
    assert rd.classify_review_reason("high_impact_signal,mixed_page") == "advisory"
    assert rd.classify_review_reason("local_draft_bootstrap") == "bootstrap"
    assert rd.classify_review_reason(
        "Removed generated items whose evidence was not exact current-segment text."
    ) == "defect"
    assert rd.classify_review_reason(
        "Deterministic exact-evidence repair suppressed 2 ungrounded metric object(s)."
    ) == "defect"
    assert rd.classify_review_reason(
        "Quarantined 1 legacy ungrounded direction metric(s)."
    ) == "defect"
    assert rd.classify_review_reason(None) is None
    assert rd.classify_review_reason("") is None
    # Unrecognized free text is NOT silently cleared.
    assert rd.classify_review_reason("something novel happened") == "unclassified"


def test_bulk_resolution_resolves_advisories_and_keeps_defects() -> None:
    conn = _conn()
    _label(conn, "lab_speaker", "Speaker turns are inferred from context because the transcript lacks explicit turns.")
    _label(conn, "lab_asr", "The caption transcript contains ASR variants.")
    _label(conn, "lab_boot", "local_draft_bootstrap")
    _label(conn, "lab_defect", "Removed generated items whose evidence was not exact current-segment text.")
    _label(conn, "lab_novel", "something novel happened")
    _label(conn, "lab_noreason", None)
    _label(conn, "lab_clean", "irrelevant", needs_review=0)

    report = rd.apply_bulk_resolutions(conn, method="test_run")

    assert report["resolved_advisory"] == 2
    assert report["resolved_bootstrap"] == 1
    assert report["kept_defect"] == 1
    assert report["kept_unclassified"] == 1
    assert report["kept_no_reason"] == 1
    assert report["already_clean_skipped"] == 0

    rows = {
        row["label_id"]: row
        for row in conn.execute("SELECT * FROM label_review_resolutions")
    }
    assert rows["lab_speaker"]["resolution"] == "advisory_acknowledged"
    assert rows["lab_boot"]["resolution"] == "superseded_bootstrap"
    assert "lab_defect" not in rows
    assert "lab_clean" not in rows
    # Canonical labels rows untouched.
    flag = conn.execute(
        "SELECT needs_review FROM labels WHERE id='lab_speaker'"
    ).fetchone()
    assert flag["needs_review"] == 1


def test_bulk_resolution_is_idempotent() -> None:
    conn = _conn()
    _label(conn, "lab_speaker", "Speaker turns are inferred from context.")
    first = rd.apply_bulk_resolutions(conn, method="test_run")
    second = rd.apply_bulk_resolutions(conn, method="test_run")
    assert first["resolved_advisory"] == 1
    assert second["resolved_advisory"] == 0
    assert second["already_resolved_skipped"] == 1
    count = conn.execute(
        "SELECT COUNT(*) AS n FROM label_review_resolutions"
    ).fetchone()
    assert count["n"] == 1


def test_actionable_backlog_excludes_resolved() -> None:
    conn = _conn()
    _label(conn, "lab_speaker", "Speaker turns are inferred from context.")
    _label(conn, "lab_defect", "Removed generated items whose evidence was not exact current-segment text.")
    before = rd.actionable_backlog(conn)
    assert before["actionable"] == 2
    rd.apply_bulk_resolutions(conn, method="test_run")
    after = rd.actionable_backlog(conn)
    assert after["actionable"] == 1
    assert after["resolved_total"] == 1
