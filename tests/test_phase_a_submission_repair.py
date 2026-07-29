from __future__ import annotations

import sqlite3

from research_factory.headless_codex import _finalize_submission_failure
from research_factory.labels import repair_label_output_for_submission


def test_submission_failure_releases_retryable_job_and_finalizes_run() -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    try:
        conn.executescript(
            """
            CREATE TABLE jobs (
              id INTEGER PRIMARY KEY,
              status TEXT NOT NULL,
              attempts INTEGER NOT NULL,
              max_attempts INTEGER NOT NULL,
              lease_owner TEXT,
              leased_until TEXT,
              error TEXT,
              updated_at TEXT
            );
            CREATE TABLE label_runs (
              id TEXT PRIMARY KEY,
              job_id INTEGER NOT NULL,
              status TEXT NOT NULL,
              error TEXT,
              completed_at TEXT,
              updated_at TEXT
            );
            INSERT INTO jobs
            VALUES (
              17, 'claimed', 1, 2, 'daily-owner',
              '2026-07-29T21:00:00+00:00', NULL, NULL
            );
            INSERT INTO label_runs
            VALUES ('run-17', 17, 'claimed', NULL, NULL, NULL);
            """
        )

        result = _finalize_submission_failure(
            conn,
            job_id=17,
            label_run_id="run-17",
            lease_owner="daily-owner",
            error="validator rejected metric",
        )

        job = conn.execute("SELECT * FROM jobs WHERE id = 17").fetchone()
        run = conn.execute(
            "SELECT * FROM label_runs WHERE id = 'run-17'"
        ).fetchone()
        assert result == {
            "finalized": True,
            "job_status": "pending",
            "label_run_status": "failed",
        }
        assert job["status"] == "pending"
        assert job["attempts"] == 1
        assert job["lease_owner"] is None
        assert job["leased_until"] is None
        assert job["error"] == "validator rejected metric"
        assert run["status"] == "failed"
        assert run["error"] == "validator rejected metric"
        assert run["completed_at"] is not None
    finally:
        conn.close()


def test_submission_failure_is_terminal_at_max_attempts() -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    try:
        conn.executescript(
            """
            CREATE TABLE jobs (
              id INTEGER PRIMARY KEY,
              status TEXT NOT NULL,
              attempts INTEGER NOT NULL,
              max_attempts INTEGER NOT NULL,
              lease_owner TEXT,
              leased_until TEXT,
              error TEXT,
              updated_at TEXT
            );
            CREATE TABLE label_runs (
              id TEXT PRIMARY KEY,
              job_id INTEGER NOT NULL,
              status TEXT NOT NULL,
              error TEXT,
              completed_at TEXT,
              updated_at TEXT
            );
            INSERT INTO jobs
            VALUES (18, 'claimed', 2, 2, 'daily-owner', NULL, NULL, NULL);
            INSERT INTO label_runs
            VALUES ('run-18', 18, 'claimed', NULL, NULL, NULL);
            """
        )

        result = _finalize_submission_failure(
            conn,
            job_id=18,
            label_run_id="run-18",
            lease_owner="daily-owner",
            error="invalid output",
        )

        assert result["job_status"] == "failed"
        assert (
            conn.execute("SELECT status FROM jobs WHERE id = 18").fetchone()[0]
            == "failed"
        )
    finally:
        conn.close()


def test_v31_repair_suppresses_only_ungrounded_metric_object() -> None:
    output = {
        "needs_review": False,
        "review_reason": None,
        "discourse_events": [
            {
                "evidence": "Revenue increased by 20 percent this year.",
                "metric": {
                    "value": "20",
                    "unit": "percent",
                    "comparator": None,
                    "direction": "increase",
                    "raw_text": "20 percent",
                },
                "quality_flags": [],
            },
            {
                "evidence": "The guest described a product launch.",
                "metric": {
                    "value": "99",
                    "unit": "percent",
                    "comparator": None,
                    "direction": "increase",
                    "raw_text": "99 percent",
                },
                "quality_flags": [],
            },
        ],
    }

    repairs = repair_label_output_for_submission(
        "ai_discourse_v3_1",
        output,
        segment_text=(
            "Revenue increased by 20 percent this year. "
            "The guest described a product launch."
        ),
    )

    assert repairs == 1
    assert output["discourse_events"][0]["metric"]["value"] == "20"
    repaired = output["discourse_events"][1]
    assert repaired["metric"] == {
        "value": None,
        "unit": None,
        "comparator": None,
        "direction": "not_applicable",
        "raw_text": None,
    }
    assert repaired["quality_flags"] == ["validator_rejected_metric"]
    assert output["needs_review"] is True
    assert "suppressed 1 ungrounded metric" in output["review_reason"]


def test_v31_repair_bounds_long_exact_evidence_around_claim_terms() -> None:
    prefix = "background material " * 70
    conclusion = "The system reduced inference latency for production users."
    segment_text = prefix + conclusion
    output = {
        "needs_review": False,
        "review_reason": None,
        "discourse_events": [
            {
                "claim_text": conclusion,
                "evidence": segment_text,
                "evidence_start": 0,
                "evidence_end": len(segment_text),
                "quality_flags": [],
            }
        ],
    }

    repairs = repair_label_output_for_submission(
        "ai_discourse_v3_1",
        output,
        segment_text=segment_text,
    )

    repaired = output["discourse_events"][0]
    assert repairs == 1
    assert len(repaired["evidence"]) == 1000
    assert conclusion in repaired["evidence"]
    assert (
        segment_text[repaired["evidence_start"] : repaired["evidence_end"]]
        == repaired["evidence"]
    )
