from __future__ import annotations

import sqlite3

from research_factory.worker import bulk_enqueue_missing_episode_context_jobs


def _connection() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE episodes (id TEXT PRIMARY KEY);
        CREATE TABLE transcripts (
          id TEXT PRIMARY KEY,
          episode_id TEXT NOT NULL,
          status TEXT NOT NULL
        );
        CREATE TABLE segments (
          id TEXT PRIMARY KEY,
          episode_id TEXT NOT NULL,
          segment_index INTEGER NOT NULL
        );
        CREATE TABLE episode_context_runs (
          episode_id TEXT NOT NULL,
          label_pack TEXT NOT NULL,
          model TEXT NOT NULL,
          status TEXT NOT NULL
        );
        CREATE TABLE transcript_acquisition_status (
          episode_id TEXT PRIMARY KEY,
          status TEXT NOT NULL
        );
        CREATE TABLE jobs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          lane TEXT NOT NULL,
          job_type TEXT NOT NULL,
          target_id TEXT NOT NULL,
          payload_json TEXT NOT NULL DEFAULT '{}',
          status TEXT NOT NULL DEFAULT 'pending',
          priority INTEGER NOT NULL DEFAULT 100,
          attempts INTEGER NOT NULL DEFAULT 0,
          max_attempts INTEGER NOT NULL DEFAULT 2,
          lease_owner TEXT,
          leased_until TEXT,
          dedupe_key TEXT NOT NULL UNIQUE,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          completed_at TEXT,
          error TEXT
        );
        """
    )
    episodes = (
        "eligible",
        "quarantined",
        "terminal",
        "active",
        "failed",
        "no-segments",
        "completed",
    )
    conn.executemany("INSERT INTO episodes VALUES (?)", ((item,) for item in episodes))
    conn.executemany(
        "INSERT INTO transcripts VALUES (?, ?, ?)",
        [
            ("tr-eligible", "eligible", "ready"),
            ("tr-quarantined", "quarantined", "quarantined"),
            ("tr-terminal", "terminal", "ready"),
            ("tr-active", "active", "ready"),
            ("tr-failed", "failed", "ready"),
            ("tr-no-segments", "no-segments", "ready"),
            ("tr-completed", "completed", "ready"),
        ],
    )
    conn.executemany(
        "INSERT INTO segments VALUES (?, ?, 0)",
        [
            ("seg-eligible", "eligible"),
            ("seg-quarantined", "quarantined"),
            ("seg-terminal", "terminal"),
            ("seg-active", "active"),
            ("seg-failed", "failed"),
            ("seg-completed", "completed"),
        ],
    )
    payload = (
        '{"episode_context_version":"ai_discourse_v3_1_episode_context",'
        '"label_pack":"ai_discourse_v3_1","model":"gpt-5.5",'
        '"priority_reason":"required_before_v3_1_segment_extraction"}'
    )
    conn.execute(
        """
        INSERT INTO transcript_acquisition_status
          (episode_id, status)
        VALUES ('terminal', 'manual_transcript_required')
        """
    )
    for target, status in (("active", "pending"), ("failed", "failed")):
        conn.execute(
            """
            INSERT INTO jobs
              (
                lane, job_type, target_id, payload_json, status, priority,
                attempts, max_attempts, dedupe_key, created_at, updated_at
              )
            VALUES ('podcast', 'episode_context', ?, ?, ?, 99, 0, 2, ?, 'now', 'now')
            """,
            (
                target,
                payload,
                status,
                f"podcast:episode_context:{target}:{payload}",
            ),
        )
    conn.execute(
        """
        INSERT INTO episode_context_runs
          (episode_id, label_pack, model, status)
        VALUES ('completed', 'ai_discourse_v3_1', 'gpt-5.5', 'completed')
        """
    )
    return conn


def test_bulk_context_enqueue_is_idempotent_and_counts_skips() -> None:
    conn = _connection()
    first = bulk_enqueue_missing_episode_context_jobs(conn)
    conn.commit()
    second = bulk_enqueue_missing_episode_context_jobs(conn)

    assert first["candidate_episodes"] == 6
    assert first["enqueued"] == 1
    assert first["skipped_by_reason"] == {
        "already_pending_or_claimed": 1,
        "existing_failed_context_job": 1,
        "no_segments": 1,
        "quarantined_transcript": 1,
        "terminal_transcript_fetch": 1,
    }
    assert first["pending_episode_context"] == 2
    assert first["duplicate_dedupe_keys"] == 0
    assert second["enqueued"] == 0
    assert second["skipped_by_reason"]["already_pending_or_claimed"] == 2
    assert second["duplicate_dedupe_keys"] == 0
