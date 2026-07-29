from __future__ import annotations

from pathlib import Path

from research_factory import db
from research_factory.ingest import (
    classify_fetch_failure,
    enqueue_transcript_backlog,
    route_terminal_fetch_failures,
)


def _insert_episode(conn, *, episode_id: str, transcript_url: str) -> None:
    ts = "2026-07-29T00:00:00+00:00"
    conn.execute(
        """
        INSERT OR IGNORE INTO sources (
          id, name, rss_url, homepage_url, category, policy,
          transcript_policy, enabled, metadata_json, created_at, updated_at
        )
        VALUES (
          'odd-lots', 'Odd Lots', NULL, NULL, 'technology',
          'private_analysis_only',
          'creator_rss_or_official_public_transcripts',
          1, '{}', ?, ?
        )
        """,
        (ts, ts),
    )
    conn.execute(
        """
        INSERT INTO episodes (
          id, source_id, guid, title, published_at, transcript_url,
          transcript_type, created_at, updated_at
        )
        VALUES (?, 'odd-lots', ?, ?, ?, ?, 'text/plain', ?, ?)
        """,
        (
            episode_id,
            episode_id,
            f"Episode {episode_id}",
            ts,
            transcript_url,
            ts,
            ts,
        ),
    )


def _failed_fetch(
    conn,
    *,
    episode_id: str,
    source_kind: str,
    error: str,
) -> int:
    job_id = db.enqueue_job(
        conn,
        lane="podcast",
        job_type="fetch_transcript",
        target_id=episode_id,
        payload={"source_kind": source_kind, "label_pack": "ai_discourse_v3_1"},
        max_attempts=2,
    )
    conn.execute(
        """
        UPDATE jobs
        SET status = 'failed', attempts = max_attempts, error = ?
        WHERE id = ?
        """,
        (error, job_id),
    )
    return int(job_id)


def test_fetch_failure_classifier_separates_terminal_and_retryable() -> None:
    youtube = classify_fetch_failure(
        source_kind="youtube_captions",
        error="YouTube is blocking requests from your IP.",
    )
    lex = classify_fetch_failure(
        source_kind="official_show_transcript",
        error="lex_slug_derived_route_disabled_after_high_failure_rate",
    )
    transient = classify_fetch_failure(
        source_kind="youtube_captions",
        error="stale_fetch_claim_recovered_no_live_worker",
    )

    assert youtube["classification"] == "terminal"
    assert youtube["reason_code"] == "youtube_public_caption_unavailable"
    assert lex["classification"] == "terminal"
    assert lex["reason_code"] == "disabled_lex_transcript_route"
    assert transient["classification"] == "retryable"
    assert transient["status"] == "fetch_failed"


def test_terminal_failure_router_is_bounded_and_idempotent(
    tmp_path: Path,
) -> None:
    conn = db.connect(tmp_path / "factory.sqlite")
    try:
        db.init_db(conn)
        _insert_episode(
            conn,
            episode_id="ep_youtube",
            transcript_url="https://www.youtube.com/watch?v=fixture",
        )
        _insert_episode(
            conn,
            episode_id="ep_lex",
            transcript_url="https://example.test/lex-transcript",
        )
        _insert_episode(
            conn,
            episode_id="ep_retryable",
            transcript_url="https://example.test/retryable-transcript",
        )
        _failed_fetch(
            conn,
            episode_id="ep_youtube",
            source_kind="youtube_captions",
            error="YouTube is blocking requests from your IP.",
        )
        _failed_fetch(
            conn,
            episode_id="ep_lex",
            source_kind="official_show_transcript",
            error="lex_slug_derived_route_disabled_after_high_failure_rate",
        )
        _failed_fetch(
            conn,
            episode_id="ep_retryable",
            source_kind="official_show_transcript",
            error="temporary connection reset",
        )
        conn.commit()

        first = route_terminal_fetch_failures(
            conn,
            lane="podcast",
            limit=1,
        )
        second = route_terminal_fetch_failures(
            conn,
            lane="podcast",
            limit=1,
        )
        third = route_terminal_fetch_failures(
            conn,
            lane="podcast",
            limit=1,
        )

        assert first["routed"] == 1
        assert second["routed"] == 1
        assert third["routed"] == 0
        statuses = {
            row["episode_id"]: row["status"]
            for row in conn.execute(
                "SELECT episode_id, status FROM transcript_acquisition_status"
            )
        }
        assert statuses == {
            "ep_youtube": "manual_transcript_required",
            "ep_lex": "manual_transcript_required",
        }
        manual_jobs = conn.execute(
            """
            SELECT target_id, payload_json
            FROM jobs
            WHERE job_type = 'manual_transcript_required'
            ORDER BY target_id
            """
        ).fetchall()
        assert [row["target_id"] for row in manual_jobs] == [
            "ep_lex",
            "ep_youtube",
        ]
        assert all('"disposition":' in row["payload_json"] for row in manual_jobs)

        backlog = enqueue_transcript_backlog(
            conn,
            lane="podcast",
            label_pack="ai_discourse_v3_1",
            limit=10,
            include_youtube_captions=True,
            dry_run=True,
        )
        selected_ids = {item["episode_id"] for item in backlog["jobs"]}
        assert "ep_youtube" not in selected_ids
        assert "ep_lex" not in selected_ids
        assert "ep_retryable" in selected_ids
    finally:
        conn.close()
