from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from research_factory import db
from research_factory.ingest import record_transcript_acquisition_attempt, transcript_candidates
from research_factory.util import now_iso


class TranscriptAcquisitionIdempotencyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db_file = Path(self.tmp.name) / "factory.sqlite"
        self.conn = db.connect(self.db_file)
        db.init_db(self.conn)
        ts = now_iso()
        self.conn.execute(
            "INSERT INTO sources (id, name, created_at, updated_at) VALUES ('source-1', 'Source 1', ?, ?)",
            (ts, ts),
        )
        self.conn.execute(
            """
            INSERT INTO episodes (id, source_id, guid, title, created_at, updated_at)
            VALUES ('episode-1', 'source-1', 'guid-1', 'Episode 1', ?, ?)
            """,
            (ts, ts),
        )
        db.enqueue_job(
            self.conn,
            lane="podcast",
            job_type="manual_transcript_required",
            target_id="episode-1",
            payload={"reason": "fixture"},
            max_attempts=1,
        )
        self.conn.commit()

    def tearDown(self) -> None:
        self.conn.close()
        self.tmp.cleanup()

    def test_identical_attempt_is_idempotent_and_does_not_increment_count(self) -> None:
        first = record_transcript_acquisition_attempt(
            self.conn,
            episode_id="episode-1",
            method="youtube_caption",
            status="youtube_caption_blocked",
            error_class="youtube_caption_blocked",
        )
        second = record_transcript_acquisition_attempt(
            self.conn,
            episode_id="episode-1",
            method="youtube_caption",
            status="youtube_caption_blocked",
            error_class="youtube_caption_blocked",
        )

        self.assertFalse(first["duplicate"])
        self.assertTrue(second["duplicate"])
        self.assertEqual(first["attempt_id"], second["attempt_id"])
        self.assertIsNotNone(first["next_eligible_at"])
        count = self.conn.execute("SELECT COUNT(*) FROM transcript_acquisition_attempts").fetchone()[0]
        status = self.conn.execute(
            "SELECT attempts_count FROM transcript_acquisition_status WHERE episode_id = 'episode-1'"
        ).fetchone()
        self.assertEqual(count, 1)
        self.assertEqual(status["attempts_count"], 1)

    def test_candidate_is_hidden_until_cooldown_expires(self) -> None:
        record_transcript_acquisition_attempt(
            self.conn,
            episode_id="episode-1",
            method="official_page",
            status="not_found",
        )
        self.conn.commit()
        self.assertEqual(transcript_candidates(self.conn, lane="podcast", limit=10), [])

        self.conn.execute(
            "UPDATE transcript_acquisition_status SET next_eligible_at = '2000-01-01T00:00:00+00:00' WHERE episode_id = 'episode-1'"
        )
        self.conn.commit()
        self.assertEqual(len(transcript_candidates(self.conn, lane="podcast", limit=10)), 1)


if __name__ == "__main__":
    unittest.main()
