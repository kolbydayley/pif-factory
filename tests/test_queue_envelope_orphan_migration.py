from __future__ import annotations

import sqlite3
import unittest

from research_factory import db


TS = "2026-07-20T12:00:00+00:00"


class QueueEnvelopeOrphanMigrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(db.SCHEMA)
        db.migrate_schema(self.conn)
        self.conn.commit()

        self.conn.execute("PRAGMA foreign_keys = OFF")
        self.conn.execute(
            """
            INSERT INTO jobs (
              id, lane, job_type, target_id, payload_json, status, priority,
              attempts, max_attempts, dedupe_key, created_at, updated_at
            )
            VALUES (41, 'local', 'fixture', 'valid-target', '{}', 'pending', 100,
                    0, 2, 'valid-job', ?, ?)
            """,
            (TS, TS),
        )
        self.original = {
            "job_id": 999,
            "queue_name": "semantic-review",
            "worker_role": "claim-adjudicator",
            "content_type": "podcast_episode",
            "source_id": "source-1",
            "item_id": "item-1",
            "artifact_id": "artifact-1",
            "label_pack": "ai_discourse_v3_1",
            "model_required": "gpt-5.5",
            "privacy_tier": "local_only",
            "lease_ttl_seconds": 1234,
            "input_context_ref": "private/input.json",
            "output_schema_ref": "schemas/output.json",
            "callback_mode": "local_submit",
            "run_tag": "orphan-fixture",
            "allowed_tools_json": '["read"]',
            "capabilities_json": '["semantic_judgment"]',
            "remote_claimable": 0,
            "created_at": TS,
            "updated_at": TS,
        }
        columns = ", ".join(self.original)
        placeholders = ", ".join("?" for _ in self.original)
        self.conn.execute(
            f"INSERT INTO queue_envelopes ({columns}) VALUES ({placeholders})",
            tuple(self.original.values()),
        )
        valid = dict(self.original)
        valid.update(job_id=41, queue_name="valid-queue", run_tag="valid-fixture")
        self.conn.execute(
            f"INSERT INTO queue_envelopes ({columns}) VALUES ({placeholders})",
            tuple(valid.values()),
        )
        self.conn.commit()
        self.conn.execute("PRAGMA foreign_keys = ON")

    def tearDown(self) -> None:
        self.conn.close()

    def test_v2_archives_complete_orphan_then_removes_only_derived_row(self) -> None:
        before = self.conn.execute("PRAGMA foreign_key_check").fetchall()
        self.assertEqual(len(before), 1)
        self.assertEqual(before[0]["table"], "queue_envelopes")

        self.assertEqual(db.apply_schema_migrations(self.conn), [1, 2, 3, 4])
        self.assertEqual(db.schema_migration_version(self.conn), 4)

        remaining = self.conn.execute(
            "SELECT job_id, queue_name, run_tag FROM queue_envelopes ORDER BY job_id"
        ).fetchall()
        self.assertEqual(
            [dict(row) for row in remaining],
            [{"job_id": 41, "queue_name": "valid-queue", "run_tag": "valid-fixture"}],
        )

        archived = self.conn.execute(
            "SELECT * FROM queue_envelope_orphan_archive WHERE job_id = 999"
        ).fetchone()
        self.assertIsNotNone(archived)
        for key, expected in self.original.items():
            self.assertEqual(archived[key], expected, key)
        self.assertEqual(archived["reason"], "missing_jobs_parent_at_migration_v2")
        self.assertTrue(archived["archived_at"].endswith("Z"))
        self.assertEqual(self.conn.execute("PRAGMA foreign_key_check").fetchall(), [])

        self.assertEqual(db.apply_schema_migrations(self.conn), [])
        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) FROM queue_envelope_orphan_archive"
            ).fetchone()[0],
            1,
        )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "immutable"):
            self.conn.execute(
                "UPDATE queue_envelope_orphan_archive SET reason = 'changed' WHERE job_id = 999"
            )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "immutable"):
            self.conn.execute("DELETE FROM queue_envelope_orphan_archive WHERE job_id = 999")


if __name__ == "__main__":
    unittest.main()
