from __future__ import annotations

import sqlite3
import unittest
from unittest import mock

from research_factory import db


class SchemaMigrationBaselineAdoptionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.original = (
            (
                1,
                "fixture_v1",
                ("CREATE TABLE fixture (id INTEGER PRIMARY KEY)",),
            ),
        )
        self.adopted = (
            (
                1,
                "fixture_v1",
                ("CREATE TABLE fixture (id INTEGER PRIMARY KEY, note TEXT)",),
            ),
        )
        with mock.patch.object(db, "SCHEMA_MIGRATIONS", self.original):
            self.assertEqual(db.apply_schema_migrations(self.conn), [1])
        self.conn.commit()

    def tearDown(self) -> None:
        self.conn.close()

    def _record_adoption(self) -> None:
        old_name, old_statements = self.original[0][1:]
        new_name, new_statements = self.adopted[0][1:]
        db.record_schema_migration_baseline_adoption(
            self.conn,
            migration_version=1,
            migration_name=old_name,
            superseded_checksum_sha256=db.schema_migration_checksum(
                old_name, old_statements
            ),
            adopted_checksum_sha256=db.schema_migration_checksum(
                new_name, new_statements
            ),
            declared_objects=("fixture",),
            divergent_views=("fixture_view",),
            justification="Fixture-only recorded checksum transition.",
        )

    def test_unrecorded_mismatch_still_raises(self) -> None:
        with mock.patch.object(db, "SCHEMA_MIGRATIONS", self.adopted):
            with self.assertRaisesRegex(
                RuntimeError, "differs from immutable applied history"
            ):
                db.apply_schema_migrations(self.conn)

    def test_recorded_adoption_permits_exact_triple_only(self) -> None:
        self._record_adoption()
        with mock.patch.object(db, "SCHEMA_MIGRATIONS", self.adopted):
            self.assertEqual(db.apply_schema_migrations(self.conn), [])

        another_edit = (
            (
                1,
                "fixture_v1",
                (
                    "CREATE TABLE fixture "
                    "(id INTEGER PRIMARY KEY, note TEXT, other TEXT)",
                ),
            ),
        )
        with mock.patch.object(db, "SCHEMA_MIGRATIONS", another_edit):
            with self.assertRaisesRegex(
                RuntimeError, "differs from immutable applied history"
            ):
                db.apply_schema_migrations(self.conn)

    def test_adoption_rows_cannot_be_updated_or_deleted(self) -> None:
        self._record_adoption()
        with self.assertRaisesRegex(sqlite3.IntegrityError, "immutable"):
            self.conn.execute(
                """
                UPDATE schema_migration_baseline_adoptions
                SET justification = 'changed'
                WHERE migration_version = 1
                """
            )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "immutable"):
            self.conn.execute(
                """
                DELETE FROM schema_migration_baseline_adoptions
                WHERE migration_version = 1
                """
            )

    def test_record_cannot_rebind_an_applied_checksum(self) -> None:
        original_name, original_statements = self.original[0][1:]
        adopted_name, adopted_statements = self.adopted[0][1:]
        with self.assertRaisesRegex(RuntimeError, "does not match applied history"):
            db.record_schema_migration_baseline_adoption(
                self.conn,
                migration_version=1,
                migration_name=original_name,
                superseded_checksum_sha256="f" * 64,
                adopted_checksum_sha256=db.schema_migration_checksum(
                    adopted_name, adopted_statements
                ),
                declared_objects=("fixture",),
                divergent_views=("fixture_view",),
                justification="Must not be accepted.",
            )
        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) FROM schema_migration_baseline_adoptions"
            ).fetchone()[0],
            0,
        )


if __name__ == "__main__":
    unittest.main()
