from __future__ import annotations

import hashlib
import sqlite3
import tempfile
import unittest
from pathlib import Path

from research_factory.migration_ops import (
    MigrationRecoveryError,
    _online_backup,
    _verify_sqlite,
    migrate_with_verified_backup,
)


class MigrationRecoveryTest(unittest.TestCase):
    def test_wal_source_produces_standalone_read_only_verifiable_backup(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            database = root / "factory.sqlite"
            backup = root / "backup.sqlite"
            conn = sqlite3.connect(database)
            self.assertEqual(conn.execute("PRAGMA journal_mode=WAL").fetchone()[0], "wal")
            conn.execute("CREATE TABLE durable_probe (value TEXT NOT NULL)")
            conn.execute("INSERT INTO durable_probe VALUES ('before')")
            conn.commit()
            conn.close()

            _online_backup(database, backup)
            _verify_sqlite(backup)

            verified = sqlite3.connect(f"file:{backup}?mode=ro", uri=True)
            try:
                self.assertEqual(
                    verified.execute("PRAGMA journal_mode").fetchone()[0],
                    "delete",
                )
                self.assertEqual(
                    verified.execute("SELECT value FROM durable_probe").fetchone()[0],
                    "before",
                )
            finally:
                verified.close()

    def test_failed_migration_restores_verified_backup(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            database = root / "factory.sqlite"
            conn = sqlite3.connect(database)
            conn.execute("CREATE TABLE durable_probe (value TEXT NOT NULL)")
            conn.execute("INSERT INTO durable_probe VALUES ('before')")
            conn.commit()
            conn.close()
            before = hashlib.sha256(database.read_bytes()).hexdigest()

            def fail(connection: sqlite3.Connection) -> None:
                connection.execute("UPDATE durable_probe SET value='corrupted'")
                connection.commit()
                raise RuntimeError("synthetic migration failure")

            with self.assertRaises(MigrationRecoveryError) as raised:
                migrate_with_verified_backup(
                    database,
                    backup_dir=root / "backups",
                    initialize=fail,
                )
            self.assertTrue(raised.exception.receipt["restored"])
            restored = sqlite3.connect(database)
            try:
                self.assertEqual(restored.execute("SELECT value FROM durable_probe").fetchone()[0], "before")
                self.assertEqual(restored.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            finally:
                restored.close()
            backup = Path(raised.exception.receipt["backup_path"])
            self.assertEqual(hashlib.sha256(database.read_bytes()).hexdigest(), hashlib.sha256(backup.read_bytes()).hexdigest())
            self.assertNotEqual(before, "")


if __name__ == "__main__":
    unittest.main()
