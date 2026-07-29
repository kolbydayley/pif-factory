"""Recoverable, receipt-bound SQLite migration orchestration.

``db.apply_schema_migrations`` provides transactional forward migrations.  This
module adds the outer filesystem safety boundary required for production: a
standalone verified SQLite backup is created before any pending migration, and
the exact backup is restored if initialization or post-migration validation
fails.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
from pathlib import Path
from typing import Any, Callable

from . import db
from .util import dumps_json, now_iso, stable_id


class MigrationRecoveryError(RuntimeError):
    """A migration failed after the verified backup was restored."""

    def __init__(self, message: str, *, receipt: dict[str, Any]):
        super().__init__(message)
        self.receipt = receipt


def latest_schema_version() -> int:
    return max((version for version, _name, _statements in db.SCHEMA_MIGRATIONS), default=0)


def pending_schema_versions(database_path: str | Path) -> list[int]:
    path = Path(database_path).expanduser().resolve()
    known = [version for version, _name, _statements in db.SCHEMA_MIGRATIONS]
    if not path.exists() or path.stat().st_size == 0:
        return known
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_migrations'"
        ).fetchone()
        applied = (
            {int(row[0]) for row in conn.execute("SELECT version FROM schema_migrations")}
            if exists
            else set()
        )
    finally:
        conn.close()
    return [version for version in known if version not in applied]


def migrate_with_verified_backup(
    database_path: str | Path,
    *,
    backup_dir: str | Path,
    receipt_dir: str | Path | None = None,
    initialize: Callable[[sqlite3.Connection], None] = db.init_db,
) -> dict[str, Any]:
    """Apply pending migrations or restore the verified pre-migration backup.

    New empty databases do not need a recovery backup. Existing databases are
    checkpointed through SQLite's online backup API into a standalone file,
    integrity-checked, and hash-bound before ``initialize`` is called.
    """

    path = Path(database_path).expanduser().resolve()
    pending = pending_schema_versions(path)
    if not pending:
        return {
            "ok": True,
            "state": "already_current",
            "database_path": str(path),
            "schema_version": latest_schema_version(),
            "applied_versions": [],
            "restored": False,
        }
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = path.exists() and path.stat().st_size > 0
    generated_at = now_iso()
    migration_id = stable_id(str(path), generated_at, *map(str, pending), prefix="mig_")
    backup_root = Path(backup_dir).expanduser().resolve()
    backup_root.mkdir(parents=True, exist_ok=True)
    backup_path = backup_root / f"{path.name}.{migration_id}.sqlite"
    failed_path = backup_root / f"{path.name}.{migration_id}.failed.sqlite"
    receipt_root = Path(receipt_dir).expanduser().resolve() if receipt_dir else backup_root
    receipt_root.mkdir(parents=True, exist_ok=True)
    receipt_path = receipt_root / f"{migration_id}.json"

    source_sha256 = _sha256(path) if existing else None
    backup_sha256 = None
    if existing:
        _online_backup(path, backup_path)
        _verify_sqlite(backup_path)
        backup_sha256 = _sha256(backup_path)

    applied_before = _applied_versions(path) if existing else []
    try:
        conn = db.connect(path)
        try:
            initialize(conn)
            integrity = str(conn.execute("PRAGMA integrity_check").fetchone()[0])
            foreign_key_errors = len(conn.execute("PRAGMA foreign_key_check").fetchall())
            if integrity != "ok" or foreign_key_errors:
                raise RuntimeError(
                    f"post-migration validation failed: integrity={integrity}, "
                    f"foreign_key_errors={foreign_key_errors}"
                )
            applied_after = [
                int(row[0])
                for row in conn.execute(
                    "SELECT version FROM schema_migrations ORDER BY version"
                ).fetchall()
            ]
        finally:
            conn.close()
    except Exception as exc:
        if existing:
            if path.exists():
                os.replace(path, failed_path)
            shutil.copy2(backup_path, path)
            _verify_sqlite(path)
            restored_sha256 = _sha256(path)
            if restored_sha256 != backup_sha256:
                raise RuntimeError("restored SQLite backup hash does not match") from exc
        receipt = {
            "schema_version": "pif_migration_receipt_v1",
            "id": migration_id,
            "ok": False,
            "state": "failed_restored" if existing else "failed_new_database",
            "database_path": str(path),
            "backup_path": str(backup_path) if existing else None,
            "failed_database_path": str(failed_path) if existing else None,
            "source_sha256": source_sha256,
            "backup_sha256": backup_sha256,
            "pending_versions": pending,
            "applied_versions_before": applied_before,
            "restored": existing,
            "error_type": type(exc).__name__,
            "error": str(exc)[:1000],
            "generated_at": generated_at,
        }
        _write_receipt(receipt_path, receipt)
        raise MigrationRecoveryError(
            "schema migration failed; verified backup restored" if existing else "new database initialization failed",
            receipt=receipt,
        ) from exc

    receipt = {
        "schema_version": "pif_migration_receipt_v1",
        "id": migration_id,
        "ok": True,
        "state": "migrated",
        "database_path": str(path),
        "backup_path": str(backup_path) if existing else None,
        "source_sha256": source_sha256,
        "backup_sha256": backup_sha256,
        "pending_versions": pending,
        "applied_versions_before": applied_before,
        "applied_versions_after": applied_after,
        "applied_versions": sorted(set(applied_after) - set(applied_before)),
        "integrity_check": "ok",
        "foreign_key_errors": 0,
        "restored": False,
        "generated_at": generated_at,
    }
    _write_receipt(receipt_path, receipt)
    receipt["receipt_path"] = str(receipt_path)
    return receipt


def _online_backup(source_path: Path, destination_path: Path) -> None:
    source = sqlite3.connect(f"file:{source_path}?mode=ro", uri=True)
    destination = sqlite3.connect(destination_path)
    try:
        source.backup(destination)
        destination.commit()
    finally:
        destination.close()
        source.close()
    os.chmod(destination_path, 0o600)


def _verify_sqlite(path: Path) -> None:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        result = str(conn.execute("PRAGMA integrity_check").fetchone()[0])
        if result != "ok":
            raise RuntimeError(f"SQLite backup integrity check failed: {result}")
    finally:
        conn.close()


def _applied_versions(path: Path) -> list[int]:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_migrations'"
        ).fetchone()
        if not exists:
            return []
        return [int(row[0]) for row in conn.execute("SELECT version FROM schema_migrations ORDER BY version")]
    finally:
        conn.close()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_receipt(path: Path, receipt: dict[str, Any]) -> None:
    payload = dumps_json(receipt) + "\n"
    with path.open("x", encoding="utf-8") as handle:
        handle.write(payload)
    os.chmod(path, 0o600)


__all__ = [
    "MigrationRecoveryError",
    "latest_schema_version",
    "migrate_with_verified_backup",
    "pending_schema_versions",
]
