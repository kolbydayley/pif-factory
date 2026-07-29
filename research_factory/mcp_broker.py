from __future__ import annotations

import base64
import calendar
import copy
import hashlib
import hmac
import json
import os
import secrets
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

from .util import dumps_json, loads_json, now_iso, stable_id


DEFAULT_OBSERVER_URL = "https://observer-ui-production.up.railway.app"
DEFAULT_SCOPES = {"factory.status", "factory.control"}
FUTURE_SCOPES = {"factory.claim", "factory.submit"}
ALL_SCOPES = DEFAULT_SCOPES | FUTURE_SCOPES
DEFAULT_CONTEXT_CHUNK_CHARS = 8000
MAX_WORK_NOTE_CHARS = 6000
RAW_CHUNK_ENV = "PIF_MCP_RAW_CHUNKS_ENABLED"
EPISODE_CONTEXT_SCHEMA_VERSION = "ai_discourse_v3_1_episode_context"
MAX_SOURCE_CARD_EXCERPT_CHARS = 500
MAX_SOURCE_CARD_TEXT_CHARS = 1500
MAX_SOURCE_CARDS = 12
MAX_SOURCE_CARD_BATCH_TASKS = 3
MAX_CHATGPT_TURN_SUCCESS = 6
MAX_SOURCE_CARD_RESERVE_ATTEMPTS = 12
SOURCE_CARD_SKIP_REASON_CODES = {
    "tool_safety_block",
    "validation_failed",
    "insufficient_features",
    "worker_timeout",
    "other",
}


def default_broker_path() -> Path:
    return Path(os.environ.get("PIF_MCP_BROKER_DB", "broker_state/mcp-broker.sqlite")).expanduser()


def default_broker_database_url() -> str | None:
    return os.environ.get("PIF_MCP_DATABASE_URL") or os.environ.get("DATABASE_URL")


def broker_store_from_env() -> "BrokerStore":
    return BrokerStore(database_url=default_broker_database_url())


def context_chunk_chars() -> int:
    raw = os.environ.get("PIF_MCP_CONTEXT_CHUNK_CHARS")
    if not raw:
        return DEFAULT_CONTEXT_CHUNK_CHARS
    try:
        value = int(raw)
    except ValueError:
        return DEFAULT_CONTEXT_CHUNK_CHARS
    return max(1000, min(value, 20000))


def raw_chunks_enabled() -> bool:
    return os.environ.get(RAW_CHUNK_ENV, "0") == "1"


def _parse_iso_seconds(value: str) -> float:
    parsed = time.strptime(value.replace("Z", "+00:00"), "%Y-%m-%dT%H:%M:%S+00:00")
    return float(calendar.timegm(parsed))


class BrokerStore:
    def __init__(self, path: str | Path | None = None, *, database_url: str | None = None):
        self.database_url = database_url if path is None else None
        self.backend = "postgres" if self.database_url else "sqlite"
        self.path = Path(path).expanduser() if path else default_broker_path()
        self._lock = threading.RLock()
        if self.backend == "postgres":
            try:
                import psycopg
                from psycopg.rows import dict_row
            except ImportError as exc:  # pragma: no cover - exercised only in Railway envs without deps.
                raise RuntimeError("PIF_MCP_DATABASE_URL/DATABASE_URL is set but psycopg is not installed") from exc
            self.conn = psycopg.connect(self.database_url, row_factory=dict_row)
        else:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.conn = sqlite3.connect(self.path, check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
            self._execute("PRAGMA busy_timeout = 30000")
        self.init()

    def close(self) -> None:
        with self._lock:
            self.conn.close()

    def storage_info(self) -> dict[str, Any]:
        return {
            "backend": self.backend,
            "durable": self.backend == "postgres",
            "location": "railway_postgres" if self.backend == "postgres" else str(self.path),
        }

    def _execute(self, sql: str, params: tuple[Any, ...] = ()):
        if self.backend == "postgres":
            sql = sql.replace("?", "%s")
        return self.conn.execute(sql, params)

    def init(self) -> None:
        statements = [
            """
            CREATE TABLE IF NOT EXISTS broker_snapshots (
              id TEXT PRIMARY KEY,
              payload_json TEXT NOT NULL,
              created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS broker_control_requests (
              id TEXT PRIMARY KEY,
              request_type TEXT NOT NULL,
              requested_by TEXT NOT NULL,
              status TEXT NOT NULL DEFAULT 'pending',
              payload_json TEXT NOT NULL DEFAULT '{}',
              created_at TEXT NOT NULL,
              claimed_at TEXT,
              completed_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS oauth_codes (
              code TEXT PRIMARY KEY,
              client_id TEXT NOT NULL,
              redirect_uri TEXT NOT NULL,
              scope TEXT NOT NULL,
              code_challenge TEXT,
              code_challenge_method TEXT,
              created_at INTEGER NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS oauth_clients (
              client_id TEXT PRIMARY KEY,
              metadata_json TEXT NOT NULL DEFAULT '{}',
              created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS oauth_refresh_tokens (
              token_hash TEXT PRIMARY KEY,
              client_id TEXT NOT NULL,
              scope TEXT NOT NULL,
              created_at INTEGER NOT NULL,
              expires_at INTEGER NOT NULL,
              revoked_at INTEGER
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS broker_events (
              id TEXT PRIMARY KEY,
              event_type TEXT NOT NULL,
              event_json TEXT NOT NULL DEFAULT '{}',
              created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS remote_work_packages (
              id TEXT PRIMARY KEY,
              title TEXT NOT NULL,
              privacy_tier TEXT NOT NULL,
              status TEXT NOT NULL DEFAULT 'pending',
              worker_id TEXT,
              context_json TEXT NOT NULL,
              output_schema_json TEXT NOT NULL DEFAULT '{}',
              output_json TEXT,
              validation_json TEXT NOT NULL DEFAULT '{}',
              created_at TEXT NOT NULL,
              claimed_at TEXT,
              leased_until TEXT,
              submitted_at TEXT,
              released_at TEXT
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_broker_snapshots_created_at ON broker_snapshots(created_at)",
            "CREATE INDEX IF NOT EXISTS idx_broker_events_created_at ON broker_events(created_at)",
            "CREATE INDEX IF NOT EXISTS idx_oauth_refresh_client ON oauth_refresh_tokens(client_id, revoked_at, expires_at)",
            "CREATE INDEX IF NOT EXISTS idx_remote_work_status_created ON remote_work_packages(status, created_at)",
            "CREATE INDEX IF NOT EXISTS idx_remote_work_submitted ON remote_work_packages(status, submitted_at)",
        ]
        with self._lock:
            for statement in statements:
                self._execute(statement)
            self.conn.commit()

    def write_snapshot(self, payload: dict[str, Any]) -> dict[str, Any]:
        snapshot = sanitize_broker_snapshot(payload)
        snapshot_id = stable_id(snapshot.get("generated_at") or now_iso(), dumps_json(snapshot), prefix="bs_")
        with self._lock:
            if self.backend == "postgres":
                self._execute(
                    """
                    INSERT INTO broker_snapshots (id, payload_json, created_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                      payload_json = excluded.payload_json,
                      created_at = excluded.created_at
                    """,
                    (snapshot_id, dumps_json(snapshot), now_iso()),
                )
            else:
                self._execute(
                    "INSERT OR REPLACE INTO broker_snapshots (id, payload_json, created_at) VALUES (?, ?, ?)",
                    (snapshot_id, dumps_json(snapshot), now_iso()),
                )
            self.record_event("snapshot_ingested", {"snapshot_id": snapshot_id})
            self.conn.commit()
        return {"ok": True, "snapshot_id": snapshot_id}

    def latest_snapshot(self) -> dict[str, Any]:
        with self._lock:
            row = self._execute(
                "SELECT payload_json, created_at FROM broker_snapshots ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
        if not row:
            return default_snapshot()
        payload = loads_json(row["payload_json"], {})
        payload.setdefault("broker_snapshot_created_at", row["created_at"])
        return payload

    def create_control_request(self, *, request_type: str, requested_by: str, payload: dict[str, Any]) -> dict[str, Any]:
        request_id = stable_id(request_type, requested_by, dumps_json(payload), now_iso(), prefix="cr_")
        with self._lock:
            self._execute(
                """
            INSERT INTO broker_control_requests
              (id, request_type, requested_by, status, payload_json, created_at)
            VALUES (?, ?, ?, 'pending', ?, ?)
            """,
                (request_id, request_type, requested_by, dumps_json(payload), now_iso()),
            )
            self.record_event("control_request_created", {"request_id": request_id, "request_type": request_type})
            self.conn.commit()
        return {"ok": True, "request_id": request_id, "status": "pending"}

    def control_request_counts(self) -> dict[str, int]:
        with self._lock:
            return {
                row["status"]: int(row["count"])
                for row in self._execute(
                    "SELECT status, COUNT(*) AS count FROM broker_control_requests GROUP BY status"
                ).fetchall()
            }

    def register_client(self, metadata: dict[str, Any]) -> dict[str, Any]:
        client_id = metadata.get("client_id") or f"pif-client-{secrets.token_urlsafe(18)}"
        with self._lock:
            if self.backend == "postgres":
                self._execute(
                    """
                    INSERT INTO oauth_clients (client_id, metadata_json, created_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(client_id) DO UPDATE SET
                      metadata_json = excluded.metadata_json,
                      created_at = excluded.created_at
                    """,
                    (client_id, dumps_json(metadata), now_iso()),
                )
            else:
                self._execute(
                    "INSERT OR REPLACE INTO oauth_clients (client_id, metadata_json, created_at) VALUES (?, ?, ?)",
                    (client_id, dumps_json(metadata), now_iso()),
                )
            self.record_event("oauth_client_registered", {"client_id": client_id})
            self.conn.commit()
        return {"client_id": client_id, "client_id_issued_at": int(time.time())}

    def create_oauth_code(
        self,
        *,
        client_id: str,
        redirect_uri: str,
        scope: str,
        code_challenge: str | None,
        code_challenge_method: str | None,
    ) -> str:
        code = secrets.token_urlsafe(32)
        with self._lock:
            self._execute(
                """
            INSERT INTO oauth_codes
              (code, client_id, redirect_uri, scope, code_challenge, code_challenge_method, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                (code, client_id, redirect_uri, scope, code_challenge, code_challenge_method, int(time.time())),
            )
            self.conn.commit()
        return code

    def pop_oauth_code(self, code: str) -> Any | None:
        with self._lock:
            row = self._execute("SELECT * FROM oauth_codes WHERE code = ?", (code,)).fetchone()
            if row:
                self._execute("DELETE FROM oauth_codes WHERE code = ?", (code,))
                self.conn.commit()
            return row

    def create_refresh_token(self, *, client_id: str, scope: str) -> str:
        token = secrets.token_urlsafe(48)
        now = int(time.time())
        with self._lock:
            self._execute(
                """
                INSERT INTO oauth_refresh_tokens
                  (token_hash, client_id, scope, created_at, expires_at, revoked_at)
                VALUES (?, ?, ?, ?, ?, NULL)
                """,
                (_refresh_token_hash(token), client_id, scope, now, now + 90 * 24 * 60 * 60),
            )
            self.record_event("oauth_refresh_token_created", {"client_id": client_id})
            self.conn.commit()
        return token

    def rotate_refresh_token(self, token: str) -> dict[str, Any] | None:
        token_hash = _refresh_token_hash(token)
        now = int(time.time())
        with self._lock:
            row = self._execute(
                """
                SELECT client_id, scope
                FROM oauth_refresh_tokens
                WHERE token_hash = ?
                  AND revoked_at IS NULL
                  AND expires_at > ?
                """,
                (token_hash, now),
            ).fetchone()
            if not row:
                return None
            self._execute("UPDATE oauth_refresh_tokens SET revoked_at = ? WHERE token_hash = ?", (now, token_hash))
            new_token = secrets.token_urlsafe(48)
            self._execute(
                """
                INSERT INTO oauth_refresh_tokens
                  (token_hash, client_id, scope, created_at, expires_at, revoked_at)
                VALUES (?, ?, ?, ?, ?, NULL)
                """,
                (_refresh_token_hash(new_token), row["client_id"], row["scope"], now, now + 90 * 24 * 60 * 60),
            )
            self.record_event("oauth_refresh_token_rotated", {"client_id": row["client_id"]})
            self.conn.commit()
        return {"client_id": row["client_id"], "scope": row["scope"], "refresh_token": new_token}

    def record_event(self, event_type: str, event: dict[str, Any]) -> str:
        event_id = stable_id(event_type, dumps_json(event), now_iso(), prefix="be_")
        self._execute(
            "INSERT INTO broker_events (id, event_type, event_json, created_at) VALUES (?, ?, ?, ?)",
            (event_id, event_type, dumps_json(event), now_iso()),
        )
        return event_id

    def seed_smoke_work_package(self) -> dict[str, Any]:
        package = smoke_work_package()
        return self.upsert_work_packages([package], replace=True)

    def upsert_work_packages(self, packages: list[dict[str, Any]], *, replace: bool = False) -> dict[str, Any]:
        accepted = 0
        skipped = 0
        with self._lock:
            for package in packages:
                work_id = str(package.get("id") or "")
                if not work_id:
                    skipped += 1
                    continue
                existing = self._execute("SELECT status FROM remote_work_packages WHERE id = ?", (work_id,)).fetchone()
                if existing and existing["status"] in {"claimed", "submitted"} and not replace:
                    skipped += 1
                    continue
                self._execute(
                    """
                    INSERT INTO remote_work_packages
                      (id, title, privacy_tier, status, worker_id, context_json, output_schema_json,
                       output_json, validation_json, created_at, claimed_at, leased_until, submitted_at, released_at)
                    VALUES (?, ?, ?, 'pending', NULL, ?, ?, NULL, '{}', ?, NULL, NULL, NULL, NULL)
                    ON CONFLICT(id) DO UPDATE SET
                      title = excluded.title,
                      privacy_tier = excluded.privacy_tier,
                      status = CASE WHEN remote_work_packages.status = 'submitted' AND ? = 0 THEN remote_work_packages.status ELSE 'pending' END,
                      worker_id = CASE WHEN remote_work_packages.status = 'submitted' AND ? = 0 THEN remote_work_packages.worker_id ELSE NULL END,
                      context_json = excluded.context_json,
                      output_schema_json = excluded.output_schema_json,
                      output_json = CASE WHEN ? = 1 THEN NULL ELSE remote_work_packages.output_json END,
                      validation_json = CASE WHEN ? = 1 THEN '{}' ELSE remote_work_packages.validation_json END,
                      claimed_at = CASE WHEN ? = 1 THEN NULL ELSE remote_work_packages.claimed_at END,
                      leased_until = CASE WHEN ? = 1 THEN NULL ELSE remote_work_packages.leased_until END,
                      submitted_at = CASE WHEN ? = 1 THEN NULL ELSE remote_work_packages.submitted_at END,
                      released_at = NULL
                    """,
                    (
                        work_id,
                        str(package.get("title") or work_id)[:300],
                        str(package.get("privacy_tier") or "local_only"),
                        dumps_json(build_chunked_context(package.get("context") or {})),
                        dumps_json(package.get("output_schema") or {}),
                        now_iso(),
                        1 if replace else 0,
                        1 if replace else 0,
                        1 if replace else 0,
                        1 if replace else 0,
                        1 if replace else 0,
                        1 if replace else 0,
                        1 if replace else 0,
                    ),
                )
                accepted += 1
                self.record_event("remote_work_package_upserted", {"work_id": work_id, "privacy_tier": package.get("privacy_tier")})
            self.conn.commit()
        if len(packages) == 1 and accepted == 1:
            return {"ok": True, "work_id": packages[0]["id"], "privacy_tier": packages[0].get("privacy_tier"), "status": "pending"}
        return {"ok": True, "accepted": accepted, "skipped": skipped}

    def remote_work_summary(self) -> dict[str, Any]:
        with self._lock:
            counts = {
                row["status"]: int(row["count"])
                for row in self._execute(
                    "SELECT status, COUNT(*) AS count FROM remote_work_packages GROUP BY status"
                ).fetchall()
            }
            active_leases = self._execute(
                "SELECT COUNT(*) AS count FROM remote_work_packages WHERE status = 'claimed'"
            ).fetchone()["count"]
            submissions = [
                {"work_id": row["id"], "status": row["status"], "submitted_at": row["submitted_at"]}
                for row in self._execute(
                    "SELECT id, status, submitted_at FROM remote_work_packages WHERE status = 'submitted' ORDER BY submitted_at DESC LIMIT 10"
                ).fetchall()
            ]
            validation_rows = self._execute(
                "SELECT validation_json FROM remote_work_packages WHERE status IN ('submitted', 'imported', 'import_failed', 'rejected')"
            ).fetchall()
            claimed_rows = self._execute(
                "SELECT claimed_at FROM remote_work_packages WHERE status = 'claimed' AND claimed_at IS NOT NULL"
            ).fetchall()
            skipped_rows = self._execute(
                "SELECT validation_json FROM remote_work_packages WHERE status = 'skipped'"
            ).fetchall()
        validation_ok = 0
        validation_failed = 0
        for row in validation_rows:
            validation = loads_json(row["validation_json"], {})
            if validation.get("ok") is True:
                validation_ok += 1
            elif validation:
                validation_failed += 1
        skip_reasons: dict[str, int] = {}
        for row in skipped_rows:
            reason = str(loads_json(row["validation_json"], {}).get("reason_code") or "other")
            skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
        oldest_claim_age_seconds = None
        now_seconds = time.time()
        for row in claimed_rows:
            try:
                claimed_seconds = _parse_iso_seconds(str(row["claimed_at"]))
            except ValueError:
                continue
            age = max(0, int(now_seconds - claimed_seconds))
            oldest_claim_age_seconds = age if oldest_claim_age_seconds is None else max(oldest_claim_age_seconds, age)
        return {
            "counts": counts,
            "active_remote_leases": int(active_leases),
            "remote_output_submissions": submissions,
            "source_card_metrics": {
                "pending": counts.get("pending", 0),
                "claimed": counts.get("claimed", 0),
                "submitted": counts.get("submitted", 0),
                "imported": counts.get("imported", 0),
                "skipped": counts.get("skipped", 0),
                "validation_ok": validation_ok,
                "validation_failed": validation_failed,
                "skip_reasons": skip_reasons,
                "oldest_active_lease_age_seconds": oldest_claim_age_seconds,
                "turn_success_target": MAX_CHATGPT_TURN_SUCCESS,
                "reserve_attempt_cap": MAX_SOURCE_CARD_RESERVE_ATTEMPTS,
            },
        }

    def claim_work(self, *, worker_id: str) -> dict[str, Any]:
        ts = now_iso()
        leased_until = time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime(time.time() + 45 * 60))
        with self._lock:
            self._expire_stale_leases(ts)
            row = self._execute(
                """
                UPDATE remote_work_packages
                SET status = 'claimed',
                    worker_id = ?,
                    claimed_at = ?,
                    leased_until = ?
                WHERE id = (
                  SELECT id FROM remote_work_packages
                  WHERE status = 'pending'
                  ORDER BY created_at ASC
                  LIMIT 1
                )
                RETURNING id, title, privacy_tier, status, leased_until
                """,
                (worker_id, ts, leased_until),
            ).fetchone()
            if not row:
                self.conn.commit()
                return {"ok": True, "claimed": []}
            self.record_event("remote_work_claimed", {"work_id": row["id"], "worker_id": worker_id})
            self.conn.commit()
        return {
            "ok": True,
            "claimed": [
                {
                    "work_id": row["id"],
                    "title": row["title"],
                    "privacy_tier": row["privacy_tier"],
                    "status": row["status"],
                    "leased_until": row["leased_until"],
                }
            ],
        }

    def _expire_stale_leases(self, ts: str) -> None:
        expired = self._execute(
            """
            UPDATE remote_work_packages
            SET status = 'pending',
                worker_id = NULL,
                claimed_at = NULL,
                leased_until = NULL
            WHERE status = 'claimed'
              AND leased_until IS NOT NULL
              AND leased_until < ?
            RETURNING id
            """,
            (ts,),
        ).fetchall()
        for expired_row in expired:
            self.record_event("remote_work_lease_expired", {"work_id": expired_row["id"]})

    def _claim_source_card_work(self, *, worker_id: str) -> dict[str, Any] | None:
        ts = now_iso()
        leased_until = time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime(time.time() + 45 * 60))
        with self._lock:
            self._expire_stale_leases(ts)
            rows = self._execute(
                """
                SELECT id, title, privacy_tier, context_json
                FROM remote_work_packages
                WHERE status = 'pending'
                ORDER BY created_at ASC
                LIMIT 50
                """
            ).fetchall()
            for candidate in rows:
                context = loads_json(candidate["context_json"], {})
                if not source_card_context_ready(context):
                    continue
                row = self._execute(
                    """
                    UPDATE remote_work_packages
                    SET status = 'claimed',
                        worker_id = ?,
                        claimed_at = ?,
                        leased_until = ?
                    WHERE id = ? AND status = 'pending'
                    RETURNING id, title, privacy_tier, status, leased_until, context_json
                    """,
                    (worker_id, ts, leased_until, candidate["id"]),
                ).fetchone()
                if not row:
                    continue
                self.record_event("remote_work_claimed", {"work_id": row["id"], "worker_id": worker_id, "delivery_mode": "bounded_source_card"})
                self.conn.commit()
                claimed = {
                    "work_id": row["id"],
                    "title": row["title"],
                    "privacy_tier": row["privacy_tier"],
                    "status": row["status"],
                    "leased_until": row["leased_until"],
                }
                claimed.update(source_card_task_brief(loads_json(row["context_json"], {})))
                return claimed
            self.conn.commit()
        return None

    def claim_work_batch(self, *, worker_id: str, max_tasks: int = MAX_SOURCE_CARD_BATCH_TASKS) -> dict[str, Any]:
        max_tasks = int(max_tasks or 1)
        if max_tasks < 1 or max_tasks > MAX_SOURCE_CARD_BATCH_TASKS:
            raise ValueError(f"max_tasks must be between 1 and {MAX_SOURCE_CARD_BATCH_TASKS}")
        claimed: list[dict[str, Any]] = []
        for _ in range(max_tasks):
            result = self.claim_work(worker_id=worker_id)
            items = result.get("claimed") if isinstance(result.get("claimed"), list) else []
            if not items:
                break
            claimed.extend(items)
        return {"ok": True, "claimed": claimed, "max_tasks": max_tasks}

    def reserve_evidence_task(self, *, worker_id: str) -> dict[str, Any]:
        result = self.claim_work(worker_id=worker_id)
        claimed = result.get("claimed") if isinstance(result.get("claimed"), list) else []
        if not claimed:
            return result
        work_id = str(claimed[0].get("work_id") or "")
        with self._lock:
            row = self._execute("SELECT context_json FROM remote_work_packages WHERE id = ?", (work_id,)).fetchone()
        context = loads_json(row["context_json"], {}) if row else {}
        claimed[0].update(evidence_task_brief(context))
        result["claimed"] = claimed
        return result

    def reserve_source_card_task(self, *, worker_id: str) -> dict[str, Any]:
        claimed = self._claim_source_card_work(worker_id=worker_id)
        return {"ok": True, "claimed": [claimed] if claimed else []}

    def reserve_source_card_batch(self, *, worker_id: str, max_tasks: int = MAX_SOURCE_CARD_BATCH_TASKS) -> dict[str, Any]:
        max_tasks = int(max_tasks or 1)
        if max_tasks < 1 or max_tasks > MAX_SOURCE_CARD_BATCH_TASKS:
            raise ValueError(f"max_tasks must be between 1 and {MAX_SOURCE_CARD_BATCH_TASKS}")
        claimed: list[dict[str, Any]] = []
        for _ in range(max_tasks):
            item = self._claim_source_card_work(worker_id=worker_id)
            if not item:
                break
            claimed.append(item)
        return {
            "ok": True,
            "claimed": claimed,
            "max_tasks": max_tasks,
            "batch_policy": {
                "max_tasks": MAX_SOURCE_CARD_BATCH_TASKS,
                "turn_success_target": MAX_CHATGPT_TURN_SUCCESS,
                "reserve_attempt_cap": MAX_SOURCE_CARD_RESERVE_ATTEMPTS,
                "model_visible_delivery": "features_only_no_continuous_transcript",
                "final_tool": "submit_work_outputs",
                "skip_tool": "skip_source_card_task",
            },
        }

    def get_work_context(self, *, work_id: str, worker_id: str) -> dict[str, Any]:
        with self._lock:
            row = self._execute("SELECT * FROM remote_work_packages WHERE id = ?", (work_id,)).fetchone()
        if not row:
            raise ValueError(f"Work package not found: {work_id}")
        if row["status"] != "claimed" or row["worker_id"] != worker_id:
            raise ValueError("Work package is not leased to this worker")
        context = loads_json(row["context_json"], {})
        schema = loads_json(row["output_schema_json"], {})
        return {
            "ok": True,
            "work_id": row["id"],
            "title": row["title"],
            "privacy_tier": row["privacy_tier"],
            "context": work_context_manifest(context),
            "output_schema": schema,
        }

    def get_evidence_manifest(self, *, work_id: str, worker_id: str) -> dict[str, Any]:
        with self._lock:
            row = self._execute("SELECT * FROM remote_work_packages WHERE id = ?", (work_id,)).fetchone()
        if not row:
            raise ValueError(f"Work package not found: {work_id}")
        if row["status"] != "claimed" or row["worker_id"] != worker_id:
            raise ValueError("Work package is not leased to this worker")
        context = loads_json(row["context_json"], {})
        return {
            "ok": True,
            "work_id": row["id"],
            "task_kind": "safe_evidence_analysis",
            **evidence_task_brief(context),
        }

    def get_source_card(self, *, work_id: str, worker_id: str, card_id: str) -> dict[str, Any]:
        with self._lock:
            row = self._execute("SELECT * FROM remote_work_packages WHERE id = ?", (work_id,)).fetchone()
        if not row:
            raise ValueError(f"Work package not found: {work_id}")
        if row["status"] != "claimed" or row["worker_id"] != worker_id:
            raise ValueError("Work package is not leased to this worker")
        context = loads_json(row["context_json"], {})
        cards = context.get("source_cards") if isinstance(context.get("source_cards"), list) else []
        for index, card in enumerate(cards):
            if card.get("card_id") == card_id:
                validate_source_card(card)
                next_card_id = cards[index + 1].get("card_id") if index + 1 < len(cards) else None
                visible_card = model_visible_source_card(card)
                return {
                    "ok": True,
                    "work_id": row["id"],
                    "card_id": card_id,
                    "card_index": int(card.get("index") or index),
                    "card_count": len(cards),
                    "delivery_mode": "bounded_source_card",
                    "raw_transcript_returned": False,
                    "source_text_returned": "features_only_no_continuous_transcript",
                    "card": visible_card,
                    "next_card_id": next_card_id,
                }
        raise ValueError(f"Source card not found: {card_id}")

    def get_source_card_batch(self, *, worker_id: str, items: list[dict[str, Any]]) -> dict[str, Any]:
        if not isinstance(items, list) or not items:
            raise ValueError("items must be a non-empty array")
        if len(items) > MAX_SOURCE_CARD_BATCH_TASKS:
            raise ValueError(f"items must contain at most {MAX_SOURCE_CARD_BATCH_TASKS} source-card requests")
        cards: list[dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict):
                raise ValueError("each item must be an object")
            work_id = str(item.get("work_id") or "")
            card_id = str(item.get("card_id") or item.get("first_card_id") or "")
            if not work_id or not card_id:
                raise ValueError("each item must include work_id and card_id")
            cards.append(self.get_source_card(work_id=work_id, worker_id=worker_id, card_id=card_id))
        serialized = json.dumps(cards, ensure_ascii=True, sort_keys=True)
        for marker in ("WEBVTT", "===== SEGMENT", "Full Prepared Episode Transcript", "BEGIN RAW TRANSCRIPT"):
            if marker in serialized:
                raise ValueError(f"source-card batch contains forbidden marker: {marker}")
        return {
            "ok": True,
            "card_count": len(cards),
            "max_tasks": MAX_SOURCE_CARD_BATCH_TASKS,
            "raw_transcript_returned": False,
            "source_text_returned": "features_only_no_continuous_transcript",
            "cards": cards,
        }

    def get_work_chunk(self, *, work_id: str, worker_id: str, chunk_id: str) -> dict[str, Any]:
        with self._lock:
            row = self._execute("SELECT * FROM remote_work_packages WHERE id = ?", (work_id,)).fetchone()
        if not row:
            raise ValueError(f"Work package not found: {work_id}")
        if row["status"] != "claimed" or row["worker_id"] != worker_id:
            raise ValueError("Work package is not leased to this worker")
        context = loads_json(row["context_json"], {})
        chunks = context.get("chunks") if isinstance(context.get("chunks"), list) else []
        for index, chunk in enumerate(chunks):
            if chunk.get("chunk_id") == chunk_id:
                next_chunk_id = chunks[index + 1].get("chunk_id") if index + 1 < len(chunks) else None
                return {
                    "ok": True,
                    "work_id": row["id"],
                    "chunk_id": chunk_id,
                    "chunk_index": int(chunk.get("index") or index),
                    "chunk_count": len(chunks),
                    "char_count": int(chunk.get("char_count") or len(chunk.get("text") or "")),
                    "sha256": chunk.get("sha256"),
                    "segment_start": chunk.get("segment_start"),
                    "segment_end": chunk.get("segment_end"),
                    **chunk_delivery_payload(chunk.get("text") or ""),
                    "next_chunk_id": next_chunk_id,
                }
        raise ValueError(f"Chunk not found: {chunk_id}")

    def submit_work_notes(self, *, work_id: str, worker_id: str, chunk_id: str, notes: dict[str, Any]) -> dict[str, Any]:
        serialized_notes = json.dumps(notes, ensure_ascii=True, sort_keys=True)
        if len(serialized_notes) > MAX_WORK_NOTE_CHARS:
            raise ValueError(f"notes must be compact and under {MAX_WORK_NOTE_CHARS} characters")
        if "===== SEGMENT" in serialized_notes or "# Full Prepared Episode Transcript" in serialized_notes:
            raise ValueError("notes appear to contain raw transcript text")
        ts = now_iso()
        with self._lock:
            row = self._execute("SELECT * FROM remote_work_packages WHERE id = ?", (work_id,)).fetchone()
            if not row:
                raise ValueError(f"Work package not found: {work_id}")
            if row["status"] != "claimed" or row["worker_id"] != worker_id:
                raise ValueError("Work package is not leased to this worker")
            context = loads_json(row["context_json"], {})
            chunk_ids = {chunk.get("chunk_id") for chunk in context.get("chunks") or []}
            chunk_ids.update(card.get("card_id") for card in context.get("source_cards") or [])
            if chunk_id not in chunk_ids:
                raise ValueError(f"Chunk not found: {chunk_id}")
            existing_notes = context.get("notes") if isinstance(context.get("notes"), list) else []
            updated_notes = [
                note
                for note in existing_notes
                if not (note.get("worker_id") == worker_id and note.get("chunk_id") == chunk_id)
            ]
            updated_notes.append(
                {
                    "worker_id": worker_id,
                    "chunk_id": chunk_id,
                    "notes": notes,
                    "updated_at": ts,
                }
            )
            context["notes"] = sorted(updated_notes, key=lambda item: (str(item.get("chunk_id")), str(item.get("worker_id"))))
            self._execute("UPDATE remote_work_packages SET context_json = ? WHERE id = ?", (dumps_json(context), work_id))
            self.record_event("remote_work_notes_submitted", {"work_id": work_id, "worker_id": worker_id, "chunk_id": chunk_id})
            self.conn.commit()
        return {"ok": True, "work_id": work_id, "chunk_id": chunk_id, "status": "noted", "idempotent_key": f"{worker_id}:{chunk_id}"}

    def get_work_notes(self, *, work_id: str, worker_id: str) -> dict[str, Any]:
        with self._lock:
            row = self._execute("SELECT * FROM remote_work_packages WHERE id = ?", (work_id,)).fetchone()
        if not row:
            raise ValueError(f"Work package not found: {work_id}")
        if row["status"] != "claimed" or row["worker_id"] != worker_id:
            raise ValueError("Work package is not leased to this worker")
        context = loads_json(row["context_json"], {})
        notes = [
            {
                "chunk_id": note.get("chunk_id"),
                "notes": note.get("notes") or {},
                "updated_at": note.get("updated_at"),
            }
            for note in (context.get("notes") or [])
            if note.get("worker_id") == worker_id
        ]
        chunk_ids = [chunk.get("chunk_id") for chunk in (context.get("chunks") or [])]
        source_card_ids = [card.get("card_id") for card in (context.get("source_cards") or [])]
        covered = {note.get("chunk_id") for note in notes}
        return {
            "ok": True,
            "work_id": work_id,
            "chunk_count": len(chunk_ids),
            "covered_chunk_count": len({chunk_id for chunk_id in chunk_ids if chunk_id in covered}),
            "source_card_count": len(source_card_ids),
            "covered_source_card_count": len({card_id for card_id in source_card_ids if card_id in covered}),
            "missing_chunk_ids": [chunk_id for chunk_id in chunk_ids if chunk_id not in covered],
            "missing_source_card_ids": [card_id for card_id in source_card_ids if card_id not in covered],
            "notes": notes,
        }

    def submit_work_output(self, *, work_id: str, worker_id: str, output: dict[str, Any]) -> dict[str, Any]:
        ts = now_iso()
        with self._lock:
            row = self._execute("SELECT * FROM remote_work_packages WHERE id = ?", (work_id,)).fetchone()
            if not row:
                raise ValueError(f"Work package not found: {work_id}")
            if row["status"] not in {"claimed", "submitted"} or row["worker_id"] != worker_id:
                raise ValueError("Work package is not leased to this worker")
            context = loads_json(row["context_json"], {})
            validation = validate_remote_output(output, context=context)
            existing = loads_json(row["output_json"], None) if row["output_json"] else None
            if existing == output and row["status"] == "submitted":
                return {"ok": True, "work_id": work_id, "status": "submitted", "idempotent": True, "validation": validation}
            self._execute(
                """
                UPDATE remote_work_packages
                SET status = 'submitted',
                    output_json = ?,
                    validation_json = ?,
                    submitted_at = ?
                WHERE id = ?
                """,
                (dumps_json(output), dumps_json(validation), ts, work_id),
            )
            self.record_event("remote_work_submitted", {"work_id": work_id, "worker_id": worker_id, "validation_ok": validation["ok"]})
            self.conn.commit()
        return {"ok": True, "work_id": work_id, "status": "submitted", "validation": validation}

    def submit_work_outputs(self, *, worker_id: str, outputs: list[dict[str, Any]]) -> dict[str, Any]:
        if not isinstance(outputs, list) or not outputs:
            raise ValueError("outputs must be a non-empty array")
        if len(outputs) > MAX_SOURCE_CARD_BATCH_TASKS:
            raise ValueError(f"outputs must contain at most {MAX_SOURCE_CARD_BATCH_TASKS} submissions")
        submitted: list[dict[str, Any]] = []
        validation_ok = 0
        validation_failed = 0
        for item in outputs:
            if not isinstance(item, dict):
                raise ValueError("each output item must be an object")
            work_id = str(item.get("work_id") or "")
            output = item.get("output")
            if not work_id or not isinstance(output, dict):
                raise ValueError("each output item must include work_id and object output")
            result = self.submit_work_output(work_id=work_id, worker_id=worker_id, output=output)
            submitted.append(result)
            if (result.get("validation") or {}).get("ok"):
                validation_ok += 1
            else:
                validation_failed += 1
        return {
            "ok": validation_failed == 0,
            "submitted_count": len(submitted),
            "validation_ok_count": validation_ok,
            "validation_failed_count": validation_failed,
            "submitted": submitted,
        }

    def skip_source_card_task(self, *, work_id: str, worker_id: str, reason_code: str, stage: str, card_id: str | None = None) -> dict[str, Any]:
        if reason_code not in SOURCE_CARD_SKIP_REASON_CODES:
            raise ValueError(f"reason_code must be one of {sorted(SOURCE_CARD_SKIP_REASON_CODES)}")
        validation = {
            "ok": False,
            "status": "skipped",
            "reason_code": reason_code,
            "stage": str(stage or "unknown")[:80],
            "card_id": str(card_id or "")[:120] or None,
        }
        with self._lock:
            row = self._execute(
                """
                UPDATE remote_work_packages
                SET status = 'skipped',
                    worker_id = NULL,
                    claimed_at = NULL,
                    leased_until = NULL,
                    released_at = ?,
                    validation_json = ?
                WHERE id = ? AND worker_id = ? AND status = 'claimed'
                RETURNING id
                """,
                (now_iso(), dumps_json(validation), work_id, worker_id),
            ).fetchone()
            if row:
                self.record_event(
                    "remote_work_skipped",
                    {"work_id": work_id, "worker_id": worker_id, "reason_code": reason_code, "stage": validation["stage"]},
                )
            self.conn.commit()
        if not row:
            raise ValueError("Work package is not actively leased to this worker")
        return {"ok": True, "work_id": work_id, "status": "skipped", "reason_code": reason_code}

    def pending_submissions(self, *, limit: int = 20) -> dict[str, Any]:
        with self._lock:
            rows = self._execute(
                """
                SELECT id, title, privacy_tier, worker_id, output_json, validation_json, submitted_at
                FROM remote_work_packages
                WHERE status = 'submitted'
                  AND submitted_at IS NOT NULL
                ORDER BY submitted_at ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        submissions = []
        for row in rows:
            submissions.append(
                {
                    "work_id": row["id"],
                    "title": row["title"],
                    "privacy_tier": row["privacy_tier"],
                    "worker_id": row["worker_id"],
                    "output": loads_json(row["output_json"], {}),
                    "validation": loads_json(row["validation_json"], {}),
                    "submitted_at": row["submitted_at"],
                }
            )
        return {"ok": True, "submissions": submissions}

    def pending_skips(self, *, limit: int = 20) -> dict[str, Any]:
        with self._lock:
            rows = self._execute(
                """
                SELECT id, title, privacy_tier, validation_json, released_at
                FROM remote_work_packages
                WHERE status = 'skipped'
                ORDER BY released_at ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return {
            "ok": True,
            "skips": [
                {
                    "work_id": row["id"],
                    "title": row["title"],
                    "privacy_tier": row["privacy_tier"],
                    "validation": loads_json(row["validation_json"], {}),
                    "skipped_at": row["released_at"],
                }
                for row in rows
            ],
        }

    def mark_submission_imported(self, *, work_id: str, status: str, validation: dict[str, Any] | None = None) -> dict[str, Any]:
        if status not in {"imported", "import_failed", "rejected", "skip_imported"}:
            raise ValueError("status must be imported, import_failed, rejected, or skip_imported")
        with self._lock:
            row = self._execute("SELECT id FROM remote_work_packages WHERE id = ?", (work_id,)).fetchone()
            if not row:
                raise ValueError(f"Work package not found: {work_id}")
            self._execute(
                "UPDATE remote_work_packages SET status = ?, validation_json = ? WHERE id = ?",
                (status, dumps_json(validation or {}), work_id),
            )
            self.record_event("remote_submission_import_marked", {"work_id": work_id, "status": status})
            self.conn.commit()
        return {"ok": True, "work_id": work_id, "status": status}

    def audit_events(self, *, limit: int = 100) -> dict[str, Any]:
        with self._lock:
            rows = self._execute(
                """
                SELECT id, event_type, event_json, created_at
                FROM broker_events
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        events = []
        for row in rows:
            event = loads_json(row["event_json"], {})
            events.append(
                {
                    "id": row["id"],
                    "event_type": row["event_type"],
                    "event": redact_audit_event(event),
                    "created_at": row["created_at"],
                }
            )
        return {"ok": True, "events": events}

    def heartbeat_work(self, *, work_id: str, worker_id: str) -> dict[str, Any]:
        leased_until = time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime(time.time() + 45 * 60))
        with self._lock:
            row = self._execute(
                "UPDATE remote_work_packages SET leased_until = ? WHERE id = ? AND worker_id = ? AND status = 'claimed' RETURNING id",
                (leased_until, work_id, worker_id),
            ).fetchone()
            self.conn.commit()
        if not row:
            raise ValueError("Work package is not actively leased to this worker")
        return {"ok": True, "work_id": work_id, "leased_until": leased_until}

    def release_work(self, *, work_id: str, worker_id: str, reason: str) -> dict[str, Any]:
        with self._lock:
            row = self._execute(
                """
                UPDATE remote_work_packages
                SET status = 'released',
                    worker_id = NULL,
                    claimed_at = NULL,
                    leased_until = NULL,
                    released_at = ?
                WHERE id = ? AND worker_id = ? AND status = 'claimed'
                RETURNING id
                """,
                (now_iso(), work_id, worker_id),
            ).fetchone()
            if row:
                self.record_event("remote_work_released", {"work_id": work_id, "worker_id": worker_id, "reason": reason[:500]})
            self.conn.commit()
        if not row:
            raise ValueError("Work package is not actively leased to this worker")
        return {"ok": True, "work_id": work_id, "status": "released"}


def default_snapshot() -> dict[str, Any]:
    return {
        "contract_version": "railway-operational-v2",
        "generated_at": None,
        "privacy": "sanitized_broker_state_no_raw_transcripts",
        "counts": {},
        "queues": {
            "by_status": {},
            "by_lane_type_status": [],
            "by_role_content_status": [],
            "privacy_tiers": {},
            "remote_claimable_by_privacy_tier": {},
        },
        "runs": {"label_runs": [], "worker_runs": [], "worker_status": {}, "service_status": {}},
        "failures": {
            "jobs": [],
            "expired_claims": [],
            "label_runs": [],
            "output_submissions": [],
            "remote_import_failures": 0,
            "missing_claimed_run_outputs": 0,
            "queue_sync": "unknown",
        },
        "artifacts": [],
        "intervention_flags": [],
    }


def sanitize_broker_snapshot(payload: dict[str, Any]) -> dict[str, Any]:
    from .ui_server import _sanitize_snapshot, _validate_snapshot_contract

    candidate = dict(payload)
    if candidate.get("contract_version") != "railway-operational-v2":
        candidate = _legacy_snapshot_to_v2(candidate)
    elif candidate.get("privacy") == "sanitized_broker_state_no_raw_transcripts":
        candidate["privacy"] = "sanitized_operational_snapshot_no_raw_transcripts"
    _validate_snapshot_contract(candidate, reject_unknown=True)
    snapshot = _sanitize_snapshot(candidate)
    snapshot["privacy"] = "sanitized_broker_state_no_raw_transcripts"
    return snapshot


def _legacy_snapshot_to_v2(payload: dict[str, Any]) -> dict[str, Any]:
    queue = payload.get("research_queue_metrics") if isinstance(payload.get("research_queue_metrics"), dict) else {}
    depth_rows: list[dict[str, Any]] = []
    for row in queue.get("depth_by_role") or []:
        if not isinstance(row, dict):
            continue
        depth_rows.append(
            {
                "worker_role": row.get("worker_role") or row.get("role") or "unknown",
                "content_type": row.get("content_type") or "unknown",
                "status": row.get("status") or "unknown",
                "count": row.get("count") or 0,
            }
        )
    job_counts = payload.get("job_counts") if isinstance(payload.get("job_counts"), dict) else {}
    remote_submissions = queue.get("remote_output_submissions") or []
    return {
        "contract_version": "railway-operational-v2",
        "generated_at": payload.get("generated_at"),
        "privacy": "sanitized_operational_snapshot_no_raw_transcripts",
        "counts": payload.get("counts") if isinstance(payload.get("counts"), dict) else {},
        "queues": {
            "by_status": job_counts,
            "by_lane_type_status": [],
            "by_role_content_status": depth_rows,
            "privacy_tiers": {},
            "remote_claimable_by_privacy_tier": queue.get("remote_claimable_by_privacy_tier") or {},
        },
        "runs": {
            "label_runs": [],
            "worker_runs": queue.get("worker_runs") or [],
            "worker_status": {
                "remote": {
                    "state": "active" if queue.get("active_remote_leases") else "disabled",
                    "active_claims": queue.get("active_remote_leases") or 0,
                    "evidence": "legacy_operational_snapshot",
                }
            },
            "service_status": {},
        },
        "failures": {
            "jobs": [],
            "expired_claims": [],
            "label_runs": [],
            "output_submissions": remote_submissions,
            "remote_import_failures": queue.get("remote_import_failures") or 0,
            "missing_claimed_run_outputs": 0,
            "queue_sync": "unknown",
        },
        "artifacts": [],
        "intervention_flags": payload.get("intervention_flags") or [],
        "snapshot_health": payload.get("snapshot_health") or {},
    }


def redact_audit_event(event: dict[str, Any]) -> dict[str, Any]:
    blocked = {"prompt_text", "transcript_text", "output", "context", "secret", "token"}
    redacted: dict[str, Any] = {}
    for key, value in event.items():
        if key in blocked or any(marker in key.lower() for marker in ("token", "secret", "prompt", "transcript")):
            redacted[key] = "[redacted]"
        else:
            redacted[key] = value
    return redacted


def _refresh_token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def status_from_snapshot(snapshot: dict[str, Any], control_counts: dict[str, int] | None = None) -> dict[str, Any]:
    queue = snapshot.get("queues") or {}
    runs = snapshot.get("runs") or {}
    failures = snapshot.get("failures") or {}
    remote_status = (runs.get("worker_status") or {}).get("remote") or {}
    return {
        "ok": True,
        "mode": "podcast_intelligence_factory_mcp_broker",
        "local_source_of_truth": True,
        "railway_compute_enabled": False,
        "mcp_phase": "control_status_only",
        "observer_url": DEFAULT_OBSERVER_URL,
        "generated_at": snapshot.get("generated_at"),
        "remote_claimable_by_privacy_tier": queue.get("remote_claimable_by_privacy_tier") or {},
        "active_remote_leases": int(remote_status.get("active_claims") or 0),
        "remote_output_submissions": failures.get("output_submissions") or [],
        "remote_import_failures": int(failures.get("remote_import_failures") or 0),
        "mcp_remote_worker_enabled": False,
        "control_requests": control_counts or {},
        "phase2_tools_enabled": False,
    }


def queue_summary_from_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    queue = snapshot.get("queues") or {}
    runs = snapshot.get("runs") or {}
    failures = snapshot.get("failures") or {}
    remote_status = (runs.get("worker_status") or {}).get("remote") or {}
    return {
        "ok": True,
        "source_of_truth": "local_sqlite",
        "remote_contract": "control_status_only",
        "depth_by_role": queue.get("by_role_content_status") or [],
        "worker_runs": runs.get("worker_runs") or [],
        "remote_claimable_by_privacy_tier": queue.get("remote_claimable_by_privacy_tier") or {},
        "active_remote_leases": int(remote_status.get("active_claims") or 0),
        "remote_output_submissions": failures.get("output_submissions") or [],
        "remote_import_failures": int(failures.get("remote_import_failures") or 0),
    }


def smoke_work_package() -> dict[str, Any]:
    return {
        "id": "smoke_pif_remote_extraction_v1",
        "title": "Synthetic Phase 2 extraction smoke",
        "privacy_tier": "full_text_allowed",
        "context": {
            "task": "Extract structured discourse signals from this synthetic transcript excerpt.",
            "content_type": "synthetic_transcript",
            "source_policy": "Synthetic fixture only; not a real podcast transcript.",
            "transcript_text": (
                "HOST: Today we discuss Acme AI launching its Studio model for finance teams. "
                "GUEST: The launch matters because procurement leaders are shifting budgets from pilots to production. "
                "HOST: What evidence would convince you? "
                "GUEST: Paid customer adoption, lower inference costs, and integrations with existing approval workflows."
            ),
            "instructions": [
                "Return concise structured JSON only.",
                "Identify organizations, products, market or release signals, and one uncertainty.",
                "Do not include long transcript excerpts.",
            ],
        },
        "output_schema": {
            "type": "object",
            "required": ["schema_version", "organizations", "products", "signals", "uncertainties"],
            "properties": {
                "schema_version": {"const": "pif_remote_smoke_v1"},
                "organizations": {"type": "array", "items": {"type": "string"}},
                "products": {"type": "array", "items": {"type": "string"}},
                "signals": {"type": "array", "items": {"type": "object"}},
                "uncertainties": {"type": "array", "items": {"type": "string"}},
            },
        },
    }


def build_chunked_context(context: dict[str, Any]) -> dict[str, Any]:
    package = copy.deepcopy(context)
    if package.get("context_protocol") == "chunked_v1":
        return package
    prompt_text = package.pop("prompt_text", None)
    transcript_text = package.pop("transcript_text", None)
    output_contract = str(package.get("output_contract") or "Return only the JSON object requested by the prompt. Do not include Markdown fences.")
    if isinstance(prompt_text, str) and prompt_text:
        preamble, transcript, parsed_contract = split_episode_context_prompt(prompt_text)
        if parsed_contract:
            output_contract = parsed_contract
        package["instructions_text"] = preamble
        package["source_text_kind"] = "full_prepared_episode_transcript"
    elif isinstance(transcript_text, str) and transcript_text:
        transcript = transcript_text
        package["instructions_text"] = "\n".join(str(item) for item in package.get("instructions") or [])
        package["source_text_kind"] = "transcript_excerpt"
    else:
        transcript = ""
        package["instructions_text"] = "\n".join(str(item) for item in package.get("instructions") or [])
        package["source_text_kind"] = "none"
    chunks = chunk_context_text(transcript, chunk_chars=context_chunk_chars())
    package["context_protocol"] = "chunked_v1"
    package["output_contract"] = output_contract
    package["chunks"] = chunks
    package["chunk_manifest"] = chunk_manifest(chunks, transcript)
    package["source_cards"] = source_cards_for_transcript(transcript, episode_id=str(package.get("episode_id") or ""))
    package["source_card_manifest"] = source_card_manifest(package["source_cards"], transcript)
    package["notes"] = []
    return package


def split_episode_context_prompt(prompt_text: str) -> tuple[str, str, str | None]:
    transcript_marker = "# Full Prepared Episode Transcript"
    output_marker = "# Output Contract"
    if transcript_marker not in prompt_text:
        return prompt_text.strip(), "", None
    preamble, remainder = prompt_text.split(transcript_marker, 1)
    output_contract = None
    transcript = remainder
    if output_marker in remainder:
        transcript, contract = remainder.split(output_marker, 1)
        output_contract = f"{output_marker}{contract}".strip()
    return preamble.strip(), transcript.strip(), output_contract


def chunk_context_text(text: str, *, chunk_chars: int) -> list[dict[str, Any]]:
    if not text:
        return []
    segments = split_segment_blocks(text)
    if not segments:
        segments = [{"text": part, "segment_start": None, "segment_end": None} for part in fixed_text_chunks(text, chunk_chars)]
    chunks: list[dict[str, Any]] = []
    pending: list[str] = []
    pending_start: int | None = None
    pending_end: int | None = None

    def flush() -> None:
        nonlocal pending, pending_start, pending_end
        if not pending:
            return
        chunk_text = "\n\n".join(pending).strip()
        chunks.append(make_context_chunk(len(chunks), chunk_text, pending_start, pending_end))
        pending = []
        pending_start = None
        pending_end = None

    for segment in segments:
        block = segment["text"].strip()
        if not block:
            continue
        if len(block) > chunk_chars:
            flush()
            for part in fixed_text_chunks(block, chunk_chars):
                chunks.append(make_context_chunk(len(chunks), part, segment.get("segment_start"), segment.get("segment_end")))
            continue
        candidate = "\n\n".join([*pending, block]).strip()
        if pending and len(candidate) > chunk_chars:
            flush()
        if pending_start is None:
            pending_start = segment.get("segment_start")
        pending_end = segment.get("segment_end")
        pending.append(block)
    flush()
    return chunks


def source_cards_for_transcript(text: str, *, episode_id: str) -> list[dict[str, Any]]:
    if not text:
        return []
    transcript_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
    blocks = transcript_blocks_with_offsets(text)
    scored: list[tuple[int, int, dict[str, Any]]] = []
    for index, block in enumerate(blocks):
        excerpt = bounded_excerpt(block["text"], MAX_SOURCE_CARD_EXCERPT_CHARS)
        if not excerpt:
            continue
        lower = excerpt.lower()
        entities = entity_candidates(excerpt)[:12]
        concepts = concept_candidates(lower)[:12]
        numbers = numeric_cues(excerpt)[:10]
        topic_tags = controlled_concept_tags([str(value).lower() for value in concepts], entities)
        score = len(entities) * 2 + len(concepts) * 3 + len(numbers)
        if any(term in lower for term in ("launch", "release", "pricing", "customer", "revenue", "adoption", "workflow")):
            score += 4
        if "?" in excerpt:
            score += 1
        card_hash = hashlib.sha256(f"{transcript_hash}:{block['offset_start']}:{block['offset_end']}:{excerpt}".encode("utf-8")).hexdigest()
        card = {
            "card_id": f"card_{index:04d}_{card_hash[:12]}",
            "index": index,
            "delivery_mode": "bounded_source_card",
            "episode_id": episode_id,
            "segment_start": block.get("segment_start"),
            "segment_end": block.get("segment_end"),
            "offset_start": block["offset_start"],
            "offset_end": min(block["offset_start"] + len(excerpt), block["offset_end"]),
            "source_sha256": transcript_hash,
            "excerpt_sha256": hashlib.sha256(excerpt.encode("utf-8")).hexdigest(),
            "excerpt_text": excerpt,
            "excerpt_char_count": len(excerpt),
            "speaker_surfaces": speaker_surfaces([excerpt])[:8],
            "entity_candidates": entities,
            "concept_candidates": concepts,
            "numeric_cues": numbers,
            "claim_abstract": source_card_claim_abstract(concepts, numbers, lower),
            "topic_tags": topic_tags,
            "candidate_relation_types": candidate_relation_types(lower),
            "discourse_cues": {
                "question_count": excerpt.count("?"),
                "contains_contrast_language": any(term in lower for term in ("but", "however", "although", "instead", "whereas")),
                "contains_uncertainty_language": any(term in lower for term in ("maybe", "might", "could", "uncertain", "unclear", "risk")),
                "contains_launch_or_market_language": any(term in lower for term in ("launch", "release", "customer", "market", "revenue", "adoption", "pricing")),
            },
            "safety": {
                "bounded_excerpt_only": True,
                "max_excerpt_chars": MAX_SOURCE_CARD_EXCERPT_CHARS,
                "not_full_transcript": True,
            },
        }
        scored.append((score, index, enforce_source_card_limits(card)))
    selected = sorted(sorted(scored, key=lambda item: (-item[0], item[1]))[:MAX_SOURCE_CARDS], key=lambda item: item[1])
    return [card for _, _, card in selected]


def transcript_blocks_with_offsets(text: str) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = []
    if "===== SEGMENT" in text:
        import re

        matches = list(re.finditer(r"(?m)^===== SEGMENT\s+(\d+).*?$", text))
        for index, match in enumerate(matches):
            start = match.end()
            end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
            segment_text = text[start:end].strip()
            if not segment_text:
                continue
            segment_id = int(match.group(1))
            blocks.append(
                {
                    "text": segment_text,
                    "offset_start": start + (len(text[start:end]) - len(text[start:end].lstrip())),
                    "offset_end": end,
                    "segment_start": segment_id,
                    "segment_end": segment_id,
                }
            )
    if blocks:
        return blocks
    window = MAX_SOURCE_CARD_EXCERPT_CHARS
    step = MAX_SOURCE_CARD_EXCERPT_CHARS
    return [
        {
            "text": text[index : index + window],
            "offset_start": index,
            "offset_end": min(index + window, len(text)),
            "segment_start": None,
            "segment_end": None,
        }
        for index in range(0, len(text), step)
        if text[index : index + window].strip()
    ]


def bounded_excerpt(text: str, max_chars: int) -> str:
    cleaned = sanitize_source_card_text(text)
    if len(cleaned) <= max_chars:
        return cleaned
    boundary = cleaned.rfind(" ", 0, max_chars)
    if boundary < max_chars // 2:
        boundary = max_chars
    return cleaned[:boundary].strip()


def sanitize_source_card_text(text: str) -> str:
    forbidden = ("WEBVTT", "Full Prepared Episode Transcript", "===== SEGMENT", "BEGIN RAW TRANSCRIPT")
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    cleaned = " ".join(line for line in lines if all(marker not in line for marker in forbidden))
    return " ".join(cleaned.split())


def enforce_source_card_limits(card: dict[str, Any]) -> dict[str, Any]:
    excerpt = str(card.get("excerpt_text") or "")
    if len(excerpt) > MAX_SOURCE_CARD_EXCERPT_CHARS:
        card["excerpt_text"] = bounded_excerpt(excerpt, MAX_SOURCE_CARD_EXCERPT_CHARS)
        card["excerpt_char_count"] = len(card["excerpt_text"])
        card["excerpt_sha256"] = hashlib.sha256(card["excerpt_text"].encode("utf-8")).hexdigest()
    while source_card_text_size(card) > MAX_SOURCE_CARD_TEXT_CHARS and card.get("entity_candidates"):
        card["entity_candidates"] = card["entity_candidates"][:-1]
    while source_card_text_size(card) > MAX_SOURCE_CARD_TEXT_CHARS and card.get("numeric_cues"):
        card["numeric_cues"] = card["numeric_cues"][:-1]
    while source_card_text_size(card) > MAX_SOURCE_CARD_TEXT_CHARS and card.get("concept_candidates"):
        card["concept_candidates"] = card["concept_candidates"][:-1]
    return card


def source_card_text_size(card: dict[str, Any]) -> int:
    values: list[str] = []
    for key in ("excerpt_text", "claim_abstract"):
        if isinstance(card.get(key), str):
            values.append(card[key])
    for key in ("speaker_surfaces", "entity_candidates", "concept_candidates", "numeric_cues", "topic_tags", "candidate_relation_types"):
        values.extend(str(item) for item in card.get(key) or [])
    return sum(len(value) for value in values)


def source_card_manifest(cards: list[dict[str, Any]], full_text: str) -> dict[str, Any]:
    return {
        "delivery_mode": "bounded_source_card",
        "card_count": len(cards),
        "max_cards": MAX_SOURCE_CARDS,
        "max_excerpt_chars": MAX_SOURCE_CARD_EXCERPT_CHARS,
        "max_card_text_chars": MAX_SOURCE_CARD_TEXT_CHARS,
        "source_sha256": hashlib.sha256(full_text.encode("utf-8")).hexdigest() if full_text else None,
        "cards": [
            {
                "card_id": card["card_id"],
                "index": card["index"],
                "excerpt_char_count": card["excerpt_char_count"],
                "segment_start": card.get("segment_start"),
                "segment_end": card.get("segment_end"),
                "offset_start": card.get("offset_start"),
                "offset_end": card.get("offset_end"),
                "excerpt_sha256": card.get("excerpt_sha256"),
            }
            for card in cards
        ],
    }


def model_visible_source_card(card: dict[str, Any]) -> dict[str, Any]:
    visible = {
        key: copy.deepcopy(card.get(key))
        for key in (
            "card_id",
            "index",
            "delivery_mode",
            "episode_id",
            "segment_start",
            "segment_end",
            "offset_start",
            "offset_end",
            "source_sha256",
            "excerpt_sha256",
            "excerpt_char_count",
            "claim_abstract",
            "topic_tags",
            "candidate_relation_types",
            "discourse_cues",
        )
        if key in card
    }
    concepts = [str(value).lower() for value in card.get("concept_candidates") or []]
    entities = [str(value) for value in card.get("entity_candidates") or []]
    numbers = [str(value) for value in card.get("numeric_cues") or []]
    visible["controlled_concept_tags"] = controlled_concept_tags(concepts, entities)
    visible["feature_counts"] = {
        "speaker_surface_count": len(card.get("speaker_surfaces") or []),
        "entity_candidate_count": len(entities),
        "concept_candidate_count": len(concepts),
        "numeric_cue_count": len(numbers),
    }
    visible["numeric_signal_profile"] = numeric_signal_profile(numbers)
    visible["excerpt_text_returned"] = False
    visible["source_text_returned"] = "features_only_no_continuous_transcript"
    visible["evidence_profile"] = {
        "speaker_count": visible["feature_counts"]["speaker_surface_count"],
        "entity_count": visible["feature_counts"]["entity_candidate_count"],
        "concept_count": visible["feature_counts"]["concept_candidate_count"],
        "numeric_cue_count": visible["feature_counts"]["numeric_cue_count"],
    }
    visible["safety"] = {
        "no_continuous_transcript_text": True,
        "no_quote_body": True,
        "traceable_by_hash_and_offsets": True,
    }
    return visible


def source_card_context_ready(context: dict[str, Any]) -> bool:
    manifest = context.get("source_card_manifest") if isinstance(context.get("source_card_manifest"), dict) else {}
    cards = context.get("source_cards") if isinstance(context.get("source_cards"), list) else []
    manifest_cards = manifest.get("cards") if isinstance(manifest.get("cards"), list) else []
    if not cards or not manifest_cards or manifest.get("delivery_mode") != "bounded_source_card":
        return False
    try:
        validate_source_card(cards[0])
    except ValueError:
        return False
    return bool(cards[0].get("card_id") and manifest_cards[0].get("card_id"))


def source_card_claim_abstract(concepts: list[str], numbers: list[str], lower_text: str) -> str:
    tags = controlled_concept_tags([str(value).lower() for value in concepts], [])
    relation_types = candidate_relation_types(lower_text)
    parts: list[str] = []
    if tags:
        parts.append("topic " + ", ".join(tags[:3]))
    if relation_types:
        parts.append("signal " + ", ".join(relation_types[:3]))
    if numbers:
        parts.append("numeric evidence present")
    if not parts:
        parts.append("thin transcript-derived feature signal")
    abstract = "; ".join(parts)
    return abstract[:240]


def candidate_relation_types(lower_text: str) -> list[str]:
    relation_terms = {
        "launch": ("launch", "release", "ship", "announce"),
        "adoption": ("adoption", "customer", "user", "deploy", "workflow"),
        "risk": ("risk", "safety", "uncertain", "security", "vulnerability"),
        "model_capability": ("benchmark", "capability", "inference", "reasoning", "model"),
        "market_signal": ("pricing", "revenue", "market", "startup", "enterprise"),
        "policy": ("policy", "regulation", "legal", "law"),
    }
    relations: list[str] = []
    for relation, terms in relation_terms.items():
        if any(term in lower_text for term in terms):
            relations.append(relation)
    return relations[:6]


def controlled_concept_tags(concepts: list[str], entities: list[str]) -> list[str]:
    joined = " ".join(concepts + [entity.lower() for entity in entities])
    tags: list[str] = []
    tag_terms = {
        "ai": ("ai", "artificial intelligence"),
        "agi": ("agi", "artificial general intelligence"),
        "model": ("model", "gpt", "llm", "sonnet", "grok"),
        "agent": ("agent", "agentic"),
        "enterprise_ai": ("enterprise", "workflow", "deployment", "production"),
        "alignment": ("alignment", "safety", "ethics"),
        "data_analytics": ("analytics", "data"),
        "career_skills": ("career", "reskilling", "talent", "skills"),
        "infrastructure": ("gpu", "kubernetes", "inference", "compute"),
        "cybersecurity": ("cyber", "vulnerability", "security"),
        "law_policy": ("law", "legal", "policy"),
    }
    for tag, terms in tag_terms.items():
        if any(term in joined for term in terms):
            tags.append(tag)
    return tags[:8]


def numeric_signal_profile(numbers: list[str]) -> dict[str, Any]:
    return {
        "count": len(numbers),
        "has_percent": any("%" in value for value in numbers),
        "has_year_like": any(len(value) == 4 and value.isdigit() for value in numbers),
        "has_decimal": any("." in value for value in numbers),
    }


def split_segment_blocks(text: str) -> list[dict[str, Any]]:
    if "===== SEGMENT" not in text:
        return []
    blocks: list[dict[str, Any]] = []
    current: list[str] = []
    for line in text.splitlines():
        if line.startswith("===== SEGMENT") and current:
            blocks.append(segment_block("\n".join(current)))
            current = []
        current.append(line)
    if current:
        blocks.append(segment_block("\n".join(current)))
    return blocks


def segment_block(text: str) -> dict[str, Any]:
    import re

    segment_ids = [int(value) for value in re.findall(r"SEGMENT\s+(\d+)", text)]
    return {
        "text": text,
        "segment_start": min(segment_ids) if segment_ids else None,
        "segment_end": max(segment_ids) if segment_ids else None,
    }


def fixed_text_chunks(text: str, chunk_chars: int) -> list[str]:
    return [text[index : index + chunk_chars].strip() for index in range(0, len(text), chunk_chars) if text[index : index + chunk_chars].strip()]


def make_context_chunk(index: int, text: str, segment_start: int | None, segment_end: int | None) -> dict[str, Any]:
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    return {
        "chunk_id": f"chunk_{index:04d}_{digest[:12]}",
        "index": index,
        "text": text,
        "char_count": len(text),
        "sha256": digest,
        "segment_start": segment_start,
        "segment_end": segment_end,
    }


def chunk_delivery_payload(text: str) -> dict[str, Any]:
    if raw_chunks_enabled():
        return {
            "delivery_mode": "raw_transcript_chunk",
            "raw_transcript_returned": True,
            "chunk_text": text,
        }
    return {
        "delivery_mode": "remote_safe_evidence",
        "raw_transcript_returned": False,
        "evidence": remote_safe_evidence(text),
    }


def evidence_task_brief(context: dict[str, Any]) -> dict[str, Any]:
    chunk_manifest_value = context.get("chunk_manifest") if isinstance(context.get("chunk_manifest"), dict) else {}
    chunks = chunk_manifest_value.get("chunks") if isinstance(chunk_manifest_value.get("chunks"), list) else []
    first_packet_id = chunks[0].get("chunk_id") if chunks else None
    return {
        "delivery_mode": "remote_safe_evidence",
        "raw_transcript_returned": False,
        "source_text_returned": False,
        "packet_count": len(chunks),
        "first_packet_id": first_packet_id,
        "packet_ids": [chunk.get("chunk_id") for chunk in chunks[:3] if chunk.get("chunk_id")],
        "analysis_contract": {
            "source": "derived evidence packets only",
            "notes_tool": "submit_work_notes",
            "final_tool": "submit_work_output",
            "final_schema_name": EPISODE_CONTEXT_SCHEMA_VERSION,
            "required_schema_version": EPISODE_CONTEXT_SCHEMA_VERSION,
            "target_episode_id": context.get("episode_id"),
            "required_fields": [
                "schema_version",
                "episode_id",
                "context_summary",
                "speaker_map",
                "section_map",
                "entity_seed",
                "concept_seed",
                "extraction_guidance",
                "quality_flags",
                "overall_confidence",
                "needs_review",
                "review_reason",
            ],
            "quality_rule": "Use conservative confidence and set needs_review when evidence is thin.",
        },
        "safety": {
            "no_source_text_in_manifest": True,
            "no_source_text_in_packets": not raw_chunks_enabled(),
            "do_not_quote_or_reconstruct_source": True,
        },
    }


def source_card_task_brief(context: dict[str, Any]) -> dict[str, Any]:
    manifest = context.get("source_card_manifest") if isinstance(context.get("source_card_manifest"), dict) else {}
    cards = manifest.get("cards") if isinstance(manifest.get("cards"), list) else []
    first_card_id = cards[0].get("card_id") if cards else None
    return {
        "delivery_mode": "bounded_source_card",
        "raw_transcript_returned": False,
        "source_text_returned": "features_only_no_continuous_transcript",
        "card_count": len(cards),
        "first_card_id": first_card_id,
        "card_ids": [card.get("card_id") for card in cards[:3] if card.get("card_id")],
        "source_card_policy": {
            "max_excerpt_chars": MAX_SOURCE_CARD_EXCERPT_CHARS,
            "max_card_text_chars": MAX_SOURCE_CARD_TEXT_CHARS,
            "max_cards": MAX_SOURCE_CARDS,
            "model_visible_excerpt_text": False,
            "model_visible_delivery": "features_only_no_continuous_transcript",
            "forbidden_markers": ["WEBVTT", "===== SEGMENT", "Full Prepared Episode Transcript", "BEGIN RAW TRANSCRIPT"],
        },
        "analysis_contract": {
            "source": "bounded transcript-derived source-card features",
            "notes_tool": "submit_work_notes",
            "final_tool": "submit_work_output",
            "final_schema_name": EPISODE_CONTEXT_SCHEMA_VERSION,
            "required_schema_version": EPISODE_CONTEXT_SCHEMA_VERSION,
            "target_episode_id": context.get("episode_id"),
            "required_fields": [
                "schema_version",
                "episode_id",
                "context_summary",
                "speaker_map",
                "section_map",
                "entity_seed",
                "concept_seed",
                "extraction_guidance",
                "quality_flags",
                "overall_confidence",
                "needs_review",
                "review_reason",
            ],
            "required_field_shapes": {
                "schema_version": "string constant ai_discourse_v3_1_episode_context",
                "episode_id": "string matching target_episode_id",
                "context_summary": "useful string",
                "speaker_map": "array of speaker objects",
                "section_map": "array of section objects",
                "entity_seed": "object with array fields: people, organizations, products, models, other",
                "concept_seed": "array of strings",
                "extraction_guidance": "useful string, not an array",
                "quality_flags": "array of strings",
                "overall_confidence": "number from 0.0 to 1.0",
                "needs_review": "boolean",
                "review_reason": "string when needs_review is true; otherwise null or concise string",
            },
            "quality_rule": "Extract only from source-card feature fields and traceability metadata; use conservative confidence when card coverage is thin.",
        },
    }


def validate_source_card(card: dict[str, Any]) -> None:
    excerpt = str(card.get("excerpt_text") or "")
    if card.get("delivery_mode") != "bounded_source_card":
        raise ValueError("source card delivery_mode must be bounded_source_card")
    if len(excerpt) > MAX_SOURCE_CARD_EXCERPT_CHARS:
        raise ValueError("source card excerpt exceeds max chars")
    if source_card_text_size(card) > MAX_SOURCE_CARD_TEXT_CHARS:
        raise ValueError("source card text payload exceeds max chars")
    serialized = json.dumps(card, ensure_ascii=True, sort_keys=True)
    for marker in ("WEBVTT", "===== SEGMENT", "Full Prepared Episode Transcript", "BEGIN RAW TRANSCRIPT"):
        if marker in serialized:
            raise ValueError(f"source card contains forbidden marker: {marker}")
    for key in ("card_id", "source_sha256", "excerpt_sha256", "offset_start", "offset_end"):
        if key not in card:
            raise ValueError(f"source card missing {key}")


def remote_safe_evidence(text: str) -> dict[str, Any]:
    clean_lines = [
        line.strip()
        for line in text.splitlines()
        if line.strip() and not line.strip().startswith("=====") and not line.strip().startswith("#")
    ]
    joined = " ".join(clean_lines)
    lower = joined.lower()
    return {
        "policy": {
            "raw_transcript_text_omitted": True,
            "model_visible_text_kind": "derived_evidence_not_transcript",
            "use_for": "Generate compact notes and low-to-medium-confidence episode context; set needs_review when evidence is thin.",
        },
        "speaker_surfaces": speaker_surfaces(clean_lines),
        "entity_candidates": entity_candidates(joined),
        "concept_candidates": concept_candidates(lower),
        "numeric_cues": numeric_cues(joined),
        "discourse_cues": {
            "question_count": joined.count("?"),
            "contains_contrast_language": any(term in lower for term in ("but", "however", "although", "instead", "whereas")),
            "contains_uncertainty_language": any(term in lower for term in ("maybe", "might", "could", "uncertain", "unclear", "risk")),
            "contains_launch_or_market_language": any(term in lower for term in ("launch", "release", "customer", "market", "revenue", "adoption", "pricing")),
        },
        "shape": {
            "line_count": len(clean_lines),
            "approx_word_count": len(joined.split()),
            "contains_segment_markers": "===== SEGMENT" in text,
        },
    }


def speaker_surfaces(lines: list[str]) -> list[str]:
    import re

    speakers: list[str] = []
    for line in lines:
        match = re.match(r"^([A-Za-z][A-Za-z0-9 ._-]{0,40}):\s+", line)
        if not match:
            continue
        speaker = " ".join(match.group(1).split())
        if speaker and speaker not in speakers:
            speakers.append(speaker)
        if len(speakers) >= 12:
            break
    return speakers


def entity_candidates(text: str) -> list[str]:
    import re

    blocked = {
        "The",
        "This",
        "That",
        "Host",
        "Guest",
        "Segment",
        "Episode",
        "Transcript",
        "Full Prepared Episode Transcript",
    }
    candidates: list[str] = []
    for match in re.finditer(r"\b[A-Z][A-Za-z0-9&.+-]*(?:\s+[A-Z][A-Za-z0-9&.+-]*){0,4}\b", text):
        value = " ".join(match.group(0).split())
        if len(value) < 3 or value in blocked or value.isupper() and len(value) <= 3:
            continue
        if value not in candidates:
            candidates.append(value)
        if len(candidates) >= 30:
            break
    return candidates


def concept_candidates(lower_text: str) -> list[str]:
    concepts = [
        "ai",
        "agi",
        "agent",
        "agents",
        "alignment",
        "benchmark",
        "business model",
        "cloud",
        "coding agent",
        "compute",
        "customer adoption",
        "data center",
        "enterprise",
        "foundation model",
        "gpu",
        "inference",
        "open source",
        "product launch",
        "regulation",
        "research lab",
        "robotics",
        "security",
        "startup",
        "venture",
        "workflow",
    ]
    found: list[str] = []
    for concept in concepts:
        if concept in lower_text and concept not in found:
            found.append(concept)
    return found[:20]


def numeric_cues(text: str) -> list[str]:
    import re

    cues: list[str] = []
    for match in re.finditer(r"\b(?:20\d{2}|\d+(?:\.\d+)?%|\$?\d+(?:\.\d+)?\s?(?:million|billion|trillion|M|B|K)?)\b", text):
        value = match.group(0)
        if value not in cues:
            cues.append(value)
        if len(cues) >= 20:
            break
    return cues


def chunk_manifest(chunks: list[dict[str, Any]], full_text: str) -> dict[str, Any]:
    return {
        "chunk_count": len(chunks),
        "chunk_chars": context_chunk_chars(),
        "total_chars": len(full_text),
        "total_sha256": hashlib.sha256(full_text.encode("utf-8")).hexdigest(),
        "chunks": [
            {
                "chunk_id": chunk["chunk_id"],
                "index": chunk["index"],
                "char_count": chunk["char_count"],
                "sha256": chunk["sha256"],
                "segment_start": chunk.get("segment_start"),
                "segment_end": chunk.get("segment_end"),
            }
            for chunk in chunks
        ],
    }


def work_context_manifest(context: dict[str, Any]) -> dict[str, Any]:
    chunk_manifest_value = context.get("chunk_manifest") or {}
    first_chunk_id = None
    chunks = chunk_manifest_value.get("chunks") if isinstance(chunk_manifest_value.get("chunks"), list) else []
    if chunks:
        first_chunk_id = chunks[0].get("chunk_id")
    allowed_keys = {
        "task",
        "job_id",
        "job_type",
        "label_pack",
        "model_required",
        "worker_id",
        "content_type",
        "source_policy",
        "instructions_text",
        "output_contract",
        "context_protocol",
        "source_text_kind",
    }
    manifest = {key: context.get(key) for key in sorted(allowed_keys) if key in context}
    if "instructions_text" in manifest:
        manifest["instructions_text"] = compact_remote_instructions(str(manifest.get("instructions_text") or ""))
    if "source_text_kind" in manifest:
        manifest["source_text_kind"] = "derived_remote_safe_evidence"
    manifest["chunk_manifest"] = chunk_manifest_value
    manifest["first_chunk_id"] = first_chunk_id
    return manifest


def compact_remote_instructions(_: str) -> str:
    return (
        "Use the chunk tools as remote-safe evidence packets, not raw transcript text. "
        "Generate compact per-chunk notes from speaker/entity/concept/discourse cues only. "
        "For the final ai_discourse_v3_1 episode_context JSON, keep confidence conservative, "
        "set needs_review=true when evidence is thin, do not invent speakers or claims, and never include raw transcript text."
    )


def validate_remote_output(output: dict[str, Any], *, context: dict[str, Any] | None = None) -> dict[str, Any]:
    if output.get("schema_version") == "pif_remote_smoke_v1":
        return validate_smoke_output(output)
    errors: list[str] = []
    if not isinstance(output, dict):
        errors.append("output must be an object")
        return {"ok": False, "errors": errors}
    if output.get("schema_version") == EPISODE_CONTEXT_SCHEMA_VERSION:
        errors.extend(validate_episode_context_output_shape(output, expected_episode_id=(context or {}).get("episode_id")))
    else:
        errors.append(f"schema_version must be {EPISODE_CONTEXT_SCHEMA_VERSION} or pif_remote_smoke_v1")
    return {"ok": not errors, "errors": errors}


def validate_episode_context_output_shape(output: dict[str, Any], *, expected_episode_id: Any = None) -> list[str]:
    errors: list[str] = []
    required = {
        "schema_version",
        "episode_id",
        "context_summary",
        "speaker_map",
        "section_map",
        "entity_seed",
        "concept_seed",
        "extraction_guidance",
        "quality_flags",
        "overall_confidence",
        "needs_review",
        "review_reason",
    }
    missing = sorted(required - set(output))
    if missing:
        errors.append("missing required fields: " + ", ".join(missing))
    if expected_episode_id and output.get("episode_id") != expected_episode_id:
        errors.append("episode_id does not match task target")
    if "speaker_map" in output and not isinstance(output.get("speaker_map"), list):
        errors.append("speaker_map must be a list")
    if "section_map" in output and not isinstance(output.get("section_map"), list):
        errors.append("section_map must be a list")
    if "entity_seed" in output and not isinstance(output.get("entity_seed"), dict):
        errors.append("entity_seed must be an object")
    if "concept_seed" in output and not isinstance(output.get("concept_seed"), list):
        errors.append("concept_seed must be a list")
    if "quality_flags" in output and not isinstance(output.get("quality_flags"), list):
        errors.append("quality_flags must be a list")
    if "needs_review" in output and not isinstance(output.get("needs_review"), bool):
        errors.append("needs_review must be a boolean")
    guidance = output.get("extraction_guidance")
    if "extraction_guidance" in output and (not isinstance(guidance, str) or len(guidance.split()) < 6):
        errors.append("extraction_guidance must be a useful string")
    serialized = json.dumps(output, ensure_ascii=True, sort_keys=True)
    for marker in ("full_segmented_episode_text", "===== SEGMENT", "WEBVTT"):
        if marker in serialized:
            errors.append(f"output appears to contain raw transcript marker: {marker}")
    return errors


def validate_smoke_output(output: dict[str, Any]) -> dict[str, Any]:
    errors: list[str] = []
    if output.get("schema_version") != "pif_remote_smoke_v1":
        errors.append("schema_version must be pif_remote_smoke_v1")
    for key in ("organizations", "products", "signals", "uncertainties"):
        if not isinstance(output.get(key), list):
            errors.append(f"{key} must be an array")
    return {"ok": not errors, "errors": errors}


def worker_policy() -> dict[str, Any]:
    return {
        "ok": True,
        "railway_role": "mcp_broker_and_observer_only",
        "local_source_of_truth": True,
        "canonical_mutation": "local_codex_after_validation",
        "phase1_scopes": sorted(DEFAULT_SCOPES),
        "phase2_scopes_reserved": sorted(FUTURE_SCOPES),
        "data_policy": {
            "default_privacy_tier": "local_only",
            "full_transcript_remote_allowed_only_when": "queue envelope privacy_tier is full_text_allowed and phase2 is enabled",
            "model_visible_remote_context": "remote_safe_evidence_packets_by_default",
            "raw_transcript_chunks_enabled": raw_chunks_enabled(),
            "source_card_extraction_enabled": True,
            "source_card_limits": {
                "max_excerpt_chars": MAX_SOURCE_CARD_EXCERPT_CHARS,
                "max_card_text_chars": MAX_SOURCE_CARD_TEXT_CHARS,
                "max_cards": MAX_SOURCE_CARDS,
                "max_batch_tasks": MAX_SOURCE_CARD_BATCH_TASKS,
            },
            "phase1_raw_text_exposed": False,
        },
        "tools": {
            "enabled": ["get_status", "get_queue_summary", "get_worker_policy", "request_local_cycle", "get_observer_url"],
            "reserved": [
                "reserve_evidence_task",
                "get_evidence_manifest",
                "get_evidence_packet",
                "reserve_source_card_task",
                "reserve_source_card_batch",
                "get_source_card",
                "get_source_card_batch",
                "submit_work_notes",
                "get_work_notes",
                "submit_work_output",
                "submit_work_outputs",
                "heartbeat_work",
                "release_work",
                "skip_source_card_task",
            ],
        },
    }


def make_access_token(*, secret: str, issuer: str, audience: str, subject: str, scope: str, lifetime_seconds: int = 3600) -> str:
    header = {"alg": "HS256", "typ": "JWT"}
    payload = {
        "iss": issuer,
        "aud": audience,
        "sub": subject,
        "scope": scope,
        "iat": int(time.time()),
        "exp": int(time.time()) + lifetime_seconds,
    }
    signing_input = f"{_b64_json(header)}.{_b64_json(payload)}"
    signature = hmac.new(secret.encode("utf-8"), signing_input.encode("utf-8"), hashlib.sha256).digest()
    return f"{signing_input}.{_b64_bytes(signature)}"


def verify_access_token(token: str, *, secret: str, issuer: str, audience: str, required_scopes: set[str]) -> dict[str, Any]:
    try:
        header_b64, payload_b64, signature_b64 = token.split(".", 2)
        signing_input = f"{header_b64}.{payload_b64}"
        expected = hmac.new(secret.encode("utf-8"), signing_input.encode("utf-8"), hashlib.sha256).digest()
        if not hmac.compare_digest(_b64_bytes(expected), signature_b64):
            raise ValueError("bad signature")
        payload = json.loads(_b64_decode(payload_b64))
    except Exception as exc:
        raise PermissionError(f"invalid bearer token: {exc}") from exc
    if payload.get("iss") != issuer:
        raise PermissionError("invalid token issuer")
    aud = payload.get("aud")
    if aud != audience and not (isinstance(aud, list) and audience in aud):
        raise PermissionError("invalid token audience")
    if int(payload.get("exp") or 0) < int(time.time()):
        raise PermissionError("token expired")
    scopes = set(str(payload.get("scope") or "").split())
    if not required_scopes.issubset(scopes):
        raise PermissionError("missing required scope")
    return payload


def verify_pkce(*, verifier: str | None, challenge: str | None, method: str | None) -> bool:
    if not challenge:
        return True
    if not verifier:
        return False
    if method == "S256":
        digest = hashlib.sha256(verifier.encode("utf-8")).digest()
        return _b64_bytes(digest) == challenge
    return verifier == challenge


def _b64_json(value: dict[str, Any]) -> str:
    return _b64_bytes(json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8"))


def _b64_bytes(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _b64_decode(value: str) -> str:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode((value + padding).encode("ascii")).decode("utf-8")
