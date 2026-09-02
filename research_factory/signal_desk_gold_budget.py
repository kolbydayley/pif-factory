"""Weekly-window grant and reservations for Signal Desk gold authoring."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import subprocess
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

from .pif_budget_governor import read_weekly_snapshot
from .signal_desk_model_capacity import record_capacity_snapshot
from .subscription_budget import ensure_budget_schema, record_usage
from .util import dumps_json, now_iso, stable_id


SCHEMA_VERSION = "pif_signal_desk_gold_authoring_grant_v2"
RECEIPT_VERSION = "pif_signal_desk_gold_authoring_weekly_draw_v1"
RESUME_CLEARANCE_RECEIPT_VERSION = "pif_signal_desk_gold_authoring_resume_clearance_v1"
EXPECTED_SCOPE = "gpt_5_6_sol_gold_authoring"
EXPECTED_MODEL = "gpt-5.6-sol"
EXPECTED_PURPOSE = "frozen_804_benchmark_gold_a_b_c_and_blind_audit_only"
MAX_GRANT_DAYS = 30
WARNING_PERCENT = 85.0
KILL_PERCENT = 120.0
MIN_CONCURRENCY = 2
MAX_CONCURRENCY = 8
TURN_TYPES = frozenset({"A", "B", "C", "AUDIT"})
GOLD_LEASE_SECONDS = 1_800
RESUME_GUARD_CONTRACT = {
    "adaptive_concurrency": {"minimum": 2, "maximum": 8},
    "gold_call_deadline_seconds": 900,
    "gold_lease_seconds": 1_800,
    "latency_p95_degradation_seconds": 900,
}


class GoldBudgetError(RuntimeError):
    pass


def normalize_weekly_reset(value: int) -> int:
    """Stabilize provider reset estimates that drift by a few seconds."""

    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise GoldBudgetError("weekly reset timestamp is invalid")
    return int((value + 30) // 60 * 60)


def _instant(value: str | datetime) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise GoldBudgetError("timestamps must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def grant_hash(payload: Mapping[str, Any]) -> str:
    body = {key: value for key, value in payload.items() if key != "grant_sha256"}
    return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def gold_completion_satisfied(receipt: Mapping[str, Any] | None) -> bool:
    if not receipt or receipt.get("manifest_windows") != 804:
        return False
    gold = receipt.get("gold_turns") or {}
    if any(int(gold.get(turn) or 0) != 804 for turn in ("A", "B", "C")):
        return False
    slices = receipt.get("sealed_audit_slices") or {}
    return (
        set(slices) == {"development", "validation", "sealed_holdout"}
        and all(bool((value or {}).get("sealed")) for value in slices.values())
        and sum(int((value or {}).get("windows") or 0) for value in slices.values()) == 81
    )


@dataclass(frozen=True)
class GoldGrant:
    path: Path
    granted_at: datetime
    expires_at: datetime
    payload: Mapping[str, Any]


def load_gold_grant(
    path: Path, *, at: datetime | None = None,
    completion_receipt: Mapping[str, Any] | None = None,
) -> GoldGrant:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise GoldBudgetError("gold-authoring grant is unreadable") from exc
    required = {
        "schema_version", "granted_at", "expires_at", "expiry_conditions",
        "scope", "model", "purpose", "authorized_by", "campaign_id",
        "benchmark_manifest_sha256", "binding_constraint", "warning_percent",
        "kill_percent", "adaptive_concurrency", "grant_sha256",
    }
    if not isinstance(payload, dict) or not required.issubset(payload):
        raise GoldBudgetError("gold-authoring grant is incomplete")
    if payload["schema_version"] != SCHEMA_VERSION or payload["grant_sha256"] != grant_hash(payload):
        raise GoldBudgetError("gold-authoring grant hash or schema mismatch")
    if payload["scope"] != EXPECTED_SCOPE or payload["model"] != EXPECTED_MODEL:
        raise GoldBudgetError("gold-authoring grant scope or model mismatch")
    if payload["purpose"] != EXPECTED_PURPOSE or str(payload["authorized_by"]).casefold() != "kolby":
        raise GoldBudgetError("gold-authoring grant purpose or authority mismatch")
    if payload["binding_constraint"] != "provider_weekly_subscription_window":
        raise GoldBudgetError("daily gold ceiling must remain removed")
    if payload["warning_percent"] != WARNING_PERCENT or payload["kill_percent"] != KILL_PERCENT:
        raise GoldBudgetError("weekly warning or kill threshold drifted")
    if payload["adaptive_concurrency"] != [MIN_CONCURRENCY, MAX_CONCURRENCY]:
        raise GoldBudgetError("gold concurrency contract drifted")
    if payload["expiry_conditions"] != ["benchmark_gold_complete", "30_days"]:
        raise GoldBudgetError("gold-authoring grant expiry conditions drifted")
    granted_at, expires_at = _instant(payload["granted_at"]), _instant(payload["expires_at"])
    if expires_at - granted_at != timedelta(days=MAX_GRANT_DAYS):
        raise GoldBudgetError("gold-authoring grant must expire exactly 30 days after activation")
    if gold_completion_satisfied(completion_receipt):
        raise GoldBudgetError("gold-authoring grant expired at benchmark-gold completion")
    current = (at or datetime.now(timezone.utc)).astimezone(timezone.utc)
    if current < granted_at or current >= expires_at:
        raise GoldBudgetError("gold-authoring grant is not active")
    return GoldGrant(path, granted_at, expires_at, payload)


def ensure_gold_budget_schema(conn: sqlite3.Connection) -> None:
    ensure_budget_schema(conn)
    conn.execute(
        """CREATE TABLE IF NOT EXISTS signal_desk_gold_budget_reservations (
             id TEXT PRIMARY KEY, weekly_resets_at INTEGER NOT NULL,
             task_key TEXT NOT NULL, turn_type TEXT NOT NULL, model TEXT NOT NULL,
             lane TEXT NOT NULL, reserved_tokens INTEGER NOT NULL CHECK(reserved_tokens > 0),
             actual_tokens INTEGER, provider_calls INTEGER,
             status TEXT NOT NULL CHECK(status IN ('active','settled','released')),
             provider_started INTEGER NOT NULL DEFAULT 0,
             created_at TEXT NOT NULL, settled_at TEXT,
             UNIQUE(weekly_resets_at,task_key,turn_type)
           )"""
    )
    conn.execute(
        """CREATE TABLE IF NOT EXISTS signal_desk_gold_weekly_notifications (
             resets_at INTEGER NOT NULL, notification_type TEXT NOT NULL,
             sent_at TEXT NOT NULL, used_percent REAL, details_json TEXT NOT NULL,
             PRIMARY KEY(resets_at,notification_type)
           )"""
    )


def gold_kill_path(budget_dir: Path) -> Path:
    return budget_dir / "KILL-signal-desk-gold-authoring.json"


def _receipt_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        dumps_json({key: value for key, value in payload.items() if key != "receipt_sha256"}).encode("utf-8")
    ).hexdigest()


def clear_operator_requested_gold_kill(
    *, grant_path: Path, budget_dir: Path, authorization_source: str,
    at: datetime | None = None, receipt_dir: Path | None = None,
    pre_resume_reconciliation: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Clear only an explicitly operator-requested Gold stop with a receipt.

    The durable 120%-usage kill is intentionally *not* clearable through this
    path.  The caller must provide a concise source label for current-turn
    owner authorization; the exact conversation text is not persisted.  The
    prior kill is moved into an immutable archive rather than deleted, and a
    hash-bound receipt links the clearance to the active grant.
    """

    source = str(authorization_source or "").strip()
    if not source:
        raise GoldBudgetError("operator resume clearance requires an authorization source")
    instant = _instant(at or datetime.now(timezone.utc))
    grant = load_gold_grant(grant_path, at=instant)
    kill_path = gold_kill_path(budget_dir)
    try:
        raw_kill = kill_path.read_bytes()
        kill = json.loads(raw_kill.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise GoldBudgetError("operator-requested Gold kill is unreadable") from exc
    if not isinstance(kill, Mapping):
        raise GoldBudgetError("operator-requested Gold kill has an invalid shape")
    if (
        kill.get("schema_version") != "pif_signal_desk_gold_authoring_kill_v2"
        or kill.get("reason") != "operator_requested_stop"
        or kill.get("operator_requested") is not True
        or kill.get("clear_requires") != "explicit authorization"
        or kill.get("lane") != EXPECTED_SCOPE
    ):
        raise GoldBudgetError("this clearance path only accepts an operator-requested Gold stop")

    timestamp = instant.strftime("%Y%m%dT%H%M%SZ")
    archive_dir = budget_dir / "cleared-kills"
    destination_dir = receipt_dir or budget_dir / "receipts"
    archived_kill_path = archive_dir / f"KILL-signal-desk-gold-authoring.{timestamp}.json"
    receipt_path = destination_dir / f"signal-desk-gold-resume-{timestamp}.json"
    if archived_kill_path.exists() or receipt_path.exists():
        raise GoldBudgetError("Gold resume receipt or archived kill already exists for this clearance instant")
    archive_dir.mkdir(parents=True, exist_ok=True)
    destination_dir.mkdir(parents=True, exist_ok=True)

    receipt: dict[str, Any] = {
        "schema_version": RESUME_CLEARANCE_RECEIPT_VERSION,
        "cleared_at": instant.isoformat().replace("+00:00", "Z"),
        "authorization": {
            "authorized_by": "Kolby",
            "source": source,
            "source_sha256": hashlib.sha256(source.encode("utf-8")).hexdigest(),
        },
        "grant": {
            "path": str(grant.path),
            "grant_sha256": str(grant.payload["grant_sha256"]),
            "scope": EXPECTED_SCOPE,
            "model": EXPECTED_MODEL,
            "expires_at": grant.expires_at.isoformat().replace("+00:00", "Z"),
        },
        "cleared_kill": {
            "original_path": str(kill_path),
            "archived_path": str(archived_kill_path),
            "kill_sha256": hashlib.sha256(raw_kill).hexdigest(),
            "engaged_at": str(kill.get("engaged_at") or ""),
            "reason": "operator_requested_stop",
        },
        "resume_guard_contract": dict(RESUME_GUARD_CONTRACT),
        "pre_resume_reconciliation": dict(pre_resume_reconciliation or {}),
        "provider_calls_started_by_clearance": 0,
    }
    receipt["receipt_sha256"] = _receipt_hash(receipt)

    # A clearance must be recoverable if the receipt cannot be made durable.
    # Move rather than delete the KILL, then restore it if the exclusive receipt
    # write fails for any reason.
    os.replace(kill_path, archived_kill_path)
    try:
        with receipt_path.open("x", encoding="utf-8") as handle:
            handle.write(dumps_json(receipt) + "\n")
        receipt_path.chmod(0o600)
    except BaseException:
        if not kill_path.exists() and archived_kill_path.exists():
            os.replace(archived_kill_path, kill_path)
        raise
    return {**receipt, "receipt_path": str(receipt_path)}


def reconcile_orphaned_started_gold_reservations(
    conn: sqlite3.Connection,
    *, at: datetime | None = None, minimum_age_seconds: int = GOLD_LEASE_SECONDS,
    task_keys: set[str] | None = None,
    actual_tokens_by_reservation: Mapping[str, int] | None = None,
) -> dict[str, Any]:
    """Conservatively settle old started reservations before a clean resume.

    A process can die after the app-server turn begins and before its normal
    ``finally`` settles the reservation. Such a row must never be released:
    provider usage may already have occurred. Once it is older than the Gold
    lease and is explicitly within this resume's task set (when supplied), the
    only safe action is to charge the original reservation and preserve the
    task for a fresh, fenced retry attempt. A caller may provide exact tokens
    from a completed, hash-bound sidecar; otherwise the full reservation is
    charged conservatively.
    """

    if minimum_age_seconds < GOLD_LEASE_SECONDS:
        raise GoldBudgetError("orphaned Gold reservations require at least one full lease age")
    instant = _instant(at or datetime.now(timezone.utc))
    ensure_gold_budget_schema(conn)
    rows = conn.execute(
        """SELECT id,weekly_resets_at,task_key,turn_type,reserved_tokens,created_at
           FROM signal_desk_gold_budget_reservations
           WHERE status='active' AND provider_started=1 AND lane=? AND model=?
           ORDER BY created_at,id""",
        (EXPECTED_SCOPE, EXPECTED_MODEL),
    ).fetchall()
    settled: list[dict[str, Any]] = []
    skipped_recent = 0
    skipped_unscoped = 0
    normalized_keys = None if task_keys is None else {str(value) for value in task_keys}
    exact_usage = {
        str(reservation_id): int(tokens)
        for reservation_id, tokens in (actual_tokens_by_reservation or {}).items()
    }
    if any(tokens < 0 for tokens in exact_usage.values()):
        raise GoldBudgetError("orphaned Gold sidecar usage must be non-negative")
    examined_ids: set[str] = set()
    for row in rows:
        mapping = dict(row) if isinstance(row, sqlite3.Row) else {
            "id": row[0], "weekly_resets_at": row[1], "task_key": row[2],
            "turn_type": row[3], "reserved_tokens": row[4], "created_at": row[5],
        }
        if normalized_keys is not None and str(mapping["task_key"]) not in normalized_keys:
            skipped_unscoped += 1
            continue
        age_seconds = (instant - _instant(str(mapping["created_at"]))).total_seconds()
        if age_seconds < minimum_age_seconds:
            skipped_recent += 1
            continue
        reservation_id = str(mapping["id"])
        examined_ids.add(reservation_id)
        reserved_tokens = int(mapping["reserved_tokens"])
        charged_tokens = exact_usage.get(reservation_id, reserved_tokens)
        settlement_basis = (
            "completed_sidecar_usage" if reservation_id in exact_usage
            else "full_reservation_conservative"
        )
        # This mirrors normal settlement, intentionally charging the full
        # reservation because a started provider call has no trustworthy usage
        # result after a process loss.
        record_usage(
            conn,
            day=f"weekly:{int(mapping['weekly_resets_at'])}",
            provider_lane="codex_subscription",
            lane=EXPECTED_SCOPE,
            run_id=f"gold:{reservation_id}",
            tokens=charged_tokens,
            provider_calls=1,
        )
        changed = conn.execute(
            """UPDATE signal_desk_gold_budget_reservations
               SET status='settled',actual_tokens=?,provider_calls=1,settled_at=?
               WHERE id=? AND status='active' AND provider_started=1""",
            (charged_tokens, now_iso(), reservation_id),
        ).rowcount
        if changed != 1:
            conn.rollback()
            raise GoldBudgetError("orphaned Gold reservation changed during reconciliation")
        conn.commit()
        settled.append({
            "reservation_id": reservation_id,
            "task_key": str(mapping["task_key"]),
            "turn_type": str(mapping["turn_type"]),
            "charged_tokens": charged_tokens,
            "settlement_basis": settlement_basis,
            "age_seconds": int(age_seconds),
        })
    unused_usage = sorted(set(exact_usage) - examined_ids)
    if unused_usage:
        raise GoldBudgetError("orphaned Gold sidecar usage references a non-reconciled reservation")
    result = {
        "schema_version": "pif_signal_desk_gold_orphaned_reservation_reconciliation_v1",
        "reconciled_at": instant.isoformat().replace("+00:00", "Z"),
        "minimum_age_seconds": minimum_age_seconds,
        "settled_count": len(settled),
        "settled_reserved_tokens": sum(item["charged_tokens"] for item in settled),
        "settled": settled,
        "skipped_recent": skipped_recent,
        "skipped_outside_resume_scope": skipped_unscoped,
        "exact_usage_reservation_count": len(exact_usage),
    }
    result["reconciliation_sha256"] = hashlib.sha256(dumps_json(result).encode("utf-8")).hexdigest()
    return result


def _notify_once(
    conn: sqlite3.Connection, *, snapshot: Mapping[str, Any], notification_type: str,
    summary: str, details: str, next_step: str,
) -> bool:
    resets_at = int(snapshot["resets_at"])
    if conn.execute(
        "SELECT 1 FROM signal_desk_gold_weekly_notifications WHERE resets_at=? AND notification_type=?",
        (resets_at, notification_type),
    ).fetchone():
        return False
    completed = subprocess.run(
        ["codex-ops", "notify", "--source", "signal-desk-gold-authoring",
         "--summary", summary, "--severity", "high", "--details", details,
         "--next-step", next_step, "--dedupe-key", f"signal-desk-gold:{resets_at}:{notification_type}",
         "--telegram-mode", "prefer", "--json"],
        capture_output=True, text=True, timeout=30, check=False,
    )
    if completed.returncode != 0:
        raise GoldBudgetError("gold stall/warning notification failed")
    conn.execute(
        "INSERT INTO signal_desk_gold_weekly_notifications VALUES (?,?,?,?,?)",
        (resets_at, notification_type, now_iso(), float(snapshot["used_percent"]),
         dumps_json({"summary": summary, "next_step": next_step})),
    )
    conn.commit()
    return True


def weekly_health(
    conn: sqlite3.Connection, *, session_root: Path, budget_dir: Path,
    notify: bool = True, live_snapshot: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    ensure_gold_budget_schema(conn)
    # The live app-server read is authoritative when present.  Session-log
    # snapshots remain only a compatibility fallback for recovery tooling;
    # they are not allowed to override a fresh account read.
    snapshot = dict(live_snapshot) if live_snapshot is not None else read_weekly_snapshot(session_root)
    if not snapshot:
        synthetic = {"used_percent": 100.0, "resets_at": 0}
        if notify:
            _notify_once(
                conn, snapshot=synthetic, notification_type="snapshot_missing",
                summary="Signal Desk gold authoring stalled: weekly usage unavailable",
                details="The weekly subscription snapshot is missing, so the gold lane failed closed.",
                next_step="Restore a fresh Codex weekly usage snapshot; authoring resumes automatically afterward.",
            )
        return {"allowed": False, "reason": "weekly_snapshot_missing", "notified": notify}
    if "source" not in snapshot:
        snapshot["source"] = (
            "app_server_live" if live_snapshot is not None else "session_log_fallback"
        )
    record_capacity_snapshot(conn, lane=EXPECTED_SCOPE, snapshot=snapshot)
    snapshot = {
        **snapshot,
        "provider_resets_at": int(snapshot["resets_at"]),
        "resets_at": normalize_weekly_reset(int(snapshot["resets_at"])),
    }
    used = float(snapshot["used_percent"])
    kill_path = gold_kill_path(budget_dir)
    if used >= KILL_PERCENT and not kill_path.exists():
        budget_dir.mkdir(parents=True, exist_ok=True)
        body = {"schema_version": "pif_signal_desk_gold_authoring_kill_v2",
                "engaged_at": now_iso(), "weekly_resets_at": int(snapshot["resets_at"]),
                "used_percent": used, "kill_percent": KILL_PERCENT, "lane": EXPECTED_SCOPE}
        body["kill_sha256"] = hashlib.sha256(dumps_json(body).encode()).hexdigest()
        kill_path.write_text(dumps_json(body) + "\n", encoding="utf-8")
    if kill_path.exists():
        if notify:
            _notify_once(
                conn, snapshot=snapshot, notification_type="kill",
                summary="Signal Desk gold authoring stopped by weekly kill switch",
                details=f"Weekly usage is {used:.1f}%; the persistent 120% gold kill is engaged.",
                next_step="Reconcile the weekly ledger and explicitly clear the kill after authorization.",
            )
        return {"allowed": False, "reason": "gold_weekly_kill_present", **snapshot, "notified": notify}
    warned = False
    if used >= WARNING_PERCENT and notify:
        warned = _notify_once(
            conn, snapshot=snapshot, notification_type="85_percent",
            summary="Signal Desk gold authoring will need a usage reset soon",
            details=f"The Codex weekly window is {used:.1f}% consumed. Gold continues until actual exhaustion.",
            next_step="Be ready to authorize/redeem the available usage reset when the provider exhausts.",
        )
    return {"allowed": True, "reason": None, **snapshot,
            "remaining_percent": max(0.0, 100.0 - used), "warning_sent_now": warned}


def reserve_gold_call(
    conn: sqlite3.Connection, *, grant_path: Path, session_root: Path,
    budget_dir: Path, task_key: str, turn_type: str, reserve_tokens: int,
    model: str = EXPECTED_MODEL, at: datetime | None = None,
    completion_receipt: Mapping[str, Any] | None = None,
    live_snapshot: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    grant = load_gold_grant(grant_path, at=at, completion_receipt=completion_receipt)
    if turn_type not in TURN_TYPES or model != EXPECTED_MODEL or reserve_tokens <= 0:
        raise GoldBudgetError("call is outside the authorized gold lane contract")
    health = weekly_health(
        conn, session_root=session_root, budget_dir=budget_dir, live_snapshot=live_snapshot
    )
    if not health["allowed"]:
        return health
    resets_at = int(health["resets_at"])
    reservation_id = stable_id(str(resets_at), task_key, turn_type, prefix="sdgr_")
    try:
        conn.execute(
            """INSERT INTO signal_desk_gold_budget_reservations
               (id,weekly_resets_at,task_key,turn_type,model,lane,reserved_tokens,status,created_at)
               VALUES (?,?,?,?,?,?,?,'active',?)""",
            (reservation_id, resets_at, task_key, turn_type, model, EXPECTED_SCOPE, reserve_tokens, now_iso()),
        )
        conn.commit()
    except sqlite3.IntegrityError as exc:
        raise GoldBudgetError("gold task already has a reservation in this weekly window") from exc
    return {"allowed": True, "reservation_id": reservation_id, "lane": EXPECTED_SCOPE,
            "model": model, "turn_type": turn_type, "reserved_tokens": reserve_tokens,
            "weekly_used_percent": health["used_percent"],
            "weekly_remaining_percent": health["remaining_percent"],
            "weekly_resets_at": resets_at, "grant_expires_at": grant.expires_at.isoformat()}


def mark_provider_started(conn: sqlite3.Connection, reservation_id: str) -> None:
    changed = conn.execute(
        "UPDATE signal_desk_gold_budget_reservations SET provider_started=1 WHERE id=? AND status='active'",
        (reservation_id,),
    ).rowcount
    conn.commit()
    if changed != 1:
        raise GoldBudgetError("active gold reservation not found")


def settle_gold_call(
    conn: sqlite3.Connection, *, reservation_id: str, actual_tokens: int,
    provider_calls: int = 1,
) -> dict[str, Any]:
    ensure_gold_budget_schema(conn)
    row = conn.execute(
        "SELECT * FROM signal_desk_gold_budget_reservations WHERE id=? AND status='active'",
        (reservation_id,),
    ).fetchone()
    if row is None:
        raise GoldBudgetError("active gold reservation not found")
    record_usage(
        conn, day=f"weekly:{row['weekly_resets_at']}", provider_lane="codex_subscription",
        lane=EXPECTED_SCOPE, run_id=f"gold:{reservation_id}", tokens=actual_tokens,
        provider_calls=provider_calls,
    )
    conn.execute(
        """UPDATE signal_desk_gold_budget_reservations
           SET status='settled',actual_tokens=?,provider_calls=?,settled_at=? WHERE id=?""",
        (actual_tokens, provider_calls, now_iso(), reservation_id),
    )
    conn.commit()
    return {"reservation_id": reservation_id, "settled": True,
            "actual_tokens": actual_tokens, "provider_calls": provider_calls}


def release_unstarted_reservation(conn: sqlite3.Connection, reservation_id: str) -> None:
    changed = conn.execute(
        """UPDATE signal_desk_gold_budget_reservations SET status='released',settled_at=?
           WHERE id=? AND status='active' AND provider_started=0""",
        (now_iso(), reservation_id),
    ).rowcount
    conn.commit()
    if changed != 1:
        raise GoldBudgetError("only an unstarted active reservation may be released")


def weekly_draw_receipt(
    conn: sqlite3.Connection, *, grant: GoldGrant, snapshot: Mapping[str, Any],
    completion_receipt: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    ensure_gold_budget_schema(conn)
    resets_at = int(snapshot["resets_at"])
    token_row = conn.execute(
        "SELECT COALESCE(SUM(actual_tokens),0),COUNT(*) FROM signal_desk_gold_budget_reservations WHERE weekly_resets_at=? AND status='settled'",
        (resets_at,),
    ).fetchone()
    return {"schema_version": RECEIPT_VERSION, "lane": EXPECTED_SCOPE,
            "model": EXPECTED_MODEL, "weekly_resets_at": resets_at,
            "provider_used_percent": float(snapshot["used_percent"]),
            "gold_tokens_metered": int(token_row[0]), "gold_calls_metered": int(token_row[1]),
            "warning_percent": WARNING_PERCENT, "kill_percent": KILL_PERCENT,
            "grant_path": str(grant.path), "grant_expires_at": grant.expires_at.isoformat(),
            "completion_condition_met": gold_completion_satisfied(completion_receipt),
            "daily_ceiling": None, "binding_constraint": "provider_weekly_subscription_window"}
