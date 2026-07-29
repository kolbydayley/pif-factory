from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Optional

from .paths import root
from .util import dumps_json, loads_json, now_iso, sha256_text, stable_id, write_text_atomic


DAILY_STAGE_NAMES = (
    "backup_health",
    "rss_ingestion_and_due_transcript_strategies",
    "normalize",
    "bounded_baseline_extraction",
    "evidence_schema_privacy_validation",
    "identity_and_semantic_reconciliation",
    "due_outcomes",
    "local_intelligence_refresh",
    "sanitized_observer_publish",
    "immutable_daily_receipt",
)
OPERATIONAL_STAGE_NAMES = DAILY_STAGE_NAMES[:-1]
MAX_DAILY_RUNTIME_SECONDS = 7_200
MAX_DAILY_ITEMS = 500
MAX_EXCEPTION_RUNTIME_MS = 1_200_000
DEFAULT_DAILY_RUNTIME_SECONDS = 5_400
DEFAULT_DAILY_ITEMS = 25
AUTHORITY_CHECKPOINT_RETENTION = 7
ZOMBIE_WORKER_GRACE_SECONDS = 3_600
SCALE_GATE_REQUIRED_DAYS = 7
SCALE_GATE_TIERS = (25, 100, 500)
SCALE_GATE_MINIMUMS = {
    "accepted_atomic_claims": 200,
    "accepted_claim_subjects": 10,
    "accepted_claim_relations": 50,
    "accepted_outcome_resolutions": 10,
}


DAILY_SCHEMA = """
CREATE TABLE IF NOT EXISTS pif_daily_runs (
  id TEXT PRIMARY KEY,
  idempotency_key TEXT NOT NULL UNIQUE,
  run_date TEXT NOT NULL,
  config_json TEXT NOT NULL,
  status TEXT NOT NULL,
  lease_owner TEXT,
  leased_until TEXT,
  started_at TEXT NOT NULL,
  completed_at TEXT,
  receipt_path TEXT,
  receipt_sha256 TEXT,
  receipt_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS pif_daily_stage_receipts (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES pif_daily_runs(id),
  stage_index INTEGER NOT NULL,
  stage_name TEXT NOT NULL,
  status TEXT NOT NULL,
  receipt_sha256 TEXT NOT NULL,
  receipt_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE(run_id, stage_name)
);

CREATE TABLE IF NOT EXISTS pif_exception_dispatches (
  id TEXT PRIMARY KEY,
  source_task_id TEXT NOT NULL UNIQUE,
  contract_json TEXT NOT NULL,
  contract_sha256 TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'recorded_only',
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pif_scale_gate_state_receipts (
  id TEXT PRIMARY KEY,
  daily_run_id TEXT NOT NULL REFERENCES pif_daily_runs(id),
  run_date TEXT NOT NULL,
  corpus_release_id TEXT,
  cohort_tier TEXT NOT NULL,
  cohort_item_count INTEGER NOT NULL DEFAULT 0,
  next_tier TEXT,
  genuinely_successful INTEGER NOT NULL CHECK(genuinely_successful IN (0, 1)),
  consecutive_success_days INTEGER NOT NULL DEFAULT 0 CHECK(consecutive_success_days >= 0),
  promotion_eligible INTEGER NOT NULL CHECK(promotion_eligible IN (0, 1)),
  gate_json TEXT NOT NULL,
  receipt_sha256 TEXT NOT NULL CHECK(length(receipt_sha256) = 64),
  receipt_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE(daily_run_id)
);

CREATE INDEX IF NOT EXISTS idx_pif_daily_runs_date ON pif_daily_runs(run_date, status);
CREATE INDEX IF NOT EXISTS idx_pif_daily_stage_run ON pif_daily_stage_receipts(run_id, stage_index);
CREATE INDEX IF NOT EXISTS idx_pif_scale_gate_release_date
  ON pif_scale_gate_state_receipts(corpus_release_id, cohort_tier, run_date, genuinely_successful);

CREATE TRIGGER IF NOT EXISTS pif_scale_gate_state_receipts_no_update
BEFORE UPDATE ON pif_scale_gate_state_receipts BEGIN
  SELECT RAISE(ABORT, 'scale gate state receipts are append-only');
END;

CREATE TRIGGER IF NOT EXISTS pif_scale_gate_state_receipts_no_delete
BEFORE DELETE ON pif_scale_gate_state_receipts BEGIN
  SELECT RAISE(ABORT, 'scale gate state receipts are append-only');
END;
"""


StageHandler = Callable[["DailyStageContext"], Optional[Mapping[str, Any]]]


@dataclass(frozen=True)
class DailyStageContext:
    conn: sqlite3.Connection
    run_id: str
    run_date: str
    stage_name: str
    stage_index: int
    max_items: int
    remaining_seconds: float
    deadline_monotonic: float
    artifact_dir: Path


def ensure_daily_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(DAILY_SCHEMA)
    conn.commit()


def scale_gate_status(conn: sqlite3.Connection) -> dict[str, Any]:
    """Return the latest immutable local scale-gate state without taking action."""

    ensure_daily_schema(conn)
    row = conn.execute(
        """
        SELECT receipt_json FROM pif_scale_gate_state_receipts
        ORDER BY run_date DESC, created_at DESC, id DESC
        LIMIT 1
        """
    ).fetchone()
    if not row:
        return {
            "ok": True,
            "state": "no_receipts",
            "promotion_eligible": False,
            "required_consecutive_days": SCALE_GATE_REQUIRED_DAYS,
            "action_taken": False,
        }
    receipt = loads_json(row["receipt_json"], {})
    if not receipt:
        return {
            "ok": False,
            "state": "invalid_receipt",
            "promotion_eligible": False,
            "action_taken": False,
        }
    return {
        "ok": True,
        "state": "eligible" if receipt.get("promotion_eligible") else "gated",
        "promotion_eligible": bool(receipt.get("promotion_eligible")),
        "action_taken": False,
        "receipt": receipt,
    }


def build_headless_exception_contract(
    *,
    source_task_id: str,
    prompt: str,
    title: str,
    cwd: str | Path | None = None,
    project_id: str = "podcast-intelligence-factory",
    source_app: str = "pif",
    policy_profile: str = "managed-chatgpt-auth-only",
    priority: int = 50,
    sandbox: str = "workspace-write",
    max_runtime_ms: int = MAX_EXCEPTION_RUNTIME_MS,
) -> dict[str, Any]:
    """Build, but never launch, one bounded exception task contract."""

    if not source_task_id.strip():
        raise ValueError("source_task_id is required")
    if not title.strip():
        raise ValueError("title is required")
    clean_prompt = prompt.strip()
    if not clean_prompt:
        raise ValueError("prompt is required")
    if len(clean_prompt) > 4_000:
        raise ValueError("exception prompt exceeds the 4,000 character bound")
    if "BEGIN RAW TRANSCRIPT" in clean_prompt.upper():
        raise ValueError("raw transcripts are not allowed in exception dispatch contracts")
    if not 0 <= int(priority) <= 1_000:
        raise ValueError("priority must be between 0 and 1,000")
    runtime = int(max_runtime_ms)
    if not 1 <= runtime <= MAX_EXCEPTION_RUNTIME_MS:
        raise ValueError(f"max_runtime_ms must be between 1 and {MAX_EXCEPTION_RUNTIME_MS}")
    resolved_cwd = Path(cwd or root()).expanduser().resolve()
    return {
        "projectId": project_id,
        "sourceApp": source_app,
        "sourceTaskId": source_task_id,
        "cwd": str(resolved_cwd),
        "prompt": clean_prompt,
        "policyProfile": policy_profile,
        "priority": int(priority),
        "sandbox": sandbox,
        "maxRuntimeMs": runtime,
        "title": title.strip(),
    }


def record_exception_contract(conn: sqlite3.Connection, contract: Mapping[str, Any]) -> dict[str, Any]:
    ensure_daily_schema(conn)
    required = {
        "projectId",
        "sourceApp",
        "sourceTaskId",
        "cwd",
        "prompt",
        "policyProfile",
        "priority",
        "sandbox",
        "maxRuntimeMs",
        "title",
    }
    if set(contract) != required:
        raise ValueError("exception contract fields do not match the bounded dispatch contract")
    payload = dumps_json(dict(contract))
    digest = sha256_text(payload)
    dispatch_id = stable_id("pif_exception_dispatch", str(contract["sourceTaskId"]), prefix="pxd_")
    existing = conn.execute(
        "SELECT contract_json, contract_sha256 FROM pif_exception_dispatches WHERE source_task_id = ?",
        (contract["sourceTaskId"],),
    ).fetchone()
    if existing:
        if existing["contract_sha256"] != digest or existing["contract_json"] != payload:
            raise ValueError(f"exception contract drift for {contract['sourceTaskId']}")
        return {"ok": True, "recorded": False, "state": "recorded_only", "contract": dict(contract)}
    conn.execute(
        """
        INSERT INTO pif_exception_dispatches
          (id, source_task_id, contract_json, contract_sha256, state, created_at)
        VALUES (?, ?, ?, ?, 'recorded_only', ?)
        """,
        (dispatch_id, contract["sourceTaskId"], payload, digest, now_iso()),
    )
    conn.commit()
    return {"ok": True, "recorded": True, "state": "recorded_only", "contract": dict(contract)}


def run_daily_cycle(
    conn: sqlite3.Connection,
    *,
    run_date: str | None = None,
    receipt_dir: str | Path | None = None,
    max_runtime_seconds: int = DEFAULT_DAILY_RUNTIME_SECONDS,
    max_items: int = DEFAULT_DAILY_ITEMS,
    idempotency_key: str | None = None,
    stage_handlers: Mapping[str, StageHandler] | None = None,
    source_list: str | Path | None = None,
    since: str | None = None,
    execute_ingestion: bool = False,
    execute_normalize: bool = False,
    execute_extraction: bool = False,
    apply_reconcile: bool = False,
    record_exception_contracts: bool = False,
    publish_observer: bool = False,
    snapshot_output: str | Path | None = None,
    observer_url: str | None = None,
    observer_token: str | None = None,
    lane: str = "podcast",
    label_pack: str = "ai_discourse_v3_1",
    model: str = "gpt-5.5",
    pilot_id: str | None = None,
    _monotonic: Callable[[], float] = time.monotonic,
    _now: Callable[[], str] = now_iso,
) -> dict[str, Any]:
    """Run one deterministic, bounded local daily cycle.

    Extraction remains disabled unless ``execute_extraction`` is explicitly
    set.  When enabled, the bounded baseline uses subscription-authenticated
    local Codex CLI handoffs only.
    """

    runtime = int(max_runtime_seconds)
    item_limit = int(max_items)
    if not 1 <= runtime <= MAX_DAILY_RUNTIME_SECONDS:
        raise ValueError(f"max_runtime_seconds must be between 1 and {MAX_DAILY_RUNTIME_SECONDS}")
    if not 1 <= item_limit <= MAX_DAILY_ITEMS:
        raise ValueError(f"max_items must be between 1 and {MAX_DAILY_ITEMS}")
    effective_date = run_date or dt.datetime.now(dt.timezone.utc).date().isoformat()
    try:
        dt.date.fromisoformat(effective_date)
    except ValueError as exc:
        raise ValueError("run_date must be YYYY-MM-DD") from exc

    receipts_root = Path(receipt_dir or (root() / "work" / "pif-ops" / "daily")).expanduser().resolve()
    safe_config = {
        "run_date": effective_date,
        "max_runtime_seconds": runtime,
        "max_items": item_limit,
        "source_list": str(Path(source_list).expanduser().resolve()) if source_list else None,
        "since": since,
        "execute_ingestion": bool(execute_ingestion),
        "execute_normalize": bool(execute_normalize),
        "execute_extraction": bool(execute_extraction),
        "apply_reconcile": bool(apply_reconcile),
        "record_exception_contracts": bool(record_exception_contracts),
        "publish_observer": bool(publish_observer),
        "snapshot_output": str(Path(snapshot_output).expanduser().resolve()) if snapshot_output else None,
        "observer_url_configured": bool(observer_url),
        "observer_token_configured": bool(observer_token),
        "lane": lane,
        "label_pack": label_pack,
        "model": model,
        "pilot_id": pilot_id,
        "stage_order": list(DAILY_STAGE_NAMES),
        "custom_handlers": sorted((stage_handlers or {}).keys()),
    }
    config_json = dumps_json(safe_config)
    effective_key = idempotency_key or stable_id("pif_daily", effective_date, config_json, prefix="pif-daily-")
    run_id = stable_id("pif_daily_run", effective_key, prefix="pdr_")
    artifact_dir = receipts_root / effective_date / run_id
    started_at = _now()
    owner = stable_id(str(os.getpid()), run_id, started_at, prefix="pdl_")
    leased_until = (
        dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=runtime + 60)
    ).replace(microsecond=0).isoformat()

    ensure_daily_schema(conn)
    conn.execute(
        """
        INSERT OR IGNORE INTO pif_daily_runs
          (id, idempotency_key, run_date, config_json, status, started_at)
        VALUES (?, ?, ?, ?, 'running', ?)
        """,
        (run_id, effective_key, effective_date, config_json, started_at),
    )
    conn.commit()
    run_row = conn.execute("SELECT * FROM pif_daily_runs WHERE idempotency_key = ?", (effective_key,)).fetchone()
    if not run_row:
        raise RuntimeError("daily run record could not be created")
    if run_row["config_json"] != config_json:
        raise ValueError("idempotency_key is already bound to different daily-cycle configuration")
    if run_row["receipt_json"] and run_row["receipt_json"] != "{}":
        return _replay_daily_receipt(run_row)

    now_value = _now()
    acquired = conn.execute(
        """
        UPDATE pif_daily_runs
        SET lease_owner = ?, leased_until = ?
        WHERE id = ?
          AND (lease_owner IS NULL OR lease_owner = ? OR leased_until IS NULL OR leased_until < ?)
        """,
        (owner, leased_until, run_id, owner, now_value),
    ).rowcount
    conn.commit()
    if not acquired:
        return {
            "ok": False,
            "status": "already_running",
            "run_id": run_id,
            "idempotency_key": effective_key,
            "external_launch_attempted": False,
            "self_resuming_chats": False,
        }

    handlers = _default_stage_handlers(
        conn,
        source_list=source_list,
        since=since,
        execute_ingestion=execute_ingestion,
        execute_normalize=execute_normalize,
        execute_extraction=execute_extraction,
        apply_reconcile=apply_reconcile,
        record_exception_contracts=record_exception_contracts,
        publish_observer=publish_observer,
        snapshot_output=snapshot_output,
        observer_url=observer_url,
        observer_token=observer_token,
        lane=lane,
        label_pack=label_pack,
        model=model,
        pilot_id=pilot_id,
        now=_now,
    )
    handlers.update(stage_handlers or {})
    cycle_started = _monotonic()
    deadline = cycle_started + runtime
    stage_receipts: list[dict[str, Any]] = []
    terminal_error: dict[str, Any] | None = None

    for stage_index, stage_name in enumerate(OPERATIONAL_STAGE_NAMES, start=1):
        existing = _load_stage_receipt(conn, run_id, stage_name)
        if existing:
            _ensure_stage_receipt_file(artifact_dir, existing)
            stage_receipts.append(existing)
            if existing["status"] == "failed":
                terminal_error = existing.get("result") or {"error": "prior_stage_failed"}
                break
            continue

        remaining = deadline - _monotonic()
        if remaining <= 0:
            receipt = _stage_receipt(
                run_id=run_id,
                stage_index=stage_index,
                stage_name=stage_name,
                status="failed",
                started_at=_now(),
                completed_at=_now(),
                elapsed_ms=0,
                max_items=item_limit,
                result={"error": "daily_runtime_exceeded", "processed": 0},
            )
        else:
            handler = handlers.get(stage_name, _skipped_handler)
            stage_started_at = _now()
            stage_started = _monotonic()
            context = DailyStageContext(
                conn=conn,
                run_id=run_id,
                run_date=effective_date,
                stage_name=stage_name,
                stage_index=stage_index,
                max_items=item_limit,
                remaining_seconds=max(0.0, remaining),
                deadline_monotonic=deadline,
                artifact_dir=artifact_dir,
            )
            try:
                raw_result = handler(context) or {}
                result = _bounded_json_value(dict(raw_result), list_limit=item_limit)
                processed = int(result.get("processed", 0))
                if processed < 0 or processed > item_limit:
                    raise ValueError(f"stage processed {processed} items; bound is {item_limit}")
                status = str(result.get("status") or "completed")
                if status not in {"completed", "skipped", "failed"}:
                    raise ValueError(f"unsupported stage status: {status}")
                elapsed_ms = max(0, int((_monotonic() - stage_started) * 1_000))
                if _monotonic() > deadline:
                    status = "failed"
                    result = {
                        "error": "daily_runtime_exceeded",
                        "processed": processed,
                        "handler_status": result.get("status"),
                    }
                receipt = _stage_receipt(
                    run_id=run_id,
                    stage_index=stage_index,
                    stage_name=stage_name,
                    status=status,
                    started_at=stage_started_at,
                    completed_at=_now(),
                    elapsed_ms=elapsed_ms,
                    max_items=item_limit,
                    result=result,
                )
            except Exception as exc:
                conn.rollback()
                receipt = _stage_receipt(
                    run_id=run_id,
                    stage_index=stage_index,
                    stage_name=stage_name,
                    status="failed",
                    started_at=stage_started_at,
                    completed_at=_now(),
                    elapsed_ms=max(0, int((_monotonic() - stage_started) * 1_000)),
                    max_items=item_limit,
                    result={
                        "error": "stage_handler_failed",
                        "error_class": type(exc).__name__,
                        "message": str(exc)[:500],
                        "processed": 0,
                    },
                )

        receipt = _store_stage_receipt(conn, receipt)
        _ensure_stage_receipt_file(artifact_dir, receipt)
        stage_receipts.append(receipt)
        if receipt["status"] == "failed":
            terminal_error = receipt.get("result") or {"error": "stage_failed"}
            break

    stage_truth = _assess_required_stage_truth(stage_receipts, conn=conn)
    if terminal_error:
        final_status = "failed"
    elif stage_truth["blockers"]:
        final_status = "blocked_required_work"
        terminal_error = {
            "error": "required_production_work_not_completed",
            "blockers": stage_truth["blockers"],
        }
    elif stage_truth["healthy_no_work_stages"]:
        final_status = "completed"
    elif any(item["status"] == "skipped" for item in stage_receipts):
        final_status = "completed_with_skips"
    else:
        final_status = "completed"

    elapsed_seconds = max(0.0, _monotonic() - cycle_started)
    try:
        scale_gate = _record_scale_gate_state_receipt(
            conn,
            run_id=run_id,
            run_date=effective_date,
            stage_receipts=stage_receipts,
            stage_truth=stage_truth,
            cycle_status=final_status,
            elapsed_seconds=elapsed_seconds,
            max_runtime_seconds=runtime,
            max_items=item_limit,
            created_at=_now(),
        )
        _write_immutable_json(artifact_dir / "scale-gate.json", scale_gate)
    except Exception as exc:
        conn.rollback()
        final_status = "failed"
        terminal_error = {
            "error": "scale_gate_receipt_failed",
            "error_class": type(exc).__name__,
            "message": str(exc)[:500],
        }
        scale_gate = {
            "ok": False,
            "genuinely_successful": False,
            "promotion_eligible": False,
            "error": "scale_gate_receipt_failed",
        }

    final_stage = _load_stage_receipt(conn, run_id, DAILY_STAGE_NAMES[-1])
    if not final_stage:
        final_stage = _stage_receipt(
            run_id=run_id,
            stage_index=len(DAILY_STAGE_NAMES),
            stage_name=DAILY_STAGE_NAMES[-1],
            status="failed" if final_status in {"failed", "blocked_required_work"} else "completed",
            started_at=_now(),
            completed_at=_now(),
            elapsed_ms=0,
            max_items=item_limit,
            result={
                "processed": 1,
                "terminal_status": final_status,
                "operational_stage_receipts": [item["receipt_sha256"] for item in stage_receipts],
                "required_stage_truth": stage_truth,
                "scale_gate": scale_gate,
            },
        )
        final_stage = _store_stage_receipt(conn, final_stage)
    _ensure_stage_receipt_file(artifact_dir, final_stage)
    all_stage_receipts = [*stage_receipts, final_stage]
    final_base = {
        "schema_version": "pif_daily_receipt_v1",
        "run_id": run_id,
        "idempotency_key": effective_key,
        "run_date": effective_date,
        "status": final_status,
        "started_at": run_row["started_at"],
        "completed_at": _now(),
        "limits": {"max_runtime_seconds": runtime, "max_items_per_stage": item_limit},
        "stage_order": list(DAILY_STAGE_NAMES),
        "stage_receipts": [
            {
                "stage_index": item["stage_index"],
                "stage_name": item["stage_name"],
                "status": item["status"],
                "receipt_sha256": item["receipt_sha256"],
            }
            for item in all_stage_receipts
        ],
        "terminal_error": terminal_error,
        "required_stage_truth": stage_truth,
        "scale_gate": scale_gate,
        "local_source_of_truth": True,
        "managed_app_server_boundary": True,
        "headless_exception_dispatch_only": True,
        "external_launch_attempted": False,
        "self_resuming_chats": False,
    }
    final_receipt = {**final_base, "receipt_sha256": sha256_text(dumps_json(final_base))}
    final_path = artifact_dir / "daily-receipt.json"
    conn.execute(
        """
        UPDATE pif_daily_runs
        SET status = ?, completed_at = ?, receipt_path = ?, receipt_sha256 = ?, receipt_json = ?,
            lease_owner = NULL, leased_until = NULL
        WHERE id = ? AND lease_owner = ?
        """,
        (
            final_status,
            final_receipt["completed_at"],
            str(final_path),
            final_receipt["receipt_sha256"],
            dumps_json(final_receipt),
            run_id,
            owner,
        ),
    )
    conn.commit()
    _write_immutable_json(final_path, final_receipt)
    return {
        "ok": final_status not in {"failed", "blocked_required_work"},
        "status": final_status,
        "run_id": run_id,
        "idempotency_key": effective_key,
        "idempotent_replay": False,
        "receipt_path": str(final_path),
        "receipt": final_receipt,
    }


def _default_stage_handlers(
    conn: sqlite3.Connection,
    *,
    source_list: str | Path | None,
    since: str | None,
    execute_ingestion: bool,
    execute_normalize: bool,
    execute_extraction: bool,
    apply_reconcile: bool,
    record_exception_contracts: bool,
    publish_observer: bool,
    snapshot_output: str | Path | None,
    observer_url: str | None,
    observer_token: str | None,
    lane: str,
    label_pack: str,
    model: str,
    pilot_id: str | None,
    now: Callable[[], str] = now_iso,
) -> dict[str, StageHandler]:
    def backup_health(context: DailyStageContext) -> Mapping[str, Any]:
        context.artifact_dir.mkdir(parents=True, exist_ok=True)
        quick_check = [str(row[0]) for row in conn.execute("PRAGMA quick_check").fetchall()]
        fk_rows = conn.execute("PRAGMA foreign_key_check").fetchmany(context.max_items + 1)
        checkpoint = _checkpoint_authority(
            conn,
            receipt_root=context.artifact_dir.parents[1],
            run_date=context.run_date,
            retention=AUTHORITY_CHECKPOINT_RETENTION,
        )
        recovery = _recover_expired_queue_state(
            conn,
            limit=context.max_items,
        )
        healthy = (
            quick_check == ["ok"]
            and not fk_rows
            and checkpoint["verified"]
        )
        return {
            "status": "completed" if healthy else "failed",
            "processed": recovery["recovered_total"],
            "work_due": True,
            "required_work_enabled": True,
            "healthy_no_work": False,
            "work_satisfied": healthy,
            "quick_check": quick_check[:5],
            "foreign_key_issue_count": len(fk_rows),
            "foreign_key_results_truncated": len(fk_rows) > context.max_items,
            "authority_checkpoint": checkpoint,
            "lease_recovery": recovery,
        }

    def rss_and_strategies(context: DailyStageContext) -> Mapping[str, Any]:
        from .ingest import enqueue_sources, enqueue_transcript_backlog
        from .transcript_strategies import transcript_strategy_report

        strategy = transcript_strategy_report(conn)
        if not execute_ingestion:
            return {
                "status": "skipped",
                "processed": 0,
                "reason": "ingestion_not_explicitly_enabled",
                "work_due": True,
                "required_work_enabled": False,
                "healthy_no_work": False,
                "work_satisfied": False,
                "strategy_count": strategy.get("strategy_count", 0),
                "strategy_totals": strategy.get("totals", {}),
            }
        if not source_list:
            raise ValueError("source_list is required when ingestion is enabled")
        ingestion = enqueue_sources(
            conn,
            source_list,
            lane=lane,
            since=since,
            label_pack=label_pack,
            max_items=context.max_items,
            enqueue_transcripts=True,
            fetch_concurrency=min(4, context.max_items),
        )
        processed = min(context.max_items, int(ingestion.get("episodes", 0)))
        remaining = context.max_items - processed
        backlog: Mapping[str, Any] = {"selected": 0, "enqueued": 0}
        if remaining:
            backlog = enqueue_transcript_backlog(
                conn,
                lane=lane,
                label_pack=label_pack,
                limit=remaining,
            )
        return {
            "status": "completed",
            "processed": processed + int(backlog.get("selected", 0)),
            "work_due": True,
            "required_work_enabled": True,
            "healthy_no_work": False,
            "work_satisfied": not int(ingestion.get("source_errors", 0)),
            "ingestion": {
                "sources": ingestion.get("sources", 0),
                "episodes": ingestion.get("episodes", 0),
                "episodes_inserted": ingestion.get("episodes_inserted", 0),
                "source_errors": ingestion.get("source_errors", 0),
            },
            "due_transcript_strategies": {
                "selected": backlog.get("selected", 0),
                "enqueued": backlog.get("enqueued", 0),
            },
        }

    def normalize(context: DailyStageContext) -> Mapping[str, Any]:
        pending = _job_count(conn, ("prepare_transcript",))
        if not execute_normalize:
            return {
                "status": "skipped",
                "processed": 0,
                "reason": "normalization_not_explicitly_enabled",
                "pending": pending,
                "work_due": pending > 0,
                "required_work_enabled": False,
                "healthy_no_work": pending == 0,
                "work_satisfied": pending == 0,
            }
        from .worker import run_jobs

        result = run_jobs(
            conn,
            lane=lane,
            limit=context.max_items,
            model=model,
            label_pack=label_pack,
            worker_id=f"pif-daily-normalize-{context.run_date}",
            claim_prompts=False,
            job_types=("prepare_transcript",),
            max_label_prompts=0,
        )
        return {
            "status": "completed" if not result.get("failed") else "failed",
            "processed": int(result.get("processed", 0)),
            "work_due": pending > 0,
            "required_work_enabled": True,
            "healthy_no_work": pending == 0,
            "work_satisfied": not result.get("failed") and (
                pending == 0 or int(result.get("processed", 0)) > 0
            ),
            "completed": int(result.get("completed", 0)),
            "failed": int(result.get("failed", 0)),
        }

    def bounded_baseline(context: DailyStageContext) -> Mapping[str, Any]:
        job_types = ("episode_context", "label_segment")
        backlog_total = _job_count(conn, job_types)
        recent_since = _recent_job_window_start(
            conn,
            current_run_id=context.run_id,
            at=now(),
        )
        recent_pending_before = _job_count_recent(conn, job_types, recent_since)
        planned = min(recent_pending_before, context.max_items)
        if not execute_extraction:
            return {
                "status": "skipped",
                "processed": 0,
                "reason": "managed_app_server_dispatch_required",
                "backlog_total": backlog_total,
                "pending": recent_pending_before,
                "recent_pending": recent_pending_before,
                "recent_window_since": recent_since,
                "planned_items": planned,
                "work_due": recent_pending_before > 0,
                "required_work_enabled": False,
                "healthy_no_work": recent_pending_before == 0,
                "work_satisfied": recent_pending_before == 0,
                "dispatch_boundary": "managed_auth_app_server_local_sqlite_queue",
                "dispatch_contracts": [],
                "headless_exception_dispatch_created": False,
                "external_launch_attempted": False,
                "self_resuming_chats": False,
            }

        from . import headless_codex
        from .worker import run_jobs

        lease_owner = f"pif-daily-extraction-{context.run_date}-{context.run_id}"
        worker_result = run_jobs(
            conn,
            lane=lane,
            limit=context.max_items,
            model=model,
            label_pack=label_pack,
            worker_id=lease_owner,
            claim_prompts=True,
            job_types=job_types,
            max_label_prompts=context.max_items,
        )
        claimed_prompts = min(
            context.max_items,
            int(worker_result.get("claimed_prompts", 0) or 0),
        )
        concurrency = 3
        waves = max(1, (claimed_prompts + concurrency - 1) // concurrency)
        timeout_seconds = max(
            1,
            min(900, int(max(1.0, context.remaining_seconds) / waves)),
        )
        headless_result = headless_codex.execute_claimed_label_runs(
            conn,
            lease_owner=lease_owner,
            limit=claimed_prompts,
            model=model,
            timeout_seconds=timeout_seconds,
            audit=True,
            concurrency=concurrency,
        )
        recent_pending_after = _job_count_recent(conn, job_types, recent_since)
        worker_failed = int(worker_result.get("failed", 0) or 0)
        headless_failed = int(headless_result.get("failed", 0) or 0)
        work_satisfied = (
            recent_pending_after == 0
            and worker_failed == 0
            and headless_failed == 0
        )
        return {
            "status": "completed" if worker_failed == 0 and headless_failed == 0 else "failed",
            # Headless execution is the second phase of the same claimed queue
            # items, so it is not added again to this unique-item count.
            "processed": int(worker_result.get("processed", 0) or 0),
            "reason": None,
            "backlog_total": backlog_total,
            "backlog_total_after": _job_count(conn, job_types),
            "pending": recent_pending_before,
            "recent_pending": recent_pending_before,
            "recent_pending_after": recent_pending_after,
            "recent_window_since": recent_since,
            "planned_items": planned,
            "work_due": recent_pending_before > 0,
            "required_work_enabled": True,
            "healthy_no_work": recent_pending_before == 0,
            "work_satisfied": work_satisfied,
            "worker_result": worker_result,
            "headless_result": headless_result,
            "dispatch_boundary": "managed_auth_app_server_local_sqlite_queue",
            "dispatch_contracts": [],
            "headless_exception_dispatch_created": False,
            "external_launch_attempted": bool(headless_result.get("selected", 0)),
            "self_resuming_chats": False,
        }

    def validate(context: DailyStageContext) -> Mapping[str, Any]:
        validation = _validate_current_accepted_release(conn, at=_now())
        return {
            "status": "completed" if validation["ok"] else "failed",
            # This is one deterministic corpus-wide validation pass.  Claims
            # checked is deliberately not used as the queue item count.
            "processed": 1,
            "work_due": True,
            "required_work_enabled": True,
            "healthy_no_work": False,
            "work_satisfied": bool(validation["ok"]),
            **validation,
        }

    def reconcile(context: DailyStageContext) -> Mapping[str, Any]:
        from .production_ops import reconcile_all

        result = reconcile_all(
            conn,
            apply=apply_reconcile,
            release_id=None,
            model=model,
            limit=context.max_items,
        )
        planned = sum(
            int(item.get("planned_items", 0) or 0)
            for item in result.get("targets", [])
            if isinstance(item, Mapping)
        )
        work_due = planned > 0 or int(result.get("processed", 0) or 0) > 0
        # The daily orchestrator may prepare bounded packets, but a packet is
        # not a completed LLM judgment.  An external managed-auth worker must
        # finish due reconciliation before this stage is healthy.
        work_satisfied = not work_due
        return {
            "status": "completed" if result.get("ok") and work_satisfied else "skipped",
            "processed": int(result.get("processed", 0)),
            "reason": None if work_satisfied else (
                "managed_app_server_reconciliation_required"
                if apply_reconcile
                else "reconciliation_not_explicitly_enabled"
            ),
            "planned_items": planned,
            "work_due": work_due,
            "required_work_enabled": bool(apply_reconcile),
            "healthy_no_work": not work_due,
            "work_satisfied": work_satisfied,
            "result": result,
            "model_execution_attempted": False,
        }

    def outcomes(context: DailyStageContext) -> Mapping[str, Any]:
        from .production_ops import plan_due_outcome_dispatches

        result = plan_due_outcome_dispatches(
            conn,
            limit=context.max_items,
            max_runtime_ms=min(MAX_EXCEPTION_RUNTIME_MS, max(1, int(context.remaining_seconds * 1_000))),
            record=record_exception_contracts,
        )
        return {
            "status": "skipped" if result.get("due") else "completed",
            "processed": 0,
            "due": result.get("due", 0),
            "work_due": bool(result.get("due")),
            "required_work_enabled": False,
            "healthy_no_work": not bool(result.get("due")),
            "work_satisfied": not bool(result.get("due")),
            "dispatch_contracts": result.get("dispatch_contracts", []),
            "dispatch_state": result.get("dispatch_state"),
            "external_launch_attempted": False,
        }

    def intelligence_refresh(context: DailyStageContext) -> Mapping[str, Any]:
        from .production_ops import pif_status

        status = pif_status(conn)
        output = context.artifact_dir / "local-intelligence.json"
        summary = {
            "schema_version": "pif_local_intelligence_v1",
            "generated_at": now_iso(),
            "counts": status["counts"],
            "jobs": status["jobs"],
            "operations": status["operations"],
            "privacy": "local_derived_intelligence_no_raw_transcripts",
        }
        write_text_atomic(output, json.dumps(summary, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
        return {
            "status": "completed",
            "processed": 1,
            "work_due": True,
            "required_work_enabled": True,
            "healthy_no_work": False,
            "work_satisfied": True,
            "path": str(output),
        }

    def observer_publish(context: DailyStageContext) -> Mapping[str, Any]:
        from .production_ops import publish_ops

        output = Path(snapshot_output).expanduser().resolve() if snapshot_output else context.artifact_dir / "observer-snapshot.json"
        result = publish_ops(
            conn,
            output=output,
            publish=publish_observer,
            observer_url=observer_url,
            token=observer_token,
        )
        return {
            "status": "completed" if result.get("ok") else "failed",
            "processed": 1,
            "work_due": True,
            "required_work_enabled": True,
            "healthy_no_work": False,
            "work_satisfied": bool(result.get("ok")),
            **result,
        }

    return {
        "backup_health": backup_health,
        "rss_ingestion_and_due_transcript_strategies": rss_and_strategies,
        "normalize": normalize,
        "bounded_baseline_extraction": bounded_baseline,
        "evidence_schema_privacy_validation": validate,
        "identity_and_semantic_reconciliation": reconcile,
        "due_outcomes": outcomes,
        "local_intelligence_refresh": intelligence_refresh,
        "sanitized_observer_publish": observer_publish,
    }


def _skipped_handler(context: DailyStageContext) -> Mapping[str, Any]:
    return {"status": "skipped", "processed": 0, "reason": "handler_not_configured"}


def _job_count(conn: sqlite3.Connection, job_types: tuple[str, ...]) -> int:
    placeholders = ",".join("?" for _ in job_types)
    row = conn.execute(
        f"SELECT COUNT(*) FROM jobs WHERE status = 'pending' AND job_type IN ({placeholders})",
        job_types,
    ).fetchone()
    return int(row[0]) if row else 0


def _job_count_recent(
    conn: sqlite3.Connection,
    job_types: tuple[str, ...],
    since: str,
) -> int:
    placeholders = ",".join("?" for _ in job_types)
    row = conn.execute(
        f"""
        SELECT COUNT(*) FROM jobs
        WHERE status = 'pending'
          AND job_type IN ({placeholders})
          AND created_at >= ?
        """,
        (*job_types, since),
    ).fetchone()
    return int(row[0]) if row else 0


def _recent_job_window_start(
    conn: sqlite3.Connection,
    *,
    current_run_id: str,
    at: str,
) -> str:
    row = conn.execute(
        """
        SELECT completed_at
        FROM pif_daily_runs
        WHERE id != ?
          AND status IN ('completed', 'completed_with_skips')
          AND completed_at IS NOT NULL
        ORDER BY completed_at DESC, id DESC
        LIMIT 1
        """,
        (current_run_id,),
    ).fetchone()
    if row and row["completed_at"]:
        return str(row["completed_at"])
    parsed = dt.datetime.fromisoformat(at.replace("Z", "+00:00"))
    return (parsed - dt.timedelta(hours=24)).replace(microsecond=0).isoformat()


def _assess_required_stage_truth(
    stage_receipts: list[Mapping[str, Any]],
    *,
    conn: sqlite3.Connection | None = None,
) -> dict[str, Any]:
    """Separate a proven empty queue from skipped or no-op required work."""

    blockers: list[dict[str, Any]] = []
    healthy_no_work: list[str] = []
    for receipt in stage_receipts:
        stage_name = str(receipt.get("stage_name") or "unknown")
        status = str(receipt.get("status") or "failed")
        result = receipt.get("result")
        result = result if isinstance(result, Mapping) else {}
        declared_work_due = "work_due" in result
        work_due = bool(result.get("work_due")) or any(
            int(result.get(field, 0) or 0) > 0 for field in ("pending", "due", "planned_items")
        )
        if conn is not None and not work_due and not declared_work_due:
            if stage_name == "normalize":
                work_due = _job_count(conn, ("prepare_transcript",)) > 0
            elif stage_name == "bounded_baseline_extraction":
                work_due = _job_count(conn, ("episode_context", "label_segment")) > 0
        no_work = bool(result.get("healthy_no_work")) and not work_due
        work_satisfied = result.get("work_satisfied")
        if status == "failed":
            blockers.append({"stage": stage_name, "reason": "stage_failed"})
        elif status == "skipped":
            if no_work:
                healthy_no_work.append(stage_name)
            else:
                blockers.append(
                    {
                        "stage": stage_name,
                        "reason": (
                            "required_work_disabled_or_deferred"
                            if work_due
                            else "skip_without_healthy_no_work_proof"
                        ),
                        "work_due": work_due,
                    }
                )
        elif work_due and (
            work_satisfied is False or (work_satisfied is None and int(result.get("processed", 0) or 0) == 0)
        ):
            blockers.append(
                {
                    "stage": stage_name,
                    "reason": "required_work_noop",
                    "work_due": True,
                }
            )
    return {
        "ok": not blockers,
        "blockers": blockers,
        "healthy_no_work_stages": healthy_no_work,
        "required_stage_count": len(stage_receipts),
    }


def _validate_current_accepted_release(
    conn: sqlite3.Connection,
    *,
    at: str | None = None,
) -> dict[str, Any]:
    """Validate every accepted claim and its private-local evidence lineage.

    The validation intentionally has no item limit.  A release cannot be called
    healthy on the strength of a sample while a later accepted row is corrupt.
    Receipts contain identifiers and issue codes only, never claim/evidence text.
    """

    timestamp = at or now_iso()
    issues: list[dict[str, Any]] = []
    required_relations = {
        "current_accepted_corpus_releases",
        "current_accepted_atomic_claims",
        "current_accepted_pipeline_stage_runs",
        "corpus_release_episodes",
        "corpus_release_transcripts",
        "corpus_release_segments",
        "corpus_release_labels",
        "pipeline_runs",
        "segments",
        "discourse_events",
    }
    missing = sorted(name for name in required_relations if not _relation_exists(conn, name))
    if missing:
        return {
            "ok": False,
            "error": "accepted_release_schema_unavailable",
            "current_release_id": None,
            "claims_checked": 0,
            "issues": [{"issue": "missing_relation", "name": name} for name in missing],
            "lineage": {"ok": False, "claims_checked": 0, "issue_count": len(missing)},
            "privacy": {"ok": False, "reason": "release_schema_unavailable"},
            "queue": _queue_and_lease_health(conn, at=timestamp),
        }

    release_rows = conn.execute(
        "SELECT id, item_count, source_count, manifest_sha256 FROM current_accepted_corpus_releases"
    ).fetchall()
    if len(release_rows) != 1:
        issues.append(
            {
                "issue": "current_accepted_release_count",
                "observed": len(release_rows),
                "expected": 1,
            }
        )
    release_id = str(release_rows[0]["id"]) if len(release_rows) == 1 else None
    rows: list[sqlite3.Row] = []
    if release_id:
        rows = conn.execute(
            """
            SELECT
              claim.id,
              claim.schema_version,
              claim.corpus_release_id,
              claim.pipeline_run_id,
              claim.discourse_event_id,
              claim.segment_id,
              claim.source_id,
              claim.episode_id,
              claim.evidence_unit_type,
              claim.evidence_unit_id,
              claim.evidence_text,
              claim.evidence_start,
              claim.evidence_end,
              claim.extractor_model,
              claim.extractor_schema,
              claim.extractor_schema_version,
              claim.source_artifact_sha256,
              claim.provenance_json,
              segment.transcript_id,
              segment.episode_id AS segment_episode_id,
              segment.source_id AS segment_source_id,
              segment.text_path,
              segment.text_sha256,
              release_segment.content_sha256 AS release_segment_sha256,
              release_segment.transcript_id AS release_segment_transcript_id,
              release_segment.episode_id AS release_segment_episode_id,
              release_transcript.transcript_id AS release_transcript_id,
              release_transcript.episode_id AS release_transcript_episode_id,
              release_transcript.content_sha256 AS release_transcript_sha256,
              release_episode.episode_id AS release_episode_id,
              release_episode.content_sha256 AS release_episode_sha256,
              event.label_id AS event_label_id,
              event.segment_id AS event_segment_id,
              event.evidence_text AS event_evidence_text,
              event.evidence_start AS event_evidence_start,
              event.evidence_end AS event_evidence_end,
              release_label.label_id AS release_label_id,
              release_label.content_sha256 AS release_label_sha256,
              producing_run.status AS producing_run_status,
              producing_run.corpus_release_id AS producing_run_release_id,
              producing_run.accepted_stage AS producing_run_accepted_stage
            FROM current_accepted_atomic_claims AS claim
            JOIN current_accepted_corpus_releases AS release
              ON release.id = claim.corpus_release_id
             AND release.id = ?
            LEFT JOIN segments AS segment ON segment.id = claim.segment_id
            LEFT JOIN corpus_release_segments AS release_segment
              ON release_segment.corpus_release_id = claim.corpus_release_id
             AND release_segment.segment_id = claim.segment_id
            LEFT JOIN corpus_release_transcripts AS release_transcript
              ON release_transcript.corpus_release_id = claim.corpus_release_id
             AND release_transcript.transcript_id = segment.transcript_id
            LEFT JOIN corpus_release_episodes AS release_episode
              ON release_episode.corpus_release_id = claim.corpus_release_id
             AND release_episode.episode_id = segment.episode_id
            LEFT JOIN discourse_events AS event ON event.id = claim.discourse_event_id
            LEFT JOIN corpus_release_labels AS release_label
              ON release_label.corpus_release_id = claim.corpus_release_id
             AND release_label.label_id = event.label_id
            LEFT JOIN current_accepted_pipeline_stage_runs AS producing_run
              ON producing_run.id = claim.pipeline_run_id
             AND producing_run.accepted_stage = 'atomic_claims'
            ORDER BY claim.id
            """,
            (release_id,),
        ).fetchall()
        if not rows:
            issues.append({"issue": "no_current_accepted_atomic_claims"})

    verified_paths: dict[str, tuple[str, str | None]] = {}
    verified_members: dict[tuple[str, str], str | None] = {}
    for row in rows:
        claim_id = str(row["id"])

        def add(issue: str) -> None:
            issues.append({"claim_id": claim_id, "issue": issue})

        if row["schema_version"] != "atomic_claim_v1":
            add("invalid_atomic_claim_schema")
        if row["corpus_release_id"] != release_id:
            add("claim_release_mismatch")
        if row["producing_run_status"] != "succeeded":
            add("producing_run_not_succeeded")
        if row["producing_run_release_id"] != release_id:
            add("producing_run_release_mismatch")
        if row["producing_run_accepted_stage"] != "atomic_claims":
            add("producing_run_not_accepted_atomic_stage")
        if not row["text_path"]:
            add("segment_lineage_missing")
            continue
        if row["event_segment_id"] != row["segment_id"]:
            add("discourse_event_segment_mismatch")
        if not row["release_label_id"]:
            add("event_label_not_in_release")
        if row["evidence_unit_type"] != "segment" or row["evidence_unit_id"] != row["segment_id"]:
            add("evidence_unit_mismatch")
        if row["segment_episode_id"] != row["episode_id"]:
            add("episode_lineage_mismatch")
        if row["segment_source_id"] != row["source_id"]:
            add("source_lineage_mismatch")
        if row["release_segment_transcript_id"] != row["transcript_id"]:
            add("release_segment_transcript_mismatch")
        if row["release_segment_episode_id"] != row["episode_id"]:
            add("release_segment_episode_mismatch")
        if row["release_transcript_id"] != row["transcript_id"]:
            add("transcript_not_in_current_release")
        if row["release_transcript_episode_id"] != row["episode_id"]:
            add("release_transcript_episode_mismatch")
        if row["release_episode_id"] != row["episode_id"]:
            add("episode_not_in_current_release")
        if any(not str(row[field] or "").strip() for field in (
            "extractor_model",
            "extractor_schema",
            "extractor_schema_version",
        )):
            add("extractor_lineage_missing")

        try:
            provenance = json.loads(str(row["provenance_json"] or "{}"))
        except json.JSONDecodeError:
            provenance = None
        if not isinstance(provenance, dict):
            add("invalid_provenance_json")
        else:
            if provenance.get("label_id") != row["event_label_id"]:
                add("provenance_label_mismatch")
            if not str(provenance.get("projection") or "").strip():
                add("projection_lineage_missing")

        path = Path(str(row["text_path"])).expanduser()
        if not path.is_absolute():
            path = (root() / path).resolve()
        path_key = str(path)
        expected_segment_sha = str(row["text_sha256"] or "")
        expected_release_sha = str(row["release_segment_sha256"] or "")
        if path_key not in verified_paths:
            if not path.is_file():
                verified_paths[path_key] = ("", "missing")
            else:
                try:
                    verified_paths[path_key] = (_sha256_file(path), None)
                except OSError:
                    verified_paths[path_key] = ("", "unreadable")
        actual_sha, path_error = verified_paths[path_key]
        if path_error:
            add(f"segment_file_{path_error}")
            continue
        if actual_sha != expected_segment_sha:
            add("segment_file_hash_mismatch")
        if str(row["source_artifact_sha256"] or "") != expected_release_sha:
            add("claim_source_artifact_hash_mismatch")

        member_specs = (
            ("segments", str(row["segment_id"]), expected_release_sha, "release_segment_hash_mismatch"),
            (
                "transcripts",
                str(row["transcript_id"]),
                str(row["release_transcript_sha256"] or ""),
                "release_transcript_hash_mismatch",
            ),
            (
                "episodes",
                str(row["episode_id"]),
                str(row["release_episode_sha256"] or ""),
                "release_episode_hash_mismatch",
            ),
            (
                "labels",
                str(row["event_label_id"] or ""),
                str(row["release_label_sha256"] or ""),
                "release_label_hash_mismatch",
            ),
        )
        for table, record_id, expected_member_sha, issue_code in member_specs:
            member_key = (table, record_id)
            if member_key not in verified_members:
                verified_members[member_key] = _release_member_content_sha(
                    conn, table=table, record_id=record_id
                )
            if verified_members[member_key] != expected_member_sha:
                add(issue_code)

        try:
            text = path.read_text(encoding="utf-8")
            start = int(row["evidence_start"])
            end = int(row["evidence_end"])
            evidence = str(row["evidence_text"])
            if start < 0 or end <= start or end > len(text):
                add("evidence_offsets_out_of_bounds")
            elif text[start:end] != evidence:
                add("evidence_offset_text_mismatch")
            if (
                row["event_evidence_text"] != row["evidence_text"]
                or row["event_evidence_start"] != row["evidence_start"]
                or row["event_evidence_end"] != row["evidence_end"]
            ):
                add("discourse_event_evidence_mismatch")
        except (OSError, TypeError, ValueError):
            add("evidence_validation_error")

    privacy = _observer_privacy_health(conn)
    queue = _queue_and_lease_health(conn, at=timestamp)
    if not privacy["ok"]:
        issues.append({"issue": "observer_privacy_contract_failed"})
    if not queue["ok"]:
        issues.append({"issue": "queue_or_lease_health_failed"})
    return {
        "ok": not issues,
        "current_release_id": release_id,
        "claims_checked": len(rows),
        "segment_files_checked": len(verified_paths),
        "release_members_checked": len(verified_members),
        "issues": issues,
        "lineage": {
            "ok": not any("privacy" not in item["issue"] and "queue" not in item["issue"] for item in issues),
            "claims_checked": len(rows),
            "issue_count": sum(
                1 for item in issues if "privacy" not in item["issue"] and "queue" not in item["issue"]
            ),
            "exact_offsets_checked_for_every_current_claim": bool(rows),
            "segment_hash_checked_for_every_current_claim": bool(rows),
        },
        "privacy": privacy,
        "queue": queue,
    }


def _observer_privacy_health(conn: sqlite3.Connection) -> dict[str, Any]:
    try:
        from .observer import build_snapshot

        snapshot = build_snapshot(conn)
    except Exception as exc:
        return {
            "ok": False,
            "contract": None,
            "forbidden_keys": [],
            "error_class": type(exc).__name__,
        }
    forbidden = {
        "raw_text",
        "transcript_text",
        "claim_text",
        "evidence_text",
        "output_json",
        "prompt",
        "speaker",
        "speaker_name",
        "person_name",
        "network_name",
    }
    found: set[str] = set()

    def walk(value: Any) -> None:
        if isinstance(value, Mapping):
            for key, item in value.items():
                if str(key).lower() in forbidden:
                    found.add(str(key))
                walk(item)
        elif isinstance(value, list):
            for item in value:
                walk(item)

    walk(snapshot)
    contract = str(snapshot.get("contract_version") or "")
    privacy = str(snapshot.get("privacy") or "")
    return {
        "ok": (
            contract == "railway-operational-v2"
            and privacy == "sanitized_operational_snapshot_no_raw_transcripts"
            and not found
        ),
        "contract": contract,
        "privacy": privacy,
        "forbidden_keys": sorted(found),
        "claim_or_identity_content_exposed": bool(found),
    }


def _queue_and_lease_health(conn: sqlite3.Connection, *, at: str) -> dict[str, Any]:
    expired_claims = int(
        conn.execute(
            """
            SELECT COUNT(*) FROM jobs
            WHERE status = 'claimed'
              AND (leased_until IS NULL OR leased_until < ?)
            """,
            (at,),
        ).fetchone()[0]
    )
    pending = int(conn.execute("SELECT COUNT(*) FROM jobs WHERE status = 'pending'").fetchone()[0])
    claimed = int(conn.execute("SELECT COUNT(*) FROM jobs WHERE status = 'claimed'").fetchone()[0])
    zombie_workers = 0
    if _table_exists(conn, "worker_runs"):
        parsed = dt.datetime.fromisoformat(at.replace("Z", "+00:00"))
        cutoff = (parsed - dt.timedelta(seconds=ZOMBIE_WORKER_GRACE_SECONDS)).replace(
            microsecond=0
        ).isoformat()
        zombie_workers = int(
            conn.execute(
                """
                SELECT COUNT(*) FROM worker_runs AS runs
                WHERE runs.status = 'running'
                  AND runs.updated_at < ?
                  AND NOT EXISTS (
                    SELECT 1 FROM jobs
                    WHERE jobs.status = 'claimed'
                      AND jobs.lease_owner = runs.worker_id
                      AND jobs.leased_until IS NOT NULL
                      AND jobs.leased_until >= ?
                  )
                """,
                (cutoff, at),
            ).fetchone()[0]
        )
    orphan_envelopes = 0
    if _table_exists(conn, "queue_envelopes"):
        orphan_envelopes = int(
            conn.execute(
                """
                SELECT COUNT(*) FROM queue_envelopes AS envelope
                LEFT JOIN jobs ON jobs.id = envelope.job_id
                WHERE jobs.id IS NULL
                """
            ).fetchone()[0]
        )
    return {
        "ok": expired_claims == 0 and zombie_workers == 0 and orphan_envelopes == 0,
        "pending_jobs": pending,
        "claimed_jobs": claimed,
        "expired_or_missing_leases": expired_claims,
        "zombie_worker_runs": zombie_workers,
        "orphan_queue_envelopes": orphan_envelopes,
    }


def _relation_exists(conn: sqlite3.Connection, name: str) -> bool:
    return bool(
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name = ?",
            (name,),
        ).fetchone()
    )


def _release_member_content_sha(
    conn: sqlite3.Connection,
    *,
    table: str,
    record_id: str,
) -> str | None:
    if table not in {"episodes", "transcripts", "segments", "labels"} or not record_id:
        return None
    row = conn.execute(f"SELECT * FROM {table} WHERE id = ?", (record_id,)).fetchone()
    if row is None:
        return None
    content = {
        key: row[key]
        for key in row.keys()
        if key not in {"created_at", "updated_at", "fetched_at"}
    }
    return sha256_text(dumps_json(content))


def _record_scale_gate_state_receipt(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    run_date: str,
    stage_receipts: list[Mapping[str, Any]],
    stage_truth: Mapping[str, Any],
    cycle_status: str,
    elapsed_seconds: float,
    max_runtime_seconds: int,
    max_items: int,
    created_at: str,
) -> dict[str, Any]:
    """Append one immutable scale-gate state receipt for this daily run."""

    ensure_daily_schema(conn)
    existing = conn.execute(
        "SELECT receipt_json FROM pif_scale_gate_state_receipts WHERE daily_run_id = ?",
        (run_id,),
    ).fetchone()
    if existing:
        receipt = loads_json(existing["receipt_json"], {})
        if not receipt:
            raise RuntimeError("stored scale-gate receipt is invalid")
        return receipt

    release = _current_release_for_scale_gate(conn)
    release_id = str(release["id"]) if release else None
    item_count = int(release["item_count"]) if release else 0
    tier, next_tier = _scale_tier(item_count)
    validation_result = _stage_result(stage_receipts, "evidence_schema_privacy_validation")
    lineage_result = validation_result.get("lineage")
    lineage_result = lineage_result if isinstance(lineage_result, Mapping) else {}
    privacy_result = validation_result.get("privacy")
    privacy_result = privacy_result if isinstance(privacy_result, Mapping) else {}
    queue_result = validation_result.get("queue")
    queue_result = queue_result if isinstance(queue_result, Mapping) else _queue_and_lease_health(
        conn, at=created_at
    )

    quality = _quality_gate(conn, release_id=release_id, cohort_item_count=item_count)
    lineage = {
        "passed": bool(lineage_result.get("ok"))
        and int(lineage_result.get("claims_checked", 0) or 0) > 0,
        "claims_checked": int(lineage_result.get("claims_checked", 0) or 0),
        "issue_count": int(lineage_result.get("issue_count", 0) or 0),
        "exact_offsets_all_current_claims": bool(
            lineage_result.get("exact_offsets_checked_for_every_current_claim")
        ),
        "segment_hashes_all_current_claims": bool(
            lineage_result.get("segment_hash_checked_for_every_current_claim")
        ),
    }
    privacy = {
        "passed": bool(privacy_result.get("ok")),
        "contract": privacy_result.get("contract"),
        "forbidden_keys": list(privacy_result.get("forbidden_keys") or []),
        "railway_operational_only": privacy_result.get("contract") == "railway-operational-v2",
    }
    prior_pending = _prior_scale_gate_pending_jobs(conn, release_id=release_id, tier=tier)
    pending = int(queue_result.get("pending_jobs", 0) or 0)
    growth = None if prior_pending is None else pending - prior_pending
    growth_bounded = prior_pending is None or growth <= int(max_items)
    queue = {
        "passed": bool(queue_result.get("ok")) and growth_bounded,
        "pending_jobs": pending,
        "claimed_jobs": int(queue_result.get("claimed_jobs", 0) or 0),
        "expired_or_missing_leases": int(queue_result.get("expired_or_missing_leases", 0) or 0),
        "zombie_worker_runs": int(queue_result.get("zombie_worker_runs", 0) or 0),
        "orphan_queue_envelopes": int(queue_result.get("orphan_queue_envelopes", 0) or 0),
        "prior_pending_jobs": prior_pending,
        "pending_growth": growth,
        "growth_allowance": int(max_items),
        "growth_bounded": growth_bounded,
        "absolute_pending_is_pass_condition": False,
    }
    runtime = {
        "passed": elapsed_seconds <= float(max_runtime_seconds),
        "elapsed_seconds": round(float(elapsed_seconds), 3),
        "max_runtime_seconds": int(max_runtime_seconds),
        "stage_runtime_ms": sum(int(item.get("elapsed_ms", 0) or 0) for item in stage_receipts),
    }
    cost = _cost_gate(conn, release_id=release_id)
    release_gate = {
        "passed": release is not None and tier != "unrecognized",
        "corpus_release_id": release_id,
        "cohort_item_count": item_count,
        "cohort_tier": tier,
        "next_tier": next_tier,
        "accepted_release_required": True,
    }
    operations = {
        "passed": bool(stage_truth.get("ok"))
        and cycle_status in {"completed", "completed_with_skips"},
        "cycle_status": cycle_status,
        "required_work_blockers": list(stage_truth.get("blockers") or []),
        "healthy_no_work_stages": list(stage_truth.get("healthy_no_work_stages") or []),
    }
    gates = {
        "release": release_gate,
        "quality": quality,
        "privacy": privacy,
        "lineage": lineage,
        "queue": queue,
        "runtime": runtime,
        "cost": cost,
        "operations": operations,
    }
    genuinely_successful = all(bool(value.get("passed")) for value in gates.values())
    streak = _consecutive_success_days(
        conn,
        release_id=release_id,
        tier=tier,
        run_date=run_date,
        include_current=genuinely_successful,
    )
    eligible = bool(
        genuinely_successful
        and streak >= SCALE_GATE_REQUIRED_DAYS
        and next_tier is not None
    )
    receipt_id = stable_id("pif_scale_gate", run_id, prefix="psg_")
    base = {
        "schema_version": "pif_scale_gate_state_receipt_v1",
        "id": receipt_id,
        "daily_run_id": run_id,
        "run_date": run_date,
        "corpus_release_id": release_id,
        "cohort_tier": tier,
        "cohort_item_count": item_count,
        "next_tier": next_tier,
        "genuinely_successful": genuinely_successful,
        "consecutive_success_days": streak,
        "required_consecutive_days": SCALE_GATE_REQUIRED_DAYS,
        "promotion_eligible": eligible,
        "gates": gates,
        "action": "eligibility_receipt_only_no_scale_enqueue_or_publish",
        "created_at": created_at,
    }
    receipt = {**base, "receipt_sha256": sha256_text(dumps_json(base))}
    conn.execute(
        """
        INSERT INTO pif_scale_gate_state_receipts
          (id, daily_run_id, run_date, corpus_release_id, cohort_tier,
           cohort_item_count, next_tier, genuinely_successful,
           consecutive_success_days, promotion_eligible, gate_json,
           receipt_sha256, receipt_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            receipt_id,
            run_id,
            run_date,
            release_id,
            tier,
            item_count,
            next_tier,
            int(genuinely_successful),
            streak,
            int(eligible),
            dumps_json(gates),
            receipt["receipt_sha256"],
            dumps_json(receipt),
            created_at,
        ),
    )
    conn.commit()
    return receipt


def _current_release_for_scale_gate(conn: sqlite3.Connection) -> dict[str, Any] | None:
    if not _relation_exists(conn, "current_accepted_corpus_releases"):
        return None
    rows = conn.execute(
        "SELECT id, item_count, source_count, manifest_sha256 FROM current_accepted_corpus_releases"
    ).fetchall()
    return dict(rows[0]) if len(rows) == 1 else None


def _scale_tier(item_count: int) -> tuple[str, str | None]:
    if item_count == 25:
        return "25", "100"
    if item_count == 100:
        return "100", "500"
    if item_count == 500:
        return "500", "remaining"
    if item_count > 500:
        return "remaining", None
    return "unrecognized", None


def _stage_result(stage_receipts: list[Mapping[str, Any]], stage_name: str) -> Mapping[str, Any]:
    for receipt in stage_receipts:
        if receipt.get("stage_name") == stage_name and isinstance(receipt.get("result"), Mapping):
            return receipt["result"]
    return {}


def _quality_gate(
    conn: sqlite3.Connection,
    *,
    release_id: str | None,
    cohort_item_count: int,
) -> dict[str, Any]:
    counts = {
        "accepted_atomic_claims": _accepted_release_count(
            conn, "current_accepted_atomic_claims", release_id
        ),
        "accepted_claim_subjects": _accepted_release_count(
            conn, "current_accepted_claim_subjects", release_id
        ),
        "accepted_claim_relations": _accepted_release_count(
            conn, "current_accepted_claim_relations", release_id
        ),
        "accepted_outcome_resolutions": _accepted_release_count(
            conn, "current_accepted_outcome_resolutions", release_id
        ),
    }
    thresholds_met = {
        name: counts[name] >= minimum for name, minimum in SCALE_GATE_MINIMUMS.items()
    }
    return {
        "passed": bool(release_id)
        and (cohort_item_count in SCALE_GATE_TIERS or cohort_item_count > 500)
        and all(thresholds_met.values()),
        "counts": counts,
        "minimums": dict(SCALE_GATE_MINIMUMS),
        "thresholds_met": thresholds_met,
        "current_release_only": True,
    }


def _accepted_release_count(
    conn: sqlite3.Connection,
    relation: str,
    release_id: str | None,
) -> int:
    if not release_id or not _relation_exists(conn, relation):
        return 0
    try:
        return int(
            conn.execute(
                f"SELECT COUNT(*) FROM {relation} WHERE corpus_release_id = ?",
                (release_id,),
            ).fetchone()[0]
        )
    except sqlite3.OperationalError:
        return 0


def _cost_gate(conn: sqlite3.Connection, *, release_id: str | None) -> dict[str, Any]:
    if not release_id or not _table_exists(conn, "pipeline_runs"):
        return {
            "passed": False,
            "telemetry_mode": "unavailable",
            "succeeded_pipeline_runs": 0,
            "model_runs": 0,
            "reported_cost_usd": 0.0,
            "paid_api_billing_detected": False,
        }
    rows = conn.execute(
        """
        SELECT model, parameters_json, metrics_json, receipt_json
        FROM pipeline_runs
        WHERE corpus_release_id = ? AND status = 'succeeded'
        ORDER BY id
        """,
        (release_id,),
    ).fetchall()
    reported_cost = 0.0
    paid_api = False
    token_total = 0
    for row in rows:
        documents: list[Any] = []
        for field in ("parameters_json", "metrics_json", "receipt_json"):
            try:
                documents.append(json.loads(str(row[field] or "{}")))
            except json.JSONDecodeError:
                documents.append({})
        for document in documents:
            reported_cost += _sum_numeric_keys(document, {"cost_usd", "api_cost_usd", "estimated_cost_usd"})
            token_total += int(
                _sum_numeric_keys(
                    document,
                    {"input_tokens", "output_tokens", "total_tokens", "cached_input_tokens"},
                )
            )
            if _truthy_key(document, {"paid_api", "api_billing", "openai_api_billing"}):
                paid_api = True
    model_runs = sum(1 for row in rows if str(row["model"] or "").strip())
    return {
        "passed": bool(rows) and not paid_api,
        "telemetry_mode": "managed_auth_app_server_no_metered_api_billing",
        "succeeded_pipeline_runs": len(rows),
        "model_runs": model_runs,
        "reported_cost_usd": round(reported_cost, 6),
        "token_telemetry_total": token_total,
        "paid_api_billing_detected": paid_api,
        "cost_is_telemetry_not_release_quality_authority": True,
    }


def _sum_numeric_keys(value: Any, keys: set[str]) -> float:
    total = 0.0
    if isinstance(value, Mapping):
        for key, item in value.items():
            if str(key).lower() in keys and isinstance(item, (int, float)) and not isinstance(item, bool):
                total += float(item)
            else:
                total += _sum_numeric_keys(item, keys)
    elif isinstance(value, list):
        for item in value:
            total += _sum_numeric_keys(item, keys)
    return total


def _truthy_key(value: Any, keys: set[str]) -> bool:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if str(key).lower() in keys and item is True:
                return True
            if _truthy_key(item, keys):
                return True
    elif isinstance(value, list):
        return any(_truthy_key(item, keys) for item in value)
    return False


def _prior_scale_gate_pending_jobs(
    conn: sqlite3.Connection,
    *,
    release_id: str | None,
    tier: str,
) -> int | None:
    if not release_id:
        return None
    rows = conn.execute(
        """
        SELECT gate_json FROM pif_scale_gate_state_receipts
        WHERE corpus_release_id = ? AND cohort_tier = ?
          AND genuinely_successful = 1
        ORDER BY run_date DESC, created_at DESC, id DESC
        LIMIT 1
        """,
        (release_id, tier),
    ).fetchall()
    if not rows:
        return None
    gate = loads_json(rows[0]["gate_json"], {})
    queue = gate.get("queue") if isinstance(gate, Mapping) else None
    if not isinstance(queue, Mapping) or queue.get("pending_jobs") is None:
        return None
    return int(queue["pending_jobs"])


def _consecutive_success_days(
    conn: sqlite3.Connection,
    *,
    release_id: str | None,
    tier: str,
    run_date: str,
    include_current: bool,
) -> int:
    if not release_id or tier == "unrecognized" or not include_current:
        return 0
    dates = {
        str(row["run_date"])
        for row in conn.execute(
            """
            SELECT DISTINCT run_date FROM pif_scale_gate_state_receipts
            WHERE corpus_release_id = ? AND cohort_tier = ?
              AND genuinely_successful = 1 AND run_date <= ?
            """,
            (release_id, tier, run_date),
        ).fetchall()
    }
    dates.add(run_date)
    cursor = dt.date.fromisoformat(run_date)
    streak = 0
    while cursor.isoformat() in dates:
        streak += 1
        cursor -= dt.timedelta(days=1)
    return streak


def _stage_receipt(
    *,
    run_id: str,
    stage_index: int,
    stage_name: str,
    status: str,
    started_at: str,
    completed_at: str,
    elapsed_ms: int,
    max_items: int,
    result: Mapping[str, Any],
) -> dict[str, Any]:
    base = {
        "schema_version": "pif_daily_stage_receipt_v1",
        "run_id": run_id,
        "stage_index": stage_index,
        "stage_name": stage_name,
        "status": status,
        "started_at": started_at,
        "completed_at": completed_at,
        "elapsed_ms": elapsed_ms,
        "max_items": max_items,
        "result": dict(result),
    }
    return {**base, "receipt_sha256": sha256_text(dumps_json(base))}


def _store_stage_receipt(conn: sqlite3.Connection, receipt: Mapping[str, Any]) -> dict[str, Any]:
    payload = dumps_json(dict(receipt))
    receipt_id = stable_id(receipt["run_id"], receipt["stage_name"], prefix="pds_")
    conn.execute(
        """
        INSERT OR IGNORE INTO pif_daily_stage_receipts
          (id, run_id, stage_index, stage_name, status, receipt_sha256, receipt_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            receipt_id,
            receipt["run_id"],
            receipt["stage_index"],
            receipt["stage_name"],
            receipt["status"],
            receipt["receipt_sha256"],
            payload,
            receipt["completed_at"],
        ),
    )
    conn.commit()
    stored = conn.execute(
        "SELECT receipt_json FROM pif_daily_stage_receipts WHERE run_id = ? AND stage_name = ?",
        (receipt["run_id"], receipt["stage_name"]),
    ).fetchone()
    if not stored:
        raise RuntimeError("stage receipt could not be persisted")
    value = loads_json(stored["receipt_json"], {})
    if value != dict(receipt):
        raise RuntimeError(f"immutable stage receipt drift for {receipt['stage_name']}")
    return value


def _load_stage_receipt(conn: sqlite3.Connection, run_id: str, stage_name: str) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT receipt_json FROM pif_daily_stage_receipts WHERE run_id = ? AND stage_name = ?",
        (run_id, stage_name),
    ).fetchone()
    return loads_json(row["receipt_json"], {}) if row else None


def _ensure_stage_receipt_file(artifact_dir: Path, receipt: Mapping[str, Any]) -> None:
    stage_dir = artifact_dir / "stages"
    filename = f"{int(receipt['stage_index']):02d}-{receipt['stage_name']}.json"
    _write_immutable_json(stage_dir / filename, dict(receipt))


def _write_immutable_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        existing = json.loads(path.read_text(encoding="utf-8"))
        if existing != dict(value):
            raise RuntimeError(f"immutable receipt drift: {path}")
        return
    text = json.dumps(dict(value), ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    try:
        with path.open("x", encoding="utf-8") as handle:
            handle.write(text)
    except FileExistsError:
        existing = json.loads(path.read_text(encoding="utf-8"))
        if existing != dict(value):
            raise RuntimeError(f"immutable receipt drift: {path}")


def _replay_daily_receipt(run_row: sqlite3.Row) -> dict[str, Any]:
    receipt = loads_json(run_row["receipt_json"], {})
    if not receipt:
        raise RuntimeError("completed daily run is missing its immutable receipt")
    if receipt.get("receipt_sha256") != run_row["receipt_sha256"]:
        raise RuntimeError("daily receipt hash drift")
    path = Path(run_row["receipt_path"])
    _write_immutable_json(path, receipt)
    return {
        "ok": run_row["status"] not in {"failed", "blocked_required_work"},
        "status": run_row["status"],
        "run_id": run_row["id"],
        "idempotency_key": run_row["idempotency_key"],
        "idempotent_replay": True,
        "receipt_path": str(path),
        "receipt": receipt,
    }


def _bounded_json_value(value: Any, *, list_limit: int, depth: int = 0) -> Any:
    if depth > 8:
        return "[depth limit]"
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, str):
        return value if len(value) <= 2_000 else value[:2_000] + "...[truncated]"
    if isinstance(value, Mapping):
        return {
            str(key): _bounded_json_value(item, list_limit=list_limit, depth=depth + 1)
            for key, item in list(value.items())[:200]
        }
    if isinstance(value, (list, tuple)):
        return [_bounded_json_value(item, list_limit=list_limit, depth=depth + 1) for item in value[:list_limit]]
    return str(value)[:2_000]


def _checkpoint_authority(
    conn: sqlite3.Connection,
    *,
    receipt_root: Path,
    run_date: str,
    retention: int,
    _force_online_backup: bool = False,
) -> dict[str, Any]:
    """Create or reuse a verified shared SQLite checkpoint.

    APFS clones share unchanged extents with the authority database.  Other
    platforms use SQLite's online backup API, which is intentionally exercised
    by the temporary-database tests.
    """

    keep = int(retention)
    if keep < 1:
        raise ValueError("checkpoint retention must be positive")
    conn.commit()
    wal_row = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    wal_state = [int(value) for value in wal_row] if wal_row is not None else []
    database_row = conn.execute("PRAGMA database_list").fetchone()
    database_file = ""
    if database_row is not None:
        database_file = str(database_row["file"] if isinstance(database_row, sqlite3.Row) else database_row[2])
    source_path = Path(database_file).expanduser().resolve() if database_file else None
    page_count = int(conn.execute("PRAGMA page_count").fetchone()[0])
    page_size = int(conn.execute("PRAGMA page_size").fetchone()[0])
    schema_version = int(conn.execute("PRAGMA schema_version").fetchone()[0])
    source_stat = source_path.stat() if source_path and source_path.is_file() else None
    header_sha256 = _sqlite_header_sha256(source_path) if source_path and source_path.is_file() else None
    descriptor = {
        "page_count": page_count,
        "page_size": page_size,
        "schema_version": schema_version,
        "source_size": int(source_stat.st_size) if source_stat else None,
        "source_mtime_ns": int(source_stat.st_mtime_ns) if source_stat else None,
        "sqlite_header_sha256": header_sha256,
        "wal_checkpoint": wal_state,
    }
    checkpoint_key = sha256_text(dumps_json(descriptor))
    checkpoint_dir = receipt_root.expanduser().resolve() / "_authority_checkpoints"
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    destination = checkpoint_dir / f"{run_date}-{checkpoint_key[:24]}.sqlite"
    method = "reused_verified_checkpoint"
    reused = destination.exists()
    if not reused:
        with tempfile.TemporaryDirectory(prefix="authority-checkpoint-", dir=checkpoint_dir) as temp_dir:
            temporary = Path(temp_dir) / destination.name
            method = _copy_sqlite_checkpoint(
                conn,
                source_path=source_path,
                destination=temporary,
                wal_state=wal_state,
                force_online_backup=_force_online_backup,
            )
            verification = _verify_sqlite_checkpoint(temporary)
            if not verification["ok"]:
                raise RuntimeError("authority checkpoint failed SQLite verification")
            os.replace(temporary, destination)
    verification = _verify_sqlite_checkpoint(destination)
    pruned = _prune_authority_checkpoints(
        checkpoint_dir,
        retain=keep,
        preserve=destination,
    )
    return {
        "verified": verification["ok"],
        "path": str(destination),
        "checkpoint_key": checkpoint_key,
        "method": method,
        "reused": reused,
        "quick_check": verification["quick_check"],
        "foreign_key_issue_count": verification["foreign_key_issue_count"],
        "page_count": page_count,
        "page_size": page_size,
        "retention": keep,
        "pruned_count": len(pruned),
        "pruned_names": pruned,
        "scope": "private_local_sqlite_authority_shared_checkpoint",
    }


def _copy_sqlite_checkpoint(
    conn: sqlite3.Connection,
    *,
    source_path: Path | None,
    destination: Path,
    wal_state: list[int],
    force_online_backup: bool = False,
) -> str:
    clone_safe = bool(
        not force_online_backup
        and sys.platform == "darwin"
        and source_path is not None
        and source_path.is_file()
        and (not wal_state or wal_state[0] == 0)
    )
    if clone_safe:
        try:
            subprocess.run(
                ["/bin/cp", "-c", str(source_path), str(destination)],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
            )
            return "apfs_clone_after_wal_checkpoint"
        except (OSError, subprocess.CalledProcessError):
            if destination.exists():
                destination.unlink()
    target = sqlite3.connect(destination)
    try:
        conn.backup(target)
        target.commit()
    finally:
        target.close()
    return "sqlite_online_backup_fallback"


def _verify_sqlite_checkpoint(path: Path) -> dict[str, Any]:
    target = sqlite3.connect(str(path))
    try:
        quick_check = [str(row[0]) for row in target.execute("PRAGMA quick_check").fetchall()]
        fk_issue = target.execute("PRAGMA foreign_key_check").fetchone()
    finally:
        target.close()
    return {
        "ok": quick_check == ["ok"] and fk_issue is None,
        "quick_check": quick_check[:5],
        "foreign_key_issue_count": 0 if fk_issue is None else 1,
    }


def _sqlite_header_sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.sha256(handle.read(100)).hexdigest()


def _prune_authority_checkpoints(
    checkpoint_dir: Path,
    *,
    retain: int,
    preserve: Path,
) -> list[str]:
    candidates = sorted(
        (path for path in checkpoint_dir.glob("*.sqlite") if path.is_file()),
        key=lambda path: (path.stat().st_mtime_ns, path.name),
        reverse=True,
    )
    kept = {path.resolve() for path in candidates[:retain]}
    kept.add(preserve.resolve())
    pruned: list[str] = []
    for path in candidates:
        if path.resolve() in kept:
            continue
        path.unlink()
        pruned.append(path.name)
    return pruned


def _recover_expired_queue_state(
    conn: sqlite3.Connection,
    *,
    limit: int,
    at: str | None = None,
) -> dict[str, Any]:
    """Recover bounded expired leases, then close stale orphan worker runs."""

    item_limit = int(limit)
    if item_limit < 1:
        raise ValueError("lease recovery limit must be positive")
    timestamp = at or now_iso()
    expired = conn.execute(
        """
        SELECT id, attempts, max_attempts
        FROM jobs
        WHERE status = 'claimed'
          AND leased_until IS NOT NULL
          AND leased_until < ?
        ORDER BY leased_until, id
        LIMIT ?
        """,
        (timestamp, item_limit),
    ).fetchall()
    requeued = 0
    failed = 0
    recovered_job_ids: list[int] = []
    for row in expired:
        terminal = int(row["attempts"]) >= int(row["max_attempts"])
        status = "failed" if terminal else "pending"
        error = (
            "expired lease recovered by daily cycle; max attempts exhausted"
            if terminal
            else "expired lease recovered by daily cycle; returned to pending"
        )
        changed = conn.execute(
            """
            UPDATE jobs
            SET status = ?, lease_owner = NULL, leased_until = NULL,
                error = ?, updated_at = ?
            WHERE id = ? AND status = 'claimed'
              AND leased_until IS NOT NULL AND leased_until < ?
            """,
            (status, error, timestamp, row["id"], timestamp),
        ).rowcount
        if not changed:
            continue
        recovered_job_ids.append(int(row["id"]))
        if terminal:
            failed += 1
        else:
            requeued += 1
        if _table_exists(conn, "job_claims"):
            conn.execute(
                """
                UPDATE job_claims
                SET claim_status = 'expired', released_at = ?,
                    notes = 'expired lease recovered by daily cycle'
                WHERE job_id = ? AND claim_status = 'claimed'
                """,
                (timestamp, row["id"]),
            )

    remaining = max(0, item_limit - len(recovered_job_ids))
    zombie_ids: list[str] = []
    if remaining and _table_exists(conn, "worker_runs"):
        parsed = dt.datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        cutoff = (parsed - dt.timedelta(seconds=ZOMBIE_WORKER_GRACE_SECONDS)).replace(
            microsecond=0
        ).isoformat()
        zombies = conn.execute(
            """
            SELECT runs.id, runs.worker_id, runs.updated_at
            FROM worker_runs AS runs
            WHERE runs.status = 'running'
              AND runs.updated_at < ?
              AND NOT EXISTS (
                SELECT 1 FROM jobs
                WHERE jobs.status = 'claimed'
                  AND jobs.lease_owner = runs.worker_id
                  AND jobs.leased_until IS NOT NULL
                  AND jobs.leased_until >= ?
              )
            ORDER BY runs.updated_at, runs.id
            LIMIT ?
            """,
            (cutoff, timestamp, remaining),
        ).fetchall()
        for row in zombies:
            changed = conn.execute(
                """
                UPDATE worker_runs
                SET status = 'failed',
                    metrics_json = ?,
                    updated_at = ?, completed_at = ?
                WHERE id = ? AND status = 'running'
                  AND updated_at = ?
                  AND updated_at < ?
                  AND NOT EXISTS (
                    SELECT 1 FROM jobs
                    WHERE jobs.status = 'claimed'
                      AND jobs.lease_owner = worker_runs.worker_id
                      AND jobs.leased_until IS NOT NULL
                      AND jobs.leased_until >= ?
                  )
                """,
                (
                    dumps_json(
                        {
                            "recovery": "zombie_worker_run_closed",
                            "reason": "no unexpired claimed job after grace window",
                        }
                    ),
                    timestamp,
                    timestamp,
                    row["id"],
                    row["updated_at"],
                    cutoff,
                    timestamp,
                ),
            ).rowcount
            if changed:
                zombie_ids.append(str(row["id"]))
    conn.commit()
    return {
        "expired_jobs_found": len(expired),
        "expired_jobs_requeued": requeued,
        "expired_jobs_failed": failed,
        "recovered_job_ids": recovered_job_ids,
        "zombie_worker_runs_closed": len(zombie_ids),
        "zombie_worker_run_ids": zombie_ids,
        "recovered_total": len(recovered_job_ids) + len(zombie_ids),
        "bounded_limit": item_limit,
        "unexpired_claims_preserved": True,
    }


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return bool(
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (name,),
        ).fetchone()
    )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
