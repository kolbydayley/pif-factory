from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import signal
import threading
import time
import datetime as dt
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Callable

from . import db
from .context_campaign_series import _episodes_lacking_completed_context
from .context_throughput_probe import LABEL_PACK, MODEL, run_context_throughput_probe
from .direction_metric_audit import _population as _direction_metric_population
from .headless_codex import _usage_profile_from_jsonl
from .instrumented_backfill import run_instrumented_backfill
from .orchestrator import pipeline_lock
from .paths import resolve_recorded_path, root
from .util import dumps_json, now_iso, stable_id, write_text_atomic
from .worker import completed_episode_context_for_episode


RECOVERABLE_CONTEXT_ERRORS = {
    "episode_context_segment_read_timeout": "segment_read_timeout",
    "source_card_package_failed:[Errno 11] Resource deadlock avoided": (
        "source_card_resource_deadlock"
    ),
    (
        "source_card_package_failed:Timed out reading segment text: "
        "corpus/segments/seg_45cb979927ab5c2a05498bb0.txt"
    ): "source_card_read_timeout",
}


LABEL_STRUCTURAL_VALIDATION_HALT_FLOOR = 0.90
LABEL_FIELD_QUARANTINE_EMERGENCY_COUNT = 8
LABEL_FIELD_QUARANTINE_ROLLING_LABELS = 400
LABEL_FIELD_QUARANTINE_ROLLING_HALT_COUNT = 20
LABEL_TIMEOUT_RATE_HALT_THRESHOLD = 0.05
LABEL_DIRECTION_RETENTION_MIN_PER_80 = 8.0
LABEL_DIRECTION_RETENTION_MAX_PER_80 = 22.0
# Review-loop Ruling 1 (2026-08-01) established the quarantine-instrumented
# carry-in immediately preceding this series. Keep it in the rolling gate so a
# new bounded invocation cannot erase recent quality history.
LABEL_FIELD_QUARANTINE_CARRY_IN_LABELS = 239
LABEL_FIELD_QUARANTINE_CARRY_IN_COUNT = 7
INTERRUPTED_SERIES_ERROR = (
    "campaign_series_stopped_after_retention_tripwire_parent_terminated"
)


def _series_control_root() -> Path:
    return root() / "work/pif-ops/label-campaign-series-control"


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


@contextlib.contextmanager
def _deferred_stop_signals(stop_event: threading.Event):
    """Turn TERM/INT into a finish-current-campaign stop request."""

    if threading.current_thread() is not threading.main_thread():
        yield
        return
    previous: dict[int, Any] = {}

    def request_stop(_signum: int, _frame: Any) -> None:
        stop_event.set()

    for signum in (signal.SIGTERM, signal.SIGINT):
        previous[signum] = signal.getsignal(signum)
        signal.signal(signum, request_stop)
    try:
        yield
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


def request_active_label_campaign_stop(
    *,
    reason: str,
    control_root: Path | None = None,
) -> dict[str, Any]:
    """Durably stop an active series at its next safe handoff boundary."""

    base = control_root or _series_control_root()
    active_path = base / "active-series.json"
    active = _read_json(active_path)
    if active.get("status") != "active":
        return {"requested": False, "reason": "no_active_series"}
    handoff_lock = Path(str(active["handoff_lock_path"]))
    stop_path = Path(str(active["stop_request_path"]))
    request = {
        "schema_version": "pif_label_campaign_stop_request_v1",
        "series_id": active["series_id"],
        "requested_at": now_iso(),
        "reason": reason,
        "requester_pid": os.getpid(),
    }
    request["content_sha256"] = hashlib.sha256(
        dumps_json(request).encode("utf-8")
    ).hexdigest()
    # The durable request is the linearization point. A campaign that has
    # already passed its under-lock check may finish; every later handoff sees
    # this file before it can dispatch.
    write_text_atomic(stop_path, dumps_json(request) + "\n")
    handoff_lock.parent.mkdir(parents=True, exist_ok=True)
    with handoff_lock.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    write_text_atomic(
        active_path,
        dumps_json({**active, "status": "stop_requested", "stop_request": request})
        + "\n",
    )
    return {"requested": True, **request, "path": str(stop_path)}


def restore_verified_prelaunch_attempts(
    conn,
    *,
    error_marker: str = INTERRUPTED_SERIES_ERROR,
    artifact_root: Path | None = None,
    receipt_root: Path | None = None,
) -> dict[str, Any]:
    """Restore attempts only when logs prove semantic dispatch never began."""

    artifacts = artifact_root or root()
    rows = conn.execute(
        """
        SELECT jobs.id AS job_id, jobs.attempts, jobs.max_attempts,
               jobs.status AS job_status, jobs.error AS job_error,
               label_runs.id AS label_run_id, label_runs.output_path,
               label_runs.error AS label_run_error
        FROM jobs
        JOIN label_runs ON label_runs.job_id = jobs.id
        WHERE jobs.lane = 'podcast'
          AND jobs.job_type = 'label_segment'
          AND jobs.status = 'pending'
          AND jobs.error = ?
          AND label_runs.status = 'failed'
          AND label_runs.error = ?
        ORDER BY jobs.id
        """,
        (error_marker, error_marker),
    ).fetchall()
    evidence: list[dict[str, Any]] = []
    recoverable: list[Any] = []
    for row in rows:
        log_path = artifacts / "runs/headless_logs" / f"{row['label_run_id']}.log"
        output_path = resolve_recorded_path(row["output_path"]) if row["output_path"] else None
        log_bytes = log_path.stat().st_size if log_path.exists() else 0
        output_absent = output_path is None or not output_path.exists()
        provider_call_started = log_bytes > 0 or not output_absent
        usage_profile = (
            _usage_profile_from_jsonl(log_path) if log_bytes > 0 else None
        )
        item = {
            "job_id": int(row["job_id"]),
            "label_run_id": str(row["label_run_id"]),
            "attempts_before": int(row["attempts"]),
            "log_path": str(log_path),
            "log_bytes": log_bytes,
            "output_path": str(output_path) if output_path else None,
            "output_absent": output_absent,
            "provider_call_started": provider_call_started,
            "usage_profile": usage_profile,
        }
        evidence.append(item)
        if not provider_call_started and int(row["attempts"]) > 0:
            recoverable.append((row, item))

    timestamp = now_iso()
    conn.execute("BEGIN IMMEDIATE")
    try:
        restored = 0
        for row, item in recoverable:
            updated = conn.execute(
                """
                UPDATE jobs
                SET attempts = attempts - 1,
                    error = ?,
                    updated_at = ?
                WHERE id = ?
                  AND status = 'pending'
                  AND attempts = ?
                  AND error = ?
                """,
                (
                    "verified_prelaunch_attempt_restored_after_series_stop",
                    timestamp,
                    row["job_id"],
                    row["attempts"],
                    error_marker,
                ),
            )
            if updated.rowcount != 1:
                raise RuntimeError(
                    f"prelaunch recovery state changed for job {row['job_id']}"
                )
            item["attempts_after"] = int(row["attempts"]) - 1
            restored += 1
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    recovery_id = stable_id(timestamp, error_marker, prefix="plpr_")
    report = {
        "schema_version": "pif_prelaunch_claim_recovery_v1",
        "recovery_id": recovery_id,
        "started_at": timestamp,
        "completed_at": now_iso(),
        "error_marker": error_marker,
        "selected": len(rows),
        "recovered_to_pending": restored,
        "provider_started_unchanged": len(rows) - restored,
        "usage_reconciliation": {
            "started_calls": sum(
                bool(item["provider_call_started"]) for item in evidence
            ),
            "terminal_usage_recovered_calls": sum(
                item["usage_profile"] is not None for item in evidence
            ),
            "started_calls_without_terminal_usage": sum(
                bool(item["provider_call_started"])
                and item["usage_profile"] is None
                for item in evidence
            ),
            "recovered_billed_tokens": sum(
                int((item["usage_profile"] or {}).get("cumulative_billed_total_tokens") or 0)
                for item in evidence
            ),
            "recovered_last_turn_unique_tokens": sum(
                int((item["usage_profile"] or {}).get("last_turn_unique_total_tokens") or 0)
                for item in evidence
            ),
            "accounting_complete": all(
                not item["provider_call_started"]
                or item["usage_profile"] is not None
                for item in evidence
            ),
        },
        "evidence": evidence,
    }
    report["content_sha256"] = hashlib.sha256(
        dumps_json(report).encode("utf-8")
    ).hexdigest()
    destination = (
        receipt_root or root() / "work/pif-ops/prelaunch-claim-recovery"
    ) / recovery_id
    destination.mkdir(parents=True, exist_ok=False)
    report_path = destination / "report.json"
    write_text_atomic(report_path, dumps_json(report) + "\n")
    return {**report, "report_path": str(report_path)}


def _pending_label_counts(conn) -> dict[str, int]:
    pending = int(
        conn.execute(
            """
            SELECT COUNT(*) FROM jobs
            WHERE lane = 'podcast'
              AND job_type = 'label_segment'
              AND status = 'pending'
            """
        ).fetchone()[0]
    )
    dispatchable = int(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM jobs
            JOIN segments ON segments.id = jobs.target_id
            WHERE jobs.lane = 'podcast'
              AND jobs.job_type = 'label_segment'
              AND jobs.status = 'pending'
              AND EXISTS (
                SELECT 1
                FROM episode_context_runs
                WHERE episode_context_runs.episode_id = segments.episode_id
                  AND episode_context_runs.label_pack = ?
                  AND episode_context_runs.model = ?
                  AND episode_context_runs.status = 'completed'
              )
            """,
            (LABEL_PACK, MODEL),
        ).fetchone()[0]
    )
    return {"pending": pending, "dispatchable": dispatchable}


def _run_with_lock_retries(
    runner: Callable[[], dict[str, Any]],
    *,
    retries: int = 3,
    backoff_seconds: tuple[float, ...] = (5.0, 10.0, 15.0),
) -> dict[str, Any] | None:
    if retries != 3 or len(backoff_seconds) != retries:
        raise ValueError("production campaigns require exactly three lock retries")
    for attempt in range(retries + 1):
        result = runner()
        if result.get("reason") != "pipeline_lock_busy":
            return result
        if attempt < retries:
            time.sleep(backoff_seconds[attempt])
    return None


def retry_recoverable_contexts(
    *,
    expected_jobs: int = 13,
    concurrency: int = 10,
    max_runtime_seconds: int = 2_400,
) -> dict[str, Any]:
    started_at = now_iso()
    recovery_id = stable_id(started_at, "recoverable-context-retry", prefix="pecr_")
    output_root = root() / "work" / "pif-ops" / "context-recovery" / recovery_id
    output_root.mkdir(parents=True, exist_ok=True)

    with pipeline_lock(wait=False) as acquired:
        if not acquired:
            return {
                "ok": False,
                "stopped": True,
                "reason": "pipeline_lock_busy",
                "provider_calls": 0,
                "tokens": 0,
            }
        conn = db.connect()
        try:
            placeholders = ",".join("?" for _ in RECOVERABLE_CONTEXT_ERRORS)
            rows = conn.execute(
                f"""
                SELECT id, target_id, error, attempts, max_attempts
                FROM jobs
                WHERE lane = 'podcast'
                  AND job_type = 'episode_context'
                  AND status = 'failed'
                  AND error IN ({placeholders})
                ORDER BY id
                """,
                tuple(RECOVERABLE_CONTEXT_ERRORS),
            ).fetchall()
            if len(rows) != expected_jobs:
                return {
                    "ok": False,
                    "stopped": True,
                    "reason": "recoverable_context_inventory_mismatch",
                    "expected_jobs": expected_jobs,
                    "actual_jobs": len(rows),
                    "provider_calls": 0,
                    "tokens": 0,
                }
            episodes_lacking_at_start = _episodes_lacking_completed_context(conn)
            selected: list[dict[str, Any]] = []
            provider_candidate_ids: list[int] = []
            already_completed_ids: list[int] = []
            ts = now_iso()
            for row in rows:
                item = {
                    "job_id": int(row["id"]),
                    "episode_id": str(row["target_id"]),
                    "failure_reason": str(row["error"]),
                    "failure_class": RECOVERABLE_CONTEXT_ERRORS[str(row["error"])],
                    "attempts_before_retry": int(row["attempts"]),
                    "max_attempts": int(row["max_attempts"]),
                }
                completed = completed_episode_context_for_episode(
                    conn,
                    str(row["target_id"]),
                    label_pack=LABEL_PACK,
                    model=MODEL,
                )
                if completed:
                    conn.execute(
                        """
                        UPDATE jobs
                        SET status = 'completed',
                            completed_at = ?,
                            updated_at = ?,
                            error = NULL,
                            lease_owner = NULL,
                            leased_until = NULL
                        WHERE id = ? AND status = 'failed'
                        """,
                        (ts, ts, row["id"]),
                    )
                    item["retry_action"] = "resolved_from_existing_completed_context"
                    already_completed_ids.append(int(row["id"]))
                else:
                    # Bounded recovery credit (durability plan Phase 1): a
                    # failed job may be granted at most ONE attempts reset in
                    # its lifetime. The unbounded `attempts = 0` reset let the
                    # August wave re-dispatch the same failures indefinitely.
                    already_reset = conn.execute(
                        """
                        SELECT 1 FROM jobs
                        WHERE id = ?
                          AND json_extract(payload, '$.recovery_credit_spent') = 1
                        """,
                        (row["id"],),
                    ).fetchone()
                    if already_reset:
                        item["retry_action"] = "recovery_credit_exhausted"
                        selected.append(item)
                        continue
                    conn.execute(
                        """
                        UPDATE jobs
                        SET status = 'pending',
                            attempts = 0,
                            completed_at = NULL,
                            updated_at = ?,
                            lease_owner = NULL,
                            leased_until = NULL,
                            payload = json_set(
                                COALESCE(payload, '{}'),
                                '$.recovery_credit_spent', 1
                            )
                        WHERE id = ? AND status = 'failed'
                        """,
                        (ts, row["id"]),
                    )
                    item["retry_action"] = "requeued_for_bounded_codex_retry"
                    provider_candidate_ids.append(int(row["id"]))
                selected.append(item)
            conn.commit()
        finally:
            conn.close()

    selection_manifest = {
        "schema_version": "pif_recoverable_context_retry_manifest_v1",
        "recovery_id": recovery_id,
        "created_at": now_iso(),
        "model": MODEL,
        "provider_lane": "codex_subscription",
        "selected_jobs": selected,
        "provider_candidate_job_ids": provider_candidate_ids,
        "already_completed_job_ids": already_completed_ids,
    }
    selection_manifest["content_sha256"] = hashlib.sha256(
        dumps_json(selection_manifest).encode("utf-8")
    ).hexdigest()
    manifest_path = output_root / "selection-manifest.json"
    write_text_atomic(
        manifest_path,
        json.dumps(selection_manifest, indent=2, sort_keys=True) + "\n",
    )

    probe = _run_with_lock_retries(
        lambda: run_context_throughput_probe(
            max_contexts=len(provider_candidate_ids),
            concurrency=concurrency,
            max_runtime_seconds=max_runtime_seconds,
        )
    )
    if probe is None:
        probe = {
            "ok": False,
            "stopped": True,
            "reason": "pipeline_lock_busy_after_three_retries",
            "provider_calls": 0,
            "tokens": 0,
        }

    conn = db.connect()
    try:
        outcomes_by_class: dict[str, Counter[str]] = defaultdict(Counter)
        outcomes: list[dict[str, Any]] = []
        for item in selected:
            job = conn.execute(
                "SELECT status, error FROM jobs WHERE id = ?",
                (item["job_id"],),
            ).fetchone()
            context = completed_episode_context_for_episode(
                conn,
                item["episode_id"],
                label_pack=LABEL_PACK,
                model=MODEL,
            )
            if item["job_id"] in already_completed_ids and context:
                outcome = "resolved_existing_context"
            elif context and job and job["status"] == "completed":
                outcome = "completed_retry"
            elif job and job["status"] == "failed":
                outcome = "failed_retry"
            else:
                outcome = str(job["status"] if job else "missing")
            outcomes_by_class[item["failure_class"]][outcome] += 1
            outcomes.append(
                {
                    **item,
                    "outcome": outcome,
                    "final_job_status": str(job["status"] if job else "missing"),
                    "final_error": job["error"] if job else None,
                }
            )
        episodes_lacking_at_finish = _episodes_lacking_completed_context(conn)
    finally:
        conn.close()

    report = {
        "schema_version": "pif_recoverable_context_retry_report_v1",
        "recovery_id": recovery_id,
        "started_at": started_at,
        "completed_at": now_iso(),
        "selected_jobs": len(selected),
        "provider_candidates": len(provider_candidate_ids),
        "resolved_from_existing_context": len(already_completed_ids),
        "outcomes": outcomes,
        "outcomes_by_class": {
            key: dict(sorted(value.items()))
            for key, value in sorted(outcomes_by_class.items())
        },
        "episodes_lacking_completed_context_at_start": episodes_lacking_at_start,
        "episodes_lacking_completed_context_at_finish": episodes_lacking_at_finish,
        "probe": probe,
        "selection_manifest_path": str(manifest_path),
        "selection_manifest_sha256": selection_manifest["content_sha256"],
    }
    report["ok"] = bool(
        probe.get("ok")
        and all(
            item["outcome"] in {"resolved_existing_context", "completed_retry"}
            for item in outcomes
        )
    )
    report["content_sha256"] = hashlib.sha256(
        dumps_json(report).encode("utf-8")
    ).hexdigest()
    report_path = output_root / "report.json"
    write_text_atomic(
        report_path,
        json.dumps(report, indent=2, sort_keys=True) + "\n",
    )
    return {**report, "report_path": str(report_path)}


def evaluate_label_campaign_gate(
    report: dict[str, Any],
    *,
    rolling_labels: int = 0,
    rolling_field_bearing_quarantines: int = 0,
) -> dict[str, Any]:
    selected = int(report.get("selected_label_jobs") or 0)
    prepared = int(report.get("prepared_label_calls") or 0)
    provider_calls = int(report.get("provider_calls") or 0)
    completed = int(report.get("segments_completed") or 0)
    structural_rate = completed / provider_calls if provider_calls else 0.0
    audit = report.get("audit_lane") or {}
    audit_rate = audit.get("pass_rate")
    audited = int(audit.get("audited_labels") or 0)
    spans = report.get("new_labels") or {}
    evidence_rate = spans.get("evidence_span_validity_rate")
    repairs = report.get("deterministic_repair") or {}
    diagnostics = report.get("execution_diagnostics") or {}
    terminal = report.get("terminal_failures") or {}
    isolation = report.get("isolation") or {}
    cost_gate = report.get("cost_gate") or {}
    field_quarantines = int(
        repairs.get("field_bearing_metric_quarantine_count") or 0
    )
    direction_only_quarantines = int(
        repairs.get("direction_only_metric_quarantine_count") or 0
    )
    reasons: list[str] = []
    if selected != prepared or prepared != provider_calls:
        reasons.append("campaign_accounting_incomplete")
    if structural_rate < LABEL_STRUCTURAL_VALIDATION_HALT_FLOOR:
        reasons.append("structural_validation_below_90_percent_floor")
    if audited != completed or audit_rate is None or float(audit_rate) < 0.99:
        reasons.append("audit_pass_rate_below_campaign_gate")
    if evidence_rate is None or float(evidence_rate) != 1.0:
        reasons.append("evidence_span_validity_not_100_percent")
    if field_quarantines >= LABEL_FIELD_QUARANTINE_EMERGENCY_COUNT:
        reasons.append("field_bearing_quarantine_emergency_tripwire")
    if (
        rolling_labels >= LABEL_FIELD_QUARANTINE_ROLLING_LABELS
        and rolling_field_bearing_quarantines
        >= LABEL_FIELD_QUARANTINE_ROLLING_HALT_COUNT
    ):
        reasons.append("field_bearing_quarantine_rolling_gate")
    if float(terminal.get("rate") or 0.0) > 0.02:
        reasons.append("terminal_failure_rate_above_two_percent")
    if int(diagnostics.get("expired_leases") or 0):
        reasons.append("lease_expiries")
    timeouts = int(diagnostics.get("timeouts") or 0)
    safely_requeued_timeouts = int(
        diagnostics.get("timeouts_safely_requeued") or 0
    )
    timeout_rate = timeouts / provider_calls if provider_calls else 0.0
    if timeouts and safely_requeued_timeouts != timeouts:
        # Preserve the established receipt reason while distinguishing the
        # unsafe-return condition in the measured fields below.
        reasons.append("provider_timeouts")
    if timeout_rate > LABEL_TIMEOUT_RATE_HALT_THRESHOLD:
        reasons.append("provider_timeout_rate_above_five_percent")
    if int(diagnostics.get("provider_pressure_signal_calls") or 0):
        reasons.append("provider_pressure")
    if not cost_gate.get("passed"):
        reasons.append("cost_gate_failed")
    if cost_gate.get("paid_api_billing_detected"):
        reasons.append("paid_api_billing_detected")
    if int(isolation.get("paid_api_telemetry_delta") or 0):
        reasons.append("paid_api_telemetry_delta")
    if isolation.get("unexpected_changed_tables"):
        reasons.append("unexpected_table_writes")
    if any(
        int(value or 0)
        for value in (isolation.get("protected_table_deltas") or {}).values()
    ):
        reasons.append("protected_table_deltas")
    return {
        "passed": not reasons,
        "reasons": reasons,
        "audit_pass_rate": audit_rate,
        "evidence_span_validity_rate": evidence_rate,
        "structural_validation_rate": structural_rate,
        "structural_validation_halt_floor": (
            LABEL_STRUCTURAL_VALIDATION_HALT_FLOOR
        ),
        "terminal_failure_rate": float(terminal.get("rate") or 0.0),
        "timeouts": timeouts,
        "timeouts_safely_requeued": safely_requeued_timeouts,
        "timeout_rate": timeout_rate,
        "timeout_rate_halt_threshold": LABEL_TIMEOUT_RATE_HALT_THRESHOLD,
        "field_bearing_metric_quarantine_count": field_quarantines,
        "direction_only_metric_quarantine_count": direction_only_quarantines,
        "field_bearing_quarantine_emergency_count": (
            LABEL_FIELD_QUARANTINE_EMERGENCY_COUNT
        ),
        "rolling_labels": rolling_labels,
        "rolling_field_bearing_quarantines": rolling_field_bearing_quarantines,
        "rolling_field_bearing_quarantine_halt_count": (
            LABEL_FIELD_QUARANTINE_ROLLING_HALT_COUNT
        ),
    }


def _direction_retention_boundary(result: dict[str, Any]) -> dict[str, Any]:
    completed = int(result.get("segments_completed") or 0)
    manifest_value = result.get("manifest_path")
    retained = 0
    if manifest_value:
        manifest_path = resolve_recorded_path(str(manifest_value))
        if manifest_path.is_file():
            retained = len(_direction_metric_population(manifest_path))
    per_80 = retained * 80 / completed if completed else 0.0
    out_of_band = bool(
        completed
        and (
            per_80 < LABEL_DIRECTION_RETENTION_MIN_PER_80
            or per_80 > LABEL_DIRECTION_RETENTION_MAX_PER_80
        )
    )
    return {
        "schema_version": "pif_direction_retention_boundary_v1",
        "run_id": result.get("run_id"),
        "labels_completed": completed,
        "retained_direction_only_metrics": retained,
        "retained_per_80_labels": per_80,
        "adopted_band_per_80": [
            LABEL_DIRECTION_RETENTION_MIN_PER_80,
            LABEL_DIRECTION_RETENTION_MAX_PER_80,
        ],
        "out_of_band": out_of_band,
        "action": "re_audit_requested_continue_series" if out_of_band else "within_band",
        "evaluated_at": now_iso(),
    }


def run_label_campaign_series(
    *,
    campaign_limit: int = 8,
    wave_size: int = 80,
    concurrency: int = 10,
    max_runtime_seconds: int = 3_600,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
    control_root: Path | None = None,
) -> dict[str, Any]:
    if not 1 <= campaign_limit <= 12:
        raise ValueError("label campaign series is bounded to one through twelve campaigns")
    if wave_size != 80:
        raise ValueError("the authorized label claim wave size is exactly 80")
    if concurrency != 10 or max_runtime_seconds != 3_600:
        raise ValueError("label campaigns require concurrency 10 and 3600 seconds")

    started_at = now_iso()
    started_monotonic = time.monotonic()
    series_id = stable_id(started_at, "label-campaign-series", prefix="plcs_")
    output_root = root() / "work" / "pif-ops" / "label-campaign-series" / series_id
    output_root.mkdir(parents=True, exist_ok=True)
    controls = control_root or _series_control_root()
    controls.mkdir(parents=True, exist_ok=True)
    handoff_lock_path = controls / f"{series_id}.handoff.lock"
    stop_request_path = controls / f"{series_id}.stop.json"
    active_path = controls / "active-series.json"
    active_record = {
        "schema_version": "pif_active_label_campaign_series_v1",
        "series_id": series_id,
        "status": "active",
        "started_at": started_at,
        "pid": os.getpid(),
        "handoff_lock_path": str(handoff_lock_path),
        "stop_request_path": str(stop_request_path),
    }
    write_text_atomic(active_path, dumps_json(active_record) + "\n")
    stop_event = threading.Event()
    signal_context = _deferred_stop_signals(stop_event)
    signal_context.__enter__()
    conn = db.connect()
    try:
        counts_at_start = _pending_label_counts(conn)
    finally:
        conn.close()

    campaigns: list[dict[str, Any]] = []
    skips: list[dict[str, Any]] = []
    daily_priority_waits: list[dict[str, Any]] = []
    quarantine_windows: list[tuple[int, int]] = [
        (
            LABEL_FIELD_QUARANTINE_CARRY_IN_LABELS,
            LABEL_FIELD_QUARANTINE_CARRY_IN_COUNT,
        )
    ]
    stop_reason = "campaign_limit_reached"
    for slot in range(1, campaign_limit + 1):
        if stop_event.is_set() or stop_request_path.exists():
            stop_reason = "external_stop_requested"
            break
        intent_path = (
            root()
            / "work"
            / "pif-ops"
            / "controller-daily-intent"
            / dt.datetime.now().astimezone().date().isoformat()
            / "intent.json"
        )
        wait_started = time.monotonic()
        wait_reported = False
        while intent_path.exists():
            try:
                intent = json.loads(intent_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                intent = {"status": "unreadable"}
            intent_status = str(intent.get("status") or "unknown")
            if intent_status == "completed_genuinely_successful":
                break
            if intent_status == "failed":
                stop_reason = "daily_cycle_failed_priority_stop"
                break
            if not wait_reported:
                wait_event = {
                    "campaign_slot": slot,
                    "reason": "daily_cycle_priority_window",
                    "intent_status": intent_status,
                    "intent_path": str(intent_path),
                    "started_at": now_iso(),
                }
                daily_priority_waits.append(wait_event)
                if progress_callback:
                    progress_callback({"event": "daily_priority_wait", **wait_event})
                wait_reported = True
            if time.monotonic() - wait_started >= max_runtime_seconds + 300:
                stop_reason = "daily_cycle_priority_wait_timeout"
                break
            time.sleep(5.0)
        if stop_reason in {
            "daily_cycle_failed_priority_stop",
            "daily_cycle_priority_wait_timeout",
        }:
            break
        if stop_event.is_set() or stop_request_path.exists():
            stop_reason = "external_stop_requested"
            break
        if wait_reported:
            daily_priority_waits[-1]["completed_at"] = now_iso()
            daily_priority_waits[-1]["wall_time_seconds"] = (
                time.monotonic() - wait_started
            )
        handoff_lock_path.parent.mkdir(parents=True, exist_ok=True)
        with handoff_lock_path.open("a+", encoding="utf-8") as handoff:
            fcntl.flock(handoff.fileno(), fcntl.LOCK_EX)
            if stop_event.is_set() or stop_request_path.exists():
                stop_reason = "external_stop_requested"
                result = None
            else:
                result = _run_with_lock_retries(
                    lambda: run_instrumented_backfill(
                        max_contexts=0,
                        max_labels=wave_size,
                        max_runtime_seconds=max_runtime_seconds,
                        concurrency=concurrency,
                        label_only_existing_context=True,
                    )
                )
            if stop_event.is_set() and not stop_request_path.exists():
                request = {
                    "schema_version": "pif_label_campaign_stop_request_v1",
                    "series_id": series_id,
                    "requested_at": now_iso(),
                    "reason": "termination_signal_deferred_until_campaign_boundary",
                    "requester_pid": os.getpid(),
                }
                request["content_sha256"] = hashlib.sha256(
                    dumps_json(request).encode("utf-8")
                ).hexdigest()
                write_text_atomic(stop_request_path, dumps_json(request) + "\n")
            fcntl.flock(handoff.fileno(), fcntl.LOCK_UN)
        if stop_reason == "external_stop_requested":
            break
        if stop_event.is_set() or stop_request_path.exists():
            stop_reason = "external_stop_requested"
            break
        if result is None:
            skip = {
                "campaign_slot": slot,
                "reason": "pipeline_lock_busy_after_three_retries",
                "recorded_at": now_iso(),
            }
            skips.append(skip)
            if progress_callback:
                progress_callback({"event": "campaign_skip", **skip})
            continue
        repairs = result.get("deterministic_repair") or {}
        quarantine_windows.append(
            (
                int(result.get("segments_completed") or 0),
                int(repairs.get("field_bearing_metric_quarantine_count") or 0),
            )
        )
        rolling_labels = 0
        rolling_field_quarantines = 0
        for label_count, quarantine_count in reversed(quarantine_windows):
            if rolling_labels >= LABEL_FIELD_QUARANTINE_ROLLING_LABELS:
                break
            rolling_labels += label_count
            rolling_field_quarantines += quarantine_count
        gate = evaluate_label_campaign_gate(
            result,
            rolling_labels=rolling_labels,
            rolling_field_bearing_quarantines=rolling_field_quarantines,
        )
        retention = _direction_retention_boundary(result)
        if retention["out_of_band"]:
            request_path = output_root / f"campaign-{slot:02d}-direction-reaudit.json"
            retention["request_path"] = str(request_path)
            retention["content_sha256"] = hashlib.sha256(
                dumps_json(retention).encode("utf-8")
            ).hexdigest()
            write_text_atomic(request_path, dumps_json(retention) + "\n")
            if progress_callback:
                progress_callback(
                    {
                        "event": "direction_retention_reaudit_requested",
                        "campaign_slot": slot,
                        **retention,
                    }
                )
        campaign = {
            **result,
            "label_series_gate": gate,
            "direction_retention_boundary": retention,
            "campaign_slot": slot,
        }
        campaigns.append(campaign)
        if progress_callback:
            progress_callback(
                {
                    "event": "campaign_result",
                    "campaign_slot": slot,
                    "run_id": result.get("run_id"),
                    "segments_completed": result.get("segments_completed"),
                    "completions_per_hour": result.get(
                        "label_completions_per_hour"
                    ),
                    "provider_calls": result.get("provider_calls"),
                    "tokens": result.get("tokens"),
                    "gate": gate,
                    "report_path": result.get("report_path"),
                }
            )
        if not gate["passed"]:
            stop_reason = "campaign_quality_gate_failure"
            break

    signal_context.__exit__(None, None, None)

    conn = db.connect()
    try:
        counts_at_finish = _pending_label_counts(conn)
    finally:
        conn.close()
    wall_seconds = time.monotonic() - started_monotonic
    completed = sum(int(item.get("segments_completed") or 0) for item in campaigns)
    provider_calls = sum(int(item.get("provider_calls") or 0) for item in campaigns)
    billed_tokens = sum(int(item.get("tokens") or 0) for item in campaigns)
    unique_tokens = sum(
        int((call.get("usage_profile") or {}).get("last_turn_unique_total_tokens") or 0)
        for campaign in campaigns
        for call in campaign.get("token_usage_by_call") or []
    )
    cumulative_rate = (
        completed * 3600 / wall_seconds if completed and wall_seconds else None
    )
    billed_per_call = billed_tokens / provider_calls if provider_calls else None
    unique_per_call = unique_tokens / provider_calls if provider_calls else None
    remaining = counts_at_finish["pending"]
    report = {
        "schema_version": "pif_label_campaign_series_v1",
        "series_id": series_id,
        "started_at": started_at,
        "completed_at": now_iso(),
        "bounds": {
            "campaign_limit": campaign_limit,
            "claim_wave_size": wave_size,
            "max_labels_per_campaign": wave_size,
            "concurrency": concurrency,
            "max_runtime_seconds_per_campaign": max_runtime_seconds,
            "model": MODEL,
            "provider_lane": "codex_subscription",
            "lock_retries_per_campaign": 3,
            "rolling_quarantine_carry_in_labels": (
                LABEL_FIELD_QUARANTINE_CARRY_IN_LABELS
            ),
            "rolling_quarantine_carry_in_count": (
                LABEL_FIELD_QUARANTINE_CARRY_IN_COUNT
            ),
        },
        "campaigns_run": len(campaigns),
        "campaigns_skipped": len(skips),
        "campaigns": campaigns,
        "skips": skips,
        "daily_priority_waits": daily_priority_waits,
        "stop_reason": stop_reason,
        "wall_time_seconds": wall_seconds,
        "segments_completed": completed,
        "provider_calls": provider_calls,
        "cumulative_completions_per_hour": cumulative_rate,
        "billed_tokens": billed_tokens,
        "unique_tokens": unique_tokens,
        "billed_tokens_per_call": billed_per_call,
        "unique_tokens_per_call": unique_per_call,
        "pending_labels_at_start": counts_at_start["pending"],
        "pending_labels_at_finish": remaining,
        "dispatchable_labels_at_start": counts_at_start["dispatchable"],
        "dispatchable_labels_at_finish": counts_at_finish["dispatchable"],
        "projection": {
            "implied_hours_to_clear_remaining": (
                remaining / cumulative_rate if cumulative_rate else None
            ),
            "implied_days_to_clear_remaining": (
                remaining / cumulative_rate / 24 if cumulative_rate else None
            ),
            "projected_billed_tokens_to_clear_remaining": (
                remaining * billed_per_call if billed_per_call else None
            ),
            "projected_unique_tokens_to_clear_remaining": (
                remaining * unique_per_call if unique_per_call else None
            ),
        },
    }
    report["content_sha256"] = hashlib.sha256(
        dumps_json(report).encode("utf-8")
    ).hexdigest()
    report_path = output_root / "report.json"
    write_text_atomic(
        report_path,
        json.dumps(report, indent=2, sort_keys=True) + "\n",
    )
    current_active = _read_json(active_path)
    if current_active.get("series_id") == series_id:
        write_text_atomic(
            active_path,
            dumps_json(
                {
                    **current_active,
                    "status": "stopped" if stop_reason == "external_stop_requested" else "completed",
                    "completed_at": report["completed_at"],
                    "stop_reason": stop_reason,
                    "report_path": str(report_path),
                }
            )
            + "\n",
        )
    return {**report, "report_path": str(report_path)}
