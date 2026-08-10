from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any, Callable

from . import db
from .context_throughput_probe import (
    LABEL_PACK,
    MODEL,
    run_context_throughput_probe,
)
from .paths import root
from .util import dumps_json, now_iso, stable_id, write_text_atomic


def _pending_contexts(conn) -> int:
    return int(
        conn.execute(
            """
            SELECT COUNT(*) FROM jobs
            WHERE lane = 'podcast'
              AND job_type = 'episode_context'
              AND status = 'pending'
              AND json_extract(payload_json, '$.label_pack') = ?
              AND json_extract(payload_json, '$.model') = ?
            """,
            (LABEL_PACK, MODEL),
        ).fetchone()[0]
    )


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


def _episodes_lacking_completed_context(conn) -> int:
    return int(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM episodes
            WHERE EXISTS (
                SELECT 1 FROM transcripts
                WHERE transcripts.episode_id = episodes.id
            )
              AND NOT EXISTS (
                SELECT 1 FROM episode_context_runs
                WHERE episode_context_runs.episode_id = episodes.id
                  AND episode_context_runs.label_pack = ?
                  AND episode_context_runs.model = ?
                  AND episode_context_runs.status = 'completed'
            )
            """,
            (LABEL_PACK, MODEL),
        ).fetchone()[0]
    )


def run_context_campaign_series(
    *,
    campaign_limit: int = 8,
    max_contexts: int = 350,
    concurrency: int = 10,
    max_runtime_seconds: int = 3_600,
    lock_retries: int = 3,
    lock_backoff_seconds: tuple[float, ...] = (5.0, 10.0, 15.0),
    campaign_runner: Callable[..., dict[str, Any]] = run_context_throughput_probe,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    if not 1 <= campaign_limit <= 10:
        raise ValueError("context campaign series is bounded to 1 through 10 campaigns")
    if lock_retries != 3 or len(lock_backoff_seconds) != lock_retries:
        raise ValueError("context series requires exactly three lock retries")
    lock_attempts = lock_retries + 1

    started_at = now_iso()
    started_monotonic = time.monotonic()
    series_id = stable_id(started_at, "episode-context-campaign-series", prefix="pecs_")
    output_root = root() / "work" / "pif-ops" / "context-campaign-series" / series_id
    output_root.mkdir(parents=True, exist_ok=True)
    conn = db.connect()
    try:
        pending_at_start = _pending_contexts(conn)
        labels_at_start = _pending_label_counts(conn)
    finally:
        conn.close()

    campaigns: list[dict[str, Any]] = []
    skips: list[dict[str, Any]] = []
    campaign_slots_used = 0
    consecutive_contention_skips = 0
    stop_reason = "campaign_limit_reached"
    while campaign_slots_used < campaign_limit:
        conn = db.connect()
        try:
            if _pending_contexts(conn) == 0:
                stop_reason = "pending_context_queue_empty"
                break
        finally:
            conn.close()

        campaign_slots_used += 1
        result: dict[str, Any] | None = None
        for lock_attempt in range(1, lock_attempts + 1):
            candidate = campaign_runner(
                max_contexts=max_contexts,
                concurrency=concurrency,
                max_runtime_seconds=max_runtime_seconds,
            )
            if candidate.get("reason") != "pipeline_lock_busy":
                result = candidate
                break
            if lock_attempt < lock_attempts:
                time.sleep(lock_backoff_seconds[lock_attempt - 1])
        if result is None:
            consecutive_contention_skips += 1
            skips.append(
                {
                    "skip_index": len(skips) + 1,
                    "reason": "pipeline_lock_busy_after_three_attempts",
                    "lock_attempts": lock_attempts,
                    "recorded_at": now_iso(),
                }
            )
            if progress_callback:
                progress_callback({"event": "campaign_skip", **skips[-1]})
            if consecutive_contention_skips >= 3:
                stop_reason = "three_consecutive_lock_contention_skips"
                break
            continue

        consecutive_contention_skips = 0
        campaigns.append(result)
        if progress_callback:
            progress_callback(
                {
                    "event": "campaign_result",
                    "campaign": len(campaigns),
                    "run_id": result.get("run_id"),
                    "ok": result.get("ok"),
                    "completed_contexts": result.get("completed_contexts"),
                    "provider_calls": result.get("provider_calls"),
                    "tokens": result.get("tokens"),
                    "validation": result.get("validation"),
                    "execution_diagnostics": result.get("execution_diagnostics"),
                    "series_gate": result.get("series_gate"),
                    "completions_per_hour": result.get("completions_per_hour"),
                    "pending_contexts_at_finish": result.get(
                        "pending_contexts_at_finish"
                    ),
                    "report_path": result.get("report_path"),
                }
            )
        if not result.get("ok"):
            stop_reason = "campaign_quality_gate_failure"
            break

    conn = db.connect()
    try:
        pending_at_finish = _pending_contexts(conn)
        labels_at_finish = _pending_label_counts(conn)
        episodes_lacking = _episodes_lacking_completed_context(conn)
    finally:
        conn.close()
    elapsed_seconds = time.monotonic() - started_monotonic
    completed_contexts = sum(
        int(campaign.get("completed_contexts") or 0) for campaign in campaigns
    )
    billed_tokens = sum(int(campaign.get("tokens") or 0) for campaign in campaigns)
    unique_tokens = sum(
        int(
            ((item.get("usage_profile") or {}).get("last_turn_unique_total_tokens"))
            or 0
        )
        for campaign in campaigns
        for item in campaign.get("token_usage_by_call") or []
    )
    provider_calls = sum(
        int(campaign.get("provider_calls") or 0) for campaign in campaigns
    )
    report = {
        "schema_version": "pif_context_campaign_series_v3",
        "series_id": series_id,
        "started_at": started_at,
        "completed_at": now_iso(),
        "bounds": {
            "campaign_limit": campaign_limit,
            "max_contexts_per_campaign": max_contexts,
            "concurrency": concurrency,
            "max_runtime_seconds_per_campaign": max_runtime_seconds,
            "lock_retries_per_campaign": lock_retries,
            "lock_attempts_per_campaign": lock_attempts,
        },
        "campaigns_run": len(campaigns),
        "campaigns_skipped": len(skips),
        "campaign_slots_used": campaign_slots_used,
        "campaigns": campaigns,
        "skips": skips,
        "stop_reason": stop_reason,
        "wall_time_seconds": elapsed_seconds,
        "completed_contexts": completed_contexts,
        "provider_calls": provider_calls,
        "cumulative_completions_per_hour": (
            completed_contexts * 3600 / elapsed_seconds
            if completed_contexts and elapsed_seconds
            else None
        ),
        "billed_tokens": billed_tokens,
        "unique_tokens": unique_tokens,
        "pending_contexts_at_start": pending_at_start,
        "pending_contexts_at_finish": pending_at_finish,
        "episodes_lacking_completed_context": episodes_lacking,
        "pending_labels_at_start": labels_at_start["pending"],
        "pending_labels_at_finish": labels_at_finish["pending"],
        "dispatchable_pending_labels_at_start": labels_at_start["dispatchable"],
        "dispatchable_pending_labels_at_finish": labels_at_finish["dispatchable"],
        "labels_newly_unblocked": max(
            0,
            labels_at_finish["dispatchable"] - labels_at_start["dispatchable"],
        ),
    }
    report["content_sha256"] = hashlib.sha256(
        dumps_json(report).encode("utf-8")
    ).hexdigest()
    report_path = output_root / "report.json"
    write_text_atomic(
        report_path,
        json.dumps(report, indent=2, sort_keys=True) + "\n",
    )
    return {**report, "report_path": str(report_path)}
