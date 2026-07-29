from __future__ import annotations

import datetime as dt
import hashlib
import json
import random
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from . import db
from .headless_codex import execute_claimed_label_runs
from .labels import audit_label_grounding
from .orchestrator import pipeline_lock
from .paths import root
from .util import dumps_json, now_iso, stable_id, write_text_atomic
from .windowed_evaluation import execute_instrumented_episode_context_job
from .worker import (
    enqueue_episode_context_job_for_segment,
    run_jobs,
    segment_for_job,
)


MODEL = "gpt-5.5"
LABEL_PACK = "ai_discourse_v3_1"
EXPECTED_MUTATION_TABLES = {
    "actor_positions",
    "coded_observations",
    "claims",
    "discourse_event_contexts",
    "discourse_events",
    "entity_mentions",
    "episode_context_run_attempts",
    "episode_context_runs",
    "frame_usages",
    "jobs",
    "label_runs",
    "labels",
    "orgs",
    "people",
    "person_person_mentions",
    "product_signals",
    "products",
    "quality_audits",
    "raw_actor_mentions",
    "raw_speaker_mentions",
    "relationship_edges",
    "release_signal_links",
    "speaker_positions",
    "term_mentions",
    "term_usages",
    "topic_mentions",
}


def _usage_total(value: Any) -> int:
    if not isinstance(value, dict):
        return 0
    for key in ("total_tokens", "total_token_count"):
        if isinstance(value.get(key), int):
            return int(value[key])
    return sum(
        int(value.get(key) or 0)
        for key in ("input_tokens", "cached_input_tokens", "output_tokens")
    )


def _table_counts(conn) -> dict[str, int]:
    names = [
        row["name"]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
    ]
    return {
        name: int(conn.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0])
        for name in names
    }


def _label_metrics(
    conn,
    label_rows: list[Any],
    *,
    deadline_monotonic: float | None = None,
    skip_segment_errors: bool = False,
) -> dict[str, Any]:
    event_counts: list[int] = []
    valid_spans = 0
    total_spans = 0
    zero_by_source: Counter[str] = Counter()
    audit_distribution: Counter[str] = Counter()
    skipped_reasons: Counter[str] = Counter()
    timed_out = False
    rows_examined = 0
    for row in label_rows:
        if deadline_monotonic is not None and time.monotonic() >= deadline_monotonic:
            timed_out = True
            break
        rows_examined += 1
        try:
            output = json.loads(row["output_json"])
            events = output.get("discourse_events") or []
            segment = segment_for_job(conn, {"target_id": row["segment_id"]})
        except Exception as exc:
            if not skip_segment_errors:
                raise
            skipped_reasons[type(exc).__name__] += 1
            continue
        event_counts.append(len(events))
        for event in events:
            if not isinstance(event, dict):
                continue
            evidence = event.get("evidence")
            start = event.get("evidence_start")
            end = event.get("evidence_end")
            total_spans += 1
            if (
                isinstance(evidence, str)
                and isinstance(start, int)
                and isinstance(end, int)
                and 0 <= start < end <= len(segment["segment_text"])
                and segment["segment_text"][start:end] == evidence
            ):
                valid_spans += 1
        if not events:
            source = conn.execute(
                """
                SELECT sources.name
                FROM segments
                JOIN episodes ON episodes.id = segments.episode_id
                JOIN sources ON sources.id = episodes.source_id
                WHERE segments.id = ?
                """,
                (row["segment_id"],),
            ).fetchone()
            zero_by_source[str(source["name"] if source else "unknown")] += 1
        audit = audit_label_grounding(
            row["label_pack"],
            output,
            segment_text=segment["segment_text"],
            expected_context=segment["context"],
        )
        audit_distribution[str(audit["status"])] += 1
    return {
        "requested_labels": len(label_rows),
        "labels": len(event_counts),
        "labels_measured": len(event_counts),
        "labels_skipped": len(label_rows) - len(event_counts),
        "labels_skipped_due_to_time_budget": len(label_rows) - rows_examined,
        "skip_reasons": dict(sorted(skipped_reasons.items())),
        "time_budget_exhausted": timed_out,
        "events": sum(event_counts),
        "events_per_segment": (
            sum(event_counts) / len(event_counts) if event_counts else None
        ),
        "zero_event_segments": sum(1 for count in event_counts if count == 0),
        "zero_event_segments_by_source": dict(sorted(zero_by_source.items())),
        "evidence_spans_checked": total_spans,
        "evidence_spans_valid": valid_spans,
        "evidence_span_validity_rate": (
            valid_spans / total_spans if total_spans else None
        ),
        "audit_distribution": dict(sorted(audit_distribution.items())),
    }


def _historic_baseline(
    conn,
    *,
    before: str,
    limit: int = 400,
    max_seconds: float = 45.0,
) -> dict[str, Any]:
    if limit < 1:
        raise ValueError("historical baseline limit must be positive")
    if max_seconds <= 0:
        raise ValueError("historical baseline time budget must be positive")
    rows = conn.execute(
        """
        SELECT *
        FROM labels
        WHERE label_pack = ?
          AND model = ?
          AND created_at < ?
        ORDER BY created_at DESC, id DESC
        LIMIT ?
        """,
        (LABEL_PACK, MODEL, before, limit),
    ).fetchall()
    measured = _label_metrics(
        conn,
        list(rows),
        deadline_monotonic=time.monotonic() + max_seconds,
        skip_segment_errors=True,
    )
    measured["available"] = bool(measured["labels_measured"])
    measured["baseline_unavailable"] = (
        None
        if measured["available"]
        else "no_historical_segments_measured_within_bounds"
    )
    measured["row_limit"] = limit
    measured["time_limit_seconds"] = max_seconds
    return measured


def _context_candidates(conn, *, limit: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT jobs.*, segments.episode_id
        FROM jobs
        JOIN segments ON segments.id = jobs.target_id
        WHERE jobs.job_type = 'label_segment'
          AND jobs.status = 'pending'
          AND jobs.lane = 'podcast'
          AND json_extract(jobs.payload_json, '$.label_pack') = ?
          AND NOT EXISTS (
            SELECT 1 FROM episode_context_runs
            WHERE episode_context_runs.episode_id = segments.episode_id
              AND episode_context_runs.label_pack = ?
              AND episode_context_runs.model = ?
              AND episode_context_runs.status = 'completed'
          )
          AND NOT EXISTS (
            SELECT 1 FROM episode_context_runs
            WHERE episode_context_runs.episode_id = segments.episode_id
              AND episode_context_runs.label_pack = ?
              AND episode_context_runs.model = ?
              AND episode_context_runs.status = 'failed'
          )
        GROUP BY segments.episode_id
        ORDER BY jobs.priority, MIN(jobs.id)
        LIMIT ?
        """,
        (LABEL_PACK, LABEL_PACK, MODEL, LABEL_PACK, MODEL, limit),
    ).fetchall()
    return [dict(row) for row in rows]


def _run_context_batch(job_ids: list[int], worker_id: str, timeout: int) -> list[dict[str, Any]]:
    def run_one(job_id: int) -> dict[str, Any]:
        conn = db.connect()
        try:
            return execute_instrumented_episode_context_job(
                conn,
                job_id=job_id,
                worker_id=worker_id,
                timeout_seconds=timeout,
            )
        finally:
            conn.close()

    results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=min(3, len(job_ids))) as executor:
        futures = {executor.submit(run_one, job_id): job_id for job_id in job_ids}
        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception as exc:
                results.append(
                    {
                        "job_id": futures[future],
                        "state": "failed",
                        "status_ok": False,
                        "submission_error": type(exc).__name__,
                        "error": str(exc)[:500],
                    }
                )
    return results


def run_instrumented_backfill(
    *,
    max_contexts: int = 40,
    max_labels: int = 400,
    max_runtime_seconds: int = 3600,
    concurrency: int = 3,
) -> dict[str, Any]:
    if concurrency != 3:
        raise ValueError("the authorized backfill concurrency is exactly 3")
    started_at = now_iso()
    started_monotonic = time.monotonic()
    run_id = stable_id(started_at, "instrumented-backfill", prefix="pib_")
    output_root = root() / "work" / "pif-ops" / "backfills" / run_id
    output_root.mkdir(parents=True, exist_ok=True)
    conn = db.connect()
    with pipeline_lock(wait=False) as acquired:
        if not acquired:
            conn.close()
            return {"ok": False, "stopped": True, "reason": "pipeline_lock_busy"}
        before_counts = _table_counts(conn)
        release_count_before = int(
            conn.execute("SELECT COUNT(*) FROM corpus_releases").fetchone()[0]
        )
        scale_count_before = int(
            conn.execute("SELECT COUNT(*) FROM pif_scale_gate_state_receipts").fetchone()[0]
        )
        paid_before = int(
            conn.execute(
                """
                SELECT COUNT(*) FROM pipeline_runs
                WHERE json_extract(parameters_json, '$.paid_api') = 1
                """
            ).fetchone()[0]
        )
        candidates = _context_candidates(conn, limit=max_contexts)
        context_results: list[dict[str, Any]] = []
        label_results: list[dict[str, Any]] = []
        completed_episode_ids: list[str] = []
        stop_reason = "candidate_inventory_exhausted"

        for offset in range(0, len(candidates), concurrency):
            elapsed = time.monotonic() - started_monotonic
            if elapsed >= max_runtime_seconds - 120:
                stop_reason = "runtime_headroom_reached"
                break
            if len(context_results) >= max_contexts or len(label_results) >= max_labels:
                stop_reason = "authorized_item_bound_reached"
                break
            batch = candidates[offset : offset + concurrency]
            episode_ids = tuple(str(row["episode_id"]) for row in batch)
            for row in batch:
                enqueue_episode_context_job_for_segment(
                    conn,
                    row,
                    label_pack=LABEL_PACK,
                    model=MODEL,
                )
            conn.commit()
            worker_id = f"{run_id}-context"
            claimed = run_jobs(
                conn,
                lane="podcast",
                limit=len(batch),
                model=MODEL,
                label_pack=LABEL_PACK,
                worker_id=worker_id,
                claim_prompts=True,
                job_types=("episode_context",),
                max_label_prompts=len(batch),
                target_ids=episode_ids,
            )
            job_ids = [
                int(item["job_id"])
                for item in claimed["details"]
                if item.get("prompt")
            ]
            remaining = max(1, int(max_runtime_seconds - (time.monotonic() - started_monotonic)))
            batch_context = _run_context_batch(
                job_ids,
                worker_id,
                timeout=min(900, remaining),
            )
            context_results.extend(batch_context)
            successful = {
                str(item.get("episode_id"))
                for item in batch_context
                if item.get("status_ok")
            }
            completed_episode_ids.extend(sorted(successful))
            context_failures = sum(
                1 for item in context_results if not item.get("status_ok")
            )
            if context_results and context_failures / len(context_results) > 0.20:
                stop_reason = "first_attempt_validation_failure_rate_above_20_percent"
                break
            if not successful:
                continue
            remaining_labels = max_labels - len(label_results)
            selected_label_jobs = conn.execute(
                    f"""
                    SELECT jobs.id, jobs.target_id
                    FROM jobs
                    JOIN segments ON segments.id = jobs.target_id
                    WHERE jobs.job_type = 'label_segment'
                      AND jobs.status = 'pending'
                      AND jobs.lane = 'podcast'
                      AND json_extract(jobs.payload_json, '$.label_pack') = ?
                      AND segments.episode_id IN ({','.join('?' for _ in successful)})
                    ORDER BY jobs.priority, jobs.id
                    LIMIT ?
                    """,
                    (LABEL_PACK, *sorted(successful), remaining_labels),
                ).fetchall()
            segment_ids = tuple(str(row["target_id"]) for row in selected_label_jobs)
            selected_job_ids = tuple(int(row["id"]) for row in selected_label_jobs)
            if not segment_ids:
                continue
            label_worker = f"{run_id}-labels"
            prepared = run_jobs(
                conn,
                lane="podcast",
                limit=len(segment_ids),
                model=MODEL,
                label_pack=LABEL_PACK,
                worker_id=label_worker,
                claim_prompts=True,
                job_types=("label_segment",),
                max_label_prompts=len(segment_ids),
                job_ids=selected_job_ids,
            )
            prompt_count = int(prepared.get("claimed_prompts", 0))
            remaining = max(1, int(max_runtime_seconds - (time.monotonic() - started_monotonic)))
            executed = execute_claimed_label_runs(
                conn,
                lease_owner=label_worker,
                limit=prompt_count,
                model=MODEL,
                timeout_seconds=min(900, remaining),
                audit=True,
                concurrency=concurrency,
                capture_usage=True,
                capture_validation=True,
            )
            label_results.extend(executed["results"])
            attempted = [
                item for item in label_results if item.get("first_attempt_validation")
            ]
            invalid = sum(
                1
                for item in attempted
                if not item["first_attempt_validation"]["passed"]
            )
            if attempted and invalid / len(attempted) > 0.20:
                stop_reason = "first_attempt_validation_failure_rate_above_20_percent"
                break
        else:
            if len(context_results) >= max_contexts or len(label_results) >= max_labels:
                stop_reason = "authorized_item_bound_reached"

        label_ids = [
            str(item["submission"]["label_id"])
            for item in label_results
            if item.get("status") == "submitted"
        ]
        new_label_rows = (
            conn.execute(
                f"SELECT * FROM labels WHERE id IN ({','.join('?' for _ in label_ids)})",
                label_ids,
            ).fetchall()
            if label_ids
            else []
        )
        sample_size = min(30, len(label_ids))
        sample_ids = (
            random.Random(run_id).sample(sorted(label_ids), sample_size)
            if sample_size
            else []
        )
        sample_rows = (
            conn.execute(
                f"SELECT * FROM labels WHERE id IN ({','.join('?' for _ in sample_ids)})",
                sample_ids,
            ).fetchall()
            if sample_ids
            else []
        )
        new_metrics = _label_metrics(conn, list(new_label_rows))
        audit_sample = _label_metrics(conn, list(sample_rows))
        try:
            historical = _historic_baseline(
                conn,
                before=started_at,
                limit=400,
                max_seconds=45.0,
            )
        except Exception as exc:
            historical = {
                "available": False,
                "baseline_unavailable": f"{type(exc).__name__}: {str(exc)[:500]}",
                "requested_labels": 400,
                "labels_measured": 0,
                "labels_skipped": 0,
                "labels_skipped_due_to_time_budget": 0,
                "skip_reasons": {},
                "time_budget_exhausted": False,
                "row_limit": 400,
                "time_limit_seconds": 45.0,
            }
        sample_pass_rate = (
            int(audit_sample["audit_distribution"].get("passed", 0))
            / int(audit_sample["labels_measured"])
            if audit_sample.get("labels_measured")
            else None
        )
        baseline_pass_rate = (
            int((historical.get("audit_distribution") or {}).get("passed", 0))
            / int(historical["labels_measured"])
            if historical.get("labels_measured")
            else None
        )
        quality_loss_reasons: list[str] = []
        if (
            sample_pass_rate is not None
            and baseline_pass_rate is not None
            and sample_pass_rate + 0.10 < baseline_pass_rate
        ):
            quality_loss_reasons.append("audit_pass_rate_more_than_10pp_below_baseline")
        sample_span_rate = audit_sample.get("evidence_span_validity_rate")
        baseline_span_rate = historical.get("evidence_span_validity_rate")
        if (
            isinstance(sample_span_rate, (int, float))
            and isinstance(baseline_span_rate, (int, float))
            and sample_span_rate + 0.02 < baseline_span_rate
        ):
            quality_loss_reasons.append(
                "evidence_span_validity_more_than_2pp_below_baseline"
            )
        systematic_quality_loss = bool(quality_loss_reasons)
        if systematic_quality_loss:
            stop_reason = "audit_sample_systematic_quality_loss"
        after_counts = _table_counts(conn)
        changed_tables = sorted(
            name
            for name in set(before_counts) | set(after_counts)
            if before_counts.get(name, 0) != after_counts.get(name, 0)
        )
        unexpected_tables = sorted(set(changed_tables) - EXPECTED_MUTATION_TABLES)
        release_count_after = int(
            conn.execute("SELECT COUNT(*) FROM corpus_releases").fetchone()[0]
        )
        scale_count_after = int(
            conn.execute("SELECT COUNT(*) FROM pif_scale_gate_state_receipts").fetchone()[0]
        )
        paid_after = int(
            conn.execute(
                """
                SELECT COUNT(*) FROM pipeline_runs
                WHERE json_extract(parameters_json, '$.paid_api') = 1
                """
            ).fetchone()[0]
        )
        elapsed_seconds = time.monotonic() - started_monotonic
        calls = len(context_results) + len(label_results)
        tokens = sum(_usage_total(item.get("usage")) for item in context_results + label_results)
        validation_attempts = [
            item for item in label_results if item.get("first_attempt_validation")
        ]
        validation_failures = [
            item for item in validation_attempts
            if not item["first_attempt_validation"]["passed"]
        ]
        failure_kinds = Counter(
            item["first_attempt_validation"].get("error_kind") or "unknown"
            for item in validation_failures
        )
        repair_items = [
            item for item in label_results if item.get("deterministic_repair")
        ]
        manifest = {
            "schema_version": "pif_instrumented_backfill_manifest_v1",
            "run_id": run_id,
            "model": MODEL,
            "label_pack": LABEL_PACK,
            "episode_ids": sorted(set(completed_episode_ids)),
            "segment_ids": sorted(
                str(row["segment_id"]) for row in new_label_rows
            ),
            "label_ids": sorted(label_ids),
        }
        manifest["content_sha256"] = hashlib.sha256(
            dumps_json(manifest).encode("utf-8")
        ).hexdigest()
        manifest_path = output_root / "manifest.json"
        write_text_atomic(
            manifest_path,
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        )
        report = {
            "schema_version": "pif_instrumented_backfill_report_v1",
            "run_id": run_id,
            "started_at": started_at,
            "completed_at": now_iso(),
            "bounds": {
                "max_context_episodes": max_contexts,
                "max_label_segments": max_labels,
                "max_runtime_seconds": max_runtime_seconds,
                "concurrency": concurrency,
            },
            "stop_reason": stop_reason,
            "wall_time_seconds": elapsed_seconds,
            "episode_contexts_completed": sum(
                1 for item in context_results if item.get("status_ok")
            ),
            "segments_completed": len(label_ids),
            "provider_calls": calls,
            "tokens": tokens,
            "completions_per_hour": (
                (len(label_ids) + len(completed_episode_ids)) * 3600 / elapsed_seconds
                if elapsed_seconds
                else None
            ),
            "label_completions_per_hour": (
                len(label_ids) * 3600 / elapsed_seconds if elapsed_seconds else None
            ),
            "backlog_pending_labels_at_finish": int(
                conn.execute(
                    "SELECT COUNT(*) FROM jobs WHERE job_type='label_segment' AND status='pending'"
                ).fetchone()[0]
            ),
            "implied_days_to_clear": (
                int(
                    conn.execute(
                        "SELECT COUNT(*) FROM jobs WHERE job_type='label_segment' AND status='pending'"
                    ).fetchone()[0]
                )
                / (len(label_ids) * 86400 / elapsed_seconds)
                if label_ids and elapsed_seconds
                else None
            ),
            "first_attempt_validation": {
                "attempted": len(validation_attempts),
                "failed": len(validation_failures),
                "failure_rate": (
                    len(validation_failures) / len(validation_attempts)
                    if validation_attempts
                    else None
                ),
                "reasons_by_kind": dict(sorted(failure_kinds.items())),
            },
            "deterministic_repair": {
                "items_repaired": sum(
                    1
                    for item in repair_items
                    if int(item["deterministic_repair"]["repair_count"]) > 0
                ),
                "repair_rate": (
                    sum(
                        1
                        for item in repair_items
                        if int(item["deterministic_repair"]["repair_count"]) > 0
                    )
                    / len(repair_items)
                    if repair_items
                    else None
                ),
                "events_dropped_unresolved_evidence": sum(
                    int(item["deterministic_repair"]["events_dropped_unresolved_evidence"])
                    for item in repair_items
                ),
            },
            "new_labels": new_metrics,
            "historic_baseline": historical,
            "audit_sample": {
                "selection": "seeded_random_without_replacement",
                "seed": run_id,
                "sample_size": len(sample_rows),
                **audit_sample,
            },
            "historical_comparison": {
                "available": bool(historical.get("available")),
                "audit_pass_rate": {
                    "batch_sample": sample_pass_rate,
                    "historical": baseline_pass_rate,
                    "delta": (
                        sample_pass_rate - baseline_pass_rate
                        if sample_pass_rate is not None
                        and baseline_pass_rate is not None
                        else None
                    ),
                },
                "evidence_span_validity_rate": {
                    "batch_sample": sample_span_rate,
                    "historical": baseline_span_rate,
                    "delta": (
                        sample_span_rate - baseline_span_rate
                        if isinstance(sample_span_rate, (int, float))
                        and isinstance(baseline_span_rate, (int, float))
                        else None
                    ),
                },
                "systematic_quality_loss": systematic_quality_loss,
                "quality_loss_reasons": quality_loss_reasons,
            },
            "isolation": {
                "pipeline_lock_acquired": True,
                "changed_tables": changed_tables,
                "unexpected_changed_tables": unexpected_tables,
                "corpus_release_delta": release_count_after - release_count_before,
                "scale_gate_receipt_delta": scale_count_after - scale_count_before,
                "paid_api_telemetry_delta": paid_after - paid_before,
            },
            "manifest_path": str(manifest_path),
            "manifest_sha256": manifest["content_sha256"],
            "provider_lane": "codex_subscription",
            "model": MODEL,
            "label_pack": LABEL_PACK,
            "glm_initialized": False,
        }
        report["ok"] = not (
            unexpected_tables
            or report["isolation"]["corpus_release_delta"]
            or report["isolation"]["scale_gate_receipt_delta"]
            or report["isolation"]["paid_api_telemetry_delta"]
            or systematic_quality_loss
            or (len(label_ids) >= 30 and len(sample_rows) < 30)
            or (
                report["first_attempt_validation"]["failure_rate"] is not None
                and report["first_attempt_validation"]["failure_rate"] > 0.20
            )
        )
        report_path = output_root / "report.json"
        write_text_atomic(
            report_path,
            json.dumps(report, indent=2, sort_keys=True) + "\n",
        )
        conn.close()
        return {**report, "report_path": str(report_path)}


if __name__ == "__main__":
    print(json.dumps(run_instrumented_backfill(), indent=2, sort_keys=True))
