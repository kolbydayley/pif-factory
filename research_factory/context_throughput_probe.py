from __future__ import annotations

import hashlib
import json
import math
import time
from collections import Counter
from pathlib import Path
from typing import Any

from . import db
from .daily_cycle import _cost_gate
from .instrumented_backfill import (
    EXPECTED_MUTATION_TABLES,
    MAX_BACKFILL_TOKENS,
    _add_readiness_totals,
    _run_context_batch,
    _table_counts,
)
from .orchestrator import pipeline_lock
from .paths import root
from .util import dumps_json, now_iso, stable_id, write_text_atomic
from .worker import run_jobs


MODEL = "gpt-5.5"
LABEL_PACK = "ai_discourse_v3_1"
CONTEXT_LEASE_SECONDS = 45 * 60
CONTEXT_CLAIM_WAVE_SIZE = 80


def context_wave_timing(
    *,
    wave_size: int,
    concurrency: int,
    call_seconds: float,
) -> dict[str, float | int | bool]:
    if wave_size < 1 or concurrency < 1 or call_seconds <= 0:
        raise ValueError("wave timing inputs must be positive")
    rounds = math.ceil(wave_size / concurrency)
    final_job_wait_seconds = max(0, rounds - 1) * call_seconds
    wave_completion_seconds = rounds * call_seconds
    return {
        "wave_size": wave_size,
        "concurrency": concurrency,
        "rounds": rounds,
        "call_seconds": call_seconds,
        "final_job_wait_seconds": final_job_wait_seconds,
        "wave_completion_seconds": wave_completion_seconds,
        "inside_lease": wave_completion_seconds < CONTEXT_LEASE_SECONDS,
        "lease_headroom_seconds": CONTEXT_LEASE_SECONDS
        - wave_completion_seconds,
    }


def context_job_waves(
    job_ids: tuple[int, ...] | list[int],
    *,
    wave_size: int = CONTEXT_CLAIM_WAVE_SIZE,
) -> list[tuple[int, ...]]:
    if wave_size < 1:
        raise ValueError("context claim wave size must be positive")
    selected = tuple(int(job_id) for job_id in job_ids)
    return [
        selected[index : index + wave_size]
        for index in range(0, len(selected), wave_size)
    ]


def evaluate_context_campaign_gate(report: dict[str, Any]) -> dict[str, Any]:
    selected = int(report.get("selected_jobs") or 0)
    attempted = int((report.get("validation") or {}).get("attempted") or 0)
    failed = int((report.get("validation") or {}).get("failed") or 0)
    validation_rate = (
        float((report.get("validation") or {}).get("pass_rate"))
        if (report.get("validation") or {}).get("pass_rate") is not None
        else (1.0 if selected == 0 else 0.0)
    )
    failure_rate = failed / attempted if attempted else 0.0
    diagnostics = report.get("execution_diagnostics") or {}
    isolation = report.get("isolation") or {}
    cost_gate = report.get("cost_gate") or {}
    reasons: list[str] = []
    if attempted != selected:
        reasons.append("campaign_accounting_incomplete")
    if validation_rate < 0.95:
        reasons.append("validation_below_immediate_halt_floor")
    elif validation_rate < 0.99:
        reasons.append("validation_below_campaign_gate")
    if failure_rate > 0.02:
        reasons.append("failure_rate_above_two_percent")
    if int(diagnostics.get("timeouts") or 0):
        reasons.append("timeouts")
    if int(diagnostics.get("expired_leases") or 0):
        reasons.append("lease_expiries")
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
    if any(int(value or 0) for value in (isolation.get("protected_table_deltas") or {}).values()):
        reasons.append("protected_table_deltas")
    return {
        "passed": not reasons,
        "reasons": reasons,
        "validation_rate": validation_rate,
        "failure_rate": failure_rate,
        "validation_continue_threshold": 0.99,
        "validation_immediate_halt_threshold": 0.95,
        "failure_rate_halt_threshold": 0.02,
    }


def _usage_total(item: dict[str, Any]) -> int:
    profile = item.get("usage_profile") or {}
    return int(profile.get("cumulative_billed_total_tokens") or 0)


def run_context_throughput_probe(
    *,
    max_contexts: int = 20,
    concurrency: int = 10,
    max_runtime_seconds: int = 2_400,
) -> dict[str, Any]:
    if not 1 <= max_contexts <= 350:
        raise ValueError("context campaign size must be between 1 and 350 jobs")
    if concurrency != 10:
        raise ValueError("the authorized context probe concurrency is exactly 10")
    if max_runtime_seconds not in {2_400, 3_600}:
        raise ValueError("context runtime must be 2400 or 3600 seconds")

    started_at = now_iso()
    started_monotonic = time.monotonic()
    run_id = stable_id(started_at, "episode-context-throughput-probe", prefix="pectp_")
    output_root = root() / "work" / "pif-ops" / "context-probes" / run_id
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
            before_counts = _table_counts(conn)
            protected_before = {
                name: int(conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0])
                for name in (
                    "labels",
                    "corpus_releases",
                    "pif_daily_runs",
                    "pif_scale_gate_state_receipts",
                    "pipeline_runs",
                )
            }
            paid_before = int(
                conn.execute(
                    """
                    SELECT COUNT(*) FROM pipeline_runs
                    WHERE json_extract(parameters_json, '$.paid_api') = 1
                    """
                ).fetchone()[0]
            )
            pending_at_start = int(
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
            expected_calls = min(max_contexts, pending_at_start)
            selected = conn.execute(
                """
                SELECT id, target_id
                FROM jobs
                WHERE lane = 'podcast'
                  AND job_type = 'episode_context'
                  AND status = 'pending'
                  AND json_extract(payload_json, '$.label_pack') = ?
                  AND json_extract(payload_json, '$.model') = ?
                ORDER BY priority, id
                LIMIT ?
                """,
                (LABEL_PACK, MODEL, max_contexts),
            ).fetchall()
            selected_job_ids = tuple(int(row["id"]) for row in selected)
            selected_episode_ids = tuple(str(row["target_id"]) for row in selected)

            ledger = {
                "schema_version": "pif_episode_context_probe_budget_ledger_v1",
                "run_id": run_id,
                "declared_at": now_iso(),
                "model": MODEL,
                "provider_lane": "codex_subscription",
                "bounds": {
                    "max_context_episodes": max_contexts,
                    "max_provider_calls": max_contexts * 2,
                    "max_total_tokens": MAX_BACKFILL_TOKENS,
                    "max_runtime_seconds": max_runtime_seconds,
                    "concurrency": concurrency,
                    "claim_wave_size": CONTEXT_CLAIM_WAVE_SIZE,
                    "lease_seconds": CONTEXT_LEASE_SECONDS,
                    "max_corrective_retries_per_job": 1,
                },
                "cumulative_spend_before_first_call": {
                    "provider_calls": 0,
                    "tokens": 0,
                },
            }
            ledger["content_sha256"] = hashlib.sha256(
                dumps_json(ledger).encode("utf-8")
            ).hexdigest()
            ledger_path = output_root / "budget-ledger.json"
            write_text_atomic(
                ledger_path,
                json.dumps(ledger, indent=2, sort_keys=True) + "\n",
            )

            worker_id = f"{run_id}-context"
            readiness_totals: dict[str, float | int] = {
                "checked": 0,
                "already_hydrated": 0,
                "materialized_on_demand": 0,
                "materialization_wall_seconds": 0.0,
            }
            provider_started = time.monotonic()
            results: list[dict[str, Any]] = []
            prepared_job_ids: list[int] = []
            wave_reports: list[dict[str, Any]] = []
            for wave_index, wave_job_ids in enumerate(
                context_job_waves(selected_job_ids),
                start=1,
            ):
                remaining = max_runtime_seconds - (
                    time.monotonic() - started_monotonic
                )
                if remaining <= 0:
                    break
                wave_started = time.monotonic()
                prepared = run_jobs(
                    conn,
                    lane="podcast",
                    limit=len(wave_job_ids),
                    model=MODEL,
                    label_pack=LABEL_PACK,
                    worker_id=worker_id,
                    claim_prompts=True,
                    job_types=("episode_context",),
                    max_label_prompts=len(wave_job_ids),
                    job_ids=wave_job_ids,
                )
                _add_readiness_totals(readiness_totals, prepared)
                wave_prepared_job_ids = [
                    int(item["job_id"])
                    for item in prepared["details"]
                    if item.get("prompt")
                ]
                prepared_job_ids.extend(wave_prepared_job_ids)
                wave_results = _run_context_batch(
                    wave_prepared_job_ids,
                    worker_id,
                    timeout=min(900, max(1, int(remaining))),
                    concurrency=concurrency,
                )
                results.extend(wave_results)
                wave_reports.append(
                    {
                        "wave_index": wave_index,
                        "selected_jobs": len(wave_job_ids),
                        "claimed_jobs": len(wave_prepared_job_ids),
                        "completed_jobs": sum(
                            bool(item.get("status_ok"))
                            for item in wave_results
                        ),
                        "provider_calls": sum(
                            int(item.get("provider_calls_this_invocation") or 1)
                            for item in wave_results
                        ),
                        "wall_time_seconds": time.monotonic() - wave_started,
                        "lease_expiries": sum(
                            bool(item.get("lease_expired_before_submission"))
                            for item in wave_results
                        ),
                    }
                )
            provider_wall_seconds = time.monotonic() - provider_started

            call_durations = [
                float(item["call_wall_seconds"])
                for item in results
                if isinstance(item.get("call_wall_seconds"), (int, float))
            ]
            completed = [item for item in results if item.get("status_ok")]
            failures = [item for item in results if not item.get("status_ok")]
            timeouts = [item for item in results if item.get("timed_out")]
            lease_expiries = [
                item
                for item in results
                if item.get("lease_expired_before_submission")
            ]
            pressure_items = [
                item for item in results if item.get("provider_pressure_signals")
            ]
            failure_reasons = Counter(
                str(
                    item.get("submission_error")
                    or item.get("validation_error_kind")
                    or (
                        "timeout"
                        if item.get("timed_out")
                        else "invalid_json"
                        if not item.get("json_ok")
                        else item.get("state")
                        or "unknown"
                    )
                )
                for item in failures
            )
            elapsed_seconds = time.monotonic() - started_monotonic
            completions_per_hour = (
                len(completed) * 3600 / elapsed_seconds
                if completed and elapsed_seconds
                else None
            )
            pending_at_finish = int(
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

            after_counts = _table_counts(conn)
            changed_tables = sorted(
                name
                for name in set(before_counts) | set(after_counts)
                if before_counts.get(name, 0) != after_counts.get(name, 0)
            )
            unexpected_tables = sorted(
                set(changed_tables) - EXPECTED_MUTATION_TABLES
            )
            protected_after = {
                name: int(conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0])
                for name in protected_before
            }
            protected_deltas = {
                name: protected_after[name] - protected_before[name]
                for name in protected_before
            }
            paid_after = int(
                conn.execute(
                    """
                    SELECT COUNT(*) FROM pipeline_runs
                    WHERE json_extract(parameters_json, '$.paid_api') = 1
                    """
                ).fetchone()[0]
            )
            release_row = conn.execute(
                "SELECT id FROM corpus_releases ORDER BY created_at DESC, id DESC LIMIT 1"
            ).fetchone()
            cost_gate = _cost_gate(
                conn,
                release_id=str(release_row["id"]) if release_row else None,
            )

            manifest = {
                "schema_version": "pif_episode_context_probe_manifest_v1",
                "run_id": run_id,
                "model": MODEL,
                "label_pack": LABEL_PACK,
                "selected_job_ids": list(selected_job_ids),
                "selected_episode_ids": list(selected_episode_ids),
                "completed_episode_ids": sorted(
                    str(item["episode_id"]) for item in completed
                ),
                "episode_context_run_ids": sorted(
                    str(item["episode_context_run_id"])
                    for item in results
                    if item.get("episode_context_run_id")
                ),
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
                "schema_version": "pif_episode_context_throughput_probe_v1",
                "run_id": run_id,
                "started_at": started_at,
                "completed_at": now_iso(),
                "bounds": ledger["bounds"],
                "model": MODEL,
                "provider_lane": "codex_subscription",
                "selected_jobs": len(selected_job_ids),
                "prepared_calls": len(prepared_job_ids),
                "provider_calls": sum(
                    int(item.get("provider_calls_this_invocation") or 1)
                    for item in results
                ),
                "completed_contexts": len(completed),
                "wall_time_seconds": elapsed_seconds,
                "provider_execution_wall_seconds": provider_wall_seconds,
                "completions_per_hour": completions_per_hour,
                "pending_contexts_at_start": pending_at_start,
                "pending_contexts_at_finish": pending_at_finish,
                "implied_hours_to_clear_starting_backlog": (
                    pending_at_start / completions_per_hour
                    if completions_per_hour
                    else None
                ),
                "token_accounting_complete": all(
                    isinstance(item.get("usage_profile"), dict)
                    for item in results
                ),
                "tokens": sum(_usage_total(item) for item in results),
                "token_usage_by_call": [
                    {
                        "job_id": item.get("job_id"),
                        "episode_id": item.get("episode_id"),
                        "episode_context_run_id": item.get(
                            "episode_context_run_id"
                        ),
                        "usage": item.get("usage"),
                        "usage_profile": item.get("usage_profile"),
                        "call_wall_seconds": item.get("call_wall_seconds"),
                        "job_attempt_number": item.get("job_attempt_number"),
                        "job_retry_count": item.get("job_retry_count"),
                        "provider_retry_count": item.get(
                            "provider_retry_count", 0
                        ),
                        "provider_calls_this_invocation": item.get(
                            "provider_calls_this_invocation", 1
                        ),
                        "timed_out": bool(item.get("timed_out")),
                        "lease_expired_before_submission": bool(
                            item.get("lease_expired_before_submission")
                        ),
                        "provider_pressure_signals": item.get(
                            "provider_pressure_signals", []
                        ),
                    }
                    for item in results
                ],
                "validation": {
                    "attempted": len(results),
                    "passed": len(completed),
                    "failed": len(failures),
                    "pass_rate": (
                        len(completed) / len(results) if results else None
                    ),
                    "failure_reasons": dict(sorted(failure_reasons.items())),
                    "structure_validity_rate": (
                        len(completed) / len(results) if results else None
                    ),
                },
                "audit": {
                    "status": "not_applicable",
                    "reason": "episode_context has no independent audit lane",
                },
                "execution_diagnostics": {
                    "claim_waves": wave_reports,
                    "call_wall_seconds": {
                        "count": len(call_durations),
                        "minimum": min(call_durations) if call_durations else None,
                        "maximum": max(call_durations) if call_durations else None,
                        "mean": (
                            sum(call_durations) / len(call_durations)
                            if call_durations
                            else None
                        ),
                    },
                    "retry_calls": sum(
                        1
                        for item in results
                        if int(item.get("provider_retry_count") or 0) > 0
                    ),
                    "retry_count": sum(
                        int(item.get("provider_retry_count") or 0)
                        for item in results
                    ),
                    "timeouts": len(timeouts),
                    "expired_leases": len(lease_expiries),
                    "provider_pressure_signal_calls": len(pressure_items),
                    "provider_pressure_signals": sorted(
                        {
                            signal
                            for item in pressure_items
                            for signal in item.get(
                                "provider_pressure_signals", []
                            )
                        }
                    ),
                },
                "on_demand_hydration": {
                    **readiness_totals,
                    "hydration_wall_share": (
                        float(
                            readiness_totals["materialization_wall_seconds"]
                        )
                        / elapsed_seconds
                        if elapsed_seconds
                        else None
                    ),
                },
                "cost_gate": cost_gate,
                "isolation": {
                    "pipeline_lock_acquired": True,
                    "changed_tables": changed_tables,
                    "unexpected_changed_tables": unexpected_tables,
                    "protected_table_deltas": protected_deltas,
                    "paid_api_telemetry_delta": paid_after - paid_before,
                },
                "manifest_path": str(manifest_path),
                "manifest_sha256": manifest["content_sha256"],
                "budget_ledger_path": str(ledger_path),
                "budget_ledger_sha256": ledger["content_sha256"],
            }
            report["series_gate"] = evaluate_context_campaign_gate(report)
            report["ok"] = bool(
                len(selected_job_ids) == expected_calls
                and report["series_gate"]["passed"]
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
        finally:
            conn.close()
