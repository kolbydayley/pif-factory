from __future__ import annotations

import concurrent.futures
import copy
import json
import os
import re
import shutil
import subprocess
import threading
import time
from pathlib import Path
from typing import Any

from . import db
from .paths import resolve_recorded_path, root, runs_dir
from .scale_ops import submit_reviewer_audit
from .util import now_iso
from .labels import repair_label_output_for_submission, validate_label_output
from .worker import run_jobs, segment_for_job, submit_label_output


def resolve_codex_binary() -> str:
    """Resolve Codex in interactive shells and minimal launchd environments."""

    configured = os.environ.get("PIF_CODEX_BIN")
    candidates = [
        Path(configured).expanduser() if configured else None,
        Path(found) if (found := shutil.which("codex")) else None,
        Path.home() / ".local" / "bin" / "codex",
    ]
    for candidate in candidates:
        if candidate and candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate.resolve())
    raise FileNotFoundError(
        "Codex CLI is unavailable; checked PIF_CODEX_BIN, PATH, and ~/.local/bin/codex"
    )


def _stdin_handoff_prompt(rendered_prompt: str, output_path: Path) -> str:
    return (
        "Complete the bounded task below. Write the final JSON object, and only "
        f"the JSON object, to {output_path}. Do not edit any other file.\n\n"
        + rendered_prompt
    )


def _usage_profile_from_jsonl(log_path: Path) -> dict[str, int] | None:
    usage_events: list[dict[str, Any]] = []
    try:
        with log_path.open(encoding="utf-8") as handle:
            for line in handle:
                try:
                    event = json.loads(line)
                except Exception:
                    continue
                if not isinstance(event, dict):
                    continue
                candidates = [event.get("usage")]
                item = event.get("item")
                if isinstance(item, dict):
                    candidates.append(item.get("usage"))
                for candidate in candidates:
                    if isinstance(candidate, dict) and any(
                        key in candidate
                        for key in ("input_tokens", "output_tokens", "total_tokens")
                    ):
                        usage_events.append(candidate)
    except OSError:
        return None
    if not usage_events:
        return None
    last = usage_events[-1]
    input_tokens = int(last.get("input_tokens") or 0)
    cached_input_tokens = int(last.get("cached_input_tokens") or 0)
    output_tokens = int(last.get("output_tokens") or 0)
    total_tokens = int(last.get("total_tokens") or 0) or (
        input_tokens + output_tokens
    )
    return {
        "usage_event_count": len(usage_events),
        "cumulative_billed_total_tokens": total_tokens,
        "cumulative_input_tokens": input_tokens,
        "cumulative_cached_input_tokens": cached_input_tokens,
        "cumulative_output_tokens": output_tokens,
        "last_turn_unique_input_tokens": max(
            0, input_tokens - cached_input_tokens
        ),
        "last_turn_unique_total_tokens": max(
            0, input_tokens - cached_input_tokens
        )
        + output_tokens,
    }


def _provider_pressure_signals(log_path: Path) -> list[str]:
    signals: list[str] = []
    try:
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return signals
    for line in lines:
        candidate = line
        try:
            event = json.loads(line)
        except Exception:
            event = None
        if isinstance(event, dict):
            item = event.get("item")
            event_type = str(event.get("type") or "").lower()
            item_type = (
                str(item.get("type") or "").lower()
                if isinstance(item, dict)
                else ""
            )
            if "error" not in event_type and "error" not in item_type:
                continue
            candidate = json.dumps(event, sort_keys=True)
        lowered = candidate.lower()
        rate_limit = bool(re.search(r"\brate[ _-]?limit(?:ed|ing)?\b", lowered))
        quota = bool(
            re.search(
                r"\b(?:quota (?:exceeded|exhausted)|"
                r"insufficient(?:_| )quota)\b",
                lowered,
            )
        )
        capacity = bool(
            re.search(
                r"\b(?:capacity (?:exceeded|exhausted)|"
                r"over capacity|at capacity)\b",
                lowered,
            )
        )
        too_many_requests = "too many requests" in lowered
        # The Aug 2026 outage signature: Codex subscription exhaustion. The
        # provider phrases it as a usage limit, not a quota or 429, so none of
        # the patterns above caught it and three full waves burned against it.
        usage_limit = bool(
            re.search(r"you.?ve hit your usage limit", lowered)
        )
        provider_429 = bool(
            re.search(r"\b429\b", lowered)
            and (rate_limit or quota or capacity or too_many_requests)
        )
        matches = []
        if usage_limit:
            matches.append("usage limit")
        if provider_429:
            matches.append("429")
        if rate_limit or too_many_requests:
            matches.append("rate limit")
        if quota:
            matches.append("quota")
        if capacity:
            matches.append("capacity")
        if matches:
            signals.append(",".join(matches))
    return sorted(set(signals))


def _usage_limit_retry_at(log_path: Path) -> str | None:
    """Extract the provider's "try again at ..." timestamp, verbatim text."""

    try:
        text = log_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None
    match = re.search(
        r"usage limit.*?try again at ([^.\n\"]+(?:\.[^.\n\"]+)*?)\.?(?:\n|$|\")",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    return match.group(1).strip()


def _quota_reclassification(
    status: str,
    pressure_signals: list[str],
    log_path: Path,
) -> tuple[str, str | None]:
    """Reclassify a failed call as quota exhaustion when the log proves it.

    Completed runs are never reclassified: an rc=0 transcript that merely
    mentions limits still produced output.
    """

    if status != "codex_exec_failed":
        return status, None
    if not any("usage limit" in signal for signal in pressure_signals):
        return status, None
    return "codex_usage_limit", _usage_limit_retry_at(log_path)


def _attempt_consumed(
    *,
    provider_call_started: bool,
    timed_out: bool,
    status: str,
) -> bool:
    """Whether a failure spends one of the job's durable attempts.

    A timeout proves neither bad input nor an invalid model result, and a
    quota-exhausted call proves nothing about the job at all — both return the
    job to pending without spending an attempt, so an outage cannot
    terminalize a queue.
    """

    if status in {
        "codex_usage_limit",
        "provider_quota_exhausted",
        "budget_cap_hit",
    }:
        return False
    return bool(provider_call_started) and not bool(timed_out)


_GLM_HANDOFF_MESSAGE = (
    "Follow the instructions in the attached file exactly. They are complete "
    "and self-contained. Reply with ONLY the final JSON output the "
    "instructions require - no prose, no code fences, no commentary."
)


def _resolve_opencode_binary() -> str:
    binary = shutil.which("opencode") or "/opt/homebrew/bin/opencode"
    return binary


def _run_glm_opencode(
    *,
    prompt_path: Path,
    output_path: Path,
    log_path: Path,
    model: str,
    timeout_seconds: int,
) -> dict[str, Any]:
    """Execute one claimed run on the GLM opencode lane.

    Honors the codex path's contract exactly: the rendered prompt file is the
    instruction source, the validated answer is written to the recorded
    output path, and the JSONL log carries a usage event the budget ledger
    meters unchanged. Status strings are shared with the codex path so
    post-processing needs no provider awareness.
    """

    from .glm_workhorse import (
        _decode_answer,
        _parse_opencode_stream,
        opencode_config,
    )

    item: dict[str, Any] = {
        "provider": "glm_opencode",
        "dispatch_mode": "opencode_run_file_handoff",
        "started_at": now_iso(),
    }
    env = os.environ.copy()
    env["OPENCODE_CONFIG_CONTENT"] = json.dumps(
        opencode_config(model), sort_keys=True
    )
    command = [
        _resolve_opencode_binary(),
        "run",
        "--pure",
        "--dir",
        str(log_path.parent),
        "--model",
        model,
        "--format",
        "json",
        _GLM_HANDOFF_MESSAGE,
        "--file",
        str(prompt_path),
    ]
    call_started = time.monotonic()
    try:
        completed = subprocess.run(
            command,
            env=env,
            text=True,
            errors="replace",
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=timeout_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired:
        item["returncode"] = None
        item["timed_out"] = True
        item["status"] = "codex_exec_timeout"
        item["provider_call_started"] = True
        log_path.write_text("", encoding="utf-8")
        item["call_wall_seconds"] = time.monotonic() - call_started
        return item
    except OSError as exc:
        item["returncode"] = None
        item["timed_out"] = False
        item["status"] = "codex_exec_launch_failed"
        item["provider_call_started"] = False
        item["error"] = f"{type(exc).__name__}: {str(exc)[:500]}"
        item["call_wall_seconds"] = time.monotonic() - call_started
        return item
    item["returncode"] = completed.returncode
    item["timed_out"] = False
    item["provider_call_started"] = True
    item["call_wall_seconds"] = time.monotonic() - call_started
    answer, finish, _stream_events = _parse_opencode_stream(
        completed.stdout or ""
    )
    # Persist the raw stream plus a synthetic usage event in the shape the
    # existing meter (_codex_usage_from_jsonl) already reads.
    log_lines = [(completed.stdout or "").rstrip("\n")]
    tokens = (
        finish.get("tokens")
        if isinstance(finish, dict) and isinstance(finish.get("tokens"), dict)
        else {}
    )
    cache = tokens.get("cache") if isinstance(tokens.get("cache"), dict) else {}
    usage_event = {
        "type": "turn.completed",
        "provider": "glm_opencode",
        "usage": {
            "input_tokens": int(tokens.get("input") or 0),
            "cached_input_tokens": int(cache.get("read") or 0),
            "output_tokens": int(tokens.get("output") or 0),
            "total_tokens": int(tokens.get("total") or 0),
        },
    }
    log_lines.append(json.dumps(usage_event, sort_keys=True))
    log_path.write_text(
        "\n".join(line for line in log_lines if line) + "\n", encoding="utf-8"
    )
    if completed.returncode != 0:
        item["status"] = "codex_exec_failed"
        return item
    try:
        payload, fence_removed = _decode_answer(answer)
    except Exception as exc:
        item["status"] = "codex_exec_failed"
        item["error"] = f"glm_answer_not_json: {str(exc)[:300]}"
        return item
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    item["fence_removed"] = bool(fence_removed)
    item["status"] = "codex_exec_completed"
    return item


def execute_claimed_label_runs(
    conn,
    *,
    lease_owner: str,
    limit: int,
    model: str,
    timeout_seconds: int = 900,
    audit: bool = True,
    concurrency: int = 1,
    capture_usage: bool = False,
    capture_validation: bool = False,
    budget_lane: str = "labels",
    provider: str = "codex",
) -> dict[str, Any]:
    concurrency = max(1, int(concurrency or 1))
    bounded_limit = max(0, int(limit))
    log_dir = runs_dir() / "headless_logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    # Daily subscription-token budget: checked before any dispatch (Kolby
    # ruling 2026-08-10 — hard 5M/day, KILL at 120%). Fail closed.
    from .subscription_budget import (
        budget_gate,
        record_usage,
        usage_tokens_from_item,
    )

    budget_day = now_iso()[:10]
    budget = budget_gate(
        conn,
        day=budget_day,
        additional_budget_db_paths=(
            root() / "data" / "factory.sqlite",
            root() / "work" / "attribution-lab" / "lab.sqlite",
        ),
    )
    rows = conn.execute(
        """
        SELECT jobs.id AS job_id, jobs.lease_owner, jobs.attempts,
               label_runs.id AS label_run_id,
               label_runs.prompt_path, label_runs.output_path
        FROM label_runs
        JOIN jobs ON jobs.id = label_runs.job_id
        WHERE label_runs.status = 'claimed'
          AND jobs.status = 'claimed'
          AND jobs.job_type = 'label_segment'
          AND jobs.lease_owner = ?
        ORDER BY jobs.id
        LIMIT ?
        """,
        (lease_owner, bounded_limit),
    ).fetchall()
    selected = len(rows)
    immediate_results: dict[int, dict[str, Any]] = {}
    executable_rows: list[dict[str, Any]] = []
    for raw_row in rows:
        row = dict(raw_row)
        job_id = int(row["job_id"])
        current = conn.execute(
            """
            SELECT jobs.status AS job_status, jobs.lease_owner, label_runs.status AS run_status
            FROM jobs
            JOIN label_runs ON label_runs.job_id = jobs.id
            WHERE jobs.id = ? AND label_runs.id = ?
            """,
            (job_id, row["label_run_id"]),
        ).fetchone()
        if not current or current["job_status"] != "claimed" or current["run_status"] != "claimed" or current["lease_owner"] != lease_owner:
            immediate_results[job_id] = {
                "job_id": str(job_id),
                "label_run_id": row["label_run_id"],
                "prompt_artifact": "local_prompt_file",
                "output_artifact": "local_output_file",
                "status": "stale_handoff_skipped",
                "completed_at": now_iso(),
            }
            continue
        executable_rows.append(row)

    quota_exhausted = threading.Event()
    quota_retry_at: list[str | None] = [None]

    def _execute_one(row: dict[str, Any]) -> dict[str, Any]:
        job_id = int(row["job_id"])
        if provider == "codex" and not budget["allowed"]:
            # Over the daily subscription cap (or KILL engaged): no call
            # starts, no attempt is spent, the job returns to pending.
            return {
                "job_id": str(job_id),
                "label_run_id": row["label_run_id"],
                "prompt_artifact": "local_prompt_file",
                "output_artifact": "local_output_file",
                "status": "budget_cap_hit",
                "provider_call_started": False,
                "timed_out": False,
                "returncode": None,
                "budget_refusal_reason": budget["reason"],
                "_provider_completed_monotonic": time.monotonic(),
                "completed_at": now_iso(),
                "provider_pressure_signals": [],
            }
        if quota_exhausted.is_set():
            # Another worker just proved the subscription is exhausted; do not
            # burn the rest of the wave against it. No call starts, no durable
            # attempt is spent, and the job returns to pending untouched.
            return {
                "job_id": str(job_id),
                "label_run_id": row["label_run_id"],
                "prompt_artifact": "local_prompt_file",
                "output_artifact": "local_output_file",
                "status": "provider_quota_exhausted",
                "provider_call_started": False,
                "timed_out": False,
                "returncode": None,
                "usage_limit_retry_at": quota_retry_at[0],
                "_provider_completed_monotonic": time.monotonic(),
                "completed_at": now_iso(),
                "provider_pressure_signals": ["usage limit"],
            }
        prompt_path = resolve_recorded_path(row["prompt_path"])
        output_path = resolve_recorded_path(row["output_path"])
        log_path = log_dir / f"{row['label_run_id']}.log"
        last_message_path = log_dir / f"{row['label_run_id']}.last.txt"
        if provider == "glm_opencode":
            glm_item = _run_glm_opencode(
                prompt_path=prompt_path,
                output_path=output_path,
                log_path=log_path,
                model=model,
                timeout_seconds=timeout_seconds,
            )
            glm_item.update(
                {
                    "job_id": str(job_id),
                    "label_run_id": row["label_run_id"],
                    "job_attempt_number": int(row.get("attempts") or 0),
                    "job_retry_count": max(
                        0, int(row.get("attempts") or 0) - 1
                    ),
                    "prompt_artifact": "local_prompt_file",
                    "output_artifact": "local_output_file",
                    "log_path": str(log_path),
                    "_provider_completed_monotonic": time.monotonic(),
                    "completed_at": now_iso(),
                    "provider_pressure_signals": _provider_pressure_signals(
                        log_path
                    ),
                }
            )
            from .efficient_backtest import _codex_usage_from_jsonl

            glm_item["usage"] = _codex_usage_from_jsonl(log_path)
            return glm_item
        rendered_prompt = prompt_path.read_text(encoding="utf-8")
        prompt = _stdin_handoff_prompt(rendered_prompt, output_path)
        item: dict[str, Any] = {
            "job_id": str(job_id),
            "label_run_id": row["label_run_id"],
            "job_attempt_number": int(row.get("attempts") or 0),
            "job_retry_count": max(0, int(row.get("attempts") or 0) - 1),
            "prompt_artifact": "local_prompt_file",
            "output_artifact": "local_output_file",
            "log_path": str(log_path),
            "dispatch_mode": "single_shot_stdin",
            "prompt_bytes": len(prompt.encode("utf-8")),
            "started_at": now_iso(),
        }
        call_started_monotonic = time.monotonic()
        with log_path.open("w", encoding="utf-8") as log_file:
            command = [
                    resolve_codex_binary(),
                    "exec",
                    "--ephemeral",
                    "-m",
                    model,
                    "-C",
                    str(root()),
                    "--sandbox",
                    "danger-full-access",
                    "--output-last-message",
                    str(last_message_path),
                    # Always JSON: the subscription budget ledger meters from
                    # the stream's usage events, so a text-mode call would be
                    # invisible to the daily cap.
                    "--json",
                ]
            command.append("-")
            try:
                completed = subprocess.run(
                    command,
                    input=prompt,
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    text=True,
                    timeout=timeout_seconds,
                    check=False,
                )
            except subprocess.TimeoutExpired:
                item["returncode"] = None
                item["timed_out"] = True
                item["status"] = "codex_exec_timeout"
                item["provider_call_started"] = True
            except OSError as exc:
                item["returncode"] = None
                item["timed_out"] = False
                item["status"] = "codex_exec_launch_failed"
                item["provider_call_started"] = False
                item["error"] = f"{type(exc).__name__}: {str(exc)[:500]}"
            else:
                item["returncode"] = completed.returncode
                item["timed_out"] = False
                item["provider_call_started"] = True
                item["status"] = (
                    "codex_exec_completed"
                    if completed.returncode == 0
                    else "codex_exec_failed"
                )
        item["call_wall_seconds"] = time.monotonic() - call_started_monotonic
        item["_provider_completed_monotonic"] = time.monotonic()
        item["completed_at"] = now_iso()
        item["provider_pressure_signals"] = _provider_pressure_signals(log_path)
        reclassified, retry_at = _quota_reclassification(
            str(item.get("status") or ""),
            item["provider_pressure_signals"],
            log_path,
        )
        if reclassified == "codex_usage_limit":
            item["status"] = reclassified
            item["usage_limit_retry_at"] = retry_at
            quota_retry_at[0] = quota_retry_at[0] or retry_at
            quota_exhausted.set()
        # Usage is always captured — the budget ledger meters every call.
        from .efficient_backtest import _codex_usage_from_jsonl

        item["usage"] = _codex_usage_from_jsonl(log_path)
        if capture_usage:
            item["usage_profile"] = _usage_profile_from_jsonl(log_path)
        return item

    # SQLite connections stay on the caller thread. Worker threads only run
    # independent subprocesses against already-claimed, lease-protected
    # handoffs. The caller consumes completed futures immediately, preserving
    # a single serialized SQLite writer while overlapping post-processing with
    # provider calls that are still running.
    def _postprocess_one(
        row: dict[str, Any],
        item: dict[str, Any],
    ) -> dict[str, Any]:
        job_id = int(row["job_id"])
        item["serial_queue_wait_seconds"] = max(
            0.0,
            time.monotonic()
            - float(item.pop("_provider_completed_monotonic", time.monotonic())),
        )
        item["validation_seconds"] = 0.0
        item["submission_seconds"] = 0.0
        item["audit_enqueue_seconds"] = 0.0
        item["audit_execution_seconds"] = 0.0
        item["failure_finalization_seconds"] = 0.0
        postprocess_started = time.monotonic()
        if item["status"] != "codex_exec_completed":
            phase_started = time.monotonic()
            consume_attempt = _attempt_consumed(
                provider_call_started=bool(
                    item.get("provider_call_started", True)
                ),
                timed_out=bool(item.get("timed_out")),
                status=str(item.get("status") or ""),
            )
            item["failure_finalization"] = _finalize_submission_failure(
                conn,
                job_id=job_id,
                label_run_id=row["label_run_id"],
                lease_owner=lease_owner,
                error=item["status"],
                consume_attempt=consume_attempt,
            )
            item["failure_finalization_seconds"] = (
                time.monotonic() - phase_started
            )
            item["postprocess_total_seconds"] = (
                time.monotonic() - postprocess_started
            )
            return item
        output_path = resolve_recorded_path(row["output_path"])
        try:
            validation_started = time.monotonic()
            if capture_validation:
                target_row = conn.execute(
                    "SELECT target_id, leased_until FROM jobs WHERE id = ?",
                    (job_id,),
                ).fetchone()
                item["lease_expired_before_submission"] = bool(
                    target_row["leased_until"]
                    and str(target_row["leased_until"]) < now_iso()
                )
                segment_context = segment_for_job(
                    conn,
                    {"target_id": target_row["target_id"]},
                )
                raw_output = json.loads(output_path.read_text(encoding="utf-8"))
                try:
                    validate_label_output(
                        "ai_discourse_v3_1",
                        raw_output,
                        segment_text=segment_context["segment_text"],
                    )
                    item["first_attempt_validation"] = {
                        "passed": True,
                        "error_kind": None,
                    }
                except Exception as validation_exc:
                    item["first_attempt_validation"] = {
                        "passed": False,
                        "error_kind": type(validation_exc).__name__,
                        "error": str(validation_exc)[:500],
                    }
                repaired = copy.deepcopy(raw_output)
                before_events = len(repaired.get("discourse_events") or [])
                metric_quarantines: list[dict[str, Any]] = []
                repair_count = repair_label_output_for_submission(
                    "ai_discourse_v3_1",
                    repaired,
                    segment_text=segment_context["segment_text"],
                    metric_quarantines=metric_quarantines,
                )
                after_events = len(repaired.get("discourse_events") or [])
                metric_failure_kinds = {
                    rule: sum(
                        rule in quarantine["failed_rules"]
                        for quarantine in metric_quarantines
                    )
                    for rule in sorted(
                        {
                            rule
                            for quarantine in metric_quarantines
                            for rule in quarantine["failed_rules"]
                        }
                    )
                }
                field_bearing_quarantines = sum(
                    any(
                        quarantine["original_metric"].get(field) not in (None, "")
                        for field in ("raw_text", "value", "unit", "comparator")
                    )
                    for quarantine in metric_quarantines
                )
                item["deterministic_repair"] = {
                    "repair_count": repair_count,
                    "blanked_metric_count": len(metric_quarantines),
                    "field_bearing_metric_quarantine_count": (
                        field_bearing_quarantines
                    ),
                    "direction_only_metric_quarantine_count": (
                        len(metric_quarantines) - field_bearing_quarantines
                    ),
                    "blanked_metric_failure_kinds": metric_failure_kinds,
                    "events_dropped_unresolved_evidence": max(
                        0, before_events - after_events
                    ),
                }
            item["validation_seconds"] = time.monotonic() - validation_started
            submission_started = time.monotonic()
            submission = submit_label_output(
                conn,
                job_id=job_id,
                output_json_path=str(output_path),
                worker_id=lease_owner,
                allow_expired=True,
            )
            item["submission_seconds"] = time.monotonic() - submission_started
            item["status"] = "submitted"
            item["submission"] = submission
            if audit:
                audit_enqueue_started = time.monotonic()
                audit_job_created = db.enqueue_job(
                    conn,
                    lane="quality",
                    job_type="audit_label",
                    target_id=submission["label_id"],
                    payload={
                        "label_pack": "ai_discourse_v3_1",
                        "model": model,
                        "headless_codex_label_run_id": row["label_run_id"],
                    },
                    priority=1,
                )
                conn.commit()
                item["audit_enqueue_seconds"] = (
                    time.monotonic() - audit_enqueue_started
                )
                audit_execution_started = time.monotonic()
                audit_result = run_jobs(
                    conn,
                    lane="quality",
                    limit=1,
                    model=model,
                    label_pack="ai_discourse_v3_1",
                    worker_id=f"{lease_owner}-audit",
                    claim_prompts=False,
                    job_types=("audit_label",),
                    max_label_prompts=0,
                )
                item["audit_execution_seconds"] = (
                    time.monotonic() - audit_execution_started
                )
                item["audit_job_created"] = audit_job_created
                item["audit_result"] = audit_result
        except Exception as exc:
            item["status"] = "submission_failed"
            item["error"] = str(exc)
            failure_started = time.monotonic()
            item["failure_finalization"] = _finalize_submission_failure(
                conn,
                job_id=job_id,
                label_run_id=row["label_run_id"],
                lease_owner=lease_owner,
                error=str(exc),
            )
            item["failure_finalization_seconds"] = (
                time.monotonic() - failure_started
            )
        item["postprocess_total_seconds"] = (
            time.monotonic() - postprocess_started
        )
        return item

    results: list[dict[str, Any]] = list(immediate_results.values())
    if executable_rows:
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = {
                executor.submit(_execute_one, row): row
                for row in executable_rows
            }
            for future in concurrent.futures.as_completed(futures):
                results.append(_postprocess_one(futures[future], future.result()))
    results.sort(key=lambda item: int(item["job_id"]))
    # Record actuals in the daily subscription ledger: every started call is
    # metered, whether it succeeded, failed, or hit the provider's limit.
    metered_tokens = sum(usage_tokens_from_item(item) for item in results)
    metered_calls = sum(
        1 for item in results if item.get("provider_call_started")
    )
    if metered_calls or metered_tokens:
        record_usage(
            conn,
            day=budget_day,
            provider_lane=(
                "codex_subscription"
                if provider == "codex"
                else "opencode_glm"
            ),
            lane=budget_lane,
            run_id=lease_owner,
            tokens=metered_tokens,
            provider_calls=metered_calls,
        )
        conn.commit()
    timing_fields = (
        "serial_queue_wait_seconds",
        "validation_seconds",
        "submission_seconds",
        "audit_enqueue_seconds",
        "audit_execution_seconds",
        "failure_finalization_seconds",
        "postprocess_total_seconds",
    )
    return {
        "ok": all(item.get("status") in {"submitted", "stale_handoff_skipped"} for item in results),
        "lease_owner": lease_owner,
        "model": model,
        "provider": provider,
        "concurrency": concurrency,
        "selected": selected,
        "processed": len(results),
        "submitted": sum(1 for item in results if item.get("status") == "submitted"),
        "skipped": sum(1 for item in results if item.get("status") == "stale_handoff_skipped"),
        "failed": sum(1 for item in results if item.get("status") not in {"submitted", "stale_handoff_skipped"}),
        "provider_quota_exhausted": quota_exhausted.is_set(),
        "usage_limit_retry_at": quota_retry_at[0],
        "quota_skipped": sum(
            1
            for item in results
            if item.get("status") == "provider_quota_exhausted"
        ),
        "budget": {
            **budget,
            "lane": budget_lane,
            "metered_tokens": metered_tokens,
            "metered_calls": metered_calls,
        },
        "budget_cap_hit": sum(
            1 for item in results if item.get("status") == "budget_cap_hit"
        ),
        "timing_profile": {
            "serial_section_wall_seconds": sum(
                float(item.get("postprocess_total_seconds") or 0.0)
                for item in results
            ),
            "lock_wait_seconds": 0.0,
            **{
                f"{field}_total": sum(
                    float(item.get(field) or 0.0) for item in results
                )
                for field in timing_fields
            },
        },
        "results": results,
    }


def _finalize_submission_failure(
    conn,
    *,
    job_id: int,
    label_run_id: str,
    lease_owner: str,
    error: str,
    consume_attempt: bool = True,
) -> dict[str, Any]:
    """Rollback partial submission state and release or fail the exact handoff."""

    conn.rollback()
    current = conn.execute(
        """
        SELECT jobs.status AS job_status,
               jobs.attempts,
               jobs.max_attempts,
               jobs.lease_owner,
               label_runs.status AS run_status
        FROM jobs
        JOIN label_runs ON label_runs.job_id = jobs.id
        WHERE jobs.id = ?
          AND label_runs.id = ?
        """,
        (job_id, label_run_id),
    ).fetchone()
    if (
        current is None
        or current["job_status"] != "claimed"
        or current["run_status"] != "claimed"
        or current["lease_owner"] != lease_owner
    ):
        return {
            "finalized": False,
            "reason": "handoff_state_changed",
        }
    ts = now_iso()
    conn.execute(
        """
        UPDATE label_runs
        SET status = 'failed',
            error = ?,
            completed_at = ?,
            updated_at = ?
        WHERE id = ?
          AND job_id = ?
          AND status = 'claimed'
        """,
        (error, ts, ts, label_run_id, job_id),
    )
    resulting_attempts = int(current["attempts"])
    if not consume_attempt:
        resulting_attempts = max(0, resulting_attempts - 1)
    terminal = (
        consume_attempt
        and resulting_attempts >= int(current["max_attempts"])
    )
    conn.execute(
        """
        UPDATE jobs
        SET status = ?,
            error = ?,
            attempts = ?,
            lease_owner = NULL,
            leased_until = NULL,
            updated_at = ?
        WHERE id = ?
          AND status = 'claimed'
          AND lease_owner = ?
        """,
        (
            "failed" if terminal else "pending",
            error,
            resulting_attempts,
            ts,
            job_id,
            lease_owner,
        ),
    )
    conn.commit()
    return {
        "finalized": True,
        "job_status": "failed" if terminal else "pending",
        "label_run_status": "failed",
        "attempt_consumed": consume_attempt,
        "resulting_attempts": resulting_attempts,
    }


def execute_pending_reviewer_audits(
    conn,
    *,
    patch_tag: str | None,
    limit: int,
    model: str,
    timeout_seconds: int = 1200,
    concurrency: int = 1,
) -> dict[str, Any]:
    concurrency = max(1, int(concurrency or 1))
    params: list[Any] = [model]
    patch_sql = ""
    if patch_tag:
        patch_sql = "AND patch_tag = ?"
        params.append(patch_tag)
    params.append(limit)
    rows = conn.execute(
        f"""
        SELECT id, prompt_path, output_path, patch_tag
        FROM reviewer_audits
        WHERE status = 'pending'
          AND model = ?
          {patch_sql}
        ORDER BY created_at, id
        LIMIT ?
        """,
        params,
    ).fetchall()
    log_dir = runs_dir() / "headless_logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    claimed_rows = []
    for row in rows:
        ts = now_iso()
        updated = conn.execute(
            """
            UPDATE reviewer_audits
            SET status = 'claimed',
                updated_at = ?
            WHERE id = ?
              AND status = 'pending'
            """,
            (ts, row["id"]),
        ).rowcount
        if updated:
            claimed_rows.append(dict(row))
    conn.commit()

    def _execute_one(row: dict[str, Any]) -> dict[str, Any]:
        audit_id = row["id"]
        prompt_path = resolve_recorded_path(row["prompt_path"])
        output_path = resolve_recorded_path(row["output_path"])
        log_path = log_dir / f"{audit_id}.reviewer.log"
        last_message_path = log_dir / f"{audit_id}.reviewer.last.txt"
        rendered_prompt = prompt_path.read_text(encoding="utf-8")
        prompt = _stdin_handoff_prompt(rendered_prompt, output_path)
        worker_conn = db.connect()
        current = worker_conn.execute("SELECT status FROM reviewer_audits WHERE id = ?", (audit_id,)).fetchone()
        if not current or current["status"] != "claimed":
            worker_conn.close()
            return {"audit_id": audit_id, "status": "stale_audit_skipped"}
        started_at = now_iso()
        with log_path.open("w", encoding="utf-8") as log_file:
            completed = subprocess.run(
                [
                    resolve_codex_binary(),
                    "exec",
                    "--ephemeral",
                    "-m",
                    model,
                    "-C",
                    str(root()),
                    "--sandbox",
                    "danger-full-access",
                    "--output-last-message",
                    str(last_message_path),
                    # Always JSON so the subscription budget ledger can meter
                    # reviewer audits like every other lane.
                    "--json",
                    "-",
                ],
                input=prompt,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=timeout_seconds,
                check=False,
            )
        item: dict[str, Any] = {
            "audit_id": audit_id,
            "returncode": completed.returncode,
            "prompt_artifact": "local_reviewer_prompt_file",
            "output_artifact": "local_reviewer_output_file",
            "log_path": str(log_path),
            "dispatch_mode": "single_shot_stdin",
            "prompt_bytes": len(prompt.encode("utf-8")),
            "started_at": started_at,
            "completed_at": now_iso(),
        }
        if completed.returncode != 0:
            worker_conn.execute(
                "UPDATE reviewer_audits SET status = 'failed', updated_at = ? WHERE id = ?",
                (now_iso(), audit_id),
            )
            worker_conn.commit()
            worker_conn.close()
            item["status"] = "codex_exec_failed"
            return item
        try:
            item["submission"] = submit_reviewer_audit(worker_conn, audit_id=audit_id, output_json_path=str(output_path))
            item["status"] = "submitted"
        except Exception as exc:
            worker_conn.execute(
                "UPDATE reviewer_audits SET status = 'failed', updated_at = ? WHERE id = ?",
                (now_iso(), audit_id),
            )
            worker_conn.commit()
            item["status"] = "submission_failed"
            item["error"] = str(exc)
        worker_conn.close()
        return item

    results: list[dict[str, Any]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(_execute_one, row) for row in claimed_rows]
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())
    results.sort(key=lambda item: item.get("started_at", ""))
    # Meter reviewer audits into the daily subscription ledger like every
    # other lane (Kolby ruling 2026-08-10).
    from .efficient_backtest import _codex_usage_from_jsonl
    from .subscription_budget import record_usage

    audit_tokens = 0
    audit_calls = 0
    for row in claimed_rows:
        audit_log = log_dir / f"{row['id']}.reviewer.log"
        if not audit_log.exists():
            continue
        audit_calls += 1
        usage = _codex_usage_from_jsonl(audit_log)
        if usage:
            audit_tokens += int(usage.get("total_tokens") or 0)
    if audit_calls or audit_tokens:
        record_usage(
            conn,
            day=now_iso()[:10],
            provider_lane="codex_subscription",
            lane="reviewer_audits",
            run_id=patch_tag or "reviewer_audits",
            tokens=audit_tokens,
            provider_calls=audit_calls,
        )
        conn.commit()
    return {
        "ok": all(item.get("status") in {"submitted", "stale_audit_skipped"} for item in results),
        "model": model,
        "patch_tag": patch_tag,
        "selected": len(rows),
        "claimed": len(claimed_rows),
        "concurrency": concurrency,
        "processed": len(results),
        "submitted": sum(1 for item in results if item.get("status") == "submitted"),
        "skipped": sum(1 for item in results if item.get("status") == "stale_audit_skipped"),
        "failed": sum(1 for item in results if item.get("status") not in {"submitted", "stale_audit_skipped"}),
        "results": results,
    }
