from __future__ import annotations

import concurrent.futures
import copy
import json
import subprocess
from pathlib import Path
from typing import Any

from . import db
from .paths import root, runs_dir
from .scale_ops import submit_reviewer_audit
from .util import now_iso
from .labels import repair_label_output_for_submission, validate_label_output
from .worker import run_jobs, segment_for_job, submit_label_output


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
) -> dict[str, Any]:
    concurrency = max(1, int(concurrency or 1))
    bounded_limit = max(0, int(limit))
    log_dir = runs_dir() / "headless_logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    rows = conn.execute(
        """
        SELECT jobs.id AS job_id, jobs.lease_owner, label_runs.id AS label_run_id,
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

    def _execute_one(row: dict[str, Any]) -> dict[str, Any]:
        job_id = int(row["job_id"])
        prompt_path = Path(row["prompt_path"]).expanduser().resolve()
        output_path = Path(row["output_path"]).expanduser().resolve()
        log_path = log_dir / f"{row['label_run_id']}.log"
        last_message_path = log_dir / f"{row['label_run_id']}.last.txt"
        prompt = (
            "You are a bounded Research Intelligence Factory extraction worker. "
            f"Read {prompt_path} completely and perform the extraction exactly as instructed. "
            f"Write the final JSON object, and only the JSON object, to {output_path}. "
            "Do not edit any other file. Do not include markdown fences or commentary."
        )
        item: dict[str, Any] = {
            "job_id": str(job_id),
            "label_run_id": row["label_run_id"],
            "prompt_artifact": "local_prompt_file",
            "output_artifact": "local_output_file",
            "log_path": str(log_path),
            "started_at": now_iso(),
        }
        with log_path.open("w", encoding="utf-8") as log_file:
            command = [
                    "codex",
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
                ]
            if capture_usage:
                command.append("--json")
            command.append(prompt)
            completed = subprocess.run(
                command,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=timeout_seconds,
                check=False,
            )
        item["returncode"] = completed.returncode
        item["completed_at"] = now_iso()
        if completed.returncode != 0:
            item["status"] = "codex_exec_failed"
        else:
            item["status"] = "codex_exec_completed"
        if capture_usage:
            from .efficient_backtest import _codex_usage_from_jsonl

            item["usage"] = _codex_usage_from_jsonl(log_path)
        return item

    # SQLite connections stay on the caller thread.  Worker threads only run
    # independent subprocesses against already-claimed, lease-protected
    # handoffs; submission and audit mutations are serialized below.
    executed_results: dict[int, dict[str, Any]] = {}
    if executable_rows:
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = {
                executor.submit(_execute_one, row): int(row["job_id"])
                for row in executable_rows
            }
            for future in concurrent.futures.as_completed(futures):
                executed_results[futures[future]] = future.result()

    results: list[dict[str, Any]] = []
    for raw_row in rows:
        row = dict(raw_row)
        job_id = int(row["job_id"])
        if job_id in immediate_results:
            results.append(immediate_results[job_id])
            continue
        item = executed_results[job_id]
        if item["status"] != "codex_exec_completed":
            results.append(item)
            continue
        output_path = Path(row["output_path"]).expanduser().resolve()
        try:
            if capture_validation:
                target_row = conn.execute(
                    "SELECT target_id FROM jobs WHERE id = ?",
                    (job_id,),
                ).fetchone()
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
                repair_count = repair_label_output_for_submission(
                    "ai_discourse_v3_1",
                    repaired,
                    segment_text=segment_context["segment_text"],
                )
                after_events = len(repaired.get("discourse_events") or [])
                item["deterministic_repair"] = {
                    "repair_count": repair_count,
                    "events_dropped_unresolved_evidence": max(
                        0, before_events - after_events
                    ),
                }
            submission = submit_label_output(
                conn,
                job_id=job_id,
                output_json_path=str(output_path),
                worker_id=lease_owner,
                allow_expired=True,
            )
            item["status"] = "submitted"
            item["submission"] = submission
            if audit:
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
                item["audit_job_created"] = audit_job_created
                item["audit_result"] = audit_result
        except Exception as exc:
            item["status"] = "submission_failed"
            item["error"] = str(exc)
            item["failure_finalization"] = _finalize_submission_failure(
                conn,
                job_id=job_id,
                label_run_id=row["label_run_id"],
                lease_owner=lease_owner,
                error=str(exc),
            )
        results.append(item)
    return {
        "ok": all(item.get("status") in {"submitted", "stale_handoff_skipped"} for item in results),
        "lease_owner": lease_owner,
        "model": model,
        "concurrency": concurrency,
        "selected": selected,
        "processed": len(results),
        "submitted": sum(1 for item in results if item.get("status") == "submitted"),
        "skipped": sum(1 for item in results if item.get("status") == "stale_handoff_skipped"),
        "failed": sum(1 for item in results if item.get("status") not in {"submitted", "stale_handoff_skipped"}),
        "results": results,
    }


def _finalize_submission_failure(
    conn,
    *,
    job_id: int,
    label_run_id: str,
    lease_owner: str,
    error: str,
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
    terminal = int(current["attempts"]) >= int(current["max_attempts"])
    conn.execute(
        """
        UPDATE jobs
        SET status = ?,
            error = ?,
            lease_owner = NULL,
            leased_until = NULL,
            updated_at = ?
        WHERE id = ?
          AND status = 'claimed'
          AND lease_owner = ?
        """,
        ("failed" if terminal else "pending", error, ts, job_id, lease_owner),
    )
    conn.commit()
    return {
        "finalized": True,
        "job_status": "failed" if terminal else "pending",
        "label_run_status": "failed",
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
        prompt_path = Path(row["prompt_path"]).expanduser().resolve()
        output_path = Path(row["output_path"]).expanduser().resolve()
        log_path = log_dir / f"{audit_id}.reviewer.log"
        last_message_path = log_dir / f"{audit_id}.reviewer.last.txt"
        prompt = (
            "You are a bounded Research Intelligence Factory semantic reviewer. "
            f"Read {prompt_path} completely and perform the reviewer audit exactly as instructed. "
            f"Write the final JSON object, and only the JSON object, to {output_path}. "
            "Do not edit any other file. Do not include markdown fences or commentary."
        )
        worker_conn = db.connect()
        current = worker_conn.execute("SELECT status FROM reviewer_audits WHERE id = ?", (audit_id,)).fetchone()
        if not current or current["status"] != "claimed":
            worker_conn.close()
            return {"audit_id": audit_id, "status": "stale_audit_skipped"}
        started_at = now_iso()
        with log_path.open("w", encoding="utf-8") as log_file:
            completed = subprocess.run(
                [
                    "codex",
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
                    prompt,
                ],
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
