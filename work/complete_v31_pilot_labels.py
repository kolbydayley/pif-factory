from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
import time
from pathlib import Path


PROJECT = Path(__file__).resolve().parents[1]


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pilot-id")
    parser.add_argument("--label-pack", default="ai_discourse_v3_1")
    parser.add_argument("--model", default="gpt-5.5")
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--max-jobs", type=int, default=25)
    parser.add_argument("--timeout-seconds", type=int, default=1200)
    parser.add_argument("--worker-prefix", default="scale-v31-pilot")
    parser.add_argument("--max-failures", type=int, default=8)
    args = parser.parse_args()
    stats = {
        "claimed": 0,
        "completed": 0,
        "failed": 0,
        "usage_limited": 0,
        "no_pending_workers": 0,
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "details": [],
    }
    lock = asyncio.Lock()
    stop_event = asyncio.Event()

    async def worker(index: int) -> None:
        worker_id = f"{args.worker_prefix}-{index}"
        while True:
            if stop_event.is_set():
                return
            async with lock:
                if stats["claimed"] >= args.max_jobs:
                    return
            claim = await claim_with_retry(args, worker_id)
            if not claim.get("job_id"):
                async with lock:
                    stats["no_pending_workers"] += 1
                    stats["details"].append({"worker": worker_id, "result": claim.get("message", "no pending jobs")})
                return
            async with lock:
                stats["claimed"] += 1
                ordinal = stats["claimed"]
            result = await process_claim(claim, worker_id=worker_id, model=args.model, timeout_seconds=args.timeout_seconds)
            result["ordinal"] = ordinal
            async with lock:
                if result["status"] == "completed":
                    stats["completed"] += 1
                elif result["status"] == "usage_limited":
                    stats["usage_limited"] += 1
                    stop_event.set()
                else:
                    stats["failed"] += 1
                    if stats["failed"] >= args.max_failures:
                        stop_event.set()
                stats["details"].append(result)
                print(json.dumps({"progress": {key: stats[key] for key in ["claimed", "completed", "failed", "usage_limited"]}, "last": result}, sort_keys=True), flush=True)

    await asyncio.gather(*(worker(index) for index in range(1, max(args.concurrency, 1) + 1)))
    stats["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    print(json.dumps(stats, indent=2, sort_keys=True))
    return 0 if stats["failed"] == 0 else 1


async def process_claim(claim: dict, *, worker_id: str, model: str, timeout_seconds: int) -> dict:
    job_id = str(claim["job_id"])
    prompt_path = Path(claim["prompt_path"])
    output_path = Path(claim["output_path"])
    prompt = prompt_path.read_text(encoding="utf-8")
    errors: list[str] = []
    for attempt in range(1, 3):
        if output_path.exists():
            output_path.unlink()
        attempt_prompt = prompt
        if errors:
            feedback = errors[-1][-1200:]
            attempt_prompt = (
                f"{prompt}\n\n"
                "RETRY VALIDATION FEEDBACK:\n"
                "Your previous output did not pass the local validator. "
                "Correct the JSON only; keep the same schema and do not add commentary.\n"
                f"{feedback}\n"
            )
        codex = await asyncio.create_subprocess_exec(
            "codex",
            "exec",
            "-m",
            model,
            "-s",
            "read-only",
            "-C",
            str(PROJECT),
            "-o",
            str(output_path),
            "-",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=str(PROJECT),
            env=os.environ.copy(),
        )
        try:
            stdout, stderr = await asyncio.wait_for(codex.communicate(attempt_prompt.encode("utf-8")), timeout=timeout_seconds)
        except asyncio.TimeoutError:
            codex.send_signal(signal.SIGTERM)
            await codex.wait()
            errors.append(f"codex attempt {attempt} timed out after {timeout_seconds}s")
            continue
        if codex.returncode != 0:
            error_text = stderr.decode("utf-8", "replace") + stdout.decode("utf-8", "replace")
            if "hit your usage limit" in error_text.lower():
                await run_cli_process(["recover-label-runs", "--release-missing-outputs"])
                return {
                    "status": "usage_limited",
                    "job_id": int(job_id),
                    "attempts": attempt,
                    "error": "Codex usage limit reached; released missing-output label handoffs and stopped the wave.",
                }
            errors.append(f"codex attempt {attempt} exited {codex.returncode}: {sanitize_error(error_text)}")
            continue
        if not output_path.exists():
            errors.append(f"codex attempt {attempt} exited 0 but did not write {output_path}")
            continue
        submitted = await run_cli_process_with_retry(
            [
                "submit",
                "--job-id",
                job_id,
                "--output-json",
                str(output_path),
                "--worker-id",
                worker_id,
                "--allow-expired",
            ],
            attempts=10,
            base_delay=0.5,
            max_delay=8.0,
        )
        if submitted.returncode == 0:
            payload = json.loads(submitted.stdout)
            return {
                "status": "completed",
                "job_id": int(job_id),
                "label_id": payload.get("label_id"),
                "attempts": attempt,
            }
        submit_error = submitted.stderr or submitted.stdout
        if f"Job {job_id} is completed, not claimed" in submit_error:
            return {
                "status": "completed",
                "job_id": int(job_id),
                "attempts": attempt,
                "note": "job was already completed before this worker submitted",
            }
        errors.append(f"submit attempt {attempt} failed: {sanitize_error(submit_error)}")
    reason = " | ".join(errors)[-1500:] or "GPT-5.5 label worker failed without a specific error."
    await run_cli_process_with_retry(["fail", "--job-id", job_id, "--reason", reason], attempts=8)
    return {"status": "failed", "job_id": int(job_id), "attempts": 2, "error": reason}


async def run_cli_json(args: list[str]) -> dict:
    proc = await run_cli_process(args)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr or proc.stdout)
    return json.loads(proc.stdout)


async def claim_with_retry(args: argparse.Namespace, worker_id: str) -> dict:
    command = [
        "claim",
        "--lane",
        "podcast",
        "--label-pack",
        args.label_pack,
        "--model",
        args.model,
        "--worker-id",
        worker_id,
    ]
    if args.pilot_id:
        command.extend(["--pilot-id", args.pilot_id])
    last_error = ""
    for attempt in range(1, 16):
        proc = await run_cli_process(command)
        if proc.returncode == 0:
            return json.loads(proc.stdout)
        last_error = proc.stderr or proc.stdout
        retryable_error = last_error.lower()
        if "database is locked" not in retryable_error and "initialization_in_progress" not in retryable_error:
            raise RuntimeError(last_error)
        await asyncio.sleep(min(0.5 * attempt, 8.0))
    raise RuntimeError(last_error)


async def run_cli_process_with_retry(
    args: list[str],
    *,
    attempts: int = 8,
    base_delay: float = 0.5,
    max_delay: float = 8.0,
) -> asyncio.subprocess.Process:
    last_proc: asyncio.subprocess.Process | None = None
    for attempt in range(1, max(attempts, 1) + 1):
        proc = await run_cli_process(args)
        if proc.returncode == 0:
            return proc
        last_proc = proc
        error_text = (proc.stderr or proc.stdout).lower()
        if "database is locked" not in error_text and "initialization_in_progress" not in error_text:
            return proc
        await asyncio.sleep(min(base_delay * attempt, max_delay))
    if last_proc is None:
        return await run_cli_process(args)
    return last_proc


async def run_cli_process(args: list[str]) -> asyncio.subprocess.Process:
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "research_factory",
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=str(PROJECT),
        env=os.environ.copy(),
    )
    stdout, stderr = await proc.communicate()
    proc.stdout = stdout.decode("utf-8", "replace")  # type: ignore[attr-defined]
    proc.stderr = stderr.decode("utf-8", "replace")  # type: ignore[attr-defined]
    return proc


def sanitize_error(text: str, *, limit: int = 500) -> str:
    redacted_lines = []
    for line in text.splitlines():
        if "evidence='" in line or ".evidence=" in line:
            continue
        redacted_lines.append(line.replace(str(PROJECT), "[PROJECT]"))
    redacted = "\n".join(redacted_lines).strip()
    if len(redacted) > limit:
        redacted = redacted[-limit:]
    return redacted or "validation_or_codex_error_redacted"


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
