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
    parser.add_argument("--pilot-id", required=True)
    parser.add_argument("--model", default="gpt-5.5")
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--max-jobs", type=int, default=25)
    parser.add_argument("--timeout-seconds", type=int, default=1200)
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
        worker_id = f"scale-v31-pilot-{index}"
        while True:
            if stop_event.is_set():
                return
            async with lock:
                if stats["claimed"] >= args.max_jobs:
                    return
            claim = await run_cli_json(
                [
                    "claim",
                    "--lane",
                    "podcast",
                    "--label-pack",
                    "ai_discourse_v3_1",
                    "--model",
                    args.model,
                    "--worker-id",
                    worker_id,
                    "--pilot-id",
                    args.pilot_id,
                ]
            )
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
            stdout, stderr = await asyncio.wait_for(codex.communicate(prompt.encode("utf-8")), timeout=timeout_seconds)
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
            errors.append(f"codex attempt {attempt} exited {codex.returncode}: {error_text[-500:]}")
            continue
        if not output_path.exists():
            errors.append(f"codex attempt {attempt} exited 0 but did not write {output_path}")
            continue
        submitted = await run_cli_process(
            [
                "submit",
                "--job-id",
                job_id,
                "--output-json",
                str(output_path),
                "--worker-id",
                worker_id,
                "--allow-expired",
            ]
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
        errors.append(f"submit attempt {attempt} failed: {submit_error[-500:]}")
    reason = " | ".join(errors)[-1500:] or "GPT-5.5 label worker failed without a specific error."
    await run_cli_process(["fail", "--job-id", job_id, "--reason", reason])
    return {"status": "failed", "job_id": int(job_id), "attempts": 2, "error": reason}


async def run_cli_json(args: list[str]) -> dict:
    proc = await run_cli_process(args)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr or proc.stdout)
    return json.loads(proc.stdout)


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


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
