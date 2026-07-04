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
    parser.add_argument("--concurrency", type=int, default=2)
    parser.add_argument("--timeout-seconds", type=int, default=1800)
    args = parser.parse_args()
    audits = await pending_audits(args.pilot_id)
    queue = asyncio.Queue()
    for audit in audits:
        queue.put_nowait(audit)
    stats = {
        "pending_seen": len(audits),
        "completed": 0,
        "failed": 0,
        "usage_limited": 0,
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "details": [],
    }
    lock = asyncio.Lock()
    stop = asyncio.Event()

    async def worker(index: int) -> None:
        while not stop.is_set():
            try:
                audit = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            result = await run_one(audit, model=args.model, timeout_seconds=args.timeout_seconds)
            async with lock:
                if result["status"] == "completed":
                    stats["completed"] += 1
                elif result["status"] == "usage_limited":
                    stats["usage_limited"] += 1
                    stop.set()
                else:
                    stats["failed"] += 1
                stats["details"].append(result)
                print(json.dumps({"progress": {key: stats[key] for key in ["completed", "failed", "usage_limited"]}, "last": result}, sort_keys=True), flush=True)
            queue.task_done()

    await asyncio.gather(*(worker(index) for index in range(max(args.concurrency, 1))))
    stats["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    print(json.dumps(stats, indent=2, sort_keys=True))
    return 0 if stats["failed"] == 0 and stats["usage_limited"] == 0 else 1


async def pending_audits(pilot_id: str) -> list[dict]:
    proc = await run_python(
        """
from research_factory import db
from research_factory.paths import db_path
import json
conn=db.connect(db_path())
rows=conn.execute(\"\"\"
SELECT id, episode_id, prompt_path, output_path
FROM reviewer_audits
WHERE pilot_id=?
  AND status='pending'
ORDER BY created_at, id
\"\"\", (pilot_id,)).fetchall()
print(json.dumps([dict(r) for r in rows]))
conn.close()
""",
        {"pilot_id": pilot_id},
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr or proc.stdout)
    return json.loads(proc.stdout)


async def run_one(audit: dict, *, model: str, timeout_seconds: int) -> dict:
    audit_id = audit["id"]
    prompt_path = Path(audit["prompt_path"])
    output_path = Path(audit["output_path"])
    prompt = prompt_path.read_text(encoding="utf-8")
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
        return {"status": "failed", "audit_id": audit_id, "error": f"reviewer timed out after {timeout_seconds}s"}
    text = stderr.decode("utf-8", "replace") + stdout.decode("utf-8", "replace")
    if codex.returncode != 0:
        if "hit your usage limit" in text.lower():
            return {"status": "usage_limited", "audit_id": audit_id, "error": "Codex usage limit reached; stopped reviewer wave."}
        return {"status": "failed", "audit_id": audit_id, "error": text[-1500:]}
    submit = await cli(["submit-reviewer-audit", "--audit-id", audit_id, "--output-json", str(output_path)])
    if submit.returncode != 0:
        return {"status": "failed", "audit_id": audit_id, "error": (submit.stderr or submit.stdout)[-1500:]}
    payload = json.loads(submit.stdout)
    return {"status": "completed", "audit_id": audit_id, "summary": payload.get("summary", {})}


async def cli(args: list[str]):
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


async def run_python(code: str, values: dict[str, str]):
    prefix = "\n".join(f"{key}={value!r}" for key, value in values.items())
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        prefix + "\n" + code,
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
