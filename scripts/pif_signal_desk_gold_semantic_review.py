#!/usr/bin/env python3
"""Run resumable GPT-5.5 review of Gold-C possible meaning reversals."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

PIF_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIF_ROOT))

from research_factory.codex_app_server import CodexAppServerClient
from research_factory.signal_desk_gold_semantic_review import (
    MODEL, build_review_batches, output_schema, validate_decisions,
)
from research_factory.signal_desk_rebuild_budget import rebuild_budget_gate
from research_factory.subscription_budget import (
    record_usage, subscription_budget_window,
)
from research_factory.util import now_iso, write_text_atomic

CAMPAIGN_ID = "signal-desk-clean-corpus-2026-08-31"
GRANT = PIF_ROOT / "config/signal_desk_rebuild_budget_grant.json"
BUDGET_DIR = PIF_ROOT / "work/pif-ops/budget"
DB = PIF_ROOT / "data/factory.sqlite"
ROOT = PIF_ROOT / "work/signal-desk-rebuild/gold-authoring-v2"
PRIVATE = ROOT / "private-gpt55-semantic-review"
SYSTEM_PROMPT = """You are the final semantic approval authority for a podcast-extraction gold audit.
Review each Gold-C claim and independent-audit claim against the supplied frozen transcript window.
Classify only the relationship between the two claims:
- equivalent: materially the same proposition, including equivalent positive/negative wording.
- reversed_meaning: one asserts the material opposite of the other.
- different_claim: both may be supported, but they assert materially different propositions.
- uncertain: the transcript is insufficient or the relationship cannot be decided reliably.
Do not infer facts outside the transcript. Do not repair spelling from outside knowledge. Return exactly one decision for every candidate ID and attest model gpt-5.5."""


def _prompt(batch: dict) -> str:
    return "Adjudicate this private development-split batch:\n" + json.dumps(
        batch, ensure_ascii=False, separators=(",", ":")
    )


def _gate_and_record(*, run_id: str, tokens: int | None = None) -> dict:
    instant = datetime.now(timezone.utc)
    day, window_start = subscription_budget_window(instant)
    conn = sqlite3.connect(DB)
    try:
        if tokens is None:
            receipt = dict(rebuild_budget_gate(
                conn, day=day, campaign_id=CAMPAIGN_ID, grant_path=GRANT,
                budget_dir=BUDGET_DIR, at=instant, window_start_iso=window_start,
            ))
            conn.commit()
            return receipt
        ledger_id = record_usage(
            conn, day=day, provider_lane="openai-codex/gpt-5.5",
            lane="signal_desk_rebuild_approval", run_id=run_id,
            tokens=tokens, provider_calls=1,
        )
        conn.commit()
        return {"ledger_id": ledger_id, "tokens": tokens}
    finally:
        conn.close()


async def run(args: argparse.Namespace) -> int:
    manifest = json.loads((PIF_ROOT / "work/signal-desk-rebuild/benchmark/partial-manifest.json").read_text())
    audit_path = ROOT / "artifacts/gold-audit-dev.json"
    audit = json.loads(audit_path.read_text())
    batches = build_review_batches(
        manifest=manifest, audit_receipt=audit,
        result_root=ROOT / "results/development", project_root=PIF_ROOT,
    )
    PRIVATE.mkdir(parents=True, exist_ok=True)
    PRIVATE.chmod(0o700)
    queue: asyncio.Queue[dict] = asyncio.Queue()
    for batch in batches:
        output_path = PRIVATE / f"{batch['window_id']}.output.json"
        if not output_path.exists():
            queue.put_nowait(batch)

    async def worker(worker_id: int) -> None:
        async with CodexAppServerClient(
            command=[args.binary, "app-server", "--stdio", "--strict-config"],
            expected_cli_version="0.147.0",
        ) as client:
            while not queue.empty():
                batch = await queue.get()
                window_id = str(batch["window_id"])
                run_id = f"gold-semantic-review:{window_id}"
                sidecar = PRIVATE / f"{window_id}.sidecar.json"
                output = PRIVATE / f"{window_id}.output.json"
                temporary_output = PRIVATE / f"{window_id}.output.pending.json"
                try:
                    gate = _gate_and_record(run_id=run_id)
                    if not gate.get("allowed"):
                        raise RuntimeError(f"rebuild approval budget denied: {gate.get('reason')}")
                    result = await client.run_ephemeral_structured_turn(
                        model=MODEL, effort="high", base_instructions=SYSTEM_PROMPT,
                        prompt=_prompt(batch), output_schema=output_schema(), cwd=PIF_ROOT,
                        sidecar_path=sidecar, output_path=temporary_output, timeout_seconds=900,
                    )
                    if not result.status_ok or result.output is None or result.usage is None:
                        raise RuntimeError(f"GPT-5.5 review failed: {result.error_class or result.status}")
                    validate_decisions(
                        result.output,
                        expected_candidate_ids=[row["candidate_id"] for row in batch["candidates"]],
                    )
                    _gate_and_record(run_id=run_id, tokens=result.usage.total_tokens)
                    write_text_atomic(
                        output,
                        json.dumps(result.output, indent=2, sort_keys=True) + "\n",
                    )
                finally:
                    temporary_output.unlink(missing_ok=True)
                    queue.task_done()

    await asyncio.gather(*(worker(index) for index in range(args.concurrency)))
    decisions = {}
    for batch in batches:
        output = json.loads((PRIVATE / f"{batch['window_id']}.output.json").read_text())
        decisions.update(validate_decisions(
            output, expected_candidate_ids=[row["candidate_id"] for row in batch["candidates"]],
        ))
    receipt = {
        "schema_version": "pif_signal_desk_gold_semantic_review_receipt_v1",
        "created_at": now_iso(), "model": MODEL, "reasoning_effort": "high",
        "candidate_count": len(decisions), "batch_count": len(batches),
        "complete": len(decisions) == audit["semantic_reversal_review"]["candidate_count"],
        "decisions": decisions,
        "privacy": "candidate ids and verdicts only; transcript and rationales remain private",
    }
    body = json.dumps(receipt, sort_keys=True, separators=(",", ":"))
    receipt["receipt_sha256"] = hashlib.sha256(body.encode()).hexdigest()
    out = ROOT / "artifacts/gold-semantic-review-dev.json"
    out.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    print(json.dumps({k: receipt[k] for k in ("complete", "candidate_count", "batch_count")}, indent=2))
    return 0 if receipt["complete"] else 2


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--concurrency", type=int, choices=(1, 2), default=1)
    parser.add_argument("--binary", default="codex")
    return asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
