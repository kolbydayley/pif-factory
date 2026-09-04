#!/usr/bin/env python3
"""Run GPT-5.5 final review of one-span-many-claim Gold audit cases."""

from __future__ import annotations

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
    MODEL, atomicity_output_schema, build_atomicity_batches, validate_atomicity_decision,
)
from research_factory.signal_desk_rebuild_budget import rebuild_budget_gate
from research_factory.subscription_budget import record_usage, subscription_budget_window
from research_factory.util import now_iso, write_text_atomic

CAMPAIGN_ID = "signal-desk-clean-corpus-2026-08-31"
GRANT = PIF_ROOT / "config/signal_desk_rebuild_budget_grant.json"
BUDGET_DIR = PIF_ROOT / "work/pif-ops/budget"
DB = PIF_ROOT / "data/factory.sqlite"
ROOT = PIF_ROOT / "work/signal-desk-rebuild/gold-authoring-v2"
PRIVATE = ROOT / "private-gpt55-atomicity-review"
SYSTEM_PROMPT = """You are the final GPT-5.5 atomicity authority for podcast gold data.
Inspect the repeated-evidence-span event groups against the full frozen transcript window.
Distinct propositions supported by one sentence are legitimate. Near-duplicate restatements,
or mechanically splitting a list into low-value fragments, are over-splitting. Decide whether
the Gold-C group is valid, the independent audit is over-split, Gold-C needs correction, both
need correction, or the evidence is uncertain. If Gold-C needs correction, list only redundant
Gold-C event indices that can be dropped without losing a distinct consequential proposition.
Use transcript evidence only and attest model gpt-5.5."""


def _budget(*, run_id: str, tokens: int | None = None) -> dict:
    instant = datetime.now(timezone.utc)
    day, window_start = subscription_budget_window(instant)
    conn = sqlite3.connect(DB)
    try:
        if tokens is None:
            result = dict(rebuild_budget_gate(
                conn, day=day, campaign_id=CAMPAIGN_ID, grant_path=GRANT,
                budget_dir=BUDGET_DIR, at=instant, window_start_iso=window_start,
            ))
        else:
            result = {"ledger_id": record_usage(
                conn, day=day, provider_lane="openai-codex/gpt-5.5",
                lane="signal_desk_rebuild_approval", run_id=run_id,
                tokens=tokens, provider_calls=1,
            )}
        conn.commit()
        return result
    finally:
        conn.close()


async def main_async() -> int:
    manifest = json.loads((PIF_ROOT / "work/signal-desk-rebuild/benchmark/partial-manifest.json").read_text())
    primary = json.loads((ROOT / "artifacts/gold-atomicity-review-dev.json").read_text())
    audit = json.loads((ROOT / "artifacts/gold-audit-dev.json").read_text())
    window_ids = [str(window_id) for window_id in primary.get("flagged_windows", [])]
    window_ids += [row["window_id"] for row in audit["over_splitting"].get("flagged_windows", [])]
    batches = build_atomicity_batches(
        manifest=manifest, window_ids=window_ids,
        result_root=ROOT / "results/development", project_root=PIF_ROOT,
    )
    PRIVATE.mkdir(parents=True, exist_ok=True); PRIVATE.chmod(0o700)
    decisions = {}
    async with CodexAppServerClient(
        command=["codex", "app-server", "--stdio", "--strict-config"],
        expected_cli_version="0.147.0",
    ) as client:
        for batch in batches:
            window_id = batch["window_id"]
            final = PRIVATE / f"{window_id}.output.json"
            if final.exists():
                output = json.loads(final.read_text())
            else:
                gate = _budget(run_id=f"gold-atomicity-review:{window_id}")
                if not gate.get("allowed"):
                    raise RuntimeError(f"rebuild approval budget denied: {gate.get('reason')}")
                pending = PRIVATE / f"{window_id}.pending.json"
                result = await client.run_ephemeral_structured_turn(
                    model=MODEL, effort="high", base_instructions=SYSTEM_PROMPT,
                    prompt="Review this private development batch:\n" + json.dumps(batch, separators=(",", ":")),
                    output_schema=atomicity_output_schema(), cwd=PIF_ROOT,
                    sidecar_path=PRIVATE / f"{window_id}.sidecar.json",
                    output_path=pending, timeout_seconds=900,
                )
                if not result.status_ok or result.output is None or result.usage is None:
                    raise RuntimeError(f"GPT-5.5 atomicity review failed: {result.error_class or result.status}")
                output = dict(result.output)
                validate_atomicity_decision(output, window_id=window_id)
                _budget(run_id=f"gold-atomicity-review:{window_id}", tokens=result.usage.total_tokens)
                write_text_atomic(final, json.dumps(output, indent=2, sort_keys=True) + "\n")
                pending.unlink(missing_ok=True)
            decisions[window_id] = validate_atomicity_decision(output, window_id=window_id)
    receipt = {
        "schema_version": "pif_signal_desk_gold_atomicity_review_receipt_v1",
        "created_at": now_iso(), "model": MODEL, "reasoning_effort": "high",
        "complete": len(decisions) == len(batches), "decisions": decisions,
        "privacy": "window ids, verdicts, and drop indices only; rationale remains private",
    }
    body = json.dumps(receipt, sort_keys=True, separators=(",", ":"))
    receipt["receipt_sha256"] = hashlib.sha256(body.encode()).hexdigest()
    write_text_atomic(ROOT / "artifacts/gold-atomicity-adjudication-dev.json",
                      json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    print(json.dumps(receipt, indent=2, sort_keys=True))
    return 0 if receipt["complete"] else 2


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main_async()))
