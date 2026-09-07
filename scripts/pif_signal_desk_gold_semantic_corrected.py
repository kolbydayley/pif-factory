#!/usr/bin/env python3
"""Metered semantic checks bound to the corrected dev audit, never the legacy epoch."""
import asyncio
import json
import sqlite3
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from research_factory.codex_app_server import CodexAppServerClient
from research_factory.signal_desk_gold_semantic_review import build_review_batches, output_schema, validate_decisions
from research_factory.signal_desk_gold_wider_speaker import reserve_call, settle_call
from research_factory.signal_desk_rebuild_approval import ApprovalBudgetConfig
from scripts.pif_signal_desk_gold_merge_provenance import immutable_json
from scripts.pif_signal_desk_gold_review_corrected import R, A, _sha_json
from scripts.pif_signal_desk_gold_semantic_review import SYSTEM_PROMPT

OUT = R / "corrected-semantic-review-v1"


async def run():
    manifest = json.loads((R / "merged-manifest.json").read_text())
    audit = json.loads((A / "audit.json").read_text())
    if manifest["manifest_sha256"] != audit["manifest_sha256"]:
        raise ValueError("manifest drift")
    batches = build_review_batches(manifest=manifest, audit_receipt=audit, result_root=A / "results", project_root=ROOT)
    OUT.mkdir(parents=True, exist_ok=True, mode=0o700)
    immutable_json(OUT / "plan.json", {"batch_hashes": [b["input_sha256"] for b in batches],
        "candidates": sum(len(b["candidates"]) for b in batches), "audit_sha256": _sha_json(audit),
        "manifest_sha256": manifest["manifest_sha256"], "gold_accepted": False})
    db = sqlite3.connect(ROOT / "data/factory.sqlite", timeout=30)
    budget = ApprovalBudgetConfig(campaign_id="signal-desk-clean-corpus-2026-08-31",
        grant_path=ROOT / "config/signal_desk_rebuild_budget_grant.json", budget_dir=ROOT / "work/pif-ops/budget")
    decisions = {}
    try:
        async with CodexAppServerClient(command=["codex", "app-server", "--stdio", "--strict-config"], expected_cli_version="0.147.0") as client:
            for batch in batches:
                digest = batch["input_sha256"]
                immutable_json(OUT / f"{digest}.packet.json", batch)
                target = OUT / f"{digest}.decision.json"
                ids = [c["candidate_id"] for c in batch["candidates"]]
                if target.exists():
                    decisions.update(validate_decisions(json.loads(target.read_text()), expected_candidate_ids=ids))
                    continue
                key = "corrected-semantic-v1:" + digest
                reservation = reserve_call(db, task_key=key, budget=budget)
                if reservation.get("existing"):
                    raise RuntimeError("existing reservation: recover saved response before retry")
                usage = None
                try:
                    schema = output_schema()
                    schema["properties"]["decisions"]["items"]["properties"]["candidate_id"]["enum"] = ids
                    result = await client.run_ephemeral_structured_turn(model="gpt-5.5", effort="high",
                        base_instructions=SYSTEM_PROMPT, prompt=json.dumps(batch, ensure_ascii=False), output_schema=schema,
                        cwd=ROOT, sidecar_path=OUT / f"{digest}.sidecar.json", output_path=OUT / f"{digest}.output.json", timeout_seconds=900)
                    usage = result.usage.total_tokens if result.usage else None
                    if not result.status_ok or result.output is None:
                        raise RuntimeError(result.error_class or result.status)
                    decisions.update(validate_decisions(result.output, expected_candidate_ids=ids))
                    immutable_json(target, result.output)
                    print(json.dumps({"reviewed_candidates": len(decisions), "total": 36}), flush=True)
                finally:
                    settle_call(db, reservation_id=reservation["reservation_id"], task_key=key, actual_tokens=usage)
    finally:
        db.close()
    immutable_json(OUT / "receipt.json", {"complete": len(decisions) == audit["semantic_reversal_review"]["candidate_count"],
        "decisions": decisions, "gold_accepted": False,
        "warning": "reversal between claims requires source-based fault attribution before diagnosing Gold-C fabrication"})


if __name__ == "__main__":
    asyncio.run(run())
