#!/usr/bin/env python3
"""Independent 16-window rubric check, never a corpus reliability verdict."""
import argparse
import asyncio
import fcntl
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.pif_signal_desk_gold_rubric_qualification import OUT, R
from scripts.pif_signal_desk_gold_merge_provenance import immutable_json
from research_factory.signal_desk_gold_runner import run_gold_split_phase, GoldCapacityDeferred
from research_factory.signal_desk_gold_shared_rubric import RUBRIC_ID
from research_factory.signal_desk_qualification_audit import validate_qualification_scope


def run_arguments(plan):
    return dict(manifest_path=R/"merged-manifest.json", project_root=ROOT,
        result_root=OUT/"results", dispatch_database=OUT/"independent-audit-dispatch.sqlite",
        budget_database=ROOT/"data/factory.sqlite",
        grant_path=ROOT/"config/signal_desk_gold_authoring_budget_grant.json",
        session_root=Path.home()/".codex/sessions", budget_dir=ROOT/"work/pif-ops/budget",
        split="development", turn_type="AUDIT", task_namespace="shared-rubric-qualification-audit-v1",
        concurrency=2, prompt_variant_id=RUBRIC_ID, target_window_ids=plan["window_ids"],
        qualification_audit_plan=plan)


async def execute(plan):
    while True:
        try:
            result = await run_gold_split_phase(**run_arguments(plan))
            immutable_json(OUT/"independent-audit-run.json", result)
            return 0 if result.get("complete") else 2
        except GoldCapacityDeferred as exc:
            delay = max(1, int(exc.capacity.get("retry_after_seconds") or 300))
            print(json.dumps({"status":"capacity_backoff", "retry_after_seconds":delay}), flush=True)
            while delay:
                step = min(60, delay); await asyncio.sleep(step); delay -= step


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    plan = json.loads((OUT/"plan.json").read_text())
    validate_qualification_scope(plan, manifest=json.loads((R/"merged-manifest.json").read_text()),
        split="development", phase_order=("AUDIT",), target_window_ids=plan["window_ids"],
        prompt_variant_id=RUBRIC_ID, task_namespace="shared-rubric-qualification-audit-v1")
    # File existence is only a preliminary check. Runner source-validates every
    # selected A/B/C before enqueue. It sends none of those answers to AUDIT.
    missing = [(phase, wid) for phase in ("A", "B", "C") for wid in plan["window_ids"]
               if not (OUT/"results"/phase/f"{wid}.json").exists()]
    if missing:
        raise RuntimeError(f"qualification authoring incomplete: {len(missing)} missing outputs")
    with (OUT/"independent-audit.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX|fcntl.LOCK_NB)
        print(json.dumps({"windows":16, "calls":16, "reliability_gate_eligible":False}), flush=True)
        if args.execute: raise SystemExit(asyncio.run(execute(plan)))


if __name__ == "__main__": main()
