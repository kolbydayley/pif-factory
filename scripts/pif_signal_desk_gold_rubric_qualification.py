#!/usr/bin/env python3
"""Bounded dev-only Gold A/B/C authoring under a shared candidate rubric."""
import argparse
import asyncio
import json
import sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from scripts.pif_signal_desk_gold_review_corrected import R,_sha_json
from scripts.pif_signal_desk_gold_merge_provenance import immutable_json
from research_factory.signal_desk_gold_runner import run_gold_split_phase,GoldCapacityDeferred
from research_factory.signal_desk_gold_shared_rubric import RUBRIC_ID,receipt
from research_factory.signal_desk_gold_prompt_variant import system_prompts_for_variant

OUT=R/"shared-rubric-qualification-v1"


def select_windows(rows):
    """Source-property selection, never prior judge verdicts or target answers."""
    selected=[]; used_shows=set()
    for structure in ("speaker_turn","paragraph","flattened","asr_diarized"):
        pool=sorted((r for r in rows if r["split"]=="development" and r["transcript_structure"]==structure),
                    key=lambda r:_sha_json(["shared-rubric-qualification-v1",r["window_id"]]))
        picked=[]
        for row in pool:
            if row["show_id"] not in used_shows:
                picked.append(row);used_shows.add(row["show_id"])
                if len(picked)==4:break
        if len(picked)!=4:raise ValueError(f"insufficient show-disjoint development windows for {structure}")
        selected.extend(picked)
    return selected


def prepare():
    manifest=json.loads((R/"merged-manifest.json").read_text())
    selected=select_windows(manifest["windows"])
    prompts=system_prompts_for_variant(RUBRIC_ID)
    plan={"rubric":receipt(),"manifest_sha256":manifest["manifest_sha256"],
        "window_ids":[r["window_id"] for r in selected],
        "sources":[{k:r[k] for k in ("window_id","show_id","transcript_structure","text_sha256","transcript_sha256")} for r in selected],
        "system_prompt_hashes":{k:_sha_json(v) for k,v in prompts.items()},
        "calls":48,"concurrency":2,"model":"gpt-5.6-sol","reasoning":"medium",
        "phase":"qualification_gold_authoring_not_tournament","gold_accepted":False,"rubric_qualified":False,
        "selection_uses_prior_answers":False,"sealed_items_opened":False,
        "next":"GPT-5.5 source adjudication then independent cross-role consistency measurement; no promotion from authoring"}
    OUT.mkdir(parents=True,exist_ok=True,mode=0o700)
    immutable_json(OUT/"plan.json",plan)
    return plan


async def run(plan):
    while True:
        try:
            return await run_gold_split_phase(manifest_path=R/"merged-manifest.json",project_root=ROOT,result_root=OUT/"results",
                dispatch_database=OUT/"dispatch.sqlite",budget_database=ROOT/"data/factory.sqlite",
                grant_path=ROOT/"config/signal_desk_gold_authoring_budget_grant.json",session_root=Path.home()/".codex/sessions",
                budget_dir=ROOT/"work/pif-ops/budget",split="development",turn_type="PIPELINE",
                task_namespace="shared-rubric-qualification-v1",concurrency=2,prompt_variant_id=RUBRIC_ID,
                target_window_ids=plan["window_ids"])
        except GoldCapacityDeferred as exc:
            delay=max(1,int(exc.capacity.get("retry_after_seconds") or 300))
            print(json.dumps({"status":"capacity_backoff","retry_after_seconds":delay,"automatic_resume":True}),flush=True)
            while delay:
                step=min(60,delay);await asyncio.sleep(step);delay-=step


if __name__=="__main__":
    p=argparse.ArgumentParser();p.add_argument("--execute",action="store_true");args=p.parse_args()
    plan=prepare();print(json.dumps({"windows":len(plan["window_ids"]),"calls":plan["calls"],"rubric_qualified":False}),flush=True)
    if args.execute:
        result=asyncio.run(run(plan));immutable_json(OUT/"authoring-run.json",result)
        raise SystemExit(0 if result.get("complete") else 2)
