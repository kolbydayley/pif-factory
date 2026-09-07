#!/usr/bin/env python3
"""Finalize bounded conflict decisions into a candidate and independently re-audit it."""
import argparse
import asyncio
import json
import sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from scripts.pif_signal_desk_gold_review_corrected import R,A,_sha_json,build_cases
from scripts.pif_signal_desk_gold_project_repairs import patch_event
from scripts.pif_signal_desk_gold_merge_provenance import immutable_json
from research_factory.signal_desk_gold_audit import _load_frozen_window_text,evaluate_dev_audit
from research_factory.signal_desk_gold_runner import run_gold_split_phase, GoldCapacityDeferred
from research_factory.util import write_text_atomic


async def run_with_capacity_recovery(**kwargs):
    while True:
        try:
            return await run_gold_split_phase(**kwargs)
        except GoldCapacityDeferred as exc:
            delay=max(1,int(exc.capacity.get("retry_after_seconds") or 300))
            write_text_atomic(OUT/"capacity-wait.json",json.dumps({"status":"capacity_backoff", "capacity":exc.capacity,
                "retry_after_seconds":delay,"automatic_resume":True,"completed_outputs_preserved":True})+"\n")
            print(json.dumps({"status":"capacity_backoff","retry_after_seconds":delay,"automatic_resume":True}),flush=True)
            while delay:
                step=min(60,delay)
                await asyncio.sleep(step)
                delay-=step

OUT=R/"repaired-dev-reaudit-v1"


def prepare():
    manifest=json.loads((R/"merged-manifest.json").read_text())
    rows={w["window_id"]:w for w in manifest["windows"] if w["split"]=="development"}
    raw_audit=json.loads((A/"audit.json").read_text())
    cases={c["case_id"]:c for c in build_cases(manifest_path=R/"merged-manifest.json",result_root=A/"results",project_root=ROOT,
        selected_window_ids=raw_audit["audit_selection"]["window_ids"])}
    base=R/"verified-field-repair-candidate-v1"
    base_receipt=json.loads((base/"receipt.json").read_text())
    outputs={wid:json.loads((base/"C"/f"{wid}.json").read_text()) for wid in rows}
    for wid,obj in outputs.items():
        if _sha_json(obj)!=base_receipt["after_inventory"][wid]:raise ValueError("candidate inventory drift")
    conflict=R/"field-repair-conflict-resolution-v1"
    changes=[];retained=[];quarantined=[]
    for digest in json.loads((conflict/"plan.json").read_text())["packet_digests"]:
        p=json.loads((conflict/f"{digest}.packet.json").read_text())
        if _sha_json({k:v for k,v in p.items() if k!="packet_sha256"})!=digest:raise ValueError("conflict packet drift")
        decision=json.loads((conflict/f"{digest}.verification.json").read_text())
        proposal=p["repair_proposal"];cid=proposal["case_id"]
        if decision["case_id"]!=cid:raise ValueError("decision identity mismatch")
        c=cases[cid];wid=c["window_id"];i=c["gold_index"]
        record={"case_id":cid,"window_id":wid,"gold_index":i,"packet_sha256":digest,"verification_sha256":_sha_json(decision)}
        if decision["verdict"]=="retain_original":retained.append(record)
        elif decision["verdict"]=="quarantine":quarantined.append({**record,"reason":"final_conflict_quarantine"})
        elif decision["verdict"]=="accept_repair":
            try:value=patch_event(outputs[wid]["events"][i],proposal["proposed_patch"],_load_frozen_window_text(rows[wid],project_root=ROOT))
            except ValueError as exc:
                quarantined.append({**record,"reason":"schema_rejected","detail":str(exc)});continue
            outputs[wid]["events"][i]=value;changes.append({**record,"patch":proposal["proposed_patch"]})
        else:raise ValueError("unknown conflict verdict")
    events=sum(len(v["events"]) for v in outputs.values())
    if events!=base_receipt["events_preserved"]:raise ValueError("event denominator changed")
    inventory={}
    for wid,obj in outputs.items():
        immutable_json(OUT/"results/C"/f"{wid}.json",obj);inventory[wid]=_sha_json(obj)
    immutable_json(OUT/"quarantine-ledger.json",quarantined)
    immutable_json(OUT/"conflict-patches.json",changes)
    immutable_json(OUT/"retained-originals.json",retained)
    original_plan=json.loads((A/"plan.json").read_text())
    plan={"candidate_inventory":inventory,"initial_ids":original_plan["initial_ids"],"target_ids":original_plan["target_ids"],
        "baseline_patches":base_receipt["applied_to_candidate"],"additional_patches":len(changes),"retained_originals":len(retained),
        "quarantined_records_retained":len(quarantined),"events_preserved":events,"audit_denominator":1909,
        "gold_accepted":False,"original_failure_preserved":True,"old_audit_outputs_reused":False}
    immutable_json(OUT/"plan.json",plan)
    return plan


def main():
    p=argparse.ArgumentParser();p.add_argument("--execute",action="store_true");args=p.parse_args()
    plan=prepare();print(json.dumps({k:v for k,v in plan.items() if k not in {"candidate_inventory","initial_ids","target_ids"}}),flush=True)
    if not args.execute:return 0
    run=asyncio.run(run_with_capacity_recovery(manifest_path=R/"merged-manifest.json",project_root=ROOT,result_root=OUT/"results",
        dispatch_database=OUT/"dispatch.sqlite",budget_database=ROOT/"data/factory.sqlite",
        grant_path=ROOT/"config/signal_desk_gold_authoring_budget_grant.json",session_root=Path.home()/".codex/sessions",
        budget_dir=ROOT/"work/pif-ops/budget",split="development",turn_type="AUDIT",task_namespace="repaired-dev-reaudit-v1",concurrency=2))
    immutable_json(OUT/"run.json",run)
    if not run.get("complete"):return 2
    audit=evaluate_dev_audit(manifest_path=R/"merged-manifest.json",result_root=OUT/"results",initial_window_ids=plan["initial_ids"],project_root=ROOT)
    immutable_json(OUT/"audit.json",audit)
    print(json.dumps({"independent_audit_passed":audit["passed"],"gold_accepted":False,
        "remaining":"quarantines, rubric consistency and any new audit disagreements require resolution"}),flush=True)
    return 0 if audit["passed"] else 2


if __name__=="__main__":raise SystemExit(main())
