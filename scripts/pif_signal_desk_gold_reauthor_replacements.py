#!/usr/bin/env python3
"""Plan or run post-refreeze Gold for nine development windows only."""
from __future__ import annotations
import argparse,asyncio,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from research_factory.signal_desk_gold_reauthor_pipeline import *  # noqa:E402,F403
G=ROOT/'work/signal-desk-rebuild/gold-authoring-v2';BASE=ROOT/'work/signal-desk-rebuild/benchmark/partial-manifest.json';REFREEZE=G/'artifacts/development-source-refreeze-v1.json';OUT=G/'development-source-reauthor-v1';PLAN=OUT/'plan.json';MERGED_MANIFEST=OUT/'merged-manifest.json';ACTIVATION=OUT/'activation.json';RESULTS=OUT/'replacement-results';MERGED=OUT/'merged-development'
def write(path,value):path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(value,indent=2,sort_keys=True)+'\n')
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--execute',action='store_true');p.add_argument('--concurrency',type=int,choices=range(2,9),default=2);p.add_argument('--binary',default='codex');a=p.parse_args();plan=build_waiting_plan(base_manifest_path=BASE,refreeze_path=REFREEZE,output_root=OUT);write(PLAN,plan)
 result={'status':'waiting_for_refreeze' if not REFREEZE.exists() else 'ready_to_activate','provider_calls_started':False,'expected_calls':plan['expected_calls'],'expected_tokens':plan['expected_tokens_at_measured_semantic_mean'],'reserved_token_ceiling':plan['reserved_token_ceiling'],'execute_command':'python3 -B scripts/pif_signal_desk_gold_reauthor_replacements.py --execute --concurrency 2'}
 if not a.execute:print(json.dumps(result,indent=2,sort_keys=True));return 0
 activation=activate_overlay(base_manifest_path=BASE,refreeze_path=REFREEZE,project_root=ROOT,merged_manifest_path=MERGED_MANIFEST,waiting_plan=plan);write(ACTIVATION,activation)
 run=asyncio.run(execute_reauthor(merged_manifest_path=MERGED_MANIFEST,target_ids=activation['target_window_ids'],project_root=ROOT,result_root=RESULTS,dispatch_database=OUT/'dispatch.sqlite',budget_database=ROOT/'data/factory.sqlite',grant_path=ROOT/'config/signal_desk_gold_authoring_budget_grant.json',session_root=Path.home()/'.codex/sessions',budget_dir=ROOT/'work/pif-ops/budget',concurrency=a.concurrency,binary=a.binary))
 write(OUT/'run.json',run)
 if not run.get('complete'):raise ReauthorPlanError('replacement Gold pipeline incomplete')
 merged_receipt=merge_development_gold(base_root=G/'results/development',replacement_root=RESULTS,merged_root=MERGED,base_manifest_path=BASE,merged_manifest_path=MERGED_MANIFEST,target_ids=activation['target_window_ids']);write(MERGED/'receipt.json',merged_receipt)
 ready=readiness_receipt(merged_manifest_sha256=activation['merged_manifest_sha256'],merged_gold_receipt=merged_receipt);write(OUT/'gate-readiness.json',ready)
 print(json.dumps({'status':'complete','provider_calls_started':True,'run':run,'merged':merged_receipt,'gate_readiness':ready},indent=2,sort_keys=True));return 0
if __name__=='__main__':raise SystemExit(main())
