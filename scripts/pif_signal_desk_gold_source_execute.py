#!/usr/bin/env python3
"""Prepare or execute the five-episode source-remediation lane."""
from __future__ import annotations
import argparse,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from research_factory.signal_desk_gold_source_executor import *  # noqa:E402,F403
G=ROOT/'work/signal-desk-rebuild/gold-authoring-v2';PLAN=G/'artifacts/gold-source-remediation-plan-v1.json';DISPATCH=G/'dispatch-source-remediation-asr-v1.sqlite';PRIVATE=G/'private-source-remediation-asr-v1';RUN=G/'artifacts/gold-source-remediation-run-v1.json';REFREEZE=G/'artifacts/development-source-refreeze-v1.json'
def write(path,value):path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(value,indent=2,sort_keys=True)+'\n')
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--execute',action='store_true');p.add_argument('--refreeze',action='store_true');p.add_argument('--concurrency',type=int,choices=(1,2),default=1);a=p.parse_args();plan=json.loads(PLAN.read_text());counts=prepare_dispatch(dispatch_path=DISPATCH,plan=plan)
 result={'status':'planned','dispatch':counts,'model':'xai_speech_to_text_endpoint_unversioned','expected_calls':5,'estimated_cost_usd':plan['paid_network_preflight']['estimated_cost_usd'],'concurrency':a.concurrency,'provider_calls_started':False,'credential_contract':'XAI_API_KEY or XAI_API_KEY_FILE','execute_command':'python3 -B scripts/pif_signal_desk_gold_source_execute.py --execute --refreeze --concurrency 2'}
 if a.execute:run=execute(plan=plan,database_path=ROOT/'data/factory.sqlite',dispatch_path=DISPATCH,output_root=PRIVATE,concurrency=a.concurrency);write(RUN,run);result.update(status='source_complete' if run['complete'] else 'source_incomplete',provider_calls_started=True,run=run)
 if a.refreeze:
  run=json.loads(RUN.read_text());refreeze=build_refreeze(base_manifest_path=ROOT/'work/signal-desk-rebuild/benchmark/partial-manifest.json',source_plan=plan,run_receipt=run,database_path=ROOT/'data/factory.sqlite',output_root=PRIVATE,project_root=ROOT);write(REFREEZE,refreeze);result.update(status='refreeze_ready',refreeze={k:refreeze[k] for k in ('replacement_window_count','ready_for_development_gold_authoring','receipt_sha256')})
 print(json.dumps(result,indent=2,sort_keys=True));return 0
if __name__=='__main__':raise SystemExit(main())
