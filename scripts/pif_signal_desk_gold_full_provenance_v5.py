#!/usr/bin/env python3
"""Plan, execute, or compare full-development Gold-C provenance V5."""
from __future__ import annotations
import argparse,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from research_factory.signal_desk_gold_repair_v5 import VARIANT_ID,build_plan,compare,execute  # noqa:E402
M=ROOT/'work/signal-desk-rebuild/benchmark/partial-manifest.json';G=ROOT/'work/signal-desk-rebuild/gold-authoring-v2';B=G/'results/development';T=G/'results/development-adjudicated';V3=G/'results/development-attribution-variants/gold-c-structured-speaker-projection-v3/full';OUT=G/'results/development-attribution-variants'/VARIANT_ID/'full';DB=ROOT/'data/factory.sqlite'
def write(p,v):p.parent.mkdir(parents=True,exist_ok=True);p.write_text(json.dumps(v,indent=2,sort_keys=True)+'\n')
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--execute',action='store_true');p.add_argument('--compare',action='store_true');p.add_argument('--output-root',type=Path);a=p.parse_args();out=a.output_root or OUT
 plan=build_plan(manifest_path=M,baseline_root=B,v3_root=V3,database_path=DB,output_root=out);write(out/'repair-plan.json',plan)
 if a.execute:write(out/'variant-run-receipt.json',execute(manifest_path=M,project_root=ROOT,baseline_root=B,v3_root=V3,database_path=DB,output_root=out,plan=plan))
 if a.compare:write(out/'comparison-receipt.json',compare(manifest_path=M,project_root=ROOT,baseline_root=B,truth_root=T,variant_root=out,window_ids=plan['selected_window_ids']))
 print(json.dumps({'status':'complete' if a.execute or a.compare else 'planned','variant_id':VARIANT_ID,'windows':189,'provider_calls_started':False,'deterministic_calls':0,'planned_wider_context_calls':plan['expected_calls']['gpt_5_5_wider_context_planned'],'execute_command':'python3 -B scripts/pif_signal_desk_gold_full_provenance_v5.py --execute --compare'},indent=2,sort_keys=True));return 0
if __name__=='__main__':raise SystemExit(main())
