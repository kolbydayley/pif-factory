#!/usr/bin/env python3
"""Plan, execute, and compare the deterministic V6 wider-speaker projection."""
from __future__ import annotations
import argparse,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from research_factory.signal_desk_gold_wider_projection import VARIANT_ID,build_plan,build_resurrection_receipt,compare,execute  # noqa:E402
G=ROOT/'work/signal-desk-rebuild/gold-authoring-v2';M=ROOT/'work/signal-desk-rebuild/benchmark/partial-manifest.json';B=G/'results/development';T=G/'results/development-adjudicated';V5=G/'results/development-attribution-variants/gold-c-full-source-provenance-v5/full';P=G/'private-gpt55-wider-speaker-v1';D=G/'dispatch-gpt55-wider-speaker-v1.sqlite';OUT=G/'results/development-attribution-variants'/VARIANT_ID/'full'
def write(p,v):p.parent.mkdir(parents=True,exist_ok=True);p.write_text(json.dumps(v,indent=2,sort_keys=True)+'\n')
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--execute',action='store_true');p.add_argument('--compare',action='store_true');p.add_argument('--output-root',type=Path);a=p.parse_args();out=a.output_root or OUT
 plan=build_plan(manifest_path=M,baseline_root=B,v5_root=V5,private_root=P,dispatch_path=D,output_root=out);write(out/'repair-plan.json',plan)
 failed=[p.name.removesuffix('.packet.json') for p in P.glob('*.packet.json') if not (P/p.name.replace('.packet.json','.decision.json')).exists()]
 if len(failed)!=1:raise RuntimeError(f'expected one failed packet, found {len(failed)}')
 write(out/'resurrection-receipt.json',build_resurrection_receipt(failed_window_id=failed[0],private_root=P,dispatch_path=D))
 if a.execute:write(out/'variant-run-receipt.json',execute(manifest_path=M,project_root=ROOT,baseline_root=B,v5_root=V5,private_root=P,dispatch_path=D,output_root=out,plan=plan))
 if a.compare:write(out/'comparison-receipt.json',compare(manifest_path=M,project_root=ROOT,baseline_root=B,truth_root=T,variant_root=out,window_ids=plan['selected_window_ids']))
 print(json.dumps({'status':'complete' if a.execute or a.compare else 'planned','variant_id':VARIANT_ID,'windows':len(plan['selected_window_ids']),'accepted_decisions':plan['accepted_decision_count'],'provider_calls_started':False,'execute_command':'python3 -B scripts/pif_signal_desk_gold_apply_wider_speakers.py --execute --compare'},indent=2,sort_keys=True));return 0
if __name__=='__main__':raise SystemExit(main())
