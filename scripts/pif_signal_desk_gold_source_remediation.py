#!/usr/bin/env python3
"""Write the content-free development source-remediation preflight."""
from __future__ import annotations
import json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from research_factory.signal_desk_gold_source_remediation import build_plan  # noqa:E402
G=ROOT/'work/signal-desk-rebuild/gold-authoring-v2';V6=G/'results/development-attribution-variants/gold-c-gpt55-wider-speaker-projection-v6/full';OUT=G/'artifacts/gold-source-remediation-plan-v1.json'
def main():
 plan=build_plan(manifest_path=ROOT/'work/signal-desk-rebuild/benchmark/partial-manifest.json',v6_root=V6,database_path=ROOT/'data/factory.sqlite',project_root=ROOT,xai_contract_path=ROOT/'config/signal_desk_rebuild_xai_asr_contract.json')
 OUT.parent.mkdir(parents=True,exist_ok=True);OUT.write_text(json.dumps(plan,indent=2,sort_keys=True)+'\n')
 print(json.dumps({'status':'preflight_complete','provider_calls_started':0,'residual_claims':plan['residual_claim_count'],'invalid_source_episodes':plan['invalid_source_episode_count'],'windows_to_refreeze':plan['development_windows_requiring_refreeze_and_new_gold'],'paid_network_preflight':plan['paid_network_preflight']},indent=2,sort_keys=True));return 0
if __name__=='__main__':raise SystemExit(main())
