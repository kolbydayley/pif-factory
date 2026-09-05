#!/usr/bin/env python3
"""Plan, execute, or compare the deterministic explicit-speaker v4 pilot."""

from __future__ import annotations
import argparse, json, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from research_factory.signal_desk_gold_repair_v4 import VARIANT_ID, build_plan, compare, execute  # noqa:E402

MANIFEST = ROOT / "work/signal-desk-rebuild/benchmark/partial-manifest.json"
GOLD = ROOT / "work/signal-desk-rebuild/gold-authoring-v2"
BASELINE = GOLD / "results/development"
TRUTH = GOLD / "results/development-adjudicated"
V3 = GOLD / "results/development-attribution-variants/gold-c-structured-speaker-projection-v3/pilot"
DEFAULT = GOLD / "results/development-attribution-variants" / VARIANT_ID / "pilot"
DB = ROOT / "data/factory.sqlite"

def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True); path.write_text(json.dumps(value, indent=2, sort_keys=True)+"\n")

def main():
    p=argparse.ArgumentParser(description=__doc__); p.add_argument("--execute",action="store_true"); p.add_argument("--compare",action="store_true"); p.add_argument("--output-root",type=Path)
    a=p.parse_args(); out=a.output_root or DEFAULT
    plan=build_plan(manifest_path=MANIFEST,baseline_root=BASELINE,v3_root=V3,database_path=DB,output_root=out); write(out/"repair-plan.json",plan)
    if a.execute: write(out/"variant-run-receipt.json",execute(manifest_path=MANIFEST,project_root=ROOT,baseline_root=BASELINE,v3_root=V3,database_path=DB,output_root=out,plan=plan))
    if a.compare: write(out/"comparison-receipt.json",compare(manifest_path=MANIFEST,project_root=ROOT,baseline_root=BASELINE,truth_root=TRUTH,variant_root=out,window_ids=plan["selected_window_ids"]))
    print(json.dumps({"status":"complete" if a.execute or a.compare else "planned","variant_id":VARIANT_ID,"windows":plan["selected_window_count"],"expected_provider_calls":0,"provider_calls_started":False,"execute_command":"python3 -B scripts/pif_signal_desk_gold_explicit_speaker_variant_v4.py --execute --compare"},indent=2,sort_keys=True)); return 0
if __name__ == "__main__": raise SystemExit(main())
