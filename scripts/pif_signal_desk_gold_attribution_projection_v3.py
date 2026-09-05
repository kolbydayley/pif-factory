#!/usr/bin/env python3
"""Plan, execute, or compare the zero-provider-call Gold-C v3 pilot."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PIF_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIF_ROOT))

from research_factory.signal_desk_gold_repair_v3 import (  # noqa: E402
    VARIANT_ID, build_plan, compare, execute_projection,
)

MANIFEST = PIF_ROOT / "work/signal-desk-rebuild/benchmark/partial-manifest.json"
GOLD_ROOT = PIF_ROOT / "work/signal-desk-rebuild/gold-authoring-v2"
BASELINE_ROOT = GOLD_ROOT / "results/development"
TRUTH_ROOT = GOLD_ROOT / "results/development-adjudicated"
DEFAULT_ROOT = GOLD_ROOT / "results/development-attribution-variants" / VARIANT_ID


def _write(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pilot", action="store_true")
    parser.add_argument("--full", action="store_true")
    parser.add_argument("--execute", action="store_true", help="materialize deterministic projection; starts no provider calls")
    parser.add_argument("--compare", action="store_true")
    parser.add_argument("--output-root", type=Path)
    args = parser.parse_args()
    if args.pilot and args.full:
        parser.error("choose --pilot or --full")
    pilot = not args.full
    root = args.output_root or DEFAULT_ROOT / ("pilot" if pilot else "full")
    plan = build_plan(
        manifest_path=MANIFEST, project_root=PIF_ROOT, baseline_root=BASELINE_ROOT,
        output_root=root, pilot=pilot,
    )
    _write(root / "repair-plan.json", plan)
    if args.execute:
        _write(root / "variant-run-receipt.json", execute_projection(
            manifest_path=MANIFEST, project_root=PIF_ROOT, baseline_root=BASELINE_ROOT,
            output_root=root, plan=plan,
        ))
    if args.compare:
        receipt = compare(
            manifest_path=MANIFEST, project_root=PIF_ROOT, baseline_root=BASELINE_ROOT,
            truth_root=TRUTH_ROOT, variant_root=root,
            window_ids=plan["selected_window_ids"],
        )
        receipt["receipt_sha256"] = __import__("hashlib").sha256(
            json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        _write(root / "comparison-receipt.json", receipt)
    print(json.dumps({
        "status": "complete" if args.execute or args.compare else "planned",
        "variant_id": VARIANT_ID,
        "windows": plan["selected_window_count"],
        "expected_provider_calls": 0,
        "provider_calls_started": False,
        "output_root": str(root),
        "execute_command": "python3 -B scripts/pif_signal_desk_gold_attribution_projection_v3.py --pilot --execute --compare",
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
