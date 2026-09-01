#!/usr/bin/env python3
"""Freeze the paid gold-authoring/A1/A2 execution plan without model calls."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PIF_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIF_ROOT))

from research_factory.signal_desk_rebuild_authoring import (  # noqa: E402
    build_authoring_plan,
    materialize_authoring_inputs,
    write_authoring_plan,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest",
        type=Path,
        default=PIF_ROOT / "work/signal-desk-rebuild/benchmark/partial-manifest.json",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=PIF_ROOT / "work/signal-desk-rebuild/gold-authoring/plan.json",
    )
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--materialize-inputs", action="store_true")
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    plan = build_authoring_plan(manifest, concurrency=args.concurrency)
    write_authoring_plan(args.output, plan)
    receipt = None
    if args.materialize_inputs:
        receipt = materialize_authoring_inputs(
            manifest,
            project_root=PIF_ROOT,
            output_root=args.output.parent / "inputs",
        )
        receipt_path = args.output.parent / "input-receipt.json"
        receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({
        "plan_sha256": plan["plan_sha256"],
        "manifest_sha256": plan["manifest_sha256"],
        "calls": plan["calls"],
        "sealed_item_outputs": True,
        "paid_calls_started": False,
        "inputs_materialized": bool(receipt),
        "input_receipt_sha256": receipt["receipt_sha256"] if receipt else None,
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
