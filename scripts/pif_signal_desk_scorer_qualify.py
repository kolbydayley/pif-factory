#!/usr/bin/env python3
"""Run or resume the A2 scorer qualification adjudications."""

import argparse
import asyncio
import json
import sys
from pathlib import Path

PIF_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIF_ROOT))

from research_factory.signal_desk_scorer_runner import run_scorer_qualification  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--total", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=4)
    args = parser.parse_args()
    root = PIF_ROOT / "work/signal-desk-rebuild/gold-authoring-v2"
    receipt = asyncio.run(
        run_scorer_qualification(
            manifest_path=PIF_ROOT / "work/signal-desk-rebuild/benchmark/partial-manifest.json",
            project_root=PIF_ROOT,
            gold_root=root / "results/development/C",
            prediction_root=root / "results/development/A1",
            artifact_root=root / "artifacts",
            dispatch_database=root / "dispatch-a2.sqlite",
            budget_database=PIF_ROOT / "data/factory.sqlite",
            budget_dir=PIF_ROOT / "work/pif-ops/budget",
            total=args.total,
            concurrency=args.concurrency,
        )
    )
    print(json.dumps(receipt, indent=2, sort_keys=True))
    return 0 if receipt["status"] == "qualified" else 2


if __name__ == "__main__":
    raise SystemExit(main())
