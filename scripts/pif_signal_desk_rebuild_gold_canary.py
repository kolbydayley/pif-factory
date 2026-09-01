#!/usr/bin/env python3
"""Run the bounded ten-window semantic gold-authoring canary."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

PIF_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIF_ROOT))

from research_factory.signal_desk_rebuild_gold_canary import (  # noqa: E402
    record_canary_usage,
    run_gold_canary,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest", type=Path,
        default=PIF_ROOT / "work/signal-desk-rebuild/benchmark/partial-manifest.json",
    )
    parser.add_argument(
        "--output-root", type=Path,
        default=PIF_ROOT / "work/signal-desk-rebuild/gold-authoring/semantic-canary-v2",
    )
    parser.add_argument("--binary", default="codex")
    parser.add_argument("--budget-database", type=Path, default=PIF_ROOT / "data/factory.sqlite")
    args = parser.parse_args()
    receipt = asyncio.run(run_gold_canary(
        manifest_path=args.manifest.resolve(),
        project_root=PIF_ROOT,
        output_root=args.output_root.resolve(),
        binary=args.binary,
    ))
    budget = record_canary_usage(receipt, args.budget_database.resolve())
    print(json.dumps({
        "passed": receipt["passed"],
        "calls": receipt["calls"],
        "total_tokens": receipt["total_tokens"],
        "mean_tokens_per_call": receipt["mean_tokens_per_call"],
        "maximum_tokens_per_call": receipt["maximum_tokens_per_call"],
        "receipt_sha256": receipt["receipt_sha256"],
        "budget": budget,
    }, indent=2, sort_keys=True))
    return 0 if receipt["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
