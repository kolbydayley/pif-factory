#!/usr/bin/env python3
"""Run the J2/J3 Gold B/C/audit measurement using the frozen Gold A canary."""

import argparse
import asyncio
import json
import sys
from pathlib import Path

PIF_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIF_ROOT))

from research_factory.signal_desk_gold_measurement import run_measurement  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--concurrency", type=int, default=4)
    args = parser.parse_args()
    receipt = asyncio.run(run_measurement(
        manifest_path=PIF_ROOT / "work/signal-desk-rebuild/benchmark/partial-manifest.json",
        project_root=PIF_ROOT,
        output_root=PIF_ROOT / "work/signal-desk-rebuild/gold-authoring/j2-j3-v1",
        existing_a_root=PIF_ROOT / "work/signal-desk-rebuild/gold-authoring/semantic-canary-v2",
        grant_path=PIF_ROOT / "config/signal_desk_gold_authoring_budget_grant.json",
        budget_database=PIF_ROOT / "data/factory.sqlite",
        budget_dir=PIF_ROOT / "work/pif-ops/budget",
        session_root=Path.home() / ".codex/sessions",
        concurrency=args.concurrency,
    ))
    print(json.dumps({
        "passed": receipt["passed"], "sample_windows": receipt["sample_windows"],
        "turn_metrics": receipt["turn_metrics"],
        "revised_total_program_tokens": receipt["revised_total_program_tokens"],
        "normal_5m_full_days_at_mean": receipt["normal_5m_full_days_at_mean"],
        "receipt_sha256": receipt["receipt_sha256"],
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
