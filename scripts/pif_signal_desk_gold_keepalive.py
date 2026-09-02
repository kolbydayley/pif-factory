#!/usr/bin/env python3
"""Relaunch the Signal Desk Gold resume runner when it dies for a resumable reason.

``--once`` is for the scheduler backstop (idempotent, exits immediately).
``--loop`` is for a long-lived tmux window.  Both never relaunch over an
operator KILL receipt, a complete campaign, or an operator-required checkpoint.
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

PIF_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIF_ROOT))

from research_factory.signal_desk_gold_keepalive import run_once  # noqa: E402

GOLD_ROOT = PIF_ROOT / "work/signal-desk-rebuild/gold-authoring-v2"
BUDGET_DIR = PIF_ROOT / "work/pif-ops/budget"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--once", action="store_true")
    mode.add_argument("--loop", action="store_true")
    parser.add_argument("--interval-seconds", type=int, default=60)
    args = parser.parse_args(argv)
    while True:
        decision = run_once(project_root=PIF_ROOT, gold_root=GOLD_ROOT, budget_dir=BUDGET_DIR)
        print(f"{decision.action}: {decision.reason}", flush=True)
        if args.once or decision.action == "done":
            return 0
        time.sleep(max(15, args.interval_seconds))


if __name__ == "__main__":
    raise SystemExit(main())
