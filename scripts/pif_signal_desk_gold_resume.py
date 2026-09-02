#!/usr/bin/env python3
"""Resume only the user-authorized Signal Desk Gold authoring stages.

This supervisor deliberately owns no A1 calibration or A2 scorer work.  It
uses the frozen staged order in ``signal_desk_gold_runner`` and leaves the
operator-controlled Gold kill/grant receipts to the budget subsystem.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path


PIF_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIF_ROOT))

from research_factory.signal_desk_gold_runner import (  # noqa: E402
    GoldRunnerError,
    run_gold_resume_supervisor,
)


def _write_checkpoint(root: Path, receipt: dict[str, object]) -> None:
    """Persist only aggregate stage state; never transcript-bearing packets."""

    target = root / "artifacts" / "gold-resume-supervisor.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--allow-sealed-holdout",
        action="store_true",
        help="required explicit authorization before any sealed-holdout Gold call",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=8,
        help="maximum adaptive Gold worker pool (fresh lane still begins at 2)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="return after one foreground-safe attempt or deferred checkpoint",
    )
    parser.add_argument("--binary", default="codex")
    args = parser.parse_args()
    if not args.allow_sealed_holdout:
        parser.error("--allow-sealed-holdout is required for this authorized staged run")
    if not 2 <= args.concurrency <= 8:
        parser.error("--concurrency must be between 2 and 8")

    gold_root = PIF_ROOT / "work/signal-desk-rebuild/gold-authoring-v2"
    dev_seed_roots = {
        "A": PIF_ROOT / "work/signal-desk-rebuild/gold-authoring/semantic-canary-v2/private-development",
        "B": PIF_ROOT / "work/signal-desk-rebuild/gold-authoring/j2-j3-v1/private-b",
        "C": PIF_ROOT / "work/signal-desk-rebuild/gold-authoring/j2-j3-v1/private-c",
        "AUDIT": PIF_ROOT / "work/signal-desk-rebuild/gold-authoring/j2-j3-v1/private-audit",
    }
    while True:
        try:
            receipt = asyncio.run(
                run_gold_resume_supervisor(
                    manifest_path=PIF_ROOT / "work/signal-desk-rebuild/benchmark/partial-manifest.json",
                    project_root=PIF_ROOT,
                    gold_root=gold_root,
                    budget_database=PIF_ROOT / "data/factory.sqlite",
                    grant_path=PIF_ROOT / "config/signal_desk_gold_authoring_budget_grant.json",
                    session_root=Path.home() / ".codex/sessions",
                    budget_dir=PIF_ROOT / "work/pif-ops/budget",
                    allow_sealed_holdout=True,
                    seed_roots_by_split={"development": dev_seed_roots},
                    concurrency=args.concurrency,
                    binary=args.binary,
                )
            )
            _write_checkpoint(gold_root, receipt)
            print(json.dumps(receipt, indent=2, sort_keys=True))
            if receipt["status"] == "complete":
                return 0
            # Only the two explicitly transient, zero-call states are
            # supervised.  A manifest, prerequisite, contract, or semantic
            # failure must remain fail-closed for operator adjudication.
            reason = str(receipt.get("reason") or "")
            transient = (
                reason == "foreground_codex_active"
                or reason == "recent_local_input"
                or reason == "foreground_state_unknown"
                or reason.startswith("gold_model_capacity_")
            )
            if not transient:
                failure = {
                    "schema_version": "pif_signal_desk_gold_resume_supervision_v1",
                    "status": "checkpointed_failure",
                    "error": f"unexpected non-transient deferred state: {reason}",
                    "contains_a1_or_a2": False,
                }
                _write_checkpoint(gold_root, failure)
                return 2
            if args.once:
                return 0
            time.sleep(min(max(1, int(receipt["retry_after_seconds"])), 60))
        except GoldRunnerError as exc:
            failure = {
                "schema_version": "pif_signal_desk_gold_resume_supervision_v1",
                "status": "checkpointed_failure",
                "error": str(exc),
                "contains_a1_or_a2": False,
            }
            _write_checkpoint(gold_root, failure)
            print(json.dumps(failure, indent=2, sort_keys=True), file=sys.stderr)
            # All runner exceptions are non-transient by construction.  The
            # capacity and foreground paths return a deferred receipt above.
            return 2


if __name__ == "__main__":
    raise SystemExit(main())
