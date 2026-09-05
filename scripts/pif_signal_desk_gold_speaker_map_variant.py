#!/usr/bin/env python3
"""Plan, execute, or compare the isolated development Gold-C v2 family.

Default behavior is plan-only.  ``--execute`` is the only provider-call path;
it imports frozen baseline A/B and runs C for the deterministic pilot or all
189 development windows.  Validation and sealed-holdout data are never read.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import sys
from pathlib import Path

PIF_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIF_ROOT))

from research_factory.signal_desk_gold_repair_v2 import (  # noqa: E402
    FULL_WINDOW_COUNT,
    PROMPT_VARIANT_ID,
    REPRESENTATION_VARIANT_ID,
    build_repair_plan,
    baseline_ab_digest,
    compare_outputs,
    development_windows,
    select_powered_pilot_windows,
)
from research_factory.signal_desk_gold_runner import (  # noqa: E402
    gold_model_admission,
    run_gold_split_phase,
)


MANIFEST = PIF_ROOT / "work/signal-desk-rebuild/benchmark/partial-manifest.json"
GOLD_ROOT = PIF_ROOT / "work/signal-desk-rebuild/gold-authoring-v2"
BASELINE_ROOT = GOLD_ROOT / "results/development"
TRUTH_ROOT = GOLD_ROOT / "results/development-adjudicated"
DEFAULT_ROOT = GOLD_ROOT / "results/development-representation-variants" / REPRESENTATION_VARIANT_ID


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _selected_ids(*, pilot: bool) -> list[str]:
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    if pilot:
        return select_powered_pilot_windows(manifest=manifest, baseline_c_root=BASELINE_ROOT / "C")
    return sorted(str(row["window_id"]) for row in development_windows(manifest))


async def _execute(*, root: Path, selected_ids: list[str], concurrency: int, expected_ab_digest: str) -> dict:
    observed_ab_digest = baseline_ab_digest(baseline_root=BASELINE_ROOT, window_ids=selected_ids)
    if observed_ab_digest != expected_ab_digest:
        raise RuntimeError("frozen baseline A/B digest changed after plan creation")
    dispatch = GOLD_ROOT / f"dispatch-{REPRESENTATION_VARIANT_ID}.sqlite"
    receipt = await run_gold_split_phase(
        manifest_path=MANIFEST,
        project_root=PIF_ROOT,
        result_root=root,
        dispatch_database=dispatch,
        budget_database=PIF_ROOT / "data/factory.sqlite",
        grant_path=PIF_ROOT / "config/signal_desk_gold_authoring_budget_grant.json",
        session_root=Path.home() / ".codex/sessions",
        budget_dir=PIF_ROOT / "work/pif-ops/budget",
        split="development",
        turn_type="C",
        task_namespace=f"dev-{REPRESENTATION_VARIANT_ID}",
        seed_roots={"A": BASELINE_ROOT / "A", "B": BASELINE_ROOT / "B"},
        concurrency=concurrency,
        foreground_admission=gold_model_admission,
        prompt_variant_id=None,
        representation_variant_id=REPRESENTATION_VARIANT_ID,
        target_window_ids=selected_ids,
    )
    _write_json(root / "variant-run-receipt.json", receipt)
    return receipt


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pilot", action="store_true", help="plan/execute the 40-window powered pilot")
    parser.add_argument("--full", action="store_true", help="plan/execute all 189 development windows")
    parser.add_argument("--execute", action="store_true", help="explicitly opt in to provider calls")
    parser.add_argument("--compare", action="store_true", help="compare completed C outputs against adjudicated truth")
    parser.add_argument("--concurrency", type=int, choices=(2, 3, 4, 5, 6, 7, 8), default=2)
    parser.add_argument("--output-root", type=Path, default=None)
    args = parser.parse_args()
    if args.pilot and args.full:
        parser.error("choose --pilot or --full")
    pilot = not args.full
    selected = _selected_ids(pilot=pilot)
    root = args.output_root or DEFAULT_ROOT / ("pilot" if pilot else "full")
    plan = build_repair_plan(
        manifest_path=MANIFEST, project_root=PIF_ROOT, baseline_root=BASELINE_ROOT,
        output_root=root, pilot=pilot,
    )
    plan["selected_window_ids"] = selected
    plan["selected_window_ids_sha256"] = hashlib.sha256(
        json.dumps(selected, separators=(",", ":")).encode()
    ).hexdigest()
    _write_json(root / "repair-plan.json", plan)
    if args.compare:
        receipt = compare_outputs(
            manifest_path=MANIFEST, baseline_root=BASELINE_ROOT, truth_root=TRUTH_ROOT,
            variant_root=root, window_ids=selected, project_root=PIF_ROOT,
        )
        _write_json(root / "comparison-receipt.json", receipt)
    if args.execute:
        receipt = asyncio.run(_execute(
            root=root, selected_ids=selected, concurrency=args.concurrency,
            expected_ab_digest=str(plan["baseline_ab_digest"]),
        ))
        print(json.dumps(receipt, indent=2, sort_keys=True))
    else:
        print(json.dumps({
            "status": "planned",
            "provider_calls_started": False,
            "variant_id": REPRESENTATION_VARIANT_ID,
            "prompt_variant_id": PROMPT_VARIANT_ID,
            "windows": len(selected),
            "expected_calls": len(selected),
            "reserved_token_ceiling": len(selected) * 57_000,
            "output_root": str(root),
            "execute_command": (
                "python3 -B scripts/pif_signal_desk_gold_speaker_map_variant.py "
                + ("--pilot " if pilot else "--full ")
                + "--execute --concurrency 2"
            ),
        }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
