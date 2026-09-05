#!/usr/bin/env python3
"""Plan, run, or compare the development-only Gold prompt repair.

The default action is ``plan`` and makes no provider calls.  ``--execute`` is
an explicit opt-in for the future resumable A/B/C development rerun.  No
validation or sealed-holdout packets are ever opened by this entrypoint.
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

from research_factory.signal_desk_gold_prompt_variant import (  # noqa: E402
    VARIANT_ID,
    build_dev_variant_plan,
    build_rejected_event_taxonomy,
    variant_registry_entry,
)
from research_factory.signal_desk_gold_runner import (  # noqa: E402
    gold_model_admission,
    run_gold_split_phase,
)
from research_factory.signal_desk_rebuild_evaluation import evaluate_windows  # noqa: E402


MANIFEST = PIF_ROOT / "work/signal-desk-rebuild/benchmark/partial-manifest.json"
GOLD_ROOT = PIF_ROOT / "work/signal-desk-rebuild/gold-authoring-v2"
BASELINE_ROOT = GOLD_ROOT / "results/development"
TRUTH_ROOT = GOLD_ROOT / "results/development-adjudicated"
DEFAULT_ROOT = GOLD_ROOT / "results/development-prompt-variants" / VARIANT_ID


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _taxonomy() -> dict:
    return build_rejected_event_taxonomy(
        manifest_path=MANIFEST,
        result_root=BASELINE_ROOT,
        project_root=PIF_ROOT,
        audit_receipt_path=GOLD_ROOT / "artifacts/gold-audit-dev.json",
        private_decisions_root=GOLD_ROOT / "private-gpt55-disagreement-review",
    )


def _load_output(root: Path, turn: str, window_id: str) -> dict:
    return json.loads((root / turn / f"{window_id}.json").read_text(encoding="utf-8"))


def _comparison(root: Path) -> dict:
    """Compare variant Gold-C against adjudicated dev truth and baseline.

    Only aggregate evaluation fields are emitted; claim/evidence text never
    enters the receipt.  Comparison is intentionally unavailable until the
    explicit rerun has produced all 189 A/B/C outputs.
    """

    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    dev = [row for row in manifest["windows"] if row.get("split") == "development"]
    variant_c = root / "C"
    if not all((variant_c / f"{row['window_id']}.json").exists() for row in dev):
        raise RuntimeError("variant Gold-C is incomplete; run the explicit --execute plan first")
    rows = []
    for row in dev:
        window_id = str(row["window_id"])
        truth = _load_output(TRUTH_ROOT, "C", window_id)
        baseline = _load_output(BASELINE_ROOT, "C", window_id)
        variant = _load_output(root, "C", window_id)
        rows.append({
            "window_id": window_id,
            "show_id": row["show_id"],
            "episode_id": row["episode_id"],
            "transcript_structure": row["transcript_structure"],
            "gold": truth,
            "predicted": variant,
        })
        # Keep the baseline evaluation as a separate row set so the output
        # makes no claim that the variant changed the frozen truth.
    variant_eval = evaluate_windows(rows)
    baseline_eval = evaluate_windows([
        {**row, "predicted": _load_output(BASELINE_ROOT, "C", str(row["window_id"]))}
        for row in rows
    ])
    def aggregate(eval_result: dict) -> dict:
        return {
            "evaluation_version": eval_result.get("evaluation_version"),
            "metrics": eval_result.get("metrics"),
            "micro_metrics": eval_result.get("micro_metrics"),
            "aggregation": eval_result.get("aggregation"),
            "strata": eval_result.get("strata"),
            "windows": len(eval_result.get("per_window_scores") or []),
        }
    receipt = {
        "schema_version": "pif_signal_desk_gold_prompt_variant_comparison_v1",
        "variant_id": VARIANT_ID,
        "development_only": True,
        "sealed_items_opened": False,
        "variant": aggregate(variant_eval),
        "baseline": aggregate(baseline_eval),
        "delta": {
            key: variant_eval.get("metrics", {}).get(key, 0)
            - baseline_eval.get("metrics", {}).get(key, 0)
            for key in ("macro_composite", "event_precision", "event_recall", "attribution", "speaker_role", "stance", "atomicity")
        },
    }
    receipt["receipt_sha256"] = hashlib.sha256(
        json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return receipt


async def _execute(root: Path, concurrency: int) -> dict:
    dispatch = GOLD_ROOT / "dispatch-dev-prompt-variant-gold-c-context-adjudication-v1.sqlite"
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
        turn_type="PIPELINE",
        task_namespace="dev-prompt-variant-gold-c-context-adjudication-v1",
        seed_roots={},
        concurrency=concurrency,
        prompt_variant_id=VARIANT_ID,
        foreground_admission=gold_model_admission,
    )
    _write_json(root / "variant-run-receipt.json", receipt)
    return receipt


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true", help="opt in to provider calls")
    parser.add_argument("--compare", action="store_true", help="compare completed variant outputs")
    parser.add_argument("--taxonomy", action="store_true", help="emit the development rejection taxonomy")
    parser.add_argument("--concurrency", type=int, choices=(2, 3, 4, 5, 6, 7, 8), default=2)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_ROOT)
    args = parser.parse_args()
    plan = build_dev_variant_plan(manifest_path=MANIFEST, output_root=args.output_root)
    args.output_root.mkdir(parents=True, exist_ok=True)
    _write_json(args.output_root / "variant-plan.json", plan)
    _write_json(args.output_root / "variant-registry-entry.json", variant_registry_entry())
    if args.taxonomy:
        _write_json(GOLD_ROOT / "artifacts/gold-c-repair-taxonomy-v1.json", _taxonomy())
    if args.compare:
        _write_json(args.output_root / "comparison-receipt.json", _comparison(args.output_root))
    if args.execute:
        receipt = asyncio.run(_execute(args.output_root, args.concurrency))
        print(json.dumps(receipt, indent=2, sort_keys=True))
    else:
        print(json.dumps({
            "status": "planned",
            "provider_calls_started": False,
            "variant_id": VARIANT_ID,
            "expected_calls": 567,
            "output_root": str(args.output_root),
            "execute_command": (
                "python3 -B scripts/pif_signal_desk_gold_prompt_variant.py "
                "--execute --concurrency 2"
            ),
        }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
