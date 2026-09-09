"""Measure and freeze the A1 single-pass development frontier ceiling."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from .signal_desk_rebuild_contracts import contract_sha256
from .signal_desk_rebuild_evaluation import evaluate_windows
from .signal_desk_rebuild_gates import (
    bootstrap_window_metric_bounds,
    freeze_frontier_ceiling,
)
from .signal_desk_rebuild_scorer import scorer_sha256
from .util import now_iso
from .gold_exclusions import default_exclusion_path, load_exclusions, load_gold_windows


SCHEMA_VERSION = "pif_signal_desk_frontier_ceiling_v2"


class FrontierCalibrationError(RuntimeError):
    pass


def _load_outputs(path: Path) -> dict[str, Mapping[str, Any]]:
    return {
        item.stem: json.loads(item.read_text(encoding="utf-8"))
        for item in sorted(path.glob("*.json"))
    }


def _sha(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def measure_frontier(
    *, manifest_path: Path, gold_c_root: Path, prediction_root: Path
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return the measured ceiling and absolute gates from one shared A1 run."""

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    development = {
        str(row["window_id"]): row
        for row in manifest["windows"]
        if row["split"] == "development"
    }
    # audited-illegitimate claims are dropped in memory; sealed files are untouched
    gold = load_gold_windows(
        gold_c_root, exclusions=load_exclusions(default_exclusion_path(gold_c_root))
    )
    predictions = _load_outputs(prediction_root)
    missing_gold = sorted(set(development) - set(gold))
    missing_predictions = sorted(set(development) - set(predictions))
    if missing_gold or missing_predictions:
        raise FrontierCalibrationError(
            f"A1 inputs incomplete: gold={len(missing_gold)} predictions={len(missing_predictions)}"
        )
    rows = [
        {
            "window_id": window_id,
            "show_id": meta["show_id"],
            "episode_id": meta["episode_id"],
            "transcript_structure": meta["transcript_structure"],
            "gold": gold[window_id],
            "predicted": predictions[window_id],
        }
        for window_id, meta in sorted(development.items())
    ]
    evaluation = evaluate_windows(rows)
    split_sha = _sha(
        [
            (window_id, row["text_sha256"], row["show_id"], row["episode_id"])
            for window_id, row in sorted(development.items())
        ]
    )
    run_sha = _sha(
        [
            (window_id, hashlib.sha256((prediction_root / f"{window_id}.json").read_bytes()).hexdigest())
            for window_id in sorted(development)
        ]
    )
    window_metric_bounds = bootstrap_window_metric_bounds(
        evaluation["per_window_scores"],
        metrics=tuple(sorted(evaluation["metrics"])),
    )
    gates = freeze_frontier_ceiling(
        evaluation["metrics"],
        window_metric_bounds=window_metric_bounds,
        scorer_sha256=scorer_sha256(),
        contract_sha256=contract_sha256(),
        split_sha256=split_sha,
        run_sha256=run_sha,
    )
    receipt: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "created_at": now_iso(),
        "model": "gpt-5.6-sol",
        "reasoning_effort": "medium",
        "passes": 1,
        "split": "development",
        "window_characters": 6000,
        "windows": len(rows),
        "metrics": evaluation["metrics"],
        "micro_metrics": evaluation["micro_metrics"],
        "aggregation": evaluation["aggregation"],
        "window_bootstrap_bounds": window_metric_bounds,
        "counts": evaluation["counts"],
        "strata": evaluation["strata"],
        "item_outputs_exposed": False,
        "manifest_sha256": manifest["manifest_sha256"],
        "split_sha256": split_sha,
        "run_sha256": run_sha,
        "scorer_sha256": scorer_sha256(),
        "contract_sha256": contract_sha256(),
    }
    receipt["receipt_sha256"] = _sha(receipt)
    return receipt, gates
