"""Aggregate-only development reliability audit for adjudicated Gold C."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from .signal_desk_rebuild_evaluation import diagnostic_pairs, evaluate_windows
from .util import now_iso


SCHEMA_VERSION = "pif_signal_desk_dev_gold_audit_v1"
AGREEMENT_MINIMUM = 0.95
CRITICAL_ERROR_MAXIMUM = 0.01


class GoldAuditError(RuntimeError):
    pass


def _load_outputs(path: Path) -> dict[str, Mapping[str, Any]]:
    return {
        item.stem: json.loads(item.read_text(encoding="utf-8"))
        for item in sorted(path.glob("*.json"))
    }


def evaluate_dev_audit(
    *, manifest_path: Path, result_root: Path, expected_windows: int = 19
) -> dict[str, Any]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    metadata = {str(row["window_id"]): row for row in manifest["windows"]}
    adjudicated = _load_outputs(result_root / "C")
    audit = _load_outputs(result_root / "AUDIT")
    common = sorted(set(adjudicated) & set(audit))
    if len(common) != expected_windows:
        raise GoldAuditError(
            f"development audit is incomplete: {len(common)}/{expected_windows} windows"
        )
    rows = []
    critical_errors = 0
    catastrophic_windows = 0
    critical_denominator = 0
    for window_id in common:
        meta = metadata[window_id]
        gold = adjudicated[window_id]
        independent = audit[window_id]
        structure = str(meta["transcript_structure"])
        pairs = diagnostic_pairs(
            gold["events"], independent["events"], transcript_structure=structure
        )
        critical_denominator += len(gold["events"])
        paired_gold = {int(pair["gold_index"]) for pair in pairs}
        window_critical = len(gold["events"]) - len(paired_gold)
        for pair in pairs:
            agreement = pair["field_agreement"]
            if not agreement["stance"] or not agreement["speaker"] or pair["unsupported_attribution"]:
                window_critical += 1
        if window_critical:
            catastrophic_windows += 1
            critical_errors += window_critical
        rows.append(
            {
                "window_id": window_id,
                "show_id": meta["show_id"],
                "episode_id": meta["episode_id"],
                "transcript_structure": structure,
                "gold": gold,
                "predicted": independent,
            }
        )
    evaluation = evaluate_windows(rows)
    agreement_numerator = 2 * evaluation["counts"]["diagnostic_pairs"]
    agreement_denominator = (
        evaluation["counts"]["gold_events"] + evaluation["counts"]["predicted_events"]
    )
    agreement = agreement_numerator / agreement_denominator if agreement_denominator else 1.0
    critical_rate = critical_errors / critical_denominator if critical_denominator else 0.0
    passed = agreement >= AGREEMENT_MINIMUM and critical_rate < CRITICAL_ERROR_MAXIMUM
    receipt: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "created_at": now_iso(),
        "status": "passed" if passed else "failed",
        "passed": passed,
        "split": "development",
        "audited_windows": len(common),
        "agreement": {
            "metric": "diagnostic_event_f1",
            "numerator": agreement_numerator,
            "denominator": agreement_denominator,
            "point": round(agreement, 6),
            "minimum": AGREEMENT_MINIMUM,
            "passed": agreement >= AGREEMENT_MINIMUM,
        },
        "critical_errors": {
            "definition": (
                "adjudicated event absent from independent audit or a paired event with "
                "speaker, stance, or unsupported-attribution disagreement"
            ),
            "errors": critical_errors,
            "event_denominator": critical_denominator,
            "point": round(critical_rate, 6),
            "maximum_exclusive": CRITICAL_ERROR_MAXIMUM,
            "passed": critical_rate < CRITICAL_ERROR_MAXIMUM,
        },
        "catastrophic_windows": catastrophic_windows,
        "aggregate_metrics": evaluation["metrics"],
        "strata": evaluation["strata"],
        "item_outputs_exposed": False,
        "manifest_sha256": manifest["manifest_sha256"],
    }
    receipt["receipt_sha256"] = hashlib.sha256(
        json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return receipt
