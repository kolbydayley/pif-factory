"""Aggregate-only development reliability audit for adjudicated Gold C."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from .signal_desk_rebuild_evaluation import diagnostic_pairs, evaluate_windows
from .signal_desk_rebuild_gates import evaluate_rate_gate
from .util import now_iso


SCHEMA_VERSION = "pif_signal_desk_dev_gold_audit_v1"
AGREEMENT_MINIMUM = 0.95
CRITICAL_ERROR_MAXIMUM = 0.01
MINIMUM_CONSEQUENTIAL_EVENTS = 1_000
EXPANSION_BLOCK_WINDOWS = 40


class GoldAuditError(RuntimeError):
    pass


def select_dev_audit_windows(
    manifest: Mapping[str, Any], event_counts: Mapping[str, int], *,
    initial_window_ids: Sequence[str], minimum_events: int = MINIMUM_CONSEQUENTIAL_EVENTS,
    expansion_block: int = EXPANSION_BLOCK_WINDOWS,
) -> dict[str, Any]:
    """Expand the frozen 19-window dev slice in preordered 40-window blocks."""

    dev_ids = {
        str(row["window_id"])
        for row in manifest["windows"]
        if row["split"] == "development"
    }
    selected = [window_id for window_id in initial_window_ids if window_id in dev_ids]
    if len(selected) != len(set(selected)) or set(selected) - dev_ids:
        raise GoldAuditError("initial development audit ids are invalid")
    missing_counts = dev_ids - set(event_counts)
    if missing_counts:
        raise GoldAuditError(f"development event counts missing for {len(missing_counts)} windows")
    remaining = sorted(
        dev_ids - set(selected),
        key=lambda window_id: hashlib.sha256(
            f"signal-desk-dev-audit-expansion-v1:{window_id}".encode()
        ).hexdigest(),
    )
    event_total = sum(int(event_counts[window_id]) for window_id in selected)
    expansion_blocks = 0
    while event_total < minimum_events and remaining:
        block, remaining = remaining[:expansion_block], remaining[expansion_block:]
        selected.extend(block)
        event_total += sum(int(event_counts[window_id]) for window_id in block)
        expansion_blocks += 1
    return {
        "window_ids": tuple(selected),
        "initial_windows": len(initial_window_ids),
        "expanded_windows": len(selected),
        "expansion_blocks": expansion_blocks,
        "event_denominator": event_total,
        "minimum_events": minimum_events,
        "decision_ready": event_total >= minimum_events,
    }


def _load_outputs(path: Path) -> dict[str, Mapping[str, Any]]:
    return {
        item.stem: json.loads(item.read_text(encoding="utf-8"))
        for item in sorted(path.glob("*.json"))
    }


def evaluate_dev_audit(
    *, manifest_path: Path, result_root: Path, expected_windows: int = 19,
    blind_window_ids: Sequence[str] | None = None,
    initial_windows: int = 19,
    minimum_events: int = MINIMUM_CONSEQUENTIAL_EVENTS,
    agreement_minimum: float = AGREEMENT_MINIMUM,
    critical_error_maximum: float = CRITICAL_ERROR_MAXIMUM,
) -> dict[str, Any]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    metadata = {str(row["window_id"]): row for row in manifest["windows"]}
    adjudicated = _load_outputs(result_root / "C")
    audit = _load_outputs(result_root / "AUDIT")
    eligible_ids = set(blind_window_ids) if blind_window_ids is not None else set(audit)
    common = sorted(set(adjudicated) & set(audit) & eligible_ids)
    if len(common) != expected_windows:
        raise GoldAuditError(
            f"development audit is incomplete: {len(common)}/{expected_windows} windows"
        )
    rows = []
    critical_errors = 0
    catastrophic_window_ids: set[str] = set()
    critical_denominator = 0
    for window_id in common:
        meta = metadata[window_id]
        gold = adjudicated[window_id]
        independent = audit[window_id]
        if str(gold.get("window_id")) != window_id or str(independent.get("window_id")) != window_id:
            catastrophic_window_ids.add(window_id)
        if (
            independent.get("window_disposition") == "unusable_input"
            and gold.get("window_disposition") == "claims_found"
        ):
            catastrophic_window_ids.add(window_id)
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
            gold_claim = str(gold["events"][int(pair["gold_index"])]["claim_text"]).casefold()
            predicted_claim = str(
                independent["events"][int(pair["predicted_index"])]["claim_text"]
            ).casefold()
            negations = (" not ", " never ", " no ", " cannot ", " won't ", " isn't ", "n't ")
            if any(token in f" {gold_claim} " for token in negations) != any(
                token in f" {predicted_claim} " for token in negations
            ):
                catastrophic_window_ids.add(window_id)
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
    agreement_gate = evaluate_rate_gate(
        agreement_numerator, agreement_denominator,
        threshold=agreement_minimum, direction="minimum",
    )
    critical_gate = evaluate_rate_gate(
        critical_errors, critical_denominator,
        threshold=critical_error_maximum, direction="maximum",
    )
    powered = critical_denominator >= minimum_events
    passed = (
        agreement_gate["passed"]
        and critical_gate["passed"]
        and not catastrophic_window_ids
        and powered
    )
    receipt: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "created_at": now_iso(),
        "status": "passed" if passed else "failed",
        "passed": passed,
        "split": "development",
        "audited_windows": len(common),
        "initial_windows": initial_windows,
        "expansion_windows": max(0, len(common) - initial_windows),
        "event_power": {
            "event_denominator": critical_denominator,
            "minimum": minimum_events,
            "passed": powered,
        },
        "agreement": {
            "metric": "diagnostic_event_f1",
            "numerator": agreement_numerator,
            "denominator": agreement_denominator,
            **agreement_gate,
        },
        "critical_errors": {
            "definition": (
                "adjudicated event absent from independent audit or a paired event with "
                "speaker, stance, or unsupported-attribution disagreement"
            ),
            "errors": critical_errors,
            "event_denominator": critical_denominator,
            "one_sided_wilson_ucb": critical_gate["bound"],
            **critical_gate,
        },
        "catastrophic": {
            "definition": (
                "wrong window/source identity, unusable context accepted as claims, "
                "or paired claims with reversed negation polarity"
            ),
            "windows": len(catastrophic_window_ids),
            "passed": not catastrophic_window_ids,
        },
        "catastrophic_windows": len(catastrophic_window_ids),
        "aggregate_metrics": evaluation["metrics"],
        "strata": evaluation["strata"],
        "item_outputs_exposed": False,
        "manifest_sha256": manifest["manifest_sha256"],
    }
    receipt["receipt_sha256"] = hashlib.sha256(
        json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return receipt
