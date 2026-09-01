"""Statistical gates for the Signal Desk clean-corpus rebuild.

All hard rate gates use one-sided confidence bounds.  Aggregate paired metrics
use a deterministic stratified bootstrap.  The sealed holdout surface is
intentionally narrower than validation and cannot issue per-show verdicts.
"""

from __future__ import annotations

import hashlib
import json
import math
import random
from collections import defaultdict
from statistics import NormalDist
from typing import Any, Mapping, Sequence


GATE_MANIFEST_VERSION = "signal-desk-rebuild-gates-v1"
ALPHA = 0.05
BOOTSTRAP_ITERATIONS = 10_000
PER_SHOW_EVENT_FLOOR = 50
GOLD_AUDIT_INITIAL_WINDOWS = 120
GOLD_AUDIT_WINDOW_BLOCK = 40
GOLD_AUDIT_MIN_EVENTS = 1_000
GOLD_AUDIT_CRITICAL_ERROR_CEILING = 0.01
ALLOWED_HOLDOUT_METRICS = frozenset(
    {"macro_composite", "contamination", "ood_recall", "ood_precision"}
)


class SignalDeskGateError(ValueError):
    """Raised when a gate would violate the frozen evaluation protocol."""


def wilson_bounds(successes: int, total: int, *, alpha: float = ALPHA) -> tuple[float, float]:
    """One-sided Wilson lower and upper bounds at the stated alpha."""

    if total < 0 or successes < 0 or successes > total:
        raise SignalDeskGateError("successes and total must satisfy 0 <= successes <= total")
    if not 0.0 < alpha < 0.5:
        raise SignalDeskGateError("alpha must be between 0 and 0.5")
    if total == 0:
        return 0.0, 1.0
    z = NormalDist().inv_cdf(1.0 - alpha)
    p = successes / total
    denominator = 1.0 + z * z / total
    center = (p + z * z / (2.0 * total)) / denominator
    margin = z * math.sqrt(p * (1.0 - p) / total + z * z / (4.0 * total * total)) / denominator
    return max(0.0, center - margin), min(1.0, center + margin)


def evaluate_rate_gate(
    successes: int,
    total: int,
    *,
    threshold: float,
    direction: str = "minimum",
    alpha: float = ALPHA,
) -> dict[str, Any]:
    """Evaluate a binomial gate using its conservative one-sided bound."""

    if not 0.0 <= threshold <= 1.0:
        raise SignalDeskGateError("threshold must be in [0, 1]")
    lower, upper = wilson_bounds(successes, total, alpha=alpha)
    if direction == "minimum":
        bound, passed = lower, lower >= threshold
    elif direction == "maximum":
        # Here successes means observed failures/contaminants.
        bound, passed = upper, upper <= threshold
    else:
        raise SignalDeskGateError("direction must be 'minimum' or 'maximum'")
    return {
        "passed": passed,
        "successes": successes,
        "total": total,
        "point": round(successes / total, 6) if total else None,
        "bound": round(bound, 6),
        "threshold": threshold,
        "direction": direction,
        "alpha": alpha,
    }


def paired_stratified_bootstrap(
    candidate: Sequence[float],
    ceiling: Sequence[float],
    strata: Sequence[Any],
    *,
    iterations: int = BOOTSTRAP_ITERATIONS,
    alpha: float = ALPHA,
    seed: int = 20260831,
) -> dict[str, float]:
    """LCB for mean paired difference with fixed-size sampling per stratum."""

    if not candidate or len(candidate) != len(ceiling) or len(candidate) != len(strata):
        raise SignalDeskGateError("candidate, ceiling, and strata must have equal non-zero length")
    if iterations < 100:
        raise SignalDeskGateError("at least 100 bootstrap iterations are required")
    groups: dict[str, list[float]] = defaultdict(list)
    for candidate_value, ceiling_value, stratum in zip(candidate, ceiling, strata):
        groups[repr(stratum)].append(float(candidate_value) - float(ceiling_value))
    rng = random.Random(seed)
    sampled: list[float] = []
    for _ in range(iterations):
        values: list[float] = []
        for key in sorted(groups):
            group = groups[key]
            values.extend(group[rng.randrange(len(group))] for _ in range(len(group)))
        sampled.append(sum(values) / len(values))
    sampled.sort()
    lower_index = max(0, min(iterations - 1, math.floor(alpha * iterations)))
    point = sum(c - f for c, f in zip(candidate, ceiling)) / len(candidate)
    return {
        "point_difference": round(point, 6),
        "difference_lcb": round(sampled[lower_index], 6),
        "alpha": alpha,
        "iterations": iterations,
    }


def freeze_frontier_ceiling(
    metrics: Mapping[str, float],
    *,
    scorer_sha256: str,
    contract_sha256: str,
    split_sha256: str,
    run_sha256: str,
    model: str = "gpt-5.6-sol",
) -> dict[str, Any]:
    """Convert the measured single-pass development ceiling into fixed gates."""

    required = {
        "macro_composite",
        "event_recall",
        "event_precision",
        "attribution",
        "speaker_role",
        "issue",
        "stance",
        "atomicity",
        "contamination",
        "schema_validity",
        "evidence_grounding",
    }
    missing = sorted(required - metrics.keys())
    if missing:
        raise SignalDeskGateError(f"frontier ceiling is missing metrics: {', '.join(missing)}")
    measured = {key: float(metrics[key]) for key in sorted(required)}
    if any(not 0.0 <= value <= 1.0 for value in measured.values()):
        raise SignalDeskGateError("all frontier metrics must be in [0, 1]")
    gates = {
        "macro_composite": {"kind": "paired_bootstrap_difference_lcb", "minimum": -0.02},
        "event_recall": {"kind": "wilson_lcb", "minimum": max(0.0, measured["event_recall"] - 0.03)},
        "event_precision": {"kind": "wilson_lcb", "minimum": max(0.0, measured["event_precision"] - 0.03)},
        "attribution": {"kind": "wilson_lcb", "minimum": max(0.0, measured["attribution"] - 0.02)},
        "speaker_role": {"kind": "wilson_lcb", "minimum": max(0.0, measured["speaker_role"] - 0.02)},
        "issue": {"kind": "wilson_lcb", "minimum": max(0.0, measured["issue"] - 0.02)},
        "stance": {"kind": "wilson_lcb", "minimum": max(0.0, measured["stance"] - 0.02)},
        "atomicity": {"kind": "wilson_lcb", "minimum": max(0.0, measured["atomicity"] - 0.02)},
        "contamination": {
            "kind": "wilson_ucb",
            "maximum": min(1.0, measured["contamination"] + 0.005),
        },
        "schema_validity": {"kind": "wilson_lcb", "minimum": measured["schema_validity"]},
        "evidence_grounding": {"kind": "wilson_lcb", "minimum": measured["evidence_grounding"]},
    }
    payload: dict[str, Any] = {
        "version": GATE_MANIFEST_VERSION,
        "frozen": True,
        "calibration": {
            "model": model,
            "reasoning_effort": "medium",
            "passes": 1,
            "split": "development",
            "window_characters": 6000,
            "measured_ceiling": measured,
        },
        "statistics": {
            "alpha": ALPHA,
            "rate_interval": "one_sided_wilson",
            "paired_difference": "show_episode_stratified_bootstrap",
            "bootstrap_iterations": BOOTSTRAP_ITERATIONS,
        },
        "gates": gates,
        "lineage": {
            "scorer_sha256": scorer_sha256,
            "contract_sha256": contract_sha256,
            "split_sha256": split_sha256,
            "run_sha256": run_sha256,
        },
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    payload["manifest_sha256"] = hashlib.sha256(encoded).hexdigest()
    return payload


def evaluate_per_show_validation(
    rows: Sequence[Mapping[str, Any]],
    *,
    recall_threshold: float,
    event_floor: int = PER_SHOW_EVENT_FLOOR,
    alpha: float = ALPHA,
) -> dict[str, Any]:
    """Apply per-show recall gates only to sufficiently powered validation data."""

    by_show: dict[str, dict[str, int]] = defaultdict(lambda: {"gold_events": 0, "matched_events": 0})
    for row in rows:
        if row.get("split") != "validation":
            raise SignalDeskGateError("per-show gates may evaluate validation rows only")
        show_id = str(row.get("show_id") or "").strip()
        if not show_id:
            raise SignalDeskGateError("every validation row requires show_id")
        gold, matched = int(row.get("gold_events", 0)), int(row.get("matched_events", 0))
        if gold < 0 or matched < 0 or matched > gold:
            raise SignalDeskGateError("matched_events must satisfy 0 <= matched_events <= gold_events")
        by_show[show_id]["gold_events"] += gold
        by_show[show_id]["matched_events"] += matched
    results: dict[str, Any] = {}
    for show_id in sorted(by_show):
        counts = by_show[show_id]
        powered = counts["gold_events"] >= event_floor
        gate = (
            evaluate_rate_gate(
                counts["matched_events"],
                counts["gold_events"],
                threshold=recall_threshold,
                direction="minimum",
                alpha=alpha,
            )
            if powered
            else None
        )
        results[show_id] = {**counts, "powered": powered, "gate": gate}
    powered_results = [row for row in results.values() if row["powered"]]
    return {
        "split": "validation",
        "event_floor": event_floor,
        "shows": results,
        "powered_show_count": len(powered_results),
        "all_powered_shows_passed": (
            all(row["gate"]["passed"] for row in powered_results)
            if powered_results
            else None
        ),
    }


def validate_holdout_report(report: Mapping[str, Any]) -> dict[str, Any]:
    """Enforce that sealed holdout reports aggregate metrics only."""

    if report.get("split") != "sealed_holdout":
        raise SignalDeskGateError("holdout report must identify split='sealed_holdout'")
    if report.get("per_show") or report.get("show_results"):
        raise SignalDeskGateError("sealed holdout cannot issue per-show verdicts")
    metrics = report.get("metrics")
    if not isinstance(metrics, Mapping):
        raise SignalDeskGateError("holdout report requires an aggregate metrics object")
    forbidden = sorted(set(metrics) - ALLOWED_HOLDOUT_METRICS)
    if forbidden:
        raise SignalDeskGateError(f"holdout metric(s) not authorized: {', '.join(forbidden)}")
    return {"valid": True, "metrics": dict(metrics), "aggregate_only": True}


def evaluate_gold_audit(
    *,
    audited_windows: int,
    audited_events: int,
    critical_errors: int,
    catastrophic_windows: int,
    total_windows: int = 804,
) -> dict[str, Any]:
    """Evaluate or expand the independent gold audit using explicit denominators."""

    if not 0 <= audited_windows <= total_windows:
        raise SignalDeskGateError("audited_windows must be within the corpus")
    if not 0 <= critical_errors <= audited_events:
        raise SignalDeskGateError("critical_errors must be within audited_events")
    if not 0 <= catastrophic_windows <= audited_windows:
        raise SignalDeskGateError("catastrophic_windows must be within audited_windows")
    enough_windows = audited_windows >= min(GOLD_AUDIT_INITIAL_WINDOWS, total_windows)
    enough_events = audited_events >= GOLD_AUDIT_MIN_EVENTS
    exhausted = audited_windows == total_windows
    if (not enough_windows or not enough_events) and not exhausted:
        minimum_target = max(GOLD_AUDIT_INITIAL_WINDOWS, audited_windows + GOLD_AUDIT_WINDOW_BLOCK)
        next_target = min(total_windows, minimum_target)
        return {
            "status": "needs_more_windows",
            "passed": False,
            "next_window_target": next_target,
            "audited_windows": audited_windows,
            "audited_events": audited_events,
            "required_events": GOLD_AUDIT_MIN_EVENTS,
        }
    critical_gate = evaluate_rate_gate(
        critical_errors,
        audited_events,
        threshold=GOLD_AUDIT_CRITICAL_ERROR_CEILING,
        direction="maximum",
    )
    passed = enough_windows and enough_events and catastrophic_windows == 0 and critical_gate["passed"]
    return {
        "status": "passed" if passed else "failed",
        "passed": passed,
        "audited_windows": audited_windows,
        "audited_events": audited_events,
        "critical_error_gate": critical_gate,
        "catastrophic_windows": catastrophic_windows,
        "window_catastrophe_gate_passed": catastrophic_windows == 0,
        "denominator_exhausted": exhausted,
    }
