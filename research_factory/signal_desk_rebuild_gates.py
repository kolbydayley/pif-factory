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


GATE_MANIFEST_VERSION = "signal-desk-rebuild-gates-v2"
ALPHA = 0.05
BOOTSTRAP_ITERATIONS = 10_000
PER_SHOW_EVENT_FLOOR = 50
FLATTENED_SUPPORTED_ATTRIBUTION_MINIMUM = 0.99
INDETERMINABLE_ATTRIBUTION_STRATA = frozenset({"flattened", "asr_diarized"})
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


def bootstrap_window_metric_bounds(
    rows: Sequence[Mapping[str, Any]],
    *,
    metrics: Sequence[str],
    iterations: int = BOOTSTRAP_ITERATIONS,
    alpha: float = ALPHA,
    seed: int = 20260902,
) -> dict[str, dict[str, Any]]:
    """Measure one-pass frontier uncertainty by resampling frozen windows.

    A1 is a single model pass, so its gate calibration cannot treat the
    observed point estimate as an exact ceiling.  These bounds deliberately
    resample the exact frozen development windows and are retained alongside
    the point estimate.  Positive-quality gates source their floor from the
    lower bound; error-rate gates source their ceiling from the upper bound.
    """

    if not rows:
        raise SignalDeskGateError("frontier bootstrap requires at least one window")
    if iterations < 100:
        raise SignalDeskGateError("at least 100 bootstrap iterations are required")
    if not metrics or len(set(metrics)) != len(metrics):
        raise SignalDeskGateError("frontier bootstrap metrics must be non-empty and unique")
    values: dict[str, list[float]] = {metric: [] for metric in metrics}
    for row in rows:
        for metric in metrics:
            value = row.get(metric)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise SignalDeskGateError(f"frontier window is missing numeric metric: {metric}")
            if not 0.0 <= float(value) <= 1.0:
                raise SignalDeskGateError(f"frontier window metric is outside [0,1]: {metric}")
            values[metric].append(float(value))
    lower_index = max(0, min(iterations - 1, math.floor(alpha * iterations)))
    upper_index = max(0, min(iterations - 1, math.ceil((1.0 - alpha) * iterations) - 1))
    result: dict[str, dict[str, Any]] = {}
    for offset, metric in enumerate(metrics):
        metric_values = values[metric]
        rng = random.Random(seed + offset)
        sampled = [
            sum(metric_values[rng.randrange(len(metric_values))] for _ in metric_values)
            / len(metric_values)
            for _ in range(iterations)
        ]
        sampled.sort()
        result[metric] = {
            "resampling_unit": "window",
            "window_count": len(metric_values),
            "point": round(sum(metric_values) / len(metric_values), 6),
            "lcb": round(sampled[lower_index], 6),
            "ucb": round(sampled[upper_index], 6),
            "alpha": alpha,
            "iterations": iterations,
        }
    return result


def show_macro_composite_lcb(
    per_show: Mapping[str, Mapping[str, Any]],
    *,
    iterations: int = BOOTSTRAP_ITERATIONS,
    alpha: float = ALPHA,
    seed: int = 20260901,
) -> dict[str, Any]:
    """Bootstrap the unweighted show-macro composite, never pooled events.

    A show is the resampling unit because raw event volume varies sharply by
    feed and transcript structure.  Sampling windows or events here would let
    a single prolific show choose a prompt for the corpus.
    """

    if not per_show:
        raise SignalDeskGateError("show-macro bootstrap requires at least one show")
    if iterations < 100:
        raise SignalDeskGateError("at least 100 bootstrap iterations are required")
    values: list[float] = []
    for show_id, row in sorted(per_show.items()):
        metrics = row.get("metrics") if isinstance(row, Mapping) else None
        value = metrics.get("macro_composite") if isinstance(metrics, Mapping) else None
        if not show_id or isinstance(value, bool) or not isinstance(value, (int, float)):
            raise SignalDeskGateError("each show requires a numeric macro_composite")
        if not 0.0 <= float(value) <= 1.0:
            raise SignalDeskGateError("show macro composite must be in [0, 1]")
        values.append(float(value))
    rng = random.Random(seed)
    sampled = [sum(values[rng.randrange(len(values))] for _ in values) / len(values) for _ in range(iterations)]
    sampled.sort()
    lower_index = max(0, min(iterations - 1, math.floor(alpha * iterations)))
    return {
        "resampling_unit": "show",
        "show_count": len(values),
        "point": round(sum(values) / len(values), 6),
        "lcb": round(sampled[lower_index], 6),
        "alpha": alpha,
        "iterations": iterations,
    }


def evaluate_powered_show_promotion(
    candidate_per_show: Mapping[str, Mapping[str, Any]],
    parent_per_show: Mapping[str, Mapping[str, Any]],
    *,
    event_floor: int = PER_SHOW_EVENT_FLOOR,
    improvement_threshold: float = 0.80,
    alpha: float = ALPHA,
) -> dict[str, Any]:
    """Require a candidate improvement on at least 80% of powered shows.

    The gate itself is a one-sided Wilson lower-confidence-bound test.  Shows
    below the frozen consequential-event floor remain visible in receipts but
    cannot decide a prompt promotion.
    """

    if set(candidate_per_show) != set(parent_per_show):
        raise SignalDeskGateError("candidate and parent must cover the same shows")
    if not 0.0 < improvement_threshold <= 1.0:
        raise SignalDeskGateError("improvement threshold must be in (0, 1]")
    rows: dict[str, Any] = {}
    improved = 0
    powered = 0
    for show_id in sorted(candidate_per_show):
        candidate = candidate_per_show[show_id]
        parent = parent_per_show[show_id]
        candidate_counts = candidate.get("counts") if isinstance(candidate, Mapping) else None
        parent_counts = parent.get("counts") if isinstance(parent, Mapping) else None
        candidate_metrics = candidate.get("metrics") if isinstance(candidate, Mapping) else None
        parent_metrics = parent.get("metrics") if isinstance(parent, Mapping) else None
        if not all(isinstance(value, Mapping) for value in (candidate_counts, parent_counts, candidate_metrics, parent_metrics)):
            raise SignalDeskGateError("per-show promotion rows require counts and metrics")
        candidate_events = int(candidate_counts.get("gold_events", -1))
        parent_events = int(parent_counts.get("gold_events", -1))
        if candidate_events < 0 or parent_events < 0 or candidate_events != parent_events:
            raise SignalDeskGateError("candidate and parent gold event counts must match")
        candidate_score = candidate_metrics.get("macro_composite")
        parent_score = parent_metrics.get("macro_composite")
        if any(isinstance(value, bool) or not isinstance(value, (int, float)) for value in (candidate_score, parent_score)):
            raise SignalDeskGateError("per-show promotion rows require numeric macro composites")
        is_powered = candidate_events >= event_floor
        is_improved = float(candidate_score) > float(parent_score)
        if is_powered:
            powered += 1
            improved += int(is_improved)
        rows[show_id] = {
            "gold_events": candidate_events,
            "powered": is_powered,
            "parent_macro_composite": round(float(parent_score), 6),
            "candidate_macro_composite": round(float(candidate_score), 6),
            "improved": is_improved,
        }
    gate = (
        evaluate_rate_gate(
            improved, powered, threshold=improvement_threshold, direction="minimum", alpha=alpha
        )
        if powered
        else None
    )
    return {
        "event_floor": event_floor,
        "improvement_threshold": improvement_threshold,
        "powered_show_count": powered,
        "improved_powered_show_count": improved,
        "gate": gate,
        "passed": bool(gate and gate["passed"]),
        "shows": rows,
    }


def freeze_frontier_ceiling(
    metrics: Mapping[str, float],
    *,
    window_metric_bounds: Mapping[str, Mapping[str, Any]],
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
    missing_bounds = sorted(required - set(window_metric_bounds))
    if missing_bounds:
        raise SignalDeskGateError(
            f"frontier bootstrap bounds are missing metrics: {', '.join(missing_bounds)}"
        )
    calibrated: dict[str, dict[str, float]] = {}
    for key in sorted(required):
        bound = window_metric_bounds[key]
        if not isinstance(bound, Mapping):
            raise SignalDeskGateError("frontier bootstrap bound must be a mapping")
        point, lcb, ucb = bound.get("point"), bound.get("lcb"), bound.get("ucb")
        if any(isinstance(value, bool) or not isinstance(value, (int, float)) for value in (point, lcb, ucb)):
            raise SignalDeskGateError("frontier bootstrap bound must provide numeric point/lcb/ucb")
        if not 0.0 <= float(lcb) <= float(point) <= float(ucb) <= 1.0:
            raise SignalDeskGateError("frontier bootstrap bounds must satisfy lcb <= point <= ucb")
        calibrated[key] = {"point": float(point), "lcb": float(lcb), "ucb": float(ucb)}
    # The bootstrap mean and the show-macro point can differ slightly because
    # they answer different questions.  They must nevertheless be directionally
    # consistent; the raw point remains in the immutable calibration receipt.
    gate_source = {
        key: (calibrated[key]["ucb"] if key == "contamination" else calibrated[key]["lcb"])
        for key in required
    }
    gates = {
        "macro_composite": {
            "kind": "paired_bootstrap_difference_lcb",
            "minimum": -0.02,
            "reference_ceiling_lcb": gate_source["macro_composite"],
        },
        "event_recall": {"kind": "wilson_lcb", "minimum": max(0.0, gate_source["event_recall"] - 0.03)},
        "event_precision": {"kind": "wilson_lcb", "minimum": max(0.0, gate_source["event_precision"] - 0.03)},
        "attribution": {"kind": "wilson_lcb", "minimum": max(0.0, gate_source["attribution"] - 0.02)},
        "speaker_role": {"kind": "wilson_lcb", "minimum": max(0.0, gate_source["speaker_role"] - 0.02)},
        "issue": {"kind": "wilson_lcb", "minimum": max(0.0, gate_source["issue"] - 0.02)},
        "stance": {"kind": "wilson_lcb", "minimum": max(0.0, gate_source["stance"] - 0.02)},
        "atomicity": {"kind": "wilson_lcb", "minimum": max(0.0, gate_source["atomicity"] - 0.02)},
        "contamination": {
            "kind": "wilson_ucb",
            "maximum": min(1.0, gate_source["contamination"] + 0.005),
        },
        "schema_validity": {"kind": "wilson_lcb", "minimum": gate_source["schema_validity"]},
        "evidence_grounding": {"kind": "wilson_lcb", "minimum": gate_source["evidence_grounding"]},
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
            "measured_ceiling_point": measured,
            "window_bootstrap_bounds": calibrated,
            "gate_source": "window_bootstrap_lcb_except_contamination_ucb",
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


def evaluate_attribution_strata(
    rows: Sequence[Mapping[str, Any]],
    *,
    speaker_turn_threshold: float,
    flattened_supported_threshold: float = FLATTENED_SUPPORTED_ATTRIBUTION_MINIMUM,
    alpha: float = ALPHA,
) -> dict[str, Any]:
    """Apply full attribution accuracy only where speaker structure exists.

    Flattened transcripts instead gate the absence of fabricated attribution.
    Each row supplies ``transcript_structure``, ``attribution_total`` and either
    ``attribution_correct`` or ``attribution_supported`` as appropriate.
    """

    totals: dict[str, dict[str, int]] = defaultdict(lambda: {"successes": 0, "total": 0})
    for row in rows:
        structure = str(row.get("transcript_structure") or "")
        if structure not in {"speaker_turn", "paragraph", *INDETERMINABLE_ATTRIBUTION_STRATA}:
            raise SignalDeskGateError("unknown transcript structure")
        total = int(row.get("attribution_total", 0))
        success_key = (
            "attribution_supported"
            if structure in INDETERMINABLE_ATTRIBUTION_STRATA
            else "attribution_correct"
        )
        successes = int(row.get(success_key, 0))
        if total < 0 or successes < 0 or successes > total:
            raise SignalDeskGateError("attribution counts must satisfy 0 <= successes <= total")
        totals[structure]["successes"] += successes
        totals[structure]["total"] += total

    results: dict[str, Any] = {}
    for structure in ("speaker_turn", "paragraph", "flattened", "asr_diarized"):
        values = totals[structure]
        threshold = (
            flattened_supported_threshold
            if structure in INDETERMINABLE_ATTRIBUTION_STRATA
            else speaker_turn_threshold
        )
        results[structure] = {
            **values,
            "metric": (
                "supported_attribution_rate"
                if structure in INDETERMINABLE_ATTRIBUTION_STRATA
                else "attribution_accuracy"
            ),
            "gate": evaluate_rate_gate(
                values["successes"],
                values["total"],
                threshold=threshold,
                direction="minimum",
                alpha=alpha,
            ),
        }
    return {
        "alpha": alpha,
        "indeterminable_attribution_policy": "no_fabricated_attribution",
        "strata": results,
        "passed": all(row["gate"]["passed"] for row in results.values() if row["total"]),
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
