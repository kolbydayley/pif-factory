from __future__ import annotations

import pytest

from research_factory.signal_desk_rebuild_gates import (
    SignalDeskGateError,
    evaluate_gold_audit,
    evaluate_per_show_validation,
    evaluate_rate_gate,
    freeze_frontier_ceiling,
    paired_stratified_bootstrap,
    validate_holdout_report,
    wilson_bounds,
)


def _ceiling() -> dict[str, float]:
    return {
        "macro_composite": 0.90,
        "event_recall": 0.88,
        "event_precision": 0.92,
        "attribution": 0.96,
        "speaker_role": 0.95,
        "issue": 0.93,
        "stance": 0.91,
        "atomicity": 0.89,
        "contamination": 0.002,
        "schema_validity": 1.0,
        "evidence_grounding": 0.99,
    }


def test_wilson_gates_use_conservative_one_sided_bound() -> None:
    lower, upper = wilson_bounds(97, 100)
    assert lower < 0.97 < upper
    assert evaluate_rate_gate(97, 100, threshold=0.97)["passed"] is False
    assert evaluate_rate_gate(0, 1000, threshold=0.01, direction="maximum")["passed"] is True


def test_paired_stratified_bootstrap_is_seeded_and_uses_difference_lcb() -> None:
    candidate = [0.90, 0.92, 0.88, 0.91]
    ceiling = [0.91, 0.93, 0.89, 0.92]
    strata = [("show-a", "ep-1"), ("show-a", "ep-1"), ("show-b", "ep-2"), ("show-b", "ep-2")]
    first = paired_stratified_bootstrap(candidate, ceiling, strata, iterations=500)
    second = paired_stratified_bootstrap(candidate, ceiling, strata, iterations=500)
    assert first == second
    assert first["point_difference"] == pytest.approx(-0.01)
    assert first["difference_lcb"] == pytest.approx(-0.01)


def test_frontier_freeze_converts_relative_tolerances_to_absolute_gates() -> None:
    manifest = freeze_frontier_ceiling(
        _ceiling(),
        scorer_sha256="a" * 64,
        contract_sha256="b" * 64,
        split_sha256="c" * 64,
        run_sha256="d" * 64,
    )
    assert manifest["frozen"] is True
    assert manifest["calibration"]["passes"] == 1
    assert manifest["calibration"]["window_characters"] == 6000
    assert manifest["gates"]["event_recall"]["minimum"] == pytest.approx(0.85)
    assert manifest["gates"]["attribution"]["minimum"] == pytest.approx(0.94)
    assert manifest["gates"]["contamination"]["maximum"] == pytest.approx(0.007)
    assert manifest["gates"]["schema_validity"]["minimum"] == 1.0
    assert len(manifest["manifest_sha256"]) == 64


def test_per_show_validation_gates_only_powered_shows() -> None:
    report = evaluate_per_show_validation(
        [
            {"split": "validation", "show_id": "powered", "gold_events": 30, "matched_events": 30},
            {"split": "validation", "show_id": "powered", "gold_events": 30, "matched_events": 30},
            {"split": "validation", "show_id": "small", "gold_events": 49, "matched_events": 49},
        ],
        recall_threshold=0.90,
    )
    assert report["shows"]["powered"]["powered"] is True
    assert report["shows"]["powered"]["gate"]["passed"] is True
    assert report["shows"]["small"]["powered"] is False
    assert report["shows"]["small"]["gate"] is None

    with pytest.raises(SignalDeskGateError, match="validation rows only"):
        evaluate_per_show_validation(
            [{"split": "sealed_holdout", "show_id": "x", "gold_events": 50, "matched_events": 50}],
            recall_threshold=0.9,
        )


def test_holdout_is_aggregate_only_and_metric_limited() -> None:
    valid = validate_holdout_report(
        {
            "split": "sealed_holdout",
            "metrics": {"macro_composite": 0.9, "contamination": 0.001, "ood_recall": 0.8},
        }
    )
    assert valid["aggregate_only"] is True
    with pytest.raises(SignalDeskGateError, match="per-show"):
        validate_holdout_report(
            {"split": "sealed_holdout", "metrics": {"macro_composite": 0.9}, "per_show": {"x": {}}}
        )
    with pytest.raises(SignalDeskGateError, match="not authorized"):
        validate_holdout_report(
            {"split": "sealed_holdout", "metrics": {"per_show_recall": 0.9}}
        )


def test_gold_audit_expands_for_event_denominator_then_uses_error_ucb() -> None:
    expand = evaluate_gold_audit(
        audited_windows=120,
        audited_events=800,
        critical_errors=0,
        catastrophic_windows=0,
    )
    assert expand["status"] == "needs_more_windows"
    assert expand["next_window_target"] == 160

    passed = evaluate_gold_audit(
        audited_windows=160,
        audited_events=1000,
        critical_errors=0,
        catastrophic_windows=0,
    )
    assert passed["status"] == "passed"
    assert passed["critical_error_gate"]["total"] == 1000

    catastrophic = evaluate_gold_audit(
        audited_windows=160,
        audited_events=1000,
        critical_errors=0,
        catastrophic_windows=1,
    )
    assert catastrophic["status"] == "failed"
