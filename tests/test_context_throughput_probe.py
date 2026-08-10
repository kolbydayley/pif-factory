from __future__ import annotations

import contextlib
from unittest.mock import patch

import pytest

from research_factory.context_throughput_probe import (
    CONTEXT_CLAIM_WAVE_SIZE,
    context_job_waves,
    context_wave_timing,
    evaluate_context_campaign_gate,
    run_context_throughput_probe,
)


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"max_contexts": 351}, "between 1 and 350"),
        ({"concurrency": 9}, "exactly 10"),
        ({"max_runtime_seconds": 2399}, "2400 or 3600"),
    ],
)
def test_context_probe_rejects_out_of_scope_bounds(kwargs, message) -> None:
    with pytest.raises(ValueError, match=message):
        run_context_throughput_probe(**kwargs)


def test_context_probe_reports_lock_contention_without_opening_database() -> None:
    with patch(
        "research_factory.context_throughput_probe.pipeline_lock",
        return_value=contextlib.nullcontext(False),
    ), patch(
        "research_factory.context_throughput_probe.db.connect"
    ) as connect:
        result = run_context_throughput_probe()

    assert result == {
        "ok": False,
        "stopped": True,
        "reason": "pipeline_lock_busy",
        "provider_calls": 0,
        "tokens": 0,
    }
    connect.assert_not_called()


def test_context_claim_waves_keep_every_job_inside_lease_window() -> None:
    waves = context_job_waves(tuple(range(350)))

    assert [len(wave) for wave in waves] == [80, 80, 80, 80, 30]
    assert CONTEXT_CLAIM_WAVE_SIZE == 80
    observed = context_wave_timing(
        wave_size=80,
        concurrency=10,
        call_seconds=84.3,
    )
    assert observed["inside_lease"] is True
    assert observed["wave_completion_seconds"] == pytest.approx(674.4)
    assert observed["lease_headroom_seconds"] == pytest.approx(2025.6)


def test_slow_wave_stays_safe_where_old_batch_would_expire() -> None:
    slow_wave = context_wave_timing(
        wave_size=80,
        concurrency=10,
        call_seconds=180,
    )
    old_batch = context_wave_timing(
        wave_size=350,
        concurrency=10,
        call_seconds=180,
    )

    assert slow_wave["inside_lease"] is True
    assert slow_wave["wave_completion_seconds"] == 1440
    assert old_batch["inside_lease"] is False
    assert old_batch["wave_completion_seconds"] == 6300


def _gate_report(*, attempted: int, failed: int, expired_leases: int = 0) -> dict:
    return {
        "selected_jobs": attempted,
        "validation": {
            "attempted": attempted,
            "failed": failed,
            "pass_rate": (attempted - failed) / attempted,
        },
        "execution_diagnostics": {
            "timeouts": 0,
            "expired_leases": expired_leases,
            "provider_pressure_signal_calls": 0,
        },
        "cost_gate": {
            "passed": True,
            "paid_api_billing_detected": False,
        },
        "isolation": {
            "paid_api_telemetry_delta": 0,
            "unexpected_changed_tables": [],
            "protected_table_deltas": {"labels": 0},
        },
    }


def test_revised_series_gate_allows_rejected_sub_one_percent_failure() -> None:
    gate = evaluate_context_campaign_gate(
        _gate_report(attempted=350, failed=1)
    )

    assert gate["passed"] is True
    assert gate["validation_rate"] == pytest.approx(349 / 350)


def test_revised_series_gate_keeps_lease_and_quality_halts() -> None:
    lease_gate = evaluate_context_campaign_gate(
        _gate_report(attempted=350, failed=1, expired_leases=1)
    )
    quality_gate = evaluate_context_campaign_gate(
        _gate_report(attempted=350, failed=4)
    )

    assert lease_gate["passed"] is False
    assert "lease_expiries" in lease_gate["reasons"]
    assert quality_gate["passed"] is False
    assert "validation_below_campaign_gate" in quality_gate["reasons"]
