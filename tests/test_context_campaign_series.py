from __future__ import annotations

from unittest.mock import patch

from research_factory.context_campaign_series import run_context_campaign_series


class _Scalar:
    def __init__(self, value: int):
        self.value = value

    def fetchone(self):
        return (self.value,)


class _Connection:
    def execute(self, sql, params=()):
        if "job_type = 'episode_context'" in sql:
            return _Scalar(1)
        if "EXISTS (" in sql and "episode_context_runs" in sql:
            return _Scalar(0)
        if "job_type = 'label_segment'" in sql:
            return _Scalar(10)
        if "FROM episodes" in sql:
            return _Scalar(1)
        raise AssertionError(sql)

    def close(self):
        pass


def test_series_retries_lock_three_times_then_records_skip(tmp_path) -> None:
    calls = 0

    def busy(**kwargs):
        nonlocal calls
        calls += 1
        return {
            "ok": False,
            "reason": "pipeline_lock_busy",
            "provider_calls": 0,
            "tokens": 0,
        }

    with patch(
        "research_factory.context_campaign_series.db.connect",
        return_value=_Connection(),
    ), patch(
        "research_factory.context_campaign_series.root",
        return_value=tmp_path,
    ), patch(
        "research_factory.context_campaign_series.time.sleep",
    ):
        result = run_context_campaign_series(
            campaign_limit=3,
            lock_backoff_seconds=(0, 0, 0),
            campaign_runner=busy,
        )

    assert calls == 12
    assert result["campaigns_run"] == 0
    assert result["campaigns_skipped"] == 3
    assert result["campaign_slots_used"] == 3
    assert result["stop_reason"] == "three_consecutive_lock_contention_skips"


def test_series_stops_after_first_failed_campaign_gate(tmp_path) -> None:
    result_row = {
        "ok": False,
        "completed_contexts": 349,
        "provider_calls": 350,
        "tokens": 100,
        "token_usage_by_call": [],
    }
    with patch(
        "research_factory.context_campaign_series.db.connect",
        return_value=_Connection(),
    ), patch(
        "research_factory.context_campaign_series.root",
        return_value=tmp_path,
    ):
        result = run_context_campaign_series(
            campaign_limit=8,
            campaign_runner=lambda **kwargs: result_row,
        )

    assert result["campaigns_run"] == 1
    assert result["completed_contexts"] == 349
    assert result["campaign_slots_used"] == 1
    assert result["stop_reason"] == "campaign_quality_gate_failure"


def test_series_accepts_authorized_ten_campaign_bound(tmp_path) -> None:
    with patch(
        "research_factory.context_campaign_series.db.connect",
        return_value=_Connection(),
    ), patch(
        "research_factory.context_campaign_series.root",
        return_value=tmp_path,
    ):
        result = run_context_campaign_series(
            campaign_limit=10,
            campaign_runner=lambda **kwargs: {
                "ok": False,
                "completed_contexts": 0,
                "provider_calls": 0,
                "tokens": 0,
                "token_usage_by_call": [],
            },
        )

    assert result["bounds"]["campaign_limit"] == 10
