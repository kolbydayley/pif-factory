from __future__ import annotations

import json
import signal
import sqlite3
import threading
from pathlib import Path

import research_factory.production_backfill_series as series
from research_factory.production_backfill_series import evaluate_label_campaign_gate


def _passing_report() -> dict:
    return {
        "ok": True,
        "selected_label_jobs": 80,
        "prepared_label_calls": 80,
        "provider_calls": 80,
        "segments_completed": 80,
        "audit_lane": {
            "audited_labels": 80,
            "pass_rate": 1.0,
        },
        "new_labels": {"evidence_span_validity_rate": 1.0},
        "deterministic_repair": {
            "blanked_metric_count": 0,
            "field_bearing_metric_quarantine_count": 0,
            "direction_only_metric_quarantine_count": 0,
        },
        "execution_diagnostics": {
            "expired_leases": 0,
            "timeouts": 0,
            "provider_pressure_signal_calls": 0,
        },
        "terminal_failures": {"count": 0, "rate": 0.0},
        "isolation": {
            "paid_api_telemetry_delta": 0,
            "unexpected_changed_tables": [],
            "protected_table_deltas": {
                "corpus_releases": 0,
                "pif_daily_runs": 0,
                "pif_scale_gate_state_receipts": 0,
                "pipeline_runs": 0,
            },
        },
        "cost_gate": {
            "passed": True,
            "paid_api_billing_detected": False,
        },
    }


def test_label_campaign_gate_accepts_clean_batch() -> None:
    result = evaluate_label_campaign_gate(_passing_report())
    assert result["passed"] is True
    assert result["reasons"] == []


def test_label_campaign_gate_rejects_quality_or_operational_loss() -> None:
    report = _passing_report()
    report["audit_lane"]["pass_rate"] = 0.98
    report["new_labels"]["evidence_span_validity_rate"] = 0.999
    report["deterministic_repair"]["field_bearing_metric_quarantine_count"] = 8
    report["execution_diagnostics"]["expired_leases"] = 1
    result = evaluate_label_campaign_gate(report)
    assert result["passed"] is False
    assert "audit_pass_rate_below_campaign_gate" in result["reasons"]
    assert "evidence_span_validity_not_100_percent" in result["reasons"]
    assert "field_bearing_quarantine_emergency_tripwire" in result["reasons"]
    assert "lease_expiries" in result["reasons"]


def test_label_campaign_gate_rejects_incomplete_accounting() -> None:
    report = _passing_report()
    report["prepared_label_calls"] = 79
    result = evaluate_label_campaign_gate(report)
    assert result["passed"] is False
    assert "campaign_accounting_incomplete" in result["reasons"]


def test_label_campaign_gate_treats_safe_rejections_as_diagnostics() -> None:
    report = _passing_report()
    report["ok"] = False
    report["segments_completed"] = 78
    report["audit_lane"]["audited_labels"] = 78
    report["deterministic_repair"]["field_bearing_metric_quarantine_count"] = 1
    report["deterministic_repair"]["direction_only_metric_quarantine_count"] = 4
    report["execution_diagnostics"]["timeouts"] = 1

    result = evaluate_label_campaign_gate(report)

    assert result["passed"] is False
    assert result["structural_validation_rate"] == 78 / 80
    assert "provider_timeouts" in result["reasons"]


def test_label_campaign_gate_applies_rolling_field_quarantine_gate() -> None:
    result = evaluate_label_campaign_gate(
        _passing_report(),
        rolling_labels=400,
        rolling_field_bearing_quarantines=29,
    )

    assert result["passed"] is False
    assert "field_bearing_quarantine_rolling_gate" in result["reasons"]


def test_label_campaign_gate_uses_tightened_twenty_per_four_hundred_boundary() -> None:
    below = evaluate_label_campaign_gate(
        _passing_report(),
        rolling_labels=400,
        rolling_field_bearing_quarantines=19,
    )
    at = evaluate_label_campaign_gate(
        _passing_report(),
        rolling_labels=400,
        rolling_field_bearing_quarantines=20,
    )

    assert below["passed"] is True
    assert at["passed"] is False
    assert "field_bearing_quarantine_rolling_gate" in at["reasons"]


def test_label_campaign_gate_halts_below_structural_floor() -> None:
    report = _passing_report()
    report["segments_completed"] = 71
    report["audit_lane"]["audited_labels"] = 71

    result = evaluate_label_campaign_gate(report)

    assert result["passed"] is False
    assert "structural_validation_below_90_percent_floor" in result["reasons"]


def test_series_rejects_limit_above_authorized_twelve(monkeypatch) -> None:
    # The current review-loop authorization is explicitly capped at 12.
    monkeypatch.setattr(series, "now_iso", lambda: "2026-08-02T12:00:00+00:00")
    monkeypatch.setattr(series, "root", lambda: Path("/nonexistent"))
    try:
        series.run_label_campaign_series(campaign_limit=13)
    except ValueError as exc:
        assert "one through twelve" in str(exc)
    else:
        raise AssertionError("campaign limit 13 must remain rejected")


def test_direction_retention_band_is_evaluated_and_recorded_at_boundary(
    tmp_path, monkeypatch
) -> None:
    result = _passing_report()
    result.update(
        {
            "run_id": "pib_low",
            "manifest_path": str(tmp_path / "manifest.json"),
            "segments_completed": 80,
        }
    )
    (tmp_path / "manifest.json").write_text('{"label_ids": []}')
    monkeypatch.setattr(series, "_direction_metric_population", lambda _path: [{}, {}])
    boundary = series._direction_retention_boundary(result)
    assert boundary["retained_per_80_labels"] == 2.0
    assert boundary["out_of_band"] is True
    assert boundary["action"] == "re_audit_requested_continue_series"


def test_deferred_termination_signal_finishes_current_boundary() -> None:
    stop_event = threading.Event()
    prior = signal.getsignal(signal.SIGTERM)

    with series._deferred_stop_signals(stop_event):
        signal.raise_signal(signal.SIGTERM)
        assert stop_event.is_set()

    assert signal.getsignal(signal.SIGTERM) == prior


def test_stop_request_cannot_race_into_another_campaign(tmp_path, monkeypatch) -> None:
    control_root = tmp_path / "controls"
    project_root = tmp_path / "project"
    intent = (
        project_root
        / "work/pif-ops/controller-daily-intent/2026-08-02/intent.json"
    )
    intent.parent.mkdir(parents=True)
    intent.write_text(
        json.dumps({"status": "completed_genuinely_successful"}),
        encoding="utf-8",
    )
    campaign_started = threading.Event()
    release_campaign = threading.Event()
    calls = []

    class FakeConnection:
        def close(self) -> None:
            pass

    def fake_campaign(**_kwargs):
        calls.append(len(calls) + 1)
        campaign_started.set()
        assert release_campaign.wait(timeout=5)
        return _passing_report()

    monkeypatch.setattr(series, "root", lambda: project_root)
    monkeypatch.setattr(series.db, "connect", lambda: FakeConnection())
    monkeypatch.setattr(
        series,
        "_pending_label_counts",
        lambda _conn: {"pending": 100, "dispatchable": 100},
    )
    monkeypatch.setattr(series, "run_instrumented_backfill", fake_campaign)
    result_box = {}
    runner = threading.Thread(
        target=lambda: result_box.setdefault(
            "result",
            series.run_label_campaign_series(
                campaign_limit=2,
                control_root=control_root,
            ),
        )
    )
    runner.start()
    assert campaign_started.wait(timeout=5)

    stop_box = {}
    stopper = threading.Thread(
        target=lambda: stop_box.setdefault(
            "result",
            series.request_active_label_campaign_stop(
                reason="test_stop",
                control_root=control_root,
            ),
        )
    )
    stopper.start()
    stop_files = list(control_root.glob("*.stop.json"))
    for _ in range(100):
        stop_files = list(control_root.glob("*.stop.json"))
        if stop_files:
            break
        threading.Event().wait(0.01)
    assert len(stop_files) == 1
    release_campaign.set()
    runner.join(timeout=5)
    stopper.join(timeout=5)

    assert not runner.is_alive()
    assert not stopper.is_alive()
    assert calls == [1]
    assert stop_box["result"]["requested"] is True
    assert result_box["result"]["stop_reason"] == "external_stop_requested"


def test_prelaunch_attempt_restore_requires_absent_output_and_empty_log(
    tmp_path,
) -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE jobs (
          id INTEGER PRIMARY KEY, lane TEXT, job_type TEXT, status TEXT,
          attempts INTEGER, max_attempts INTEGER, error TEXT, updated_at TEXT
        );
        CREATE TABLE label_runs (
          id TEXT PRIMARY KEY, job_id INTEGER, output_path TEXT,
          status TEXT, error TEXT
        );
        """
    )
    output = tmp_path / "provider-output.json"
    rows = [
        (1, "lr_never_started", str(tmp_path / "missing.json")),
        (2, "lr_started", str(output)),
    ]
    for job_id, run_id, output_path in rows:
        conn.execute(
            "INSERT INTO jobs VALUES (?, 'podcast', 'label_segment', 'pending', 1, 2, ?, '')",
            (job_id, series.INTERRUPTED_SERIES_ERROR),
        )
        conn.execute(
            "INSERT INTO label_runs VALUES (?, ?, ?, 'failed', ?)",
            (run_id, job_id, output_path, series.INTERRUPTED_SERIES_ERROR),
        )
    logs = tmp_path / "runs/headless_logs"
    logs.mkdir(parents=True)
    (logs / "lr_never_started.log").write_bytes(b"")
    (logs / "lr_started.log").write_text('{"type":"turn.started"}\n')
    conn.commit()

    report = series.restore_verified_prelaunch_attempts(
        conn,
        artifact_root=tmp_path,
        receipt_root=tmp_path / "receipts",
    )

    attempts = dict(conn.execute("SELECT id, attempts FROM jobs"))
    assert attempts == {1: 0, 2: 1}
    assert report["selected"] == 2
    assert report["recovered_to_pending"] == 1
    assert report["provider_started_unchanged"] == 1
    assert report["usage_reconciliation"]["started_calls_without_terminal_usage"] == 1
    assert report["usage_reconciliation"]["accounting_complete"] is False
