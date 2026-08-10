from __future__ import annotations

import json

from research_factory.label_backfill_daemon import LabelBackfillDaemon


def _campaign(*, passed: bool = True, reasons: list[str] | None = None) -> dict:
    gate = {"passed": passed, "reasons": reasons or []}
    report = {
        "run_id": "pib_test",
        "completed_at": "2026-08-01T00:00:00+00:00",
        "selected_label_jobs": 80,
        "prepared_label_calls": 80,
        "provider_calls": 80,
        "segments_completed": 80,
        "audit_lane": {"audited_labels": 80, "pass_rate": 1.0},
        "new_labels": {"evidence_span_validity_rate": 1.0},
        "deterministic_repair": {"field_bearing_metric_quarantine_count": 0},
        "execution_diagnostics": {"expired_leases": 0, "timeouts": 0, "timeouts_safely_requeued": 0, "provider_pressure_signal_calls": 0},
        "terminal_failures": {"rate": 0.0},
        "isolation": {"paid_api_telemetry_delta": 0, "unexpected_changed_tables": [], "protected_table_deltas": {}},
        "cost_gate": {"passed": True, "paid_api_billing_detected": False},
        "tokens": 100,
        "report_path": "/tmp/report.json",
        "label_series_gate": gate,
    }
    return {"series_id": "plcs_test", "campaigns": [report], "skips": [], "stop_reason": "campaign_limit_reached"}


def test_daemon_chains_campaigns_and_persists_status(tmp_path) -> None:
    calls = []
    daemon = LabelBackfillDaemon(
        status_path=tmp_path / "status.json",
        runner=lambda **kwargs: calls.append(kwargs) or _campaign(),
        claim_probe=lambda: 0,
        pending_probe=lambda: 10,
        window_seed=lambda: [],
        flag_seed=lambda: [],
        expired_recover=lambda started_at: {"released_jobs": 0, "failed_label_runs": 0},
    )
    state = daemon.serve(max_iterations=2, pause_seconds=0)
    assert len(calls) == 2
    assert state["campaigns_completed"] == 2
    assert state["status"] == "idle"


def test_gate_trip_stops_and_records_durable_reason(tmp_path) -> None:
    report = _campaign()
    report["campaigns"][0]["audit_lane"]["pass_rate"] = 0.98
    daemon = LabelBackfillDaemon(
        status_path=tmp_path / "status.json",
        runner=lambda **kwargs: report,
        claim_probe=lambda: 0,
        pending_probe=lambda: 10,
        window_seed=lambda: [],
        flag_seed=lambda: [],
        expired_recover=lambda started_at: {"released_jobs": 0, "failed_label_runs": 0},
    )
    state = daemon.run_iteration()
    assert state["halted"] is True
    assert "audit_pass_rate_below_campaign_gate" in state["halt_reason"]["reasons"]
    assert json.loads((tmp_path / "status.json").read_text())["halted"] is True


def test_daily_cycle_contention_yields_without_halting(tmp_path) -> None:
    daemon = LabelBackfillDaemon(
        status_path=tmp_path / "status.json",
        runner=lambda **kwargs: {"campaigns": [], "skips": [{"reason": "pipeline_lock_busy_after_three_retries"}], "stop_reason": "campaign_limit_reached"},
        claim_probe=lambda: 0,
        pending_probe=lambda: 10,
        window_seed=lambda: [],
        flag_seed=lambda: [],
        expired_recover=lambda started_at: {"released_jobs": 0, "failed_label_runs": 0},
    )
    state = daemon.run_iteration()
    assert state["status"] == "yielded"
    assert state["halted"] is False


def test_restart_mid_campaign_waits_and_does_not_double_claim(tmp_path) -> None:
    status = tmp_path / "status.json"
    status.write_text(json.dumps({
        "schema_version": "pif_label_backfill_daemon_state_v1",
        "generation": 1,
        "status": "running",
        "halted": False,
        "halt_reason": None,
        "current_campaign": {"started_at": "2026-08-01T00:00:00+00:00"},
        "campaigns_completed": 0,
        "campaign_history": [],
        "rolling_windows": [],
        "updated_at": "2026-08-01T00:00:00+00:00",
        "configuration": {},
    }))
    calls = []
    daemon = LabelBackfillDaemon(
        status_path=status,
        runner=lambda **kwargs: calls.append(kwargs) or _campaign(),
        claim_probe=lambda: 7,
        pending_probe=lambda: 10,
        window_seed=lambda: [],
        flag_seed=lambda: [],
        expired_recover=lambda started_at: {"released_jobs": 0, "failed_label_runs": 0},
    )
    state = daemon.run_iteration()
    assert state["status"] == "waiting_for_claim_reconciliation"
    assert state["unexpired_claims"] == 7
    assert calls == []
    resumed = LabelBackfillDaemon(
        status_path=status,
        runner=lambda **kwargs: calls.append(kwargs) or _campaign(),
        claim_probe=lambda: 0,
        pending_probe=lambda: 10,
        window_seed=lambda: [],
        flag_seed=lambda: [],
        expired_recover=lambda started_at: {"released_jobs": 7, "failed_label_runs": 7},
    ).run_iteration()
    assert len(calls) == 1
    assert resumed["campaigns_completed"] == 1
    assert resumed["campaign_history"][0]["segments_completed"] == 80
    assert resumed["last_restart_recovery"]["released_jobs"] == 7


def test_queue_empty_stops_without_claiming(tmp_path) -> None:
    calls = []
    daemon = LabelBackfillDaemon(
        status_path=tmp_path / "status.json",
        runner=lambda **kwargs: calls.append(kwargs) or _campaign(),
        claim_probe=lambda: 0,
        pending_probe=lambda: 0,
        window_seed=lambda: [],
        flag_seed=lambda: [],
        expired_recover=lambda started_at: {"released_jobs": 0, "failed_label_runs": 0},
    )
    state = daemon.run_iteration()
    assert state["status"] == "queue_empty"
    assert calls == []
