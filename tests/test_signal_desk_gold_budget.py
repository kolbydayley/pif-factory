from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone

import pytest

from research_factory import signal_desk_gold_budget as gold


def _grant(tmp_path, **overrides):
    body = {"schema_version": gold.SCHEMA_VERSION,
            "granted_at": "2026-09-01T12:46:17Z", "expires_at": "2026-10-01T12:46:17Z",
            "expiry_conditions": ["benchmark_gold_complete", "30_days"],
            "scope": gold.EXPECTED_SCOPE, "model": gold.EXPECTED_MODEL,
            "purpose": gold.EXPECTED_PURPOSE, "authorized_by": "Kolby",
            "campaign_id": "signal-desk-clean-corpus-2026-08-31",
            "benchmark_manifest_sha256": "m" * 64,
            "binding_constraint": "provider_weekly_subscription_window",
            "warning_percent": 85.0, "kill_percent": 120.0,
            "adaptive_concurrency": [2, 8]}
    body.update(overrides); body["grant_sha256"] = gold.grant_hash(body)
    path = tmp_path / "grant.json"; path.write_text(json.dumps(body)); return path


def _conn():
    conn = sqlite3.connect(":memory:"); conn.row_factory = sqlite3.Row; return conn


def _complete():
    return {"manifest_windows": 804, "gold_turns": {"A": 804, "B": 804, "C": 804},
            "sealed_audit_slices": {"development": {"windows": 20, "sealed": True},
                                    "validation": {"windows": 40, "sealed": True},
                                    "sealed_holdout": {"windows": 21, "sealed": True}}}


def test_grant_has_no_daily_cap_and_expires_on_completion(tmp_path):
    grant = gold.load_gold_grant(_grant(tmp_path), at=datetime(2026, 9, 2, tzinfo=timezone.utc))
    assert grant.payload["binding_constraint"] == "provider_weekly_subscription_window"
    assert "daily_cap_tokens" not in grant.payload
    with pytest.raises(gold.GoldBudgetError, match="completion"):
        gold.load_gold_grant(_grant(tmp_path), at=datetime(2026, 9, 2, tzinfo=timezone.utc), completion_receipt=_complete())


def test_weekly_reset_estimate_is_minute_stable():
    assert gold.normalize_weekly_reset(1788748267) == 1788748260
    assert gold.normalize_weekly_reset(1788748268) == 1788748260


def test_wrong_model_or_concurrency_is_rejected(tmp_path):
    with pytest.raises(gold.GoldBudgetError, match="scope or model"):
        gold.load_gold_grant(_grant(tmp_path, model="gpt-5.5"), at=datetime(2026, 9, 2, tzinfo=timezone.utc))
    with pytest.raises(gold.GoldBudgetError, match="concurrency"):
        gold.load_gold_grant(_grant(tmp_path, adaptive_concurrency=[1, 8]), at=datetime(2026, 9, 2, tzinfo=timezone.utc))


def test_85_percent_warns_but_does_not_stop(tmp_path, monkeypatch):
    conn = _conn(); calls = []
    monkeypatch.setattr(gold, "read_weekly_snapshot", lambda _root: {"used_percent": 85.0, "resets_at": 9})
    monkeypatch.setattr(gold.subprocess, "run", lambda *a, **k: calls.append(a) or type("R",(),{"returncode":0})())
    health = gold.weekly_health(conn, session_root=tmp_path, budget_dir=tmp_path)
    assert health["allowed"] is True and health["warning_sent_now"] is True
    assert len(calls) == 1
    assert gold.weekly_health(conn, session_root=tmp_path, budget_dir=tmp_path)["allowed"] is True
    assert len(calls) == 1


def test_missing_snapshot_notifies_and_fails_closed(tmp_path, monkeypatch):
    conn = _conn(); calls = []
    monkeypatch.setattr(gold, "read_weekly_snapshot", lambda _root: None)
    monkeypatch.setattr(gold.subprocess, "run", lambda *a, **k: calls.append(a) or type("R",(),{"returncode":0})())
    health = gold.weekly_health(conn, session_root=tmp_path, budget_dir=tmp_path)
    assert health["allowed"] is False and health["reason"] == "weekly_snapshot_missing"
    assert len(calls) == 1


def test_reservation_and_settlement_are_weekly_lane_metered(tmp_path, monkeypatch):
    conn = _conn()
    monkeypatch.setattr(gold, "read_weekly_snapshot", lambda _root: {"used_percent": 14.0, "resets_at": 1788748267})
    reserved = gold.reserve_gold_call(conn, grant_path=_grant(tmp_path), session_root=tmp_path,
        budget_dir=tmp_path, task_key="dev:w1:A", turn_type="A", reserve_tokens=40_000,
        at=datetime(2026, 9, 2, tzinfo=timezone.utc))
    gold.mark_provider_started(conn, reserved["reservation_id"])
    gold.settle_gold_call(conn, reservation_id=reserved["reservation_id"], actual_tokens=35_000)
    row = conn.execute("SELECT day,lane,tokens FROM pif_subscription_budget_ledger").fetchone()
    assert tuple(row) == ("weekly:1788748260", gold.EXPECTED_SCOPE, 35_000)


def test_live_app_server_snapshot_overrides_stale_session_log(tmp_path, monkeypatch):
    conn = _conn()
    monkeypatch.setattr(
        gold,
        "read_weekly_snapshot",
        lambda _root: {"used_percent": 99.0, "resets_at": 1},
    )
    health = gold.weekly_health(
        conn,
        session_root=tmp_path,
        budget_dir=tmp_path,
        live_snapshot={
            "used_percent": 17.0,
            "resets_at": 1_788_748_267,
            "source": "app_server_live",
        },
    )
    assert health["allowed"] is True
    assert health["used_percent"] == 17.0
    assert health["provider_resets_at"] == 1_788_748_267


def test_started_reservation_cannot_be_released(tmp_path, monkeypatch):
    conn = _conn(); monkeypatch.setattr(gold, "read_weekly_snapshot", lambda _root: {"used_percent": 14.0, "resets_at": 99})
    reserved = gold.reserve_gold_call(conn, grant_path=_grant(tmp_path), session_root=tmp_path,
        budget_dir=tmp_path, task_key="dev:w2:A", turn_type="A", reserve_tokens=40_000,
        at=datetime(2026, 9, 2, tzinfo=timezone.utc))
    gold.mark_provider_started(conn, reserved["reservation_id"])
    with pytest.raises(gold.GoldBudgetError, match="unstarted"):
        gold.release_unstarted_reservation(conn, reserved["reservation_id"])
