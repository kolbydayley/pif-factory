from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone

import pytest

from research_factory import signal_desk_rebuild_budget as budget


CAMPAIGN = "signal-desk-clean-corpus-2026-08-31"


def _grant(tmp_path, **overrides):
    body = {
        "schema_version": budget.SCHEMA_VERSION,
        "granted_at": "2026-09-01T00:05:24Z",
        "expires_at": "2026-10-16T00:05:24Z",
        "expiry_condition": "first_clean_release",
        "daily_cap_tokens": 20_000_000,
        "scope": budget.EXPECTED_SCOPE,
        "authorized_by": "Kolby",
        "campaign_id": CAMPAIGN,
    }
    body.update(overrides)
    body["grant_sha256"] = budget.grant_hash(body)
    path = tmp_path / "grant.json"
    path.write_text(json.dumps(body), encoding="utf-8")
    return path


def test_durable_grant_is_active_across_days_and_expires_at_clean_release(tmp_path):
    path = _grant(tmp_path)
    first = budget.load_grant(
        path,
        campaign_id=CAMPAIGN,
        at=datetime(2026, 9, 2, tzinfo=timezone.utc),
    )
    later = budget.load_grant(
        path,
        campaign_id=CAMPAIGN,
        at=datetime(2026, 9, 20, tzinfo=timezone.utc),
    )
    assert first.daily_cap_tokens == later.daily_cap_tokens == 20_000_000
    with pytest.raises(budget.RebuildBudgetError, match="first clean release"):
        budget.load_grant(
            path,
            campaign_id=CAMPAIGN,
            at=datetime(2026, 9, 20, tzinfo=timezone.utc),
            clean_release_exists=True,
        )


def test_grant_is_hash_scope_campaign_and_expiry_bound(tmp_path):
    path = _grant(tmp_path)
    payload = json.loads(path.read_text())
    payload["daily_cap_tokens"] = 19_000_000
    path.write_text(json.dumps(payload))
    with pytest.raises(budget.RebuildBudgetError, match="hash mismatch"):
        budget.load_grant(
            path,
            campaign_id=CAMPAIGN,
            at=datetime(2026, 9, 2, tzinfo=timezone.utc),
        )


def test_campaign_kill_persists_until_explicit_clearance(tmp_path):
    grant = _grant(tmp_path)
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    kill = budget.campaign_kill_path(tmp_path / "budget", CAMPAIGN)
    kill.parent.mkdir()
    kill.write_text("{}")
    result = budget.rebuild_budget_gate(
        conn,
        day="2026-09-02",
        campaign_id=CAMPAIGN,
        grant_path=grant,
        budget_dir=tmp_path / "budget",
        at=datetime(2026, 9, 2, tzinfo=timezone.utc),
    )
    assert result["allowed"] is False
    assert result["reason"] == "rebuild_campaign_kill_present"
    assert kill.exists()


def test_burn_probe_requires_five_consecutive_useful_days():
    rows = [
        {"day": f"2026-09-0{day}", "tokens": 18_500_000, "useful_work": True}
        for day in range(1, 6)
    ]
    assert budget.qualify_burn_probe(rows)["passed"] is True
    rows[2]["provider_quota_failure"] = True
    assert budget.qualify_burn_probe(rows)["passed"] is False


def test_burn_probe_rejects_gaps():
    rows = [
        {"day": day, "tokens": 19_000_000, "useful_work": True}
        for day in (
            "2026-09-01",
            "2026-09-02",
            "2026-09-04",
            "2026-09-05",
            "2026-09-06",
        )
    ]
    assert budget.qualify_burn_probe(rows)["passed"] is False


def test_strict_five_day_probe_requires_one_real_provider_window_and_contiguous_run(tmp_path):
    rows = [
        {
            "day": f"2026-09-0{day}",
            "tokens": 19_000_000,
            "useful_work": True,
            "provider_window_id": "weekly-reset-1",
        }
        for day in range(1, 6)
    ]
    receipt = budget.five_day_budget_burn_probe(rows)
    assert receipt["passed"] is True
    assert receipt["provider_window_verified"] is True
    assert receipt["longest_contiguous_qualifying_streak_days"] == 5
    written = budget.write_five_day_budget_burn_probe(
        tmp_path / "burn-probe.json", receipts=rows
    )
    assert written["receipt_sha256"]
    with pytest.raises(budget.RebuildBudgetError, match="immutable"):
        budget.write_five_day_budget_burn_probe(
            tmp_path / "burn-probe.json", receipts=rows
        )

    rows[2]["provider_quota_failure"] = True
    assert budget.five_day_budget_burn_probe(rows)["passed"] is False
