from __future__ import annotations

import json
from datetime import datetime, timezone

from research_factory.signal_desk_rebuild import (
    ROUND1_ARTIFACTS,
    campaign_preflight,
    initialize_campaign_database,
    round1_readiness,
)
from research_factory.signal_desk_rebuild_budget import grant_hash


def _campaign(tmp_path):
    source = json.loads(open("config/signal_desk_rebuild_campaign.json").read())
    path = tmp_path / "campaign.json"
    path.write_text(json.dumps(source))
    return path


def _grant(tmp_path):
    source = json.loads(open("config/signal_desk_rebuild_budget_grant.json").read())
    source["granted_at"] = "2026-09-01T00:00:00Z"
    source["expires_at"] = "2026-10-16T00:00:00Z"
    source["grant_sha256"] = grant_hash(source)
    path = tmp_path / "grant.json"
    path.write_text(json.dumps(source))
    return path


def test_preflight_binds_live_lanes_prompt_campaign_and_grant(tmp_path):
    result = campaign_preflight(
        campaign_path=_campaign(tmp_path),
        grant_path=_grant(tmp_path),
        prompt_path=__import__("pathlib").Path("config/signal_desk_rebuild_system_prompt_v1.txt"),
        at=datetime(2026, 9, 2, tzinfo=timezone.utc),
    )
    assert result["passed"] is True
    assert result["glm_concurrency"]["total_glm_ceiling"] == 23
    assert len(result["prompt_sha256"]) == 64
    assert len(result["contract_sha256"]) == 64
    assert len(result["scorer_sha256"]) == 64
    assert len(result["show_alias_registry_sha256"]) == 64


def test_database_initializes_dispatch_and_tournament_tables(tmp_path):
    result = initialize_campaign_database(tmp_path / "shadow.sqlite")
    assert result["initialized"] is True
    assert "signal_desk_rebuild_tasks" in result["tables"]
    assert "signal_desk_rebuild_experiments" in result["tables"]
    assert "signal_desk_rebuild_approval_runs" in result["tables"]


def test_round1_status_is_fail_closed_until_every_receipt_passes(tmp_path):
    status = round1_readiness(tmp_path)
    assert status["passed"] is False
    assert set(status["checks"]) == set(ROUND1_ARTIFACTS)
    assert all(check["reason"] == "missing" for check in status["checks"].values())


def test_round1_status_accepts_only_gate_shaped_receipts(tmp_path):
    artifacts = {
        "split-manifest.json": {"frozen": True, "manifest_sha256": "a"},
        "gold-adjudication.json": {"status": "adjudicated", "gold_sha256": "b"},
        "gold-audit.json": {"status": "passed", "passed": True},
        "scorer-qualified.json": {"status": "qualified", "agreement_lcb": 0.971},
        "frontier-ceiling.json": {"model": "gpt-5.6-sol", "passes": 1},
        "frozen-gates.json": {"frozen": True, "manifest_sha256": "c"},
        "dispatch-qualified.json": {"passed": True, "tests_passed": 15},
    }
    for filename, payload in artifacts.items():
        (tmp_path / filename).write_text(json.dumps(payload))
    assert round1_readiness(tmp_path)["passed"] is True
    artifacts["scorer-qualified.json"]["agreement_lcb"] = 0.96
    (tmp_path / "scorer-qualified.json").write_text(
        json.dumps(artifacts["scorer-qualified.json"])
    )
    assert round1_readiness(tmp_path)["passed"] is False
