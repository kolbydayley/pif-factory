from __future__ import annotations

import json

import pytest

from research_factory.signal_desk_rebuild_release import (
    CleanReleaseError,
    evaluate_release,
    write_clean_release_receipt,
)
from research_factory.signal_desk_rebuild_release_fixtures import (
    FIXTURE_EXECUTION_VERSION,
    RELEASE_REGRESSION_FIXTURES,
    evaluate_rendered_fixture_execution,
    fixture_registry_sha256,
)


def _fixture_receipt():
    return evaluate_rendered_fixture_execution({
        "schema_version": FIXTURE_EXECUTION_VERSION,
        "fixture_registry_sha256": fixture_registry_sha256(),
        "execution_target": "built_site_rendered_dom",
        "site_build_sha256": "a" * 64,
        "rendered_dom_bundle_sha256": "b" * 64,
        "results": [
            {
                "fixture_id": fixture_id,
                "route": f"#fixture/{fixture_id}",
                "source_window_sha256": "c" * 64,
                "candidate_sha256": "d" * 64,
                "rendered_dom_sha256": "e" * 64,
                "passed": True,
                "checks": {check: True for check in spec["required_checks"]},
            }
            for fixture_id, spec in RELEASE_REGRESSION_FIXTURES.items()
        ],
    })


def _evidence():
    return {
        "gold_audit_passed": True,
        "scorer_qualified": True,
        "frontier_gates_frozen": True,
        "sealed_aggregate_gates_passed": True,
        "budget_burn_probe_passed": True,
        "release_regression_fixtures_passed": True,
        "regression_fixture_receipt": _fixture_receipt(),
        "zero_person_attribution_violations": True,
        "zero_unsupported_public_claims": True,
        "zero_duplicate_canonical_people": True,
        "zero_duplicate_resolved_feeds": True,
        "coverage_totals_reconciled": True,
        "route_crawl_zero_broken_links": True,
        "mobile_390_passed": True,
        "mobile_430_passed": True,
        "public_payload_smoke_passed": True,
        "rendered_dom_smoke_passed": True,
        "railway_smoke_passed": True,
        "shadow_processed_windows": 1000,
        "gpt55_processed_windows": 1000,
        "shadow_approval_rate": 0.80,
    }


def test_release_requires_complete_end_to_end_evidence():
    assert evaluate_release(_evidence())["passed"] is True
    bad = _evidence()
    bad["gpt55_processed_windows"] = 999
    assert evaluate_release(bad)["passed"] is False
    bad = _evidence()
    bad["shadow_approval_rate"] = 0.99
    assert evaluate_release(bad)["disposition"] == "intelligence_remains_withdrawn"
    bad = _evidence()
    bad.pop("regression_fixture_receipt")
    assert evaluate_release(bad)["checks"]["release_regression_fixture_receipt"] is False


def test_clean_release_receipt_is_hash_bound_immutable_and_expires_grant(tmp_path):
    path = tmp_path / "first-clean-release.json"
    receipt = write_clean_release_receipt(
        path,
        campaign_id="campaign",
        deployment_url="https://signal.example.test/",
        evidence=_evidence(),
    )
    assert receipt["expires_budget_grant"] is True
    assert json.loads(path.read_text())["receipt_sha256"] == receipt["receipt_sha256"]
    with pytest.raises(CleanReleaseError, match="immutable"):
        write_clean_release_receipt(
            path,
            campaign_id="campaign",
            deployment_url="https://signal.example.test/",
            evidence=_evidence(),
        )


def test_failed_gate_cannot_write_release(tmp_path):
    evidence = _evidence()
    evidence["zero_person_attribution_violations"] = False
    with pytest.raises(CleanReleaseError, match="remains withdrawn"):
        write_clean_release_receipt(
            tmp_path / "receipt.json",
            campaign_id="campaign",
            deployment_url="https://signal.example.test/",
            evidence=evidence,
        )
