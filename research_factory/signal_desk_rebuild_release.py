"""Final fail-closed release receipt for clean Signal Desk intelligence."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from .signal_desk_rebuild_release_fixtures import (
    ReleaseFixtureError,
    validate_release_fixture_receipt,
)
from .util import dumps_json, now_iso, sha256_text


SCHEMA_VERSION = "pif_signal_desk_clean_release_v2"


class CleanReleaseError(RuntimeError):
    pass


def evaluate_release(evidence: Mapping[str, Any]) -> dict[str, Any]:
    required_boolean_checks = (
        "gold_audit_passed",
        "scorer_qualified",
        "frontier_gates_frozen",
        "sealed_aggregate_gates_passed",
        "budget_burn_probe_passed",
        "release_regression_fixtures_passed",
        "zero_person_attribution_violations",
        "zero_unsupported_public_claims",
        "zero_duplicate_canonical_people",
        "zero_duplicate_resolved_feeds",
        "coverage_totals_reconciled",
        "route_crawl_zero_broken_links",
        "mobile_390_passed",
        "mobile_430_passed",
        "public_payload_smoke_passed",
        "rendered_dom_smoke_passed",
        "railway_smoke_passed",
    )
    missing = [name for name in required_boolean_checks if name not in evidence]
    if missing:
        raise CleanReleaseError(f"release evidence missing: {', '.join(missing)}")
    checks = {name: evidence.get(name) is True for name in required_boolean_checks}
    try:
        validate_release_fixture_receipt(evidence.get("regression_fixture_receipt"))
    except ReleaseFixtureError:
        checks["release_regression_fixture_receipt"] = False
    else:
        checks["release_regression_fixture_receipt"] = True
    processed = int(evidence.get("shadow_processed_windows") or 0)
    approval_rate = float(evidence.get("shadow_approval_rate") or 0.0)
    checks["shadow_processed_windows"] = processed >= 1_000
    checks["shadow_approval_band"] = 0.60 <= approval_rate <= 0.95
    checks["gpt55_processed_every_shadow_window"] = int(
        evidence.get("gpt55_processed_windows") or 0
    ) == processed
    passed = all(checks.values())
    return {
        "schema_version": SCHEMA_VERSION,
        "passed": passed,
        "disposition": "first_clean_release" if passed else "intelligence_remains_withdrawn",
        "checks": checks,
        "shadow_processed_windows": processed,
        "shadow_approval_rate": approval_rate,
    }


def write_clean_release_receipt(
    path: Path,
    *,
    campaign_id: str,
    deployment_url: str,
    evidence: Mapping[str, Any],
) -> dict[str, Any]:
    evaluation = evaluate_release(evidence)
    if not evaluation["passed"]:
        raise CleanReleaseError("release gates failed; intelligence remains withdrawn")
    if not deployment_url.startswith("https://"):
        raise CleanReleaseError("clean release requires a verified HTTPS deployment URL")
    body = {
        **evaluation,
        "campaign_id": campaign_id,
        "deployment_url": deployment_url,
        "released_at": now_iso(),
        "evidence": dict(evidence),
        "expires_budget_grant": True,
    }
    body["receipt_sha256"] = sha256_text(dumps_json(body))
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise CleanReleaseError("clean release receipt is immutable")
    path.write_text(dumps_json(body) + "\n", encoding="utf-8")
    return body
