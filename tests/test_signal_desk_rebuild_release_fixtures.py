from __future__ import annotations

import copy

import pytest

from research_factory.signal_desk_rebuild_release_fixtures import (
    BUILT_SITE_ASSETS,
    FIXTURE_EXECUTION_VERSION,
    RELEASE_REGRESSION_FIXTURES,
    ReleaseFixtureError,
    built_site_sha256,
    evaluate_rendered_fixture_execution,
    fixture_registry_sha256,
    validate_release_fixture_receipt,
)


def _execution():
    return {
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
    }


def test_all_named_data_quality_regressions_are_required_on_rendered_site():
    receipt = evaluate_rendered_fixture_execution(_execution())
    assert receipt["passed"] is True
    assert receipt["fixture_count"] == 10
    assert receipt["execution_target"] == "built_site_rendered_dom"
    assert validate_release_fixture_receipt(receipt)["receipt_sha256"] == receipt["receipt_sha256"]


def test_fixture_gate_rejects_missing_safety_assertion_or_tampered_receipt():
    execution = _execution()
    execution["results"][0]["checks"].pop("rendered_route_verified")
    receipt = evaluate_rendered_fixture_execution(execution)
    assert receipt["passed"] is False
    with pytest.raises(ReleaseFixtureError, match="did not all pass"):
        validate_release_fixture_receipt(receipt)

    good = evaluate_rendered_fixture_execution(_execution())
    tampered = copy.deepcopy(good)
    tampered["site_build_sha256"] = "f" * 64
    with pytest.raises(ReleaseFixtureError, match="hash does not match"):
        validate_release_fixture_receipt(tampered)


def test_built_site_hash_binds_all_companion_payloads(tmp_path):
    for name in BUILT_SITE_ASSETS:
        (tmp_path / name).write_text(f"asset:{name}", encoding="utf-8")
    first = built_site_sha256(tmp_path)
    (tmp_path / "signal-desk-issues.json").write_text("changed", encoding="utf-8")
    assert built_site_sha256(tmp_path) != first
    (tmp_path / "pif-signal-desk.html").unlink()
    with pytest.raises(ReleaseFixtureError, match="missing"):
        built_site_sha256(tmp_path)
