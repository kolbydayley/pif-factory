"""Hash-bound end-to-end regression fixtures for a clean Signal Desk release.

The source excerpts for these cases remain private benchmark material. This
module records only fixture identities, required safety assertions, hashed
source bindings, and browser-produced rendered-DOM receipts. A release cannot
replace these checks with a broad "tests passed" boolean.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from .util import dumps_json, now_iso, sha256_text


FIXTURE_REGISTRY_VERSION = "pif_signal_desk_release_fixture_registry_v1"
FIXTURE_EXECUTION_VERSION = "pif_signal_desk_release_fixture_execution_v1"
FIXTURE_RECEIPT_VERSION = "pif_signal_desk_release_fixture_receipt_v1"

# These are deliberately named by failure mode, not the raw quote. The linked
# private fixture input binds a source-window hash at execution time.
RELEASE_REGRESSION_FIXTURES: dict[str, dict[str, Any]] = {
    "eric-ho-clipped-answer": {
        "policy": "clipped_answer_requires_wider_context_before_publication",
        "required_checks": (
            "source_window_hash_matches",
            "wider_context_attempted",
            "unsupported_direct_claim_not_public",
            "rendered_route_verified",
        ),
    },
    "ajeya-cotra-reversed-meaning": {
        "policy": "reversed_meaning_is_quarantined_or_corrected_with_evidence",
        "required_checks": (
            "source_window_hash_matches",
            "semantic_polarity_preserved",
            "unsupported_direct_claim_not_public",
            "rendered_route_verified",
        ),
    },
    "tracy-alloway-question": {
        "policy": "questions_are_not_published_as_consequential_claims",
        "required_checks": (
            "source_window_hash_matches",
            "question_not_accepted_as_claim",
            "rendered_route_verified",
        ),
    },
    "micha-kaufman-attribution": {
        "policy": "direct_claim_requires_verified_speaker_identity",
        "required_checks": (
            "source_window_hash_matches",
            "speaker_identity_verified_or_quarantined",
            "rendered_route_verified",
        ),
    },
    "dario-amodei-third-party-mention": {
        "policy": "third_party_mention_never_appears_under_what_they_say",
        "required_checks": (
            "source_window_hash_matches",
            "mentioned_person_not_direct_speaker",
            "rendered_route_verified",
        ),
    },
    "verge-webpage-chrome": {
        "policy": "webpage_chrome_is_quarantined",
        "required_checks": (
            "source_window_hash_matches",
            "chrome_not_accepted",
            "rendered_route_verified",
        ),
    },
    "spacex-episode-listing": {
        "policy": "episode_listing_is_not_discourse_evidence",
        "required_checks": (
            "source_window_hash_matches",
            "listing_not_accepted",
            "rendered_route_verified",
        ),
    },
    "nuclear-analogy": {
        "policy": "analogy_does_not_become_an_unsupported_strategic_claim",
        "required_checks": (
            "source_window_hash_matches",
            "analogy_claim_scope_preserved_or_quarantined",
            "rendered_route_verified",
        ),
    },
    "python-versus-c": {
        "policy": "comparison_claim_keeps_its_subject_and_conditions",
        "required_checks": (
            "source_window_hash_matches",
            "comparison_subject_preserved",
            "rendered_route_verified",
        ),
    },
    "open-code-versus-open-weights": {
        "policy": "nearby_terms_remain_distinct_canonical_issues",
        "required_checks": (
            "source_window_hash_matches",
            "canonical_issues_remain_distinct",
            "rendered_route_verified",
        ),
    },
}

BUILT_SITE_ASSETS = (
    "pif-signal-desk.html",
    "signal-desk.js",
    "signal-desk.css",
    "signal-desk-index.json",
    "signal-desk-issues.json",
    "signal-desk-voices.json",
    "pif-signal-desk-funnel.json",
)


class ReleaseFixtureError(RuntimeError):
    """A release fixture ledger or browser result is incomplete or forged."""


def fixture_registry() -> dict[str, Any]:
    """Return the public-safe fixture registry; it contains no transcript text."""

    return {
        "schema_version": FIXTURE_REGISTRY_VERSION,
        "fixtures": [
            {
                "fixture_id": fixture_id,
                "policy": spec["policy"],
                "required_checks": list(spec["required_checks"]),
            }
            for fixture_id, spec in sorted(RELEASE_REGRESSION_FIXTURES.items())
        ],
    }


def fixture_registry_sha256() -> str:
    return sha256_text(dumps_json(fixture_registry()))


def built_site_sha256(site_root: Path) -> str:
    """Hash the exact built static surface that the browser fixture saw."""

    records: list[dict[str, str]] = []
    for name in BUILT_SITE_ASSETS:
        path = site_root / name
        if not path.is_file():
            raise ReleaseFixtureError(f"built Signal Desk asset is missing: {name}")
        records.append({
            "name": name,
            "sha256": sha256_text(path.read_text(encoding="utf-8")),
        })
    return sha256_text(dumps_json(records))


def _expect_sha256(value: object, *, field: str) -> str:
    candidate = str(value or "")
    if len(candidate) != 64 or any(char not in "0123456789abcdef" for char in candidate.lower()):
        raise ReleaseFixtureError(f"{field} must be a SHA-256 hex digest")
    return candidate.lower()


def evaluate_rendered_fixture_execution(execution: Mapping[str, Any]) -> dict[str, Any]:
    """Validate one real-browser run against every named regression fixture.

    The browser harness is allowed to keep its raw DOM/source screenshots in
    private build storage. This durable receipt only preserves their hashes,
    routes, and pass/fail assertions. It therefore proves the checks executed
    against the built public DOM without publishing raw transcript excerpts.
    """

    if not isinstance(execution, Mapping):
        raise ReleaseFixtureError("fixture execution must be a mapping")
    if execution.get("schema_version") != FIXTURE_EXECUTION_VERSION:
        raise ReleaseFixtureError("unsupported fixture execution schema")
    if execution.get("fixture_registry_sha256") != fixture_registry_sha256():
        raise ReleaseFixtureError("fixture execution is bound to the wrong registry")
    if execution.get("execution_target") != "built_site_rendered_dom":
        raise ReleaseFixtureError("fixture execution must target the built rendered DOM")
    _expect_sha256(execution.get("site_build_sha256"), field="site_build_sha256")
    _expect_sha256(execution.get("rendered_dom_bundle_sha256"), field="rendered_dom_bundle_sha256")
    results = execution.get("results")
    if not isinstance(results, Sequence) or isinstance(results, (str, bytes)):
        raise ReleaseFixtureError("fixture execution requires results")
    by_id: dict[str, Mapping[str, Any]] = {}
    for result in results:
        if not isinstance(result, Mapping):
            raise ReleaseFixtureError("fixture result must be a mapping")
        fixture_id = str(result.get("fixture_id") or "")
        if fixture_id in by_id:
            raise ReleaseFixtureError("fixture execution repeats a fixture id")
        by_id[fixture_id] = result
    expected_ids = set(RELEASE_REGRESSION_FIXTURES)
    if set(by_id) != expected_ids:
        missing = sorted(expected_ids - set(by_id))
        unexpected = sorted(set(by_id) - expected_ids)
        raise ReleaseFixtureError(
            f"fixture execution coverage mismatch: missing={missing}, unexpected={unexpected}"
        )

    failures: list[str] = []
    normalized_results: list[dict[str, Any]] = []
    for fixture_id in sorted(expected_ids):
        result = by_id[fixture_id]
        route = str(result.get("route") or "")
        if not route.startswith("#"):
            raise ReleaseFixtureError(f"fixture {fixture_id} lacks a rendered site route")
        _expect_sha256(result.get("source_window_sha256"), field=f"{fixture_id}.source_window_sha256")
        _expect_sha256(result.get("candidate_sha256"), field=f"{fixture_id}.candidate_sha256")
        _expect_sha256(result.get("rendered_dom_sha256"), field=f"{fixture_id}.rendered_dom_sha256")
        checks = result.get("checks")
        if not isinstance(checks, Mapping):
            raise ReleaseFixtureError(f"fixture {fixture_id} has no check map")
        missing_checks = [
            key for key in RELEASE_REGRESSION_FIXTURES[fixture_id]["required_checks"]
            if checks.get(key) is not True
        ]
        passed = result.get("passed") is True and not missing_checks
        if not passed:
            failures.append(fixture_id)
        normalized_results.append(
            {
                "fixture_id": fixture_id,
                "route": route,
                "source_window_sha256": str(result["source_window_sha256"]),
                "candidate_sha256": str(result["candidate_sha256"]),
                "rendered_dom_sha256": str(result["rendered_dom_sha256"]),
                "passed": passed,
                "missing_checks": missing_checks,
            }
        )
    body: dict[str, Any] = {
        "schema_version": FIXTURE_RECEIPT_VERSION,
        "created_at": now_iso(),
        "fixture_registry_sha256": fixture_registry_sha256(),
        "execution_target": "built_site_rendered_dom",
        "site_build_sha256": str(execution["site_build_sha256"]),
        "rendered_dom_bundle_sha256": str(execution["rendered_dom_bundle_sha256"]),
        "fixture_count": len(normalized_results),
        "passed": not failures,
        "failed_fixture_ids": failures,
        "results": normalized_results,
        "raw_dom_or_transcript_exposed": False,
    }
    body["receipt_sha256"] = sha256_text(dumps_json(body))
    return body


def validate_release_fixture_receipt(receipt: Mapping[str, Any]) -> dict[str, Any]:
    """Validate a persisted fixture receipt before a clean release can use it."""

    if not isinstance(receipt, Mapping):
        raise ReleaseFixtureError("fixture receipt must be a mapping")
    if receipt.get("schema_version") != FIXTURE_RECEIPT_VERSION:
        raise ReleaseFixtureError("unsupported fixture receipt schema")
    if receipt.get("fixture_registry_sha256") != fixture_registry_sha256():
        raise ReleaseFixtureError("fixture receipt registry does not match current required cases")
    if receipt.get("execution_target") != "built_site_rendered_dom":
        raise ReleaseFixtureError("fixture receipt was not executed against rendered built site")
    _expect_sha256(receipt.get("site_build_sha256"), field="site_build_sha256")
    _expect_sha256(receipt.get("rendered_dom_bundle_sha256"), field="rendered_dom_bundle_sha256")
    _expect_sha256(receipt.get("receipt_sha256"), field="receipt_sha256")
    unsigned = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    if receipt["receipt_sha256"] != sha256_text(dumps_json(unsigned)):
        raise ReleaseFixtureError("fixture receipt hash does not match payload")
    if receipt.get("fixture_count") != len(RELEASE_REGRESSION_FIXTURES):
        raise ReleaseFixtureError("fixture receipt has the wrong fixture count")
    results = receipt.get("results")
    if not isinstance(results, Sequence) or isinstance(results, (str, bytes)):
        raise ReleaseFixtureError("fixture receipt results are missing")
    ids = {str(row.get("fixture_id") or "") for row in results if isinstance(row, Mapping)}
    if ids != set(RELEASE_REGRESSION_FIXTURES):
        raise ReleaseFixtureError("fixture receipt does not cover every required regression")
    if receipt.get("passed") is not True or receipt.get("failed_fixture_ids"):
        raise ReleaseFixtureError("release regression fixtures did not all pass")
    if any(not isinstance(row, Mapping) or row.get("passed") is not True for row in results):
        raise ReleaseFixtureError("fixture receipt contains a failed case")
    return dict(receipt)
