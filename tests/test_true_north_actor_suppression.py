from __future__ import annotations

from research_factory import true_north_actor_suppression as rule


def _atomic(actor: str, claim: str, evidence: str) -> dict:
    return {
        "reported_actor": actor,
        "claim_text": claim,
        "evidence_text": evidence,
    }


def test_risk_uses_only_runtime_atomic_fields() -> None:
    low = _atomic(
        "Anthropic",
        "Anthropic announced a policy.",
        "Anthropic announced a policy.",
    )
    high = _atomic(
        "Amazon white paper",
        "A document described capabilities.",
        "The Amazon white paper described capabilities.",
    )

    assert rule.actor_emission_risk(low) < rule.actor_emission_risk(high)


def test_suppression_changes_only_reported_actor() -> None:
    source = [
        {
            "candidate_id": "c1",
            "atomic_claims": [
                {
                    **_atomic(
                        "government framework",
                        "A framework exists.",
                        "The government framework exists.",
                    ),
                    "raw_speaker": "Host",
                }
            ],
        }
    ]

    output, report = rule.suppress_predictions(source, threshold=1)

    assert output[0]["atomic_claims"][0]["reported_actor"] is None
    assert output[0]["atomic_claims"][0]["raw_speaker"] == "Host"
    assert source[0]["atomic_claims"][0]["reported_actor"] == (
        "government framework"
    )
    assert report["suppressed"] == 1
