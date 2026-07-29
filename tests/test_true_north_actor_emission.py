from __future__ import annotations

import pytest

from research_factory.true_north_actor_emission import (
    ActorEmissionAuditError,
    compute_actor_emission_feasibility,
)


def _gold(actor: str | None) -> dict:
    return {
        "items": [
            {
                "candidate_id": "candidate",
                "atomic_claims": [{"reported_actor": actor}],
            }
        ]
    }


def test_perfect_emission_existing_value_upper_bound() -> None:
    result = compute_actor_emission_feasibility(
        pass_a={"candidate:00": "Lab"},
        pass_b={"candidate:00": "Lab"},
        pass_c={},
        gold=_gold("Lab"),
        candidates={
            "candidate": {
                "reported_actor": {"name": "Lab"},
                "actor_name": "Fallback",
            }
        },
    )

    assert result["post_repair_interannotator"]["ceiling"] == 1.0
    assert result["post_repair_interannotator"][
        "ceiling_referenced_gate"
    ] == 0.95
    assert result["upper_bounds"][
        "perfect_emission_existing_prior_values"
    ] == 1.0
    assert result["task_6"]["authorized"] is True


def test_null_gold_counts_as_correct_under_perfect_emission() -> None:
    result = compute_actor_emission_feasibility(
        pass_a={"candidate:00": None},
        pass_b={"candidate:00": None},
        pass_c={},
        gold=_gold(None),
        candidates={
            "candidate": {
                "reported_actor": {"name": "Spurious"},
                "actor_name": "Spurious",
            }
        },
    )

    assert result["gold_prevalence"]["reported_actor_null_rate"] == 1.0
    assert result["upper_bounds"][
        "perfect_emission_existing_prior_values"
    ] == 1.0


def test_pass_c_must_cover_disagreement() -> None:
    with pytest.raises(
        ActorEmissionAuditError, match="does not cover"
    ):
        compute_actor_emission_feasibility(
            pass_a={"candidate:00": "Lab"},
            pass_b={"candidate:00": None},
            pass_c={},
            gold=_gold("Lab"),
            candidates={"candidate": {"reported_actor": "Lab"}},
        )


def test_promoted_gold_must_match_adjudicated_value() -> None:
    with pytest.raises(
        ActorEmissionAuditError, match="does not match"
    ):
        compute_actor_emission_feasibility(
            pass_a={"candidate:00": "Lab"},
            pass_b={"candidate:00": None},
            pass_c={"candidate:00": "Other"},
            gold=_gold("Lab"),
            candidates={"candidate": {"reported_actor": "Lab"}},
        )
