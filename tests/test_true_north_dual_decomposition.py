from __future__ import annotations

import pytest

from research_factory import true_north
from research_factory.true_north_dual_decomposition import (
    DualDecompositionError,
    compose_agreement_output,
    count_disagreements,
)


def _output(candidate_id: str, count: int, prefix: str) -> dict:
    return {
        "schema_version": true_north.MULTIPASS_SCHEMA_VERSION,
        "items": [
            {
                "candidate_id": candidate_id,
                "split": count > 1,
                "atomic_claims": [
                    {"claim_text": f"{prefix} {index}"}
                    for index in range(count)
                ],
                "edit_reason": (
                    "compound_split" if count > 1 else "qualifier_repair"
                ),
            }
        ],
    }


def test_same_count_uses_frozen_first_pass() -> None:
    first = _output("candidate", 1, "first")
    second = _output("candidate", 1, "second")

    result = compose_agreement_output(first, second, None)

    assert count_disagreements(first, second) == []
    assert result["items"][0]["atomic_claims"] == [
        {"claim_text": "first 0"}
    ]


def test_count_disagreement_uses_spark() -> None:
    first = _output("candidate", 1, "first")
    second = _output("candidate", 2, "second")
    spark = _output("candidate", 2, "spark")

    result = compose_agreement_output(first, second, spark)

    assert count_disagreements(first, second) == ["candidate"]
    assert result["items"][0]["atomic_claims"] == [
        {"claim_text": "spark 0"},
        {"claim_text": "spark 1"},
    ]


def test_disagreement_without_exact_spark_scope_fails() -> None:
    with pytest.raises(
        DualDecompositionError, match="exactly cover"
    ):
        compose_agreement_output(
            _output("candidate", 1, "first"),
            _output("candidate", 2, "second"),
            None,
        )


def test_pass_scope_mismatch_fails() -> None:
    with pytest.raises(
        DualDecompositionError, match="different candidate scope"
    ):
        count_disagreements(
            _output("one", 1, "first"),
            _output("two", 1, "second"),
        )
