from __future__ import annotations

from research_factory import true_north_best_stack as best


def test_distribution_reports_quantiles_and_fixed_bins() -> None:
    result = best.distribution([0.0, 0.25, 0.5, 0.75, 0.9, 1.0])

    assert result["count"] == 6
    assert result["median"] == 0.625
    assert result["bins"] == {
        "0.00_to_lt_0.25": 1,
        "0.25_to_lt_0.50": 1,
        "0.50_to_lt_0.75": 1,
        "0.75_to_lt_0.90": 1,
        "0.90_to_1.00": 2,
    }


def test_distribution_handles_empty_input() -> None:
    result = best.distribution([])

    assert result["count"] == 0
    assert result["mean"] is None
    assert result["median"] is None
