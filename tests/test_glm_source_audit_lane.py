from __future__ import annotations

from research_factory.glm_source_audit_lane import _balanced_batches, _correlations


def test_balanced_batches_preserve_cases_and_bounds() -> None:
    cases = [
        {"case_id": f"seg_{index}", "candidates": [{}] * count}
        for index, count in enumerate((0, 8, 12, 15, 6, 10, 3))
    ]
    batches = _balanced_batches(cases, max_cases=3, max_candidates=25)
    flattened = [case["case_id"] for batch in batches for case in batch]
    assert flattened == [f"seg_{index}" for index in range(1, 7)]
    assert all(len(batch) <= 3 for batch in batches)
    assert all(sum(len(case["candidates"]) for case in batch) <= 25 for batch in batches)


def test_correlations_capture_monotonic_relationship() -> None:
    result = _correlations([1, 2, 3, 4], [10, 20, 30, 40])
    assert result == {"pearson": 1.0, "spearman": 1.0}
