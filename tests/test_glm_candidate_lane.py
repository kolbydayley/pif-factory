from __future__ import annotations

from research_factory.glm_candidate_lane import (
    _density_representative,
    _stratified_sample,
    density_summary,
    fair_match_events,
    fair_narrow_event_similarity,
)


def _rows(source_counts: dict[str, list[int]]) -> list[dict[str, object]]:
    rows = []
    sequence = 0
    for source_id, counts in source_counts.items():
        for event_count in counts:
            sequence += 1
            rows.append(
                {
                    "segment_id": f"seg_{sequence:03d}",
                    "episode_id": f"ep_{source_id}_{sequence // 2:03d}",
                    "source_id": source_id,
                    "segment_index": sequence,
                    "event_count": event_count,
                }
            )
    return rows


def test_density_summary_reports_distribution() -> None:
    result = density_summary([0, 5, 10, 15, 20, 25, 30, 35])
    assert result["segment_count"] == 8
    assert result["events_per_segment_mean"] == 17.5
    assert result["p50"] == 17.5
    assert result["zero_event_ratio"] == 0.125
    assert result["histogram"]["31_plus"] == 1


def test_stratified_sample_covers_every_allowed_source() -> None:
    source_counts = {
        f"source_{index}": list(range(index, index + 20))
        for index in range(5)
    }
    rows = _rows(source_counts)
    sample = _stratified_sample(
        rows,
        count=20,
        corpus_values=[int(row["event_count"]) for row in rows],
        seed="test-seed",
    )
    assert len(sample) == 20
    assert len({row["segment_id"] for row in sample}) == 20
    assert {row["source_id"] for row in sample} == set(source_counts)


def test_density_representative_rejects_under_dense_fold() -> None:
    corpus = density_summary([10, 15, 20, 25, 30] * 20)
    sparse = density_summary([0, 1, 2, 3, 4] * 12)
    verdict = _density_representative(sparse, corpus)
    assert verdict["representative"] is False
    assert verdict["checks"]["mean_relative_delta_lte_0_10"] is False


def test_fair_narrow_similarity_renormalizes_comparable_fields() -> None:
    golden = {
        "event_type": "capability_claim",
        "evidence": "the model can solve this task",
        "claim_text": "The model can solve the task.",
        "claim_type": "descriptive",
        "actor": {"name": "Example Labs"},
        "target": {"candidate_concept": "task solving"},
    }
    narrow = {
        "event_type": "capability_claim",
        "evidence": "the model can solve this task",
        "claim_text": "The model can solve the task.",
    }
    assert fair_narrow_event_similarity(golden, narrow) == 1.0


def test_fair_matcher_accepts_event_blocked_by_unreachable_fields() -> None:
    golden = [{
        "event_type": "capability_claim",
        "evidence": "model performance improves on the benchmark",
        "claim_text": "Performance improves on the benchmark.",
        "claim_type": "descriptive",
        "actor": {"name": "Example Labs"},
        "target": {"candidate_concept": "benchmark performance"},
    }]
    narrow = [{
        "event_type": "capability_claim",
        "evidence": "model performance improves on benchmark",
        "claim_text": "Benchmark performance improves.",
    }]
    matches = fair_match_events(golden, narrow)
    assert len(matches) == 1
    assert matches[0][2] >= 0.48
