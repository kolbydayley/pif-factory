from __future__ import annotations

import json
import random

from research_factory.direction_metric_audit import (
    _seeded_stratified_sample,
    build_campaign_stratified_manifest,
)


def test_campaign_manifest_preserves_source_strata_and_hashes(tmp_path) -> None:
    paths = []
    for run_id, labels in (("run_a", ["a1", "a2"]), ("run_b", ["b1"])):
        path = tmp_path / f"{run_id}.json"
        path.write_text(
            json.dumps({"run_id": run_id, "label_ids": labels}),
            encoding="utf-8",
        )
        paths.append(path)

    result = build_campaign_stratified_manifest(
        paths,
        destination=tmp_path / "combined.json",
    )

    assert result["label_ids"] == ["a1", "a2", "b1"]
    assert result["strata_by_label_id"] == {
        "a1": "run_a",
        "a2": "run_a",
        "b1": "run_b",
    }
    assert all(item["sha256"] for item in result["source_manifests"])


def test_seeded_sample_allocates_equal_campaign_strata() -> None:
    population = [
        {
            "stratum": stratum,
            "direction": direction,
            "label_id": f"{stratum}-{index}",
        }
        for stratum in ("a", "b", "c")
        for index, direction in enumerate(
            ["increase", "decrease", "stable", "unknown", "increase", "decrease"]
        )
    ]

    sample = _seeded_stratified_sample(
        population,
        max_calls=15,
        rng=random.Random("fixed"),
    )

    assert len(sample) == 15
    assert {stratum: sum(item["stratum"] == stratum for item in sample) for stratum in "abc"} == {
        "a": 5,
        "b": 5,
        "c": 5,
    }
