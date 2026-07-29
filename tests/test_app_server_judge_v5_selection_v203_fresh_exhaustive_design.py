from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from research_factory.app_server_judge_v5_selection_v203_fresh_exhaustive_design import (
    EXCLUSION_MANIFESTS,
    _cost_bound,
    _validate_v202_nonacceptance,
    freeze_v203,
    select_fresh_development_episodes,
)


def _walk(value):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk(child)


def test_v203_binds_v202_quality_failure_without_reclassifying_it():
    predecessor = _validate_v202_nonacceptance()
    assert predecessor["terminal"]["development_quality_passed"] is False
    assert predecessor["terminal"]["production_amortized_token_target_passed"] is True
    assert predecessor["report"]["infrastructure_failure"] is False


def test_v203_selects_four_fresh_sources_using_only_llm_label_counts():
    conn = sqlite3.connect("file:data/factory.sqlite?mode=ro", uri=True)
    try:
        selected, audit = select_fresh_development_episodes(conn)
    finally:
        conn.close()
    assert len(selected) == 4
    assert len({row["source_id"] for row in selected}) == 4
    assert len({row["episode_id"] for row in selected}) == 4
    assert all(row["no_signal_label_count"] >= 1 for row in selected)
    assert all(row["dense_label_count"] >= 3 for row in selected)
    assert all(row["observed_label_event_max"] <= 32 for row in selected)
    assert audit["semantic_regex_or_keyword_rules_used"] is False
    assert audit["embeddings_or_semantic_similarity_used"] is False


def test_v203_cost_bound_is_below_frozen_target():
    bound = _cost_bound()
    assert bound["development_measured_token_bound"] == 560000
    assert bound["production_amortized_total_token_ratio_bound"] == 0.268298
    assert bound["passed_lte_0_28"] is True


def test_v203_freezes_nonoverlapping_manifest_and_zero_token_authorization(
    tmp_path: Path,
):
    root = tmp_path / "v203"
    first = freeze_v203(output_dir=root, database_path=Path("data/factory.sqlite"))
    second = freeze_v203(output_dir=root, database_path=Path("data/factory.sqlite"))
    assert first == second
    manifest = json.loads((root / "manifest-v3.json").read_text())
    assert manifest["segment_count"] == 16
    assert manifest["source_count"] == 4
    assert manifest["density_counts"] == {"dense": 12, "no_signal": 4}
    excluded_episodes = set()
    excluded_hashes = set()
    for path in EXCLUSION_MANIFESTS:
        for row in _walk(json.loads(path.read_text())):
            if row.get("episode_id"):
                excluded_episodes.add(str(row["episode_id"]))
            if row.get("text_sha256") or row.get("segment_text_sha256"):
                excluded_hashes.add(
                    str(row.get("text_sha256") or row.get("segment_text_sha256"))
                )
    selected_episodes = {str(row["episode_id"]) for row in manifest["episodes"]}
    selected_hashes = {
        str(segment["text_sha256"])
        for episode in manifest["episodes"]
        for segment in episode["segments"]
    }
    assert not selected_episodes.intersection(excluded_episodes)
    assert not selected_hashes.intersection(excluded_hashes)
    assert first["semantic_attempt_authorized"] is True
    assert first["prior_episode_or_text_replayed"] is False
    assert first["extraction_rerun_authorized"] is False
    assert first["production_amortized_total_token_ratio_bound"] < 0.28
    assert first["holdout_authorized"] is False
    assert first["production_mutated"] is False
    assert first["usage"]["total_tokens"] == 0
    spec = json.loads((root / "fresh-exhaustive-spec.json").read_text())
    assert len(spec["runtime_files"]) == 4
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))
