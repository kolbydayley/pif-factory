from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from research_factory import (
    app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic as v86,
    app_server_judge_v5_selection_v220_integrated_base_design as v220,
)
from research_factory.paths import db_path


def _fake_predecessor(root: Path) -> dict:
    paths = {}
    for name in ("spec", "report", "audit", "runtime_lock", "terminal"):
        path = root / f"v219-{name}.json"
        path.write_text(json.dumps({"name": name}) + "\n", encoding="utf-8")
        paths[name] = path
    return {
        "root": root,
        "paths": paths,
        "records": {name: v86._record(path) for name, path in paths.items()},
        "terminal": {
            "cumulative_known_usage_lower_bound": {
                "input_tokens": 7_677_377,
                "cached_input_tokens": 924_032,
                "output_tokens": 1_409_236,
                "reasoning_output_tokens": 460_265,
                "total_tokens": 9_086_613,
            },
            "cumulative_unknown_usage_turn_count": 3,
            "cumulative_conservative_unknown_usage_upper_bound": 245_000,
        },
        "report": {"required_strategy": "integrated"},
    }


@pytest.fixture(scope="module")
def frozen_v220(tmp_path_factory: pytest.TempPathFactory):
    temp = tmp_path_factory.mktemp("v220")
    predecessor = _fake_predecessor(temp)
    patch = pytest.MonkeyPatch()
    patch.setattr(v220, "_validate_v219_checkpoint", lambda: predecessor)
    patch.setattr(
        v220,
        "_expected_runtime_paths",
        lambda: (Path(v220.__file__).resolve(),),
    )
    root = temp / "attempt"
    terminal = v220.freeze_v220(output_dir=root, database_path=db_path())
    try:
        yield root, terminal, predecessor
    finally:
        patch.undo()


def test_v220_cost_bound_preserves_followup_headroom():
    cost = v220._cost_bound()
    assert cost["maximum_base_development_tokens"] == 140_000
    assert cost["maximum_followup_development_tokens"] == 80_000
    assert cost["base_production_amortized_total_token_ratio"] <= 0.18
    assert cost["base_plus_followup_production_amortized_total_token_ratio"] <= 0.28
    assert cost["base_plus_followup_passes_lte_0_28"] is True


def test_v220_integrated_schema_requires_bounded_completeness_receipt():
    schema = v220.integrated_episode_batch_schema(
        episode_id="episode-1", segment_ids=["segment-a", "segment-b"]
    )
    segment = schema["properties"]["segments"]["items"]
    assert "coverage_receipt" in segment["required"]
    receipt = segment["properties"]["coverage_receipt"]
    assert receipt["additionalProperties"] is False
    assert receipt["properties"]["coverage_status"]["enum"] == [
        "complete",
        "material_gaps",
        "no_eligible_events",
        "abstain",
    ]
    assert receipt["properties"]["gap_evidence"]["maxItems"] == 4
    assert receipt["properties"]["gap_event_types"]["maxItems"] == 8
    assert schema["properties"]["segments"]["minItems"] == 2
    assert schema["properties"]["segments"]["maxItems"] == 2


def test_v220_instructions_make_omission_decision_llm_owned():
    instructions, guideline_sha = v220.integrated_core_instructions()
    normalized = " ".join(instructions.split())
    assert guideline_sha
    assert "perform one fresh source-wide omission pass" in normalized
    assert "This receipt is an LLM semantic decision" in normalized
    assert "Do not use a count heuristic" in normalized
    assert "infer hidden reference labels" in normalized


def test_v220_real_selection_is_fresh_balanced_and_pre_cutoff():
    source = db_path().expanduser().resolve()
    conn = sqlite3.connect(f"file:{source}?mode=ro", uri=True)
    try:
        selected, audit = v220.select_fresh_development_episodes(conn)
    finally:
        conn.close()
    assert len(selected) == 4
    assert len({row["episode_id"] for row in selected}) == 4
    assert len({row["source_id"] for row in selected}) == 4
    assert all(row["published_at"] < v220.PUBLISH_CUTOFF for row in selected)
    assert all(row["dense_label_count"] >= 1 for row in selected)
    assert all(row["no_signal_label_count"] >= 1 for row in selected)
    assert audit["semantic_regex_or_keyword_rules_used"] is False
    assert audit["embeddings_or_semantic_similarity_used"] is False


def test_v220_freeze_is_nonreplay_and_keeps_later_gates_closed(frozen_v220):
    root, terminal, _predecessor = frozen_v220
    manifest = json.loads((root / "manifest-v4.json").read_text())
    spec = json.loads((root / "attempt-spec.json").read_text())
    design = json.loads((root / "integrated-base-design.json").read_text())
    assert terminal["state"] == "completed"
    assert terminal["semantic_attempt_authorized"] is True
    assert terminal["fresh_nonreplay_extraction_authorized"] is True
    assert terminal["extraction_rerun_authorized"] is False
    assert terminal["batch_5_replayed"] is False
    assert terminal["prior_episode_or_text_replayed"] is False
    assert terminal["holdout_authorized"] is False
    assert terminal["production_mutated"] is False
    assert terminal["usage"]["total_tokens"] == 0
    assert manifest["segment_count"] == 8
    assert manifest["density_counts"] == {"dense": 4, "no_signal": 4}
    assert manifest["unique_text_sha256_count"] == 8
    assert spec["semantic_model_calls_declared"] == 4
    assert spec["extraction_rerun_authorized"] is False
    assert design["semantic_quality_scored_in_this_phase"] is False


def test_v220_runtime_lock_is_exact_and_idempotent(frozen_v220):
    root, terminal, _predecessor = frozen_v220
    lock = v220.verify_runtime_lock(root / "runtime-lock.json")
    assert lock["fresh_nonreplay_extraction_only"] is True
    assert lock["extraction_rerun_allowed"] is False
    assert lock["batch_5_replay_allowed"] is False
    assert lock["holdout_authorized"] is False
    assert lock["production_mutation_allowed"] is False
    assert v220.freeze_v220(output_dir=root, database_path=db_path()) == terminal


def test_v220_runtime_lock_rejects_exclusion_manifest_loss(frozen_v220):
    root, _terminal, _predecessor = frozen_v220
    lock = json.loads((root / "runtime-lock.json").read_text())
    lock["exclusion_manifests"] = lock["exclusion_manifests"][1:]
    mutated = root / "runtime-lock-mutated.json"
    mutated.write_text(json.dumps(lock) + "\n", encoding="utf-8")
    with pytest.raises(v220.JudgeV5SelectionV220Error, match="drifted"):
        v220.verify_runtime_lock(mutated)
