from __future__ import annotations

import json
from pathlib import Path

from research_factory.app_server_judge_v5_selection_v190_composite_convergence import (
    _validate_v189_success,
    freeze_v190,
)


def test_v190_validates_one_measured_v189_turn_without_replay():
    success = _validate_v189_success()
    assert success["terminal"]["usage"]["total_tokens"] == 61764
    assert success["terminal"]["semantic_retry_count"] == 0
    assert success["terminal"]["cumulative_known_usage_lower_bound"]["total_tokens"] == 8294282


def test_v190_zero_token_convergence_keeps_holdout_closed(tmp_path: Path):
    root = tmp_path / "v190"
    first = freeze_v190(output_dir=root)
    second = freeze_v190(output_dir=root)
    assert first == second
    assert first["overall_evaluation_complete"] is False
    assert first["development_winner_frozen"] is False
    assert first["holdout_authorized"] is False
    assert first["production_mutated"] is False
    assert first["attempt_usage"]["total_tokens"] == 0
    spec = json.loads((root / "composite-convergence-spec.json").read_text())
    assert spec["semantic_model_calls_started"] == 0
    assert spec["extraction_model_calls_started"] == 0
    assert spec["semantic_regex_or_keyword_rules_used"] is False
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("capacity.json"))
