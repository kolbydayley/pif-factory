from __future__ import annotations

import json
from pathlib import Path

from research_factory.app_server_judge_v5_selection_v195_metric_patch_adoption import (
    _validate_v194_completed_failed_attempt,
    freeze_v195,
)


def test_v195_preserves_v194_failure_and_adopts_only_complete_measured_output():
    predecessor = _validate_v194_completed_failed_attempt()
    assert predecessor["terminal"]["state"] == "failed"
    assert predecessor["failure"]["error_class"] == "ReserveCapacityError"
    assert predecessor["sidecar"]["state"] == "completed"
    assert predecessor["usage"]["total_tokens"] == 22368
    assert predecessor["failure"]["unknown_usage_turn_count"] == 0


def test_v195_zero_call_adoption_clears_phase_one_gates(tmp_path: Path):
    root = tmp_path / "v195"
    first = freeze_v195(output_dir=root)
    second = freeze_v195(output_dir=root)
    assert first == second
    assert first["v194_turn_replayed"] is False
    assert first["v194_failure_preserved"] is True
    assert first["support_judge_authorized"] is True
    assert first["alignment_judge_authorized"] is False
    assert first["holdout_authorized"] is False
    assert first["production_mutated"] is False
    assert first["attempt_usage"]["total_tokens"] == 0
    spec = json.loads((root / "metric-patch-adoption-spec.json").read_text())
    assert spec["semantic_model_calls_started"] == 0
    assert spec["v194_turn_replayed"] is False
    gate = json.loads((root / "metric-patch-adoption-gate.json").read_text())
    assert gate["passed"] is True
    assert gate["checks"]["metric_grounding"] is True
    assert gate["checks"]["production_amortized_total_token_ratio"] is True
    assert gate["production_amortized_total_token_ratio"] <= 0.28
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("capacity.json"))
