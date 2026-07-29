from __future__ import annotations

import json
from pathlib import Path

from research_factory.app_server_judge_v5_selection_v191_extraction_quality_nonacceptance import (
    _validate_v190_stop,
    freeze_v191,
)


def test_v191_validates_v190_quality_stop_and_token_pass():
    predecessor = _validate_v190_stop()
    leader = predecessor["score"]["systems"][
        predecessor["score"]["observed_quality_leader"]
    ]
    assert leader["cost"]["passed_lte_0_28"] is True
    assert leader["gate"]["semantic_passed"] is False
    assert predecessor["score"]["source_critical_sample"]["met_required_average"] is False


def test_v191_freezes_truthful_zero_token_nonacceptance(tmp_path: Path):
    root = tmp_path / "v191"
    first = freeze_v191(output_dir=root)
    second = freeze_v191(output_dir=root)
    assert first == second
    assert first["overall_evaluation_complete"] is False
    assert first["evaluation_accepted"] is False
    assert first["semantic_quality_passed"] is False
    assert first["production_amortized_token_target_passed"] is True
    assert first["development_winner_frozen"] is False
    assert first["holdout_authorized"] is False
    assert first["production_mutated"] is False
    assert first["attempt_usage"]["total_tokens"] == 0
    report = json.loads((root / "development-nonacceptance-report.json").read_text())
    assert report["blocker_class"] == "measured_development_semantic_quality_shortfall"
    assert report["safe_local_experiment_remaining_under_current_constraints"] is False
    assert report["evaluation_acceptance_receipt_emitted"] is False
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("capacity.json"))
