from __future__ import annotations

import tempfile
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v91_fresh_luna_diagnostic import (
    DEFAULT_OUTPUT_ROOT as V91_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v92_speaker_repair import (
    DEFAULT_OUTPUT_ROOT as V92_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v93_repair_scoring_receipt import (
    _validate_predecessors,
    build_v93_score,
    freeze_v93,
)


def test_v93_separates_original_canary_from_repaired_primary() -> None:
    predecessor = _validate_predecessors(v92_root=V92_ROOT, v91_root=V91_ROOT)
    score, audit = build_v93_score(predecessor)
    assert score["metrics"]["repaired_exact_count"] == 13
    assert score["metrics"]["repaired_primary_abstention_count"] == 0
    assert score["metrics"]["original_permutation_canary_exact_count"] == 4
    assert audit["old_repaired_vs_old_canary_exact_count"] == 3
    assert audit["original_pre_repair_canary_exact_count"] == 4
    assert score["observable_repair_trigger_cleared"] is True
    assert score["residual_reference_audit_authorized"] is True


def test_v93_freeze_is_zero_token_and_nonpromoting() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "v93"
        terminal = freeze_v93(output_dir=root)
        assert freeze_v93(output_dir=root) == terminal
        assert terminal["state"] == "completed"
        assert terminal["residual_reference_audit_authorized"] is True
        assert terminal["fresh_full_development_calibration_authorized"] is False
        assert terminal["selection_authorized"] is False
        assert terminal["holdout_authorized"] is False
        assert terminal["production_mutated"] is False
        assert terminal["semantic_attempt_started"] is False
        assert terminal["usage"]["total_tokens"] == 0
        assert not (root / "turns").exists()
