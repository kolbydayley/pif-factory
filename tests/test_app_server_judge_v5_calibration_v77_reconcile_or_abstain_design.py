from __future__ import annotations

import json
import tempfile
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v75_exact_span_remaining_shard import (
    DEFAULT_OUTPUT_ROOT as V75_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v76_neutral_contested_adjudication import (
    DEFAULT_OUTPUT_ROOT as V76_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v77_reconcile_or_abstain_design import (
    build_reconciliation,
    freeze_v77,
)


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_v77_reconciliation_accepts_agreement_and_abstains_on_disagreement() -> None:
    delta, receipt = build_reconciliation(
        _load(V75_ROOT / "selected-truth.private.json"),
        _load(V75_ROOT / "direct-field-output.private.json"),
        _load(V76_ROOT / "neutral-adjudication-truth.private.json"),
        _load(V76_ROOT / "neutral-adjudication-output.private.json"),
    )
    assert delta["change_count"] == 4
    assert delta["abstention_count"] == 3
    assert receipt["settled_change_field_counts"] == {"event_boundary": 1, "target": 3}
    assert receipt["abstention_field_counts"] == {
        "attribution": 1,
        "metric": 1,
        "stance": 1,
    }
    assert receipt["majority_voting_used"] is False
    assert receipt["reference_mutated"] is False
    assert receipt["fresh_15_development_diagnostic_authorized"] is True
    assert receipt["holdout_authorized"] is False


def test_v77_freeze_is_zero_token_design_only_and_immutable() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "v77"
        terminal = freeze_v77(output_dir=root)
        again = freeze_v77(output_dir=root)
        assert again == terminal
        assert terminal["state"] == "completed"
        assert terminal["overall_evaluation_complete"] is False
        assert terminal["semantic_attempt_started"] is False
        assert terminal["fresh_15_development_diagnostic_authorized"] is True
        assert terminal["reference_mutated"] is False
        assert terminal["full_calibration_authorized"] is False
        assert terminal["holdout_authorized"] is False
        assert terminal["production_mutated"] is False
        assert terminal["accounting_complete"] is True
        assert terminal["usage_status"] == "complete"
        assert all(value == 0 for value in terminal["usage"].values())
