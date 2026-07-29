from __future__ import annotations

import json
import tempfile
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v67_layered_residual import (
    DEFAULT_OUTPUT_ROOT as V67_ROOT,
    validate_pointwise_output,
)
from research_factory.app_server_judge_v5_calibration_v68_exact_span_recovery import (
    ROOT_EFFORT,
    ROOT_MODEL,
    TURN_NAME,
    _validate_v67,
    freeze_v68,
    project_pointwise_exact_spans,
)


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_v68_accepts_only_the_measured_v67_exact_span_failure() -> None:
    records = _validate_v67(V67_ROOT)
    assert records["v67_terminal"]["sha256"]
    terminal = _load(V67_ROOT / "terminal.json")
    assert terminal["usage"] == {
        "cached_input_tokens": 0,
        "input_tokens": 22961,
        "output_tokens": 6761,
        "reasoning_output_tokens": 322,
        "total_tokens": 29722,
    }
    assert not (V67_ROOT / "turns/neutral-root-verification/sidecar.json").exists()


def test_v68_projection_drops_one_nonexact_span_without_semantic_change() -> None:
    value = _load(V67_ROOT / "layered-input.private.json")
    output = _load(V67_ROOT / "turns/pointwise-field-facts/output.private.json")
    assert validate_pointwise_output(output, value) == ["unit_4_row_2_evidence"]
    projected, operations = project_pointwise_exact_spans(output, value)
    assert validate_pointwise_output(projected, value) == []
    assert len(operations) == 1
    assert operations[0]["operation_type"] == "drop_nonexact_pointwise_evidence_span"
    before = [
        (unit["case_id"], unit["witness_id"], [(row["field"], row["field_state"]) for row in unit["field_facts"]])
        for unit in output["units"]
    ]
    after = [
        (unit["case_id"], unit["witness_id"], [(row["field"], row["field_state"]) for row in unit["field_facts"]])
        for unit in projected["units"]
    ]
    assert after == before


def test_v68_freeze_declares_only_the_unstarted_sol_turn() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "v68"
        frozen = freeze_v68(output_dir=root)
        again = freeze_v68(output_dir=root)
        assert frozen["spec"] == again["spec"]
        assert frozen["spec"]["turn_plan"] == [
            {"turn_name": TURN_NAME, "model": ROOT_MODEL, "effort": ROOT_EFFORT}
        ]
        assert frozen["spec"]["v67_pointwise_replayed"] is False
        assert frozen["spec"]["retry_count_per_turn"] == 0
        assert frozen["spec"]["holdout_authorized"] is False
        assert frozen["spec"]["production_mutation_allowed"] is False
        assert not (root / "terminal.json").exists()
        assert not (root / "turns/neutral-root-verification/capacity.json").exists()
        assert not (root / "turns/neutral-root-verification/sidecar.json").exists()
        assert not (root / "turns/pointwise-field-facts").exists()
