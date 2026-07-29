from __future__ import annotations

import tempfile
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v58_recovery import (
    DEFAULT_OUTPUT_ROOT as V58_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v59_projection import (
    project_v59,
    run_v59_projection,
)


def test_v59_projects_only_two_frozen_unsupported_receipts() -> None:
    _output, operations, score, _taxonomy = project_v59(V58_ROOT)
    assert len(operations) == 2
    assert {operation["field"] for operation in operations} == {"unsupported_inference"}
    assert all(
        operation["projection_source"] == "frozen_llm_proposition_receipt"
        for operation in operations
    )
    assert score["passed"] is False
    assert score["metrics"] == {
        "witness_count": 18,
        "structured_field_accuracy": 1.0,
        "field_issue_f1": 0.607143,
        "checklist_row_accuracy": 0.918519,
        "unsupported_inference_accuracy": 1.0,
        "event_boundary_scope_accuracy": 1.0,
        "attribution_specificity": 0.944444,
        "abstention_count": 7,
        "exact_case_rate": 0.388889,
        "control_exact_rate": 1.0,
    }


def test_v59_is_zero_token_and_idempotent() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "v59"
        terminal = run_v59_projection(output_dir=root)
        again = run_v59_projection(output_dir=root)
        assert again == terminal
        assert terminal["semantic_attempt_started"] is False
        assert terminal["new_semantic_turn_count"] == 0
        assert terminal["new_usage"]["total_tokens"] == 0
        assert terminal["projection_passed"] is False
        assert terminal["alternate_structured_specialist_diagnostic_authorized"] is True
