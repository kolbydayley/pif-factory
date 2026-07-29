from __future__ import annotations

import json
import tempfile
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v52_reference import (
    DEFAULT_OUTPUT_ROOT as V52_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v53_reference import (
    freeze_v53_reference,
    project_v52_pointwise_scope,
)


def test_v53_projection_removes_only_out_of_scope_boundary_evidence() -> None:
    truth = json.loads((V52_ROOT / "adjudicated-calibration-truth.private.json").read_text())
    output = json.loads(
        (V52_ROOT / "turns/structured-reference-adjudication/output.private.json").read_text()
    )
    value = json.loads(
        (V52_ROOT / "turns/structured-reference-adjudication/input.private.json").read_text()
    )
    projected, operations = project_v52_pointwise_scope(
        truth=truth, semantic_output=output, semantic_input=value
    )
    assert len(operations) == 4
    for operation in operations:
        assert operation["removed_fields"] == ["event_boundary", "evidence"]
        case = projected["cases"][operation["case_id"]]
        fields = case["field_issues"][operation["witness_id"]]
        assert "event_boundary" not in fields
        assert "evidence" not in fields


def test_v53_freeze_is_zero_token_idempotent() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "v53"
        terminal = freeze_v53_reference(output_dir=root)
        again = freeze_v53_reference(output_dir=root)
        assert again == terminal
        assert terminal["reference_frozen"] is True
        assert terminal["fresh_diagnostic_required"] is True
        assert terminal["semantic_attempt_started"] is False
        assert terminal["usage"]["total_tokens"] == 0
        assert terminal["holdout_authorized"] is False
