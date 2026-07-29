from __future__ import annotations

import json
import tempfile
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v55_structured import (
    DEFAULT_OUTPUT_ROOT as V55_ROOT,
    validate_v55_output,
)
from research_factory.app_server_judge_v5_calibration_v56_structured import (
    freeze_v56_structured,
    sanitize_nonexact_spans,
)


def test_v56_sanitizer_drops_only_nonexact_spans() -> None:
    source = V55_ROOT / "turns/structured-checklist-shard-00"
    value = json.loads((source / "input.private.json").read_text())
    output = json.loads((source / "output.private.json").read_text())
    sanitized, operations = sanitize_nonexact_spans(output, value)
    assert len(operations) == 2
    assert all(operation["semantic_decision_changed"] is False for operation in operations)
    assert validate_v55_output(sanitized, value) == []


def test_v56_freeze_adopts_once_and_schedules_only_remaining_turns() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "v56"
        frozen = freeze_v56_structured(output_dir=root)
        again = freeze_v56_structured(output_dir=root)
        assert again["spec"] == frozen["spec"]
        assert frozen["spec"]["adopted_turn_names"] == ["structured_checklist_shard_00"]
        assert frozen["spec"]["new_turn_plan"] == [
            "structured_checklist_shard_01",
            "structured_checklist_shard_02",
            "structured_checklist_shard_03",
        ]
        assert frozen["spec"]["retry_count_per_turn"] == 0
        assert not list(root.glob("turns/*/capacity.json"))
