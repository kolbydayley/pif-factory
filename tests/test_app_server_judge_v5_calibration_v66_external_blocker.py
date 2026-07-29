from __future__ import annotations

import json
import tempfile
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v65_fresh_structured import (
    DEFAULT_OUTPUT_ROOT as V65_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v66_external_blocker import (
    build_v66_packet,
    decision_schema,
    freeze_v66_blocker,
)


def test_v66_packet_has_ten_private_units_and_sanitized_summary() -> None:
    value = json.loads((V65_ROOT / "fresh-structured-input.private.json").read_text())
    output = json.loads((V65_ROOT / "fresh-structured-output.private.json").read_text())
    truth = json.loads((V65_ROOT / "diagnostic-truth.private.json").read_text())
    packet, summary = build_v66_packet(value, output, truth)
    assert packet["unit_count"] == 10
    assert len(packet["units"]) == 10
    assert summary["residual_unit_count"] == 10
    assert summary["safe_local_semantic_experiment_remaining"] is False
    rendered = json.dumps(summary)
    assert "source_excerpt" not in rendered
    assert "structured_event" not in rendered


def test_v66_decision_schema_requires_human_owner_and_exact_coverage() -> None:
    schema = decision_schema()
    assert schema["properties"]["reviewer_type"]["const"] == "human_reference_owner"
    decisions = schema["properties"]["decisions"]
    assert decisions["minItems"] == 10
    assert decisions["maxItems"] == 10


def test_v66_freeze_is_zero_token_external_blocker() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "v66"
        terminal = freeze_v66_blocker(output_dir=root)
        again = freeze_v66_blocker(output_dir=root)
        assert again == terminal
        assert terminal["state"] == "blocked"
        assert terminal["blocker_is_external"] is True
        assert terminal["safe_local_semantic_experiment_remaining"] is False
        assert terminal["new_semantic_turn_count"] == 0
        assert terminal["new_usage"]["total_tokens"] == 0
        assert terminal["holdout_authorized"] is False
        assert terminal["production_mutated"] is False
        assert terminal["heartbeat_disable_authorized"] is True
