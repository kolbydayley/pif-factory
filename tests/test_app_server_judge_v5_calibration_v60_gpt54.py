from __future__ import annotations

import json
import tempfile
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v55_structured import validate_v55_output
from research_factory.app_server_judge_v5_calibration_v57_repair import (
    DEFAULT_OUTPUT_ROOT as V57_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v60_gpt54 import (
    freeze_v60,
    project_frozen_contract,
)


def test_v60_contract_projection_repairs_only_frozen_support_and_structure() -> None:
    source = V57_ROOT / "turns/root-projection-shard-01"
    value = json.loads((source / "input.private.json").read_text())
    expected = {"units": []}
    for row in value["units"]:
        expected["units"].append(
            {
                "case_id": row["case_id"],
                "witness_id": row["witness_id"],
                "structured_field_verdict": "correct",
                "checklist": [
                    {
                        "field": field,
                        "decision": "same",
                        "source_evidence_spans": [],
                        "rationale": "Synthetic decision.",
                    }
                    for field in value["checklist_field_order"]
                ],
            }
        )
    projected, operations = project_frozen_contract(expected, value)
    assert len(operations) == 2
    assert {operation["operation_type"] for operation in operations} == {
        "project_frozen_support_receipt"
    }
    assert validate_v55_output(projected, value) == []


def test_v60_freeze_is_model_only_and_idempotent() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "v60"
        frozen = freeze_v60(output_dir=root)
        again = freeze_v60(output_dir=root)
        assert again["spec"] == frozen["spec"]
        assert frozen["spec"]["model"] == "gpt-5.4"
        assert frozen["spec"]["request_content_changed_from_v57"] is False
        assert frozen["spec"]["retry_count_per_turn"] == 0
        for name in frozen["spec"]["turn_plan"]:
            source = V57_ROOT / "turns" / name.replace("_", "-")
            target = root / "turns" / name.replace("_", "-")
            for filename in ("input.private.json", "prompt.private.md", "schema.json"):
                assert (target / filename).read_bytes() == (source / filename).read_bytes()
        assert not list(root.glob("turns/*/capacity.json"))
        assert not list(root.glob("turns/*/sidecar.json"))
