from __future__ import annotations

import json
import tempfile
from pathlib import Path

from research_factory.app_server_judge_v5 import CHECKLIST_FIELDS
from research_factory.app_server_judge_v5_calibration_v59_projection import (
    DEFAULT_OUTPUT_ROOT as V59_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v60_gpt54 import (
    DEFAULT_OUTPUT_ROOT as V60_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v61_adjudication import (
    build_v61_input,
    freeze_v61,
    merge_v61,
    score_v57,
)


def _inputs() -> tuple[dict, dict, dict]:
    base = {
        "units": [
            row
            for index in range(2)
            for row in json.loads(
                (
                    V60_ROOT
                    / "turns"
                    / f"root-projection-shard-{index:02d}"
                    / "input.private.json"
                ).read_text()
            )["units"]
        ]
    }
    luna = json.loads((V59_ROOT / "projected-root-output.private.json").read_text())
    gpt54 = json.loads((V60_ROOT / "gpt54-specialist-output-full.private.json").read_text())
    return base, luna, gpt54


def test_v61_routes_whole_disagreement_units_with_neutral_balance() -> None:
    value, agreement = build_v61_input(*_inputs())
    assert len(value["units"]) == 13
    assert len(agreement) == 5
    assert value["candidate_origin_labels_present"] is False
    assert value["system_identity_present"] is False
    assert all(
        [row["field"] for row in unit["candidate_a"]] == list(CHECKLIST_FIELDS)
        and [row["field"] for row in unit["candidate_b"]] == list(CHECKLIST_FIELDS)
        for unit in value["units"]
    )
    rendered = json.dumps(value)
    assert "gpt-5.4" not in rendered and "luna" not in rendered


def test_v61_oracle_adjudication_ceiling_passes_all_frozen_gates() -> None:
    value, agreement = build_v61_input(*_inputs())
    truth = json.loads((V60_ROOT / "diagnostic-truth.private.json").read_text())
    taxonomy = json.loads((V60_ROOT / "error-taxonomy.json").read_text())
    gpt54 = _inputs()[2]
    expected = {
        (case_id, witness_id): set(fields)
        for case_id, case in truth["cases"].items()
        for witness_id, fields in case["field_issues"].items()
    }
    adjudicated = {"units": []}
    for row in value["units"]:
        key = (row["case_id"], row["witness_id"])
        adjudicated["units"].append(
            {
                "case_id": key[0],
                "witness_id": key[1],
                "structured_field_verdict": "incorrect" if expected[key] else "correct",
                "checklist": [
                    {
                        "field": field,
                        "decision": "different" if field in expected[key] else "same",
                        "source_evidence_spans": [],
                        "rationale": "Synthetic source-first decision.",
                    }
                    for field in CHECKLIST_FIELDS
                ],
            }
        )
    merged = merge_v61(adjudicated, agreement, gpt54)
    assert score_v57(merged, truth, taxonomy)["passed"] is True


def test_v61_freeze_is_one_capped_presemantic_turn() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "v61"
        frozen = freeze_v61(output_dir=root)
        again = freeze_v61(output_dir=root)
        assert again["spec"] == frozen["spec"]
        assert frozen["spec"]["model"] == "gpt-5.5"
        assert frozen["spec"]["disagreement_unit_count"] == 13
        assert frozen["spec"]["agreement_unit_count"] == 5
        assert frozen["spec"]["candidate_order_balance"] == {
            "luna_as_a": 7,
            "gpt54_as_a": 6,
        }
        assert frozen["spec"]["retry_count_per_turn"] == 0
        assert not list(root.glob("turns/*/capacity.json"))
        assert not list(root.glob("turns/*/sidecar.json"))
