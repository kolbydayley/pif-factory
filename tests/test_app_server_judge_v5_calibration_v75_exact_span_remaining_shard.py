from __future__ import annotations

import json
import tempfile
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v74_stable_direct_field_audit import (
    DEFAULT_OUTPUT_ROOT as V74_ROOT,
)
from research_factory.app_server_judge_v5_calibration_v75_exact_span_remaining_shard import (
    MODEL,
    TURN_NAME,
    _real_attempts,
    _validate_v74,
    freeze_v75,
    project_exact_spans,
)
from research_factory.app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_v75_binds_four_measured_v74_turns_and_one_unstarted_turn() -> None:
    predecessor = _validate_v74(V74_ROOT)
    assert len(predecessor["projected_outputs"]) == 4
    assert len(predecessor["projection_operations"]) == 1
    assert predecessor["projection_operations"][0]["operation_type"] == "drop_nonexact_source_span"
    assert predecessor["corrected_usage"]["accounting_complete"] is True
    assert predecessor["corrected_usage"]["turn_count"] == 4
    expected = {field: 0 for field in USAGE_FIELDS}
    for index in range(4):
        sidecar = _load(
            V74_ROOT
            / "turns"
            / f"direct-field-shard-{index:02d}"
            / "sidecar.json"
        )
        for field in USAGE_FIELDS:
            expected[field] += sidecar["usage"][field]
    assert predecessor["corrected_usage"]["usage"] == expected
    fifth = V74_ROOT / "turns/direct-field-shard-04"
    assert not (fifth / "capacity.json").exists()
    assert not (fifth / "sidecar.json").exists()
    assert not (fifth / "output.private.json").exists()


def test_v75_projection_drops_only_nonexact_span_and_keeps_exact_evidence() -> None:
    root = V74_ROOT / "turns/direct-field-shard-03"
    projected, operations = project_exact_spans(
        _load(root / "output.private.json"), _load(root / "input.private.json")
    )
    assert len(operations) == 1
    affected_id = operations[0]["task_id"]
    affected = next(row for row in projected["decisions"] if row["task_id"] == affected_id)
    assert len(affected["source_evidence_spans"]) == 1
    assert operations[0]["operation_type"] == "drop_nonexact_source_span"


def test_v75_freeze_reuses_only_v74_never_started_fifth_request() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "v75"
        frozen = freeze_v75(output_dir=root)
        again = freeze_v75(output_dir=root)
        assert again["spec"] == frozen["spec"]
        assert frozen["spec"]["model"] == MODEL
        assert frozen["spec"]["turn_plan"] == [TURN_NAME]
        assert frozen["spec"]["v74_started_turn_count"] == 4
        assert frozen["spec"]["v74_adopted_measured_turn_count"] == 4
        assert frozen["spec"]["v74_unstarted_turn_count"] == 1
        assert frozen["spec"]["v74_semantic_turn_replayed"] is False
        assert frozen["spec"]["retry_count_per_turn"] == 0
        assert frozen["spec"]["holdout_authorized"] is False
        assert frozen["spec"]["production_mutation_allowed"] is False
        reuse = frozen["spec"]["exact_request_reuse"]
        assert frozen["paths"]["input"].read_bytes() == Path(reuse["v74_input"]["path"]).read_bytes()
        assert frozen["paths"]["prompt"].read_bytes() == Path(reuse["v74_prompt"]["path"]).read_bytes()
        assert frozen["paths"]["schema"].read_bytes() == Path(reuse["v74_schema"]["path"]).read_bytes()
        adopted = _load(root / "adopted-prefix-output.private.json")
        assert len(adopted["decisions"]) == 12
        turn_root = root / "turns/direct-field-shard-04-recovery"
        assert not (turn_root / "capacity.json").exists()
        assert not (turn_root / "sidecar.json").exists()
        assert not (turn_root / "output.private.json").exists()
        assert not (root / "terminal.json").exists()


def test_v75_real_attempt_filter_excludes_frozen_but_unstarted_turn() -> None:
    planned = _attempt_records(V74_ROOT)
    assert len(planned) == 5
    assert len(_real_attempts(V74_ROOT)) == 4
