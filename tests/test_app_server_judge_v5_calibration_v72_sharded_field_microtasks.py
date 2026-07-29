from __future__ import annotations

import json
import tempfile
from pathlib import Path

from research_factory.app_server_judge_v5_calibration_v71_field_microtasks import (
    DEFAULT_OUTPUT_ROOT as V71_ROOT,
    assemble_checklist,
    project_output,
    score_v71,
    validate_output,
)
from research_factory.app_server_judge_v5_calibration_v72_sharded_field_microtasks import (
    EFFORT,
    MODEL,
    TASKS_PER_SHARD,
    TURN_NAMES,
    _validate_v71,
    build_v72_shards,
    freeze_v72,
    merge_shard_outputs,
    output_schema,
)


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _source() -> tuple[dict, dict, dict]:
    return (
        _load(V71_ROOT / "field-microtask-input.private.json"),
        _load(V71_ROOT / "diagnostic-truth.private.json"),
        _load(V71_ROOT / "cohort-roles.json"),
    )


def _perfect_output(shard: dict, truth: dict) -> dict:
    expected = {
        (case_id, witness_id): set(fields)
        for case_id, case in truth["cases"].items()
        for witness_id, fields in case["field_issues"].items()
    }
    return {
        "decisions": [
            {
                "task_id": task["task_id"],
                "root_status": (
                    "root"
                    if task["field"]
                    in expected[(task["case_id"], task["witness_id"])]
                    else "not_root"
                ),
                "source_evidence_spans": [],
                "rationale": "Independent field decision.",
            }
            for task in shard["tasks"]
        ]
    }


def test_v72_requires_the_immutable_unknown_usage_v71_timeout() -> None:
    records = _validate_v71(V71_ROOT)
    assert records["v71_terminal"]["sha256"]
    terminal = _load(V71_ROOT / "terminal.json")
    sidecar = _load(V71_ROOT / "turns/field-microtask-diagnostic/sidecar.json")
    assert terminal["usage_status"] == "unknown"
    assert terminal["accounting_complete"] is False
    assert sidecar["state"] == "interrupted"
    assert sidecar["error_class"] == "turn_timeout"
    assert not (V71_ROOT / "turns/field-microtask-diagnostic/output.private.json").exists()


def test_v72_shards_cover_identical_tasks_once_without_semantic_changes() -> None:
    value, _, _ = _source()
    shards = build_v72_shards(value)
    assert len(shards) == len(TURN_NAMES) == 6
    assert all(shard["task_count"] == TASKS_PER_SHARD for shard in shards)
    assert all(len(shard["units"]) == 1 for shard in shards)
    task_ids = [row["task_id"] for shard in shards for row in shard["tasks"]]
    assert len(task_ids) == len(set(task_ids)) == 90
    assert set(task_ids) == {row["task_id"] for row in value["tasks"]}
    assert [shard["units"][0] for shard in shards] == value["units"]
    assert all(shard["field_contracts"] == value["field_contracts"] for shard in shards)


def test_v72_each_shard_fits_and_perfect_merge_passes_strict_gate() -> None:
    value, truth, roles = _source()
    outputs = []
    for shard in build_v72_shards(value):
        schema_bytes = len(json.dumps(output_schema(shard), sort_keys=True).encode("utf-8"))
        assert schema_bytes <= 64 * 1024
        output = _perfect_output(shard, truth)
        projected, operations = project_output(output, shard)
        assert operations == []
        assert validate_output(projected, shard) == []
        outputs.append(projected)
    merged = merge_shard_outputs(outputs)
    score = score_v71(assemble_checklist(merged, value), truth, roles)
    assert score["passed"] is True
    assert score["failed_checks"] == []


def test_v72_one_wrong_shard_decision_fails_after_merge() -> None:
    value, truth, roles = _source()
    shards = build_v72_shards(value)
    outputs = [_perfect_output(shard, truth) for shard in shards]
    task_by_id = {row["task_id"]: row for row in value["tasks"]}
    decision = next(
        row
        for output in outputs
        for row in output["decisions"]
        if task_by_id[row["task_id"]]["field"]
        not in {"evidence", "unsupported_inference"}
    )
    decision["root_status"] = (
        "not_root" if decision["root_status"] == "root" else "root"
    )
    projected = [project_output(output, shard)[0] for output, shard in zip(outputs, shards)]
    score = score_v71(
        assemble_checklist(merge_shard_outputs(projected), value), truth, roles
    )
    assert score["passed"] is False


def test_v72_freeze_is_six_presemantic_zero_retry_turns_and_is_immutable() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / "v72"
        frozen = freeze_v72(output_dir=root)
        again = freeze_v72(output_dir=root)
        assert again["spec"] == frozen["spec"]
        assert frozen["spec"]["model"] == MODEL
        assert frozen["spec"]["reasoning_effort"] == EFFORT
        assert frozen["spec"]["turn_plan"] == list(TURN_NAMES)
        assert frozen["spec"]["tasks_per_shard"] == TASKS_PER_SHARD
        assert frozen["spec"]["retry_count_per_turn"] == 0
        assert frozen["spec"]["v71_turn_replayed"] is False
        assert frozen["spec"]["holdout_authorized"] is False
        assert frozen["spec"]["production_mutation_allowed"] is False
        assert not (root / "terminal.json").exists()
        for turn_name in TURN_NAMES:
            turn_root = root / "turns" / turn_name.replace("_", "-")
            assert not (turn_root / "capacity.json").exists()
            assert not (turn_root / "sidecar.json").exists()
            assert not (turn_root / "output.private.json").exists()
