from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path

import pytest


SCRIPT = (
    Path(__file__).resolve().parents[1]
    / "automation"
    / "build_pif_native_goal_supervisor_runtime.py"
)
PROJECT_ROOT = SCRIPT.parents[1]


def _module():
    spec = importlib.util.spec_from_file_location("pif_native_supervisor_builder", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write_plan(root: Path, *, epoch: int, suffix: str) -> Path:
    directive = root / f"directive-{suffix}.json"
    directive.write_text(
        json.dumps({"schema_version": f"directive-{epoch}"}, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    digest = hashlib.sha256(directive.read_bytes()).hexdigest()
    plan = root / f"pif-evaluation-semantic-plan-{suffix}.json"
    plan.write_text(
        json.dumps(
            {
                "schema_version": "pif_evaluation_semantic_plan_v1",
                "thread_id": "019f4cf1-c46e-7db3-acd2-bf03c4459a10",
                "plan_epoch": epoch,
                "state": "executable",
                "step": {
                    "step_id": f"step-{epoch}",
                    "state": "executable",
                    "max_model_calls": 2,
                    "max_total_tokens": 100,
                    "expected_receipt_path": str((root / f"receipt-{suffix}.json").resolve()),
                    "accepted_receipt_states": ["passed", "rejected", "waiting"],
                    "directive_path": str(directive.resolve()),
                    "directive_sha256": digest,
                },
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return plan


def test_current_epoch_is_selected_without_mutating_predecessor(tmp_path: Path):
    builder = _module()
    prior = _write_plan(tmp_path, epoch=2, suffix="epoch-2")
    current = _write_plan(tmp_path, epoch=3, suffix="epoch-3")
    prior_before = prior.read_bytes()
    current_before = current.read_bytes()

    selected = builder.select_current_semantic_plan_source(tmp_path)

    assert selected == current.resolve()
    assert builder.verify_semantic_plan_source(selected) == current.resolve()
    assert prior.read_bytes() == prior_before
    assert current.read_bytes() == current_before


def test_duplicate_current_epoch_fails_closed(tmp_path: Path):
    builder = _module()
    _write_plan(tmp_path, epoch=3, suffix="epoch-3-a")
    _write_plan(tmp_path, epoch=3, suffix="epoch-3-b")

    with pytest.raises(SystemExit, match="multiple semantic plans"):
        builder.select_current_semantic_plan_source(tmp_path)


def test_selected_plan_rejects_directive_drift(tmp_path: Path):
    builder = _module()
    current = _write_plan(tmp_path, epoch=3, suffix="epoch-3")
    directive = tmp_path / "directive-epoch-3.json"
    directive.write_text("{}\n", encoding="utf-8")

    with pytest.raises(SystemExit, match="directive is unavailable or drifted"):
        builder.verify_semantic_plan_source(current)


def test_selected_plan_rejects_noncanonical_control_plane_keys(tmp_path: Path):
    builder = _module()
    current = _write_plan(tmp_path, epoch=3, suffix="epoch-3")
    plan = json.loads(current.read_text(encoding="utf-8"))
    plan["created_at"] = "not-allowed"
    current.write_text(json.dumps(plan, sort_keys=True) + "\n", encoding="utf-8")

    with pytest.raises(SystemExit, match="drifted semantic plan"):
        builder.verify_semantic_plan_source(current)


def test_repository_epoch44_advances_without_invalidating_epoch2_plan_record():
    builder = _module()
    epoch2_lock = (
        PROJECT_ROOT
        / "work/app-server-development-v2/unattended-pipeline-v5"
        / "development-selection-v249-true-full-output-shared-reference-v2"
        / "runtime-lock.json"
    )
    lock = json.loads(epoch2_lock.read_text(encoding="utf-8"))
    epoch2_rows = [
        row
        for row in lock["source_records"]
        if row["path"].endswith("automation/pif-evaluation-semantic-plan-v1.json")
    ]
    assert len(epoch2_rows) == 1
    epoch2_path = Path(epoch2_rows[0]["path"])
    assert hashlib.sha256(epoch2_path.read_bytes()).hexdigest() == epoch2_rows[0]["sha256"]

    selected = builder.select_current_semantic_plan_source(PROJECT_ROOT / "automation")
    selected_plan = json.loads(selected.read_text(encoding="utf-8"))
    assert selected.name == "pif-evaluation-semantic-plan-v44.json"
    assert selected_plan["plan_epoch"] == 44
    assert selected_plan["state"] == "executable"
    assert selected_plan["step"]["step_id"] == (
        "canonical_v31_epoch44_low_effort_keyed_enum_recovery_v44"
    )
    assert selected_plan["step"]["state"] == "executable"
    assert builder.verify_semantic_plan_source(selected) == selected.resolve()


def test_packaged_supervisor_includes_structural_schema_validator_closure():
    builder = _module()
    packaged_paths = set(builder.FILES.values())
    assert Path("research_factory/labels.py") in packaged_paths
    assert Path("research_factory/paths.py") in packaged_paths
