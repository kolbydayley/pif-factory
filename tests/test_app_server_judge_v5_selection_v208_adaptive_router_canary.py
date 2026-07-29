from __future__ import annotations

import json
from pathlib import Path

import pytest

from research_factory import app_server_judge_v5_selection_v192_residual_repair_design as v192
from research_factory.app_server_dev_selection import BASELINE_REPAIRED_SYSTEM
from research_factory.app_server_judge_v5_selection_v207_adaptive_router_design import (
    freeze_v207,
)
from research_factory.app_server_judge_v5_selection_v208_adaptive_router_canary import (
    JudgeV5SelectionV208Error,
    _freeze_launch_receipt,
    _score_router,
    _validate_v207_authorization,
    freeze_v208,
    validate_router_output,
    verify_runtime_lock,
)


@pytest.fixture(scope="module")
def frozen(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, Path, dict]:
    root = tmp_path_factory.mktemp("v208")
    design_root = root / "v207"
    attempt_root = root / "v208"
    freeze_v207(output_dir=design_root)
    frozen_attempt = freeze_v208(output_dir=attempt_root, design_root=design_root)
    return design_root, attempt_root, frozen_attempt


def _oracle_output(predecessor: dict) -> dict:
    _augmented, score, _combinations = v192._all_composite_score()
    baseline = {
        str(row["case_id"]): str(row["segment_id"])
        for row in score["systems"][BASELINE_REPAIRED_SYSTEM]["cases"]
    }
    picks = {
        str(row["segment_id"]): row
        for row in predecessor["oracle"]["canary"]["best_affordable_picks"]
    }
    packages = predecessor["design"]["package_mapping_private"]
    rows = []
    for case_id in predecessor["design"]["canary_case_ids"]:
        arms = picks[baseline[case_id]]["arms"]
        selected = sorted(packages[arm] for arm in arms)
        rows.append(
            {
                "case_id": case_id,
                "base_coverage": (
                    "unsupported_or_no_signal" if not arms else "material_gaps"
                ),
                "route_reason": (
                    "no_eligible_events"
                    if not arms
                    else "additional_independent_coverage_needed"
                ),
                "selected_package_ids": selected,
            }
        )
    return {"cases": rows}


def test_v208_freezes_exact_runtime_and_no_semantic_artifacts(frozen):
    design_root, attempt_root, value = frozen
    lock = verify_runtime_lock(value["runtime_lock"], design_root=design_root)
    spec = json.loads((attempt_root / "attempt-spec.json").read_text())
    assert len(lock["runtime_files"]) == 13
    assert spec["turn_plan"] == ["selection_adaptive_router_canary_00"]
    assert spec["retry_count_per_turn"] == 0
    assert spec["maximum_total_tokens_per_turn"] == 25_000
    assert spec["extraction_model_calls_authorized"] == 0
    assert spec["extraction_replay_allowed"] is False
    assert spec["batch_5_replay_allowed"] is False
    assert spec["holdout_authorized"] is False
    assert not (attempt_root / "launch-receipt.json").exists()
    assert not list(attempt_root.rglob("capacity.json"))
    assert not list(attempt_root.rglob("sidecar.json"))
    assert not list(attempt_root.rglob("output.private.json"))


def test_v208_oracle_route_passes_frozen_canary_gate(frozen):
    design_root, _attempt_root, _value = frozen
    predecessor = _validate_v207_authorization(design_root)
    output = _oracle_output(predecessor)
    assert validate_router_output(output, predecessor) == []
    usage = {
        "input_tokens": 5_000,
        "cached_input_tokens": 0,
        "output_tokens": 500,
        "reasoning_output_tokens": 100,
        "total_tokens": 5_500,
    }
    gate, private = _score_router(
        output=output,
        usage=usage,
        predecessor=predecessor,
    )
    assert gate["passed"] is True
    assert gate["mean_f1_regret_to_oracle"] == 0.0
    assert gate["dense_improvement_count"] >= 3
    assert gate["projected_full_router_total_tokens"] <= 56_000
    assert len(private["cases"]) == 8


def test_v208_validator_rejects_order_and_budget_drift(frozen):
    design_root, _attempt_root, _value = frozen
    predecessor = _validate_v207_authorization(design_root)
    output = _oracle_output(predecessor)
    output["cases"] = list(reversed(output["cases"]))
    assert validate_router_output(output, predecessor) == ["case_order_or_coverage"]
    output = _oracle_output(predecessor)
    package_ids = sorted(
        predecessor["design"]["package_mapping_private"].values()
    )
    for row in output["cases"]:
        row["selected_package_ids"] = package_ids
    assert validate_router_output(output, predecessor) == [
        "declared_extraction_budget"
    ]


def test_v208_runtime_lock_requires_exact_runtime_coverage(frozen):
    design_root, _attempt_root, value = frozen
    lock_path = value["runtime_lock"]
    lock = json.loads(lock_path.read_text())
    lock["runtime_files"] = lock["runtime_files"][1:]
    mutated = lock_path.parent / "runtime-lock-mutated.json"
    mutated.write_text(json.dumps(lock) + "\n", encoding="utf-8")
    with pytest.raises(JudgeV5SelectionV208Error, match="runtime lock drifted"):
        verify_runtime_lock(mutated, design_root=design_root)


def test_v208_launch_receipt_is_single_and_presemantic(frozen):
    _design_root, attempt_root, value = frozen
    first = _freeze_launch_receipt(value)
    second = _freeze_launch_receipt(value)
    assert first == second
    receipt = json.loads(first.read_text())
    assert receipt["capacity_checkpoint_exists_before_launch"] is False
    assert receipt["sidecar_exists_before_launch"] is False
    assert receipt["output_exists_before_launch"] is False
    assert receipt["retry_count_per_turn"] == 0
    assert not (attempt_root / "terminal.json").exists()
