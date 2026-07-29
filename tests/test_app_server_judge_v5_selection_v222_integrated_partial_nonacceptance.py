from __future__ import annotations

import json
from pathlib import Path

import pytest

from research_factory import (
    app_server_judge_v5_selection_v222_integrated_partial_nonacceptance as v222,
)


@pytest.fixture(scope="module")
def partial() -> dict:
    return v222._validate_v221_partial()


@pytest.fixture(scope="module")
def frozen_root(
    tmp_path_factory: pytest.TempPathFactory, partial: dict
) -> Path:
    root = tmp_path_factory.mktemp("v222") / "attempt"
    patcher = pytest.MonkeyPatch()
    patcher.setattr(
        v222,
        "_validate_v221_partial",
        lambda database_path=None: partial,
    )
    try:
        terminal = v222.freeze_v222(output_dir=root)
    finally:
        patcher.undo()
    assert terminal["state"] == "development_strategy_not_accepted"
    return root


def test_v222_binds_exact_v221_partial_attempt_and_usage(partial: dict):
    assert len(partial["paths"]) == 31
    assert partial["completed_turn_names"] == partial["turn_names"][:2]
    assert partial["not_started_turn_names"] == partial["turn_names"][2:]
    assert partial["usage"] == v222.EXPECTED_USAGE
    assert partial["terminal"]["accounting_complete"] is False
    assert partial["failure"]["unknown_usage_turn_count"] == 0


def test_v222_adopts_second_output_and_eliminates_frozen_strategy(
    partial: dict,
):
    gate, private = v222._partial_gate(partial)
    assert len(private["turns"]) == 2
    assert all(turn["normalized"] for turn in private["turns"])
    assert gate["passed"] is False
    assert gate["failed_checks"] == [
        "unflagged_dense_coverage_shortfall_count_0"
    ]
    assert gate["dense_candidate_to_reference_event_count_ratios"] == [
        0.407407,
        0.73913,
    ]
    assert gate["unflagged_dense_coverage_shortfall_count"] == 2
    assert (
        gate["remaining_turns_can_change_frozen_v220_strategy_verdict"]
        is False
    )


def test_v222_does_not_deterministically_reject_one_sided_no_signal_event(
    partial: dict,
):
    gate, _private = v222._partial_gate(partial)
    assert gate["no_signal_candidate_positive_segment_count"] == 1
    assert gate["no_signal_candidate_positive_disposition"] == (
        "requires_frozen_llm_adjudication_not_automatically_false_positive"
    )


def test_v222_freezes_zero_token_nonacceptance_and_later_gates_closed(
    frozen_root: Path,
):
    terminal = json.loads((frozen_root / "terminal.json").read_text())
    assert terminal["terminal_classification"] == (
        "inactive_incomplete_recovery_required"
    )
    assert terminal["usage"]["total_tokens"] == 0
    assert terminal["accounting_complete"] is True
    assert terminal["fresh_frozen_judge_audit_authorized"] is True
    assert terminal["development_winner_frozen"] is False
    assert terminal["v221_not_started_turns_replayed"] is False
    assert terminal["holdout_authorized"] is False
    assert terminal["production_mutated"] is False
    assert not list(frozen_root.rglob("capacity.json"))
    assert not list(frozen_root.rglob("sidecar.json"))
    assert not list(frozen_root.rglob("output.private.json"))
    assert not (frozen_root / "launch-receipt.json").exists()
    assert v222.freeze_v222(output_dir=frozen_root) == terminal


def test_v222_runtime_lock_rejects_mutated_runtime_record(
    frozen_root: Path,
):
    lock = json.loads((frozen_root / "runtime-lock.json").read_text())
    lock["runtime_files"][0]["sha256"] = "0" * 64
    mutated = frozen_root / "mutated-runtime-lock.json"
    mutated.write_text(json.dumps(lock) + "\n", encoding="utf-8")
    with pytest.raises(v222.JudgeV5SelectionV222Error):
        v222.verify_runtime_lock(mutated)
