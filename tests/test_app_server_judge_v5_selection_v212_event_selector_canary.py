from __future__ import annotations

import json
from pathlib import Path

import pytest

from research_factory import app_server_judge_v5_selection_v201_residual_repair_score as v201
from research_factory.app_server_judge_v5_selection_v211_event_selector_design import (
    freeze_v211,
)
from research_factory.app_server_judge_v5_selection_v212_event_selector_canary import (
    JudgeV5SelectionV212Error,
    _freeze_launch_receipt,
    _score_selector,
    _validate_v211_authorization,
    freeze_v212,
    validate_selector_output,
    verify_runtime_lock,
)


@pytest.fixture(scope="module")
def frozen_v212(
    tmp_path_factory: pytest.TempPathFactory,
) -> tuple[Path, Path, dict]:
    root = tmp_path_factory.mktemp("v212")
    design_root = root / "v211"
    attempt_root = root / "v212"
    freeze_v211(output_dir=design_root)
    frozen = freeze_v212(output_dir=attempt_root, design_root=design_root)
    return design_root, attempt_root, frozen


def _oracle_output(predecessor: dict) -> dict:
    consensus, _record = v201._base_consensus()
    consensus_by_case = {
        str(row["case_id"]): row for row in consensus["cases"]
    }
    provenance_by_case = {
        str(row["case_id"]): row for row in predecessor["provenance"]["cases"]
    }
    output = {"cases": []}
    for model_case in predecessor["input"]["cases"]:
        case_id = str(model_case["case_id"])
        consensus_case = consensus_by_case[case_id]
        support = {
            str(row["witness_id"]): str(row["verdict"])
            for row in consensus_case["support_results"]
        }
        group_by_witness = {
            str(witness_id): tuple(sorted(map(str, group)))
            for group in consensus_case["equivalence_groups"]
            for witness_id in group
        }
        witness_by_event = {
            str(row["event_id"]): str(row["witness_id"])
            for row in provenance_by_case[case_id]["events"]
        }
        canonical_by_group = {}
        decisions = []
        for event in model_case["candidate_events"]:
            event_id = str(event["event_id"])
            witness_id = witness_by_event[event_id]
            support_verdict = support[witness_id]
            if support_verdict == "supported":
                group = group_by_witness[witness_id]
                if group in canonical_by_group:
                    verdict = "duplicate"
                    canonical = canonical_by_group[group]
                else:
                    verdict = "keep"
                    canonical = event_id
                    canonical_by_group[group] = event_id
            elif support_verdict == "unsupported":
                verdict = "unsupported"
                canonical = ""
            else:
                verdict = "abstain"
                canonical = ""
            decisions.append(
                {
                    "event_id": event_id,
                    "verdict": verdict,
                    "canonical_event_id": canonical,
                }
            )
        output["cases"].append({"case_id": case_id, "decisions": decisions})
    return output


def test_v212_freezes_exact_runtime_without_semantic_artifacts(frozen_v212):
    design_root, attempt_root, value = frozen_v212
    lock = verify_runtime_lock(value["runtime_lock"], design_root=design_root)
    spec = json.loads((attempt_root / "attempt-spec.json").read_text())
    assert len(lock["runtime_files"]) == 18
    assert spec["turn_plan"] == ["selection_event_selector_canary_00"]
    assert spec["retry_count_per_turn"] == 0
    assert spec["maximum_total_tokens_per_turn"] == 45_000
    assert spec["promotion_total_token_gate"] == 35_000
    assert spec["v209_router_turn_replayed"] is False
    assert spec["extraction_model_calls_authorized"] == 0
    assert spec["extraction_replay_allowed"] is False
    assert spec["batch_5_replay_allowed"] is False
    assert spec["holdout_authorized"] is False
    assert not (attempt_root / "launch-receipt.json").exists()
    assert not list(attempt_root.rglob("capacity.json"))
    assert not list(attempt_root.rglob("sidecar.json"))
    assert not list(attempt_root.rglob("output.private.json"))


def test_v212_oracle_selector_passes_frozen_canary_gate(frozen_v212):
    design_root, _attempt_root, _value = frozen_v212
    predecessor = _validate_v211_authorization(design_root)
    output = _oracle_output(predecessor)
    assert validate_selector_output(output, predecessor) == []
    usage = {
        "input_tokens": 18_000,
        "cached_input_tokens": 0,
        "output_tokens": 1_800,
        "reasoning_output_tokens": 200,
        "total_tokens": 20_000,
    }
    gate, private = _score_selector(
        output=output,
        usage=usage,
        predecessor=predecessor,
    )
    assert gate["passed"] is True
    assert gate["candidate_mean_f1"] == 0.81506
    assert gate["mean_f1_regret_to_oracle"] == 0.017481
    assert gate["maximum_dense_case_regret_to_oracle"] == 0.124286
    assert gate["dense_improvement_count"] == 4
    assert gate["actual_joint_canary_tokens"] == 217_781.75
    assert gate["full_development_router_authorized"] is True
    assert gate["full_development_selector_authorized"] is False
    assert len(private["cases"]) == 8


def test_v212_validator_rejects_missing_event_and_bad_canonical_link(frozen_v212):
    design_root, _attempt_root, _value = frozen_v212
    predecessor = _validate_v211_authorization(design_root)
    output = _oracle_output(predecessor)
    output["cases"][0]["decisions"].pop()
    assert validate_selector_output(output, predecessor) == [
        "event_order_or_coverage"
    ]
    output = _oracle_output(predecessor)
    first = output["cases"][0]["decisions"][0]
    first["verdict"] = "duplicate"
    first["canonical_event_id"] = first["event_id"]
    assert validate_selector_output(output, predecessor) == [
        "duplicate_canonical_link"
    ]


def test_v212_runtime_lock_requires_exact_runtime_coverage(frozen_v212):
    design_root, attempt_root, value = frozen_v212
    lock = json.loads(value["runtime_lock"].read_text())
    lock["runtime_files"] = lock["runtime_files"][1:]
    mutated = attempt_root / "runtime-lock-mutated.json"
    mutated.write_text(json.dumps(lock) + "\n", encoding="utf-8")
    with pytest.raises(JudgeV5SelectionV212Error, match="runtime lock drifted"):
        verify_runtime_lock(mutated, design_root=design_root)


def test_v212_launch_receipt_is_single_and_presemantic(frozen_v212):
    _design_root, attempt_root, value = frozen_v212
    first = _freeze_launch_receipt(value)
    second = _freeze_launch_receipt(value)
    assert first == second
    receipt = json.loads(first.read_text())
    assert receipt["capacity_checkpoint_exists_before_launch"] is False
    assert receipt["sidecar_exists_before_launch"] is False
    assert receipt["output_exists_before_launch"] is False
    assert receipt["retry_count_per_turn"] == 0
    assert not (attempt_root / "terminal.json").exists()


def test_v212_rejects_nonempty_unterminaled_root(tmp_path: Path):
    design_root = tmp_path / "v211"
    freeze_v211(output_dir=design_root)
    attempt_root = tmp_path / "v212"
    attempt_root.mkdir()
    (attempt_root / "unexpected.json").write_text("{}\n", encoding="utf-8")
    with pytest.raises(JudgeV5SelectionV212Error, match="nonempty"):
        freeze_v212(output_dir=attempt_root, design_root=design_root)
