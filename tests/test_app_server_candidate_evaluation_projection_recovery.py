from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import app_server_candidate_evaluation_projection_recovery as recovery
from research_factory import app_server_capacity_reserve as reserve
from research_factory import app_server_judge_v5 as judge


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


class FakeAdjudicationClient:
    def __init__(self) -> None:
        self.calls = 0

    async def __aenter__(self) -> "FakeAdjudicationClient":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def run_ephemeral_structured_turn(self, **kwargs: object) -> SimpleNamespace:
        self.calls += 1
        sidecar = Path(str(kwargs["sidecar_path"]))
        output = Path(str(kwargs["output_path"]))
        capacity = Path(str(kwargs["capacity_checkpoint_path"]))
        value = _load(sidecar.parent / "input.private.json")["alignment_input"]
        case_ids = {str(row["case_id"]) for row in value["cases"]}
        base = _load(
            recovery.DEFAULT_PREDECESSOR_ROOT
            / "alignment"
            / "turns"
            / "neutral-alignment-base"
            / "output.private.json"
        )
        result = {
            "cases": [row for row in base["cases"] if str(row["case_id"]) in case_ids]
        }
        assert judge.validate_neutral_alignment_output(result, value) == []
        capacity.write_text(
            json.dumps(
                {
                    "schema_version": reserve.RESERVE_CAPACITY_CHECKPOINT_VERSION,
                    "managed_chatgpt_auth_verified": True,
                    "cleared_for_semantic_turn": True,
                    "rate_limit_reached_type": None,
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        output.write_text(json.dumps(result, sort_keys=True), encoding="utf-8")
        sidecar.write_text(
            json.dumps(
                {
                    "state": "completed",
                    "status": "completed",
                    "usage_status": "measured",
                    "usage_complete": True,
                    "auth_type": "chatgpt",
                    "model": kwargs["model"],
                    "effort": kwargs["effort"],
                    "usage": {
                        "input_tokens": 50,
                        "cached_input_tokens": 0,
                        "output_tokens": 40,
                        "reasoning_output_tokens": 10,
                        "total_tokens": 100,
                    },
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        return SimpleNamespace(output=result)


def test_projection_removes_only_two_duplicate_exact_spans() -> None:
    predecessor = recovery._validate_predecessor(  # noqa: SLF001
        recovery.DEFAULT_PREDECESSOR_ROOT
    )
    audit = predecessor["canary_audit"]
    assert audit["removed_exact_duplicate_span_count"] == 2
    assert audit["changed_checklist_row_count"] == 2
    assert audit["semantic_fields_changed"] is False
    assert audit["witness_assignments_changed"] is False
    turns = {
        row["permutation"]: row for row in predecessor["alignment_bundle"]["turns"]
    }
    assert (
        judge.validate_neutral_alignment_output(
            predecessor["canary_projected"],
            turns["balanced_canary"]["value"],
        )
        == []
    )


def test_recovery_freeze_is_zero_turn_and_directly_bound(tmp_path: Path) -> None:
    frozen = recovery.freeze_recovery(output_dir=tmp_path / "recovery")
    assert frozen["adjudication_bundle"]["adjudication_required"] is True
    assert not (frozen["root"] / "launch-receipt.json").exists()
    assert not frozen["turn"]["capacity"].exists()
    assert not frozen["turn"]["sidecar"].exists()
    assert recovery.verify_recovery_lock(frozen["runtime_lock"]) == frozen["runtime_lock"]


def test_recovery_runs_one_adjudication_without_replay(tmp_path: Path) -> None:
    fake = FakeAdjudicationClient()
    root = tmp_path / "recovery"
    terminal = asyncio.run(
        recovery.run_recovery(
            output_dir=root,
            client_factory=lambda _policy: fake,
        )
    )
    assert fake.calls == 1
    assert terminal["terminal_reason"] == "candidate_semantic_quality_gate_not_passed"
    assert terminal["accounting_complete"] is True
    assert terminal["measured_turn_count"] == 4
    assert terminal["usage"]["total_tokens"] == 214_140
    assert terminal["holdout_authorized"] is False
    assert terminal["production_mutated"] is False
    score = _load(root / "alignment-score.json")
    assert score["checks"]["observable_disagreement_adjudication_complete"] is True
    prior_calls = fake.calls
    assert (
        asyncio.run(
            recovery.run_recovery(
                output_dir=root,
                client_factory=lambda _policy: fake,
            )
        )
        == terminal
    )
    assert fake.calls == prior_calls


def test_recovery_lock_rejects_projected_output_mutation(tmp_path: Path) -> None:
    frozen = recovery.freeze_recovery(output_dir=tmp_path / "recovery")
    frozen["canary"].write_text(
        frozen["canary"].read_text(encoding="utf-8") + "\n",
        encoding="utf-8",
    )
    with pytest.raises(recovery.CandidateProjectionRecoveryError):
        recovery.verify_recovery_lock(frozen["runtime_lock"])
