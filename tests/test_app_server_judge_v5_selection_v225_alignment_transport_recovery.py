from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import (
    app_server_judge_v5_selection_v225_alignment_transport_recovery as v225,
)


def _equivalent_output(value: dict) -> dict:
    cases = []
    for case in value["cases"]:
        witness_ids = sorted(row["witness_id"] for row in case["witnesses"])
        pairs = []
        for index in range(0, len(witness_ids) - 1, 2):
            first, second = witness_ids[index : index + 2]
            pairs.append(
                {
                    "witness_id_1": first,
                    "witness_id_2": second,
                    "relation": "equivalent",
                    "checklist": [
                        {
                            "field": field,
                            "decision": "same",
                            "source_evidence_spans": [],
                            "witness_evidence_ids": [first, second],
                            "rationale": "Synthetic equivalent fixture decision.",
                        }
                        for field in value["checklist_field_order"]
                    ],
                    "rationale": "Synthetic equivalent fixture pair.",
                }
            )
        used = {
            item
            for pair in pairs
            for item in (pair["witness_id_1"], pair["witness_id_2"])
        }
        cases.append(
            {
                "case_id": case["case_id"],
                "equivalence_groups": [
                    {
                        "witness_ids": witness_ids,
                        "rationale": "Synthetic one-group fixture.",
                    }
                ],
                "alignment_pairs": pairs,
                "unpaired_witness_ids": sorted(set(witness_ids) - used),
            }
        )
    return {"cases": cases}


class _FakeClient:
    def __init__(self, root: Path, outputs: list[dict]):
        self.root = root
        self.outputs = outputs
        self.calls = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return None

    async def run_ephemeral_structured_turn(self, **kwargs):
        assert "output_validator" not in kwargs
        assert (self.root / "launch-receipt.json").exists()
        output = self.outputs[self.calls]
        self.calls += 1
        capacity_path = Path(kwargs["capacity_checkpoint_path"])
        sidecar_path = Path(kwargs["sidecar_path"])
        output_path = Path(kwargs["output_path"])
        capacity_path.write_text(
            json.dumps(
                {
                    "cleared_for_semantic_turn": True,
                    "managed_chatgpt_auth_verified": True,
                    "rate_limit_reached_type": None,
                }
            )
            + "\n",
            encoding="utf-8",
        )
        usage = {
            "input_tokens": 80,
            "cached_input_tokens": 0,
            "output_tokens": 20,
            "reasoning_output_tokens": 10,
            "total_tokens": 100,
        }
        sidecar_path.write_text(
            json.dumps(
                {
                    "state": "completed",
                    "status": "completed",
                    "usage_status": "measured",
                    "usage_complete": True,
                    "usage": usage,
                    "auth_type": "chatgpt",
                    "plan_type": "pro",
                    "model": v225.MODEL,
                    "effort": v225.EFFORT,
                    "error_class": None,
                }
            )
            + "\n",
            encoding="utf-8",
        )
        output_path.write_text(json.dumps(output) + "\n", encoding="utf-8")
        return SimpleNamespace(status_ok=True, output=output)


def test_v225_proves_v224_failed_before_semantic_thread_or_turn():
    predecessor = v225._validate_v224_presemantic_failure()
    assert predecessor["reproduced"]["error_message_sha256"] == (
        v225.EXPECTED_V224_ERROR_SHA256
    )
    assert predecessor["checkpoint"]["thread_started"] is False
    assert predecessor["checkpoint"]["turn_started"] is False
    assert predecessor["checkpoint"]["sidecar_started"] is False


def test_v225_freezes_exact_v224_requests_before_launch(tmp_path: Path):
    root = tmp_path / "attempt"
    frozen = v225.freeze_v225(output_dir=root)
    assert not (root / "launch-receipt.json").exists()
    assert not list(root.rglob("capacity.json"))
    assert frozen["spec"]["prompt_schema_or_instructions_changed"] is False
    assert frozen["spec"]["v224_semantic_usage_resolution"] == (
        "known_zero_presemantic"
    )
    assert len(frozen["spec"]["frozen_requests"]) == 6


def test_v225_runs_without_unsupported_keyword_and_measures_both_turns(
    tmp_path: Path,
):
    root = tmp_path / "attempt"
    frozen = v225.freeze_v225(output_dir=root)
    outputs = [
        _equivalent_output(turn["value"])
        for turn in frozen["bundle"]["turns"]
    ]
    client = _FakeClient(root, outputs)
    terminal = asyncio.run(
        v225.run_v225(
            output_dir=root,
            client_factory=lambda policy_path: client,
        )
    )
    assert client.calls == 2
    assert terminal["state"] == "completed"
    assert terminal["v224_semantic_usage_resolution"] == (
        "known_zero_presemantic"
    )
    assert terminal["development_winner_frozen"] is True
    assert terminal["holdout_authorized"] is True
    assert terminal["usage"]["total_tokens"] == 200
    assert terminal["production_mutated"] is False


def test_v225_runtime_lock_rejects_mutated_predecessor_record(tmp_path: Path):
    root = tmp_path / "attempt"
    frozen = v225.freeze_v225(output_dir=root)
    lock = json.loads(frozen["runtime_lock"].read_text())
    lock["v224_attempt"][0]["sha256"] = "0" * 64
    mutated = root / "mutated-runtime-lock.json"
    mutated.write_text(json.dumps(lock) + "\n", encoding="utf-8")
    with pytest.raises(v225.JudgeV5SelectionV225Error):
        v225.verify_runtime_lock(mutated)
