from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import (
    app_server_judge_v5_selection_v224_frozen_alignment_audit as v224,
)
from research_factory.util import sha256_text


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
                    "model": v224.MODEL,
                    "effort": v224.EFFORT,
                    "error_class": None,
                    "output_sha256": sha256_text(
                        json.dumps(output, sort_keys=True, separators=(",", ":"))
                    ),
                }
            )
            + "\n",
            encoding="utf-8",
        )
        output_path.write_text(json.dumps(output) + "\n", encoding="utf-8")
        return SimpleNamespace(status_ok=True, output=output)


@pytest.fixture(scope="module")
def predecessor() -> dict:
    return v224._validate_v223_success()


@pytest.fixture(scope="module")
def bundle(predecessor: dict) -> dict:
    return v224.build_v224_pool(predecessor)


def test_v224_normalizes_ids_without_semantic_pruning(bundle: dict):
    assert len(bundle["pool"]["cases"]) == v224.EXPECTED_CASE_COUNT
    assert len(bundle["mapping"]["rows"]) == v224.EXPECTED_WITNESS_COUNT
    assert len(bundle["mapping"]["empty_cases"]) == 1
    assert all(
        row["case_id"].startswith("jcase_")
        for row in bundle["mapping"]["rows"]
    )
    assert all(
        row["witness_id"].startswith("wit_")
        for row in bundle["mapping"]["rows"]
    )
    assert all(
        set(witness) == {"witness_id", "event", "support_receipt"}
        for turn in bundle["turns"]
        for case in turn["value"]["cases"]
        for witness in case["witnesses"]
    )


def test_v224_permutations_have_identical_membership_and_frozen_request_caps(
    bundle: dict,
):
    base, canary = bundle["turns"]
    assert [row["case_id"] for row in canary["value"]["cases"]] == list(
        reversed([row["case_id"] for row in base["value"]["cases"]])
    )
    assert all(
        turn["prompt_bytes"] <= v224.MAXIMUM_PROMPT_BYTES
        for turn in bundle["turns"]
    )
    assert all(
        turn["schema_bytes"] <= v224.MAXIMUM_SCHEMA_BYTES
        for turn in bundle["turns"]
    )
    assert v224.sha256_text(v224.v130.alignment_instructions_v130()) == (
        "e432a33b8e941fd38a7a8dd7ccae5e215a22e158e9d7ea35f9a3526f73a69226"
    )


def test_v224_score_fails_closed_on_permutation_disagreement(bundle: dict):
    base_turn, canary_turn = bundle["turns"]
    base_output = _equivalent_output(base_turn["value"])
    canary_output = _equivalent_output(canary_turn["value"])
    base = v224.judge.normalize_neutral_alignment_output(
        base_output, base_turn["value"]
    )
    canary = v224.judge.normalize_neutral_alignment_output(
        canary_output, canary_turn["value"]
    )
    canary["cases"][0]["unpaired_witness_ids"] = []
    score = v224.score_alignment(
        base=base,
        canary=canary,
        mapping=bundle["mapping"],
        observed_token_ratio=0.16,
    )
    assert score["passed"] is False
    assert score["checks"]["permutation_projection_exact"] is False
    assert score["holdout_authorized"] is False


def test_v224_freezes_before_launch_and_runs_two_measured_turns(tmp_path: Path):
    root = tmp_path / "attempt"
    frozen = v224.freeze_v224(output_dir=root)
    assert not (root / "launch-receipt.json").exists()
    assert not list(root.rglob("capacity.json"))
    outputs = [
        _equivalent_output(turn["value"]) for turn in frozen["bundle"]["turns"]
    ]
    client = _FakeClient(root, outputs)
    terminal = asyncio.run(
        v224.run_v224(
            output_dir=root,
            client_factory=lambda policy_path: client,
        )
    )
    assert client.calls == 2
    assert terminal["state"] == "completed"
    assert terminal["development_winner_frozen"] is True
    assert terminal["holdout_authorized"] is True
    assert terminal["usage_status"] == "complete"
    assert terminal["usage"]["total_tokens"] == 200
    assert terminal["production_mutated"] is False
    assert v224.freeze_v224(output_dir=root)["terminal"] == terminal


def test_v224_runtime_lock_rejects_mutated_record(tmp_path: Path):
    root = tmp_path / "attempt"
    frozen = v224.freeze_v224(output_dir=root)
    lock = json.loads(frozen["runtime_lock"].read_text())
    lock["runtime_files"][0]["sha256"] = "0" * 64
    mutated = root / "mutated-runtime-lock.json"
    mutated.write_text(json.dumps(lock) + "\n", encoding="utf-8")
    with pytest.raises(v224.JudgeV5SelectionV224Error):
        v224.verify_runtime_lock(mutated)
