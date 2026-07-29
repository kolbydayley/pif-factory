from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import app_server_judge_v5_selection_v277_frozen_alignment as v277


@pytest.fixture(scope="module")
def bundle() -> dict:
    return v277.build_alignment_bundle(v277._validate_lineage())


def _equivalent_output(bundle: dict, turn: dict) -> dict:
    mapping = [row for row in bundle["mapping"]["rows"] if row["witness_id"]]
    references = sorted(row["witness_id"] for row in mapping if row["origin"] == "reference")
    candidates = sorted(row["witness_id"] for row in mapping if row["origin"] == "candidate")
    groups = []
    pairs = []
    for reference_id, candidate_id in zip(references, candidates):
        ids = [reference_id, candidate_id]
        groups.append({"witness_ids": ids, "rationale": "Synthetic equivalent group."})
        pairs.append(
            {
                "witness_id_1": reference_id,
                "witness_id_2": candidate_id,
                "relation": "equivalent",
                "checklist": [
                    {
                        "field": field,
                        "decision": "same",
                        "source_evidence_spans": [],
                        "witness_evidence_ids": ids,
                        "rationale": "Synthetic same-field decision.",
                    }
                    for field in turn["value"]["checklist_field_order"]
                ],
                "rationale": "Synthetic equivalent pair.",
            }
        )
    unpaired = candidates[len(references) :]
    groups.extend(
        {"witness_ids": [witness_id], "rationale": "Synthetic candidate-only group."}
        for witness_id in unpaired
    )
    output = {
        "cases": [
            {
                "case_id": turn["value"]["cases"][0]["case_id"],
                "equivalence_groups": groups,
                "alignment_pairs": pairs,
                "unpaired_witness_ids": unpaired,
            }
        ]
    }
    assert not v277.judge.validate_neutral_alignment_output(output, turn["value"])
    return output


def test_bundle_is_origin_neutral_complete_and_balanced(bundle: dict) -> None:
    assert len(bundle["turns"]) == 2
    assert len(bundle["mapping"]["rows"]) == 59
    assert len(bundle["mapping"]["no_signal_rows"]) == 1
    assert sum(row["origin"] == "reference" for row in bundle["mapping"]["rows"]) == 27
    assert sum(row["origin"] == "candidate" for row in bundle["mapping"]["rows"]) == 32
    base_ids = [
        row["witness_id"] for row in bundle["turns"][0]["value"]["cases"][0]["witnesses"]
    ]
    canary_ids = [
        row["witness_id"] for row in bundle["turns"][1]["value"]["cases"][0]["witnesses"]
    ]
    assert len(base_ids) == 59
    assert canary_ids == list(reversed(base_ids))
    assert bundle["turns"][0]["prompt_bytes"] <= v277.MAX_PROMPT_BYTES
    assert bundle["turns"][0]["schema_bytes"] <= v277.MAX_SCHEMA_BYTES


def test_freeze_is_presemantic_and_hash_bound(tmp_path: Path) -> None:
    root = tmp_path / "v277"
    frozen = v277.freeze_v277(output_dir=root)
    lock = v277.verify_runtime_lock(frozen["runtime_lock"])
    assert lock["prompt_schema_model_or_scoring_changed"] is False
    assert lock["declared_turn_count"] == 2
    assert lock["retry_count"] == 0
    assert not (root / "launch-receipt.json").exists()
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))


class _FakeClient:
    def __init__(self, root: Path, outputs: list[dict]) -> None:
        self.root = root
        self.outputs = outputs
        self.calls = 0

    async def __aenter__(self) -> "_FakeClient":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def run_ephemeral_structured_turn(self, **kwargs: object) -> SimpleNamespace:
        assert "output_validator" not in kwargs
        assert (self.root / "launch-receipt.json").is_file()
        output = self.outputs[self.calls]
        self.calls += 1
        Path(str(kwargs["capacity_checkpoint_path"])).write_text(
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
            "input_tokens": 10_000,
            "cached_input_tokens": 500,
            "output_tokens": 2_000,
            "reasoning_output_tokens": 500,
            "total_tokens": 12_000,
        }
        Path(str(kwargs["sidecar_path"])).write_text(
            json.dumps(
                {
                    "state": "completed",
                    "status": "completed",
                    "usage_status": "measured",
                    "usage_complete": True,
                    "usage": usage,
                    "auth_type": "chatgpt",
                    "plan_type": "pro",
                    "model": v277.MODEL,
                    "effort": v277.EFFORT,
                    "error_class": None,
                }
            )
            + "\n",
            encoding="utf-8",
        )
        Path(str(kwargs["output_path"])).write_text(
            json.dumps(output) + "\n", encoding="utf-8"
        )
        return SimpleNamespace(status_ok=True, output=output)


def test_fake_run_freezes_winner_and_authorizes_holdout(tmp_path: Path) -> None:
    root = tmp_path / "v277"
    frozen = v277.freeze_v277(output_dir=root)
    outputs = [_equivalent_output(frozen["bundle"], turn) for turn in frozen["bundle"]["turns"]]
    fake = _FakeClient(root, outputs)
    terminal = asyncio.run(v277.run_v277(output_dir=root, client_factory=lambda _path: fake))
    assert fake.calls == 2
    assert terminal["state"] == "completed"
    assert terminal["usage"]["total_tokens"] == 24_000
    assert terminal["development_quality_passed"] is True
    assert terminal["development_winner_frozen"] is True
    assert terminal["holdout_authorized"] is True
    assert terminal["overall_goal_complete"] is False
    assert terminal["production_mutated"] is False


def test_runtime_lock_rejects_mutated_alignment_prompt(tmp_path: Path) -> None:
    root = tmp_path / "v277"
    frozen = v277.freeze_v277(output_dir=root)
    prompt = next(root.glob("turns/*/prompt.private.md"))
    prompt.write_text(prompt.read_text(encoding="utf-8") + "drift", encoding="utf-8")
    with pytest.raises(v277.V277FrozenAlignmentError):
        v277.verify_runtime_lock(frozen["runtime_lock"])
