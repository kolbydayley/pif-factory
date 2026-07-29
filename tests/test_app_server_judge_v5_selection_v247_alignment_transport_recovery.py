from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import (
    app_server_judge_v5_selection_v247_alignment_transport_recovery as v247,
)


@pytest.fixture(scope="module")
def bundle() -> dict:
    return v247.build_recovery_bundle()


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
    assert not v247.judge.validate_neutral_alignment_output(output, turn["value"])
    return output


def test_v246_failure_is_hash_identified_pre_thread_and_requests_are_identical(
    bundle: dict,
) -> None:
    predecessor = v247._validate_v246_failure()
    assert predecessor["terminal"]["error_message_sha256"] == v247.V246_TYPE_ERROR_SHA256
    assert predecessor["checkpoint"]["cleared_for_semantic_turn"] is True
    assert predecessor["checkpoint"]["thread_started"] is False
    assert predecessor["checkpoint"]["turn_started"] is False
    assert predecessor["checkpoint"]["sidecar_started"] is False
    for turn in bundle["turns"]:
        assert turn["prompt_bytes"] <= v247.MAX_PROMPT_BYTES
        assert turn["schema_bytes"] <= v247.MAX_SCHEMA_BYTES
        for name in ("input", "prompt", "schema"):
            source = turn["source_records"][name]
            assert source["sha256"]
            assert source["size_bytes"] > 0


def test_v247_freeze_is_presemantic_and_hash_bound(tmp_path: Path) -> None:
    root = tmp_path / "v247"
    frozen = v247.freeze_v247(output_dir=root)
    lock = v247.verify_runtime_lock(frozen["runtime_lock"])
    assert lock["semantic_prompt_schema_or_model_changed"] is False
    assert lock["unsupported_output_validator_kwarg_removed"] is True
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
                    "model": v247.MODEL,
                    "effort": v247.EFFORT,
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


def test_fake_recovery_omits_bad_kwarg_and_freezes_winner(tmp_path: Path) -> None:
    root = tmp_path / "v247"
    frozen = v247.freeze_v247(output_dir=root)
    outputs = [_equivalent_output(frozen["bundle"], turn) for turn in frozen["bundle"]["turns"]]
    fake = _FakeClient(root, outputs)
    terminal = asyncio.run(
        v247.run_v247(output_dir=root, client_factory=lambda _policy: fake)
    )
    assert fake.calls == 2
    assert terminal["state"] == "completed"
    assert terminal["usage_status"] == "complete"
    assert terminal["usage"]["total_tokens"] == 24_000
    assert terminal["development_quality_passed"] is True
    assert terminal["development_winner_frozen"] is True
    assert terminal["holdout_authorized"] is True
    assert terminal["overall_goal_complete"] is False
    assert terminal["production_mutated"] is False


def test_runtime_lock_rejects_mutated_recovery_prompt(tmp_path: Path) -> None:
    root = tmp_path / "v247"
    frozen = v247.freeze_v247(output_dir=root)
    prompt = next(root.glob("turns/*/prompt.private.md"))
    prompt.write_text(prompt.read_text(encoding="utf-8") + "drift", encoding="utf-8")
    with pytest.raises(v247.V247AlignmentTransportRecoveryError):
        v247.verify_runtime_lock(frozen["runtime_lock"])
