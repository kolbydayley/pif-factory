from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import app_server_judge_v5_selection_v246_frozen_alignment as v246


@pytest.fixture(scope="module")
def bundle() -> dict:
    return v246.build_alignment_bundle(v246._validate_lineage())


def _equivalent_output(bundle: dict, turn: dict) -> dict:
    supported = [row for row in bundle["mapping"]["rows"] if row["witness_id"]]
    reference_ids = sorted(
        row["witness_id"] for row in supported if row["origin"] == "reference"
    )
    candidate_ids = sorted(
        row["witness_id"] for row in supported if row["origin"] == "candidate"
    )
    pairs = []
    groups = []
    for reference_id, candidate_id in zip(reference_ids, candidate_ids):
        witness_ids = [reference_id, candidate_id]
        groups.append(
            {
                "witness_ids": witness_ids,
                "rationale": "Synthetic full-field equivalent fixture group.",
            }
        )
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
                        "witness_evidence_ids": witness_ids,
                        "rationale": "Synthetic independent same-field fixture decision.",
                    }
                    for field in turn["value"]["checklist_field_order"]
                ],
                "rationale": "Synthetic full-field equivalent fixture pair.",
            }
        )
    unpaired = candidate_ids[len(reference_ids) :]
    groups.extend(
        {
            "witness_ids": [witness_id],
            "rationale": "Synthetic source-supported candidate-only fixture group.",
        }
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
    assert not v246.judge.validate_neutral_alignment_output(output, turn["value"])
    return output


def test_bundle_is_support_positive_origin_neutral_and_order_balanced(bundle: dict) -> None:
    assert len(bundle["pool"]["cases"]) == 1
    assert len(bundle["mapping"]["rows"]) == 58
    assert len(bundle["mapping"]["no_signal_rows"]) == 1
    assert sum(row["witness_id"] is not None for row in bundle["mapping"]["rows"]) == 57
    assert sum(
        row["origin"] == "reference" and row["support_status"] == "supported"
        for row in bundle["mapping"]["rows"]
    ) == 27
    assert sum(
        row["origin"] == "candidate" and row["support_status"] == "supported"
        for row in bundle["mapping"]["rows"]
    ) == 30
    assert sum(row["support_status"] == "unsupported" for row in bundle["mapping"]["rows"]) == 1
    base_ids = [
        row["witness_id"] for row in bundle["turns"][0]["value"]["cases"][0]["witnesses"]
    ]
    canary_ids = [
        row["witness_id"] for row in bundle["turns"][1]["value"]["cases"][0]["witnesses"]
    ]
    assert canary_ids == list(reversed(base_ids))
    assert all(turn["prompt_bytes"] <= v246.MAX_PROMPT_BYTES for turn in bundle["turns"])
    assert all(turn["schema_bytes"] <= v246.MAX_SCHEMA_BYTES for turn in bundle["turns"])


def test_score_passes_only_with_strict_cross_origin_coverage(bundle: dict) -> None:
    raw = [_equivalent_output(bundle, turn) for turn in bundle["turns"]]
    normalized = [
        v246.judge.normalize_neutral_alignment_output(value, turn["value"])
        for value, turn in zip(raw, bundle["turns"])
    ]
    score = v246.score_alignment(
        base=normalized[0], canary=normalized[1], mapping=bundle["mapping"]
    )
    assert score["passed"] is True
    assert score["metrics"]["reference_semantic_unit_count"] == 27
    assert score["metrics"]["strictly_equivalent_reference_semantic_unit_count"] == 27
    assert score["metrics"]["dense_source_supported_candidate_precision"] == 0.967742
    assert score["metrics"]["development_strict_full_field_macro_f1"] >= 0.97
    assert score["holdout_authorized"] is True


def test_score_fails_closed_on_permutation_disagreement(bundle: dict) -> None:
    raw = [_equivalent_output(bundle, turn) for turn in bundle["turns"]]
    normalized = [
        v246.judge.normalize_neutral_alignment_output(value, turn["value"])
        for value, turn in zip(raw, bundle["turns"])
    ]
    normalized[1]["cases"][0]["unpaired_witness_ids"] = []
    score = v246.score_alignment(
        base=normalized[0], canary=normalized[1], mapping=bundle["mapping"]
    )
    assert score["passed"] is False
    assert score["checks"]["permutation_projection_exact"] is False
    assert score["holdout_authorized"] is False


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
        assert (self.root / "launch-receipt.json").exists()
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
                    "model": v246.MODEL,
                    "effort": v246.EFFORT,
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


def test_freeze_is_presemantic_and_fake_run_freezes_winner(tmp_path: Path) -> None:
    root = tmp_path / "v246"
    frozen = v246.freeze_v246(output_dir=root)
    lock = v246.verify_runtime_lock(frozen["runtime_lock"])
    assert lock["declared_turn_count"] == 2
    assert lock["retry_count"] == 0
    assert not (root / "launch-receipt.json").exists()
    assert not list(root.rglob("capacity.json"))
    outputs = [_equivalent_output(frozen["bundle"], turn) for turn in frozen["bundle"]["turns"]]
    fake = _FakeClient(root, outputs)
    terminal = asyncio.run(
        v246.run_v246(output_dir=root, client_factory=lambda _policy: fake)
    )
    assert fake.calls == 2
    assert terminal["state"] == "completed"
    assert terminal["usage"]["total_tokens"] == 24_000
    assert terminal["development_quality_passed"] is True
    assert terminal["development_winner_frozen"] is True
    assert terminal["holdout_authorized"] is True
    assert terminal["overall_goal_complete"] is False
    assert terminal["production_mutated"] is False
    assert (root / "development-winner.json").is_file()


def test_runtime_lock_rejects_mutated_request(tmp_path: Path) -> None:
    root = tmp_path / "v246"
    frozen = v246.freeze_v246(output_dir=root)
    prompt = next(root.glob("turns/*/prompt.private.md"))
    prompt.write_text(prompt.read_text(encoding="utf-8") + "drift", encoding="utf-8")
    with pytest.raises(v246.V246FrozenAlignmentError):
        v246.verify_runtime_lock(frozen["runtime_lock"])
