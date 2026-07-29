from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import app_server_judge_v5_selection_v252_capped_adjudication as v252


@pytest.fixture(scope="module")
def lineage() -> dict:
    return v252._validate_lineage()


@pytest.fixture(scope="module")
def bundle(lineage: dict) -> dict:
    return v252.build_adjudication_bundle(lineage)


def _base_owner_output(bundle: dict) -> dict:
    case_id = bundle["alignment_input"]["cases"][0]["case_id"]
    base = next(
        row for row in bundle["base_output"]["cases"] if row["case_id"] == case_id
    )
    output = {"cases": [base]}
    assert not v252.judge.validate_neutral_alignment_output(
        output, bundle["alignment_input"]
    )
    return output


def test_bundle_is_one_capped_origin_neutral_disagreement(bundle: dict) -> None:
    assert len(bundle["packet"]["cases"]) == 1
    assert bundle["packet"]["call_cap"] == 1
    assert bundle["packet"]["candidate_order_has_no_vote_meaning"] is True
    assert bundle["alignment_input"]["side_labels_present"] is False
    assert bundle["alignment_input"]["system_identity_present"] is False
    assert len(bundle["alignment_input"]["cases"][0]["witnesses"]) == 59
    assert bundle["prompt_bytes"] <= v252.MAX_PROMPT_BYTES
    assert bundle["schema_bytes"] <= v252.MAX_SCHEMA_BYTES
    assert (
        v252.sha256_text(v252.adjudication_instructions())
        == v252.FROZEN_V153_INSTRUCTION_HASH
    )


def test_reconciled_owner_clears_only_permutation_gate(bundle: dict) -> None:
    _, reconciled, score = v252.reconcile_and_score(
        bundle=bundle, output=_base_owner_output(bundle)
    )
    assert reconciled["observable_disagreement_case_count"] == 1
    assert reconciled["adjudication_call_count"] == 1
    assert reconciled["majority_voting_used"] is False
    assert score["passed"] is True
    assert score["failed_checks"] == []
    assert score["checks"]["permutation_projection_exact"] is True
    assert score["checks"]["strict_full_field_macro_noninferior_margin_0_03"] is True


def test_freeze_is_presemantic_hash_bound_and_zero_retry(tmp_path: Path) -> None:
    root = tmp_path / "v252"
    frozen = v252.freeze_v252(output_dir=root)
    lock = v252.verify_runtime_lock(frozen["runtime_lock"])
    assert lock["declared_turn_count"] == 1
    assert lock["adjudication_call_cap"] == 1
    assert lock["retry_count"] == 0
    assert lock["frozen_adjudication_instruction_hash"] == v252.FROZEN_V153_INSTRUCTION_HASH
    assert not (root / "launch-receipt.json").exists()
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))


class _FakeClient:
    def __init__(self, root: Path, output: dict) -> None:
        self.root = root
        self.output = output
        self.calls = 0

    async def __aenter__(self) -> "_FakeClient":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def run_ephemeral_structured_turn(self, **kwargs: object) -> SimpleNamespace:
        assert self.calls == 0
        self.calls += 1
        assert (self.root / "launch-receipt.json").is_file()
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
            "input_tokens": 20_000,
            "cached_input_tokens": 2_000,
            "output_tokens": 5_000,
            "reasoning_output_tokens": 1_000,
            "total_tokens": 25_000,
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
                    "model": v252.MODEL,
                    "effort": v252.EFFORT,
                    "error_class": None,
                }
            )
            + "\n",
            encoding="utf-8",
        )
        Path(str(kwargs["output_path"])).write_text(
            json.dumps(self.output) + "\n", encoding="utf-8"
        )
        return SimpleNamespace(status_ok=True, output=self.output)


def test_fake_run_freezes_winner_and_authorizes_holdout(tmp_path: Path) -> None:
    root = tmp_path / "v252"
    frozen = v252.freeze_v252(output_dir=root)
    fake = _FakeClient(root, _base_owner_output(frozen["bundle"]))
    terminal = asyncio.run(
        v252.run_v252(output_dir=root, client_factory=lambda _path: fake)
    )
    assert fake.calls == 1
    assert terminal["state"] == "completed"
    assert terminal["usage"]["total_tokens"] == 25_000
    assert terminal["development_quality_passed"] is True
    assert terminal["development_winner_frozen"] is True
    assert terminal["holdout_authorized"] is True
    assert terminal["overall_goal_complete"] is False
    assert terminal["production_mutated"] is False


def test_runtime_lock_rejects_mutated_prompt(tmp_path: Path) -> None:
    root = tmp_path / "v252"
    frozen = v252.freeze_v252(output_dir=root)
    prompt = next(root.glob("turns/*/prompt.private.md"))
    prompt.write_text(prompt.read_text(encoding="utf-8") + "drift", encoding="utf-8")
    with pytest.raises(v252.V252CappedAdjudicationError):
        v252.verify_runtime_lock(frozen["runtime_lock"])
