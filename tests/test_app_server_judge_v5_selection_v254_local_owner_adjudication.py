from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import app_server_judge_v5_selection_v254_local_owner_adjudication as v254


@pytest.fixture(scope="module")
def lineage() -> dict:
    return v254._validate_lineage()


@pytest.fixture(scope="module")
def bundle(lineage: dict) -> dict:
    return v254.build_local_bundle(lineage)


def _base_local_output(bundle: dict) -> dict:
    case_id = bundle["local_input"]["cases"][0]["case_id"]
    raw_case = next(
        row for row in bundle["base_output"]["cases"] if row["case_id"] == case_id
    )
    output = {"cases": [v254._restrict_raw_case(raw_case, set(bundle["closure"]))]}
    assert not v254.judge.validate_neutral_alignment_output(
        output, bundle["local_input"]
    )
    return output


def test_mechanical_closure_is_small_origin_neutral_and_complete(bundle: dict) -> None:
    assert len(bundle["base_input"]["cases"][0]["witnesses"]) == 59
    assert len(bundle["closure"]) == 19
    assert len(bundle["local_input"]["cases"][0]["witnesses"]) == 19
    assert bundle["local_input"]["side_labels_present"] is False
    assert bundle["local_input"]["system_identity_present"] is False
    assert bundle["packet"]["mechanical_subset_rule"] == (
        "owner_membership_pair_unpaired_difference_then_group_closure"
    )
    assert bundle["prompt_bytes"] < 60_000
    assert bundle["schema_bytes"] <= v254.MAX_SCHEMA_BYTES


def test_base_local_owner_reconciles_to_passing_score(bundle: dict) -> None:
    _, reconciled, score = v254.reconcile_and_score(
        bundle=bundle, output=_base_local_output(bundle)
    )
    assert reconciled["mechanically_local_adjudication_witness_count"] == 19
    assert reconciled["adjudication_call_count"] == 1
    assert score["passed"] is True
    assert score["failed_checks"] == []
    assert score["checks"]["permutation_projection_exact"] is True


def test_freeze_is_presemantic_hash_bound_and_zero_retry(tmp_path: Path) -> None:
    root = tmp_path / "v254"
    frozen = v254.freeze_v254(output_dir=root)
    lock = v254.verify_runtime_lock(frozen["runtime_lock"])
    assert lock["declared_turn_count"] == 1
    assert lock["adjudication_call_cap"] == 1
    assert lock["mechanical_local_witness_count"] == 19
    assert lock["retry_count"] == 0
    assert not (root / "launch-receipt.json").exists()
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))


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
            "input_tokens": 25_000,
            "cached_input_tokens": 2_000,
            "output_tokens": 8_000,
            "reasoning_output_tokens": 2_000,
            "total_tokens": 33_000,
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
                    "model": v254.MODEL,
                    "effort": v254.EFFORT,
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
    root = tmp_path / "v254"
    frozen = v254.freeze_v254(output_dir=root)
    fake = _FakeClient(root, _base_local_output(frozen["bundle"]))
    terminal = asyncio.run(
        v254.run_v254(output_dir=root, client_factory=lambda _path: fake)
    )
    assert fake.calls == 1
    assert terminal["state"] == "completed"
    assert terminal["usage"]["total_tokens"] == 33_000
    assert terminal["development_quality_passed"] is True
    assert terminal["development_winner_frozen"] is True
    assert terminal["holdout_authorized"] is True
    assert terminal["production_mutated"] is False


def test_runtime_lock_rejects_mutated_prompt(tmp_path: Path) -> None:
    root = tmp_path / "v254"
    frozen = v254.freeze_v254(output_dir=root)
    prompt = next(root.glob("turns/*/prompt.private.md"))
    prompt.write_text(prompt.read_text(encoding="utf-8") + "drift", encoding="utf-8")
    with pytest.raises(v254.V254LocalOwnerError):
        v254.verify_runtime_lock(frozen["runtime_lock"])
