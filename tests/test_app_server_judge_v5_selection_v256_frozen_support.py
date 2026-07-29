from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import app_server_judge_v5_selection_v256_frozen_support as v256


@pytest.fixture(scope="module")
def pool() -> dict:
    return v256.build_support_pool(v256._validate_lineage())


def _output(pool: dict, *, residual_status: str = "supported") -> dict:
    origin = {row["witness_id"]: row for row in pool["origin_rows"]}
    rows = []
    for row in pool["support_value"]["units"]:
        metadata = origin[row["witness_id"]]
        status = "supported"
        if metadata["density_stratum"] == "no_signal" and metadata["origin"] == "candidate":
            status = residual_status
        rows.append(
            {
                "case_id": row["case_id"],
                "witness_id": row["witness_id"],
                "support_status": status,
                "source_evidence_spans": [row["source_excerpt"][:80]],
                "rationale": "The proposition is evaluated directly against the source.",
            }
        )
    return {"units": rows}


def test_pool_is_complete_origin_neutral_and_protocol_frozen(pool: dict) -> None:
    assert len(pool["cases"]) == 2
    assert len(pool["units"]) == 59
    assert sum(row["origin"] == "reference" for row in pool["origin_rows"]) == 27
    assert sum(row["origin"] == "candidate" for row in pool["origin_rows"]) == 32
    assert all(
        set(row) == {"case_id", "witness_id", "proposition", "source_excerpt"}
        for row in pool["support_value"]["units"]
    )
    assert pool["prompt_bytes"] <= v256.MAX_PROMPT_BYTES
    assert pool["schema_bytes"] <= v256.MAX_SCHEMA_BYTES
    assert (
        v256._lineage_paths()["v223_instructions"].read_text(encoding="utf-8")
        == v256.v250.v245.v143.support_base_instructions_v143()
    )


def test_scoring_preserves_one_sided_residual_support(pool: dict) -> None:
    audit, private = v256.v250.v245.v223.score_support_output(
        output=_output(pool),
        support_value=pool["support_value"],
        origin_rows=pool["origin_rows"],
    )
    assert audit["witness_count"] == 59
    assert audit["exact_evidence_validated"] is True
    assert audit["alignment_audit_authorized"] is True
    assert audit["no_signal_candidate_event_support_status"] == "supported"
    assert len(private["rows"]) == 59


def test_freeze_is_presemantic_and_hash_bound(tmp_path: Path) -> None:
    root = tmp_path / "v256"
    frozen = v256.freeze_v256(output_dir=root)
    lock = v256.verify_runtime_lock(frozen["runtime_lock"])
    assert lock["declared_turn_count"] == 1
    assert lock["retry_count"] == 0
    assert frozen["spec"]["witness_count"] == 59
    assert not (root / "launch-receipt.json").exists()
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))


class _FakeClient:
    def __init__(self, output: dict) -> None:
        self.output = output
        self.calls = 0

    async def __aenter__(self) -> "_FakeClient":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def run_ephemeral_structured_turn(self, **kwargs: object) -> SimpleNamespace:
        self.calls += 1
        Path(str(kwargs["capacity_checkpoint_path"])).write_text(
            json.dumps(
                {
                    "cleared_for_semantic_turn": True,
                    "managed_chatgpt_auth_verified": True,
                    "rate_limit_reached_type": None,
                }
            ),
            encoding="utf-8",
        )
        usage = {
            "input_tokens": 10_000,
            "cached_input_tokens": 0,
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
                    "model": v256.MODEL,
                    "effort": v256.EFFORT,
                    "error_class": None,
                }
            ),
            encoding="utf-8",
        )
        Path(str(kwargs["output_path"])).write_text(
            json.dumps(self.output), encoding="utf-8"
        )
        return SimpleNamespace(status_ok=True, output=self.output)


def test_fake_run_is_one_measured_turn_and_authorizes_alignment(
    tmp_path: Path, pool: dict
) -> None:
    fake = _FakeClient(_output(pool))
    terminal = asyncio.run(
        v256.run_v256(
            output_dir=tmp_path / "v256-run", client_factory=lambda _path: fake
        )
    )
    assert fake.calls == 1
    assert terminal["state"] == "completed"
    assert terminal["usage"]["total_tokens"] == 12_000
    assert terminal["alignment_audit_authorized"] is True
    assert terminal["holdout_authorized"] is False
    assert terminal["production_mutated"] is False


def test_runtime_lock_rejects_mutated_support_prompt(tmp_path: Path) -> None:
    root = tmp_path / "v256"
    frozen = v256.freeze_v256(output_dir=root)
    prompt = root / "support-prompt.private.md"
    prompt.write_text(prompt.read_text(encoding="utf-8") + "drift", encoding="utf-8")
    with pytest.raises(v256.V256FrozenSupportError):
        v256.verify_runtime_lock(frozen["runtime_lock"])
