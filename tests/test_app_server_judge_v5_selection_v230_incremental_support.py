from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

from research_factory import (
    app_server_judge_v5_selection_v230_incremental_support as v230,
)


def _supported_output(value: dict) -> dict:
    return {
        "units": [
            {
                "case_id": row["case_id"],
                "witness_id": row["witness_id"],
                "support_status": "supported",
                "source_evidence_spans": [row["source_excerpt"][:80]],
                "rationale": "The source directly supports the proposition.",
            }
            for row in value["units"]
        ]
    }


def test_pool_contains_only_two_new_side_free_witnesses():
    lineage = v230._validate_lineage()
    pool = v230._build_pool(lineage)
    assert len(pool["units"]) == 2
    assert len(pool["witnesses"]) == 2
    assert len(pool["origins"]) == 2
    assert all(row["origin"] == "candidate" for row in pool["origins"])
    assert all(
        set(row) == {"case_id", "witness_id", "proposition", "source_excerpt"}
        for row in pool["support_value"]["units"]
    )
    assert "origin" not in pool["prompt"]
    assert (
        v230.sha256_text(v230.v223.v143.support_base_instructions_v143())
        == v230.FROZEN_SUPPORT_INSTRUCTION_HASH
    )


def test_support_score_requires_both_new_witnesses_supported():
    pool = v230._build_pool(v230._validate_lineage())
    output = _supported_output(pool["support_value"])
    audit, private = v230._score_support(output, pool)
    assert audit["passed"] is True
    assert audit["alignment_audit_authorized"] is True
    assert audit["support_status_counts"] == {"supported": 2}
    assert len(private["rows"]) == 2
    output["units"][0]["support_status"] = "unsupported"
    output["units"][0]["rationale"] = (
        "The cited source contradicts or does not entail the proposition."
    )
    audit, _ = v230._score_support(output, pool)
    assert audit["passed"] is False
    assert audit["alignment_audit_authorized"] is False


def test_freeze_is_one_frozen_support_turn_and_keeps_holdout_closed(tmp_path: Path):
    root = tmp_path / "development-selection-v5_4-v230-incremental-support"
    frozen = v230.freeze_v230(output_dir=root)
    spec = frozen["spec"]
    assert spec["declared_turn_count"] == 1
    assert spec["retry_count"] == 0
    assert spec["model"] == "gpt-5.6-sol"
    assert spec["effort"] == "high"
    assert spec["witness_count"] == 2
    assert spec["side_labels_in_model_input"] is False
    assert spec["v229_replayed"] is False
    assert spec["holdout_authorized"] is False
    assert spec["production_mutation_allowed"] is False
    assert not (root / "launch-receipt.json").exists()
    assert not list(root.rglob("capacity.json"))
    v230.verify_runtime_lock(root / "runtime-lock.json")


def test_execution_runs_once_with_measured_usage(tmp_path: Path):
    root = tmp_path / "development-selection-v5_4-v230-incremental-support"
    frozen = v230.freeze_v230(output_dir=root)
    output = _supported_output(frozen["support_value"])
    calls = []

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return None

        async def run_ephemeral_structured_turn(self, **kwargs):
            calls.append(kwargs)
            kwargs["capacity_checkpoint_path"].write_text("{}\n")
            kwargs["output_path"].write_text(json.dumps(output) + "\n")
            kwargs["sidecar_path"].write_text(
                json.dumps(
                    {
                        "state": "completed",
                        "status": "completed",
                        "usage_status": "measured",
                        "usage_complete": True,
                        "auth_type": "chatgpt",
                        "plan_type": "pro",
                        "model": v230.MODEL,
                        "effort": v230.EFFORT,
                        "error_class": None,
                        "usage": {
                            "input_tokens": 1_000,
                            "cached_input_tokens": 0,
                            "output_tokens": 100,
                            "reasoning_output_tokens": 20,
                            "total_tokens": 1_100,
                        },
                    }
                )
                + "\n"
            )
            return SimpleNamespace(status_ok=True, output=output)

    terminal = asyncio.run(
        v230.run_v230(
            output_dir=root,
            timeout_seconds=30,
            client_factory=lambda _policy: FakeClient(),
        )
    )
    assert len(calls) == 1
    assert calls[0]["model"] == "gpt-5.6-sol"
    assert calls[0]["effort"] == "high"
    assert terminal["support_passed"] is True
    assert terminal["alignment_authorized"] is True
    assert terminal["holdout_authorized"] is False
    repeated = asyncio.run(
        v230.run_v230(
            output_dir=root,
            timeout_seconds=30,
            client_factory=lambda _policy: FakeClient(),
        )
    )
    assert repeated == terminal
    assert len(calls) == 1
