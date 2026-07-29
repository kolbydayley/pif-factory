from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import (
    app_server_judge_v5_selection_v223_frozen_support_audit as v223,
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


class _FakeClient:
    def __init__(self, root: Path, output: dict):
        self.root = root
        self.output = output
        self.calls = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return None

    async def run_ephemeral_structured_turn(self, **kwargs):
        assert (self.root / "launch-receipt.json").exists()
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
                    "model": v223.MODEL,
                    "effort": v223.EFFORT,
                    "error_class": None,
                }
            )
            + "\n",
            encoding="utf-8",
        )
        output_path.write_text(
            json.dumps(self.output) + "\n", encoding="utf-8"
        )
        return SimpleNamespace(status_ok=True, output=self.output)


@pytest.fixture(scope="module")
def predecessor() -> dict:
    return v223._validate_v222_authorization()


@pytest.fixture(scope="module")
def pool(predecessor: dict) -> dict:
    return v223.build_residual_pool(predecessor)


def test_v223_reconstructs_overlapping_windows_without_semantic_rules():
    windows = [
        {
            "extract_start": 0,
            "extract_end": 7,
            "extract_text": "one two",
        },
        {
            "extract_start": 4,
            "extract_end": 13,
            "extract_text": "two three",
        },
    ]
    assert v223.reconstruct_segment_text(windows) == "one two three"


def test_v223_pool_covers_all_observed_witnesses_without_origin_labels(
    pool: dict,
):
    assert len(pool["cases"]) == 3
    assert len(pool["units"]) == 79
    assert len(pool["origin_rows"]) == 79
    assert sum(row["origin"] == "reference" for row in pool["origin_rows"]) == 50
    assert sum(row["origin"] == "candidate" for row in pool["origin_rows"]) == 29
    assert pool["prompt_bytes"] <= v223.MAXIMUM_PROMPT_BYTES
    assert pool["schema_bytes"] <= v223.MAXIMUM_SCHEMA_BYTES
    assert all(
        set(row) == {"case_id", "witness_id", "proposition", "source_excerpt"}
        for row in pool["support_value"]["units"]
    )
    compact = v223.v175._compact_packet(pool["units"])
    assert all(
        set(witness) == {"witness_id", "proposition"}
        for case in compact["cases"]
        for witness in case["witnesses"]
    )


def test_v223_support_projection_authorizes_alignment_for_supported_pool(
    pool: dict,
):
    output = _supported_output(pool["support_value"])
    audit, private = v223.score_support_output(
        output=output,
        support_value=pool["support_value"],
        origin_rows=pool["origin_rows"],
    )
    assert audit["witness_count"] == 79
    assert audit["exact_evidence_validated"] is True
    assert audit["alignment_audit_authorized"] is True
    assert audit["no_signal_candidate_event_support_status"] == "supported"
    assert len(private["rows"]) == 79


def test_v223_freezes_before_launch_and_runs_once_with_measured_usage(
    tmp_path: Path,
):
    root = tmp_path / "attempt"
    frozen = v223.freeze_v223(output_dir=root)
    assert not (root / "launch-receipt.json").exists()
    assert not list(root.rglob("capacity.json"))
    output = _supported_output(frozen["support_value"])
    client = _FakeClient(root, output)
    terminal = asyncio.run(
        v223.run_v223(
            output_dir=root,
            client_factory=lambda policy_path: client,
        )
    )
    assert client.calls == 1
    assert terminal["state"] == "completed"
    assert terminal["alignment_audit_authorized"] is True
    assert terminal["usage_status"] == "complete"
    assert terminal["accounting_complete"] is True
    assert terminal["usage"]["total_tokens"] == 100
    assert terminal["development_winner_frozen"] is False
    assert terminal["holdout_authorized"] is False
    assert terminal["production_mutated"] is False
    assert v223.freeze_v223(output_dir=root)["terminal"] == terminal


def test_v223_runtime_lock_rejects_mutated_record(tmp_path: Path):
    root = tmp_path / "attempt"
    frozen = v223.freeze_v223(output_dir=root)
    lock = json.loads(frozen["runtime_lock"].read_text())
    lock["runtime_files"][0]["sha256"] = "0" * 64
    mutated = root / "mutated-runtime-lock.json"
    mutated.write_text(json.dumps(lock) + "\n", encoding="utf-8")
    with pytest.raises(v223.JudgeV5SelectionV223Error):
        v223.verify_runtime_lock(mutated)
