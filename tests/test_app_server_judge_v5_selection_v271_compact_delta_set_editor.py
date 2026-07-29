from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import app_server_judge_v5_selection_v271_compact_delta_set_editor as v271


@pytest.fixture(scope="module")
def lineage() -> dict:
    return v271._validate_lineage()


@pytest.fixture(scope="module")
def turn(lineage: dict) -> dict:
    return v271.prepare_turn(lineage)


def _keep_output(turn: dict) -> dict:
    base_by_id = {
        str(row["segment_id"]): row for row in turn["base_raw"]["segments"]
    }
    segments = []
    for segment_id in turn["segment_ids"]:
        base = base_by_id[segment_id]
        ids = list(turn["proposal_ids"][segment_id])
        segments.append(
            {
                "segment_id": segment_id,
                "final_status": base["status"],
                "segment_source_context": base["segment_source_context"],
                "no_signal_reason": base["no_signal_reason"],
                "coverage_audit": {
                    "all_source_units_reviewed": True,
                    "all_base_events_reviewed": True,
                    "unresolved_count": 0,
                },
                "proposal_receipts": [
                    {
                        "proposal_id": proposal_id,
                        "action": "keep",
                        "final_event_id": proposal_id,
                    }
                    for proposal_id in ids
                ],
                "replacement_events": [],
            }
        )
    output = {"episode_id": turn["episode_id"], "segments": segments}
    v271._validate_schema(turn["schema"], output, path="$")
    return output


def test_compact_packet_and_cost_contract(turn: dict) -> None:
    counts = [len(turn["proposal_ids"][item]) for item in turn["segment_ids"]]
    assert counts == [32, 1]
    assert turn["prompt_bytes"] <= v271.MAX_PROMPT_BYTES
    assert turn["base_bytes"] <= v271.MAX_BASE_BYTES
    assert turn["schema_bytes"] <= v271.MAX_SCHEMA_BYTES
    assert v271.ADOPTED_V249_TOKENS + v271.MAX_TOTAL_TOKENS == 72_474
    assert v271._production_ratio(v271.MAX_COMBINED_TOKENS)[1] < 0.28
    for segment in turn["private_input"]["segments"]:
        for event in segment["base_events"]:
            assert "evidence" not in event
            assert all(value not in ("", [], None) for value in event.values())


def test_keep_projection_reproduces_all_base_events(turn: dict) -> None:
    normalized, _, diagnostics, applicability, coverage = v271.project_output(
        _keep_output(turn), turn
    )
    assert [len(row["events"]) for row in normalized["segments"]] == [32, 1]
    assert [row["event_count"] for row in diagnostics] == [32, 1]
    assert applicability["deterministic_projection_only"] is True
    assert coverage["all_base_events_reviewed_exactly_once"] is True
    assert sum(row["replacement_event_count"] for row in coverage["segments"]) == 0


def test_projection_rejects_receipt_coverage_drift(turn: dict) -> None:
    output = _keep_output(turn)
    receipts = output["segments"][0]["proposal_receipts"]
    receipts[1]["proposal_id"] = receipts[0]["proposal_id"]
    with pytest.raises(v271.V271OutputContractError):
        v271.project_output(output, turn)


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
            "input_tokens": 16_000,
            "cached_input_tokens": 1_000,
            "output_tokens": 3_000,
            "reasoning_output_tokens": 500,
            "total_tokens": 19_500,
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
                    "model": v271.MODEL,
                    "effort": v271.EFFORT,
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


def test_freeze_and_fake_run_are_single_turn_and_holdout_closed(tmp_path: Path) -> None:
    root = tmp_path / "v271"
    frozen = v271.freeze_v271(output_dir=root)
    lock = v271.verify_runtime_lock(frozen["runtime_lock"])
    assert lock["declared_turn_count"] == 1
    assert lock["retry_count"] == 0
    assert lock["adopted_v249_tokens"] == 44_474
    assert lock["max_total_tokens"] == 28_000
    assert lock["max_combined_tokens"] == 72_474
    assert not (root / "launch-receipt.json").exists()
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    fake = _FakeClient(root, _keep_output(frozen["turn"]))
    terminal = asyncio.run(
        v271.run_v271(output_dir=root, client_factory=lambda _path: fake)
    )
    assert fake.calls == 1
    assert terminal["terminal_reason"] == (
        "v271_compact_delta_set_editor_structural_cost_gate_passed"
    )
    assert terminal["support_alignment_authorized"] is True
    assert terminal["holdout_authorized"] is False
    assert terminal["production_mutated"] is False
