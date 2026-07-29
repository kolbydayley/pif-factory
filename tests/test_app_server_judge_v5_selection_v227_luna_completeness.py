from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import (
    app_server_judge_v5_selection_v227_luna_completeness as v227,
)


def _synthetic_event(turn: dict, segment_index: int) -> dict:
    packet = turn["private_input"]["prompt_packets"][segment_index]
    templates = [
        event
        for row in turn["private_input"]["normalization_segments"]
        for event in row["existing_events"]
    ]
    event = copy.deepcopy(templates[0])
    window = packet["windows"][0]
    text = window["extract_text"]
    event["window_id"] = window.get("window_id", window.get("chunk_index"))
    event["evidence"] = text[: min(160, len(text))]
    event["claim_text"] = (
        event.get("claim_text", "") + " Synthetic distinct fixture claim."
    )
    event["metric_value"] = ""
    event["metric_unit"] = ""
    event["metric_comparator"] = ""
    event["metric_raw_text"] = ""
    return event


def _all_keys(value) -> set[str]:
    if isinstance(value, dict):
        return set(value) | {
            key
            for item in value.values()
            for key in _all_keys(item)
        }
    if isinstance(value, list):
        return {key for item in value for key in _all_keys(item)}
    return set()


def _gap_output(turn: dict) -> dict:
    rows = []
    for index, segment_id in enumerate(turn["segment_ids"]):
        rows.append(
            {
                "segment_id": segment_id,
                "status": "coded",
                "segment_source_context": {
                    "kind": "substantive_dialogue",
                    "confidence": 1.0,
                    "rationale": "Synthetic fixture context.",
                },
                "no_signal_reason": "",
                "events": [_synthetic_event(turn, index)],
            }
        )
    return {"episode_id": turn["episode_id"], "segments": rows}


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
        assert "output_validator" not in kwargs
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
                    "model": v227.MODEL,
                    "effort": v227.EFFORT,
                    "error_class": None,
                }
            )
            + "\n",
            encoding="utf-8",
        )
        output_path.write_text(json.dumps(output) + "\n", encoding="utf-8")
        return SimpleNamespace(status_ok=True, output=output)


def test_v227_builds_two_blind_luna_gap_turns():
    predecessor = v227._validate_predecessor()
    bundle = v227._load_prepared_turns(predecessor)
    assert bundle["episode_count"] == 2
    assert bundle["case_count"] == 4
    assert len(bundle["turns"]) == 2
    for turn in bundle["turns"]:
        assert len(turn["segment_ids"]) == 2
        assert turn["prompt_bytes"] <= v227.MAXIMUM_PROMPT_BYTES_PER_TURN
        assert turn["schema_bytes"] <= v227.MAXIMUM_SCHEMA_BYTES_PER_TURN
        prompt_input = turn["private_input"]["prompt_packets"]
        keys = _all_keys(prompt_input)
        assert "density_stratum" not in keys
        assert "golden_event_count" not in keys
        assert "reference_events" not in keys


def test_v227_freezes_before_launch_without_semantic_artifacts(tmp_path: Path):
    root = tmp_path / "v227"
    frozen = v227.freeze_v227(output_dir=root)
    assert frozen["spec"]["model"] == "gpt-5.6-luna"
    assert frozen["spec"]["reasoning_effort"] == "low"
    assert frozen["spec"]["extraction_replay_allowed"] is False
    assert len(frozen["spec"]["frozen_turns"]) == 2
    assert not (root / "launch-receipt.json").exists()
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))


def test_v227_fake_run_measures_both_turns_and_authorizes_support(
    tmp_path: Path,
):
    root = tmp_path / "v227"
    frozen = v227.freeze_v227(output_dir=root)
    outputs = [_gap_output(turn) for turn in frozen["turns"]]
    client = _FakeClient(root, outputs)
    terminal = asyncio.run(
        v227.run_v227(
            output_dir=root,
            client_factory=lambda policy_path: client,
        )
    )
    assert client.calls == 2
    assert terminal["state"] == "completed"
    assert terminal["support_audit_authorized"] is True
    assert terminal["development_winner_frozen"] is False
    assert terminal["holdout_authorized"] is False
    assert terminal["usage"]["total_tokens"] == 200
    assert terminal["production_mutated"] is False


def test_v227_rejects_mutated_runtime_file_coverage(tmp_path: Path):
    root = tmp_path / "v227"
    frozen = v227.freeze_v227(output_dir=root)
    lock = json.loads(frozen["runtime_lock"].read_text())
    lock["runtime_files"] = lock["runtime_files"][:-1]
    mutated = root / "mutated-runtime-lock.json"
    mutated.write_text(json.dumps(lock) + "\n", encoding="utf-8")
    with pytest.raises(v227.JudgeV5SelectionV227Error):
        v227.verify_runtime_lock(mutated)


def test_v227_rejects_launched_unfinished_attempt(tmp_path: Path):
    root = tmp_path / "v227"
    v227.freeze_v227(output_dir=root)
    (root / "launch-receipt.json").write_text("{}\n", encoding="utf-8")
    with pytest.raises(v227.JudgeV5SelectionV227Error):
        v227.freeze_v227(output_dir=root)
