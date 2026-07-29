from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import (
    app_server_judge_v5_selection_v270_graph_canonicalizer_recovery as v270,
)


@pytest.fixture(scope="module")
def lineage() -> dict:
    return v270._validate_lineage()


@pytest.fixture(scope="module")
def turn(lineage: dict) -> dict:
    return v270.prepare_turn(lineage)


def _synthetic_output(turn: dict) -> dict:
    source_root = v270._source_turn_root(
        v270.V249_ROOT, "v249-explicit-applicability-"
    )
    output = json.loads((source_root / "output.private.json").read_text(encoding="utf-8"))
    result = copy.deepcopy(output)
    for position, segment in enumerate(result["segments"]):
        graph_ids = list(turn["graph_ids"][segment["segment_id"]])
        segment["events"] = segment["events"][: len(graph_ids)]
        for index, event in enumerate(segment["events"]):
            event["event_id"] = f"S{position}C{index:03d}"
            event["source_graph_event_ids"] = [graph_ids[index]]
        segment["dropped_source_graph_events"] = []
        starts = {}
        for event in segment["events"]:
            unit_id = event["evidence_start_unit_id"]
            starts[unit_id] = starts.get(unit_id, 0) + 1
        for receipt in segment["unit_receipts"]:
            receipt["eligible_event_count"] = starts.get(receipt["unit_id"], 0)
    assert not v270.labels_module._validate_schema(turn["schema"], result, path="$")
    return result


def test_turn_is_ranked_distinct_bounded_architecture(turn: dict) -> None:
    assert v270.MODEL == "gpt-5.6-sol"
    assert v270.EFFORT == "low"
    assert [len(turn["graph_ids"][item]) for item in turn["segment_ids"]] == [31, 1]
    assert turn["prompt_bytes"] <= v270.MAX_PROMPT_BYTES
    assert turn["base_bytes"] <= v270.MAX_BASE_BYTES
    assert turn["schema_bytes"] <= v270.MAX_SCHEMA_BYTES
    total, ratio = v270._production_ratio(v270.MAX_COMBINED_TOKENS)
    assert total == 2_795_398
    assert round(ratio, 6) == 0.277723


def test_projection_accounts_every_graph_node_exactly_once(turn: dict) -> None:
    normalized, _, diagnostics, applicability, coverage = v270.project_output(
        _synthetic_output(turn), turn
    )
    assert [len(row["events"]) for row in normalized["segments"]] == [31, 1]
    assert [row["event_count"] for row in diagnostics] == [31, 1]
    assert applicability["deterministic_projection_only"] is True
    assert coverage["total_source_graph_event_count"] == 32
    assert coverage["dropped_source_graph_event_count"] == 0
    assert coverage["all_source_graph_events_accounted_exactly_once"] is True


def test_projection_rejects_duplicate_graph_ownership(turn: dict) -> None:
    output = _synthetic_output(turn)
    output["segments"][0]["events"][1]["source_graph_event_ids"] = list(
        output["segments"][0]["events"][0]["source_graph_event_ids"]
    )
    with pytest.raises(v270.V270OutputContractError):
        v270.project_output(output, turn)


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
            "input_tokens": 15_000,
            "cached_input_tokens": 1_000,
            "output_tokens": 7_000,
            "reasoning_output_tokens": 1_000,
            "total_tokens": 22_000,
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
                    "model": v270.MODEL,
                    "effort": v270.EFFORT,
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


def test_freeze_and_fake_run_reuse_v253_request_without_holdout(tmp_path: Path) -> None:
    root = tmp_path / "v270"
    frozen = v270.freeze_v270(output_dir=root)
    lock = v270.verify_runtime_lock(frozen["runtime_lock"])
    audit = json.loads((root / "v253-infrastructure-failure-audit.json").read_text())
    predecessor = frozen["turn"]
    source_paths = v270._lineage_paths()
    assert lock["declared_turn_count"] == 1
    assert lock["retry_count"] == 0
    assert lock["max_total_tokens"] == 35_000
    assert audit["sidecar_usage_status"] == "unknown"
    assert audit["predecessor_output_absent"] is True
    assert audit["frozen_request_reused_byte_for_byte"] is True
    for name, source_name in {
        "input": "v253_input",
        "prompt": "v253_prompt",
        "base": "v253_base",
        "schema": "v253_schema",
        "projection_schema": "v253_projection_schema",
        "direct_schema": "v253_direct_schema",
    }.items():
        assert v270._record(predecessor["paths"][name])["sha256"] == v270._record(
            source_paths[source_name]
        )["sha256"]
    assert not (root / "launch-receipt.json").exists()
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    fake = _FakeClient(root, _synthetic_output(frozen["turn"]))
    terminal = asyncio.run(
        v270.run_v270(output_dir=root, client_factory=lambda _path: fake)
    )
    assert fake.calls == 1
    assert terminal["terminal_reason"] == "v270_graph_canonicalizer_structural_cost_gate_passed"
    assert terminal["support_alignment_authorized"] is True
    assert terminal["holdout_authorized"] is False
    assert terminal["production_mutated"] is False
