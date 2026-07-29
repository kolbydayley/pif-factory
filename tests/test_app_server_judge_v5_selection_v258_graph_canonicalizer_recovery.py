from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import app_server_judge_v5_selection_v258_graph_canonicalizer_recovery as v258


@pytest.fixture(scope="module")
def lineage() -> dict:
    return v258._validate_lineage()


@pytest.fixture(scope="module")
def turn(lineage: dict) -> dict:
    return v258.prepare_turn(lineage)


def _synthetic_output(turn: dict) -> dict:
    source_root = v258.v253._source_turn_root(
        v258.v253.V249_ROOT, "v249-explicit-applicability-"
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
    assert not v258.labels_module._validate_schema(turn["schema"], result, path="$")
    return result


def test_recovery_changes_only_unique_items_schema_keyword(
    lineage: dict, turn: dict
) -> None:
    prior = v258.v253.prepare_turn(lineage)
    reconstructed = copy.deepcopy(turn["schema"])
    source_ids = reconstructed["properties"]["segments"]["items"]["properties"][
        "events"
    ]["items"]["properties"]["source_graph_event_ids"]
    source_ids["uniqueItems"] = True
    assert reconstructed == prior["schema"]
    assert turn["prompt"] == prior["prompt"]
    assert turn["base"] == prior["base"]
    assert turn["graph_ids"] == prior["graph_ids"]
    assert v258._schema_unique_items_paths(turn["schema"]) == []
    assert round(v258._production_ratio(v258.MAX_COMBINED_TOKENS)[1], 6) == 0.277723


def test_projection_accounts_every_graph_node_exactly_once(turn: dict) -> None:
    normalized, _, diagnostics, applicability, coverage = v258.project_output(
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
    with pytest.raises(v258.V258OutputContractError):
        v258.project_output(output, turn)


def test_freeze_is_presemantic_and_hash_bound(tmp_path: Path) -> None:
    root = tmp_path / "v258"
    frozen = v258.freeze_v258(output_dir=root)
    lock = v258.verify_runtime_lock(frozen["runtime_lock"])
    audit = json.loads((root / "v253-infrastructure-recovery-audit.json").read_text())
    assert lock["declared_turn_count"] == 1
    assert lock["retry_count"] == 0
    assert lock["max_total_tokens"] == 35_000
    assert audit["v253_usage_status"] == "unknown"
    assert audit["semantic_prompt_changed"] is False
    assert audit["deterministic_exact_once_validation_retained"] is True
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
                    "model": v258.MODEL,
                    "effort": v258.EFFORT,
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


def test_fake_run_passes_structural_gate_without_holdout(tmp_path: Path) -> None:
    root = tmp_path / "v258"
    frozen = v258.freeze_v258(output_dir=root)
    fake = _FakeClient(root, _synthetic_output(frozen["turn"]))
    terminal = asyncio.run(
        v258.run_v258(output_dir=root, client_factory=lambda _path: fake)
    )
    assert fake.calls == 1
    assert terminal["terminal_reason"] == (
        "v258_graph_canonicalizer_structural_cost_gate_passed"
    )
    assert terminal["support_alignment_authorized"] is True
    assert terminal["holdout_authorized"] is False
    assert terminal["production_mutated"] is False


def test_runtime_lock_rejects_mutated_prompt(tmp_path: Path) -> None:
    root = tmp_path / "v258"
    frozen = v258.freeze_v258(output_dir=root)
    prompt = next(root.glob("turns/*/prompt.private.md"))
    prompt.write_text(prompt.read_text(encoding="utf-8") + "drift", encoding="utf-8")
    with pytest.raises(v258.V258GraphCanonicalizerError):
        v258.verify_runtime_lock(frozen["runtime_lock"])
