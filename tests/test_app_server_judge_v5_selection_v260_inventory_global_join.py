from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import app_server_judge_v5_selection_v249_explicit_applicability as v249
from research_factory import app_server_judge_v5_selection_v260_inventory_global_join as v260


@pytest.fixture(scope="module")
def lineage() -> dict:
    return v260._validate_lineage()


@pytest.fixture(scope="module")
def turn(lineage: dict) -> dict:
    return v260.prepare_turn(lineage)


def _synthetic_output(turn: dict) -> dict:
    old = json.loads(v249._lineage_paths()["v239_output"].read_text(encoding="utf-8"))
    old_by_id = {str(row["segment_id"]): row for row in old["segments"]}
    segments = []
    for inventory_row in turn["inventory"]["segments"]:
        segment_id = str(inventory_row["segment_id"])
        source_row = old_by_id[segment_id]
        templates = list(source_row["events"])
        events = []
        for index, proposition in enumerate(inventory_row["propositions"]):
            event = copy.deepcopy(templates[min(index, len(templates) - 1)])
            event["source_proposition_ids"] = [proposition["proposition_id"]]
            event["evidence_start_unit_id"] = proposition["evidence_start_unit_id"]
            event["evidence_end_unit_id"] = proposition["evidence_end_unit_id"]
            event["claim_text"] = proposition["claim_text"]
            event["event_type"] = proposition["event_type"]
            for field in (
                "metric_value",
                "metric_unit",
                "metric_comparator",
                "metric_raw_text",
            ):
                event[field] = ""
            event["metric_direction"] = "not_applicable"
            events.append(event)
        segments.append(
            {
                "segment_id": segment_id,
                "status": "coded" if events else "no_signal",
                "segment_source_context": source_row["segment_source_context"],
                "no_signal_reason": "" if events else source_row["no_signal_reason"],
                "events": events,
                "dropped_propositions": [],
            }
        )
    output = {"episode_id": turn["episode_id"], "segments": segments}
    v260._validate_schema(turn["schema"], output, path="$")
    return output


def test_design_adopts_measured_inventory_and_fits_cost_target(turn: dict) -> None:
    assert turn["segment_ids"] == [v260.v234.DENSE_SEGMENT_ID, v260.v234.NO_SIGNAL_SEGMENT_ID]
    assert [len(row["propositions"]) for row in turn["inventory"]["segments"]] == [32, 1]
    assert turn["prompt_bytes"] <= v260.MAX_PROMPT_BYTES
    assert turn["base_bytes"] <= v260.MAX_BASE_BYTES
    assert turn["schema_bytes"] <= v260.MAX_SCHEMA_BYTES
    assert v260.ADOPTED_INVENTORY_TOKENS + v260.MAX_NEW_TOKENS == 70_000
    assert round(v260._production_accounting(v260.MAX_COMBINED_TOKENS)[1], 6) == 0.268298
    assert "reference answer" in v260.GLOBAL_JOIN_INSTRUCTIONS
    assert v260.MODEL == "gpt-5.6-sol"
    assert v260.EFFORT == "low"


def test_global_join_projects_exact_once_ownership(turn: dict) -> None:
    normalized, provenance, diagnostics, ownership = v260.project_output(
        _synthetic_output(turn), turn
    )
    assert [len(row["events"]) for row in normalized["segments"]] == [32, 1]
    assert len(provenance["events"]) == 33
    assert [row["realized_event_count"] for row in diagnostics] == [32, 1]
    assert all(
        row["all_propositions_accounted_exactly_once"] is True
        for row in ownership["segments"]
    )
    assert ownership["all_join_semantics_selected_by_llm"] is True
    assert ownership["deterministic_exact_ownership_projection_only"] is True


def test_global_join_rejects_duplicate_proposition_ownership(turn: dict) -> None:
    output = _synthetic_output(turn)
    duplicate = output["segments"][0]["events"][0]["source_proposition_ids"][0]
    output["segments"][0]["events"][1]["source_proposition_ids"].append(duplicate)
    with pytest.raises(v260.V260OutputContractError):
        v260.project_output(output, turn)


class _FakeClient:
    def __init__(self, root: Path, output: dict, total_tokens: int = 20_000) -> None:
        self.root = root
        self.output = output
        self.total_tokens = total_tokens
        self.calls = 0

    async def __aenter__(self) -> "_FakeClient":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def run_ephemeral_structured_turn(self, **kwargs: object) -> SimpleNamespace:
        assert "output_validator" not in kwargs
        assert (self.root / "launch-receipt.json").is_file()
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
            "input_tokens": self.total_tokens - 4_500,
            "cached_input_tokens": 1_000,
            "output_tokens": 4_000,
            "reasoning_output_tokens": 500,
            "total_tokens": self.total_tokens,
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
                    "model": v260.MODEL,
                    "effort": v260.EFFORT,
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


def test_freeze_and_fake_run_are_hash_bound_single_turn(tmp_path: Path) -> None:
    root = tmp_path / "v260"
    frozen = v260.freeze_v260(output_dir=root)
    lock = v260.verify_runtime_lock(frozen["runtime_lock"])
    assert lock["declared_new_turn_count"] == 1
    assert lock["retry_count"] == 0
    assert lock["adopted_inventory_tokens"] == 29_362
    assert lock["max_new_tokens"] == 40_638
    assert lock["max_combined_tokens"] == 70_000
    assert lock["semantic_regex_or_keyword_filtering"] is False
    assert not (root / "launch-receipt.json").exists()
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))

    fake = _FakeClient(root, _synthetic_output(frozen["turn"]))
    terminal = asyncio.run(v260.run_v260(output_dir=root, client_factory=lambda _path: fake))
    assert fake.calls == 1
    assert terminal["state"] == "v260_architecture_structural_gate_passed"
    assert terminal["new_turn_usage"]["total_tokens"] == 20_000
    assert terminal["combined_usage"]["total_tokens"] == 49_362
    assert terminal["production_amortized_total_token_ratio"] < 0.28
    assert terminal["support_alignment_authorized"] is True
    assert terminal["holdout_authorized"] is False
    assert terminal["production_mutated"] is False
