from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import app_server_judge_v5_selection_v240_global_llm_reducer as v240
from research_factory.app_server_capacity_reserve import (
    ReserveCapacityGatedCodexAppServerClient,
)


def _prepared_turn() -> dict:
    return v240._prepare_turn(v240._validate_lineage())


def _clean_event(event: dict) -> dict:
    result = copy.deepcopy(event)
    result["metric_value"] = ""
    result["metric_unit"] = ""
    result["metric_comparator"] = ""
    result["metric_raw_text"] = ""
    result["metric_direction"] = "not_applicable"
    return result


def _output(turn: dict, dense_event_count: int = 24, keep_residual: bool = False) -> dict:
    rows = []
    for source in turn["private_input"]["segments"]:
        proposals = source["proposals"]
        is_dense = source["segment_id"] == v240.DENSE_SEGMENT_ID
        keep_count = dense_event_count if is_dense else int(keep_residual)
        receipts = []
        events = []
        for index, proposal in enumerate(proposals):
            proposal_id = proposal["proposal_id"]
            if index < keep_count:
                event_id = f"E{index:03d}"
                receipts.append(
                    {"proposal_id": proposal_id, "action": "keep", "final_event_id": event_id}
                )
                event = _clean_event(proposal["event"])
                event["event_id"] = event_id
                event["source_proposal_ids"] = [proposal_id]
                events.append(event)
            else:
                receipts.append(
                    {"proposal_id": proposal_id, "action": "drop", "final_event_id": ""}
                )
        rows.append(
            {
                "segment_id": source["segment_id"],
                "status": "coded" if events else "no_signal",
                "segment_source_context": {
                    "kind": "substantive_dialogue",
                    "confidence": 1.0,
                    "rationale": "fixture",
                },
                "no_signal_reason": "" if events else "All proposals were rejected.",
                "coverage_audit": {
                    "all_source_units_reviewed": True,
                    "all_proposals_reviewed": True,
                    "unresolved_count": 0,
                },
                "proposal_receipts": receipts,
                "events": events,
            }
        )
    return {"episode_id": turn["episode_id"], "segments": rows}


def _usage(total: int = 30_000) -> dict:
    return {
        "input_tokens": total - 8_000,
        "cached_input_tokens": 0,
        "output_tokens": 8_000,
        "reasoning_output_tokens": 500,
        "total_tokens": total,
    }


def test_request_compacts_empty_fields_without_semantic_pruning() -> None:
    turn = _prepared_turn()
    packet = json.loads(turn["prompt"].split("\n", 1)[1])
    assert "density_stratum" not in turn["prompt"]
    assert "target_count" not in turn["prompt"]
    assert "reference_answer" not in turn["prompt"]
    assert turn["prompt_bytes"] <= v240.MAX_PROMPT_BYTES
    assert turn["base_bytes"] <= v240.MAX_BASE_BYTES
    assert turn["schema_bytes"] <= v240.MAX_SCHEMA_BYTES
    private_by_id = {
        proposal["proposal_id"]: proposal["event"]
        for segment in turn["private_input"]["segments"]
        for proposal in segment["proposals"]
    }
    prompt_by_id = {
        proposal["proposal_id"]: proposal["event"]
        for segment in packet["segments"]
        for proposal in segment["proposals"]
    }
    assert set(prompt_by_id) == set(private_by_id)
    for proposal_id, event in private_by_id.items():
        assert prompt_by_id[proposal_id] == {
            key: value for key, value in event.items() if value not in (None, "", [], {})
        }


def test_projection_accounts_all_proposals_and_passes_gate() -> None:
    turn = _prepared_turn()
    normalized, provenance, receipts, diagnostics = v240._project_output(
        _output(turn), turn
    )
    assert len(normalized["segments"][0]["events"]) == 24
    assert len(normalized["segments"][1]["events"]) == 0
    assert len(provenance["events"]) == 24
    assert len(receipts["segments"][0]["proposal_receipts"]) == 31
    gate = v240._gate(usage=_usage(), diagnostics=diagnostics)
    assert gate["passed"] is True
    assert gate["combined_development_tokens"] == 69_004
    assert gate["production_amortized_total_token_ratio"] < 0.28


def test_projection_rejects_hidden_drop_and_invalid_merge_mapping() -> None:
    turn = _prepared_turn()
    hidden = _output(turn, dense_event_count=1)
    hidden["segments"][0]["proposal_receipts"][0]["final_event_id"] = "missing"
    with pytest.raises(v240.V240OutputContractError, match="lacks a final event"):
        v240._project_output(hidden, turn)
    merge = _output(turn, dense_event_count=2)
    merge["segments"][0]["proposal_receipts"][1]["action"] = "merge"
    with pytest.raises(v240.V240OutputContractError, match="single proposal event must be kept"):
        v240._project_output(merge, turn)


def test_freeze_binds_adopted_map_runtime_and_capacity(tmp_path: Path) -> None:
    root = tmp_path / "v240"
    frozen = v240.freeze_v240(output_dir=root)
    lock = v240.verify_runtime_lock(frozen["runtime_lock"])
    assert lock["declared_turn_count"] == 1
    assert lock["retry_count"] == 0
    assert lock["adopted_prior_map_tokens"] == 39_004
    assert frozen["spec"]["isolated_field_repair"] is False
    design = v240._load_json(frozen["design_path"], "design")
    assert design["production_cost_projection"]["projected_production_amortized_total_token_ratio"] < 0.28
    client = object.__new__(ReserveCapacityGatedCodexAppServerClient)
    client.policy = v240.reserve.load_reserve_capacity_policy(frozen["capacity_policy"])
    index, path = client._turn_index(frozen["turn"]["paths"]["capacity"])
    assert index == 0
    assert path == frozen["turn"]["paths"]["capacity"].resolve()
    assert not frozen["turn"]["paths"]["sidecar"].exists()


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
            json.dumps({"schema_version": "pif_app_server_reserve_capacity_checkpoint_v1", "cleared_for_semantic_turn": True}),
            encoding="utf-8",
        )
        Path(str(kwargs["sidecar_path"])).write_text(
            json.dumps(
                {
                    "state": "completed",
                    "status": "completed",
                    "usage_status": "measured",
                    "usage_complete": True,
                    "auth_type": "chatgpt",
                    "plan_type": "pro",
                    "model": v240.MODEL,
                    "effort": v240.EFFORT,
                    "error_class": None,
                    "usage": _usage(),
                }
            ),
            encoding="utf-8",
        )
        Path(str(kwargs["output_path"])).write_text(json.dumps(self.output), encoding="utf-8")
        return SimpleNamespace(status_ok=True, output=self.output)


def test_fake_run_is_one_measured_reducer_and_authorizes_support(tmp_path: Path) -> None:
    turn = _prepared_turn()
    fake = _FakeClient(_output(turn))
    terminal = asyncio.run(
        v240.run_v240(output_dir=tmp_path / "v240-run", client_factory=lambda _policy: fake)
    )
    assert fake.calls == 1
    assert terminal["state"] == "v240_architecture_structural_gate_passed"
    assert terminal["semantic_retry_count"] == 0
    assert terminal["usage_status"] == "complete"
    assert terminal["combined_development_tokens"] == 69_004
    assert terminal["support_alignment_authorized"] is True
    assert terminal["holdout_authorized"] is False
    assert terminal["production_mutated"] is False
