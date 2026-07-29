from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import app_server_judge_v5_selection_v241_delta_llm_reducer as v241
from research_factory.app_server_capacity_reserve import (
    ReserveCapacityGatedCodexAppServerClient,
)


def _prepared_turn() -> dict:
    return v241._prepare_turn(v241._validate_lineage())


def _metric_applicability_invalid(event: dict) -> bool:
    values = [
        str(event.get(field) or "")
        for field in ("metric_value", "metric_unit", "metric_comparator", "metric_raw_text")
    ]
    return any(values) == (event.get("metric_direction") == "not_applicable")


def _corrected_event(event: dict) -> dict:
    result = copy.deepcopy(event)
    values = [
        str(result.get(field) or "")
        for field in ("metric_value", "metric_unit", "metric_comparator", "metric_raw_text")
    ]
    result["metric_direction"] = "unknown" if any(values) else "not_applicable"
    return result


def _output(turn: dict, *, keep_invalid: bool = False) -> dict:
    rows = []
    for source in turn["private_input"]["segments"]:
        is_dense = source["segment_id"] == v241.DENSE_SEGMENT_ID
        receipts = []
        replacements = []
        for index, proposal in enumerate(source["proposals"]):
            proposal_id = proposal["proposal_id"]
            event = proposal["event"]
            if not is_dense:
                receipts.append({"proposal_id": proposal_id, "action": "drop", "final_event_id": ""})
            elif _metric_applicability_invalid(event) and not keep_invalid:
                event_id = f"R{index:03d}"
                receipts.append({"proposal_id": proposal_id, "action": "replace", "final_event_id": event_id})
                replacement = _corrected_event(event)
                replacement["event_id"] = event_id
                replacement["source_proposal_ids"] = [proposal_id]
                replacements.append(replacement)
            else:
                receipts.append({"proposal_id": proposal_id, "action": "keep", "final_event_id": proposal_id})
        rows.append(
            {
                "segment_id": source["segment_id"],
                "final_status": "coded" if is_dense else "no_signal",
                "segment_source_context": {
                    "kind": "substantive_dialogue",
                    "confidence": 1.0,
                    "rationale": "fixture",
                },
                "no_signal_reason": "" if is_dense else "The proposal was rejected.",
                "coverage_audit": {
                    "all_source_units_reviewed": True,
                    "all_proposals_reviewed": True,
                    "unresolved_count": 0,
                },
                "proposal_receipts": receipts,
                "replacement_events": replacements,
            }
        )
    return {"episode_id": turn["episode_id"], "segments": rows}


def _usage(total: int = 30_000) -> dict:
    return {
        "input_tokens": total - 2_000,
        "cached_input_tokens": 0,
        "output_tokens": 2_000,
        "reasoning_output_tokens": 400,
        "total_tokens": total,
    }


def test_delta_request_reuses_full_proposal_packet_with_smaller_response_contract() -> None:
    turn = _prepared_turn()
    assert turn["prompt"] == v241.v240._prepare_turn(v241.v240._validate_lineage())["prompt"]
    assert "density_stratum" not in turn["prompt"]
    assert turn["prompt_bytes"] <= v241.MAX_PROMPT_BYTES
    assert turn["schema_bytes"] <= v241.MAX_SCHEMA_BYTES
    segment_schema = turn["schema"]["properties"]["segments"]["items"]
    assert "replacement_events" in segment_schema["properties"]
    assert "events" not in segment_schema["properties"]
    assert "keep" in segment_schema["properties"]["proposal_receipts"]["items"]["properties"]["action"]["enum"]


def test_projection_applies_llm_replacements_and_passes_cost_gate() -> None:
    turn = _prepared_turn()
    normalized, provenance, receipts, diagnostics = v241._project_output(_output(turn), turn)
    assert len(normalized["segments"][0]["events"]) == 31
    assert len(normalized["segments"][1]["events"]) == 0
    assert len(provenance["events"]) == 31
    dense_receipts = receipts["segments"][0]["proposal_receipts"]
    assert sum(row["action"] == "replace" for row in dense_receipts) == 4
    gate = v241._gate(usage=_usage(), diagnostics=diagnostics)
    assert gate["passed"] is True
    assert gate["combined_development_tokens"] == 69_004
    assert gate["production_amortized_total_token_ratio"] < 0.28


def test_projection_rejects_llm_keep_of_invalid_field_and_hidden_replacement() -> None:
    turn = _prepared_turn()
    with pytest.raises(v241.V241OutputContractError, match="metric direction applicability"):
        v241._project_output(_output(turn, keep_invalid=True), turn)
    hidden = _output(turn)
    first_replacement = hidden["segments"][0]["replacement_events"][0]
    first_replacement["source_proposal_ids"] = []
    with pytest.raises(v241.V241OutputContractError, match="mapping duplicate or empty"):
        v241._project_output(hidden, turn)


def test_freeze_binds_delta_architecture_runtime_and_capacity(tmp_path: Path) -> None:
    root = tmp_path / "v241"
    frozen = v241.freeze_v241(output_dir=root)
    lock = v241.verify_runtime_lock(frozen["runtime_lock"])
    assert lock["declared_turn_count"] == 1
    assert lock["retry_count"] == 0
    assert lock["adopted_prior_map_tokens"] == 39_004
    assert frozen["spec"]["isolated_field_repair"] is False
    design = v241._load_json(frozen["design_path"], "design")
    assert design["production_cost_projection"]["projected_production_amortized_total_token_ratio"] < 0.28
    client = object.__new__(ReserveCapacityGatedCodexAppServerClient)
    client.policy = v241.reserve.load_reserve_capacity_policy(frozen["capacity_policy"])
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
                    "model": v241.MODEL,
                    "effort": v241.EFFORT,
                    "error_class": None,
                    "usage": _usage(),
                }
            ),
            encoding="utf-8",
        )
        Path(str(kwargs["output_path"])).write_text(json.dumps(self.output), encoding="utf-8")
        return SimpleNamespace(status_ok=True, output=self.output)


def test_fake_run_is_one_measured_delta_reducer_and_authorizes_support(tmp_path: Path) -> None:
    turn = _prepared_turn()
    fake = _FakeClient(_output(turn))
    terminal = asyncio.run(
        v241.run_v241(output_dir=tmp_path / "v241-run", client_factory=lambda _policy: fake)
    )
    assert fake.calls == 1
    assert terminal["state"] == "v241_architecture_structural_gate_passed"
    assert terminal["semantic_retry_count"] == 0
    assert terminal["usage_status"] == "complete"
    assert terminal["combined_development_tokens"] == 69_004
    assert terminal["support_alignment_authorized"] is True
    assert terminal["holdout_authorized"] is False
    assert terminal["production_mutated"] is False
