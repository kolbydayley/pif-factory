from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import app_server_judge_v5_selection_v233_blind_unit_sweep as v233
from research_factory.app_server_capacity_reserve import (
    ReserveCapacityGatedCodexAppServerClient,
)


def _prepared_turn() -> dict:
    return v233._prepare_turn(v233._validate_lineage())


def _context() -> dict:
    return {"kind": "substantive_dialogue", "confidence": 1.0, "rationale": "fixture"}


def _template_event() -> dict:
    source = v233._load_json(
        v233.SOURCE_TURN_ROOT / "input.private.json", "fixture source"
    )
    dense = next(
        row
        for row in source["normalization_segments"]
        if row["segment_id"] == v233.DENSE_SEGMENT_ID
    )
    event = copy.deepcopy(dense["existing_events"][0])
    event.pop("window_id", None)
    event.pop("evidence", None)
    event["metric_value"] = ""
    event["metric_unit"] = ""
    event["metric_comparator"] = ""
    event["metric_raw_text"] = ""
    event["metric_direction"] = "not_applicable"
    return event


def _output(turn: dict, dense_event_count: int = 24) -> dict:
    template = _template_event()
    rows = []
    for source in turn["private_input"]["segments"]:
        units = source["units"]
        receipts = [
            {
                "unit_id": unit["unit_id"],
                "eligible_event_count": 0,
                "unresolved_count": 0,
            }
            for unit in units
        ]
        events = []
        if source["segment_id"] == v233.DENSE_SEGMENT_ID:
            assert dense_event_count <= len(units)
            for index, unit in enumerate(units[:dense_event_count]):
                event = copy.deepcopy(template)
                event["claim_text"] = f"{event['claim_text']} fixture-{index}"
                event["evidence_start_unit_id"] = unit["unit_id"]
                event["evidence_end_unit_id"] = unit["unit_id"]
                events.append(event)
                receipts[index]["eligible_event_count"] = 1
        rows.append(
            {
                "segment_id": source["segment_id"],
                "status": "coded" if events else "no_signal",
                "segment_source_context": _context(),
                "no_signal_reason": "" if events else "No eligible event is present.",
                "unit_receipts": receipts,
                "coverage_audit": {
                    "all_source_units_reviewed": True,
                    "unresolved_count": 0,
                },
                "events": events,
            }
        )
    return {"episode_id": turn["episode_id"], "segments": rows}


def _usage(total: int = 50_000) -> dict:
    return {
        "input_tokens": total - 10_000,
        "cached_input_tokens": 0,
        "output_tokens": 10_000,
        "reasoning_output_tokens": 2_000,
        "total_tokens": total,
    }


def test_prompt_is_blind_and_every_source_unit_is_schema_addressable() -> None:
    turn = _prepared_turn()
    packet = json.loads(turn["prompt"].split("\n", 1)[1])
    assert list(packet) == ["episode_id", "segments"]
    assert turn["segment_ids"] == [
        v233.DENSE_SEGMENT_ID,
        v233.NO_SIGNAL_SEGMENT_ID,
    ]
    assert "existing_events" not in turn["prompt"]
    assert "density_stratum" not in turn["prompt"]
    assert "reference_count" not in turn["prompt"]
    prompt_units = [
        unit["unit_id"]
        for segment in packet["segments"]
        for unit in segment["source_units"]
    ]
    private_units = [
        unit["unit_id"]
        for segment in turn["private_input"]["segments"]
        for unit in segment["units"]
    ]
    assert prompt_units == private_units
    event_schema = turn["schema"]["properties"]["segments"]["items"][
        "properties"
    ]["events"]["items"]
    assert "evidence" not in event_schema["properties"]
    assert "window_id" not in event_schema["properties"]
    assert "evidence_start_unit_id" in event_schema["required"]


def test_projection_and_count_gate_pass_for_complete_exact_fixture() -> None:
    turn = _prepared_turn()
    normalized, provenance, diagnostics = v233._project_output(_output(turn), turn)
    dense = next(
        row
        for row in normalized["segments"]
        if row["segment_id"] == v233.DENSE_SEGMENT_ID
    )
    assert len(dense["events"]) == 24
    assert len(provenance["events"]) == 24
    assert all(event["evidence"] for event in dense["events"])
    gate = v233._build_gate(usage=_usage(), diagnostics=diagnostics)
    assert gate["passed"] is True
    assert gate["support_alignment_authorized"] is True
    assert gate["production_amortized_total_token_ratio"] < 0.28


def test_projection_rejects_unit_omission_unresolved_metric_and_duplicate() -> None:
    turn = _prepared_turn()
    valid = _output(turn, dense_event_count=1)

    omitted = copy.deepcopy(valid)
    omitted["segments"][0]["unit_receipts"].pop()
    with pytest.raises(v233.V233OutputContractError, match="unit receipt order"):
        v233._project_output(omitted, turn)

    unresolved = copy.deepcopy(valid)
    unresolved["segments"][0]["unit_receipts"][0]["unresolved_count"] = 1
    unresolved["segments"][0]["coverage_audit"]["unresolved_count"] = 1
    with pytest.raises(v233.V233OutputContractError, match="coverage audit"):
        v233._project_output(unresolved, turn)

    metric = copy.deepcopy(valid)
    event = metric["segments"][0]["events"][0]
    event["metric_value"] = "not-in-source"
    event["metric_direction"] = "increase"
    with pytest.raises(v233.V233OutputContractError, match="metric literal"):
        v233._project_output(metric, turn)

    duplicate = copy.deepcopy(valid)
    row = duplicate["segments"][0]
    row["events"].append(copy.deepcopy(row["events"][0]))
    row["unit_receipts"][0]["eligible_event_count"] = 2
    with pytest.raises(v233.V233OutputContractError, match="identity duplicate"):
        v233._project_output(duplicate, turn)

def test_count_gate_fails_below_frozen_source_floor() -> None:
    turn = _prepared_turn()
    _, _, diagnostics = v233._project_output(
        _output(
            turn,
            dense_event_count=v233.MIN_DENSE_EVENTS_FOR_FROZEN_SOURCE_FLOOR - 1,
        ),
        turn,
    )
    gate = v233._build_gate(usage=_usage(), diagnostics=diagnostics)
    assert gate["passed"] is False
    assert "dense_event_count_gte_24" in gate["failed_checks"]


def test_freeze_binds_ranked_architecture_runtime_and_capacity(tmp_path: Path) -> None:
    root = tmp_path / "v233"
    frozen = v233.freeze_v233(output_dir=root)
    lock = v233.verify_runtime_lock(frozen["runtime_lock"])
    ranking = v233._load_json(frozen["ranking_path"], "ranking")
    design = v233._load_json(frozen["design_path"], "design")
    assert lock["declared_turn_count"] == 1
    assert lock["retry_count"] == 0
    assert len(ranking["architectures"]) >= 3
    assert ranking["architectures"][0]["architecture_id"] == (
        "blind_per_unit_standalone_sweep"
    )
    assert design["production_cost_projection"][
        "projected_production_amortized_total_token_ratio"
    ] < 0.28
    assert frozen["spec"]["existing_events_visible_to_model"] is False
    client = object.__new__(ReserveCapacityGatedCodexAppServerClient)
    client.policy = v233.reserve.load_reserve_capacity_policy(
        frozen["capacity_policy"]
    )
    observed_index, observed_path = client._turn_index(
        frozen["turn"]["paths"]["capacity"]
    )
    assert observed_index == 0
    assert observed_path == frozen["turn"]["paths"]["capacity"].resolve()
    assert not frozen["turn"]["paths"]["capacity"].exists()
    assert not frozen["turn"]["paths"]["sidecar"].exists()


class _FakeClient:
    def __init__(self, output: dict) -> None:
        self.output = output

    async def __aenter__(self) -> "_FakeClient":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def run_ephemeral_structured_turn(self, **kwargs: object) -> SimpleNamespace:
        capacity_path = Path(str(kwargs["capacity_checkpoint_path"]))
        capacity_path.write_text(
            json.dumps(
                {
                    "cleared_for_semantic_turn": True,
                    "managed_chatgpt_auth_verified": True,
                }
            ),
            encoding="utf-8",
        )
        sidecar_path = Path(str(kwargs["sidecar_path"]))
        sidecar_path.write_text(
            json.dumps(
                {
                    "state": "completed",
                    "status": "completed",
                    "usage_status": "measured",
                    "usage_complete": True,
                    "auth_type": "chatgpt",
                    "plan_type": "pro",
                    "model": v233.MODEL,
                    "effort": v233.EFFORT,
                    "error_class": None,
                    "usage": _usage(),
                }
            ),
            encoding="utf-8",
        )
        Path(str(kwargs["output_path"])).write_text(
            json.dumps(self.output), encoding="utf-8"
        )
        return SimpleNamespace(status_ok=True, output=self.output)


def test_fake_run_freezes_measured_pass_without_retry(tmp_path: Path) -> None:
    output = _output(_prepared_turn())
    root = tmp_path / "v233-run"
    terminal = asyncio.run(
        v233.run_v233(
            output_dir=root,
            client_factory=lambda _policy: _FakeClient(output),
        )
    )
    assert terminal["state"] == "v233_architecture_structural_gate_passed"
    assert terminal["semantic_attempt_count"] == 1
    assert terminal["semantic_retry_count"] == 0
    assert terminal["usage_status"] == "complete"
    assert terminal["support_alignment_authorized"] is True
    assert terminal["holdout_authorized"] is False
    assert terminal["production_mutated"] is False
