from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import (
    app_server_judge_v5_selection_v234_two_pass_blind_inventory as v234,
)
from research_factory.app_server_capacity_reserve import (
    ReserveCapacityGatedCodexAppServerClient,
)


def _inventory_turn() -> dict:
    return v234._prepare_inventory_turn(v234._validate_lineage())


def _context() -> dict:
    return {"kind": "substantive_dialogue", "confidence": 1.0, "rationale": "fixture"}


def _template_event() -> dict:
    source = v234._load_json(
        v234.SOURCE_TURN_ROOT / "input.private.json", "fixture source"
    )
    dense = next(
        row
        for row in source["normalization_segments"]
        if row["segment_id"] == v234.DENSE_SEGMENT_ID
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


def _inventory_output(turn: dict, dense_count: int = 24, no_signal_count: int = 0) -> dict:
    rows = []
    for segment_position, source in enumerate(turn["private_input"]["segments"]):
        units = source["units"]
        requested_count = (
            dense_count
            if source["segment_id"] == v234.DENSE_SEGMENT_ID
            else no_signal_count
        )
        assert requested_count <= len(units)
        receipts = [
            {
                "unit_id": unit["unit_id"],
                "eligible_proposition_count": 0,
                "unresolved_count": 0,
            }
            for unit in units
        ]
        propositions = []
        for index, unit in enumerate(units[:requested_count]):
            propositions.append(
                {
                    "proposition_id": f"S{segment_position}P{index:03d}",
                    "evidence_start_unit_id": unit["unit_id"],
                    "evidence_end_unit_id": unit["unit_id"],
                    "event_type": "capability_claim",
                    "claim_text": f"Fixture proposition {segment_position}-{index}.",
                }
            )
            receipts[index]["eligible_proposition_count"] = 1
        rows.append(
            {
                "segment_id": source["segment_id"],
                "unit_receipts": receipts,
                "coverage_audit": {
                    "all_source_units_reviewed": True,
                    "unresolved_count": 0,
                },
                "propositions": propositions,
            }
        )
    return {"episode_id": turn["episode_id"], "segments": rows}


def _realization_output(realization: dict, inventory: dict) -> dict:
    template = _template_event()
    rows = []
    for inventory_row in inventory["segments"]:
        events = []
        for index, proposition in enumerate(inventory_row["propositions"]):
            event = copy.deepcopy(template)
            event["claim_text"] = f"{event['claim_text']} fixture-realized-{index}"
            event["proposition_id"] = proposition["proposition_id"]
            event["evidence_start_unit_id"] = proposition["evidence_start_unit_id"]
            event["evidence_end_unit_id"] = proposition["evidence_end_unit_id"]
            events.append(event)
        rows.append(
            {
                "segment_id": inventory_row["segment_id"],
                "status": "coded" if events else "no_signal",
                "segment_source_context": _context(),
                "no_signal_reason": "" if events else "No inventoried proposition is present.",
                "events": events,
            }
        )
    return {"episode_id": realization["episode_id"], "segments": rows}


def _usage(total: int = 30_000) -> dict:
    return {
        "input_tokens": total - 8_000,
        "cached_input_tokens": 0,
        "output_tokens": 8_000,
        "reasoning_output_tokens": 500,
        "total_tokens": total,
    }


def test_inventory_prompt_is_blind_and_realization_is_separate() -> None:
    turn = _inventory_turn()
    assert "existing_events" not in turn["prompt"]
    assert "density_stratum" not in turn["prompt"]
    assert "reference_count" not in turn["prompt"]
    assert "segment_source_context" not in json.dumps(turn["schema"])
    inventory, _, diagnostics = v234._project_inventory(
        _inventory_output(turn), turn
    )
    assert v234._build_inventory_gate(diagnostics)["passed"] is True
    packet = v234._source_packet(v234._validate_lineage())
    realization = v234._prepare_realization_turn(packet, inventory)
    assert "frozen_propositions" in realization["prompt"]
    assert "existing_events" not in realization["prompt"]
    assert "density_stratum" not in realization["prompt"]
    assert "reference_count" not in realization["prompt"]


def test_inventory_requires_every_unit_sequential_ids_and_count_floor() -> None:
    turn = _inventory_turn()
    valid = _inventory_output(turn)
    inventory, provenance, diagnostics = v234._project_inventory(valid, turn)
    assert len(inventory["segments"][0]["propositions"]) == 24
    assert len(provenance["propositions"]) == 24
    assert v234._build_inventory_gate(diagnostics)["passed"] is True

    omitted = copy.deepcopy(valid)
    omitted["segments"][0]["unit_receipts"].pop()
    with pytest.raises(v234.V234OutputContractError, match="receipt order"):
        v234._project_inventory(omitted, turn)

    bad_id = copy.deepcopy(valid)
    bad_id["segments"][0]["propositions"][0]["proposition_id"] = "S0P001"
    with pytest.raises(v234.V234OutputContractError, match="id order"):
        v234._project_inventory(bad_id, turn)

    below = _inventory_output(turn, dense_count=23)
    _, _, below_diagnostics = v234._project_inventory(below, turn)
    gate = v234._build_inventory_gate(below_diagnostics)
    assert gate["passed"] is False
    assert "dense_proposition_count_gte_24" in gate["failed_checks"]

    false_positive = _inventory_output(turn, no_signal_count=1)
    _, _, false_positive_diagnostics = v234._project_inventory(false_positive, turn)
    gate = v234._build_inventory_gate(false_positive_diagnostics)
    assert gate["passed"] is False
    assert "no_signal_proposition_count_0" in gate["failed_checks"]


def test_realization_requires_one_event_per_inventory_and_exact_grounding() -> None:
    inventory_turn = _inventory_turn()
    inventory, _, _ = v234._project_inventory(
        _inventory_output(inventory_turn), inventory_turn
    )
    packet = v234._source_packet(v234._validate_lineage())
    prepared = v234._prepare_realization_turn(packet, inventory)
    valid = _realization_output(prepared, inventory)
    normalized, provenance, diagnostics = v234._project_realization(
        valid, prepared, inventory_turn["private_input"], inventory
    )
    assert len(normalized["segments"][0]["events"]) == 24
    assert len(provenance["events"]) == 24
    assert all(
        row["inventory_proposition_count"] == row["realized_event_count"]
        for row in diagnostics
    )

    omitted = copy.deepcopy(valid)
    omitted["segments"][0]["events"].pop()
    with pytest.raises(v234.V234OutputContractError, match="proposition coverage"):
        v234._project_realization(
            omitted, prepared, inventory_turn["private_input"], inventory
        )

    metric = copy.deepcopy(valid)
    metric["segments"][0]["events"][0]["metric_value"] = "not-in-evidence"
    metric["segments"][0]["events"][0]["metric_direction"] = "increase"
    with pytest.raises(v234.V234OutputContractError, match="metric literal"):
        v234._project_realization(
            metric, prepared, inventory_turn["private_input"], inventory
        )


def test_freeze_binds_two_turn_order_cost_and_conditional_request(tmp_path: Path) -> None:
    root = tmp_path / "v234"
    frozen = v234.freeze_v234(output_dir=root)
    lock = v234.verify_runtime_lock(frozen["runtime_lock"])
    design = v234._load_json(frozen["design_path"], "design")
    ranking = v234._load_json(frozen["ranking_path"], "ranking")
    assert lock["declared_turn_count"] == 2
    assert lock["retry_count_per_turn"] == 0
    assert ranking["selected_architecture_id"] == (
        "two_pass_blind_inventory_then_realization"
    )
    assert ranking["architectures"][0]["measured_result"]["dense_event_count"] == 23
    assert design["production_cost_projection"][
        "projected_production_amortized_total_token_ratio"
    ] < 0.28
    assert "realization" not in frozen
    assert not (root / "realization-continuation-lock.json").exists()
    client = object.__new__(ReserveCapacityGatedCodexAppServerClient)
    client.policy = v234.reserve.load_reserve_capacity_policy(
        frozen["capacity_policy"]
    )
    first_index, first_path = client._turn_index(
        frozen["inventory"]["paths"]["capacity"]
    )
    second_path = v234._turn_paths(
        root, frozen["realization_turn_name"]
    )["capacity"]
    second_index, observed_second_path = client._turn_index(second_path)
    assert (first_index, second_index) == (0, 1)
    assert first_path == frozen["inventory"]["paths"]["capacity"].resolve()
    assert observed_second_path == second_path.resolve()


class _FakeClient:
    def __init__(self, outputs: list[dict], usages: list[dict] | None = None) -> None:
        self.outputs = list(outputs)
        self.usages = list(usages or [_usage(), _usage()])
        self.calls = 0

    async def __aenter__(self) -> "_FakeClient":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def run_ephemeral_structured_turn(self, **kwargs: object) -> SimpleNamespace:
        output = self.outputs[self.calls]
        usage = self.usages[self.calls]
        self.calls += 1
        capacity_path = Path(str(kwargs["capacity_checkpoint_path"]))
        capacity_path.write_text(
            json.dumps(
                {
                    "schema_version": "pif_app_server_reserve_capacity_checkpoint_v1",
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
                    "model": v234.MODEL,
                    "effort": v234.EFFORT,
                    "error_class": None,
                    "usage": usage,
                }
            ),
            encoding="utf-8",
        )
        Path(str(kwargs["output_path"])).write_text(
            json.dumps(output), encoding="utf-8"
        )
        return SimpleNamespace(status_ok=True, output=output)


def test_fake_run_stops_before_realization_when_inventory_fails(tmp_path: Path) -> None:
    inventory_turn = _inventory_turn()
    fake = _FakeClient([_inventory_output(inventory_turn, dense_count=23)])
    root = tmp_path / "v234-stop"
    terminal = asyncio.run(
        v234.run_v234(output_dir=root, client_factory=lambda _policy: fake)
    )
    assert fake.calls == 1
    assert terminal["architecture_strategy_rejected"] is True
    assert terminal["next_distinct_architecture_authorized"] is True
    assert terminal["semantic_attempt_count"] == 1
    assert terminal["usage_status"] == "complete"
    assert not (root / "realization-continuation-lock.json").exists()


def test_fake_run_freezes_both_measured_turns_and_structural_pass(tmp_path: Path) -> None:
    inventory_turn = _inventory_turn()
    inventory_output = _inventory_output(inventory_turn)
    inventory, _, _ = v234._project_inventory(inventory_output, inventory_turn)
    packet = v234._source_packet(v234._validate_lineage())
    realization = v234._prepare_realization_turn(packet, inventory)
    realization_output = _realization_output(realization, inventory)
    fake = _FakeClient([inventory_output, realization_output])
    root = tmp_path / "v234-pass"
    terminal = asyncio.run(
        v234.run_v234(output_dir=root, client_factory=lambda _policy: fake)
    )
    assert fake.calls == 2
    assert terminal["state"] == "v234_architecture_structural_gate_passed"
    assert terminal["semantic_attempt_count"] == 2
    assert terminal["semantic_retry_count"] == 0
    assert terminal["usage"]["total_tokens"] == 60_000
    assert terminal["support_alignment_authorized"] is True
    assert terminal["holdout_authorized"] is False
    assert terminal["production_mutated"] is False
    v234.verify_continuation_lock(root / "realization-continuation-lock.json")
