from __future__ import annotations

import copy
from pathlib import Path

import pytest

from research_factory import app_server_judge_v5_selection_v232_window_ledger as v232
from research_factory.app_server_capacity_reserve import (
    ReserveCapacityGatedCodexAppServerClient,
)


def _prepared_turns() -> list[dict]:
    return v232._prepare_turns(v232._validate_lineage())


def _context(kind: str = "substantive_dialogue") -> dict:
    return {"kind": kind, "confidence": 1.0, "rationale": "fixture"}


def _empty_receipts() -> list[dict]:
    return [
        {
            "window_id": window_id,
            "eligible_atomic_claim_count": 0,
            "already_represented_count": 0,
            "new_event_count": 0,
            "unresolved_count": 0,
        }
        for window_id in range(4)
    ]


def _event_for_unit(segment: dict, unit: dict) -> dict:
    event = copy.deepcopy(segment["existing_events"][0])
    event.pop("window_id", None)
    event.pop("evidence", None)
    event["claim_text"] = str(event["claim_text"]) + " fixture-distinct"
    event["metric_value"] = ""
    event["metric_unit"] = ""
    event["metric_comparator"] = ""
    event["metric_raw_text"] = ""
    event["metric_direction"] = "not_applicable"
    event["evidence_start_unit_id"] = unit["unit_id"]
    event["evidence_end_unit_id"] = unit["unit_id"]
    return event


def _output_with_one_event(turn: dict) -> tuple[dict, str]:
    dense = next(
        row
        for row in turn["private_input"]["segments"]
        if row["existing_events"]
    )
    unit = dense["units"][0]
    event = _event_for_unit(dense, unit)
    rows = []
    for segment_id in turn["segment_ids"]:
        receipts = _empty_receipts()
        events = []
        status = "no_signal"
        reason = "No unrepresented event remains."
        if segment_id == dense["segment_id"]:
            receipts[int(unit["window_id"])]["eligible_atomic_claim_count"] = 1
            receipts[int(unit["window_id"])]["new_event_count"] = 1
            events = [event]
            status = "coded"
            reason = ""
        rows.append(
            {
                "segment_id": segment_id,
                "status": status,
                "segment_source_context": _context(),
                "no_signal_reason": reason,
                "window_receipts": receipts,
                "events": events,
            }
        )
    return {"episode_id": turn["episode_id"], "segments": rows}, dense["segment_id"]


def test_turns_are_source_first_reference_blind_and_unit_projectable() -> None:
    turns = _prepared_turns()
    assert len(turns) == 2
    assert sum(len(turn["segment_ids"]) for turn in turns) == 4
    for turn in turns:
        assert "density_stratum" not in turn["prompt"]
        assert "minimum_new_for_possible_pass" not in turn["prompt"]
        assert turn["prompt"].index('"segments"') < turn["prompt"].index(
            '"existing_event_ledger"'
        )
        event_schema = turn["schema"]["properties"]["segments"]["items"][
            "properties"
        ]["events"]["items"]
        assert "evidence" not in event_schema["properties"]
        assert "window_id" not in event_schema["properties"]
        assert "evidence_start_unit_id" in event_schema["required"]
        assert "evidence_end_unit_id" in event_schema["required"]
        for segment in turn["private_input"]["segments"]:
            text = segment["segment_text"]
            for unit in segment["units"]:
                assert text[unit["start_char"] : unit["end_char"]] == unit["text"]


def test_projection_adds_exact_evidence_offsets_and_owner() -> None:
    turn = _prepared_turns()[0]
    output, dense_id = _output_with_one_event(turn)
    normalized, provenance, diagnostics = v232._project_output(output, turn)
    dense = next(row for row in normalized["segments"] if row["segment_id"] == dense_id)
    assert len(dense["events"]) == 1
    event = dense["events"][0]
    receipt = provenance["events"][0]
    source = next(
        row
        for row in turn["private_input"]["segments"]
        if row["segment_id"] == dense_id
    )
    assert event["evidence"] == source["segment_text"][
        receipt["start_char"] : receipt["end_char"]
    ]
    assert event["window_id"] == receipt["window_id"]
    assert sum(row["new_event_count"] for row in diagnostics) == 1


def test_projection_rejects_unresolved_metric_and_exact_duplicate() -> None:
    turn = _prepared_turns()[0]
    output, dense_id = _output_with_one_event(turn)
    unresolved = copy.deepcopy(output)
    dense_row = next(row for row in unresolved["segments"] if row["segment_id"] == dense_id)
    receipt = next(row for row in dense_row["window_receipts"] if row["new_event_count"])
    receipt["eligible_atomic_claim_count"] += 1
    receipt["unresolved_count"] = 1
    with pytest.raises(v232.V232OutputContractError, match="unresolved"):
        v232._project_output(unresolved, turn)

    bad_metric = copy.deepcopy(output)
    event = next(
        row for row in bad_metric["segments"] if row["segment_id"] == dense_id
    )["events"][0]
    event["metric_value"] = "not-in-selected-source"
    event["metric_direction"] = "increase"
    with pytest.raises(v232.V232OutputContractError, match="metric literal"):
        v232._project_output(bad_metric, turn)

    duplicate = copy.deepcopy(output)
    source = next(
        row
        for row in turn["private_input"]["segments"]
        if row["segment_id"] == dense_id
    )
    existing = copy.deepcopy(source["existing_events"][0])
    evidence = existing.pop("evidence")
    existing.pop("window_id", None)
    start = source["segment_text"].index(evidence)
    end = start + len(evidence)
    covered = [
        unit
        for unit in source["units"]
        if unit["end_char"] > start and unit["start_char"] < end
    ]
    existing["evidence_start_unit_id"] = covered[0]["unit_id"]
    existing["evidence_end_unit_id"] = covered[-1]["unit_id"]
    row = next(
        item for item in duplicate["segments"] if item["segment_id"] == dense_id
    )
    row["events"] = [existing]
    owner = int(covered[0]["window_id"])
    row["window_receipts"] = _empty_receipts()
    row["window_receipts"][owner]["eligible_atomic_claim_count"] = 1
    row["window_receipts"][owner]["new_event_count"] = 1
    with pytest.raises(v232.V232OutputContractError, match="identity duplicate"):
        v232._project_output(duplicate, turn)


def test_count_ceiling_requires_both_dense_sources() -> None:
    rows = [
        {"segment_id": segment_id, "new_event_count": 0, "unresolved_count": 0}
        for segment_id in v232.NO_SIGNAL_SEGMENT_IDS
    ]
    for segment_id, basis in v232.DENSE_CASES.items():
        rows.append(
            {
                "segment_id": segment_id,
                "new_event_count": basis["minimum_new_for_possible_pass"],
                "unresolved_count": 0,
            }
        )
    ceiling = v232._count_ceiling(rows)
    assert ceiling["passes_frozen_0_97_floor"] is True
    failed = copy.deepcopy(rows)
    target = next(row for row in failed if row["segment_id"] in v232.DENSE_CASES)
    target["new_event_count"] -= 1
    assert v232._count_ceiling(failed)["passes_frozen_0_97_floor"] is False


def test_freeze_binds_exact_runtime_and_capacity_paths(tmp_path: Path) -> None:
    root = tmp_path / "v232"
    frozen = v232.freeze_v232(output_dir=root)
    lock = v232.verify_runtime_lock(frozen["runtime_lock"])
    assert lock["declared_turn_count"] == 2
    assert lock["retry_count"] == 0
    assert frozen["spec"]["reference_visible_to_model"] is False
    design = v232._load_json(frozen["design_path"], "design")
    assert design["cost_projection"]["projected_ratio_at_hard_max"] < 0.28
    client = object.__new__(ReserveCapacityGatedCodexAppServerClient)
    client.policy = v232.reserve.load_reserve_capacity_policy(
        frozen["capacity_policy"]
    )
    for index, turn in enumerate(frozen["turns"]):
        observed_index, observed_path = client._turn_index(turn["paths"]["capacity"])
        assert observed_index == index
        assert observed_path == turn["paths"]["capacity"].resolve()
        assert not turn["paths"]["capacity"].exists()
        assert not turn["paths"]["sidecar"].exists()
