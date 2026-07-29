from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import app_server_judge_v5_selection_v237_opaque_coverage_gap as v237
from research_factory.app_server_capacity_reserve import (
    ReserveCapacityGatedCodexAppServerClient,
)


def _prepared_turn() -> dict:
    return v237._prepare_turn(v237._validate_lineage())


def _context() -> dict:
    return {
        "kind": "substantive_dialogue",
        "confidence": 1.0,
        "rationale": "fixture",
    }


def _template_event() -> dict:
    turn = _prepared_turn()
    dense = next(
        row
        for row in turn["private_input"]["segments"]
        if row["segment_id"] == v237.DENSE_SEGMENT_ID
    )
    event = copy.deepcopy(dense["base_events"][0])
    event.pop("window_id", None)
    event.pop("evidence", None)
    event["metric_value"] = ""
    event["metric_unit"] = ""
    event["metric_comparator"] = ""
    event["metric_raw_text"] = ""
    event["metric_direction"] = "not_applicable"
    return event


def _output(turn: dict, dense_gap_count: int = 13) -> dict:
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
        if source["segment_id"] == v237.DENSE_SEGMENT_ID:
            uncovered = [
                (index, unit)
                for index, unit in enumerate(units)
                if not source["coverage_by_unit"][unit["unit_id"]]
            ]
            assert dense_gap_count <= len(uncovered)
            for event_index, (unit_index, unit) in enumerate(
                uncovered[:dense_gap_count]
            ):
                event = copy.deepcopy(template)
                event["claim_text"] = (
                    f"{event['claim_text']} opaque-gap-{event_index}"
                )
                event["evidence_start_unit_id"] = unit["unit_id"]
                event["evidence_end_unit_id"] = unit["unit_id"]
                events.append(event)
                receipts[unit_index]["eligible_event_count"] = 1
        rows.append(
            {
                "segment_id": source["segment_id"],
                "status": "coded" if events else "no_signal",
                "segment_source_context": _context(),
                "no_signal_reason": "" if events else "No new gap event is present.",
                "unit_receipts": receipts,
                "coverage_audit": {
                    "all_source_units_reviewed": True,
                    "unresolved_count": 0,
                },
                "events": events,
            }
        )
    return {"episode_id": turn["episode_id"], "segments": rows}


def _project_and_merge(turn: dict, output: dict) -> tuple[dict, list[dict]]:
    gap, provenance, _ = v237.v233._project_output(output, turn)
    return v237._merge_gap(gap=gap, provenance=provenance, turn=turn)


def _usage(total: int = 30_000) -> dict:
    return {
        "input_tokens": total - 8_000,
        "cached_input_tokens": 0,
        "output_tokens": 8_000,
        "reasoning_output_tokens": 500,
        "total_tokens": total,
    }


def test_prompt_exposes_only_opaque_coverage_and_fits_size_caps() -> None:
    turn = _prepared_turn()
    packet = json.loads(turn["prompt"].split("\n", 1)[1])
    assert "base_events" not in turn["prompt"]
    assert "existing_events" not in turn["prompt"]
    assert "claim_text" not in turn["prompt"]
    assert "density_stratum" not in turn["prompt"]
    assert "reference" not in turn["prompt"].lower()
    assert any(
        unit["base_coverage_ids"]
        for segment in packet["segments"]
        for unit in segment["source_units"]
    )
    counts = {
        row["segment_id"]: len(row["base_events"])
        for row in turn["private_input"]["segments"]
    }
    assert counts == {
        v237.DENSE_SEGMENT_ID: 11,
        v237.NO_SIGNAL_SEGMENT_ID: 1,
    }
    assert turn["prompt_bytes"] <= v237.MAX_PROMPT_BYTES
    assert turn["base_bytes"] <= v237.MAX_BASE_BYTES
    assert turn["schema_bytes"] <= v237.MAX_SCHEMA_BYTES


def test_projection_merges_thirteen_uncovered_events_with_frozen_base() -> None:
    turn = _prepared_turn()
    merged, diagnostics = _project_and_merge(turn, _output(turn))
    by_id = {row["segment_id"]: row for row in merged["segments"]}
    assert len(by_id[v237.DENSE_SEGMENT_ID]["events"]) == 24
    assert len(by_id[v237.NO_SIGNAL_SEGMENT_ID]["events"]) == 1
    gate = v237._gate(usage=_usage(), diagnostics=diagnostics)
    assert gate["passed"] is True
    assert gate["residual_support_audit_required"] is True
    assert gate["production_amortized_total_token_ratio"] < 0.28


def test_merge_rejects_gap_start_owned_by_frozen_base() -> None:
    turn = _prepared_turn()
    output = _output(turn, dense_gap_count=1)
    source = turn["private_input"]["segments"][0]
    covered_index, covered_unit = next(
        (index, unit)
        for index, unit in enumerate(source["units"])
        if source["coverage_by_unit"][unit["unit_id"]]
    )
    row = output["segments"][0]
    prior_start = row["events"][0]["evidence_start_unit_id"]
    prior_index = next(
        index
        for index, receipt in enumerate(row["unit_receipts"])
        if receipt["unit_id"] == prior_start
    )
    row["unit_receipts"][prior_index]["eligible_event_count"] = 0
    row["unit_receipts"][covered_index]["eligible_event_count"] = 1
    row["events"][0]["evidence_start_unit_id"] = covered_unit["unit_id"]
    row["events"][0]["evidence_end_unit_id"] = covered_unit["unit_id"]
    gap, provenance, _ = v237.v233._project_output(output, turn)
    with pytest.raises(v237.V237OutputContractError, match="base-owned"):
        v237._merge_gap(gap=gap, provenance=provenance, turn=turn)


def test_freeze_binds_runtime_lineage_and_capacity_path(tmp_path: Path) -> None:
    root = tmp_path / "v237"
    frozen = v237.freeze_v237(output_dir=root)
    lock = v237.verify_runtime_lock(frozen["runtime_lock"])
    assert lock["declared_turn_count"] == 1
    assert lock["retry_count"] == 0
    assert frozen["spec"]["base_semantics_visible_to_model"] is False
    design = v237._load_json(frozen["design_path"], "design")
    assert (
        design["production_cost_projection"]
        ["projected_production_amortized_total_token_ratio"]
        < 0.28
    )
    client = object.__new__(ReserveCapacityGatedCodexAppServerClient)
    client.policy = v237.reserve.load_reserve_capacity_policy(
        frozen["capacity_policy"]
    )
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
            json.dumps(
                {
                    "schema_version": "pif_app_server_reserve_capacity_checkpoint_v1",
                    "cleared_for_semantic_turn": True,
                    "managed_chatgpt_auth_verified": True,
                }
            ),
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
                    "model": v237.MODEL,
                    "effort": v237.EFFORT,
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


def test_fake_run_is_one_measured_turn_and_authorizes_support(tmp_path: Path) -> None:
    turn = _prepared_turn()
    fake = _FakeClient(_output(turn))
    terminal = asyncio.run(
        v237.run_v237(
            output_dir=tmp_path / "v237-run",
            client_factory=lambda _policy: fake,
        )
    )
    assert fake.calls == 1
    assert terminal["state"] == "v237_architecture_structural_gate_passed"
    assert terminal["semantic_attempt_count"] == 1
    assert terminal["semantic_retry_count"] == 0
    assert terminal["usage_status"] == "complete"
    assert terminal["support_alignment_authorized"] is True
    assert terminal["residual_support_audit_required"] is True
    assert terminal["holdout_authorized"] is False
    assert terminal["production_mutated"] is False
