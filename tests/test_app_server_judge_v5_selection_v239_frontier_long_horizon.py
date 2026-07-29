from __future__ import annotations

import asyncio
import copy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

from research_factory import app_server_judge_v5_selection_v239_frontier_long_horizon as v239
from research_factory.app_server_capacity_reserve import (
    ReserveCapacityGatedCodexAppServerClient,
)


def _prepared_turn() -> dict:
    return v239._prepare_turn(v239._validate_lineage())


def _context() -> dict:
    return {
        "kind": "substantive_dialogue",
        "confidence": 1.0,
        "rationale": "fixture",
    }


def _template_event() -> dict:
    source = v239._load_json(v239.v233.SOURCE_TURN_ROOT / "input.private.json", "source")
    dense = next(
        row
        for row in source["normalization_segments"]
        if row["segment_id"] == v239.DENSE_SEGMENT_ID
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


def _output(turn: dict, dense_event_count: int = 24, residual_count: int = 0) -> dict:
    template = _template_event()
    rows = []
    for source in turn["private_input"]["segments"]:
        units = source["units"]
        count = (
            dense_event_count
            if source["segment_id"] == v239.DENSE_SEGMENT_ID
            else residual_count
        )
        receipts = [
            {"unit_id": unit["unit_id"], "eligible_event_count": 0, "unresolved_count": 0}
            for unit in units
        ]
        events = []
        for index, unit in enumerate(units[:count]):
            event = copy.deepcopy(template)
            event["claim_text"] = f"{event['claim_text']} frontier-{index}-{source['segment_id']}"
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


def _usage(total: int = 35_000) -> dict:
    return {
        "input_tokens": total - 9_000,
        "cached_input_tokens": 0,
        "output_tokens": 9_000,
        "reasoning_output_tokens": 1_000,
        "total_tokens": total,
    }


def test_request_is_byte_identical_to_v233_and_only_effort_changes() -> None:
    turn = _prepared_turn()
    lineage = v239._validate_lineage()
    assert hashlib.sha256(turn["prompt"].encode()).hexdigest() == v239.EXPECTED_LINEAGE_HASHES["v233_prompt"]
    assert hashlib.sha256(turn["base"].encode()).hexdigest() == v239.EXPECTED_LINEAGE_HASHES["v233_base"]
    assert turn["private_input"] == v239._load_json(lineage["paths"]["v233_input"], "input")
    assert v239.MODEL == v239.v233.MODEL
    assert v239.EFFORT == "high"
    assert v239.v233.EFFORT == "low"


def test_projection_and_gate_pass_with_twenty_four_dense_events() -> None:
    turn = _prepared_turn()
    normalized, provenance, diagnostics = v239.v233._project_output(_output(turn), turn)
    assert len(normalized["segments"][0]["events"]) == 24
    assert len(provenance["events"]) == 24
    gate = v239._gate(usage=_usage(), diagnostics=diagnostics)
    assert gate["passed"] is True
    assert gate["production_amortized_total_token_ratio"] < 0.28


def test_gate_routes_residual_to_support_but_still_rejects_dense_shortfall() -> None:
    turn = _prepared_turn()
    _, _, diagnostics = v239.v233._project_output(
        _output(turn, dense_event_count=23, residual_count=1), turn
    )
    gate = v239._gate(usage=_usage(), diagnostics=diagnostics)
    assert gate["passed"] is False
    assert gate["failed_checks"] == ["dense_event_count_gte_24"]
    assert gate["residual_support_audit_required"] is True


def test_freeze_binds_runtime_lineage_request_and_capacity(tmp_path: Path) -> None:
    root = tmp_path / "v239"
    frozen = v239.freeze_v239(output_dir=root)
    lock = v239.verify_runtime_lock(frozen["runtime_lock"])
    assert lock["declared_turn_count"] == 1
    assert lock["retry_count"] == 0
    assert frozen["spec"]["request_byte_identical_to_v233"] is True
    design = v239._load_json(frozen["design_path"], "design")
    assert design["production_cost_projection"]["projected_production_amortized_total_token_ratio"] < 0.28
    client = object.__new__(ReserveCapacityGatedCodexAppServerClient)
    client.policy = v239.reserve.load_reserve_capacity_policy(frozen["capacity_policy"])
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
        assert kwargs["effort"] == "high"
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
                    "model": v239.MODEL,
                    "effort": v239.EFFORT,
                    "error_class": None,
                    "usage": _usage(),
                }
            ),
            encoding="utf-8",
        )
        Path(str(kwargs["output_path"])).write_text(json.dumps(self.output), encoding="utf-8")
        return SimpleNamespace(status_ok=True, output=self.output)


def test_fake_run_is_one_measured_turn_and_authorizes_support(tmp_path: Path) -> None:
    turn = _prepared_turn()
    fake = _FakeClient(_output(turn))
    terminal = asyncio.run(
        v239.run_v239(output_dir=tmp_path / "v239-run", client_factory=lambda _policy: fake)
    )
    assert fake.calls == 1
    assert terminal["state"] == "v239_architecture_structural_gate_passed"
    assert terminal["semantic_retry_count"] == 0
    assert terminal["usage_status"] == "complete"
    assert terminal["support_alignment_authorized"] is True
    assert terminal["holdout_authorized"] is False
    assert terminal["production_mutated"] is False
