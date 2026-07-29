from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import app_server_judge_v5_selection_v244_source_indexed_graph as v244
from research_factory.app_server_capacity_reserve import (
    ReserveCapacityGatedCodexAppServerClient,
)


def _prepared() -> tuple[dict, dict]:
    lineage = v244._validate_lineage()
    return v244._prepare_turn(lineage), v244._load_json(
        next(v244.V239_ROOT.glob("turns/*/output.private.json")), "v239 output"
    )


def _output(turn: dict, prior: dict, *, dense_limit: int | None = None) -> dict:
    rows = []
    for position, (source, old_segment) in enumerate(
        zip(turn["private_input"]["segments"], prior["segments"])
    ):
        source_events = list(old_segment["events"])
        if source["segment_id"] == v244.NO_SIGNAL_SEGMENT_ID:
            source_events = []
        if dense_limit is not None and source["segment_id"] == v244.DENSE_SEGMENT_ID:
            source_events = source_events[:dense_limit]
        units = list(source["units"])
        unit_index = {str(unit["unit_id"]): index for index, unit in enumerate(units)}
        events = []
        for event_position, old_event in enumerate(source_events):
            event = {
                key: copy.deepcopy(value)
                for key, value in old_event.items()
                if key not in v244.METRIC_LITERAL_FIELDS
            }
            event["event_id"] = f"S{position}G{event_position:03d}"
            start = unit_index[str(old_event["evidence_start_unit_id"])]
            end = unit_index[str(old_event["evidence_end_unit_id"])]
            spans = []
            for field in v244.METRIC_LITERAL_FIELDS:
                value = str(old_event.get(field) or "")
                if not value:
                    continue
                unit = next(
                    (item for item in units[start : end + 1] if value in str(item["text"])),
                    None,
                )
                if unit is not None:
                    spans.append(
                        {"field": field, "unit_id": unit["unit_id"], "exact_text": value}
                    )
            if spans and not any(item["field"] == "metric_raw_text" for item in spans):
                spans = []
            event["metric_literal_spans"] = spans
            event["metric_direction"] = "unknown" if spans else "not_applicable"
            events.append(event)
        rows.append(
            {
                "segment_id": source["segment_id"],
                "status": "coded" if events else "no_signal",
                "segment_source_context": old_segment["segment_source_context"],
                "no_signal_reason": "" if events else "No eligible event was found.",
                "coverage_audit": {
                    "all_source_units_reviewed": True,
                    "unresolved_count": 0,
                },
                "events": events,
            }
        )
    return {"episode_id": turn["episode_id"], "segments": rows}


def _usage(total: int = 33_000) -> dict:
    return {
        "input_tokens": total - 8_000,
        "cached_input_tokens": 0,
        "output_tokens": 8_000,
        "reasoning_output_tokens": 2_000,
        "total_tokens": total,
    }


def test_request_is_blind_source_indexed_and_within_caps() -> None:
    turn, _prior = _prepared()
    assert "density_stratum" not in turn["prompt"]
    assert "target count" in turn["base"]
    assert turn["prompt_bytes"] < 10_000
    assert turn["base_bytes"] < v244.MAX_BASE_BYTES
    assert turn["schema_bytes"] < v244.MAX_SCHEMA_BYTES
    event = turn["schema"]["properties"]["segments"]["items"]["properties"][
        "events"
    ]["items"]
    assert "metric_literal_spans" in event["properties"]
    for field in v244.METRIC_LITERAL_FIELDS:
        assert field not in event["properties"]


def test_projection_expands_exact_metric_spans_and_passes_gate() -> None:
    turn, prior = _prepared()
    normalized, provenance, diagnostics, receipts = v244._project_output(
        _output(turn, prior), turn
    )
    assert [len(row["events"]) for row in normalized["segments"]] == [31, 0]
    assert len(provenance["events"]) == 31
    assert sum(len(row["spans"]) for row in receipts["events"]) > 0
    gate = v244._gate(usage=_usage(), diagnostics=diagnostics)
    assert gate["passed"] is True
    assert gate["production_amortized_total_token_ratio"] < 0.28


@pytest.mark.parametrize("mutation", ["duplicate", "outside", "inexact", "missing_raw"])
def test_projection_rejects_invalid_source_span_graph(mutation: str) -> None:
    turn, prior = _prepared()
    output = _output(turn, prior)
    target = next(
        event
        for event in output["segments"][0]["events"]
        if event["metric_literal_spans"]
    )
    spans = target["metric_literal_spans"]
    if mutation == "duplicate":
        spans.append(copy.deepcopy(spans[0]))
        expected = "field span duplicate"
    elif mutation == "outside":
        no_signal_unit = turn["private_input"]["segments"][1]["units"][0]
        spans[0]["unit_id"] = no_signal_unit["unit_id"]
        spans[0]["exact_text"] = no_signal_unit["text"][:8]
        expected = "outside event evidence"
    elif mutation == "inexact":
        spans[0]["exact_text"] = "not an exact source substring"
        expected = "span is not exact"
    else:
        target["metric_literal_spans"] = [
            row for row in spans if row["field"] != "metric_raw_text"
        ]
        expected = "omit raw text"
    with pytest.raises(v244.V244OutputContractError, match=expected):
        v244._project_output(output, turn)


def test_dense_floor_failure_is_predeclared() -> None:
    turn, prior = _prepared()
    _normalized, _provenance, diagnostics, _receipts = v244._project_output(
        _output(turn, prior, dense_limit=23), turn
    )
    gate = v244._gate(usage=_usage(), diagnostics=diagnostics)
    assert gate["passed"] is False
    assert gate["failed_checks"] == ["dense_event_count_gte_24"]


def test_freeze_binds_graph_runtime_capacity_and_request(tmp_path: Path) -> None:
    root = tmp_path / "v244"
    frozen = v244.freeze_v244(output_dir=root)
    lock = v244.verify_runtime_lock(frozen["runtime_lock"])
    assert lock["declared_turn_count"] == 1
    assert lock["retry_count"] == 0
    assert lock["model"] == v244.MODEL
    assert lock["effort"] == "high"
    design = v244._load_json(frozen["design_path"], "design")
    assert design["production_cost_projection"][
        "projected_production_amortized_total_token_ratio"
    ] < 0.28
    client = object.__new__(ReserveCapacityGatedCodexAppServerClient)
    client.policy = v244.reserve.load_reserve_capacity_policy(frozen["capacity_policy"])
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
                    "model": v244.MODEL,
                    "effort": v244.EFFORT,
                    "error_class": None,
                    "usage": _usage(),
                }
            ),
            encoding="utf-8",
        )
        Path(str(kwargs["output_path"])).write_text(json.dumps(self.output), encoding="utf-8")
        return SimpleNamespace(status_ok=True, output=self.output)


def test_fake_run_is_one_measured_graph_turn_and_authorizes_support(tmp_path: Path) -> None:
    turn, prior = _prepared()
    fake = _FakeClient(_output(turn, prior))
    terminal = asyncio.run(
        v244.run_v244(output_dir=tmp_path / "v244-run", client_factory=lambda _policy: fake)
    )
    assert fake.calls == 1
    assert terminal["state"] == "v244_architecture_structural_gate_passed"
    assert terminal["semantic_retry_count"] == 0
    assert terminal["usage_status"] == "complete"
    assert terminal["support_alignment_authorized"] is True
    assert terminal["holdout_authorized"] is False
    assert terminal["production_mutated"] is False
