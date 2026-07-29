from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import app_server_judge_v5_selection_v243_core_enrichment as v243
from research_factory.app_server_capacity_reserve import (
    ReserveCapacityGatedCodexAppServerClient,
)


def _prepared_core() -> tuple[dict, dict, dict]:
    lineage = v243._validate_lineage()
    turn = v243._prepare_core_turn(lineage)
    source = v243._load_json(v243.v242.V239_TURN_ROOT / "output.private.json", "v239")
    return lineage, turn, source


def _core_output(turn: dict, source: dict, *, dense_limit: int | None = None) -> dict:
    rows = []
    for position, (private, prior) in enumerate(
        zip(turn["private_input"]["segments"], source["segments"])
    ):
        source_events = list(prior["events"])
        if private["segment_id"] == v243.NO_SIGNAL_SEGMENT_ID:
            source_events = []
        if dense_limit is not None and private["segment_id"] == v243.DENSE_SEGMENT_ID:
            source_events = source_events[:dense_limit]
        events = [
            {
                "event_id": f"S{position}E{index:03d}",
                **{
                    name: copy.deepcopy(event[name])
                    for name in v243.CORE_FIELDS
                },
            }
            for index, event in enumerate(source_events)
        ]
        rows.append(
            {
                "segment_id": private["segment_id"],
                "status": "coded" if events else "no_signal",
                "segment_source_context": prior["segment_source_context"],
                "no_signal_reason": "" if events else "No eligible event was found.",
                "coverage_audit": {
                    "all_source_units_reviewed": True,
                    "unresolved_count": 0,
                },
                "events": events,
            }
        )
    return {"episode_id": turn["episode_id"], "segments": rows}


def _enrichment_output(enrichment: dict, source: dict) -> dict:
    rows = []
    for packet in enrichment["private_input"]["events"]:
        event_id = packet["core_event"]["event_id"]
        segment_position = int(event_id[1])
        event_position = int(event_id[3:])
        prior = source["segments"][segment_position]["events"][event_position]
        row = {
            "core_event_id": event_id,
            **{
                name: copy.deepcopy(prior[name])
                for name in v243.ENRICHMENT_FIELDS
            },
        }
        values = [
            str(row[name] or "")
            for name in ("metric_value", "metric_unit", "metric_comparator", "metric_raw_text")
        ]
        row["metric_direction"] = "unknown" if any(values) else "not_applicable"
        rows.append(row)
    return {"episode_id": enrichment["episode_id"], "events": rows}


def _usage(total: int) -> dict:
    return {
        "input_tokens": total - 3_000,
        "cached_input_tokens": 0,
        "output_tokens": 3_000,
        "reasoning_output_tokens": 500,
        "total_tokens": total,
    }


def test_core_request_is_blind_compact_and_covers_material_fields() -> None:
    _lineage, turn, _source = _prepared_core()
    assert "density_stratum" not in turn["prompt"]
    assert "target count" in turn["base"]
    assert set(v243.CORE_FIELDS).isdisjoint(v243.ENRICHMENT_FIELDS)
    assert len(set(v243.CORE_FIELDS) | set(v243.ENRICHMENT_FIELDS)) == 30
    assert turn["prompt_bytes"] < 10_000
    assert turn["base_bytes"] < v243.MAX_BASE_BYTES
    assert turn["schema_bytes"] < v243.MAX_SCHEMA_BYTES


def test_core_projection_and_conditional_gate() -> None:
    _lineage, turn, source = _prepared_core()
    core, diagnostics = v243._core_projection(_core_output(turn, source), turn)
    assert [len(row["events"]) for row in core["segments"]] == [31, 0]
    passing = v243._core_gate(usage=_usage(24_000), diagnostics=diagnostics)
    assert passing["passed"] is True
    assert passing["enrichment_authorized"] is True
    short_core, short_diagnostics = v243._core_projection(
        _core_output(turn, source, dense_limit=23), turn
    )
    assert len(short_core["segments"][0]["events"]) == 23
    failing = v243._core_gate(usage=_usage(24_000), diagnostics=short_diagnostics)
    assert failing["passed"] is False
    assert failing["failed_checks"] == ["dense_event_count_gte_24"]


def test_enrichment_schema_cannot_override_core_and_final_projection_passes() -> None:
    lineage, turn, source = _prepared_core()
    core, _diagnostics = v243._core_projection(_core_output(turn, source), turn)
    enrichment = v243._prepare_enrichment_turn(lineage["packet"], core)
    enrichment["packet"] = lineage["packet"]
    output = _enrichment_output(enrichment, source)
    normalized, provenance, diagnostics = v243._final_projection(output, core, enrichment)
    assert [len(row["events"]) for row in normalized["segments"]] == [31, 0]
    assert len(provenance["events"]) == 31
    gate = v243._final_gate(
        core_usage=_usage(24_000),
        enrichment_usage=_usage(20_000),
        diagnostics=diagnostics,
    )
    assert gate["passed"] is True
    assert gate["production_amortized_total_token_ratio"] < 0.28
    output["events"][0]["claim_text"] = "An attempted core override."
    with pytest.raises(v243.V243OutputContractError, match="enrichment output schema failed"):
        v243._final_projection(output, core, enrichment)


def test_final_projection_rejects_metric_applicability_conflict() -> None:
    lineage, turn, source = _prepared_core()
    core, _ = v243._core_projection(_core_output(turn, source), turn)
    enrichment = v243._prepare_enrichment_turn(lineage["packet"], core)
    enrichment["packet"] = lineage["packet"]
    output = _enrichment_output(enrichment, source)
    output["events"][0]["metric_value"] = ""
    output["events"][0]["metric_unit"] = ""
    output["events"][0]["metric_comparator"] = ""
    output["events"][0]["metric_raw_text"] = ""
    output["events"][0]["metric_direction"] = "increase"
    with pytest.raises(v243.V243OutputContractError, match="metric direction applicability"):
        v243._final_projection(output, core, enrichment)


def test_freeze_binds_two_turn_runtime_and_capacity_order(tmp_path: Path) -> None:
    root = tmp_path / "v243"
    frozen = v243.freeze_v243(output_dir=root)
    lock = v243.verify_runtime_lock(frozen["runtime_lock"])
    assert lock["declared_turn_count"] == 2
    assert lock["retry_count"] == 0
    assert lock["phase_total_token_bound"] == v243.MAX_COMBINED_TOKENS
    design = v243._load_json(frozen["design_path"], "design")
    assert design["production_cost_projection"][
        "projected_production_amortized_total_token_ratio"
    ] < 0.28
    policy = v243.reserve.load_reserve_capacity_policy(frozen["capacity_policy"])
    assert tuple(policy["ordered_turn_names"]) == (
        frozen["spec"]["core_turn_name"],
        frozen["spec"]["enrichment_turn_name"],
    )
    client = object.__new__(ReserveCapacityGatedCodexAppServerClient)
    client.policy = policy
    core_index, _ = client._turn_index(frozen["core"]["paths"]["capacity"])
    enrichment_paths = v243._turn_paths(root, frozen["spec"]["enrichment_turn_name"])
    enrichment_index, _ = client._turn_index(enrichment_paths["capacity"])
    assert (core_index, enrichment_index) == (0, 1)
    assert not frozen["core"]["paths"]["sidecar"].exists()
    assert not enrichment_paths["sidecar"].exists()


class _FakeClient:
    def __init__(self, outputs: list[dict], usages: list[int]) -> None:
        self.outputs = outputs
        self.usages = usages
        self.calls = 0

    async def __aenter__(self) -> "_FakeClient":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def run_ephemeral_structured_turn(self, **kwargs: object) -> SimpleNamespace:
        index = self.calls
        self.calls += 1
        output = self.outputs[index]
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
                    "model": v243.MODEL,
                    "effort": v243.EFFORT,
                    "error_class": None,
                    "usage": _usage(self.usages[index]),
                }
            ),
            encoding="utf-8",
        )
        Path(str(kwargs["output_path"])).write_text(json.dumps(output), encoding="utf-8")
        return SimpleNamespace(status_ok=True, output=output)


def test_fake_run_uses_one_process_two_measured_turns_and_authorizes_support(
    tmp_path: Path,
) -> None:
    lineage, core_turn, source = _prepared_core()
    core_output = _core_output(core_turn, source)
    core, _ = v243._core_projection(core_output, core_turn)
    enrichment = v243._prepare_enrichment_turn(lineage["packet"], core)
    fake = _FakeClient(
        [core_output, _enrichment_output(enrichment, source)],
        [24_000, 20_000],
    )
    terminal = asyncio.run(
        v243.run_v243(output_dir=tmp_path / "v243-run", client_factory=lambda _policy: fake)
    )
    assert fake.calls == 2
    assert terminal["state"] == "v243_architecture_structural_gate_passed"
    assert terminal["semantic_retry_count"] == 0
    assert terminal["combined_tokens"] == 44_000
    assert terminal["support_alignment_authorized"] is True
    assert terminal["holdout_authorized"] is False
    assert terminal["production_mutated"] is False


def test_failed_core_gate_prevents_enrichment_turn(tmp_path: Path) -> None:
    _lineage, core_turn, source = _prepared_core()
    fake = _FakeClient([_core_output(core_turn, source, dense_limit=23)], [24_000])
    terminal = asyncio.run(
        v243.run_v243(output_dir=tmp_path / "v243-stop", client_factory=lambda _policy: fake)
    )
    assert fake.calls == 1
    assert terminal["terminal_reason"] == (
        "v243_core_enrichment_structural_quality_or_cost_gate_not_passed"
    )
    assert terminal["semantic_attempt_count"] == 1
    assert terminal["support_alignment_authorized"] is False
    assert not (tmp_path / "v243-stop" / "enrichment-request-lock.json").exists()
