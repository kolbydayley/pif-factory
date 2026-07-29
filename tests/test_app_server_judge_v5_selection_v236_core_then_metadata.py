from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import app_server_judge_v5_selection_v236_core_then_metadata as v236
from research_factory.app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient


def _core_turn() -> dict:
    return v236._prepare_core_turn(v236._validate_lineage())


def _source_template() -> dict:
    source = v236._load_json(v236.SOURCE_TURN_ROOT / "input.private.json", "source")
    dense = next(row for row in source["normalization_segments"] if row["segment_id"] == v236.DENSE_SEGMENT_ID)
    return copy.deepcopy(dense["existing_events"][0])


def _core_output(turn: dict, dense_count: int = 24, residual_count: int = 0) -> dict:
    template = _source_template()
    rows = []
    for position, source in enumerate(turn["private_input"]["segments"]):
        count = dense_count if source["segment_id"] == v236.DENSE_SEGMENT_ID else residual_count
        units = source["units"]
        assert count <= len(units)
        receipts = [{"unit_id": unit["unit_id"], "eligible_event_count": 0, "unresolved_count": 0} for unit in units]
        events = []
        for index, unit in enumerate(units[:count]):
            event = copy.deepcopy(template)
            for field in (*v236.ENRICHMENT_FIELDS, "window_id", "evidence"):
                event.pop(field, None)
            event["core_event_id"] = f"S{position}E{index:03d}"
            event["claim_text"] = f"{event['claim_text']} core-fixture-{position}-{index}"
            event["metric_value"] = ""
            event["metric_unit"] = ""
            event["metric_comparator"] = ""
            event["metric_raw_text"] = ""
            event["metric_direction"] = "not_applicable"
            event["evidence_start_unit_id"] = unit["unit_id"]
            event["evidence_end_unit_id"] = unit["unit_id"]
            events.append(event)
            receipts[index]["eligible_event_count"] = 1
        rows.append({
            "segment_id": source["segment_id"],
            "status": "coded" if events else "no_signal",
            "segment_source_context": {"kind": "substantive_dialogue", "confidence": 1.0, "rationale": "fixture"},
            "no_signal_reason": "" if events else "No eligible core event.",
            "unit_receipts": receipts,
            "coverage_audit": {"all_source_units_reviewed": True, "unresolved_count": 0},
            "events": events,
        })
    return {"episode_id": turn["episode_id"], "segments": rows}


def _metadata_output(turn: dict, core: dict) -> dict:
    template = _source_template()
    rows = []
    for row in core["segments"]:
        events = []
        for event in row["events"]:
            events.append({
                "core_event_id": event["core_event_id"],
                **{field: copy.deepcopy(template[field]) for field in v236.ENRICHMENT_FIELDS},
            })
        rows.append({"segment_id": row["segment_id"], "events": events})
    return {"episode_id": turn["episode_id"], "segments": rows}


def _usage(total: int = 25_000) -> dict:
    return {"input_tokens": total - 6_000, "cached_input_tokens": 0, "output_tokens": 6_000, "reasoning_output_tokens": 300, "total_tokens": total}


def test_core_schema_removes_only_noncritical_metadata_and_prompt_is_blind() -> None:
    turn = _core_turn()
    event = turn["schema"]["properties"]["segments"]["items"]["properties"]["events"]["items"]
    for field in v236.ENRICHMENT_FIELDS:
        assert field not in event["properties"]
    for field in ("claim_text", "actor_name", "stance", "metric_direction"):
        assert field in event["properties"]
    assert "existing_events" not in turn["prompt"]
    assert "density_stratum" not in turn["prompt"]
    assert "reference_count" not in turn["prompt"]


def test_core_projection_and_gate_validate_coverage_exactness_and_residual_routing() -> None:
    turn = _core_turn()
    core, provenance, diagnostics = v236._project_core(_core_output(turn), turn)
    assert len(core["segments"][0]["events"]) == 24
    assert len(provenance["events"]) == 24
    gate = v236._core_gate(diagnostics)
    assert gate["passed"] is True
    assert gate["residual_support_audit_required"] is False

    residual_core, _, residual_diagnostics = v236._project_core(_core_output(turn, residual_count=1), turn)
    assert len(residual_core["segments"][1]["events"]) == 1
    residual_gate = v236._core_gate(residual_diagnostics)
    assert residual_gate["passed"] is True
    assert residual_gate["residual_support_audit_required"] is True

    sparse = _core_output(turn, dense_count=23)
    _, _, sparse_diagnostics = v236._project_core(sparse, turn)
    assert v236._core_gate(sparse_diagnostics)["passed"] is False

    metric = _core_output(turn)
    metric["segments"][0]["events"][0]["metric_value"] = "not-in-evidence"
    metric["segments"][0]["events"][0]["metric_direction"] = "increase"
    with pytest.raises(v236.V236OutputContractError, match="metric literal"):
        v236._project_core(metric, turn)


def test_metadata_is_one_to_one_and_cannot_override_core() -> None:
    core_turn = _core_turn()
    core, _, _ = v236._project_core(_core_output(core_turn), core_turn)
    packet = v236._source_packet(v236._validate_lineage())
    metadata_turn = v236._prepare_metadata_turn(packet=packet, core=core)
    metadata = _metadata_output(metadata_turn, core)
    merged, diagnostics = v236._merge_metadata(
        output=metadata, turn=metadata_turn, core=core, source_schema=packet["source_schema"]
    )
    assert len(merged["segments"][0]["events"]) == 24
    assert diagnostics[0]["metadata_event_count"] == 24
    assert set(metadata["segments"][0]["events"][0]) == {"core_event_id", *v236.ENRICHMENT_FIELDS}

    omitted = copy.deepcopy(metadata)
    omitted["segments"][0]["events"].pop()
    with pytest.raises(v236.V236OutputContractError, match="coverage"):
        v236._merge_metadata(output=omitted, turn=metadata_turn, core=core, source_schema=packet["source_schema"])


def test_freeze_binds_ranked_architecture_conditional_turns_and_cost(tmp_path: Path) -> None:
    root = tmp_path / "v236"
    frozen = v236.freeze_v236(output_dir=root)
    lock = v236.verify_runtime_lock(frozen["runtime_lock"])
    design = v236._load_json(frozen["design_path"], "design")
    assert lock["declared_turn_count"] == 2
    assert lock["core_model"] == "gpt-5.6-sol"
    assert lock["enrichment_model"] == "gpt-5.4-mini"
    assert design["production_cost_projection"]["projected_production_amortized_total_token_ratio"] < 0.28
    assert "metadata" not in frozen
    client = object.__new__(ReserveCapacityGatedCodexAppServerClient)
    client.policy = v236.reserve.load_reserve_capacity_policy(frozen["capacity_policy"])
    first, _ = client._turn_index(frozen["core"]["paths"]["capacity"])
    second_path = v236._turn_paths(root, frozen["metadata_turn_name"])["capacity"]
    second, _ = client._turn_index(second_path)
    assert (first, second) == (0, 1)


class _FakeClient:
    def __init__(self, outputs: list[dict]) -> None:
        self.outputs = list(outputs)
        self.calls = 0

    async def __aenter__(self) -> "_FakeClient":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def run_ephemeral_structured_turn(self, **kwargs: object) -> SimpleNamespace:
        output = self.outputs[self.calls]
        model = str(kwargs["model"])
        effort = str(kwargs["effort"])
        self.calls += 1
        Path(str(kwargs["capacity_checkpoint_path"])).write_text(json.dumps({"schema_version": "pif_app_server_reserve_capacity_checkpoint_v1", "cleared_for_semantic_turn": True}), encoding="utf-8")
        Path(str(kwargs["sidecar_path"])).write_text(json.dumps({
            "state": "completed", "status": "completed", "usage_status": "measured",
            "usage_complete": True, "auth_type": "chatgpt", "plan_type": "pro",
            "model": model, "effort": effort, "error_class": None, "usage": _usage(),
        }), encoding="utf-8")
        Path(str(kwargs["output_path"])).write_text(json.dumps(output), encoding="utf-8")
        return SimpleNamespace(status_ok=True, output=output)


def test_fake_run_freezes_two_measured_turns_and_support_authorization(tmp_path: Path) -> None:
    core_turn = _core_turn()
    core_output = _core_output(core_turn)
    core, _, _ = v236._project_core(core_output, core_turn)
    packet = v236._source_packet(v236._validate_lineage())
    metadata_turn = v236._prepare_metadata_turn(packet=packet, core=core)
    fake = _FakeClient([core_output, _metadata_output(metadata_turn, core)])
    root = tmp_path / "v236-run"
    terminal = asyncio.run(v236.run_v236(output_dir=root, client_factory=lambda _policy: fake))
    assert fake.calls == 2
    assert terminal["state"] == "v236_architecture_structural_gate_passed"
    assert terminal["usage"]["total_tokens"] == 50_000
    assert terminal["support_alignment_authorized"] is True
    assert terminal["holdout_authorized"] is False
    assert terminal["production_mutated"] is False
    v236.verify_continuation_lock(root / "metadata-continuation-lock.json")
