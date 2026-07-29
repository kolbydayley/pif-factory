from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import app_server_judge_v5_selection_v242_set_coded_reducer as v242
from research_factory.app_server_capacity_reserve import (
    ReserveCapacityError,
    ReserveCapacityGatedCodexAppServerClient,
)


def _prepared_turn() -> dict:
    return v242._prepare_turn(v242._validate_lineage())


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
        is_dense = source["segment_id"] == v242.DENSE_SEGMENT_ID
        keep_ids = []
        drop_ids = []
        replacements = []
        for index, proposal in enumerate(source["proposals"]):
            proposal_id = proposal["proposal_id"]
            event = proposal["event"]
            if not is_dense:
                drop_ids.append(proposal_id)
            elif _metric_applicability_invalid(event) and not keep_invalid:
                replacement = _corrected_event(event)
                replacement["event_id"] = f"R{index:03d}"
                replacement["source_proposal_ids"] = [proposal_id]
                replacements.append(replacement)
            else:
                keep_ids.append(proposal_id)
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
                "keep_proposal_ids": keep_ids,
                "drop_proposal_ids": drop_ids,
                "replacement_events": replacements,
            }
        )
    return {"episode_id": turn["episode_id"], "segments": rows}


def _usage(total: int = 32_000) -> dict:
    return {
        "input_tokens": total - 2_000,
        "cached_input_tokens": 0,
        "output_tokens": 2_000,
        "reasoning_output_tokens": 400,
        "total_tokens": total,
    }


def test_set_coded_request_is_source_local_and_removes_receipt_rows() -> None:
    turn = _prepared_turn()
    prior = v242.v241._prepare_turn(v242.v241._validate_lineage())
    assert turn["prompt"] == prior["prompt"]
    assert "density_stratum" not in turn["prompt"]
    assert "episode_context" not in turn["private_input"]
    assert turn["base_bytes"] < v242.MAX_BASE_BYTES
    assert turn["schema_bytes"] <= v242.MAX_SCHEMA_BYTES
    segment_schema = turn["schema"]["properties"]["segments"]["items"]
    assert "proposal_receipts" not in segment_schema["properties"]
    assert "keep_proposal_ids" in segment_schema["properties"]
    assert "drop_proposal_ids" in segment_schema["properties"]
    assert "replacement_events" in segment_schema["properties"]


def test_projection_applies_sets_and_passes_structural_cost_gate() -> None:
    turn = _prepared_turn()
    normalized, provenance, decisions, diagnostics = v242._project_output(_output(turn), turn)
    assert len(normalized["segments"][0]["events"]) == 31
    assert len(normalized["segments"][1]["events"]) == 0
    assert len(provenance["events"]) == 31
    assert len(decisions["segments"][0]["replacement_event_sources"]) == 4
    gate = v242._gate(usage=_usage(), diagnostics=diagnostics)
    assert gate["passed"] is True
    assert gate["combined_development_tokens"] == 71_004
    assert gate["production_amortized_total_token_ratio"] < 0.28
    assert gate["residual_support_audit_required"] is False


@pytest.mark.parametrize("mutation", ["duplicate", "missing", "cross_segment"])
def test_projection_rejects_invalid_proposal_set_coverage(mutation: str) -> None:
    turn = _prepared_turn()
    output = _output(turn)
    dense = output["segments"][0]
    no_signal = output["segments"][1]
    if mutation == "duplicate":
        dense["drop_proposal_ids"].append(dense["keep_proposal_ids"][0])
        expected = "multiple decision sets"
    elif mutation == "missing":
        dense["keep_proposal_ids"].pop()
        expected = "coverage drifted"
    else:
        dense["keep_proposal_ids"][0] = no_signal["drop_proposal_ids"][0]
        expected = "another segment proposal"
    with pytest.raises(v242.V242OutputContractError, match=expected):
        v242._project_output(output, turn)


def test_projection_rejects_llm_keep_of_invalid_metric_field() -> None:
    turn = _prepared_turn()
    with pytest.raises(v242.V242OutputContractError, match="metric direction applicability"):
        v242._project_output(_output(turn, keep_invalid=True), turn)


def test_freeze_binds_lineage_runtime_capacity_and_request(tmp_path: Path) -> None:
    root = tmp_path / "v242"
    frozen = v242.freeze_v242(output_dir=root)
    lock = v242.verify_runtime_lock(frozen["runtime_lock"])
    assert lock["declared_turn_count"] == 1
    assert lock["retry_count"] == 0
    assert lock["adopted_prior_map_tokens"] == v242.PRIOR_MAP_TOKENS
    assert frozen["spec"]["isolated_field_repair"] is False
    design = v242._load_json(frozen["design_path"], "design")
    projection = design["production_cost_projection"]
    assert projection["projected_production_amortized_total_token_ratio"] < 0.28
    assert projection["set_reducer_turn_hard_max"] == v242.MAX_NEW_TOKENS
    client = object.__new__(ReserveCapacityGatedCodexAppServerClient)
    client.policy = v242.reserve.load_reserve_capacity_policy(frozen["capacity_policy"])
    index, path = client._turn_index(frozen["turn"]["paths"]["capacity"])
    assert index == 0
    assert path == frozen["turn"]["paths"]["capacity"].resolve()
    assert not frozen["turn"]["paths"]["sidecar"].exists()


class _FakeClient:
    def __init__(self, output: dict, *, total_tokens: int = 32_000) -> None:
        self.output = output
        self.total_tokens = total_tokens
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
                    "model": v242.MODEL,
                    "effort": v242.EFFORT,
                    "error_class": None,
                    "usage": _usage(self.total_tokens),
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
        v242.run_v242(output_dir=tmp_path / "v242-run", client_factory=lambda _policy: fake)
    )
    assert fake.calls == 1
    assert terminal["state"] == "v242_architecture_structural_gate_passed"
    assert terminal["semantic_retry_count"] == 0
    assert terminal["usage_status"] == "complete"
    assert terminal["combined_development_tokens"] == 71_004
    assert terminal["support_alignment_authorized"] is True
    assert terminal["holdout_authorized"] is False
    assert terminal["production_mutated"] is False


def test_measured_post_turn_capacity_overage_is_quality_cost_failure(tmp_path: Path) -> None:
    root = tmp_path / "v242-overage"
    frozen = v242.freeze_v242(output_dir=root)
    sidecar = frozen["turn"]["paths"]["sidecar"]
    sidecar.write_text(
        json.dumps(
            {
                "usage": _usage(v242.MAX_NEW_TOKENS + 1),
                "usage_complete": True,
                "usage_status": "measured",
            }
        ),
        encoding="utf-8",
    )
    terminal = v242._failure_terminal(root, frozen, ReserveCapacityError("measured cap exceeded"))
    assert terminal["terminal_reason"] == (
        "v242_set_coded_reducer_structural_quality_or_cost_gate_not_passed"
    )
    assert terminal["usage_status"] == "complete"
    assert terminal["accounting_complete"] is True
    assert terminal["next_distinct_architecture_authorized"] is True
