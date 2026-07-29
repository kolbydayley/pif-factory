from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import (
    app_server_judge_v5_selection_v235_support_critic_realization as v235,
)
from research_factory.app_server_capacity_reserve import (
    ReserveCapacityGatedCodexAppServerClient,
)


def _turn() -> dict:
    return v235._prepare_turn(v235._validate_lineage())


def _context() -> dict:
    return {"kind": "substantive_dialogue", "confidence": 1.0, "rationale": "fixture"}


def _template_event() -> dict:
    source = v235._load_json(
        v235.v234.SOURCE_TURN_ROOT / "input.private.json", "fixture source"
    )
    dense = next(
        row
        for row in source["normalization_segments"]
        if row["segment_id"] == v235.DENSE_SEGMENT_ID
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


def _output(turn: dict, *, dense_keep: int = 32, no_signal_keep: int = 0) -> dict:
    template = _template_event()
    rows = []
    for segment in turn["private_input"]["segments"]:
        segment_id = segment["segment_id"]
        propositions = segment["frozen_propositions"]
        keep_count = dense_keep if segment_id == v235.DENSE_SEGMENT_ID else no_signal_keep
        decisions = []
        events = []
        for index, proposition in enumerate(propositions):
            keep = index < keep_count
            decisions.append(
                {
                    "proposition_id": proposition["proposition_id"],
                    "verdict": "keep" if keep else "drop",
                    "drop_reason": "keep" if keep else "not_research_event",
                    "examined_start_unit_id": proposition["evidence_start_unit_id"],
                    "examined_end_unit_id": proposition["evidence_end_unit_id"],
                    "rationale": "Fixture source-side decision.",
                }
            )
            if keep:
                event = copy.deepcopy(template)
                event["claim_text"] = (
                    f"{event['claim_text']} critic-fixture-{segment_id}-{index}"
                )
                event["proposition_id"] = proposition["proposition_id"]
                event["evidence_start_unit_id"] = proposition["evidence_start_unit_id"]
                event["evidence_end_unit_id"] = proposition["evidence_end_unit_id"]
                events.append(event)
        rows.append(
            {
                "segment_id": segment_id,
                "status": "coded" if events else "no_signal",
                "segment_source_context": _context(),
                "no_signal_reason": "" if events else "No proposal survived source review.",
                "decisions": decisions,
                "events": events,
            }
        )
    return {"episode_id": turn["episode_id"], "segments": rows}


def _usage(total: int = 30_000) -> dict:
    return {
        "input_tokens": total - 8_000,
        "cached_input_tokens": 0,
        "output_tokens": 8_000,
        "reasoning_output_tokens": 500,
        "total_tokens": total,
    }


def test_prompt_is_side_free_and_architecture_is_ranked_from_v234() -> None:
    turn = _turn()
    assert "existing_events" not in turn["prompt"]
    assert "density_stratum" not in turn["prompt"]
    assert "reference_count" not in turn["prompt"]
    assert "frozen_propositions" in turn["prompt"]
    ranking = v235._ranking()
    assert len(ranking) >= 3
    assert ranking[0]["architecture_id"] == (
        "frozen_inventory_source_support_critic_realization"
    )


def test_projection_enforces_decision_coverage_and_llm_keep_drop_mapping() -> None:
    turn = _turn()
    valid = _output(turn)
    normalized, decisions, provenance, diagnostics = v235._project_output(valid, turn)
    dense = next(
        row for row in normalized["segments"] if row["segment_id"] == v235.DENSE_SEGMENT_ID
    )
    no_signal = next(
        row for row in normalized["segments"] if row["segment_id"] == v235.NO_SIGNAL_SEGMENT_ID
    )
    assert len(dense["events"]) == 32
    assert len(no_signal["events"]) == 0
    assert len(decisions["segments"][0]["decisions"]) == 32
    assert len(provenance["events"]) == 32
    assert diagnostics[0]["decision_count"] == 32

    missing = copy.deepcopy(valid)
    missing["segments"][0]["decisions"].pop()
    with pytest.raises(v235.V235OutputContractError, match="decision proposition coverage"):
        v235._project_output(missing, turn)

    mismatch = copy.deepcopy(valid)
    mismatch["segments"][0]["decisions"][0]["drop_reason"] = "unsupported"
    with pytest.raises(v235.V235OutputContractError, match="verdict and drop reason"):
        v235._project_output(mismatch, turn)

    event_without_keep = copy.deepcopy(valid)
    event_without_keep["segments"][0]["decisions"][0]["verdict"] = "drop"
    event_without_keep["segments"][0]["decisions"][0]["drop_reason"] = "unsupported"
    with pytest.raises(v235.V235OutputContractError, match="kept.*event projection"):
        v235._project_output(event_without_keep, turn)


def test_projection_rejects_nonliteral_metric_and_gate_enforces_counts_and_cost() -> None:
    turn = _turn()
    invalid = _output(turn)
    invalid["segments"][0]["events"][0]["metric_value"] = "not-in-evidence"
    invalid["segments"][0]["events"][0]["metric_direction"] = "increase"
    with pytest.raises(v235.V235OutputContractError, match="metric literal"):
        v235._project_output(invalid, turn)

    _, _, _, diagnostics = v235._project_output(_output(turn), turn)
    gate = v235._build_gate(usage=_usage(), diagnostics=diagnostics)
    assert gate["passed"] is True
    assert gate["combined_total_tokens"] == 59_362
    assert gate["production_amortized_total_token_ratio"] < 0.28

    _, _, _, sparse = v235._project_output(_output(turn, dense_keep=23), turn)
    sparse_gate = v235._build_gate(usage=_usage(), diagnostics=sparse)
    assert sparse_gate["passed"] is False
    assert "dense_kept_event_count_gte_24" in sparse_gate["failed_checks"]

    _, _, _, false_positive = v235._project_output(
        _output(turn, no_signal_keep=1), turn
    )
    fp_gate = v235._build_gate(usage=_usage(), diagnostics=false_positive)
    assert fp_gate["passed"] is False
    assert "no_signal_kept_event_count_0" in fp_gate["failed_checks"]


def test_freeze_binds_v234_reuse_runtime_capacity_and_cost(tmp_path: Path) -> None:
    root = tmp_path / "v235"
    frozen = v235.freeze_v235(output_dir=root)
    lock = v235.verify_runtime_lock(frozen["runtime_lock"])
    reuse = v235._load_json(frozen["reuse_path"], "reuse")
    design = v235._load_json(frozen["design_path"], "design")
    assert lock["declared_new_turn_count"] == 1
    assert lock["retry_count"] == 0
    assert len(lock["direct_lineage"]) == len(v235.EXPECTED_LINEAGE_HASHES)
    assert reuse["reused_total_tokens"] == 29_362
    assert reuse["replay_allowed"] is False
    assert design["production_cost_projection"][
        "projected_production_amortized_total_token_ratio"
    ] < 0.28
    client = object.__new__(ReserveCapacityGatedCodexAppServerClient)
    client.policy = v235.reserve.load_reserve_capacity_policy(
        frozen["capacity_policy"]
    )
    index, path = client._turn_index(frozen["turn"]["paths"]["capacity"])
    assert index == 0
    assert path == frozen["turn"]["paths"]["capacity"].resolve()
    assert not frozen["turn"]["paths"]["capacity"].exists()
    assert not frozen["turn"]["paths"]["sidecar"].exists()


class _FakeClient:
    def __init__(self, output: dict, usage: dict | None = None) -> None:
        self.output = output
        self.usage = usage or _usage()
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
                    "model": v235.MODEL,
                    "effort": v235.EFFORT,
                    "error_class": None,
                    "usage": self.usage,
                }
            ),
            encoding="utf-8",
        )
        Path(str(kwargs["output_path"])).write_text(
            json.dumps(self.output), encoding="utf-8"
        )
        return SimpleNamespace(status_ok=True, output=self.output)


def test_fake_run_freezes_measured_structural_pass(tmp_path: Path) -> None:
    fake = _FakeClient(_output(_turn()))
    root = tmp_path / "v235-pass"
    terminal = asyncio.run(
        v235.run_v235(output_dir=root, client_factory=lambda _policy: fake)
    )
    assert fake.calls == 1
    assert terminal["state"] == "v235_architecture_structural_gate_passed"
    assert terminal["semantic_retry_count"] == 0
    assert terminal["combined_total_tokens"] == 59_362
    assert terminal["support_alignment_authorized"] is True
    assert terminal["holdout_authorized"] is False
    assert terminal["production_mutated"] is False


def test_fake_run_freezes_count_failure_and_authorizes_distinct_architecture(
    tmp_path: Path,
) -> None:
    fake = _FakeClient(_output(_turn(), dense_keep=23))
    terminal = asyncio.run(
        v235.run_v235(
            output_dir=tmp_path / "v235-fail",
            client_factory=lambda _policy: fake,
        )
    )
    assert fake.calls == 1
    assert terminal["architecture_strategy_rejected"] is True
    assert terminal["next_distinct_architecture_authorized"] is True
    assert terminal["usage_status"] == "complete"
