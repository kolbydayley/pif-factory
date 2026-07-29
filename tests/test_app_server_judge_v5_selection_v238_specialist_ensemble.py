from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import app_server_judge_v5_selection_v238_specialist_ensemble as v238
from research_factory.app_server_capacity_reserve import (
    ReserveCapacityGatedCodexAppServerClient,
)


def _prepared_turn() -> dict:
    return v238._prepare_turn(v238._validate_lineage())


def _context() -> dict:
    return {
        "kind": "substantive_dialogue",
        "confidence": 1.0,
        "rationale": "fixture",
    }


def _template_event() -> dict:
    source = v238._load_json(
        v238.SOURCE_TURN_ROOT / "input.private.json", "fixture source"
    )
    dense = next(
        row
        for row in source["normalization_segments"]
        if row["segment_id"] == v238.DENSE_SEGMENT_ID
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
                "specialists_reviewed": list(v238.SPECIALISTS),
                "candidate_count": 0,
                "unresolved_count": 0,
            }
            for unit in units
        ]
        candidates = []
        events = []
        if source["segment_id"] == v238.DENSE_SEGMENT_ID:
            assert dense_event_count + 2 <= len(units)
            for index, unit in enumerate(units[:dense_event_count]):
                candidate_id = f"C{index:03d}"
                event_id = f"E{index:03d}"
                candidates.append(
                    {
                        "candidate_id": candidate_id,
                        "specialist": v238.SPECIALISTS[index % len(v238.SPECIALISTS)],
                        "evidence_start_unit_id": unit["unit_id"],
                        "evidence_end_unit_id": unit["unit_id"],
                        "proposition": f"fixture proposition {index}",
                        "disposition": "emit",
                        "final_event_id": event_id,
                    }
                )
                receipts[index]["candidate_count"] = 1
                event = copy.deepcopy(template)
                event["claim_text"] = f"{event['claim_text']} specialist-{index}"
                event["event_id"] = event_id
                event["source_candidate_ids"] = [candidate_id]
                event["evidence_start_unit_id"] = unit["unit_id"]
                event["evidence_end_unit_id"] = unit["unit_id"]
                events.append(event)
            for offset, unit in enumerate(
                units[dense_event_count : dense_event_count + 2], start=dense_event_count
            ):
                candidates.append(
                    {
                        "candidate_id": f"C{offset:03d}",
                        "specialist": v238.SPECIALISTS[offset % len(v238.SPECIALISTS)],
                        "evidence_start_unit_id": unit["unit_id"],
                        "evidence_end_unit_id": unit["unit_id"],
                        "proposition": f"rejected fixture {offset}",
                        "disposition": "reject_non_event",
                        "final_event_id": "",
                    }
                )
                receipts[offset]["candidate_count"] = 1
        else:
            unit = units[0]
            candidates.append(
                {
                    "candidate_id": "C000",
                    "specialist": "relations",
                    "evidence_start_unit_id": unit["unit_id"],
                    "evidence_end_unit_id": unit["unit_id"],
                    "proposition": "reviewed but rejected fixture",
                    "disposition": "reject_non_event",
                    "final_event_id": "",
                }
            )
            receipts[0]["candidate_count"] = 1
        rows.append(
            {
                "segment_id": source["segment_id"],
                "status": "coded" if events else "no_signal",
                "segment_source_context": _context(),
                "no_signal_reason": "" if events else "No eligible event remains after consolidation.",
                "unit_receipts": receipts,
                "candidates": candidates,
                "events": events,
            }
        )
    return {"episode_id": turn["episode_id"], "segments": rows}


def _usage(total: int = 35_000) -> dict:
    return {
        "input_tokens": total - 10_000,
        "cached_input_tokens": 0,
        "output_tokens": 10_000,
        "reasoning_output_tokens": 600,
        "total_tokens": total,
    }


def test_request_is_blind_compact_and_exposes_three_specialist_contract() -> None:
    turn = _prepared_turn()
    packet = json.loads(turn["prompt"].split("\n", 1)[1])
    assert list(packet) == ["episode_id", "segments"]
    assert "existing_events" not in turn["prompt"]
    assert "density_stratum" not in turn["prompt"]
    assert "reference_count" not in turn["prompt"]
    assert "candidate" not in turn["prompt"]
    assert all(name in turn["base"] for name in v238.SPECIALISTS)
    assert turn["prompt_bytes"] <= v238.MAX_PROMPT_BYTES
    assert turn["base_bytes"] <= v238.MAX_BASE_BYTES
    assert turn["schema_bytes"] <= v238.MAX_SCHEMA_BYTES
    segment_schema = turn["schema"]["properties"]["segments"]["items"]
    assert "candidates" in segment_schema["properties"]
    event_schema = segment_schema["properties"]["events"]["items"]
    assert "event_id" in event_schema["required"]
    assert "source_candidate_ids" in event_schema["required"]
    assert "evidence" not in event_schema["properties"]


def test_projection_accounts_candidates_and_passes_structural_gate() -> None:
    turn = _prepared_turn()
    normalized, provenance, ledger, diagnostics = v238._project_output(
        _output(turn), turn
    )
    by_id = {row["segment_id"]: row for row in normalized["segments"]}
    assert len(by_id[v238.DENSE_SEGMENT_ID]["events"]) == 24
    assert len(by_id[v238.NO_SIGNAL_SEGMENT_ID]["events"]) == 0
    assert len(provenance["events"]) == 24
    assert len(ledger["segments"][0]["candidates"]) == 26
    gate = v238._gate(usage=_usage(), diagnostics=diagnostics)
    assert gate["passed"] is True
    assert gate["residual_support_audit_required"] is False
    assert gate["production_amortized_total_token_ratio"] < 0.28


def test_projection_rejects_unaccounted_candidate_and_receipt_drift() -> None:
    turn = _prepared_turn()
    output = _output(turn, dense_event_count=1)
    active = copy.deepcopy(output)
    active["segments"][0]["candidates"][0]["final_event_id"] = "missing"
    with pytest.raises(v238.V238OutputContractError, match="mapping drifted"):
        v238._project_output(active, turn)
    receipt = copy.deepcopy(output)
    receipt["segments"][0]["unit_receipts"][0]["candidate_count"] = 0
    with pytest.raises(v238.V238OutputContractError, match="receipt ownership"):
        v238._project_output(receipt, turn)


def test_freeze_binds_ranked_design_runtime_lineage_and_capacity(tmp_path: Path) -> None:
    root = tmp_path / "v238"
    frozen = v238.freeze_v238(output_dir=root)
    lock = v238.verify_runtime_lock(frozen["runtime_lock"])
    assert lock["declared_turn_count"] == 1
    assert lock["retry_count"] == 0
    assert frozen["spec"]["specialists"] == list(v238.SPECIALISTS)
    assert frozen["spec"]["reference_visible_to_model"] is False
    design = v238._load_json(frozen["design_path"], "design")
    assert (
        design["production_cost_projection"]
        ["projected_production_amortized_total_token_ratio"]
        < 0.28
    )
    ranking = v238._load_json(frozen["ranking_path"], "ranking")
    assert len(ranking["architectures"]) == 3
    assert ranking["architectures"][0]["rank"] == 1
    client = object.__new__(ReserveCapacityGatedCodexAppServerClient)
    client.policy = v238.reserve.load_reserve_capacity_policy(
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
                    "model": v238.MODEL,
                    "effort": v238.EFFORT,
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
        v238.run_v238(
            output_dir=tmp_path / "v238-run",
            client_factory=lambda _policy: fake,
        )
    )
    assert fake.calls == 1
    assert terminal["state"] == "v238_architecture_structural_gate_passed"
    assert terminal["semantic_attempt_count"] == 1
    assert terminal["semantic_retry_count"] == 0
    assert terminal["usage_status"] == "complete"
    assert terminal["support_alignment_authorized"] is True
    assert terminal["holdout_authorized"] is False
    assert terminal["production_mutated"] is False
