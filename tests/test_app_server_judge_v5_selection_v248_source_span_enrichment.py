from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import app_server_judge_v5_selection_v248_source_span_enrichment as v248


@pytest.fixture(scope="module")
def lineage() -> dict:
    return v248._validate_lineage()


@pytest.fixture(scope="module")
def turn(lineage: dict) -> dict:
    return v248.prepare_turn(lineage)


def _synthetic_output(turn: dict) -> dict:
    old_path = next(
        v248.V243_ROOT.glob("turns/v243-enrichment-*/output.private.json")
    )
    old = json.loads(old_path.read_text(encoding="utf-8"))
    old_by_id = {row["core_event_id"]: row for row in old["events"]}
    none_span = {"unit_id": "none", "start": 0, "end": 0}
    events = []
    for event_id in turn["event_ids"]:
        source = old_by_id[event_id]
        events.append(
            {
                "core_event_id": event_id,
                "event_subtype": source["event_subtype"],
                "claim_type": source["claim_type"],
                "actor_type": source["actor_type"],
                "speaker_role": source["speaker_role"],
                "reported_actor_type": source["reported_actor_type"],
                "metric_direction": "not_applicable",
                "signal_reason": source["signal_reason"],
                "confidence": source["confidence"],
                "metric_value_span": dict(none_span),
                "metric_unit_span": dict(none_span),
                "metric_comparator_span": dict(none_span),
                "metric_raw_text_span": dict(none_span),
                "model_name_spans": [],
                "product_name_spans": [],
                "organization_spans": [],
                "people_spans": [],
            }
        )
    output = {"episode_id": turn["episode_id"], "events": events}
    v248._validate_schema(turn["schema"], output, path="$")
    return output


def test_ranked_canary_reuses_measured_core_and_fits_cost_bound(turn: dict) -> None:
    assert len(turn["event_ids"]) == 28
    assert turn["prompt_bytes"] <= v248.MAX_PROMPT_BYTES
    assert turn["base_bytes"] <= v248.MAX_BASE_BYTES
    assert turn["schema_bytes"] <= v248.MAX_SCHEMA_BYTES
    assert round(v248._production_ratio(v248.NEW_TURN_MAX_TOKENS)[1], 6) == 0.274507
    assert v248.ADOPTED_CORE_TOKENS == 30_083
    assert v248.NEW_TURN_MAX_TOKENS == 42_000


def test_source_span_projection_is_exact_and_evidence_bound() -> None:
    units = {"u0": {"text": "alpha 42 percent"}}
    value, receipt = v248._slice_span(
        {"unit_id": "u0", "start": 6, "end": 8},
        unit_by_id=units,
        allowed_ids={"u0"},
    )
    assert value == "42"
    assert receipt["start"] == 6
    with pytest.raises(v248.V248OutputContractError):
        v248._slice_span(
            {"unit_id": "u1", "start": 0, "end": 2},
            unit_by_id=units,
            allowed_ids={"u0"},
        )
    with pytest.raises(v248.V248OutputContractError):
        v248._slice_span(
            {"unit_id": "none", "start": 0, "end": 1},
            unit_by_id=units,
            allowed_ids={"u0"},
        )


def test_projection_preserves_core_count_and_exact_literal_contract(turn: dict) -> None:
    normalized, provenance, diagnostics, receipts = v248.project_output(
        _synthetic_output(turn), turn
    )
    assert sum(len(row["events"]) for row in normalized["segments"]) == 28
    assert len(diagnostics) == 2
    assert receipts[
        "all_literal_values_projected_from_llm_selected_exact_source_spans"
    ] is True
    assert provenance


def test_freeze_is_presemantic_and_runtime_hash_bound(tmp_path: Path) -> None:
    root = tmp_path / "v248"
    frozen = v248.freeze_v248(output_dir=root)
    lock = v248.verify_runtime_lock(frozen["runtime_lock"])
    assert lock["declared_new_turn_count"] == 1
    assert lock["adopted_predecessor_turn_count"] == 1
    assert lock["retry_count"] == 0
    assert lock["semantic_regex_or_keyword_filtering"] is False
    assert not (root / "launch-receipt.json").exists()
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))
    assert not list(root.rglob("output.private.json"))


class _FakeClient:
    def __init__(self, root: Path, output: dict) -> None:
        self.root = root
        self.output = output
        self.calls = 0

    async def __aenter__(self) -> "_FakeClient":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def run_ephemeral_structured_turn(self, **kwargs: object) -> SimpleNamespace:
        assert "output_validator" not in kwargs
        assert (self.root / "launch-receipt.json").is_file()
        self.calls += 1
        Path(str(kwargs["capacity_checkpoint_path"])).write_text(
            json.dumps(
                {
                    "cleared_for_semantic_turn": True,
                    "managed_chatgpt_auth_verified": True,
                    "rate_limit_reached_type": None,
                }
            )
            + "\n",
            encoding="utf-8",
        )
        usage = {
            "input_tokens": 16_000,
            "cached_input_tokens": 1_000,
            "output_tokens": 4_000,
            "reasoning_output_tokens": 500,
            "total_tokens": 20_000,
        }
        Path(str(kwargs["sidecar_path"])).write_text(
            json.dumps(
                {
                    "state": "completed",
                    "status": "completed",
                    "usage_status": "measured",
                    "usage_complete": True,
                    "usage": usage,
                    "auth_type": "chatgpt",
                    "plan_type": "pro",
                    "model": v248.MODEL,
                    "effort": v248.EFFORT,
                    "error_class": None,
                }
            )
            + "\n",
            encoding="utf-8",
        )
        Path(str(kwargs["output_path"])).write_text(
            json.dumps(self.output) + "\n", encoding="utf-8"
        )
        return SimpleNamespace(status_ok=True, output=self.output)


def test_fake_run_passes_structure_and_accounts_adopted_core(tmp_path: Path) -> None:
    root = tmp_path / "v248"
    frozen = v248.freeze_v248(output_dir=root)
    fake = _FakeClient(root, _synthetic_output(frozen["turn"]))
    terminal = asyncio.run(
        v248.run_v248(output_dir=root, client_factory=lambda _policy: fake)
    )
    assert fake.calls == 1
    assert terminal["state"] == "v248_architecture_structural_gate_passed"
    assert terminal["new_turn_usage"]["total_tokens"] == 20_000
    assert terminal["adopted_core_usage"]["total_tokens"] == 30_083
    assert terminal["combined_tokens"] == 50_083
    assert terminal["production_amortized_total_token_ratio"] < 0.28
    assert terminal["support_alignment_authorized"] is True
    assert terminal["holdout_authorized"] is False
    assert terminal["production_mutated"] is False


def test_runtime_lock_rejects_mutated_span_schema(tmp_path: Path) -> None:
    root = tmp_path / "v248"
    frozen = v248.freeze_v248(output_dir=root)
    schema = next(root.glob("turns/*/schema.json"))
    schema.write_text(schema.read_text(encoding="utf-8") + "drift", encoding="utf-8")
    with pytest.raises(v248.V248SourceSpanEnrichmentError):
        v248.verify_runtime_lock(frozen["runtime_lock"])
