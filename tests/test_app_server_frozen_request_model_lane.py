from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import app_server_frozen_request_model_lane as lane
from research_factory.app_server_runtime_verifier import ContentHashCache


def _write_json(path: Path, value: object) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def _record(path: Path) -> dict:
    return ContentHashCache().record(path)


def _schema(value: object) -> dict:
    if isinstance(value, dict):
        return {
            "type": "object",
            "additionalProperties": False,
            "required": list(value),
            "properties": {key: _schema(item) for key, item in value.items()},
        }
    if isinstance(value, list):
        return {
            "type": "array",
            "minItems": 0,
            "items": _schema(value[0]) if value else {"type": "string"},
        }
    if isinstance(value, bool):
        return {"type": "boolean"}
    if isinstance(value, int):
        return {"type": "integer"}
    if isinstance(value, float):
        return {"type": "number"}
    return {"type": "string"}


def _source() -> dict:
    def segment(segment_id: str, text: str, density: str, unit_id: str) -> dict:
        return {
            "segment_id": segment_id,
            "segment_text": text,
            "density_stratum": density,
            "boundaries": [
                {
                    "window_id": 0,
                    "extract_start": 0,
                    "extract_end": len(text),
                    "owner_start": 0,
                    "owner_end": len(text),
                }
            ],
            "units": [
                {
                    "unit_id": unit_id,
                    "window_id": 0,
                    "text": text,
                    "start_char": 0,
                    "end_char": len(text),
                }
            ],
        }

    return {
        "schema_version": "synthetic_source_v1",
        "episode_id": "episode_test",
        "segments": [
            segment("segment_dense", "Alpha source evidence.", "dense", "S0U000"),
            segment("segment_empty", "No event here.", "no_signal", "S1U000"),
        ],
    }


def _app(value: object, state: str = "present") -> dict:
    return {"applicability": state, "value": value}


def _explicit_event() -> dict:
    return {
        "event_type": "capability_claim",
        "evidence_start_unit_id": "S0U000",
        "evidence_end_unit_id": "S0U000",
        "claim_text": "Alpha source evidence.",
        "certainty": "high",
        "temporal_horizon": "present",
        "source_context_kind": "substantive_dialogue",
        "confidence": 0.9,
        "event_subtype": _app("test"),
        "target_concept": _app("alpha"),
        "causal_mechanism": _app("", "not_applicable"),
        "counterclaim": _app("", "not_applicable"),
        "signal_reason": _app("", "not_applicable"),
        "claim_type": _app("descriptive"),
        "stance": _app("neutral"),
        "speaker": {"applicability": "present", "name": "Speaker", "type": "guest"},
        "actor": {"applicability": "present", "name": "Speaker", "type": "person"},
        "reported_actor": {
            "applicability": "not_applicable",
            "name": "",
            "type": "none",
        },
        "metric": {
            "applicability": "not_applicable",
            "value": "",
            "unit": "",
            "comparator": "",
            "raw_text": "",
            "direction": "not_applicable",
        },
        "model_names": _app([], "not_applicable"),
        "product_names": _app([], "not_applicable"),
        "organizations": _app([], "not_applicable"),
        "people": _app(["Speaker"]),
    }


def _output() -> dict:
    return {
        "episode_id": "episode_test",
        "segments": [
            {
                "segment_id": "segment_dense",
                "status": "coded",
                "segment_source_context": "substantive_dialogue",
                "no_signal_reason": "",
                "unit_receipts": [
                    {"unit_id": "S0U000", "eligible_event_count": 1, "unresolved_count": 0}
                ],
                "coverage_audit": {
                    "all_source_units_reviewed": True,
                    "unresolved_count": 0,
                },
                "events": [_explicit_event()],
            },
            {
                "segment_id": "segment_empty",
                "status": "no_signal",
                "segment_source_context": "substantive_dialogue",
                "no_signal_reason": "No eligible event.",
                "unit_receipts": [
                    {"unit_id": "S1U000", "eligible_event_count": 0, "unresolved_count": 0}
                ],
                "coverage_audit": {
                    "all_source_units_reviewed": True,
                    "unresolved_count": 0,
                },
                "events": [],
            },
        ],
    }


def _direct_output() -> dict:
    event = {
        "event_type": "capability_claim",
        "event_subtype": "test",
        "claim_type": "descriptive",
        "actor_name": "Speaker",
        "actor_type": "person",
        "speaker_name": "Speaker",
        "speaker_role": "guest",
        "reported_actor_name": "",
        "reported_actor_type": "none",
        "source_context_kind": "substantive_dialogue",
        "target_concept": "alpha",
        "claim_text": "Alpha source evidence.",
        "stance": "neutral",
        "certainty": "high",
        "temporal_horizon": "present",
        "causal_mechanism": "",
        "counterclaim": "",
        "metric_value": "",
        "metric_unit": "",
        "metric_comparator": "",
        "metric_direction": "not_applicable",
        "metric_raw_text": "",
        "signal_reason": "",
        "model_names": [],
        "product_names": [],
        "organizations": [],
        "people": ["Speaker"],
        "confidence": 0.9,
        "window_id": 0,
        "evidence": "Alpha source evidence.",
    }
    return {
        "episode_id": "episode_test",
        "segments": [
            {
                "segment_id": "segment_dense",
                "status": "coded",
                "segment_source_context": "substantive_dialogue",
                "no_signal_reason": "",
                "events": [event],
            },
            {
                "segment_id": "segment_empty",
                "status": "no_signal",
                "segment_source_context": "substantive_dialogue",
                "no_signal_reason": "No eligible event.",
                "events": [],
            },
        ],
    }


def _fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path]:
    root = tmp_path / "model-lane"
    root.mkdir()
    binary = tmp_path / "codex"
    binary.write_text("synthetic pinned binary\n", encoding="utf-8")
    monkeypatch.setattr(lane, "PINNED_CODEX", binary.resolve())
    source = _write_json(tmp_path / "source.private.json", _source())
    request_input = _write_json(tmp_path / "request-input.private.json", _source())
    request_prompt = tmp_path / "request-prompt.private.md"
    request_prompt.write_text("Synthetic frozen prompt.\n", encoding="utf-8")
    request_base = tmp_path / "request-base.private.md"
    request_base.write_text("Synthetic frozen base instructions.\n", encoding="utf-8")
    request_schema = _write_json(tmp_path / "request-schema.json", _schema(_output()))
    projection_schema = _write_json(
        tmp_path / "projection-schema.json", _schema(_direct_output())
    )
    paths = {
        "source_input": source,
        "request_input": request_input,
        "request_prompt": request_prompt,
        "request_base": request_base,
        "request_schema": request_schema,
        "projection_schema": projection_schema,
    }
    for key in lane.LINEAGE_KEYS:
        if key not in paths:
            paths[key] = _write_json(tmp_path / f"{key}.json", {"artifact": key})
    config = {
        "schema_version": lane.CONFIG_VERSION,
        "experiment_id": "synthetic_frontier_model_lane",
        "architecture_id": lane.ARCHITECTURE_ID,
        "output_root": str(root.resolve()),
        "model": lane.MODEL,
        "effort": lane.EFFORT,
        "declared_turn_count": 1,
        "retry_count": 0,
        "maximum_total_tokens": lane.MAX_TOTAL_TOKENS,
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "semantic_regex_or_keyword_filtering": False,
        "production_mutation_allowed": False,
        "holdout_authorized": False,
        "minimum_remaining_reserve_percent": 20,
        "quota_points_per_million_tokens": 17,
        "production_amortized_context_tokens": lane.PRODUCTION_CONTEXT_TOKENS,
        "production_scale": lane.PRODUCTION_SCALE,
        "baseline_total_tokens": lane.BASELINE_TOTAL_TOKENS,
        **{key: _record(path) for key, path in paths.items()},
    }
    config_path = _write_json(root / "experiment-config.json", config)
    return config_path, request_prompt


class _FakeClient:
    def __init__(self, output: dict | None = None, *, measured: bool = True):
        self.output = copy.deepcopy(output or _output())
        self.measured = measured
        self.calls = 0

    async def __aenter__(self) -> "_FakeClient":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def run_ephemeral_structured_turn(self, **kwargs: object) -> SimpleNamespace:
        self.calls += 1
        capacity = Path(str(kwargs["capacity_checkpoint_path"]))
        sidecar = Path(str(kwargs["sidecar_path"]))
        output = Path(str(kwargs["output_path"]))
        _write_json(capacity, {"cleared_for_semantic_turn": True})
        _write_json(output, self.output)
        if self.measured:
            _write_json(
                sidecar,
                {
                    "state": "completed",
                    "status": "completed",
                    "usage_status": "measured",
                    "usage_complete": True,
                    "auth_type": "chatgpt",
                    "plan_type": "pro",
                    "model": lane.MODEL,
                    "effort": lane.EFFORT,
                    "error_class": None,
                    "usage": {
                        "input_tokens": 1000,
                        "cached_input_tokens": 0,
                        "output_tokens": 2000,
                        "reasoning_output_tokens": 100,
                        "total_tokens": 3000,
                    },
                },
            )
        else:
            _write_json(
                sidecar,
                {
                    "state": "failed",
                    "status": "failed",
                    "usage_status": "unknown",
                    "usage_complete": False,
                    "auth_type": "chatgpt",
                    "plan_type": "pro",
                    "model": lane.MODEL,
                    "effort": lane.EFFORT,
                    "error_class": "serverOverloaded",
                },
            )
        return SimpleNamespace()


def test_freeze_copies_exact_request_and_verifies(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_path, source_prompt = _fixture(tmp_path, monkeypatch)
    frozen = lane.freeze_experiment(config_path)
    copied = frozen["turn"]["prompt"]
    assert copied.read_bytes() == source_prompt.read_bytes()
    assert lane.verify_frozen(config_path.parent)["config"]["model"] == lane.MODEL


def test_success_runs_once_projects_exact_evidence_and_uses_no_count_proxy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path, _ = _fixture(tmp_path, monkeypatch)
    client = _FakeClient()
    terminal = asyncio.run(
        lane.run_experiment(config_path, client_factory=lambda _policy: client)
    )
    assert client.calls == 1
    assert terminal["support_alignment_authorized"] is True
    gate = json.loads((config_path.parent / "architecture-structural-gate.json").read_text())
    assert gate["checks"]["semantic_event_count_proxy_not_used"] is True
    normalized = json.loads((config_path.parent / "normalized-output.private.json").read_text())
    assert normalized["segments"][0]["events"][0]["evidence"] == "Alpha source evidence."
    assert normalized["segments"][0]["events"][0]["window_id"] == 0
    again = asyncio.run(
        lane.run_experiment(config_path, client_factory=lambda _policy: client)
    )
    assert again == terminal
    assert client.calls == 1


def test_applicability_failure_is_measured_semantic_rejection_without_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path, _ = _fixture(tmp_path, monkeypatch)
    output = _output()
    output["segments"][0]["events"][0]["actor"]["name"] = ""
    client = _FakeClient(output)
    terminal = asyncio.run(
        lane.run_experiment(config_path, client_factory=lambda _policy: client)
    )
    assert terminal["terminal_reason"] == "frozen_request_model_lane_structural_or_cost_gate_not_passed"
    assert terminal["usage_status"] == "complete"
    assert terminal["semantic_retry_count"] == 0
    assert client.calls == 1


def test_unknown_usage_freezes_infrastructure_failure_without_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path, _ = _fixture(tmp_path, monkeypatch)
    client = _FakeClient(measured=False)
    terminal = asyncio.run(
        lane.run_experiment(config_path, client_factory=lambda _policy: client)
    )
    assert terminal["terminal_reason"] == "infrastructure_or_judge_attempt_failed"
    assert terminal["usage_status"] == "unknown"
    assert terminal["accounting_complete"] is False
    assert client.calls == 1
    asyncio.run(lane.run_experiment(config_path, client_factory=lambda _policy: client))
    assert client.calls == 1


def test_source_lineage_and_copied_request_mutation_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path, source_prompt = _fixture(tmp_path, monkeypatch)
    frozen = lane.freeze_experiment(config_path)
    source_prompt.write_text("mutated source prompt\n", encoding="utf-8")
    with pytest.raises(lane.FrozenRequestModelLaneError):
        lane.verify_frozen(config_path.parent)
    source_prompt.write_text("Synthetic frozen prompt.\n", encoding="utf-8")
    frozen["turn"]["prompt"].write_text("mutated copied prompt\n", encoding="utf-8")
    with pytest.raises(lane.FrozenRequestModelLaneError):
        lane.verify_frozen(config_path.parent)
