from __future__ import annotations

import copy
import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import app_server_configured_experiment as experiment
from research_factory.app_server_runtime_verifier import ContentHashCache


def _write_json(path: Path, value: object) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _record(path: Path) -> dict:
    return ContentHashCache().record(path)


def _event_schema() -> dict:
    string_fields = {
        "event_type",
        "event_subtype",
        "claim_type",
        "actor_name",
        "actor_type",
        "speaker_name",
        "speaker_role",
        "reported_actor_name",
        "reported_actor_type",
        "source_context_kind",
        "target_concept",
        "claim_text",
        "stance",
        "certainty",
        "temporal_horizon",
        "causal_mechanism",
        "counterclaim",
        "metric_value",
        "metric_unit",
        "metric_comparator",
        "metric_direction",
        "metric_raw_text",
        "signal_reason",
        "evidence_start_unit_id",
        "evidence_end_unit_id",
    }
    properties = {field: {"type": "string"} for field in string_fields}
    for field in ("model_names", "product_names", "organizations", "people"):
        properties[field] = {"type": "array", "items": {"type": "string"}}
    properties["confidence"] = {"type": "number", "minimum": 0, "maximum": 1}
    return {
        "type": "object",
        "additionalProperties": False,
        "required": list(properties),
        "properties": properties,
    }


def _projection_schema() -> dict:
    event = _event_schema()
    segment = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "segment_id",
            "status",
            "segment_source_context",
            "no_signal_reason",
            "events",
        ],
        "properties": {
            "segment_id": {"type": "string"},
            "status": {"type": "string", "enum": ["coded", "no_signal"]},
            "segment_source_context": {"type": "string"},
            "no_signal_reason": {"type": "string"},
            "events": {"type": "array", "minItems": 0, "maxItems": 32, "items": event},
        },
    }
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["episode_id", "segments"],
        "properties": {
            "episode_id": {"type": "string"},
            "segments": {"type": "array", "minItems": 2, "maxItems": 2, "items": segment},
        },
    }


def _source() -> dict:
    def segment(segment_id: str, unit_id: str, density: str) -> dict:
        text = "Alpha source evidence."
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
            segment("segment_dense", "S0U000", "dense"),
            segment("segment_empty", "S1U000", "no_signal"),
        ],
    }


def _full_event(index: int, *, core_ids: list[str] | None = None) -> dict:
    value = {
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
        "target_concept": "test",
        "claim_text": f"Claim {index}",
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
        "evidence_start_unit_id": "S0U000",
        "evidence_end_unit_id": "S0U000",
    }
    if core_ids is not None:
        value["core_ids"] = core_ids
    return value


def _core_event(index: int) -> dict:
    full = _full_event(index)
    return {field: full[field] for field in experiment.CORE_FIELDS}


def _core_output() -> dict:
    return {
        "episode_id": "episode_test",
        "segments": [
            {
                "segment_id": "segment_dense",
                "status": "coded",
                "covered_unit_ids": ["S0U000"],
                "events": [_core_event(index) for index in range(27)],
            },
            {
                "segment_id": "segment_empty",
                "status": "no_signal",
                "covered_unit_ids": ["S1U000"],
                "events": [],
            },
        ],
    }


def _join_output() -> dict:
    return {
        "episode_id": "episode_test",
        "segments": [
            {
                "segment_id": "segment_dense",
                "status": "coded",
                "segment_source_context": "substantive_dialogue",
                "no_signal_reason": "",
                "events": [
                    _full_event(index, core_ids=[f"S0C{index:03d}"])
                    for index in range(27)
                ],
            },
            {
                "segment_id": "segment_empty",
                "status": "no_signal",
                "segment_source_context": "substantive_dialogue",
                "no_signal_reason": "No eligible event.",
                "events": [],
            },
        ],
        "dropped_core_events": [],
    }


def _fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path]:
    root = tmp_path / "configured-canary"
    root.mkdir()
    source = _write_json(tmp_path / "source.private.json", _source())
    projection = _write_json(tmp_path / "projection-schema.json", _projection_schema())
    base = tmp_path / "base.private.md"
    base.write_text("Synthetic immutable extraction rules.\n", encoding="utf-8")
    binary = tmp_path / "codex"
    binary.write_text("synthetic codex binary\n", encoding="utf-8")
    monkeypatch.setattr(experiment, "PINNED_CODEX", binary.resolve())
    predecessors = {}
    for name in (
        "architecture_comparison",
        "phase_boundary",
        "verifier_benchmark",
        "v277_runtime_lock",
        "v277_terminal",
    ):
        predecessors[name] = _write_json(tmp_path / f"{name}.json", {"name": name})
    config_path = root / "experiment-config.json"
    config = {
        "schema_version": experiment.CONFIG_VERSION,
        "experiment_id": "synthetic_configured_canary",
        "architecture_id": "compact_segment_event_cores_then_episode_global_llm_owner_join",
        "output_root": str(root.resolve()),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "semantic_regex_or_keyword_filtering": False,
        "production_mutation_allowed": False,
        "holdout_authorized": False,
        "retry_count_per_turn": 0,
        "combined_total_tokens_maximum": 73000,
        "capacity_maximum_total_tokens_per_turn": 45000,
        "minimum_remaining_reserve_percent": 20,
        "quota_points_per_million_tokens": 17,
        "turns": [
            {
                "name": "compact_segment_event_cores",
                "model": "gpt-5.6-sol",
                "effort": "low",
                "total_tokens_maximum": 33000,
            },
            {
                "name": "episode_global_owner_join",
                "model": "gpt-5.6-sol",
                "effort": "low",
                "total_tokens_maximum": 40000,
            },
        ],
        "source_input": _record(source),
        "base_instructions": _record(base),
        "projection_schema": _record(projection),
        **{name: _record(path) for name, path in predecessors.items()},
    }
    _write_json(config_path, config)
    return config_path, source


class _FakeClient:
    def __init__(self, *, core_tokens: int = 30000, join_tokens: int = 39000):
        self.core_tokens = core_tokens
        self.join_tokens = join_tokens
        self.calls: list[Path] = []

    async def __aenter__(self) -> "_FakeClient":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def run_ephemeral_structured_turn(self, **kwargs: object) -> SimpleNamespace:
        sidecar = Path(str(kwargs["sidecar_path"]))
        output = Path(str(kwargs["output_path"]))
        checkpoint = Path(str(kwargs["capacity_checkpoint_path"]))
        self.calls.append(sidecar)
        is_core = "compact-segment-event-cores" in str(sidecar)
        tokens = self.core_tokens if is_core else self.join_tokens
        payload = _core_output() if is_core else _join_output()
        _write_json(checkpoint, {"schema_version": "synthetic_capacity", "cleared_for_semantic_turn": True})
        _write_json(output, payload)
        _write_json(
            sidecar,
            {
                "state": "completed",
                "status": "completed",
                "usage_status": "measured",
                "usage_complete": True,
                "auth_type": "chatgpt",
                "plan_type": "pro",
                "model": kwargs["model"],
                "effort": kwargs["effort"],
                "error_class": None,
                "usage": {
                    "input_tokens": tokens - 1000,
                    "cached_input_tokens": 0,
                    "output_tokens": 1000,
                    "reasoning_output_tokens": 100,
                    "total_tokens": tokens,
                },
            },
        )
        return SimpleNamespace(status_ok=True)


def test_freeze_verify_and_direct_lineage_mutation_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path, source = _fixture(tmp_path, monkeypatch)
    frozen = experiment.freeze_experiment(config_path)
    assert frozen["runtime_lock"].is_file()
    assert frozen["runtime_closure"].is_file()
    experiment.verify_frozen(config_path.parent)
    source.write_text(source.read_text(encoding="utf-8") + " ", encoding="utf-8")
    with pytest.raises(experiment.ConfiguredExperimentError):
        experiment.verify_frozen(config_path.parent)


def test_core_and_join_validation_enforce_exact_partition(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path, _source_path = _fixture(tmp_path, monkeypatch)
    frozen = experiment.freeze_experiment(config_path)
    core_packet, diagnostics = experiment.validate_core_output(_core_output(), frozen)
    assert diagnostics["core_event_count"] == 27
    valid = _join_output()
    normalized, provenance, join_diagnostics = experiment.validate_join_output(
        copy.deepcopy(valid), core_packet, frozen
    )
    assert len(normalized["segments"][0]["events"]) == 27
    assert len(provenance["events"]) == 27
    assert join_diagnostics["accounted_core_event_count"] == 27
    invalid = copy.deepcopy(valid)
    invalid["segments"][0]["events"][1]["core_ids"] = ["S0C000"]
    with pytest.raises(experiment.ConfiguredOutputContractError):
        experiment.validate_join_output(invalid, core_packet, frozen)
    moved = copy.deepcopy(valid)
    moved["segments"][0]["events"][0]["core_ids"] = []
    moved["segments"][1]["events"] = [
        {
            **_full_event(99, core_ids=["S0C000"]),
            "evidence_start_unit_id": "S1U000",
            "evidence_end_unit_id": "S1U000",
        }
    ]
    moved["segments"][1]["status"] = "coded"
    with pytest.raises(experiment.ConfiguredOutputContractError):
        experiment.validate_join_output(moved, core_packet, frozen)


def test_measured_two_turn_run_passes_and_terminal_prevents_replay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path, _source_path = _fixture(tmp_path, monkeypatch)
    client = _FakeClient()
    terminal = asyncio.run(
        experiment.run_experiment(config_path, client_factory=lambda _path: client)
    )
    assert terminal["terminal_reason"].endswith("support_alignment_required")
    assert terminal["usage"]["total_tokens"] == 69000
    assert len(client.calls) == 2
    repeated = asyncio.run(
        experiment.run_experiment(config_path, client_factory=lambda _path: client)
    )
    assert repeated == terminal
    assert len(client.calls) == 2


def test_core_token_ceiling_stops_before_join(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path, _source_path = _fixture(tmp_path, monkeypatch)
    client = _FakeClient(core_tokens=33001)
    terminal = asyncio.run(
        experiment.run_experiment(config_path, client_factory=lambda _path: client)
    )
    assert terminal["terminal_reason"] == "configured_canary_structural_or_cost_gate_not_passed"
    assert terminal["architecture_strategy_rejected"] is True
    assert len(client.calls) == 1
    assert not experiment._turn_paths(
        config_path.parent, "episode_global_owner_join"
    )["capacity"].exists()
