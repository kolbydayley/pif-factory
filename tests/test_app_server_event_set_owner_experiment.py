from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import app_server_event_set_owner_experiment as owner
from research_factory import app_server_compact_owner_protocol as compact
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


def _event_schema() -> dict:
    strings = (
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
    )
    properties = {name: {"type": "string"} for name in strings}
    for name in ("model_names", "product_names", "organizations", "people"):
        properties[name] = {"type": "array", "items": {"type": "string"}}
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
            "events": {"type": "array", "items": event},
        },
    }
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["episode_id", "segments"],
        "properties": {
            "episode_id": {"type": "string"},
            "segments": {"type": "array", "items": segment},
        },
    }


def _event() -> dict:
    return {
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
        "claim_text": "Alpha works",
        "stance": "supportive",
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


def _source(long_text: bool = False) -> dict:
    dense_text = "Alpha works." if not long_text else "Alpha works. " * 5000
    return {
        "episode_id": "episode_test",
        "segments": [
            {
                "segment_id": "segment_dense",
                "segment_text": dense_text,
                "density_stratum": "dense",
                "boundaries": [
                    {
                        "window_id": 0,
                        "extract_start": 0,
                        "extract_end": len(dense_text),
                        "owner_start": 0,
                        "owner_end": len(dense_text),
                    }
                ],
                "units": [
                    {
                        "unit_id": "S0U000",
                        "window_id": 0,
                        "text": dense_text,
                        "start_char": 0,
                        "end_char": len(dense_text),
                    }
                ],
            },
            {
                "segment_id": "segment_empty",
                "segment_text": "General introduction.",
                "density_stratum": "no_signal",
                "boundaries": [
                    {
                        "window_id": 0,
                        "extract_start": 0,
                        "extract_end": 21,
                        "owner_start": 0,
                        "owner_end": 21,
                    }
                ],
                "units": [
                    {
                        "unit_id": "S1U000",
                        "window_id": 0,
                        "text": "General introduction.",
                        "start_char": 0,
                        "end_char": 21,
                    }
                ],
            },
        ],
    }


def _base_output() -> dict:
    event = _event()
    event.pop("evidence_start_unit_id")
    event.pop("evidence_end_unit_id")
    event["window_id"] = 0
    event["evidence"] = "Alpha works."
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


def _provenance() -> dict:
    return {
        "episode_id": "episode_test",
        "events": [
            {
                "segment_id": "segment_dense",
                "event_index": 0,
                "evidence_start_unit_id": "S0U000",
                "evidence_end_unit_id": "S0U000",
                "start_char": 0,
                "end_char": 12,
                "window_id": 0,
                "evidence_sha256": "synthetic",
            }
        ],
    }


def _fixture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, long_text: bool = False
) -> Path:
    root = tmp_path / "event-set-owner"
    root.mkdir()
    binary = tmp_path / "codex"
    binary.write_text("synthetic pinned binary\n", encoding="utf-8")
    monkeypatch.setattr(owner, "PINNED_CODEX", binary.resolve())
    source = _write_json(tmp_path / "source.private.json", _source(long_text))
    output = _write_json(tmp_path / "base-output.private.json", _base_output())
    provenance = _write_json(tmp_path / "base-provenance.private.json", _provenance())
    projection = _write_json(tmp_path / "projection-schema.json", _projection_schema())
    instructions = tmp_path / "base.private.md"
    instructions.write_text("Synthetic immutable extraction rules.\n", encoding="utf-8")
    records = {}
    for key in owner.LINEAGE_KEYS:
        if key == "source_input":
            path = source
        elif key == "base_output":
            path = output
        elif key == "base_provenance":
            path = provenance
        elif key == "projection_schema":
            path = projection
        elif key == "base_instructions":
            path = instructions
        else:
            path = _write_json(tmp_path / f"{key}.json", {"artifact": key})
        records[key] = _record(path)
    config = {
        "schema_version": owner.CONFIG_VERSION,
        "experiment_id": "synthetic_event_set_owner_v1",
        "architecture_id": owner.ARCHITECTURE_ID,
        "output_root": str(root.resolve()),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "semantic_regex_or_keyword_filtering": False,
        "production_mutation_allowed": False,
        "holdout_authorized": False,
        "retry_count": 0,
        "model": "gpt-5.6-sol",
        "effort": "low",
        "new_turn_total_tokens_maximum": 28000,
        "adopted_base_total_tokens": 44474,
        "combined_total_tokens_maximum": 72474,
        "minimum_remaining_reserve_percent": 20,
        "quota_points_per_million_tokens": 17,
        "production_amortized_context_tokens": 600538,
        "production_scale": 30,
        "baseline_total_tokens": 10065426,
        "prompt_bytes_maximum": 50000,
        "schema_bytes_maximum": 6000,
        **records,
    }
    return _write_json(root / "experiment-config.json", config)


def _keep_output() -> dict:
    return {
        "episode_id": "episode_test",
        "operations": [
            {
                "segment_id": "segment_dense",
                "action": "keep",
                "input_event_ids": ["S0E000"],
                "replacement_events": [],
                "rationale": "The proposal is source-entailing.",
            }
        ],
    }


class _FakeClient:
    def __init__(self, output: dict | None = None):
        self.output = output or _keep_output()
        self.capacity_path: Path | None = None

    async def __aenter__(self) -> "_FakeClient":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def run_ephemeral_structured_turn(self, **kwargs: object) -> SimpleNamespace:
        capacity = Path(str(kwargs["capacity_checkpoint_path"]))
        sidecar = Path(str(kwargs["sidecar_path"]))
        output = Path(str(kwargs["output_path"]))
        self.capacity_path = capacity
        _write_json(capacity, {"schema_version": "synthetic_capacity"})
        _write_json(output, self.output)
        _write_json(
            sidecar,
            {
                "state": "completed",
                "status": "completed",
                "usage_status": "measured",
                "usage_complete": True,
                "auth_type": "chatgpt",
                "plan_type": "pro",
                "model": "gpt-5.6-sol",
                "effort": "low",
                "error_class": None,
                "usage": {
                    "input_tokens": 12000,
                    "cached_input_tokens": 0,
                    "output_tokens": 3000,
                    "reasoning_output_tokens": 500,
                    "total_tokens": 15000,
                },
            },
        )
        return SimpleNamespace()


def test_freeze_is_bounded_and_verifiable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = _fixture(tmp_path, monkeypatch)
    frozen = owner.freeze_experiment(config)
    audit = json.loads((config.parent / "serialization-audit.json").read_text())
    assert audit["reference_visible_to_model"] is False
    assert audit["semantic_fields_omitted_when_nonempty"] == 0
    assert audit["prompt_bytes"] <= 50000
    assert audit["schema_bytes"] <= 6000
    assert frozen["turn"]["capacity"] == (
        config.parent / "turns/compact-source-unit-global-owner/capacity.json"
    )
    owner.verify_frozen(config.parent)


def test_run_projects_llm_operations_and_uses_exact_capacity_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _fixture(tmp_path, monkeypatch)
    fake = _FakeClient()
    terminal = asyncio.run(
        owner.run_experiment(config, client_factory=lambda _path: fake)
    )
    assert terminal["support_alignment_authorized"] is True
    assert terminal["production_amortized_total_token_ratio"] < 0.28
    assert fake.capacity_path == (
        config.parent / "turns/compact-source-unit-global-owner/capacity.json"
    )
    normalized = json.loads((config.parent / "normalized-output.private.json").read_text())
    assert normalized["segments"][0]["events"][0]["claim_text"] == "Alpha works"


def test_missing_base_event_fails_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = _fixture(tmp_path, monkeypatch)
    frozen = owner.freeze_experiment(config)
    output = {
        "episode_id": "episode_test",
        "operations": [
            {
                "segment_id": "segment_dense",
                "action": "add",
                "input_event_ids": [],
                "replacement_events": [_event()],
                "rationale": "Synthetic add.",
            }
        ],
    }
    with pytest.raises(owner.EventSetOwnerOutputError, match="exact partition"):
        owner.validate_and_project_output(output, frozen)


def test_replacement_semantics_are_llm_owned_and_exactly_projected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _fixture(tmp_path, monkeypatch)
    frozen = owner.freeze_experiment(config)
    replacement = _event()
    replacement["claim_text"] = "Alpha works exactly"
    output = {
        "episode_id": "episode_test",
        "operations": [
            {
                "segment_id": "segment_dense",
                "action": "replace",
                "input_event_ids": ["S0E000"],
                "replacement_events": [replacement],
                "rationale": "Rewrite the complete event.",
            }
        ],
    }
    normalized, provenance, diagnostics = owner.validate_and_project_output(output, frozen)
    assert normalized["segments"][0]["events"][0]["claim_text"] == "Alpha works exactly"
    assert normalized["segments"][0]["events"][0]["evidence"] == "Alpha works."
    assert len(provenance["events"]) == 1
    assert diagnostics["semantic_event_count_proxy_used"] is False


def test_nonliteral_metric_fails_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = _fixture(tmp_path, monkeypatch)
    frozen = owner.freeze_experiment(config)
    replacement = _event()
    replacement.update(
        {
            "metric_value": "99",
            "metric_unit": "percent",
            "metric_raw_text": "99 percent",
            "metric_direction": "increase",
        }
    )
    output = {
        "episode_id": "episode_test",
        "operations": [
            {
                "segment_id": "segment_dense",
                "action": "replace",
                "input_event_ids": ["S0E000"],
                "replacement_events": [replacement],
                "rationale": "Synthetic metric.",
            }
        ],
    }
    with pytest.raises(owner.EventSetOwnerOutputError, match="literal evidence"):
        owner.validate_and_project_output(output, frozen)


def test_direct_lineage_mutation_invalidates_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _fixture(tmp_path, monkeypatch)
    owner.freeze_experiment(config)
    value = json.loads(config.read_text())
    predecessor = Path(value["architecture_comparison"]["path"])
    predecessor.write_text('{"mutated":true}\n', encoding="utf-8")
    with pytest.raises(owner.EventSetOwnerError):
        owner.verify_frozen(config.parent)


def test_prompt_size_cap_fails_before_launch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _fixture(tmp_path, monkeypatch, long_text=True)
    with pytest.raises(owner.EventSetOwnerError, match="prompt exceeds byte cap"):
        owner.freeze_experiment(config)
    assert not (config.parent / "launch-receipt.json").exists()
    assert not (config.parent / "turns/compact-source-unit-global-owner/sidecar.json").exists()


def _compact_fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    old_config_path = _fixture(tmp_path, monkeypatch)
    old_config = json.loads(old_config_path.read_text())
    root = tmp_path / "compact-owner"
    root.mkdir()
    monkeypatch.setattr(compact, "PINNED_CODEX", owner.PINNED_CODEX)
    protocol = _write_json(tmp_path / "protocol-design.json", {"protocol": "compact"})
    nonacceptance = _write_json(
        tmp_path / "prelaunch-nonacceptance.json", {"semantic_attempt_started": False}
    )
    config = {
        "schema_version": compact.CONFIG_VERSION,
        "experiment_id": "synthetic_compact_owner_v1",
        "architecture_id": compact.ARCHITECTURE_ID,
        "output_root": str(root.resolve()),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "semantic_regex_or_keyword_filtering": False,
        "production_mutation_allowed": False,
        "holdout_authorized": False,
        "retry_count": 0,
        "model": "gpt-5.6-sol",
        "effort": "low",
        "new_turn_total_tokens_maximum": 29400,
        "adopted_base_total_tokens": 44474,
        "combined_total_tokens_maximum": 73874,
        "minimum_remaining_reserve_percent": 20,
        "quota_points_per_million_tokens": 17,
        "production_amortized_context_tokens": 600538,
        "production_scale": 30,
        "baseline_total_tokens": 10065426,
        "serialized_tokens_maximum": compact.MAX_SERIALIZED_TOKENS,
        "fixed_input_overhead_tokens_maximum": compact.MAX_FIXED_INPUT_OVERHEAD_TOKENS,
        "minimum_output_headroom_tokens": compact.MIN_OUTPUT_HEADROOM_TOKENS,
        **{key: old_config[key] for key in owner.LINEAGE_KEYS},
        "prelaunch_nonacceptance": _record(nonacceptance),
        "protocol_design": _record(protocol),
    }
    return _write_json(root / "experiment-config.json", config)


def _compact_keep_output(event_id: str = "B0000") -> dict:
    return {
        "e": "episode_test",
        "ops": [{"s": "segment_dense", "a": "k", "i": [event_id], "o": []}],
    }


class _CompactFakeClient(_FakeClient):
    def __init__(self, output: dict | None = None):
        super().__init__(output or _compact_keep_output())

    async def run_ephemeral_structured_turn(self, **kwargs: object) -> SimpleNamespace:
        await super().run_ephemeral_structured_turn(**kwargs)
        sidecar = Path(str(kwargs["sidecar_path"]))
        _write_json(
            sidecar,
            {
                "state": "completed",
                "status": "completed",
                "usage_status": "measured",
                "usage_complete": True,
                "auth_type": "chatgpt",
                "plan_type": "pro",
                "model": "gpt-5.6-sol",
                "effort": "low",
                "error_class": None,
                "usage": {
                    "input_tokens": 22000,
                    "cached_input_tokens": 0,
                    "output_tokens": 2500,
                    "reasoning_output_tokens": 500,
                    "total_tokens": 24500,
                },
            },
        )
        return SimpleNamespace(status_ok=True, output=self.output)


def test_compact_protocol_has_measured_output_headroom(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _compact_fixture(tmp_path, monkeypatch)
    frozen = compact.freeze_experiment(config)
    audit = json.loads((config.parent / "token-envelope-audit.json").read_text())
    assert audit["serialized_o200k_tokens"] <= compact.MAX_SERIALIZED_TOKENS
    assert audit["projected_output_headroom_tokens"] >= compact.MIN_OUTPUT_HEADROOM_TOKENS
    assert audit["semantic_source_units_pruned"] == 0
    assert sorted(frozen["catalog"]) == ["B0000"]
    compact.verify_frozen(config.parent)


def test_compact_protocol_translates_without_semantic_projection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _compact_fixture(tmp_path, monkeypatch)
    frozen = compact.freeze_experiment(config)
    translated = compact._translate_output(_compact_keep_output())
    owner_frozen = {
        "turn": {"schema": config.parent / "translated-owner-schema.json"},
        "source": frozen["source"],
        "catalog": frozen["catalog"],
        "base_output": frozen["base_output"],
    }
    normalized, _provenance, diagnostics = owner.validate_and_project_output(
        translated, owner_frozen
    )
    assert normalized["segments"][0]["events"][0]["claim_text"] == "Alpha works"
    assert diagnostics["accounted_base_event_count"] == 1


def test_compact_replacement_key_coverage_is_fail_closed() -> None:
    with pytest.raises(compact.CompactOwnerError, match="field coverage"):
        compact._translate_output(
            {
                "e": "episode_test",
                "ops": [
                    {
                        "s": "segment_dense",
                        "a": "r",
                        "i": ["B0000"],
                        "o": [{"a": "incomplete"}],
                    }
                ],
            }
        )


def test_compact_run_uses_one_measured_managed_auth_turn(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _compact_fixture(tmp_path, monkeypatch)
    fake = _CompactFakeClient()
    terminal = asyncio.run(
        compact.run_experiment(config, client_factory=lambda _path: fake)
    )
    assert terminal["support_alignment_authorized"] is True
    assert terminal["production_amortized_total_token_ratio"] < 0.28
    assert fake.capacity_path == (
        config.parent / "turns/source-complete-columnar-global-owner/capacity.json"
    )
    assert terminal["semantic_attempt_count"] == 1
