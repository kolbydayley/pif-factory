from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import app_server_sparse_event_frame_lane as lane
from research_factory import app_server_sparse_event_frame_recovery as recovery
from research_factory import app_server_signal_router_frontier_experiment as router
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
            segment("segment_dense", "Speaker reports 40 percent growth.", "dense", "S0U000"),
            segment("segment_empty", "No event here.", "no_signal", "S1U000"),
        ],
    }


def _direct_event() -> dict:
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
        "target_concept": "growth",
        "claim_text": "Speaker reports 40 percent growth.",
        "stance": "neutral",
        "certainty": "high",
        "temporal_horizon": "present",
        "causal_mechanism": "",
        "counterclaim": "",
        "metric_value": "40",
        "metric_unit": "percent",
        "metric_comparator": "",
        "metric_direction": "increase",
        "metric_raw_text": "40 percent",
        "signal_reason": "",
        "model_names": [],
        "product_names": [],
        "organizations": [],
        "people": ["Speaker"],
        "confidence": 0.9,
        "evidence_start_unit_id": "S0U000",
        "evidence_end_unit_id": "S0U000",
    }


def _direct_output() -> dict:
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
                "events": [_direct_event()],
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


def _sparse_event() -> dict:
    direct = _direct_event()
    return {
        "event_type": direct["event_type"],
        "claim_text": direct["claim_text"],
        "certainty": direct["certainty"],
        "temporal_horizon": direct["temporal_horizon"],
        "source_context_kind": direct["source_context_kind"],
        "confidence": direct["confidence"],
        "evidence_start_unit_id": direct["evidence_start_unit_id"],
        "evidence_end_unit_id": direct["evidence_end_unit_id"],
        "event_subtype": [direct["event_subtype"]],
        "target_concept": [direct["target_concept"]],
        "causal_mechanism": [],
        "counterclaim": [],
        "signal_reason": [],
        "claim_type": [direct["claim_type"]],
        "stance": [direct["stance"]],
        "speaker": [{"identity_kind": "named", "name": "Speaker", "type": "guest"}],
        "actor": [{"identity_kind": "named", "name": "Speaker", "type": "person"}],
        "reported_actor": [],
        "metric": [
            {
                "value": "40",
                "unit": "percent",
                "comparator": "",
                "direction": "increase",
                "raw_text": "40 percent",
            }
        ],
        "model_names": [],
        "product_names": [],
        "organizations": [],
        "people": ["Speaker"],
    }


def _sparse_output() -> dict:
    value = _direct_output()
    value["segments"][0]["events"] = [_sparse_event()]
    return value


def _fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "sparse-lane"
    root.mkdir(parents=True)
    binary = tmp_path / "codex"
    binary.write_text("synthetic pinned binary\n", encoding="utf-8")
    monkeypatch.setattr(lane, "PINNED_CODEX", binary.resolve())
    source_input = _write_json(tmp_path / "source-input.private.json", _source())
    source_prompt = tmp_path / "source-prompt.private.md"
    source_prompt.write_text("Synthetic prompt.\n", encoding="utf-8")
    source_base = tmp_path / "source-base.private.md"
    source_base.write_text("Synthetic base.\n", encoding="utf-8")
    projection_schema = _write_json(
        tmp_path / "projection-schema.json", _schema(_direct_output())
    )
    paths = {
        "source_input": source_input,
        "source_prompt": source_prompt,
        "source_base": source_base,
        "projection_schema": projection_schema,
    }
    for key in lane.LINEAGE_KEYS:
        if key not in paths:
            paths[key] = _write_json(tmp_path / f"{key}.json", {"artifact": key})
    config = {
        "schema_version": lane.CONFIG_VERSION,
        "experiment_id": "synthetic_sparse_event_frame_lane",
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
        "minimum_remaining_reserve_percent": lane.MINIMUM_REMAINING_RESERVE_PERCENT,
        "quota_points_per_million_tokens": lane.QUOTA_POINTS_PER_MILLION_TOKENS,
        "production_amortized_context_tokens": lane.PRODUCTION_CONTEXT_TOKENS,
        "production_scale": lane.PRODUCTION_SCALE,
        "baseline_total_tokens": lane.BASELINE_TOTAL_TOKENS,
        **{key: _record(path) for key, path in paths.items()},
    }
    return _write_json(root / "experiment-config.json", config)


def _recovery_fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "sparse-recovery"
    root.mkdir(parents=True)
    binary = tmp_path / "recovery-codex"
    binary.write_text("synthetic pinned binary\n", encoding="utf-8")
    monkeypatch.setattr(recovery, "PINNED_CODEX", binary.resolve())
    source_input = _write_json(tmp_path / "recovery-source-input.private.json", _source())
    source_prompt = tmp_path / "recovery-source-prompt.private.md"
    source_prompt.write_text("Synthetic prompt.\n", encoding="utf-8")
    source_base = tmp_path / "recovery-source-base.private.md"
    source_base.write_text("Synthetic base.\n", encoding="utf-8")
    projection_schema = _write_json(
        tmp_path / "recovery-projection-schema.json", _schema(_direct_output())
    )
    paths = {
        "source_input": source_input,
        "source_prompt": source_prompt,
        "source_base": source_base,
        "projection_schema": projection_schema,
    }
    for key in recovery.LINEAGE_KEYS:
        if key not in paths:
            paths[key] = _write_json(tmp_path / f"recovery-{key}.json", {"artifact": key})
    config = {
        "schema_version": recovery.CONFIG_VERSION,
        "experiment_id": "synthetic_sparse_event_frame_recovery",
        "architecture_id": recovery.ARCHITECTURE_ID,
        "recovery_version": 2,
        "output_root": str(root.resolve()),
        "model": recovery.MODEL,
        "effort": recovery.EFFORT,
        "declared_turn_count": 1,
        "retry_count": 0,
        "maximum_total_tokens": recovery.MAX_TOTAL_TOKENS,
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "semantic_regex_or_keyword_filtering": False,
        "production_mutation_allowed": False,
        "holdout_authorized": False,
        "minimum_remaining_reserve_percent": lane.MINIMUM_REMAINING_RESERVE_PERCENT,
        "quota_points_per_million_tokens": lane.QUOTA_POINTS_PER_MILLION_TOKENS,
        "production_amortized_context_tokens": lane.PRODUCTION_CONTEXT_TOKENS,
        "production_scale": lane.PRODUCTION_SCALE,
        "baseline_total_tokens": lane.BASELINE_TOTAL_TOKENS,
        **{key: _record(path) for key, path in paths.items()},
    }
    return _write_json(root / "experiment-config.json", config)


class _FakeClient:
    def __init__(self, output: dict | None = None, *, measured: bool = True):
        self.output = copy.deepcopy(output or _sparse_output())
        self.measured = measured
        self.calls = 0

    async def __aenter__(self) -> "_FakeClient":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def run_ephemeral_structured_turn(self, **kwargs: object) -> SimpleNamespace:
        self.calls += 1
        _write_json(Path(str(kwargs["capacity_checkpoint_path"])), {"cleared": True})
        _write_json(Path(str(kwargs["output_path"])), self.output)
        if self.measured:
            sidecar = {
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
            }
        else:
            sidecar = {
                "state": "failed",
                "status": "failed",
                "usage_status": "unknown",
                "usage_complete": False,
                "auth_type": "chatgpt",
                "plan_type": "pro",
                "model": lane.MODEL,
                "effort": lane.EFFORT,
                "error_class": "serverOverloaded",
            }
        _write_json(Path(str(kwargs["sidecar_path"])), sidecar)
        return SimpleNamespace()


def test_sparse_schema_and_projection_are_total_without_applicability_pairs() -> None:
    schema = lane.build_sparse_schema(_schema(_direct_output()))
    event_schema = schema["properties"]["segments"]["items"]["properties"]["events"]["items"]
    assert "metric_direction" not in event_schema["properties"]
    assert event_schema["properties"]["metric"]["maxItems"] == 1
    projected = lane.project_sparse_event(_sparse_event())
    assert projected["metric_direction"] == "increase"
    assert projected["reported_actor_type"] == "none"
    assert projected["causal_mechanism"] == ""


def test_success_runs_once_projects_exact_evidence_and_uses_no_count_proxy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = _fixture(tmp_path, monkeypatch)
    client = _FakeClient()
    terminal = asyncio.run(
        lane.run_experiment(config_path, client_factory=lambda _policy: client)
    )
    assert terminal["support_alignment_authorized"] is True
    assert client.calls == 1
    gate = json.loads((config_path.parent / "architecture-structural-gate.json").read_text())
    assert gate["checks"]["semantic_event_count_proxy_not_used"] is True
    normalized = json.loads((config_path.parent / "normalized-output.private.json").read_text())
    assert normalized["segments"][0]["events"][0]["evidence"] == "Speaker reports 40 percent growth."
    assert normalized["segments"][0]["events"][0]["window_id"] == 0
    assert asyncio.run(
        lane.run_experiment(config_path, client_factory=lambda _policy: client)
    ) == terminal
    assert client.calls == 1


def test_nonliteral_metric_rejects_whole_architecture_without_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = _fixture(tmp_path, monkeypatch)
    output = _sparse_output()
    output["segments"][0]["events"][0]["metric"][0]["raw_text"] = "99 percent"
    client = _FakeClient(output)
    terminal = asyncio.run(
        lane.run_experiment(config_path, client_factory=lambda _policy: client)
    )
    assert terminal["terminal_reason"] == "sparse_event_frame_structural_or_cost_gate_not_passed"
    assert terminal["usage_status"] == "complete"
    assert terminal["semantic_retry_count"] == 0
    assert client.calls == 1


def test_unknown_usage_and_lineage_mutation_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = _fixture(tmp_path, monkeypatch)
    client = _FakeClient(measured=False)
    terminal = asyncio.run(
        lane.run_experiment(config_path, client_factory=lambda _policy: client)
    )
    assert terminal["terminal_reason"] == "infrastructure_or_extraction_attempt_failed"
    assert terminal["usage_status"] == "unknown"
    assert terminal["accounting_complete"] is False
    asyncio.run(lane.run_experiment(config_path, client_factory=lambda _policy: client))
    assert client.calls == 1

    config_path2 = _fixture(tmp_path / "mutation", monkeypatch)
    frozen = lane.freeze_experiment(config_path2)
    lineage = Path(json.loads(config_path2.read_text())["source_prompt"]["path"])
    lineage.write_text("mutated prompt\n", encoding="utf-8")
    with pytest.raises(lane.SparseEventFrameLaneError):
        lane.verify_frozen(frozen["root"])


def test_recovery_schema_removes_only_provider_constraints_and_retains_validation() -> None:
    schema = recovery.build_provider_compatible_schema(_schema(_direct_output()))
    serialized = json.dumps(schema, sort_keys=True)
    assert '"minLength"' not in serialized
    assert '"uniqueItems"' not in serialized
    assert '"maxItems": 1' in serialized

    event = _sparse_event()
    event["target_concept"] = [""]
    with pytest.raises(recovery.SparseEventFrameRecoveryOutputError):
        recovery._validate_sparse_semantic_containers(event)
    event = _sparse_event()
    event["people"] = ["Speaker", "Speaker"]
    with pytest.raises(recovery.SparseEventFrameRecoveryOutputError):
        recovery._validate_sparse_semantic_containers(event)


def test_recovery_runs_once_with_exact_request_bytes_and_no_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = _recovery_fixture(tmp_path, monkeypatch)
    client = _FakeClient()
    terminal = asyncio.run(
        recovery.run_recovery(config_path, client_factory=lambda _policy: client)
    )
    assert terminal["support_alignment_authorized"] is True
    assert terminal["predecessor_unknown_usage_attempt_count"] == 1
    assert client.calls == 1
    config = json.loads(config_path.read_text())
    turn = recovery._turn_paths(config_path.parent)
    for name, key in (
        ("input", "source_input"),
        ("prompt", "source_prompt"),
        ("base", "source_base"),
        ("projection_schema", "projection_schema"),
    ):
        assert _record(turn[name])["sha256"] == config[key]["sha256"]
    asyncio.run(recovery.run_recovery(config_path, client_factory=lambda _policy: client))
    assert client.calls == 1


def test_recovery_unknown_usage_is_terminal_and_disallows_successor_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = _recovery_fixture(tmp_path, monkeypatch)
    client = _FakeClient(measured=False)
    terminal = asyncio.run(
        recovery.run_recovery(config_path, client_factory=lambda _policy: client)
    )
    assert terminal["terminal_reason"] == "infrastructure_or_extraction_attempt_failed"
    assert terminal["usage_status"] == "unknown"
    assert terminal["further_infrastructure_recovery_authorized"] is False
    assert terminal["architecture_strategy_rejected"] is True
    asyncio.run(recovery.run_recovery(config_path, client_factory=lambda _policy: client))
    assert client.calls == 1


def _router_fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "router-frontier"
    root.mkdir(parents=True)
    binary = tmp_path / "router-codex"
    binary.write_text("synthetic pinned binary\n", encoding="utf-8")
    monkeypatch.setattr(router, "PINNED_CODEX", binary.resolve())
    source_input = _write_json(tmp_path / "router-source-input.private.json", _source())
    original_base = tmp_path / "router-original-base.private.md"
    original_base.write_text("Synthetic immutable extraction rules.\n", encoding="utf-8")
    projection_schema = _write_json(
        tmp_path / "router-projection-schema.json", _schema(_direct_output())
    )
    paths = {
        "source_input": source_input,
        "original_base": original_base,
        "projection_schema": projection_schema,
    }
    for key in router.LINEAGE_KEYS:
        if key not in paths:
            paths[key] = _write_json(tmp_path / f"router-{key}.json", {"artifact": key})
    sparse_checkpoint = _write_json(
        tmp_path / "router-sparse-frame-checkpoint.json", {"state": "rejected"}
    )
    config = {
        "schema_version": router.CONFIG_VERSION,
        "experiment_id": "synthetic_signal_router_frontier",
        "architecture_id": router.ARCHITECTURE_ID,
        "output_root": str(root.resolve()),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "semantic_regex_or_keyword_filtering": False,
        "production_mutation_allowed": False,
        "holdout_authorized": False,
        "retry_count_per_turn": 0,
        "router_model": router.ROUTER_MODEL,
        "router_effort": router.ROUTER_EFFORT,
        "router_total_tokens_maximum": router.ROUTER_MAX_TOKENS,
        "extractor_model": router.EXTRACTOR_MODEL,
        "extractor_effort": router.EXTRACTOR_EFFORT,
        "extractor_total_tokens_maximum": router.EXTRACTOR_MAX_TOKENS,
        "combined_total_tokens_maximum": router.COMBINED_MAX_TOKENS,
        "minimum_remaining_reserve_percent": router.MINIMUM_REMAINING_RESERVE_PERCENT,
        "quota_points_per_million_tokens": router.QUOTA_POINTS_PER_MILLION_TOKENS,
        "production_amortized_context_tokens": router.PRODUCTION_CONTEXT_TOKENS,
        "production_scale": router.PRODUCTION_SCALE,
        "baseline_total_tokens": router.BASELINE_TOTAL_TOKENS,
        **{key: _record(path) for key, path in paths.items()},
        "sparse_frame_checkpoint": _record(sparse_checkpoint),
    }
    return _write_json(root / "experiment-config.json", config)


def _router_output(*, route_dense: bool = True) -> dict:
    return {
        "episode_id": "episode_test",
        "segments": [
            {
                "segment_id": "segment_dense",
                "route": "full_extraction" if route_dense else "no_signal",
                "segment_source_context": "substantive_dialogue",
                "no_signal_reason": "" if route_dense else "No eligible event.",
                "reviewed_unit_ids": ["S0U000"],
            },
            {
                "segment_id": "segment_empty",
                "route": "no_signal",
                "segment_source_context": "substantive_dialogue",
                "no_signal_reason": "No eligible event.",
                "reviewed_unit_ids": ["S1U000"],
            },
        ],
    }


class _RouterFakeClient:
    def __init__(self, *, route_dense: bool = True, unknown_extractor: bool = False):
        self.route_dense = route_dense
        self.unknown_extractor = unknown_extractor
        self.calls = 0

    async def __aenter__(self) -> "_RouterFakeClient":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def run_ephemeral_structured_turn(self, **kwargs: object) -> SimpleNamespace:
        self.calls += 1
        model = str(kwargs["model"])
        effort = str(kwargs["effort"])
        _write_json(Path(str(kwargs["capacity_checkpoint_path"])), {"cleared": True})
        if model == router.ROUTER_MODEL:
            output = _router_output(route_dense=self.route_dense)
            total = 3000
            unknown = False
        else:
            output = copy.deepcopy(_direct_output())
            output["segments"] = [output["segments"][0]]
            total = 4000
            unknown = self.unknown_extractor
        if not unknown:
            _write_json(Path(str(kwargs["output_path"])), output)
            sidecar = {
                "state": "completed",
                "status": "completed",
                "usage_status": "measured",
                "usage_complete": True,
                "auth_type": "chatgpt",
                "plan_type": "pro",
                "model": model,
                "effort": effort,
                "error_class": None,
                "usage": {
                    "input_tokens": total // 2,
                    "cached_input_tokens": 0,
                    "output_tokens": total - total // 2,
                    "reasoning_output_tokens": 100,
                    "total_tokens": total,
                },
            }
        else:
            sidecar = {
                "state": "failed",
                "status": "failed",
                "usage_status": "unknown",
                "usage_complete": False,
                "auth_type": "chatgpt",
                "plan_type": "pro",
                "model": model,
                "effort": effort,
                "error_class": "serverOverloaded",
            }
        _write_json(Path(str(kwargs["sidecar_path"])), sidecar)
        return SimpleNamespace()


def test_router_freezes_every_nonempty_route_subset_and_validates_exact_coverage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = _router_fixture(tmp_path, monkeypatch)
    frozen = router.freeze_experiment(config_path)
    subsets = router._route_subsets(frozen["source"])
    assert subsets == [
        ("segment_dense",),
        ("segment_empty",),
        ("segment_dense", "segment_empty"),
    ]
    for subset in subsets:
        assert router._variant_paths(config_path.parent, subset)["schema"].is_file()
    routed, rows = router._validate_router_output(
        _router_output(), frozen["source"], frozen["router"]["schema"]
    )
    assert routed == ("segment_dense",)
    assert rows["segment_empty"]["route"] == "no_signal"


def test_router_frontier_success_runs_two_turns_and_projects_exact_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = _router_fixture(tmp_path, monkeypatch)
    client = _RouterFakeClient()
    terminal = asyncio.run(
        router.run_experiment(config_path, client_factory=lambda _policy: client)
    )
    assert terminal["support_alignment_authorized"] is True
    assert client.calls == 2
    gate = json.loads((config_path.parent / "architecture-structural-gate.json").read_text())
    assert gate["checks"]["semantic_event_count_proxy_not_used"] is True
    assert gate["usage"]["router_total_tokens"] == 3000
    assert gate["usage"]["extractor_total_tokens"] == 4000
    normalized = json.loads((config_path.parent / "normalized-output.private.json").read_text())
    assert normalized["segments"][0]["events"][0]["evidence"] == "Speaker reports 40 percent growth."
    assert normalized["segments"][1]["events"] == []
    asyncio.run(router.run_experiment(config_path, client_factory=lambda _policy: client))
    assert client.calls == 2


def test_router_all_no_signal_stops_before_extractor_without_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = _router_fixture(tmp_path, monkeypatch)
    client = _RouterFakeClient(route_dense=False)
    terminal = asyncio.run(
        router.run_experiment(config_path, client_factory=lambda _policy: client)
    )
    assert terminal["terminal_reason"] == "signal_router_frontier_structural_or_cost_gate_not_passed"
    assert terminal["usage_status"] == "complete"
    assert terminal["semantic_attempt_count"] == 1
    assert client.calls == 1


def test_router_unknown_extractor_usage_is_immutable_and_not_retried(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = _router_fixture(tmp_path, monkeypatch)
    client = _RouterFakeClient(unknown_extractor=True)
    terminal = asyncio.run(
        router.run_experiment(config_path, client_factory=lambda _policy: client)
    )
    assert terminal["terminal_reason"] == "infrastructure_or_extraction_attempt_failed"
    assert terminal["usage_status"] == "unknown"
    assert terminal["accounting_complete"] is False
    assert terminal["semantic_attempt_count"] == 2
    asyncio.run(router.run_experiment(config_path, client_factory=lambda _policy: client))
    assert client.calls == 2
