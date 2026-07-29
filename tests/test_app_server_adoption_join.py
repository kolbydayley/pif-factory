from __future__ import annotations

import asyncio
import copy
import json
import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import app_server_adoption_join as adoption
from research_factory.app_server_runtime_verifier import ContentHashCache


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_ROOT = (
    PROJECT_ROOT / "work/app-server-development-v2/unattended-pipeline-v5"
).resolve()
CANARY_ROOT = (
    PIPELINE_ROOT / "development-canary-compact-core-global-join-2026-07-17"
).resolve()
PHASE_ROOT = (PIPELINE_ROOT / "phase-boundary-v275-v277-2026-07-17").resolve()


def _write_json(path: Path, value: object) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def _record(path: Path) -> dict:
    return ContentHashCache().record(path)


def _copy(source: Path, target: Path) -> Path:
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, target)
    return target


def _fixture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[Path, dict, Path]:
    root = tmp_path / "adoption-only"
    root.mkdir()
    copied = tmp_path / "lineage"
    source_input = _copy(
        Path(
            json.loads((CANARY_ROOT / "experiment-config.json").read_text())["source_input"][
                "path"
            ]
        ),
        copied / "source.private.json",
    )
    core_turn = CANARY_ROOT / "turns/compact-segment-event-cores"
    original_join = tmp_path / "original-join-never-started"
    original_join.mkdir()
    records = {
        "source_input": source_input,
        "core_schema": _copy(core_turn / "schema.json", copied / "core-schema.json"),
        "core_output": _copy(
            core_turn / "output.private.json", copied / "core-output.private.json"
        ),
        "core_sidecar": _copy(core_turn / "sidecar.json", copied / "core-sidecar.json"),
        "core_capacity": _copy(core_turn / "capacity.json", copied / "core-capacity.json"),
        "predecessor_terminal": _copy(
            CANARY_ROOT / "terminal.json", copied / "terminal.json"
        ),
        "predecessor_runtime_lock": CANARY_ROOT / "runtime-lock.json",
        "predecessor_runtime_closure": CANARY_ROOT / "runtime-lock-closure.json",
        "convergence_blocker": _copy(
            PHASE_ROOT / "development-convergence-blocker.json",
            copied / "convergence-blocker.json",
        ),
        "join_base": _copy(
            CANARY_ROOT / "turns/episode-global-owner-join/base-instructions.private.md",
            copied / "join-base.private.md",
        ),
        "join_schema": _copy(
            CANARY_ROOT / "turns/episode-global-owner-join/schema.json",
            copied / "join-schema.json",
        ),
    }
    binary = tmp_path / "codex"
    binary.write_text("synthetic pinned codex\n", encoding="utf-8")
    monkeypatch.setattr(adoption, "PINNED_CODEX", binary.resolve())
    config = {
        "schema_version": adoption.CONFIG_VERSION,
        "experiment_id": "synthetic_adoption_join",
        "architecture_id": "adopt_measured_compact_cores_then_episode_global_llm_owner_join",
        "output_root": str(root.resolve()),
        "original_join_root": str(original_join.resolve()),
        "model": "gpt-5.6-sol",
        "effort": "low",
        "turn_name": adoption.TURN_NAME,
        "semantic_launch_authorized": False,
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "retry_count": 0,
        "adopted_core_tokens": 30253,
        "join_total_tokens_maximum": 40000,
        "combined_total_tokens_maximum": 70253,
        "minimum_remaining_reserve_percent": 20,
        "quota_points_per_million_tokens": 17,
        "production_mutation_allowed": False,
        "holdout_authorized": False,
        **{name: _record(path) for name, path in records.items()},
    }
    config_path = _write_json(root / "experiment-config.json", config)
    return config_path, config, original_join


def _full_event(core: dict, *, core_ids: list[str], suffix: str = "") -> dict:
    raw_metric = str(core.get("metric_raw_text") or "")
    return {
        "event_type": core["event_type"],
        "event_subtype": "",
        "claim_type": "descriptive",
        "actor_name": core["actor_name"],
        "actor_type": core["actor_type"],
        "speaker_name": core["speaker_name"],
        "speaker_role": core["speaker_role"],
        "reported_actor_name": core["reported_actor_name"],
        "reported_actor_type": core["reported_actor_type"],
        "source_context_kind": core["source_context_kind"],
        "target_concept": core["target_concept"],
        "claim_text": str(core["claim_text"]) + suffix,
        "stance": core["stance"],
        "certainty": core["certainty"],
        "temporal_horizon": core["temporal_horizon"],
        "causal_mechanism": core["causal_mechanism"],
        "counterclaim": "",
        "metric_value": "",
        "metric_unit": "",
        "metric_comparator": "",
        "metric_direction": "unknown" if raw_metric else "not_applicable",
        "metric_raw_text": raw_metric,
        "signal_reason": "",
        "model_names": [],
        "product_names": [],
        "organizations": [],
        "people": [],
        "confidence": 0.8,
        "evidence_start_unit_id": core["evidence_start_unit_id"],
        "evidence_end_unit_id": core["evidence_end_unit_id"],
        "core_ids": core_ids,
    }


def _join_output(core_packet: dict) -> dict:
    rows = []
    for segment_index, segment in enumerate(core_packet["segments"]):
        events = [
            _full_event(event, core_ids=[event["core_id"]])
            for event in segment["events"]
        ]
        if segment_index == 0:
            last = segment["events"][-1]
            events.extend(
                _full_event(last, core_ids=[], suffix=f" added-{index}")
                for index in range(3)
            )
        rows.append(
            {
                "segment_id": segment["segment_id"],
                "status": "coded" if events else "no_signal",
                "segment_source_context": {
                    "kind": "substantive_dialogue",
                    "confidence": 0.9,
                    "rationale": "Synthetic test context.",
                },
                "no_signal_reason": "",
                "events": events,
            }
        )
    return {
        "episode_id": core_packet["episode_id"],
        "segments": rows,
        "dropped_core_events": [],
    }


class _FakeClient:
    def __init__(self, output: dict):
        self.output = output
        self.calls: list[dict] = []

    async def __aenter__(self) -> "_FakeClient":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def run_ephemeral_structured_turn(self, **kwargs: object) -> SimpleNamespace:
        self.calls.append(dict(kwargs))
        sidecar = Path(str(kwargs["sidecar_path"]))
        output = Path(str(kwargs["output_path"]))
        capacity = Path(str(kwargs["capacity_checkpoint_path"]))
        _write_json(capacity, {"cleared_for_semantic_turn": True})
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
                "model": kwargs["model"],
                "effort": kwargs["effort"],
                "error_class": None,
                "usage": {
                    "input_tokens": 38000,
                    "cached_input_tokens": 0,
                    "output_tokens": 1000,
                    "reasoning_output_tokens": 100,
                    "total_tokens": 39000,
                },
            },
        )
        return SimpleNamespace(status_ok=True)


def _authorize(config_path: Path, config: dict) -> None:
    root = config_path.parent
    _write_json(
        root / "operator-authorization.json",
        {
            "schema_version": adoption.AUTHORIZATION_VERSION,
            "authority": "direct_user_instruction",
            "scope": "one_adoption_only_episode_global_owner_join",
            "semantic_launch_authorized": True,
            "retry_count": 0,
            "adopted_core_turn_replayed": False,
            "production_mutation_allowed": False,
            "holdout_authorized": False,
            "runtime_lock": _record(root / "runtime-lock.json"),
            "convergence_blocker": config["convergence_blocker"],
        },
    )


def test_prepare_is_presemantic_and_run_refuses_without_override(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path, _config, _original_join = _fixture(tmp_path, monkeypatch)
    frozen = adoption.freeze_prepared(config_path)
    assert frozen["prepared_receipt"].is_file()
    turn = frozen["turn"]
    assert not any(turn[name].exists() for name in ("capacity", "sidecar", "output"))
    with pytest.raises(adoption.AdoptionAuthorizationRequired):
        asyncio.run(adoption.run_authorized(config_path))
    assert not (config_path.parent / "launch-receipt.json").exists()
    assert not any(turn[name].exists() for name in ("capacity", "sidecar", "output"))


def test_predecessor_mutation_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path, config, _original_join = _fixture(tmp_path, monkeypatch)
    adoption.freeze_prepared(config_path)
    core_output = Path(config["core_output"]["path"])
    core_output.write_text(core_output.read_text(encoding="utf-8") + " ", encoding="utf-8")
    with pytest.raises(adoption.AdoptionJoinError):
        adoption.verify_prepared(config_path.parent)


def test_original_join_artifact_appearance_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path, _config, original_join = _fixture(tmp_path, monkeypatch)
    adoption.freeze_prepared(config_path)
    _write_json(original_join / "capacity.json", {"unexpected": True})
    with pytest.raises(adoption.AdoptionJoinError):
        adoption.verify_prepared(config_path.parent)


def test_authorized_fake_run_is_one_turn_and_terminal_prevents_replay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path, config, _original_join = _fixture(tmp_path, monkeypatch)
    frozen = adoption.freeze_prepared(config_path)
    _authorize(config_path, config)
    client = _FakeClient(_join_output(frozen["core_packet"]))
    terminal = asyncio.run(
        adoption.run_authorized(config_path, client_factory=lambda _path: client)
    )
    assert terminal["terminal_reason"].endswith("support_alignment_required")
    assert terminal["semantic_attempt_count"] == 1
    assert terminal["semantic_retry_count"] == 0
    assert terminal["combined_extraction_tokens"] == 69253
    assert len(client.calls) == 1
    expected_capacity = adoption._turn_paths(config_path.parent)["capacity"]
    assert Path(str(client.calls[0]["capacity_checkpoint_path"])) == expected_capacity
    repeated = asyncio.run(
        adoption.run_authorized(config_path, client_factory=lambda _path: client)
    )
    assert repeated == terminal
    assert len(client.calls) == 1
