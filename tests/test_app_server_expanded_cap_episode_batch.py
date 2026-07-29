from __future__ import annotations

import asyncio
import copy
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import app_server_expanded_cap_episode_batch as adapter
from research_factory import codex_app_server
from research_factory.util import sha256_text


@pytest.fixture(autouse=True)
def _restore_main_thread_event_loop() -> None:
    """Keep Python 3.9 tests from leaking asyncio.run's closed-loop state."""

    yield
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())
    else:
        if loop.is_closed():
            asyncio.set_event_loop(asyncio.new_event_loop())


def _segment(index: int) -> dict:
    text = f"Speaker {index}:\nSystem {index} improves latency.\n"
    end = len(text)
    return {
        "segment_id": f"seg_fixture_{index}",
        "segment_text": text,
        "density_stratum": "fixture",
        "boundaries": [
            {
                "window_id": 0,
                "chunk_index": 0,
                "center_start": 0,
                "center_end": end,
                "owner_start": 0,
                "owner_end": end,
                "extract_start": 0,
                "extract_end": end,
            }
        ],
    }


def _episode(*, segment_count: int = 7, suffix: str = "one") -> dict:
    segments = [_segment(index) for index in range(segment_count)]
    if suffix != "one":
        for segment in segments:
            segment["segment_id"] += f"_{suffix}"
    return {
        "episode_id": f"ep_fixture_{suffix}",
        "source_name": "Fixture Source",
        "episode_title": "Fixture Episode",
        "context_summary": "Synthetic fixture context only.",
        "speaker_map": [],
        "section_map": [
            {"section_id": "fixture-section", "segment_ids": [segments[0]["segment_id"]]}
        ],
        "entity_seed": {"organizations": ["Fixture Org"]},
        "concept_seed": ["fixture latency"],
        "extraction_guidance": "Use only the synthetic source units.",
        "excluded_source_context": ["synthetic sponsor reads"],
        "segments": segments,
    }


def _fixture_capacity_args(
    episodes: list[dict],
    *,
    batch_size: int,
    thread_mode: str,
    concurrency: int = 1,
) -> dict:
    requests = [
        request
        for episode in episodes
        for request in adapter.prepare_episode_batches(
            episode, batch_size=batch_size, thread_mode=thread_mode
        )
    ]
    now = datetime.now(timezone.utc)
    episode_ids = [episode["episode_id"] for episode in episodes]
    receipt = {
        "schema_version": adapter.CAPACITY_ADMISSION_VERSION,
        "admission_id": f"fixture-admission-{thread_mode}-{batch_size}",
        "issued_at": (now - timedelta(seconds=1)).isoformat(),
        "expires_at": (now + timedelta(minutes=5)).isoformat(),
        "state": "admitted",
        "issued_by": "evaluation_coordinator",
        "verification_mode": "fixture_verified",
        "fixture": True,
        "winner_system_id": adapter.WINNER_SYSTEM_ID,
        "batch_size": batch_size,
        "thread_mode": thread_mode,
        "model": adapter.MODEL,
        "effort": adapter.EFFORT,
        "concurrency": concurrency,
        "episode_ids_sha256": sha256_text(
            adapter._canonical_json(episode_ids)  # noqa: SLF001
        ),
        "episode_count": len(episodes),
        "segment_count": sum(len(episode["segments"]) for episode in episodes),
        "batch_count": len(requests),
        "live_capacity_available": True,
        "managed_chatgpt_auth_only": True,
        "verification_evidence_sha256": "e" * 64,
    }
    return {
        "capacity_admission": receipt,
        "capacity_admission_sha256": adapter.capacity_admission_sha256(receipt),
        "fixture_mode": True,
    }


def _no_signal_output(prompt: str) -> dict:
    packet = json.loads(prompt.split("\n", 1)[1])
    return {
        "episode_id": packet["episode_id"],
        "segments": [
            {
                "segment_id": segment["segment_id"],
                "status": "no_signal",
                "segment_source_context": {
                    "kind": "show_setup",
                    "confidence": 1.0,
                    "rationale": "Synthetic fixture classification.",
                },
                "no_signal_reason": "Synthetic fixture returns no semantic event.",
                "unit_receipts": [
                    {
                        "unit_id": unit["unit_id"],
                        "eligible_event_count": 0,
                        "unresolved_count": 0,
                    }
                    for unit in segment["source_units"]
                ],
                "coverage_audit": {
                    "all_source_units_reviewed": True,
                    "unresolved_count": 0,
                },
                "events": [],
            }
            for segment in packet["segments"]
        ],
    }


def _event(unit_id: str) -> dict:
    return {
        "event_type": "capability_claim",
        "evidence_start_unit_id": unit_id,
        "evidence_end_unit_id": unit_id,
        "claim_text": "System 0 improves latency.",
        "certainty": "high",
        "temporal_horizon": "present",
        "source_context_kind": "substantive_dialogue",
        "confidence": 0.9,
        "event_subtype": {"applicability": "present", "value": "latency"},
        "target_concept": {
            "applicability": "present",
            "value": "system latency",
        },
        "causal_mechanism": {
            "applicability": "not_applicable",
            "value": "",
        },
        "counterclaim": {"applicability": "not_applicable", "value": ""},
        "signal_reason": {
            "applicability": "present",
            "value": "The synthetic source states the capability directly.",
        },
        "claim_type": {"applicability": "present", "value": "descriptive"},
        "stance": {"applicability": "present", "value": "supportive"},
        "speaker": {
            "applicability": "present",
            "name": "Speaker 0",
            "type": "speaker",
        },
        "actor": {"applicability": "unknown", "name": "", "type": "unknown"},
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
        "model_names": {"applicability": "not_applicable", "value": []},
        "product_names": {"applicability": "not_applicable", "value": []},
        "organizations": {"applicability": "not_applicable", "value": []},
        "people": {"applicability": "not_applicable", "value": []},
    }


class _FixtureClient:
    def __init__(
        self,
        *,
        duplicate_turn_ids: bool = False,
        instruction_source_drift: bool = False,
        forged_thread_lineage: bool = False,
        divergent_result_output: bool = False,
        timeout_after_dispatch: bool = False,
    ) -> None:
        self.account_summary = {"type": "chatgpt", "plan_type": "pro"}
        self.duplicate_turn_ids = duplicate_turn_ids
        self.instruction_source_drift = instruction_source_drift
        self.forged_thread_lineage = forged_thread_lineage
        self.divergent_result_output = divergent_result_output
        self.timeout_after_dispatch = timeout_after_dispatch
        self.thread_count = 0
        self.turn_count = 0
        self.model_calls = 0
        self.thread_totals: dict[str, dict[str, int]] = {}

    async def __aenter__(self) -> "_FixtureClient":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def start_thread(
        self,
        *,
        model: str,
        base_instructions: str,
        cwd: Path,
        ephemeral: bool,
    ) -> codex_app_server.AppServerThread:
        assert model == adapter.MODEL
        assert cwd == adapter.PROJECT_ROOT
        assert ephemeral is True
        self.thread_count += 1
        thread_id = f"fixture-thread-{self.thread_count}"
        return codex_app_server.AppServerThread(
            thread_id=thread_id,
            model=model,
            cwd=str(cwd),
            ephemeral=True,
            instruction_sources_sha256=(
                "f" * 64
                if self.instruction_source_drift
                else adapter._epoch4_instruction_source_contract()[  # noqa: SLF001
                    "effective_instruction_sources_sha256"
                ]
            ),
            instruction_sources_count=(
                99
                if self.instruction_source_drift
                else adapter._epoch4_instruction_source_contract()[  # noqa: SLF001
                    "effective_instruction_sources_count"
                ]
            ),
            base_instructions_sha256=sha256_text(base_instructions),
            base_instructions_bytes=len(base_instructions.encode("utf-8")),
        )

    async def run_ephemeral_structured_turn(self, **kwargs: object) -> SimpleNamespace:
        thread = await self.start_thread(
            model=str(kwargs["model"]),
            base_instructions=str(kwargs["base_instructions"]),
            cwd=Path(str(kwargs["cwd"])),
            ephemeral=True,
        )
        return await self._turn(thread=thread, **kwargs)

    async def run_structured_turn(self, **kwargs: object) -> SimpleNamespace:
        thread = kwargs.pop("thread")
        return await self._turn(thread=thread, **kwargs)

    async def _turn(
        self, *, thread: codex_app_server.AppServerThread, **kwargs: object
    ) -> SimpleNamespace:
        self.model_calls += 1
        self.turn_count += 1
        if self.timeout_after_dispatch and self.model_calls == 1:
            raise asyncio.TimeoutError("synthetic post-dispatch timeout")
        turn_id = (
            "fixture-turn-duplicate"
            if self.duplicate_turn_ids
            else f"fixture-turn-{self.turn_count}"
        )
        prompt = str(kwargs["prompt"])
        output = _no_signal_output(prompt)
        result_output = copy.deepcopy(output)
        if self.divergent_result_output:
            first = result_output["segments"][0]
            event_unit = first["unit_receipts"][1]["unit_id"]
            first["status"] = "coded"
            first["no_signal_reason"] = ""
            first["events"] = [_event(event_unit)]
            first["unit_receipts"][1]["eligible_event_count"] = 1
        output_text = json.dumps(
            output, ensure_ascii=True, sort_keys=True, separators=(",", ":")
        )
        output_path = Path(str(kwargs["output_path"]))
        sidecar_path = Path(str(kwargs["sidecar_path"]))
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(output_text + "\n", encoding="utf-8")
        cached = 100 if kwargs["thread_mode"] == "same_thread" and self.turn_count > 1 else 0
        usage = {
            "input_tokens": 1_000,
            "cached_input_tokens": cached,
            "output_tokens": 200,
            "reasoning_output_tokens": 50,
            "total_tokens": 1_200,
        }
        total = self.thread_totals.setdefault(
            thread.thread_id,
            {field: 0 for field in adapter.USAGE_FIELDS},
        )
        for field in adapter.USAGE_FIELDS:
            total[field] += usage[field]
        reported_thread_id = (
            f"forged-{thread.thread_id}"
            if self.forged_thread_lineage
            else thread.thread_id
        )
        schema_json = adapter._canonical_json(kwargs["output_schema"])  # noqa: SLF001
        sidecar = {
            "schema_version": codex_app_server.TURN_SIDECAR_SCHEMA_VERSION,
            "state": "completed",
            "started_at": "2026-07-18T00:00:00+00:00",
            "finished_at": "2026-07-18T00:00:01+00:00",
            "client_version": codex_app_server.APP_SERVER_CLIENT_VERSION,
            "cli_version": codex_app_server.PINNED_CODEX_CLI_VERSION,
            "app_server_user_agent": "pif-fixture-client",
            "protocol_schema_sha256": adapter._sha256_file(  # noqa: SLF001
                codex_app_server.PROTOCOL_SCHEMA_PATH
            ),
            "transport": "stdio",
            "max_message_bytes": 32 * 1024 * 1024,
            "synthetic_debug_errors": False,
            "status": "completed",
            "usage_status": "measured",
            "usage_complete": True,
            "usage": usage,
            "thread_total_usage": copy.deepcopy(total),
            "auth_type": "chatgpt",
            "plan_type": "pro",
            "thread_id": reported_thread_id,
            "turn_id": turn_id,
            "model": thread.model,
            "effort": kwargs["effort"],
            "thread_mode": kwargs["thread_mode"],
            "batch_size": kwargs["batch_size"],
            "error_class": None,
            "wall_elapsed_seconds": 1.25,
            "prompt_sha256": sha256_text(prompt),
            "prompt_bytes": len(prompt.encode("utf-8")),
            "base_instructions_sha256": thread.base_instructions_sha256,
            "base_instructions_bytes": thread.base_instructions_bytes,
            "instruction_sources_sha256": thread.instruction_sources_sha256,
            "instruction_sources_count": thread.instruction_sources_count,
            "output_schema_sha256": sha256_text(schema_json),
            "output_schema_bytes": len(schema_json.encode("utf-8")),
            "output_sha256": sha256_text(output_text),
            "output_path": str(output_path.resolve()),
            "stderr_sha256": "0" * 64,
            "stderr_bytes": 0,
            "recovery_reran_model": False,
        }
        sidecar_path.write_text(
            json.dumps(sidecar, ensure_ascii=True, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return SimpleNamespace(
            status_ok=True,
            status="completed",
            output=result_output,
            error_class=None,
            thread_id=reported_thread_id,
            turn_id=turn_id,
        )


class _FixtureFactory:
    def __init__(
        self,
        *,
        duplicate_turn_ids: bool = False,
        instruction_source_drift: bool = False,
        forged_thread_lineage: bool = False,
        divergent_result_output: bool = False,
        timeout_after_dispatch: bool = False,
    ) -> None:
        self.client = _FixtureClient(
            duplicate_turn_ids=duplicate_turn_ids,
            instruction_source_drift=instruction_source_drift,
            forged_thread_lineage=forged_thread_lineage,
            divergent_result_output=divergent_result_output,
            timeout_after_dispatch=timeout_after_dispatch,
        )

    def __call__(self) -> _FixtureClient:
        return self.client


def test_precommit_surface_and_frozen_configuration_bind_exact_epoch4(
    tmp_path: Path,
) -> None:
    surface = adapter.precommit_selection_surface()
    assert surface["arm_count"] == 6
    assert {
        (row["batch_size"], row["thread_mode"]) for row in surface["arms"]
    } == {
        (batch_size, thread_mode)
        for batch_size in (3, 5, 8)
        for thread_mode in ("new_thread", "same_thread")
    }
    frozen = adapter.freeze_precommit_selection(
        tmp_path / "selection.json", batch_size=5, thread_mode="same_thread"
    )
    assert frozen["winner_system_id"] == adapter.WINNER_SYSTEM_ID
    assert frozen["model"] == adapter.MODEL
    assert frozen["effort"] == adapter.EFFORT
    assert frozen["max_events_per_segment"] == 48
    assert frozen["semantic_postprocessing"] is False
    assert frozen["project_instruction_content_byte_budget"] == 0
    assert frozen["project_instruction_content_included"] is False
    assert frozen["epoch4_runtime_lock"]["sha256"] == (
        adapter.EPOCH4_RUNTIME_LOCK_SHA256
    )
    assert frozen["operational_non_parity_boundary"]["live_capacity_gating"] == (
        "coordinator_supplied_exact_hash_bound_not_measured_by_adapter"
    )
    assert frozen["capacity_admission_contract"][
        "required_before_app_server_start"
    ] is True
    assert frozen[
        "prompt_instructions_sha256"
    ] == adapter._prompt_instruction_hash()  # noqa: SLF001
    assert frozen["canonical_dynamic_schema_sha256"] == (
        adapter.canonical_dynamic_schema_bundle()["canonical_dynamic_schema_sha256"]
    )
    (tmp_path / "selection.json").write_text("{}\n", encoding="utf-8")
    with pytest.raises(adapter.ExpandedCapEpisodeBatchError, match="drifted"):
        adapter.verify_frozen_configuration(tmp_path / "selection.json")


def test_epoch4_snapshot_excludes_zero_budget_ambient_instruction_content(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    global_agents = (Path.home() / ".codex" / "AGENTS.md").resolve()
    original_record = adapter._record  # noqa: SLF001

    def guarded_record(path: Path) -> dict:
        if path.expanduser().resolve() == global_agents:
            raise AssertionError("zero-budget AGENTS content was re-hashed")
        return original_record(path)

    adapter._epoch4_snapshot.cache_clear()  # noqa: SLF001
    adapter._epoch4_template_json.cache_clear()  # noqa: SLF001
    monkeypatch.setattr(adapter, "_record", guarded_record)
    try:
        snapshot = adapter._epoch4_snapshot()  # noqa: SLF001
        contract = adapter._epoch4_instruction_source_contract()  # noqa: SLF001
        assert snapshot["overlay"]["project_doc_max_bytes"] == 0
        assert contract["project_instruction_content_byte_budget"] == 0
        assert contract["project_instruction_content_included"] is False
        assert str(global_agents) in contract["effective_instruction_source_paths"]
    finally:
        adapter._epoch4_snapshot.cache_clear()  # noqa: SLF001
        adapter._epoch4_template_json.cache_clear()  # noqa: SLF001


def test_epoch4_snapshot_rejects_runtime_lock_tamper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_lock = (adapter.epoch4.DEFAULT_OUTPUT_ROOT / "runtime-lock.json").resolve()
    original_sha256_file = adapter._sha256_file  # noqa: SLF001

    def tampered_sha256(path: Path) -> str:
        if path.expanduser().resolve() == runtime_lock:
            return "0" * 64
        return original_sha256_file(path)

    adapter._epoch4_snapshot.cache_clear()  # noqa: SLF001
    monkeypatch.setattr(adapter, "_sha256_file", tampered_sha256)
    try:
        with pytest.raises(
            adapter.ExpandedCapEpisodeBatchError, match="runtime-lock snapshot"
        ):
            adapter._epoch4_snapshot()  # noqa: SLF001
    finally:
        adapter._epoch4_snapshot.cache_clear()  # noqa: SLF001


def test_template_render_is_byte_exact_for_original_epoch4_context() -> None:
    base = adapter._epoch4_template()["base"]  # noqa: SLF001
    context_json = base.split(adapter._CONTEXT_MARKER, 1)[1].split(  # noqa: SLF001
        adapter._APPLICABILITY_MARKER, 1  # noqa: SLF001
    )[0]
    context = json.loads(context_json)
    assert adapter._render_base_instructions(context) == base  # noqa: SLF001


@pytest.mark.parametrize(
    ("batch_size", "expected_sizes"),
    [(3, [3, 3, 1]), (5, [5, 2]), (8, [7])],
)
def test_prepare_specializes_only_dynamic_ids_and_cardinalities(
    batch_size: int, expected_sizes: list[int]
) -> None:
    requests = adapter.prepare_episode_batches(
        _episode(), batch_size=batch_size, thread_mode="new_thread"
    )
    assert [row["effective_batch_size"] for row in requests] == expected_sizes
    seen_segments = []
    canonical = adapter.canonical_dynamic_schema_bundle()
    for request in requests:
        adapter.validate_prepared_request(request)
        seen_segments.extend(request["segment_ids"])
        schema = request["schema"]
        nodes = adapter._schema_dynamic_nodes(schema)  # noqa: SLF001
        assert nodes["episode"]["enum"] == [request["episode_id"]]
        assert nodes["segments"]["minItems"] == request["effective_batch_size"]
        assert nodes["segments"]["maxItems"] == request["effective_batch_size"]
        assert nodes["segment_id"]["enum"] == request["segment_ids"]
        assert nodes["events"]["maxItems"] == 48
        assert nodes["eligible_count"]["maximum"] == 48
        assert nodes["unresolved_count"]["maximum"] == 32
        assert adapter._canonicalize_dynamic_schema(schema) == canonical[  # noqa: SLF001
            "explicit_applicability_schema"
        ]
        assert adapter.epoch4.EXHAUSTIVE_INSTRUCTIONS in request["base_instructions"]
        assert adapter.epoch4.NEW_CAP_INSTRUCTION in request["base_instructions"]
        assert adapter.epoch4.OLD_CAP_INSTRUCTION not in request["base_instructions"]
        assert request["semantic_postprocessing"] is False
    assert seen_segments == [f"seg_fixture_{index}" for index in range(7)]


def test_episode_context_renders_every_frozen_semantic_authority_field() -> None:
    episode = _episode(segment_count=3)
    request = adapter.prepare_episode_batches(
        episode, batch_size=3, thread_mode="new_thread"
    )[0]
    expected = {
        "episode_id": episode["episode_id"],
        "source_name": episode["source_name"],
        "episode_title": episode["episode_title"],
        "context_summary": episode["context_summary"],
        "speaker_map": episode["speaker_map"],
        "section_map": episode["section_map"],
        "entity_seed": episode["entity_seed"],
        "concept_seed": episode["concept_seed"],
        "extraction_guidance": episode["extraction_guidance"],
        "excluded_source_context": episode["excluded_source_context"],
    }
    assert request["episode_context"] == expected
    rendered = request["base_instructions"].split(adapter._CONTEXT_MARKER, 1)[1].split(  # noqa: SLF001
        adapter._APPLICABILITY_MARKER, 1  # noqa: SLF001
    )[0]
    assert json.loads(rendered) == expected

    tampered = copy.deepcopy(request)
    tampered["episode_context"]["excluded_source_context"].append("unbound")
    with pytest.raises(adapter.ExpandedCapEpisodeBatchError, match="drifted"):
        adapter.validate_prepared_request(tampered)


@pytest.mark.parametrize(
    ("field", "invalid"),
    [
        ("section_map", ["not-an-object"]),
        ("entity_seed", []),
        ("concept_seed", [{"not": "a string"}]),
        ("excluded_source_context", [1]),
    ],
)
def test_episode_context_rejects_missing_or_invalid_semantic_authority(
    field: str, invalid: object
) -> None:
    missing = _episode(segment_count=3)
    del missing[field]
    with pytest.raises(adapter.ExpandedCapEpisodeBatchError, match=field):
        adapter.prepare_episode_batches(
            missing, batch_size=3, thread_mode="new_thread"
        )

    malformed = _episode(segment_count=3)
    malformed[field] = invalid
    with pytest.raises(adapter.ExpandedCapEpisodeBatchError, match=field):
        adapter.prepare_episode_batches(
            malformed, batch_size=3, thread_mode="new_thread"
        )


def test_projection_preserves_exact_duplicates_and_projects_exact_offsets() -> None:
    request = adapter.prepare_episode_batches(
        _episode(segment_count=3), batch_size=3, thread_mode="new_thread"
    )[0]
    output = _no_signal_output(request["prompt"])
    first = output["segments"][0]
    event_unit = first["unit_receipts"][1]["unit_id"]
    first["status"] = "coded"
    first["no_signal_reason"] = ""
    first["events"] = [_event(event_unit), _event(event_unit)]
    first["unit_receipts"][1]["eligible_event_count"] = 2
    projected = adapter.validate_and_project_output(request, output)
    events = projected["normalized"]["segments"][0]["events"]
    assert len(events) == 2
    assert events[0] == events[1]
    assert events[0]["evidence"] == "System 0 improves latency."
    assert projected["applicability"]["emitted_event_count"] == 2
    assert projected["applicability"]["normalized_event_count"] == 2
    assert projected["applicability"]["exact_identity_duplicate_count"] == 1
    assert projected["applicability"]["deduplication_performed"] is False
    assert projected["applicability"]["semantic_pruning_performed"] is False
    assert len(projected["provenance"]["events"]) == 2


def test_prepared_request_rejects_tampered_source_unit_offsets() -> None:
    request = adapter.prepare_episode_batches(
        _episode(segment_count=3), batch_size=3, thread_mode="new_thread"
    )[0]
    request["private_input"]["segments"][0]["units"][0]["end_char"] -= 1
    with pytest.raises(
        adapter.ExpandedCapEpisodeBatchError, match="source-unit projection"
    ):
        adapter.validate_prepared_request(request)


@pytest.mark.parametrize(
    ("thread_mode", "expected_threads"),
    [("new_thread", 3), ("same_thread", 1)],
)
def test_fixture_run_has_complete_usage_cache_reasoning_wall_and_lifecycle(
    tmp_path: Path, thread_mode: str, expected_threads: int
) -> None:
    factory = _FixtureFactory()
    episodes = [_episode()]
    report = asyncio.run(
        adapter.run_episode_batch_arm(
            episodes,
            output_dir=tmp_path / thread_mode,
            batch_size=3,
            thread_mode=thread_mode,
            client_factory=factory,
            **_fixture_capacity_args(
                episodes, batch_size=3, thread_mode=thread_mode
            ),
        )
    )
    assert report["state"] == "passed"
    assert report["development_regression_eligible"] is True
    assert report["requested_calls"] == 3
    assert report["attempted_calls"] == 3
    assert report["validated_calls"] == 3
    assert report["terminal_sidecars"] == 3
    assert report["accounting_complete"] is True
    assert report["attempt_contract_failures"] == 0
    assert report["ambiguous_outcome_attempts"] == 0
    assert report["capacity_binding_valid"] is True
    assert report["fixture_mode"] is True
    assert report["usage_status"] == "complete"
    assert report["usage"] == {
        "input_tokens": 3_000,
        "cached_input_tokens": 200 if thread_mode == "same_thread" else 0,
        "output_tokens": 600,
        "reasoning_output_tokens": 150,
        "total_tokens": 3_600,
    }
    assert report["reasoning_output_tokens"] == 150
    assert report["cached_input_tokens"] == (
        200 if thread_mode == "same_thread" else 0
    )
    assert report["turn_wall_elapsed_seconds_sum"] == 3.75
    assert report["observed_thread_count"] == expected_threads
    assert report["observed_turn_count"] == 3
    assert report["preflight_lineage_valid"] is True
    assert report["no_model_preflight"] is not None
    assert report["turn_ids_unique"] is True
    assert report["thread_lineage_valid"] is True
    assert report["ambiguous_retry_count"] == 0
    assert report["semantic_postprocessing"] is False
    assert factory.client.model_calls == 3
    assert all(row["sidecar_contract_valid"] for row in report["turn_telemetry"])


def test_duplicate_turn_id_fails_precommit_without_retry(tmp_path: Path) -> None:
    factory = _FixtureFactory(duplicate_turn_ids=True)
    episodes = [_episode()]
    report = asyncio.run(
        adapter.run_episode_batch_arm(
            episodes,
            output_dir=tmp_path / "duplicate-turn",
            batch_size=3,
            thread_mode="new_thread",
            client_factory=factory,
            **_fixture_capacity_args(
                episodes, batch_size=3, thread_mode="new_thread"
            ),
        )
    )
    assert report["state"] == "not_eligible"
    assert report["turn_ids_unique"] is False
    assert report["development_regression_eligible"] is False
    assert report["retry_count"] == 0
    assert report["ambiguous_retry_count"] == 0
    assert factory.client.model_calls == report["requested_calls"]


def test_same_thread_uses_one_distinct_thread_per_episode(tmp_path: Path) -> None:
    factory = _FixtureFactory()
    episodes = [
        _episode(segment_count=4, suffix="one"),
        _episode(segment_count=4, suffix="two"),
    ]
    report = asyncio.run(
        adapter.run_episode_batch_arm(
            episodes,
            output_dir=tmp_path / "two-episodes",
            batch_size=3,
            thread_mode="same_thread",
            client_factory=factory,
            **_fixture_capacity_args(
                episodes, batch_size=3, thread_mode="same_thread"
            ),
        )
    )
    assert report["state"] == "passed"
    assert report["requested_calls"] == 4
    assert report["observed_thread_count"] == 2
    assert report["observed_turn_count"] == 4
    assert report["thread_lineage_valid"] is True
    assert report["turn_ids_unique"] is True


def test_instruction_source_drift_stops_before_semantic_turn(tmp_path: Path) -> None:
    factory = _FixtureFactory(instruction_source_drift=True)
    episodes = [_episode()]
    with pytest.raises(
        adapter.ExpandedCapEpisodeBatchTelemetryError, match="preflight"
    ):
        asyncio.run(
            adapter.run_episode_batch_arm(
                episodes,
                output_dir=tmp_path / "instruction-drift",
                batch_size=3,
                thread_mode="new_thread",
                client_factory=factory,
                **_fixture_capacity_args(
                    episodes, batch_size=3, thread_mode="new_thread"
                ),
            )
        )
    assert factory.client.model_calls == 0


def test_forged_result_and_sidecar_thread_cannot_replace_started_thread(
    tmp_path: Path,
) -> None:
    factory = _FixtureFactory(forged_thread_lineage=True)
    episodes = [_episode(segment_count=3)]
    report = asyncio.run(
        adapter.run_episode_batch_arm(
            episodes,
            output_dir=tmp_path / "forged-thread",
            batch_size=3,
            thread_mode="new_thread",
            client_factory=factory,
            **_fixture_capacity_args(
                episodes, batch_size=3, thread_mode="new_thread"
            ),
        )
    )
    assert report["state"] == "not_eligible"
    assert report["thread_lineage_valid"] is False
    assert report["sidecar_contract_failures"] == 1
    assert report["turn_telemetry"][0]["thread_matches_attempt"] is False
    assert report["development_regression_eligible"] is False


def test_projection_rejects_in_memory_output_that_differs_from_bound_raw_bytes(
    tmp_path: Path,
) -> None:
    factory = _FixtureFactory(divergent_result_output=True)
    episodes = [_episode(segment_count=3)]
    output_root = tmp_path / "divergent-output"
    report = asyncio.run(
        adapter.run_episode_batch_arm(
            episodes,
            output_dir=output_root,
            batch_size=3,
            thread_mode="new_thread",
            client_factory=factory,
            **_fixture_capacity_args(
                episodes, batch_size=3, thread_mode="new_thread"
            ),
        )
    )
    assert report["state"] == "not_eligible"
    assert report["validated_calls"] == 0
    assert report["emitted_event_count"] == 0
    assert not any(output_root.glob("batches/*/normalized-output.private.json"))
    assert report["development_regression_eligible"] is False


def test_post_dispatch_timeout_is_attempted_unknown_and_ambiguous(
    tmp_path: Path,
) -> None:
    factory = _FixtureFactory(timeout_after_dispatch=True)
    episodes = [_episode(segment_count=3)]
    report = asyncio.run(
        adapter.run_episode_batch_arm(
            episodes,
            output_dir=tmp_path / "post-dispatch-timeout",
            batch_size=3,
            thread_mode="new_thread",
            client_factory=factory,
            **_fixture_capacity_args(
                episodes, batch_size=3, thread_mode="new_thread"
            ),
        )
    )
    assert factory.client.model_calls == 1
    assert report["requested_calls"] == 1
    assert report["attempted_calls"] == 1
    assert report["terminal_sidecars"] == 0
    assert report["usage_measured_attempts"] == 0
    assert report["usage_unknown_attempts"] == 1
    assert report["ambiguous_outcome_attempts"] == 1
    assert report["accounting_complete"] is False
    assert report["development_regression_eligible"] is False
    assert report["turn_telemetry"][0]["attempt_contract_valid"] is True


def test_capacity_admission_exact_hash_fails_before_app_server_start(
    tmp_path: Path,
) -> None:
    factory = _FixtureFactory()
    episodes = [_episode(segment_count=3)]
    capacity_args = _fixture_capacity_args(
        episodes, batch_size=3, thread_mode="new_thread"
    )
    capacity_args["capacity_admission"]["batch_count"] = 99
    with pytest.raises(
        adapter.ExpandedCapEpisodeBatchTelemetryError,
        match="capacity admission",
    ):
        asyncio.run(
            adapter.run_episode_batch_arm(
                episodes,
                output_dir=tmp_path / "capacity-drift",
                batch_size=3,
                thread_mode="new_thread",
                client_factory=factory,
                **capacity_args,
            )
        )
    assert factory.client.thread_count == 0
    assert factory.client.model_calls == 0


@pytest.mark.parametrize(
    ("batch_size", "thread_mode"),
    [(2, "new_thread"), (3, "resumed_thread")],
)
def test_nonfrozen_arm_choices_fail_before_any_model_surface(
    batch_size: int, thread_mode: str
) -> None:
    with pytest.raises(adapter.ExpandedCapEpisodeBatchError):
        adapter.prepare_episode_batches(
            _episode(), batch_size=batch_size, thread_mode=thread_mode
        )
