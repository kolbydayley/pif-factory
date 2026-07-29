from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import app_server_candidate_expanded_cap_exhaustive_full_schema as canary
from research_factory import app_server_judge_v5_selection_v249_explicit_applicability as v249
from research_factory import codex_app_server
from research_factory.util import sha256_text


def _frozen_output() -> dict:
    path = next(v249.DEFAULT_OUTPUT_ROOT.glob("turns/*/output.private.json"))
    return json.loads(path.read_text(encoding="utf-8"))


def _usage(total: int) -> dict[str, int]:
    output = min(5_000, total)
    return {
        "input_tokens": total - output,
        "cached_input_tokens": min(5_000, total - output),
        "output_tokens": output,
        "reasoning_output_tokens": min(1_000, output),
        "total_tokens": total,
    }


class _FakeClient:
    def __init__(
        self,
        overlay: dict,
        *,
        output: dict | None = None,
        total_tokens: int = 30_000,
        instruction_sources: tuple[str, ...] = (),
        start_error: BaseException | None = None,
    ) -> None:
        self.overlay = copy.deepcopy(overlay)
        self.output = copy.deepcopy(output) if output is not None else _frozen_output()
        self.total_tokens = total_tokens
        self.sources = instruction_sources
        self.start_error = start_error
        self.account_summary = {"type": "chatgpt", "plan_type": "pro"}
        self.started_threads = 0
        self.semantic_turns = 0
        self.turn_lineage: dict | None = None
        self.instruction_verification: dict | None = None
        self.instruction_verification_path: Path | None = None
        sources_json = canary._canonical_json(list(instruction_sources))  # noqa: SLF001
        self.thread = SimpleNamespace(
            thread_id="epoch4-thread",
            model=canary.MODEL,
            ephemeral=True,
            instruction_sources_sha256=sha256_text(sources_json),
            instruction_sources_count=len(instruction_sources),
        )

    async def __aenter__(self) -> "_FakeClient":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def _request(self, method: str, params: dict) -> dict:
        assert method == "account/rateLimits/read"
        assert params == {}
        return {
            "rateLimitsByLimitId": {
                "codex": {
                    "limitId": "codex",
                    "primary": {"usedPercent": 10, "resetsAt": 1_800_000_000},
                    "rateLimitReachedType": None,
                }
            }
        }

    async def start_thread(self, **kwargs: object) -> SimpleNamespace:
        if self.start_error is not None:
            raise self.start_error
        self.started_threads += 1
        assert kwargs["model"] == canary.MODEL
        assert kwargs["ephemeral"] is True
        assert canary.NEW_CAP_INSTRUCTION in str(kwargs["base_instructions"])
        assert canary.OLD_CAP_INSTRUCTION not in str(kwargs["base_instructions"])
        return self.thread

    def instruction_sources_for(self, thread_id: str) -> tuple[str, ...]:
        assert thread_id == self.thread.thread_id
        return self.sources

    def bind_turn_lineage(self, **kwargs: object) -> None:
        assert self.turn_lineage is None
        self.turn_lineage = {
            "lineage_schema_version": canary.SIDECAR_LINEAGE_VERSION,
            **copy.deepcopy(kwargs),
        }

    def bind_instruction_verification(
        self, *, verification: dict, path: Path
    ) -> None:
        assert self.instruction_verification is None
        assert path.name == "instruction-source-verification.json"
        self.instruction_verification = copy.deepcopy(verification)
        self.instruction_verification_path = path.resolve()

    async def run_structured_turn(self, **kwargs: object) -> SimpleNamespace:
        self.semantic_turns += 1
        assert kwargs["thread"] is self.thread
        assert kwargs["thread_mode"] == "new_thread"
        assert kwargs["batch_size"] == 2
        output_text = json.dumps(
            self.output, ensure_ascii=True, sort_keys=True, separators=(",", ":")
        )
        output_path = Path(str(kwargs["output_path"]))
        sidecar_path = Path(str(kwargs["sidecar_path"]))
        output_path.write_text(output_text + "\n", encoding="utf-8")
        schema_json = canary._canonical_json(kwargs["output_schema"])  # noqa: SLF001
        usage = _usage(self.total_tokens)
        sidecar = {
            "schema_version": codex_app_server.TURN_SIDECAR_SCHEMA_VERSION,
            "state": "completed",
            "started_at": "2026-07-18T00:00:00+00:00",
            "finished_at": "2026-07-18T00:00:01+00:00",
            "client_version": codex_app_server.APP_SERVER_CLIENT_VERSION,
            "cli_version": codex_app_server.PINNED_CODEX_CLI_VERSION,
            "app_server_user_agent": "pif-epoch4-test",
            "protocol_schema_sha256": canary._sha256_file(  # noqa: SLF001
                codex_app_server.PROTOCOL_SCHEMA_PATH
            ),
            "transport": "stdio",
            "max_message_bytes": 32 * 1024 * 1024,
            "synthetic_debug_errors": False,
            "status": "completed",
            "usage_status": "measured",
            "usage_complete": True,
            "usage": usage,
            "thread_total_usage": copy.deepcopy(usage),
            "auth_type": "chatgpt",
            "plan_type": "pro",
            "thread_id": self.thread.thread_id,
            "turn_id": "epoch4-turn",
            "model": canary.MODEL,
            "effort": canary.EFFORT,
            "thread_mode": "new_thread",
            "batch_size": 2,
            "error_class": None,
            "wall_elapsed_seconds": 1.25,
            "prompt_sha256": sha256_text(str(kwargs["prompt"])),
            "prompt_bytes": len(str(kwargs["prompt"]).encode("utf-8")),
            "base_instructions_sha256": sha256_text(
                str(kwargs["thread"].base_instructions)
            )
            if hasattr(kwargs["thread"], "base_instructions")
            else None,
            "base_instructions_bytes": 0,
            "output_schema_sha256": sha256_text(schema_json),
            "output_schema_bytes": len(schema_json.encode("utf-8")),
            "instruction_sources_sha256": self.thread.instruction_sources_sha256,
            "instruction_sources_count": self.thread.instruction_sources_count,
            "output_sha256": sha256_text(output_text),
            "output_path": str(output_path.resolve()),
            "stderr_sha256": "0" * 64,
            "stderr_bytes": 0,
            "recovery_reran_model": False,
        }
        # The real transport stores this hash from thread/start.
        sidecar["base_instructions_sha256"] = sha256_text(
            canary.prepare_turn(canary.load_contract())["base"]
        )
        sidecar["base_instructions_bytes"] = len(
            canary.prepare_turn(canary.load_contract())["base"].encode("utf-8")
        )
        assert self.turn_lineage is not None
        assert self.instruction_verification is not None
        assert self.instruction_verification_path is not None
        completed_verification = copy.deepcopy(self.instruction_verification)
        completed_verification["semantic_turn_started"] = True
        completed_verification["semantic_turn_id"] = "epoch4-turn"
        canary._write_stable_time(  # noqa: SLF001
            self.instruction_verification_path,
            completed_verification,
            "verified_at",
        )
        sidecar.update(copy.deepcopy(self.turn_lineage))
        sidecar["semantic_thread_id"] = self.thread.thread_id
        sidecar["semantic_turn_id"] = "epoch4-turn"
        sidecar["instruction_verification"] = canary._record(  # noqa: SLF001
            self.instruction_verification_path
        )
        sidecar_path.write_text(json.dumps(sidecar) + "\n", encoding="utf-8")
        return SimpleNamespace(
            status_ok=True,
            output=copy.deepcopy(self.output),
            thread_id=self.thread.thread_id,
            turn_id="epoch4-turn",
        )


class _FakeFactory:
    def __init__(self, **client_kwargs: object) -> None:
        self.client_kwargs = client_kwargs
        self.clients: list[_FakeClient] = []

    def __call__(self, overlay: dict) -> _FakeClient:
        client = _FakeClient(overlay, **self.client_kwargs)
        self.clients.append(client)
        return client


class _TerminalFakeClient(_FakeClient):
    def __init__(self, overlay: dict, *, terminal_kind: str) -> None:
        super().__init__(overlay)
        self.terminal_kind = terminal_kind

    async def run_structured_turn(self, **kwargs: object) -> SimpleNamespace:
        await super().run_structured_turn(**kwargs)
        sidecar_path = Path(str(kwargs["sidecar_path"]))
        output_path = Path(str(kwargs["output_path"]))
        sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
        output_path.unlink()
        if self.terminal_kind == "interrupted":
            sidecar.update(
                {
                    "state": "interrupted",
                    "status": "timeout",
                    "error_class": "turn_timeout",
                    "usage": None,
                    "thread_total_usage": None,
                    "usage_complete": False,
                    "usage_status": "unknown",
                }
            )
            sidecar_path.write_text(json.dumps(sidecar) + "\n", encoding="utf-8")
            raise codex_app_server.AppServerTurnTimeout("synthetic timeout")
        assert self.terminal_kind == "structured_output_invalid"
        sidecar.update(
            {
                "state": "failed",
                "status": "completed",
                "error_class": "structured_output_invalid",
            }
        )
        sidecar_path.write_text(json.dumps(sidecar) + "\n", encoding="utf-8")
        raise codex_app_server.AppServerStructuredOutputError(
            "synthetic structured output failure"
        )


class _TerminalFakeFactory:
    def __init__(self, terminal_kind: str) -> None:
        self.terminal_kind = terminal_kind
        self.clients: list[_TerminalFakeClient] = []

    def __call__(self, overlay: dict) -> _TerminalFakeClient:
        client = _TerminalFakeClient(overlay, terminal_kind=self.terminal_kind)
        self.clients.append(client)
        return client


def _preflight_and_freeze(root: Path) -> dict:
    factory = _FakeFactory()
    preflight = asyncio.run(
        canary.run_no_model_preflight(root, client_factory=factory)
    )
    assert preflight["semantic_model_call_count"] == 0
    frozen = canary.freeze_run(root)
    return frozen


def _write_valid_launch_and_capacity(root: Path) -> dict:
    frozen = _preflight_and_freeze(root)
    launch_path = root / "launch-receipt.json"
    canary._write_immutable_json(  # noqa: SLF001
        launch_path, canary._launch_receipt_payload(frozen)  # noqa: SLF001
    )
    paths = canary._turn_paths(root)  # noqa: SLF001
    asyncio.run(
        canary._probe_capacity(  # noqa: SLF001
            _FakeClient(frozen["turn"]["config_overlay"]),
            checkpoint_path=paths["capacity"],
            runtime_lock_record=canary._record(frozen["runtime_lock"]),  # noqa: SLF001
            launch_receipt_record=canary._record(launch_path),  # noqa: SLF001
        )
    )
    return frozen


def _write_valid_completed_artifacts(root: Path, frozen: dict) -> dict[str, Path]:
    paths = canary._turn_paths(root)  # noqa: SLF001
    client = _FakeClient(frozen["turn"]["config_overlay"])
    client.bind_turn_lineage(
        runtime_lock=canary._record(frozen["runtime_lock"]),  # noqa: SLF001
        launch_receipt=canary._record(root / "launch-receipt.json"),  # noqa: SLF001
        capacity_checkpoint=canary._record(paths["capacity"]),  # noqa: SLF001
    )
    verification = canary._semantic_instruction_verification(  # noqa: SLF001
        client=client, thread=client.thread, frozen=frozen
    )
    client.bind_instruction_verification(
        verification=verification, path=paths["instruction_verification"]
    )
    asyncio.run(
        client.run_structured_turn(
            thread=client.thread,
            effort=canary.EFFORT,
            prompt=frozen["turn"]["prompt"],
            output_schema=frozen["turn"]["schema"],
            sidecar_path=paths["sidecar"],
            output_path=paths["output"],
            batch_size=2,
            thread_mode="new_thread",
        )
    )
    return paths


def _rewrite_receipt_records(root: Path, receipt: dict) -> None:
    records = canary._artifact_records(root)  # noqa: SLF001
    receipt["records"] = records
    receipt["records_sha256"] = sha256_text(canary._canonical_json(records))  # noqa: SLF001
    payload = json.dumps(receipt, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    (root / "plan-step-receipt.json").write_text(payload, encoding="utf-8")
    (root / "terminal.json").write_text(payload, encoding="utf-8")


def test_contract_plan_overlay_and_post_completion_token_ceiling_are_exact() -> None:
    contract = canary.load_contract()
    execution = contract["directive"]["execution_contract"]
    overlay = contract["overlay"]
    assert canary._sha256_file(canary.PLAN_PATH) == canary.EXPECTED_PLAN_SHA256  # noqa: SLF001
    assert canary._sha256_file(canary.DIRECTIVE_PATH) == canary.EXPECTED_DIRECTIVE_SHA256  # noqa: SLF001
    assert execution["transport_native_maximum_token_field_available"] is False
    assert execution["token_ceiling_is_post_completion_acceptance_not_transport_limit"] is True
    assert execution["measured_total_token_acceptance_ceiling"] == 72_891
    assert execution["app_server_arguments"] == ["app-server", "--stdio", "--strict-config"]
    assert overlay["project_doc_max_bytes"] == 0
    assert overlay["skills"]["include_instructions"] is False
    assert overlay["orchestrator"] == {
        "skills": {"enabled": False},
        "mcp": {"enabled": False},
    }
    assert overlay["tools"]["experimental_request_user_input"]["enabled"] is False
    assert set(overlay["mcp_servers"]) == set(canary._configured_mcp_server_names())  # noqa: SLF001
    assert canary._summary_none_supported() is True  # noqa: SLF001


def test_actual_epoch4_request_replaces_stale_32_cap_and_raises_only_event_limits(
    tmp_path: Path,
) -> None:
    historical_base = canary._source_paths()["base"].read_text(encoding="utf-8")  # noqa: SLF001
    assert historical_base.count(canary.OLD_CAP_INSTRUCTION) == 1
    prepared = canary.prepare_run(tmp_path / "prepared")
    base = prepared["turn"]["paths"]["base"].read_text(encoding="utf-8")
    assert canary.OLD_CAP_INSTRUCTION not in base
    assert base.count(canary.NEW_CAP_INSTRUCTION) == 1
    assert "Enumerate every independent grounded eligible proposition" in base
    assert canary._source_paths()["base"].read_text(encoding="utf-8") == historical_base  # noqa: SLF001
    for name in ("schema", "direct_schema"):
        schema = prepared["turn"][name]
        segment = schema["properties"]["segments"]["items"]
        assert segment["properties"]["events"]["maxItems"] == 48
        receipt = segment["properties"]["unit_receipts"]["items"]["properties"]
        assert receipt["eligible_event_count"]["maximum"] == 48
        assert receipt["unresolved_count"]["maximum"] == 32


def test_overlay_client_injects_config_and_summary_without_shared_runtime_edit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[tuple[str, dict]] = []

    async def base_request(_self: object, method: str, params: dict) -> dict:
        captured.append((method, copy.deepcopy(params)))
        if method == "thread/start":
            return {
                "thread": {"id": "thread-overlay"},
                "model": canary.MODEL,
                "instructionSources": [],
            }
        return {"turn": {"id": "turn-overlay"}}

    monkeypatch.setattr(codex_app_server.CodexAppServerClient, "_request", base_request)
    overlay = canary.load_contract()["overlay"]
    client = canary.Epoch4CodexAppServerClient(
        config_overlay=overlay, verify_cli=False
    )
    asyncio.run(client._request("thread/start", {"model": canary.MODEL}))  # noqa: SLF001
    asyncio.run(client._request("turn/start", {"threadId": "thread-overlay"}))  # noqa: SLF001
    assert captured[0][1]["config"] == overlay
    assert captured[0][1]["personality"] == "none"
    assert captured[0][1]["dynamicTools"] == []
    assert captured[0][1]["environments"] == []
    assert captured[1][1]["summary"] == "none"


def test_no_model_preflight_precedes_runtime_lock_and_binds_effective_sources(
    tmp_path: Path,
) -> None:
    root = tmp_path / "preflight"
    canary.prepare_run(root)
    with pytest.raises(canary.ExpandedCapCanaryError, match="preflight"):
        canary.freeze_run(root)
    preflight_factory = _FakeFactory()
    preflight = asyncio.run(
        canary.run_no_model_preflight(root, client_factory=preflight_factory)
    )
    assert preflight["thread_started"] is True
    assert preflight["turn_started"] is False
    assert preflight_factory.clients[0].semantic_turns == 0
    frozen = canary.freeze_run(root)
    lock = canary.verify_runtime_lock(frozen["runtime_lock"])
    assert lock["effective_instruction_source_paths"] == []
    assert lock["effective_instruction_source_records"] == []
    assert lock["config_overlay"] == canary._record(root / "config-overlay.json")  # noqa: SLF001
    assert lock["pinned_codex_cli"] == canary._record(canary.PINNED_CODEX)  # noqa: SLF001
    assert lock["transport_native_token_limit_configured"] is False


def test_structural_pass_preserves_every_event_and_authorizes_only_full_event_judge(
    tmp_path: Path,
) -> None:
    root = tmp_path / "pass"
    _preflight_and_freeze(root)
    factory = _FakeFactory(output=_frozen_output(), total_tokens=30_000)
    receipt = asyncio.run(canary.run(output_dir=root, client_factory=factory))
    assert receipt["state"] == "passed"
    assert receipt["semantic_model_call_count"] == 1
    assert receipt["semantic_retry_count"] == 0
    assert receipt["full_event_ab_ba_evaluation_authorized"] is True
    assert receipt["development_winner_frozen"] is False
    assert receipt["holdout_authorized"] is False
    gate = json.loads((root / "structural-gate.json").read_text(encoding="utf-8"))
    assert gate["passed"] is True
    assert gate["event_count_is_semantic_acceptance_proxy"] is False
    assert gate["exact_identity_duplicate_count_is_acceptance_gate"] is False
    applicability = json.loads(
        (root / "turns" / canary.TURN_NAME / "applicability-receipt.json").read_text(
            encoding="utf-8"
        )
    )
    assert applicability["all_emitted_events_preserved"] is True
    assert applicability["emitted_event_count"] == applicability["normalized_event_count"]
    assert canary.verify_receipt(root) == receipt


def test_exact_duplicate_is_preserved_and_diagnostic_only(tmp_path: Path) -> None:
    output = _frozen_output()
    dense = output["segments"][0]
    duplicate = copy.deepcopy(dense["events"][0])
    dense["events"].insert(1, duplicate)
    start_id = duplicate["evidence_start_unit_id"]
    owner = next(row for row in dense["unit_receipts"] if row["unit_id"] == start_id)
    owner["eligible_event_count"] += 1
    root = tmp_path / "duplicate"
    _preflight_and_freeze(root)
    receipt = asyncio.run(
        canary.run(
            output_dir=root,
            client_factory=_FakeFactory(output=output, total_tokens=30_000),
        )
    )
    assert receipt["state"] == "passed"
    normalized = json.loads(
        (root / "turns" / canary.TURN_NAME / "normalized-output.private.json").read_text(
            encoding="utf-8"
        )
    )
    assert len(normalized["segments"][0]["events"]) == len(dense["events"])
    applicability = json.loads(
        (root / "turns" / canary.TURN_NAME / "applicability-receipt.json").read_text(
            encoding="utf-8"
        )
    )
    assert applicability["exact_identity_duplicate_count"] >= 1
    assert applicability["deduplication_performed"] is False


def test_measured_token_overage_rejects_without_retry(tmp_path: Path) -> None:
    root = tmp_path / "overage"
    _preflight_and_freeze(root)
    factory = _FakeFactory(total_tokens=72_892)
    receipt = asyncio.run(canary.run(output_dir=root, client_factory=factory))
    assert receipt["state"] == "rejected"
    assert receipt["semantic_model_call_count"] == 1
    assert receipt["accounting_complete"] is True
    assert receipt["usage"]["total_tokens"] == 72_892
    assert receipt["semantic_retry_count"] == 0
    assert len(factory.clients) == 1
    assert factory.clients[0].semantic_turns == 1


def test_capacity_failure_starts_no_thread_or_turn_and_programming_error_escapes(
    tmp_path: Path,
) -> None:
    root = tmp_path / "capacity"
    _preflight_and_freeze(root)
    factory = _FakeFactory()

    async def capacity_wait(_client: object, **_kwargs: object) -> dict:
        raise canary.OperationalWaitingError("capacity unavailable")

    receipt = asyncio.run(
        canary.run(
            output_dir=root,
            client_factory=factory,
            capacity_probe=capacity_wait,
        )
    )
    assert receipt["state"] == "waiting"
    assert receipt["semantic_model_call_count"] == 0
    assert factory.clients[0].started_threads == 0
    assert factory.clients[0].semantic_turns == 0

    bad_root = tmp_path / "programming"
    _preflight_and_freeze(bad_root)

    async def programming_defect(_client: object, **_kwargs: object) -> dict:
        raise KeyError("not operational")

    with pytest.raises(KeyError, match="not operational"):
        asyncio.run(
            canary.run(
                output_dir=bad_root,
                client_factory=_FakeFactory(),
                capacity_probe=programming_defect,
            )
        )
    assert not (bad_root / "terminal.json").exists()


def test_runtime_and_terminal_tamper_are_rejected(tmp_path: Path) -> None:
    root = tmp_path / "tamper"
    _preflight_and_freeze(root)
    overlay = root / "config-overlay.json"
    original = overlay.read_text(encoding="utf-8")
    overlay.write_text(original + " ", encoding="utf-8")
    with pytest.raises(canary.ExpandedCapCanaryError, match="no-model preflight drifted"):
        canary.verify_runtime_lock(root / "runtime-lock.json")

    terminal_root = tmp_path / "terminal-tamper"
    _preflight_and_freeze(terminal_root)
    receipt = asyncio.run(
        canary.run(output_dir=terminal_root, client_factory=_FakeFactory())
    )
    artifact = Path(receipt["records"][0]["path"])
    artifact.write_text(artifact.read_text(encoding="utf-8") + "drift", encoding="utf-8")
    with pytest.raises(canary.ExpandedCapCanaryError):
        canary.verify_receipt(terminal_root)


def test_forged_zero_client_artifacts_cannot_mint_pass(tmp_path: Path) -> None:
    root = tmp_path / "forged-launch"
    _preflight_and_freeze(root)
    paths = canary._turn_paths(root)  # noqa: SLF001
    output_text = json.dumps(_frozen_output(), sort_keys=True, separators=(",", ":"))
    (root / "launch-receipt.json").write_text("{}\n", encoding="utf-8")
    paths["capacity"].write_text("{}\n", encoding="utf-8")
    paths["instruction_verification"].write_text(
        json.dumps({"matches_no_model_preflight": True}) + "\n", encoding="utf-8"
    )
    paths["sidecar"].write_text(
        json.dumps({"output_sha256": sha256_text(output_text)}) + "\n",
        encoding="utf-8",
    )
    paths["output"].write_text(output_text + "\n", encoding="utf-8")
    factory = _FakeFactory()
    with pytest.raises(canary.ExpandedCapCanaryError, match="launch receipt"):
        asyncio.run(canary.run(output_dir=root, client_factory=factory))
    assert factory.clients == []
    assert not (root / "plan-step-receipt.json").exists()
    assert not (root / "terminal.json").exists()


def test_forged_true_instruction_verification_is_rejected_without_client(
    tmp_path: Path,
) -> None:
    root = tmp_path / "forged-verification"
    _write_valid_launch_and_capacity(root)
    paths = canary._turn_paths(root)  # noqa: SLF001
    output_text = json.dumps(_frozen_output(), sort_keys=True, separators=(",", ":"))
    paths["instruction_verification"].write_text(
        json.dumps({"matches_no_model_preflight": True}) + "\n", encoding="utf-8"
    )
    paths["sidecar"].write_text("{}\n", encoding="utf-8")
    paths["output"].write_text(output_text + "\n", encoding="utf-8")
    factory = _FakeFactory()
    with pytest.raises(canary.ExpandedCapCanaryError, match="instruction verification"):
        asyncio.run(canary.run(output_dir=root, client_factory=factory))
    assert factory.clients == []
    assert not (root / "plan-step-receipt.json").exists()


def test_forged_capacity_checkpoint_is_rejected_without_client(tmp_path: Path) -> None:
    root = tmp_path / "forged-capacity"
    frozen = _preflight_and_freeze(root)
    launch_path = root / "launch-receipt.json"
    canary._write_immutable_json(  # noqa: SLF001
        launch_path, canary._launch_receipt_payload(frozen)  # noqa: SLF001
    )
    canary._turn_paths(root)["capacity"].write_text("{}\n", encoding="utf-8")  # noqa: SLF001
    factory = _FakeFactory()
    with pytest.raises(canary.ExpandedCapCanaryError, match="capacity checkpoint"):
        asyncio.run(canary.run(output_dir=root, client_factory=factory))
    assert factory.clients == []
    assert not (root / "plan-step-receipt.json").exists()


def test_partial_sidecar_and_output_freeze_waiting_without_replay(
    tmp_path: Path,
) -> None:
    root = tmp_path / "partial-sidecar-output"
    frozen = _write_valid_launch_and_capacity(root)
    paths = canary._turn_paths(root)  # noqa: SLF001
    client = _FakeClient(frozen["turn"]["config_overlay"])
    client.bind_turn_lineage(
        runtime_lock=canary._record(frozen["runtime_lock"]),  # noqa: SLF001
        launch_receipt=canary._record(root / "launch-receipt.json"),  # noqa: SLF001
        capacity_checkpoint=canary._record(paths["capacity"]),  # noqa: SLF001
    )
    verification = canary._semantic_instruction_verification(  # noqa: SLF001
        client=client, thread=client.thread, frozen=frozen
    )
    client.bind_instruction_verification(
        verification=verification, path=paths["instruction_verification"]
    )
    asyncio.run(
        client.run_structured_turn(
            thread=client.thread,
            effort=canary.EFFORT,
            prompt=frozen["turn"]["prompt"],
            output_schema=frozen["turn"]["schema"],
            sidecar_path=paths["sidecar"],
            output_path=paths["output"],
            batch_size=2,
            thread_mode="new_thread",
        )
    )
    paths["instruction_verification"].unlink()
    sidecar = json.loads(paths["sidecar"].read_text(encoding="utf-8"))
    sidecar.pop("instruction_verification")
    paths["sidecar"].write_text(json.dumps(sidecar) + "\n", encoding="utf-8")
    recovery_factory = _FakeFactory()
    receipt = asyncio.run(
        canary.run(output_dir=root, client_factory=recovery_factory)
    )
    assert receipt["state"] == "waiting"
    assert receipt["terminal_reason"] == (
        "epoch4_partial_semantic_artifacts_require_no_retry_recovery"
    )
    assert receipt["semantic_model_call_count"] == 1
    assert receipt["accounting_complete"] is True
    assert recovery_factory.clients == []
    assert receipt["full_event_ab_ba_evaluation_authorized"] is False


def test_output_committed_with_in_progress_sidecar_freezes_verified_waiting(
    tmp_path: Path,
) -> None:
    root = tmp_path / "output-committed-in-progress"
    frozen = _write_valid_launch_and_capacity(root)
    paths = _write_valid_completed_artifacts(root, frozen)
    sidecar = json.loads(paths["sidecar"].read_text(encoding="utf-8"))
    sidecar["state"] = "in_progress"
    for field in (
        "finished_at",
        "status",
        "error_class",
        "wall_elapsed_seconds",
        "usage",
        "thread_total_usage",
        "usage_complete",
        "usage_status",
        "output_sha256",
    ):
        sidecar.pop(field, None)
    paths["sidecar"].write_text(json.dumps(sidecar) + "\n", encoding="utf-8")

    recovery_factory = _FakeFactory()
    receipt = asyncio.run(canary.run(output_dir=root, client_factory=recovery_factory))
    assert receipt["state"] == "waiting"
    assert receipt["terminal_reason"] == (
        "epoch4_partial_semantic_artifacts_require_no_retry_recovery"
    )
    assert receipt["semantic_model_call_count"] == 1
    assert receipt["usage_status"] == "unknown"
    assert receipt["accounting_complete"] is False
    assert receipt["full_event_ab_ba_evaluation_authorized"] is False
    assert recovery_factory.clients == []
    assert canary.verify_receipt(root) == receipt


def test_completed_missing_telemetry_receipts_are_verified_waiting(
    tmp_path: Path,
) -> None:
    expected_usage = {
        "token_usage_missing": ("unknown", False),
        "agent_message_missing": ("complete", True),
    }
    for error_class, (usage_status, accounting_complete) in expected_usage.items():
        root = tmp_path / error_class
        frozen = _write_valid_launch_and_capacity(root)
        paths = _write_valid_completed_artifacts(root, frozen)
        paths["output"].unlink()
        sidecar = json.loads(paths["sidecar"].read_text(encoding="utf-8"))
        sidecar.update(
            {
                "state": "failed",
                "status": "completed",
                "error_class": error_class,
            }
        )
        if error_class == "token_usage_missing":
            sidecar.update(
                {
                    "usage": None,
                    "thread_total_usage": None,
                    "usage_complete": False,
                    "usage_status": "unknown",
                }
            )
        else:
            sidecar["output_sha256"] = None
        paths["sidecar"].write_text(json.dumps(sidecar) + "\n", encoding="utf-8")

        recovery_factory = _FakeFactory()
        receipt = asyncio.run(
            canary.run(output_dir=root, client_factory=recovery_factory)
        )
        assert receipt["state"] == "waiting"
        assert receipt["semantic_model_call_count"] == 1
        assert receipt["usage_status"] == usage_status
        assert receipt["accounting_complete"] is accounting_complete
        assert receipt["full_event_ab_ba_evaluation_authorized"] is False
        assert recovery_factory.clients == []
        assert canary.verify_receipt(root) == receipt


def test_pre_turn_id_process_death_freezes_verified_waiting_without_replay(
    tmp_path: Path,
) -> None:
    root = tmp_path / "pre-turn-process-death"
    frozen = _write_valid_launch_and_capacity(root)
    paths = _write_valid_completed_artifacts(root, frozen)
    paths["output"].unlink()
    paths["instruction_verification"].unlink()
    sidecar = json.loads(paths["sidecar"].read_text(encoding="utf-8"))
    sidecar.update(
        {
            "state": "failed",
            "status": "client_error",
            "error_class": "AppServerProcessDied",
            "turn_id": None,
            "semantic_turn_id": None,
            "usage": None,
            "thread_total_usage": None,
            "usage_complete": False,
            "usage_status": "unknown",
            "output_sha256": None,
        }
    )
    sidecar.pop("instruction_verification", None)
    paths["sidecar"].write_text(json.dumps(sidecar) + "\n", encoding="utf-8")

    recovery_factory = _FakeFactory()
    receipt = asyncio.run(canary.run(output_dir=root, client_factory=recovery_factory))
    assert receipt["state"] == "waiting"
    assert receipt["terminal_reason"] == (
        "epoch4_partial_semantic_artifacts_require_no_retry_recovery"
    )
    assert receipt["semantic_model_call_count"] == 1
    assert receipt["usage_status"] == "unknown"
    assert receipt["accounting_complete"] is False
    assert receipt["full_event_ab_ba_evaluation_authorized"] is False
    assert recovery_factory.clients == []
    assert canary.verify_receipt(root) == receipt


@pytest.mark.parametrize(
    ("terminal_kind", "expected_state", "expected_usage_status"),
    [
        ("interrupted", "waiting", "unknown"),
        ("structured_output_invalid", "rejected", "complete"),
    ],
)
def test_lineage_bound_noncompleted_turn_receipts_verify_without_output(
    tmp_path: Path,
    terminal_kind: str,
    expected_state: str,
    expected_usage_status: str,
) -> None:
    root = tmp_path / terminal_kind
    _preflight_and_freeze(root)
    factory = _TerminalFakeFactory(terminal_kind)
    receipt = asyncio.run(canary.run(output_dir=root, client_factory=factory))
    assert receipt["state"] == expected_state
    assert receipt["semantic_model_call_count"] == 1
    assert receipt["usage_status"] == expected_usage_status
    assert receipt["full_event_ab_ba_evaluation_authorized"] is False
    assert not canary._turn_paths(root)["output"].exists()  # noqa: SLF001
    assert canary.verify_receipt(root) == receipt


def test_receipt_verification_rejects_alien_thread_and_turn(tmp_path: Path) -> None:
    root = tmp_path / "alien-lineage"
    _preflight_and_freeze(root)
    receipt = asyncio.run(canary.run(output_dir=root, client_factory=_FakeFactory()))
    sidecar_path = canary._turn_paths(root)["sidecar"]  # noqa: SLF001
    sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
    sidecar["thread_id"] = "alien-thread"
    sidecar["turn_id"] = "alien-turn"
    sidecar["semantic_thread_id"] = "alien-thread"
    sidecar["semantic_turn_id"] = "alien-turn"
    sidecar_path.write_text(json.dumps(sidecar) + "\n", encoding="utf-8")
    _rewrite_receipt_records(root, receipt)
    with pytest.raises(canary.ExpandedCapCanaryError, match="sidecar"):
        canary.verify_receipt(root)


def test_call_count_cannot_hide_semantic_artifacts(tmp_path: Path) -> None:
    root = tmp_path / "call-count"
    _preflight_and_freeze(root)
    receipt = asyncio.run(canary.run(output_dir=root, client_factory=_FakeFactory()))
    receipt["semantic_model_call_count"] = 0
    _rewrite_receipt_records(root, receipt)
    with pytest.raises(canary.ExpandedCapCanaryError, match="zero-call"):
        canary.verify_receipt(root)


@pytest.mark.parametrize("missing_name", ["plan-step-receipt.json", "terminal.json"])
def test_single_terminal_mirror_is_recreated_without_model_replay(
    tmp_path: Path, missing_name: str
) -> None:
    root = tmp_path / missing_name.removesuffix(".json")
    _preflight_and_freeze(root)
    original = asyncio.run(canary.run(output_dir=root, client_factory=_FakeFactory()))
    (root / missing_name).unlink()
    recovery_factory = _FakeFactory()
    recovered = asyncio.run(
        canary.run(output_dir=root, client_factory=recovery_factory)
    )
    assert recovered == original
    assert recovery_factory.clients == []
    assert (root / "plan-step-receipt.json").read_bytes() == (
        root / "terminal.json"
    ).read_bytes()
