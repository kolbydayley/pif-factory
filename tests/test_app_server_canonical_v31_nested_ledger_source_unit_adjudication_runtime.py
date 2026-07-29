from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import app_server_canonical_v31_nested_ledger_source_unit_adjudication_runtime as source_unit


def _run(coro):
    try:
        prior_loop = asyncio.get_event_loop()
    except RuntimeError:
        prior_loop = asyncio.new_event_loop()
    try:
        return asyncio.run(coro)
    finally:
        asyncio.set_event_loop(prior_loop)


def _frozen_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    contract = source_unit.load_contract()
    root = tmp_path / "epoch36"
    changed = copy.deepcopy(contract)
    changed["receipt_path"] = root / "plan-step-receipt.json"
    monkeypatch.setattr(source_unit, "load_contract", lambda: copy.deepcopy(changed))
    source_unit.freeze_run(root)
    return root


def _valid_output(variant: dict) -> dict:
    cases = []
    for case in variant["cases"]:
        witnesses = case["event_set_a"] + case["event_set_b"]
        cases.append(
            {
                "case_id": case["case_id"],
                "support_results": [
                    {
                        "witness_id": witness["witness_id"],
                        "verdict": "supported",
                        "evidence_unit_ids": [case["source_units"][0]["unit_id"]],
                        "rationale": "Synthetic exact support for transport testing.",
                    }
                    for witness in witnesses
                ],
                "equivalence_groups": [
                    {
                        "witness_ids": [witness["witness_id"]],
                        "rationale": "Synthetic singleton full-event partition.",
                    }
                    for witness in witnesses
                ],
            }
        )
    output = {"cases": cases}
    assert source_unit.validate_source_unit_output(output, variant) == []
    return output


class _CompletedClient:
    def __init__(
        self,
        root: Path,
        *,
        identity_from_epoch35: bool = False,
        invalid_unit_reference: bool = False,
    ) -> None:
        self.root = root.resolve()
        self.policy_path = self.root / "capacity-policy.json"
        self.policy = source_unit.reserve.load_reserve_capacity_policy(self.policy_path)
        self.calls: list[dict] = []
        self.identity_from_epoch35 = identity_from_epoch35
        self.invalid_unit_reference = invalid_unit_reference

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return None

    def _write_capacity(self, path: Path) -> None:
        evaluation = source_unit.reserve.evaluate_reserve_capacity(
            {"primary_used_percent": 10, "rate_limit_reached_type": None},
            policy=self.policy,
            remaining_turn_count=1,
        )
        value = {
            "schema_version": source_unit.reserve.RESERVE_CAPACITY_CHECKPOINT_VERSION,
            "checked_at": "2026-07-20T05:00:00+00:00",
            "policy_path": str(self.policy_path),
            "policy_sha256": source_unit._sha256_file(self.policy_path),  # noqa: SLF001
            "phase_id": source_unit.STEP_ID,
            "turn_name": source_unit.CAPACITY_TURN_NAMES[0],
            "turn_ordinal": 0,
            "managed_chatgpt_auth_verified": True,
            "plan_type": "pro",
            "primary_used_percent": evaluation["primary_used_percent"],
            "primary_remaining_percent": evaluation["primary_remaining_percent"],
            "primary_resets_at": 1_800_000_000,
            "rate_limit_reached_type": None,
            "minimum_remaining_reserve_percent": evaluation[
                "minimum_remaining_reserve_percent"
            ],
            "usable_percent_above_reserve": evaluation["usable_percent_above_reserve"],
            "remaining_turn_count": evaluation["remaining_turn_count"],
            "projected_remaining_tokens": evaluation["projected_remaining_tokens"],
            "quota_points_per_million_tokens": evaluation[
                "quota_points_per_million_tokens"
            ],
            "projected_remaining_quota_points": evaluation[
                "projected_remaining_quota_points"
            ],
            "projected_terminal_remaining_percent": evaluation[
                "projected_terminal_remaining_percent"
            ],
            "cleared_for_semantic_turn": True,
            "thread_started": False,
            "turn_started": False,
            "sidecar_started": False,
            "retry_checkpoint_reuse_allowed": False,
            "privacy": (
                "capacity_status_policy_hash_and_counts_no_prompt_output_email_"
                "credentials_or_thread_ids"
            ),
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value, sort_keys=True) + "\n", encoding="utf-8")

    async def run_ephemeral_structured_turn(self, **kwargs):
        variant = json.loads(
            (self.root / "requests/variant.private.json").read_text(encoding="utf-8")
        )
        output = _valid_output(variant)
        if self.invalid_unit_reference:
            output["cases"][0]["support_results"][0]["evidence_unit_ids"] = [
                "S9999U9999"
            ]
        output_text = json.dumps(
            output, ensure_ascii=True, sort_keys=True, separators=(",", ":")
        )
        output_path = Path(kwargs["output_path"]).resolve()
        sidecar_path = Path(kwargs["sidecar_path"]).resolve()
        capacity_path = Path(kwargs["capacity_checkpoint_path"]).resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        sidecar_path.parent.mkdir(parents=True, exist_ok=True)
        self._write_capacity(capacity_path)
        output_path.write_text(output_text + "\n", encoding="utf-8")
        schema_text = source_unit._canonical_json(kwargs["output_schema"])  # noqa: SLF001
        usage = {
            "input_tokens": 100,
            "cached_input_tokens": 10,
            "output_tokens": 20,
            "reasoning_output_tokens": 5,
            "total_tokens": 120,
        }
        prior_turn = source_unit._validate_epoch35_terminal()["turns"][0]  # noqa: SLF001
        thread_id = (
            prior_turn["thread_id"]
            if self.identity_from_epoch35
            else "fixture-epoch36-thread"
        )
        turn_id = (
            prior_turn["turn_id"] if self.identity_from_epoch35 else "fixture-epoch36-turn"
        )
        sidecar = {
            "schema_version": source_unit.codex_app_server.TURN_SIDECAR_SCHEMA_VERSION,
            "state": "completed",
            "status": "completed",
            "cli_version": source_unit.codex_app_server.PINNED_CODEX_CLI_VERSION,
            "client_version": source_unit.codex_app_server.APP_SERVER_CLIENT_VERSION,
            "protocol_schema_sha256": source_unit.codex_app_server.PROTOCOL_SCHEMA_SHA256,
            "transport": "stdio",
            "auth_type": "chatgpt",
            "plan_type": "pro",
            "thread_mode": "new_thread",
            "synthetic_debug_errors": False,
            "recovery_reran_model": False,
            "model": kwargs["model"],
            "effort": kwargs["effort"],
            "batch_size": kwargs["batch_size"],
            "instruction_sources_count": source_unit.EXPECTED_INSTRUCTION_SOURCES_COUNT,
            "instruction_sources_sha256": source_unit.EXPECTED_INSTRUCTION_SOURCES_SHA256,
            "thread_id": thread_id,
            "turn_id": turn_id,
            "app_server_user_agent": "fixture-codex-app-server",
            "output_path": str(output_path),
            "prompt_sha256": source_unit._sha256_text(kwargs["prompt"]),  # noqa: SLF001
            "prompt_bytes": len(kwargs["prompt"].encode("utf-8")),
            "base_instructions_sha256": source_unit._sha256_text(  # noqa: SLF001
                kwargs["base_instructions"]
            ),
            "base_instructions_bytes": len(kwargs["base_instructions"].encode("utf-8")),
            "output_schema_sha256": source_unit._sha256_text(schema_text),  # noqa: SLF001
            "output_schema_bytes": len(schema_text.encode("utf-8")),
            "output_sha256": source_unit._sha256_text(output_text),  # noqa: SLF001
            "usage_status": "measured",
            "usage_complete": True,
            "usage": usage,
            "thread_total_usage": usage,
            "wall_elapsed_seconds": 1.25,
            "finished_at": "2026-07-20T05:00:01+00:00",
        }
        sidecar_path.write_text(json.dumps(sidecar, sort_keys=True) + "\n", encoding="utf-8")
        self.calls.append(copy.deepcopy(kwargs))
        return SimpleNamespace(
            status_ok=True,
            status="completed",
            output=output,
            error_class=None,
            usage=SimpleNamespace(total_tokens=120),
        )


def _started_sidecar(root: Path) -> dict:
    variant, base, prompt, schema = source_unit._request_material()  # noqa: SLF001
    schema_text = source_unit._canonical_json(schema)  # noqa: SLF001
    return {
        "schema_version": source_unit.codex_app_server.TURN_SIDECAR_SCHEMA_VERSION,
        "state": "started",
        "status": "in_progress",
        "cli_version": source_unit.codex_app_server.PINNED_CODEX_CLI_VERSION,
        "client_version": source_unit.codex_app_server.APP_SERVER_CLIENT_VERSION,
        "protocol_schema_sha256": source_unit.codex_app_server.PROTOCOL_SCHEMA_SHA256,
        "transport": "stdio",
        "auth_type": "chatgpt",
        "plan_type": "pro",
        "thread_mode": "new_thread",
        "synthetic_debug_errors": False,
        "recovery_reran_model": False,
        "model": source_unit.MODEL,
        "effort": source_unit.EFFORT,
        "batch_size": len(variant["cases"]),
        "instruction_sources_count": source_unit.EXPECTED_INSTRUCTION_SOURCES_COUNT,
        "instruction_sources_sha256": source_unit.EXPECTED_INSTRUCTION_SOURCES_SHA256,
        "thread_id": "fixture-partial-epoch36-thread",
        "turn_id": "fixture-partial-epoch36-turn",
        "output_path": str(
            (root / "turns" / source_unit.TURN_DIRECTORY_NAME / "output.private.json").resolve()
        ),
        "prompt_sha256": source_unit._sha256_text(prompt),  # noqa: SLF001
        "prompt_bytes": len(prompt.encode("utf-8")),
        "base_instructions_sha256": source_unit._sha256_text(base),  # noqa: SLF001
        "base_instructions_bytes": len(base.encode("utf-8")),
        "output_schema_sha256": source_unit._sha256_text(schema_text),  # noqa: SLF001
        "output_schema_bytes": len(schema_text.encode("utf-8")),
    }


def test_exact_predecessor_disagreement_is_two_side_free_cases_covering_all_witnesses() -> None:
    contract = source_unit.load_contract()
    bundle = source_unit._base_bundle()  # noqa: SLF001
    case_ids = source_unit.observable_disagreement_case_ids(
        bundle["pool"], bundle["outputs"]
    )
    variant = source_unit.build_adjudication_variant(bundle["pool"], case_ids)

    assert contract["predecessor_receipt"]["state"] == "waiting"
    assert case_ids == list(source_unit.EXPECTED_DISAGREEMENT_CASE_IDS)
    assert len(variant["cases"]) == 2
    assert [len(case["event_set_a"]) for case in variant["cases"]] == [38, 35]
    assert all(case["event_set_b"] == [] for case in variant["cases"])
    assert [len(case["source_units"]) for case in variant["cases"]] == [67, 68]
    assert sum(len(case["event_set_a"]) for case in variant["cases"]) == 73
    assert max(
        len(unit["text"])
        for case in variant["cases"]
        for unit in case["source_units"]
    ) <= 900
    serialized = json.dumps(variant, sort_keys=True)
    assert source_unit.epoch35.SYSTEM_BASELINE not in serialized
    assert source_unit.epoch35.SYSTEM_CANDIDATE not in serialized


def test_source_unit_projection_is_exact_and_standard_judge_valid() -> None:
    bundle = source_unit._base_bundle()  # noqa: SLF001
    case_ids = source_unit.observable_disagreement_case_ids(
        bundle["pool"], bundle["outputs"]
    )
    variant = source_unit.build_adjudication_variant(bundle["pool"], case_ids)
    output = _valid_output(variant)
    standard = source_unit._standard_adjudication_variant(  # noqa: SLF001
        bundle["pool"], case_ids
    )

    projected = source_unit.project_source_unit_output(output, variant, standard)

    assert source_unit.judge.validate_judge_output(projected, standard) == []
    assert "evidence_spans" not in json.dumps(output, sort_keys=True)
    source = standard["cases"][0]["source_excerpt"]
    assert all(
        span in source and 0 < len(span) <= source_unit.SOURCE_UNIT_MAX_CHARS
        for row in projected["cases"][0]["support_results"]
        for span in row["evidence_spans"]
    )


def test_freeze_is_zero_call_and_binds_predecessor_reserve_and_transport(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _frozen_root(tmp_path, monkeypatch)
    lock = source_unit.verify_runtime_lock(root / "runtime-lock.json")
    policy = json.loads((root / "capacity-policy.json").read_text(encoding="utf-8"))

    assert lock["plan_epoch"] == 36
    assert lock["semantic_model_call_cap"] == 1
    assert lock["prior_semantic_replay_allowed"] is False
    assert lock["source_unit_ids_opaque"] is True
    assert lock["predecessor_records"] == source_unit._expected_predecessor_records()  # noqa: SLF001
    assert policy["ordered_turn_names"] == ["epoch36_quality_adjudication"]
    assert policy["minimum_remaining_reserve_percent"] == 20
    assert not (root / "launch-receipt.json").exists()
    assert not (root / "turns").exists()


def test_completed_fixture_terminalizes_once_with_exact_accounting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _frozen_root(tmp_path, monkeypatch)
    fake = _CompletedClient(root)

    receipt = _run(source_unit.run(root, client_factory=lambda: fake))

    assert receipt["state"] == "rejected"
    assert receipt["semantic_model_call_count"] == 1
    assert receipt["measured_model_call_count"] == 1
    assert receipt["unknown_usage_turn_count"] == 0
    assert receipt["usage"]["total_tokens"] == 120
    assert receipt["quality_total_tokens_including_prior_quality_calls"] == 148_769
    assert receipt["quality_measured_by_this_step"] is True
    assert len(fake.calls) == 1
    assert source_unit.verify_receipt(root)["state"] == "rejected"


def test_invalid_unit_reference_terminalizes_measured_rejection_without_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _frozen_root(tmp_path, monkeypatch)
    fake = _CompletedClient(root, invalid_unit_reference=True)

    receipt = _run(source_unit.run(root, client_factory=lambda: fake))

    assert receipt["state"] == "rejected"
    assert receipt["terminal_reason"] == "epoch36_source_unit_output_rejected"
    assert receipt["semantic_model_call_count"] == 1
    assert receipt["measured_model_call_count"] == 1
    assert receipt["unknown_usage_turn_count"] == 0
    assert receipt["usage"]["total_tokens"] == 120
    assert receipt["diagnostic"]["validation_error_count"] == 1
    assert receipt["diagnostic"]["semantic_retry_allowed"] is False
    assert len(fake.calls) == 1
    assert source_unit.verify_receipt(root) == receipt


def test_partial_attempt_terminalizes_waiting_without_replay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _frozen_root(tmp_path, monkeypatch)
    source_unit._launch_receipt(root)  # noqa: SLF001
    sidecar_path = root / "turns" / source_unit.TURN_DIRECTORY_NAME / "sidecar.json"
    sidecar_path.parent.mkdir(parents=True)
    sidecar_path.write_text(
        json.dumps(_started_sidecar(root), sort_keys=True) + "\n", encoding="utf-8"
    )
    called = False

    def must_not_create_client():
        nonlocal called
        called = True
        raise AssertionError("partial source_unit was replayed")

    receipt = _run(source_unit.run(root, client_factory=must_not_create_client))

    assert called is False
    assert receipt["state"] == "waiting"
    assert receipt["terminal_reason"] == (
        "epoch36_partial_or_interrupted_adjudication_preserved_without_replay"
    )
    assert receipt["semantic_model_call_count"] == 1
    assert receipt["unknown_usage_turn_count"] == 1
    assert source_unit.verify_receipt(root)["state"] == "waiting"


def test_complete_turn_crash_finalizes_without_new_client(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _frozen_root(tmp_path, monkeypatch)
    fake = _CompletedClient(root)
    source_unit._launch_receipt(root)  # noqa: SLF001
    variant, base, prompt, schema = source_unit._request_material()  # noqa: SLF001
    turn_root = root / "turns" / source_unit.TURN_DIRECTORY_NAME
    _run(
        fake.run_ephemeral_structured_turn(
            model=source_unit.MODEL,
            effort=source_unit.EFFORT,
            base_instructions=base,
            prompt=prompt,
            output_schema=schema,
            cwd=source_unit.PROJECT_ROOT,
            sidecar_path=turn_root / "sidecar.json",
            output_path=turn_root / "output.private.json",
            capacity_checkpoint_path=turn_root / "capacity.json",
            batch_size=len(variant["cases"]),
            thread_mode="new_thread",
            timeout_seconds=source_unit.TIMEOUT_SECONDS,
        )
    )
    called = False

    def must_not_create_client():
        nonlocal called
        called = True
        raise AssertionError("complete source_unit was replayed")

    receipt = _run(source_unit.run(root, client_factory=must_not_create_client))
    assert called is False
    assert receipt["state"] == "rejected"
    assert receipt["semantic_model_call_count"] == 1


def test_prior_thread_turn_identity_reuse_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _frozen_root(tmp_path, monkeypatch)
    fake = _CompletedClient(root, identity_from_epoch35=True)

    with pytest.raises(source_unit.Epoch36SourceUnitAdjudicationError, match="reused"):
        _run(source_unit.run(root, client_factory=lambda: fake))


def test_runtime_rejects_request_tamper(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _frozen_root(tmp_path, monkeypatch)
    prompt = root / "requests/prompt.private.md"
    prompt.write_text(prompt.read_text(encoding="utf-8") + "tamper\n", encoding="utf-8")

    with pytest.raises(source_unit.Epoch36SourceUnitAdjudicationError, match="runtime lock"):
        source_unit.verify_runtime_lock(root / "runtime-lock.json")


def test_single_valid_terminal_mirror_repairs_without_client(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _frozen_root(tmp_path, monkeypatch)
    receipt = _run(
        source_unit.run(root, client_factory=lambda: _CompletedClient(root))
    )
    (root / "terminal.json").unlink()
    called = False

    def must_not_create_client():
        nonlocal called
        called = True
        raise AssertionError("terminal mirror repair opened a client")

    recovered = _run(source_unit.run(root, client_factory=must_not_create_client))
    assert called is False
    assert recovered == receipt
    assert (root / "terminal.json").is_file()


def test_external_auth_and_default_root_injection_are_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(source_unit.Epoch36SourceUnitAdjudicationError, match="forbids injected"):
        _run(source_unit.run(source_unit.DEFAULT_ROOT, client_factory=lambda: object()))
    monkeypatch.setenv("OPENAI_API_KEY", "forbidden-fixture-value")
    with pytest.raises(
        source_unit.Epoch36SourceUnitAdjudicationError,
        match="managed ChatGPT execution rejects API-key/raw-session auth",
    ):
        source_unit._reject_external_auth_material()  # noqa: SLF001


def test_cli_exposes_all_bounded_phases() -> None:
    for command in ("freeze", "verify-runtime", "run", "verify-receipt", "status"):
        assert source_unit._parser().parse_args([command]).command == command  # noqa: SLF001
