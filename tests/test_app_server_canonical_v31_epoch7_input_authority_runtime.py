from __future__ import annotations

import asyncio
import copy
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from research_factory import app_server_canonical_v31_epoch7_input_authority_runtime as runtime


AUTHORITY_ROOT = runtime.DEFAULT_AUTHORITY_ROOT


def _freeze(tmp_path: Path) -> Path:
    root = tmp_path / "authority-runtime"
    runtime.freeze_preauthorization(root=root, authority_root=AUTHORITY_ROOT)
    return root


def _authorize(root: Path, *, authorization_id: str = "kolby-epoch7-authority-001") -> datetime:
    issued = datetime.now(timezone.utc).replace(microsecond=0)
    runtime.authorize_runtime(
        root=root,
        operator_authorization_id=authorization_id,
        issued_at=issued,
        expires_at=issued + timedelta(hours=2),
    )
    return issued


def _capacity_response(*, used_percent: int) -> dict:
    reset_at = int(datetime.now(timezone.utc).timestamp()) + 3600
    snapshot = {
        "limitId": "codex",
        "primary": {
            "usedPercent": used_percent,
            "resetsAt": reset_at,
            "windowDurationMins": 300,
        },
        "secondary": {
            "usedPercent": used_percent,
            "resetsAt": reset_at,
            "windowDurationMins": 10_080,
        },
        "rateLimitReachedType": None,
    }
    return {
        "rateLimits": copy.deepcopy(snapshot),
        "rateLimitsByLimitId": {"codex": snapshot},
        "rateLimitResetCredits": {"availableCount": 0},
    }


def test_zero_call_preauthorization_freeze_is_nonexecutable(tmp_path: Path) -> None:
    root = _freeze(tmp_path)

    receipt = runtime.verify_preauthorization(root)
    status = runtime.status_runtime(root)

    assert receipt["state"] == "passed_zero_call_preauthorization_only"
    assert receipt["semantic_model_call_count"] == 0
    assert receipt["operator_authorization_present"] is False
    assert status["state"] == "waiting"
    assert status["reason"] == "explicit_kolby_authorization_required"
    assert {path.name for path in root.iterdir()} == {
        runtime.CONTRACT_FILENAME,
        runtime.RUNTIME_LOCK_FILENAME,
        runtime.PREAUTHORIZATION_FILENAME,
    }


def test_authorization_is_exact_bounded_and_round_trips(tmp_path: Path) -> None:
    root = _freeze(tmp_path)
    issued = _authorize(root)

    authorization = runtime.verify_authorization(
        root,
        expected_authorization_id="kolby-epoch7-authority-001",
        now=issued + timedelta(minutes=1),
    )

    assert authorization["authorized_by"] == "kolby"
    assert authorization["exact_model_call_cap"] == 4
    assert authorization["exact_total_token_cap"] == 408_000
    assert authorization["caller_supplied_capacity_admission_allowed"] is False
    assert authorization["holdout_authorized"] is False
    assert authorization["production_mutation_allowed"] is False

    with pytest.raises(runtime.CanonicalV31Epoch7AuthorityRuntimeError, match="ID differs"):
        runtime.verify_authorization(
            root,
            expected_authorization_id="kolby-wrong-authority-999",
            now=issued + timedelta(minutes=1),
        )


def test_api_key_environment_is_rejected_before_runtime_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _freeze(tmp_path)
    _authorize(root)
    monkeypatch.setenv("OPENAI_API_KEY", "test-only-forbidden-value")

    with pytest.raises(
        runtime.CanonicalV31Epoch7AuthorityRuntimeError,
        match="rejects API-key/raw-session auth",
    ):
        asyncio.run(
            runtime.execute_authority_runtime(
                root=root,
                operator_authorization_id="kolby-epoch7-authority-001",
            )
        )

    assert (root / runtime.EXECUTION_RECEIPT_FILENAME).exists() is False
    assert (root / runtime.TURNS_DIRECTORY).exists() is False


def test_capacity_uses_reserve_arithmetic_and_not_provider_token_claims() -> None:
    measured = datetime.now(timezone.utc).replace(microsecond=0)
    authorization = {"expires_at": (measured + timedelta(hours=2)).isoformat()}
    contract = {
        "minimum_remaining_reserve_percent": 20,
        "capacity_safety_margin_percent": 1,
        "operator_wall_safety_margin_seconds": 60,
    }
    request = {
        "boundary": "initial_before_thread",
        "turn_id": "turn_fixture",
        "episode_id": "episode_fixture",
        "required_remaining_quota_points": 7,
        "required_remaining_total_token_ceiling": 408_000,
        "required_remaining_wall_seconds_ceiling": 2_856,
        "semantic_thread_started": False,
    }

    clear = runtime._capacity_measurement(
        _capacity_response(used_percent=72),
        request=request,
        contract=contract,
        authorization=authorization,
        measured_at=measured,
    )
    denied = runtime._capacity_measurement(
        _capacity_response(used_percent=73),
        request=request,
        contract=contract,
        authorization=authorization,
        measured_at=measured,
    )

    assert clear["minimum_applicable_remaining_percent"] == 28
    assert clear["usable_quota_points_above_reserve"] == 7
    assert clear["capacity_available"] is True
    assert clear["provider_reports_tokens_remaining"] is False
    assert clear["capacity_is_estimate_not_reservation"] is True
    assert denied["usable_quota_points_above_reserve"] == 6
    assert denied["capacity_available"] is False


def test_partial_attempt_terminalizes_waiting_without_client_or_replay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _freeze(tmp_path)
    _authorize(root)
    contract = runtime._load_object(root / runtime.CONTRACT_FILENAME, label="contract")
    authorization = runtime.verify_authorization(root)
    first = contract["turns"][0]
    paths = runtime._turn_paths(root, first["turn_id"])
    paths["root"].mkdir(parents=True)
    runtime._write_immutable_json(
        paths["attempt"],
        runtime._attempt_payload(
            root=root,
            contract=contract,
            authorization=authorization,
            turn=first,
        ),
    )

    def forbidden_client():
        raise AssertionError("partial recovery must not create an app-server client")

    monkeypatch.setattr(runtime.adapter, "_client_factory", forbidden_client)
    receipt = asyncio.run(
        runtime.execute_authority_runtime(
            root=root,
            operator_authorization_id=authorization["operator_authorization_id"],
        )
    )

    assert receipt["state"] == "waiting"
    assert receipt["terminal_reason"] == "epoch7_authority_partial_attempt_preserved_no_replay"
    assert receipt["semantic_model_call_count"] == 0
    assert receipt["semantic_retry_count"] == 0
    assert receipt["completed_validated_turn_count"] == 0
    assert runtime.verify_execution_receipt(root) == receipt
    (root / runtime.TERMINAL_FILENAME).unlink()
    assert runtime.verify_execution_receipt(root) == receipt
    assert (root / runtime.TERMINAL_FILENAME).read_bytes() == (
        root / runtime.EXECUTION_RECEIPT_FILENAME
    ).read_bytes()


def test_completed_output_crash_finalizes_prefix_without_client_or_replay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _freeze(tmp_path)
    _authorize(root)
    contract = runtime._load_object(root / runtime.CONTRACT_FILENAME, label="contract")
    authorization = runtime.verify_authorization(root)
    first, second = contract["turns"][:2]
    paths = runtime._turn_paths(root, first["turn_id"])
    paths["root"].mkdir(parents=True)
    runtime._write_immutable_json(
        paths["attempt"],
        runtime._attempt_payload(
            root=root,
            contract=contract,
            authorization=authorization,
            turn=first,
        ),
    )
    measured = datetime.now(timezone.utc).replace(microsecond=0)
    for boundary, bundle_path in (
        ("initial_before_thread", paths["initial_capacity"]),
        ("preturn_before_turn", paths["preturn_capacity"]),
    ):
        request = runtime._capacity_request(
            root=root,
            contract=contract,
            authorization=authorization,
            turn=first,
            boundary=boundary,
            completed_turn_count=0,
        )
        response = _capacity_response(used_percent=0)
        measurement = runtime._capacity_measurement(
            response,
            request=request,
            contract=contract,
            authorization=authorization,
            measured_at=measured,
        )
        runtime._publish_capacity_bundle(
            bundle_path,
            request=request,
            response=response,
            measurement=measurement,
        )
    manifest = runtime._load_object(
        runtime._verify_record(first["turn_manifest"], label="turn manifest"),
        label="turn manifest",
    )
    base_path = runtime._verify_record(manifest["base_instructions"], label="base")
    sources = runtime.adapter.expected_instruction_source_contract()
    thread_binding = {
        "schema_version": runtime.THREAD_BINDING_VERSION,
        "turn_id": first["turn_id"],
        "episode_id": first["episode_id"],
        "thread_id": "thread_completed_crash_fixture",
        "model": contract["model"],
        "effort": contract["effort"],
        "cwd": str(runtime.PROJECT_ROOT.resolve()),
        "ephemeral": True,
        "base_instructions_sha256": runtime._sha256_bytes(base_path.read_bytes()),
        "base_instructions_bytes": base_path.stat().st_size,
        "instruction_sources_sha256": sources["effective_instruction_sources_sha256"],
        "instruction_sources_count": sources["effective_instruction_sources_count"],
        "persisted_path_sha256": None,
        "quality_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    runtime._write_immutable_json(paths["thread"], thread_binding)
    raw_output = {"fixture_authority_output": True}
    paths["raw_output"].write_text(runtime._pretty_json(raw_output), encoding="ascii")
    prompt_path = runtime._verify_record(manifest["prompt"], label="prompt")
    schema = runtime._load_object(
        runtime._verify_record(manifest["output_schema"], label="schema"),
        label="schema",
    )
    raw_message = paths["raw_output"].read_text(encoding="utf-8")[:-1]
    usage = {
        "input_tokens": 100,
        "cached_input_tokens": 50,
        "output_tokens": 20,
        "reasoning_output_tokens": 10,
        "total_tokens": 120,
    }
    sidecar = {
        "schema_version": runtime.codex_app_server.TURN_SIDECAR_SCHEMA_VERSION,
        "started_at": measured.isoformat(),
        "finished_at": (measured + timedelta(seconds=1)).isoformat(),
        "client_version": runtime.codex_app_server.APP_SERVER_CLIENT_VERSION,
        "cli_version": runtime.codex_app_server.PINNED_CODEX_CLI_VERSION,
        "app_server_user_agent": "codex-test",
        "protocol_schema_sha256": runtime._record(
            runtime.codex_app_server.PROTOCOL_SCHEMA_PATH
        )["sha256"],
        "transport": "stdio",
        "max_message_bytes": 1_000_000,
        "synthetic_debug_errors": False,
        "auth_type": "chatgpt",
        "plan_type": "pro",
        "thread_id": thread_binding["thread_id"],
        "turn_id": "semantic_turn_completed_crash_fixture",
        "model": contract["model"],
        "effort": contract["effort"],
        "batch_size": 1,
        "thread_mode": "new_thread",
        "prompt_sha256": runtime._sha256_bytes(prompt_path.read_bytes()),
        "prompt_bytes": prompt_path.stat().st_size,
        "base_instructions_sha256": thread_binding["base_instructions_sha256"],
        "base_instructions_bytes": thread_binding["base_instructions_bytes"],
        "instruction_sources_sha256": thread_binding["instruction_sources_sha256"],
        "instruction_sources_count": thread_binding["instruction_sources_count"],
        "output_schema_sha256": runtime._sha256_bytes(
            runtime._canonical_json(schema).encode("utf-8")
        ),
        "output_schema_bytes": len(runtime._canonical_json(schema).encode("utf-8")),
        "output_path": str(paths["raw_output"].resolve()),
        "state": "completed",
        "status": "completed",
        "error_class": None,
        "usage_status": "measured",
        "usage_complete": True,
        "usage": usage,
        "thread_total_usage": usage,
        "wall_elapsed_seconds": 1.0,
        "recovery_reran_model": False,
        "output_sha256": runtime._sha256_bytes(raw_message.encode("utf-8")),
        "stderr_sha256": "0" * 64,
        "stderr_bytes": 0,
    }
    runtime._write_immutable_json(paths["sidecar"], sidecar)
    second_paths = runtime._turn_paths(root, second["turn_id"])
    second_paths["root"].mkdir(parents=True)
    runtime._write_immutable_json(
        second_paths["attempt"],
        runtime._attempt_payload(
            root=root,
            contract=contract,
            authorization=authorization,
            turn=second,
        ),
    )

    monkeypatch.setattr(
        runtime.authority_plan,
        "validate_authority_output",
        lambda value, **_kwargs: copy.deepcopy(dict(value)),
    )

    def forbidden_client():
        raise AssertionError("completed-prefix recovery must not create a client")

    monkeypatch.setattr(runtime, "TRUSTED_CLIENT_FACTORY", forbidden_client)
    receipt = asyncio.run(
        runtime.execute_authority_runtime(
            root=root,
            operator_authorization_id=authorization["operator_authorization_id"],
        )
    )

    assert receipt["state"] == "waiting"
    assert receipt["completed_validated_turn_count"] == 1
    assert receipt["semantic_model_call_count"] == 1
    assert paths["result"].is_file()
    assert second_paths["result"].exists() is False


def test_runtime_lock_source_tamper_is_rejected(tmp_path: Path) -> None:
    root = _freeze(tmp_path)
    lock_path = root / runtime.RUNTIME_LOCK_FILENAME
    value = json.loads(lock_path.read_text(encoding="utf-8"))
    value["model"] = "drifted-model"
    lock_path.write_text(runtime._pretty_json(value), encoding="ascii")

    with pytest.raises(runtime.CanonicalV31Epoch7AuthorityRuntimeError, match="lock drifted"):
        runtime.verify_preauthorization(root)


def test_merge_replaces_only_invalid_labels_and_preserves_context_and_valid_label() -> None:
    valid_label = {"segment_id": "seg_valid", "semantic": {"value": "preserve"}}
    repaired_label = {"segment_id": "seg_invalid", "semantic": {"value": "repair"}}
    original_context = {"episode_id": "ep_fixture", "speaker_map": ["A", "B"]}
    turn_input = {
        "episode_id": "ep_fixture",
        "original_context": copy.deepcopy(original_context),
        "invalid_reference_segment_ids": ["seg_invalid"],
        "preserved_valid_reference_segment_ids": ["seg_valid"],
    }
    output = {
        "excluded_source_context": ["opening credits"],
        "repaired_reference_labels": [copy.deepcopy(repaired_label)],
    }

    merged = runtime._merge_episode_authority(
        turn_input=turn_input,
        validated_output=output,
        reference_rows=[
            {
                "episode_id": "ep_fixture",
                "segment_id": "seg_invalid",
                "canonical_v31_valid": False,
            },
            {
                "episode_id": "ep_fixture",
                "segment_id": "seg_valid",
                "canonical_v31_valid": True,
            },
        ],
        reference_by_segment={"seg_valid": {"golden_output": valid_label}},
    )

    assert merged["reference_labels"] == [repaired_label, valid_label]
    assert merged["reference_labels"][1] == valid_label
    assert merged["episode_context"] == {
        **original_context,
        "excluded_source_context": ["opening credits"],
    }
    assert original_context == {"episode_id": "ep_fixture", "speaker_map": ["A", "B"]}


def test_completed_sidecar_rejects_alien_thread_lineage(tmp_path: Path) -> None:
    prompt = tmp_path / "prompt.txt"
    base = tmp_path / "base.txt"
    schema = tmp_path / "schema.json"
    output = tmp_path / "output.json"
    sidecar_path = tmp_path / "sidecar.json"
    prompt.write_text("prompt", encoding="ascii")
    base.write_text("base", encoding="ascii")
    schema.write_text("{}\n", encoding="ascii")
    output.write_text('{"ok":true}\n', encoding="utf-8")
    manifest = {
        "prompt": runtime._record(prompt),
        "base_instructions": runtime._record(base),
        "output_schema": runtime._record(schema),
    }
    thread_binding = {
        "thread_id": "thread_expected",
        "instruction_sources_sha256": "a" * 64,
        "instruction_sources_count": 1,
    }
    usage = {
        "input_tokens": 10,
        "cached_input_tokens": 4,
        "output_tokens": 3,
        "reasoning_output_tokens": 2,
        "total_tokens": 13,
    }
    sidecar = {
        "schema_version": runtime.codex_app_server.TURN_SIDECAR_SCHEMA_VERSION,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "client_version": runtime.codex_app_server.APP_SERVER_CLIENT_VERSION,
        "cli_version": runtime.codex_app_server.PINNED_CODEX_CLI_VERSION,
        "app_server_user_agent": "codex-test",
        "protocol_schema_sha256": runtime._record(
            runtime.codex_app_server.PROTOCOL_SCHEMA_PATH
        )["sha256"],
        "transport": "stdio",
        "max_message_bytes": 1_000_000,
        "synthetic_debug_errors": False,
        "auth_type": "chatgpt",
        "plan_type": "pro",
        "thread_id": "thread_alien",
        "turn_id": "turn_fixture",
        "model": runtime.authority_plan.MODEL,
        "effort": runtime.authority_plan.EFFORT,
        "batch_size": 1,
        "thread_mode": "new_thread",
        "prompt_sha256": runtime._sha256_bytes(prompt.read_bytes()),
        "prompt_bytes": prompt.stat().st_size,
        "base_instructions_sha256": runtime._sha256_bytes(base.read_bytes()),
        "base_instructions_bytes": base.stat().st_size,
        "instruction_sources_sha256": "a" * 64,
        "instruction_sources_count": 1,
        "output_schema_sha256": runtime._sha256_bytes(b"{}"),
        "output_schema_bytes": 2,
        "output_path": str(output.resolve()),
        "state": "completed",
        "status": "completed",
        "error_class": None,
        "usage_status": "measured",
        "usage_complete": True,
        "usage": usage,
        "thread_total_usage": usage,
        "wall_elapsed_seconds": 1.0,
        "recovery_reran_model": False,
        "output_sha256": runtime._sha256_bytes(b'{"ok":true}'),
        "stderr_sha256": "0" * 64,
        "stderr_bytes": 0,
    }
    sidecar_path.write_text(runtime._pretty_json(sidecar), encoding="ascii")

    with pytest.raises(
        runtime.CanonicalV31Epoch7AuthorityRuntimeError,
        match="sidecar drifted at thread_id",
    ):
        runtime._validate_completed_sidecar(
            sidecar_path=sidecar_path,
            output_path=output,
            manifest=manifest,
            thread_binding=thread_binding,
        )
