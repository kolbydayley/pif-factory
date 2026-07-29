from __future__ import annotations

import datetime as dt
import fcntl
import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

from research_factory.app_server_thread_supervisor import (
    AUTOMATION_ID,
    AUTOMATION_NAME,
    AUTOMATION_RRULE,
    DEFAULT_AUTOMATION_TOML,
    DEFAULT_CANONICAL_AUTOMATION_TOML,
    EXPECTED_COMPLETION_TELEGRAM_ACCOUNT,
    EXPECTED_COMPLETION_TELEGRAM_CHANNEL,
    EXPECTED_COMPLETION_TELEGRAM_IDENTITY,
    EXPECTED_COMPLETION_TELEGRAM_SERVICE,
    EXPECTED_COMPLETION_TELEGRAM_TARGET_SHA256,
    END_TO_END_COMPLETION_SCHEMA_VERSION,
    OPERATOR_HOLD_SCHEMA_VERSION,
    SEMANTIC_PLAN_SCHEMA_VERSION,
    SEMANTIC_STEP_RECEIPT_SCHEMA_VERSION,
    SupervisorError,
    SupervisorConfig,
    TARGET_THREAD_ID,
    TriggerEvidence,
    WriterState,
    _reconcile_codex_ops_delivery,
    _verified_delivery_from_notification,
    _verify_notification_readiness_source,
    _verify_production_cutover_source,
    evaluate_semantic_plan,
    finalize_completed_supervision,
    force_native_automation_due,
    inspect_completion_authorization,
    pause_native_automation_row,
    read_appended_records,
    read_operator_hold_marker,
    read_rollout_tail,
    read_semantic_plan,
    reconcile_native_automation_row,
    restore_operator_automation,
    run_supervisor_cycle,
    update_trigger_evidence,
    validate_operator_automation,
    verify_native_trigger,
)


NOW = dt.datetime(2026, 7, 15, 4, 0, tzinfo=dt.timezone.utc)


def record(timestamp: str, record_type: str, payload: dict) -> dict:
    return {"timestamp": timestamp, "type": record_type, "payload": payload}


def lifecycle(timestamp: str, event_type: str, turn_id: str) -> dict:
    return record(timestamp, "event_msg", {"type": event_type, "turn_id": turn_id})


def context_message(timestamp: str, turn_id: str, text: str) -> dict:
    return record(
        timestamp,
        "response_item",
        {
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": text}],
            "internal_chat_message_metadata_passthrough": {"turn_id": turn_id},
        },
    )


def progress_record(timestamp: str, turn_id: str) -> dict:
    return record(
        timestamp,
        "response_item",
        {
            "type": "reasoning",
            "summary": [],
            "encrypted_content": "opaque",
            "internal_chat_message_metadata_passthrough": {"turn_id": turn_id},
        },
    )


def append_jsonl(path: Path, rows: list[dict]) -> None:
    with path.open("ab") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True).encode() + b"\n")


def write_toml(path: Path, prompt: str = "operator prompt") -> dict:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = f'''version = 1
id = "{AUTOMATION_ID}"
kind = "heartbeat"
name = "{AUTOMATION_NAME}"
prompt = "{prompt}"
status = "ACTIVE"
rrule = "{AUTOMATION_RRULE}"
target_thread_id = "{TARGET_THREAD_ID}"
created_at = 100
updated_at = 100
'''
    path.write_text(text)
    return {
        "version": 1,
        "id": AUTOMATION_ID,
        "kind": "heartbeat",
        "name": AUTOMATION_NAME,
        "prompt": prompt,
        "status": "ACTIVE",
        "rrule": AUTOMATION_RRULE,
        "target_thread_id": TARGET_THREAD_ID,
        "created_at": 100,
        "updated_at": 100,
    }


def create_automation_db(path: Path, prompt: str = "operator prompt") -> None:
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE automations (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          prompt TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'ACTIVE',
          next_run_at INTEGER,
          last_run_at INTEGER,
          cwds TEXT NOT NULL DEFAULT '[]',
          rrule TEXT NOT NULL,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL,
          model TEXT,
          reasoning_effort TEXT,
          target_type TEXT,
          project_id TEXT
        );
        """
    )
    connection.execute(
        """
        INSERT INTO automations
        (id,name,prompt,status,next_run_at,last_run_at,cwds,rrule,created_at,updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?)
        """,
        (
            AUTOMATION_ID,
            AUTOMATION_NAME,
            prompt,
            "PAUSED",
            999_999,
            123_000,
            "[]",
            "RRULE:FREQ=MINUTELY;INTERVAL=5",
            100,
            100,
        ),
    )
    connection.commit()
    connection.close()


def write_operator_hold(path: Path, *, thread_id: str = TARGET_THREAD_ID) -> dict:
    payload = {
        "schema_version": OPERATOR_HOLD_SCHEMA_VERSION,
        "thread_id": thread_id,
        "state": "active",
        "authorized_by": "kolby",
        "hold_id": "a6b76ef0-0182-4b05-ab04-94e01a9b8c08",
        "created_at": "2026-07-16T12:00:00-04:00",
        "reason_sha256": hashlib.sha256(b"direct operator hold").hexdigest(),
    }
    path.write_text(json.dumps(payload, sort_keys=True))
    return payload


def write_semantic_plan(
    path: Path,
    project_root: Path,
    *,
    state: str = "executable",
    step_state: str = "executable",
    max_model_calls: int = 3,
    max_total_tokens: int = 400_000,
) -> dict:
    directive = project_root / "automation" / "semantic-directive.json"
    expected_receipt = project_root / "work" / "semantic-step-receipt.json"
    directive.parent.mkdir(parents=True, exist_ok=True)
    expected_receipt.parent.mkdir(parents=True, exist_ok=True)
    directive_payload = {
        "schema_version": "test_semantic_directive_v1",
        "thread_id": TARGET_THREAD_ID,
        "plan_epoch": 1,
        "step_id": "shared_reference_repair_v1",
        "expected_receipt_path": str(expected_receipt),
    }
    directive.write_text(json.dumps(directive_payload, sort_keys=True))
    payload = {
        "schema_version": SEMANTIC_PLAN_SCHEMA_VERSION,
        "thread_id": TARGET_THREAD_ID,
        "plan_epoch": 1,
        "state": state,
        "step": {
            "step_id": "shared_reference_repair_v1",
            "state": step_state,
            "max_model_calls": max_model_calls,
            "max_total_tokens": max_total_tokens,
            "expected_receipt_path": str(expected_receipt),
            "accepted_receipt_states": ["passed", "rejected", "waiting"],
            "directive_path": str(directive),
            "directive_sha256": hashlib.sha256(directive.read_bytes()).hexdigest(),
        },
    }
    path.write_text(json.dumps(payload, sort_keys=True))
    return payload


def write_semantic_receipt(path: Path, *, state: str = "passed") -> dict:
    payload = {
        "schema_version": SEMANTIC_STEP_RECEIPT_SCHEMA_VERSION,
        "thread_id": TARGET_THREAD_ID,
        "plan_epoch": 1,
        "step_id": "shared_reference_repair_v1",
        "state": state,
    }
    path.write_text(json.dumps(payload, sort_keys=True))
    return payload


def write_completion_authorization(path: Path, project_root: Path) -> dict:
    evidence: dict[str, dict[str, object]] = {}
    evidence_root = project_root / "work" / "completion-evidence"
    evidence_root.mkdir(parents=True, exist_ok=True)
    contracts = {
        "development_quality": (
            "pif_semantic_plan_step_receipt_v1",
            "epoch5_direct_reference_verify_receipt_v1",
        ),
        "untouched_holdout": (
            "pif_pipeline_evaluation_receipt_v2",
            "pipeline_evaluation_receipt_verify_v2",
        ),
        "production_app_server_cutover": (
            "pif_production_app_server_cutover_receipt_v1",
            "production_app_server_cutover_verify_v1",
        ),
        "notification_readiness": (
            "pif_completion_notification_readiness_receipt_v1",
            "codex_ops_telegram_readiness_verify_v1",
        ),
    }
    for criterion, (schema, verifier) in contracts.items():
        evidence_path = evidence_root / f"{criterion}.json"
        evidence_payload = {"schema_version": schema, "test_source": criterion}
        evidence_path.write_text(json.dumps(evidence_payload, sort_keys=True))
        evidence[criterion] = {
            "path": str(evidence_path.resolve()),
            "sha256": hashlib.sha256(evidence_path.read_bytes()).hexdigest(),
            "size_bytes": evidence_path.stat().st_size,
            "schema_version": schema,
            "verifier": verifier,
        }
    cutover_path = Path(evidence["production_app_server_cutover"]["path"])
    cutover_payload = {
        "schema_version": "pif_production_app_server_cutover_receipt_v1",
        "records": {
            "evaluation_receipt": {
                key: evidence["untouched_holdout"][key]
                for key in ("path", "sha256", "size_bytes")
            }
        },
    }
    cutover_path.write_text(json.dumps(cutover_payload, sort_keys=True))
    evidence["production_app_server_cutover"]["sha256"] = hashlib.sha256(
        cutover_path.read_bytes()
    ).hexdigest()
    evidence["production_app_server_cutover"]["size_bytes"] = (
        cutover_path.stat().st_size
    )
    backlog_path = evidence_root / "eligible_extraction_backlog_zero.json"
    upstream_records = {
        "evaluation_receipt": {
            key: evidence["untouched_holdout"][key]
            for key in ("path", "sha256", "size_bytes")
        },
        "production_cutover_receipt": {
            key: evidence["production_app_server_cutover"][key]
            for key in ("path", "sha256", "size_bytes")
        },
    }
    backlog_payload = {
        "schema_version": "pif_production_extraction_completion_manifest_v1",
        "thread_id": TARGET_THREAD_ID,
        "state": "completed",
        "all_app_server_usage_accounted": True,
        "all_outputs_validated": True,
        "no_unresolved_audit_failures": True,
        "terminal_quarantine_audited": True,
        "records": upstream_records,
    }
    backlog_path.write_text(json.dumps(backlog_payload, sort_keys=True))
    evidence["eligible_extraction_backlog_zero"] = {
        "path": str(backlog_path.resolve()),
        "sha256": hashlib.sha256(backlog_path.read_bytes()).hexdigest(),
        "size_bytes": backlog_path.stat().st_size,
        "schema_version": "pif_production_extraction_completion_manifest_v1",
        "verifier": "pipeline_completion_audit_sqlite_v1",
    }
    payload = {
        "schema_version": END_TO_END_COMPLETION_SCHEMA_VERSION,
        "thread_id": TARGET_THREAD_ID,
        "state": "authorized",
        "authorization_id": "366dc3da-509a-47df-b4b4-5e0ae65a18cf",
        "authorized_at": "2026-07-15T04:00:00Z",
        "evidence": evidence,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True))
    return payload


def fake_completion_source_verifier(
    criterion: str,
    source_path: Path,
    _project_root: Path,
) -> dict:
    raw = source_path.read_bytes()
    return {
        "criterion": criterion,
        "passed": True,
        "source_sha256": hashlib.sha256(raw).hexdigest(),
        "source_size_bytes": len(raw),
    }


def notification_command_result(
    authorization,
    *,
    delivered: bool = True,
) -> Mock:
    payload = {
        "item": {
            "id": "pif-native-goal-supervisor-20260715-040000",
            "source": "pif-native-goal-supervisor",
            "dedupe_key": "pif-evaluation-goal-complete",
            "summary": "Podcast evaluation goal completed; babysitter stopped",
            "severity": "low",
            "details": (
                "Development quality, untouched holdout, production cutover, and "
                "the eligible extraction backlog are verified complete. Both "
                "supervisors are being stopped. Completion authorization sha256 "
                f"{authorization.sha256}."
            ),
            "next_step": (
                "Review the completed evaluation and extraction handoff receipts."
            ),
        },
        "delivery": {
            "ok": delivered,
            "status": "sent" if delivered else "telegram_error",
            "transport": "openclaw-railway",
            "service": EXPECTED_COMPLETION_TELEGRAM_SERVICE,
            "channel": EXPECTED_COMPLETION_TELEGRAM_CHANNEL,
            "target_sha256": EXPECTED_COMPLETION_TELEGRAM_TARGET_SHA256,
            "account_id": EXPECTED_COMPLETION_TELEGRAM_ACCOUNT,
            "identity_file": str(
                Path.home() / ".ssh" / "railway_codex_agent_ed25519"
            ),
        },
    }
    return Mock(returncode=0, stdout=json.dumps(payload), stderr="")


def artifact_record(path: Path) -> dict[str, object]:
    raw = path.read_bytes()
    return {
        "path": str(path.resolve()),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "size_bytes": len(raw),
    }


def write_production_cutover_fixture(
    project_root: Path,
    evaluation_receipt: Path,
) -> tuple[Path, dict[str, Path]]:
    root = project_root / "work" / "cutover"
    root.mkdir(parents=True, exist_ok=True)
    cutover_id = "cutover-fixture-1"
    winner_system_id = "candidate-fixture-1"
    model = "gpt-5.6-sol"
    reasoning_effort = "low"
    batch_size = 5
    prompt = root / "winning-prompt.txt"
    prompt.write_text(
        "Extract every grounded discourse event from the supplied podcast segment "
        "and return only the frozen structured-output object."
    )
    output_schema = root / "winning-output-schema.json"
    output_schema_payload = {
        "type": "object",
        "additionalProperties": False,
        "required": ["schema_version", "segment_id", "events"],
        "properties": {
            "schema_version": {"type": "string", "enum": ["pif_fixture_output_v1"]},
            "segment_id": {"type": "string", "minLength": 1},
            "events": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["event_type", "evidence"],
                    "properties": {
                        "event_type": {"type": "string", "minLength": 1},
                        "evidence": {"type": "string", "minLength": 1},
                    },
                },
            },
        },
    }
    output_schema.write_text(json.dumps(output_schema_payload, sort_keys=True))
    prompt_sha256 = hashlib.sha256(prompt.read_bytes()).hexdigest()
    canonical_schema = json.dumps(
        output_schema_payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    output_schema_sha256 = hashlib.sha256(canonical_schema).hexdigest()
    frozen_configuration = root / "frozen-winner-configuration.json"
    frozen_configuration.write_text(
        json.dumps(
            {
                "schema_version": "pif_frozen_app_server_winner_configuration_v1",
                "winner_system_id": winner_system_id,
                "model": model,
                "reasoning_effort": reasoning_effort,
                "batch_size": batch_size,
                "transport": "official_codex_app_server_stdio_managed_chatgpt_auth",
                "auth_mode": "chatgpt",
                "persistent_transport": True,
                "structured_output": True,
                "winning_prompt": artifact_record(prompt),
                "prompt_sha256": prompt_sha256,
                "winning_output_schema": artifact_record(output_schema),
                "output_schema_sha256": output_schema_sha256,
            },
            sort_keys=True,
        )
    )
    frozen_configuration_sha256 = hashlib.sha256(
        frozen_configuration.read_bytes()
    ).hexdigest()
    evaluation_root = project_root / "work" / "app-server-development-v2"
    evaluation_root.mkdir(parents=True, exist_ok=True)
    development_freeze = evaluation_root / "development-freeze.json"
    development_freeze.write_text(
        json.dumps(
            {
                "artifact_role": "development_freeze",
                "development_frozen": True,
                "selection_frozen": True,
                "development_winner_frozen": True,
                "winner_system_id": winner_system_id,
                "frozen_configuration_sha256": frozen_configuration_sha256,
            },
            sort_keys=True,
        )
    )
    evaluation_receipt.write_text(
        json.dumps(
            {
                "schema_version": "pif_pipeline_evaluation_receipt_v2",
                "frozen_configuration_sha256": frozen_configuration_sha256,
                "artifact_hashes": {
                    "development_freeze": {
                        "path": development_freeze.name,
                        "sha256": hashlib.sha256(
                            development_freeze.read_bytes()
                        ).hexdigest(),
                    }
                },
            },
            sort_keys=True,
        )
    )
    runner = project_root / "research_factory" / "production_runner.py"
    runner.parent.mkdir(parents=True, exist_ok=True)
    runner.write_text(
        "from research_factory.codex_app_server import CodexAppServerClient\n\n"
        "async def extract_with_frozen_winner(client: CodexAppServerClient, "
        "thread, prompt, schema, sidecar, output):\n"
        "    return await client.run_turn(thread, effort='low', prompt=prompt, "
        "output_schema=schema, sidecar_path=sidecar, output_path=output, "
        "batch_size=5, thread_mode='same_thread')\n"
    )
    runtime = root / "runtime.json"
    runtime.write_text(
        json.dumps(
            {
                "schema_version": "pif_production_app_server_runtime_config_v1",
                "thread_id": TARGET_THREAD_ID,
                "cutover_id": cutover_id,
                "transport": "official_codex_app_server_stdio_managed_chatgpt_auth",
                "auth_mode": "chatgpt",
                "persistent_transport": True,
                "structured_output": True,
                "production_runner": artifact_record(runner),
                "evaluation_receipt": artifact_record(evaluation_receipt),
                "frozen_configuration": artifact_record(frozen_configuration),
                "frozen_configuration_sha256": frozen_configuration_sha256,
                "winning_prompt": artifact_record(prompt),
                "winning_output_schema": artifact_record(output_schema),
                "winner_system_id": winner_system_id,
                "model": model,
                "reasoning_effort": reasoning_effort,
                "batch_size": batch_size,
            },
            sort_keys=True,
        )
    )
    sidecars: list[Path] = []
    outputs: list[Path] = []
    turn_usage = [
        {
            "input_tokens": 100,
            "cached_input_tokens": 20,
            "output_tokens": 10,
            "reasoning_output_tokens": 3,
            "total_tokens": 110,
        },
        {
            "input_tokens": 80,
            "cached_input_tokens": 40,
            "output_tokens": 8,
            "reasoning_output_tokens": 2,
            "total_tokens": 88,
        },
    ]
    for index, usage_counts in enumerate(turn_usage, start=1):
        output = root / ("smoke-output.json" if index == 1 else "warm-output.json")
        output_payload = {
            "schema_version": "pif_fixture_output_v1",
            "segment_id": f"segment-{index}",
            "events": [
                {
                    "event_type": "claim",
                    "evidence": f"exact evidence {index}",
                }
            ],
        }
        raw_message = json.dumps(
            output_payload,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        output.write_text(raw_message + "\n")
        sidecar = root / (
            "smoke-sidecar.json" if index == 1 else "warm-sidecar.json"
        )
        sidecar.write_text(
            json.dumps(
                {
                    "schema_version": "pif_codex_app_server_turn_v2",
                    "state": "completed",
                    "status": "completed",
                    "transport": "stdio",
                    "auth_type": "chatgpt",
                    "thread_mode": "same_thread",
                    "thread_id": "production-thread-1",
                    "turn_id": f"production-turn-{index}",
                    "model": model,
                    "effort": reasoning_effort,
                    "batch_size": batch_size,
                    "base_instructions_sha256": prompt_sha256,
                    "base_instructions_bytes": len(prompt.read_bytes()),
                    "prompt_sha256": hashlib.sha256(
                        f"rendered private batch prompt {index}".encode()
                    ).hexdigest(),
                    "prompt_bytes": len(f"rendered private batch prompt {index}"),
                    "output_schema_sha256": output_schema_sha256,
                    "output_schema_bytes": len(canonical_schema),
                    "usage_complete": True,
                    "usage_status": "measured",
                    "usage": usage_counts,
                    "wall_elapsed_seconds": 1.25,
                    "output_path": str(output.resolve()),
                    "output_sha256": hashlib.sha256(raw_message.encode()).hexdigest(),
                },
                sort_keys=True,
            )
        )
        outputs.append(output)
        sidecars.append(sidecar)
    output = outputs[0]
    sidecar = sidecars[0]
    usage_counts = {
        field: sum(turn[field] for turn in turn_usage)
        for field in (
            "input_tokens",
            "cached_input_tokens",
            "output_tokens",
            "reasoning_output_tokens",
            "total_tokens",
        )
    }
    telemetry = root / "usage.json"
    telemetry.write_text(
        json.dumps(
            {
                "schema_version": "pif_production_app_server_usage_telemetry_v1",
                "thread_id": TARGET_THREAD_ID,
                "cutover_id": cutover_id,
                "transport": "official_codex_app_server_stdio_managed_chatgpt_auth",
                "auth_mode": "chatgpt",
                "persistent_transport": True,
                "structured_output": True,
                "accounting_complete": True,
                "cache_telemetry_complete": True,
                "measured_turn_count": 2,
                **usage_counts,
                "wall_time_seconds": 2.5,
                "turn_sidecars": [artifact_record(item) for item in sidecars],
                "frozen_configuration_sha256": frozen_configuration_sha256,
                "winner_system_id": winner_system_id,
                "model": model,
                "reasoning_effort": reasoning_effort,
                "batch_size": batch_size,
            },
            sort_keys=True,
        )
    )
    smoke = root / "smoke.json"
    smoke.write_text(
        json.dumps(
            {
                "schema_version": "pif_production_app_server_smoke_receipt_v1",
                "thread_id": TARGET_THREAD_ID,
                "cutover_id": cutover_id,
                "state": "passed",
                "transport": "official_codex_app_server_stdio_managed_chatgpt_auth",
                "auth_mode": "chatgpt",
                "runtime_config": artifact_record(runtime),
                "output": artifact_record(output),
                "sidecar": artifact_record(sidecar),
                "usage_telemetry": artifact_record(telemetry),
                "validated_output_count": 1,
                "production_mutated": True,
                "frozen_configuration_sha256": frozen_configuration_sha256,
                "winner_system_id": winner_system_id,
                "model": model,
                "reasoning_effort": reasoning_effort,
                "batch_size": batch_size,
                "prompt_sha256": prompt_sha256,
                "output_schema_sha256": output_schema_sha256,
            },
            sort_keys=True,
        )
    )
    receipt = root / "cutover.json"
    receipt.write_text(
        json.dumps(
            {
                "schema_version": "pif_production_app_server_cutover_receipt_v1",
                "thread_id": TARGET_THREAD_ID,
                "cutover_id": cutover_id,
                "state": "passed",
                "transport": "official_codex_app_server_stdio_managed_chatgpt_auth",
                "auth_mode": "chatgpt",
                "persistent_transport": True,
                "structured_output": True,
                "production_verified": True,
                "frozen_configuration_sha256": frozen_configuration_sha256,
                "winner_system_id": winner_system_id,
                "model": model,
                "reasoning_effort": reasoning_effort,
                "batch_size": batch_size,
                "records": {
                    "evaluation_receipt": artifact_record(evaluation_receipt),
                    "frozen_configuration": artifact_record(frozen_configuration),
                    "production_runner": artifact_record(runner),
                    "runtime_config": artifact_record(runtime),
                    "smoke_receipt": artifact_record(smoke),
                    "smoke_output": artifact_record(output),
                    "smoke_sidecar": artifact_record(sidecar),
                    "usage_telemetry": artifact_record(telemetry),
                    "winning_output_schema": artifact_record(output_schema),
                    "winning_prompt": artifact_record(prompt),
                },
            },
            sort_keys=True,
        )
    )
    return receipt, {
        "runtime": runtime,
        "smoke": smoke,
        "output": output,
        "sidecar": sidecar,
        "warm_sidecar": sidecars[1],
        "telemetry": telemetry,
        "frozen_configuration": frozen_configuration,
        "prompt": prompt,
        "output_schema": output_schema,
        "development_freeze": development_freeze,
    }


class NativeGoalSupervisorTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.rollout = self.root / "rollout.jsonl"
        self.toml = self.root / "automation.toml"
        self.canonical_toml = self.root / "canonical.toml"
        self.db = self.root / "codex-dev.db"
        self.receipts = self.root / "events.jsonl"
        self.lock = self.root / "supervisor.lock"
        self.operator_hold = self.root / "operator-hold.json"
        self.semantic_plan = self.root / "semantic-plan.json"
        self.project_root = self.root / "project"
        self.completion_authorization = (
            self.project_root
            / "work"
            / "pif-end-to-end-completion-authorization-v1.json"
        )
        self.completion_delivery_receipt = self.root / "completion-delivery.json"
        self.semantic_receipt = (
            self.project_root / "work" / "semantic-step-receipt.json"
        )
        self.binary = self.root / "codex"
        self.binary.touch()
        write_toml(self.toml)
        self.canonical_toml.write_text(self.toml.read_text())
        create_automation_db(self.db)
        write_semantic_plan(self.semantic_plan, self.project_root)
        self.config = SupervisorConfig(
            binary=self.binary,
            automation_toml=self.toml,
            canonical_automation_toml=self.canonical_toml,
            automations_db=self.db,
            rollout_path=self.rollout,
            lock_path=self.lock,
            receipt_path=self.receipts,
            operator_hold_path=self.operator_hold,
            semantic_plan_path=self.semantic_plan,
            completion_authorization_path=self.completion_authorization,
            completion_delivery_receipt_path=self.completion_delivery_receipt,
            project_root=self.project_root,
            fresh_activity_seconds=1_800,
            verify_timeout_seconds=0.1,
            verify_poll_seconds=0.001,
            rollout_tail_bytes=64 * 1024,
        )
        self.desktop_writer = WriterState(1, True, (5290,))

    def tearDown(self) -> None:
        self.temp.cleanup()

    def receipt_rows(self) -> list[dict]:
        return [json.loads(line) for line in self.receipts.read_text().splitlines()]

    def test_rollout_tail_distinguishes_fresh_stale_and_terminal(self) -> None:
        append_jsonl(
            self.rollout,
            [
                lifecycle("2026-07-15T02:00:00Z", "task_complete", "turn-old"),
                lifecycle("2026-07-15T03:55:00Z", "task_started", "turn-live"),
                record("2026-07-15T03:59:00Z", "event_msg", {"type": "token_count"}),
            ],
        )
        fresh = read_rollout_tail(self.rollout, now=NOW, fresh_activity_seconds=1_800)
        self.assertEqual(fresh.classification, "fresh_open")
        self.assertEqual(fresh.turn_id, "turn-live")

        stale = read_rollout_tail(
            self.rollout,
            now=NOW + dt.timedelta(hours=2),
            fresh_activity_seconds=1_800,
        )
        self.assertEqual(stale.classification, "stale_open")

        append_jsonl(
            self.rollout,
            [lifecycle("2026-07-15T06:01:00Z", "task_complete", "turn-live")],
        )
        terminal = read_rollout_tail(
            self.rollout,
            now=NOW + dt.timedelta(hours=2, minutes=2),
        )
        self.assertEqual(terminal.classification, "idle_terminal")

    def test_dispatch_evidence_requires_same_turn_for_all_four_signals(self) -> None:
        rows = [
            lifecycle("2026-07-15T04:00:01Z", "task_started", "turn-native"),
            context_message(
                "2026-07-15T04:00:02Z",
                "turn-native",
                '<codex_internal_context source="goal">active</codex_internal_context>',
            ),
            context_message(
                "2026-07-15T04:00:03Z",
                "turn-native",
                f"<heartbeat><automation_id>{AUTOMATION_ID}</automation_id></heartbeat>",
            ),
            progress_record("2026-07-15T04:00:04Z", "turn-native"),
        ]
        append_jsonl(self.rollout, rows)
        parsed, end = read_appended_records(self.rollout, 0)
        evidence = TriggerEvidence()
        for row in parsed:
            update_trigger_evidence(row, evidence)
        self.assertEqual(evidence.verified_turn_id(), "turn-native")
        self.assertEqual(end, self.rollout.stat().st_size)

        mismatched = TriggerEvidence()
        for row in rows[:-1]:
            update_trigger_evidence(row, mismatched)
        update_trigger_evidence(progress_record("2026-07-15T04:00:04Z", "other"), mismatched)
        self.assertIsNone(mismatched.verified_turn_id())

    async def test_verify_native_trigger_requires_desktop_writer(self) -> None:
        append_jsonl(
            self.rollout,
            [
                lifecycle("2026-07-15T04:00:01Z", "task_started", "turn-native"),
                context_message(
                    "2026-07-15T04:00:02Z",
                    "turn-native",
                    '<codex_internal_context source="goal">active</codex_internal_context>',
                ),
                context_message(
                    "2026-07-15T04:00:03Z",
                    "turn-native",
                    f"<heartbeat><automation_id>{AUTOMATION_ID}</automation_id></heartbeat>",
                ),
                progress_record("2026-07-15T04:00:04Z", "turn-native"),
            ],
        )
        verified = await verify_native_trigger(
            self.config,
            start_offset=0,
            writer_probe=lambda _path: self.desktop_writer,
        )
        self.assertEqual(verified["turn_id"], "turn-native")
        self.assertTrue(verified["desktop_owned"])
        self.assertTrue(verified["native_turn_started"])
        self.assertTrue(verified["dispatch_verified"])
        self.assertTrue(verified["dispatch_activity_verified"])
        self.assertFalse(verified["semantic_progress_verified"])
        self.assertNotIn("progress", verified)

    def test_operator_toml_is_exact_and_prompt_hash_pinned(self) -> None:
        prompt_hash = hashlib.sha256(b"operator prompt").hexdigest()
        with patch(
            "research_factory.app_server_thread_supervisor.AUTOMATION_PROMPT_SHA256",
            prompt_hash,
        ):
            parsed = validate_operator_automation(self.toml)
        self.assertEqual(parsed["target_thread_id"], TARGET_THREAD_ID)

        drifted = self.toml.read_text().replace('status = "ACTIVE"', 'status = "PAUSED"')
        self.toml.write_text(drifted)
        with self.assertRaisesRegex(Exception, "drifted at status"):
            validate_operator_automation(self.toml)

    def test_operator_hold_requires_exact_independent_marker(self) -> None:
        absent = read_operator_hold_marker(
            self.operator_hold,
            thread_id=TARGET_THREAD_ID,
        )
        self.assertEqual(absent.classification, "absent")
        self.assertFalse(absent.valid)

        write_operator_hold(self.operator_hold)
        valid = read_operator_hold_marker(
            self.operator_hold,
            thread_id=TARGET_THREAD_ID,
        )
        self.assertTrue(valid.valid)
        self.assertIsNotNone(valid.marker_sha256)
        self.assertIsNotNone(valid.hold_id_sha256)

        payload = json.loads(self.operator_hold.read_text())
        payload["extra"] = "not allowed"
        self.operator_hold.write_text(json.dumps(payload))
        invalid = read_operator_hold_marker(
            self.operator_hold,
            thread_id=TARGET_THREAD_ID,
        )
        self.assertEqual(invalid.classification, "invalid")
        self.assertFalse(invalid.valid)

    def test_semantic_plan_contract_is_strict_and_checksum_bound(self) -> None:
        plan = read_semantic_plan(
            self.semantic_plan,
            thread_id=TARGET_THREAD_ID,
            project_root=self.project_root,
        )
        self.assertEqual(plan.step_id, "shared_reference_repair_v1")
        self.assertEqual(plan.plan_epoch, 1)
        self.assertEqual(plan.max_model_calls, 3)
        self.assertEqual(plan.max_total_tokens, 400_000)
        self.assertEqual(plan.expected_receipt_path, self.semantic_receipt.resolve())

        payload = json.loads(self.semantic_plan.read_text())
        payload["unexpected"] = True
        self.semantic_plan.write_text(json.dumps(payload, sort_keys=True))
        with self.assertRaisesRegex(SupervisorError, "schema drifted"):
            read_semantic_plan(
                self.semantic_plan,
                thread_id=TARGET_THREAD_ID,
                project_root=self.project_root,
            )

    def test_each_terminal_step_receipt_is_a_continuation_checkpoint(self) -> None:
        plan = read_semantic_plan(
            self.semantic_plan,
            thread_id=TARGET_THREAD_ID,
            project_root=self.project_root,
        )
        for state in ("passed", "rejected", "waiting"):
            with self.subTest(state=state):
                write_semantic_receipt(self.semantic_receipt, state=state)
                gate_reason, receipt = evaluate_semantic_plan(plan)
                self.assertIsNone(gate_reason)
                self.assertIsNotNone(receipt)
                self.assertEqual(receipt.state, state)

    async def test_waiting_semantic_plan_inspects_goal_and_wakes_recovery_only(
        self,
    ) -> None:
        append_jsonl(
            self.rollout,
            [lifecycle("2026-07-15T03:00:00Z", "task_complete", "turn-old")],
        )
        payload = write_semantic_plan(
            self.semantic_plan,
            self.project_root,
            state="waiting",
        )
        goal_controller = AsyncMock(
            return_value={
                "thread_id": TARGET_THREAD_ID,
                "auth_mode": "chatgpt",
                "before_status": "blocked",
                "after_status": "active",
                "action": "resumed",
                "objective_preserved": True,
                "token_budget_preserved": True,
                "accounting_preserved": True,
            }
        )
        paused_controller = AsyncMock()
        verify = AsyncMock(
            return_value={
                "turn_id": "turn-native",
                "native_turn_started": True,
                "dispatch_verified": True,
                "goal_context": True,
                "heartbeat_context": True,
                "dispatch_activity_verified": True,
                "semantic_progress_verified": False,
                "writer_count": 1,
                "desktop_owned": True,
                "end_offset": 999,
            }
        )
        prompt_hash = hashlib.sha256(b"operator prompt").hexdigest()
        with (
            patch(
                "research_factory.app_server_thread_supervisor.AUTOMATION_PROMPT_SHA256",
                prompt_hash,
            ),
            patch(
                "research_factory.app_server_thread_supervisor.verify_native_trigger",
                verify,
            ),
        ):
            result = await run_supervisor_cycle(
                self.config,
                goal_controller=goal_controller,
                paused_goal_controller=paused_controller,
                writer_probe=lambda _path: self.desktop_writer,
                now=NOW,
            )

        self.assertTrue(result["ok"])
        self.assertEqual(
            result["outcome"], "semantic_plan_gate_recovery_trigger_verified"
        )
        self.assertEqual(result["semantic_plan_gate_reason"], "plan_waiting")
        self.assertEqual(
            result["semantic_plan_sha256"],
            hashlib.sha256(
                json.dumps(payload, sort_keys=True).encode()
            ).hexdigest(),
        )
        self.assertEqual(
            result["semantic_plan_step_id"], "shared_reference_repair_v1"
        )
        self.assertEqual(
            result["semantic_dispatch_mode"], "recovery_or_successor_plan_only"
        )
        self.assertFalse(result["current_semantic_call_authorized"])
        self.assertTrue(result["native_turn_started"])
        self.assertTrue(result["dispatch_verified"])
        self.assertFalse(result["semantic_progress_verified"])
        goal_controller.assert_awaited_once()
        paused_controller.assert_not_awaited()
        verify.assert_awaited_once()

    async def test_exhausted_semantic_plan_wakes_successor_without_semantic_call(
        self,
    ) -> None:
        append_jsonl(
            self.rollout,
            [lifecycle("2026-07-15T03:00:00Z", "task_complete", "turn-old")],
        )
        write_semantic_plan(
            self.semantic_plan,
            self.project_root,
            max_model_calls=0,
        )
        goal_controller = AsyncMock(
            return_value={
                "thread_id": TARGET_THREAD_ID,
                "auth_mode": "chatgpt",
                "before_status": "active",
                "after_status": "active",
                "action": "already_active",
                "objective_preserved": True,
                "token_budget_preserved": True,
                "accounting_preserved": True,
            }
        )
        verify = AsyncMock(
            return_value={
                "turn_id": "turn-native",
                "native_turn_started": True,
                "dispatch_verified": True,
                "goal_context": True,
                "heartbeat_context": True,
                "dispatch_activity_verified": True,
                "semantic_progress_verified": False,
                "writer_count": 1,
                "desktop_owned": True,
                "end_offset": 999,
            }
        )
        prompt_hash = hashlib.sha256(b"operator prompt").hexdigest()
        with (
            patch(
                "research_factory.app_server_thread_supervisor.AUTOMATION_PROMPT_SHA256",
                prompt_hash,
            ),
            patch(
                "research_factory.app_server_thread_supervisor.verify_native_trigger",
                verify,
            ),
        ):
            result = await run_supervisor_cycle(
                self.config,
                goal_controller=goal_controller,
                writer_probe=lambda _path: self.desktop_writer,
                now=NOW,
            )

        self.assertTrue(result["ok"])
        self.assertEqual(
            result["outcome"], "semantic_plan_gate_recovery_trigger_verified"
        )
        self.assertEqual(
            result["semantic_plan_gate_reason"], "max_model_calls_exhausted"
        )
        self.assertFalse(result["current_semantic_call_authorized"])
        goal_controller.assert_awaited_once()
        verify.assert_awaited_once()

    async def test_every_terminal_or_zero_cap_plan_gate_reaches_goal_preflight(
        self,
    ) -> None:
        append_jsonl(
            self.rollout,
            [lifecycle("2026-07-15T03:00:00Z", "task_complete", "turn-old")],
        )
        goal_controller = AsyncMock(
            return_value={
                "thread_id": TARGET_THREAD_ID,
                "auth_mode": "chatgpt",
                "before_status": "active",
                "after_status": "active",
                "action": "already_active",
                "objective_preserved": True,
                "token_budget_preserved": True,
                "accounting_preserved": True,
            }
        )
        verify = AsyncMock(
            return_value={
                "turn_id": "turn-native",
                "native_turn_started": True,
                "dispatch_verified": True,
                "goal_context": True,
                "heartbeat_context": True,
                "dispatch_activity_verified": True,
                "semantic_progress_verified": False,
                "writer_count": 1,
                "desktop_owned": True,
                "end_offset": 999,
            }
        )
        cases = (
            ({"state": "complete"}, "plan_complete"),
            ({"step_state": "waiting"}, "step_waiting"),
            ({"step_state": "complete"}, "step_complete"),
            ({"max_total_tokens": 0}, "max_total_tokens_exhausted"),
        )
        prompt_hash = hashlib.sha256(b"operator prompt").hexdigest()
        with (
            patch(
                "research_factory.app_server_thread_supervisor.AUTOMATION_PROMPT_SHA256",
                prompt_hash,
            ),
            patch(
                "research_factory.app_server_thread_supervisor.verify_native_trigger",
                verify,
            ),
        ):
            for arguments, reason in cases:
                with self.subTest(reason=reason):
                    write_semantic_plan(
                        self.semantic_plan,
                        self.project_root,
                        **arguments,
                    )
                    # A non-executable plan cannot authorize this old directive;
                    # recovery must not depend on or replay it.
                    directive = self.project_root / "automation" / "semantic-directive.json"
                    directive.write_text('{"intentionally":"stale"}')
                    result = await run_supervisor_cycle(
                        self.config,
                        goal_controller=goal_controller,
                        writer_probe=lambda _path: self.desktop_writer,
                        now=NOW,
                    )
                    self.assertEqual(
                        result["outcome"],
                        "semantic_plan_gate_recovery_trigger_verified",
                    )
                    self.assertEqual(result["semantic_plan_gate_reason"], reason)
                    self.assertFalse(result["current_semantic_call_authorized"])

        self.assertEqual(goal_controller.await_count, len(cases))
        self.assertEqual(verify.await_count, len(cases))

    async def test_accepted_step_receipt_resumes_goal_for_next_branch(self) -> None:
        append_jsonl(
            self.rollout,
            [lifecycle("2026-07-15T03:00:00Z", "task_complete", "turn-old")],
        )
        write_semantic_receipt(self.semantic_receipt, state="passed")
        goal_controller = AsyncMock(
            return_value={
                "thread_id": TARGET_THREAD_ID,
                "auth_mode": "chatgpt",
                "before_status": "blocked",
                "after_status": "active",
                "action": "resumed",
                "objective_preserved": True,
                "token_budget_preserved": True,
                "accounting_preserved": True,
            }
        )
        verify = AsyncMock(
            return_value={
                "turn_id": "turn-native",
                "native_turn_started": True,
                "dispatch_verified": True,
                "goal_context": True,
                "heartbeat_context": True,
                "dispatch_activity_verified": True,
                "semantic_progress_verified": False,
                "writer_count": 1,
                "desktop_owned": True,
                "end_offset": 999,
            }
        )
        prompt_hash = hashlib.sha256(b"operator prompt").hexdigest()
        with (
            patch(
                "research_factory.app_server_thread_supervisor.AUTOMATION_PROMPT_SHA256",
                prompt_hash,
            ),
            patch(
                "research_factory.app_server_thread_supervisor.verify_native_trigger",
                verify,
            ),
        ):
            result = await run_supervisor_cycle(
                self.config,
                goal_controller=goal_controller,
                writer_probe=lambda _path: self.desktop_writer,
                now=NOW,
            )

        self.assertTrue(result["ok"])
        self.assertEqual(
            result["outcome"],
            "semantic_step_receipt_successor_trigger_verified",
        )
        self.assertIsNone(result["semantic_plan_gate_reason"])
        self.assertEqual(result["semantic_step_receipt_state"], "passed")
        self.assertEqual(result["semantic_dispatch_mode"], "receipt_branch_only")
        self.assertFalse(result["current_semantic_call_authorized"])
        self.assertFalse(result["semantic_progress_verified"])
        goal_controller.assert_awaited_once()
        verify.assert_awaited_once()

    async def test_directive_checksum_drift_fails_before_goal_mutation(self) -> None:
        append_jsonl(
            self.rollout,
            [lifecycle("2026-07-15T03:00:00Z", "task_complete", "turn-old")],
        )
        plan = read_semantic_plan(
            self.semantic_plan,
            thread_id=TARGET_THREAD_ID,
            project_root=self.project_root,
        )
        plan.directive_path.write_text('{"drifted":true}')
        goal_controller = AsyncMock()

        result = await run_supervisor_cycle(
            self.config,
            goal_controller=goal_controller,
            writer_probe=lambda _path: self.desktop_writer,
            now=NOW,
        )

        self.assertFalse(result["ok"])
        self.assertEqual(result["outcome"], "supervisor_failed")
        self.assertEqual(result["failure_stage"], "semantic_plan_preflight")
        self.assertEqual(result["error_class"], "SupervisorError")
        self.assertEqual(
            result["semantic_plan_step_id"], "shared_reference_repair_v1"
        )
        self.assertEqual(
            result["semantic_plan_sha256"],
            hashlib.sha256(self.semantic_plan.read_bytes()).hexdigest(),
        )
        goal_controller.assert_not_awaited()

    def test_native_database_update_is_one_exact_transaction(self) -> None:
        automation = write_toml(self.toml)
        result = force_native_automation_due(self.db, automation, now_ms=500_000)
        self.assertEqual(result["before_status"], "PAUSED")
        self.assertEqual(result["after_status"], "ACTIVE")
        self.assertEqual(result["after_rrule"], AUTOMATION_RRULE)

        connection = sqlite3.connect(self.db)
        row = connection.execute(
            "SELECT status,rrule,next_run_at,last_run_at FROM automations WHERE id=?",
            (AUTOMATION_ID,),
        ).fetchone()
        connection.close()
        self.assertEqual(row, ("ACTIVE", AUTOMATION_RRULE, 499_000, 123_000))

    def test_completion_authorization_is_strict_hash_bound_and_local(self) -> None:
        payload = write_completion_authorization(
            self.completion_authorization,
            self.project_root,
        )
        valid = inspect_completion_authorization(
            self.completion_authorization,
            thread_id=TARGET_THREAD_ID,
            project_root=self.project_root,
            source_verifier=fake_completion_source_verifier,
        )
        self.assertTrue(valid.valid)
        self.assertEqual(len(valid.authorization.evidence_sha256), 5)

        development = Path(payload["evidence"]["development_quality"]["path"])
        development.write_text(development.read_text() + "\n")
        tampered = inspect_completion_authorization(
            self.completion_authorization,
            thread_id=TARGET_THREAD_ID,
            project_root=self.project_root,
            source_verifier=fake_completion_source_verifier,
        )
        self.assertEqual(tampered.classification, "invalid")
        self.assertIn("checksum", tampered.reason)

        payload = write_completion_authorization(
            self.completion_authorization,
            self.project_root,
        )
        outside = self.root / "outside.json"
        outside.write_text("{}")
        payload["evidence"]["development_quality"].update(
            {
                "path": str(outside.resolve()),
                "sha256": hashlib.sha256(outside.read_bytes()).hexdigest(),
                "size_bytes": outside.stat().st_size,
            }
        )
        self.completion_authorization.write_text(
            json.dumps(payload, sort_keys=True)
        )
        escaped = inspect_completion_authorization(
            self.completion_authorization,
            thread_id=TARGET_THREAD_ID,
            project_root=self.project_root,
            source_verifier=fake_completion_source_verifier,
        )
        self.assertEqual(escaped.classification, "invalid")
        self.assertIn("inside", escaped.reason)

    def test_notification_readiness_binds_exact_completion_destination(self) -> None:
        codex_home = self.root / "codex-home"
        codex_ops = codex_home / "bin" / "codex-ops"
        telegram_config = codex_home / "ops" / "config.json"
        repo = self.root / "railway-repo"
        identity = self.root / "railway-identity"
        readiness = self.project_root / "work" / "notification-readiness.json"
        codex_ops.parent.mkdir(parents=True)
        telegram_config.parent.mkdir(parents=True)
        repo.mkdir(parents=True)
        readiness.parent.mkdir(parents=True, exist_ok=True)
        codex_ops.write_text("#!/bin/sh\nexit 0\n")
        codex_ops.chmod(0o700)
        identity.write_text("fixture identity\n")
        target = "fixture-completion-target"
        expected_target_sha256 = hashlib.sha256(target.encode()).hexdigest()
        base_telegram = {
            "enabled": True,
            "transport": "openclaw-railway",
            "repo_path": str(repo),
            "service": EXPECTED_COMPLETION_TELEGRAM_SERVICE,
            "identity_file": str(identity),
            "channel": EXPECTED_COMPLETION_TELEGRAM_CHANNEL,
            "target": target,
            "account_id": EXPECTED_COMPLETION_TELEGRAM_ACCOUNT,
            "source_allowlist": ["pif-native-goal-supervisor"],
        }

        def write_readiness(telegram: dict) -> None:
            telegram_config.write_text(
                json.dumps({"telegram": telegram}, sort_keys=True)
            )
            readiness.write_text(
                json.dumps(
                    {
                        "schema_version": "pif_completion_notification_readiness_receipt_v1",
                        "thread_id": TARGET_THREAD_ID,
                        "state": "ready",
                        "dedupe_key": "pif-evaluation-goal-complete",
                        "codex_ops": artifact_record(codex_ops),
                        "telegram_config": artifact_record(telegram_config),
                    },
                    sort_keys=True,
                )
            )

        with (
            patch(
                "research_factory.app_server_thread_supervisor.CODEX_HOME",
                codex_home,
            ),
            patch(
                "research_factory.app_server_thread_supervisor.DEFAULT_CODEX_OPS",
                codex_ops,
            ),
            patch(
                "research_factory.app_server_thread_supervisor.DEFAULT_CODEX_OPS_CONFIG",
                telegram_config,
            ),
            patch(
                "research_factory.app_server_thread_supervisor.EXPECTED_COMPLETION_TELEGRAM_REPO",
                repo,
            ),
            patch(
                "research_factory.app_server_thread_supervisor.EXPECTED_COMPLETION_TELEGRAM_IDENTITY",
                identity,
            ),
            patch(
                "research_factory.app_server_thread_supervisor.EXPECTED_COMPLETION_TELEGRAM_TARGET_SHA256",
                expected_target_sha256,
            ),
        ):
            write_readiness(dict(base_telegram))
            self.assertTrue(
                _verify_notification_readiness_source(
                    readiness, self.project_root
                )["passed"]
            )
            mutations = (
                ("repo_path", str(self.root / "wrong-repo")),
                ("service", "wrong-service"),
                ("identity_file", str(self.root / "wrong-identity")),
                ("channel", "wrong-channel"),
                ("target", "wrong-target"),
                ("account_id", "wrong-account"),
                ("source_allowlist", ["unrelated-watchdog"]),
            )
            for key, bad_value in mutations:
                drifted = dict(base_telegram)
                drifted[key] = bad_value
                write_readiness(drifted)
                with self.assertRaisesRegex(
                    SupervisorError, "Telegram is not configured"
                ):
                    _verify_notification_readiness_source(
                        readiness, self.project_root
                    )

    def test_production_cutover_verifier_checks_real_sidecars_and_newline_hash(
        self,
    ) -> None:
        project = self.root / "cutover-pass-project"
        evaluation = project / "work" / "evaluation.json"
        evaluation.parent.mkdir(parents=True, exist_ok=True)
        evaluation.write_text('{"schema_version":"pif_pipeline_evaluation_receipt_v2"}')
        receipt, _paths = write_production_cutover_fixture(project, evaluation)
        verified = _verify_production_cutover_source(receipt, project)
        self.assertTrue(verified["passed"])
        self.assertEqual(
            verified["source_sha256"],
            hashlib.sha256(receipt.read_bytes()).hexdigest(),
        )

    def test_production_cutover_verifier_rejects_telemetry_and_lineage_drift(
        self,
    ) -> None:
        telemetry_project = self.root / "cutover-telemetry-project"
        evaluation = telemetry_project / "work" / "evaluation.json"
        evaluation.parent.mkdir(parents=True, exist_ok=True)
        evaluation.write_text('{"schema_version":"pif_pipeline_evaluation_receipt_v2"}')
        receipt, paths = write_production_cutover_fixture(
            telemetry_project,
            evaluation,
        )
        telemetry = json.loads(paths["telemetry"].read_text())
        telemetry["total_tokens"] = 111
        paths["telemetry"].write_text(json.dumps(telemetry, sort_keys=True))
        smoke = json.loads(paths["smoke"].read_text())
        smoke["usage_telemetry"] = artifact_record(paths["telemetry"])
        paths["smoke"].write_text(json.dumps(smoke, sort_keys=True))
        cutover = json.loads(receipt.read_text())
        cutover["records"]["usage_telemetry"] = artifact_record(paths["telemetry"])
        cutover["records"]["smoke_receipt"] = artifact_record(paths["smoke"])
        receipt.write_text(json.dumps(cutover, sort_keys=True))
        with self.assertRaisesRegex(SupervisorError, "usage telemetry"):
            _verify_production_cutover_source(receipt, telemetry_project)

        lineage_project = self.root / "cutover-lineage-project"
        evaluation = lineage_project / "work" / "evaluation.json"
        evaluation.parent.mkdir(parents=True, exist_ok=True)
        evaluation.write_text('{"schema_version":"pif_pipeline_evaluation_receipt_v2"}')
        receipt, paths = write_production_cutover_fixture(lineage_project, evaluation)
        other_evaluation = lineage_project / "work" / "other-evaluation.json"
        other_evaluation.write_text('{"different":true}')
        cutover = json.loads(receipt.read_text())
        cutover["records"]["evaluation_receipt"] = artifact_record(other_evaluation)
        receipt.write_text(json.dumps(cutover, sort_keys=True))
        with self.assertRaisesRegex(SupervisorError, "evaluation frozen configuration"):
            _verify_production_cutover_source(receipt, lineage_project)

        output_project = self.root / "cutover-output-project"
        evaluation = output_project / "work" / "evaluation.json"
        evaluation.parent.mkdir(parents=True, exist_ok=True)
        evaluation.write_text('{"schema_version":"pif_pipeline_evaluation_receipt_v2"}')
        receipt, paths = write_production_cutover_fixture(output_project, evaluation)
        unrelated = output_project / "work" / "cutover" / "unrelated-output.json"
        unrelated.write_text('{"unrelated":true}\n')
        smoke = json.loads(paths["smoke"].read_text())
        smoke["output"] = artifact_record(unrelated)
        paths["smoke"].write_text(json.dumps(smoke, sort_keys=True))
        cutover = json.loads(receipt.read_text())
        cutover["records"]["smoke_output"] = artifact_record(unrelated)
        cutover["records"]["smoke_receipt"] = artifact_record(paths["smoke"])
        receipt.write_text(json.dumps(cutover, sort_keys=True))
        with self.assertRaisesRegex(SupervisorError, "smoke output lineage"):
            _verify_production_cutover_source(receipt, output_project)

        schema_project = self.root / "cutover-schema-project"
        evaluation = schema_project / "work" / "evaluation.json"
        evaluation.parent.mkdir(parents=True, exist_ok=True)
        receipt, paths = write_production_cutover_fixture(schema_project, evaluation)
        invalid_output = {
            "schema_version": "pif_fixture_output_v1",
            "segment_id": "segment-1",
            "events": "not-an-array",
        }
        invalid_raw = json.dumps(
            invalid_output,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        paths["output"].write_text(invalid_raw + "\n")
        sidecar = json.loads(paths["sidecar"].read_text())
        sidecar["output_sha256"] = hashlib.sha256(invalid_raw.encode()).hexdigest()
        paths["sidecar"].write_text(json.dumps(sidecar, sort_keys=True))
        telemetry = json.loads(paths["telemetry"].read_text())
        telemetry["turn_sidecars"][0] = artifact_record(paths["sidecar"])
        paths["telemetry"].write_text(json.dumps(telemetry, sort_keys=True))
        smoke = json.loads(paths["smoke"].read_text())
        smoke["output"] = artifact_record(paths["output"])
        smoke["sidecar"] = artifact_record(paths["sidecar"])
        smoke["usage_telemetry"] = artifact_record(paths["telemetry"])
        paths["smoke"].write_text(json.dumps(smoke, sort_keys=True))
        cutover = json.loads(receipt.read_text())
        cutover["records"]["smoke_output"] = artifact_record(paths["output"])
        cutover["records"]["smoke_sidecar"] = artifact_record(paths["sidecar"])
        cutover["records"]["usage_telemetry"] = artifact_record(paths["telemetry"])
        cutover["records"]["smoke_receipt"] = artifact_record(paths["smoke"])
        receipt.write_text(json.dumps(cutover, sort_keys=True))
        with self.assertRaisesRegex(SupervisorError, "frozen schema"):
            _verify_production_cutover_source(receipt, schema_project)

    def test_production_cutover_verifier_binds_frozen_winner_through_runtime(
        self,
    ) -> None:
        project = self.root / "cutover-frozen-winner-project"
        evaluation = project / "work" / "evaluation.json"
        evaluation.parent.mkdir(parents=True, exist_ok=True)
        receipt, paths = write_production_cutover_fixture(project, evaluation)

        frozen = json.loads(paths["frozen_configuration"].read_text())
        frozen["model"] = "unselected-model"
        paths["frozen_configuration"].write_text(json.dumps(frozen, sort_keys=True))
        cutover = json.loads(receipt.read_text())
        cutover["records"]["frozen_configuration"] = artifact_record(
            paths["frozen_configuration"]
        )
        cutover["frozen_configuration_sha256"] = hashlib.sha256(
            paths["frozen_configuration"].read_bytes()
        ).hexdigest()
        cutover["model"] = "unselected-model"
        receipt.write_text(json.dumps(cutover, sort_keys=True))
        with self.assertRaisesRegex(SupervisorError, "evaluation winner"):
            _verify_production_cutover_source(receipt, project)

        project = self.root / "cutover-sidecar-winner-project"
        evaluation = project / "work" / "evaluation.json"
        evaluation.parent.mkdir(parents=True, exist_ok=True)
        receipt, paths = write_production_cutover_fixture(project, evaluation)
        sidecar = json.loads(paths["warm_sidecar"].read_text())
        sidecar["model"] = "unselected-model"
        paths["warm_sidecar"].write_text(json.dumps(sidecar, sort_keys=True))
        telemetry = json.loads(paths["telemetry"].read_text())
        telemetry["turn_sidecars"][1] = artifact_record(paths["warm_sidecar"])
        paths["telemetry"].write_text(json.dumps(telemetry, sort_keys=True))
        smoke = json.loads(paths["smoke"].read_text())
        smoke["usage_telemetry"] = artifact_record(paths["telemetry"])
        paths["smoke"].write_text(json.dumps(smoke, sort_keys=True))
        cutover = json.loads(receipt.read_text())
        cutover["records"]["usage_telemetry"] = artifact_record(paths["telemetry"])
        cutover["records"]["smoke_receipt"] = artifact_record(paths["smoke"])
        receipt.write_text(json.dumps(cutover, sort_keys=True))
        with self.assertRaisesRegex(SupervisorError, "sidecar did not pass"):
            _verify_production_cutover_source(receipt, project)

    def test_native_automation_pause_is_exact_and_idempotent(self) -> None:
        connection = sqlite3.connect(self.db)
        connection.execute(
            "UPDATE automations SET status='ACTIVE' WHERE id=?",
            (AUTOMATION_ID,),
        )
        connection.commit()
        connection.close()

        prompt_hash = hashlib.sha256(b"operator prompt").hexdigest()
        with patch(
            "research_factory.app_server_thread_supervisor.AUTOMATION_PROMPT_SHA256",
            prompt_hash,
        ):
            first = pause_native_automation_row(
                self.db,
                automation_toml=self.toml,
                canonical_automation_toml=self.canonical_toml,
                now_ms=500_000,
            )
            second = pause_native_automation_row(
                self.db,
                automation_toml=self.toml,
                canonical_automation_toml=self.canonical_toml,
                now_ms=501_000,
            )
        self.assertEqual(first["before_status"], "ACTIVE")
        self.assertFalse(first["already_paused"])
        self.assertEqual(second["before_status"], "PAUSED")
        self.assertTrue(second["already_paused"])
        self.assertIn('status = "PAUSED"', self.toml.read_text())
        self.assertIn('status = "PAUSED"', self.canonical_toml.read_text())
        connection = sqlite3.connect(self.db)
        paused_row = connection.execute(
            "SELECT status,next_run_at FROM automations WHERE id=?",
            (AUTOMATION_ID,),
        ).fetchone()
        connection.close()
        self.assertEqual(paused_row, ("PAUSED", None))

    def test_completion_lifecycle_order_is_notify_pause_then_disable(self) -> None:
        write_completion_authorization(
            self.completion_authorization,
            self.project_root,
        )
        state = inspect_completion_authorization(
            self.completion_authorization,
            thread_id=TARGET_THREAD_ID,
            project_root=self.project_root,
            source_verifier=fake_completion_source_verifier,
        )
        self.assertTrue(state.valid)
        events: list[str] = []

        def run_command(command: list[str]) -> Mock:
            events.append("notify" if "notify" in command else "disable")
            return (
                notification_command_result(state.authorization)
                if "notify" in command
                else Mock(returncode=0, stdout="", stderr="")
            )

        def pause(_path: Path, **_kwargs: object) -> dict:
            events.append("pause")
            return {
                "before_status": "ACTIVE",
                "after_status": "PAUSED",
                "already_paused": False,
            }

        with (
            patch(
                "research_factory.app_server_thread_supervisor._run_completion_command",
                side_effect=run_command,
            ),
            patch(
                "research_factory.app_server_thread_supervisor.pause_native_automation_row",
                side_effect=pause,
            ),
        ):
            result = finalize_completed_supervision(
                state.authorization,
                automations_db=self.db,
                delivery_receipt_path=self.completion_delivery_receipt,
            )
        self.assertEqual(events, ["notify", "pause", "disable"])
        self.assertTrue(result["desktop_native_automation_paused"])

    def test_completion_delivery_rejects_wrong_endpoint_evidence(self) -> None:
        write_completion_authorization(
            self.completion_authorization,
            self.project_root,
        )
        state = inspect_completion_authorization(
            self.completion_authorization,
            thread_id=TARGET_THREAD_ID,
            project_root=self.project_root,
            source_verifier=fake_completion_source_verifier,
        )
        authorization = state.authorization
        self.assertIsNotNone(authorization)
        for field, bad_value in (
            ("transport", "wrong-transport"),
            ("service", "wrong-service"),
            ("channel", "wrong-channel"),
            ("target_sha256", "0" * 64),
            ("account_id", "wrong-account"),
            ("identity_file", str(self.root / "wrong-identity")),
        ):
            notification = notification_command_result(authorization)
            payload = json.loads(notification.stdout)
            payload["delivery"][field] = bad_value
            notification.stdout = json.dumps(payload)
            with self.assertRaisesRegex(SupervisorError, "Telegram delivery"):
                _verified_delivery_from_notification(
                    notification,
                    authorization,
                )

    def test_completion_lifecycle_failures_leave_a_retry_owner(self) -> None:
        write_completion_authorization(
            self.completion_authorization,
            self.project_root,
        )
        state = inspect_completion_authorization(
            self.completion_authorization,
            thread_id=TARGET_THREAD_ID,
            project_root=self.project_root,
            source_verifier=fake_completion_source_verifier,
        )
        pause = Mock(
            return_value={
                "before_status": "ACTIVE",
                "after_status": "PAUSED",
                "already_paused": False,
            }
        )
        with (
            patch(
                "research_factory.app_server_thread_supervisor._run_completion_command",
                return_value=Mock(returncode=1),
            ),
            patch(
                "research_factory.app_server_thread_supervisor.pause_native_automation_row",
                pause,
            ),
        ):
            with self.assertRaisesRegex(SupervisorError, "both supervisors remain"):
                finalize_completed_supervision(
                    state.authorization,
                    automations_db=self.db,
                    delivery_receipt_path=self.completion_delivery_receipt,
                )
        pause.assert_not_called()

        with (
            patch(
                "research_factory.app_server_thread_supervisor._run_completion_command",
                return_value=notification_command_result(
                    state.authorization,
                    delivered=False,
                ),
            ),
            patch(
                "research_factory.app_server_thread_supervisor.pause_native_automation_row",
                pause,
            ),
        ):
            with self.assertRaisesRegex(SupervisorError, "Telegram delivery"):
                finalize_completed_supervision(
                    state.authorization,
                    automations_db=self.db,
                    delivery_receipt_path=self.completion_delivery_receipt,
                )
        pause.assert_not_called()
        self.assertFalse(self.completion_delivery_receipt.exists())

        commands = [notification_command_result(state.authorization), Mock(returncode=1)]
        with (
            patch(
                "research_factory.app_server_thread_supervisor._run_completion_command",
                side_effect=commands,
            ),
            patch(
                "research_factory.app_server_thread_supervisor.pause_native_automation_row",
                pause,
            ),
        ):
            with self.assertRaisesRegex(SupervisorError, "external supervisor"):
                finalize_completed_supervision(
                    state.authorization,
                    automations_db=self.db,
                    delivery_receipt_path=self.completion_delivery_receipt,
                )
        pause.assert_called_once_with(
            self.db,
            automation_toml=DEFAULT_AUTOMATION_TOML,
            canonical_automation_toml=DEFAULT_CANONICAL_AUTOMATION_TOML,
        )
        self.assertTrue(self.completion_delivery_receipt.is_file())

        pause.reset_mock()
        disable = Mock(return_value=Mock(returncode=0, stdout="", stderr=""))
        with (
            patch(
                "research_factory.app_server_thread_supervisor._run_completion_command",
                disable,
            ),
            patch(
                "research_factory.app_server_thread_supervisor.pause_native_automation_row",
                pause,
            ),
        ):
            retried = finalize_completed_supervision(
                state.authorization,
                automations_db=self.db,
                delivery_receipt_path=self.completion_delivery_receipt,
            )
        self.assertTrue(retried["completion_delivery_receipt_reused"])
        pause.assert_called_once_with(
            self.db,
            automation_toml=DEFAULT_AUTOMATION_TOML,
            canonical_automation_toml=DEFAULT_CANONICAL_AUTOMATION_TOML,
        )
        disable.assert_called_once()
        self.assertIn("disable", disable.call_args.args[0])

    def test_post_send_crash_reconciles_durable_codex_ops_delivery(self) -> None:
        write_completion_authorization(
            self.completion_authorization,
            self.project_root,
        )
        state = inspect_completion_authorization(
            self.completion_authorization,
            thread_id=TARGET_THREAD_ID,
            project_root=self.project_root,
            source_verifier=fake_completion_source_verifier,
        )
        pause = Mock(
            return_value={
                "before_status": "ACTIVE",
                "after_status": "PAUSED",
                "already_paused": False,
            }
        )
        with (
            patch(
                "research_factory.app_server_thread_supervisor._run_completion_command",
                return_value=notification_command_result(state.authorization),
            ),
            patch(
                "research_factory.app_server_thread_supervisor._persist_completion_delivery_receipt",
                side_effect=SupervisorError("simulated post-send crash"),
            ),
            patch(
                "research_factory.app_server_thread_supervisor.pause_native_automation_row",
                pause,
            ),
        ):
            with self.assertRaisesRegex(SupervisorError, "post-send crash"):
                finalize_completed_supervision(
                    state.authorization,
                    automations_db=self.db,
                    delivery_receipt_path=self.completion_delivery_receipt,
                    codex_ops_state_path=self.root / "missing-state.json",
                    codex_ops_events_path=self.root / "missing-events.jsonl",
                )
        pause.assert_not_called()
        self.assertFalse(self.completion_delivery_receipt.exists())

        item_id = "pif-native-goal-supervisor-20260715-040000"
        details = (
            "Development quality, untouched holdout, production cutover, and the "
            "eligible extraction backlog are verified complete. Both supervisors "
            "are being stopped. Completion authorization sha256 "
            f"{state.authorization.sha256}."
        )
        ops_state = self.root / "ops-state.json"
        ops_events = self.root / "ops-events.jsonl"
        ops_state.write_text(
            json.dumps(
                {
                    "items": {
                        item_id: {
                            "id": item_id,
                            "source": "pif-native-goal-supervisor",
                            "dedupe_key": "pif-evaluation-goal-complete",
                            "summary": "Podcast evaluation goal completed; babysitter stopped",
                            "severity": "low",
                            "details": details,
                            "next_step": "Review the completed evaluation and extraction handoff receipts.",
                            "transport": {
                                "telegram": {
                                    "status": "sent",
                                    "updated_at": "2026-07-15T04:00:00Z",
                                    "transport": "openclaw-railway",
                                    "service": EXPECTED_COMPLETION_TELEGRAM_SERVICE,
                                    "channel": EXPECTED_COMPLETION_TELEGRAM_CHANNEL,
                                    "target_sha256": EXPECTED_COMPLETION_TELEGRAM_TARGET_SHA256,
                                    "account_id": EXPECTED_COMPLETION_TELEGRAM_ACCOUNT,
                                    "identity_file": str(
                                        EXPECTED_COMPLETION_TELEGRAM_IDENTITY.resolve()
                                    ),
                                }
                            },
                        }
                    },
                    "dedupe": {
                        "pif-evaluation-goal-complete": {
                            "item_id": item_id,
                            "last_notified_at": "2026-07-15T04:00:00Z",
                        }
                    },
                },
                sort_keys=True,
            )
        )
        ops_events.write_text(
            json.dumps(
                {
                    "type": "notify",
                    "item_id": item_id,
                    "dedupe_key": "pif-evaluation-goal-complete",
                    "source": "pif-native-goal-supervisor",
                    "severity": "low",
                    "summary": "Podcast evaluation goal completed; babysitter stopped",
                    "telegram_status": "sent",
                    "telegram_transport": "openclaw-railway",
                    "telegram_service": EXPECTED_COMPLETION_TELEGRAM_SERVICE,
                    "telegram_channel": EXPECTED_COMPLETION_TELEGRAM_CHANNEL,
                    "telegram_target_sha256": EXPECTED_COMPLETION_TELEGRAM_TARGET_SHA256,
                    "telegram_account_id": EXPECTED_COMPLETION_TELEGRAM_ACCOUNT,
                    "telegram_identity_file": str(
                        EXPECTED_COMPLETION_TELEGRAM_IDENTITY.resolve()
                    ),
                    "at": "2026-07-15T04:00:00Z",
                },
                sort_keys=True,
            )
            + "\n"
        )
        state_payload = json.loads(ops_state.read_text())
        event_payload = json.loads(ops_events.read_text())
        endpoint_mutations = {
            "transport": "wrong-transport",
            "service": "wrong-service",
            "channel": "wrong-channel",
            "target_sha256": "0" * 64,
            "account_id": "wrong-account",
            "identity_file": "/tmp/wrong-identity",
        }
        for field, drifted in endpoint_mutations.items():
            mutated_state = json.loads(json.dumps(state_payload))
            mutated_state["items"][item_id]["transport"]["telegram"][field] = drifted
            ops_state.write_text(json.dumps(mutated_state, sort_keys=True))
            self.assertIsNone(
                _reconcile_codex_ops_delivery(
                    state.authorization,
                    state_path=ops_state,
                    events_path=ops_events,
                ),
                f"state endpoint drift was accepted for {field}",
            )
            ops_state.write_text(json.dumps(state_payload, sort_keys=True))

            mutated_event = json.loads(json.dumps(event_payload))
            mutated_event[f"telegram_{field}"] = drifted
            ops_events.write_text(json.dumps(mutated_event, sort_keys=True) + "\n")
            self.assertIsNone(
                _reconcile_codex_ops_delivery(
                    state.authorization,
                    state_path=ops_state,
                    events_path=ops_events,
                ),
                f"event endpoint drift was accepted for {field}",
            )
            ops_events.write_text(json.dumps(event_payload, sort_keys=True) + "\n")
        disable = Mock(return_value=Mock(returncode=0, stdout="", stderr=""))
        with (
            patch(
                "research_factory.app_server_thread_supervisor._run_completion_command",
                disable,
            ),
            patch(
                "research_factory.app_server_thread_supervisor.pause_native_automation_row",
                pause,
            ),
        ):
            result = finalize_completed_supervision(
                state.authorization,
                automations_db=self.db,
                delivery_receipt_path=self.completion_delivery_receipt,
                codex_ops_state_path=ops_state,
                codex_ops_events_path=ops_events,
            )
        self.assertTrue(result["completion_delivery_receipt_reused"])
        self.assertTrue(self.completion_delivery_receipt.is_file())
        disable.assert_called_once()
        self.assertIn("disable", disable.call_args.args[0])

    def test_operator_automation_and_database_drift_are_restored(self) -> None:
        self.toml.write_text(
            self.toml.read_text()
            .replace('status = "ACTIVE"', 'status = "PAUSED"')
            .replace(AUTOMATION_RRULE, "RRULE:FREQ=MINUTELY;INTERVAL=5")
        )
        prompt_hash = hashlib.sha256(b"operator prompt").hexdigest()
        with patch(
            "research_factory.app_server_thread_supervisor.AUTOMATION_PROMPT_SHA256",
            prompt_hash,
        ):
            automation, repaired = restore_operator_automation(
                self.toml,
                self.canonical_toml,
            )
        self.assertTrue(repaired)
        self.assertEqual(automation["status"], "ACTIVE")
        result = reconcile_native_automation_row(
            self.db,
            automation,
            force_due=False,
            now_ms=500_000,
        )
        self.assertEqual(result["before_status"], "PAUSED")
        self.assertEqual(result["after_status"], "ACTIVE")
        self.assertFalse(result["forced_due"])
        connection = sqlite3.connect(self.db)
        row = connection.execute(
            "SELECT status,rrule,next_run_at FROM automations WHERE id=?",
            (AUTOMATION_ID,),
        ).fetchone()
        connection.close()
        self.assertEqual(row, ("ACTIVE", AUTOMATION_RRULE, 999_999))

    async def test_valid_operator_hold_respects_paused_goal_without_mutation(self) -> None:
        append_jsonl(
            self.rollout,
            [lifecycle("2026-07-15T03:00:00Z", "task_complete", "turn-old")],
        )
        write_semantic_plan(
            self.semantic_plan,
            self.project_root,
            state="waiting",
        )
        write_operator_hold(self.operator_hold)
        blocked_controller = AsyncMock()
        paused_controller = AsyncMock()
        goal_reader = AsyncMock(
            return_value={
                "thread_id": TARGET_THREAD_ID,
                "auth_mode": "chatgpt",
                "before_status": "paused",
                "after_status": "paused",
                "action": "inspected",
                "objective_preserved": True,
                "token_budget_preserved": True,
                "accounting_preserved": True,
            }
        )

        result = await run_supervisor_cycle(
            self.config,
            goal_controller=blocked_controller,
            paused_goal_controller=paused_controller,
            goal_reader=goal_reader,
            writer_probe=lambda _path: self.desktop_writer,
            now=NOW,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["outcome"], "intentional_operator_hold_noop")
        self.assertTrue(result["operator_hold_valid"])
        self.assertEqual(result["semantic_plan_gate_reason"], "plan_waiting")
        self.assertEqual(result["goal_after_status"], "paused")
        blocked_controller.assert_not_awaited()
        paused_controller.assert_not_awaited()
        goal_reader.assert_awaited_once()
        connection = sqlite3.connect(self.db)
        status = connection.execute(
            "SELECT status FROM automations WHERE id=?", (AUTOMATION_ID,)
        ).fetchone()[0]
        connection.close()
        self.assertEqual(status, "PAUSED")

    async def test_valid_operator_hold_suppresses_verified_completion_lifecycle(
        self,
    ) -> None:
        append_jsonl(
            self.rollout,
            [lifecycle("2026-07-15T03:00:00Z", "task_complete", "turn-old")],
        )
        write_operator_hold(self.operator_hold)
        write_completion_authorization(
            self.completion_authorization,
            self.project_root,
        )
        goal_reader = AsyncMock(
            return_value={
                "thread_id": TARGET_THREAD_ID,
                "auth_mode": "chatgpt",
                "before_status": "complete",
                "after_status": "complete",
                "action": "inspected",
                "objective_preserved": True,
                "token_budget_preserved": True,
                "accounting_preserved": True,
            }
        )
        goal_controller = AsyncMock()
        paused_controller = AsyncMock()
        completion_controller = Mock()
        result = await run_supervisor_cycle(
            self.config,
            goal_controller=goal_controller,
            paused_goal_controller=paused_controller,
            goal_reader=goal_reader,
            completion_controller=completion_controller,
            completion_source_verifier=fake_completion_source_verifier,
            writer_probe=lambda _path: self.desktop_writer,
            now=NOW,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["outcome"], "intentional_operator_hold_noop")
        self.assertEqual(result["goal_after_status"], "complete")
        goal_reader.assert_awaited_once()
        goal_controller.assert_not_awaited()
        paused_controller.assert_not_awaited()
        completion_controller.assert_not_called()
        connection = sqlite3.connect(self.db)
        status = connection.execute(
            "SELECT status FROM automations WHERE id=?",
            (AUTOMATION_ID,),
        ).fetchone()[0]
        connection.close()
        self.assertEqual(status, "PAUSED")

    async def test_accidental_pause_recovers_and_requires_new_native_trigger(self) -> None:
        append_jsonl(
            self.rollout,
            [lifecycle("2026-07-15T03:00:00Z", "task_complete", "turn-old")],
        )
        expected_start_offset = self.rollout.stat().st_size
        blocked_controller = AsyncMock(
            return_value={
                "thread_id": TARGET_THREAD_ID,
                "auth_mode": "chatgpt",
                "before_status": "paused",
                "after_status": "paused",
                "action": "not_resumable",
                "objective_preserved": True,
                "token_budget_preserved": True,
                "accounting_preserved": True,
            }
        )
        paused_controller = AsyncMock(
            return_value={
                "thread_id": TARGET_THREAD_ID,
                "auth_mode": "chatgpt",
                "before_status": "paused",
                "after_status": "active",
                "action": "resumed_paused",
                "objective_preserved": True,
                "token_budget_preserved": True,
                "accounting_preserved": True,
            }
        )
        verify = AsyncMock(
            return_value={
                "turn_id": "turn-native-after-pause",
                "native_turn_started": True,
                "dispatch_verified": True,
                "goal_context": True,
                "heartbeat_context": True,
                "dispatch_activity_verified": True,
                "semantic_progress_verified": False,
                "writer_count": 1,
                "desktop_owned": True,
                "end_offset": expected_start_offset + 1,
            }
        )
        prompt_hash = hashlib.sha256(b"operator prompt").hexdigest()
        with (
            patch(
                "research_factory.app_server_thread_supervisor.AUTOMATION_PROMPT_SHA256",
                prompt_hash,
            ),
            patch(
                "research_factory.app_server_thread_supervisor.verify_native_trigger",
                verify,
            ),
        ):
            result = await run_supervisor_cycle(
                self.config,
                goal_controller=blocked_controller,
                paused_goal_controller=paused_controller,
                goal_reader=AsyncMock(),
                writer_probe=lambda _path: self.desktop_writer,
                now=NOW,
            )

        self.assertTrue(result["ok"])
        self.assertEqual(
            result["outcome"],
            "accidental_pause_recovered_native_trigger_verified",
        )
        self.assertTrue(result["accidental_pause_recovered"])
        self.assertEqual(result["operator_hold_status"], "absent")
        self.assertEqual(result["goal_after_status"], "active")
        paused_controller.assert_awaited_once()
        self.assertEqual(verify.await_args.kwargs["start_offset"], expected_start_offset)

    async def test_malformed_hold_cannot_authorize_a_pause(self) -> None:
        append_jsonl(
            self.rollout,
            [lifecycle("2026-07-15T03:00:00Z", "task_complete", "turn-old")],
        )
        self.operator_hold.write_text('{"state":"active"}')
        blocked_controller = AsyncMock(
            return_value={
                "thread_id": TARGET_THREAD_ID,
                "auth_mode": "chatgpt",
                "before_status": "paused",
                "after_status": "paused",
                "action": "not_resumable",
                "objective_preserved": True,
                "token_budget_preserved": True,
                "accounting_preserved": True,
            }
        )
        paused_controller = AsyncMock(
            return_value={
                "thread_id": TARGET_THREAD_ID,
                "auth_mode": "chatgpt",
                "before_status": "paused",
                "after_status": "active",
                "action": "resumed_paused",
                "objective_preserved": True,
                "token_budget_preserved": True,
                "accounting_preserved": True,
            }
        )
        prompt_hash = hashlib.sha256(b"operator prompt").hexdigest()
        with (
            patch(
                "research_factory.app_server_thread_supervisor.AUTOMATION_PROMPT_SHA256",
                prompt_hash,
            ),
            patch(
                "research_factory.app_server_thread_supervisor.verify_native_trigger",
                AsyncMock(
                    return_value={
                        "turn_id": "turn-native",
                        "native_turn_started": True,
                        "dispatch_verified": True,
                        "goal_context": True,
                        "heartbeat_context": True,
                        "dispatch_activity_verified": True,
                        "semantic_progress_verified": False,
                        "writer_count": 1,
                        "desktop_owned": True,
                        "end_offset": 999,
                    }
                ),
            ),
        ):
            result = await run_supervisor_cycle(
                self.config,
                goal_controller=blocked_controller,
                paused_goal_controller=paused_controller,
                writer_probe=lambda _path: self.desktop_writer,
                now=NOW,
            )
        self.assertTrue(result["accidental_pause_recovered"])
        self.assertEqual(result["operator_hold_status"], "invalid")
        paused_controller.assert_awaited_once()

    async def test_fresh_open_target_preflights_goal_but_never_forces_due(self) -> None:
        append_jsonl(
            self.rollout,
            [
                lifecycle("2026-07-15T03:55:00Z", "task_started", "turn-live"),
                record("2026-07-15T03:59:00Z", "event_msg", {"type": "token_count"}),
            ],
        )
        goal_controller = AsyncMock(
            return_value={
                "thread_id": TARGET_THREAD_ID,
                "auth_mode": "chatgpt",
                "before_status": "blocked",
                "after_status": "active",
                "action": "resumed",
                "objective_preserved": True,
                "token_budget_preserved": True,
                "accounting_preserved": True,
            }
        )
        prompt_hash = hashlib.sha256(b"operator prompt").hexdigest()
        with patch(
            "research_factory.app_server_thread_supervisor.AUTOMATION_PROMPT_SHA256",
            prompt_hash,
        ):
            result = await run_supervisor_cycle(
                self.config,
                goal_controller=goal_controller,
                writer_probe=lambda _path: self.desktop_writer,
                now=NOW,
            )
        self.assertEqual(result["outcome"], "fresh_open_noop")
        self.assertTrue(result["ok"])
        self.assertEqual(result["goal_before_status"], "blocked")
        self.assertEqual(result["goal_after_status"], "active")
        goal_controller.assert_awaited_once()
        connection = sqlite3.connect(self.db)
        status, next_run_at = connection.execute(
            "SELECT status,next_run_at FROM automations WHERE id=?", (AUTOMATION_ID,)
        ).fetchone()
        connection.close()
        self.assertEqual(status, "ACTIVE")
        self.assertEqual(next_run_at, 999_999)

    async def test_idle_target_preflights_goal_before_native_trigger(self) -> None:
        append_jsonl(
            self.rollout,
            [lifecycle("2026-07-15T03:00:00Z", "task_complete", "turn-old")],
        )

        async def goal_controller(thread_id: str, *, binary: Path) -> dict:
            self.assertEqual(thread_id, TARGET_THREAD_ID)
            connection = sqlite3.connect(self.db)
            status = connection.execute(
                "SELECT status FROM automations WHERE id=?", (AUTOMATION_ID,)
            ).fetchone()[0]
            connection.close()
            self.assertEqual(status, "PAUSED", "goal preflight must happen before due update")
            return {
                "thread_id": TARGET_THREAD_ID,
                "auth_mode": "chatgpt",
                "before_status": "blocked",
                "after_status": "active",
                "action": "resumed",
                "objective_preserved": True,
                "token_budget_preserved": True,
                "accounting_preserved": True,
            }

        verify = AsyncMock(
            return_value={
                "turn_id": "turn-native",
                "native_turn_started": True,
                "dispatch_verified": True,
                "goal_context": True,
                "heartbeat_context": True,
                "dispatch_activity_verified": True,
                "semantic_progress_verified": False,
                "writer_count": 1,
                "desktop_owned": True,
                "end_offset": 999,
            }
        )
        prompt_hash = hashlib.sha256(b"operator prompt").hexdigest()
        with (
            patch(
                "research_factory.app_server_thread_supervisor.AUTOMATION_PROMPT_SHA256",
                prompt_hash,
            ),
            patch(
                "research_factory.app_server_thread_supervisor.verify_native_trigger",
                verify,
            ),
        ):
            result = await run_supervisor_cycle(
                self.config,
                goal_controller=goal_controller,
                writer_probe=lambda _path: self.desktop_writer,
                now=NOW,
            )

        self.assertEqual(result["outcome"], "native_trigger_verified")
        self.assertEqual(result["goal_before_status"], "blocked")
        self.assertEqual(result["goal_after_status"], "active")
        self.assertEqual(result["automation_after_status"], "ACTIVE")
        self.assertEqual(result["automation_after_rrule"], AUTOMATION_RRULE)
        self.assertTrue(result["desktop_owned"])
        self.assertTrue(result["native_turn_started"])
        self.assertTrue(result["dispatch_verified"])
        self.assertTrue(result["dispatch_activity_verified"])
        self.assertFalse(result["semantic_progress_verified"])
        self.assertEqual(
            result["semantic_plan_step_id"], "shared_reference_repair_v1"
        )
        raw = self.receipts.read_text()
        self.assertNotIn("operator prompt", raw)
        self.assertNotIn("private objective text", raw)

    async def test_new_matching_step_receipt_verifies_semantic_progress(self) -> None:
        append_jsonl(
            self.rollout,
            [lifecycle("2026-07-15T03:00:00Z", "task_complete", "turn-old")],
        )
        goal_controller = AsyncMock(
            return_value={
                "thread_id": TARGET_THREAD_ID,
                "auth_mode": "chatgpt",
                "before_status": "blocked",
                "after_status": "active",
                "action": "resumed",
                "objective_preserved": True,
                "token_budget_preserved": True,
                "accounting_preserved": True,
            }
        )

        async def verify(*_args: object, **_kwargs: object) -> dict:
            write_semantic_receipt(self.semantic_receipt, state="passed")
            return {
                "turn_id": "turn-native",
                "native_turn_started": True,
                "dispatch_verified": True,
                "goal_context": True,
                "heartbeat_context": True,
                "dispatch_activity_verified": True,
                "semantic_progress_verified": False,
                "writer_count": 1,
                "desktop_owned": True,
                "end_offset": 999,
            }

        prompt_hash = hashlib.sha256(b"operator prompt").hexdigest()
        with (
            patch(
                "research_factory.app_server_thread_supervisor.AUTOMATION_PROMPT_SHA256",
                prompt_hash,
            ),
            patch(
                "research_factory.app_server_thread_supervisor.verify_native_trigger",
                verify,
            ),
        ):
            result = await run_supervisor_cycle(
                self.config,
                goal_controller=goal_controller,
                writer_probe=lambda _path: self.desktop_writer,
                now=NOW,
            )

        self.assertEqual(result["outcome"], "native_trigger_verified")
        self.assertTrue(result["dispatch_verified"])
        self.assertTrue(result["semantic_progress_verified"])
        self.assertEqual(result["semantic_step_receipt_state"], "passed")

    async def test_idle_terminal_with_zero_writers_resumes_and_verifies_new_writer(
        self,
    ) -> None:
        append_jsonl(
            self.rollout,
            [lifecycle("2026-07-15T03:00:00Z", "task_complete", "turn-old")],
        )
        goal_controller = AsyncMock(
            return_value={
                "thread_id": TARGET_THREAD_ID,
                "auth_mode": "chatgpt",
                "before_status": "blocked",
                "after_status": "active",
                "action": "resumed",
                "objective_preserved": True,
                "token_budget_preserved": True,
                "accounting_preserved": True,
            }
        )
        verify = AsyncMock(
            return_value={
                "turn_id": "turn-native",
                "native_turn_started": True,
                "dispatch_verified": True,
                "goal_context": True,
                "heartbeat_context": True,
                "dispatch_activity_verified": True,
                "semantic_progress_verified": False,
                "writer_count": 1,
                "desktop_owned": True,
                "end_offset": 999,
            }
        )
        prompt_hash = hashlib.sha256(b"operator prompt").hexdigest()
        with (
            patch(
                "research_factory.app_server_thread_supervisor.AUTOMATION_PROMPT_SHA256",
                prompt_hash,
            ),
            patch(
                "research_factory.app_server_thread_supervisor.verify_native_trigger",
                verify,
            ),
        ):
            result = await run_supervisor_cycle(
                self.config,
                goal_controller=goal_controller,
                writer_probe=lambda _path: WriterState(0, False, ()),
                now=NOW,
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["outcome"], "native_trigger_verified")
        self.assertEqual(result["goal_before_status"], "blocked")
        self.assertEqual(result["goal_after_status"], "active")
        self.assertEqual(result["writer_count"], 1)
        self.assertTrue(result["desktop_owned"])
        goal_controller.assert_awaited_once()
        verify.assert_awaited_once()

    async def test_fresh_open_with_zero_writers_still_fails_closed(self) -> None:
        append_jsonl(
            self.rollout,
            [
                lifecycle("2026-07-15T03:55:00Z", "task_started", "turn-live"),
                record("2026-07-15T03:59:00Z", "event_msg", {"type": "token_count"}),
            ],
        )
        goal = AsyncMock()
        result = await run_supervisor_cycle(
            self.config,
            goal_controller=goal,
            writer_probe=lambda _path: WriterState(0, False, ()),
            now=NOW,
        )

        self.assertFalse(result["ok"])
        self.assertEqual(result["outcome"], "supervisor_failed")
        self.assertEqual(result["failure_stage"], "rollout_preflight")
        self.assertEqual(result["writer_count"], 0)
        goal.assert_not_awaited()

    async def test_stale_open_uses_native_actuator_but_never_direct_thread_rpc(self) -> None:
        append_jsonl(
            self.rollout,
            [lifecycle("2026-07-15T01:00:00Z", "task_started", "turn-stale")],
        )
        goal_controller = AsyncMock(
            return_value={
                "thread_id": TARGET_THREAD_ID,
                "auth_mode": "chatgpt",
                "before_status": "active",
                "after_status": "active",
                "action": "already_active",
                "objective_preserved": True,
                "token_budget_preserved": True,
                "accounting_preserved": True,
            }
        )
        prompt_hash = hashlib.sha256(b"operator prompt").hexdigest()
        with (
            patch(
                "research_factory.app_server_thread_supervisor.AUTOMATION_PROMPT_SHA256",
                prompt_hash,
            ),
            patch(
                "research_factory.app_server_thread_supervisor.verify_native_trigger",
                AsyncMock(
                    return_value={
                        "turn_id": "turn-native",
                        "native_turn_started": True,
                        "dispatch_verified": True,
                        "goal_context": True,
                        "heartbeat_context": True,
                        "dispatch_activity_verified": True,
                        "semantic_progress_verified": False,
                        "writer_count": 1,
                        "desktop_owned": True,
                        "end_offset": 999,
                    }
                ),
            ),
        ):
            result = await run_supervisor_cycle(
                self.config,
                goal_controller=goal_controller,
                writer_probe=lambda _path: self.desktop_writer,
                now=NOW,
            )
        self.assertEqual(result["rollout_classification"], "stale_open")
        self.assertEqual(result["action"], "native_automation_forced_due")

    async def test_completed_goal_without_authorization_wakes_recovery(self) -> None:
        append_jsonl(
            self.rollout,
            [lifecycle("2026-07-15T03:00:00Z", "task_complete", "turn-old")],
        )
        goal_controller = AsyncMock(
            return_value={
                "thread_id": TARGET_THREAD_ID,
                "auth_mode": "chatgpt",
                "before_status": "complete",
                "after_status": "complete",
                "action": "complete",
                "objective_preserved": True,
                "token_budget_preserved": True,
                "accounting_preserved": True,
            }
        )
        completion_controller = Mock(
            return_value={
                "completion_notified": True,
                "desktop_native_automation_paused": True,
                "external_supervisor_disabled": True,
            }
        )
        verify = AsyncMock(
            return_value={
                "turn_id": "turn-native",
                "native_turn_started": True,
                "dispatch_verified": True,
                "goal_context": True,
                "heartbeat_context": True,
                "dispatch_activity_verified": True,
                "semantic_progress_verified": False,
                "writer_count": 1,
                "desktop_owned": True,
                "end_offset": 999,
            }
        )
        prompt_hash = hashlib.sha256(b"operator prompt").hexdigest()
        with (
            patch(
                "research_factory.app_server_thread_supervisor.AUTOMATION_PROMPT_SHA256",
                prompt_hash,
            ),
            patch(
                "research_factory.app_server_thread_supervisor.verify_native_trigger",
                verify,
            ),
        ):
            result = await run_supervisor_cycle(
                self.config,
                goal_controller=goal_controller,
                completion_controller=completion_controller,
                writer_probe=lambda _path: self.desktop_writer,
                now=NOW,
            )
        self.assertEqual(
            result["outcome"],
            "goal_complete_without_authorization_recovery_trigger_verified",
        )
        self.assertEqual(result["completion_authorization_status"], "absent")
        self.assertFalse(result["completion_authorization_valid"])
        completion_controller.assert_not_called()
        verify.assert_awaited_once()

    async def test_completed_goal_with_valid_bound_authorization_finalizes(
        self,
    ) -> None:
        append_jsonl(
            self.rollout,
            [lifecycle("2026-07-15T03:00:00Z", "task_complete", "turn-old")],
        )
        write_completion_authorization(
            self.completion_authorization,
            self.project_root,
        )
        goal_controller = AsyncMock(
            return_value={
                "thread_id": TARGET_THREAD_ID,
                "auth_mode": "chatgpt",
                "before_status": "complete",
                "after_status": "complete",
                "action": "complete",
                "objective_preserved": True,
                "token_budget_preserved": True,
                "accounting_preserved": True,
            }
        )
        completion_controller = Mock(
            return_value={
                "completion_notified": True,
                "desktop_native_automation_paused": True,
                "desktop_native_automation_already_paused": False,
                "external_supervisor_disabled": True,
            }
        )
        result = await run_supervisor_cycle(
            self.config,
            goal_controller=goal_controller,
            completion_controller=completion_controller,
            completion_source_verifier=fake_completion_source_verifier,
            writer_probe=lambda _path: self.desktop_writer,
            now=NOW,
        )
        self.assertEqual(result["outcome"], "end_to_end_completion_verified")
        self.assertTrue(result["completion_authorization_valid"])
        self.assertTrue(result["completion_notified"])
        self.assertTrue(result["desktop_native_automation_paused"])
        self.assertTrue(result["external_supervisor_disabled"])
        completion_controller.assert_called_once()
        call = completion_controller.call_args
        self.assertEqual(
            call.kwargs,
            {
                "automations_db": self.db,
                "automation_toml": self.toml,
                "canonical_automation_toml": self.canonical_toml,
                "delivery_receipt_path": self.completion_delivery_receipt,
            },
        )
        self.assertEqual(
            call.args[0].sha256,
            hashlib.sha256(self.completion_authorization.read_bytes()).hexdigest(),
        )

    async def test_completed_goal_with_invalid_authorization_stays_recoverable(
        self,
    ) -> None:
        append_jsonl(
            self.rollout,
            [lifecycle("2026-07-15T03:00:00Z", "task_complete", "turn-old")],
        )
        payload = write_completion_authorization(
            self.completion_authorization,
            self.project_root,
        )
        self.assertIn("eligible_extraction_backlog_zero", payload["evidence"])

        def reject_backlog(
            criterion: str,
            source_path: Path,
            project_root: Path,
        ) -> dict:
            result = fake_completion_source_verifier(
                criterion,
                source_path,
                project_root,
            )
            if criterion == "eligible_extraction_backlog_zero":
                result["passed"] = False
            return result
        goal_controller = AsyncMock(
            return_value={
                "thread_id": TARGET_THREAD_ID,
                "auth_mode": "chatgpt",
                "before_status": "complete",
                "after_status": "complete",
                "action": "complete",
                "objective_preserved": True,
                "token_budget_preserved": True,
                "accounting_preserved": True,
            }
        )
        verify = AsyncMock(
            return_value={
                "turn_id": "turn-native",
                "native_turn_started": True,
                "dispatch_verified": True,
                "goal_context": True,
                "heartbeat_context": True,
                "dispatch_activity_verified": True,
                "semantic_progress_verified": False,
                "writer_count": 1,
                "desktop_owned": True,
                "end_offset": 999,
            }
        )
        completion_controller = Mock()
        prompt_hash = hashlib.sha256(b"operator prompt").hexdigest()
        with (
            patch(
                "research_factory.app_server_thread_supervisor.AUTOMATION_PROMPT_SHA256",
                prompt_hash,
            ),
            patch(
                "research_factory.app_server_thread_supervisor.verify_native_trigger",
                verify,
            ),
        ):
            result = await run_supervisor_cycle(
                self.config,
                goal_controller=goal_controller,
                completion_controller=completion_controller,
                completion_source_verifier=reject_backlog,
                writer_probe=lambda _path: self.desktop_writer,
                now=NOW,
            )
        self.assertEqual(result["completion_authorization_status"], "invalid")
        self.assertFalse(result["completion_authorization_valid"])
        self.assertEqual(
            result["outcome"],
            "goal_complete_without_authorization_recovery_trigger_verified",
        )
        completion_controller.assert_not_called()
        verify.assert_awaited_once()

    async def test_unverified_native_restart_fails_closed_for_owner_recovery(self) -> None:
        append_jsonl(
            self.rollout,
            [lifecycle("2026-07-15T01:00:00Z", "task_started", "turn-stale")],
        )
        goal_controller = AsyncMock(
            return_value={
                "thread_id": TARGET_THREAD_ID,
                "auth_mode": "chatgpt",
                "before_status": "blocked",
                "after_status": "active",
                "action": "resumed",
                "objective_preserved": True,
                "token_budget_preserved": True,
                "accounting_preserved": True,
            }
        )
        prompt_hash = hashlib.sha256(b"operator prompt").hexdigest()
        with (
            patch(
                "research_factory.app_server_thread_supervisor.AUTOMATION_PROMPT_SHA256",
                prompt_hash,
            ),
            patch(
                "research_factory.app_server_thread_supervisor.verify_native_trigger",
                AsyncMock(
                    side_effect=SupervisorError(
                        "native automation trigger was not fully verified"
                    )
                ),
            ),
        ):
            result = await run_supervisor_cycle(
                self.config,
                goal_controller=goal_controller,
                writer_probe=lambda _path: self.desktop_writer,
                now=NOW,
            )
        self.assertFalse(result["ok"])
        self.assertEqual(result["outcome"], "native_trigger_unverified")
        self.assertEqual(
            result["owner_recovery_policy"], "retry_and_alert_no_second_writer"
        )
        self.assertEqual(result["goal_after_status"], "active")

    async def test_foreign_or_missing_writer_fails_before_goal_preflight(self) -> None:
        append_jsonl(
            self.rollout,
            [lifecycle("2026-07-15T03:00:00Z", "task_complete", "turn-old")],
        )
        goal = AsyncMock()
        result = await run_supervisor_cycle(
            self.config,
            goal_controller=goal,
            writer_probe=lambda _path: WriterState(2, False, (1, 2)),
            now=NOW,
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["outcome"], "supervisor_failed")
        goal.assert_not_awaited()

    async def test_fresh_open_with_foreign_writer_fails_without_reactivating_actuator(self) -> None:
        append_jsonl(
            self.rollout,
            [
                lifecycle("2026-07-15T03:55:00Z", "task_started", "turn-live"),
                record("2026-07-15T03:59:00Z", "event_msg", {"type": "token_count"}),
            ],
        )
        result = await run_supervisor_cycle(
            self.config,
            goal_controller=AsyncMock(),
            writer_probe=lambda _path: WriterState(2, False, (1, 2)),
            now=NOW,
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["outcome"], "supervisor_failed")
        connection = sqlite3.connect(self.db)
        status = connection.execute(
            "SELECT status FROM automations WHERE id=?", (AUTOMATION_ID,)
        ).fetchone()[0]
        connection.close()
        self.assertEqual(status, "PAUSED")

    async def test_lock_busy_is_a_noop(self) -> None:
        self.lock.touch()
        with self.lock.open("a+") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            try:
                result = await run_supervisor_cycle(self.config)
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        self.assertEqual(result["outcome"], "lock_busy")
        self.assertFalse(self.receipts.exists())

    async def test_other_target_is_rejected(self) -> None:
        config = SupervisorConfig(
            thread_id="different",
            automation_toml=self.toml,
            canonical_automation_toml=self.canonical_toml,
            automations_db=self.db,
            rollout_path=self.rollout,
            lock_path=self.lock,
            receipt_path=self.receipts,
        )
        with self.assertRaisesRegex(ValueError, "exact evaluation thread"):
            await run_supervisor_cycle(config)


if __name__ == "__main__":
    unittest.main()
