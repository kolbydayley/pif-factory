from __future__ import annotations

import argparse
import asyncio
import copy
import hashlib
import json
import math
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_canonical_v31_episode_batch as adapter
from . import app_server_canonical_v31_epoch7_input_authority_plan as authority_plan
from . import app_server_canonical_v31_epoch7_input_authority_runtime as epoch7
from . import app_server_canonical_v31_epoch8_source_only_authority_recovery as epoch8
from . import codex_app_server


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EPOCH7_ROOT = epoch8.PREDECESSOR_ROOT
EPOCH8_ROOT = epoch8.DEFAULT_ROOT
DEFAULT_ROOT = (
    PROJECT_ROOT
    / "work"
    / "app-server-development-v2"
    / "unattended-pipeline-v5"
    / "canonical-v31-epoch9-split-authority-recovery-v1"
)
DIRECTIVE_PATH = (
    PROJECT_ROOT
    / "automation"
    / "pif-evaluation-epoch9-split-authority-recovery-v9.json"
)
DIRECTIVE_SHA256 = "dc9d9ff25000f02e9e7097788abb5fbbebca5196dcbb6d5afc9b590bfa2db60a"

CONTRACT_FILENAME = "runtime-contract.json"
RUNTIME_LOCK_FILENAME = "runtime-lock.json"
PREAUTHORIZATION_FILENAME = "preauthorization-receipt.json"
AUTHORIZATION_FILENAME = "operator-authorization.json"
RECEIPT_FILENAME = "authority-recovery-receipt.json"
TERMINAL_FILENAME = "terminal.json"
MERGED_OUTPUT_FILENAME = epoch7.MERGED_OUTPUT_FILENAME
PREPARED_DIRECTORY = "prepared-turns"
TURNS_DIRECTORY = "turns"
ADOPTED_DIRECTORY = "adopted-evidence"

CONTRACT_VERSION = "pif_canonical_v31_epoch9_split_authority_recovery_contract_v1"
RUNTIME_LOCK_VERSION = "pif_canonical_v31_epoch9_split_authority_recovery_runtime_lock_v1"
PREAUTHORIZATION_VERSION = "pif_canonical_v31_epoch9_split_authority_recovery_preauthorization_v1"
AUTHORIZATION_VERSION = "pif_canonical_v31_epoch9_split_authority_recovery_authorization_v1"
TURN_MANIFEST_VERSION = "pif_canonical_v31_epoch9_split_authority_turn_manifest_v1"
TURN_RESULT_VERSION = "pif_canonical_v31_epoch9_split_authority_turn_result_v1"
RECEIPT_VERSION = "pif_canonical_v31_epoch9_split_authority_recovery_receipt_v1"
ACCOUNTING_REJECTION_VERSION = "pif_canonical_v31_epoch8_full_thread_accounting_rejection_v1"
CONTEXT_INPUT_VERSION = "pif_canonical_v31_epoch9_context_input_v1"
LABEL_STATIC_INPUT_VERSION = "pif_canonical_v31_epoch9_label_static_input_v1"
CONTEXT_OUTPUT_VERSION = "pif_canonical_v31_epoch9_context_output_v1"
LABEL_OUTPUT_VERSION = "pif_canonical_v31_epoch9_label_output_v1"

MODEL = authority_plan.MODEL
EFFORT = authority_plan.EFFORT
ADOPTED_EPOCH7_TURN_COUNT = 1
ADOPTED_EPOCH8_TURN_COUNT = 1
UNTOUCHED_EPISODE_COUNT = 2
EPOCH8_VALID_LABEL_COUNT = 5
EPOCH8_INVALID_LABEL_COUNT = 2
NEW_TURN_COUNT = 5
NEW_PER_TURN_THREAD_TOTAL_TOKEN_CAP = 110_000
NEW_TOTAL_TOKEN_CAP = 550_000
EPOCH7_ADOPTED_TOTAL_TOKENS = 124_404
EPOCH8_LAST_INCREMENT_TOTAL_TOKENS = 92_732
EPOCH8_FULL_THREAD_TOTAL_TOKENS = 580_618
ADOPTED_DEVELOPMENT_QA_TOTAL_TOKENS = (
    EPOCH7_ADOPTED_TOTAL_TOKENS + EPOCH8_FULL_THREAD_TOTAL_TOKENS
)
CUMULATIVE_DEVELOPMENT_QA_TOTAL_TOKEN_CAP = (
    ADOPTED_DEVELOPMENT_QA_TOTAL_TOKENS + NEW_TOTAL_TOKEN_CAP
)
MAXIMUM_WALL_SECONDS_PER_TURN = authority_plan.MAXIMUM_WALL_SECONDS_PER_TURN
AUTHORIZATION_WINDOW_MAX_SECONDS = 14_400

AUTHORIZATION_STATEMENT = (
    "You have my full permission to continue. No need to seek out my approval anymore."
)
AUTHORIZATION_STATEMENT_SHA256 = hashlib.sha256(
    AUTHORIZATION_STATEMENT.encode("utf-8")
).hexdigest()

CONTEXT_BASE_INSTRUCTIONS = """You are the sole LLM authority for one private development episode-context completion. This is not production or holdout work.

Read the complete checksum-bound episode source and return only the source categories or regions that future canonical extraction should exclude because they are non-substantive framing, setup, advertising, credits, navigation, or other non-research context. Return an empty list only when the complete source supports that conclusion. Do not use keyword, regex, topic-list, majority-vote, or deterministic semantic rules.

Do not invoke tools, shell commands, filesystem access, web access, or other agents. Produce the schema-valid JSON directly. Never reproduce the full transcript in the response.
"""

LABEL_BASE_INSTRUCTIONS = """You are the sole LLM authority for private development-only canonical ai_discourse_v3_1 reference regeneration. This is not production or holdout work.

For each supplied segment, author one complete canonical label in the exact supplied order. The input contains only the exact source segments requiring regeneration, the existing episode context, and an LLM-authored excluded-source context. No prior malformed label or validation diagnostic is present. Preserve segment_quality exactly. Every evidence string and every nonempty metric raw_text, value, unit, and comparator must be an exact contiguous substring of that event's evidence. Understand the source semantically. Do not use keyword, regex, topic-list, majority-vote, deterministic pruning, deduplication, relabeling, or semantic rules.

Do not invoke tools, shell commands, filesystem access, web access, or other agents. Produce the schema-valid JSON directly.
"""


class CanonicalV31Epoch9SplitRecoveryError(RuntimeError):
    pass


class CanonicalV31Epoch9SplitRecoveryWaiting(RuntimeError):
    pass


class CanonicalV31Epoch9SplitRecoveryRejected(RuntimeError):
    pass


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _pretty_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _record(path: Path) -> dict[str, Any]:
    try:
        return epoch7._record(path)  # noqa: SLF001 - checksum-bound pure helper
    except epoch7.CanonicalV31Epoch7AuthorityRuntimeError as exc:
        raise CanonicalV31Epoch9SplitRecoveryError(str(exc)) from exc


def _verify_record(value: Any, *, label: str) -> Path:
    try:
        return epoch7._verify_record(value, label=label)  # noqa: SLF001
    except epoch7.CanonicalV31Epoch7AuthorityRuntimeError as exc:
        raise CanonicalV31Epoch9SplitRecoveryError(str(exc)) from exc


def _load_object(path: Path, *, label: str) -> dict[str, Any]:
    try:
        return epoch7._load_object(path, label=label)  # noqa: SLF001
    except epoch7.CanonicalV31Epoch7AuthorityRuntimeError as exc:
        raise CanonicalV31Epoch9SplitRecoveryError(str(exc)) from exc


def _write_json(path: Path, value: Any) -> dict[str, Any]:
    try:
        return epoch7._write_immutable_json(path, value)  # noqa: SLF001
    except epoch7.CanonicalV31Epoch7AuthorityRuntimeError as exc:
        raise CanonicalV31Epoch9SplitRecoveryError(str(exc)) from exc


def _write_bytes(path: Path, value: bytes) -> dict[str, Any]:
    try:
        return epoch7._write_immutable_bytes(path, value)  # noqa: SLF001
    except epoch7.CanonicalV31Epoch7AuthorityRuntimeError as exc:
        raise CanonicalV31Epoch9SplitRecoveryError(str(exc)) from exc


def _parse_timestamp(value: Any, *, label: str) -> datetime:
    try:
        return epoch7._parse_timestamp(value, label=label)  # noqa: SLF001
    except epoch7.CanonicalV31Epoch7AuthorityRuntimeError as exc:
        raise CanonicalV31Epoch9SplitRecoveryError(str(exc)) from exc


def _usage(value: Any, *, label: str) -> dict[str, int]:
    try:
        return epoch7._usage(value, label=label)  # noqa: SLF001
    except epoch7.CanonicalV31Epoch7AuthorityRuntimeError as exc:
        raise CanonicalV31Epoch9SplitRecoveryError(str(exc)) from exc


def _sum_usage(values: Sequence[Mapping[str, int]]) -> dict[str, int]:
    return {
        field: sum(int(value[field]) for value in values)
        for field in epoch7.USAGE_FIELDS
    }


def _now(value: datetime | None = None) -> datetime:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None:
        raise CanonicalV31Epoch9SplitRecoveryError("current time is not timezone-aware")
    return current.astimezone(timezone.utc)


def _verify_directive() -> dict[str, Any]:
    record = _record(DIRECTIVE_PATH)
    if record["sha256"] != DIRECTIVE_SHA256:
        raise CanonicalV31Epoch9SplitRecoveryError("epoch-9 directive drifted")
    directive = _load_object(DIRECTIVE_PATH, label="epoch-9 directive")
    fixed = {
        "schema_version": "pif_evaluation_epoch9_split_authority_recovery_directive_v1",
        "thread_id": "019f4cf1-c46e-7db3-acd2-bf03c4459a10",
        "plan_epoch": 9,
        "step_id": "canonical_v31_epoch9_split_authority_recovery_v9",
        "state": "ready_for_direct_user_authorized_execute",
        "expected_receipt_path": str((DEFAULT_ROOT / RECEIPT_FILENAME).resolve()),
    }
    for key, expected in fixed.items():
        if directive.get(key) != expected:
            raise CanonicalV31Epoch9SplitRecoveryError(
                f"epoch-9 directive drifted at {key}"
            )
    authorization = directive.get("authorization_contract")
    predecessor_contract = directive.get("predecessor_contract")
    architecture = directive.get("architecture_contract")
    execution = directive.get("execution_contract")
    terminal = directive.get("terminal_contract")
    if (
        not isinstance(authorization, Mapping)
        or authorization.get("authority") != "direct_user_instruction"
        or authorization.get("authorized_by") != "kolby"
        or authorization.get("operator_authorization_statement")
        != AUTHORIZATION_STATEMENT
        or authorization.get("additional_interactive_approval_required") is not False
        or not isinstance(predecessor_contract, Mapping)
        or predecessor_contract.get("epoch7_root") != str(EPOCH7_ROOT.resolve())
        or predecessor_contract.get("epoch8_root") != str(EPOCH8_ROOT.resolve())
        or predecessor_contract.get("epoch7_adopted_turn_count") != 1
        or predecessor_contract.get("epoch8_diagnostic_turn_count") != 1
        or predecessor_contract.get("epoch8_last_increment_total_tokens")
        != EPOCH8_LAST_INCREMENT_TOTAL_TOKENS
        or predecessor_contract.get("epoch8_full_thread_total_tokens")
        != EPOCH8_FULL_THREAD_TOTAL_TOKENS
        or predecessor_contract.get("epoch8_frozen_per_turn_ceiling")
        != epoch8.NEW_PER_TURN_TOTAL_TOKEN_CAP
        or predecessor_contract.get("epoch8_valid_label_count")
        != EPOCH8_VALID_LABEL_COUNT
        or predecessor_contract.get("epoch8_invalid_label_count")
        != EPOCH8_INVALID_LABEL_COUNT
        or predecessor_contract.get("epoch8_valid_context_component_count") != 1
        or predecessor_contract.get("completed_turn_replay_allowed") is not False
        or not isinstance(architecture, Mapping)
        or architecture.get("name")
        != "episode_context_then_segment_only_canonical_labels"
        or architecture.get("untouched_episode_count") != UNTOUCHED_EPISODE_COUNT
        or architecture.get("epoch8_invalid_label_repair_turn_count") != 1
        or architecture.get("turns_per_untouched_episode") != 2
        or architecture.get("model_visible_legacy_reference_label_count") != 0
        or architecture.get("model_visible_validation_diagnostic_count") != 0
        or architecture.get("context_output_is_bound_into_label_turn") is not True
        or architecture.get("full_combined_output_validator_unchanged_from_epoch7")
        is not True
        or architecture.get("deterministic_semantic_mutation_allowed") is not False
        or architecture.get("semantic_regex_keyword_pruning_allowed") is not False
        or not isinstance(execution, Mapping)
        or execution.get("model") != MODEL
        or execution.get("effort") != EFFORT
        or execution.get("new_model_call_cap") != NEW_TURN_COUNT
        or execution.get("new_total_token_cap") != NEW_TOTAL_TOKEN_CAP
        or execution.get("new_per_turn_thread_total_token_cap")
        != NEW_PER_TURN_THREAD_TOTAL_TOKEN_CAP
        or execution.get("adopted_development_qa_total_tokens")
        != ADOPTED_DEVELOPMENT_QA_TOTAL_TOKENS
        or execution.get("cumulative_development_qa_total_token_cap")
        != CUMULATIVE_DEVELOPMENT_QA_TOTAL_TOKEN_CAP
        or execution.get("semantic_retry_count") != 0
        or execution.get("official_persistent_codex_app_server_only") is not True
        or execution.get("managed_chatgpt_auth_only") is not True
        or execution.get("managed_chatgpt_plan_type") != "pro"
        or execution.get("fresh_capacity_probe_before_thread_and_turn") is not True
        or execution.get("caller_supplied_capacity_admission_allowed") is not False
        or execution.get("full_thread_total_usage_is_accounting_authority") is not True
        or execution.get("last_usage_increment_is_diagnostic_only") is not True
        or execution.get("partial_attempt_replay_forbidden") is not True
        or execution.get("api_key_auth_allowed") is not False
        or execution.get("raw_session_token_auth_allowed") is not False
        or execution.get("codex_exec_allowed") is not False
        or not isinstance(terminal, Mapping)
        or terminal.get("accepted_receipt_states") != ["passed", "rejected", "waiting"]
        or terminal.get("quality_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutation_allowed") is not False
    ):
        raise CanonicalV31Epoch9SplitRecoveryError(
            "epoch-9 directive contract drifted"
        )
    return directive


def _usage_dominates(total: Mapping[str, int], last: Mapping[str, int]) -> bool:
    return all(int(total[field]) >= int(last[field]) for field in epoch7.USAGE_FIELDS)


def _validate_completed_sidecar_common(
    *,
    sidecar_path: Path,
    output_path: Path,
    prompt_path: Path,
    base_path: Path,
    schema_path: Path,
    thread_binding: Mapping[str, Any],
) -> dict[str, Any]:
    sidecar = _load_object(sidecar_path, label="completed authority sidecar")
    schema = _load_object(schema_path, label="authority output schema")
    raw_text = output_path.read_text(encoding="utf-8")
    raw_message = raw_text[:-1] if raw_text.endswith("\n") else raw_text
    fixed = {
        "schema_version": codex_app_server.TURN_SIDECAR_SCHEMA_VERSION,
        "client_version": codex_app_server.APP_SERVER_CLIENT_VERSION,
        "cli_version": codex_app_server.PINNED_CODEX_CLI_VERSION,
        "protocol_schema_sha256": _record(codex_app_server.PROTOCOL_SCHEMA_PATH)[
            "sha256"
        ],
        "transport": "stdio",
        "synthetic_debug_errors": False,
        "auth_type": "chatgpt",
        "plan_type": "pro",
        "thread_id": thread_binding["thread_id"],
        "model": MODEL,
        "effort": EFFORT,
        "batch_size": 1,
        "thread_mode": "new_thread",
        "prompt_sha256": _sha256_bytes(prompt_path.read_bytes()),
        "prompt_bytes": prompt_path.stat().st_size,
        "base_instructions_sha256": _sha256_bytes(base_path.read_bytes()),
        "base_instructions_bytes": base_path.stat().st_size,
        "instruction_sources_sha256": thread_binding["instruction_sources_sha256"],
        "instruction_sources_count": thread_binding["instruction_sources_count"],
        "output_schema_sha256": _sha256_bytes(
            _canonical_json(schema).encode("utf-8")
        ),
        "output_schema_bytes": len(_canonical_json(schema).encode("utf-8")),
        "state": "completed",
        "status": "completed",
        "error_class": None,
        "usage_status": "measured",
        "usage_complete": True,
        "recovery_reran_model": False,
        "output_sha256": _sha256_bytes(raw_message.encode("utf-8")),
    }
    for key, expected in fixed.items():
        if sidecar.get(key) != expected:
            raise CanonicalV31Epoch9SplitRecoveryError(
                f"completed authority sidecar drifted at {key}"
            )
    _parse_timestamp(sidecar.get("started_at"), label="sidecar started_at")
    _parse_timestamp(sidecar.get("finished_at"), label="sidecar finished_at")
    if (
        not isinstance(sidecar.get("turn_id"), str)
        or not sidecar["turn_id"]
        or not isinstance(sidecar.get("app_server_user_agent"), str)
        or not sidecar["app_server_user_agent"]
        or not isinstance(sidecar.get("max_message_bytes"), int)
        or isinstance(sidecar.get("max_message_bytes"), bool)
        or sidecar["max_message_bytes"] < 64 * 1024
        or not epoch7._is_sha256(sidecar.get("stderr_sha256"))  # noqa: SLF001
        or not isinstance(sidecar.get("stderr_bytes"), int)
        or isinstance(sidecar.get("stderr_bytes"), bool)
        or sidecar["stderr_bytes"] < 0
        or Path(str(sidecar.get("output_path") or "")).expanduser().resolve()
        != output_path.resolve()
        or not isinstance(sidecar.get("wall_elapsed_seconds"), (int, float))
        or isinstance(sidecar.get("wall_elapsed_seconds"), bool)
        or float(sidecar["wall_elapsed_seconds"]) < 0
    ):
        raise CanonicalV31Epoch9SplitRecoveryError(
            "completed authority sidecar lifecycle drifted"
        )
    last = _usage(sidecar.get("usage"), label="last inference increment")
    total = _usage(sidecar.get("thread_total_usage"), label="full thread total")
    if not _usage_dominates(total, last):
        raise CanonicalV31Epoch9SplitRecoveryError(
            "full thread usage does not dominate the last inference increment"
        )
    return {
        "sidecar": sidecar,
        "last_usage": last,
        "thread_total_usage": total,
        "thread_id": sidecar["thread_id"],
        "turn_id": sidecar["turn_id"],
        "wall_elapsed_seconds": float(sidecar["wall_elapsed_seconds"]),
        "raw_message": raw_message,
    }


def _validate_epoch8_diagnostic() -> dict[str, Any]:
    epoch8.verify_preauthorization(EPOCH8_ROOT)
    authorization = epoch8.verify_authorization(
        EPOCH8_ROOT, expected_authorization_id=None, require_current=False
    )
    contract = _load_object(EPOCH8_ROOT / epoch8.CONTRACT_FILENAME, label="epoch-8 contract")
    turn = contract["turns"][0]
    paths = epoch7._turn_paths(EPOCH8_ROOT, str(turn["turn_id"]))  # noqa: SLF001
    expected_attempt = epoch7._attempt_payload(  # noqa: SLF001
        root=EPOCH8_ROOT, contract=contract, authorization=authorization, turn=turn
    )
    if _load_object(paths["attempt"], label="epoch-8 semantic attempt") != expected_attempt:
        raise CanonicalV31Epoch9SplitRecoveryError("epoch-8 semantic attempt drifted")
    initial_request = epoch7._capacity_request(  # noqa: SLF001
        root=EPOCH8_ROOT,
        contract=contract,
        authorization=authorization,
        turn=turn,
        boundary="initial_before_thread",
        completed_turn_count=0,
    )
    preturn_request = epoch7._capacity_request(  # noqa: SLF001
        root=EPOCH8_ROOT,
        contract=contract,
        authorization=authorization,
        turn=turn,
        boundary="preturn_before_turn",
        completed_turn_count=0,
    )
    initial = epoch7._verify_capacity_bundle(  # noqa: SLF001
        paths["initial_capacity"],
        expected_request=initial_request,
        contract=contract,
        authorization=authorization,
        historical=True,
    )
    preturn = epoch7._verify_capacity_bundle(  # noqa: SLF001
        paths["preturn_capacity"],
        expected_request=preturn_request,
        contract=contract,
        authorization=authorization,
        historical=True,
    )
    thread_binding = epoch7._verify_thread_binding(  # noqa: SLF001
        _load_object(paths["thread"], label="epoch-8 thread binding"),
        contract=contract,
        turn=turn,
    )
    manifest_path = _verify_record(turn["turn_manifest"], label="epoch-8 turn manifest")
    manifest = _load_object(manifest_path, label="epoch-8 turn manifest")
    prompt_path = _verify_record(manifest["prompt"], label="epoch-8 prompt")
    base_path = _verify_record(manifest["base_instructions"], label="epoch-8 base")
    schema_path = _verify_record(manifest["output_schema"], label="epoch-8 schema")
    telemetry = _validate_completed_sidecar_common(
        sidecar_path=paths["sidecar"],
        output_path=paths["raw_output"],
        prompt_path=prompt_path,
        base_path=base_path,
        schema_path=schema_path,
        thread_binding=thread_binding,
    )
    if (
        telemetry["last_usage"]["total_tokens"] != EPOCH8_LAST_INCREMENT_TOTAL_TOKENS
        or telemetry["thread_total_usage"]["total_tokens"]
        != EPOCH8_FULL_THREAD_TOTAL_TOKENS
        or telemetry["thread_total_usage"]["total_tokens"]
        <= epoch8.NEW_PER_TURN_TOTAL_TOKEN_CAP
    ):
        raise CanonicalV31Epoch9SplitRecoveryError(
            "epoch-8 measured accounting witness drifted"
        )
    try:
        raw_output = json.loads(telemetry["raw_message"])
    except json.JSONDecodeError as exc:
        raise CanonicalV31Epoch9SplitRecoveryError(
            "epoch-8 diagnostic output is not structured JSON"
        ) from exc
    original_input = _load_object(
        _verify_record(manifest["input"], label="epoch-8 original authority input"),
        label="epoch-8 original authority input",
    )
    schema = _load_object(schema_path, label="epoch-8 output schema")
    try:
        adapter._validate_schema(dict(raw_output), schema, "$")  # noqa: SLF001
    except adapter.CanonicalV31OutputError as exc:
        raise CanonicalV31Epoch9SplitRecoveryError(
            "epoch-8 diagnostic output failed its frozen JSON schema"
        ) from exc
    episode_id = str(original_input["episode_id"])
    labels = raw_output.get("repaired_reference_labels")
    expected_ids = list(original_input["invalid_reference_segment_ids"])
    if (
        raw_output.get("episode_id") != episode_id
        or not isinstance(labels, list)
        or [row.get("segment_id") for row in labels] != expected_ids
    ):
        raise CanonicalV31Epoch9SplitRecoveryError(
            "epoch-8 diagnostic output identity or order drifted"
        )
    excluded = raw_output.get("excluded_source_context")
    if not isinstance(excluded, list) or any(
        not isinstance(item, str) or not item for item in excluded
    ):
        raise CanonicalV31Epoch9SplitRecoveryError(
            "epoch-8 diagnostic context component is malformed"
        )
    repairs_by_id = {
        str(row["segment_id"]): row for row in original_input["reference_repairs"]
    }
    valid_labels: list[dict[str, Any]] = []
    invalid_diagnostics: list[dict[str, str]] = []
    for label in labels:
        segment_id = str(label["segment_id"])
        source = repairs_by_id[segment_id]
        if (
            label.get("episode_id") != episode_id
            or label.get("segment_quality")
            != source["legacy_reference_label"].get("segment_quality")
        ):
            raise CanonicalV31Epoch9SplitRecoveryError(
                "epoch-8 diagnostic label provenance drifted"
            )
        try:
            authority_plan.validate_label_output(
                adapter.CANONICAL_LABEL_PACK,
                label,
                segment_text=source["segment_text"],
            )
        except authority_plan.ValidationError as exc:
            invalid_diagnostics.append(
                {
                    "segment_id": segment_id,
                    "error_class": type(exc).__name__,
                    "diagnostic_path": str(exc),
                }
            )
        else:
            valid_labels.append(copy.deepcopy(dict(label)))
    invalid_ids = [row["segment_id"] for row in invalid_diagnostics]
    if (
        len(valid_labels) != EPOCH8_VALID_LABEL_COUNT
        or len(invalid_ids) != EPOCH8_INVALID_LABEL_COUNT
        or set(invalid_ids) & {str(row["segment_id"]) for row in valid_labels}
    ):
        raise CanonicalV31Epoch9SplitRecoveryError(
            "epoch-8 valid/invalid label partition drifted"
        )
    return {
        "schema_version": ACCOUNTING_REJECTION_VERSION,
        "state": "rejected",
        "terminal_reason": (
            "epoch8_full_thread_total_exceeded_ceiling_and_two_labels_failed_"
            "canonical_exactness"
        ),
        "epoch8_root": str(EPOCH8_ROOT.resolve()),
        "epoch8_runtime_contract": _record(EPOCH8_ROOT / epoch8.CONTRACT_FILENAME),
        "epoch8_runtime_lock": _record(EPOCH8_ROOT / epoch8.RUNTIME_LOCK_FILENAME),
        "epoch8_authorization": _record(EPOCH8_ROOT / epoch8.AUTHORIZATION_FILENAME),
        "epoch8_attempt": _record(paths["attempt"]),
        "epoch8_initial_capacity": initial["records"],
        "epoch8_thread_binding": _record(paths["thread"]),
        "epoch8_preturn_capacity": preturn["records"],
        "epoch8_sidecar": _record(paths["sidecar"]),
        "epoch8_raw_output": _record(paths["raw_output"]),
        "epoch8_authority_turn_manifest": _record(manifest_path),
        "last_inference_increment_usage": telemetry["last_usage"],
        "full_thread_total_usage": telemetry["thread_total_usage"],
        "frozen_per_turn_total_token_ceiling": epoch8.NEW_PER_TURN_TOTAL_TOKEN_CAP,
        "full_thread_total_usage_exceeded_ceiling": True,
        "valid_context_output": {
            "schema_version": CONTEXT_OUTPUT_VERSION,
            "episode_id": episode_id,
            "excluded_source_context": copy.deepcopy(excluded),
        },
        "valid_reference_labels": valid_labels,
        "invalid_reference_segment_ids": invalid_ids,
        "invalid_reference_diagnostics": invalid_diagnostics,
        "valid_component_adoption_authorized_for_successor": True,
        "whole_output_adoption_authorized": False,
        "completed_turn_replay_allowed": False,
        "semantic_retry_count": 0,
        "quality_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def _context_schema(combined: Mapping[str, Any], *, episode_id: str) -> dict[str, Any]:
    excluded = copy.deepcopy(combined["properties"]["excluded_source_context"])
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["schema_version", "episode_id", "excluded_source_context"],
        "properties": {
            "schema_version": {"type": "string", "const": CONTEXT_OUTPUT_VERSION},
            "episode_id": {"type": "string", "const": episode_id},
            "excluded_source_context": excluded,
        },
    }


def _label_schema(
    combined: Mapping[str, Any],
    *,
    episode_id: str,
    segment_ids: Sequence[str] | None = None,
) -> dict[str, Any]:
    labels = copy.deepcopy(combined["properties"]["repaired_reference_labels"])
    if segment_ids is not None:
        labels["minItems"] = len(segment_ids)
        labels["maxItems"] = len(segment_ids)
        labels["items"]["properties"]["segment_id"] = {
            "type": "string",
            "enum": list(segment_ids),
        }
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["schema_version", "episode_id", "repaired_reference_labels"],
        "properties": {
            "schema_version": {"type": "string", "const": LABEL_OUTPUT_VERSION},
            "episode_id": {"type": "string", "const": episode_id},
            "repaired_reference_labels": labels,
        },
    }


def _turn_id(*, episode_id: str, stage: str, static_input: Mapping[str, Any]) -> str:
    digest = _sha256_bytes(
        _canonical_json(
            {
                "episode_id": episode_id,
                "stage": stage,
                "static_input_sha256": _sha256_bytes(
                    _canonical_json(static_input).encode("ascii")
                ),
                "architecture": "epoch9_context_then_segment_only_labels",
            }
        ).encode("ascii")
    )
    return f"authority_{stage}_{digest[:24]}"


def _source_entries() -> list[dict[str, Any]]:
    epoch8.verify_preauthorization(EPOCH8_ROOT)
    contract = _load_object(EPOCH8_ROOT / epoch8.CONTRACT_FILENAME, label="epoch-8 contract")
    entries: list[dict[str, Any]] = []
    for turn in contract["turns"][1:]:
        manifest_path = _verify_record(turn["turn_manifest"], label="epoch-8 manifest")
        manifest = _load_object(manifest_path, label="epoch-8 manifest")
        compact_path = _verify_record(manifest["compact_input"], label="source-only input")
        original_path = _verify_record(manifest["input"], label="original authority input")
        schema_path = _verify_record(manifest["output_schema"], label="combined schema")
        compact = _load_object(compact_path, label="source-only input")
        original = _load_object(original_path, label="original authority input")
        schema = _load_object(schema_path, label="combined schema")
        if (
            compact.get("episode_id") != turn.get("episode_id")
            or original.get("episode_id") != turn.get("episode_id")
            or len(compact.get("reference_regeneration") or []) != 6
            or "legacy_reference_label" in _canonical_json(compact)
            or "validation_diagnostic" in _canonical_json(compact)
        ):
            raise CanonicalV31Epoch9SplitRecoveryError(
                "epoch-9 source-only episode partition drifted"
            )
        entries.append(
            {
                "source_turn": copy.deepcopy(turn),
                "source_manifest": _record(manifest_path),
                "compact_input": _record(compact_path),
                "original_input": _record(original_path),
                "combined_schema": _record(schema_path),
                "compact": compact,
                "original": original,
                "schema": schema,
            }
        )
    if len(entries) != UNTOUCHED_EPISODE_COUNT:
        raise CanonicalV31Epoch9SplitRecoveryError(
            "epoch-9 untouched episode count drifted"
        )
    return entries


def _prepare_turns(root: Path, *, write: bool) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    turns: list[dict[str, Any]] = []
    episodes: list[dict[str, Any]] = []
    epoch8_contract = _load_object(
        EPOCH8_ROOT / epoch8.CONTRACT_FILENAME, label="epoch-8 contract"
    )
    repair_source_turn = epoch8_contract["turns"][0]
    repair_source_manifest_path = _verify_record(
        repair_source_turn["turn_manifest"], label="epoch-8 repair source manifest"
    )
    repair_source_manifest = _load_object(
        repair_source_manifest_path, label="epoch-8 repair source manifest"
    )
    repair_compact = _load_object(
        _verify_record(
            repair_source_manifest["compact_input"], label="epoch-8 compact input"
        ),
        label="epoch-8 compact input",
    )
    repair_original_record = copy.deepcopy(repair_source_manifest["input"])
    repair_combined_schema_record = copy.deepcopy(
        repair_source_manifest["output_schema"]
    )
    repair_combined_schema = _load_object(
        _verify_record(repair_combined_schema_record, label="epoch-8 combined schema"),
        label="epoch-8 combined schema",
    )
    diagnostic = _validate_epoch8_diagnostic()
    invalid_ids = list(diagnostic["invalid_reference_segment_ids"])
    repair_rows = [
        copy.deepcopy(row)
        for row in repair_compact["reference_regeneration"]
        if row["segment_id"] in invalid_ids
    ]
    if [row["segment_id"] for row in repair_rows] != invalid_ids:
        raise CanonicalV31Epoch9SplitRecoveryError(
            "epoch-8 repair source membership drifted"
        )
    repair_episode_id = str(repair_compact["episode_id"])
    repair_input = {
        "schema_version": "pif_canonical_v31_epoch9_epoch8_label_repair_input_v1",
        "episode_id": repair_episode_id,
        "original_context": copy.deepcopy(repair_compact["original_context"]),
        "original_context_sha256": repair_compact["original_context_sha256"],
        "excluded_source_context": copy.deepcopy(
            diagnostic["valid_context_output"]["excluded_source_context"]
        ),
        "reference_regeneration": repair_rows,
        "invalid_reference_segment_ids": invalid_ids,
        "preserved_epoch8_valid_reference_segment_ids": [
            row["segment_id"] for row in diagnostic["valid_reference_labels"]
        ],
        "privacy": "private two-segment canonical exactness regeneration",
    }
    repair_schema = _label_schema(
        repair_combined_schema,
        episode_id=repair_episode_id,
        segment_ids=invalid_ids,
    )
    repair_prompt = (
        "# Private checksum-bound two-segment label packet\n"
        + _canonical_json(repair_input)
        + "\n"
    )
    repair_root = root / PREPARED_DIRECTORY / "epoch8-two-label-repair"
    repair_paths = {
        "input": repair_root / "input.private.json",
        "prompt": repair_root / "prompt.private.txt",
        "base": repair_root / "base-instructions.txt",
        "schema": repair_root / "output-schema.json",
        "combined_schema": repair_root / "combined-output-schema.json",
        "manifest": repair_root / "turn-manifest.json",
    }
    if write:
        repair_root.mkdir(parents=True, exist_ok=False)
        _write_json(repair_paths["input"], repair_input)
        _write_bytes(repair_paths["prompt"], repair_prompt.encode("ascii"))
        _write_bytes(repair_paths["base"], LABEL_BASE_INSTRUCTIONS.encode("ascii"))
        _write_json(repair_paths["schema"], repair_schema)
        _write_json(repair_paths["combined_schema"], repair_combined_schema)
    elif (
        _load_object(repair_paths["input"], label="epoch-8 repair input")
        != repair_input
        or repair_paths["prompt"].read_bytes() != repair_prompt.encode("ascii")
        or repair_paths["base"].read_bytes()
        != LABEL_BASE_INSTRUCTIONS.encode("ascii")
        or _load_object(repair_paths["schema"], label="epoch-8 repair schema")
        != repair_schema
        or _load_object(
            repair_paths["combined_schema"], label="epoch-8 combined schema"
        )
        != repair_combined_schema
    ):
        raise CanonicalV31Epoch9SplitRecoveryError(
            "epoch-8 two-label repair artifacts drifted"
        )
    repair_turn_id = _turn_id(
        episode_id=repair_episode_id,
        stage="repair_labels",
        static_input=repair_input,
    )
    repair_manifest = {
        "schema_version": TURN_MANIFEST_VERSION,
        "architecture": "two_epoch8_invalid_labels_from_exact_source_segments",
        "ordinal": 0,
        "episode_ordinal": 1,
        "stage": "repair_labels",
        "turn_id": repair_turn_id,
        "episode_id": repair_episode_id,
        "source_epoch8_turn": copy.deepcopy(repair_source_turn),
        "source_epoch8_manifest": _record(repair_source_manifest_path),
        "original_authority_input": repair_original_record,
        "input": _record(repair_paths["input"]),
        "prompt": _record(repair_paths["prompt"]),
        "base_instructions": _record(repair_paths["base"]),
        "output_schema": _record(repair_paths["schema"]),
        "combined_output_schema": _record(repair_paths["combined_schema"]),
        "invalid_reference_segment_ids": invalid_ids,
        "adopted_epoch8_valid_context_output": _record(
            root / ADOPTED_DIRECTORY / "epoch8-valid-context-output.private.json"
        ),
        "adopted_epoch8_valid_reference_labels": _record(
            root / ADOPTED_DIRECTORY / "epoch8-five-valid-labels.private.json"
        ),
        "model_visible_legacy_reference_label_count": 0,
        "model_visible_validation_diagnostic_count": 0,
        "semantic_retry_count": 0,
    }
    if write:
        _write_json(repair_paths["manifest"], repair_manifest)
    elif _load_object(repair_paths["manifest"], label="repair turn manifest") != repair_manifest:
        raise CanonicalV31Epoch9SplitRecoveryError("repair turn manifest drifted")
    turns.append(
        {
            "ordinal": 0,
            "episode_ordinal": 1,
            "stage": "repair_labels",
            "turn_id": repair_turn_id,
            "episode_id": repair_episode_id,
            "turn_manifest": _record(repair_paths["manifest"]),
        }
    )
    ordinal = 1
    for episode_ordinal, entry in enumerate(_source_entries()):
        compact = entry["compact"]
        episode_id = str(compact["episode_id"])
        episode_root = root / PREPARED_DIRECTORY / episode_id
        context_input = {
            "schema_version": CONTEXT_INPUT_VERSION,
            "episode_id": episode_id,
            "episode_metadata": copy.deepcopy(compact["episode_metadata"]),
            "full_segmented_episode_text": compact["full_segmented_episode_text"],
            "original_context": copy.deepcopy(compact["original_context"]),
            "original_context_sha256": compact["original_context_sha256"],
            "missing_context_fields": ["excluded_source_context"],
            "privacy": "private complete episode source",
        }
        label_static_input = {
            "schema_version": LABEL_STATIC_INPUT_VERSION,
            "episode_id": episode_id,
            "original_context": copy.deepcopy(compact["original_context"]),
            "original_context_sha256": compact["original_context_sha256"],
            "reference_regeneration": copy.deepcopy(compact["reference_regeneration"]),
            "invalid_reference_segment_ids": copy.deepcopy(
                compact["invalid_reference_segment_ids"]
            ),
            "preserved_valid_reference_segment_ids": copy.deepcopy(
                compact["preserved_valid_reference_segment_ids"]
            ),
            "privacy": "private six-segment canonical reference regeneration",
        }
        context_schema = _context_schema(entry["schema"], episode_id=episode_id)
        label_schema = _label_schema(entry["schema"], episode_id=episode_id)
        context_prompt = (
            "# Private checksum-bound complete episode source\n"
            + _canonical_json(context_input)
            + "\n"
        )
        paths = {
            "context_input": episode_root / "context-input.private.json",
            "context_prompt": episode_root / "context-prompt.private.txt",
            "context_base": episode_root / "context-base-instructions.txt",
            "context_schema": episode_root / "context-output-schema.json",
            "label_static_input": episode_root / "label-static-input.private.json",
            "label_base": episode_root / "label-base-instructions.txt",
            "label_schema": episode_root / "label-output-schema.json",
            "combined_schema": episode_root / "combined-output-schema.json",
        }
        if write:
            episode_root.mkdir(parents=True, exist_ok=False)
            _write_json(paths["context_input"], context_input)
            _write_bytes(paths["context_prompt"], context_prompt.encode("ascii"))
            _write_bytes(paths["context_base"], CONTEXT_BASE_INSTRUCTIONS.encode("ascii"))
            _write_json(paths["context_schema"], context_schema)
            _write_json(paths["label_static_input"], label_static_input)
            _write_bytes(paths["label_base"], LABEL_BASE_INSTRUCTIONS.encode("ascii"))
            _write_json(paths["label_schema"], label_schema)
            _write_json(paths["combined_schema"], entry["schema"])
        else:
            if (
                _load_object(paths["context_input"], label="context input") != context_input
                or paths["context_prompt"].read_bytes() != context_prompt.encode("ascii")
                or paths["context_base"].read_bytes()
                != CONTEXT_BASE_INSTRUCTIONS.encode("ascii")
                or _load_object(paths["context_schema"], label="context schema")
                != context_schema
                or _load_object(paths["label_static_input"], label="label static input")
                != label_static_input
                or paths["label_base"].read_bytes()
                != LABEL_BASE_INSTRUCTIONS.encode("ascii")
                or _load_object(paths["label_schema"], label="label schema")
                != label_schema
                or _load_object(paths["combined_schema"], label="combined schema")
                != entry["schema"]
            ):
                raise CanonicalV31Epoch9SplitRecoveryError(
                    "epoch-9 prepared episode artifacts drifted"
                )

        context_turn_id = _turn_id(
            episode_id=episode_id, stage="context", static_input=context_input
        )
        label_turn_id = _turn_id(
            episode_id=episode_id, stage="labels", static_input=label_static_input
        )
        context_manifest_path = episode_root / "context-turn-manifest.json"
        label_manifest_path = episode_root / "label-turn-manifest.json"
        context_manifest = {
            "schema_version": TURN_MANIFEST_VERSION,
            "architecture": "episode_context_from_complete_episode_source",
            "ordinal": ordinal,
            "episode_ordinal": episode_ordinal,
            "stage": "context",
            "turn_id": context_turn_id,
            "episode_id": episode_id,
            "source_epoch8_turn": copy.deepcopy(entry["source_turn"]),
            "source_epoch8_manifest": copy.deepcopy(entry["source_manifest"]),
            "original_authority_input": copy.deepcopy(entry["original_input"]),
            "input": _record(paths["context_input"]),
            "prompt": _record(paths["context_prompt"]),
            "base_instructions": _record(paths["context_base"]),
            "output_schema": _record(paths["context_schema"]),
            "combined_output_schema": _record(paths["combined_schema"]),
            "model_visible_legacy_reference_label_count": 0,
            "semantic_retry_count": 0,
        }
        ordinal += 1
        label_manifest = {
            "schema_version": TURN_MANIFEST_VERSION,
            "architecture": "canonical_labels_from_six_exact_source_segments",
            "ordinal": ordinal,
            "episode_ordinal": episode_ordinal,
            "stage": "labels",
            "turn_id": label_turn_id,
            "episode_id": episode_id,
            "source_epoch8_turn": copy.deepcopy(entry["source_turn"]),
            "source_epoch8_manifest": copy.deepcopy(entry["source_manifest"]),
            "original_authority_input": copy.deepcopy(entry["original_input"]),
            "static_input": _record(paths["label_static_input"]),
            "dynamic_input_relative_path": (
                f"{TURNS_DIRECTORY}/{label_turn_id}/input.private.json"
            ),
            "dynamic_prompt_relative_path": (
                f"{TURNS_DIRECTORY}/{label_turn_id}/prompt.private.txt"
            ),
            "base_instructions": _record(paths["label_base"]),
            "output_schema": _record(paths["label_schema"]),
            "combined_output_schema": _record(paths["combined_schema"]),
            "context_turn_id": context_turn_id,
            "model_visible_legacy_reference_label_count": 0,
            "model_visible_validation_diagnostic_count": 0,
            "semantic_retry_count": 0,
        }
        ordinal += 1
        if write:
            _write_json(context_manifest_path, context_manifest)
            _write_json(label_manifest_path, label_manifest)
        elif (
            _load_object(context_manifest_path, label="context turn manifest")
            != context_manifest
            or _load_object(label_manifest_path, label="label turn manifest")
            != label_manifest
        ):
            raise CanonicalV31Epoch9SplitRecoveryError(
                "epoch-9 turn manifest drifted"
            )
        context_turn = {
            "ordinal": context_manifest["ordinal"],
            "episode_ordinal": episode_ordinal,
            "stage": "context",
            "turn_id": context_turn_id,
            "episode_id": episode_id,
            "turn_manifest": _record(context_manifest_path),
        }
        label_turn = {
            "ordinal": label_manifest["ordinal"],
            "episode_ordinal": episode_ordinal,
            "stage": "labels",
            "turn_id": label_turn_id,
            "episode_id": episode_id,
            "turn_manifest": _record(label_manifest_path),
        }
        turns.extend([context_turn, label_turn])
        episodes.append(
            {
                "episode_ordinal": episode_ordinal,
                "episode_id": episode_id,
                "source_epoch8_turn": copy.deepcopy(entry["source_turn"]),
                "source_epoch8_manifest": copy.deepcopy(entry["source_manifest"]),
                "original_authority_input": copy.deepcopy(entry["original_input"]),
                "combined_output_schema": _record(paths["combined_schema"]),
                "context_turn_id": context_turn_id,
                "label_turn_id": label_turn_id,
            }
        )
    if len(turns) != NEW_TURN_COUNT or [turn["ordinal"] for turn in turns] != list(
        range(NEW_TURN_COUNT)
    ):
        raise CanonicalV31Epoch9SplitRecoveryError("epoch-9 turn order drifted")
    return turns, episodes


def _adopted_evidence(root: Path, *, write: bool) -> dict[str, Any]:
    try:
        epoch7_receipt = epoch7.verify_execution_receipt(EPOCH7_ROOT)
    except epoch7.CanonicalV31Epoch7AuthorityRuntimeError as exc:
        raise CanonicalV31Epoch9SplitRecoveryError(str(exc)) from exc
    if (
        epoch7_receipt.get("completed_validated_turn_count") != 1
        or epoch7_receipt.get("semantic_retry_count") != 0
        or epoch7_receipt.get("measured_usage", {}).get("total_tokens")
        != EPOCH7_ADOPTED_TOTAL_TOKENS
    ):
        raise CanonicalV31Epoch9SplitRecoveryError(
            "epoch-7 adopted authority witness drifted"
        )
    epoch7_result_path = _verify_record(
        epoch7_receipt["turn_results"][0], label="epoch-7 turn result"
    )
    epoch7_result = _load_object(epoch7_result_path, label="epoch-7 turn result")
    epoch7_validated_path = _verify_record(
        epoch7_result["validated_output"], label="epoch-7 validated output"
    )
    epoch7_validated = _load_object(
        epoch7_validated_path, label="epoch-7 validated output"
    )
    epoch8_diagnostic = _validate_epoch8_diagnostic()

    adopted_root = root / ADOPTED_DIRECTORY
    epoch7_copy = adopted_root / "epoch7-validated-output.private.json"
    epoch8_context_copy = adopted_root / "epoch8-valid-context-output.private.json"
    epoch8_labels_copy = adopted_root / "epoch8-five-valid-labels.private.json"
    epoch8_rejection = adopted_root / "epoch8-accounting-rejection.json"
    if write:
        adopted_root.mkdir(parents=True, exist_ok=False)
        _write_json(epoch7_copy, epoch7_validated)
        _write_json(epoch8_context_copy, epoch8_diagnostic["valid_context_output"])
        _write_json(
            epoch8_labels_copy,
            {"labels": epoch8_diagnostic["valid_reference_labels"]},
        )
        rejection_payload = copy.deepcopy(epoch8_diagnostic)
        rejection_payload["valid_context_output"] = _record(epoch8_context_copy)
        rejection_payload["valid_reference_labels"] = _record(epoch8_labels_copy)
        _write_json(epoch8_rejection, rejection_payload)
    else:
        if (
            _load_object(epoch7_copy, label="adopted epoch-7 output")
            != epoch7_validated
            or _load_object(epoch8_context_copy, label="adopted epoch-8 context")
            != epoch8_diagnostic["valid_context_output"]
            or _load_object(epoch8_labels_copy, label="adopted epoch-8 labels")
            != {"labels": epoch8_diagnostic["valid_reference_labels"]}
        ):
            raise CanonicalV31Epoch9SplitRecoveryError(
                "adopted authority output drifted"
            )
        expected_rejection = copy.deepcopy(epoch8_diagnostic)
        expected_rejection["valid_context_output"] = _record(epoch8_context_copy)
        expected_rejection["valid_reference_labels"] = _record(epoch8_labels_copy)
        if (
            _load_object(epoch8_rejection, label="epoch-8 accounting rejection")
            != expected_rejection
        ):
            raise CanonicalV31Epoch9SplitRecoveryError(
                "epoch-8 accounting rejection drifted"
            )
    return {
        "epoch7_receipt": _record(EPOCH7_ROOT / epoch7.EXECUTION_RECEIPT_FILENAME),
        "epoch7_turn_result": _record(epoch7_result_path),
        "epoch7_validated_output": _record(epoch7_copy),
        "epoch8_accounting_rejection": _record(epoch8_rejection),
        "epoch8_valid_context_output": _record(epoch8_context_copy),
        "epoch8_valid_reference_labels": _record(epoch8_labels_copy),
        "epoch8_invalid_reference_segment_ids": copy.deepcopy(
            epoch8_diagnostic["invalid_reference_segment_ids"]
        ),
        "epoch8_sidecar": copy.deepcopy(epoch8_diagnostic["epoch8_sidecar"]),
        "epoch8_full_thread_total_usage": copy.deepcopy(
            epoch8_diagnostic["full_thread_total_usage"]
        ),
    }


def _build_contract(root: Path, *, write: bool) -> dict[str, Any]:
    _verify_directive()
    adopted = _adopted_evidence(root, write=write)
    turns, episodes = _prepare_turns(root, write=write)
    epoch7_receipt = _load_object(
        EPOCH7_ROOT / epoch7.EXECUTION_RECEIPT_FILENAME,
        label="epoch-7 execution receipt",
    )
    epoch7_contract = _load_object(
        _verify_record(epoch7_receipt["runtime_contract"], label="epoch-7 contract"),
        label="epoch-7 contract",
    )
    epoch8_contract = _load_object(
        EPOCH8_ROOT / epoch8.CONTRACT_FILENAME, label="epoch-8 contract"
    )
    quota_rate = int(epoch8_contract["quota_points_per_million_tokens"])
    return {
        "schema_version": CONTRACT_VERSION,
        "state": "frozen_split_authority_recovery_ready",
        "directive": _record(DIRECTIVE_PATH),
        "epoch7_root": str(EPOCH7_ROOT.resolve()),
        "epoch8_root": str(EPOCH8_ROOT.resolve()),
        "adopted_evidence": adopted,
        "authority_plan_receipt": copy.deepcopy(
            epoch7_contract["authority_plan_receipt"]
        ),
        "authority_plan": copy.deepcopy(epoch7_contract["authority_plan"]),
        "source_authority_turns": copy.deepcopy(epoch7_contract["turns"]),
        "turns": turns,
        "episodes": episodes,
        "model": MODEL,
        "effort": EFFORT,
        "exact_turn_count": NEW_TURN_COUNT,
        "maximum_total_tokens_per_turn": NEW_PER_TURN_THREAD_TOTAL_TOKEN_CAP,
        "phase_total_token_bound": NEW_TOTAL_TOKEN_CAP,
        "adopted_development_qa_total_tokens": ADOPTED_DEVELOPMENT_QA_TOTAL_TOKENS,
        "cumulative_development_qa_total_token_bound": (
            CUMULATIVE_DEVELOPMENT_QA_TOTAL_TOKEN_CAP
        ),
        "quota_points_per_million_tokens": quota_rate,
        "projected_phase_quota_points": math.ceil(
            NEW_TOTAL_TOKEN_CAP * quota_rate / 1_000_000
        ),
        "minimum_remaining_reserve_percent": int(
            epoch8_contract["minimum_remaining_reserve_percent"]
        ),
        "capacity_safety_margin_percent": int(
            epoch8_contract["capacity_safety_margin_percent"]
        ),
        "maximum_wall_seconds_per_turn": MAXIMUM_WALL_SECONDS_PER_TURN,
        "phase_wall_seconds_ceiling": (
            NEW_TURN_COUNT * MAXIMUM_WALL_SECONDS_PER_TURN
        ),
        "operator_wall_safety_margin_seconds": int(
            epoch8_contract["operator_wall_safety_margin_seconds"]
        ),
        "full_thread_total_usage_is_accounting_authority": True,
        "last_usage_increment_is_diagnostic_only": True,
        "semantic_retry_count": 0,
        "official_persistent_codex_app_server_only": True,
        "managed_chatgpt_auth_only": True,
        "managed_chatgpt_plan_type": "pro",
        "quality_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }


def _runtime_lock(root: Path, contract: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": RUNTIME_LOCK_VERSION,
        "state": "frozen_zero_new_calls",
        "recovery_runtime_module": _record(Path(__file__)),
        "epoch8_recovery_runtime_module": _record(Path(epoch8.__file__)),
        "epoch7_authority_runtime_module": _record(Path(epoch7.__file__)),
        "authority_plan_module": _record(Path(authority_plan.__file__)),
        "canonical_adapter_module": _record(Path(adapter.__file__)),
        "official_app_server_transport_module": _record(
            Path(codex_app_server.__file__)
        ),
        "directive": _record(DIRECTIVE_PATH),
        "runtime_contract": _record(root / CONTRACT_FILENAME),
        "adopted_evidence": copy.deepcopy(contract["adopted_evidence"]),
        "turn_manifests": [
            copy.deepcopy(turn["turn_manifest"]) for turn in contract["turns"]
        ],
        "pinned_official_codex_binary": _record(adapter.PINNED_CODEX),
        "pinned_protocol_schema": _record(codex_app_server.PROTOCOL_SCHEMA_PATH),
        "pinned_codex_cli_version": codex_app_server.PINNED_CODEX_CLI_VERSION,
        "canonical_adapter_binding": adapter.build_six_arm_matrix_binding(),
        "effective_instruction_sources": adapter.expected_instruction_source_contract(),
        "trusted_client_factory": "canonical_adapter._client_factory",
        "trusted_capacity_parser": (
            "app_server_canonical_v31_development_matrix_runtime."
            "parse_trusted_rate_limit_capacity"
        ),
        "model": MODEL,
        "effort": EFFORT,
        "new_turn_count": NEW_TURN_COUNT,
        "new_per_turn_thread_total_token_cap": (
            NEW_PER_TURN_THREAD_TOTAL_TOKEN_CAP
        ),
        "new_total_token_cap": NEW_TOTAL_TOKEN_CAP,
        "full_thread_total_usage_is_accounting_authority": True,
        "semantic_retry_count": 0,
        "official_persistent_codex_app_server_only": True,
        "managed_chatgpt_auth_only": True,
        "api_key_and_raw_session_token_forbidden": True,
        "codex_exec_forbidden": True,
        "quality_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }


def _preauthorization(root: Path, contract: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": PREAUTHORIZATION_VERSION,
        "state": "verified_zero_new_calls_ready_for_direct_authorization",
        "output_root": str(root.resolve()),
        "directive": _record(DIRECTIVE_PATH),
        "runtime_contract": _record(root / CONTRACT_FILENAME),
        "runtime_lock": _record(root / RUNTIME_LOCK_FILENAME),
        "adopted_evidence": copy.deepcopy(contract["adopted_evidence"]),
        "prepared_turn_manifests": [
            copy.deepcopy(turn["turn_manifest"]) for turn in contract["turns"]
        ],
        "adopted_semantic_app_server_turn_count": 2,
        "adopted_development_qa_total_tokens": ADOPTED_DEVELOPMENT_QA_TOTAL_TOKENS,
        "new_semantic_model_call_count": 0,
        "new_semantic_retry_count": 0,
        "quality_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def _authorization(
    root: Path,
    *,
    operator_authorization_id: str,
    issued_at: str,
    expires_at: str,
) -> dict[str, Any]:
    issued = _parse_timestamp(issued_at, label="authorization issued_at")
    expires = _parse_timestamp(expires_at, label="authorization expires_at")
    if (
        not operator_authorization_id.startswith("kolby-")
        or expires <= issued
        or (expires - issued).total_seconds() > AUTHORIZATION_WINDOW_MAX_SECONDS
    ):
        raise CanonicalV31Epoch9SplitRecoveryError(
            "operator authorization window or identity is invalid"
        )
    return {
        "schema_version": AUTHORIZATION_VERSION,
        "state": "authorized_for_one_repair_plus_two_untouched_episode_turn_pairs",
        "authority": "direct_user_instruction",
        "authorized_by": "kolby",
        "operator_authorization_id": operator_authorization_id,
        "operator_authorization_statement_sha256": (
            AUTHORIZATION_STATEMENT_SHA256
        ),
        "issued_at": issued.isoformat(),
        "expires_at": expires.isoformat(),
        "authorized_scope": (
            "one_two_label_repair_plus_context_then_labels_for_two_untouched_episodes"
        ),
        "runtime_contract": _record(root / CONTRACT_FILENAME),
        "runtime_lock": _record(root / RUNTIME_LOCK_FILENAME),
        "preauthorization_receipt": _record(root / PREAUTHORIZATION_FILENAME),
        "model": MODEL,
        "effort": EFFORT,
        "new_model_call_cap": NEW_TURN_COUNT,
        "new_total_token_cap": NEW_TOTAL_TOKEN_CAP,
        "new_per_turn_thread_total_token_cap": (
            NEW_PER_TURN_THREAD_TOTAL_TOKEN_CAP
        ),
        "semantic_retry_count": 0,
        "caller_supplied_capacity_admission_allowed": False,
        "managed_chatgpt_auth_only": True,
        "managed_chatgpt_plan_type": "pro",
        "quality_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }


def freeze_recovery(
    *,
    root: Path = DEFAULT_ROOT,
    operator_authorization_id: str,
    issued_at: str,
    expires_at: str,
) -> dict[str, Any]:
    output_root = root.expanduser().resolve()
    if output_root.exists():
        raise CanonicalV31Epoch9SplitRecoveryError(
            "epoch-9 recovery root must be fresh and absent"
        )
    output_root.mkdir(parents=True)
    contract = _build_contract(output_root, write=True)
    _write_json(output_root / CONTRACT_FILENAME, contract)
    _write_json(output_root / RUNTIME_LOCK_FILENAME, _runtime_lock(output_root, contract))
    _write_json(
        output_root / PREAUTHORIZATION_FILENAME,
        _preauthorization(output_root, contract),
    )
    _write_json(
        output_root / AUTHORIZATION_FILENAME,
        _authorization(
            output_root,
            operator_authorization_id=operator_authorization_id,
            issued_at=issued_at,
            expires_at=expires_at,
        ),
    )
    verify_preauthorization(output_root)
    verify_authorization(
        output_root,
        expected_authorization_id=operator_authorization_id,
        require_current=True,
    )
    return status_recovery(output_root)


def verify_preauthorization(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    output_root = root.expanduser().resolve()
    contract = _load_object(output_root / CONTRACT_FILENAME, label="runtime contract")
    if contract != _build_contract(output_root, write=False):
        raise CanonicalV31Epoch9SplitRecoveryError("epoch-9 runtime contract drifted")
    lock = _load_object(output_root / RUNTIME_LOCK_FILENAME, label="runtime lock")
    if lock != _runtime_lock(output_root, contract):
        raise CanonicalV31Epoch9SplitRecoveryError("epoch-9 runtime lock drifted")
    preauthorization = _load_object(
        output_root / PREAUTHORIZATION_FILENAME, label="preauthorization receipt"
    )
    if preauthorization != _preauthorization(output_root, contract):
        raise CanonicalV31Epoch9SplitRecoveryError(
            "epoch-9 preauthorization receipt drifted"
        )
    return copy.deepcopy(preauthorization)


def verify_authorization(
    root: Path = DEFAULT_ROOT,
    *,
    expected_authorization_id: str | None,
    require_current: bool,
) -> dict[str, Any]:
    verify_preauthorization(root)
    output_root = root.expanduser().resolve()
    observed = _load_object(
        output_root / AUTHORIZATION_FILENAME, label="operator authorization"
    )
    authorization_id = observed.get("operator_authorization_id")
    issued_at = observed.get("issued_at")
    expires_at = observed.get("expires_at")
    if not all(
        isinstance(value, str)
        for value in (authorization_id, issued_at, expires_at)
    ):
        raise CanonicalV31Epoch9SplitRecoveryError(
            "operator authorization fields are absent"
        )
    expected = _authorization(
        output_root,
        operator_authorization_id=str(authorization_id),
        issued_at=str(issued_at),
        expires_at=str(expires_at),
    )
    if observed != expected or (
        expected_authorization_id is not None
        and authorization_id != expected_authorization_id
    ):
        raise CanonicalV31Epoch9SplitRecoveryError("operator authorization drifted")
    if require_current and not (
        _parse_timestamp(issued_at, label="authorization issued_at")
        <= _now()
        < _parse_timestamp(expires_at, label="authorization expires_at")
    ):
        raise CanonicalV31Epoch9SplitRecoveryWaiting(
            "operator authorization is not current"
        )
    return copy.deepcopy(observed)


def _turn_paths(root: Path, turn_id: str) -> dict[str, Path]:
    paths = epoch7._turn_paths(root, turn_id)  # noqa: SLF001
    paths.update(
        {
            "dynamic_input": paths["root"] / "input.private.json",
            "dynamic_prompt": paths["root"] / "prompt.private.txt",
            "combined_output": paths["root"] / "combined-output.private.json",
        }
    )
    return paths


def _manifest(turn: Mapping[str, Any]) -> dict[str, Any]:
    return _load_object(
        _verify_record(turn["turn_manifest"], label="authority turn manifest"),
        label="authority turn manifest",
    )


def _turn_by_id(contract: Mapping[str, Any], turn_id: str) -> dict[str, Any]:
    matches = [turn for turn in contract["turns"] if turn["turn_id"] == turn_id]
    if len(matches) != 1:
        raise CanonicalV31Epoch9SplitRecoveryError("authority turn lookup drifted")
    return copy.deepcopy(matches[0])


def _materialize_label_input(
    root: Path,
    contract: Mapping[str, Any],
    turn: Mapping[str, Any],
    *,
    write: bool,
) -> tuple[Path, Path, dict[str, Any]]:
    manifest = _manifest(turn)
    if manifest.get("stage") != "labels":
        raise CanonicalV31Epoch9SplitRecoveryError("label turn stage drifted")
    context_turn = _turn_by_id(contract, str(manifest["context_turn_id"]))
    context_result_path = _turn_paths(root, str(context_turn["turn_id"]))["result"]
    context_result = _load_object(context_result_path, label="context turn result")
    context_output = _load_object(
        _verify_record(context_result["validated_output"], label="context output"),
        label="context output",
    )
    static_input = _load_object(
        _verify_record(manifest["static_input"], label="label static input"),
        label="label static input",
    )
    dynamic = {
        **copy.deepcopy(static_input),
        "schema_version": "pif_canonical_v31_epoch9_label_dynamic_input_v1",
        "excluded_source_context": copy.deepcopy(
            context_output["excluded_source_context"]
        ),
        "context_turn_result": _record(context_result_path),
        "context_validated_output": copy.deepcopy(context_result["validated_output"]),
    }
    raw = _canonical_json(dynamic)
    if "legacy_reference_label" in raw or "validation_diagnostic" in raw:
        raise CanonicalV31Epoch9SplitRecoveryError(
            "dynamic label input contains forbidden legacy semantics"
        )
    prompt = "# Private checksum-bound segment-only label packet\n" + raw + "\n"
    paths = _turn_paths(root, str(turn["turn_id"]))
    if write:
        paths["root"].mkdir(parents=True, exist_ok=True)
        if paths["dynamic_input"].exists() or paths["dynamic_prompt"].exists():
            if (
                _load_object(paths["dynamic_input"], label="dynamic label input")
                != dynamic
                or paths["dynamic_prompt"].read_bytes() != prompt.encode("ascii")
            ):
                raise CanonicalV31Epoch9SplitRecoveryError(
                    "dynamic label artifacts drifted"
                )
        else:
            _write_json(paths["dynamic_input"], dynamic)
            _write_bytes(paths["dynamic_prompt"], prompt.encode("ascii"))
    else:
        if (
            _load_object(paths["dynamic_input"], label="dynamic label input")
            != dynamic
            or paths["dynamic_prompt"].read_bytes() != prompt.encode("ascii")
        ):
            raise CanonicalV31Epoch9SplitRecoveryError(
                "dynamic label artifacts drifted"
            )
    return paths["dynamic_input"], paths["dynamic_prompt"], dynamic


def _turn_io(
    root: Path,
    contract: Mapping[str, Any],
    turn: Mapping[str, Any],
    *,
    materialize_label: bool,
) -> tuple[Path, Path, Path, Path]:
    manifest = _manifest(turn)
    base_path = _verify_record(manifest["base_instructions"], label="base instructions")
    schema_path = _verify_record(manifest["output_schema"], label="output schema")
    if manifest["stage"] in {"context", "repair_labels"}:
        input_path = _verify_record(manifest["input"], label="context input")
        prompt_path = _verify_record(manifest["prompt"], label="context prompt")
    else:
        input_path, prompt_path, _ = _materialize_label_input(
            root, contract, turn, write=materialize_label
        )
    return input_path, prompt_path, base_path, schema_path


def _validate_turn_output(
    *,
    root: Path,
    contract: Mapping[str, Any],
    turn: Mapping[str, Any],
    raw_output: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    manifest = _manifest(turn)
    schema = _load_object(
        _verify_record(manifest["output_schema"], label="turn output schema"),
        label="turn output schema",
    )
    try:
        adapter._validate_schema(dict(raw_output), schema, "$")  # noqa: SLF001
    except adapter.CanonicalV31OutputError as exc:
        raise CanonicalV31Epoch9SplitRecoveryRejected(
            "split authority output failed its stage schema"
        ) from exc
    episode_id = str(turn["episode_id"])
    if raw_output.get("episode_id") != episode_id:
        raise CanonicalV31Epoch9SplitRecoveryRejected(
            "split authority output episode drifted"
        )
    if manifest["stage"] == "context":
        excluded = raw_output.get("excluded_source_context")
        if not isinstance(excluded, list) or any(
            not isinstance(item, str) or not item for item in excluded
        ):
            raise CanonicalV31Epoch9SplitRecoveryRejected(
                "split authority context output is malformed"
            )
        return copy.deepcopy(dict(raw_output)), None

    if manifest["stage"] == "repair_labels":
        observed_labels = raw_output.get("repaired_reference_labels")
        expected_repair_ids = list(manifest["invalid_reference_segment_ids"])
        if (
            not isinstance(observed_labels, list)
            or [row.get("segment_id") for row in observed_labels]
            != expected_repair_ids
        ):
            raise CanonicalV31Epoch9SplitRecoveryRejected(
                "epoch-8 label repair order or membership drifted"
            )
        adopted_context = _load_object(
            _verify_record(
                manifest["adopted_epoch8_valid_context_output"],
                label="adopted epoch-8 context",
            ),
            label="adopted epoch-8 context",
        )
        adopted_labels = _load_object(
            _verify_record(
                manifest["adopted_epoch8_valid_reference_labels"],
                label="adopted epoch-8 labels",
            ),
            label="adopted epoch-8 labels",
        )
        original_input = _load_object(
            _verify_record(
                manifest["original_authority_input"],
                label="original authority input",
            ),
            label="original authority input",
        )
        combined_schema = _load_object(
            _verify_record(
                manifest["combined_output_schema"], label="combined output schema"
            ),
            label="combined output schema",
        )
        labels_by_id = {
            str(row["segment_id"]): copy.deepcopy(dict(row))
            for row in [*adopted_labels["labels"], *observed_labels]
        }
        expected_all_ids = list(original_input["invalid_reference_segment_ids"])
        if set(labels_by_id) != set(expected_all_ids):
            raise CanonicalV31Epoch9SplitRecoveryRejected(
                "epoch-8 adopted and repaired label partition drifted"
            )
        combined = {
            "schema_version": authority_plan.OUTPUT_VERSION,
            "episode_id": episode_id,
            "excluded_source_context": copy.deepcopy(
                adopted_context["excluded_source_context"]
            ),
            "repaired_reference_labels": [
                labels_by_id[segment_id] for segment_id in expected_all_ids
            ],
        }
        try:
            validated_combined = authority_plan.validate_authority_output(
                combined,
                turn_input=original_input,
                output_schema=combined_schema,
            )
        except authority_plan.CanonicalV31Epoch7InputAuthorityPlanError as exc:
            raise CanonicalV31Epoch9SplitRecoveryRejected(
                "epoch-8 partial adoption plus repaired labels failed the unchanged validator"
            ) from exc
        return copy.deepcopy(dict(raw_output)), validated_combined

    context_turn = _turn_by_id(contract, str(manifest["context_turn_id"]))
    context_result = _load_object(
        _turn_paths(root, str(context_turn["turn_id"]))["result"],
        label="context turn result",
    )
    context_output = _load_object(
        _verify_record(context_result["validated_output"], label="context output"),
        label="context output",
    )
    combined_schema = _load_object(
        _verify_record(manifest["combined_output_schema"], label="combined schema"),
        label="combined schema",
    )
    original_input = _load_object(
        _verify_record(
            manifest["original_authority_input"], label="original authority input"
        ),
        label="original authority input",
    )
    combined = {
        "schema_version": authority_plan.OUTPUT_VERSION,
        "episode_id": episode_id,
        "excluded_source_context": copy.deepcopy(
            context_output["excluded_source_context"]
        ),
        "repaired_reference_labels": copy.deepcopy(
            raw_output["repaired_reference_labels"]
        ),
    }
    try:
        validated_combined = authority_plan.validate_authority_output(
            combined, turn_input=original_input, output_schema=combined_schema
        )
    except authority_plan.CanonicalV31Epoch7InputAuthorityPlanError as exc:
        raise CanonicalV31Epoch9SplitRecoveryRejected(
            "split authority labels failed the unchanged full combined validator"
        ) from exc
    return copy.deepcopy(dict(raw_output)), validated_combined


def _verify_complete_turn(
    *,
    root: Path,
    contract: Mapping[str, Any],
    authorization: Mapping[str, Any],
    turn: Mapping[str, Any],
    completed_turn_count: int,
    allow_finalize: bool,
) -> dict[str, Any]:
    paths = _turn_paths(root, str(turn["turn_id"]))
    expected_attempt = epoch7._attempt_payload(  # noqa: SLF001
        root=root, contract=contract, authorization=authorization, turn=turn
    )
    if _load_object(paths["attempt"], label="semantic attempt") != expected_attempt:
        raise CanonicalV31Epoch9SplitRecoveryError("semantic attempt drifted")
    initial_request = epoch7._capacity_request(  # noqa: SLF001
        root=root,
        contract=contract,
        authorization=authorization,
        turn=turn,
        boundary="initial_before_thread",
        completed_turn_count=completed_turn_count,
    )
    preturn_request = epoch7._capacity_request(  # noqa: SLF001
        root=root,
        contract=contract,
        authorization=authorization,
        turn=turn,
        boundary="preturn_before_turn",
        completed_turn_count=completed_turn_count,
    )
    initial = epoch7._verify_capacity_bundle(  # noqa: SLF001
        paths["initial_capacity"],
        expected_request=initial_request,
        contract=contract,
        authorization=authorization,
        historical=True,
    )
    preturn = epoch7._verify_capacity_bundle(  # noqa: SLF001
        paths["preturn_capacity"],
        expected_request=preturn_request,
        contract=contract,
        authorization=authorization,
        historical=True,
    )
    thread_binding = epoch7._verify_thread_binding(  # noqa: SLF001
        _load_object(paths["thread"], label="thread binding"),
        contract=contract,
        turn=turn,
    )
    input_path, prompt_path, base_path, schema_path = _turn_io(
        root, contract, turn, materialize_label=False
    )
    if not paths["raw_output"].is_file() or not paths["sidecar"].is_file():
        raise CanonicalV31Epoch9SplitRecoveryWaiting(
            "completed split authority evidence is partial"
        )
    telemetry = _validate_completed_sidecar_common(
        sidecar_path=paths["sidecar"],
        output_path=paths["raw_output"],
        prompt_path=prompt_path,
        base_path=base_path,
        schema_path=schema_path,
        thread_binding=thread_binding,
    )
    try:
        raw_output = json.loads(telemetry["raw_message"])
    except json.JSONDecodeError as exc:
        raise CanonicalV31Epoch9SplitRecoveryRejected(
            "split authority output is not structured JSON"
        ) from exc
    if not isinstance(raw_output, Mapping):
        raise CanonicalV31Epoch9SplitRecoveryRejected(
            "split authority output is not an object"
        )
    validated, combined = _validate_turn_output(
        root=root, contract=contract, turn=turn, raw_output=raw_output
    )
    if paths["validated_output"].exists():
        if (
            _load_object(paths["validated_output"], label="validated output")
            != validated
        ):
            raise CanonicalV31Epoch9SplitRecoveryError(
                "validated split authority output drifted"
            )
    elif allow_finalize:
        _write_json(paths["validated_output"], validated)
    else:
        raise CanonicalV31Epoch9SplitRecoveryWaiting(
            "completed split authority output is not finalized"
        )
    if combined is not None:
        if paths["combined_output"].exists():
            if (
                _load_object(paths["combined_output"], label="combined output")
                != combined
            ):
                raise CanonicalV31Epoch9SplitRecoveryError(
                    "combined split authority output drifted"
                )
        elif allow_finalize:
            _write_json(paths["combined_output"], combined)
        else:
            raise CanonicalV31Epoch9SplitRecoveryWaiting(
                "combined split authority output is absent"
            )
    result = {
        "schema_version": TURN_RESULT_VERSION,
        "state": "completed_validated_no_retry",
        "ordinal": turn["ordinal"],
        "stage": turn["stage"],
        "turn_id": turn["turn_id"],
        "episode_id": turn["episode_id"],
        "runtime_contract": _record(root / CONTRACT_FILENAME),
        "runtime_lock": _record(root / RUNTIME_LOCK_FILENAME),
        "operator_authorization": _record(root / AUTHORIZATION_FILENAME),
        "authority_turn_manifest": copy.deepcopy(turn["turn_manifest"]),
        "attempt": _record(paths["attempt"]),
        "initial_capacity": initial["records"],
        "thread_binding": _record(paths["thread"]),
        "preturn_capacity": preturn["records"],
        "input": _record(input_path),
        "prompt": _record(prompt_path),
        "sidecar": _record(paths["sidecar"]),
        "raw_output": _record(paths["raw_output"]),
        "validated_output": _record(paths["validated_output"]),
        "combined_authority_output": (
            _record(paths["combined_output"]) if combined is not None else None
        ),
        "thread_id": telemetry["thread_id"],
        "semantic_turn_id": telemetry["turn_id"],
        "last_inference_increment_usage": telemetry["last_usage"],
        "thread_total_usage": telemetry["thread_total_usage"],
        "usage": telemetry["thread_total_usage"],
        "wall_elapsed_seconds": telemetry["wall_elapsed_seconds"],
        "semantic_model_call_count": 1,
        "semantic_retry_count": 0,
        "quality_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    if paths["result"].exists():
        if _load_object(paths["result"], label="turn result") != result:
            raise CanonicalV31Epoch9SplitRecoveryError("turn result drifted")
    elif allow_finalize:
        _write_json(paths["result"], result)
    else:
        raise CanonicalV31Epoch9SplitRecoveryWaiting("turn result is absent")
    return result


def _turn_state(root: Path, turn: Mapping[str, Any]) -> str:
    paths = _turn_paths(root, str(turn["turn_id"]))
    if not paths["root"].exists():
        return "absent"
    if not paths["root"].is_dir() or paths["root"].is_symlink():
        return "partial"
    if paths["result"].is_file():
        return "complete"
    if paths["sidecar"].is_file() and paths["raw_output"].is_file():
        sidecar = _load_object(paths["sidecar"], label="candidate completed sidecar")
        if sidecar.get("state") == "completed" and sidecar.get("status") == "completed":
            return "recoverable_completed"
    names = {path.name for path in paths["root"].iterdir()}
    if names and names <= {"input.private.json", "prompt.private.txt"}:
        return "prepared_only"
    return "partial"


def _validate_partial_turn(
    *,
    root: Path,
    contract: Mapping[str, Any],
    authorization: Mapping[str, Any],
    turn: Mapping[str, Any],
    completed_turn_count: int,
) -> None:
    paths = _turn_paths(root, str(turn["turn_id"]))
    if paths["result"].exists():
        raise CanonicalV31Epoch9SplitRecoveryError(
            "partial turn unexpectedly has a result"
        )
    if not paths["attempt"].exists():
        if _turn_state(root, turn) == "prepared_only":
            _turn_io(root, contract, turn, materialize_label=False)
            return
        raise CanonicalV31Epoch9SplitRecoveryError(
            "partial turn lacks its semantic attempt"
        )
    expected_attempt = epoch7._attempt_payload(  # noqa: SLF001
        root=root, contract=contract, authorization=authorization, turn=turn
    )
    if _load_object(paths["attempt"], label="partial semantic attempt") != expected_attempt:
        raise CanonicalV31Epoch9SplitRecoveryError("partial semantic attempt drifted")
    input_path, prompt_path, base_path, schema_path = _turn_io(
        root, contract, turn, materialize_label=False
    )
    del input_path
    initial_request = epoch7._capacity_request(  # noqa: SLF001
        root=root,
        contract=contract,
        authorization=authorization,
        turn=turn,
        boundary="initial_before_thread",
        completed_turn_count=completed_turn_count,
    )
    if paths["initial_capacity"].exists():
        epoch7._verify_capacity_bundle(  # noqa: SLF001
            paths["initial_capacity"],
            expected_request=initial_request,
            contract=contract,
            authorization=authorization,
            historical=True,
            require_available=False,
        )
    if paths["thread"].exists():
        thread_binding = epoch7._verify_thread_binding(  # noqa: SLF001
            _load_object(paths["thread"], label="partial thread binding"),
            contract=contract,
            turn=turn,
        )
    else:
        thread_binding = None
    if paths["preturn_capacity"].exists():
        if thread_binding is None:
            raise CanonicalV31Epoch9SplitRecoveryError(
                "preturn capacity exists without a thread binding"
            )
        preturn_request = epoch7._capacity_request(  # noqa: SLF001
            root=root,
            contract=contract,
            authorization=authorization,
            turn=turn,
            boundary="preturn_before_turn",
            completed_turn_count=completed_turn_count,
        )
        epoch7._verify_capacity_bundle(  # noqa: SLF001
            paths["preturn_capacity"],
            expected_request=preturn_request,
            contract=contract,
            authorization=authorization,
            historical=True,
            require_available=False,
        )
    if paths["sidecar"].exists():
        if thread_binding is None:
            raise CanonicalV31Epoch9SplitRecoveryError(
                "partial sidecar exists without a thread binding"
            )
        synthetic_manifest = {
            "prompt": _record(prompt_path),
            "base_instructions": _record(base_path),
            "output_schema": _record(schema_path),
        }
        try:
            epoch7._validate_partial_sidecar(  # noqa: SLF001
                sidecar_path=paths["sidecar"],
                output_path=paths["raw_output"],
                manifest=synthetic_manifest,
                thread_binding=thread_binding,
            )
        except epoch7.CanonicalV31Epoch7AuthorityRuntimeError as exc:
            raise CanonicalV31Epoch9SplitRecoveryError(str(exc)) from exc
    elif paths["raw_output"].exists():
        raise CanonicalV31Epoch9SplitRecoveryError(
            "partial raw output exists without a sidecar"
        )


def _validate_turn_prefix(
    root: Path,
    contract: Mapping[str, Any],
    authorization: Mapping[str, Any],
    *,
    allow_finalize: bool,
) -> tuple[list[dict[str, Any]], list[str]]:
    states = [_turn_state(root, turn) for turn in contract["turns"]]
    completed: list[dict[str, Any]] = []
    open_seen = False
    for index, (turn, state) in enumerate(zip(contract["turns"], states)):
        if state in {"complete", "recoverable_completed"}:
            if open_seen:
                raise CanonicalV31Epoch9SplitRecoveryError(
                    "split authority turns are not a contiguous completed prefix"
                )
            completed.append(
                _verify_complete_turn(
                    root=root,
                    contract=contract,
                    authorization=authorization,
                    turn=turn,
                    completed_turn_count=index,
                    allow_finalize=allow_finalize,
                )
            )
        elif state in {"absent", "prepared_only"}:
            open_seen = True
        else:
            open_seen = True
            _validate_partial_turn(
                root=root,
                contract=contract,
                authorization=authorization,
                turn=turn,
                completed_turn_count=index,
            )
    thread_ids = [row["thread_id"] for row in completed]
    turn_ids = [row["semantic_turn_id"] for row in completed]
    if len(thread_ids) != len(set(thread_ids)) or len(turn_ids) != len(set(turn_ids)):
        raise CanonicalV31Epoch9SplitRecoveryError(
            "split authority semantic IDs are not unique"
        )
    return completed, states


def _terminal_accounting(root: Path, contract: Mapping[str, Any]) -> dict[str, Any]:
    usage_values: list[dict[str, int]] = []
    last_values: list[dict[str, int]] = []
    wall = 0.0
    call_count = 0
    thread_ids: list[str] = []
    semantic_turn_ids: list[str] = []
    for turn in contract["turns"]:
        sidecar_path = _turn_paths(root, str(turn["turn_id"]))["sidecar"]
        if not sidecar_path.is_file():
            continue
        sidecar = _load_object(sidecar_path, label="accounting sidecar")
        if isinstance(sidecar.get("thread_id"), str) and sidecar["thread_id"]:
            thread_ids.append(sidecar["thread_id"])
        if isinstance(sidecar.get("turn_id"), str) and sidecar["turn_id"]:
            semantic_turn_ids.append(sidecar["turn_id"])
            call_count += 1
        if isinstance(sidecar.get("usage"), Mapping):
            last_values.append(_usage(sidecar["usage"], label="last usage"))
        if isinstance(sidecar.get("thread_total_usage"), Mapping):
            usage_values.append(
                _usage(sidecar["thread_total_usage"], label="thread total usage")
            )
        elapsed = sidecar.get("wall_elapsed_seconds")
        if isinstance(elapsed, (int, float)) and not isinstance(elapsed, bool) and elapsed >= 0:
            wall += float(elapsed)
    if len(thread_ids) != len(set(thread_ids)) or len(semantic_turn_ids) != len(
        set(semantic_turn_ids)
    ):
        raise CanonicalV31Epoch9SplitRecoveryError(
            "terminal accounting semantic IDs are not unique"
        )
    measured = _sum_usage(usage_values)
    return {
        "semantic_model_call_count": call_count,
        "semantic_retry_count": 0,
        "measured_usage": measured,
        "last_inference_increment_usage": _sum_usage(last_values),
        "measured_usage_turn_count": len(usage_values),
        "unknown_usage_turn_count": max(0, call_count - len(usage_values)),
        "wall_elapsed_seconds": wall,
        "thread_ids": thread_ids,
        "semantic_turn_ids": semantic_turn_ids,
    }


def _failed_checks(
    *,
    completed: Sequence[Mapping[str, Any]],
    accounting: Mapping[str, Any],
) -> list[str]:
    failed: list[str] = []
    if accounting["semantic_model_call_count"] > NEW_TURN_COUNT:
        failed.append("new_semantic_model_call_cap")
    if accounting["measured_usage"]["total_tokens"] > NEW_TOTAL_TOKEN_CAP:
        failed.append("new_full_thread_total_token_cap")
    if any(
        row["thread_total_usage"]["total_tokens"]
        > NEW_PER_TURN_THREAD_TOTAL_TOKEN_CAP
        for row in completed
    ):
        failed.append("new_per_turn_full_thread_total_token_cap")
    if (
        ADOPTED_DEVELOPMENT_QA_TOTAL_TOKENS
        + accounting["measured_usage"]["total_tokens"]
        > CUMULATIVE_DEVELOPMENT_QA_TOTAL_TOKEN_CAP
    ):
        failed.append("cumulative_development_qa_total_token_cap")
    return failed


def _merge_output(
    root: Path,
    contract: Mapping[str, Any],
    completed: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    label_results = {
        row["episode_id"]: row
        for row in completed
        if row.get("stage") == "labels"
    }
    if len(label_results) != UNTOUCHED_EPISODE_COUNT:
        raise CanonicalV31Epoch9SplitRecoveryError(
            "split authority merge lacks both label results"
        )
    repair_results = [
        row for row in completed if row.get("stage") == "repair_labels"
    ]
    if len(repair_results) != 1 or not isinstance(
        repair_results[0].get("combined_authority_output"), Mapping
    ):
        raise CanonicalV31Epoch9SplitRecoveryError(
            "split authority merge lacks the epoch-8 repair result"
        )
    validated_records: list[dict[str, Any]] = [
        copy.deepcopy(contract["adopted_evidence"]["epoch7_validated_output"]),
        copy.deepcopy(repair_results[0]["combined_authority_output"]),
    ]
    for episode in contract["episodes"]:
        result = label_results.get(str(episode["episode_id"]))
        if not isinstance(result, Mapping) or not isinstance(
            result.get("combined_authority_output"), Mapping
        ):
            raise CanonicalV31Epoch9SplitRecoveryError(
                "split authority episode merge output is absent"
            )
        validated_records.append(copy.deepcopy(result["combined_authority_output"]))
    if len(validated_records) != len(contract["source_authority_turns"]):
        raise CanonicalV31Epoch9SplitRecoveryError(
            "split authority merged turn count drifted"
        )
    synthetic_results = [
        {
            "turn_id": source_turn["turn_id"],
            "validated_output": record,
        }
        for source_turn, record in zip(
            contract["source_authority_turns"], validated_records
        )
    ]
    merge_contract = {
        "authority_plan": copy.deepcopy(contract["authority_plan"]),
        "authority_plan_receipt": copy.deepcopy(contract["authority_plan_receipt"]),
        "turns": copy.deepcopy(contract["source_authority_turns"]),
    }
    try:
        merged = epoch7._build_merged_output(  # noqa: SLF001
            root=root, contract=merge_contract, turn_results=synthetic_results
        )
    except epoch7.CanonicalV31Epoch7AuthorityRuntimeError as exc:
        raise CanonicalV31Epoch9SplitRecoveryError(str(exc)) from exc
    path = root / MERGED_OUTPUT_FILENAME
    if path.exists():
        if _load_object(path, label="merged authority output") != merged:
            raise CanonicalV31Epoch9SplitRecoveryError(
                "merged split authority output drifted"
            )
    else:
        _write_json(path, merged)
    return merged


def _adopted_usage(contract: Mapping[str, Any]) -> dict[str, int]:
    epoch7_receipt = _load_object(
        _verify_record(
            contract["adopted_evidence"]["epoch7_receipt"],
            label="adopted epoch-7 receipt",
        ),
        label="adopted epoch-7 receipt",
    )
    epoch8_rejection = _load_object(
        _verify_record(
            contract["adopted_evidence"]["epoch8_accounting_rejection"],
            label="epoch-8 accounting rejection",
        ),
        label="epoch-8 accounting rejection",
    )
    usage = _sum_usage(
        [
            _usage(epoch7_receipt["measured_usage"], label="epoch-7 adopted usage"),
            _usage(
                epoch8_rejection["full_thread_total_usage"],
                label="epoch-8 adopted full thread usage",
            ),
        ]
    )
    if usage["total_tokens"] != ADOPTED_DEVELOPMENT_QA_TOTAL_TOKENS:
        raise CanonicalV31Epoch9SplitRecoveryError("adopted usage drifted")
    return usage


def _receipt_payload(
    *,
    root: Path,
    contract: Mapping[str, Any],
    state: str,
    reason: str,
    completed: Sequence[Mapping[str, Any]],
    merged: Mapping[str, Any] | None,
) -> dict[str, Any]:
    accounting = _terminal_accounting(root, contract)
    failed = _failed_checks(completed=completed, accounting=accounting)
    if state == "passed" and failed:
        state = "rejected"
        reason = "epoch9_split_authority_cost_contract_rejected"
        merged = None
    adopted_usage = _adopted_usage(contract)
    cumulative_usage = _sum_usage([adopted_usage, accounting["measured_usage"]])
    return {
        "schema_version": RECEIPT_VERSION,
        "state": state,
        "terminal_reason": reason,
        "output_root": str(root.resolve()),
        "directive": _record(DIRECTIVE_PATH),
        "runtime_contract": _record(root / CONTRACT_FILENAME),
        "runtime_lock": _record(root / RUNTIME_LOCK_FILENAME),
        "preauthorization_receipt": _record(root / PREAUTHORIZATION_FILENAME),
        "operator_authorization": _record(root / AUTHORIZATION_FILENAME),
        "adopted_evidence": copy.deepcopy(contract["adopted_evidence"]),
        "new_turn_results": [
            _record(_turn_paths(root, str(turn["turn_id"]))["result"])
            for turn in contract["turns"]
            if _turn_paths(root, str(turn["turn_id"]))["result"].is_file()
        ],
        "adopted_semantic_app_server_turn_count": 2,
        "new_completed_validated_turn_count": len(completed),
        "cumulative_completed_validated_turn_count": 2 + len(completed),
        "new_exact_turn_count": NEW_TURN_COUNT,
        "adopted_measured_usage": adopted_usage,
        "new_measured_usage": copy.deepcopy(accounting["measured_usage"]),
        "cumulative_development_qa_measured_usage": cumulative_usage,
        "last_inference_increment_usage": copy.deepcopy(
            accounting["last_inference_increment_usage"]
        ),
        "new_semantic_model_call_count": accounting["semantic_model_call_count"],
        "cumulative_semantic_app_server_turn_count": (
            2 + accounting["semantic_model_call_count"]
        ),
        "semantic_retry_count": 0,
        "new_measured_usage_turn_count": accounting["measured_usage_turn_count"],
        "new_unknown_usage_turn_count": accounting["unknown_usage_turn_count"],
        "new_wall_elapsed_seconds": accounting["wall_elapsed_seconds"],
        "thread_ids": copy.deepcopy(accounting["thread_ids"]),
        "semantic_turn_ids": copy.deepcopy(accounting["semantic_turn_ids"]),
        "failed_checks": failed,
        "full_thread_total_usage_is_accounting_authority": True,
        "merged_authority_output": (
            _record(root / MERGED_OUTPUT_FILENAME) if merged is not None else None
        ),
        "extraction_plan_rebuild_required": state == "passed",
        "quality_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def _write_terminal(
    *,
    root: Path,
    contract: Mapping[str, Any],
    state: str,
    reason: str,
    completed: Sequence[Mapping[str, Any]],
    merged: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    payload = _receipt_payload(
        root=root,
        contract=contract,
        state=state,
        reason=reason,
        completed=completed,
        merged=merged,
    )
    raw = _pretty_json(payload).encode("ascii")
    _write_bytes(root / RECEIPT_FILENAME, raw)
    _write_bytes(root / TERMINAL_FILENAME, raw)
    return verify_recovery_receipt(root)


def _validated_receipt_from_one_mirror(
    *,
    root: Path,
    contract: Mapping[str, Any],
    source: Path,
    completed: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    observed = _load_object(source, label="single terminal mirror")
    state = observed.get("state")
    reason = observed.get("terminal_reason")
    if state not in {"passed", "rejected", "waiting"} or not isinstance(
        reason, str
    ):
        raise CanonicalV31Epoch9SplitRecoveryError(
            "single terminal mirror is malformed"
        )
    merged = None
    if state == "passed":
        if not (root / MERGED_OUTPUT_FILENAME).is_file():
            raise CanonicalV31Epoch9SplitRecoveryError(
                "single passing mirror lacks merged output"
            )
        merged = _load_object(root / MERGED_OUTPUT_FILENAME, label="merged output")
    expected = _receipt_payload(
        root=root,
        contract=contract,
        state=state,
        reason=reason,
        completed=completed,
        merged=merged,
    )
    if observed != expected:
        raise CanonicalV31Epoch9SplitRecoveryError(
            "single terminal mirror failed verification"
        )
    return observed


def verify_recovery_receipt(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    verify_preauthorization(root)
    output_root = root.expanduser().resolve()
    authorization = verify_authorization(
        output_root, expected_authorization_id=None, require_current=False
    )
    contract = _load_object(output_root / CONTRACT_FILENAME, label="runtime contract")
    completed, states = _validate_turn_prefix(
        output_root, contract, authorization, allow_finalize=False
    )
    receipt_path = output_root / RECEIPT_FILENAME
    terminal_path = output_root / TERMINAL_FILENAME
    if receipt_path.is_file() != terminal_path.is_file():
        source = receipt_path if receipt_path.is_file() else terminal_path
        target = terminal_path if receipt_path.is_file() else receipt_path
        _validated_receipt_from_one_mirror(
            root=output_root,
            contract=contract,
            source=source,
            completed=completed,
        )
        _write_bytes(target, source.read_bytes())
    receipt = _load_object(receipt_path, label="authority recovery receipt")
    terminal = _load_object(terminal_path, label="authority recovery terminal")
    if receipt != terminal or receipt_path.read_bytes() != terminal_path.read_bytes():
        raise CanonicalV31Epoch9SplitRecoveryError(
            "authority recovery terminal mirrors drifted"
        )
    state = receipt.get("state")
    reason = receipt.get("terminal_reason")
    if state not in {"passed", "rejected", "waiting"} or not isinstance(
        reason, str
    ):
        raise CanonicalV31Epoch9SplitRecoveryError(
            "authority recovery receipt state is invalid"
        )
    merged = None
    if state == "passed":
        if states != ["complete"] * NEW_TURN_COUNT:
            raise CanonicalV31Epoch9SplitRecoveryError(
                "passing split authority receipt lacks all completed turns"
            )
        merged = _merge_output(output_root, contract, completed)
    expected = _receipt_payload(
        root=output_root,
        contract=contract,
        state=state,
        reason=reason,
        completed=completed,
        merged=merged,
    )
    if receipt != expected:
        raise CanonicalV31Epoch9SplitRecoveryError(
            "authority recovery receipt drifted"
        )
    if state == "passed" and (
        receipt["failed_checks"]
        or receipt["new_unknown_usage_turn_count"] != 0
        or receipt["merged_authority_output"]
        != _record(output_root / MERGED_OUTPUT_FILENAME)
    ):
        raise CanonicalV31Epoch9SplitRecoveryError(
            "passing split authority receipt lacks complete accounting"
        )
    if state != "passed" and receipt.get("merged_authority_output") is not None:
        raise CanonicalV31Epoch9SplitRecoveryError(
            "nonpassing split authority receipt claims merged output"
        )
    return copy.deepcopy(receipt)


def status_recovery(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    verify_preauthorization(root)
    output_root = root.expanduser().resolve()
    if (output_root / RECEIPT_FILENAME).is_file() or (
        output_root / TERMINAL_FILENAME
    ).is_file():
        receipt = verify_recovery_receipt(output_root)
        return {
            "schema_version": "pif_canonical_v31_epoch9_split_authority_status_v1",
            "state": receipt["state"],
            "reason": receipt["terminal_reason"],
            "new_semantic_model_call_count": receipt[
                "new_semantic_model_call_count"
            ],
            "new_semantic_retry_count": 0,
        }
    authorization = verify_authorization(
        output_root, expected_authorization_id=None, require_current=False
    )
    contract = _load_object(output_root / CONTRACT_FILENAME, label="runtime contract")
    completed, states = _validate_turn_prefix(
        output_root, contract, authorization, allow_finalize=True
    )
    waiting = "partial" in states
    return {
        "schema_version": "pif_canonical_v31_epoch9_split_authority_status_v1",
        "state": "waiting" if waiting else "ready",
        "reason": (
            "split_authority_partial_attempt_preserved_no_replay"
            if waiting
            else "ready_for_one_repair_plus_two_untouched_episode_turn_pairs"
        ),
        "turn_states": states,
        "adopted_semantic_app_server_turn_count": 2,
        "new_completed_turn_count": len(completed),
        "new_semantic_retry_count": 0,
    }


def _reject_external_auth_material() -> None:
    try:
        epoch7._reject_external_auth_material()  # noqa: SLF001
    except epoch7.CanonicalV31Epoch7AuthorityRuntimeError as exc:
        raise CanonicalV31Epoch9SplitRecoveryError(str(exc)) from exc


async def execute_recovery(
    *,
    root: Path = DEFAULT_ROOT,
    operator_authorization_id: str,
) -> dict[str, Any]:
    _reject_external_auth_material()
    verify_preauthorization(root)
    output_root = root.expanduser().resolve()
    if (output_root / RECEIPT_FILENAME).is_file() or (
        output_root / TERMINAL_FILENAME
    ).is_file():
        return verify_recovery_receipt(output_root)
    authorization = verify_authorization(
        output_root,
        expected_authorization_id=operator_authorization_id,
        require_current=True,
    )
    contract = _load_object(output_root / CONTRACT_FILENAME, label="runtime contract")
    try:
        completed, states = _validate_turn_prefix(
            output_root, contract, authorization, allow_finalize=True
        )
    except CanonicalV31Epoch9SplitRecoveryRejected:
        return _write_terminal(
            root=output_root,
            contract=contract,
            state="rejected",
            reason="epoch9_split_authority_structural_rejection",
            completed=[],
        )
    if "partial" in states:
        return _write_terminal(
            root=output_root,
            contract=contract,
            state="waiting",
            reason="epoch9_split_authority_partial_attempt_preserved_no_replay",
            completed=completed,
        )
    if len(completed) == NEW_TURN_COUNT:
        merged = _merge_output(output_root, contract, completed)
        return _write_terminal(
            root=output_root,
            contract=contract,
            state="passed",
            reason="epoch9_split_authority_completed_with_two_predecessor_adoptions",
            completed=completed,
            merged=merged,
        )
    try:
        async with adapter._client_factory() as client:  # noqa: SLF001
            account = getattr(client, "account_summary", None)
            if (
                not isinstance(account, Mapping)
                or account.get("type") != "chatgpt"
                or account.get("plan_type") != "pro"
            ):
                raise CanonicalV31Epoch9SplitRecoveryError(
                    "split authority recovery requires managed ChatGPT Pro auth"
                )
            for index in range(len(completed), NEW_TURN_COUNT):
                verify_preauthorization(output_root)
                verify_authorization(
                    output_root,
                    expected_authorization_id=operator_authorization_id,
                    require_current=True,
                )
                turn = contract["turns"][index]
                paths = _turn_paths(output_root, str(turn["turn_id"]))
                if turn["stage"] == "labels":
                    _materialize_label_input(
                        output_root, contract, turn, write=True
                    )
                else:
                    paths["root"].mkdir(parents=True, exist_ok=False)
                if paths["attempt"].exists():
                    raise CanonicalV31Epoch9SplitRecoveryError(
                        "semantic attempt already exists; replay is forbidden"
                    )
                attempt = epoch7._attempt_payload(  # noqa: SLF001
                    root=output_root,
                    contract=contract,
                    authorization=authorization,
                    turn=turn,
                )
                _write_json(paths["attempt"], attempt)
                initial_request = epoch7._capacity_request(  # noqa: SLF001
                    root=output_root,
                    contract=contract,
                    authorization=authorization,
                    turn=turn,
                    boundary="initial_before_thread",
                    completed_turn_count=index,
                )
                await epoch7._probe_and_publish(  # noqa: SLF001
                    client=client,
                    bundle_root=paths["initial_capacity"],
                    request=initial_request,
                    contract=contract,
                    authorization=authorization,
                )
                _, prompt_path, base_path, schema_path = _turn_io(
                    output_root, contract, turn, materialize_label=False
                )
                thread = await client.start_thread(
                    model=MODEL,
                    base_instructions=base_path.read_text(encoding="ascii"),
                    cwd=PROJECT_ROOT,
                    ephemeral=True,
                )
                binding = epoch7._thread_binding(  # noqa: SLF001
                    thread, contract=contract, turn=turn
                )
                _write_json(paths["thread"], binding)
                preturn_request = epoch7._capacity_request(  # noqa: SLF001
                    root=output_root,
                    contract=contract,
                    authorization=authorization,
                    turn=turn,
                    boundary="preturn_before_turn",
                    completed_turn_count=index,
                )
                await epoch7._probe_and_publish(  # noqa: SLF001
                    client=client,
                    bundle_root=paths["preturn_capacity"],
                    request=preturn_request,
                    contract=contract,
                    authorization=authorization,
                )
                result = await client.run_structured_turn(
                    thread=thread,
                    effort=EFFORT,
                    prompt=prompt_path.read_text(encoding="ascii"),
                    output_schema=_load_object(schema_path, label="output schema"),
                    sidecar_path=paths["sidecar"],
                    output_path=paths["raw_output"],
                    batch_size=1,
                    thread_mode="new_thread",
                    timeout_seconds=float(MAXIMUM_WALL_SECONDS_PER_TURN),
                )
                if result.status_ok is not True:
                    raise CanonicalV31Epoch9SplitRecoveryWaiting(
                        "split authority turn did not complete successfully"
                    )
                completed_result = _verify_complete_turn(
                    root=output_root,
                    contract=contract,
                    authorization=authorization,
                    turn=turn,
                    completed_turn_count=index,
                    allow_finalize=True,
                )
                raw_output = _load_object(paths["raw_output"], label="raw output")
                if (
                    result.thread_id != completed_result["thread_id"]
                    or result.turn_id != completed_result["semantic_turn_id"]
                    or not isinstance(result.output, Mapping)
                    or _canonical_json(result.output) != _canonical_json(raw_output)
                ):
                    raise CanonicalV31Epoch9SplitRecoveryError(
                        "in-memory split authority lineage drifted"
                    )
                completed.append(completed_result)
                accounting = _terminal_accounting(output_root, contract)
                failed = _failed_checks(completed=completed, accounting=accounting)
                if failed:
                    raise CanonicalV31Epoch9SplitRecoveryRejected(
                        "split authority measured full-thread token ceiling was exceeded"
                    )
    except (
        CanonicalV31Epoch9SplitRecoveryRejected,
        codex_app_server.AppServerStructuredOutputError,
    ):
        return _write_terminal(
            root=output_root,
            contract=contract,
            state="rejected",
            reason="epoch9_split_authority_structural_or_cost_rejected",
            completed=completed,
        )
    except (
        CanonicalV31Epoch9SplitRecoveryWaiting,
        epoch7.CanonicalV31Epoch7AuthorityWaiting,
        codex_app_server.AppServerError,
        asyncio.TimeoutError,
    ):
        return _write_terminal(
            root=output_root,
            contract=contract,
            state="waiting",
            reason="epoch9_split_authority_operational_waiting_no_replay",
            completed=completed,
        )
    merged = _merge_output(output_root, contract, completed)
    return _write_terminal(
        root=output_root,
        contract=contract,
        state="passed",
        reason="epoch9_split_authority_completed_with_two_predecessor_adoptions",
        completed=completed,
        merged=merged,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=(
            "python3 -m "
            "research_factory.app_server_canonical_v31_epoch9_split_authority_recovery"
        )
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    freeze = subparsers.add_parser("freeze")
    freeze.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    freeze.add_argument("--operator-authorization-id", required=True)
    freeze.add_argument("--issued-at", required=True)
    freeze.add_argument("--expires-at", required=True)
    status = subparsers.add_parser("status")
    status.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    execute = subparsers.add_parser("execute")
    execute.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    execute.add_argument("--operator-authorization-id", required=True)
    verify = subparsers.add_parser("verify")
    verify.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            payload = freeze_recovery(
                root=args.root,
                operator_authorization_id=args.operator_authorization_id,
                issued_at=args.issued_at,
                expires_at=args.expires_at,
            )
        elif args.command == "status":
            payload = status_recovery(args.root)
        elif args.command == "execute":
            payload = asyncio.run(
                execute_recovery(
                    root=args.root,
                    operator_authorization_id=args.operator_authorization_id,
                )
            )
        else:
            payload = verify_recovery_receipt(args.root)
        print(_pretty_json(payload), end="")
        return 0
    except CanonicalV31Epoch9SplitRecoveryWaiting as exc:
        print(_pretty_json({"ok": False, "waiting": True, "error": str(exc)}), end="")
        return 75
    except (
        CanonicalV31Epoch9SplitRecoveryError,
        CanonicalV31Epoch9SplitRecoveryRejected,
    ) as exc:
        print(_pretty_json({"ok": False, "error": str(exc)}), end="")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
