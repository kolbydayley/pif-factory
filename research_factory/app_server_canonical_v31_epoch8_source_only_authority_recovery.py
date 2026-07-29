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
from . import app_server_canonical_v31_epoch7_input_authority_runtime as predecessor
from . import codex_app_server


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PREDECESSOR_ROOT = (
    PROJECT_ROOT
    / "work"
    / "app-server-development-v2"
    / "unattended-pipeline-v5"
    / "canonical-v31-epoch7-input-authority-runtime-v1"
)
DEFAULT_ROOT = (
    PROJECT_ROOT
    / "work"
    / "app-server-development-v2"
    / "unattended-pipeline-v5"
    / "canonical-v31-epoch8-source-only-authority-recovery-v1"
)
DIRECTIVE_PATH = (
    PROJECT_ROOT
    / "automation"
    / "pif-evaluation-epoch8-source-only-authority-recovery-v8.json"
)
DIRECTIVE_SHA256 = "8c22610e66c7584c385d4b46a744090716dad4551205ac9aa43403be06fb9ab2"

CONTRACT_FILENAME = "runtime-contract.json"
RUNTIME_LOCK_FILENAME = "runtime-lock.json"
PREAUTHORIZATION_FILENAME = "preauthorization-receipt.json"
AUTHORIZATION_FILENAME = "operator-authorization.json"
RECEIPT_FILENAME = "authority-recovery-receipt.json"
TERMINAL_FILENAME = "terminal.json"
MERGED_OUTPUT_FILENAME = predecessor.MERGED_OUTPUT_FILENAME
PREPARED_TURNS_DIRECTORY = "prepared-turns"

CONTRACT_VERSION = "pif_canonical_v31_epoch8_source_only_authority_recovery_contract_v1"
RUNTIME_LOCK_VERSION = "pif_canonical_v31_epoch8_source_only_authority_recovery_runtime_lock_v1"
PREAUTHORIZATION_VERSION = "pif_canonical_v31_epoch8_source_only_authority_recovery_preauthorization_v1"
AUTHORIZATION_VERSION = "pif_canonical_v31_epoch8_source_only_authority_recovery_authorization_v1"
TURN_MANIFEST_VERSION = "pif_canonical_v31_epoch8_source_only_authority_turn_manifest_v1"
COMPACT_INPUT_VERSION = "pif_canonical_v31_epoch8_source_only_authority_input_v1"
RECEIPT_VERSION = "pif_canonical_v31_epoch8_source_only_authority_recovery_receipt_v1"

MODEL = authority_plan.MODEL
EFFORT = authority_plan.EFFORT
ADOPTED_TURN_COUNT = 1
NEW_TURN_COUNT = 3
CUMULATIVE_TURN_COUNT = 4
ADOPTED_TOTAL_TOKENS = 124_404
CUMULATIVE_TOTAL_TOKEN_CAP = 408_000
NEW_TOTAL_TOKEN_CAP = CUMULATIVE_TOTAL_TOKEN_CAP - ADOPTED_TOTAL_TOKENS
NEW_PER_TURN_TOTAL_TOKEN_CAP = NEW_TOTAL_TOKEN_CAP // NEW_TURN_COUNT
MAXIMUM_WALL_SECONDS_PER_TURN = authority_plan.MAXIMUM_WALL_SECONDS_PER_TURN
AUTHORIZATION_WINDOW_MAX_SECONDS = 14_400

AUTHORIZATION_STATEMENT = (
    "You have my full permission to continue. No need to seek out my approval anymore."
)
AUTHORIZATION_STATEMENT_SHA256 = hashlib.sha256(
    AUTHORIZATION_STATEMENT.encode("utf-8")
).hexdigest()

BASE_INSTRUCTIONS = """You are the sole LLM authority for a private development-only source-first canonical reference regeneration. This is not production extraction and not holdout work.

For the one supplied episode, author exactly two things:
1. excluded_source_context: the episode-level source categories or regions that future extraction should exclude because they are non-substantive framing, setup, advertising, credits, navigation, or other non-research context. Base every entry on the complete checksum-bound episode source. Return an empty list only when the source supports that conclusion.
2. repaired_reference_labels: one complete ai_discourse_v3_1 label for each listed invalid development reference, in the exact supplied segment order.

No prior malformed reference labels are present in the model input. Regenerate each label directly from the exact segment source and full schema. Preserve the supplied segment_quality exactly because it is deterministic source provenance. Understand the source semantically. Every evidence string and every nonempty metric raw_text, value, unit, and comparator must be an exact contiguous substring of that event's evidence. Do not use keyword, regex, topic-list, majority-vote, or deterministic semantic rules. Do not alter or reproduce references listed as already valid; deterministic code preserves those exact bytes.

Return only schema-valid JSON. Never reproduce the full episode transcript in the response.
"""


class CanonicalV31Epoch8SourceOnlyRecoveryError(RuntimeError):
    pass


class CanonicalV31Epoch8SourceOnlyRecoveryWaiting(RuntimeError):
    pass


class CanonicalV31Epoch8SourceOnlyRecoveryRejected(RuntimeError):
    pass


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _pretty_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _record(path: Path) -> dict[str, Any]:
    try:
        return predecessor._record(path)  # noqa: SLF001 - checksum-bound helper
    except predecessor.CanonicalV31Epoch7AuthorityRuntimeError as exc:
        raise CanonicalV31Epoch8SourceOnlyRecoveryError(str(exc)) from exc


def _verify_record(value: Any, *, label: str) -> Path:
    try:
        return predecessor._verify_record(value, label=label)  # noqa: SLF001
    except predecessor.CanonicalV31Epoch7AuthorityRuntimeError as exc:
        raise CanonicalV31Epoch8SourceOnlyRecoveryError(str(exc)) from exc


def _load_object(path: Path, *, label: str) -> dict[str, Any]:
    try:
        return predecessor._load_object(path, label=label)  # noqa: SLF001
    except predecessor.CanonicalV31Epoch7AuthorityRuntimeError as exc:
        raise CanonicalV31Epoch8SourceOnlyRecoveryError(str(exc)) from exc


def _write_json(path: Path, value: Any) -> dict[str, Any]:
    try:
        return predecessor._write_immutable_json(path, value)  # noqa: SLF001
    except predecessor.CanonicalV31Epoch7AuthorityRuntimeError as exc:
        raise CanonicalV31Epoch8SourceOnlyRecoveryError(str(exc)) from exc


def _write_bytes(path: Path, value: bytes) -> dict[str, Any]:
    try:
        return predecessor._write_immutable_bytes(path, value)  # noqa: SLF001
    except predecessor.CanonicalV31Epoch7AuthorityRuntimeError as exc:
        raise CanonicalV31Epoch8SourceOnlyRecoveryError(str(exc)) from exc


def _parse_timestamp(value: Any, *, label: str) -> datetime:
    try:
        return predecessor._parse_timestamp(value, label=label)  # noqa: SLF001
    except predecessor.CanonicalV31Epoch7AuthorityRuntimeError as exc:
        raise CanonicalV31Epoch8SourceOnlyRecoveryError(str(exc)) from exc


def _now(value: datetime | None = None) -> datetime:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None:
        raise CanonicalV31Epoch8SourceOnlyRecoveryError(
            "current time is not timezone-aware"
        )
    return current.astimezone(timezone.utc)


def _verify_directive() -> dict[str, Any]:
    record = _record(DIRECTIVE_PATH)
    if record["sha256"] != DIRECTIVE_SHA256:
        raise CanonicalV31Epoch8SourceOnlyRecoveryError(
            "epoch-8 source-only directive drifted"
        )
    directive = _load_object(DIRECTIVE_PATH, label="epoch-8 directive")
    expected = {
        "schema_version": "pif_evaluation_epoch8_source_only_authority_recovery_directive_v1",
        "thread_id": "019f4cf1-c46e-7db3-acd2-bf03c4459a10",
        "plan_epoch": 8,
        "step_id": "canonical_v31_epoch8_source_only_authority_recovery_v8",
        "state": "ready_for_direct_user_authorized_execute",
    }
    for key, value in expected.items():
        if directive.get(key) != value:
            raise CanonicalV31Epoch8SourceOnlyRecoveryError(
                f"epoch-8 directive drifted at {key}"
            )
    execution = directive.get("execution_contract")
    source_only = directive.get("source_only_contract")
    authorization = directive.get("authorization_contract")
    if (
        not isinstance(execution, Mapping)
        or execution.get("model") != MODEL
        or execution.get("effort") != EFFORT
        or execution.get("new_model_call_cap") != NEW_TURN_COUNT
        or execution.get("new_total_token_cap") != NEW_TOTAL_TOKEN_CAP
        or execution.get("new_per_turn_total_token_cap")
        != NEW_PER_TURN_TOTAL_TOKEN_CAP
        or execution.get("cumulative_model_call_cap") != CUMULATIVE_TURN_COUNT
        or execution.get("cumulative_total_token_cap")
        != CUMULATIVE_TOTAL_TOKEN_CAP
        or execution.get("semantic_retry_count") != 0
        or execution.get("official_persistent_codex_app_server_only") is not True
        or execution.get("managed_chatgpt_auth_only") is not True
        or execution.get("managed_chatgpt_plan_type") != "pro"
        or execution.get("caller_supplied_capacity_admission_allowed") is not False
        or execution.get("api_key_auth_allowed") is not False
        or execution.get("raw_session_token_auth_allowed") is not False
        or execution.get("codex_exec_allowed") is not False
        or not isinstance(source_only, Mapping)
        or source_only.get("legacy_reference_label_objects_in_prompt") != 0
        or source_only.get("output_schema_unchanged_from_epoch7") is not True
        or source_only.get("output_validator_unchanged_from_epoch7") is not True
        or source_only.get("deterministic_semantic_mutation_allowed", False) is not False
        or not isinstance(authorization, Mapping)
        or authorization.get("authorized_by") != "kolby"
        or authorization.get("authority") != "direct_user_instruction"
        or authorization.get("operator_authorization_statement_sha256")
        != AUTHORIZATION_STATEMENT_SHA256
        or authorization.get("additional_interactive_approval_required") is not False
    ):
        raise CanonicalV31Epoch8SourceOnlyRecoveryError(
            "epoch-8 directive execution contract drifted"
        )
    return directive


def _verify_predecessor() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    directive = _verify_directive()
    expected_record = directive.get("predecessor", {}).get("receipt")
    if not isinstance(expected_record, Mapping):
        raise CanonicalV31Epoch8SourceOnlyRecoveryError(
            "predecessor receipt record is absent"
        )
    receipt_path = _verify_record(expected_record, label="epoch-7 predecessor receipt")
    if receipt_path != (PREDECESSOR_ROOT / predecessor.EXECUTION_RECEIPT_FILENAME).resolve():
        raise CanonicalV31Epoch8SourceOnlyRecoveryError(
            "predecessor receipt path drifted"
        )
    try:
        receipt = predecessor.verify_execution_receipt(PREDECESSOR_ROOT)
    except predecessor.CanonicalV31Epoch7AuthorityRuntimeError as exc:
        raise CanonicalV31Epoch8SourceOnlyRecoveryError(
            "epoch-7 predecessor verification failed"
        ) from exc
    usage = receipt.get("measured_usage")
    if (
        receipt.get("state") != "rejected"
        or receipt.get("terminal_reason")
        != "epoch7_authority_structural_or_cost_contract_rejected"
        or receipt.get("completed_validated_turn_count") != ADOPTED_TURN_COUNT
        or receipt.get("semantic_model_call_count") != ADOPTED_TURN_COUNT
        or receipt.get("semantic_retry_count") != 0
        or receipt.get("unknown_usage_turn_count") != 0
        or not isinstance(usage, Mapping)
        or usage.get("input_tokens") != 102_733
        or usage.get("total_tokens") != ADOPTED_TOTAL_TOKENS
        or receipt.get("merged_authority_output") is not None
        or receipt.get("production_mutated") is not False
        or receipt.get("holdout_authorized") is not False
    ):
        raise CanonicalV31Epoch8SourceOnlyRecoveryError(
            "epoch-7 predecessor terminal contract drifted"
        )
    turn_records = receipt.get("turn_results")
    if not isinstance(turn_records, list) or len(turn_records) != 1:
        raise CanonicalV31Epoch8SourceOnlyRecoveryError(
            "epoch-7 predecessor turn result is absent"
        )
    turn_path = _verify_record(turn_records[0], label="adopted predecessor turn result")
    turn_result = _load_object(turn_path, label="adopted predecessor turn result")
    if (
        turn_result.get("state") != "completed_validated_no_retry"
        or turn_result.get("ordinal") != 0
        or turn_result.get("semantic_model_call_count") != 1
        or turn_result.get("semantic_retry_count") != 0
        or turn_result.get("usage") != usage
    ):
        raise CanonicalV31Epoch8SourceOnlyRecoveryError(
            "adopted predecessor turn result drifted"
        )
    contract_path = _verify_record(
        receipt["runtime_contract"], label="epoch-7 runtime contract"
    )
    source_contract = _load_object(contract_path, label="epoch-7 runtime contract")
    turns = source_contract.get("turns")
    if (
        not isinstance(turns, list)
        or len(turns) != CUMULATIVE_TURN_COUNT
        or turns[0].get("turn_id") != turn_result.get("turn_id")
    ):
        raise CanonicalV31Epoch8SourceOnlyRecoveryError(
            "epoch-7 source turn order drifted"
        )
    return receipt, turn_result, source_contract


def _compact_input(original: Mapping[str, Any]) -> dict[str, Any]:
    repairs = original.get("reference_repairs")
    if not isinstance(repairs, list) or not repairs:
        raise CanonicalV31Epoch8SourceOnlyRecoveryError(
            "source authority repairs are absent"
        )
    regenerated: list[dict[str, Any]] = []
    for row in repairs:
        if not isinstance(row, Mapping):
            raise CanonicalV31Epoch8SourceOnlyRecoveryError(
                "source authority repair row is malformed"
            )
        legacy_label = row.get("legacy_reference_label")
        if not isinstance(legacy_label, Mapping):
            raise CanonicalV31Epoch8SourceOnlyRecoveryError(
                "source authority segment quality provenance is absent"
            )
        regenerated.append(
            {
                "segment_id": row.get("segment_id"),
                "segment_text": row.get("segment_text"),
                "segment_text_sha256": row.get("segment_text_sha256"),
                "segment_quality": copy.deepcopy(legacy_label.get("segment_quality")),
            }
        )
    compact = {
        "schema_version": COMPACT_INPUT_VERSION,
        "episode_id": original.get("episode_id"),
        "episode_metadata": copy.deepcopy(original.get("episode_metadata")),
        "full_segmented_episode_text": original.get("full_segmented_episode_text"),
        "original_context": copy.deepcopy(original.get("original_context")),
        "original_context_sha256": original.get("original_context_sha256"),
        "missing_context_fields": copy.deepcopy(original.get("missing_context_fields")),
        "reference_regeneration": regenerated,
        "invalid_reference_segment_ids": copy.deepcopy(
            original.get("invalid_reference_segment_ids")
        ),
        "preserved_valid_reference_segment_ids": copy.deepcopy(
            original.get("preserved_valid_reference_segment_ids")
        ),
        "source_only_policy": (
            "regenerate_complete_labels_from_exact_source_without_model_visible_legacy_labels"
        ),
        "privacy": "private full transcript and development source segments",
    }
    if [row["segment_id"] for row in regenerated] != compact[
        "invalid_reference_segment_ids"
    ]:
        raise CanonicalV31Epoch8SourceOnlyRecoveryError(
            "source-only repair membership drifted"
        )
    raw = _canonical_json(compact)
    if "legacy_reference_label" in raw or "validation_diagnostic_path" in raw:
        raise CanonicalV31Epoch8SourceOnlyRecoveryError(
            "model-visible compact input contains legacy repair hints"
        )
    return compact


def _turn_identity(source_turn_id: str, compact: Mapping[str, Any]) -> str:
    digest = _sha256_bytes(
        _canonical_json(
            {
                "source_turn_id": source_turn_id,
                "compact_input_sha256": _sha256_bytes(
                    _canonical_json(compact).encode("ascii")
                ),
                "architecture": "source_only_full_schema_reference_regeneration",
            }
        ).encode("ascii")
    )
    return "authority_source_only_" + digest[:24]


def _prepare_turns(root: Path, *, write: bool) -> list[dict[str, Any]]:
    _, _, source_contract = _verify_predecessor()
    source_turns = source_contract["turns"]
    prepared: list[dict[str, Any]] = []
    for new_ordinal, source_turn in enumerate(source_turns[1:]):
        source_manifest_path = _verify_record(
            source_turn["turn_manifest"], label="source authority turn manifest"
        )
        source_manifest = _load_object(
            source_manifest_path, label="source authority turn manifest"
        )
        original_input_path = _verify_record(
            source_manifest["input"], label="source authority input"
        )
        original_input = _load_object(
            original_input_path, label="source authority input"
        )
        compact = _compact_input(original_input)
        turn_id = _turn_identity(str(source_turn["turn_id"]), compact)
        turn_root = root / PREPARED_TURNS_DIRECTORY / turn_id
        compact_path = turn_root / "source-only-input.private.json"
        prompt_path = turn_root / "prompt.private.txt"
        base_path = turn_root / "base-instructions.txt"
        schema_path = turn_root / "output-schema.json"
        manifest_path = turn_root / "turn-manifest.json"
        prompt = "# Private checksum-bound source-only authority packet\n" + _canonical_json(
            compact
        ) + "\n"
        schema_source = _verify_record(
            source_manifest["output_schema"], label="source authority output schema"
        )
        schema = _load_object(schema_source, label="source authority output schema")
        if write:
            turn_root.mkdir(parents=True, exist_ok=False)
            _write_json(compact_path, compact)
            _write_bytes(prompt_path, prompt.encode("ascii"))
            _write_bytes(base_path, BASE_INSTRUCTIONS.encode("ascii"))
            _write_json(schema_path, schema)
        else:
            if _load_object(compact_path, label="source-only compact input") != compact:
                raise CanonicalV31Epoch8SourceOnlyRecoveryError(
                    "source-only compact input drifted"
                )
            if prompt_path.read_bytes() != prompt.encode("ascii"):
                raise CanonicalV31Epoch8SourceOnlyRecoveryError(
                    "source-only prompt drifted"
                )
            if base_path.read_bytes() != BASE_INSTRUCTIONS.encode("ascii"):
                raise CanonicalV31Epoch8SourceOnlyRecoveryError(
                    "source-only base instructions drifted"
                )
            if _load_object(schema_path, label="source-only output schema") != schema:
                raise CanonicalV31Epoch8SourceOnlyRecoveryError(
                    "source-only output schema drifted"
                )
        manifest = {
            "schema_version": TURN_MANIFEST_VERSION,
            "architecture": "source_only_full_schema_reference_regeneration",
            "ordinal": new_ordinal,
            "source_ordinal": new_ordinal + ADOPTED_TURN_COUNT,
            "turn_id": turn_id,
            "source_turn_id": source_turn["turn_id"],
            "episode_id": source_turn["episode_id"],
            "original_authority_turn_manifest": _record(source_manifest_path),
            "input": _record(original_input_path),
            "compact_input": _record(compact_path),
            "prompt": _record(prompt_path),
            "base_instructions": _record(base_path),
            "output_schema": _record(schema_path),
            "model_visible_legacy_reference_label_count": 0,
            "invalid_reference_count": len(compact["invalid_reference_segment_ids"]),
            "semantic_retry_count": 0,
        }
        if write:
            _write_json(manifest_path, manifest)
        elif _load_object(manifest_path, label="source-only turn manifest") != manifest:
            raise CanonicalV31Epoch8SourceOnlyRecoveryError(
                "source-only turn manifest drifted"
            )
        prepared.append(
            {
                "ordinal": new_ordinal,
                "source_ordinal": new_ordinal + ADOPTED_TURN_COUNT,
                "turn_id": turn_id,
                "source_turn_id": source_turn["turn_id"],
                "episode_id": source_turn["episode_id"],
                "turn_manifest": _record(manifest_path),
            }
        )
    if len(prepared) != NEW_TURN_COUNT:
        raise CanonicalV31Epoch8SourceOnlyRecoveryError(
            "source-only recovery turn count drifted"
        )
    return prepared


def _build_contract(root: Path, *, write_turns: bool) -> dict[str, Any]:
    predecessor_receipt, predecessor_turn, source_contract = _verify_predecessor()
    turns = _prepare_turns(root, write=write_turns)
    all_turns = [copy.deepcopy(source_contract["turns"][0]), *copy.deepcopy(turns)]
    quota_rate = int(source_contract["quota_points_per_million_tokens"])
    return {
        "schema_version": CONTRACT_VERSION,
        "state": "frozen_source_only_recovery_ready",
        "directive": _record(DIRECTIVE_PATH),
        "predecessor_root": str(PREDECESSOR_ROOT.resolve()),
        "predecessor_receipt": _record(
            PREDECESSOR_ROOT / predecessor.EXECUTION_RECEIPT_FILENAME
        ),
        "adopted_predecessor_turn_result": copy.deepcopy(
            predecessor_receipt["turn_results"][0]
        ),
        "adopted_predecessor_validated_output": copy.deepcopy(
            predecessor_turn["validated_output"]
        ),
        "authority_plan_receipt": copy.deepcopy(source_contract["authority_plan_receipt"]),
        "authority_plan": copy.deepcopy(source_contract["authority_plan"]),
        "source_runtime_contract": copy.deepcopy(predecessor_receipt["runtime_contract"]),
        "turns": turns,
        "all_turns": all_turns,
        "adopted_turn_count": ADOPTED_TURN_COUNT,
        "exact_turn_count": NEW_TURN_COUNT,
        "cumulative_turn_count": CUMULATIVE_TURN_COUNT,
        "model": MODEL,
        "effort": EFFORT,
        "maximum_total_tokens_per_turn": NEW_PER_TURN_TOTAL_TOKEN_CAP,
        "phase_total_token_bound": NEW_TOTAL_TOKEN_CAP,
        "cumulative_total_token_bound": CUMULATIVE_TOTAL_TOKEN_CAP,
        "adopted_predecessor_total_tokens": ADOPTED_TOTAL_TOKENS,
        "quota_points_per_million_tokens": quota_rate,
        "projected_phase_quota_points": math.ceil(
            NEW_TOTAL_TOKEN_CAP * quota_rate / 1_000_000
        ),
        "minimum_remaining_reserve_percent": int(
            source_contract["minimum_remaining_reserve_percent"]
        ),
        "capacity_safety_margin_percent": int(
            source_contract["capacity_safety_margin_percent"]
        ),
        "maximum_wall_seconds_per_turn": MAXIMUM_WALL_SECONDS_PER_TURN,
        "phase_wall_seconds_ceiling": (
            NEW_TURN_COUNT * MAXIMUM_WALL_SECONDS_PER_TURN
        ),
        "operator_wall_safety_margin_seconds": int(
            source_contract["operator_wall_safety_margin_seconds"]
        ),
        "source_only_regeneration": True,
        "legacy_reference_labels_model_visible": False,
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
        "predecessor_runtime_module": _record(Path(predecessor.__file__)),
        "authority_plan_module": _record(Path(authority_plan.__file__)),
        "canonical_adapter_module": _record(Path(adapter.__file__)),
        "official_app_server_transport_module": _record(
            Path(codex_app_server.__file__)
        ),
        "directive": _record(DIRECTIVE_PATH),
        "runtime_contract": _record(root / CONTRACT_FILENAME),
        "predecessor_receipt": copy.deepcopy(contract["predecessor_receipt"]),
        "adopted_predecessor_turn_result": copy.deepcopy(
            contract["adopted_predecessor_turn_result"]
        ),
        "turn_manifests": [copy.deepcopy(row["turn_manifest"]) for row in contract["turns"]],
        "pinned_official_codex_binary": _record(adapter.PINNED_CODEX),
        "pinned_protocol_schema": _record(codex_app_server.PROTOCOL_SCHEMA_PATH),
        "pinned_codex_cli_version": codex_app_server.PINNED_CODEX_CLI_VERSION,
        "canonical_adapter_binding": adapter.build_six_arm_matrix_binding(),
        "effective_instruction_sources": adapter.expected_instruction_source_contract(),
        "trusted_client_factory": "canonical_adapter._client_factory",
        "trusted_capacity_parser": (
            "app_server_canonical_v31_development_matrix_runtime.parse_trusted_rate_limit_capacity"
        ),
        "model": MODEL,
        "effort": EFFORT,
        "new_turn_count": NEW_TURN_COUNT,
        "new_per_turn_total_token_cap": NEW_PER_TURN_TOTAL_TOKEN_CAP,
        "new_total_token_cap": NEW_TOTAL_TOKEN_CAP,
        "cumulative_total_token_cap": CUMULATIVE_TOTAL_TOKEN_CAP,
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
        "predecessor_receipt": copy.deepcopy(contract["predecessor_receipt"]),
        "adopted_predecessor_turn_result": copy.deepcopy(
            contract["adopted_predecessor_turn_result"]
        ),
        "prepared_turn_manifests": [
            copy.deepcopy(row["turn_manifest"]) for row in contract["turns"]
        ],
        "adopted_semantic_model_call_count": ADOPTED_TURN_COUNT,
        "new_semantic_model_call_count": 0,
        "new_semantic_retry_count": 0,
        "production_mutated": False,
        "holdout_authorized": False,
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
        raise CanonicalV31Epoch8SourceOnlyRecoveryError(
            "operator authorization window or identity is invalid"
        )
    return {
        "schema_version": AUTHORIZATION_VERSION,
        "state": "authorized_for_three_never_started_source_only_turns",
        "authority": "direct_user_instruction",
        "authorized_by": "kolby",
        "operator_authorization_id": operator_authorization_id,
        "operator_authorization_statement_sha256": AUTHORIZATION_STATEMENT_SHA256,
        "issued_at": issued.isoformat(),
        "expires_at": expires.isoformat(),
        "authorized_scope": "three_never_started_epoch7_authority_episodes_only",
        "runtime_contract": _record(root / CONTRACT_FILENAME),
        "runtime_lock": _record(root / RUNTIME_LOCK_FILENAME),
        "preauthorization_receipt": _record(root / PREAUTHORIZATION_FILENAME),
        "predecessor_receipt": _record(
            PREDECESSOR_ROOT / predecessor.EXECUTION_RECEIPT_FILENAME
        ),
        "model": MODEL,
        "effort": EFFORT,
        "new_model_call_cap": NEW_TURN_COUNT,
        "new_total_token_cap": NEW_TOTAL_TOKEN_CAP,
        "cumulative_total_token_cap": CUMULATIVE_TOTAL_TOKEN_CAP,
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
        raise CanonicalV31Epoch8SourceOnlyRecoveryError(
            "epoch-8 recovery root must be fresh and absent"
        )
    output_root.mkdir(parents=True)
    contract = _build_contract(output_root, write_turns=True)
    _write_json(output_root / CONTRACT_FILENAME, contract)
    lock = _runtime_lock(output_root, contract)
    _write_json(output_root / RUNTIME_LOCK_FILENAME, lock)
    preauthorization = _preauthorization(output_root, contract)
    _write_json(output_root / PREAUTHORIZATION_FILENAME, preauthorization)
    authorization = _authorization(
        output_root,
        operator_authorization_id=operator_authorization_id,
        issued_at=issued_at,
        expires_at=expires_at,
    )
    _write_json(output_root / AUTHORIZATION_FILENAME, authorization)
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
    expected_contract = _build_contract(output_root, write_turns=False)
    if contract != expected_contract:
        raise CanonicalV31Epoch8SourceOnlyRecoveryError(
            "epoch-8 runtime contract drifted"
        )
    lock = _load_object(output_root / RUNTIME_LOCK_FILENAME, label="runtime lock")
    if lock != _runtime_lock(output_root, contract):
        raise CanonicalV31Epoch8SourceOnlyRecoveryError(
            "epoch-8 runtime lock drifted"
        )
    receipt = _load_object(
        output_root / PREAUTHORIZATION_FILENAME, label="preauthorization receipt"
    )
    if receipt != _preauthorization(output_root, contract):
        raise CanonicalV31Epoch8SourceOnlyRecoveryError(
            "epoch-8 preauthorization receipt drifted"
        )
    return copy.deepcopy(receipt)


def verify_authorization(
    root: Path = DEFAULT_ROOT,
    *,
    expected_authorization_id: str | None = None,
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
    if not all(isinstance(value, str) for value in (authorization_id, issued_at, expires_at)):
        raise CanonicalV31Epoch8SourceOnlyRecoveryError(
            "operator authorization fields are absent"
        )
    expected = _authorization(
        output_root,
        operator_authorization_id=authorization_id,
        issued_at=issued_at,
        expires_at=expires_at,
    )
    if observed != expected or (
        expected_authorization_id is not None
        and authorization_id != expected_authorization_id
    ):
        raise CanonicalV31Epoch8SourceOnlyRecoveryError(
            "operator authorization drifted"
        )
    if require_current and not (
        _parse_timestamp(issued_at, label="authorization issued_at")
        <= _now()
        < _parse_timestamp(expires_at, label="authorization expires_at")
    ):
        raise CanonicalV31Epoch8SourceOnlyRecoveryWaiting(
            "operator authorization is not current"
        )
    return copy.deepcopy(observed)


def _turn_states(root: Path, contract: Mapping[str, Any]) -> list[str]:
    return [
        predecessor._turn_state(  # noqa: SLF001
            predecessor._turn_paths(root, str(turn["turn_id"]))  # noqa: SLF001
        )
        for turn in contract["turns"]
    ]


def _validate_turn_prefix(
    root: Path,
    contract: Mapping[str, Any],
    authorization: Mapping[str, Any],
    *,
    allow_finalize: bool,
) -> tuple[list[dict[str, Any]], list[str]]:
    states = _turn_states(root, contract)
    absent_seen = False
    completed: list[dict[str, Any]] = []
    for index, (turn, state) in enumerate(zip(contract["turns"], states)):
        if state == "absent":
            absent_seen = True
            continue
        if absent_seen:
            raise CanonicalV31Epoch8SourceOnlyRecoveryError(
                "source-only turns are not a contiguous prefix"
            )
        try:
            if state in {"complete", "recoverable_completed"}:
                completed.append(
                    predecessor._verify_complete_turn(  # noqa: SLF001
                        root=root,
                        contract=contract,
                        authorization=authorization,
                        turn=turn,
                        completed_turn_count=index,
                        allow_finalize=allow_finalize,
                    )
                )
            elif state == "partial":
                predecessor._validate_partial_turn(  # noqa: SLF001
                    root=root,
                    contract=contract,
                    authorization=authorization,
                    turn=turn,
                    completed_turn_count=index,
                )
        except predecessor.CanonicalV31Epoch7AuthorityWaiting as exc:
            raise CanonicalV31Epoch8SourceOnlyRecoveryWaiting(str(exc)) from exc
        except predecessor.CanonicalV31Epoch7AuthorityRejected as exc:
            raise CanonicalV31Epoch8SourceOnlyRecoveryRejected(str(exc)) from exc
        except predecessor.CanonicalV31Epoch7AuthorityRuntimeError as exc:
            raise CanonicalV31Epoch8SourceOnlyRecoveryError(str(exc)) from exc
    return completed, states


def _usage_sum(values: Sequence[Mapping[str, int]]) -> dict[str, int]:
    return predecessor._sum_usage(values)  # noqa: SLF001


def _cumulative_usage(
    predecessor_receipt: Mapping[str, Any], new_usage: Mapping[str, int]
) -> dict[str, int]:
    return _usage_sum([predecessor_receipt["measured_usage"], new_usage])


def _failed_checks(
    *,
    contract: Mapping[str, Any],
    completed: Sequence[Mapping[str, Any]],
    accounting: Mapping[str, Any],
    cumulative_usage: Mapping[str, int],
) -> list[str]:
    checks: list[str] = []
    if accounting["semantic_model_call_count"] > NEW_TURN_COUNT:
        checks.append("new_semantic_model_call_cap")
    if accounting["measured_usage"]["total_tokens"] > NEW_TOTAL_TOKEN_CAP:
        checks.append("new_total_token_cap")
    if cumulative_usage["total_tokens"] > CUMULATIVE_TOTAL_TOKEN_CAP:
        checks.append("cumulative_authority_total_token_cap")
    if any(
        int(row["usage"]["total_tokens"])
        > int(contract["maximum_total_tokens_per_turn"])
        for row in completed
    ):
        checks.append("new_per_turn_total_token_cap")
    return checks


def _merge_output(
    root: Path,
    contract: Mapping[str, Any],
    completed: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    _, predecessor_turn, _ = _verify_predecessor()
    merge_contract = copy.deepcopy(dict(contract))
    merge_contract["turns"] = copy.deepcopy(contract["all_turns"])
    try:
        merged = predecessor._build_merged_output(  # noqa: SLF001
            root=root,
            contract=merge_contract,
            turn_results=[predecessor_turn, *completed],
        )
    except predecessor.CanonicalV31Epoch7AuthorityRuntimeError as exc:
        raise CanonicalV31Epoch8SourceOnlyRecoveryError(
            "source-only merged authority output failed verification"
        ) from exc
    path = root / MERGED_OUTPUT_FILENAME
    if path.exists():
        if _load_object(path, label="merged authority output") != merged:
            raise CanonicalV31Epoch8SourceOnlyRecoveryError(
                "merged authority output drifted"
            )
    else:
        _write_json(path, merged)
    return merged


def _write_terminal(
    *,
    root: Path,
    contract: Mapping[str, Any],
    authorization: Mapping[str, Any],
    state: str,
    reason: str,
    completed: Sequence[Mapping[str, Any]],
    merged: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    predecessor_receipt, _, _ = _verify_predecessor()
    try:
        accounting = predecessor._terminal_accounting(root, contract)  # noqa: SLF001
    except predecessor.CanonicalV31Epoch7AuthorityRuntimeError as exc:
        raise CanonicalV31Epoch8SourceOnlyRecoveryError(str(exc)) from exc
    cumulative = _cumulative_usage(predecessor_receipt, accounting["measured_usage"])
    failed = _failed_checks(
        contract=contract,
        completed=completed,
        accounting=accounting,
        cumulative_usage=cumulative,
    )
    if state == "passed" and failed:
        state = "rejected"
        reason = "epoch8_source_only_authority_cost_contract_rejected"
        merged = None
    receipt = {
        "schema_version": RECEIPT_VERSION,
        "state": state,
        "terminal_reason": reason,
        "output_root": str(root.resolve()),
        "directive": _record(DIRECTIVE_PATH),
        "runtime_contract": _record(root / CONTRACT_FILENAME),
        "runtime_lock": _record(root / RUNTIME_LOCK_FILENAME),
        "preauthorization_receipt": _record(root / PREAUTHORIZATION_FILENAME),
        "operator_authorization": _record(root / AUTHORIZATION_FILENAME),
        "predecessor_receipt": copy.deepcopy(contract["predecessor_receipt"]),
        "adopted_predecessor_turn_result": copy.deepcopy(
            contract["adopted_predecessor_turn_result"]
        ),
        "new_turn_results": [
            _record(
                predecessor._turn_paths(root, str(turn["turn_id"]))["result"]  # noqa: SLF001
            )
            for turn in contract["turns"]
            if predecessor._turn_paths(root, str(turn["turn_id"]))[  # noqa: SLF001
                "result"
            ].is_file()
        ],
        "adopted_completed_turn_count": ADOPTED_TURN_COUNT,
        "new_completed_validated_turn_count": len(completed),
        "cumulative_completed_validated_turn_count": ADOPTED_TURN_COUNT
        + len(completed),
        "new_exact_turn_count": NEW_TURN_COUNT,
        "cumulative_exact_turn_count": CUMULATIVE_TURN_COUNT,
        "adopted_measured_usage": copy.deepcopy(predecessor_receipt["measured_usage"]),
        "new_measured_usage": copy.deepcopy(accounting["measured_usage"]),
        "cumulative_measured_usage": cumulative,
        "new_semantic_model_call_count": accounting["semantic_model_call_count"],
        "cumulative_semantic_model_call_count": ADOPTED_TURN_COUNT
        + accounting["semantic_model_call_count"],
        "semantic_retry_count": 0,
        "new_measured_usage_turn_count": accounting["measured_usage_turn_count"],
        "new_unknown_usage_turn_count": accounting["unknown_usage_turn_count"],
        "new_wall_elapsed_seconds": accounting["wall_elapsed_seconds"],
        "thread_ids": copy.deepcopy(accounting["thread_ids"]),
        "semantic_turn_ids": copy.deepcopy(accounting["semantic_turn_ids"]),
        "failed_checks": failed,
        "merged_authority_output": (
            _record(root / MERGED_OUTPUT_FILENAME) if merged is not None else None
        ),
        "extraction_plan_rebuild_required": state == "passed",
        "quality_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }
    raw = _pretty_json(receipt).encode("ascii")
    _write_bytes(root / RECEIPT_FILENAME, raw)
    _write_bytes(root / TERMINAL_FILENAME, raw)
    return verify_recovery_receipt(root)


def verify_recovery_receipt(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    verify_preauthorization(root)
    output_root = root.expanduser().resolve()
    authorization = verify_authorization(
        output_root, expected_authorization_id=None, require_current=False
    )
    receipt_path = output_root / RECEIPT_FILENAME
    terminal_path = output_root / TERMINAL_FILENAME
    receipt = _load_object(receipt_path, label="authority recovery receipt")
    terminal = _load_object(terminal_path, label="authority recovery terminal")
    if receipt != terminal or receipt_path.read_bytes() != terminal_path.read_bytes():
        raise CanonicalV31Epoch8SourceOnlyRecoveryError(
            "authority recovery terminal mirrors drifted"
        )
    contract = _load_object(output_root / CONTRACT_FILENAME, label="runtime contract")
    completed, states = _validate_turn_prefix(
        output_root, contract, authorization, allow_finalize=False
    )
    try:
        accounting = predecessor._terminal_accounting(output_root, contract)  # noqa: SLF001
    except predecessor.CanonicalV31Epoch7AuthorityRuntimeError as exc:
        raise CanonicalV31Epoch8SourceOnlyRecoveryError(str(exc)) from exc
    predecessor_receipt, _, _ = _verify_predecessor()
    cumulative = _cumulative_usage(predecessor_receipt, accounting["measured_usage"])
    failed = _failed_checks(
        contract=contract,
        completed=completed,
        accounting=accounting,
        cumulative_usage=cumulative,
    )
    fixed = {
        "schema_version": RECEIPT_VERSION,
        "output_root": str(output_root),
        "directive": _record(DIRECTIVE_PATH),
        "runtime_contract": _record(output_root / CONTRACT_FILENAME),
        "runtime_lock": _record(output_root / RUNTIME_LOCK_FILENAME),
        "preauthorization_receipt": _record(output_root / PREAUTHORIZATION_FILENAME),
        "operator_authorization": _record(output_root / AUTHORIZATION_FILENAME),
        "predecessor_receipt": contract["predecessor_receipt"],
        "adopted_predecessor_turn_result": contract[
            "adopted_predecessor_turn_result"
        ],
        "adopted_completed_turn_count": ADOPTED_TURN_COUNT,
        "new_completed_validated_turn_count": len(completed),
        "cumulative_completed_validated_turn_count": ADOPTED_TURN_COUNT
        + len(completed),
        "new_exact_turn_count": NEW_TURN_COUNT,
        "cumulative_exact_turn_count": CUMULATIVE_TURN_COUNT,
        "adopted_measured_usage": predecessor_receipt["measured_usage"],
        "new_measured_usage": accounting["measured_usage"],
        "cumulative_measured_usage": cumulative,
        "new_semantic_model_call_count": accounting["semantic_model_call_count"],
        "cumulative_semantic_model_call_count": ADOPTED_TURN_COUNT
        + accounting["semantic_model_call_count"],
        "semantic_retry_count": 0,
        "new_measured_usage_turn_count": accounting["measured_usage_turn_count"],
        "new_unknown_usage_turn_count": accounting["unknown_usage_turn_count"],
        "new_wall_elapsed_seconds": accounting["wall_elapsed_seconds"],
        "thread_ids": accounting["thread_ids"],
        "semantic_turn_ids": accounting["semantic_turn_ids"],
        "failed_checks": failed,
        "quality_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }
    for key, value in fixed.items():
        if receipt.get(key) != value:
            raise CanonicalV31Epoch8SourceOnlyRecoveryError(
                f"authority recovery receipt drifted at {key}"
            )
    expected_result_records = [
        _record(
            predecessor._turn_paths(output_root, str(turn["turn_id"]))["result"]  # noqa: SLF001
        )
        for turn in contract["turns"]
        if predecessor._turn_paths(output_root, str(turn["turn_id"]))[  # noqa: SLF001
            "result"
        ].is_file()
    ]
    if receipt.get("new_turn_results") != expected_result_records:
        raise CanonicalV31Epoch8SourceOnlyRecoveryError(
            "authority recovery result records drifted"
        )
    state = receipt.get("state")
    if state not in {"passed", "rejected", "waiting"}:
        raise CanonicalV31Epoch8SourceOnlyRecoveryError(
            "authority recovery state is invalid"
        )
    if state == "passed":
        if (
            states != ["complete"] * NEW_TURN_COUNT
            or failed
            or accounting["unknown_usage_turn_count"] != 0
        ):
            raise CanonicalV31Epoch8SourceOnlyRecoveryError(
                "passing authority recovery lacks complete evidence"
            )
        merged = _merge_output(output_root, contract, completed)
        if receipt.get("merged_authority_output") != _record(
            output_root / MERGED_OUTPUT_FILENAME
        ) or _load_object(
            output_root / MERGED_OUTPUT_FILENAME, label="merged authority output"
        ) != merged:
            raise CanonicalV31Epoch8SourceOnlyRecoveryError(
                "passing authority recovery merged output drifted"
            )
    elif receipt.get("merged_authority_output") is not None:
        raise CanonicalV31Epoch8SourceOnlyRecoveryError(
            "nonpassing authority recovery claims a merged output"
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
            "schema_version": "pif_canonical_v31_epoch8_source_only_authority_status_v1",
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
    return {
        "schema_version": "pif_canonical_v31_epoch8_source_only_authority_status_v1",
        "state": "ready" if "partial" not in states else "waiting",
        "reason": (
            "ready_for_three_never_started_source_only_turns"
            if states == ["absent"] * NEW_TURN_COUNT
            else "source_only_recovery_has_immutable_progress"
        ),
        "turn_states": states,
        "adopted_predecessor_turn_count": ADOPTED_TURN_COUNT,
        "new_completed_turn_count": len(completed),
        "new_semantic_retry_count": 0,
    }


def _reject_external_auth_material() -> None:
    try:
        predecessor._reject_external_auth_material()  # noqa: SLF001
    except predecessor.CanonicalV31Epoch7AuthorityRuntimeError as exc:
        raise CanonicalV31Epoch8SourceOnlyRecoveryError(str(exc)) from exc


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
    except CanonicalV31Epoch8SourceOnlyRecoveryRejected:
        return _write_terminal(
            root=output_root,
            contract=contract,
            authorization=authorization,
            state="rejected",
            reason="epoch8_source_only_authority_structural_rejection",
            completed=[],
        )
    if "partial" in states:
        return _write_terminal(
            root=output_root,
            contract=contract,
            authorization=authorization,
            state="waiting",
            reason="epoch8_source_only_authority_partial_attempt_preserved_no_replay",
            completed=completed,
        )
    if len(completed) == NEW_TURN_COUNT:
        merged = _merge_output(output_root, contract, completed)
        return _write_terminal(
            root=output_root,
            contract=contract,
            authorization=authorization,
            state="passed",
            reason="epoch8_source_only_authority_completed_with_predecessor_adoption",
            completed=completed,
            merged=merged,
        )
    predecessor_receipt, _, _ = _verify_predecessor()
    try:
        async with predecessor.TRUSTED_CLIENT_FACTORY() as client:
            account = getattr(client, "account_summary", None)
            if (
                not isinstance(account, Mapping)
                or account.get("type") != "chatgpt"
                or account.get("plan_type") != "pro"
            ):
                raise CanonicalV31Epoch8SourceOnlyRecoveryError(
                    "source-only recovery requires managed ChatGPT Pro auth"
                )
            for index in range(len(completed), NEW_TURN_COUNT):
                verify_preauthorization(output_root)
                verify_authorization(
                    output_root,
                    expected_authorization_id=operator_authorization_id,
                    require_current=True,
                )
                turn = contract["turns"][index]
                paths = predecessor._turn_paths(  # noqa: SLF001
                    output_root, str(turn["turn_id"])
                )
                paths["root"].mkdir(parents=True, exist_ok=False)
                attempt = predecessor._attempt_payload(  # noqa: SLF001
                    root=output_root,
                    contract=contract,
                    authorization=authorization,
                    turn=turn,
                )
                predecessor._write_immutable_json(paths["attempt"], attempt)  # noqa: SLF001
                initial_request = predecessor._capacity_request(  # noqa: SLF001
                    root=output_root,
                    contract=contract,
                    authorization=authorization,
                    turn=turn,
                    boundary="initial_before_thread",
                    completed_turn_count=index,
                )
                await predecessor._probe_and_publish(  # noqa: SLF001
                    client=client,
                    bundle_root=paths["initial_capacity"],
                    request=initial_request,
                    contract=contract,
                    authorization=authorization,
                )
                manifest = _load_object(
                    _verify_record(turn["turn_manifest"], label="turn manifest"),
                    label="turn manifest",
                )
                base_path = _verify_record(
                    manifest["base_instructions"], label="base instructions"
                )
                thread = await client.start_thread(
                    model=MODEL,
                    base_instructions=base_path.read_text(encoding="ascii"),
                    cwd=PROJECT_ROOT,
                    ephemeral=True,
                )
                binding = predecessor._thread_binding(  # noqa: SLF001
                    thread, contract=contract, turn=turn
                )
                predecessor._write_immutable_json(paths["thread"], binding)  # noqa: SLF001
                preturn_request = predecessor._capacity_request(  # noqa: SLF001
                    root=output_root,
                    contract=contract,
                    authorization=authorization,
                    turn=turn,
                    boundary="preturn_before_turn",
                    completed_turn_count=index,
                )
                await predecessor._probe_and_publish(  # noqa: SLF001
                    client=client,
                    bundle_root=paths["preturn_capacity"],
                    request=preturn_request,
                    contract=contract,
                    authorization=authorization,
                )
                prompt_path = _verify_record(manifest["prompt"], label="prompt")
                schema_path = _verify_record(
                    manifest["output_schema"], label="output schema"
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
                    raise CanonicalV31Epoch8SourceOnlyRecoveryWaiting(
                        "source-only authority turn did not complete successfully"
                    )
                completed_result = predecessor._verify_complete_turn(  # noqa: SLF001
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
                    raise CanonicalV31Epoch8SourceOnlyRecoveryError(
                        "in-memory source-only authority lineage drifted"
                    )
                completed.append(completed_result)
                new_usage = _usage_sum([row["usage"] for row in completed])
                cumulative = _cumulative_usage(predecessor_receipt, new_usage)
                if (
                    completed_result["usage"]["total_tokens"]
                    > NEW_PER_TURN_TOTAL_TOKEN_CAP
                    or new_usage["total_tokens"] > NEW_TOTAL_TOKEN_CAP
                    or cumulative["total_tokens"] > CUMULATIVE_TOTAL_TOKEN_CAP
                ):
                    raise CanonicalV31Epoch8SourceOnlyRecoveryRejected(
                        "source-only authority measured token ceiling was exceeded"
                    )
    except (
        CanonicalV31Epoch8SourceOnlyRecoveryRejected,
        predecessor.CanonicalV31Epoch7AuthorityRejected,
        codex_app_server.AppServerStructuredOutputError,
    ):
        return _write_terminal(
            root=output_root,
            contract=contract,
            authorization=authorization,
            state="rejected",
            reason="epoch8_source_only_authority_structural_or_cost_rejected",
            completed=completed,
        )
    except (
        CanonicalV31Epoch8SourceOnlyRecoveryWaiting,
        predecessor.CanonicalV31Epoch7AuthorityWaiting,
        codex_app_server.AppServerError,
        asyncio.TimeoutError,
    ):
        return _write_terminal(
            root=output_root,
            contract=contract,
            authorization=authorization,
            state="waiting",
            reason="epoch8_source_only_authority_operational_waiting_no_replay",
            completed=completed,
        )
    merged = _merge_output(output_root, contract, completed)
    return _write_terminal(
        root=output_root,
        contract=contract,
        authorization=authorization,
        state="passed",
        reason="epoch8_source_only_authority_completed_with_predecessor_adoption",
        completed=completed,
        merged=merged,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=(
            "python3 -m "
            "research_factory.app_server_canonical_v31_epoch8_source_only_authority_recovery"
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
            result = freeze_recovery(
                root=args.root,
                operator_authorization_id=args.operator_authorization_id,
                issued_at=args.issued_at,
                expires_at=args.expires_at,
            )
        elif args.command == "status":
            result = status_recovery(args.root)
        elif args.command == "execute":
            result = asyncio.run(
                execute_recovery(
                    root=args.root,
                    operator_authorization_id=args.operator_authorization_id,
                )
            )
        else:
            result = verify_recovery_receipt(args.root)
    except (
        CanonicalV31Epoch8SourceOnlyRecoveryError,
        CanonicalV31Epoch8SourceOnlyRecoveryWaiting,
    ) as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, sort_keys=True, indent=2))
        return 2
    print(json.dumps({"ok": True, "result": result}, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
