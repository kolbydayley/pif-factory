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
from types import SimpleNamespace
from typing import Any, Mapping, Sequence

from . import app_server_canonical_v31_bounded_evidence_episode_batch as adapter
from . import app_server_canonical_v31_episode_batch as canonical_base
from . import app_server_canonical_v31_epoch7_input_authority_plan as authority_plan
from . import app_server_canonical_v31_epoch7_input_authority_runtime as epoch7
from . import app_server_canonical_v31_epoch9_split_authority_recovery as epoch9
from . import app_server_canonical_v31_epoch10_unit_projection_authority_recovery as epoch10
from . import codex_app_server


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PREDECESSOR_ROOT = epoch10.DEFAULT_ROOT
DEFAULT_ROOT = (
    PROJECT_ROOT
    / "work"
    / "app-server-development-v2"
    / "unattended-pipeline-v5"
    / "canonical-v31-epoch11-bounded-evidence-authority-recovery-v1"
)
DIRECTIVE_PATH = (
    PROJECT_ROOT
    / "automation"
    / "pif-evaluation-epoch11-bounded-evidence-authority-recovery-v11.json"
)
DIRECTIVE_SHA256 = "8746f663f9fad79927165a73d64592c287b4f8e7d221a4fc2cd8de451c0a46f3"

THREAD_ID = "019f4cf1-c46e-7db3-acd2-bf03c4459a10"
PLAN_EPOCH = 11
STEP_ID = "canonical_v31_epoch11_bounded_evidence_authority_recovery_v11"
CONTRACT_FILENAME = "runtime-contract.json"
RUNTIME_LOCK_FILENAME = "runtime-lock.json"
PREAUTHORIZATION_FILENAME = "preauthorization-receipt.json"
AUTHORIZATION_FILENAME = "operator-authorization.json"
RECEIPT_FILENAME = "plan-step-receipt.json"
TERMINAL_FILENAME = "terminal.json"
MERGED_OUTPUT_FILENAME = epoch7.MERGED_OUTPUT_FILENAME
PREPARED_DIRECTORY = "prepared-turns"
ADOPTED_DIRECTORY = "adopted-evidence"
TURNS_DIRECTORY = "turns"

CONTRACT_VERSION = "pif_canonical_v31_epoch11_bounded_evidence_contract_v1"
RUNTIME_LOCK_VERSION = "pif_canonical_v31_epoch11_bounded_evidence_runtime_lock_v1"
PREAUTHORIZATION_VERSION = "pif_canonical_v31_epoch11_bounded_evidence_preauthorization_v1"
AUTHORIZATION_VERSION = "pif_canonical_v31_epoch11_bounded_evidence_authorization_v1"
TURN_MANIFEST_VERSION = "pif_canonical_v31_epoch11_bounded_evidence_turn_manifest_v1"
IO_BINDING_VERSION = "pif_canonical_v31_epoch11_bounded_evidence_io_binding_v1"
TURN_RESULT_VERSION = "pif_canonical_v31_epoch11_bounded_evidence_turn_result_v1"
REJECTION_VERSION = "pif_canonical_v31_epoch11_semantic_rejection_v1"
RECEIPT_VERSION = "pif_semantic_plan_step_receipt_v1"

MODEL = adapter.MODEL
EFFORT = adapter.EFFORT
NEW_TURN_COUNT = 5
NEW_PER_TURN_THREAD_TOTAL_TOKEN_CAP = 110_000
NEW_TOTAL_TOKEN_CAP = 550_000
ADOPTED_EPOCH7_TOTAL_TOKENS = 124_404
ADOPTED_EPOCH8_TOTAL_TOKENS = 580_618
ADOPTED_EPOCH9_TOTAL_TOKENS = 33_044
ADOPTED_EPOCH10_TOTAL_TOKENS = 29_005
ADOPTED_DEVELOPMENT_QA_TOTAL_TOKENS = (
    ADOPTED_EPOCH7_TOTAL_TOKENS
    + ADOPTED_EPOCH8_TOTAL_TOKENS
    + ADOPTED_EPOCH9_TOTAL_TOKENS
    + ADOPTED_EPOCH10_TOTAL_TOKENS
)
CUMULATIVE_DEVELOPMENT_QA_TOTAL_TOKEN_CAP = (
    ADOPTED_DEVELOPMENT_QA_TOTAL_TOKENS + NEW_TOTAL_TOKEN_CAP
)
MAXIMUM_WALL_SECONDS_PER_TURN = epoch10.MAXIMUM_WALL_SECONDS_PER_TURN
AUTHORIZATION_WINDOW_MAX_SECONDS = 14_400
AUTHORIZATION_STATEMENT = (
    "You have my full permission to continue. No need to seek out my approval anymore."
)
AUTHORIZATION_STATEMENT_SHA256 = hashlib.sha256(
    AUTHORIZATION_STATEMENT.encode("utf-8")
).hexdigest()


class CanonicalV31Epoch11Error(RuntimeError):
    pass


class CanonicalV31Epoch11Waiting(CanonicalV31Epoch11Error):
    pass


class CanonicalV31Epoch11Rejected(CanonicalV31Epoch11Error):
    pass


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _pretty_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _record(path: Path) -> dict[str, Any]:
    try:
        return epoch9._record(path)  # noqa: SLF001 - audited pure helper
    except epoch9.CanonicalV31Epoch9SplitRecoveryError as exc:
        raise CanonicalV31Epoch11Error(str(exc)) from exc


def _verify_record(value: Any, *, label: str) -> Path:
    try:
        return epoch9._verify_record(value, label=label)  # noqa: SLF001
    except epoch9.CanonicalV31Epoch9SplitRecoveryError as exc:
        raise CanonicalV31Epoch11Error(str(exc)) from exc


def _load_object(path: Path, *, label: str) -> dict[str, Any]:
    try:
        return epoch9._load_object(path, label=label)  # noqa: SLF001
    except epoch9.CanonicalV31Epoch9SplitRecoveryError as exc:
        raise CanonicalV31Epoch11Error(str(exc)) from exc


def _write_json(path: Path, value: Any) -> dict[str, Any]:
    try:
        return epoch9._write_json(path, value)  # noqa: SLF001
    except epoch9.CanonicalV31Epoch9SplitRecoveryError as exc:
        raise CanonicalV31Epoch11Error(str(exc)) from exc


def _write_bytes(path: Path, value: bytes) -> dict[str, Any]:
    try:
        return epoch9._write_bytes(path, value)  # noqa: SLF001
    except epoch9.CanonicalV31Epoch9SplitRecoveryError as exc:
        raise CanonicalV31Epoch11Error(str(exc)) from exc


def _parse_timestamp(value: Any, *, label: str) -> datetime:
    try:
        return epoch9._parse_timestamp(value, label=label)  # noqa: SLF001
    except epoch9.CanonicalV31Epoch9SplitRecoveryError as exc:
        raise CanonicalV31Epoch11Error(str(exc)) from exc


def _usage(value: Any, *, label: str) -> dict[str, int]:
    try:
        return epoch9._usage(value, label=label)  # noqa: SLF001
    except epoch9.CanonicalV31Epoch9SplitRecoveryError as exc:
        raise CanonicalV31Epoch11Error(str(exc)) from exc


def _sum_usage(values: Sequence[Mapping[str, int]]) -> dict[str, int]:
    return {
        field: sum(int(value[field]) for value in values)
        for field in epoch7.USAGE_FIELDS
    }


def _now(value: datetime | None = None) -> datetime:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None:
        raise CanonicalV31Epoch11Error("current time is not timezone-aware")
    return current.astimezone(timezone.utc)


def _verify_directive() -> dict[str, Any]:
    if _record(DIRECTIVE_PATH)["sha256"] != DIRECTIVE_SHA256:
        raise CanonicalV31Epoch11Error("epoch-11 directive drifted")
    directive = _load_object(DIRECTIVE_PATH, label="epoch-11 directive")
    fixed = {
        "schema_version": "pif_evaluation_epoch11_bounded_evidence_authority_recovery_directive_v1",
        "thread_id": THREAD_ID,
        "plan_epoch": PLAN_EPOCH,
        "step_id": STEP_ID,
        "state": "ready_for_direct_user_authorized_execute",
        "expected_receipt_path": str((DEFAULT_ROOT / RECEIPT_FILENAME).resolve()),
    }
    for key, expected in fixed.items():
        if directive.get(key) != expected:
            raise CanonicalV31Epoch11Error(f"epoch-11 directive drifted at {key}")
    authorization = directive.get("authorization_contract")
    predecessor = directive.get("predecessor_contract")
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
        or not isinstance(predecessor, Mapping)
        or predecessor.get("epoch10_root") != str(PREDECESSOR_ROOT.resolve())
        or predecessor.get("epoch10_state") != "rejected"
        or predecessor.get("epoch10_terminal_reason")
        != "epoch10_unit_projection_semantic_output_rejected"
        or predecessor.get("epoch10_semantic_model_call_count") != 1
        or predecessor.get("epoch10_full_thread_total_tokens")
        != ADOPTED_EPOCH10_TOTAL_TOKENS
        or predecessor.get("epoch10_completed_turn_replay_allowed") is not False
        or predecessor.get("epoch10_later_turns_started") is not False
        or predecessor.get("epoch10_representability_failure")
        != "canonical_evidence_span_exceeded_1000_characters"
        or not isinstance(architecture, Mapping)
        or architecture.get("explicit_existing_schema_evidence_span_max_chars")
        != adapter.CANONICAL_EVIDENCE_MAX_CHARS
        or architecture.get("model_selected_evidence_unit_ids") is not True
        or architecture.get("deterministic_exact_evidence_and_offset_projection_only")
        is not True
        or architecture.get("deterministic_semantic_pruning") is not False
        or architecture.get("deterministic_deduplication") is not False
        or architecture.get("deterministic_relabeling") is not False
        or architecture.get("legacy_invalid_labels_model_visible") is not False
        or architecture.get("validation_diagnostics_model_visible") is not False
        or not isinstance(execution, Mapping)
        or execution.get("model") != MODEL
        or execution.get("effort") != EFFORT
        or execution.get("new_model_call_cap") != NEW_TURN_COUNT
        or execution.get("new_total_token_cap") != NEW_TOTAL_TOKEN_CAP
        or execution.get("new_per_turn_thread_total_token_cap")
        != NEW_PER_TURN_THREAD_TOTAL_TOKEN_CAP
        or execution.get("adopted_development_qa_total_tokens")
        != ADOPTED_DEVELOPMENT_QA_TOTAL_TOKENS
        or execution.get("semantic_retry_count") != 0
        or execution.get("official_persistent_codex_app_server_only") is not True
        or execution.get("managed_chatgpt_auth_only") is not True
        or execution.get("managed_chatgpt_plan_type") != "pro"
        or execution.get("full_thread_total_usage_is_accounting_authority") is not True
        or execution.get("api_key_auth_allowed") is not False
        or execution.get("raw_session_token_auth_allowed") is not False
        or execution.get("codex_exec_allowed") is not False
        or not isinstance(terminal, Mapping)
        or terminal.get("accepted_receipt_states")
        != ["passed", "rejected", "waiting"]
        or terminal.get("quality_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutation_allowed") is not False
    ):
        raise CanonicalV31Epoch11Error("epoch-11 directive contract drifted")
    return directive


def _validate_epoch10_predecessor() -> dict[str, Any]:
    receipt = epoch10.verify_recovery_receipt(PREDECESSOR_ROOT)
    receipt_path = PREDECESSOR_ROOT / epoch10.RECEIPT_FILENAME
    terminal_path = PREDECESSOR_ROOT / epoch10.TERMINAL_FILENAME
    if receipt_path.read_bytes() != terminal_path.read_bytes():
        raise CanonicalV31Epoch11Error("epoch-10 terminal mirrors drifted")
    expected_fields = {
        "schema_version": epoch10.RECEIPT_VERSION,
        "state": "rejected",
        "terminal_reason": "epoch10_unit_projection_semantic_output_rejected",
        "new_semantic_model_call_count": 1,
        "new_completed_validated_turn_count": 0,
        "new_unknown_usage_turn_count": 0,
        "semantic_retry_count": 0,
        "quality_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }
    for key, expected in expected_fields.items():
        if receipt.get(key) != expected:
            raise CanonicalV31Epoch11Error(f"epoch-10 receipt drifted at {key}")
    expected_records = {
        "runtime_contract": _record(PREDECESSOR_ROOT / epoch10.CONTRACT_FILENAME),
        "runtime_lock": _record(PREDECESSOR_ROOT / epoch10.RUNTIME_LOCK_FILENAME),
        "preauthorization_receipt": _record(
            PREDECESSOR_ROOT / epoch10.PREAUTHORIZATION_FILENAME
        ),
        "operator_authorization": _record(
            PREDECESSOR_ROOT / epoch10.AUTHORIZATION_FILENAME
        ),
    }
    for key, expected in expected_records.items():
        if receipt.get(key) != expected:
            raise CanonicalV31Epoch11Error(f"epoch-10 lineage drifted at {key}")
    measured = _usage(receipt.get("new_measured_usage"), label="epoch-10 measured")
    if measured["total_tokens"] != ADOPTED_EPOCH10_TOTAL_TOKENS:
        raise CanonicalV31Epoch11Error("epoch-10 measured usage drifted")
    contract = _load_object(
        PREDECESSOR_ROOT / epoch10.CONTRACT_FILENAME, label="epoch-10 contract"
    )
    if [epoch10._turn_state(PREDECESSOR_ROOT, turn) for turn in contract["turns"]] != [
        "recoverable_completed",
        "absent",
        "absent",
        "absent",
        "absent",
    ]:
        raise CanonicalV31Epoch11Error("epoch-10 turn lifecycle drifted")
    turn = contract["turns"][0]
    paths = epoch10._turn_paths(PREDECESSOR_ROOT, str(turn["turn_id"]))  # noqa: SLF001
    manifest = epoch10._manifest(turn)  # noqa: SLF001
    io = epoch10._io_binding(PREDECESSOR_ROOT, turn)  # noqa: SLF001
    request = _load_object(
        _verify_record(io["request"], label="epoch-10 request"),
        label="epoch-10 request",
    )
    raw_output = _load_object(paths["raw_output"], label="epoch-10 raw output")
    try:
        canonical_base.validate_and_project_output(request, raw_output)
    except canonical_base.CanonicalV31OutputError as exc:
        diagnostic = str(exc)
    else:
        raise CanonicalV31Epoch11Error("epoch-10 rejection no longer reproduces")
    expected_diagnostic = (
        "$.discourse_events[12] projected evidence exceeds canonical maxLength=1000"
    )
    if diagnostic != expected_diagnostic:
        raise CanonicalV31Epoch11Error("epoch-10 representability diagnostic drifted")
    original = _load_object(
        _verify_record(
            manifest["original_authority_input"], label="original authority input"
        ),
        label="original authority input",
    )
    rejection = _load_object(
        _verify_record(receipt["semantic_rejection"], label="epoch-10 rejection"),
        label="epoch-10 rejection",
    )
    if (
        rejection.get("turn_id") != turn["turn_id"]
        or rejection.get("episode_id") != turn["episode_id"]
        or rejection.get("stage") != "repair_canonical_labels"
        or rejection.get("transcript_text_present") is not False
        or rejection.get("legacy_label_text_present") is not False
    ):
        raise CanonicalV31Epoch11Error("epoch-10 rejection lineage drifted")
    return {
        "receipt": receipt,
        "receipt_record": _record(receipt_path),
        "terminal_record": _record(terminal_path),
        "runtime_contract_record": _record(
            PREDECESSOR_ROOT / epoch10.CONTRACT_FILENAME
        ),
        "runtime_lock_record": _record(
            PREDECESSOR_ROOT / epoch10.RUNTIME_LOCK_FILENAME
        ),
        "preauthorization_record": _record(
            PREDECESSOR_ROOT / epoch10.PREAUTHORIZATION_FILENAME
        ),
        "authorization_record": _record(
            PREDECESSOR_ROOT / epoch10.AUTHORIZATION_FILENAME
        ),
        "semantic_rejection_record": copy.deepcopy(receipt["semantic_rejection"]),
        "sidecar_record": _record(paths["sidecar"]),
        "raw_output_record": _record(paths["raw_output"]),
        "request_record": copy.deepcopy(io["request"]),
        "measured_usage": measured,
        "contract": contract,
        "turn_manifest": manifest,
        "original_authority_input": original,
        "fresh_repair_segment_id": str(manifest["segment_ids"][0]),
        "sanitized_invalid_diagnostic": {
            "segment_id": str(manifest["segment_ids"][0]),
            "error_class": "CanonicalV31OutputError",
            "diagnostic_path": diagnostic,
        },
    }


def _full_boundary(text: str) -> list[dict[str, int]]:
    return [
        {
            "window_id": 0,
            "chunk_index": 0,
            "owner_start": 0,
            "owner_end": len(text),
            "extract_start": 0,
            "extract_end": len(text),
        }
    ]


def _episode_packet(
    original: Mapping[str, Any],
    *,
    excluded_source_context: Sequence[str],
    segment_ids: Sequence[str],
) -> dict[str, Any]:
    context = original["original_context"]
    metadata = original["episode_metadata"]
    repairs = {str(row["segment_id"]): row for row in original["reference_repairs"]}
    if set(segment_ids) - set(repairs):
        raise CanonicalV31Epoch11Error("authority repair segment is unavailable")
    segments = []
    for segment_id in segment_ids:
        source = repairs[segment_id]
        text = source["segment_text"]
        if _sha256_bytes(text.encode("utf-8")) != source["segment_text_sha256"]:
            raise CanonicalV31Epoch11Error("authority segment text drifted")
        segments.append(
            {
                "segment_id": segment_id,
                "segment_text": text,
                "segment_quality": copy.deepcopy(
                    source["legacy_reference_label"]["segment_quality"]
                ),
                "boundaries": _full_boundary(text),
                "density_stratum": "authority_regeneration",
            }
        )
    return {
        "episode_id": original["episode_id"],
        "source_name": metadata["source_name"],
        "episode_title": metadata["episode_title"],
        "context_summary": context["context_summary"],
        "speaker_map": copy.deepcopy(context["speaker_map"]),
        "section_map": copy.deepcopy(context["section_map"]),
        "entity_seed": copy.deepcopy(context["entity_seed"]),
        "concept_seed": copy.deepcopy(context["concept_seed"]),
        "extraction_guidance": context["extraction_guidance"],
        "excluded_source_context": list(excluded_source_context),
        "segments": segments,
    }


def _prepare_canonical_request(
    original: Mapping[str, Any],
    *,
    excluded_source_context: Sequence[str],
    segment_ids: Sequence[str],
) -> dict[str, Any]:
    batch_size = 3 if len(segment_ids) <= 3 else 8
    requests = adapter.prepare_episode_batches(
        _episode_packet(
            original,
            excluded_source_context=excluded_source_context,
            segment_ids=segment_ids,
        ),
        batch_size=batch_size,
        thread_mode="new_thread",
    )
    if len(requests) != 1 or requests[0]["segment_ids"] != list(segment_ids):
        raise CanonicalV31Epoch11Error("canonical authority request partition drifted")
    return requests[0]


def _turn_id(stage: str, episode_id: str, segment_ids: Sequence[str]) -> str:
    identity = {
        "plan_epoch": PLAN_EPOCH,
        "stage": stage,
        "episode_id": episode_id,
        "segment_ids": list(segment_ids),
    }
    return "authority11_" + stage + "_" + _sha256_bytes(
        _canonical_json(identity).encode("ascii")
    )[:24]


def _write_request_io(root: Path, turn_id: str, request: Mapping[str, Any]) -> dict[str, Any]:
    prepared = root / PREPARED_DIRECTORY / turn_id
    prepared.mkdir(parents=True, exist_ok=True)
    request_record = _write_json(prepared / "request.private.json", request)
    input_record = _write_json(prepared / "input.private.json", request["private_input"])
    prompt_record = _write_bytes(
        prepared / "prompt.private.txt", request["prompt"].encode("utf-8")
    )
    base_record = _write_bytes(
        prepared / "base-instructions.private.txt",
        request["base_instructions"].encode("utf-8"),
    )
    schema_record = _write_json(prepared / "output-schema.json", request["output_schema"])
    binding = {
        "schema_version": IO_BINDING_VERSION,
        "stage": "canonical_labels",
        "turn_id": turn_id,
        "episode_id": request["episode_id"],
        "segment_ids": copy.deepcopy(request["segment_ids"]),
        "request": request_record,
        "input": input_record,
        "prompt": prompt_record,
        "base_instructions": base_record,
        "output_schema": schema_record,
        "canonical_adapter_binding_sha256": _sha256_bytes(
            _canonical_json(adapter.build_six_arm_matrix_binding()).encode("ascii")
        ),
        "legacy_invalid_labels_model_visible": False,
        "validation_diagnostics_model_visible": False,
    }
    _write_json(prepared / "io-binding.json", binding)
    return binding


def _write_context_io(
    root: Path, *, turn_id: str, source_manifest: Mapping[str, Any]
) -> dict[str, Any]:
    prepared = root / PREPARED_DIRECTORY / turn_id
    prepared.mkdir(parents=True, exist_ok=True)
    input_path = _verify_record(source_manifest["input"], label="source context input")
    prompt_path = _verify_record(source_manifest["prompt"], label="source context prompt")
    base_path = _verify_record(
        source_manifest["base_instructions"], label="source context base"
    )
    schema_path = _verify_record(
        source_manifest["output_schema"], label="source context schema"
    )
    input_record = _write_bytes(prepared / "input.private.json", input_path.read_bytes())
    prompt_record = _write_bytes(prepared / "prompt.private.txt", prompt_path.read_bytes())
    base_record = _write_bytes(
        prepared / "base-instructions.private.txt", base_path.read_bytes()
    )
    schema_record = _write_bytes(prepared / "output-schema.json", schema_path.read_bytes())
    binding = {
        "schema_version": IO_BINDING_VERSION,
        "stage": "context",
        "turn_id": turn_id,
        "episode_id": source_manifest["episode_id"],
        "segment_ids": [],
        "request": None,
        "input": input_record,
        "prompt": prompt_record,
        "base_instructions": base_record,
        "output_schema": schema_record,
        "canonical_adapter_binding_sha256": _sha256_bytes(
            _canonical_json(adapter.build_six_arm_matrix_binding()).encode("ascii")
        ),
        "legacy_invalid_labels_model_visible": False,
        "validation_diagnostics_model_visible": False,
    }
    _write_json(prepared / "io-binding.json", binding)
    return binding


def _write_turn_manifest(
    root: Path,
    *,
    turn_id: str,
    episode_id: str,
    stage: str,
    segment_ids: Sequence[str],
    io_binding: Mapping[str, Any] | None,
    original_authority_input: Mapping[str, Any],
    combined_output_schema: Mapping[str, Any],
    context_turn_id: str | None = None,
) -> dict[str, Any]:
    prepared = root / PREPARED_DIRECTORY / turn_id
    prepared.mkdir(parents=True, exist_ok=True)
    manifest = {
        "schema_version": TURN_MANIFEST_VERSION,
        "stage": stage,
        "turn_id": turn_id,
        "episode_id": episode_id,
        "segment_ids": list(segment_ids),
        "io_binding": (
            _record(prepared / "io-binding.json") if io_binding is not None else None
        ),
        "dynamic_io_binding_relative_path": (
            None if io_binding is not None else str(Path(PREPARED_DIRECTORY) / turn_id / "io-binding.json")
        ),
        "original_authority_input": copy.deepcopy(original_authority_input),
        "combined_output_schema": copy.deepcopy(combined_output_schema),
        "context_turn_id": context_turn_id,
        "model": MODEL,
        "effort": EFFORT,
        "semantic_retry_count": 0,
        "legacy_invalid_labels_model_visible": False,
        "validation_diagnostics_model_visible": False,
        "quality_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    return _write_json(prepared / "turn-manifest.json", manifest)


def _prepare_contract(root: Path) -> dict[str, Any]:
    predecessor = _validate_epoch10_predecessor()
    v10_contract = predecessor["contract"]
    adopted = root / ADOPTED_DIRECTORY
    adopted.mkdir(parents=True, exist_ok=True)
    diagnostic_record = _write_json(
        adopted / "epoch10-representability-diagnostic.json",
        {
            "schema_version": REJECTION_VERSION,
            **predecessor["sanitized_invalid_diagnostic"],
            "transcript_text_present": False,
            "legacy_label_text_present": False,
            "fresh_llm_repair_required": True,
            "existing_canonical_evidence_max_chars": (
                adapter.CANONICAL_EVIDENCE_MAX_CHARS
            ),
            "diagnostic_model_visible": False,
        },
    )
    repair_manifest = predecessor["turn_manifest"]
    original_repair_record = copy.deepcopy(repair_manifest["original_authority_input"])
    original_repair = predecessor["original_authority_input"]
    context_record = copy.deepcopy(
        v10_contract["adopted_evidence"]["epoch8_valid_context_output"]
    )
    context = _load_object(
        _verify_record(context_record, label="epoch-8 context"), label="epoch-8 context"
    )
    repair_segment_id = predecessor["fresh_repair_segment_id"]
    repair_request = _prepare_canonical_request(
        original_repair,
        excluded_source_context=context["excluded_source_context"],
        segment_ids=[repair_segment_id],
    )
    repair_turn_id = _turn_id(
        "canonical_labels", str(original_repair["episode_id"]), [repair_segment_id]
    )
    repair_io = _write_request_io(root, repair_turn_id, repair_request)
    repair_manifest_record = _write_turn_manifest(
        root,
        turn_id=repair_turn_id,
        episode_id=str(original_repair["episode_id"]),
        stage="repair_canonical_labels",
        segment_ids=[repair_segment_id],
        io_binding=repair_io,
        original_authority_input=original_repair_record,
        combined_output_schema=copy.deepcopy(repair_manifest["combined_output_schema"]),
    )
    turns: list[dict[str, Any]] = [
        {
            "ordinal": 0,
            "episode_id": str(original_repair["episode_id"]),
            "stage": "repair_canonical_labels",
            "turn_id": repair_turn_id,
            "turn_manifest": repair_manifest_record,
        }
    ]
    episodes: list[dict[str, Any]] = []
    for episode_index, source_episode in enumerate(v10_contract["episodes"]):
        original_record = copy.deepcopy(source_episode["original_authority_input"])
        original = _load_object(
            _verify_record(original_record, label="untouched original input"),
            label="untouched original input",
        )
        source_context_turn = next(
            turn
            for turn in v10_contract["turns"]
            if turn["turn_id"] == source_episode["context_turn_id"]
        )
        source_context_io = epoch10._io_binding(  # noqa: SLF001
            PREDECESSOR_ROOT, source_context_turn
        )
        segment_ids = list(original["invalid_reference_segment_ids"])
        context_turn_id = _turn_id(
            "context", str(original["episode_id"]), segment_ids
        )
        label_turn_id = _turn_id(
            "canonical_labels", str(original["episode_id"]), segment_ids
        )
        context_io = _write_context_io(
            root, turn_id=context_turn_id, source_manifest=source_context_io
        )
        context_manifest_record = _write_turn_manifest(
            root,
            turn_id=context_turn_id,
            episode_id=str(original["episode_id"]),
            stage="context",
            segment_ids=[],
            io_binding=context_io,
            original_authority_input=original_record,
            combined_output_schema=copy.deepcopy(
                source_episode["combined_output_schema"]
            ),
        )
        label_manifest_record = _write_turn_manifest(
            root,
            turn_id=label_turn_id,
            episode_id=str(original["episode_id"]),
            stage="canonical_labels",
            segment_ids=segment_ids,
            io_binding=None,
            original_authority_input=original_record,
            combined_output_schema=copy.deepcopy(
                source_episode["combined_output_schema"]
            ),
            context_turn_id=context_turn_id,
        )
        turns.extend(
            [
                {
                    "ordinal": 1 + episode_index * 2,
                    "episode_id": str(original["episode_id"]),
                    "stage": "context",
                    "turn_id": context_turn_id,
                    "turn_manifest": context_manifest_record,
                },
                {
                    "ordinal": 2 + episode_index * 2,
                    "episode_id": str(original["episode_id"]),
                    "stage": "canonical_labels",
                    "turn_id": label_turn_id,
                    "turn_manifest": label_manifest_record,
                },
            ]
        )
        episodes.append(
            {
                "episode_ordinal": episode_index,
                "episode_id": str(original["episode_id"]),
                "segment_ids": segment_ids,
                "context_turn_id": context_turn_id,
                "label_turn_id": label_turn_id,
                "original_authority_input": original_record,
                "combined_output_schema": copy.deepcopy(
                    source_episode["combined_output_schema"]
                ),
            }
        )
    epoch8_contract = _load_object(
        epoch9.EPOCH8_ROOT / epoch9.epoch8.CONTRACT_FILENAME,
        label="epoch-8 contract",
    )
    adopted_evidence = copy.deepcopy(v10_contract["adopted_evidence"])
    adopted_evidence.update(
        {
            "epoch10_receipt": predecessor["receipt_record"],
            "epoch10_terminal": predecessor["terminal_record"],
            "epoch10_runtime_contract": predecessor["runtime_contract_record"],
            "epoch10_runtime_lock": predecessor["runtime_lock_record"],
            "epoch10_preauthorization": predecessor["preauthorization_record"],
            "epoch10_authorization": predecessor["authorization_record"],
            "epoch10_semantic_rejection": predecessor[
                "semantic_rejection_record"
            ],
            "epoch10_sidecar": predecessor["sidecar_record"],
            "epoch10_raw_output": predecessor["raw_output_record"],
            "epoch10_request": predecessor["request_record"],
            "epoch10_representability_diagnostic": diagnostic_record,
        }
    )
    return {
        "schema_version": CONTRACT_VERSION,
        "state": "frozen_bounded_evidence_authority_recovery_ready",
        "directive": _record(DIRECTIVE_PATH),
        "predecessor_root": str(PREDECESSOR_ROOT.resolve()),
        "adopted_evidence": adopted_evidence,
        "authority_plan": copy.deepcopy(v10_contract["authority_plan"]),
        "authority_plan_receipt": copy.deepcopy(
            v10_contract["authority_plan_receipt"]
        ),
        "source_authority_turns": copy.deepcopy(
            v10_contract["source_authority_turns"]
        ),
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
        "quota_points_per_million_tokens": int(
            epoch8_contract["quota_points_per_million_tokens"]
        ),
        "projected_phase_quota_points": math.ceil(
            NEW_TOTAL_TOKEN_CAP
            * int(epoch8_contract["quota_points_per_million_tokens"])
            / 1_000_000
        ),
        "minimum_remaining_reserve_percent": int(
            epoch8_contract["minimum_remaining_reserve_percent"]
        ),
        "capacity_safety_margin_percent": int(
            epoch8_contract["capacity_safety_margin_percent"]
        ),
        "maximum_wall_seconds_per_turn": MAXIMUM_WALL_SECONDS_PER_TURN,
        "phase_wall_seconds_ceiling": NEW_TURN_COUNT * MAXIMUM_WALL_SECONDS_PER_TURN,
        "operator_wall_safety_margin_seconds": int(
            epoch8_contract["operator_wall_safety_margin_seconds"]
        ),
        "full_thread_total_usage_is_accounting_authority": True,
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
        "runtime_module": _record(Path(__file__)),
        "epoch10_runtime_module": _record(Path(epoch10.__file__)),
        "epoch9_runtime_module": _record(Path(epoch9.__file__)),
        "epoch7_runtime_module": _record(Path(epoch7.__file__)),
        "authority_plan_module": _record(Path(authority_plan.__file__)),
        "bounded_evidence_adapter_module": _record(Path(adapter.__file__)),
        "canonical_base_adapter_module": _record(Path(canonical_base.__file__)),
        "official_transport_module": _record(Path(codex_app_server.__file__)),
        "directive": _record(DIRECTIVE_PATH),
        "runtime_contract": _record(root / CONTRACT_FILENAME),
        "turn_manifests": [copy.deepcopy(row["turn_manifest"]) for row in contract["turns"]],
        "adopted_evidence": copy.deepcopy(contract["adopted_evidence"]),
        "pinned_official_codex_binary": _record(adapter.PINNED_CODEX),
        "pinned_protocol_schema": _record(codex_app_server.PROTOCOL_SCHEMA_PATH),
        "pinned_codex_cli_version": codex_app_server.PINNED_CODEX_CLI_VERSION,
        "canonical_adapter_binding": adapter.build_six_arm_matrix_binding(),
        "effective_instruction_sources": adapter.expected_instruction_source_contract(),
        "trusted_client_factory": "bounded_evidence_adapter._client_factory",
        "model": MODEL,
        "effort": EFFORT,
        "new_turn_count": NEW_TURN_COUNT,
        "new_per_turn_thread_total_token_cap": NEW_PER_TURN_THREAD_TOTAL_TOKEN_CAP,
        "new_total_token_cap": NEW_TOTAL_TOKEN_CAP,
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
        "turn_manifests": [copy.deepcopy(row["turn_manifest"]) for row in contract["turns"]],
        "adopted_semantic_app_server_turn_count": 4,
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
        raise CanonicalV31Epoch11Error("authorization identity or window is invalid")
    return {
        "schema_version": AUTHORIZATION_VERSION,
        "state": "authorized_for_five_bounded_evidence_authority_turns",
        "authority": "direct_user_instruction",
        "authorized_by": "kolby",
        "operator_authorization_id": operator_authorization_id,
        "operator_authorization_statement_sha256": AUTHORIZATION_STATEMENT_SHA256,
        "issued_at": issued.isoformat(),
        "expires_at": expires.isoformat(),
        "authorized_scope": (
            "one_bounded_evidence_canonical_label_repair_plus_two_context_label_pairs"
        ),
        "runtime_contract": _record(root / CONTRACT_FILENAME),
        "runtime_lock": _record(root / RUNTIME_LOCK_FILENAME),
        "preauthorization_receipt": _record(root / PREAUTHORIZATION_FILENAME),
        "model": MODEL,
        "effort": EFFORT,
        "new_model_call_cap": NEW_TURN_COUNT,
        "new_total_token_cap": NEW_TOTAL_TOKEN_CAP,
        "new_per_turn_thread_total_token_cap": NEW_PER_TURN_THREAD_TOTAL_TOKEN_CAP,
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
    _verify_directive()
    output_root = root.expanduser().resolve()
    if output_root.exists():
        raise CanonicalV31Epoch11Error("epoch-11 output root already exists")
    output_root.mkdir(parents=True)
    contract = _prepare_contract(output_root)
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
    _verify_directive()
    output_root = root.expanduser().resolve()
    contract = _load_object(output_root / CONTRACT_FILENAME, label="runtime contract")
    if (
        contract.get("schema_version") != CONTRACT_VERSION
        or contract.get("state")
        != "frozen_bounded_evidence_authority_recovery_ready"
        or contract.get("exact_turn_count") != NEW_TURN_COUNT
        or [row.get("ordinal") for row in contract.get("turns", [])]
        != list(range(NEW_TURN_COUNT))
        or contract.get("model") != MODEL
        or contract.get("effort") != EFFORT
        or contract.get("semantic_retry_count") != 0
        or contract.get("quality_authorized") is not False
        or contract.get("holdout_authorized") is not False
        or contract.get("production_mutation_allowed") is not False
    ):
        raise CanonicalV31Epoch11Error("runtime contract drifted")
    lock = _load_object(output_root / RUNTIME_LOCK_FILENAME, label="runtime lock")
    if lock != _runtime_lock(output_root, contract):
        raise CanonicalV31Epoch11Error("runtime lock drifted")
    preauthorization = _load_object(
        output_root / PREAUTHORIZATION_FILENAME, label="preauthorization"
    )
    if preauthorization != _preauthorization(output_root, contract):
        raise CanonicalV31Epoch11Error("preauthorization drifted")
    return preauthorization


def verify_authorization(
    root: Path = DEFAULT_ROOT,
    *,
    expected_authorization_id: str | None,
    require_current: bool,
) -> dict[str, Any]:
    verify_preauthorization(root)
    output_root = root.expanduser().resolve()
    observed = _load_object(output_root / AUTHORIZATION_FILENAME, label="authorization")
    expected = _authorization(
        output_root,
        operator_authorization_id=str(observed.get("operator_authorization_id") or ""),
        issued_at=str(observed.get("issued_at") or ""),
        expires_at=str(observed.get("expires_at") or ""),
    )
    if observed != expected:
        raise CanonicalV31Epoch11Error("authorization drifted")
    if (
        expected_authorization_id is not None
        and observed["operator_authorization_id"] != expected_authorization_id
    ):
        raise CanonicalV31Epoch11Error("authorization ID drifted")
    if require_current and _now() >= _parse_timestamp(
        observed["expires_at"], label="authorization expires_at"
    ):
        raise CanonicalV31Epoch11Waiting("authorization expired before dispatch")
    return observed


def _turn_paths(root: Path, turn_id: str) -> dict[str, Path]:
    turn_root = root / TURNS_DIRECTORY / turn_id
    return {
        "root": turn_root,
        "attempt": turn_root / "semantic-attempt.json",
        "initial_capacity": turn_root / "capacity" / "initial",
        "preturn_capacity": turn_root / "capacity" / "preturn",
        "thread": turn_root / "thread-start.json",
        "sidecar": turn_root / "sidecar.json",
        "raw_output": turn_root / "output.private.json",
        "projected_labels": turn_root / "canonical-labels.private.json",
        "provenance": turn_root / "evidence-provenance.private.json",
        "fidelity": turn_root / "semantic-fidelity.json",
        "validated_output": turn_root / "validated-authority-output.private.json",
        "result": turn_root / "turn-result.json",
    }


def _manifest(turn: Mapping[str, Any]) -> dict[str, Any]:
    return _load_object(
        _verify_record(turn["turn_manifest"], label="turn manifest"),
        label="turn manifest",
    )


def _io_binding(root: Path, turn: Mapping[str, Any]) -> dict[str, Any]:
    value = _load_object(_io_binding_path(root, turn), label="turn I/O binding")
    expected_keys = {
        "schema_version",
        "stage",
        "turn_id",
        "episode_id",
        "segment_ids",
        "request",
        "input",
        "prompt",
        "base_instructions",
        "output_schema",
        "canonical_adapter_binding_sha256",
        "legacy_invalid_labels_model_visible",
        "validation_diagnostics_model_visible",
    }
    if (
        set(value) != expected_keys
        or value.get("schema_version") != IO_BINDING_VERSION
        or value.get("turn_id") != turn["turn_id"]
        or value.get("episode_id") != turn["episode_id"]
        or value.get("canonical_adapter_binding_sha256")
        != _sha256_bytes(
            _canonical_json(adapter.build_six_arm_matrix_binding()).encode("ascii")
        )
        or value.get("legacy_invalid_labels_model_visible") is not False
        or value.get("validation_diagnostics_model_visible") is not False
    ):
        raise CanonicalV31Epoch11Error("turn I/O binding drifted")
    input_path = _verify_record(value["input"], label="turn input")
    prompt_path = _verify_record(value["prompt"], label="turn prompt")
    base_path = _verify_record(value["base_instructions"], label="turn base")
    schema_path = _verify_record(value["output_schema"], label="turn schema")
    if value["stage"] == "canonical_labels":
        request = _load_object(
            _verify_record(value["request"], label="canonical request"),
            label="canonical request",
        )
        try:
            validated = adapter.validate_prepared_request(request)
        except adapter.CanonicalV31EpisodeBatchError as exc:
            raise CanonicalV31Epoch11Error("canonical request drifted") from exc
        if (
            value["segment_ids"] != validated["segment_ids"]
            or _load_object(input_path, label="turn input")
            != validated["private_input"]
            or prompt_path.read_text(encoding="utf-8") != validated["prompt"]
            or base_path.read_text(encoding="utf-8") != validated["base_instructions"]
            or _load_object(schema_path, label="turn schema")
            != validated["output_schema"]
        ):
            raise CanonicalV31Epoch11Error("canonical I/O artifacts drifted")
    elif (
        value["stage"] != "context"
        or value["request"] is not None
        or value["segment_ids"] != []
    ):
        raise CanonicalV31Epoch11Error("context I/O binding drifted")
    return value


def _io_binding_path(root: Path, turn: Mapping[str, Any]) -> Path:
    manifest = _manifest(turn)
    record = manifest.get("io_binding")
    if record is None:
        path = root / str(manifest["dynamic_io_binding_relative_path"])
    else:
        path = _verify_record(record, label="turn I/O binding")
    return path


def _materialize_dynamic_label_io(
    root: Path, contract: Mapping[str, Any], turn: Mapping[str, Any]
) -> dict[str, Any]:
    manifest = _manifest(turn)
    if manifest["stage"] != "canonical_labels":
        return _io_binding(root, turn)
    path = root / str(manifest["dynamic_io_binding_relative_path"])
    if path.is_file():
        return _load_object(path, label="dynamic I/O binding")
    context_turn = next(
        row for row in contract["turns"] if row["turn_id"] == manifest["context_turn_id"]
    )
    context_result = _load_object(
        _turn_paths(root, str(context_turn["turn_id"]))["result"],
        label="context turn result",
    )
    context_output = _load_object(
        _verify_record(context_result["validated_output"], label="context output"),
        label="context output",
    )
    original = _load_object(
        _verify_record(
            manifest["original_authority_input"], label="original authority input"
        ),
        label="original authority input",
    )
    request = _prepare_canonical_request(
        original,
        excluded_source_context=context_output["excluded_source_context"],
        segment_ids=manifest["segment_ids"],
    )
    return _write_request_io(root, str(turn["turn_id"]), request)


def _thread_binding(
    thread: Any,
    *,
    root: Path,
    contract: Mapping[str, Any],
    turn: Mapping[str, Any],
    io: Mapping[str, Any],
) -> dict[str, Any]:
    base_path = _verify_record(io["base_instructions"], label="base instructions")
    sources = adapter.expected_instruction_source_contract()
    if (
        getattr(thread, "model", None) != MODEL
        or getattr(thread, "ephemeral", None) is not True
        or getattr(thread, "cwd", None) != str(PROJECT_ROOT.resolve())
        or not isinstance(getattr(thread, "thread_id", None), str)
        or not thread.thread_id
        or getattr(thread, "base_instructions_sha256", None)
        != _sha256_bytes(base_path.read_bytes())
        or getattr(thread, "base_instructions_bytes", None) != base_path.stat().st_size
        or getattr(thread, "instruction_sources_sha256", None)
        != sources["effective_instruction_sources_sha256"]
        or getattr(thread, "instruction_sources_count", None)
        != sources["effective_instruction_sources_count"]
        or getattr(thread, "persisted_path_sha256", None) is not None
    ):
        raise CanonicalV31Epoch11Error("semantic thread contract drifted")
    return {
        "schema_version": epoch7.THREAD_BINDING_VERSION,
        "turn_id": turn["turn_id"],
        "episode_id": turn["episode_id"],
        "thread_id": thread.thread_id,
        "model": contract["model"],
        "effort": contract["effort"],
        "cwd": str(PROJECT_ROOT.resolve()),
        "ephemeral": True,
        "base_instructions_sha256": thread.base_instructions_sha256,
        "base_instructions_bytes": thread.base_instructions_bytes,
        "instruction_sources_sha256": thread.instruction_sources_sha256,
        "instruction_sources_count": thread.instruction_sources_count,
        "persisted_path_sha256": None,
        "io_binding": _record(_io_binding_path(root, turn)),
        "quality_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }


def _verify_thread_binding(
    value: Mapping[str, Any],
    *,
    root: Path,
    contract: Mapping[str, Any],
    turn: Mapping[str, Any],
    io: Mapping[str, Any],
) -> dict[str, Any]:
    expected_sources = adapter.expected_instruction_source_contract()
    base_path = _verify_record(io["base_instructions"], label="base instructions")
    expected_keys = {
        "schema_version",
        "turn_id",
        "episode_id",
        "thread_id",
        "model",
        "effort",
        "cwd",
        "ephemeral",
        "base_instructions_sha256",
        "base_instructions_bytes",
        "instruction_sources_sha256",
        "instruction_sources_count",
        "persisted_path_sha256",
        "io_binding",
        "quality_authorized",
        "holdout_authorized",
        "production_mutation_allowed",
    }
    if (
        set(value) != expected_keys
        or value.get("schema_version") != epoch7.THREAD_BINDING_VERSION
        or value.get("turn_id") != turn["turn_id"]
        or value.get("episode_id") != turn["episode_id"]
        or not isinstance(value.get("thread_id"), str)
        or not value["thread_id"]
        or value.get("model") != contract["model"]
        or value.get("effort") != contract["effort"]
        or value.get("cwd") != str(PROJECT_ROOT.resolve())
        or value.get("ephemeral") is not True
        or value.get("base_instructions_sha256") != _sha256_bytes(base_path.read_bytes())
        or value.get("base_instructions_bytes") != base_path.stat().st_size
        or value.get("instruction_sources_sha256")
        != expected_sources["effective_instruction_sources_sha256"]
        or value.get("instruction_sources_count")
        != expected_sources["effective_instruction_sources_count"]
        or value.get("persisted_path_sha256") is not None
        or value.get("io_binding") != _record(_io_binding_path(root, turn))
        or value.get("quality_authorized") is not False
        or value.get("holdout_authorized") is not False
        or value.get("production_mutation_allowed") is not False
    ):
        raise CanonicalV31Epoch11Error("thread binding drifted")
    return copy.deepcopy(dict(value))


def _context_output(raw: Mapping[str, Any], *, episode_id: str) -> dict[str, Any]:
    excluded = raw.get("excluded_source_context")
    if (
        raw.get("schema_version") != epoch9.CONTEXT_OUTPUT_VERSION
        or raw.get("episode_id") != episode_id
        or not isinstance(excluded, list)
        or any(not isinstance(item, str) or not item for item in excluded)
    ):
        raise CanonicalV31Epoch11Rejected("context output is malformed")
    return copy.deepcopy(dict(raw))


def _combine_repair_output(
    contract: Mapping[str, Any],
    turn: Mapping[str, Any],
    projected: Mapping[str, Any],
) -> dict[str, Any]:
    manifest = _manifest(turn)
    original = _load_object(
        _verify_record(
            manifest["original_authority_input"], label="repair original input"
        ),
        label="repair original input",
    )
    adopted_labels = _load_object(
        _verify_record(
            contract["adopted_evidence"]["epoch8_valid_reference_labels"],
            label="epoch-8 valid labels",
        ),
        label="epoch-8 valid labels",
    )["labels"]
    projected_epoch9 = _load_object(
        _verify_record(
            contract["adopted_evidence"]["epoch9_projected_valid_label"],
            label="epoch-9 projected label",
        ),
        label="epoch-9 projected label",
    )
    new_labels = projected["labels"]
    labels_by_id = {
        str(row["segment_id"]): copy.deepcopy(dict(row))
        for row in [*adopted_labels, projected_epoch9, *new_labels]
    }
    expected_ids = list(original["invalid_reference_segment_ids"])
    if set(labels_by_id) != set(expected_ids):
        raise CanonicalV31Epoch11Rejected("repair label partition drifted")
    context = _load_object(
        _verify_record(
            contract["adopted_evidence"]["epoch8_valid_context_output"],
            label="epoch-8 context",
        ),
        label="epoch-8 context",
    )
    combined = {
        "schema_version": authority_plan.OUTPUT_VERSION,
        "episode_id": str(turn["episode_id"]),
        "excluded_source_context": copy.deepcopy(context["excluded_source_context"]),
        "repaired_reference_labels": [labels_by_id[item] for item in expected_ids],
    }
    schema = _load_object(
        _verify_record(manifest["combined_output_schema"], label="combined schema"),
        label="combined schema",
    )
    try:
        return authority_plan.validate_authority_output(
            combined, turn_input=original, output_schema=schema
        )
    except authority_plan.CanonicalV31Epoch7InputAuthorityPlanError as exc:
        raise CanonicalV31Epoch11Rejected(
            "repair projection failed unchanged authority validation"
        ) from exc


def _combine_episode_output(
    root: Path,
    contract: Mapping[str, Any],
    turn: Mapping[str, Any],
    projected: Mapping[str, Any],
) -> dict[str, Any]:
    manifest = _manifest(turn)
    original = _load_object(
        _verify_record(
            manifest["original_authority_input"], label="episode original input"
        ),
        label="episode original input",
    )
    context_turn = next(
        row for row in contract["turns"] if row["turn_id"] == manifest["context_turn_id"]
    )
    context_result = _load_object(
        _turn_paths(root, str(context_turn["turn_id"]))["result"],
        label="context result",
    )
    context = _load_object(
        _verify_record(context_result["validated_output"], label="context output"),
        label="context output",
    )
    expected_ids = list(original["invalid_reference_segment_ids"])
    if [row["segment_id"] for row in projected["labels"]] != expected_ids:
        raise CanonicalV31Epoch11Rejected("canonical labels changed authority order")
    combined = {
        "schema_version": authority_plan.OUTPUT_VERSION,
        "episode_id": str(turn["episode_id"]),
        "excluded_source_context": copy.deepcopy(context["excluded_source_context"]),
        "repaired_reference_labels": copy.deepcopy(projected["labels"]),
    }
    schema = _load_object(
        _verify_record(manifest["combined_output_schema"], label="combined schema"),
        label="combined schema",
    )
    try:
        return authority_plan.validate_authority_output(
            combined, turn_input=original, output_schema=schema
        )
    except authority_plan.CanonicalV31Epoch7InputAuthorityPlanError as exc:
        raise CanonicalV31Epoch11Rejected(
            "canonical labels failed unchanged authority validation"
        ) from exc


def _finalize_turn(
    *,
    root: Path,
    contract: Mapping[str, Any],
    authorization: Mapping[str, Any],
    turn: Mapping[str, Any],
    completed_turn_count: int,
) -> dict[str, Any]:
    paths = _turn_paths(root, str(turn["turn_id"]))
    io = _io_binding(root, turn)
    expected_attempt = epoch7._attempt_payload(  # noqa: SLF001
        root=root, contract=contract, authorization=authorization, turn=turn
    )
    if _load_object(paths["attempt"], label="semantic attempt") != expected_attempt:
        raise CanonicalV31Epoch11Error("semantic attempt drifted")
    for boundary, bundle in (
        ("initial_before_thread", paths["initial_capacity"]),
        ("preturn_before_turn", paths["preturn_capacity"]),
    ):
        request = epoch7._capacity_request(  # noqa: SLF001
            root=root,
            contract=contract,
            authorization=authorization,
            turn=turn,
            boundary=boundary,
            completed_turn_count=completed_turn_count,
        )
        epoch7._verify_capacity_bundle(  # noqa: SLF001
            bundle,
            expected_request=request,
            contract=contract,
            authorization=authorization,
            historical=True,
        )
    thread_binding = _verify_thread_binding(
        _load_object(paths["thread"], label="thread binding"),
        root=root,
        contract=contract,
        turn=turn,
        io=io,
    )
    telemetry = epoch9._validate_completed_sidecar_common(  # noqa: SLF001
        sidecar_path=paths["sidecar"],
        output_path=paths["raw_output"],
        prompt_path=_verify_record(io["prompt"], label="turn prompt"),
        base_path=_verify_record(io["base_instructions"], label="turn base"),
        schema_path=_verify_record(io["output_schema"], label="turn schema"),
        thread_binding=thread_binding,
    )
    try:
        raw_output = json.loads(telemetry["raw_message"])
    except json.JSONDecodeError as exc:
        raise CanonicalV31Epoch11Rejected("turn output is not structured JSON") from exc
    schema = _load_object(
        _verify_record(io["output_schema"], label="turn output schema"),
        label="turn output schema",
    )
    try:
        canonical_base._validate_schema(raw_output, schema, "$")  # noqa: SLF001
    except canonical_base.CanonicalV31OutputError as exc:
        raise CanonicalV31Epoch11Rejected("turn output failed its frozen schema") from exc
    if turn["stage"] == "context":
        validated = _context_output(raw_output, episode_id=str(turn["episode_id"]))
        combined = None
        projected = None
    else:
        request = _load_object(
            _verify_record(io["request"], label="canonical request"),
            label="canonical request",
        )
        try:
            projected = adapter.validate_and_project_output(request, raw_output)
        except adapter.CanonicalV31EpisodeBatchError as exc:
            raise CanonicalV31Epoch11Rejected(
                "canonical source-unit output failed projection"
            ) from exc
        combined = (
            _combine_repair_output(contract, turn, projected)
            if turn["stage"] == "repair_canonical_labels"
            else _combine_episode_output(root, contract, turn, projected)
        )
        validated = combined
    if not paths["validated_output"].exists():
        _write_json(paths["validated_output"], validated)
        if projected is not None:
            _write_json(paths["projected_labels"], projected["labels"])
            _write_json(paths["provenance"], projected["provenance"])
            _write_json(paths["fidelity"], projected["fidelity"])
    else:
        if _load_object(paths["validated_output"], label="validated output") != validated:
            raise CanonicalV31Epoch11Error("validated output drifted")
    result = {
        "schema_version": TURN_RESULT_VERSION,
        "state": "validated",
        "ordinal": turn["ordinal"],
        "stage": turn["stage"],
        "turn_id": turn["turn_id"],
        "episode_id": turn["episode_id"],
        "thread_id": telemetry["thread_id"],
        "semantic_turn_id": telemetry["turn_id"],
        "last_usage": telemetry["last_usage"],
        "thread_total_usage": telemetry["thread_total_usage"],
        "wall_elapsed_seconds": telemetry["wall_elapsed_seconds"],
        "sidecar": _record(paths["sidecar"]),
        "raw_output": _record(paths["raw_output"]),
        "validated_output": _record(paths["validated_output"]),
        "canonical_labels": (
            _record(paths["projected_labels"]) if projected is not None else None
        ),
        "evidence_provenance": (
            _record(paths["provenance"]) if projected is not None else None
        ),
        "semantic_fidelity": (
            _record(paths["fidelity"]) if projected is not None else None
        ),
        "full_thread_total_usage_is_accounting_authority": True,
        "semantic_retry_count": 0,
        "quality_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    if paths["result"].exists():
        if _load_object(paths["result"], label="turn result") != result:
            raise CanonicalV31Epoch11Error("turn result drifted")
    else:
        _write_json(paths["result"], result)
    return result


def _turn_state(root: Path, turn: Mapping[str, Any]) -> str:
    paths = _turn_paths(root, str(turn["turn_id"]))
    if paths["result"].is_file():
        return "validated"
    semantic = [
        paths["attempt"],
        paths["thread"],
        paths["sidecar"],
        paths["raw_output"],
    ]
    if not any(path.exists() for path in semantic):
        return "absent"
    if (
        paths["attempt"].is_file()
        and paths["thread"].is_file()
        and paths["sidecar"].is_file()
        and paths["raw_output"].is_file()
    ):
        sidecar = _load_object(paths["sidecar"], label="turn sidecar")
        if sidecar.get("state") == "completed" and sidecar.get("status") == "completed":
            return "recoverable_completed"
    return "partial"


def _accounting(root: Path, contract: Mapping[str, Any]) -> dict[str, Any]:
    totals: list[dict[str, int]] = []
    last: list[dict[str, int]] = []
    thread_ids: list[str] = []
    turn_ids: list[str] = []
    wall = 0.0
    call_count = 0
    for turn in contract["turns"]:
        sidecar_path = _turn_paths(root, str(turn["turn_id"]))["sidecar"]
        if not sidecar_path.is_file():
            continue
        sidecar = _load_object(sidecar_path, label="accounting sidecar")
        if isinstance(sidecar.get("thread_id"), str) and sidecar["thread_id"]:
            thread_ids.append(sidecar["thread_id"])
        if isinstance(sidecar.get("turn_id"), str) and sidecar["turn_id"]:
            turn_ids.append(sidecar["turn_id"])
            call_count += 1
        if isinstance(sidecar.get("usage"), Mapping):
            last.append(_usage(sidecar["usage"], label="last usage"))
        if isinstance(sidecar.get("thread_total_usage"), Mapping):
            totals.append(_usage(sidecar["thread_total_usage"], label="thread total"))
        elapsed = sidecar.get("wall_elapsed_seconds")
        if isinstance(elapsed, (int, float)) and not isinstance(elapsed, bool) and elapsed >= 0:
            wall += float(elapsed)
    if len(thread_ids) != len(set(thread_ids)) or len(turn_ids) != len(set(turn_ids)):
        raise CanonicalV31Epoch11Error("semantic thread or turn ID was reused")
    return {
        "semantic_model_call_count": call_count,
        "semantic_retry_count": 0,
        "measured_usage": _sum_usage(totals),
        "last_inference_increment_usage": _sum_usage(last),
        "measured_usage_turn_count": len(totals),
        "unknown_usage_turn_count": max(0, call_count - len(totals)),
        "wall_elapsed_seconds": wall,
        "thread_ids": thread_ids,
        "semantic_turn_ids": turn_ids,
    }


def _attempt_evidence_records(
    root: Path, contract: Mapping[str, Any]
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for turn in contract["turns"]:
        paths = _turn_paths(root, str(turn["turn_id"]))
        if not paths["attempt"].is_file():
            continue
        records: dict[str, Any] = {"attempt": _record(paths["attempt"])}
        for boundary, bundle in (
            ("initial_capacity", paths["initial_capacity"]),
            ("preturn_capacity", paths["preturn_capacity"]),
        ):
            if bundle.is_dir():
                records[boundary] = {
                    name: _record(bundle / filename)
                    for name, filename in (
                        ("request", "request.json"),
                        ("provider_response", "provider-response.private.json"),
                        ("measurement", "measurement.json"),
                    )
                }
        for name in ("thread", "sidecar", "raw_output"):
            if paths[name].is_file():
                records[name] = _record(paths[name])
        rows.append(
            {
                "turn_id": turn["turn_id"],
                "episode_id": turn["episode_id"],
                "stage": turn["stage"],
                "records": records,
            }
        )
    return rows


def _verify_attempt_lineage(
    *,
    root: Path,
    contract: Mapping[str, Any],
    authorization: Mapping[str, Any],
    turn: Mapping[str, Any],
    completed_turn_count: int,
) -> None:
    paths = _turn_paths(root, str(turn["turn_id"]))
    semantic_paths = (
        paths["attempt"],
        paths["thread"],
        paths["sidecar"],
        paths["raw_output"],
    )
    if not paths["attempt"].is_file():
        if any(path.exists() for path in semantic_paths[1:]):
            raise CanonicalV31Epoch11Error(
                "semantic artifact exists without an attempt"
            )
        return
    expected_attempt = epoch7._attempt_payload(  # noqa: SLF001
        root=root, contract=contract, authorization=authorization, turn=turn
    )
    if _load_object(paths["attempt"], label="semantic attempt") != expected_attempt:
        raise CanonicalV31Epoch11Error("semantic attempt drifted")
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
    if paths["thread"].is_file():
        if not paths["initial_capacity"].is_dir():
            raise CanonicalV31Epoch11Error("thread exists without initial capacity")
        io = _io_binding(root, turn)
        binding = _verify_thread_binding(
            _load_object(paths["thread"], label="thread binding"),
            root=root,
            contract=contract,
            turn=turn,
            io=io,
        )
        preturn_request = epoch7._capacity_request(  # noqa: SLF001
            root=root,
            contract=contract,
            authorization=authorization,
            turn=turn,
            boundary="preturn_before_turn",
            completed_turn_count=completed_turn_count,
        )
        if paths["preturn_capacity"].exists():
            epoch7._verify_capacity_bundle(  # noqa: SLF001
                paths["preturn_capacity"],
                expected_request=preturn_request,
                contract=contract,
                authorization=authorization,
                historical=True,
                require_available=False,
            )
        if paths["sidecar"].is_file():
            sidecar = _load_object(paths["sidecar"], label="partial sidecar")
            if sidecar.get("state") == "completed" and paths["raw_output"].is_file():
                epoch9._validate_completed_sidecar_common(  # noqa: SLF001
                    sidecar_path=paths["sidecar"],
                    output_path=paths["raw_output"],
                    prompt_path=_verify_record(io["prompt"], label="turn prompt"),
                    base_path=_verify_record(io["base_instructions"], label="turn base"),
                    schema_path=_verify_record(io["output_schema"], label="turn schema"),
                    thread_binding=binding,
                )
            else:
                synthetic_manifest = {
                    "prompt": copy.deepcopy(io["prompt"]),
                    "base_instructions": copy.deepcopy(io["base_instructions"]),
                    "output_schema": copy.deepcopy(io["output_schema"]),
                }
                epoch7._validate_partial_sidecar(  # noqa: SLF001
                    sidecar_path=paths["sidecar"],
                    output_path=paths["raw_output"],
                    manifest=synthetic_manifest,
                    thread_binding=binding,
                )
        elif paths["raw_output"].exists():
            raise CanonicalV31Epoch11Error("raw output exists without a sidecar")
    elif any(path.exists() for path in semantic_paths[2:]):
        raise CanonicalV31Epoch11Error("turn artifact exists without thread binding")


def _cost_failed_checks(root: Path, contract: Mapping[str, Any]) -> list[str]:
    accounting = _accounting(root, contract)
    failed: list[str] = []
    if accounting["semantic_model_call_count"] > NEW_TURN_COUNT:
        failed.append("new_semantic_model_call_cap")
    if accounting["measured_usage"]["total_tokens"] > NEW_TOTAL_TOKEN_CAP:
        failed.append("new_full_thread_total_token_cap")
    for turn in contract["turns"]:
        path = _turn_paths(root, str(turn["turn_id"]))["sidecar"]
        if path.is_file():
            sidecar = _load_object(path, label="cost sidecar")
            if isinstance(sidecar.get("thread_total_usage"), Mapping) and _usage(
                sidecar["thread_total_usage"], label="cost thread total"
            )["total_tokens"] > NEW_PER_TURN_THREAD_TOTAL_TOKEN_CAP:
                failed.append("new_per_turn_full_thread_total_token_cap")
                break
    return failed


def _merge_output(
    root: Path, contract: Mapping[str, Any], results: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    by_stage_episode = {
        (str(row["stage"]), str(row["episode_id"])): row for row in results
    }
    repair = next(
        row for row in results if row["stage"] == "repair_canonical_labels"
    )
    validated_records = [
        copy.deepcopy(contract["adopted_evidence"]["epoch7_validated_output"]),
        copy.deepcopy(repair["validated_output"]),
    ]
    for episode in contract["episodes"]:
        result = by_stage_episode[("canonical_labels", str(episode["episode_id"]))]
        validated_records.append(copy.deepcopy(result["validated_output"]))
    synthetic = [
        {"turn_id": source["turn_id"], "validated_output": record}
        for source, record in zip(contract["source_authority_turns"], validated_records)
    ]
    merge_contract = {
        "authority_plan": copy.deepcopy(contract["authority_plan"]),
        "authority_plan_receipt": copy.deepcopy(contract["authority_plan_receipt"]),
        "turns": copy.deepcopy(contract["source_authority_turns"]),
    }
    try:
        merged = epoch7._build_merged_output(  # noqa: SLF001
            root=root, contract=merge_contract, turn_results=synthetic
        )
    except epoch7.CanonicalV31Epoch7AuthorityRuntimeError as exc:
        raise CanonicalV31Epoch11Error(str(exc)) from exc
    path = root / MERGED_OUTPUT_FILENAME
    if path.exists():
        if _load_object(path, label="merged output") != merged:
            raise CanonicalV31Epoch11Error("merged output drifted")
    else:
        _write_json(path, merged)
    return merged


def _terminal_payload(
    *,
    root: Path,
    contract: Mapping[str, Any],
    state: str,
    reason: str,
    rejection: Mapping[str, Any] | None,
) -> dict[str, Any]:
    accounting = _accounting(root, contract)
    results = [
        _load_object(_turn_paths(root, str(turn["turn_id"]))["result"], label="turn result")
        for turn in contract["turns"]
        if _turn_paths(root, str(turn["turn_id"]))["result"].is_file()
    ]
    failed = _cost_failed_checks(root, contract)
    if rejection is not None:
        failed.append("semantic_output_validity")
    failed = list(dict.fromkeys(failed))
    if state == "passed" and failed:
        state = "rejected"
        reason = "epoch11_bounded_evidence_cost_rejected"
    merged_record = None
    if state == "passed":
        if len(results) != NEW_TURN_COUNT:
            raise CanonicalV31Epoch11Error("passing receipt lacks every turn result")
        _merge_output(root, contract, results)
        merged_record = _record(root / MERGED_OUTPUT_FILENAME)
    adopted_usage = _sum_usage(
        [
            _usage(
                _load_object(
                    _verify_record(
                        contract["adopted_evidence"]["epoch7_receipt"],
                        label="epoch-7 receipt",
                    ),
                    label="epoch-7 receipt",
                )["measured_usage"],
                label="epoch-7 usage",
            ),
            _usage(
                _load_object(
                    _verify_record(
                        contract["adopted_evidence"]["epoch8_accounting_rejection"],
                        label="epoch-8 accounting",
                    ),
                    label="epoch-8 accounting",
                )["full_thread_total_usage"],
                label="epoch-8 usage",
            ),
            _usage(
                _load_object(
                    _verify_record(
                        contract["adopted_evidence"]["epoch9_receipt"],
                        label="epoch-9 receipt",
                    ),
                    label="epoch-9 receipt",
                )["new_measured_usage"],
                label="epoch-9 usage",
            ),
            _usage(
                _load_object(
                    _verify_record(
                        contract["adopted_evidence"]["epoch10_receipt"],
                        label="epoch-10 receipt",
                    ),
                    label="epoch-10 receipt",
                )["new_measured_usage"],
                label="epoch-10 usage",
            ),
        ]
    )
    if adopted_usage["total_tokens"] != ADOPTED_DEVELOPMENT_QA_TOTAL_TOKENS:
        raise CanonicalV31Epoch11Error("adopted usage drifted")
    return {
        "schema_version": RECEIPT_VERSION,
        "thread_id": THREAD_ID,
        "plan_epoch": PLAN_EPOCH,
        "step_id": STEP_ID,
        "state": state,
        "terminal_reason": reason,
        "output_root": str(root.resolve()),
        "directive": _record(DIRECTIVE_PATH),
        "runtime_contract": _record(root / CONTRACT_FILENAME),
        "runtime_lock": _record(root / RUNTIME_LOCK_FILENAME),
        "preauthorization_receipt": _record(root / PREAUTHORIZATION_FILENAME),
        "operator_authorization": _record(root / AUTHORIZATION_FILENAME),
        "adopted_evidence": copy.deepcopy(contract["adopted_evidence"]),
        "turn_results": [
            _record(_turn_paths(root, str(turn["turn_id"]))["result"])
            for turn in contract["turns"]
            if _turn_paths(root, str(turn["turn_id"]))["result"].is_file()
        ],
        "attempt_evidence": _attempt_evidence_records(root, contract),
        "semantic_rejection": (
            _record(root / "semantic-rejection.json") if rejection is not None else None
        ),
        "adopted_semantic_app_server_turn_count": 4,
        "new_completed_validated_turn_count": len(results),
        "new_exact_turn_count": NEW_TURN_COUNT,
        "adopted_measured_usage": adopted_usage,
        "new_measured_usage": accounting["measured_usage"],
        "cumulative_development_qa_measured_usage": _sum_usage(
            [adopted_usage, accounting["measured_usage"]]
        ),
        "last_inference_increment_usage": accounting[
            "last_inference_increment_usage"
        ],
        "new_semantic_model_call_count": accounting["semantic_model_call_count"],
        "cumulative_semantic_app_server_turn_count": (
            4 + accounting["semantic_model_call_count"]
        ),
        "semantic_retry_count": 0,
        "new_measured_usage_turn_count": accounting["measured_usage_turn_count"],
        "new_unknown_usage_turn_count": accounting["unknown_usage_turn_count"],
        "new_wall_elapsed_seconds": accounting["wall_elapsed_seconds"],
        "thread_ids": accounting["thread_ids"],
        "semantic_turn_ids": accounting["semantic_turn_ids"],
        "failed_checks": failed,
        "full_thread_total_usage_is_accounting_authority": True,
        "merged_authority_output": merged_record,
        "extraction_plan_rebuild_required": state == "passed",
        "winner_frozen": False,
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
    rejection: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if rejection is not None:
        rejection_payload = {
            "schema_version": REJECTION_VERSION,
            "state": "rejected",
            "turn_id": rejection.get("turn_id"),
            "episode_id": rejection.get("episode_id"),
            "stage": rejection.get("stage"),
            "error_class": rejection.get("error_class"),
            "diagnostic_path": rejection.get("diagnostic_path"),
            "transcript_text_present": False,
            "legacy_label_text_present": False,
            "semantic_retry_count": 0,
        }
        _write_json(root / "semantic-rejection.json", rejection_payload)
        rejection = rejection_payload
    payload = _terminal_payload(
        root=root,
        contract=contract,
        state=state,
        reason=reason,
        rejection=rejection,
    )
    raw = _pretty_json(payload).encode("ascii")
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
    if not receipt_path.is_file() or not terminal_path.is_file():
        raise CanonicalV31Epoch11Error("terminal mirrors are incomplete")
    if receipt_path.read_bytes() != terminal_path.read_bytes():
        raise CanonicalV31Epoch11Error("terminal mirrors differ")
    observed = _load_object(receipt_path, label="plan-step receipt")
    contract = _load_object(output_root / CONTRACT_FILENAME, label="runtime contract")
    open_seen = False
    for index, turn in enumerate(contract["turns"]):
        paths = _turn_paths(output_root, str(turn["turn_id"]))
        if paths["result"].is_file():
            if open_seen:
                raise CanonicalV31Epoch11Error(
                    "validated turns are not a contiguous prefix"
                )
            _finalize_turn(
                root=output_root,
                contract=contract,
                authorization=authorization,
                turn=turn,
                completed_turn_count=index,
            )
        else:
            open_seen = True
            _verify_attempt_lineage(
                root=output_root,
                contract=contract,
                authorization=authorization,
                turn=turn,
                completed_turn_count=index,
            )
    state = observed.get("state")
    if state not in {"passed", "rejected", "waiting"}:
        raise CanonicalV31Epoch11Error("receipt state drifted")
    rejection = None
    if observed.get("semantic_rejection") is not None:
        rejection = _load_object(
            _verify_record(observed["semantic_rejection"], label="semantic rejection"),
            label="semantic rejection",
        )
    expected = _terminal_payload(
        root=output_root,
        contract=contract,
        state=str(state),
        reason=str(observed.get("terminal_reason") or ""),
        rejection=rejection,
    )
    if observed != expected:
        raise CanonicalV31Epoch11Error("plan-step receipt drifted")
    return observed


def status_recovery(root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    verify_preauthorization(root)
    output_root = root.expanduser().resolve()
    if (output_root / RECEIPT_FILENAME).is_file():
        receipt = verify_recovery_receipt(output_root)
        return {
            "schema_version": "pif_canonical_v31_epoch11_status_v1",
            "state": receipt["state"],
            "reason": receipt["terminal_reason"],
            "new_semantic_model_call_count": receipt[
                "new_semantic_model_call_count"
            ],
            "turn_states": [
                _turn_state(output_root, turn)
                for turn in _load_object(
                    output_root / CONTRACT_FILENAME, label="runtime contract"
                )["turns"]
            ],
        }
    authorization = verify_authorization(
        output_root, expected_authorization_id=None, require_current=False
    )
    contract = _load_object(output_root / CONTRACT_FILENAME, label="runtime contract")
    return {
        "schema_version": "pif_canonical_v31_epoch11_status_v1",
        "state": "ready",
        "reason": (
            "ready_for_one_bounded_evidence_repair_plus_two_context_label_pairs"
        ),
        "authorization_expires_at": authorization["expires_at"],
        "new_semantic_model_call_count": _accounting(output_root, contract)[
            "semantic_model_call_count"
        ],
        "turn_states": [_turn_state(output_root, turn) for turn in contract["turns"]],
    }


def _reject_external_auth_material() -> None:
    try:
        epoch9._reject_external_auth_material()  # noqa: SLF001
    except epoch9.CanonicalV31Epoch9SplitRecoveryError as exc:
        raise CanonicalV31Epoch11Error(str(exc)) from exc


async def execute_recovery(
    *,
    root: Path = DEFAULT_ROOT,
    operator_authorization_id: str,
) -> dict[str, Any]:
    _reject_external_auth_material()
    verify_preauthorization(root)
    output_root = root.expanduser().resolve()
    if (output_root / RECEIPT_FILENAME).is_file():
        return verify_recovery_receipt(output_root)
    authorization = verify_authorization(
        output_root,
        expected_authorization_id=operator_authorization_id,
        require_current=True,
    )
    contract = _load_object(output_root / CONTRACT_FILENAME, label="runtime contract")
    results: list[dict[str, Any]] = []
    try:
        start_index = 0
        for index, turn in enumerate(contract["turns"]):
            state = _turn_state(output_root, turn)
            if state == "partial":
                return _write_terminal(
                    root=output_root,
                    contract=contract,
                    state="waiting",
                    reason="epoch11_partial_attempt_preserved_no_replay",
                )
            if state in {"validated", "recoverable_completed"}:
                if index != len(results):
                    raise CanonicalV31Epoch11Error(
                        "completed turns are not a contiguous prefix"
                    )
                results.append(
                    _finalize_turn(
                        root=output_root,
                        contract=contract,
                        authorization=authorization,
                        turn=turn,
                        completed_turn_count=index,
                    )
                )
                start_index = index + 1
                continue
            if any(
                _turn_state(output_root, later) != "absent"
                for later in contract["turns"][index + 1 :]
            ):
                raise CanonicalV31Epoch11Error(
                    "semantic artifacts exist after an unstarted turn"
                )
            start_index = index
            break
        if len(results) == NEW_TURN_COUNT:
            _merge_output(output_root, contract, results)
            return _write_terminal(
                root=output_root,
                contract=contract,
                state="passed",
                reason="epoch11_bounded_evidence_authority_recovery_passed",
            )
        async with adapter._client_factory() as client:  # noqa: SLF001
            account = getattr(client, "account_summary", None)
            if (
                not isinstance(account, Mapping)
                or account.get("type") != "chatgpt"
                or account.get("plan_type") != "pro"
            ):
                raise CanonicalV31Epoch11Error(
                    "epoch-11 requires managed ChatGPT Pro auth"
                )
            for index in range(start_index, NEW_TURN_COUNT):
                turn = contract["turns"][index]
                state = _turn_state(output_root, turn)
                if turn["stage"] == "canonical_labels":
                    _materialize_dynamic_label_io(output_root, contract, turn)
                io = _io_binding(output_root, turn)
                paths = _turn_paths(output_root, str(turn["turn_id"]))
                paths["root"].mkdir(parents=True, exist_ok=True)
                verify_preauthorization(output_root)
                verify_authorization(
                    output_root,
                    expected_authorization_id=operator_authorization_id,
                    require_current=True,
                )
                if paths["attempt"].exists():
                    raise CanonicalV31Epoch11Error(
                        "semantic attempt exists; replay is forbidden"
                    )
                _write_json(
                    paths["attempt"],
                    epoch7._attempt_payload(  # noqa: SLF001
                        root=output_root,
                        contract=contract,
                        authorization=authorization,
                        turn=turn,
                    ),
                )
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
                base_path = _verify_record(io["base_instructions"], label="turn base")
                thread = await client.start_thread(
                    model=MODEL,
                    base_instructions=base_path.read_text(encoding="utf-8"),
                    cwd=PROJECT_ROOT,
                    ephemeral=True,
                )
                binding = _thread_binding(
                    thread, root=output_root, contract=contract, turn=turn, io=io
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
                prompt_path = _verify_record(io["prompt"], label="turn prompt")
                schema_path = _verify_record(io["output_schema"], label="turn schema")
                result = await client.run_structured_turn(
                    thread=thread,
                    effort=EFFORT,
                    prompt=prompt_path.read_text(encoding="utf-8"),
                    output_schema=_load_object(schema_path, label="turn schema"),
                    sidecar_path=paths["sidecar"],
                    output_path=paths["raw_output"],
                    batch_size=1,
                    thread_mode="new_thread",
                    timeout_seconds=float(MAXIMUM_WALL_SECONDS_PER_TURN),
                )
                if result.status_ok is not True:
                    raise CanonicalV31Epoch11Waiting(
                        "semantic turn did not complete successfully"
                    )
                completed = _finalize_turn(
                    root=output_root,
                    contract=contract,
                    authorization=authorization,
                    turn=turn,
                    completed_turn_count=index,
                )
                raw_output = _load_object(paths["raw_output"], label="raw output")
                if (
                    result.thread_id != completed["thread_id"]
                    or result.turn_id != completed["semantic_turn_id"]
                    or not isinstance(result.output, Mapping)
                    or _canonical_json(result.output) != _canonical_json(raw_output)
                ):
                    raise CanonicalV31Epoch11Error("in-memory turn lineage drifted")
                results.append(completed)
                if _cost_failed_checks(output_root, contract):
                    return _write_terminal(
                        root=output_root,
                        contract=contract,
                        state="rejected",
                        reason="epoch11_bounded_evidence_cost_rejected",
                    )
    except CanonicalV31Epoch11Rejected as exc:
        current = contract["turns"][len(results)] if len(results) < NEW_TURN_COUNT else {}
        return _write_terminal(
            root=output_root,
            contract=contract,
            state="rejected",
            reason="epoch11_bounded_evidence_semantic_output_rejected",
            rejection={
                "turn_id": current.get("turn_id"),
                "episode_id": current.get("episode_id"),
                "stage": current.get("stage"),
                "error_class": type(exc).__name__,
                "diagnostic_path": str(exc),
            },
        )
    except codex_app_server.AppServerStructuredOutputError as exc:
        current = contract["turns"][len(results)] if len(results) < NEW_TURN_COUNT else {}
        return _write_terminal(
            root=output_root,
            contract=contract,
            state="rejected",
            reason="epoch11_structured_output_rejected",
            rejection={
                "turn_id": current.get("turn_id"),
                "episode_id": current.get("episode_id"),
                "stage": current.get("stage"),
                "error_class": type(exc).__name__,
                "diagnostic_path": "structured_output_invalid",
            },
        )
    except (
        CanonicalV31Epoch11Waiting,
        epoch7.CanonicalV31Epoch7AuthorityWaiting,
        codex_app_server.AppServerError,
        asyncio.TimeoutError,
        OSError,
    ):
        return _write_terminal(
            root=output_root,
            contract=contract,
            state="waiting",
            reason="epoch11_operational_waiting_no_replay",
        )
    _merge_output(output_root, contract, results)
    return _write_terminal(
        root=output_root,
        contract=contract,
        state="passed",
        reason="epoch11_bounded_evidence_authority_recovery_passed",
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=(
            "python3 -m "
            "research_factory.app_server_canonical_v31_epoch11_bounded_evidence_authority_recovery"
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
    except CanonicalV31Epoch11Waiting as exc:
        print(_pretty_json({"ok": False, "waiting": True, "error": str(exc)}), end="")
        return 75
    except (
        CanonicalV31Epoch11Error,
        epoch7.CanonicalV31Epoch7AuthorityRuntimeError,
        epoch9.CanonicalV31Epoch9SplitRecoveryError,
    ) as exc:
        print(_pretty_json({"ok": False, "error": str(exc)}), end="")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
