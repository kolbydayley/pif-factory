from __future__ import annotations

"""Compact canonical extraction constrained to one final structured message."""

import copy
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_canonical_v31_compact_unit_pointer_episode_batch as compact


base = compact.base
PROJECT_ROOT = compact.PROJECT_ROOT
MODEL = compact.MODEL
EFFORT = compact.EFFORT
PINNED_CODEX = compact.PINNED_CODEX
CANONICAL_LABEL_PACK = compact.CANONICAL_LABEL_PACK
CANONICAL_LABEL_SCHEMA_SHA256 = compact.CANONICAL_LABEL_SCHEMA_SHA256
CANONICAL_EVIDENCE_MAX_CHARS = compact.CANONICAL_EVIDENCE_MAX_CHARS
SOURCE_UNIT_MAX_CHARS = compact.SOURCE_UNIT_MAX_CHARS
SUPPORTED_BATCH_SIZES = compact.SUPPORTED_BATCH_SIZES
SUPPORTED_THREAD_MODES = compact.SUPPORTED_THREAD_MODES
RETRY_COUNT = compact.RETRY_COUNT
USAGE_FIELDS = compact.USAGE_FIELDS
CanonicalV31EpisodeBatchError = compact.CanonicalV31EpisodeBatchError
CanonicalV31OutputError = compact.CanonicalV31OutputError
CanonicalV31TelemetryError = compact.CanonicalV31TelemetryError
_client_factory = compact._client_factory
_verify_started_thread = compact._verify_started_thread
expected_instruction_source_contract = compact.expected_instruction_source_contract
verified_context_control_overlay = compact.verified_context_control_overlay

ADAPTER_SCHEMA_VERSION = "pif_canonical_v31_single_message_compact_pointer_batch_v1"
PROJECTION_SCHEMA_VERSION = "pif_canonical_v31_single_message_compact_projection_v1"
PROVENANCE_SCHEMA_VERSION = "pif_canonical_v31_single_message_compact_provenance_v1"
FIDELITY_SCHEMA_VERSION = "pif_canonical_v31_single_message_compact_fidelity_v1"
CANDIDATE_SYSTEM_ID = "pif_single_message_compact_pointer_ai_discourse_v3_1_v1"
CANONICAL_SCHEMA_STRATEGY = (
    "canonical_v31_one_final_structured_message_with_compact_metric_ranges_v1"
)
SINGLE_MESSAGE_PROTOCOL_VERSION = "pif_single_final_structured_message_v1"
METRIC_LITERAL_FIELDS = compact.METRIC_LITERAL_FIELDS
SINGLE_MESSAGE_INSTRUCTIONS = (
    "# Single-message structured-output contract\n"
    "Return exactly one final structured JSON response matching the supplied schema. "
    "Do not emit a status update, plan, commentary, summary, preliminary response, or "
    "prose before or after that JSON. Perform all analysis privately and submit only "
    "the completed structured result.\n\n"
)


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def semantic_integrity_contract() -> dict[str, Any]:
    value = copy.deepcopy(compact.semantic_integrity_contract())
    value.update(
        {
            "single_message_protocol_version": SINGLE_MESSAGE_PROTOCOL_VERSION,
            "exactly_one_final_structured_agent_message_requested": True,
            "preliminary_status_or_commentary_messages_requested": False,
            "semantic_contract_unchanged_from_compact_unit_pointer": True,
            "deterministic_semantic_pruning": False,
            "deterministic_support_filtering": False,
            "deterministic_deduplication": False,
            "deterministic_relabeling": False,
            "semantic_regex_or_keyword_rules": False,
        }
    )
    return value


def _single_message_request(request: Mapping[str, Any]) -> dict[str, Any]:
    compact.validate_prepared_request(request)
    value = copy.deepcopy(dict(request))
    if value["base_instructions"].startswith(SINGLE_MESSAGE_INSTRUCTIONS):
        raise CanonicalV31EpisodeBatchError("single-message instructions are duplicated")
    value["schema_version"] = ADAPTER_SCHEMA_VERSION
    value["candidate_system_id"] = CANDIDATE_SYSTEM_ID
    value["canonical_schema_strategy"] = CANONICAL_SCHEMA_STRATEGY
    value["batch_id"] = "cv31smc_" + base.sha256_text(
        _canonical_json(
            {
                "compact_batch_id": request["batch_id"],
                "candidate_system_id": CANDIDATE_SYSTEM_ID,
            }
        )
    )[:24]
    value["semantic_integrity"] = semantic_integrity_contract()
    value["private_input"]["schema_version"] = ADAPTER_SCHEMA_VERSION
    value["base_instructions"] = SINGLE_MESSAGE_INSTRUCTIONS + value["base_instructions"]
    value["base_instructions_sha256"] = base.sha256_text(value["base_instructions"])
    return value


def _compact_request(request: Mapping[str, Any]) -> dict[str, Any]:
    value = copy.deepcopy(dict(request))
    if value["base_instructions"].count(SINGLE_MESSAGE_INSTRUCTIONS) != 1:
        raise CanonicalV31EpisodeBatchError("single-message instructions drifted")
    if not value["base_instructions"].startswith(SINGLE_MESSAGE_INSTRUCTIONS):
        raise CanonicalV31EpisodeBatchError("single-message instructions are misplaced")
    value["base_instructions"] = value["base_instructions"][
        len(SINGLE_MESSAGE_INSTRUCTIONS) :
    ]
    local_request = compact._local_request(value)  # noqa: SLF001
    return compact._compact_request(local_request)  # noqa: SLF001


def prepare_episode_batches(
    episode: Mapping[str, Any], *, batch_size: int, thread_mode: str
) -> list[dict[str, Any]]:
    requests = compact.prepare_episode_batches(
        episode, batch_size=batch_size, thread_mode=thread_mode
    )
    values = [_single_message_request(request) for request in requests]
    for value in values:
        validate_prepared_request(value)
    return values


def validate_prepared_request(request: Mapping[str, Any]) -> dict[str, Any]:
    compact_request = _compact_request(request)
    validated = compact.validate_prepared_request(compact_request)
    expected = _single_message_request(validated)
    if dict(request) != expected:
        raise CanonicalV31EpisodeBatchError("single-message compact request drifted")
    return copy.deepcopy(dict(request))


def validate_and_project_output(
    request: Mapping[str, Any], output: Mapping[str, Any]
) -> dict[str, Any]:
    validated = validate_prepared_request(request)
    projected = compact.validate_and_project_output(_compact_request(validated), output)
    provenance = copy.deepcopy(projected["provenance"])
    provenance["schema_version"] = PROVENANCE_SCHEMA_VERSION
    provenance["candidate_system_id"] = CANDIDATE_SYSTEM_ID
    provenance["single_message_protocol_version"] = SINGLE_MESSAGE_PROTOCOL_VERSION
    fidelity = copy.deepcopy(projected["fidelity"])
    fidelity["schema_version"] = FIDELITY_SCHEMA_VERSION
    fidelity["candidate_system_id"] = CANDIDATE_SYSTEM_ID
    fidelity["single_final_structured_message_requested"] = True
    fidelity["semantic_contract_unchanged_from_compact_unit_pointer"] = True
    return {
        "schema_version": PROJECTION_SCHEMA_VERSION,
        "labels": projected["labels"],
        "provenance": provenance,
        "fidelity": fidelity,
    }


def validate_turn_sidecar(
    request: Mapping[str, Any],
    sidecar_path: Path,
    *,
    output_path: Path,
    expected_thread: Any,
) -> dict[str, Any]:
    validated = validate_prepared_request(request)
    sidecar = base._load_json(sidecar_path, "single-message compact sidecar")  # noqa: SLF001
    if not isinstance(sidecar, Mapping):
        raise CanonicalV31TelemetryError("turn sidecar is not an object")
    thread_preflight = _verify_started_thread(expected_thread, validated)
    if (
        sidecar.get("schema_version")
        != base.codex_app_server.TURN_SIDECAR_SCHEMA_VERSION
        or not base._is_iso_timestamp(sidecar.get("started_at"))  # noqa: SLF001
        or not base._is_iso_timestamp(sidecar.get("finished_at"))  # noqa: SLF001
        or sidecar.get("client_version")
        != base.codex_app_server.APP_SERVER_CLIENT_VERSION
        or sidecar.get("cli_version")
        != base.codex_app_server.PINNED_CODEX_CLI_VERSION
        or sidecar.get("protocol_schema_sha256")
        != base._sha256_file(base.codex_app_server.PROTOCOL_SCHEMA_PATH)  # noqa: SLF001
        or sidecar.get("transport") != "stdio"
        or not isinstance(sidecar.get("app_server_user_agent"), str)
        or not sidecar.get("app_server_user_agent")
        or isinstance(sidecar.get("max_message_bytes"), bool)
        or not isinstance(sidecar.get("max_message_bytes"), int)
        or sidecar.get("max_message_bytes") < 64 * 1024
        or sidecar.get("synthetic_debug_errors") is not False
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("plan_type") != "pro"
        or sidecar.get("thread_id") != thread_preflight["thread_id"]
        or not isinstance(sidecar.get("turn_id"), str)
        or not sidecar.get("turn_id")
        or sidecar.get("model") != MODEL
        or sidecar.get("effort") != EFFORT
        or sidecar.get("thread_mode") != validated["thread_mode"]
        or sidecar.get("batch_size") != validated["effective_batch_size"]
        or sidecar.get("prompt_sha256") != validated["prompt_sha256"]
        or sidecar.get("prompt_bytes") != len(validated["prompt"].encode("utf-8"))
        or sidecar.get("base_instructions_sha256")
        != validated["base_instructions_sha256"]
        or sidecar.get("base_instructions_bytes")
        != len(validated["base_instructions"].encode("utf-8"))
        or sidecar.get("instruction_sources_sha256")
        != thread_preflight["instruction_sources_sha256"]
        or sidecar.get("instruction_sources_count")
        != thread_preflight["instruction_sources_count"]
        or sidecar.get("output_schema_sha256")
        != validated["output_schema_sha256"]
        or sidecar.get("output_schema_bytes")
        != len(_canonical_json(validated["output_schema"]).encode("utf-8"))
        or sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("error_class") is not None
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or sidecar.get("recovery_reran_model") is not False
        or not isinstance(sidecar.get("stderr_sha256"), str)
        or len(sidecar.get("stderr_sha256")) != 64
        or isinstance(sidecar.get("stderr_bytes"), bool)
        or not isinstance(sidecar.get("stderr_bytes"), int)
        or sidecar.get("stderr_bytes") < 0
    ):
        raise CanonicalV31TelemetryError(
            "managed-auth completed single-message compact sidecar contract failed"
        )
    usage = base._usage_values(sidecar.get("usage"), "usage")  # noqa: SLF001
    total = base._usage_values(  # noqa: SLF001
        sidecar.get("thread_total_usage"), "thread_total_usage"
    )
    if any(total[field] < usage[field] for field in USAGE_FIELDS):
        raise CanonicalV31TelemetryError("thread total usage is below last usage")
    wall = base._nonnegative_number(  # noqa: SLF001
        sidecar.get("wall_elapsed_seconds"), "wall_elapsed_seconds"
    )
    if (
        not output_path.is_file()
        or sidecar.get("output_sha256") != base._output_message_hash(output_path)  # noqa: SLF001
        or Path(str(sidecar.get("output_path") or "")).expanduser().resolve()
        != output_path.expanduser().resolve()
    ):
        raise CanonicalV31TelemetryError("sidecar-bound raw output is absent or changed")
    return {
        "sidecar": copy.deepcopy(dict(sidecar)),
        "thread_id": sidecar["thread_id"],
        "turn_id": sidecar["turn_id"],
        "usage": usage,
        "thread_total_usage": total,
        "wall_elapsed_seconds": wall,
        "cached_input_tokens": total["cached_input_tokens"],
        "reasoning_output_tokens": total["reasoning_output_tokens"],
    }


def build_six_arm_matrix_binding() -> dict[str, Any]:
    return {
        "schema_version": "pif_canonical_v31_single_message_compact_binding_v1",
        "compact_adapter_binding": compact.build_six_arm_matrix_binding(),
        "compact_adapter_module_sha256": hashlib.sha256(
            Path(compact.__file__).read_bytes()
        ).hexdigest(),
        "single_message_adapter_module_sha256": hashlib.sha256(
            Path(__file__).read_bytes()
        ).hexdigest(),
        "candidate_system_id": CANDIDATE_SYSTEM_ID,
        "canonical_schema_strategy": CANONICAL_SCHEMA_STRATEGY,
        "single_message_protocol_version": SINGLE_MESSAGE_PROTOCOL_VERSION,
        "exactly_one_final_structured_agent_message_requested": True,
        "semantic_contract_unchanged_from_compact_unit_pointer": True,
        "semantic_postprocessing": False,
        "deterministic_semantic_pruning": False,
        "deterministic_deduplication": False,
        "deterministic_relabeling": False,
    }


__all__: Sequence[str] = (
    "ADAPTER_SCHEMA_VERSION",
    "CANDIDATE_SYSTEM_ID",
    "CanonicalV31EpisodeBatchError",
    "CanonicalV31OutputError",
    "CanonicalV31TelemetryError",
    "build_six_arm_matrix_binding",
    "prepare_episode_batches",
    "validate_and_project_output",
    "validate_prepared_request",
    "validate_turn_sidecar",
)
