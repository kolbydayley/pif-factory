from __future__ import annotations

"""Full-canonical extraction with primary and omission-audit event ledgers."""

import copy
import hashlib
import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_canonical_v31_nested_proposition_event_ledger_episode_batch as parent


base = parent.base
PROJECT_ROOT = parent.PROJECT_ROOT
MODEL = parent.MODEL
EFFORT = parent.EFFORT
PINNED_CODEX = parent.PINNED_CODEX
CANONICAL_LABEL_PACK = parent.CANONICAL_LABEL_PACK
CANONICAL_LABEL_SCHEMA_SHA256 = parent.CANONICAL_LABEL_SCHEMA_SHA256
SUPPORTED_BATCH_SIZES = parent.SUPPORTED_BATCH_SIZES
SUPPORTED_THREAD_MODES = parent.SUPPORTED_THREAD_MODES
RETRY_COUNT = parent.RETRY_COUNT
USAGE_FIELDS = parent.USAGE_FIELDS
CanonicalV31EpisodeBatchError = parent.CanonicalV31EpisodeBatchError
CanonicalV31OutputError = parent.CanonicalV31OutputError
CanonicalV31TelemetryError = parent.CanonicalV31TelemetryError
_client_factory = parent._client_factory
_verify_started_thread = parent._verify_started_thread
expected_instruction_source_contract = parent.expected_instruction_source_contract
verified_context_control_overlay = parent.verified_context_control_overlay

ADAPTER_SCHEMA_VERSION = "pif_canonical_v31_dual_pass_omission_audit_ledger_episode_batch_v1"
RAW_OUTPUT_SCHEMA_VERSION = "pif_canonical_v31_dual_pass_omission_audit_ledger_raw_output_v1"
PROJECTION_SCHEMA_VERSION = "pif_canonical_v31_dual_pass_omission_audit_ledger_projection_v1"
PROVENANCE_SCHEMA_VERSION = "pif_canonical_v31_dual_pass_omission_audit_ledger_provenance_v1"
FIDELITY_SCHEMA_VERSION = "pif_canonical_v31_dual_pass_omission_audit_ledger_fidelity_v1"
CANDIDATE_SYSTEM_ID = "pif_dual_pass_omission_audit_ledger_ai_discourse_v3_1_v1"
CANONICAL_SCHEMA_STRATEGY = "canonical_v31_dual_pass_omission_audit_ledger_v1"
DUAL_PASS_PROTOCOL_VERSION = "pif_source_unit_primary_plus_omission_audit_ledger_v1"
_PARENT_REQUEST_CACHE: dict[tuple[str, int, str, tuple[str, ...]], str] = {}


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def semantic_integrity_contract() -> dict[str, Any]:
    value = copy.deepcopy(parent.semantic_integrity_contract())
    value.update(
        {
            "dual_pass_protocol_version": DUAL_PASS_PROTOCOL_VERSION,
            "model_authors_primary_and_omission_audit_full_canonical_events": True,
            "deterministic_dual_ledger_concatenation_only": True,
            "audit_duplicates_are_preserved_for_llm_quality_scoring": True,
            "deterministic_semantic_defaults": {},
            "deterministic_semantic_pruning": False,
            "deterministic_support_filtering": False,
            "deterministic_deduplication": False,
            "deterministic_relabeling": False,
        }
    )
    return value


def _dual_pass_instructions() -> str:
    return (
        "# Independent omission-audit pass\n"
        "For every source unit, field 0 is the primary proposition-event ledger and "
        "field 2 is a second omission-audit proposition-event ledger. First enumerate "
        "the primary ledger normally. Then independently reread the complete supplied "
        "source unit and use field 2 only for grounded eligible propositions that the "
        "primary ledger omitted, including separately asserted recommendations, "
        "counterclaims, consequences, comparisons, mechanisms, and tradeoffs. Every "
        "item in both ledgers must contain its own complete canonical event. Do not copy "
        "an item merely to increase count, but do not suppress a supported audit item "
        "because it is adjacent to or shares an actor or topic with a primary item. "
        "Do not aim for a target count. Deterministic code preserves primary items first "
        "and audit items second, and never prunes, support-filters, deduplicates, "
        "relabels, or repairs either semantic lane.\n\n"
    )


def _episode_from_request(request: Mapping[str, Any]) -> dict[str, Any]:
    return parent._episode_from_request(request)  # noqa: SLF001


@lru_cache(maxsize=8)
def _parent_request_json(
    episode_json: str,
    batch_size: int,
    thread_mode: str,
    segment_ids: tuple[str, ...],
) -> str:
    values = parent.prepare_episode_batches(
        json.loads(episode_json), batch_size=batch_size, thread_mode=thread_mode
    )
    matches = [value for value in values if tuple(value["segment_ids"]) == segment_ids]
    if len(matches) != 1:
        raise CanonicalV31EpisodeBatchError("dual-pass parent request cannot be reconstructed")
    return _canonical_json(matches[0])


def _parent_cache_key(
    request: Mapping[str, Any],
) -> tuple[str, int, str, tuple[str, ...]]:
    return (
        _canonical_json(_episode_from_request(request)),
        int(request["batch_size_ceiling"]),
        str(request["thread_mode"]),
        tuple(request["segment_ids"]),
    )


def _parent_request(request: Mapping[str, Any]) -> dict[str, Any]:
    key = _parent_cache_key(request)
    encoded = _PARENT_REQUEST_CACHE.get(key)
    if encoded is None:
        encoded = _parent_request_json(*key)
        if len(_PARENT_REQUEST_CACHE) >= 8:
            _PARENT_REQUEST_CACHE.pop(next(iter(_PARENT_REQUEST_CACHE)))
        _PARENT_REQUEST_CACHE[key] = encoded
    return json.loads(encoded)


def _output_schema(request: Mapping[str, Any]) -> dict[str, Any]:
    schema = copy.deepcopy(request["output_schema"])
    row = schema["properties"]["s"]["items"]["properties"]["2"]["items"]
    if row.get("required") != ["0", "1"] or set(row.get("properties", {})) != {"0", "1"}:
        raise CanonicalV31EpisodeBatchError("dual-pass parent unit schema drifted")
    ledger = copy.deepcopy(row["properties"]["0"])
    concepts = copy.deepcopy(row["properties"]["1"])
    row["required"] = ["0", "1", "2"]
    row["properties"] = {
        "0": ledger,
        "1": concepts,
        "2": copy.deepcopy(ledger),
    }
    schema["$id"] = RAW_OUTPUT_SCHEMA_VERSION
    return schema


def _dual_pass_request(request: Mapping[str, Any]) -> dict[str, Any]:
    value = copy.deepcopy(dict(request))
    value["schema_version"] = ADAPTER_SCHEMA_VERSION
    value["candidate_system_id"] = CANDIDATE_SYSTEM_ID
    value["canonical_schema_strategy"] = CANONICAL_SCHEMA_STRATEGY
    value["batch_id"] = "cv31dpa_" + _sha256_text(
        _canonical_json(
            {
                "parent_batch_id": request["batch_id"],
                "candidate_system_id": CANDIDATE_SYSTEM_ID,
                "dual_pass_protocol_version": DUAL_PASS_PROTOCOL_VERSION,
            }
        )
    )[:24]
    value["semantic_integrity"] = semantic_integrity_contract()
    value["private_input"]["schema_version"] = ADAPTER_SCHEMA_VERSION
    value["base_instructions"] = _dual_pass_instructions() + value["base_instructions"]
    value["base_instructions_sha256"] = _sha256_text(value["base_instructions"])
    value["output_schema"] = _output_schema(request)
    value["output_schema_sha256"] = _sha256_text(_canonical_json(value["output_schema"]))
    _PARENT_REQUEST_CACHE[_parent_cache_key(value)] = _canonical_json(request)
    return value


def prepare_episode_batches(
    episode: Mapping[str, Any], *, batch_size: int, thread_mode: str
) -> list[dict[str, Any]]:
    values = [
        _dual_pass_request(request)
        for request in parent.prepare_episode_batches(
            episode, batch_size=batch_size, thread_mode=thread_mode
        )
    ]
    for value in values:
        validate_prepared_request(value)
    return values


def validate_prepared_request(request: Mapping[str, Any]) -> dict[str, Any]:
    expected = _dual_pass_request(_parent_request(request))
    if dict(request) != expected:
        raise CanonicalV31EpisodeBatchError("dual-pass omission-audit request drifted")
    return copy.deepcopy(dict(request))


def _parent_wire_output(
    request: Mapping[str, Any], output: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    if not isinstance(output, Mapping) or set(output) != {"s"} or not isinstance(output["s"], list):
        raise CanonicalV31OutputError("dual-pass output shape drifted")
    if len(output["s"]) != len(request["private_input"]["segments"]):
        raise CanonicalV31OutputError("dual-pass segment count drifted")
    converted = copy.deepcopy(dict(output))
    primary_count = 0
    audit_count = 0
    manifest: list[dict[str, Any]] = []
    for segment_index, segment in enumerate(converted["s"]):
        rows = segment.get("2") if isinstance(segment, Mapping) else None
        if not isinstance(rows, list):
            raise CanonicalV31OutputError("dual-pass unit rows drifted")
        for unit_index, row in enumerate(rows):
            if not isinstance(row, Mapping) or set(row) != {"0", "1", "2"}:
                raise CanonicalV31OutputError("dual-pass unit row is malformed")
            primary = row["0"]
            concepts = row["1"]
            audit = row["2"]
            if not all(isinstance(value, list) for value in (primary, concepts, audit)):
                raise CanonicalV31OutputError("dual-pass unit arrays are malformed")
            for lane, items in (("primary", primary), ("omission_audit", audit)):
                for item_index, item in enumerate(items):
                    if not isinstance(item, Mapping) or set(item) != {"0", "1"}:
                        raise CanonicalV31OutputError("dual-pass ledger item is malformed")
                    manifest.append(
                        {
                            "segment_index": segment_index,
                            "unit_index": unit_index,
                            "lane": lane,
                            "item_index": item_index,
                            "item_sha256": _sha256_text(_canonical_json(item)),
                        }
                    )
            primary_count += len(primary)
            audit_count += len(audit)
            segment["2"][unit_index] = {
                "0": copy.deepcopy([*primary, *audit]),
                "1": copy.deepcopy(concepts),
            }
    return converted, {
        "primary_ledger_item_count": primary_count,
        "omission_audit_ledger_item_count": audit_count,
        "dual_ledger_item_count": primary_count + audit_count,
        "dual_ledger_manifest_sha256": _sha256_text(_canonical_json(manifest)),
    }


def validate_and_project_output(
    request: Mapping[str, Any], output: Mapping[str, Any]
) -> dict[str, Any]:
    validated = validate_prepared_request(request)
    parent_output, diagnostics = _parent_wire_output(validated, output)
    projected = parent.validate_and_project_output(_parent_request(validated), parent_output)
    provenance = copy.deepcopy(projected["provenance"])
    provenance.update(
        {
            "schema_version": PROVENANCE_SCHEMA_VERSION,
            "candidate_system_id": CANDIDATE_SYSTEM_ID,
            "dual_pass_protocol_version": DUAL_PASS_PROTOCOL_VERSION,
            "dual_ledger_manifest_sha256": diagnostics["dual_ledger_manifest_sha256"],
        }
    )
    fidelity = copy.deepcopy(projected["fidelity"])
    fidelity.update(
        {
            "schema_version": FIDELITY_SCHEMA_VERSION,
            "candidate_system_id": CANDIDATE_SYSTEM_ID,
            "dual_pass_protocol_version": DUAL_PASS_PROTOCOL_VERSION,
            **diagnostics,
            "deterministic_dual_ledger_concatenation_only": True,
            "audit_duplicates_preserved": True,
            "deterministic_semantic_defaults": {},
            "deterministic_semantic_pruning": False,
            "deterministic_support_filtering": False,
            "deterministic_deduplication": False,
            "deterministic_relabeling": False,
        }
    )
    return {
        "schema_version": PROJECTION_SCHEMA_VERSION,
        "labels": projected["labels"],
        "provenance": provenance,
        "fidelity": fidelity,
    }


def encode_parent_output_for_test(
    request: Mapping[str, Any], parent_output: Mapping[str, Any]
) -> dict[str, Any]:
    validate_prepared_request(request)
    nested = parent.encode_parent_output_for_test(_parent_request(request), parent_output)
    value = copy.deepcopy(nested)
    for segment in value["s"]:
        for row in segment["2"]:
            row["2"] = []
    return value


def validate_turn_sidecar(
    request: Mapping[str, Any],
    sidecar_path: Path,
    *,
    output_path: Path,
    expected_thread: Any,
) -> dict[str, Any]:
    validated = validate_prepared_request(request)
    sidecar = base._load_json(sidecar_path, "dual-pass sidecar")  # noqa: SLF001
    thread_preflight = _verify_started_thread(expected_thread, validated)
    required = {
        "schema_version": base.codex_app_server.TURN_SIDECAR_SCHEMA_VERSION,
        "client_version": base.codex_app_server.APP_SERVER_CLIENT_VERSION,
        "cli_version": base.codex_app_server.PINNED_CODEX_CLI_VERSION,
        "protocol_schema_sha256": base._sha256_file(  # noqa: SLF001
            base.codex_app_server.PROTOCOL_SCHEMA_PATH
        ),
        "transport": "stdio",
        "auth_type": "chatgpt",
        "plan_type": "pro",
        "thread_id": thread_preflight["thread_id"],
        "model": MODEL,
        "effort": EFFORT,
        "thread_mode": validated["thread_mode"],
        "batch_size": validated["effective_batch_size"],
        "prompt_sha256": validated["prompt_sha256"],
        "prompt_bytes": len(validated["prompt"].encode("utf-8")),
        "base_instructions_sha256": validated["base_instructions_sha256"],
        "base_instructions_bytes": len(validated["base_instructions"].encode("utf-8")),
        "instruction_sources_sha256": thread_preflight["instruction_sources_sha256"],
        "instruction_sources_count": thread_preflight["instruction_sources_count"],
        "output_schema_sha256": validated["output_schema_sha256"],
        "output_schema_bytes": len(_canonical_json(validated["output_schema"]).encode("utf-8")),
        "state": "completed",
        "status": "completed",
        "error_class": None,
        "usage_status": "measured",
        "usage_complete": True,
        "recovery_reran_model": False,
        "synthetic_debug_errors": False,
    }
    if (
        not isinstance(sidecar, Mapping)
        or not base._is_iso_timestamp(sidecar.get("started_at"))  # noqa: SLF001
        or not base._is_iso_timestamp(sidecar.get("finished_at"))  # noqa: SLF001
        or any(sidecar.get(key) != expected for key, expected in required.items())
        or not isinstance(sidecar.get("turn_id"), str)
        or not sidecar.get("turn_id")
    ):
        raise CanonicalV31TelemetryError("managed-auth dual-pass sidecar contract failed")
    usage = base._usage_values(sidecar.get("usage"), "usage")  # noqa: SLF001
    total = base._usage_values(  # noqa: SLF001
        sidecar.get("thread_total_usage"), "thread_total_usage"
    )
    if any(total[field] < usage[field] for field in USAGE_FIELDS):
        raise CanonicalV31TelemetryError("dual-pass thread total usage is below last usage")
    wall = base._nonnegative_number(  # noqa: SLF001
        sidecar.get("wall_elapsed_seconds"), "wall_elapsed_seconds"
    )
    if (
        not output_path.is_file()
        or sidecar.get("output_sha256") != base._output_message_hash(output_path)  # noqa: SLF001
        or Path(str(sidecar.get("output_path") or "")).expanduser().resolve()
        != output_path.expanduser().resolve()
    ):
        raise CanonicalV31TelemetryError("sidecar-bound dual-pass output is absent or changed")
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
        "schema_version": "pif_canonical_v31_dual_pass_omission_audit_ledger_binding_v1",
        "parent_binding": parent.build_six_arm_matrix_binding(),
        "parent_adapter_module_sha256": hashlib.sha256(Path(parent.__file__).read_bytes()).hexdigest(),
        "adapter_module_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "candidate_system_id": CANDIDATE_SYSTEM_ID,
        "canonical_schema_strategy": CANONICAL_SCHEMA_STRATEGY,
        "dual_pass_protocol_version": DUAL_PASS_PROTOCOL_VERSION,
        "model": MODEL,
        "effort": EFFORT,
        "deterministic_dual_ledger_concatenation_only": True,
        "audit_duplicates_preserved": True,
        "deterministic_semantic_defaults": {},
        "deterministic_semantic_pruning": False,
        "deterministic_support_filtering": False,
        "deterministic_deduplication": False,
        "deterministic_relabeling": False,
    }


__all__: Sequence[str] = (
    "ADAPTER_SCHEMA_VERSION",
    "CANDIDATE_SYSTEM_ID",
    "CanonicalV31EpisodeBatchError",
    "CanonicalV31OutputError",
    "CanonicalV31TelemetryError",
    "DUAL_PASS_PROTOCOL_VERSION",
    "build_six_arm_matrix_binding",
    "encode_parent_output_for_test",
    "prepare_episode_batches",
    "validate_and_project_output",
    "validate_prepared_request",
    "validate_turn_sidecar",
)
