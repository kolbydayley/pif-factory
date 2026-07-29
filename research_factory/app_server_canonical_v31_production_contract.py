from __future__ import annotations

"""Frozen production authority for the direct canonical v3.1 adapter.

This module deliberately does not import the expanded-cap/epoch4 implementation
or the mutable development-matrix runtime.  A production freeze is valid only
when it is bound to the current canonical adapter and to independently persisted
matrix, quality, and untouched-holdout verification receipts.
"""

import copy
import hashlib
import inspect
import json
from pathlib import Path
from typing import Any, Mapping

from . import app_server_canonical_v31_episode_batch as adapter
from . import app_server_episode_context_runner as context_runner


FROZEN_CONFIGURATION_VERSION = "pif_canonical_v31_production_freeze_v1"
MATRIX_RECEIPT_VERSION = "pif_canonical_v31_production_matrix_lineage_v1"
QUALITY_RECEIPT_VERSION = "pif_canonical_v31_production_quality_lineage_v1"
HOLDOUT_RECEIPT_VERSION = "pif_canonical_v31_production_holdout_lineage_v1"
CAPACITY_CONTRACT_VERSION = "pif_canonical_v31_numeric_capacity_contract_v1"
PROMOTION_AUTHORITY_VERSION = "pif_canonical_v31_production_promotion_v1"

EXPECTED_VARIANT_IDS = tuple(
    f"canonical_v31_batch_{size}_{mode}"
    for size in adapter.SUPPORTED_BATCH_SIZES
    for mode in adapter.SUPPORTED_THREAD_MODES
)


class CanonicalV31ProductionContractError(RuntimeError):
    """The frozen canonical production authority is absent or has drifted."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.expanduser().resolve(strict=True).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _valid_sha256(value: Any) -> bool:
    return bool(
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _record(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve(strict=True)
    return {
        "path": str(resolved),
        "sha256": _sha256_file(resolved),
        "size_bytes": resolved.stat().st_size,
    }


def _verify_record(value: Any, *, label: str) -> Path:
    if not isinstance(value, Mapping) or set(value) != {
        "path",
        "sha256",
        "size_bytes",
    }:
        raise CanonicalV31ProductionContractError(f"{label} record drifted")
    path = Path(str(value["path"])).expanduser().resolve()
    if (
        not path.is_file()
        or not _valid_sha256(value.get("sha256"))
        or isinstance(value.get("size_bytes"), bool)
        or not isinstance(value.get("size_bytes"), int)
        or int(value["size_bytes"]) < 0
        or path.stat().st_size != int(value["size_bytes"])
        or _sha256_file(path) != value["sha256"]
    ):
        raise CanonicalV31ProductionContractError(f"{label} artifact drifted")
    return path


def _source_sha256(function: Any) -> str:
    return _sha256_bytes(inspect.getsource(function).encode("utf-8"))


def selected_variant_id(*, batch_size: int, thread_mode: str) -> str:
    if (
        isinstance(batch_size, bool)
        or batch_size not in adapter.SUPPORTED_BATCH_SIZES
        or thread_mode not in adapter.SUPPORTED_THREAD_MODES
    ):
        raise CanonicalV31ProductionContractError("selected canonical arm is invalid")
    return f"canonical_v31_batch_{batch_size}_{thread_mode}"


def capacity_admission_contract() -> dict[str, Any]:
    """Return the semantic-neutral numeric reserve contract.

    The managed endpoint exposes quota percentages, not token or wall-time
    reservations.  Token capacity is therefore a conservative policy-derived
    lower bound; wall capacity comes from a separately checksum-bound operator
    deadline.  Neither value can authorize or modify semantic behavior.
    """

    return {
        "schema_version": CAPACITY_CONTRACT_VERSION,
        "semantic_authority": False,
        "semantic_pruning_or_relabeling_allowed": False,
        "official_managed_auth_method": "account/rateLimits/read",
        "selected_limit_id": "codex",
        "all_applicable_rate_limit_controls_required": True,
        "quota_points_per_million_tokens": 17,
        "minimum_reserve_percent": 20,
        "concurrency_and_quantization_margin_percent": 5,
        "token_capacity_kind": "policy_derived_conservative_lower_bound",
        "reset_credits_counted_as_capacity": False,
        "wall_capacity_source": "checksum_bound_operator_deadline",
        "wall_safety_seconds": 300,
        "capacity_probe_boundary": "after_managed_client_initialize_before_semantic_thread_start",
        "required_live_receipt_state": "admitted_official_managed_auth_numeric_reserve",
        "fixture_receipts_promotable": False,
    }


def canonical_runtime_binding() -> dict[str, Any]:
    matrix = adapter.build_six_arm_matrix_binding()
    adapter_record = _record(Path(adapter.__file__).resolve())
    schema_record = copy.deepcopy(matrix["canonical_label_schema"])
    expected_arm_ids = [
        selected_variant_id(
            batch_size=int(arm["batch_size"]),
            thread_mode=str(arm["thread_mode"]),
        )
        for arm in matrix["arms"]
    ]
    if expected_arm_ids != list(EXPECTED_VARIANT_IDS):
        raise CanonicalV31ProductionContractError("canonical adapter arm matrix drifted")
    function_hashes = {
        "build_output_schema_sha256": _source_sha256(adapter.build_output_schema),
        "prepare_episode_batches_sha256": _source_sha256(adapter.prepare_episode_batches),
        "render_base_instructions_sha256": _source_sha256(adapter._render_base_instructions),
        "render_prompt_sha256": _source_sha256(adapter._render_prompt),
        "source_units_sha256": _source_sha256(adapter._source_units),
        "prepare_segment_sha256": _source_sha256(adapter._prepare_segment),
        "validate_and_project_output_sha256": _source_sha256(
            adapter.validate_and_project_output
        ),
    }
    return {
        "candidate_system_id": adapter.CANDIDATE_SYSTEM_ID,
        "model": adapter.MODEL,
        "effort": adapter.EFFORT,
        "adapter_module": adapter_record,
        "canonical_label_schema": schema_record,
        "canonical_label_schema_sha256": adapter.CANONICAL_LABEL_SCHEMA_SHA256,
        "canonical_schema_strategy": adapter.CANONICAL_SCHEMA_STRATEGY,
        "base_instructions_template_sha256": matrix[
            "base_instructions_template_sha256"
        ],
        "context_control_overlay_sha256": matrix[
            "context_control_overlay_sha256"
        ],
        "instruction_source_contract_sha256": matrix[
            "instruction_source_contract_sha256"
        ],
        "effective_instruction_sources_sha256": matrix[
            "instruction_source_contract"
        ]["effective_instruction_sources_sha256"],
        "effective_instruction_sources_count": matrix[
            "instruction_source_contract"
        ]["effective_instruction_sources_count"],
        "source_unit_chunking_version": adapter.SOURCE_UNIT_CHUNKING_VERSION,
        "source_unit_algorithm_hashes": function_hashes,
        "matrix_binding_sha256": _sha256_bytes(
            _canonical_json(matrix).encode("utf-8")
        ),
        "canonical_final_field_paths_sha256": matrix[
            "canonical_final_field_paths_sha256"
        ],
        "model_semantic_field_paths_sha256": matrix[
            "model_semantic_field_paths_sha256"
        ],
        "deterministic_provenance_field_paths_sha256": matrix[
            "deterministic_provenance_field_paths_sha256"
        ],
        "model_evidence_ownership_field_paths_sha256": matrix[
            "model_evidence_ownership_field_paths_sha256"
        ],
        "semantic_integrity": adapter.semantic_integrity_contract(),
        "expected_variant_ids": list(EXPECTED_VARIANT_IDS),
        "retry_count": 0,
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
    }


def episode_context_runtime_binding() -> dict[str, Any]:
    """Bind the exact official-live predecessor implementation.

    The fixture completion verifier is intentionally absent.  Production
    promotion binds the live contract/runtime/provision/completion chain and
    rechecks database currentness through the same SQLite connection used by
    the label queue at execution time.
    """

    return {
        "runner_module": _record(Path(context_runner.__file__).resolve()),
        "contract_version": context_runner.CONTRACT_VERSION,
        "live_runtime_authorization_version": (
            context_runner.LIVE_RUNTIME_AUTHORIZATION_VERSION
        ),
        "live_provision_execution_version": (
            context_runner.LIVE_PROVISION_EXECUTION_VERSION
        ),
        "live_completion_receipt_version": (
            context_runner.LIVE_COMPLETION_RECEIPT_VERSION
        ),
        "lane": context_runner.LIVE_LANE,
        "label_pack": context_runner.LIVE_LABEL_PACK,
        "queue_payload_model": context_runner.LIVE_QUEUE_PAYLOAD_MODEL,
        "semantic_model": context_runner.LIVE_SEMANTIC_MODEL,
        "reasoning_effort": context_runner.LIVE_REASONING_EFFORT,
        "implementation_sha256s": {
            "load_contract": _source_sha256(
                context_runner.load_episode_context_contract
            ),
            "load_runtime_authorization": _source_sha256(
                context_runner.load_live_episode_context_runtime_authorization
            ),
            "verify_provision_execution": _source_sha256(
                context_runner.verify_live_context_provision_execution_receipt
            ),
            "build_live_completion": _source_sha256(
                context_runner.build_live_episode_context_completion_receipt
            ),
            "verify_live_completion": _source_sha256(
                context_runner.verify_live_episode_context_completion_receipt
            ),
            "database_identity": _source_sha256(
                context_runner._queue_main_database_identity
            ),
            "verify_managed_artifact": _source_sha256(
                context_runner._verify_managed_context_artifact
            ),
            "queue_accounting": _source_sha256(
                context_runner.SQLiteEpisodeContextQueue.accounting
            ),
        },
        "completion_authority": "official_live_only",
        "fixture_completion_promotable": False,
        "database_currentness": (
            "same_sqlite_connection_queue_authority_required_at_execution"
        ),
    }


def _load_receipt(record: Any, *, label: str) -> dict[str, Any]:
    path = _verify_record(record, label=label)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CanonicalV31ProductionContractError(f"cannot read {label}") from exc
    if not isinstance(payload, dict):
        raise CanonicalV31ProductionContractError(f"{label} is malformed")
    return payload


def _verify_matrix_receipt(payload: Mapping[str, Any]) -> dict[str, Any]:
    expected_keys = {
        "schema_version",
        "state",
        "manifest_sha256",
        "context_set_sha256",
        "runtime_binding_sha256",
        "context_control_overlay_sha256",
        "instruction_source_contract_sha256",
        "capacity_policy_sha256",
        "precommit_sha256",
        "future_plan_binding_sha256",
        "future_execution_directive_sha256",
        "arm_envelope_sha256s",
        "full_output_validation_sha256",
        "opaque_case_order_sha256",
        "case_count",
        "arm_count",
        "per_arm_full_output_sha256s",
        "per_arm_canonical_labels_sha256s",
        "managed_chatgpt_auth_only",
        "semantic_retry_count",
        "semantic_pruning",
        "semantic_relabeling",
        "production_mutated",
    }
    arm_maps = (
        payload.get("arm_envelope_sha256s"),
        payload.get("per_arm_full_output_sha256s"),
        payload.get("per_arm_canonical_labels_sha256s"),
    )
    runtime = canonical_runtime_binding()
    if (
        set(payload) != expected_keys
        or payload.get("schema_version") != MATRIX_RECEIPT_VERSION
        or payload.get("state") != "verified_full_six_arm_canonical_matrix"
        or any(not _valid_sha256(payload.get(field)) for field in (
            "manifest_sha256",
            "context_set_sha256",
            "runtime_binding_sha256",
            "context_control_overlay_sha256",
            "instruction_source_contract_sha256",
            "capacity_policy_sha256",
            "precommit_sha256",
            "future_plan_binding_sha256",
            "future_execution_directive_sha256",
            "full_output_validation_sha256",
            "opaque_case_order_sha256",
        ))
        or payload.get("runtime_binding_sha256") != runtime["matrix_binding_sha256"]
        or payload.get("context_control_overlay_sha256")
        != runtime["context_control_overlay_sha256"]
        or payload.get("instruction_source_contract_sha256")
        != runtime["instruction_source_contract_sha256"]
        or any(
            not isinstance(value, Mapping)
            or list(value) != list(EXPECTED_VARIANT_IDS)
            or any(not _valid_sha256(item) for item in value.values())
            for value in arm_maps
        )
        or isinstance(payload.get("case_count"), bool)
        or not isinstance(payload.get("case_count"), int)
        or int(payload["case_count"]) < 1
        or payload.get("arm_count") != 6
        or payload.get("managed_chatgpt_auth_only") is not True
        or payload.get("semantic_retry_count") != 0
        or payload.get("semantic_pruning") is not False
        or payload.get("semantic_relabeling") is not False
        or payload.get("production_mutated") is not False
    ):
        raise CanonicalV31ProductionContractError("canonical matrix lineage drifted")
    return copy.deepcopy(dict(payload))


def _verify_quality_receipt(
    payload: Mapping[str, Any], *, matrix: Mapping[str, Any]
) -> dict[str, Any]:
    expected_keys = {
        "schema_version",
        "state",
        "matrix_lineage_sha256",
        "quality_result_sha256",
        "selected_variant_id",
        "selected_arm_envelope_sha256",
        "selected_full_output_sha256",
        "selected_canonical_labels_sha256",
        "strict_full_field_macro",
        "baseline_strict_full_field_macro",
        "noninferiority_margin",
        "exact_evidence_rate",
        "exact_offset_rate",
        "exact_provenance_rate",
        "production_amortized_total_token_ratio",
        "usage_complete",
        "evaluation_mode",
        "embeddings_used",
        "deterministic_semantic_matching",
        "semantic_defaults",
        "semantic_pruning",
        "semantic_relabeling",
        "production_mutated",
    }
    selected = payload.get("selected_variant_id")
    if (
        set(payload) != expected_keys
        or payload.get("schema_version") != QUALITY_RECEIPT_VERSION
        or payload.get("state") != "canonical_development_quality_passed"
        or not _valid_sha256(payload.get("matrix_lineage_sha256"))
        or not _valid_sha256(payload.get("quality_result_sha256"))
        or selected not in EXPECTED_VARIANT_IDS
        or payload.get("selected_arm_envelope_sha256")
        != matrix["arm_envelope_sha256s"].get(selected)
        or payload.get("selected_full_output_sha256")
        != matrix["per_arm_full_output_sha256s"].get(selected)
        or payload.get("selected_canonical_labels_sha256")
        != matrix["per_arm_canonical_labels_sha256s"].get(selected)
        or any(
            isinstance(payload.get(field), bool)
            or not isinstance(payload.get(field), (int, float))
            for field in (
                "strict_full_field_macro",
                "baseline_strict_full_field_macro",
                "noninferiority_margin",
                "exact_evidence_rate",
                "exact_offset_rate",
                "exact_provenance_rate",
                "production_amortized_total_token_ratio",
            )
        )
        or float(payload["strict_full_field_macro"]) < 0.97
        or float(payload["strict_full_field_macro"])
        < float(payload["baseline_strict_full_field_macro"])
        - float(payload["noninferiority_margin"])
        or any(
            float(payload[field]) != 1.0
            for field in (
                "exact_evidence_rate",
                "exact_offset_rate",
                "exact_provenance_rate",
            )
        )
        or float(payload["production_amortized_total_token_ratio"]) > 0.28
        or payload.get("usage_complete") is not True
        or payload.get("evaluation_mode") != "llm_only"
        or payload.get("embeddings_used") is not False
        or payload.get("deterministic_semantic_matching") is not False
        or payload.get("semantic_defaults") != {}
        or payload.get("semantic_pruning") is not False
        or payload.get("semantic_relabeling") is not False
        or payload.get("production_mutated") is not False
    ):
        raise CanonicalV31ProductionContractError("canonical quality lineage drifted")
    return copy.deepcopy(dict(payload))


def _verify_holdout_receipt(
    payload: Mapping[str, Any], *, quality: Mapping[str, Any]
) -> dict[str, Any]:
    expected_keys = {
        "schema_version",
        "state",
        "quality_lineage_sha256",
        "holdout_result_sha256",
        "selected_variant_id",
        "selected_arm_envelope_sha256",
        "untouched_holdout",
        "holdout_item_count",
        "holdout_passed",
        "semantic_noninferior_or_better",
        "exact_evidence_rate",
        "exact_offset_rate",
        "exact_provenance_rate",
        "managed_chatgpt_auth_only",
        "semantic_retry_count",
        "semantic_pruning",
        "semantic_relabeling",
        "production_mutated",
        "production_authorized",
    }
    if (
        set(payload) != expected_keys
        or payload.get("schema_version") != HOLDOUT_RECEIPT_VERSION
        or payload.get("state") != "untouched_canonical_holdout_passed"
        or not _valid_sha256(payload.get("quality_lineage_sha256"))
        or not _valid_sha256(payload.get("holdout_result_sha256"))
        or payload.get("selected_variant_id") != quality.get("selected_variant_id")
        or payload.get("selected_arm_envelope_sha256")
        != quality.get("selected_arm_envelope_sha256")
        or payload.get("untouched_holdout") is not True
        or isinstance(payload.get("holdout_item_count"), bool)
        or not isinstance(payload.get("holdout_item_count"), int)
        or int(payload["holdout_item_count"]) < 1
        or payload.get("holdout_passed") is not True
        or payload.get("semantic_noninferior_or_better") is not True
        or any(
            isinstance(payload.get(field), bool)
            or not isinstance(payload.get(field), (int, float))
            or float(payload[field]) != 1.0
            for field in (
                "exact_evidence_rate",
                "exact_offset_rate",
                "exact_provenance_rate",
            )
        )
        or payload.get("managed_chatgpt_auth_only") is not True
        or payload.get("semantic_retry_count") != 0
        or payload.get("semantic_pruning") is not False
        or payload.get("semantic_relabeling") is not False
        or payload.get("production_mutated") is not False
        or payload.get("production_authorized") is not True
    ):
        raise CanonicalV31ProductionContractError("canonical holdout lineage drifted")
    return copy.deepcopy(dict(payload))


def build_frozen_configuration(
    *,
    batch_size: int,
    thread_mode: str,
    matrix_lineage: Mapping[str, Any],
    quality_lineage: Mapping[str, Any],
    holdout_lineage: Mapping[str, Any],
) -> dict[str, Any]:
    """Build a freeze only from already-persisted, checksum-bound receipts."""

    matrix = _verify_matrix_receipt(
        _load_receipt(matrix_lineage, label="canonical matrix lineage")
    )
    quality = _verify_quality_receipt(
        _load_receipt(quality_lineage, label="canonical quality lineage"),
        matrix=matrix,
    )
    holdout = _verify_holdout_receipt(
        _load_receipt(holdout_lineage, label="canonical holdout lineage"),
        quality=quality,
    )
    if (
        quality["matrix_lineage_sha256"] != matrix_lineage.get("sha256")
        or holdout["quality_lineage_sha256"] != quality_lineage.get("sha256")
    ):
        raise CanonicalV31ProductionContractError(
            "canonical matrix, quality, or holdout chain drifted"
        )
    variant_id = selected_variant_id(batch_size=batch_size, thread_mode=thread_mode)
    if quality["selected_variant_id"] != variant_id:
        raise CanonicalV31ProductionContractError(
            "selected arm does not match canonical quality winner"
        )
    runtime = canonical_runtime_binding()
    return {
        "schema_version": FROZEN_CONFIGURATION_VERSION,
        "state": "frozen_canonical_v31_production_authority",
        "winner_system_id": variant_id,
        "candidate_system_id": adapter.CANDIDATE_SYSTEM_ID,
        "batch_size": batch_size,
        "thread_mode": thread_mode,
        "model": adapter.MODEL,
        "effort": adapter.EFFORT,
        "runtime_binding": runtime,
        "matrix_lineage": copy.deepcopy(dict(matrix_lineage)),
        "quality_lineage": copy.deepcopy(dict(quality_lineage)),
        "holdout_lineage": copy.deepcopy(dict(holdout_lineage)),
        "selected_arm_envelope_sha256": quality[
            "selected_arm_envelope_sha256"
        ],
        "selected_full_output_sha256": quality["selected_full_output_sha256"],
        "selected_canonical_labels_sha256": quality[
            "selected_canonical_labels_sha256"
        ],
        "capacity_admission_contract": capacity_admission_contract(),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "retry_count": 0,
        "deterministic_semantic_pruning": False,
        "deterministic_support_filtering": False,
        "deterministic_deduplication": False,
        "deterministic_relabeling": False,
    }


def verify_frozen_configuration(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.expanduser().resolve().read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CanonicalV31ProductionContractError(
            "cannot read canonical frozen configuration"
        ) from exc
    if not isinstance(payload, Mapping):
        raise CanonicalV31ProductionContractError(
            "canonical frozen configuration is malformed"
        )
    expected_keys = {
        "schema_version",
        "state",
        "winner_system_id",
        "candidate_system_id",
        "batch_size",
        "thread_mode",
        "model",
        "effort",
        "runtime_binding",
        "matrix_lineage",
        "quality_lineage",
        "holdout_lineage",
        "selected_arm_envelope_sha256",
        "selected_full_output_sha256",
        "selected_canonical_labels_sha256",
        "capacity_admission_contract",
        "managed_chatgpt_auth_only",
        "official_persistent_codex_app_server_only",
        "retry_count",
        "deterministic_semantic_pruning",
        "deterministic_support_filtering",
        "deterministic_deduplication",
        "deterministic_relabeling",
    }
    if set(payload) != expected_keys:
        raise CanonicalV31ProductionContractError(
            "canonical frozen configuration shape drifted"
        )
    batch_size = payload.get("batch_size")
    thread_mode = payload.get("thread_mode")
    variant_id = selected_variant_id(
        batch_size=batch_size, thread_mode=str(thread_mode)
    )
    matrix = _verify_matrix_receipt(
        _load_receipt(payload.get("matrix_lineage"), label="canonical matrix lineage")
    )
    quality = _verify_quality_receipt(
        _load_receipt(payload.get("quality_lineage"), label="canonical quality lineage"),
        matrix=matrix,
    )
    holdout = _verify_holdout_receipt(
        _load_receipt(payload.get("holdout_lineage"), label="canonical holdout lineage"),
        quality=quality,
    )
    if (
        quality["matrix_lineage_sha256"]
        != payload.get("matrix_lineage", {}).get("sha256")
        or holdout["quality_lineage_sha256"]
        != payload.get("quality_lineage", {}).get("sha256")
    ):
        raise CanonicalV31ProductionContractError(
            "canonical matrix, quality, or holdout chain drifted"
        )
    runtime = canonical_runtime_binding()
    if (
        payload.get("schema_version") != FROZEN_CONFIGURATION_VERSION
        or payload.get("state") != "frozen_canonical_v31_production_authority"
        or payload.get("winner_system_id") != variant_id
        or quality.get("selected_variant_id") != variant_id
        or holdout.get("selected_variant_id") != variant_id
        or payload.get("candidate_system_id") != adapter.CANDIDATE_SYSTEM_ID
        or payload.get("model") != adapter.MODEL
        or payload.get("effort") != adapter.EFFORT
        or payload.get("runtime_binding") != runtime
        or payload.get("selected_arm_envelope_sha256")
        != quality.get("selected_arm_envelope_sha256")
        or payload.get("selected_full_output_sha256")
        != quality.get("selected_full_output_sha256")
        or payload.get("selected_canonical_labels_sha256")
        != quality.get("selected_canonical_labels_sha256")
        or payload.get("capacity_admission_contract") != capacity_admission_contract()
        or payload.get("managed_chatgpt_auth_only") is not True
        or payload.get("official_persistent_codex_app_server_only") is not True
        or payload.get("retry_count") != 0
        or any(
            payload.get(field) is not False
            for field in (
                "deterministic_semantic_pruning",
                "deterministic_support_filtering",
                "deterministic_deduplication",
                "deterministic_relabeling",
            )
        )
    ):
        raise CanonicalV31ProductionContractError(
            "canonical frozen configuration values drifted"
        )
    return copy.deepcopy(dict(payload))


def _load_json_path(path: Path, *, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.expanduser().resolve().read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CanonicalV31ProductionContractError(f"cannot read {label}") from exc
    if not isinstance(payload, dict):
        raise CanonicalV31ProductionContractError(f"{label} is malformed")
    return payload


def _verify_static_live_completion(
    path: Path,
    *,
    context_loaded: Mapping[str, Any],
    runtime: Mapping[str, Any],
) -> dict[str, Any]:
    value = _load_json_path(path, label="official-live episode-context completion")
    unhashed = dict(value)
    supplied = unhashed.pop("receipt_sha256", None)
    artifact_index = value.get("context_artifact_index")
    required_ids = [
        str(item) for item in runtime["provision_plan"]["required_episode_ids"]
    ]
    required_count = len(required_ids)
    if (
        value.get("schema_version")
        != context_runner.LIVE_COMPLETION_RECEIPT_VERSION
        or value.get("state") != "passed"
        or value.get("phase") != "episode_context"
        or value.get("contract") != _record(Path(context_loaded["path"]))
        or value.get("runtime_authorization") != _record(Path(runtime["path"]))
        or value.get("provision_execution_receipt")
        != _record(Path(runtime["provision_execution_path"]))
        or value.get("database") != runtime["cutover"]["database"]
        or value.get("database") != runtime["provision_plan"]["database"]
        or value.get("database") != runtime["provision_execution"]["database"]
        or required_count < 1
        or value.get("required_episode_count") != required_count
        or value.get("completed_managed_attempt_count") != required_count
        or value.get("unique_thread_count") != required_count
        or value.get("unique_turn_count") != required_count
        or not isinstance(artifact_index, Mapping)
        or sorted(str(item) for item in artifact_index) != required_ids
        or value.get("context_artifact_index_sha256")
        != _sha256_bytes(_canonical_json(artifact_index).encode("utf-8"))
        or value.get("execution_authority") != "official_live"
        or value.get("semantic_retry_count") != 0
        or value.get("test_only") is not False
        or value.get("promotable") is not True
        or value.get("managed_chatgpt_auth_only") is not True
        or value.get("production_context_mutated") is not True
        or value.get("label_queue_mutated") is not False
        or not _valid_sha256(supplied)
        or supplied != _sha256_bytes(_canonical_json(unhashed).encode("utf-8"))
    ):
        raise CanonicalV31ProductionContractError(
            "official-live episode-context completion lineage drifted"
        )
    for episode_id, artifact_record in artifact_index.items():
        _verify_record(
            artifact_record,
            label=f"official-live context artifact {episode_id}",
        )
    return copy.deepcopy(value)


def _promotion_payload(
    *,
    frozen_path: Path,
    frozen: Mapping[str, Any],
    context_loaded: Mapping[str, Any],
    runtime: Mapping[str, Any],
    completion_path: Path,
    completion: Mapping[str, Any],
) -> dict[str, Any]:
    payload = {
        "schema_version": PROMOTION_AUTHORITY_VERSION,
        "state": "released_official_live",
        "frozen_configuration": _record(frozen_path),
        "matrix_lineage": copy.deepcopy(dict(frozen["matrix_lineage"])),
        "quality_lineage": copy.deepcopy(dict(frozen["quality_lineage"])),
        "holdout_lineage": copy.deepcopy(dict(frozen["holdout_lineage"])),
        "selected_variant_id": frozen["winner_system_id"],
        "selected_arm_envelope_sha256": frozen[
            "selected_arm_envelope_sha256"
        ],
        "episode_context_contract": _record(Path(context_loaded["path"])),
        "episode_context_runtime_authorization": _record(Path(runtime["path"])),
        "episode_context_provision_execution_receipt": _record(
            Path(runtime["provision_execution_path"])
        ),
        "episode_context_live_completion_receipt": _record(completion_path),
        "episode_context_database": copy.deepcopy(completion["database"]),
        "episode_context_artifact_index_sha256": completion[
            "context_artifact_index_sha256"
        ],
        "episode_context_runtime_binding": episode_context_runtime_binding(),
        "managed_chatgpt_auth_only": True,
        "official_live_context_completion_required": True,
        "same_sqlite_queue_authority_required_at_execution": True,
        "semantic_retry_count": 0,
        "production_authorized": True,
    }
    payload["promotion_sha256"] = _sha256_bytes(
        _canonical_json(payload).encode("utf-8")
    )
    return payload


def build_production_promotion_authority(
    *,
    frozen_configuration_path: Path,
    episode_context_contract_path: Path,
    runtime_authorization_path: Path,
    live_completion_receipt_path: Path,
    queue: Any,
) -> dict[str, Any]:
    """Mint promotion only after the official-live verifier passes now."""

    frozen_path = frozen_configuration_path.expanduser().resolve()
    frozen = verify_frozen_configuration(frozen_path)
    try:
        context_loaded = context_runner.load_episode_context_contract(
            episode_context_contract_path
        )
        runtime = context_runner.load_live_episode_context_runtime_authorization(
            runtime_authorization_path, loaded=context_loaded
        )
        completion = context_runner.verify_live_episode_context_completion_receipt(
            live_completion_receipt_path,
            contract_path=episode_context_contract_path,
            runtime_authorization_path=runtime_authorization_path,
            queue=queue,
        )
    except context_runner.EpisodeContextRunnerError as exc:
        raise CanonicalV31ProductionContractError(
            "official-live episode-context completion is not promotable"
        ) from exc
    _verify_static_live_completion(
        live_completion_receipt_path,
        context_loaded=context_loaded,
        runtime=runtime,
    )
    if context_runner._queue_main_database_identity(queue) != completion["database"]:
        raise CanonicalV31ProductionContractError(
            "promotion queue database differs from official-live completion"
        )
    return _promotion_payload(
        frozen_path=frozen_path,
        frozen=frozen,
        context_loaded=context_loaded,
        runtime=runtime,
        completion_path=live_completion_receipt_path.expanduser().resolve(),
        completion=completion,
    )


def verify_production_promotion_authority(
    path: Path,
    *,
    frozen_configuration_path: Path,
    episode_context_contract_path: Path,
    runtime_authorization_path: Path,
    live_completion_receipt_path: Path,
    queue: Any | None = None,
) -> dict[str, Any]:
    """Verify immutable lineage; optionally prove database currentness now.

    Without ``queue`` this verifies only artifact hashes and schema.  The
    returned ``database_current`` flag remains false and must never authorize
    execution.  A production execution must pass a context queue backed by the
    exact same SQLite connection as the label queue.
    """

    authority_path = path.expanduser().resolve()
    authority = _load_json_path(authority_path, label="production promotion")
    supplied = authority.get("promotion_sha256")
    unhashed = dict(authority)
    unhashed.pop("promotion_sha256", None)
    expected_keys = {
        "schema_version",
        "state",
        "frozen_configuration",
        "matrix_lineage",
        "quality_lineage",
        "holdout_lineage",
        "selected_variant_id",
        "selected_arm_envelope_sha256",
        "episode_context_contract",
        "episode_context_runtime_authorization",
        "episode_context_provision_execution_receipt",
        "episode_context_live_completion_receipt",
        "episode_context_database",
        "episode_context_artifact_index_sha256",
        "episode_context_runtime_binding",
        "managed_chatgpt_auth_only",
        "official_live_context_completion_required",
        "same_sqlite_queue_authority_required_at_execution",
        "semantic_retry_count",
        "production_authorized",
        "promotion_sha256",
    }
    if (
        set(authority) != expected_keys
        or authority.get("schema_version") != PROMOTION_AUTHORITY_VERSION
        or authority.get("state") != "released_official_live"
        or not _valid_sha256(supplied)
        or supplied != _sha256_bytes(_canonical_json(unhashed).encode("utf-8"))
        or authority.get("managed_chatgpt_auth_only") is not True
        or authority.get("official_live_context_completion_required") is not True
        or authority.get("same_sqlite_queue_authority_required_at_execution")
        is not True
        or authority.get("semantic_retry_count") != 0
        or authority.get("production_authorized") is not True
    ):
        raise CanonicalV31ProductionContractError(
            "production promotion authority drifted"
        )
    frozen_path = frozen_configuration_path.expanduser().resolve()
    frozen = verify_frozen_configuration(frozen_path)
    try:
        context_loaded = context_runner.load_episode_context_contract(
            episode_context_contract_path
        )
        runtime = context_runner.load_live_episode_context_runtime_authorization(
            runtime_authorization_path, loaded=context_loaded
        )
    except context_runner.EpisodeContextRunnerError as exc:
        raise CanonicalV31ProductionContractError(
            "official-live predecessor artifacts drifted"
        ) from exc
    completion_path = live_completion_receipt_path.expanduser().resolve()
    completion = _verify_static_live_completion(
        completion_path,
        context_loaded=context_loaded,
        runtime=runtime,
    )
    expected = _promotion_payload(
        frozen_path=frozen_path,
        frozen=frozen,
        context_loaded=context_loaded,
        runtime=runtime,
        completion_path=completion_path,
        completion=completion,
    )
    if authority != expected:
        raise CanonicalV31ProductionContractError(
            "production promotion lineage differs from current artifacts"
        )
    database_current = False
    verified_completion = completion
    if queue is not None:
        try:
            verified_completion = (
                context_runner.verify_live_episode_context_completion_receipt(
                    completion_path,
                    contract_path=episode_context_contract_path,
                    runtime_authorization_path=runtime_authorization_path,
                    queue=queue,
                )
            )
            queue_database = context_runner._queue_main_database_identity(queue)
        except context_runner.EpisodeContextRunnerError as exc:
            raise CanonicalV31ProductionContractError(
                "production promotion is not current for the SQLite authority"
            ) from exc
        if (
            queue_database != authority["episode_context_database"]
            or verified_completion != completion
        ):
            raise CanonicalV31ProductionContractError(
                "production promotion SQLite authority drifted"
            )
        database_current = True
    return {
        "path": authority_path,
        "sha256": _sha256_file(authority_path),
        "authority": copy.deepcopy(authority),
        "frozen_configuration": frozen,
        "context_loaded": context_loaded,
        "runtime": runtime,
        "live_completion": copy.deepcopy(verified_completion),
        "database_current": database_current,
    }
