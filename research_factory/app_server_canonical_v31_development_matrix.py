from __future__ import annotations

"""Offline control plane for the corrected full-canonical v3.1 matrix.

This module prepares and verifies the six development arms (3/5/8 segments by
new/same thread) around :mod:`app_server_canonical_v31_episode_batch`.  It has
no live semantic execution path.  A future executor must present a fresh,
explicit, checksum-bound directive that binds the exact precommit, plan,
manifest, context, runtime, overlay, and capacity lineage validated here.

Historical epoch6 and compact-matrix artifacts are deliberately absent from
runtime, prompt, schema, and semantic authority.  They may remain immutable
exploratory evidence in a later evaluation receipt, but are never inputs to
this control plane.
"""

import copy
import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_canonical_v31_episode_batch as adapter
from .labels import ValidationError, validate_label_output
from .util import sha256_text, write_text_atomic


PROJECT_ROOT = Path(__file__).resolve().parents[1]

MATRIX_VERSION = "pif_canonical_v31_development_matrix_v1"
MANIFEST_VERSION = "pif_canonical_v31_development_manifest_v1"
CONTEXT_ARTIFACT_VERSION = "pif_canonical_v31_episode_context_artifact_v1"
REFERENCE_SEED_VERSION = "pif_canonical_v31_shared_reference_seed_v1"
AUGMENTED_REFERENCE_VERSION = "pif_canonical_v31_shared_augmented_reference_v1"
CAPACITY_POLICY_VERSION = "pif_canonical_v31_matrix_capacity_policy_v2"
CAPACITY_ADMISSION_VERSION = "pif_canonical_v31_matrix_capacity_admission_v2"
DRY_PREFLIGHT_VERSION = "pif_canonical_v31_matrix_dry_preflight_v1"
PRECOMMIT_VERSION = "pif_canonical_v31_matrix_precommit_v1"
FUTURE_PLAN_BINDING_VERSION = "pif_canonical_v31_future_plan_binding_v2"
FUTURE_DIRECTIVE_VERSION = "pif_canonical_v31_future_execution_directive_v2"
ARM_ENVELOPE_VERSION = "pif_canonical_v31_matrix_arm_envelope_v1"
QUALITY_CONTRACT_VERSION = "pif_canonical_v31_quality_evaluator_contract_v1"
QUALITY_RESULT_VERSION = "pif_canonical_v31_quality_evaluator_result_v1"
SELECTION_VERSION = "pif_canonical_v31_development_selection_v1"

EXPECTED_ARMS = tuple(
    (batch_size, thread_mode)
    for batch_size in adapter.SUPPORTED_BATCH_SIZES
    for thread_mode in adapter.SUPPORTED_THREAD_MODES
)
USAGE_FIELDS = adapter.USAGE_FIELDS
QUALITY_THRESHOLD = 0.97
NONINFERIORITY_MARGIN = 0.0
TOKEN_RATIO_THRESHOLD = 0.28
PRODUCTION_COST_FORMULA = (
    "(measured_extraction_tokens/development_segments*production_segments+"
    "recurring_context_tokens_per_episode*production_episodes)/"
    "(baseline_tokens_per_segment*production_segments)"
)

_CONTEXT_FIELDS = (
    "episode_id",
    "source_name",
    "episode_title",
    "context_summary",
    "speaker_map",
    "section_map",
    "entity_seed",
    "concept_seed",
    "extraction_guidance",
    "excluded_source_context",
)


class CanonicalV31DevelopmentMatrixError(RuntimeError):
    """An immutable prerequisite, accounting receipt, or quality gate failed."""


class FutureExecutionDirectiveRequired(CanonicalV31DevelopmentMatrixError):
    """Live semantic dispatch is closed without a fresh bound directive."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _pretty_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"


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


def _require_sha256(value: Any, label: str) -> str:
    if not _valid_sha256(value):
        raise CanonicalV31DevelopmentMatrixError(f"{label} is not a lowercase SHA-256")
    return str(value)


def _record(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve(strict=True)
    return {
        "path": str(resolved),
        "sha256": _sha256_file(resolved),
        "size_bytes": resolved.stat().st_size,
    }


def _load_object(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.expanduser().resolve(strict=True).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CanonicalV31DevelopmentMatrixError(f"{label} is not valid UTF-8 JSON") from exc
    if not isinstance(value, dict):
        raise CanonicalV31DevelopmentMatrixError(f"{label} is not a JSON object")
    return value


def _inside(path: Path, root: Path) -> bool:
    try:
        path.expanduser().resolve().relative_to(root.expanduser().resolve())
        return True
    except ValueError:
        return False


def _verify_record(record: Any, *, label: str, allowed_root: Path) -> Path:
    if not isinstance(record, Mapping) or set(record) != {"path", "sha256", "size_bytes"}:
        raise CanonicalV31DevelopmentMatrixError(f"{label} record is malformed")
    path_value = record.get("path")
    size = record.get("size_bytes")
    if (
        not isinstance(path_value, str)
        or not path_value
        or not _valid_sha256(record.get("sha256"))
        or isinstance(size, bool)
        or not isinstance(size, int)
        or size < 0
    ):
        raise CanonicalV31DevelopmentMatrixError(f"{label} record fields are invalid")
    path = Path(path_value).expanduser().resolve()
    if not _inside(path, allowed_root) or not path.is_file():
        raise CanonicalV31DevelopmentMatrixError(f"{label} escapes or is absent")
    if _record(path) != dict(record):
        raise CanonicalV31DevelopmentMatrixError(f"{label} checksum binding failed")
    return path


def _verify_exact_file(path: Path, expected_sha256: str, label: str) -> dict[str, Any]:
    expected = _require_sha256(expected_sha256, f"{label} expected SHA-256")
    resolved = path.expanduser().resolve()
    if not resolved.is_file() or _sha256_file(resolved) != expected:
        raise CanonicalV31DevelopmentMatrixError(f"{label} checksum binding failed")
    return _record(resolved)


def _is_iso_timestamp(value: Any) -> bool:
    if not isinstance(value, str) or not value or value != value.strip():
        return False
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.tzinfo is not None


def _usage(value: Any, label: str) -> dict[str, int]:
    if not isinstance(value, Mapping):
        raise CanonicalV31DevelopmentMatrixError(f"{label} usage is absent")
    result: dict[str, int] = {}
    for field in USAGE_FIELDS:
        item = value.get(field)
        if isinstance(item, bool) or not isinstance(item, int) or item < 0:
            raise CanonicalV31DevelopmentMatrixError(f"{label}.{field} is invalid")
        result[field] = item
    if (
        result["cached_input_tokens"] > result["input_tokens"]
        or result["reasoning_output_tokens"] > result["output_tokens"]
        or result["total_tokens"] != result["input_tokens"] + result["output_tokens"]
    ):
        raise CanonicalV31DevelopmentMatrixError(f"{label} usage is inconsistent")
    return result


def _nonnegative_number(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CanonicalV31DevelopmentMatrixError(f"{label} must be numeric")
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise CanonicalV31DevelopmentMatrixError(f"{label} must be finite and nonnegative")
    return result


def _variant_id(batch_size: int, thread_mode: str) -> str:
    return f"canonical_v31_batch_{batch_size}_{thread_mode}"


def _artifact_from_manifest(value: Any, *, manifest_root: Path, label: str) -> dict[str, Any]:
    path = _verify_record(value, label=label, allowed_root=manifest_root)
    return _record(path)


def verify_capacity_policy(
    path: Path,
    *,
    expected_sha256: str,
    allowed_root: Path,
) -> dict[str, Any]:
    record = _verify_exact_file(path, expected_sha256, "matrix capacity policy")
    if not _inside(Path(record["path"]), allowed_root):
        raise CanonicalV31DevelopmentMatrixError("capacity policy escapes allowed root")
    policy = _load_object(Path(record["path"]), "matrix capacity policy")
    expected_arms = [
        {"batch_size": batch_size, "thread_mode": thread_mode}
        for batch_size, thread_mode in EXPECTED_ARMS
    ]
    positive_integer_fields = (
        "minimum_remaining_reserve_percent",
        "capacity_safety_margin_percent",
        "quota_points_per_million_tokens",
        "maximum_total_tokens_per_turn",
        "maximum_rate_limit_snapshot_age_seconds",
    )
    positive_numeric_fields = (
        "maximum_wall_seconds_per_turn",
        "operator_wall_deadline_safety_margin_seconds",
    )
    if (
        policy.get("schema_version") != CAPACITY_POLICY_VERSION
        or policy.get("state") != "frozen_offline_policy"
        or policy.get("candidate_system_id") != adapter.CANDIDATE_SYSTEM_ID
        or policy.get("managed_chatgpt_auth_only") is not True
        or policy.get("managed_chatgpt_plan_type") != "pro"
        or policy.get("official_persistent_codex_app_server_only") is not True
        or policy.get("official_app_server_initialized_before_capacity") is not True
        or policy.get("admission_required_before_thread_start") is not True
        or policy.get("admission_required_before_thread_resume") is not True
        or policy.get("admission_required_before_turn_start") is not True
        or "admission_required_before_app_server_start" in policy
        or policy.get("admission_must_bind_future_plan_and_directive") is not True
        or policy.get("complete_usage_cache_reasoning_wall_required") is not True
        or policy.get("unknown_usage_hard_stop") is not True
        or any(
            isinstance(policy.get(field), bool)
            or not isinstance(policy.get(field), int)
            or policy.get(field) <= 0
            for field in positive_integer_fields
        )
        or any(
            isinstance(policy.get(field), bool)
            or not isinstance(policy.get(field), (int, float))
            or not math.isfinite(float(policy.get(field)))
            or float(policy.get(field)) <= 0
            for field in positive_numeric_fields
        )
        or policy.get("minimum_remaining_reserve_percent")
        + policy.get("capacity_safety_margin_percent")
        >= 100
        or not isinstance(policy.get("quota_calibration_id"), str)
        or not policy.get("quota_calibration_id")
        or policy.get("quota_calibration_conservative_uplift_applied") is not True
        or policy.get("token_capacity_is_estimate_not_reservation") is not True
        or policy.get("single_turn_token_cap_is_prospective_only") is not True
        or policy.get("preturn_reprobe_required") is not True
        or policy.get("postturn_measured_stop_required") is not True
        or policy.get("semantic_retry_count") != 0
        or policy.get("required_arms") != expected_arms
        or policy.get("full_canonical_v31_outputs_required") is not True
        or policy.get("holdout_authorized") is not False
        or policy.get("production_mutation_allowed") is not False
    ):
        raise CanonicalV31DevelopmentMatrixError("capacity policy contract drifted")
    return {"record": record, "payload": policy}


def _manifest_episode_rows(manifest: Mapping[str, Any]) -> tuple[list[dict[str, Any]], list[str]]:
    episodes = manifest.get("episodes")
    case_order = manifest.get("opaque_case_order")
    if not isinstance(episodes, list) or not episodes:
        raise CanonicalV31DevelopmentMatrixError("development manifest episodes are empty")
    if not isinstance(case_order, list) or not case_order:
        raise CanonicalV31DevelopmentMatrixError("development manifest opaque order is empty")
    rows: list[dict[str, Any]] = []
    seen_episode_ids: set[str] = set()
    seen_source_ids: set[str] = set()
    seen_segment_ids: set[str] = set()
    seen_case_ids: set[str] = set()
    seen_text_hashes: set[str] = set()
    for episode in episodes:
        if not isinstance(episode, Mapping):
            raise CanonicalV31DevelopmentMatrixError("manifest episode is malformed")
        episode_id = episode.get("episode_id")
        source_id = episode.get("source_id")
        segments = episode.get("segments")
        if (
            not isinstance(episode_id, str)
            or not episode_id
            or episode_id in seen_episode_ids
            or not isinstance(source_id, str)
            or not source_id
            or source_id in seen_source_ids
            or not isinstance(segments, list)
            or not segments
        ):
            raise CanonicalV31DevelopmentMatrixError("manifest episode/source allocation drifted")
        seen_episode_ids.add(episode_id)
        seen_source_ids.add(source_id)
        for expected_index, segment in enumerate(segments):
            if not isinstance(segment, Mapping):
                raise CanonicalV31DevelopmentMatrixError("manifest segment is malformed")
            segment_id = segment.get("segment_id")
            index = segment.get("segment_index")
            case_id = segment.get("opaque_case_id")
            text_hash = segment.get("text_sha256")
            quality_hash = segment.get("segment_quality_sha256")
            density = segment.get("density_stratum")
            if (
                not isinstance(segment_id, str)
                or not segment_id
                or segment_id in seen_segment_ids
                or index != expected_index
                or not isinstance(case_id, str)
                or not case_id
                or case_id in seen_case_ids
                or case_id in {segment_id, episode_id, source_id}
                or not _valid_sha256(text_hash)
                or text_hash in seen_text_hashes
                or not _valid_sha256(quality_hash)
                or not isinstance(density, str)
                or not density
            ):
                raise CanonicalV31DevelopmentMatrixError(
                    "manifest case identity or provenance drifted"
                )
            seen_segment_ids.add(segment_id)
            seen_case_ids.add(case_id)
            seen_text_hashes.add(str(text_hash))
            rows.append(
                {
                    **copy.deepcopy(dict(segment)),
                    "episode_id": episode_id,
                    "source_id": source_id,
                    "context_artifact": copy.deepcopy(episode.get("context_artifact")),
                }
            )
    observed_order = [row["opaque_case_id"] for row in rows]
    if observed_order != case_order or len(case_order) != len(set(case_order)):
        raise CanonicalV31DevelopmentMatrixError("manifest opaque case order drifted")
    return rows, list(case_order)


def verify_development_manifest(
    path: Path,
    *,
    expected_sha256: str,
    episodes: Sequence[Mapping[str, Any]],
    allowed_root: Path,
) -> dict[str, Any]:
    """Verify manifest, episode contexts, source hashes, quality, and reference seed."""

    manifest_record = _verify_exact_file(path, expected_sha256, "development manifest")
    manifest_path = Path(manifest_record["path"])
    manifest_root = manifest_path.parent.resolve()
    if not _inside(manifest_path, allowed_root):
        raise CanonicalV31DevelopmentMatrixError("development manifest escapes allowed root")
    manifest = _load_object(manifest_path, "development manifest")
    rows, case_order = _manifest_episode_rows(manifest)
    if (
        manifest.get("schema_version") != MANIFEST_VERSION
        or manifest.get("evaluation_role") != "full_canonical_v31_development_only"
        or manifest.get("candidate_system_id") != adapter.CANDIDATE_SYSTEM_ID
        or manifest.get("case_count") != len(rows)
        or manifest.get("episode_count") != len(manifest.get("episodes") or [])
        or manifest.get("holdout_excluded") is not True
        or manifest.get("production_excluded") is not True
        or manifest.get("selection_policy")
        != "opaque_identity_only_no_transcript_semantic_selection_rules"
        or manifest.get("semantic_deterministic_pruning") is not False
        or manifest.get("semantic_deterministic_defaults") != {}
    ):
        raise CanonicalV31DevelopmentMatrixError("development manifest fixed contract drifted")
    if not isinstance(episodes, Sequence) or isinstance(episodes, (str, bytes)) or not episodes:
        raise CanonicalV31DevelopmentMatrixError("prepared development episodes are empty")
    manifest_episode_ids = [episode["episode_id"] for episode in manifest["episodes"]]
    observed_episode_ids = [episode.get("episode_id") for episode in episodes]
    if observed_episode_ids != manifest_episode_ids:
        raise CanonicalV31DevelopmentMatrixError("prepared episode order differs from manifest")

    row_by_segment = {str(row["segment_id"]): row for row in rows}
    source_by_segment: dict[str, dict[str, Any]] = {}
    context_records: list[dict[str, Any]] = []
    seen_context_paths: set[str] = set()
    for manifest_episode, episode in zip(manifest["episodes"], episodes):
        context_record = _artifact_from_manifest(
            manifest_episode.get("context_artifact"),
            manifest_root=manifest_root,
            label=f"context artifact {manifest_episode['episode_id']}",
        )
        if context_record["path"] in seen_context_paths:
            raise CanonicalV31DevelopmentMatrixError("episode context artifact was reused")
        seen_context_paths.add(context_record["path"])
        context_artifact = _load_object(Path(context_record["path"]), "episode context artifact")
        context = context_artifact.get("episode_context")
        expected_context = {field: copy.deepcopy(episode.get(field)) for field in _CONTEXT_FIELDS}
        if (
            context_artifact.get("schema_version") != CONTEXT_ARTIFACT_VERSION
            or context_artifact.get("episode_id") != episode.get("episode_id")
            or not isinstance(context, Mapping)
            or set(context) != set(_CONTEXT_FIELDS)
            or dict(context) != expected_context
            or context_artifact.get("semantic_deterministic_defaults") != {}
            or context_artifact.get("semantic_deterministic_pruning") is not False
        ):
            raise CanonicalV31DevelopmentMatrixError("episode context lineage drifted")
        context_records.append(context_record)
        prepared_segments = episode.get("segments")
        manifest_segments = manifest_episode.get("segments")
        if (
            not isinstance(prepared_segments, list)
            or not isinstance(manifest_segments, list)
            or [segment.get("segment_id") for segment in prepared_segments]
            != [segment.get("segment_id") for segment in manifest_segments]
        ):
            raise CanonicalV31DevelopmentMatrixError("prepared segment order differs from manifest")
        for prepared in prepared_segments:
            if not isinstance(prepared, Mapping):
                raise CanonicalV31DevelopmentMatrixError("prepared segment is malformed")
            segment_id = prepared.get("segment_id")
            spec = row_by_segment.get(str(segment_id))
            text = prepared.get("segment_text")
            quality = prepared.get("segment_quality")
            if (
                spec is None
                or not isinstance(text, str)
                or not text
                or sha256_text(text) != spec["text_sha256"]
                or not isinstance(quality, Mapping)
                or sha256_text(_canonical_json(quality)) != spec["segment_quality_sha256"]
                or prepared.get("density_stratum") != spec["density_stratum"]
                or not isinstance(prepared.get("boundaries"), list)
                or not prepared["boundaries"]
            ):
                raise CanonicalV31DevelopmentMatrixError(
                    "prepared source or quality provenance drifted"
                )
            source_by_segment[str(segment_id)] = copy.deepcopy(dict(prepared))

    reference_record = _artifact_from_manifest(
        manifest.get("shared_reference_seed"),
        manifest_root=manifest_root,
        label="shared canonical reference seed",
    )
    reference = _load_object(Path(reference_record["path"]), "shared canonical reference seed")
    references = reference.get("references")
    if (
        reference.get("schema_version") != REFERENCE_SEED_VERSION
        or reference.get("case_order") != case_order
        or not isinstance(references, list)
        or len(references) != len(rows)
        or reference.get("one_shared_reference_for_all_arms") is not True
        or reference.get("semantic_deterministic_defaults") != {}
        or reference.get("semantic_deterministic_pruning") is not False
    ):
        raise CanonicalV31DevelopmentMatrixError("shared reference seed contract drifted")
    references_by_case: dict[str, dict[str, Any]] = {}
    coded = 0
    noncoded = 0
    for spec, reference_row in zip(rows, references):
        if not isinstance(reference_row, Mapping):
            raise CanonicalV31DevelopmentMatrixError("shared reference row is malformed")
        label = reference_row.get("label")
        source = source_by_segment[str(spec["segment_id"])]
        if (
            reference_row.get("opaque_case_id") != spec["opaque_case_id"]
            or reference_row.get("segment_id") != spec["segment_id"]
            or reference_row.get("episode_id") != spec["episode_id"]
            or reference_row.get("text_sha256") != spec["text_sha256"]
            or not isinstance(label, dict)
            or label.get("segment_id") != spec["segment_id"]
            or label.get("episode_id") != spec["episode_id"]
            or label.get("segment_quality") != source["segment_quality"]
        ):
            raise CanonicalV31DevelopmentMatrixError("shared reference identity/provenance drifted")
        try:
            validate_label_output(
                adapter.CANONICAL_LABEL_PACK,
                label,
                segment_text=source["segment_text"],
            )
        except ValidationError as exc:
            raise CanonicalV31DevelopmentMatrixError(
                "shared reference is not full canonical v3.1"
            ) from exc
        if label["extraction_status"] == "coded":
            coded += 1
        else:
            noncoded += 1
        references_by_case[str(spec["opaque_case_id"])] = copy.deepcopy(
            dict(reference_row)
        )
    if coded < 1 or noncoded < 1:
        raise CanonicalV31DevelopmentMatrixError(
            "development reference lacks coded/no-signal coverage"
        )
    return {
        "manifest_record": manifest_record,
        "manifest": manifest,
        "rows": rows,
        "opaque_case_order": case_order,
        "opaque_case_order_sha256": sha256_text(_canonical_json(case_order)),
        "context_records": context_records,
        "context_set_sha256": sha256_text(_canonical_json(context_records)),
        "reference_record": reference_record,
        "reference": reference,
        "references_by_case": references_by_case,
        "source_by_segment": source_by_segment,
        "episodes": copy.deepcopy(list(episodes)),
    }


def _verify_runtime_binding() -> dict[str, Any]:
    binding = adapter.build_six_arm_matrix_binding()
    allowed = PROJECT_ROOT
    for key in (
        "canonical_label_schema",
        "adapter_module",
        "canonical_validator_module",
        "app_server_transport_module",
        "app_server_protocol_schema",
        "pinned_codex_cli",
    ):
        _verify_record(binding.get(key), label=f"runtime binding {key}", allowed_root=allowed)
    overlay = binding.get("context_control_overlay")
    instruction_sources = adapter.expected_instruction_source_contract()
    observed_arms = [
        (row.get("batch_size"), row.get("thread_mode"))
        for row in binding.get("arms") or []
        if isinstance(row, Mapping)
    ]
    if (
        binding.get("candidate_system_id") != adapter.CANDIDATE_SYSTEM_ID
        or binding.get("model") != adapter.MODEL
        or binding.get("effort") != adapter.EFFORT
        or binding.get("arm_count") != 6
        or binding.get("project_instruction_content_byte_budget") != 0
        or binding.get("managed_chatgpt_auth_only") is not True
        or binding.get("managed_chatgpt_plan_type") != "pro"
        or binding.get("official_persistent_codex_app_server_only") is not True
        or binding.get("retry_count") != 0
        or not isinstance(overlay, Mapping)
        or overlay.get("project_doc_max_bytes") != 0
        or binding.get("context_control_overlay_sha256")
        != sha256_text(_canonical_json(overlay))
        or binding.get("instruction_source_contract") != instruction_sources
        or binding.get("instruction_source_contract_sha256")
        != sha256_text(_canonical_json(instruction_sources))
        or observed_arms != list(EXPECTED_ARMS)
        or binding.get("canonical_final_field_paths")
        != adapter.canonical_final_field_paths()
        or binding.get("model_semantic_field_paths") != adapter.model_semantic_field_paths()
        or binding.get("deterministic_provenance_field_paths")
        != adapter.deterministic_provenance_field_paths()
        or binding.get("model_evidence_ownership_field_paths")
        != adapter.model_evidence_ownership_field_paths()
        or binding.get("semantic_integrity") != adapter.semantic_integrity_contract()
    ):
        raise CanonicalV31DevelopmentMatrixError("canonical adapter runtime binding drifted")
    return {
        "binding": binding,
        "binding_sha256": sha256_text(_canonical_json(binding)),
    }


def _parse_timestamp(value: Any, label: str) -> datetime:
    if not _is_iso_timestamp(value):
        raise CanonicalV31DevelopmentMatrixError(f"{label} is not timezone-aware ISO-8601")
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.astimezone(timezone.utc)


def _arm_rows_from_receipt(receipt: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = receipt.get("arms")
    if not isinstance(rows, list) or len(rows) != len(EXPECTED_ARMS):
        raise CanonicalV31DevelopmentMatrixError("matrix receipt does not contain six arms")
    observed = []
    result = []
    for row in rows:
        if not isinstance(row, Mapping):
            raise CanonicalV31DevelopmentMatrixError("matrix arm row is malformed")
        observed.append((row.get("batch_size"), row.get("thread_mode")))
        expected_id = _variant_id(
            int(row.get("batch_size") or 0), str(row.get("thread_mode") or "")
        )
        if row.get("variant_id") != expected_id:
            raise CanonicalV31DevelopmentMatrixError("matrix arm identity drifted")
        result.append(copy.deepcopy(dict(row)))
    if observed != list(EXPECTED_ARMS):
        raise CanonicalV31DevelopmentMatrixError("matrix arm order drifted")
    return result


def build_matrix_dry_preflight(
    *,
    manifest_path: Path,
    manifest_sha256: str,
    episodes: Sequence[Mapping[str, Any]],
    capacity_policy_path: Path,
    capacity_policy_sha256: str,
    allowed_root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    """Materialize all six request identities without auth, capacity, or model work."""

    manifest_info = verify_development_manifest(
        manifest_path,
        expected_sha256=manifest_sha256,
        episodes=episodes,
        allowed_root=allowed_root,
    )
    capacity_info = verify_capacity_policy(
        capacity_policy_path,
        expected_sha256=capacity_policy_sha256,
        allowed_root=allowed_root,
    )
    runtime_info = _verify_runtime_binding()
    expected_segment_order = [str(row["segment_id"]) for row in manifest_info["rows"]]
    expected_case_order = list(manifest_info["opaque_case_order"])
    case_by_segment = {
        str(row["segment_id"]): str(row["opaque_case_id"])
        for row in manifest_info["rows"]
    }
    requests_by_variant: dict[str, list[dict[str, Any]]] = {}
    arm_rows: list[dict[str, Any]] = []
    total_requests = 0
    for batch_size, thread_mode in EXPECTED_ARMS:
        variant_id = _variant_id(batch_size, thread_mode)
        requests = [
            adapter.validate_prepared_request(request)
            for episode in episodes
            for request in adapter.prepare_episode_batches(
                episode,
                batch_size=batch_size,
                thread_mode=thread_mode,
            )
        ]
        segment_order = [
            str(segment_id)
            for request in requests
            for segment_id in request["segment_ids"]
        ]
        case_order = [case_by_segment.get(segment_id) for segment_id in segment_order]
        if segment_order != expected_segment_order or case_order != expected_case_order:
            raise CanonicalV31DevelopmentMatrixError(
                f"{variant_id} opaque membership or order drifted"
            )
        if len({str(request["batch_id"]) for request in requests}) != len(requests):
            raise CanonicalV31DevelopmentMatrixError(f"{variant_id} batch IDs are not unique")
        request_hashes = [sha256_text(_canonical_json(request)) for request in requests]
        total_requests += len(requests)
        requests_by_variant[variant_id] = copy.deepcopy(requests)
        arm_rows.append(
            {
                "variant_id": variant_id,
                "batch_size": batch_size,
                "thread_mode": thread_mode,
                "request_count": len(requests),
                "batch_ids": [str(request["batch_id"]) for request in requests],
                "request_sha256s": request_hashes,
                "request_set_sha256": sha256_text(_canonical_json(request_hashes)),
                "prompt_sha256s": [str(request["prompt_sha256"]) for request in requests],
                "base_instructions_sha256s": [
                    str(request["base_instructions_sha256"]) for request in requests
                ],
                "output_schema_sha256s": [
                    str(request["output_schema_sha256"]) for request in requests
                ],
                "effective_batch_sizes": [
                    int(request["effective_batch_size"]) for request in requests
                ],
                "thread_lifecycle": (
                    "one_new_ephemeral_thread_per_batch"
                    if thread_mode == "new_thread"
                    else "one_ephemeral_thread_per_episode_reused_sequentially_across_batches"
                ),
                "cache_regime": (
                    "cold_new_thread_per_batch"
                    if thread_mode == "new_thread"
                    else "warm_same_episode_thread_across_batches"
                ),
                "expected_thread_count": (
                    len(requests) if thread_mode == "new_thread" else len(episodes)
                ),
                "app_server_process_count": 1,
                "same_episode_batches_execute_sequentially": True,
                "segment_order_sha256": sha256_text(_canonical_json(segment_order)),
                "opaque_case_order_sha256": manifest_info["opaque_case_order_sha256"],
                "full_canonical_v31_output_required": True,
                "semantic_postprocessing": False,
                "semantic_retry_count": 0,
            }
        )
    receipt = {
        "schema_version": DRY_PREFLIGHT_VERSION,
        "matrix_schema_version": MATRIX_VERSION,
        "state": "passed_offline_no_auth_capacity_probe_thread_or_turn",
        "candidate_system_id": adapter.CANDIDATE_SYSTEM_ID,
        "manifest_record": manifest_info["manifest_record"],
        "manifest_sha256": manifest_info["manifest_record"]["sha256"],
        "context_records": manifest_info["context_records"],
        "context_set_sha256": manifest_info["context_set_sha256"],
        "shared_reference_seed_record": manifest_info["reference_record"],
        "shared_reference_seed_sha256": manifest_info["reference_record"]["sha256"],
        "runtime_binding_sha256": runtime_info["binding_sha256"],
        "adapter_module": runtime_info["binding"]["adapter_module"],
        "context_control_overlay_sha256": runtime_info["binding"][
            "context_control_overlay_sha256"
        ],
        "instruction_source_contract_sha256": runtime_info["binding"][
            "instruction_source_contract_sha256"
        ],
        "capacity_policy_record": capacity_info["record"],
        "capacity_policy_sha256": capacity_info["record"]["sha256"],
        "canonical_final_field_paths_sha256": runtime_info["binding"][
            "canonical_final_field_paths_sha256"
        ],
        "model_semantic_field_paths_sha256": runtime_info["binding"][
            "model_semantic_field_paths_sha256"
        ],
        "deterministic_provenance_field_paths_sha256": runtime_info["binding"][
            "deterministic_provenance_field_paths_sha256"
        ],
        "opaque_case_order": expected_case_order,
        "opaque_case_order_sha256": manifest_info["opaque_case_order_sha256"],
        "case_count": len(expected_case_order),
        "episode_count": len(episodes),
        "arm_count": len(arm_rows),
        "arms": arm_rows,
        "exact_request_count": total_requests,
        "identical_opaque_case_order_across_all_arms": True,
        "full_canonical_v31_outputs_required": True,
        "managed_chatgpt_auth_only": True,
        "managed_chatgpt_plan_type": "pro",
        "official_persistent_codex_app_server_only": True,
        "semantic_retry_count": 0,
        "semantic_deterministic_defaults": {},
        "semantic_deterministic_pruning": False,
        "capacity_probe_performed": False,
        "semantic_client_started": False,
        "thread_started": False,
        "turn_started": False,
        "live_dispatch_authorized": False,
        "holdout_inspected": False,
        "production_mutated": False,
    }
    return {
        "receipt": receipt,
        "receipt_sha256": sha256_text(_canonical_json(receipt)),
        "requests_by_variant": requests_by_variant,
        "manifest_info": manifest_info,
        "capacity_info": capacity_info,
        "runtime_info": runtime_info,
    }


def build_matrix_precommit(preflight: Mapping[str, Any]) -> dict[str, Any]:
    """Build the deterministic six-arm precommit from a verified dry receipt."""

    if not isinstance(preflight, Mapping) or not isinstance(preflight.get("receipt"), Mapping):
        raise CanonicalV31DevelopmentMatrixError("dry preflight bundle is malformed")
    receipt = copy.deepcopy(dict(preflight["receipt"]))
    observed_sha = sha256_text(_canonical_json(receipt))
    if (
        preflight.get("receipt_sha256") != observed_sha
        or receipt.get("schema_version") != DRY_PREFLIGHT_VERSION
        or receipt.get("state")
        != "passed_offline_no_auth_capacity_probe_thread_or_turn"
        or receipt.get("capacity_probe_performed") is not False
        or receipt.get("semantic_client_started") is not False
        or receipt.get("thread_started") is not False
        or receipt.get("turn_started") is not False
        or receipt.get("live_dispatch_authorized") is not False
        or receipt.get("holdout_inspected") is not False
        or receipt.get("production_mutated") is not False
    ):
        raise CanonicalV31DevelopmentMatrixError("dry preflight checksum or closed-state drifted")
    arms = _arm_rows_from_receipt(receipt)
    precommit = {
        "schema_version": PRECOMMIT_VERSION,
        "matrix_schema_version": MATRIX_VERSION,
        "state": "frozen_before_capacity_admission_auth_thread_or_turn",
        "candidate_system_id": adapter.CANDIDATE_SYSTEM_ID,
        "dry_preflight_sha256": observed_sha,
        "manifest_sha256": receipt["manifest_sha256"],
        "context_set_sha256": receipt["context_set_sha256"],
        "shared_reference_seed_sha256": receipt["shared_reference_seed_sha256"],
        "runtime_binding_sha256": receipt["runtime_binding_sha256"],
        "adapter_module_sha256": receipt["adapter_module"]["sha256"],
        "context_control_overlay_sha256": receipt["context_control_overlay_sha256"],
        "instruction_source_contract_sha256": receipt[
            "instruction_source_contract_sha256"
        ],
        "capacity_policy_sha256": receipt["capacity_policy_sha256"],
        "canonical_final_field_paths_sha256": receipt[
            "canonical_final_field_paths_sha256"
        ],
        "model_semantic_field_paths_sha256": receipt[
            "model_semantic_field_paths_sha256"
        ],
        "deterministic_provenance_field_paths_sha256": receipt[
            "deterministic_provenance_field_paths_sha256"
        ],
        "opaque_case_order": copy.deepcopy(receipt["opaque_case_order"]),
        "opaque_case_order_sha256": receipt["opaque_case_order_sha256"],
        "case_count": receipt["case_count"],
        "episode_count": receipt["episode_count"],
        "arm_count": len(arms),
        "arms": arms,
        "identical_opaque_case_order_across_all_arms": True,
        "quality_evaluator_contract_sha256": sha256_text(
            _canonical_json(quality_evaluator_contract())
        ),
        "managed_chatgpt_auth_only": True,
        "managed_chatgpt_plan_type": "pro",
        "official_persistent_codex_app_server_only": True,
        "semantic_retry_count": 0,
        "semantic_deterministic_defaults": {},
        "semantic_deterministic_pruning": False,
        "requires_future_explicit_checksum_bound_directive": True,
        "live_dispatch_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    return {
        "precommit": precommit,
        "precommit_sha256": sha256_text(_canonical_json(precommit)),
    }


def freeze_matrix_precommit(path: Path, bundle: Mapping[str, Any]) -> dict[str, Any]:
    """Create one immutable precommit artifact or verify an identical existing one."""

    precommit = bundle.get("precommit") if isinstance(bundle, Mapping) else None
    if not isinstance(precommit, Mapping):
        raise CanonicalV31DevelopmentMatrixError("matrix precommit bundle is malformed")
    payload = copy.deepcopy(dict(precommit))
    expected_sha = sha256_text(_canonical_json(payload))
    if bundle.get("precommit_sha256") != expected_sha:
        raise CanonicalV31DevelopmentMatrixError("matrix precommit bundle checksum drifted")
    target = path.expanduser().resolve()
    text = _pretty_json(payload)
    if target.exists():
        try:
            existing = json.loads(target.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CanonicalV31DevelopmentMatrixError("existing precommit is unreadable") from exc
        if existing != payload:
            raise CanonicalV31DevelopmentMatrixError("immutable precommit drifted")
    else:
        write_text_atomic(target, text)
    record = _record(target)
    if record["sha256"] != sha256_text(text):
        raise CanonicalV31DevelopmentMatrixError("frozen precommit byte checksum drifted")
    return {"precommit": payload, "precommit_sha256": expected_sha, "record": record}


def _validated_precommit_bundle(bundle: Mapping[str, Any]) -> tuple[dict[str, Any], str]:
    precommit = bundle.get("precommit") if isinstance(bundle, Mapping) else None
    if not isinstance(precommit, Mapping):
        raise CanonicalV31DevelopmentMatrixError("precommit bundle is malformed")
    payload = copy.deepcopy(dict(precommit))
    checksum = sha256_text(_canonical_json(payload))
    if (
        bundle.get("precommit_sha256") != checksum
        or payload.get("schema_version") != PRECOMMIT_VERSION
        or payload.get("state") != "frozen_before_capacity_admission_auth_thread_or_turn"
        or payload.get("requires_future_explicit_checksum_bound_directive") is not True
        or payload.get("live_dispatch_authorized") is not False
        or payload.get("holdout_authorized") is not False
        or payload.get("production_mutation_allowed") is not False
        or payload.get("managed_chatgpt_plan_type") != "pro"
        or payload.get("semantic_retry_count") != 0
        or payload.get("semantic_deterministic_defaults") != {}
        or payload.get("semantic_deterministic_pruning") is not False
    ):
        raise CanonicalV31DevelopmentMatrixError("precommit fixed contract drifted")
    _arm_rows_from_receipt(payload)
    return payload, checksum


def build_future_plan_binding(
    precommit_bundle: Mapping[str, Any], *, output_root: Path
) -> dict[str, Any]:
    """Return a pure future-plan binding; this function creates no plan or directory."""

    precommit, precommit_sha = _validated_precommit_bundle(precommit_bundle)
    binding = {
        "schema_version": FUTURE_PLAN_BINDING_VERSION,
        "state": "binding_only_no_plan_installed_no_live_dispatch",
        "candidate_system_id": adapter.CANDIDATE_SYSTEM_ID,
        "precommit_sha256": precommit_sha,
        "manifest_sha256": precommit["manifest_sha256"],
        "context_set_sha256": precommit["context_set_sha256"],
        "shared_reference_seed_sha256": precommit["shared_reference_seed_sha256"],
        "runtime_binding_sha256": precommit["runtime_binding_sha256"],
        "context_control_overlay_sha256": precommit[
            "context_control_overlay_sha256"
        ],
        "instruction_source_contract_sha256": precommit[
            "instruction_source_contract_sha256"
        ],
        "capacity_policy_sha256": precommit["capacity_policy_sha256"],
        "quality_evaluator_contract_sha256": precommit[
            "quality_evaluator_contract_sha256"
        ],
        "opaque_case_order_sha256": precommit["opaque_case_order_sha256"],
        "arms": [
            {
                "variant_id": row["variant_id"],
                "batch_size": row["batch_size"],
                "thread_mode": row["thread_mode"],
                "request_set_sha256": row["request_set_sha256"],
            }
            for row in precommit["arms"]
        ],
        "output_root": str(output_root.expanduser().resolve()),
        "required_future_directive_schema_version": FUTURE_DIRECTIVE_VERSION,
        "future_directive_must_be_separate_fresh_explicit_artifact": True,
        "future_directive_must_bind_this_plan_checksum": True,
        "official_app_server_initialized_before_capacity": True,
        "capacity_admission_required_before_thread_start": True,
        "capacity_admission_required_before_thread_resume": True,
        "capacity_admission_required_before_turn_start": True,
        "managed_chatgpt_auth_only": True,
        "managed_chatgpt_plan_type": "pro",
        "official_persistent_codex_app_server_only": True,
        "semantic_retry_count": 0,
        "full_canonical_v31_outputs_required": True,
        "semantic_deterministic_defaults": {},
        "semantic_deterministic_pruning": False,
        "plan_file_created": False,
        "plan_installed": False,
        "live_dispatch_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    return {
        "binding": binding,
        "binding_sha256": sha256_text(_canonical_json(binding)),
    }


def _validated_plan_binding(bundle: Mapping[str, Any]) -> tuple[dict[str, Any], str]:
    binding = bundle.get("binding") if isinstance(bundle, Mapping) else None
    if not isinstance(binding, Mapping):
        raise CanonicalV31DevelopmentMatrixError("future plan binding is malformed")
    payload = copy.deepcopy(dict(binding))
    checksum = sha256_text(_canonical_json(payload))
    if (
        bundle.get("binding_sha256") != checksum
        or payload.get("schema_version") != FUTURE_PLAN_BINDING_VERSION
        or payload.get("state") != "binding_only_no_plan_installed_no_live_dispatch"
        or payload.get("plan_file_created") is not False
        or payload.get("plan_installed") is not False
        or payload.get("live_dispatch_authorized") is not False
        or payload.get("official_app_server_initialized_before_capacity") is not True
        or payload.get("capacity_admission_required_before_thread_start") is not True
        or payload.get("capacity_admission_required_before_thread_resume") is not True
        or payload.get("capacity_admission_required_before_turn_start") is not True
        or "capacity_admission_required_before_app_server_start" in payload
        or payload.get("managed_chatgpt_plan_type") != "pro"
        or payload.get("semantic_retry_count") != 0
        or payload.get("holdout_authorized") is not False
        or payload.get("production_mutation_allowed") is not False
    ):
        raise CanonicalV31DevelopmentMatrixError("future plan binding drifted")
    return payload, checksum


def validate_future_execution_directive(
    directive: Mapping[str, Any],
    *,
    expected_sha256: str,
    plan_binding: Mapping[str, Any],
    now: datetime | None = None,
) -> dict[str, Any]:
    """Validate a future explicit directive; no semantic action is performed."""

    if not isinstance(directive, Mapping):
        raise FutureExecutionDirectiveRequired("future execution directive is absent")
    payload = copy.deepcopy(dict(directive))
    checksum = sha256_text(_canonical_json(payload))
    if checksum != _require_sha256(expected_sha256, "future directive SHA-256"):
        raise FutureExecutionDirectiveRequired("future execution directive checksum drifted")
    plan, plan_sha = _validated_plan_binding(plan_binding)
    issued = _parse_timestamp(payload.get("issued_at"), "future directive issued_at")
    expires = _parse_timestamp(payload.get("expires_at"), "future directive expires_at")
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    if issued > current or current >= expires or expires <= issued:
        raise FutureExecutionDirectiveRequired("future execution directive is not fresh")
    expected_arms = copy.deepcopy(plan["arms"])
    if (
        payload.get("schema_version") != FUTURE_DIRECTIVE_VERSION
        or payload.get("state") != "explicit_live_development_matrix_authorization"
        or not isinstance(payload.get("authorized_by"), str)
        or not payload.get("authorized_by")
        or payload.get("candidate_system_id") != adapter.CANDIDATE_SYSTEM_ID
        or payload.get("future_plan_binding_sha256") != plan_sha
        or payload.get("precommit_sha256") != plan["precommit_sha256"]
        or payload.get("manifest_sha256") != plan["manifest_sha256"]
        or payload.get("context_set_sha256") != plan["context_set_sha256"]
        or payload.get("runtime_binding_sha256") != plan["runtime_binding_sha256"]
        or payload.get("context_control_overlay_sha256")
        != plan["context_control_overlay_sha256"]
        or payload.get("instruction_source_contract_sha256")
        != plan["instruction_source_contract_sha256"]
        or payload.get("capacity_policy_sha256") != plan["capacity_policy_sha256"]
        or payload.get("quality_evaluator_contract_sha256")
        != plan["quality_evaluator_contract_sha256"]
        or payload.get("arms") != expected_arms
        or payload.get("live_semantic_dispatch_authorized") is not True
        or payload.get("official_app_server_initialized_before_capacity") is not True
        or payload.get("capacity_admission_required_before_thread_start") is not True
        or payload.get("capacity_admission_required_before_thread_resume") is not True
        or payload.get("capacity_admission_required_before_turn_start") is not True
        or "capacity_admission_required_before_app_server_start" in payload
        or payload.get("managed_chatgpt_auth_only") is not True
        or payload.get("managed_chatgpt_plan_type") != "pro"
        or payload.get("official_persistent_codex_app_server_only") is not True
        or payload.get("full_canonical_v31_outputs_required") is not True
        or payload.get("semantic_retry_count") != 0
        or payload.get("semantic_deterministic_defaults") != {}
        or payload.get("semantic_deterministic_pruning") is not False
        or payload.get("holdout_authorized") is not False
        or payload.get("production_mutation_allowed") is not False
    ):
        raise FutureExecutionDirectiveRequired("future execution directive lineage drifted")
    return {"directive": payload, "directive_sha256": checksum}


def require_future_execution_directive(
    directive: Mapping[str, Any] | None,
    *,
    expected_sha256: str | None,
    plan_binding: Mapping[str, Any],
    now: datetime | None = None,
) -> dict[str, Any]:
    """The explicit closed gate a later executor must call before any live work."""

    if directive is None or expected_sha256 is None:
        raise FutureExecutionDirectiveRequired(
            "live dispatch requires a future explicit checksum-bound directive"
        )
    return validate_future_execution_directive(
        directive,
        expected_sha256=expected_sha256,
        plan_binding=plan_binding,
        now=now,
    )


def validate_capacity_admission(
    admission: Mapping[str, Any],
    *,
    expected_sha256: str,
    plan_binding: Mapping[str, Any],
    directive_info: Mapping[str, Any],
    precommit_bundle: Mapping[str, Any],
    variant_id: str,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Validate one fresh arm admission after init and before semantic dispatch."""

    if not isinstance(admission, Mapping):
        raise CanonicalV31DevelopmentMatrixError("capacity admission is absent")
    payload = copy.deepcopy(dict(admission))
    checksum = sha256_text(_canonical_json(payload))
    if checksum != _require_sha256(expected_sha256, "capacity admission SHA-256"):
        raise CanonicalV31DevelopmentMatrixError("capacity admission checksum drifted")
    plan, plan_sha = _validated_plan_binding(plan_binding)
    precommit, precommit_sha = _validated_precommit_bundle(precommit_bundle)
    directive = (
        directive_info.get("directive") if isinstance(directive_info, Mapping) else None
    )
    directive_sha = (
        directive_info.get("directive_sha256")
        if isinstance(directive_info, Mapping)
        else None
    )
    if (
        not isinstance(directive, Mapping)
        or not _valid_sha256(directive_sha)
        or sha256_text(_canonical_json(directive)) != directive_sha
        or directive.get("future_plan_binding_sha256") != plan_sha
        or directive.get("precommit_sha256") != precommit_sha
    ):
        raise CanonicalV31DevelopmentMatrixError("capacity admission directive lineage drifted")
    arms = {str(row["variant_id"]): row for row in precommit["arms"]}
    arm = arms.get(variant_id)
    if arm is None:
        raise CanonicalV31DevelopmentMatrixError("capacity admission variant is unknown")
    issued = _parse_timestamp(payload.get("issued_at"), "capacity admission issued_at")
    expires = _parse_timestamp(payload.get("expires_at"), "capacity admission expires_at")
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    requested_tokens = payload.get("requested_total_token_ceiling")
    available_tokens = payload.get("available_total_tokens")
    requested_wall = payload.get("requested_wall_seconds_ceiling")
    available_wall = payload.get("available_wall_seconds")
    numeric_capacity_valid = (
        not isinstance(requested_tokens, bool)
        and isinstance(requested_tokens, int)
        and requested_tokens > 0
        and not isinstance(available_tokens, bool)
        and isinstance(available_tokens, int)
        and available_tokens >= requested_tokens
        and _nonnegative_number(requested_wall, "requested wall ceiling") > 0
        and _nonnegative_number(available_wall, "available wall capacity")
        >= float(requested_wall)
    )
    if (
        issued > current
        or current >= expires
        or expires <= issued
        or not numeric_capacity_valid
        or payload.get("schema_version") != CAPACITY_ADMISSION_VERSION
        or payload.get("state")
        != "admitted_after_official_app_server_initialization_before_semantic_thread_or_turn"
        or payload.get("candidate_system_id") != adapter.CANDIDATE_SYSTEM_ID
        or payload.get("variant_id") != variant_id
        or payload.get("batch_size") != arm["batch_size"]
        or payload.get("thread_mode") != arm["thread_mode"]
        or payload.get("request_count") != arm["request_count"]
        or payload.get("request_set_sha256") != arm["request_set_sha256"]
        or payload.get("future_plan_binding_sha256") != plan_sha
        or payload.get("future_directive_sha256") != directive_sha
        or payload.get("precommit_sha256") != precommit_sha
        or payload.get("manifest_sha256") != plan["manifest_sha256"]
        or payload.get("runtime_binding_sha256") != plan["runtime_binding_sha256"]
        or payload.get("context_control_overlay_sha256")
        != plan["context_control_overlay_sha256"]
        or payload.get("instruction_source_contract_sha256")
        != plan["instruction_source_contract_sha256"]
        or payload.get("capacity_policy_sha256") != plan["capacity_policy_sha256"]
        or not _valid_sha256(payload.get("capacity_measurement_sha256"))
        or payload.get("capacity_available") is not True
        or payload.get("capacity_unknown") is not False
        or payload.get("official_app_server_initialized_before_capacity") is not True
        or payload.get("capacity_admission_required_before_thread_start") is not True
        or payload.get("capacity_admission_required_before_thread_resume") is not True
        or payload.get("capacity_admission_required_before_turn_start") is not True
        or payload.get("semantic_thread_started") is not False
        or payload.get("semantic_turn_started") is not False
        or "app_server_started" in payload
        or payload.get("managed_chatgpt_auth_only") is not True
        or payload.get("managed_chatgpt_plan_type") != "pro"
        or payload.get("semantic_retry_count") != 0
        or payload.get("holdout_authorized") is not False
        or payload.get("production_mutation_allowed") is not False
    ):
        raise CanonicalV31DevelopmentMatrixError("capacity admission lineage or headroom failed")
    return {"admission": payload, "admission_sha256": checksum}


def _validate_output_rows(
    rows: Any,
    *,
    manifest_info: Mapping[str, Any],
    label: str,
) -> dict[str, Any]:
    case_order = manifest_info.get("opaque_case_order")
    manifest_rows = manifest_info.get("rows")
    source_by_segment = manifest_info.get("source_by_segment")
    if (
        not isinstance(rows, list)
        or not isinstance(case_order, list)
        or not isinstance(manifest_rows, list)
        or not isinstance(source_by_segment, Mapping)
        or len(rows) != len(case_order)
    ):
        raise CanonicalV31DevelopmentMatrixError(f"{label} full-output bundle is malformed")
    specs = {str(row["opaque_case_id"]): row for row in manifest_rows}
    observed_order = []
    labels: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping) or set(row) != {"opaque_case_id", "label"}:
            raise CanonicalV31DevelopmentMatrixError(f"{label} output row is malformed")
        case_id = row.get("opaque_case_id")
        value = row.get("label")
        spec = specs.get(str(case_id))
        if spec is None or not isinstance(value, dict):
            raise CanonicalV31DevelopmentMatrixError(f"{label} output identity drifted")
        source = source_by_segment.get(str(spec["segment_id"]))
        if (
            not isinstance(source, Mapping)
            or value.get("segment_id") != spec["segment_id"]
            or value.get("episode_id") != spec["episode_id"]
            or value.get("segment_quality") != source.get("segment_quality")
        ):
            raise CanonicalV31DevelopmentMatrixError(f"{label} output provenance drifted")
        try:
            validate_label_output(
                adapter.CANONICAL_LABEL_PACK,
                value,
                segment_text=source.get("segment_text"),
            )
        except ValidationError as exc:
            raise CanonicalV31DevelopmentMatrixError(
                f"{label} is not a full grounded canonical v3.1 output"
            ) from exc
        observed_order.append(str(case_id))
        labels.append(copy.deepcopy(value))
    if observed_order != case_order:
        raise CanonicalV31DevelopmentMatrixError(f"{label} opaque case order drifted")
    return {
        "case_count": len(rows),
        "opaque_case_order_sha256": sha256_text(_canonical_json(observed_order)),
        "output_sha256": sha256_text(_canonical_json(rows)),
        "labels_sha256": sha256_text(_canonical_json(labels)),
    }


def validate_full_v31_outputs(
    outputs_by_variant: Mapping[str, Any], *, manifest_info: Mapping[str, Any]
) -> dict[str, Any]:
    """Require every arm to return every full canonical label in one opaque order."""

    expected_ids = [_variant_id(size, mode) for size, mode in EXPECTED_ARMS]
    if not isinstance(outputs_by_variant, Mapping) or list(outputs_by_variant) != expected_ids:
        raise CanonicalV31DevelopmentMatrixError("full-output variant set or order drifted")
    receipts = {
        variant_id: _validate_output_rows(
            outputs_by_variant[variant_id],
            manifest_info=manifest_info,
            label=variant_id,
        )
        for variant_id in expected_ids
    }
    order_hashes = {receipt["opaque_case_order_sha256"] for receipt in receipts.values()}
    if order_hashes != {manifest_info.get("opaque_case_order_sha256")}:
        raise CanonicalV31DevelopmentMatrixError("opaque case order differs across arms")
    return {
        "state": "all_six_full_canonical_v31_outputs_validated",
        "arm_count": len(receipts),
        "case_count_per_arm": len(manifest_info["opaque_case_order"]),
        "opaque_case_order_sha256": manifest_info["opaque_case_order_sha256"],
        "arms": receipts,
    }


def validate_arm_envelope(
    envelope: Mapping[str, Any],
    *,
    expected_sha256: str,
    precommit_bundle: Mapping[str, Any],
    plan_binding: Mapping[str, Any],
    directive_info: Mapping[str, Any],
    admission_info: Mapping[str, Any],
    manifest_info: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate a future arm's adapter report, full outputs, and exact lineage."""

    if not isinstance(envelope, Mapping):
        raise CanonicalV31DevelopmentMatrixError("arm envelope is absent")
    payload = copy.deepcopy(dict(envelope))
    checksum = sha256_text(_canonical_json(payload))
    if checksum != _require_sha256(expected_sha256, "arm envelope SHA-256"):
        raise CanonicalV31DevelopmentMatrixError("arm envelope checksum drifted")
    precommit, precommit_sha = _validated_precommit_bundle(precommit_bundle)
    plan, plan_sha = _validated_plan_binding(plan_binding)
    directive = directive_info.get("directive") if isinstance(directive_info, Mapping) else None
    admission = admission_info.get("admission") if isinstance(admission_info, Mapping) else None
    if not isinstance(directive, Mapping) or not isinstance(admission, Mapping):
        raise CanonicalV31DevelopmentMatrixError("arm authorization lineage is absent")
    directive_sha = directive_info.get("directive_sha256")
    admission_sha = admission_info.get("admission_sha256")
    if (
        not _valid_sha256(directive_sha)
        or sha256_text(_canonical_json(directive)) != directive_sha
        or not _valid_sha256(admission_sha)
        or sha256_text(_canonical_json(admission)) != admission_sha
    ):
        raise CanonicalV31DevelopmentMatrixError("arm authorization checksums drifted")
    variant_id = payload.get("variant_id")
    arm = next(
        (row for row in precommit["arms"] if row["variant_id"] == variant_id),
        None,
    )
    report = payload.get("adapter_report")
    rows = payload.get("outputs")
    if arm is None or not isinstance(report, Mapping):
        raise CanonicalV31DevelopmentMatrixError("arm envelope identity or report is absent")
    output_receipt = _validate_output_rows(
        rows,
        manifest_info=manifest_info,
        label=str(variant_id),
    )
    usage = _usage(report.get("usage"), f"{variant_id} adapter report")
    result_rows = report.get("results")
    if not isinstance(result_rows, list) or len(result_rows) != arm["request_count"]:
        raise CanonicalV31DevelopmentMatrixError("arm adapter result count drifted")
    if [row.get("batch_id") for row in result_rows if isinstance(row, Mapping)] != arm[
        "batch_ids"
    ]:
        raise CanonicalV31DevelopmentMatrixError("arm adapter batch order drifted")
    result_usage = []
    for result in result_rows:
        if (
            not isinstance(result, Mapping)
            or result.get("state") != "validated"
            or not isinstance(result.get("thread_id"), str)
            or not result.get("thread_id")
            or not isinstance(result.get("turn_id"), str)
            or not result.get("turn_id")
        ):
            raise CanonicalV31DevelopmentMatrixError("arm result lifecycle is incomplete")
        result_usage.append(_usage(result.get("usage"), "arm turn"))
    summed = {field: sum(item[field] for item in result_usage) for field in USAGE_FIELDS}
    expected_instruction_sources = adapter.expected_instruction_source_contract()
    if (
        summed != usage
        or payload.get("schema_version") != ARM_ENVELOPE_VERSION
        or payload.get("state") != "completed_full_canonical_v31_arm"
        or payload.get("precommit_sha256") != precommit_sha
        or payload.get("future_plan_binding_sha256") != plan_sha
        or payload.get("future_directive_sha256") != directive_sha
        or payload.get("capacity_admission_sha256") != admission_sha
        or payload.get("manifest_sha256") != plan["manifest_sha256"]
        or payload.get("context_set_sha256") != plan["context_set_sha256"]
        or payload.get("runtime_binding_sha256") != plan["runtime_binding_sha256"]
        or payload.get("context_control_overlay_sha256")
        != plan["context_control_overlay_sha256"]
        or payload.get("instruction_source_contract_sha256")
        != plan["instruction_source_contract_sha256"]
        or payload.get("capacity_policy_sha256") != plan["capacity_policy_sha256"]
        or admission.get("variant_id") != variant_id
        or report.get("schema_version") != adapter.RUN_REPORT_VERSION
        or report.get("state") != "passed"
        or report.get("candidate_system_id") != adapter.CANDIDATE_SYSTEM_ID
        or report.get("batch_size") != arm["batch_size"]
        or report.get("thread_mode") != arm["thread_mode"]
        or report.get("model") != adapter.MODEL
        or report.get("effort") != adapter.EFFORT
        or report.get("requested_calls") != arm["request_count"]
        or report.get("attempted_calls") != arm["request_count"]
        or report.get("validated_calls") != arm["request_count"]
        or report.get("rejected_calls") != 0
        or report.get("failed_calls") != 0
        or report.get("usage_status") != "complete"
        or report.get("cached_input_tokens") != usage["cached_input_tokens"]
        or report.get("reasoning_output_tokens") != usage["reasoning_output_tokens"]
        or report.get("retry_count") != 0
        or report.get("ambiguous_retry_count") != 0
        or report.get("semantic_postprocessing") is not False
        or report.get("all_emitted_semantic_values_preserved") is not True
        or report.get("thread_lineage_valid") is not True
        or report.get("turn_ids_unique") is not True
        or report.get("observed_turn_count") != arm["request_count"]
        or report.get("observed_thread_count") != arm["expected_thread_count"]
        or report.get("managed_chatgpt_auth_verified") is not True
        or report.get("managed_chatgpt_plan_type") != "pro"
        or report.get("instruction_source_contract") != expected_instruction_sources
        or report.get("instruction_source_contract_sha256")
        != sha256_text(_canonical_json(expected_instruction_sources))
        or report.get("project_instruction_content_byte_budget") != 0
        or report.get("production_mutated") is not False
        or report.get("holdout_authorized") is not False
    ):
        raise CanonicalV31DevelopmentMatrixError("arm report or lineage contract failed")
    wall = _nonnegative_number(report.get("wall_elapsed_seconds"), "arm wall telemetry")
    return {
        "envelope": payload,
        "envelope_sha256": checksum,
        "variant_id": variant_id,
        "usage": usage,
        "wall_elapsed_seconds": wall,
        "output_receipt": output_receipt,
    }


def quality_evaluator_contract() -> dict[str, Any]:
    """Return the full-field LLM-only support-first/then-alignment contract."""

    final_fields = adapter.canonical_final_field_paths()
    semantic_fields = adapter.model_semantic_field_paths()
    provenance_fields = adapter.deterministic_provenance_field_paths()
    if (
        set(semantic_fields) & set(provenance_fields)
        or set(semantic_fields) | set(provenance_fields) != set(final_fields)
    ):
        raise CanonicalV31DevelopmentMatrixError("canonical quality field partition drifted")
    return {
        "schema_version": QUALITY_CONTRACT_VERSION,
        "canonical_label_pack": adapter.CANONICAL_LABEL_PACK,
        "canonical_final_field_paths": final_fields,
        "canonical_final_field_paths_sha256": sha256_text(_canonical_json(final_fields)),
        "model_semantic_field_paths": semantic_fields,
        "model_semantic_field_paths_sha256": sha256_text(_canonical_json(semantic_fields)),
        "deterministic_provenance_field_paths": provenance_fields,
        "deterministic_provenance_field_paths_sha256": sha256_text(
            _canonical_json(provenance_fields)
        ),
        "every_canonical_final_field_covered": True,
        "evaluation_mode": "llm_only",
        "embeddings_used": False,
        "deterministic_semantic_matching": False,
        "deterministic_semantic_pruning": False,
        "deterministic_semantic_defaults": {},
        "stage_order": ["support_first", "alignment"],
        "support_first_before_alignment": True,
        "orientations_per_stage": ["ab", "ba"],
        "abstention_enabled_per_stage": True,
        "one_shared_augmented_reference_for_all_six_arms": True,
        "identical_opaque_case_order_across_all_stages_and_arms": True,
        "support_stage": {
            "purpose": "llm_only_source_support_and_exact_span_ownership",
            "reference_access": "source_plus_one_shared_augmented_reference",
        },
        "alignment_stage": {
            "purpose": "llm_only_fieldwise_full_canonical_alignment",
            "reference_access": "same_shared_augmented_reference",
        },
        "strict_full_field_macro_threshold": QUALITY_THRESHOLD,
        "noninferiority_margin": NONINFERIORITY_MARGIN,
        "noninferiority_reference": (
            "one_frozen_full_canonical_baseline_scored_in_the_same_shared_"
            "augmented_reference_evaluation"
        ),
        "exact_evidence_rate_required": 1.0,
        "exact_offset_rate_required": 1.0,
        "exact_provenance_rate_required": 1.0,
        "production_amortized_total_token_ratio_threshold": TOKEN_RATIO_THRESHOLD,
        "managed_chatgpt_auth_only": True,
        "managed_chatgpt_plan_type": "pro",
        "official_persistent_codex_app_server_only": True,
        "complete_usage_cached_reasoning_wall_telemetry_required": True,
        "semantic_retry_count": 0,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }


def _sum_usage(values: Sequence[Mapping[str, int]]) -> dict[str, int]:
    return {field: sum(int(value[field]) for value in values) for field in USAGE_FIELDS}


def _validate_shared_augmented_reference(
    record: Any,
    *,
    manifest_info: Mapping[str, Any],
    allowed_root: Path,
) -> dict[str, Any]:
    path = _verify_record(
        record,
        label="shared augmented canonical reference",
        allowed_root=allowed_root,
    )
    payload = _load_object(path, "shared augmented canonical reference")
    references = payload.get("references")
    expected_order = manifest_info.get("opaque_case_order")
    specs = {
        str(row["opaque_case_id"]): row for row in manifest_info.get("rows") or []
    }
    sources = manifest_info.get("source_by_segment")
    if (
        payload.get("schema_version") != AUGMENTED_REFERENCE_VERSION
        or payload.get("state") != "frozen_one_shared_reference_before_judging"
        or payload.get("source_reference_seed_sha256")
        != manifest_info.get("reference_record", {}).get("sha256")
        or payload.get("case_order") != expected_order
        or payload.get("case_order_sha256")
        != manifest_info.get("opaque_case_order_sha256")
        or payload.get("one_shared_reference_for_all_six_arms") is not True
        or payload.get("augmentation_mode") != "llm_only"
        or payload.get("embeddings_used") is not False
        or payload.get("deterministic_semantic_matching") is not False
        or payload.get("semantic_deterministic_defaults") != {}
        or payload.get("semantic_deterministic_pruning") is not False
        or not isinstance(references, list)
        or len(references) != len(expected_order or [])
        or not isinstance(sources, Mapping)
    ):
        raise CanonicalV31DevelopmentMatrixError("shared augmented reference contract drifted")
    observed_order = []
    for reference in references:
        if not isinstance(reference, Mapping):
            raise CanonicalV31DevelopmentMatrixError("augmented reference row is malformed")
        case_id = reference.get("opaque_case_id")
        spec = specs.get(str(case_id))
        label = reference.get("label")
        source = sources.get(str(spec.get("segment_id"))) if spec else None
        if (
            spec is None
            or not isinstance(label, dict)
            or not isinstance(source, Mapping)
            or reference.get("segment_id") != spec["segment_id"]
            or reference.get("episode_id") != spec["episode_id"]
            or reference.get("text_sha256") != spec["text_sha256"]
            or label.get("segment_id") != spec["segment_id"]
            or label.get("episode_id") != spec["episode_id"]
            or label.get("segment_quality") != source.get("segment_quality")
        ):
            raise CanonicalV31DevelopmentMatrixError("augmented reference provenance drifted")
        try:
            validate_label_output(
                adapter.CANONICAL_LABEL_PACK,
                label,
                segment_text=source.get("segment_text"),
            )
        except ValidationError as exc:
            raise CanonicalV31DevelopmentMatrixError(
                "augmented reference is not full grounded canonical v3.1"
            ) from exc
        observed_order.append(str(case_id))
    if observed_order != expected_order:
        raise CanonicalV31DevelopmentMatrixError("augmented reference case order drifted")
    return {"record": _record(path), "payload": payload}


def _validate_stage(
    stage: Any,
    *,
    expected_name: str,
    case_order_sha256: str,
    reference_sha256: str,
) -> dict[str, Any]:
    if not isinstance(stage, Mapping):
        raise CanonicalV31DevelopmentMatrixError(f"{expected_name} stage is absent")
    orientations = stage.get("orientation_receipts")
    if not isinstance(orientations, list) or len(orientations) != 2:
        raise CanonicalV31DevelopmentMatrixError(f"{expected_name} AB/BA receipts are absent")
    observed_usage: list[dict[str, int]] = []
    observed_wall = 0.0
    observed_names = []
    for receipt in orientations:
        if not isinstance(receipt, Mapping):
            raise CanonicalV31DevelopmentMatrixError("judge orientation receipt is malformed")
        orientation = receipt.get("orientation")
        usage = _usage(receipt.get("usage"), f"{expected_name}.{orientation}")
        wall = _nonnegative_number(
            receipt.get("wall_elapsed_seconds"),
            f"{expected_name}.{orientation} wall telemetry",
        )
        abstentions = receipt.get("abstention_count")
        if (
            orientation not in {"ab", "ba"}
            or receipt.get("state") != "completed"
            or receipt.get("case_order_sha256") != case_order_sha256
            or receipt.get("shared_augmented_reference_sha256") != reference_sha256
            or receipt.get("evaluation_mode") != "llm_only"
            or receipt.get("abstention_enabled") is not True
            or receipt.get("managed_chatgpt_auth_verified") is not True
            or receipt.get("managed_chatgpt_plan_type") != "pro"
            or receipt.get("usage_status") != "complete"
            or receipt.get("retry_count") != 0
            or receipt.get("deterministic_semantic_matching") is not False
            or isinstance(abstentions, bool)
            or not isinstance(abstentions, int)
            or abstentions < 0
        ):
            raise CanonicalV31DevelopmentMatrixError(
                f"{expected_name} orientation contract failed"
            )
        observed_names.append(str(orientation))
        observed_usage.append(usage)
        observed_wall += wall
    stage_usage = _usage(stage.get("usage"), f"{expected_name} stage")
    stage_wall = _nonnegative_number(
        stage.get("wall_elapsed_seconds"), f"{expected_name} stage wall telemetry"
    )
    if (
        observed_names != ["ab", "ba"]
        or stage.get("stage") != expected_name
        or stage.get("state") != "completed"
        or stage.get("orientations") != ["ab", "ba"]
        or stage.get("case_order_sha256") != case_order_sha256
        or stage.get("shared_augmented_reference_sha256") != reference_sha256
        or stage.get("evaluation_mode") != "llm_only"
        or stage.get("abstention_enabled") is not True
        or stage.get("accounting_complete") is not True
        or stage.get("managed_chatgpt_auth_only") is not True
        or stage.get("managed_chatgpt_plan_type") != "pro"
        or stage.get("official_persistent_codex_app_server_only") is not True
        or stage.get("retry_count") != 0
        or stage.get("deterministic_semantic_matching") is not False
        or stage_usage != _sum_usage(observed_usage)
        or abs(stage_wall - observed_wall) > 1e-9
    ):
        raise CanonicalV31DevelopmentMatrixError(f"{expected_name} aggregate contract failed")
    return {
        "stage": copy.deepcopy(dict(stage)),
        "usage": stage_usage,
        "wall_elapsed_seconds": stage_wall,
    }


def _validate_production_cost(
    cost: Any,
    *,
    usage: Mapping[str, int],
    development_case_count: int,
    label: str,
) -> dict[str, Any]:
    if not isinstance(cost, Mapping):
        raise CanonicalV31DevelopmentMatrixError(f"{label} production cost is absent")
    production_segments = cost.get("production_segment_count")
    production_episodes = cost.get("production_episode_count")
    context_tokens = cost.get("recurring_context_tokens_per_episode")
    baseline_per_segment = cost.get("baseline_tokens_per_segment")
    if (
        cost.get("formula") != PRODUCTION_COST_FORMULA
        or cost.get("measured_extraction_total_tokens") != usage["total_tokens"]
        or cost.get("development_segment_count") != development_case_count
        or isinstance(production_segments, bool)
        or not isinstance(production_segments, int)
        or production_segments <= 0
        or isinstance(production_episodes, bool)
        or not isinstance(production_episodes, int)
        or production_episodes <= 0
        or isinstance(context_tokens, bool)
        or not isinstance(context_tokens, int)
        or context_tokens < 0
    ):
        raise CanonicalV31DevelopmentMatrixError(f"{label} production cost inputs drifted")
    baseline_rate = _nonnegative_number(
        baseline_per_segment, f"{label} baseline tokens per segment"
    )
    if baseline_rate <= 0 or development_case_count <= 0:
        raise CanonicalV31DevelopmentMatrixError(f"{label} production denominator is invalid")
    candidate = (
        usage["total_tokens"] / development_case_count * production_segments
        + context_tokens * production_episodes
    )
    baseline = baseline_rate * production_segments
    ratio = candidate / baseline
    for key, expected in (
        ("candidate_production_amortized_total_tokens", candidate),
        ("baseline_production_total_tokens", baseline),
        ("production_amortized_total_token_ratio", ratio),
    ):
        observed = _nonnegative_number(cost.get(key), f"{label}.{key}")
        if abs(observed - expected) > 1e-9:
            raise CanonicalV31DevelopmentMatrixError(f"{label} production cost arithmetic drifted")
    return {**copy.deepcopy(dict(cost)), "production_amortized_total_token_ratio": ratio}


def validate_quality_result(
    result: Mapping[str, Any],
    *,
    expected_sha256: str,
    precommit_bundle: Mapping[str, Any],
    manifest_info: Mapping[str, Any],
    expected_arm_envelope_sha256s: Mapping[str, str],
    allowed_root: Path,
) -> dict[str, Any]:
    """Validate complete shared-reference AB/BA judging and measured arm metrics."""

    if not isinstance(result, Mapping):
        raise CanonicalV31DevelopmentMatrixError("quality result is absent")
    payload = copy.deepcopy(dict(result))
    checksum = sha256_text(_canonical_json(payload))
    if checksum != _require_sha256(expected_sha256, "quality result SHA-256"):
        raise CanonicalV31DevelopmentMatrixError("quality result checksum drifted")
    precommit, precommit_sha = _validated_precommit_bundle(precommit_bundle)
    contract = quality_evaluator_contract()
    contract_sha = sha256_text(_canonical_json(contract))
    expected_ids = [_variant_id(size, mode) for size, mode in EXPECTED_ARMS]
    expected_envelopes = dict(expected_arm_envelope_sha256s)
    if (
        list(expected_envelopes) != expected_ids
        or any(not _valid_sha256(value) for value in expected_envelopes.values())
    ):
        raise CanonicalV31DevelopmentMatrixError("expected arm envelope checksums drifted")
    augmented = _validate_shared_augmented_reference(
        payload.get("shared_augmented_reference"),
        manifest_info=manifest_info,
        allowed_root=allowed_root,
    )
    reference_sha = augmented["record"]["sha256"]
    stages = payload.get("stages")
    if not isinstance(stages, list) or len(stages) != 2:
        raise CanonicalV31DevelopmentMatrixError("quality stages are incomplete")
    validated_stages = [
        _validate_stage(
            stage,
            expected_name=name,
            case_order_sha256=manifest_info["opaque_case_order_sha256"],
            reference_sha256=reference_sha,
        )
        for stage, name in zip(stages, ("support_first", "alignment"))
    ]
    judge_usage = _usage(payload.get("judge_usage"), "quality judge aggregate")
    judge_wall = _nonnegative_number(
        payload.get("judge_wall_elapsed_seconds"), "quality judge aggregate wall telemetry"
    )
    if (
        judge_usage != _sum_usage([stage["usage"] for stage in validated_stages])
        or abs(
            judge_wall
            - sum(float(stage["wall_elapsed_seconds"]) for stage in validated_stages)
        )
        > 1e-9
    ):
        raise CanonicalV31DevelopmentMatrixError("quality judge aggregate accounting drifted")
    arms = payload.get("arms")
    if not isinstance(arms, Mapping) or list(arms) != expected_ids:
        raise CanonicalV31DevelopmentMatrixError("quality arm set or order drifted")
    validated_arms: dict[str, dict[str, Any]] = {}
    baseline_macro_global = _nonnegative_number(
        payload.get("baseline_strict_full_field_macro"),
        "shared baseline strict macro",
    )
    if baseline_macro_global > 1:
        raise CanonicalV31DevelopmentMatrixError("shared baseline strict macro is above one")
    for variant_id in expected_ids:
        metrics = arms[variant_id]
        if not isinstance(metrics, Mapping):
            raise CanonicalV31DevelopmentMatrixError("quality arm metrics are malformed")
        usage = _usage(metrics.get("usage"), f"{variant_id} extraction")
        wall = _nonnegative_number(
            metrics.get("wall_elapsed_seconds"), f"{variant_id} extraction wall telemetry"
        )
        macro = _nonnegative_number(
            metrics.get("strict_full_field_macro"), f"{variant_id} strict macro"
        )
        baseline_macro = _nonnegative_number(
            metrics.get("baseline_strict_full_field_macro"),
            f"{variant_id} baseline strict macro",
        )
        exact_evidence = _nonnegative_number(
            metrics.get("exact_evidence_rate"), f"{variant_id} exact evidence rate"
        )
        exact_offsets = _nonnegative_number(
            metrics.get("exact_offset_rate"), f"{variant_id} exact offset rate"
        )
        exact_provenance = _nonnegative_number(
            metrics.get("exact_provenance_rate"), f"{variant_id} exact provenance rate"
        )
        rates = (
            macro,
            baseline_macro,
            exact_evidence,
            exact_offsets,
            exact_provenance,
        )
        if any(value > 1 for value in rates):
            raise CanonicalV31DevelopmentMatrixError("quality rate is above one")
        cost = _validate_production_cost(
            metrics.get("production_cost"),
            usage=usage,
            development_case_count=len(manifest_info["opaque_case_order"]),
            label=variant_id,
        )
        if (
            metrics.get("arm_envelope_sha256") != expected_envelopes[variant_id]
            or metrics.get("case_order_sha256")
            != manifest_info["opaque_case_order_sha256"]
            or metrics.get("shared_augmented_reference_sha256") != reference_sha
            or metrics.get("evaluated_model_semantic_field_paths_sha256")
            != contract["model_semantic_field_paths_sha256"]
            or metrics.get("evaluated_deterministic_provenance_field_paths_sha256")
            != contract["deterministic_provenance_field_paths_sha256"]
            or metrics.get("every_canonical_final_field_evaluated") is not True
            or baseline_macro != baseline_macro_global
            or metrics.get("usage_status") != "complete"
            or metrics.get("accounting_complete") is not True
            or metrics.get("cached_input_tokens") != usage["cached_input_tokens"]
            or metrics.get("reasoning_output_tokens") != usage["reasoning_output_tokens"]
            or metrics.get("semantic_defaults_used") is not False
            or metrics.get("semantic_pruning") is not False
            or metrics.get("retry_count") != 0
        ):
            raise CanonicalV31DevelopmentMatrixError("quality arm lineage or accounting drifted")
        validated_arms[variant_id] = {
            "metrics": copy.deepcopy(dict(metrics)),
            "usage": usage,
            "wall_elapsed_seconds": wall,
            "strict_full_field_macro": macro,
            "baseline_strict_full_field_macro": baseline_macro,
            "exact_evidence_rate": exact_evidence,
            "exact_offset_rate": exact_offsets,
            "exact_provenance_rate": exact_provenance,
            "production_cost": cost,
        }
    if (
        payload.get("schema_version") != QUALITY_RESULT_VERSION
        or payload.get("state") != "completed_full_canonical_shared_reference_evaluation"
        or payload.get("precommit_sha256") != precommit_sha
        or payload.get("manifest_sha256") != precommit["manifest_sha256"]
        or payload.get("context_set_sha256") != precommit["context_set_sha256"]
        or payload.get("runtime_binding_sha256") != precommit["runtime_binding_sha256"]
        or payload.get("context_control_overlay_sha256")
        != precommit["context_control_overlay_sha256"]
        or payload.get("instruction_source_contract_sha256")
        != precommit["instruction_source_contract_sha256"]
        or payload.get("capacity_policy_sha256") != precommit["capacity_policy_sha256"]
        or payload.get("quality_evaluator_contract_sha256") != contract_sha
        or payload.get("arm_envelope_sha256s") != expected_envelopes
        or payload.get("case_order") != manifest_info["opaque_case_order"]
        or payload.get("case_order_sha256") != manifest_info["opaque_case_order_sha256"]
        or payload.get("stage_order") != ["support_first", "alignment"]
        or payload.get("orientations_per_stage") != ["ab", "ba"]
        or payload.get("abstention_enabled_per_stage") is not True
        or payload.get("one_shared_augmented_reference_for_all_six_arms") is not True
        or not isinstance(payload.get("baseline_system_id"), str)
        or not payload.get("baseline_system_id")
        or not _valid_sha256(payload.get("baseline_full_v31_output_sha256"))
        or payload.get("baseline_scored_in_same_shared_reference_evaluation") is not True
        or payload.get("evaluation_mode") != "llm_only"
        or payload.get("embeddings_used") is not False
        or payload.get("deterministic_semantic_matching") is not False
        or payload.get("semantic_deterministic_defaults") != {}
        or payload.get("semantic_deterministic_pruning") is not False
        or payload.get("judge_accounting_complete") is not True
        or payload.get("managed_chatgpt_auth_only") is not True
        or payload.get("managed_chatgpt_plan_type") != "pro"
        or payload.get("official_persistent_codex_app_server_only") is not True
        or payload.get("semantic_retry_count") != 0
        or payload.get("holdout_authorized") is not False
        or payload.get("production_mutation_allowed") is not False
    ):
        raise CanonicalV31DevelopmentMatrixError("quality result fixed contract drifted")
    return {
        "result": payload,
        "result_sha256": checksum,
        "contract": contract,
        "contract_sha256": contract_sha,
        "shared_augmented_reference": augmented,
        "stages": validated_stages,
        "judge_usage": judge_usage,
        "judge_wall_elapsed_seconds": judge_wall,
        "arms": validated_arms,
    }


def score_and_select_winner(
    result: Mapping[str, Any],
    *,
    expected_sha256: str,
    precommit_bundle: Mapping[str, Any],
    manifest_info: Mapping[str, Any],
    expected_arm_envelope_sha256s: Mapping[str, str],
    allowed_root: Path,
) -> dict[str, Any]:
    """Apply the strict development gates and choose quality-first, cost-second."""

    validated = validate_quality_result(
        result,
        expected_sha256=expected_sha256,
        precommit_bundle=precommit_bundle,
        manifest_info=manifest_info,
        expected_arm_envelope_sha256s=expected_arm_envelope_sha256s,
        allowed_root=allowed_root,
    )
    arms: dict[str, dict[str, Any]] = {}
    for variant_id, value in validated["arms"].items():
        checks = {
            "strict_full_field_macro_gte_0_97": (
                value["strict_full_field_macro"] >= QUALITY_THRESHOLD
            ),
            "noninferior_margin_0": (
                value["strict_full_field_macro"]
                >= value["baseline_strict_full_field_macro"] - NONINFERIORITY_MARGIN
            ),
            "exact_evidence_rate_1": value["exact_evidence_rate"] == 1.0,
            "exact_offset_rate_1": value["exact_offset_rate"] == 1.0,
            "exact_provenance_rate_1": value["exact_provenance_rate"] == 1.0,
            "production_amortized_total_token_ratio_lte_0_28": (
                value["production_cost"]["production_amortized_total_token_ratio"]
                <= TOKEN_RATIO_THRESHOLD
            ),
            "complete_usage_cached_reasoning_wall_telemetry": True,
            "no_semantic_defaults_or_pruning": True,
        }
        arms[variant_id] = {
            "passed": all(checks.values()),
            "checks": checks,
            "strict_full_field_macro": value["strict_full_field_macro"],
            "baseline_strict_full_field_macro": value[
                "baseline_strict_full_field_macro"
            ],
            "production_amortized_total_token_ratio": value["production_cost"][
                "production_amortized_total_token_ratio"
            ],
        }
    passing = [variant_id for variant_id, row in arms.items() if row["passed"]]
    if not passing:
        raise CanonicalV31DevelopmentMatrixError("no development arm passed every gate")
    ranked = sorted(
        passing,
        key=lambda variant_id: (
            -float(arms[variant_id]["strict_full_field_macro"]),
            float(arms[variant_id]["production_amortized_total_token_ratio"]),
            variant_id,
        ),
    )
    best = ranked[0]
    best_key = (
        arms[best]["strict_full_field_macro"],
        arms[best]["production_amortized_total_token_ratio"],
    )
    tied = [
        variant_id
        for variant_id in ranked
        if (
            arms[variant_id]["strict_full_field_macro"],
            arms[variant_id]["production_amortized_total_token_ratio"],
        )
        == best_key
    ]
    if len(tied) != 1:
        raise CanonicalV31DevelopmentMatrixError("development winner is not unique")
    return {
        "schema_version": SELECTION_VERSION,
        "state": "development_winner_selected_holdout_and_production_closed",
        "quality_result_sha256": validated["result_sha256"],
        "strict_full_field_macro_threshold": QUALITY_THRESHOLD,
        "noninferiority_margin": NONINFERIORITY_MARGIN,
        "exact_evidence_offset_provenance_required": 1.0,
        "production_amortized_total_token_ratio_threshold": TOKEN_RATIO_THRESHOLD,
        "ranking_policy": "highest_strict_macro_then_lowest_production_amortized_token_ratio",
        "arms": arms,
        "passing_arms": passing,
        "ranking": ranked,
        "winner_variant_id": best,
        "winner_is_development_only": True,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
