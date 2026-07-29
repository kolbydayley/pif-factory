from __future__ import annotations

"""Fail-closed epoch-7 development gate for the expanded-cap adapter.

The coordinator deliberately stops at one frozen development winner.  It never
opens or samples the untouched holdout and it never mutates production.  Live
semantic work is delegated through injected arm and judge runners so the full
control surface can be exercised with pure fixtures.
"""

import argparse
import asyncio
import copy
import hashlib
import inspect
import json
import math
import os
import sqlite3
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Mapping, Sequence

from . import app_server_evaluation as evaluation
from . import app_server_expanded_cap_episode_batch as adapter
from . import app_server_llm_judge
from . import app_server_capacity_reserve as reserve
from .app_server_dev_selection import (
    BASELINE_REPAIRED_SYSTEM,
    DEV_MEMBERSHIP_VERSION,
    _arm_system,
    production_amortized_cost,
    run_full_judge_shards,
    score_dev_shared_reference,
)
from .app_server_interrupted_arm_recovery import probe_app_server_rate_limits
from .app_server_checkpoint import (
    validate_completed_managed_sidecar,
)
from .app_server_holdout import (
    FROZEN_WINNER_VERSION,
    load_frozen_winner,
)
from .util import now_iso, sha256_text


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEVELOPMENT_ROOT = (PROJECT_ROOT / "work" / "app-server-development-v2").resolve()
EPOCH6_ROOT = (
    DEVELOPMENT_ROOT
    / "unattended-pipeline-v5"
    / "development-selection-v249-expanded-cap-full-event-direct-reference-reserve-v6"
).resolve()
DEFAULT_EPOCH6_RECEIPT_PATH = (EPOCH6_ROOT / "plan-step-receipt.json").resolve()
DEFAULT_EPOCH6_RECEIPT_SHA256 = (
    "ac672ac79f7ac602dc2b20a44ad902881963568aaffaea7639c5aa4bf2899282"
)
DEFAULT_MANIFEST_PATH = (DEVELOPMENT_ROOT / "manifest.json").resolve()
DEFAULT_MANIFEST_SHA256 = (
    "a25d1e9189e13aa6faeba48ea97da5e5702361b741f5d2970a97e5e8af674391"
)
DEFAULT_REFERENCE_SEED_SHA256 = (
    "167bdfb40af5608b1dd23d3199d30b9d12bc7573f369038a4f642e85adaf19b0"
)
DEFAULT_JUDGE_CALIBRATION_PATH = (
    DEVELOPMENT_ROOT
    / "unattended-pipeline-v5"
    / "judge-calibration-v5_4-v174-exact-evidence-continuation"
    / "terminal.json"
).resolve()
DEFAULT_JUDGE_CALIBRATION_SHA256 = (
    "cba627ecaaf38818be56ed771f0abaf47d49fdddc6496f76a7c2101daba23c6a"
)
DEFAULT_CONTEXT_USAGE_RECOVERY_PATH = (
    PROJECT_ROOT
    / "work/windowed-acceptance-v1/paired-run-v2/evaluator-v2/"
    "context-usage-recovery-report.json"
).resolve()
DEFAULT_EXTRACTION_CAPACITY_POLICY_PATH = (
    EPOCH6_ROOT / "capacity-policy.json"
).resolve()
DEFAULT_EXTRACTION_CAPACITY_POLICY_SHA256 = (
    "a9820940e19dafd612e2563039a9fbb53b49035f68f49e1914ae82fa3f825993"
)
DEFAULT_JUDGE_CAPACITY_POLICY_PATH = (
    DEVELOPMENT_ROOT
    / "unattended-pipeline-v5"
    / "judge-calibration-v5_4-v174-exact-evidence-continuation"
    / "capacity-policy.json"
).resolve()
DEFAULT_JUDGE_CAPACITY_POLICY_SHA256 = (
    "cf89e12e2517c7dbca05c69553387e917e5b618a312af84afed0b08af42c236a"
)
DEFAULT_ADAPTER_MODULE_SHA256 = (
    "41006292a6b0e5ecd3972556bda4e76db76711c0773449523f8a63458d683bc6"
)
DEFAULT_DB_PATH = (PROJECT_ROOT / "data" / "factory.sqlite").resolve()

MATRIX_VERSION = "pif_expanded_cap_development_matrix_v1"
PRECOMMIT_VERSION = "pif_expanded_cap_development_matrix_precommit_v1"
ASSEMBLY_VERSION = "pif_expanded_cap_shared_augmented_reference_v1"
SCORE_VERSION = "pif_expanded_cap_development_matrix_score_v1"
BLOCKED_VERSION = "pif_expanded_cap_development_matrix_blocked_v1"
LIVE_CAPACITY_PREFLIGHT_VERSION = "pif_expanded_cap_matrix_capacity_preflight_v1"
LIVE_CAPACITY_CONTROL_VERSION = "pif_expanded_cap_matrix_capacity_control_v1"
CONTEXT_PREPARATION_PLAN_VERSION = (
    "pif_expanded_cap_development_context_preparation_plan_v1"
)
CONTEXT_PREPARATION_REPORT_VERSION = (
    "pif_expanded_cap_development_context_preparation_report_v1"
)
CONTEXT_PREPARATION_DELTA_SCHEMA_VERSION = (
    "pif_expanded_cap_development_context_delta_v1"
)
CONTEXT_PREPARATION_MODEL = adapter.MODEL
CONTEXT_PREPARATION_EFFORT = adapter.EFFORT
CONTEXT_PREPARATION_REQUIRED_FIELDS = (
    "section_map",
    "entity_seed",
    "concept_seed",
    "extraction_guidance",
    "excluded_source_context",
)
CONTEXT_PREPARATION_BASE_INSTRUCTIONS = (
    "You are the development-only ai_discourse_v3_1 episode-context authority gap "
    "repairer. Read only the supplied checksum-bound development episode source and "
    "existing context authority. Author only the explicitly missing fields in the "
    "structured schema. Never repeat, reinterpret, replace, or default an existing "
    "field. Do not extract final discourse events. Do not use tools, network access, "
    "or local files. Return only the requested compact JSON object."
)
EPOCH7_STEP_ID = "expanded_cap_six_arm_development_matrix_v7"
EPOCH7_PLAN_EPOCH = 7
EPOCH7_RECEIPT_VERSION = "pif_semantic_plan_step_receipt_v1"
EXPECTED_ARMS = tuple(
    (batch_size, thread_mode)
    for batch_size in (3, 5, 8)
    for thread_mode in ("new_thread", "same_thread")
)
USAGE_FIELDS = (
    "input_tokens",
    "cached_input_tokens",
    "output_tokens",
    "reasoning_output_tokens",
    "total_tokens",
)
QUALITY_THRESHOLD = 0.97
TOKEN_RATIO_THRESHOLD = 0.28


class ExpandedCapDevelopmentMatrixError(RuntimeError):
    """A frozen prerequisite, execution receipt, or selection gate failed."""


class ExpandedCapMatrixCapacityUnavailable(ExpandedCapDevelopmentMatrixError):
    """A no-thread reserve probe stopped before the next semantic turn."""


def _isolated_matrix_semantic_client_factory() -> Any:
    """Use the adapter's pinned zero-byte project-instruction app-server lane."""

    return adapter._client_factory()  # noqa: SLF001 - frozen adapter execution surface


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _pretty_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n"


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
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
        raise ExpandedCapDevelopmentMatrixError(f"{label} is not a lowercase SHA-256")
    return str(value)


def _load_object(path: Path, label: str) -> dict[str, Any]:
    resolved = Path(path).expanduser().resolve()
    if not resolved.is_file():
        raise ExpandedCapDevelopmentMatrixError(f"{label} is missing: {resolved}")
    try:
        value = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ExpandedCapDevelopmentMatrixError(f"{label} is not valid UTF-8 JSON") from exc
    if not isinstance(value, dict):
        raise ExpandedCapDevelopmentMatrixError(f"{label} is not a JSON object")
    return value


def _record(path: Path) -> dict[str, Any]:
    resolved = Path(path).expanduser().resolve()
    if not resolved.is_file():
        raise ExpandedCapDevelopmentMatrixError(f"artifact is missing: {resolved}")
    return {
        "path": str(resolved),
        "sha256": _sha256_file(resolved),
        "size_bytes": resolved.stat().st_size,
    }


def _write_new_json(path: Path, value: Any) -> dict[str, Any]:
    target = Path(path).expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        with target.open("x", encoding="utf-8") as handle:
            handle.write(_pretty_json(value))
    except FileExistsError as exc:
        raise ExpandedCapDevelopmentMatrixError(
            f"immutable matrix artifact already exists: {target}"
        ) from exc
    return _record(target)


def _write_or_verify_json(path: Path, value: Any) -> dict[str, Any]:
    """Create one immutable checkpoint, or adopt byte-independent JSON identity."""

    target = Path(path).expanduser().resolve()
    if target.exists():
        prior = _load_object(target, f"immutable {target.name}")
        if _canonical_json(prior) != _canonical_json(value):
            raise ExpandedCapDevelopmentMatrixError(
                f"immutable matrix checkpoint drifted: {target}"
            )
        return _record(target)
    return _write_new_json(target, value)


def _write_or_verify_text(path: Path, value: str) -> dict[str, Any]:
    target = Path(path).expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        if not target.is_file() or target.read_text(encoding="utf-8") != value:
            raise ExpandedCapDevelopmentMatrixError(
                f"immutable matrix text checkpoint drifted: {target}"
            )
        return _record(target)
    try:
        with target.open("x", encoding="utf-8") as handle:
            handle.write(value)
    except FileExistsError as exc:  # pragma: no cover - competing writer defense
        raise ExpandedCapDevelopmentMatrixError(
            f"immutable matrix text artifact already exists: {target}"
        ) from exc
    return _record(target)


def _verify_exact_file_hash(path: Path, expected_sha256: str, label: str) -> dict[str, Any]:
    target = Path(path).expanduser().resolve()
    expected = _require_sha256(expected_sha256, f"{label} expected SHA-256")
    if not target.is_file() or _sha256_file(target) != expected:
        raise ExpandedCapDevelopmentMatrixError(f"{label} checksum binding failed")
    return _record(target)


def _reject_external_auth_material(environ: Mapping[str, str] | None = None) -> None:
    """Prohibit API-key/session-token paths; app-server account auth is the only lane."""

    values = os.environ if environ is None else environ
    forbidden = (
        "OPENAI_API_KEY",
        "CODEX_API_KEY",
        "OPENAI_ACCESS_TOKEN",
        "CHATGPT_ACCESS_TOKEN",
        "OPENAI_SESSION_TOKEN",
    )
    present = sorted(name for name in forbidden if values.get(name))
    if present:
        raise ExpandedCapDevelopmentMatrixError(
            "epoch-7 managed-auth execution rejects external auth material: "
            + ", ".join(present)
        )


def _inside(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


def _verify_record_tree(
    value: Any,
    *,
    label: str,
    allowed_root: Path,
) -> int:
    if not isinstance(value, Mapping) or not value:
        raise ExpandedCapDevelopmentMatrixError(f"{label} record tree is empty")
    if set(value) == {"path", "sha256", "size_bytes"}:
        path_value = value.get("path")
        size_value = value.get("size_bytes")
        if not isinstance(path_value, str) or not path_value:
            raise ExpandedCapDevelopmentMatrixError(f"{label}.path is missing")
        path = Path(path_value).expanduser().resolve()
        if not _inside(path, allowed_root):
            raise ExpandedCapDevelopmentMatrixError(f"{label} escapes its allowed root")
        if (
            not path.is_file()
            or not isinstance(size_value, int)
            or isinstance(size_value, bool)
            or size_value < 0
            or path.stat().st_size != size_value
            or _sha256_file(path) != _require_sha256(value.get("sha256"), f"{label}.sha256")
        ):
            raise ExpandedCapDevelopmentMatrixError(f"{label} checksum or size drifted")
        return 1
    if any(key in value for key in ("path", "sha256", "size_bytes")):
        raise ExpandedCapDevelopmentMatrixError(f"{label} has a partial artifact record")
    return sum(
        _verify_record_tree(child, label=f"{label}.{key}", allowed_root=allowed_root)
        for key, child in sorted(value.items())
    )


def _valid_usage(value: Any) -> bool:
    if not isinstance(value, Mapping) or set(value) != set(USAGE_FIELDS):
        return False
    if any(
        isinstance(value.get(field), bool)
        or not isinstance(value.get(field), int)
        or int(value[field]) < 0
        for field in USAGE_FIELDS
    ):
        return False
    return bool(
        value["cached_input_tokens"] <= value["input_tokens"]
        and value["reasoning_output_tokens"] <= value["output_tokens"]
        and value["total_tokens"] == value["input_tokens"] + value["output_tokens"]
    )


def _sum_usage(values: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    total = {field: 0 for field in USAGE_FIELDS}
    for value in values:
        if not _valid_usage(value):
            raise ExpandedCapDevelopmentMatrixError("usage accounting is incomplete")
        for field in USAGE_FIELDS:
            total[field] += int(value[field])
    return total


def _nonnegative_number(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        raise ExpandedCapDevelopmentMatrixError(f"{label} is not measured")
    return float(value)


async def _maybe_await(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


def verify_epoch6_passed_receipt(
    path: Path = DEFAULT_EPOCH6_RECEIPT_PATH,
    *,
    expected_sha256: str = DEFAULT_EPOCH6_RECEIPT_SHA256,
    allowed_record_root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    """Checksum-bind the immutable epoch-6 receipt and re-prove its passed state."""

    receipt_path = Path(path).expanduser().resolve()
    expected = _require_sha256(expected_sha256, "epoch-6 receipt expected SHA-256")
    if not receipt_path.is_file() or _sha256_file(receipt_path) != expected:
        raise ExpandedCapDevelopmentMatrixError("epoch-6 receipt checksum binding failed")
    payload = _load_object(receipt_path, "epoch-6 plan-step receipt")
    required = {
        "schema_version",
        "plan_epoch",
        "step_id",
        "state",
        "terminal_reason",
        "development_quality_passed",
        "candidate_strict_full_field_macro_f1",
        "exact_evidence_rate",
        "accounting_complete",
        "usage_status",
        "usage",
        "semantic_attempt_count",
        "semantic_model_call_count",
        "semantic_model_call_cap",
        "semantic_retry_count",
        "semantic_total_token_cap",
        "strict_thread_turn_call_accounting",
        "failed_checks",
        "winner_frozen",
        "development_winner_frozen",
        "holdout",
        "holdout_authorized",
        "production",
        "production_mutated",
        "predecessor_epoch5_mutated",
        "records",
    }
    usage = payload.get("usage")
    if (
        not required.issubset(payload)
        or payload.get("schema_version") != "pif_semantic_plan_step_receipt_v1"
        or payload.get("plan_epoch") != 6
        or payload.get("step_id") != "expanded_cap_full_event_direct_reference_reserve_v6"
        or payload.get("state") != "passed"
        or payload.get("terminal_reason")
        != "epoch6_reserve_aware_direct_reference_quality_passed"
        or payload.get("development_quality_passed") is not True
        or not isinstance(payload.get("candidate_strict_full_field_macro_f1"), (int, float))
        or float(payload["candidate_strict_full_field_macro_f1"]) < QUALITY_THRESHOLD
        or payload.get("exact_evidence_rate") != 1.0
        or payload.get("accounting_complete") is not True
        or payload.get("usage_status") != "complete"
        or not _valid_usage(usage)
        or payload.get("semantic_attempt_count") != 1
        or not isinstance(payload.get("semantic_model_call_count"), int)
        or payload.get("semantic_model_call_count") < 1
        or payload.get("semantic_model_call_count") > payload.get("semantic_model_call_cap", -1)
        or payload.get("semantic_retry_count") != 0
        or payload.get("semantic_total_token_cap", -1) < usage["total_tokens"]
        or payload.get("strict_thread_turn_call_accounting") is not True
        or payload.get("failed_checks") != []
        or payload.get("winner_frozen") is not False
        or payload.get("development_winner_frozen") is not False
        or payload.get("holdout") is not False
        or payload.get("holdout_authorized") is not False
        or payload.get("production") is not False
        or payload.get("production_mutated") is not False
        or payload.get("predecessor_epoch5_mutated") is not False
    ):
        raise ExpandedCapDevelopmentMatrixError("epoch-6 receipt is not the immutable passed gate")
    record_count = _verify_record_tree(
        payload["records"],
        label="epoch6.records",
        allowed_root=Path(allowed_record_root).expanduser().resolve(),
    )
    if record_count < 10:
        raise ExpandedCapDevelopmentMatrixError("epoch-6 receipt record closure is incomplete")
    return {
        "path": str(receipt_path),
        "sha256": expected,
        "payload": payload,
        "verified_record_count": record_count,
    }


def verify_judge_calibration_receipt(
    path: Path = DEFAULT_JUDGE_CALIBRATION_PATH,
    *,
    expected_sha256: str = DEFAULT_JUDGE_CALIBRATION_SHA256,
    allowed_record_root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    """Bind the frozen v5.4 calibration that authorizes development judging."""

    calibration_path = Path(path).expanduser().resolve()
    expected = _require_sha256(expected_sha256, "judge calibration expected SHA-256")
    if not calibration_path.is_file() or _sha256_file(calibration_path) != expected:
        raise ExpandedCapDevelopmentMatrixError("judge calibration checksum binding failed")
    payload = _load_object(calibration_path, "judge calibration terminal")
    metrics = payload.get("metrics")
    if (
        payload.get("schema_version") != "pif_app_server_judge_v5_4_v174_terminal_v1"
        or payload.get("state") != "completed"
        or payload.get("terminal_reason")
        != "v174_full_development_calibration_passed_selection_authorized"
        or payload.get("development_judge_frozen") is not True
        or payload.get("selection_authorized") is not True
        or payload.get("failed_quality_gates") != []
        or payload.get("accounting_complete") is not True
        or payload.get("usage_status") != "complete"
        or not _valid_usage(payload.get("usage"))
        or payload.get("semantic_retry_count") != 0
        or payload.get("production_mutated") is not False
        or payload.get("holdout_authorized") is not False
        or not isinstance(metrics, Mapping)
        or metrics.get("order_bias") != 0.0
        or metrics.get("support_abstention_count") != 0
        or metrics.get("alignment_abstention_case_count") != 0
    ):
        raise ExpandedCapDevelopmentMatrixError("judge calibration is not selection-authorized")
    record_count = 0
    for key in ("final_fields", "protocol", "reference", "score", "truth"):
        record_count += _verify_record_tree(
            payload.get(key),
            label=f"calibration.{key}",
            allowed_root=Path(allowed_record_root).expanduser().resolve(),
        )
    return {
        "path": str(calibration_path),
        "sha256": expected,
        "payload": payload,
        "verified_record_count": record_count,
    }


def _manifest_rows(manifest: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen_episode_ids: set[str] = set()
    seen_source_ids: set[str] = set()
    seen_segment_ids: set[str] = set()
    seen_text_hashes: set[str] = set()
    seen_episode_indices: set[tuple[str, int]] = set()
    episodes = manifest.get("episodes")
    if not isinstance(episodes, list) or len(episodes) != 4:
        raise ExpandedCapDevelopmentMatrixError("development manifest must have four episodes")
    for episode in episodes:
        if not isinstance(episode, Mapping):
            raise ExpandedCapDevelopmentMatrixError("development manifest episode is malformed")
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
            or len(segments) != 8
        ):
            raise ExpandedCapDevelopmentMatrixError(
                "development manifest episode/source allocation drifted"
            )
        seen_episode_ids.add(episode_id)
        seen_source_ids.add(source_id)
        for segment in segments:
            if not isinstance(segment, Mapping):
                raise ExpandedCapDevelopmentMatrixError("development manifest segment is malformed")
            segment_id = segment.get("segment_id")
            index = segment.get("segment_index")
            text_hash = segment.get("text_sha256")
            density = segment.get("density_stratum")
            golden_count = segment.get("golden_event_count")
            key = (episode_id, index) if isinstance(index, int) else None
            if (
                not isinstance(segment_id, str)
                or not segment_id
                or segment_id in seen_segment_ids
                or isinstance(index, bool)
                or not isinstance(index, int)
                or index < 0
                or key in seen_episode_indices
                or not _valid_sha256(text_hash)
                or text_hash in seen_text_hashes
                or density not in {"dense", "no_signal"}
                or isinstance(golden_count, bool)
                or not isinstance(golden_count, int)
                or golden_count < 0
            ):
                raise ExpandedCapDevelopmentMatrixError(
                    "development manifest case identity or stratum drifted"
                )
            if (density == "no_signal") != (golden_count == 0):
                raise ExpandedCapDevelopmentMatrixError(
                    "development manifest no-signal truth is inconsistent"
                )
            seen_segment_ids.add(segment_id)
            seen_text_hashes.add(str(text_hash))
            seen_episode_indices.add((episode_id, index))
            rows.append(
                {
                    **copy.deepcopy(dict(segment)),
                    "episode_id": episode_id,
                    "episode_title": episode.get("episode_title"),
                    "source_id": source_id,
                    "source_name": episode.get("source_name"),
                }
            )
    if len(rows) != 32 or len(seen_source_ids) != 4:
        raise ExpandedCapDevelopmentMatrixError("development manifest is not the frozen 32-case set")
    return rows


def verify_development_manifest(
    path: Path = DEFAULT_MANIFEST_PATH,
    *,
    expected_sha256: str = DEFAULT_MANIFEST_SHA256,
) -> dict[str, Any]:
    """Read and hash-verify the immutable 32-case development manifest and seed."""

    manifest_path = Path(path).expanduser().resolve()
    expected = _require_sha256(expected_sha256, "development manifest expected SHA-256")
    if not manifest_path.is_file() or _sha256_file(manifest_path) != expected:
        raise ExpandedCapDevelopmentMatrixError("development manifest checksum binding failed")
    manifest = _load_object(manifest_path, "development manifest")
    rows = _manifest_rows(manifest)
    densities = Counter(str(row["density_stratum"]) for row in rows)
    if (
        manifest.get("schema_version") != evaluation.APP_SERVER_DEVELOPMENT_MANIFEST_V2
        or manifest.get("evaluation_role")
        != "retrospective_balanced_development_not_acceptance"
        or manifest.get("episode_count") != 4
        or manifest.get("source_count") != 4
        or manifest.get("segment_count") != 32
        or manifest.get("unique_episode_segment_index_count") != 32
        or manifest.get("unique_text_sha256_count") != 32
        or manifest.get("density_counts") != dict(densities)
        or densities != Counter({"dense": 28, "no_signal": 4})
        or not isinstance(manifest.get("selection_policy"), str)
        or "no_transcript_semantic_rules" not in manifest["selection_policy"]
        or isinstance(manifest.get("event_cap"), bool)
        or not isinstance(manifest.get("event_cap"), int)
        or manifest["event_cap"] < max(int(row["golden_event_count"]) for row in rows)
    ):
        raise ExpandedCapDevelopmentMatrixError("development manifest frozen invariants failed")

    seed_record = manifest.get("shared_reference_seed")
    if not isinstance(seed_record, Mapping):
        raise ExpandedCapDevelopmentMatrixError("development manifest has no shared reference seed")
    seed_path_value = seed_record.get("artifact_path")
    if not isinstance(seed_path_value, str) or not seed_path_value:
        raise ExpandedCapDevelopmentMatrixError("shared reference seed path is missing")
    seed_path = Path(seed_path_value).expanduser().resolve()
    if not _inside(seed_path, manifest_path.parent):
        raise ExpandedCapDevelopmentMatrixError("shared reference seed escapes manifest root")
    seed_sha = _require_sha256(seed_record.get("artifact_sha256"), "reference seed SHA-256")
    if (
        not seed_path.is_file()
        or _sha256_file(seed_path) != seed_sha
        or seed_record.get("schema_version") != "pif_shared_reference_seed_v1"
        or seed_record.get("reference_count") != 32
    ):
        raise ExpandedCapDevelopmentMatrixError("shared reference seed checksum binding failed")
    if manifest_path == DEFAULT_MANIFEST_PATH and seed_sha != DEFAULT_REFERENCE_SEED_SHA256:
        raise ExpandedCapDevelopmentMatrixError("default shared reference seed hash drifted")
    seed = _load_object(seed_path, "shared reference seed")
    references = seed.get("references")
    if (
        seed.get("schema_version") != "pif_shared_reference_seed_v1"
        or not isinstance(references, list)
        or len(references) != 32
    ):
        raise ExpandedCapDevelopmentMatrixError("shared reference seed is incomplete")
    row_by_id = {str(row["segment_id"]): row for row in rows}
    reference_by_id: dict[str, dict[str, Any]] = {}
    ordered_reference_ids: list[str] = []
    for reference in references:
        if not isinstance(reference, Mapping):
            raise ExpandedCapDevelopmentMatrixError("shared reference row is malformed")
        segment_id = reference.get("segment_id")
        if not isinstance(segment_id, str) or segment_id in reference_by_id or segment_id not in row_by_id:
            raise ExpandedCapDevelopmentMatrixError("shared reference IDs do not partition manifest")
        spec = row_by_id[segment_id]
        golden = reference.get("golden_output")
        events = golden.get("discourse_events") if isinstance(golden, Mapping) else None
        if (
            reference.get("episode_id") != spec["episode_id"]
            or reference.get("text_sha256") != spec["text_sha256"]
            or reference.get("golden_output_sha256") != spec["golden_output_sha256"]
            or reference.get("label_id") != spec["label_id"]
            or reference.get("label_run_id") != spec["label_run_id"]
            or not isinstance(events, list)
            or any(not isinstance(event, Mapping) for event in events)
            or len(events) != int(spec["golden_event_count"])
            or golden.get("segment_id") != segment_id
            or golden.get("episode_id") != spec["episode_id"]
        ):
            raise ExpandedCapDevelopmentMatrixError("shared reference provenance drifted")
        reference_by_id[segment_id] = copy.deepcopy(dict(reference))
        ordered_reference_ids.append(segment_id)
    case_order = [str(row["segment_id"]) for row in rows]
    if ordered_reference_ids != case_order:
        raise ExpandedCapDevelopmentMatrixError("shared reference case order drifted")
    return {
        "path": str(manifest_path),
        "sha256": expected,
        "payload": manifest,
        "rows": rows,
        "case_order": case_order,
        "reference_path": str(seed_path),
        "reference_sha256": seed_sha,
        "reference": seed,
        "reference_by_segment": reference_by_id,
    }


def prepare_development_episodes(
    conn: Any,
    *,
    manifest: Mapping[str, Any],
    manifest_rows: Sequence[Mapping[str, Any]],
    window_count: int = 4,
    context_chars: int = 900,
    episode_loader: Callable[..., Any] = evaluation._load_prepared_episodes,
) -> list[dict[str, Any]]:
    """Prepare source text and boundaries through app_server_evaluation logic."""

    if (
        isinstance(window_count, bool)
        or not isinstance(window_count, int)
        or window_count < 1
        or isinstance(context_chars, bool)
        or not isinstance(context_chars, int)
        or context_chars < 0
    ):
        raise ExpandedCapDevelopmentMatrixError("window preparation parameters are invalid")
    if episode_loader is evaluation._load_prepared_episodes and conn is None:
        raise ExpandedCapDevelopmentMatrixError("live episode preparation requires a read-only DB connection")

    prior_query_only: int | None = None
    if episode_loader is evaluation._load_prepared_episodes:
        try:
            prior_query_only = int(conn.execute("PRAGMA query_only").fetchone()[0])
            if not prior_query_only:
                conn.execute("PRAGMA query_only = ON")
        except Exception as exc:  # pragma: no cover - DB driver contract
            raise ExpandedCapDevelopmentMatrixError("cannot enforce read-only episode preparation") from exc
    try:
        episodes = episode_loader(
            conn,
            manifest=copy.deepcopy(dict(manifest)),
            window_count=window_count,
            context_chars=context_chars,
        )
    finally:
        if episode_loader is evaluation._load_prepared_episodes and prior_query_only == 0:
            conn.execute("PRAGMA query_only = OFF")
    if not isinstance(episodes, list) or len(episodes) != 4:
        raise ExpandedCapDevelopmentMatrixError("episode preparation did not return four episodes")
    expected_episode_order: list[str] = []
    expected_by_episode: dict[str, list[Mapping[str, Any]]] = {}
    for row in manifest_rows:
        episode_id = str(row["episode_id"])
        if episode_id not in expected_by_episode:
            expected_episode_order.append(episode_id)
            expected_by_episode[episode_id] = []
        expected_by_episode[episode_id].append(row)
    observed_episode_order = [episode.get("episode_id") for episode in episodes if isinstance(episode, Mapping)]
    if observed_episode_order != expected_episode_order:
        raise ExpandedCapDevelopmentMatrixError("prepared episode order differs from manifest")
    for episode in episodes:
        episode_id = str(episode["episode_id"])
        segments = episode.get("segments")
        expected_segments = expected_by_episode[episode_id]
        if not isinstance(segments, list) or [row.get("segment_id") for row in segments] != [
            row["segment_id"] for row in expected_segments
        ]:
            raise ExpandedCapDevelopmentMatrixError("prepared segment order differs from manifest")
        for prepared, spec in zip(segments, expected_segments):
            text = prepared.get("segment_text") if isinstance(prepared, Mapping) else None
            if (
                not isinstance(text, str)
                or not text
                or sha256_text(text) != spec["text_sha256"]
                or prepared.get("density_stratum") != spec["density_stratum"]
                or not isinstance(prepared.get("boundaries"), list)
                or not prepared["boundaries"]
            ):
                raise ExpandedCapDevelopmentMatrixError("prepared source hash or boundaries drifted")
    return copy.deepcopy(episodes)


def _context_field_valid(field: str, value: Any) -> bool:
    if field == "section_map":
        return isinstance(value, list) and all(isinstance(item, Mapping) for item in value)
    if field == "entity_seed":
        return isinstance(value, Mapping)
    if field in {"concept_seed", "excluded_source_context"}:
        return isinstance(value, list) and all(isinstance(item, str) for item in value)
    if field == "extraction_guidance":
        return isinstance(value, str)
    raise ExpandedCapDevelopmentMatrixError(f"unknown context authority field: {field}")


def _context_delta_schema(
    *, episode_id: str, original_context_sha256: str, missing_fields: Sequence[str]
) -> dict[str, Any]:
    missing = [str(field) for field in missing_fields]
    if (
        not episode_id
        or not _valid_sha256(original_context_sha256)
        or not missing
        or len(missing) != len(set(missing))
        or any(field not in CONTEXT_PREPARATION_REQUIRED_FIELDS for field in missing)
    ):
        raise ExpandedCapDevelopmentMatrixError("context delta schema identity is invalid")
    field_schemas: dict[str, dict[str, Any]] = {
        "section_map": {"type": "array", "items": {"type": "object"}},
        "entity_seed": {"type": "object"},
        "concept_seed": {"type": "array", "items": {"type": "string"}},
        "extraction_guidance": {"type": "string"},
        "excluded_source_context": {
            "type": "array",
            "items": {"type": "string"},
        },
    }
    return {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "schema_version",
            "episode_id",
            "original_context_sha256",
            *missing,
        ],
        "properties": {
            "schema_version": {
                "type": "string",
                "const": CONTEXT_PREPARATION_DELTA_SCHEMA_VERSION,
            },
            "episode_id": {"type": "string", "const": episode_id},
            "original_context_sha256": {
                "type": "string",
                "const": original_context_sha256,
            },
            **{field: copy.deepcopy(field_schemas[field]) for field in missing},
        },
    }


def _context_artifact_binding(
    *,
    prepared_episode: Mapping[str, Any],
    manifest_episode: Mapping[str, Any],
    require_manifest_artifact: bool,
) -> dict[str, Any]:
    context = prepared_episode.get("episode_context")
    if not isinstance(context, Mapping):
        raise ExpandedCapDevelopmentMatrixError("prepared episode context is missing")
    payload = copy.deepcopy(dict(context))
    episode_id = str(prepared_episode.get("episode_id") or "")
    if payload.get("episode_id", episode_id) != episode_id:
        raise ExpandedCapDevelopmentMatrixError("prepared episode context identity drifted")
    payload_bytes = _canonical_json(payload).encode("utf-8")
    payload_sha = hashlib.sha256(payload_bytes).hexdigest()
    raw_record = manifest_episode.get("episode_context")
    if isinstance(raw_record, Mapping):
        path_value = raw_record.get("artifact_path")
        expected_sha = raw_record.get("artifact_sha256")
        if not isinstance(path_value, str) or not path_value or not _valid_sha256(expected_sha):
            raise ExpandedCapDevelopmentMatrixError("development context manifest record is invalid")
        path = Path(path_value).expanduser().resolve()
        if not path.is_file() or _sha256_file(path) != expected_sha:
            raise ExpandedCapDevelopmentMatrixError(
                "development context artifact checksum binding failed"
            )
        artifact = _load_object(path, "development episode context artifact")
        if _canonical_json(artifact) != _canonical_json(payload):
            raise ExpandedCapDevelopmentMatrixError(
                "prepared context differs from its manifest-bound artifact"
            )
        return {
            "binding_kind": "manifest_artifact",
            "run_id": raw_record.get("run_id"),
            "transcript_id": raw_record.get("transcript_id"),
            "artifact": _record(path),
            "payload_sha256": payload_sha,
            "payload_size_bytes": len(payload_bytes),
        }
    if require_manifest_artifact:
        raise ExpandedCapDevelopmentMatrixError(
            "live development context has no manifest artifact binding"
        )
    return {
        "binding_kind": "custom_episode_loader_inline_fixture",
        "artifact": None,
        "payload_sha256": payload_sha,
        "payload_size_bytes": len(payload_bytes),
    }


def inspect_development_context_preparation(
    *,
    episodes: Sequence[Mapping[str, Any]],
    manifest_info: Mapping[str, Any],
    require_manifest_artifacts: bool,
) -> dict[str, Any]:
    """Hash-bind existing context authority and plan only genuinely absent fields."""

    manifest = manifest_info.get("payload")
    manifest_episodes = manifest.get("episodes") if isinstance(manifest, Mapping) else None
    if not isinstance(manifest_episodes, list) or len(manifest_episodes) != len(episodes):
        raise ExpandedCapDevelopmentMatrixError("context preparation manifest episodes drifted")
    manifest_by_id = {
        str(item.get("episode_id")): item
        for item in manifest_episodes
        if isinstance(item, Mapping)
    }
    rows: list[dict[str, Any]] = []
    for prepared in episodes:
        if not isinstance(prepared, Mapping):
            raise ExpandedCapDevelopmentMatrixError("prepared context episode is malformed")
        episode_id = str(prepared.get("episode_id") or "")
        manifest_episode = manifest_by_id.get(episode_id)
        if not episode_id or not isinstance(manifest_episode, Mapping):
            raise ExpandedCapDevelopmentMatrixError("context preparation episode identity drifted")
        context = prepared.get("episode_context")
        if not isinstance(context, Mapping):
            raise ExpandedCapDevelopmentMatrixError("prepared episode context is missing")
        binding = _context_artifact_binding(
            prepared_episode=prepared,
            manifest_episode=manifest_episode,
            require_manifest_artifact=require_manifest_artifacts,
        )
        missing: list[str] = []
        preserved_hashes: dict[str, str] = {}
        for field in CONTEXT_PREPARATION_REQUIRED_FIELDS:
            if field not in context:
                missing.append(field)
                continue
            if not _context_field_valid(field, context[field]):
                raise ExpandedCapDevelopmentMatrixError(
                    f"existing LLM-authored context field is invalid and cannot be replaced: "
                    f"{episode_id}.{field}"
                )
            preserved_hashes[field] = sha256_text(_canonical_json(context[field]))
        for field, validator in (
            ("context_summary", lambda value: isinstance(value, str)),
            (
                "speaker_map",
                lambda value: isinstance(value, list)
                and all(isinstance(item, Mapping) for item in value),
            ),
        ):
            if field not in context or not validator(context[field]):
                raise ExpandedCapDevelopmentMatrixError(
                    f"existing non-delta context authority is invalid: {episode_id}.{field}"
                )
            preserved_hashes[field] = sha256_text(_canonical_json(context[field]))
        schema = (
            _context_delta_schema(
                episode_id=episode_id,
                original_context_sha256=str(binding["payload_sha256"]),
                missing_fields=missing,
            )
            if missing
            else None
        )
        canonical_transcript = manifest_episode.get("canonical_transcript")
        if require_manifest_artifacts and not isinstance(canonical_transcript, Mapping):
            raise ExpandedCapDevelopmentMatrixError(
                "live context preparation has no canonical transcript binding"
            )
        rows.append(
            {
                "episode_id": episode_id,
                "source_id": manifest_episode.get("source_id"),
                "source_name": manifest_episode.get("source_name"),
                "episode_title": manifest_episode.get("episode_title"),
                "original_context": binding,
                "original_context_payload_sha256": binding["payload_sha256"],
                "preserved_field_sha256s": dict(sorted(preserved_hashes.items())),
                "missing_fields": missing,
                "semantic_turn_required": bool(missing),
                "output_schema_sha256": (
                    hashlib.sha256(_pretty_json(schema).encode("utf-8")).hexdigest()
                    if schema is not None
                    else None
                ),
                "canonical_transcript": copy.deepcopy(dict(canonical_transcript))
                if isinstance(canonical_transcript, Mapping)
                else None,
                "selected_segment_ids": [
                    str(segment.get("segment_id"))
                    for segment in prepared.get("segments") or []
                    if isinstance(segment, Mapping)
                ],
                "selected_segment_text_sha256s": [
                    sha256_text(str(segment.get("segment_text")))
                    for segment in prepared.get("segments") or []
                    if isinstance(segment, Mapping)
                ],
            }
        )
    missing_rows = [row for row in rows if row["semantic_turn_required"]]
    ambient_isolation = _zero_byte_ambient_instruction_isolation(
        adapter.build_frozen_configuration(batch_size=3, thread_mode="new_thread")
    )
    return {
        "schema_version": CONTEXT_PREPARATION_PLAN_VERSION,
        "state": (
            "context_preparation_required"
            if missing_rows
            else "context_preparation_not_required"
        ),
        "development_only": True,
        "manifest_sha256": manifest_info.get("sha256"),
        "model": CONTEXT_PREPARATION_MODEL,
        "reasoning_effort": CONTEXT_PREPARATION_EFFORT,
        "thread_mode": "new_thread",
        "batch_size": 1,
        "concurrency": 1,
        "retry_count": 0,
        "required_llm_authored_fields": list(CONTEXT_PREPARATION_REQUIRED_FIELDS),
        "episode_count": len(rows),
        "semantic_turn_count": len(missing_rows),
        "episode_ids_requiring_semantic_turn": [row["episode_id"] for row in missing_rows],
        "episodes": rows,
        "preservation_policy": (
            "preserve_every_existing_context_key_and_value_exactly; author_only_schema-enumerated_"
            "fields_that_are_absent_from_the_checksum-bound_original_artifact"
        ),
        "semantic_defaults_allowed": False,
        "official_managed_app_server_only": True,
        "ambient_instruction_isolation": ambient_isolation,
        "production_mutated": False,
        "holdout_inspected": False,
    }


def _project_artifact_path(value: Any, *, label: str) -> Path:
    if not isinstance(value, str) or not value:
        raise ExpandedCapDevelopmentMatrixError(f"{label} path is missing")
    candidate = Path(value).expanduser()
    return candidate.resolve() if candidate.is_absolute() else (PROJECT_ROOT / candidate).resolve()


def _verified_text_artifact(value: Any, expected_sha256: Any, *, label: str) -> tuple[Path, str]:
    path = _project_artifact_path(value, label=label)
    expected = _require_sha256(expected_sha256, f"{label} SHA-256")
    if not path.is_file() or _sha256_file(path) != expected:
        raise ExpandedCapDevelopmentMatrixError(f"{label} checksum binding failed")
    try:
        return path, path.read_text(encoding="utf-8")
    except UnicodeDecodeError as exc:
        raise ExpandedCapDevelopmentMatrixError(f"{label} is not UTF-8") from exc


def _full_development_context_source(
    conn: Any, *, manifest_episode: Mapping[str, Any]
) -> dict[str, Any]:
    """Load only the manifest-bound canonical development transcript, read-only."""

    if conn is None:
        raise ExpandedCapDevelopmentMatrixError(
            "live context preparation requires the read-only factory DB"
        )
    episode_id = str(manifest_episode.get("episode_id") or "")
    source_id = str(manifest_episode.get("source_id") or "")
    canonical = manifest_episode.get("canonical_transcript")
    if not episode_id or not source_id or not isinstance(canonical, Mapping):
        raise ExpandedCapDevelopmentMatrixError("canonical development transcript record is missing")
    transcript_id = str(canonical.get("transcript_id") or "")
    preparation_id = str(canonical.get("preparation_id") or "")
    if not transcript_id or not preparation_id:
        raise ExpandedCapDevelopmentMatrixError("canonical transcript identity is incomplete")
    row = conn.execute(
        """
        SELECT t.id, t.episode_id, t.raw_text_path, t.raw_text_sha256,
               e.title, e.published_at, e.source_id, s.name AS source_name,
               tp.id AS preparation_id, tp.status AS preparation_status,
               tp.cleaned_text_path, tp.cleaned_text_sha256,
               tp.artifact_type, tp.substantive_word_count, tp.boilerplate_ratio,
               tp.speaker_turn_count, tp.quality_score
        FROM transcripts t
        JOIN episodes e ON e.id = t.episode_id
        JOIN sources s ON s.id = e.source_id
        JOIN transcript_preparations tp ON tp.transcript_id = t.id
        WHERE t.id = ? AND t.episode_id = ? AND tp.id = ?
          AND t.status = 'ready' AND tp.status = 'prepared'
        """,
        (transcript_id, episode_id, preparation_id),
    ).fetchone()
    if not row:
        raise ExpandedCapDevelopmentMatrixError(
            f"canonical development transcript is no longer ready: {episode_id}"
        )
    if (
        str(row["source_id"]) != source_id
        or str(row["raw_text_sha256"]) != str(canonical.get("raw_text_sha256"))
        or str(row["cleaned_text_sha256"])
        != str(canonical.get("prepared_text_sha256"))
    ):
        raise ExpandedCapDevelopmentMatrixError("canonical development transcript DB binding drifted")
    raw_path, _raw_text = _verified_text_artifact(
        row["raw_text_path"], row["raw_text_sha256"], label=f"raw transcript {episode_id}"
    )
    del _raw_text
    prepared_path, prepared_text = _verified_text_artifact(
        row["cleaned_text_path"],
        row["cleaned_text_sha256"],
        label=f"prepared transcript {episode_id}",
    )
    segment_rows = conn.execute(
        """
        SELECT id, segment_index, start_char, end_char, word_count, text_path, text_sha256
        FROM segments
        WHERE transcript_id = ? AND episode_id = ?
        ORDER BY segment_index, id
        """,
        (transcript_id, episode_id),
    ).fetchall()
    if not segment_rows:
        raise ExpandedCapDevelopmentMatrixError(
            f"canonical development transcript has no segments: {episode_id}"
        )
    parts: list[str] = []
    segment_records: list[dict[str, Any]] = []
    total_words = 0
    exact_prepared_text_projection_count = 0
    within_prepared_text_bounds_count = 0
    overlapping_segment_count = 0
    prior_start = -1
    prior_end = 0
    for segment in segment_rows:
        segment_path, text = _verified_text_artifact(
            segment["text_path"],
            segment["text_sha256"],
            label=f"canonical development segment {segment['id']}",
        )
        start = int(segment["start_char"])
        end = int(segment["end_char"])
        if start < prior_start or start < 0 or end < start or end - start != len(text):
            raise ExpandedCapDevelopmentMatrixError(
                f"canonical development segment offset drifted: {segment['id']}"
            )
        overlapping_segment_count += int(start < prior_end)
        prior_start = start
        prior_end = end
        within_prepared_bounds = end <= len(prepared_text)
        within_prepared_text_bounds_count += int(within_prepared_bounds)
        exact_projection = within_prepared_bounds and prepared_text[start:end] == text
        exact_prepared_text_projection_count += int(exact_projection)
        words = int(segment["word_count"] or len(text.split()))
        total_words += words
        parts.append(
            "\n".join(
                (
                    f"===== SEGMENT {segment['segment_index']} | {segment['id']} | "
                    f"chars={start}-{end} | words={words} =====",
                    text,
                )
            )
        )
        segment_records.append(
            {
                "segment_id": str(segment["id"]),
                "segment_index": int(segment["segment_index"]),
                "text_sha256": str(segment["text_sha256"]),
                "within_prepared_text_bounds": within_prepared_bounds,
                "prepared_text_projection_exact": exact_projection,
                "artifact": _record(segment_path),
            }
        )
    full_text = "\n\n".join(parts)
    return {
        "episode": {
            "episode_id": episode_id,
            "transcript_id": transcript_id,
            "source_id": source_id,
            "source_name": row["source_name"],
            "episode_title": row["title"],
            "episode_published_at": row["published_at"],
            "transcript_preparation_id": preparation_id,
            "transcript_artifact_type": row["artifact_type"],
            "transcript_preparation_status": row["preparation_status"],
            "transcript_substantive_word_count": row["substantive_word_count"],
            "transcript_boilerplate_ratio": row["boilerplate_ratio"],
            "transcript_speaker_turn_count": row["speaker_turn_count"],
            "transcript_quality_score": row["quality_score"],
            "segment_count": len(segment_rows),
            "total_segment_words": total_words,
            "privacy_boundary": "private_analysis_only_do_not_output_full_transcript",
        },
        "full_segmented_episode_text": full_text,
        "source_binding": {
            "canonical_transcript": copy.deepcopy(dict(canonical)),
            "raw_transcript": _record(raw_path),
            "prepared_transcript": _record(prepared_path),
            "full_segmented_episode_text_sha256": sha256_text(full_text),
            "full_segmented_episode_text_chars": len(full_text),
            "source_assembly_policy": (
                "all_manifest-canonical_transcript_segment_artifacts_hash-verified_in_DB_order; "
                "prepared_transcript_hash-verified; segment offsets require monotonicity and equal length; "
                "recorded overlap is preserved; whitespace-normalized segment artifacts remain "
                "the authoritative prompt text"
            ),
            "overlapping_segment_count": overlapping_segment_count,
            "within_prepared_text_bounds_count": within_prepared_text_bounds_count,
            "exact_prepared_text_projection_count": exact_prepared_text_projection_count,
            "normalized_projection_count": len(segment_rows)
            - exact_prepared_text_projection_count,
            "segments": segment_records,
        },
    }


def _fixture_development_context_source(episode: Mapping[str, Any]) -> dict[str, Any]:
    parts: list[str] = []
    segment_records: list[dict[str, Any]] = []
    for index, segment in enumerate(episode.get("segments") or []):
        if not isinstance(segment, Mapping):
            raise ExpandedCapDevelopmentMatrixError("fixture context segment is malformed")
        text = segment.get("segment_text")
        if not isinstance(text, str) or not text:
            raise ExpandedCapDevelopmentMatrixError("fixture context segment text is missing")
        segment_id = str(segment.get("segment_id") or "")
        parts.append(f"===== FIXTURE SEGMENT {index} | {segment_id} =====\n{text}")
        segment_records.append(
            {
                "segment_id": segment_id,
                "segment_index": index,
                "text_sha256": sha256_text(text),
            }
        )
    full_text = "\n\n".join(parts)
    return {
        "episode": {
            "episode_id": episode.get("episode_id"),
            "source_name": episode.get("source_name"),
            "episode_title": episode.get("episode_title"),
            "segment_count": len(segment_records),
            "privacy_boundary": "fixture_only",
        },
        "full_segmented_episode_text": full_text,
        "source_binding": {
            "binding_kind": "fixture_selected_segments_only",
            "full_segmented_episode_text_sha256": sha256_text(full_text),
            "full_segmented_episode_text_chars": len(full_text),
            "segments": segment_records,
        },
    }


def _context_delta_prompt(
    *,
    episode_id: str,
    original_context: Mapping[str, Any],
    missing_fields: Sequence[str],
    source: Mapping[str, Any],
) -> str:
    field_contracts = {
        "section_map": (
            "a compact ordered array of semantic episode sections with useful boundaries, "
            "topics, speakers, and uncertainty"
        ),
        "entity_seed": (
            "an object mapping people, organizations, products, and models to transcript-grounded "
            "aliases, roles, affiliations, and uncertainty"
        ),
        "concept_seed": "a compact array of recurring episode concepts as strings",
        "extraction_guidance": (
            "a useful semantic instruction string for later event extraction, including authority, "
            "speaker, alias, substantive-dialogue, quoted-source, ad/setup/page-chrome, and mixed-passage clues"
        ),
        "excluded_source_context": (
            "an array of concise semantic source-context classes that later extraction must exclude; "
            "derive them from this episode and never use keyword, regex, or phrase gates"
        ),
    }
    requested = {field: field_contracts[field] for field in missing_fields}
    return "\n\n".join(
        (
            "# Development-only context authority delta",
            (
                "The checksum-bound original context below is authoritative. Author only the "
                "requested fields that are absent. Existing fields must not be repeated, changed, "
                "repaired, summarized, or defaulted."
            ),
            "# Requested missing fields\n" + json.dumps(requested, ensure_ascii=True, indent=2, sort_keys=True),
            "# Episode metadata\n" + json.dumps(source["episode"], ensure_ascii=True, indent=2, sort_keys=True),
            "# Frozen existing context authority\n"
            + json.dumps(dict(original_context), ensure_ascii=True, indent=2, sort_keys=True),
            "# Full checksum-bound prepared development episode\n"
            + str(source["full_segmented_episode_text"]),
            "# Output contract",
            (
                f"Return one JSON object for episode_id {episode_id}. Include only schema_version, "
                "episode_id, original_context_sha256, and the requested missing fields. Do not "
                "include transcript passages, local paths, Markdown, or any already-present field."
            ),
        )
    )


def _verified_artifact_record(
    value: Any, *, label: str, allowed_root: Path | None = None
) -> Path:
    if not isinstance(value, Mapping) or set(value) != {"path", "sha256", "size_bytes"}:
        raise ExpandedCapDevelopmentMatrixError(f"{label} record is malformed")
    path_value = value.get("path")
    size = value.get("size_bytes")
    if (
        not isinstance(path_value, str)
        or not path_value
        or not _valid_sha256(value.get("sha256"))
        or isinstance(size, bool)
        or not isinstance(size, int)
        or size < 0
    ):
        raise ExpandedCapDevelopmentMatrixError(f"{label} record fields are invalid")
    path = Path(path_value).expanduser().resolve()
    if allowed_root is not None and not _inside(path, allowed_root):
        raise ExpandedCapDevelopmentMatrixError(f"{label} escapes its execution root")
    if (
        not path.is_file()
        or path.stat().st_size != size
        or _sha256_file(path) != value.get("sha256")
    ):
        raise ExpandedCapDevelopmentMatrixError(f"{label} checksum binding failed")
    return path


def _validate_context_delta_output(
    value: Any,
    *,
    episode_id: str,
    original_context_sha256: str,
    missing_fields: Sequence[str],
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ExpandedCapDevelopmentMatrixError("context delta output is not an object")
    missing = [str(field) for field in missing_fields]
    expected_keys = {
        "schema_version",
        "episode_id",
        "original_context_sha256",
        *missing,
    }
    if set(value) != expected_keys:
        raise ExpandedCapDevelopmentMatrixError(
            "context delta output repeated authority or omitted a requested field"
        )
    if (
        value.get("schema_version") != CONTEXT_PREPARATION_DELTA_SCHEMA_VERSION
        or value.get("episode_id") != episode_id
        or value.get("original_context_sha256") != original_context_sha256
    ):
        raise ExpandedCapDevelopmentMatrixError("context delta output identity drifted")
    for field in missing:
        if not _context_field_valid(field, value.get(field)):
            raise ExpandedCapDevelopmentMatrixError(
                f"LLM-authored context delta field is invalid: {episode_id}.{field}"
            )
    return copy.deepcopy(dict(value))


def _context_episode_paths(root: Path, episode_id: str) -> dict[str, Path]:
    episode_root = root / "episodes" / episode_id
    return {
        "root": episode_root,
        "base": episode_root / "base-instructions.txt",
        "prompt": episode_root / "prompt.private.md",
        "schema": episode_root / "output-schema.json",
        "source": episode_root / "source-binding.json",
        "attempt": episode_root / "attempt.json",
        "sidecar": episode_root / "turn-sidecar.json",
        "output": episode_root / "raw-output.private.json",
        "overlay": episode_root / "context-overlay.json",
        "outcome": episode_root / "outcome.json",
    }


def _partial_sidecar_accounting(path: Path) -> tuple[dict[str, int] | None, float | None]:
    if not path.is_file():
        return None, None
    try:
        sidecar = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None, None
    if not isinstance(sidecar, Mapping):
        return None, None
    usage = sidecar.get("usage")
    normalized = copy.deepcopy(dict(usage)) if _valid_usage(usage) else None
    wall = sidecar.get("wall_elapsed_seconds")
    wall_value = None
    if isinstance(wall, (int, float)) and not isinstance(wall, bool):
        try:
            wall_value = _nonnegative_number(wall, "context sidecar wall accounting")
        except ExpandedCapDevelopmentMatrixError:
            wall_value = None
    return normalized, wall_value


def _context_original_by_episode(
    episodes: Sequence[Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for episode in episodes:
        if not isinstance(episode, Mapping):
            raise ExpandedCapDevelopmentMatrixError("context episode is malformed")
        episode_id = str(episode.get("episode_id") or "")
        context = episode.get("episode_context")
        if not episode_id or episode_id in result or not isinstance(context, Mapping):
            raise ExpandedCapDevelopmentMatrixError("context episode identity is malformed")
        result[episode_id] = copy.deepcopy(dict(context))
    return result


def _validate_context_preparation_report(
    report: Any,
    *,
    plan: Mapping[str, Any],
    episodes: Sequence[Mapping[str, Any]],
    output_dir: Path,
    fixture_mode: bool,
) -> dict[str, Any]:
    root = Path(output_dir).expanduser().resolve()
    if not isinstance(report, Mapping):
        raise ExpandedCapDevelopmentMatrixError("context preparation report is missing")
    expected_rows = [
        row
        for row in plan.get("episodes") or []
        if isinstance(row, Mapping) and row.get("semantic_turn_required") is True
    ]
    outcomes = report.get("outcomes")
    if (
        report.get("schema_version") != CONTEXT_PREPARATION_REPORT_VERSION
        or report.get("state") != "completed"
        or report.get("development_only") is not True
        or report.get("manifest_sha256") != plan.get("manifest_sha256")
        or report.get("ambient_instruction_isolation")
        != plan.get("ambient_instruction_isolation")
        or report.get("model") != CONTEXT_PREPARATION_MODEL
        or report.get("reasoning_effort") != CONTEXT_PREPARATION_EFFORT
        or report.get("thread_mode") != "new_thread"
        or report.get("concurrency") != 1
        or report.get("retry_count") != 0
        or report.get("semantic_defaults_used") is not False
        or report.get("accounting_complete") is not True
        or not _valid_usage(report.get("usage"))
        or report.get("requested_calls") != len(expected_rows)
        or report.get("attempted_calls") != len(expected_rows)
        or report.get("validated_calls") != len(expected_rows)
        or not isinstance(outcomes, list)
        or len(outcomes) != len(expected_rows)
        or report.get("production_mutated") is not False
        or report.get("holdout_inspected") is not False
        or (not fixture_mode and report.get("official_managed_app_server_only") is not True)
    ):
        raise ExpandedCapDevelopmentMatrixError(
            "context preparation completion or accounting contract failed"
        )
    originals = _context_original_by_episode(episodes)
    instruction_contract = (
        _zero_byte_sidecar_instruction_contract() if not fixture_mode else None
    )
    usage_values: list[Mapping[str, Any]] = []
    wall_total = 0.0
    overlays: dict[str, dict[str, Any]] = {}
    observed_ids: list[str] = []
    turn_ids: set[str] = set()
    thread_ids: set[str] = set()
    for expected, raw_outcome in zip(expected_rows, outcomes):
        if not isinstance(raw_outcome, Mapping):
            raise ExpandedCapDevelopmentMatrixError("context preparation outcome is malformed")
        outcome = dict(raw_outcome)
        episode_id = str(expected["episode_id"])
        observed_ids.append(str(outcome.get("episode_id") or ""))
        if (
            outcome.get("episode_id") != episode_id
            or outcome.get("state") != "validated"
            or outcome.get("missing_fields") != expected.get("missing_fields")
            or outcome.get("original_context_payload_sha256")
            != expected.get("original_context_payload_sha256")
            or outcome.get("managed_chatgpt_auth_verified") is not True
            or outcome.get("retry_ordinal") != 0
            or not _valid_usage(outcome.get("usage"))
        ):
            raise ExpandedCapDevelopmentMatrixError("context preparation outcome drifted")
        usage_values.append(outcome["usage"])
        wall_total += _nonnegative_number(
            outcome.get("wall_elapsed_seconds"), "context outcome wall accounting"
        )
        attempt_path = _verified_artifact_record(
            outcome.get("attempt"), label="context attempt", allowed_root=root
        )
        sidecar_path = _verified_artifact_record(
            outcome.get("sidecar"), label="context sidecar", allowed_root=root
        )
        output_path = _verified_artifact_record(
            outcome.get("raw_output"), label="context raw output", allowed_root=root
        )
        overlay_path = _verified_artifact_record(
            outcome.get("context_overlay"), label="context overlay", allowed_root=root
        )
        prompt_path = _verified_artifact_record(
            outcome.get("prompt"), label="context prompt", allowed_root=root
        )
        schema_path = _verified_artifact_record(
            outcome.get("output_schema"), label="context schema", allowed_root=root
        )
        base_path = _verified_artifact_record(
            outcome.get("base_instructions"),
            label="context base instructions",
            allowed_root=root,
        )
        _verified_artifact_record(
            outcome.get("source_binding"),
            label="context source binding",
            allowed_root=root,
        )
        attempt = _load_object(attempt_path, "context attempt")
        if (
            attempt.get("episode_id") != episode_id
            or attempt.get("retry_ordinal") != 0
            or attempt.get("model") != CONTEXT_PREPARATION_MODEL
            or attempt.get("reasoning_effort") != CONTEXT_PREPARATION_EFFORT
            or attempt.get("prompt_sha256") != _sha256_file(prompt_path)
            or attempt.get("output_schema_sha256") != _sha256_file(schema_path)
            or attempt.get("base_instructions_sha256") != _sha256_file(base_path)
            or attempt.get("ambient_instruction_isolation_sha256")
            != sha256_text(
                _canonical_json(plan.get("ambient_instruction_isolation"))
            )
        ):
            raise ExpandedCapDevelopmentMatrixError("context attempt binding drifted")
        prompt = prompt_path.read_text(encoding="utf-8")
        base = base_path.read_text(encoding="utf-8")
        schema = _load_object(schema_path, "context output schema")
        raw_output = _load_object(output_path, "context raw output")
        delta = _validate_context_delta_output(
            raw_output,
            episode_id=episode_id,
            original_context_sha256=str(expected["original_context_payload_sha256"]),
            missing_fields=expected["missing_fields"],
        )
        if not fixture_mode:
            if outcome.get("official_managed_app_server_sidecar_valid") is not True:
                raise ExpandedCapDevelopmentMatrixError(
                    "live context outcome lacks official sidecar validation"
                )
            try:
                _sidecar, sidecar_usage = validate_completed_managed_sidecar(
                    sidecar_path=sidecar_path,
                    raw_output_path=output_path,
                    model=CONTEXT_PREPARATION_MODEL,
                    effort=CONTEXT_PREPARATION_EFFORT,
                    thread_mode="new_thread",
                    batch_size=1,
                    prompt=prompt,
                    output_schema=schema,
                    base_instructions=base,
                    instruction_contract=instruction_contract or {},
                )
            except ValueError as exc:
                raise ExpandedCapDevelopmentMatrixError(
                    "official managed context sidecar validation failed"
                ) from exc
            if sidecar_usage != outcome["usage"]:
                raise ExpandedCapDevelopmentMatrixError("context sidecar usage drifted")
        overlay = _load_object(overlay_path, "context overlay")
        original = originals[episode_id]
        expected_keys = set(original) | set(expected["missing_fields"])
        if set(overlay) != expected_keys:
            raise ExpandedCapDevelopmentMatrixError(
                "context overlay added or removed non-authorized fields"
            )
        for key, original_value in original.items():
            if _canonical_json(overlay.get(key)) != _canonical_json(original_value):
                raise ExpandedCapDevelopmentMatrixError(
                    f"context overlay changed existing authority: {episode_id}.{key}"
                )
        for field in expected["missing_fields"]:
            if _canonical_json(overlay.get(field)) != _canonical_json(delta[field]):
                raise ExpandedCapDevelopmentMatrixError("context overlay differs from model delta")
        thread_id = outcome.get("thread_id")
        turn_id = outcome.get("turn_id")
        if (
            not isinstance(thread_id, str)
            or not thread_id
            or thread_id in thread_ids
            or not isinstance(turn_id, str)
            or not turn_id
            or turn_id in turn_ids
        ):
            raise ExpandedCapDevelopmentMatrixError("context thread/turn accounting drifted")
        thread_ids.add(thread_id)
        turn_ids.add(turn_id)
        overlays[episode_id] = overlay
    if observed_ids != [str(row["episode_id"]) for row in expected_rows]:
        raise ExpandedCapDevelopmentMatrixError("context outcome order drifted")
    aggregate = _sum_usage(usage_values) if usage_values else {field: 0 for field in USAGE_FIELDS}
    if aggregate != report["usage"] or round(wall_total, 3) != report.get(
        "wall_elapsed_seconds"
    ):
        raise ExpandedCapDevelopmentMatrixError("context aggregate accounting drifted")
    augmented: list[dict[str, Any]] = []
    for episode in episodes:
        copied = copy.deepcopy(dict(episode))
        episode_id = str(copied["episode_id"])
        if episode_id in overlays:
            copied["episode_context"] = copy.deepcopy(overlays[episode_id])
        augmented.append(copied)
    return {
        "report": copy.deepcopy(dict(report)),
        "episodes": augmented,
        "overlays": overlays,
    }


async def run_development_context_preparation(
    conn: Any,
    *,
    episodes: Sequence[Mapping[str, Any]],
    manifest_info: Mapping[str, Any],
    plan: Mapping[str, Any],
    output_dir: Path,
    capacity_clearance_provider: Callable[[Mapping[str, Any]], Any],
    turn_runner: Callable[..., Any],
    timeout_seconds: float,
    fixture_mode: bool,
) -> dict[str, Any]:
    """Execute/adopt one no-retry model-authored delta per incomplete context."""

    root = Path(output_dir).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    _write_or_verify_json(root / "plan.json", plan)
    report_path = root / "report.json"
    if report_path.is_file():
        report = _load_object(report_path, "adopted context preparation report")
        return _validate_context_preparation_report(
            report,
            plan=plan,
            episodes=episodes,
            output_dir=root,
            fixture_mode=fixture_mode,
        )["report"]
    originals = _context_original_by_episode(episodes)
    prepared_by_id = {str(episode["episode_id"]): episode for episode in episodes}
    manifest_episodes = manifest_info.get("payload", {}).get("episodes")
    if not isinstance(manifest_episodes, list):
        raise ExpandedCapDevelopmentMatrixError("context preparation manifest is missing")
    manifest_by_id = {
        str(episode.get("episode_id")): episode
        for episode in manifest_episodes
        if isinstance(episode, Mapping)
    }
    required_rows = [
        row
        for row in plan.get("episodes") or []
        if isinstance(row, Mapping) and row.get("semantic_turn_required") is True
    ]
    outcomes: list[dict[str, Any]] = []
    for index, plan_row in enumerate(required_rows):
        episode_id = str(plan_row["episode_id"])
        original = originals.get(episode_id)
        prepared = prepared_by_id.get(episode_id)
        manifest_episode = manifest_by_id.get(episode_id)
        if (
            not isinstance(original, Mapping)
            or not isinstance(prepared, Mapping)
            or not isinstance(manifest_episode, Mapping)
            or sha256_text(_canonical_json(original))
            != plan_row.get("original_context_payload_sha256")
        ):
            raise ExpandedCapDevelopmentMatrixError("context preparation input binding drifted")
        paths = _context_episode_paths(root, episode_id)
        source = (
            _fixture_development_context_source(prepared)
            if fixture_mode
            else _full_development_context_source(conn, manifest_episode=manifest_episode)
        )
        schema = _context_delta_schema(
            episode_id=episode_id,
            original_context_sha256=str(plan_row["original_context_payload_sha256"]),
            missing_fields=plan_row["missing_fields"],
        )
        prompt = _context_delta_prompt(
            episode_id=episode_id,
            original_context=original,
            missing_fields=plan_row["missing_fields"],
            source=source,
        )
        base_record = _write_or_verify_text(paths["base"], CONTEXT_PREPARATION_BASE_INSTRUCTIONS)
        prompt_record = _write_or_verify_text(paths["prompt"], prompt)
        schema_record = _write_or_verify_json(paths["schema"], schema)
        source_record = _write_or_verify_json(paths["source"], source["source_binding"])
        if schema_record["sha256"] != plan_row.get("output_schema_sha256"):
            raise ExpandedCapDevelopmentMatrixError("context delta schema checksum drifted")
        if paths["outcome"].is_file():
            outcome = _load_object(paths["outcome"], "adopted context outcome")
            if outcome.get("state") != "validated":
                raise ExpandedCapDevelopmentMatrixError(
                    f"context attempt is terminal and cannot be replayed: {episode_id}"
                )
            outcomes.append(outcome)
            continue
        if paths["attempt"].exists():
            raise ExpandedCapDevelopmentMatrixError(
                f"context attempt is partial or ambiguous; semantic replay is prohibited: {episode_id}"
            )
        unexpected = [
            path
            for key, path in paths.items()
            if key in {"sidecar", "output", "overlay", "outcome"} and path.exists()
        ]
        if unexpected:
            raise ExpandedCapDevelopmentMatrixError(
                "context semantic artifacts exist without an immutable attempt: "
                + ", ".join(str(path) for path in unexpected)
            )
        remaining = len(required_rows) - index
        clearance_value = await _maybe_await(
            capacity_clearance_provider(
                {
                    "stage": "development_context_preparation",
                    "episode_id": episode_id,
                    "episode_index": index,
                    "remaining_turn_count": remaining,
                    "total_turn_count": len(required_rows),
                    "model": CONTEXT_PREPARATION_MODEL,
                    "effort": CONTEXT_PREPARATION_EFFORT,
                    "thread_mode": "new_thread",
                    "batch_size": 1,
                    "fixture_mode": fixture_mode,
                }
            )
        )
        if not isinstance(clearance_value, Mapping):
            raise ExpandedCapDevelopmentMatrixError("context capacity clearance is missing")
        clearance = copy.deepcopy(dict(clearance_value))
        if (
            clearance.get("state") != "cleared_before_semantic_client_start"
            or clearance.get("managed_chatgpt_auth_verified") is not True
            or clearance.get("plan_type") != "pro"
            or clearance.get("thread_started") is not False
            or clearance.get("turn_started") is not False
            or clearance.get("remaining_turn_count") != remaining
            or not isinstance(clearance.get("capacity_preflight"), Mapping)
        ):
            raise ExpandedCapDevelopmentMatrixError("context capacity clearance contract drifted")
        _verified_artifact_record(
            clearance["capacity_preflight"], label="context capacity preflight"
        )
        attempt = {
            "schema_version": CONTEXT_PREPARATION_REPORT_VERSION,
            "state": "semantic_attempt_committed_no_retry",
            "episode_id": episode_id,
            "episode_index": index,
            "missing_fields": copy.deepcopy(plan_row["missing_fields"]),
            "original_context_payload_sha256": plan_row[
                "original_context_payload_sha256"
            ],
            "model": CONTEXT_PREPARATION_MODEL,
            "reasoning_effort": CONTEXT_PREPARATION_EFFORT,
            "thread_mode": "new_thread",
            "batch_size": 1,
            "retry_ordinal": 0,
            "remaining_turn_count_at_clearance": remaining,
            "capacity_preflight": copy.deepcopy(clearance["capacity_preflight"]),
            "base_instructions_sha256": base_record["sha256"],
            "prompt_sha256": prompt_record["sha256"],
            "output_schema_sha256": schema_record["sha256"],
            "source_binding_sha256": source_record["sha256"],
            "ambient_instruction_isolation_sha256": sha256_text(
                _canonical_json(plan.get("ambient_instruction_isolation"))
            ),
            "managed_chatgpt_auth_only": True,
            "production_mutated": False,
            "holdout_inspected": False,
        }
        attempt_record = _write_new_json(paths["attempt"], attempt)
        result_value: Any = None
        try:
            result_value = await _maybe_await(
                turn_runner(
                    episode_id=episode_id,
                    base_instructions=CONTEXT_PREPARATION_BASE_INSTRUCTIONS,
                    prompt=prompt,
                    output_schema=schema,
                    sidecar_path=paths["sidecar"],
                    output_path=paths["output"],
                    timeout_seconds=timeout_seconds,
                    fixture_mode=fixture_mode,
                )
            )
            if not isinstance(result_value, Mapping):
                raise ExpandedCapDevelopmentMatrixError("context turn runner returned no result")
            result = copy.deepcopy(dict(result_value))
            if (
                result.get("status_ok") is not True
                or result.get("managed_chatgpt_auth_verified") is not True
                or not _valid_usage(result.get("usage"))
                or not isinstance(result.get("thread_id"), str)
                or not result["thread_id"]
                or not isinstance(result.get("turn_id"), str)
                or not result["turn_id"]
            ):
                raise ExpandedCapDevelopmentMatrixError(
                    "context semantic result or accounting is incomplete"
                )
            wall = _nonnegative_number(
                result.get("wall_elapsed_seconds"), "context semantic wall accounting"
            )
            sidecar_record = _record(paths["sidecar"])
            output_record = _record(paths["output"])
            raw_output = _load_object(paths["output"], "context raw output")
            if isinstance(result.get("output"), Mapping) and _canonical_json(
                result["output"]
            ) != _canonical_json(raw_output):
                raise ExpandedCapDevelopmentMatrixError(
                    "context runner output differs from its raw checkpoint"
                )
            delta = _validate_context_delta_output(
                raw_output,
                episode_id=episode_id,
                original_context_sha256=str(plan_row["original_context_payload_sha256"]),
                missing_fields=plan_row["missing_fields"],
            )
            overlay = copy.deepcopy(dict(original))
            for field in plan_row["missing_fields"]:
                if field in overlay:
                    raise ExpandedCapDevelopmentMatrixError(
                        "context delta attempted to replace existing authority"
                    )
                overlay[field] = copy.deepcopy(delta[field])
            overlay_record = _write_new_json(paths["overlay"], overlay)
            outcome = {
                "schema_version": CONTEXT_PREPARATION_REPORT_VERSION,
                "state": "validated",
                "episode_id": episode_id,
                "missing_fields": copy.deepcopy(plan_row["missing_fields"]),
                "original_context_payload_sha256": plan_row[
                    "original_context_payload_sha256"
                ],
                "retry_ordinal": 0,
                "managed_chatgpt_auth_verified": True,
                "official_managed_app_server_sidecar_valid": (
                    result.get("official_managed_app_server_sidecar_valid") is True
                ),
                "thread_id": result["thread_id"],
                "turn_id": result["turn_id"],
                "usage": copy.deepcopy(result["usage"]),
                "wall_elapsed_seconds": round(wall, 3),
                "attempt": attempt_record,
                "base_instructions": base_record,
                "prompt": prompt_record,
                "output_schema": schema_record,
                "source_binding": source_record,
                "sidecar": sidecar_record,
                "raw_output": output_record,
                "context_overlay": overlay_record,
                "production_mutated": False,
                "holdout_inspected": False,
            }
            _write_new_json(paths["outcome"], outcome)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            partial_usage, partial_wall = _partial_sidecar_accounting(paths["sidecar"])
            failed = {
                "schema_version": CONTEXT_PREPARATION_REPORT_VERSION,
                "state": "failed_no_retry",
                "episode_id": episode_id,
                "missing_fields": copy.deepcopy(plan_row["missing_fields"]),
                "retry_ordinal": 0,
                "failure_class": type(exc).__name__,
                "usage": partial_usage,
                "usage_complete": partial_usage is not None,
                "wall_elapsed_seconds": partial_wall,
                "attempt": attempt_record,
                "sidecar": _record(paths["sidecar"]) if paths["sidecar"].is_file() else None,
                "raw_output": _record(paths["output"]) if paths["output"].is_file() else None,
                "production_mutated": False,
                "holdout_inspected": False,
            }
            if not paths["outcome"].exists():
                _write_new_json(paths["outcome"], failed)
            raise ExpandedCapDevelopmentMatrixError(
                f"context semantic attempt failed without retry: {episode_id}"
            ) from exc
        outcomes.append(outcome)
    usage_total = _sum_usage([outcome["usage"] for outcome in outcomes])
    wall_total = round(sum(float(outcome["wall_elapsed_seconds"]) for outcome in outcomes), 3)
    report = {
        "schema_version": CONTEXT_PREPARATION_REPORT_VERSION,
        "state": "completed",
        "development_only": True,
        "manifest_sha256": plan.get("manifest_sha256"),
        "model": CONTEXT_PREPARATION_MODEL,
        "reasoning_effort": CONTEXT_PREPARATION_EFFORT,
        "thread_mode": "new_thread",
        "concurrency": 1,
        "retry_count": 0,
        "semantic_defaults_used": False,
        "ambient_instruction_isolation": copy.deepcopy(
            plan.get("ambient_instruction_isolation")
        ),
        "official_managed_app_server_only": not fixture_mode,
        "requested_calls": len(required_rows),
        "attempted_calls": len(outcomes),
        "validated_calls": len(outcomes),
        "accounting_complete": True,
        "usage": usage_total,
        "wall_elapsed_seconds": wall_total,
        "cost_treatment": {
            "classification": "measured_development_only_schema_delta_validation",
            "full_measured_usage_bound": True,
            "included_in_epoch7_total_accounting": True,
            "added_to_production_amortized_context_numerator": False,
            "reason": (
                "one-time evaluation artifact repair is not recurring per-production workload; "
                "retain the frozen exact recovered production context allocation"
            ),
        },
        "outcomes": outcomes,
        "production_mutated": False,
        "holdout_inspected": False,
    }
    _write_new_json(report_path, report)
    return _validate_context_preparation_report(
        report,
        plan=plan,
        episodes=episodes,
        output_dir=root,
        fixture_mode=fixture_mode,
    )["report"]


def _variant_id(batch_size: int, thread_mode: str) -> str:
    return f"batch_{batch_size}_{thread_mode}"


def build_matrix_dry_preflight(
    conn: Any,
    *,
    epoch6_receipt_path: Path = DEFAULT_EPOCH6_RECEIPT_PATH,
    epoch6_receipt_sha256: str = DEFAULT_EPOCH6_RECEIPT_SHA256,
    manifest_path: Path = DEFAULT_MANIFEST_PATH,
    manifest_sha256: str = DEFAULT_MANIFEST_SHA256,
    judge_calibration_path: Path = DEFAULT_JUDGE_CALIBRATION_PATH,
    judge_calibration_sha256: str = DEFAULT_JUDGE_CALIBRATION_SHA256,
    window_count: int = 4,
    context_chars: int = 900,
    episode_loader: Callable[..., Any] = evaluation._load_prepared_episodes,
    allowed_record_root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    """Prove the complete six-arm request surface without capacity or model work."""

    adapter_record = _verify_exact_file_hash(
        Path(str(adapter.__file__)).resolve(),
        DEFAULT_ADAPTER_MODULE_SHA256,
        "expanded-cap episode-batch adapter",
    )
    epoch6 = verify_epoch6_passed_receipt(
        epoch6_receipt_path,
        expected_sha256=epoch6_receipt_sha256,
        allowed_record_root=allowed_record_root,
    )
    manifest = verify_development_manifest(
        manifest_path,
        expected_sha256=manifest_sha256,
    )
    calibration = verify_judge_calibration_receipt(
        judge_calibration_path,
        expected_sha256=judge_calibration_sha256,
        allowed_record_root=allowed_record_root,
    )
    episodes = prepare_development_episodes(
        conn,
        manifest=manifest["payload"],
        manifest_rows=manifest["rows"],
        window_count=window_count,
        context_chars=context_chars,
        episode_loader=episode_loader,
    )
    context_preparation = inspect_development_context_preparation(
        episodes=episodes,
        manifest_info=manifest,
        require_manifest_artifacts=episode_loader is evaluation._load_prepared_episodes,
    )
    if context_preparation["semantic_turn_count"]:
        planned_arms: list[dict[str, Any]] = []
        total_planned_turns = 0
        ambient_isolation: dict[str, Any] | None = None
        for batch_size, thread_mode in EXPECTED_ARMS:
            request_count = sum(
                math.ceil(len(episode.get("segments") or []) / batch_size)
                for episode in episodes
            )
            total_planned_turns += request_count
            configuration = adapter.build_frozen_configuration(
                batch_size=batch_size, thread_mode=thread_mode
            )
            observed_isolation = _zero_byte_ambient_instruction_isolation(
                configuration
            )
            if ambient_isolation is None:
                ambient_isolation = observed_isolation
            elif _canonical_json(ambient_isolation) != _canonical_json(
                observed_isolation
            ):
                raise ExpandedCapDevelopmentMatrixError(
                    "ambient instruction isolation differs across planned arms"
                )
            planned_arms.append(
                {
                    "variant_id": _variant_id(batch_size, thread_mode),
                    "batch_size": batch_size,
                    "thread_mode": thread_mode,
                    "request_count": request_count,
                    "batch_ids": None,
                    "request_identity_state": (
                        "awaiting_llm_authored_context_delta_before_exact_batch_id_materialization"
                    ),
                    "case_order_sha256": sha256_text(
                        _canonical_json(manifest["case_order"])
                    ),
                    "frozen_configuration_sha256": sha256_text(
                        _canonical_json(configuration)
                    ),
                    "frozen_configuration_state": "checksum_bound",
                }
            )
        return {
            "schema_version": MATRIX_VERSION,
            "state": "context_preparation_required_no_capacity_probe_or_semantic_work",
            "epoch6_receipt_sha256": epoch6["sha256"],
            "manifest_sha256": manifest["sha256"],
            "judge_calibration_sha256": calibration["sha256"],
            "adapter_module": adapter_record,
            "arm_count": 6,
            "case_count": 32,
            "arms": planned_arms,
            "exact_extraction_turn_count": total_planned_turns,
            "extraction_turn_identity_frozen": False,
            "context_preparation": context_preparation,
            "context_preparation_required": True,
            "context_semantic_turn_count": context_preparation["semantic_turn_count"],
            "ambient_instruction_isolation": ambient_isolation,
            "judge_turn_count": "computed_exactly_after_shared_pool_freeze",
            "capacity_probe_performed": False,
            "semantic_client_started": False,
            "thread_started": False,
            "turn_started": False,
            "retry_count": 0,
            "holdout_inspected": False,
            "production_mutated": False,
            "episodes": episodes,
            "manifest_info": manifest,
        }
    arm_rows: list[dict[str, Any]] = []
    total_turns = 0
    expected_order = manifest["case_order"]
    ambient_isolation = None
    for batch_size, thread_mode in EXPECTED_ARMS:
        configuration = adapter.build_frozen_configuration(
            batch_size=batch_size, thread_mode=thread_mode
        )
        observed_isolation = _zero_byte_ambient_instruction_isolation(configuration)
        if ambient_isolation is None:
            ambient_isolation = observed_isolation
        elif _canonical_json(ambient_isolation) != _canonical_json(
            observed_isolation
        ):
            raise ExpandedCapDevelopmentMatrixError(
                "ambient instruction isolation differs across dry-preflight arms"
            )
        requests = [
            request
            for episode in episodes
            for request in adapter.prepare_episode_batches(
                episode,
                batch_size=batch_size,
                thread_mode=thread_mode,
            )
        ]
        observed_order = [
            str(segment_id)
            for request in requests
            for segment_id in request["segment_ids"]
        ]
        if observed_order != expected_order:
            raise ExpandedCapDevelopmentMatrixError("dry preflight arm membership drifted")
        total_turns += len(requests)
        arm_rows.append(
            {
                "variant_id": _variant_id(batch_size, thread_mode),
                "batch_size": batch_size,
                "thread_mode": thread_mode,
                "request_count": len(requests),
                "batch_ids": [str(request["batch_id"]) for request in requests],
                "case_order_sha256": sha256_text(_canonical_json(observed_order)),
                "frozen_configuration_sha256": sha256_text(
                    _canonical_json(configuration)
                ),
            }
        )
    return {
        "schema_version": MATRIX_VERSION,
        "state": "dry_preflight_passed_no_capacity_probe_or_semantic_work",
        "epoch6_receipt_sha256": epoch6["sha256"],
        "manifest_sha256": manifest["sha256"],
        "judge_calibration_sha256": calibration["sha256"],
        "adapter_module": adapter_record,
        "arm_count": 6,
        "case_count": 32,
        "arms": arm_rows,
        "exact_extraction_turn_count": total_turns,
        "extraction_turn_identity_frozen": True,
        "context_preparation": context_preparation,
        "context_preparation_required": False,
        "context_semantic_turn_count": 0,
        "ambient_instruction_isolation": ambient_isolation,
        "judge_turn_count": "computed_exactly_after_shared_pool_freeze",
        "capacity_probe_performed": False,
        "semantic_client_started": False,
        "thread_started": False,
        "turn_started": False,
        "retry_count": 0,
        "holdout_inspected": False,
        "production_mutated": False,
        "episodes": episodes,
        "manifest_info": manifest,
    }


def precommit_six_arms(
    *,
    output_dir: Path,
    episodes: Sequence[Mapping[str, Any]],
    case_order: Sequence[str],
    epoch6_sha256: str,
    manifest_sha256: str,
    window_count: int,
    context_chars: int,
) -> dict[str, Any]:
    """Freeze or adopt all six configurations and prove identical membership."""

    root = Path(output_dir).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    adapter_record = _verify_exact_file_hash(
        Path(str(adapter.__file__)).resolve(),
        DEFAULT_ADAPTER_MODULE_SHA256,
        "expanded-cap episode-batch adapter",
    )
    surface = adapter.precommit_selection_surface()
    observed = [
        (row.get("batch_size"), row.get("thread_mode"))
        for row in surface.get("arms") or []
        if isinstance(row, Mapping)
    ]
    if (
        surface.get("arm_count") != 6
        or tuple(observed) != EXPECTED_ARMS
        or surface.get("selection_requires_complete_usage_cache_reasoning_and_wall_telemetry")
        is not True
        or surface.get("selection_requires_semantic_judging") is not True
        or surface.get("holdout_authorized") is not False
        or surface.get("production_mutation_allowed") is not False
    ):
        raise ExpandedCapDevelopmentMatrixError("adapter six-arm precommit surface drifted")
    surface_record = _write_or_verify_json(root / "precommit-surface.json", surface)
    arm_rows: list[dict[str, Any]] = []
    prepared_by_arm: dict[str, list[dict[str, Any]]] = {}
    arm_roots: set[Path] = set()
    ambient_isolation: dict[str, Any] | None = None
    for batch_size, thread_mode in EXPECTED_ARMS:
        variant_id = _variant_id(batch_size, thread_mode)
        configuration_path = root / "configurations" / f"{variant_id}.json"
        configuration = adapter.freeze_precommit_selection(
            configuration_path,
            batch_size=batch_size,
            thread_mode=thread_mode,
        )
        observed_isolation = _zero_byte_ambient_instruction_isolation(configuration)
        if ambient_isolation is None:
            ambient_isolation = observed_isolation
        elif _canonical_json(ambient_isolation) != _canonical_json(
            observed_isolation
        ):
            raise ExpandedCapDevelopmentMatrixError(
                "ambient instruction isolation differs across precommitted arms"
            )
        requests: list[dict[str, Any]] = []
        for episode in episodes:
            requests.extend(
                adapter.prepare_episode_batches(
                    episode,
                    batch_size=batch_size,
                    thread_mode=thread_mode,
                )
            )
        for request in requests:
            adapter.validate_prepared_request(request)
        observed_cases = [
            str(segment_id)
            for request in requests
            for segment_id in request["segment_ids"]
        ]
        if observed_cases != list(case_order) or len(set(observed_cases)) != 32:
            raise ExpandedCapDevelopmentMatrixError(
                f"{variant_id} precommit case order or membership drifted"
            )
        arm_root = (root.parent / "arms" / variant_id).resolve()
        if arm_root in arm_roots or _inside(root, arm_root) or _inside(arm_root, root):
            raise ExpandedCapDevelopmentMatrixError("development arm roots are not unique")
        arm_roots.add(arm_root)
        prepared_by_arm[variant_id] = requests
        arm_rows.append(
            {
                "variant_id": variant_id,
                "batch_size": batch_size,
                "thread_mode": thread_mode,
                "configuration": _record(configuration_path),
                "request_count": len(requests),
                "batch_ids": [str(request["batch_id"]) for request in requests],
                "case_order_sha256": sha256_text(_canonical_json(observed_cases)),
                "arm_root": str(arm_root),
                "semantic_postprocessing": configuration["semantic_postprocessing"],
                "deterministic_semantic_pruning": configuration[
                    "deterministic_semantic_pruning"
                ],
                "retry_count": configuration["retry_count"],
            }
        )
    precommit_path = root / "matrix-precommit.json"
    created_at = (
        _load_object(precommit_path, "matrix precommit").get("created_at")
        if precommit_path.is_file()
        else now_iso()
    )
    if not isinstance(created_at, str) or not created_at:
        raise ExpandedCapDevelopmentMatrixError("matrix precommit timestamp drifted")
    precommit = {
        "schema_version": PRECOMMIT_VERSION,
        "state": "frozen_before_capacity_admission_or_semantic_turns",
        "created_at": created_at,
        "epoch6_receipt_sha256": epoch6_sha256,
        "manifest_sha256": manifest_sha256,
        "adapter_module": adapter_record,
        "surface": surface_record,
        "arm_count": 6,
        "arms": arm_rows,
        "case_count": 32,
        "case_order_sha256": sha256_text(_canonical_json(list(case_order))),
        "exact_case_order_and_membership_across_arms": True,
        "window_count": window_count,
        "context_chars": context_chars,
        "retry_count": 0,
        "semantic_deterministic_pruning": False,
        "ambient_instruction_isolation": ambient_isolation,
        "holdout_inspected": False,
        "holdout_frozen": False,
        "production_mutation_allowed": False,
    }
    precommit_record = _write_or_verify_json(precommit_path, precommit)
    return {
        "root": root,
        "surface": surface,
        "precommit": precommit,
        "precommit_record": precommit_record,
        "prepared_by_arm": prepared_by_arm,
        "arm_rows": {row["variant_id"]: row for row in arm_rows},
    }


def _capacity_wrapper(value: Any, fixture_mode: bool) -> tuple[dict[str, Any], str]:
    if not isinstance(value, Mapping) or set(value) != {"receipt", "sha256", "fixture_mode"}:
        raise ExpandedCapDevelopmentMatrixError(
            "capacity provider must return receipt, sha256, and fixture_mode"
        )
    receipt = value.get("receipt")
    if not isinstance(receipt, Mapping) or value.get("fixture_mode") is not fixture_mode:
        raise ExpandedCapDevelopmentMatrixError("capacity admission fixture/live mode drifted")
    expected_sha = _require_sha256(value.get("sha256"), "capacity admission expected SHA-256")
    if adapter.capacity_admission_sha256(receipt) != expected_sha:
        raise ExpandedCapDevelopmentMatrixError("capacity admission checksum binding failed")
    return copy.deepcopy(dict(receipt)), expected_sha


def _validate_historical_capacity_admission(
    receipt: Mapping[str, Any],
    *,
    expected_sha256: str,
    batch_size: int,
    thread_mode: str,
    concurrency: int,
    episode_ids: Sequence[str],
    segment_count: int,
    batch_count: int,
    fixture_mode: bool,
) -> None:
    """Revalidate a completed arm's admission without requiring it still be fresh."""

    expected_keys = {
        "schema_version",
        "admission_id",
        "issued_at",
        "expires_at",
        "state",
        "issued_by",
        "verification_mode",
        "fixture",
        "winner_system_id",
        "batch_size",
        "thread_mode",
        "model",
        "effort",
        "concurrency",
        "episode_ids_sha256",
        "episode_count",
        "segment_count",
        "batch_count",
        "live_capacity_available",
        "managed_chatgpt_auth_only",
        "verification_evidence_sha256",
    }
    try:
        issued = datetime.fromisoformat(str(receipt.get("issued_at")).replace("Z", "+00:00"))
        expires = datetime.fromisoformat(str(receipt.get("expires_at")).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ExpandedCapDevelopmentMatrixError("historical capacity timestamps are invalid") from exc
    validity = (expires.astimezone(timezone.utc) - issued.astimezone(timezone.utc)).total_seconds()
    if (
        set(receipt) != expected_keys
        or adapter.capacity_admission_sha256(receipt) != expected_sha256
        or receipt.get("schema_version") != adapter.CAPACITY_ADMISSION_VERSION
        or not isinstance(receipt.get("admission_id"), str)
        or not receipt.get("admission_id")
        or receipt.get("state") != "admitted"
        or receipt.get("issued_by") != "evaluation_coordinator"
        or receipt.get("verification_mode")
        != ("fixture_verified" if fixture_mode else "live_verified")
        or receipt.get("fixture") is not fixture_mode
        or receipt.get("winner_system_id") != adapter.WINNER_SYSTEM_ID
        or receipt.get("batch_size") != batch_size
        or receipt.get("thread_mode") != thread_mode
        or receipt.get("model") != adapter.MODEL
        or receipt.get("effort") != adapter.EFFORT
        or receipt.get("concurrency") != concurrency
        or receipt.get("episode_ids_sha256")
        != sha256_text(_canonical_json(list(episode_ids)))
        or receipt.get("episode_count") != len(episode_ids)
        or receipt.get("segment_count") != segment_count
        or receipt.get("batch_count") != batch_count
        or receipt.get("live_capacity_available") is not True
        or receipt.get("managed_chatgpt_auth_only") is not True
        or not _valid_sha256(receipt.get("verification_evidence_sha256"))
        or issued.tzinfo is None
        or expires.tzinfo is None
        or validity <= 0
        or validity > 900
    ):
        raise ExpandedCapDevelopmentMatrixError("historical capacity admission drifted")


def validate_complete_arm_report(
    report: Mapping[str, Any],
    *,
    requests: Sequence[Mapping[str, Any]],
    configuration: Mapping[str, Any],
    capacity_receipt: Mapping[str, Any],
    capacity_sha256: str,
    arm_dir: Path,
    fixture_mode: bool,
) -> dict[str, Any]:
    request_count = len(requests)
    usage = report.get("usage")
    telemetry = report.get("turn_telemetry")
    if (
        report.get("schema_version") != adapter.RUN_REPORT_VERSION
        or report.get("state") != "passed"
        or report.get("development_regression_eligible") is not True
        or report.get("winner_system_id") != adapter.WINNER_SYSTEM_ID
        or report.get("batch_size") != configuration["batch_size"]
        or report.get("thread_mode") != configuration["thread_mode"]
        or report.get("model") != adapter.MODEL
        or report.get("effort") != adapter.EFFORT
        or report.get("max_events_per_segment") != adapter.MAX_EVENTS_PER_SEGMENT
        or report.get("semantic_postprocessing") is not False
        or report.get("all_emitted_events_preserved") is not True
        or report.get("exact_identity_duplicates_are_diagnostic_only") is not True
        or report.get("retry_count") != 0
        or report.get("ambiguous_retry_count") != 0
        or report.get("ambiguous_outcome_attempts") != 0
        or report.get("requested_calls") != request_count
        or report.get("attempted_calls") != request_count
        or report.get("validated_calls") != request_count
        or report.get("terminal_sidecars") != request_count
        or report.get("usage_measured_attempts") != request_count
        or report.get("usage_unknown_attempts") != 0
        or report.get("attempt_contract_failures") != 0
        or report.get("sidecar_contract_failures") != 0
        or report.get("usage_status") != "complete"
        or report.get("accounting_complete") is not True
        or not _valid_usage(usage)
        or report.get("cached_input_tokens") != usage["cached_input_tokens"]
        or report.get("reasoning_output_tokens") != usage["reasoning_output_tokens"]
        or report.get("turn_ids_unique") is not True
        or report.get("thread_lineage_valid") is not True
        or report.get("preflight_lineage_valid") is not True
        or report.get("capacity_binding_valid") is not True
        or report.get("fixture_mode") is not fixture_mode
        or not isinstance(telemetry, list)
        or len(telemetry) != request_count
    ):
        raise ExpandedCapDevelopmentMatrixError("arm report is not complete and selectable")
    _nonnegative_number(report.get("wall_elapsed_seconds"), "arm wall_elapsed_seconds")
    declared_turn_wall = _nonnegative_number(
        report.get("turn_wall_elapsed_seconds_sum"),
        "arm turn_wall_elapsed_seconds_sum",
    )
    expected_batch_ids = [str(request["batch_id"]) for request in requests]
    observed_batch_ids = [str(row.get("batch_id")) for row in telemetry if isinstance(row, Mapping)]
    if observed_batch_ids != expected_batch_ids:
        raise ExpandedCapDevelopmentMatrixError("arm turn telemetry order drifted")
    turn_usages: list[Mapping[str, Any]] = []
    turn_walls: list[float] = []
    turn_ids: list[str] = []
    thread_ids: list[str] = []
    for row in telemetry:
        turn_id = row.get("turn_id")
        thread_id = row.get("thread_id")
        if (
            row.get("attempted") is not True
            or row.get("attempt_contract_valid") is not True
            or row.get("sidecar_contract_valid") is not True
            or row.get("thread_matches_attempt") is not True
            or row.get("usage_status") != "complete"
            or not _valid_usage(row.get("usage"))
            or not isinstance(turn_id, str)
            or not turn_id
            or not isinstance(thread_id, str)
            or not thread_id
        ):
            raise ExpandedCapDevelopmentMatrixError("arm turn accounting is incomplete")
        turn_usages.append(row["usage"])
        turn_walls.append(_nonnegative_number(row.get("wall_elapsed_seconds"), "turn wall"))
        turn_ids.append(turn_id)
        thread_ids.append(thread_id)
    if _sum_usage(turn_usages) != usage or not math.isclose(
        sum(turn_walls), declared_turn_wall, rel_tol=0.0, abs_tol=0.001
    ):
        raise ExpandedCapDevelopmentMatrixError("arm aggregate usage or wall accounting drifted")
    if len(set(turn_ids)) != len(turn_ids):
        raise ExpandedCapDevelopmentMatrixError("arm turn IDs were replayed")
    binding = report.get("capacity_binding")
    binding_path = (
        Path(str(binding.get("path") or "")).expanduser().resolve()
        if isinstance(binding, Mapping)
        else Path("/")
    )
    if (
        not isinstance(binding, Mapping)
        or set(binding) != {"path", "sha256", "size_bytes"}
        or not _inside(binding_path, Path(arm_dir).expanduser().resolve())
        or not binding_path.is_file()
        or binding.get("sha256") != _sha256_file(binding_path)
        or binding.get("size_bytes") != binding_path.stat().st_size
    ):
        raise ExpandedCapDevelopmentMatrixError("arm capacity binding is not auditable")
    binding_payload = _load_object(binding_path, "arm capacity binding")
    if (
        binding_payload.get("schema_version") != adapter.CAPACITY_BINDING_VERSION
        or binding_payload.get("state") != "verified_before_app_server_start"
        or binding_payload.get("winner_system_id") != adapter.WINNER_SYSTEM_ID
        or binding_payload.get("expected_canonical_sha256") != capacity_sha256
        or binding_payload.get("observed_canonical_sha256") != capacity_sha256
        or binding_payload.get("fixture") is not fixture_mode
    ):
        raise ExpandedCapDevelopmentMatrixError("arm capacity binding payload drifted")
    admission_id = capacity_receipt.get("admission_id")
    if not isinstance(admission_id, str) or not admission_id:
        raise ExpandedCapDevelopmentMatrixError("capacity admission ID is missing")
    return {
        "usage": copy.deepcopy(dict(usage)),
        "turn_ids": turn_ids,
        "thread_ids": sorted(set(thread_ids)),
        "admission_id": admission_id,
    }


def load_adapter_arm_outputs(
    *,
    arm_dir: Path,
    report: Mapping[str, Any],
    requests: Sequence[Mapping[str, Any]],
    episodes: Sequence[Mapping[str, Any]],
    expected_case_order: Sequence[str],
) -> dict[str, Any]:
    """Load duplicate-preserving normalized adapter artifacts without DB access."""

    del report, episodes
    root = Path(arm_dir).expanduser().resolve()
    mapping_path = root / "private-mapping.json"
    mapping = _load_object(mapping_path, "adapter arm private mapping")
    batches = mapping.get("batches")
    if (
        mapping.get("schema_version") != adapter.PRIVATE_MAPPING_VERSION
        or mapping.get("winner_system_id") != adapter.WINNER_SYSTEM_ID
        or not isinstance(batches, list)
        or len(batches) != len(requests)
    ):
        raise ExpandedCapDevelopmentMatrixError("adapter arm private mapping drifted")
    cases: list[dict[str, Any]] = []
    artifacts: list[dict[str, Any]] = []
    seen_paths: set[Path] = set()
    for request, batch in zip(requests, batches):
        if (
            not isinstance(batch, Mapping)
            or batch.get("batch_id") != request["batch_id"]
            or batch.get("episode_id") != request["episode_id"]
            or batch.get("segment_ids") != request["segment_ids"]
        ):
            raise ExpandedCapDevelopmentMatrixError("adapter arm batch mapping order drifted")
        normalized_value = batch.get("normalized_output_path")
        if not isinstance(normalized_value, str) or not normalized_value:
            raise ExpandedCapDevelopmentMatrixError("adapter normalized output path is missing")
        normalized_path = Path(normalized_value).expanduser().resolve()
        applicability_path = normalized_path.parent / "applicability-receipt.json"
        if (
            not _inside(normalized_path, root)
            or not _inside(applicability_path, root)
            or normalized_path in seen_paths
            or applicability_path in seen_paths
        ):
            raise ExpandedCapDevelopmentMatrixError("adapter output roots are not unique and contained")
        seen_paths.update({normalized_path, applicability_path})
        normalized = _load_object(normalized_path, "adapter normalized output")
        applicability = _load_object(applicability_path, "adapter applicability receipt")
        rows = normalized.get("segments")
        if (
            normalized.get("episode_id") != request["episode_id"]
            or not isinstance(rows, list)
            or [row.get("segment_id") for row in rows if isinstance(row, Mapping)]
            != request["segment_ids"]
            or applicability.get("all_emitted_events_preserved") is not True
            or applicability.get("semantic_pruning_performed") is not False
            or applicability.get("support_filtering_performed") is not False
            or applicability.get("deduplication_performed") is not False
            or applicability.get("relabeling_performed") is not False
            or applicability.get("emitted_event_count")
            != applicability.get("normalized_event_count")
        ):
            raise ExpandedCapDevelopmentMatrixError("adapter output changed emitted semantics")
        cases.extend(copy.deepcopy(rows))
        artifacts.append(
            {
                "batch_id": request["batch_id"],
                "normalized_output": _record(normalized_path),
                "applicability_receipt": _record(applicability_path),
            }
        )
    if [row.get("segment_id") for row in cases] != list(expected_case_order):
        raise ExpandedCapDevelopmentMatrixError("adapter arm output case order drifted")
    return {"cases": cases, "artifacts": artifacts, "mapping": _record(mapping_path)}


def _validate_arm_cases(
    value: Any,
    *,
    expected_case_order: Sequence[str],
) -> dict[str, Any]:
    if not isinstance(value, Mapping) or not isinstance(value.get("cases"), list):
        raise ExpandedCapDevelopmentMatrixError("arm output loader returned no cases")
    cases = value["cases"]
    if [row.get("segment_id") for row in cases if isinstance(row, Mapping)] != list(
        expected_case_order
    ):
        raise ExpandedCapDevelopmentMatrixError("arm output membership/order differs")
    for row in cases:
        events = row.get("events") if isinstance(row, Mapping) else None
        status = row.get("status") if isinstance(row, Mapping) else None
        if (
            not isinstance(events, list)
            or any(not isinstance(event, Mapping) for event in events)
            or status not in {"coded", "no_signal"}
            or (status == "coded") != bool(events)
        ):
            raise ExpandedCapDevelopmentMatrixError("arm case output is structurally invalid")
    return {"cases": copy.deepcopy(cases), "artifacts": copy.deepcopy(value.get("artifacts") or [])}


def assemble_shared_augmented_reference(
    *,
    output_dir: Path,
    manifest_info: Mapping[str, Any],
    episodes: Sequence[Mapping[str, Any]],
    arms: Mapping[str, Mapping[str, Any]],
    seed: str,
) -> dict[str, Any]:
    """Create one blinded union reference for every arm on all 32 cases."""

    if set(arms) != {_variant_id(*key) for key in EXPECTED_ARMS}:
        raise ExpandedCapDevelopmentMatrixError("shared reference requires six complete arms")
    rows = list(manifest_info["rows"])
    case_order = list(manifest_info["case_order"])
    text_by_segment = {
        str(segment["segment_id"]): str(segment["segment_text"])
        for episode in episodes
        for segment in episode["segments"]
    }
    arm_case_by_id = {
        variant_id: {str(row["segment_id"]): row for row in arm["cases"]}
        for variant_id, arm in arms.items()
    }
    systems: dict[str, dict[str, Any]] = {
        BASELINE_REPAIRED_SYSTEM: {
            "kind": "reference_seed",
            "representation": "golden_output",
            "selectable": False,
        }
    }
    for variant_id, arm in sorted(arms.items()):
        systems[_arm_system(variant_id, "normalized")] = {
            "kind": "candidate",
            "representation": "normalized",
            "variant_id": variant_id,
            "selectable": True,
            "config": copy.deepcopy(arm["winner_config"]),
        }
    system_cases = {system_id: {} for system_id in systems}
    raw_cases: list[dict[str, Any]] = []
    exact_display_dedup = 0
    for spec in rows:
        segment_id = str(spec["segment_id"])
        source_excerpt = text_by_segment.get(segment_id)
        if not isinstance(source_excerpt, str) or sha256_text(source_excerpt) != spec["text_sha256"]:
            raise ExpandedCapDevelopmentMatrixError("shared-reference source text hash drifted")
        side_entries: dict[str, dict[str, dict[str, Any]]] = {"a": {}, "b": {}}

        def add_events(system_id: str, side: str, events: Sequence[Mapping[str, Any]]) -> None:
            nonlocal exact_display_dedup
            exact_count = 0
            hashes: list[str] = []
            for index, raw_event in enumerate(events):
                event = copy.deepcopy(dict(raw_event))
                canonical = _canonical_json(event)
                event_hash = sha256_text(canonical)
                evidence = event.get("evidence")
                evidence_exact = bool(
                    isinstance(evidence, str) and evidence and evidence in source_excerpt
                )
                exact_count += int(evidence_exact)
                hashes.append(event_hash)
                membership = {
                    "system_id": system_id,
                    "event_index": index,
                    "submitted_evidence_exact": evidence_exact,
                }
                existing = side_entries[side].get(event_hash)
                if existing is None:
                    side_entries[side][event_hash] = {
                        "event": event,
                        "canonical_json": canonical,
                        "memberships": [membership],
                    }
                else:
                    if existing["canonical_json"] != canonical:
                        raise ExpandedCapDevelopmentMatrixError("canonical event hash collision")
                    existing["memberships"].append(membership)
                    exact_display_dedup += 1
            system_cases[system_id][segment_id] = {
                "status": "coded" if events else "no_signal",
                "event_hashes": sorted(set(hashes)),
                "submitted_event_count": len(events),
                "exact_evidence_event_count": exact_count,
            }

        reference = manifest_info["reference_by_segment"][segment_id]
        reference_events = reference["golden_output"].get("discourse_events") or []
        add_events(BASELINE_REPAIRED_SYSTEM, "a", reference_events)
        for variant_id in sorted(arms):
            system_id = _arm_system(variant_id, "normalized")
            add_events(system_id, "b", arm_case_by_id[variant_id][segment_id].get("events") or [])

        for event_hash in sorted(set(side_entries["a"]) & set(side_entries["b"])):
            left = side_entries["a"][event_hash]
            right = side_entries["b"].pop(event_hash)
            if left["canonical_json"] != right["canonical_json"]:
                raise ExpandedCapDevelopmentMatrixError("cross-side event hash collision")
            left["memberships"].extend(right["memberships"])
            exact_display_dedup += 1
        rendered: dict[str, list[dict[str, Any]]] = {}
        for side in ("a", "b"):
            rendered[side] = [
                {
                    "event": entry["event"],
                    "provenance": {
                        "canonical_event_sha256": event_hash,
                        "memberships": sorted(
                            entry["memberships"],
                            key=lambda item: (str(item["system_id"]), int(item["event_index"])),
                        ),
                        "structural_sentinel": False,
                    },
                }
                for event_hash, entry in sorted(side_entries[side].items())
            ]
        raw_cases.append(
            {
                "case_key": segment_id,
                "source_excerpt": source_excerpt,
                "event_set_a": rendered["a"],
                "event_set_b": rendered["b"],
                "provenance": {
                    "segment_id": segment_id,
                    "episode_id": spec["episode_id"],
                    "source_id": spec["source_id"],
                    "density_stratum": spec["density_stratum"],
                    "text_sha256": spec["text_sha256"],
                },
            }
        )
    pool, private_mapping = app_server_llm_judge.make_shared_witness_pool(raw_cases, seed=seed)
    if app_server_llm_judge.validate_shared_witness_pool(pool):
        raise ExpandedCapDevelopmentMatrixError("generated shared witness pool is invalid")
    membership_index = {
        "schema_version": DEV_MEMBERSHIP_VERSION,
        "manifest_path": manifest_info["path"],
        "manifest_sha256": manifest_info["sha256"],
        "systems": systems,
        "system_cases": system_cases,
        "case_order": [str(case["case_id"]) for case in pool["cases"]],
        "segment_order": case_order,
        "arm_artifacts": {
            variant_id: {
                "variant_id": variant_id,
                "report": copy.deepcopy(arm["report_record"]),
                "configuration": copy.deepcopy(arm["configuration_record"]),
                "usage": copy.deepcopy(arm["usage"]),
            }
            for variant_id, arm in sorted(arms.items())
        },
        "matrix_mode": "six_clean_expanded_cap",
        "interrupted_arm": None,
        "privacy": "private ids memberships and local artifact hashes no source text",
    }
    root = Path(output_dir).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    pool_record = _write_or_verify_json(root / "shared-witness-pool.private.json", pool)
    mapping_record = _write_or_verify_json(root / "private-mapping.json", private_mapping)
    membership_record = _write_or_verify_json(root / "membership-index.private.json", membership_index)
    assembly = {
        "schema_version": ASSEMBLY_VERSION,
        "state": "one_blinded_shared_augmented_reference_frozen",
        "manifest_sha256": manifest_info["sha256"],
        "reference_seed_sha256": manifest_info["reference_sha256"],
        "case_count": 32,
        "source_count": 4,
        "arm_count": 6,
        "system_count": len(systems),
        "witness_count": sum(
            len(case["event_set_a"]) + len(case["event_set_b"])
            for case in pool["cases"]
        ),
        "exact_identity_display_duplicates_collapsed_with_memberships_preserved":
        exact_display_dedup,
        "pool": pool_record,
        "private_mapping": mapping_record,
        "membership_index": membership_record,
        "case_order_sha256": sha256_text(_canonical_json(case_order)),
        "reference_policy": (
            "union_of_consensus_supported_exact_evidence_full_equivalence_units_from_"
            "frozen_seed_and_all_six_arms"
        ),
        "semantic_deterministic_pruning": False,
        "exact_identity_deduplication_is_display_only": True,
        "holdout_included": False,
        "production_included": False,
    }
    assembly_record = _write_or_verify_json(root / "assembly-report.json", assembly)
    return {
        "root": root,
        "pool": pool,
        "pool_record": pool_record,
        "private_mapping": private_mapping,
        "mapping_record": mapping_record,
        "membership_index": membership_index,
        "membership_record": membership_record,
        "assembly": assembly,
        "assembly_record": assembly_record,
    }


def _load_bound_reserve_policy(
    path: Path,
    *,
    expected_sha256: str,
    purpose: str,
) -> dict[str, Any]:
    record = _verify_exact_file_hash(path, expected_sha256, purpose)
    try:
        policy = reserve.load_reserve_capacity_policy(Path(record["path"]))
    except Exception as exc:
        raise ExpandedCapDevelopmentMatrixError(
            f"{purpose} is not an audited reserve policy"
        ) from exc
    return {"record": record, "policy": policy}


def _evaluate_exact_remaining_turns(
    snapshot: Mapping[str, Any],
    *,
    audited_policy: Mapping[str, Any],
    remaining_turn_count: int,
) -> dict[str, Any]:
    if (
        isinstance(remaining_turn_count, bool)
        or not isinstance(remaining_turn_count, int)
        or remaining_turn_count < 1
    ):
        raise ExpandedCapDevelopmentMatrixError("remaining semantic turn count is invalid")
    derived = copy.deepcopy(dict(audited_policy))
    derived["ordered_turn_names"] = [
        f"remaining_matrix_turn_{index:04d}" for index in range(remaining_turn_count)
    ]
    try:
        return reserve.evaluate_reserve_capacity(
            snapshot,
            policy=derived,
            remaining_turn_count=remaining_turn_count,
        )
    except Exception as exc:
        raise ExpandedCapDevelopmentMatrixError(
            "audited reserve evaluation failed for exact remaining turns"
        ) from exc


class LiveMatrixCapacityCoordinator:
    """No-thread capacity broker plus lazy, resumable real judge execution.

    Extraction admissions are scoped to the exact remaining turns in one
    immutable adapter arm.  Judge probes are scoped to every still-missing
    AB/BA turn across the already-frozen shard plan.  A denied probe writes no
    adapter admission and starts no semantic client, thread, sidecar, or turn.
    """

    def __init__(
        self,
        *,
        control_dir: Path,
        extraction_policy_path: Path = DEFAULT_EXTRACTION_CAPACITY_POLICY_PATH,
        extraction_policy_sha256: str = DEFAULT_EXTRACTION_CAPACITY_POLICY_SHA256,
        judge_policy_path: Path = DEFAULT_JUDGE_CAPACITY_POLICY_PATH,
        judge_policy_sha256: str = DEFAULT_JUDGE_CAPACITY_POLICY_SHA256,
        rate_limit_probe: Callable[..., Any] = probe_app_server_rate_limits,
        wait_for_capacity: bool = False,
        recheck_seconds: int = 60,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        clock: Callable[[], float] = time.time,
        semantic_client_factory: Callable[[], Any] = _isolated_matrix_semantic_client_factory,
    ) -> None:
        if not isinstance(wait_for_capacity, bool):
            raise ExpandedCapDevelopmentMatrixError("wait_for_capacity must be boolean")
        if (
            isinstance(recheck_seconds, bool)
            or not isinstance(recheck_seconds, int)
            or recheck_seconds < 1
        ):
            raise ExpandedCapDevelopmentMatrixError("capacity recheck seconds are invalid")
        self.control_dir = Path(control_dir).expanduser().resolve()
        self.extraction = _load_bound_reserve_policy(
            extraction_policy_path,
            expected_sha256=extraction_policy_sha256,
            purpose="epoch-6 extraction reserve policy",
        )
        self.judge = _load_bound_reserve_policy(
            judge_policy_path,
            expected_sha256=judge_policy_sha256,
            purpose="frozen judge reserve policy",
        )
        if (
            self.extraction["policy"].get("phase_id")
            != "epoch6_expanded_cap_full_event_direct_reference_reserve"
            or self.extraction["policy"].get("maximum_total_tokens_per_turn") != 102_000
            or self.judge["policy"].get("phase_id")
            != "judge_v5_4_v174_exact_evidence_continuation"
            or self.judge["policy"].get("maximum_total_tokens_per_turn") != 28_000
            or self.extraction["policy"].get("quota_points_per_million_tokens") != 17
            or self.judge["policy"].get("quota_points_per_million_tokens") != 17
            or self.extraction["policy"].get("minimum_remaining_reserve_percent", 0) < 20
            or self.judge["policy"].get("minimum_remaining_reserve_percent", 0) < 20
        ):
            raise ExpandedCapDevelopmentMatrixError("matrix reserve policy lineage drifted")
        self.rate_limit_probe = rate_limit_probe
        self.wait_for_capacity = wait_for_capacity
        self.recheck_seconds = recheck_seconds
        self.sleep = sleep
        self.clock = clock
        self.semantic_client_factory = semantic_client_factory

    def _next_probe_path(self, stage_id: str) -> Path:
        safe = "".join(character if character.isalnum() or character in "-_" else "-" for character in stage_id)
        root = self.control_dir / "probes" / safe
        ordinal = 1
        while (root / f"probe-{ordinal:04d}.json").exists():
            ordinal += 1
        return root / f"probe-{ordinal:04d}.json"

    def _wait_seconds(self, snapshot: Mapping[str, Any]) -> float:
        result = float(self.recheck_seconds)
        reset_at = snapshot.get("primary_resets_at")
        now = int(self.clock())
        if isinstance(reset_at, int) and not isinstance(reset_at, bool) and reset_at > now:
            result = min(result, max(1.0, float(reset_at - now + 1)))
        return result

    async def _probe_until_clear(
        self,
        *,
        stage_id: str,
        policy_binding: Mapping[str, Any],
        remaining_turn_count: int,
    ) -> dict[str, Any]:
        while True:
            snapshot_value = await _maybe_await(
                self.rate_limit_probe(maximum_primary_used_percent=100)
            )
            if not isinstance(snapshot_value, Mapping):
                raise ExpandedCapDevelopmentMatrixError("live capacity probe returned no snapshot")
            snapshot = copy.deepcopy(dict(snapshot_value))
            if (
                snapshot.get("schema_version") != "pif_app_server_rate_limit_probe_v1"
                or snapshot.get("managed_chatgpt_auth_verified") is not True
                or snapshot.get("plan_type") != "pro"
                or snapshot.get("limit_id") != "codex"
                or snapshot.get("thread_started") is not False
                or snapshot.get("turn_started") is not False
            ):
                raise ExpandedCapDevelopmentMatrixError(
                    "live capacity probe violated no-thread managed-ChatGPT Pro contract"
                )
            evaluation = _evaluate_exact_remaining_turns(
                snapshot,
                audited_policy=policy_binding["policy"],
                remaining_turn_count=remaining_turn_count,
            )
            preflight = {
                "schema_version": LIVE_CAPACITY_PREFLIGHT_VERSION,
                "checked_at": now_iso(),
                "state": (
                    "cleared_before_semantic_client_start"
                    if evaluation["cleared_for_semantic_turn"]
                    else "waiting_before_semantic_client_start"
                ),
                "stage_id": stage_id,
                "audited_policy": copy.deepcopy(policy_binding["record"]),
                "snapshot": {
                    key: copy.deepcopy(snapshot.get(key))
                    for key in (
                        "schema_version",
                        "managed_chatgpt_auth_verified",
                        "plan_type",
                        "limit_id",
                        "primary_used_percent",
                        "primary_resets_at",
                        "rate_limit_reached_type",
                        "thread_started",
                        "turn_started",
                    )
                },
                "reserve_evaluation": evaluation,
                "exact_remaining_turn_count": remaining_turn_count,
                "semantic_client_started": False,
                "thread_started": False,
                "turn_started": False,
                "sidecar_started": False,
                "retry_count": 0,
                "production_mutated": False,
                "holdout_inspected": False,
            }
            probe_record = _write_new_json(self._next_probe_path(stage_id), preflight)
            if evaluation["cleared_for_semantic_turn"]:
                return {
                    "snapshot": snapshot,
                    "evaluation": evaluation,
                    "record": probe_record,
                }
            if not self.wait_for_capacity:
                raise ExpandedCapMatrixCapacityUnavailable(
                    f"{stage_id} reserve is insufficient before semantic client start"
                )
            await self.sleep(self._wait_seconds(snapshot))

    async def arm_capacity_admission(self, context: Mapping[str, Any]) -> dict[str, Any]:
        required = {
            "variant_id",
            "batch_size",
            "thread_mode",
            "winner_system_id",
            "model",
            "effort",
            "concurrency",
            "episode_ids",
            "episode_count",
            "segment_count",
            "batch_count",
            "fixture_mode",
        }
        if (
            not required.issubset(context)
            or context.get("fixture_mode") is not False
            or context.get("winner_system_id") != adapter.WINNER_SYSTEM_ID
            or context.get("model") != adapter.MODEL
            or context.get("effort") != adapter.EFFORT
            or context.get("concurrency") != 1
            or context.get("segment_count") != 32
            or isinstance(context.get("batch_count"), bool)
            or not isinstance(context.get("batch_count"), int)
            or context["batch_count"] < 1
        ):
            raise ExpandedCapDevelopmentMatrixError("live arm capacity context drifted")
        preflight = await self._probe_until_clear(
            stage_id=f"arm-{context['variant_id']}",
            policy_binding=self.extraction,
            remaining_turn_count=int(context["batch_count"]),
        )
        issued = datetime.now(timezone.utc)
        expires = issued + timedelta(minutes=15)
        episode_ids = [str(value) for value in context["episode_ids"]]
        admission_id = "matrix_cap_" + sha256_text(
            _canonical_json(
                {
                    "variant_id": context["variant_id"],
                    "preflight_sha256": preflight["record"]["sha256"],
                    "issued_at": issued.isoformat(),
                }
            )
        )[:24]
        receipt = {
            "schema_version": adapter.CAPACITY_ADMISSION_VERSION,
            "admission_id": admission_id,
            "issued_at": issued.isoformat(),
            "expires_at": expires.isoformat(),
            "state": "admitted",
            "issued_by": "evaluation_coordinator",
            "verification_mode": "live_verified",
            "fixture": False,
            "winner_system_id": adapter.WINNER_SYSTEM_ID,
            "batch_size": context["batch_size"],
            "thread_mode": context["thread_mode"],
            "model": adapter.MODEL,
            "effort": adapter.EFFORT,
            "concurrency": 1,
            "episode_ids_sha256": sha256_text(_canonical_json(episode_ids)),
            "episode_count": len(episode_ids),
            "segment_count": 32,
            "batch_count": context["batch_count"],
            "live_capacity_available": True,
            "managed_chatgpt_auth_only": True,
            "verification_evidence_sha256": preflight["record"]["sha256"],
        }
        receipt_sha = adapter.capacity_admission_sha256(receipt)
        _write_new_json(
            self.control_dir / "admissions" / f"{context['variant_id']}-{admission_id}.json",
            receipt,
        )
        return {"receipt": receipt, "sha256": receipt_sha, "fixture_mode": False}

    async def context_capacity_clearance(self, context: Mapping[str, Any]) -> dict[str, Any]:
        remaining = context.get("remaining_turn_count")
        episode_id = context.get("episode_id")
        if (
            context.get("stage") != "development_context_preparation"
            or context.get("fixture_mode") is not False
            or context.get("model") != CONTEXT_PREPARATION_MODEL
            or context.get("effort") != CONTEXT_PREPARATION_EFFORT
            or context.get("thread_mode") != "new_thread"
            or context.get("batch_size") != 1
            or not isinstance(episode_id, str)
            or not episode_id
            or isinstance(remaining, bool)
            or not isinstance(remaining, int)
            or remaining < 1
        ):
            raise ExpandedCapDevelopmentMatrixError(
                "live context capacity request drifted"
            )
        preflight = await self._probe_until_clear(
            stage_id=f"context-{episode_id}",
            policy_binding=self.extraction,
            remaining_turn_count=remaining,
        )
        return {
            "state": "cleared_before_semantic_client_start",
            "managed_chatgpt_auth_verified": True,
            "plan_type": "pro",
            "thread_started": False,
            "turn_started": False,
            "remaining_turn_count": remaining,
            "maximum_total_tokens_per_turn": self.extraction["policy"][
                "maximum_total_tokens_per_turn"
            ],
            "capacity_preflight": copy.deepcopy(preflight["record"]),
        }

    async def run_context_preparation(
        self,
        *,
        conn: Any,
        episodes: Sequence[Mapping[str, Any]],
        manifest_info: Mapping[str, Any],
        plan: Mapping[str, Any],
        output_dir: Path,
        timeout_seconds: float,
        fixture_mode: bool,
    ) -> dict[str, Any]:
        if fixture_mode is not False:
            raise ExpandedCapDevelopmentMatrixError(
                "live context preparation runner cannot use fixture mode"
            )
        async with _PersistentManagedContextTurnRunner(self) as turn_runner:
            return await run_development_context_preparation(
                conn,
                episodes=episodes,
                manifest_info=manifest_info,
                plan=plan,
                output_dir=output_dir,
                capacity_clearance_provider=self.context_capacity_clearance,
                turn_runner=turn_runner,
                timeout_seconds=timeout_seconds,
                fixture_mode=False,
            )

    @staticmethod
    def _missing_judge_turns(pool: Mapping[str, Any], output_dir: Path) -> int:
        missing = 0
        for index, _shard in enumerate(app_server_dev_selection_plan_judge_shards(pool)):
            judge_root = Path(output_dir).expanduser().resolve() / f"shard-{index:03d}" / "judge"
            report_path = judge_root / "report.json"
            if report_path.is_file():
                report = _load_object(report_path, "adopted judge shard report")
                consensus_path = judge_root / "consensus.private.json"
                if (
                    report.get("state") != "completed"
                    or report.get("accounting_complete") is not True
                    or not _valid_usage(report.get("usage"))
                    or not consensus_path.is_file()
                    or report.get("consensus_sha256") != _sha256_file(consensus_path)
                ):
                    raise ExpandedCapDevelopmentMatrixError(
                        "completed judge shard cannot be adopted"
                    )
                continue
            for orientation in ("ab", "ba"):
                output_path = judge_root / f"output-{orientation}.private.json"
                sidecar_path = judge_root / "sidecars" / f"{orientation}.json"
                capacity_path = judge_root / "sidecars" / f"{orientation}.capacity.json"
                if output_path.is_file() and sidecar_path.is_file() and capacity_path.is_file():
                    continue
                if output_path.exists() or sidecar_path.exists() or capacity_path.exists():
                    raise ExpandedCapDevelopmentMatrixError(
                        "partial judge turn is ambiguous; semantic replay is prohibited"
                    )
                missing += 1
        return missing

    async def run_shared_judge(
        self,
        *,
        pool: Mapping[str, Any],
        pool_path: Path,
        output_dir: Path,
        calibration: Mapping[str, Any],
        timeout_seconds: float,
        fixture_mode: bool,
    ) -> dict[str, Any]:
        del pool_path
        if fixture_mode is not False:
            raise ExpandedCapDevelopmentMatrixError("live judge runner cannot use fixture mode")
        root = Path(output_dir).expanduser().resolve()
        remaining = self._missing_judge_turns(pool, root)
        client = _LazyReserveJudgeClient(
            coordinator=self,
            remaining_turn_count=remaining,
        )
        report = await run_full_judge_shards(
            pool=pool,
            output_dir=root,
            model=adapter.MODEL,
            reasoning_effort=adapter.EFFORT,
            timeout_seconds=timeout_seconds,
            client_factory=lambda: client,
        )
        consensus = _load_object(Path(str(report["consensus_path"])), "matrix judge consensus")
        capacity_records: list[dict[str, Any]] = []
        judge_wall = 0.0
        for shard in report.get("shards") or []:
            shard_report = _load_object(Path(str(shard["report_path"])), "matrix judge shard")
            for sidecar_value in shard_report.get("sidecar_paths") or []:
                sidecar_path = Path(str(sidecar_value)).expanduser().resolve()
                sidecar = _load_object(sidecar_path, "matrix judge sidecar")
                judge_wall += _nonnegative_number(
                    sidecar.get("wall_elapsed_seconds"), "judge sidecar wall accounting"
                )
                capacity_path = sidecar_path.with_suffix(".capacity.json")
                capacity_records.append(_record(capacity_path))
        return {
            "state": "completed",
            "calibration_receipt_sha256": calibration["sha256"],
            "orientations": ["ab", "ba"],
            "case_order": [str(case["case_id"]) for case in pool["cases"]],
            "abstention_enabled": True,
            "capacity_admission_verified": bool(capacity_records),
            "capacity_records": capacity_records,
            "retry_count": 0,
            "accounting_complete": report.get("accounting_complete"),
            "usage": report.get("usage"),
            "wall_elapsed_seconds": round(judge_wall, 3),
            "consensus": consensus,
            "report": report,
        }


class _PersistentManagedContextTurnRunner:
    """One lazy official app-server process for all no-retry development deltas."""

    def __init__(self, coordinator: LiveMatrixCapacityCoordinator) -> None:
        self.coordinator = coordinator
        self.inner_context: Any = None
        self.inner: Any = None
        self.instruction_contract: Mapping[str, Any] | None = None
        self.sticky_failure: str | None = None

    async def __aenter__(self) -> "_PersistentManagedContextTurnRunner":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if self.inner_context is not None:
            await self.inner_context.__aexit__(exc_type, exc, traceback)
        self.inner_context = None
        self.inner = None

    async def __call__(
        self,
        *,
        episode_id: str,
        base_instructions: str,
        prompt: str,
        output_schema: Mapping[str, Any],
        sidecar_path: Path,
        output_path: Path,
        timeout_seconds: float,
        fixture_mode: bool,
    ) -> dict[str, Any]:
        if fixture_mode is not False:
            raise ExpandedCapDevelopmentMatrixError(
                "managed context turn runner cannot use fixture mode"
            )
        if self.sticky_failure is not None:
            raise ExpandedCapDevelopmentMatrixError(
                f"context semantic phase stopped: {self.sticky_failure}"
            )
        if self.inner is None:
            self.inner_context = self.coordinator.semantic_client_factory()
            self.inner = await self.inner_context.__aenter__()
            account = getattr(self.inner, "account_summary", None)
            if (
                not isinstance(account, Mapping)
                or account.get("type") != "chatgpt"
                or account.get("plan_type") != "pro"
            ):
                self.sticky_failure = "managed_auth_drift"
                raise ExpandedCapDevelopmentMatrixError(
                    "context semantic client is not managed ChatGPT Pro"
                )
            self.instruction_contract = _zero_byte_sidecar_instruction_contract()
        try:
            result = await self.inner.run_ephemeral_structured_turn(
                model=CONTEXT_PREPARATION_MODEL,
                effort=CONTEXT_PREPARATION_EFFORT,
                base_instructions=base_instructions,
                prompt=prompt,
                output_schema=copy.deepcopy(dict(output_schema)),
                cwd=PROJECT_ROOT,
                sidecar_path=sidecar_path,
                output_path=output_path,
                batch_size=1,
                thread_mode="new_thread",
                timeout_seconds=timeout_seconds,
            )
            if getattr(result, "status_ok", False) is not True or not isinstance(
                getattr(result, "output", None), Mapping
            ):
                raise ExpandedCapDevelopmentMatrixError(
                    getattr(result, "error_class", None)
                    or f"context_turn_{getattr(result, 'status', 'unknown')}"
                )
            try:
                _sidecar, usage = validate_completed_managed_sidecar(
                    sidecar_path=Path(sidecar_path),
                    raw_output_path=Path(output_path),
                    model=CONTEXT_PREPARATION_MODEL,
                    effort=CONTEXT_PREPARATION_EFFORT,
                    thread_mode="new_thread",
                    batch_size=1,
                    prompt=prompt,
                    output_schema=output_schema,
                    base_instructions=base_instructions,
                    instruction_contract=self.instruction_contract or {},
                )
            except ValueError as exc:
                raise ExpandedCapDevelopmentMatrixError(
                    "context managed sidecar contract failed"
                ) from exc
            if usage["total_tokens"] > int(
                self.coordinator.extraction["policy"]["maximum_total_tokens_per_turn"]
            ):
                raise ExpandedCapDevelopmentMatrixError(
                    "context turn exceeded the audited maximum-token reserve bound"
                )
            return {
                "status_ok": True,
                "managed_chatgpt_auth_verified": True,
                "official_managed_app_server_sidecar_valid": True,
                "episode_id": episode_id,
                "thread_id": str(result.thread_id),
                "turn_id": str(result.turn_id),
                "usage": usage,
                "wall_elapsed_seconds": float(result.wall_elapsed_seconds),
                "output": copy.deepcopy(dict(result.output)),
            }
        except asyncio.CancelledError:
            self.sticky_failure = "cancelled_semantic_turn"
            raise
        except BaseException:
            self.sticky_failure = "ambiguous_or_failed_semantic_turn"
            raise


class _LazyReserveJudgeClient:
    """Start the semantic app-server only after an exact reserve clearance."""

    def __init__(
        self,
        *,
        coordinator: LiveMatrixCapacityCoordinator,
        remaining_turn_count: int,
    ) -> None:
        self.coordinator = coordinator
        self.remaining_turn_count = remaining_turn_count
        self.inner_context: Any = None
        self.inner: Any = None
        self.sticky_failure: str | None = None

    async def __aenter__(self) -> "_LazyReserveJudgeClient":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if self.inner_context is not None:
            await self.inner_context.__aexit__(exc_type, exc, traceback)
        self.inner_context = None
        self.inner = None

    async def run_ephemeral_structured_turn(self, *args: Any, **kwargs: Any) -> Any:
        if self.sticky_failure is not None:
            raise ExpandedCapDevelopmentMatrixError(
                f"judge semantic phase stopped: {self.sticky_failure}"
            )
        if self.remaining_turn_count < 1:
            raise ExpandedCapDevelopmentMatrixError("judge attempted an unplanned semantic turn")
        checkpoint_value = kwargs.pop("capacity_checkpoint_path", None)
        if checkpoint_value is None:
            raise ExpandedCapDevelopmentMatrixError("judge turn has no capacity checkpoint path")
        checkpoint_path = Path(checkpoint_value).expanduser().resolve()
        if checkpoint_path.exists():
            raise ExpandedCapDevelopmentMatrixError(
                "judge capacity checkpoint exists; ambiguous retry is prohibited"
            )
        stage_id = (
            "judge-"
            + checkpoint_path.parents[2].name
            + "-"
            + checkpoint_path.name.removesuffix(".capacity.json")
        )
        preflight = await self.coordinator._probe_until_clear(
            stage_id=stage_id,
            policy_binding=self.coordinator.judge,
            remaining_turn_count=self.remaining_turn_count,
        )
        checkpoint = {
            "schema_version": reserve.RESERVE_CAPACITY_CHECKPOINT_VERSION,
            "checked_at": now_iso(),
            "state": "cleared_before_semantic_client_start",
            "stage_id": stage_id,
            "audited_policy": copy.deepcopy(self.coordinator.judge["record"]),
            "capacity_preflight": copy.deepcopy(preflight["record"]),
            "reserve_evaluation": copy.deepcopy(preflight["evaluation"]),
            "remaining_turn_count": self.remaining_turn_count,
            "managed_chatgpt_auth_verified": True,
            "plan_type": "pro",
            "thread_started": False,
            "turn_started": False,
            "sidecar_started": False,
            "retry_checkpoint_reuse_allowed": False,
            "production_mutated": False,
            "holdout_inspected": False,
        }
        _write_new_json(checkpoint_path, checkpoint)
        if self.inner is None:
            self.inner_context = self.coordinator.semantic_client_factory()
            self.inner = await self.inner_context.__aenter__()
            account = getattr(self.inner, "account_summary", None)
            if (
                not isinstance(account, Mapping)
                or account.get("type") != "chatgpt"
                or account.get("plan_type") != "pro"
            ):
                self.sticky_failure = "managed_auth_drift"
                raise ExpandedCapDevelopmentMatrixError(
                    "judge semantic client is not managed ChatGPT Pro"
                )
        try:
            result = await self.inner.run_ephemeral_structured_turn(*args, **kwargs)
        except BaseException:
            self.sticky_failure = "ambiguous_or_failed_semantic_turn"
            raise
        usage = getattr(result, "usage", None)
        total = getattr(usage, "total_tokens", None)
        if (
            getattr(result, "status_ok", False) is not True
            or isinstance(total, bool)
            or not isinstance(total, int)
            or total < 0
            or total > int(self.coordinator.judge["policy"]["maximum_total_tokens_per_turn"])
        ):
            self.sticky_failure = "judge_usage_or_status_contract_failed"
            raise ExpandedCapDevelopmentMatrixError(self.sticky_failure)
        self.remaining_turn_count -= 1
        return result


def app_server_dev_selection_plan_judge_shards(
    pool: Mapping[str, Any],
) -> list[list[dict[str, Any]]]:
    """Narrow indirection keeps the exact existing shard planner fixture-patchable."""

    from .app_server_dev_selection import plan_judge_shards

    return plan_judge_shards(pool)


async def default_shared_judge_runner(
    *,
    pool: Mapping[str, Any],
    pool_path: Path,
    output_dir: Path,
    calibration: Mapping[str, Any],
    timeout_seconds: float,
    fixture_mode: bool,
) -> dict[str, Any]:
    """Use the existing sharded AB/BA support/alignment consensus runner."""

    del pool_path, fixture_mode
    report = await run_full_judge_shards(
        pool=pool,
        output_dir=Path(output_dir),
        model="gpt-5.6-sol",
        reasoning_effort="high",
        timeout_seconds=timeout_seconds,
    )
    consensus_path = Path(str(report["consensus_path"])).expanduser().resolve()
    consensus = _load_object(consensus_path, "shared judge consensus")
    wall_total = 0.0
    capacity_records: list[dict[str, Any]] = []
    for shard in report.get("shards") or []:
        shard_report_path = Path(str(shard["report_path"])).expanduser().resolve()
        shard_report = _load_object(shard_report_path, "shared judge shard report")
        if shard_report.get("variant_count") != 2:
            raise ExpandedCapDevelopmentMatrixError("judge shard did not execute AB and BA")
        for sidecar_value in shard_report.get("sidecar_paths") or []:
            sidecar_path = Path(str(sidecar_value)).expanduser().resolve()
            sidecar = _load_object(sidecar_path, "shared judge sidecar")
            wall_total += _nonnegative_number(
                sidecar.get("wall_elapsed_seconds"), "judge turn wall_elapsed_seconds"
            )
            capacity_path = sidecar_path.with_suffix(".capacity.json")
            if not capacity_path.is_file():
                # Older runner naming is <variant>.capacity.json next to <variant>.json.
                capacity_path = sidecar_path.parent / f"{sidecar_path.stem}.capacity.json"
            capacity_records.append(_record(capacity_path))
    return {
        "state": "completed",
        "calibration_receipt_sha256": calibration["sha256"],
        "orientations": ["ab", "ba"],
        "case_order": [str(case["case_id"]) for case in pool["cases"]],
        "abstention_enabled": True,
        "capacity_admission_verified": bool(capacity_records),
        "capacity_records": capacity_records,
        "retry_count": 0,
        "accounting_complete": report.get("accounting_complete"),
        "usage": report.get("usage"),
        "wall_elapsed_seconds": round(wall_total, 3),
        "consensus": consensus,
        "report": report,
    }


def validate_shared_judge_result(
    result: Any,
    *,
    pool: Mapping[str, Any],
    calibration_sha256: str,
) -> dict[str, Any]:
    if not isinstance(result, Mapping):
        raise ExpandedCapDevelopmentMatrixError("shared judge returned no report")
    consensus = result.get("consensus")
    expected_case_order = [str(case["case_id"]) for case in pool["cases"]]
    if (
        result.get("state") != "completed"
        or result.get("calibration_receipt_sha256") != calibration_sha256
        or result.get("orientations") != ["ab", "ba"]
        or result.get("case_order") != expected_case_order
        or result.get("abstention_enabled") is not True
        or result.get("capacity_admission_verified") is not True
        or result.get("retry_count") != 0
        or result.get("accounting_complete") is not True
        or not _valid_usage(result.get("usage"))
        or not isinstance(consensus, Mapping)
        or consensus.get("schema_version") != app_server_llm_judge.JUDGE_CONSENSUS_VERSION
        or [row.get("case_id") for row in consensus.get("cases") or []]
        != expected_case_order
        or consensus.get("selection_admissible") is not True
    ):
        raise ExpandedCapDevelopmentMatrixError(
            "shared judge calibration, AB/BA, capacity, or accounting gate failed"
        )
    _nonnegative_number(result.get("wall_elapsed_seconds"), "judge wall accounting")
    return copy.deepcopy(dict(result))


def _winner_config(
    *,
    variant_id: str,
    configuration: Mapping[str, Any],
    window_count: int,
    context_chars: int,
) -> dict[str, Any]:
    del window_count, context_chars
    try:
        verified = adapter.verify_frozen_configuration(configuration)
    except Exception as exc:
        raise ExpandedCapDevelopmentMatrixError(
            "winner configuration does not match the final expanded-cap adapter"
        ) from exc
    if (
        variant_id
        != _variant_id(int(verified["batch_size"]), str(verified["thread_mode"]))
        or verified.get("winner_system_id") != adapter.WINNER_SYSTEM_ID
        or verified.get("max_events_per_segment") != 48
        or verified.get("retry_count") != 0
    ):
        raise ExpandedCapDevelopmentMatrixError("winner configuration scalar binding drifted")
    return copy.deepcopy(verified)


def _zero_byte_ambient_instruction_isolation(
    configuration: Mapping[str, Any],
) -> dict[str, Any]:
    """Prove ambient paths are reported but contribute zero model-visible bytes."""

    paths = configuration.get("effective_instruction_source_paths")
    runtime_lock = configuration.get("epoch4_runtime_lock")
    if (
        not isinstance(paths, list)
        or not paths
        or any(not isinstance(path, str) or not path for path in paths)
        or len(paths) != len(set(paths))
        or configuration.get("effective_instruction_sources_count") != len(paths)
        or configuration.get("effective_instruction_sources_sha256")
        != sha256_text(_canonical_json(paths))
        or configuration.get("project_instruction_content_byte_budget") != 0
        or configuration.get("project_instruction_content_included") is not False
        or configuration.get("effective_instruction_source_records_sha256")
        != configuration.get("historical_effective_instruction_source_records_sha256")
        or not _valid_sha256(
            configuration.get("historical_effective_instruction_source_records_sha256")
        )
        or configuration.get("adapter_module_sha256")
        != DEFAULT_ADAPTER_MODULE_SHA256
        or not isinstance(runtime_lock, Mapping)
    ):
        raise ExpandedCapDevelopmentMatrixError(
            "zero-byte ambient project-instruction isolation drifted"
        )
    runtime_path = _verified_artifact_record(
        runtime_lock,
        label="epoch-4 immutable runtime lock",
        allowed_root=PROJECT_ROOT,
    )
    if not _valid_sha256(configuration.get("context_control_overlay_sha256")):
        raise ExpandedCapDevelopmentMatrixError(
            "zero-byte context-control overlay binding is invalid"
        )
    return {
        "state": "verified_zero_model_visible_project_instruction_bytes",
        "reported_instruction_source_paths": copy.deepcopy(paths),
        "reported_instruction_sources_count": len(paths),
        "reported_instruction_source_paths_sha256": configuration[
            "effective_instruction_sources_sha256"
        ],
        "historical_instruction_source_records_sha256": configuration[
            "historical_effective_instruction_source_records_sha256"
        ],
        "project_instruction_content_byte_budget": 0,
        "project_instruction_content_included": False,
        "current_ambient_instruction_content_rehashed": False,
        "semantic_authority": (
            "immutable_epoch4_runtime_lock_request_artifacts_and_context_control_overlay; "
            "current_ambient_project_instruction_content_excluded_by_zero_byte_budget"
        ),
        "epoch4_runtime_lock": _record(runtime_path),
        "context_control_overlay_sha256": configuration[
            "context_control_overlay_sha256"
        ],
        "adapter_module_sha256": DEFAULT_ADAPTER_MODULE_SHA256,
    }


def _zero_byte_sidecar_instruction_contract() -> dict[str, Any]:
    configuration = adapter.build_frozen_configuration(
        batch_size=3, thread_mode="new_thread"
    )
    isolation = _zero_byte_ambient_instruction_isolation(configuration)
    return {
        "expected_path_set_sha256": isolation[
            "reported_instruction_source_paths_sha256"
        ],
        "instruction_sources_count": isolation[
            "reported_instruction_sources_count"
        ],
    }


def score_and_select_winner(
    *,
    shared: Mapping[str, Any],
    manifest_info: Mapping[str, Any],
    consensus: Mapping[str, Any],
    arms: Mapping[str, Mapping[str, Any]],
    cost_contract: Mapping[str, Any],
) -> dict[str, Any]:
    """Apply strict quality gates, then choose by quality and measured cost."""

    try:
        score = score_dev_shared_reference(
            private_mapping=shared["private_mapping"],
            membership_index=shared["membership_index"],
            consensus=consensus,
            manifest_rows=manifest_info["rows"],
        )
    except Exception as exc:
        raise ExpandedCapDevelopmentMatrixError("shared-reference scoring failed") from exc
    arm_results: dict[str, dict[str, Any]] = {}
    for variant_id, arm in sorted(arms.items()):
        system_id = _arm_system(variant_id, "normalized")
        system_score = score.get("systems", {}).get(system_id)
        if not isinstance(system_score, Mapping) or system_score.get("case_count") != 32:
            raise ExpandedCapDevelopmentMatrixError("shared score omitted a development arm")
        cases = system_score.get("cases")
        if not isinstance(cases, list) or [row.get("segment_id") for row in cases] != list(
            manifest_info["case_order"]
        ):
            raise ExpandedCapDevelopmentMatrixError("scored case order differs across arms")
        submitted_events = sum(
            int(shared["membership_index"]["system_cases"][system_id][segment_id][
                "submitted_event_count"
            ])
            for segment_id in manifest_info["case_order"]
        )
        exact_events = sum(
            int(shared["membership_index"]["system_cases"][system_id][segment_id][
                "exact_evidence_event_count"
            ])
            for segment_id in manifest_info["case_order"]
        )
        exact_rate = 1.0 if submitted_events == 0 else exact_events / submitted_events
        no_signal_cases = [row for row in cases if row.get("density_stratum") == "no_signal"]
        no_signal_failures = [
            str(row["segment_id"])
            for row in no_signal_cases
            if int(row.get("submitted_units") or 0) != 0
            or row.get("case_abstained_worst_case") is True
        ]
        macro_f1 = float(system_score["macro_f1"])
        cost = production_amortized_cost(
            arm_usage=arm["usage"],
            arm_segments=32,
            contract=cost_contract,
        )
        checks = {
            "strict_full_field_macro_f1_gte_0_97": macro_f1 >= QUALITY_THRESHOLD,
            "exact_evidence_rate_1": exact_rate == 1.0,
            "no_signal_case_count_4": len(no_signal_cases) == 4,
            "no_signal_false_positive_or_abstained_case_count_0": not no_signal_failures,
            "production_amortized_total_token_ratio_lte_0_28": cost["passed_lte_0_28"],
            "complete_measured_arm_accounting": arm["accounting_complete"] is True,
            "semantic_deterministic_pruning_false": arm["semantic_pruning"] is False,
        }
        arm_results[variant_id] = {
            "system_id": system_id,
            "checks": checks,
            "passed": all(checks.values()),
            "strict_full_field_macro_f1": round(macro_f1, 6),
            "exact_evidence_rate": round(exact_rate, 6),
            "submitted_event_count": submitted_events,
            "exact_evidence_event_count": exact_events,
            "no_signal_failed_segment_ids": no_signal_failures,
            "cost": cost,
            "case_metrics": copy.deepcopy(cases),
        }
    passing = [variant_id for variant_id, value in arm_results.items() if value["passed"]]
    if not passing:
        raise ExpandedCapDevelopmentMatrixError("no arm passed every quality and cost gate")
    ranked = sorted(
        passing,
        key=lambda variant_id: (
            -float(arm_results[variant_id]["strict_full_field_macro_f1"]),
            int(arm_results[variant_id]["cost"]["candidate_end_to_end_tokens"]),
        ),
    )
    best = ranked[0]
    best_key = (
        arm_results[best]["strict_full_field_macro_f1"],
        arm_results[best]["cost"]["candidate_end_to_end_tokens"],
    )
    tied = [
        variant_id
        for variant_id in ranked
        if (
            arm_results[variant_id]["strict_full_field_macro_f1"],
            arm_results[variant_id]["cost"]["candidate_end_to_end_tokens"],
        )
        == best_key
    ]
    if len(tied) != 1:
        raise ExpandedCapDevelopmentMatrixError(
            "winner is not unique after quality-first measured-cost-second ranking"
        )
    return {
        "schema_version": SCORE_VERSION,
        "state": "winner_gate_passed",
        "quality_threshold": QUALITY_THRESHOLD,
        "token_ratio_threshold": TOKEN_RATIO_THRESHOLD,
        "case_count": 32,
        "arm_count": 6,
        "shared_score": score,
        "arms": arm_results,
        "passing_arms": passing,
        "ranking_policy": "highest_strict_macro_f1_then_lowest_measured_end_to_end_tokens",
        "winner_variant_id": best,
        "winner_unique": True,
        "holdout_inspected": False,
        "production_mutated": False,
    }


async def run_expanded_cap_development_matrix(
    conn: Any,
    *,
    output_dir: Path,
    capacity_admission_provider: Callable[[Mapping[str, Any]], Any],
    epoch6_receipt_path: Path = DEFAULT_EPOCH6_RECEIPT_PATH,
    epoch6_receipt_sha256: str = DEFAULT_EPOCH6_RECEIPT_SHA256,
    manifest_path: Path = DEFAULT_MANIFEST_PATH,
    manifest_sha256: str = DEFAULT_MANIFEST_SHA256,
    judge_calibration_path: Path = DEFAULT_JUDGE_CALIBRATION_PATH,
    judge_calibration_sha256: str = DEFAULT_JUDGE_CALIBRATION_SHA256,
    context_usage_recovery_path: Path = DEFAULT_CONTEXT_USAGE_RECOVERY_PATH,
    cost_contract: Mapping[str, Any] | None = None,
    window_count: int = 4,
    context_chars: int = 900,
    concurrency: int = 1,
    fixture_mode: bool = False,
    timeout_seconds: float = 1200.0,
    context_timeout_seconds: float = 1200.0,
    judge_timeout_seconds: float = 1200.0,
    episode_loader: Callable[..., Any] = evaluation._load_prepared_episodes,
    context_preparation_runner: Callable[..., Any] | None = None,
    arm_runner: Callable[..., Any] = adapter.run_episode_batch_arm,
    arm_output_loader: Callable[..., Any] = load_adapter_arm_outputs,
    judge_runner: Callable[..., Any] = default_shared_judge_runner,
    allowed_record_root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    """Run six fresh arms and freeze one winner, or raise without authorization."""

    if concurrency != 1:
        raise ExpandedCapDevelopmentMatrixError("matrix concurrency is frozen to one")
    if not isinstance(fixture_mode, bool):
        raise ExpandedCapDevelopmentMatrixError("fixture_mode must be boolean")
    final_root = Path(output_dir).expanduser().resolve()
    epoch6 = verify_epoch6_passed_receipt(
        epoch6_receipt_path,
        expected_sha256=epoch6_receipt_sha256,
        allowed_record_root=allowed_record_root,
    )
    manifest_info = verify_development_manifest(
        manifest_path,
        expected_sha256=manifest_sha256,
    )
    calibration = verify_judge_calibration_receipt(
        judge_calibration_path,
        expected_sha256=judge_calibration_sha256,
        allowed_record_root=allowed_record_root,
    )
    episodes = prepare_development_episodes(
        conn,
        manifest=manifest_info["payload"],
        manifest_rows=manifest_info["rows"],
        window_count=window_count,
        context_chars=context_chars,
        episode_loader=episode_loader,
    )
    context_plan = inspect_development_context_preparation(
        episodes=episodes,
        manifest_info=manifest_info,
        require_manifest_artifacts=episode_loader is evaluation._load_prepared_episodes,
    )
    context_report: dict[str, Any] | None = None
    if context_plan["semantic_turn_count"]:
        if context_preparation_runner is None:
            raise ExpandedCapDevelopmentMatrixError(
                "development context preparation is required before adapter precommit"
            )
        report_value = await _maybe_await(
            context_preparation_runner(
                conn=conn,
                episodes=episodes,
                manifest_info=manifest_info,
                plan=context_plan,
                output_dir=final_root / "context-preparation",
                timeout_seconds=context_timeout_seconds,
                fixture_mode=fixture_mode,
            )
        )
        adopted_context = _validate_context_preparation_report(
            report_value,
            plan=context_plan,
            episodes=episodes,
            output_dir=final_root / "context-preparation",
            fixture_mode=fixture_mode,
        )
        context_report = adopted_context["report"]
        episodes = adopted_context["episodes"]
    precommit = precommit_six_arms(
        output_dir=final_root / "precommit",
        episodes=episodes,
        case_order=manifest_info["case_order"],
        epoch6_sha256=epoch6["sha256"],
        manifest_sha256=manifest_info["sha256"],
        window_count=window_count,
        context_chars=context_chars,
    )
    ambient_instruction_isolation = copy.deepcopy(
        precommit["precommit"]["ambient_instruction_isolation"]
    )
    if cost_contract is None:
        from .app_server_dev_selection import load_exact_cost_contract

        cost_contract = load_exact_cost_contract(Path(context_usage_recovery_path))
    if not isinstance(cost_contract, Mapping):
        raise ExpandedCapDevelopmentMatrixError("production cost contract is missing")
    if not _valid_usage(cost_contract.get("production_amortized_context_usage")):
        raise ExpandedCapDevelopmentMatrixError(
            "frozen production-amortized context allocation is incomplete"
        )
    context_delta_usage = (
        copy.deepcopy(context_report["usage"])
        if context_report is not None
        else {field: 0 for field in USAGE_FIELDS}
    )
    context_cost_treatment = {
        "schema_version": CONTEXT_PREPARATION_REPORT_VERSION,
        "development_context_preparation_required": context_report is not None,
        "measured_development_schema_delta_validation_usage": context_delta_usage,
        "measured_development_schema_delta_validation_calls": (
            int(context_report["attempted_calls"]) if context_report is not None else 0
        ),
        "included_in_epoch7_total_accounting": context_report is not None,
        "added_to_production_amortized_context_numerator": False,
        "frozen_production_amortized_context_usage_retained": copy.deepcopy(
            dict(cost_contract["production_amortized_context_usage"])
        ),
        "exact_recovered_context_usage": copy.deepcopy(
            cost_contract.get("exact_context_usage")
        ),
        "context_usage_recovery_sha256": cost_contract.get(
            "context_usage_recovery_sha256"
        ),
        "allocation_reason": (
            "the bounded delta calls repair development evaluation artifacts once and are not "
            "recurring per-production workload; the exact recovered production allocation remains "
            "the shared numerator for both systems"
        ),
    }

    arms: dict[str, dict[str, Any]] = {}
    all_turn_ids: set[str] = set()
    all_thread_ids: set[str] = set()
    admission_ids: set[str] = set()
    for batch_size, thread_mode in EXPECTED_ARMS:
        variant_id = _variant_id(batch_size, thread_mode)
        precommitted = precommit["arm_rows"][variant_id]
        requests = precommit["prepared_by_arm"][variant_id]
        arm_root = Path(precommitted["arm_root"]).resolve()
        episode_ids = [str(episode["episode_id"]) for episode in episodes]
        arm_context = {
            "variant_id": variant_id,
            "batch_size": batch_size,
            "thread_mode": thread_mode,
            "winner_system_id": adapter.WINNER_SYSTEM_ID,
            "model": adapter.MODEL,
            "effort": adapter.EFFORT,
            "concurrency": concurrency,
            "episode_ids": episode_ids,
            "episode_count": len(episode_ids),
            "segment_count": 32,
            "batch_count": len(requests),
            "configuration": copy.deepcopy(
                _load_object(Path(precommitted["configuration"]["path"]), "arm configuration")
            ),
            "arm_root": str(arm_root),
            "fixture_mode": fixture_mode,
        }
        configuration = arm_context["configuration"]
        report_path = arm_root / "report.json"
        adopted = report_path.is_file()
        if arm_root.exists() and not adopted:
            raise ExpandedCapDevelopmentMatrixError(
                f"{variant_id} has partial or ambiguous arm evidence; semantic replay is prohibited"
            )
        if adopted:
            report = _load_object(report_path, f"{variant_id} adopted arm report")
            admission_path = arm_root / "capacity-admission.json"
            capacity_receipt = _load_object(
                admission_path, f"{variant_id} adopted capacity admission"
            )
            capacity_sha = adapter.capacity_admission_sha256(capacity_receipt)
        else:
            admission_value = await _maybe_await(capacity_admission_provider(arm_context))
            capacity_receipt, capacity_sha = _capacity_wrapper(admission_value, fixture_mode)
        try:
            capacity_validator = (
                _validate_historical_capacity_admission
                if adopted
                else adapter.validate_capacity_admission
            )
            capacity_validator(
                capacity_receipt,
                expected_sha256=capacity_sha,
                batch_size=batch_size,
                thread_mode=thread_mode,
                concurrency=concurrency,
                episode_ids=episode_ids,
                segment_count=32,
                batch_count=len(requests),
                fixture_mode=fixture_mode,
            )
        except Exception as exc:
            raise ExpandedCapDevelopmentMatrixError(
                f"{variant_id} reserve-capacity admission failed before semantic turns"
            ) from exc
        if capacity_receipt["admission_id"] in admission_ids:
            raise ExpandedCapDevelopmentMatrixError("capacity admission receipt was replayed")
        admission_ids.add(str(capacity_receipt["admission_id"]))
        if not adopted:
            report_value = await _maybe_await(
                arm_runner(
                    episodes,
                    output_dir=arm_root,
                    batch_size=batch_size,
                    thread_mode=thread_mode,
                    capacity_admission=capacity_receipt,
                    capacity_admission_sha256=capacity_sha,
                    frozen_configuration=configuration,
                    concurrency=concurrency,
                    fixture_mode=fixture_mode,
                    timeout_seconds=timeout_seconds,
                )
            )
            if not isinstance(report_value, Mapping):
                raise ExpandedCapDevelopmentMatrixError(
                    f"{variant_id} arm runner returned no report"
                )
            report = copy.deepcopy(dict(report_value))
            if report_path.exists():
                if _canonical_json(_load_object(report_path, "arm report")) != _canonical_json(
                    report
                ):
                    raise ExpandedCapDevelopmentMatrixError(
                        "returned arm report differs from checkpoint"
                    )
            else:
                _write_new_json(report_path, report)
        accounting = validate_complete_arm_report(
            report,
            requests=requests,
            configuration=configuration,
            capacity_receipt=capacity_receipt,
            capacity_sha256=capacity_sha,
            arm_dir=arm_root,
            fixture_mode=fixture_mode,
        )
        turn_ids = set(accounting["turn_ids"])
        thread_ids = set(accounting["thread_ids"])
        if all_turn_ids & turn_ids or all_thread_ids & thread_ids:
            raise ExpandedCapDevelopmentMatrixError("thread or turn lineage was replayed across arms")
        all_turn_ids.update(turn_ids)
        all_thread_ids.update(thread_ids)
        loaded = await _maybe_await(
            arm_output_loader(
                arm_dir=arm_root,
                report=report,
                requests=requests,
                episodes=episodes,
                expected_case_order=manifest_info["case_order"],
            )
        )
        outputs = _validate_arm_cases(loaded, expected_case_order=manifest_info["case_order"])
        winner_config = _winner_config(
            variant_id=variant_id,
            configuration=configuration,
            window_count=window_count,
            context_chars=context_chars,
        )
        arms[variant_id] = {
            "variant_id": variant_id,
            "batch_size": batch_size,
            "thread_mode": thread_mode,
            "configuration": configuration,
            "configuration_record": precommitted["configuration"],
            "report": report,
            "report_record": _record(report_path),
            "capacity_admission_sha256": capacity_sha,
            "capacity_binding": copy.deepcopy(report["capacity_binding"]),
            "usage": accounting["usage"],
            "accounting_complete": True,
            "semantic_pruning": False,
            "cases": outputs["cases"],
            "artifacts": outputs["artifacts"],
            "winner_config": winner_config,
            "adopted_without_semantic_calls": adopted,
        }

    shared = assemble_shared_augmented_reference(
        output_dir=final_root / "shared-reference",
        manifest_info=manifest_info,
        episodes=episodes,
        arms=arms,
        seed=f"expanded-cap-matrix:{epoch6['sha256']}:{manifest_info['sha256']}",
    )
    judge_value = await _maybe_await(
        judge_runner(
            pool=shared["pool"],
            pool_path=Path(shared["pool_record"]["path"]),
            output_dir=final_root / "judge",
            calibration=calibration,
            timeout_seconds=judge_timeout_seconds,
            fixture_mode=fixture_mode,
        )
    )
    judge = validate_shared_judge_result(
        judge_value,
        pool=shared["pool"],
        calibration_sha256=calibration["sha256"],
    )
    judge_root = final_root / "judge"
    judge_root.mkdir(parents=True, exist_ok=True)
    consensus_path = judge_root / "consensus.private.json"
    if consensus_path.exists():
        if _canonical_json(_load_object(consensus_path, "judge consensus")) != _canonical_json(
            judge["consensus"]
        ):
            raise ExpandedCapDevelopmentMatrixError("judge consensus checkpoint drifted")
    else:
        _write_or_verify_json(consensus_path, judge["consensus"])
    public_judge_report = {
        key: copy.deepcopy(value)
        for key, value in judge.items()
        if key not in {"consensus", "report"}
    }
    judge_report_path = judge_root / "matrix-judge-report.json"
    if judge_report_path.exists():
        if _canonical_json(_load_object(judge_report_path, "matrix judge report")) != _canonical_json(
            public_judge_report
        ):
            raise ExpandedCapDevelopmentMatrixError("matrix judge report drifted")
    else:
        _write_or_verify_json(judge_report_path, public_judge_report)
    selection = score_and_select_winner(
        shared=shared,
        manifest_info=manifest_info,
        consensus=judge["consensus"],
        arms=arms,
        cost_contract=cost_contract,
    )
    selection["development_context_cost_treatment"] = copy.deepcopy(
        context_cost_treatment
    )
    for arm_score in selection["arms"].values():
        arm_score["cost"]["development_context_cost_treatment"] = copy.deepcopy(
            context_cost_treatment
        )
    score_path = final_root / "score-report.json"
    _write_or_verify_json(score_path, selection)
    winner_id = str(selection["winner_variant_id"])
    winner_arm = arms[winner_id]
    winner_metrics = selection["arms"][winner_id]
    config = winner_arm["winner_config"]
    config_sha = sha256_text(_canonical_json(config))
    frozen_hashes = {
        "epoch6_receipt": epoch6["sha256"],
        "development_manifest": manifest_info["sha256"],
        "shared_reference_seed": manifest_info["reference_sha256"],
        "judge_calibration": calibration["sha256"],
        "matrix_precommit": precommit["precommit_record"]["sha256"],
        "shared_augmented_reference": shared["pool_record"]["sha256"],
        "shared_augmented_reference_mapping": shared["mapping_record"]["sha256"],
        "shared_augmented_reference_membership": shared["membership_record"]["sha256"],
        "judge_report": _sha256_file(judge_report_path),
        "judge_consensus": _sha256_file(consensus_path),
        "score_report": _sha256_file(score_path),
        "epoch4_runtime_lock": ambient_instruction_isolation["epoch4_runtime_lock"][
            "sha256"
        ],
        "epoch4_zero_byte_context_control_overlay": ambient_instruction_isolation[
            "context_control_overlay_sha256"
        ],
        **{
            f"arm_report_{variant_id}": arm["report_record"]["sha256"]
            for variant_id, arm in sorted(arms.items())
        },
        **{
            f"arm_configuration_{variant_id}": sha256_text(
                _canonical_json(arm["configuration"])
            )
            for variant_id, arm in sorted(arms.items())
        },
        **{
            f"arm_configuration_file_{variant_id}": arm["configuration_record"]["sha256"]
            for variant_id, arm in sorted(arms.items())
        },
        "episode_batch_adapter_module": _sha256_file(Path(str(adapter.__file__)).resolve()),
        "matrix_coordinator_module": _sha256_file(Path(__file__).resolve()),
        "evaluation_loader_module": _sha256_file(Path(str(evaluation.__file__)).resolve()),
        "semantic_judge_module": _sha256_file(Path(str(app_server_llm_judge.__file__)).resolve()),
    }
    if context_report is not None:
        frozen_hashes["development_context_preparation_plan"] = _sha256_file(
            final_root / "context-preparation" / "plan.json"
        )
        frozen_hashes["development_context_preparation_report"] = _sha256_file(
            final_root / "context-preparation" / "report.json"
        )
        for outcome in context_report["outcomes"]:
            episode_id = str(outcome["episode_id"])
            frozen_hashes[f"development_context_overlay_{episode_id}"] = str(
                outcome["context_overlay"]["sha256"]
            )
    result = {
        "schema_version": FROZEN_WINNER_VERSION,
        "selection_status": "frozen_winner",
        "winner_frozen": True,
        "selection_spec_sha256": precommit["precommit_record"]["sha256"],
        "gates": {
            "quality_noninferior": True,
            "strict_full_field_macro_f1_gte_0_97": True,
            "exact_evidence_rate_1": True,
            "no_signal_gate": True,
            "production_amortized_total_token_ratio_lte_0_28": True,
            "calibrated_support_alignment_judge": True,
            "ab_ba_order_balanced": True,
            "complete_six_arm_accounting": True,
            "development_context_authority_complete": True,
            "development_context_semantic_defaults_false": True,
            "ambient_project_instruction_content_byte_budget_zero": True,
            "ambient_project_instruction_content_included_false": True,
            "semantic_deterministic_pruning_false": True,
        },
        "winner": {
            "variant_id": winner_id,
            "winner_system_id": adapter.WINNER_SYSTEM_ID,
            "batch_size": winner_arm["batch_size"],
            "thread_mode": winner_arm["thread_mode"],
            "model": config["model"],
            "reasoning_effort": config["effort"],
            "concurrency": 1,
            "retry_count": 0,
            "window_count": window_count,
            "context_chars": context_chars,
            "max_events_per_segment": config["max_events_per_segment"],
            "frozen_configuration": config,
            "frozen_configuration_sha256": config_sha,
            "report_sha256": winner_arm["report_record"]["sha256"],
            "production_amortized_total_token_ratio": winner_metrics["cost"][
                "production_amortized_total_token_ratio"
            ],
            "quality": winner_metrics,
            "cost": winner_metrics["cost"],
        },
        "selection_policy": (
            "highest_strict_full_field_macro_f1_then_lowest_measured_production_"
            "amortized_end_to_end_tokens_unique_winner"
        ),
        "development_context_preparation": {
            "required": context_report is not None,
            "semantic_turn_count": (
                int(context_report["attempted_calls"]) if context_report is not None else 0
            ),
            "usage": context_delta_usage,
            "cost_treatment": context_cost_treatment,
            "plan_sha256": (
                frozen_hashes.get("development_context_preparation_plan")
            ),
            "report_sha256": (
                frozen_hashes.get("development_context_preparation_report")
            ),
            "semantic_defaults_used": False,
            "development_only": True,
        },
        "ambient_instruction_isolation": ambient_instruction_isolation,
        "frozen_artifact_hashes": frozen_hashes,
        "production_changed": False,
        "production_mutated": False,
        "holdout_inspected": False,
        "holdout_frozen": False,
        "holdout_preparation_authorized": True,
        "holdout_model_calls_authorized": False,
    }
    winner_path = final_root / "frozen-winner-v3.json"
    _write_or_verify_json(winner_path, result)
    try:
        verified = load_frozen_winner(winner_path)
    except ValueError as exc:
        raise ExpandedCapDevelopmentMatrixError("frozen winner v3 shape verification failed") from exc
    if verified["payload"] != result:
        raise ExpandedCapDevelopmentMatrixError("frozen winner re-read changed content")
    return result


def _partial_matrix_accounting(output_dir: Path) -> dict[str, Any]:
    root = Path(output_dir).expanduser().resolve()
    usage_values: list[Mapping[str, Any]] = []
    semantic_calls = 0
    wall_seconds = 0.0
    context_report_count = 0
    context_outcome_count = 0
    arm_report_count = 0
    judge_shard_count = 0
    context_report_path = root / "context-preparation" / "report.json"
    if context_report_path.is_file():
        context_report = _load_object(
            context_report_path, "partial context preparation report"
        )
        if _valid_usage(context_report.get("usage")):
            usage_values.append(context_report["usage"])
        calls = context_report.get("attempted_calls")
        if isinstance(calls, int) and not isinstance(calls, bool) and calls >= 0:
            semantic_calls += calls
        if isinstance(context_report.get("wall_elapsed_seconds"), (int, float)):
            wall_seconds += _nonnegative_number(
                context_report["wall_elapsed_seconds"],
                "partial context preparation wall accounting",
            )
        outcomes = context_report.get("outcomes")
        context_outcome_count = len(outcomes) if isinstance(outcomes, list) else 0
        context_report_count = 1
    else:
        context_root = root / "context-preparation" / "episodes"
        for attempt_path in sorted(context_root.glob("*/attempt.json")):
            semantic_calls += 1
            outcome_path = attempt_path.parent / "outcome.json"
            sidecar_path = attempt_path.parent / "turn-sidecar.json"
            if outcome_path.is_file():
                outcome = _load_object(outcome_path, "partial context outcome")
                if _valid_usage(outcome.get("usage")):
                    usage_values.append(outcome["usage"])
                wall = outcome.get("wall_elapsed_seconds")
                if isinstance(wall, (int, float)) and not isinstance(wall, bool):
                    wall_seconds += _nonnegative_number(
                        wall, "partial context outcome wall accounting"
                    )
                context_outcome_count += 1
            else:
                sidecar_usage, sidecar_wall = _partial_sidecar_accounting(sidecar_path)
                if sidecar_usage is not None:
                    usage_values.append(sidecar_usage)
                if sidecar_wall is not None:
                    wall_seconds += sidecar_wall
    for path in sorted((root / "arms").glob("*/report.json")):
        report = _load_object(path, "partial arm report")
        if _valid_usage(report.get("usage")):
            usage_values.append(report["usage"])
        calls = report.get("attempted_calls")
        if isinstance(calls, int) and not isinstance(calls, bool) and calls >= 0:
            semantic_calls += calls
        if isinstance(report.get("wall_elapsed_seconds"), (int, float)):
            wall_seconds += _nonnegative_number(
                report["wall_elapsed_seconds"], "partial arm wall accounting"
            )
        arm_report_count += 1
    for path in sorted((root / "judge").glob("shard-*/judge/report.json")):
        report = _load_object(path, "partial judge shard report")
        if _valid_usage(report.get("usage")):
            usage_values.append(report["usage"])
        calls = report.get("variant_count")
        if isinstance(calls, int) and not isinstance(calls, bool) and calls >= 0:
            semantic_calls += calls
        for sidecar_value in report.get("sidecar_paths") or []:
            sidecar_path = Path(str(sidecar_value)).expanduser().resolve()
            if sidecar_path.is_file():
                sidecar = _load_object(sidecar_path, "partial judge sidecar")
                if isinstance(sidecar.get("wall_elapsed_seconds"), (int, float)):
                    wall_seconds += _nonnegative_number(
                        sidecar["wall_elapsed_seconds"],
                        "partial judge wall accounting",
                    )
        judge_shard_count += 1
    usage = _sum_usage(usage_values) if usage_values else {field: 0 for field in USAGE_FIELDS}
    return {
        "usage": usage,
        "semantic_model_call_count": semantic_calls,
        "wall_elapsed_seconds": round(wall_seconds, 3),
        "completed_context_report_count": context_report_count,
        "context_outcome_count": context_outcome_count,
        "completed_arm_report_count": arm_report_count,
        "completed_judge_shard_count": judge_shard_count,
    }


def write_epoch7_plan_step_receipt(
    *,
    path: Path,
    thread_id: str,
    state: str,
    output_dir: Path,
    result: Mapping[str, Any] | None = None,
    error: BaseException | None = None,
) -> dict[str, Any]:
    """Write the caller-routed epoch-7 terminal receipt exactly once."""

    if not isinstance(thread_id, str) or not thread_id.strip():
        raise ExpandedCapDevelopmentMatrixError("epoch-7 receipt thread_id is required")
    if state not in {"passed", "rejected", "waiting"}:
        raise ExpandedCapDevelopmentMatrixError("epoch-7 receipt terminal state is invalid")
    if state == "passed" and not isinstance(result, Mapping):
        raise ExpandedCapDevelopmentMatrixError("passed epoch-7 receipt requires a winner")
    target = Path(path).expanduser().resolve()
    if target.exists():
        raise ExpandedCapDevelopmentMatrixError("epoch-7 terminal receipt already exists")
    accounting = _partial_matrix_accounting(output_dir)
    root = Path(output_dir).expanduser().resolve()
    records: dict[str, Any] = {}
    for key, artifact in (
        ("context_preparation_plan", root / "context-preparation" / "plan.json"),
        ("context_preparation_report", root / "context-preparation" / "report.json"),
        ("matrix_precommit", root / "precommit" / "matrix-precommit.json"),
        ("shared_reference", root / "shared-reference" / "assembly-report.json"),
        ("judge_report", root / "judge" / "matrix-judge-report.json"),
        ("score_report", root / "score-report.json"),
        ("frozen_winner", root / "frozen-winner-v3.json"),
    ):
        if artifact.is_file():
            records[key] = _record(artifact)
    passed = state == "passed"
    terminal_reason = (
        "epoch7_six_arm_development_winner_frozen"
        if passed
        else "epoch7_capacity_waiting_before_next_semantic_client"
        if state == "waiting"
        else "epoch7_development_matrix_rejected"
    )
    ambient_isolation = None
    if isinstance(result, Mapping):
        ambient_isolation = copy.deepcopy(result.get("ambient_instruction_isolation"))
    elif (root / "precommit" / "matrix-precommit.json").is_file():
        ambient_isolation = copy.deepcopy(
            _load_object(
                root / "precommit" / "matrix-precommit.json",
                "epoch-7 ambient instruction isolation",
            ).get("ambient_instruction_isolation")
        )
    if passed and (
        not isinstance(ambient_isolation, Mapping)
        or ambient_isolation.get("project_instruction_content_byte_budget") != 0
        or ambient_isolation.get("project_instruction_content_included") is not False
    ):
        raise ExpandedCapDevelopmentMatrixError(
            "passed epoch-7 receipt lacks zero-byte ambient instruction isolation"
        )
    payload = {
        "schema_version": EPOCH7_RECEIPT_VERSION,
        "thread_id": thread_id.strip(),
        "plan_epoch": EPOCH7_PLAN_EPOCH,
        "step_id": EPOCH7_STEP_ID,
        "state": state,
        "terminal_reason": terminal_reason,
        "terminal_at": now_iso(),
        "development_quality_passed": passed,
        "winner_frozen": passed,
        "development_winner_frozen": passed,
        "winner_schema_version": result.get("schema_version") if passed else None,
        "winner_variant_id": (
            result.get("winner", {}).get("variant_id") if passed else None
        ),
        "accounting_complete": passed,
        "usage_status": "complete" if passed else "partial_or_unknown",
        "usage": accounting["usage"],
        "semantic_attempt_count": accounting["semantic_model_call_count"],
        "semantic_model_call_count": accounting["semantic_model_call_count"],
        "semantic_model_call_cap": accounting["semantic_model_call_count"] if passed else None,
        "semantic_retry_count": 0,
        "strict_thread_turn_call_accounting": passed,
        "ambient_instruction_isolation": ambient_isolation,
        "ambient_project_instruction_content_byte_budget_zero": bool(
            isinstance(ambient_isolation, Mapping)
            and ambient_isolation.get("project_instruction_content_byte_budget") == 0
        ),
        "ambient_project_instruction_content_included": (
            ambient_isolation.get("project_instruction_content_included")
            if isinstance(ambient_isolation, Mapping)
            else None
        ),
        "wall_elapsed_seconds": accounting["wall_elapsed_seconds"],
        "completed_context_report_count": accounting[
            "completed_context_report_count"
        ],
        "context_outcome_count": accounting["context_outcome_count"],
        "completed_arm_report_count": accounting["completed_arm_report_count"],
        "completed_judge_shard_count": accounting["completed_judge_shard_count"],
        "failed_checks": [] if passed else [type(error).__name__ if error else state],
        "holdout": False,
        "holdout_authorized": False,
        "holdout_inspected": False,
        "holdout_frozen": False,
        "production": False,
        "production_mutated": False,
        "records": records,
    }
    _write_new_json(target, payload)
    return payload


def _open_read_only_db(path: Path) -> sqlite3.Connection:
    target = Path(path).expanduser().resolve()
    if not target.is_file():
        raise ExpandedCapDevelopmentMatrixError(f"factory DB is missing: {target}")
    connection = sqlite3.connect(f"file:{target}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only = ON")
    return connection


def _cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Dry-preflight or run the resumable epoch-7 expanded-cap matrix"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name in ("preflight", "run"):
        command = subparsers.add_parser(name)
        command.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
        command.add_argument("--epoch6-receipt", type=Path, default=DEFAULT_EPOCH6_RECEIPT_PATH)
        command.add_argument("--epoch6-sha256", default=DEFAULT_EPOCH6_RECEIPT_SHA256)
        command.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST_PATH)
        command.add_argument("--manifest-sha256", default=DEFAULT_MANIFEST_SHA256)
        command.add_argument(
            "--judge-calibration", type=Path, default=DEFAULT_JUDGE_CALIBRATION_PATH
        )
        command.add_argument(
            "--judge-calibration-sha256", default=DEFAULT_JUDGE_CALIBRATION_SHA256
        )
        command.add_argument("--window-count", type=int, default=4)
        command.add_argument("--context-chars", type=int, default=900)
    run_parser = subparsers.choices["run"]
    run_parser.add_argument("--output-dir", type=Path, required=True)
    run_parser.add_argument("--plan-receipt", type=Path, required=True)
    run_parser.add_argument("--thread-id", required=True)
    run_parser.add_argument("--wait-for-capacity", action="store_true")
    run_parser.add_argument("--capacity-recheck-seconds", type=int, default=60)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _cli_parser().parse_args(argv)
    try:
        _reject_external_auth_material()
        connection = _open_read_only_db(args.db)
        try:
            if args.command == "preflight":
                preflight = build_matrix_dry_preflight(
                    connection,
                    epoch6_receipt_path=args.epoch6_receipt,
                    epoch6_receipt_sha256=args.epoch6_sha256,
                    manifest_path=args.manifest,
                    manifest_sha256=args.manifest_sha256,
                    judge_calibration_path=args.judge_calibration,
                    judge_calibration_sha256=args.judge_calibration_sha256,
                    window_count=args.window_count,
                    context_chars=args.context_chars,
                )
                print(
                    json.dumps(
                        {
                            key: value
                            for key, value in preflight.items()
                            if key not in {"episodes", "manifest_info"}
                        },
                        sort_keys=True,
                    )
                )
                return 0
            if args.plan_receipt.expanduser().resolve().exists():
                raise ExpandedCapDevelopmentMatrixError(
                    "caller-specified epoch-7 plan receipt already exists"
                )
            coordinator = LiveMatrixCapacityCoordinator(
                control_dir=args.output_dir / "live-capacity",
                wait_for_capacity=args.wait_for_capacity,
                recheck_seconds=args.capacity_recheck_seconds,
            )
            try:
                result = asyncio.run(
                    run_expanded_cap_development_matrix(
                        connection,
                        output_dir=args.output_dir,
                        capacity_admission_provider=coordinator.arm_capacity_admission,
                        epoch6_receipt_path=args.epoch6_receipt,
                        epoch6_receipt_sha256=args.epoch6_sha256,
                        manifest_path=args.manifest,
                        manifest_sha256=args.manifest_sha256,
                        judge_calibration_path=args.judge_calibration,
                        judge_calibration_sha256=args.judge_calibration_sha256,
                        window_count=args.window_count,
                        context_chars=args.context_chars,
                        fixture_mode=False,
                        context_preparation_runner=coordinator.run_context_preparation,
                        arm_runner=adapter.run_episode_batch_arm,
                        arm_output_loader=load_adapter_arm_outputs,
                        judge_runner=coordinator.run_shared_judge,
                    )
                )
            except ExpandedCapMatrixCapacityUnavailable as exc:
                print(
                    json.dumps(
                        {
                            "ok": False,
                            "state": "capacity_waiting_resumable_before_semantic_client",
                            "error_class": type(exc).__name__,
                            "plan_receipt_written": False,
                        },
                        sort_keys=True,
                    )
                )
                return 75
            except Exception as exc:
                write_epoch7_plan_step_receipt(
                    path=args.plan_receipt,
                    thread_id=args.thread_id,
                    state="rejected",
                    output_dir=args.output_dir,
                    error=exc,
                )
                raise
            receipt = write_epoch7_plan_step_receipt(
                path=args.plan_receipt,
                thread_id=args.thread_id,
                state="passed",
                output_dir=args.output_dir,
                result=result,
            )
            print(
                json.dumps(
                    {
                        "ok": True,
                        "state": receipt["state"],
                        "winner_variant_id": receipt["winner_variant_id"],
                        "plan_receipt": str(args.plan_receipt.expanduser().resolve()),
                    },
                    sort_keys=True,
                )
            )
            return 0
        finally:
            connection.close()
    except Exception as exc:
        print(
            json.dumps(
                {"ok": False, "state": "rejected", "error_class": type(exc).__name__},
                sort_keys=True,
            )
        )
        return 1


run = run_expanded_cap_development_matrix


if __name__ == "__main__":
    raise SystemExit(main())
