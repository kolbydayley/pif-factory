from __future__ import annotations

"""Fail-closed development selection over the Codex app-server matrix.

The module freezes one shared, blinded witness pool, calibrates the LLM judge,
judges every development segment, scores all systems against the same augmented
reference, and only then selects on measured production-amortized tokens.  A
terminal interrupted arm is frozen as ITT exploratory evidence but cannot
enter the reference, score set, or winner set.  This module
does not modify production or prepare/run the untouched holdout.
"""

import argparse
import asyncio
import hashlib
import json
import math
import random
import sqlite3
from collections import Counter, defaultdict
from copy import deepcopy
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from .app_server_evaluation import (
    APP_SERVER_CORE_ARM_VERSION,
    APP_SERVER_DEVELOPMENT_MANIFEST_V2,
    normalize_episode_batch_output,
)
from .app_server_holdout import FROZEN_WINNER_VERSION
from .app_server_capacity import CapacityGatedCodexAppServerClient
from .app_server_llm_judge import (
    JUDGE_CONSENSUS_VERSION,
    JudgeArtifactError,
    WITNESS_MAPPING_VERSION,
    make_shared_witness_pool,
    run_app_server_judge_calibration,
    run_app_server_semantic_judge,
    validate_shared_witness_pool,
    write_immutable_json,
)
from .codex_app_server import APP_SERVER_CLIENT_VERSION, CodexAppServerClient
from .efficient_backtest import build_windowed_segment_packet
from .paths import db_path, root as factory_root
from .util import sha256_text


DEV_SELECTION_VERSION = "pif_app_server_dev_selection_v2"
DEV_POOL_ASSEMBLY_VERSION = "pif_app_server_dev_witness_assembly_v2"
DEV_MEMBERSHIP_VERSION = "pif_app_server_dev_membership_index_v2"
DEV_FULL_JUDGE_VERSION = "pif_app_server_dev_full_judge_v2"
DEV_SCORE_VERSION = "pif_app_server_dev_shared_reference_score_v2"

BASELINE_RAW_SYSTEM = "baseline_raw_label_run"
BASELINE_REPAIRED_SYSTEM = "baseline_repaired_db"
ALL_MATRIX_ARMS = tuple(
    "batch_%d_%s" % (batch_size, thread_mode)
    for batch_size in (3, 5, 8)
    for thread_mode in ("new_thread", "same_thread")
)
INTERRUPTED_ARM = "batch_5_same_thread"
EXPECTED_ARMS = ALL_MATRIX_ARMS
INTERRUPTED_SELECTABLE_ARMS = tuple(item for item in ALL_MATRIX_ARMS if item != INTERRUPTED_ARM)
EXPECTED_CONTEXT_USAGE = {
    "input_tokens": 3713379,
    "output_tokens": 257450,
    "total_tokens": 3970829,
}
EXPECTED_PRODUCTION_CONTEXT_TOTAL = 600538
QUALITY_GATES = {
    "bootstrap_iterations": 10000,
    "bootstrap_confidence": 0.95,
    "bootstrap_seed": "windowed-paired-cluster-bootstrap-v2",
    "max_paired_f1_drop": 0.03,
    "max_macro_f1_drop": 0.05,
    "max_source_f1_drop": 0.05,
    "max_no_signal_false_positive_rate": 0.05,
    "max_abstained_case_rate": 0.05,
    "max_production_amortized_total_token_ratio": 0.28,
}
FROZEN_EXTRACTION_CONFIG_FIELDS = (
    "variant_id",
    "batch_size",
    "thread_mode",
    "model",
    "reasoning_effort",
    "concurrency",
    "retry_count",
    "window_count",
    "context_chars",
    "max_events_per_segment",
    "output_schema_version",
    "transport_client_version",
    "guideline_artifact_sha256",
    "core_instructions_sha256",
)


class SelectionBlocked(RuntimeError):
    pass


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path, *, purpose: str) -> Dict[str, Any]:
    resolved = Path(path).expanduser().resolve()
    if not resolved.is_file():
        raise SelectionBlocked("%s is missing: %s" % (purpose, resolved))
    try:
        value = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SelectionBlocked("%s is not valid UTF-8 JSON" % purpose) from exc
    if not isinstance(value, dict):
        raise SelectionBlocked("%s must be a JSON object" % purpose)
    return value


def _resolve_artifact(path_value: Any, *, base: Optional[Path] = None) -> Path:
    if not isinstance(path_value, str) or not path_value:
        raise SelectionBlocked("artifact path is missing")
    path = Path(path_value).expanduser()
    if path.is_absolute():
        return path.resolve()
    return ((base or factory_root()) / path).resolve()


def _inside(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


def _valid_usage(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    fields = (
        "input_tokens",
        "cached_input_tokens",
        "output_tokens",
        "reasoning_output_tokens",
        "total_tokens",
    )
    if any(
        isinstance(value.get(field), bool)
        or not isinstance(value.get(field), int)
        or value[field] < 0
        for field in fields
    ):
        return False
    return bool(
        value["cached_input_tokens"] <= value["input_tokens"]
        and value["reasoning_output_tokens"] <= value["output_tokens"]
        and value["total_tokens"] == value["input_tokens"] + value["output_tokens"]
    )


def _sum_usage(values: Iterable[Mapping[str, Any]]) -> Dict[str, int]:
    fields = (
        "input_tokens",
        "cached_input_tokens",
        "output_tokens",
        "reasoning_output_tokens",
        "total_tokens",
    )
    result = {field: 0 for field in fields}
    for value in values:
        if not _valid_usage(value):
            raise SelectionBlocked("invalid or incomplete token accounting")
        for field in fields:
            result[field] += int(value[field])
    return result


def _output_hash_matches(path: Path, expected: Any) -> bool:
    if not isinstance(expected, str):
        return False
    text = path.read_text(encoding="utf-8")
    candidates = {sha256_text(text)}
    if text.endswith("\n"):
        candidates.add(sha256_text(text[:-1]))
    return expected in candidates


def _variant_id(batch_size: int, thread_mode: str) -> str:
    return "batch_%d_%s" % (batch_size, thread_mode)


def _arm_system(variant_id: str, representation: str) -> str:
    return "arm:%s:%s" % (variant_id, representation)


def _event_hash(event: Mapping[str, Any]) -> str:
    return sha256_text(_canonical_json(event))


def _extract_events(payload: Mapping[str, Any], *, baseline: bool) -> List[Dict[str, Any]]:
    key = "discourse_events" if baseline else "events"
    values = payload.get(key)
    if values is None:
        values = []
    if not isinstance(values, list) or any(not isinstance(item, dict) for item in values):
        raise SelectionBlocked("%s is not an event array" % key)
    return [deepcopy(item) for item in values]


def _status(payload: Mapping[str, Any], *, baseline: bool) -> str:
    if baseline:
        value = payload.get("extraction_status")
        events = payload.get("discourse_events") or []
        return str(value or ("coded" if events else "no_signal"))
    return str(payload.get("status") or ("coded" if payload.get("events") else "no_signal"))


def _manifest_segments(manifest: Mapping[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    episodes = manifest.get("episodes")
    if not isinstance(episodes, list):
        raise SelectionBlocked("development manifest episodes are missing")
    rows = []
    by_id = {}
    for episode in episodes:
        if not isinstance(episode, dict) or not isinstance(episode.get("segments"), list):
            raise SelectionBlocked("development manifest episode is malformed")
        for segment in episode["segments"]:
            if not isinstance(segment, dict):
                raise SelectionBlocked("development manifest segment is malformed")
            row = {
                **segment,
                "episode_id": episode.get("episode_id"),
                "source_id": episode.get("source_id"),
                "source_name": episode.get("source_name"),
            }
            segment_id = row.get("segment_id")
            if not isinstance(segment_id, str) or segment_id in by_id:
                raise SelectionBlocked("development manifest segment IDs are invalid or duplicated")
            rows.append(row)
            by_id[segment_id] = row
    if len(rows) != 32 or int(manifest.get("segment_count") or -1) != 32:
        raise SelectionBlocked("development selection requires the frozen 32-case manifest")
    if len({row.get("text_sha256") for row in rows}) != 32:
        raise SelectionBlocked("development manifest text hashes are not unique")
    return rows, by_id


def _load_sources(
    conn: sqlite3.Connection,
    manifest_rows: Sequence[Mapping[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    result = {}
    for spec in manifest_rows:
        segment_id = str(spec["segment_id"])
        row = conn.execute(
            """
            SELECT id, episode_id, source_id, segment_index, text_path, text_sha256
            FROM segments WHERE id = ?
            """,
            (segment_id,),
        ).fetchone()
        if row is None:
            raise SelectionBlocked("development segment is missing from DB: %s" % segment_id)
        if (
            str(row["episode_id"]) != str(spec["episode_id"])
            or str(row["source_id"]) != str(spec["source_id"])
            or int(row["segment_index"]) != int(spec["segment_index"])
            or str(row["text_sha256"]) != str(spec["text_sha256"])
        ):
            raise SelectionBlocked("development segment DB provenance drift: %s" % segment_id)
        path = _resolve_artifact(row["text_path"])
        if not path.is_file():
            raise SelectionBlocked("development segment source file is missing: %s" % segment_id)
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as exc:
            raise SelectionBlocked("development segment source is not UTF-8") from exc
        if sha256_text(text) != str(spec["text_sha256"]) or _sha256_file(path) != str(
            spec["text_sha256"]
        ):
            raise SelectionBlocked("development segment source file hash drift: %s" % segment_id)
        result[segment_id] = {
            "text": text,
            "path": str(path),
            "file_sha256": _sha256_file(path),
        }
    return result


def _load_baselines(
    conn: sqlite3.Connection,
    manifest_rows: Sequence[Mapping[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    result = {}
    for spec in manifest_rows:
        row = conn.execute(
            """
            SELECT labels.id AS label_id, labels.segment_id, labels.output_json,
                   label_runs.id AS label_run_id, label_runs.segment_id AS label_run_segment_id,
                   label_runs.output_path
            FROM labels
            JOIN label_runs ON label_runs.id = ?
            WHERE labels.id = ? AND labels.segment_id = ?
            """,
            (spec["label_run_id"], spec["label_id"], spec["segment_id"]),
        ).fetchone()
        if row is None:
            raise SelectionBlocked("manifest baseline label/run pair is missing")
        if str(row["label_run_segment_id"]) != str(spec["segment_id"]):
            raise SelectionBlocked("manifest baseline label/run segment provenance drift")
        if sha256_text(str(row["output_json"])) != str(spec["golden_output_sha256"]):
            raise SelectionBlocked("repaired DB baseline output hash drift")
        raw_path = _resolve_artifact(row["output_path"])
        if not raw_path.is_file() or _sha256_file(raw_path) != str(
            spec["label_run_output_sha256"]
        ):
            raise SelectionBlocked("raw baseline label-run artifact hash drift")
        try:
            raw = json.loads(raw_path.read_text(encoding="utf-8"))
            repaired = json.loads(str(row["output_json"]))
        except json.JSONDecodeError as exc:
            raise SelectionBlocked("baseline output is invalid JSON") from exc
        if not isinstance(raw, dict) or not isinstance(repaired, dict):
            raise SelectionBlocked("baseline output is not an object")
        if (
            raw.get("segment_id") != spec["segment_id"]
            or repaired.get("segment_id") != spec["segment_id"]
            or raw.get("episode_id") != spec["episode_id"]
            or repaired.get("episode_id") != spec["episode_id"]
        ):
            raise SelectionBlocked("baseline output identity does not match the manifest")
        result[str(spec["segment_id"])] = {
            "raw": raw,
            "repaired": repaired,
            "raw_path": str(raw_path),
            "raw_sha256": _sha256_file(raw_path),
            "repaired_output_json_sha256": sha256_text(str(row["output_json"])),
        }
    return result


def _load_arm(
    *,
    report_path: Path,
    manifest_path: Path,
    manifest_sha256: str,
    manifest_rows: Sequence[Mapping[str, Any]],
    manifest_by_id: Mapping[str, Mapping[str, Any]],
    sources: Mapping[str, Mapping[str, Any]],
) -> Dict[str, Any]:
    report_file = Path(report_path).expanduser().resolve()
    report = _load_json(report_file, purpose="app-server arm report")
    if report.get("schema_version") != APP_SERVER_CORE_ARM_VERSION:
        raise SelectionBlocked("unsupported app-server arm report")
    batch_size = report.get("batch_size_ceiling")
    thread_mode = report.get("thread_mode")
    if (
        isinstance(batch_size, bool)
        or not isinstance(batch_size, int)
        or thread_mode not in {"new_thread", "same_thread"}
    ):
        raise SelectionBlocked("arm report configuration is malformed")
    variant_id = _variant_id(batch_size, str(thread_mode))
    if variant_id not in ALL_MATRIX_ARMS:
        raise SelectionBlocked("unexpected development arm: %s" % variant_id)
    expected_segment_ids = {str(row["segment_id"]) for row in manifest_rows}
    if (
        report.get("manifest_sha256") != manifest_sha256
        or int(report.get("requested_segments") or -1) != 32
        or int(report.get("validated_segments") or -1) != 32
        or report.get("accounting_complete") is not True
        or report.get("usage_status") != "complete"
        or not _valid_usage(report.get("usage"))
        or report.get("usage_unknown_attempts") != 0
        or report.get("retry_count") != 0
        or int(report.get("concurrency") or -1) != 1
    ):
        raise SelectionBlocked("arm report is incomplete or not comparable: %s" % variant_id)
    arm_dir = report_file.parent
    mapping_path = arm_dir / "private-mapping.json"
    mapping = _load_json(mapping_path, purpose="app-server arm private mapping")
    if (
        mapping.get("schema_version") != APP_SERVER_CORE_ARM_VERSION
        or mapping.get("manifest_sha256") != manifest_sha256
        or int(mapping.get("batch_size") or -1) != batch_size
        or mapping.get("thread_mode") != thread_mode
        or not isinstance(mapping.get("batches"), list)
    ):
        raise SelectionBlocked("arm private mapping provenance mismatch: %s" % variant_id)
    mapped_ids = []
    sidecar_usages = []
    raw_by_segment = {}
    normalized_by_segment = {}
    artifact_rows = []
    for batch in mapping["batches"]:
        if not isinstance(batch, dict) or not isinstance(batch.get("segment_ids"), list):
            raise SelectionBlocked("arm batch mapping is malformed")
        segment_ids = [str(item) for item in batch["segment_ids"]]
        if (
            not segment_ids
            or len(segment_ids) > batch_size
            or any(item not in expected_segment_ids for item in segment_ids)
            or any(str(manifest_by_id[item]["episode_id"]) != str(batch.get("episode_id")) for item in segment_ids)
        ):
            raise SelectionBlocked("arm batch contains a non-manifest segment")
        mapped_ids.extend(segment_ids)
        raw_path = _resolve_artifact(batch.get("raw_output_path"))
        normalized_path = _resolve_artifact(batch.get("normalized_output_path"))
        sidecar_path = _resolve_artifact(batch.get("sidecar_path"))
        if not all(_inside(path, arm_dir) for path in (raw_path, normalized_path, sidecar_path)):
            raise SelectionBlocked("arm private mapping points outside its arm directory")
        raw = _load_json(raw_path, purpose="arm raw output")
        normalized = _load_json(normalized_path, purpose="arm normalized output")
        sidecar = _load_json(sidecar_path, purpose="arm app-server sidecar")
        if (
            sidecar.get("state") != "completed"
            or sidecar.get("status") != "completed"
            or sidecar.get("auth_type") != "chatgpt"
            or sidecar.get("transport") != "stdio"
            or sidecar.get("client_version") != APP_SERVER_CLIENT_VERSION
            or sidecar.get("model") != report.get("model")
            or sidecar.get("effort") != report.get("reasoning_effort")
            or sidecar.get("thread_mode") != thread_mode
            or int(sidecar.get("batch_size") or -1) != len(segment_ids)
            or sidecar.get("usage_complete") is not True
            or not _valid_usage(sidecar.get("usage"))
            or not _output_hash_matches(raw_path, sidecar.get("output_sha256"))
        ):
            raise SelectionBlocked("arm app-server sidecar is incomplete or inconsistent")
        sidecar_usages.append(sidecar["usage"])
        raw_rows = raw.get("segments")
        if (
            raw.get("episode_id") != batch.get("episode_id")
            or not isinstance(raw_rows, list)
            or [item.get("segment_id") for item in raw_rows if isinstance(item, dict)] != segment_ids
        ):
            raise SelectionBlocked("arm raw output does not preserve its frozen batch")
        prepared = []
        for segment_id in segment_ids:
            spec = manifest_by_id[segment_id]
            windows, boundaries = build_windowed_segment_packet(
                str(sources[segment_id]["text"]),
                window_count=int(report["window_count"]),
                context_chars=int(report["context_chars"]),
            )
            prepared.append(
                {
                    **spec,
                    "segment_text": sources[segment_id]["text"],
                    "windows": windows,
                    "boundaries": boundaries,
                }
            )
        reconstructed, _diagnostics = normalize_episode_batch_output(
            raw,
            episode_id=str(batch["episode_id"]),
            prepared_segments=prepared,
            max_events_per_segment=int(report["max_events_per_segment"]),
        )
        if _canonical_json(reconstructed) != _canonical_json(normalized):
            raise SelectionBlocked("arm normalized output is not reproducible from its raw output")
        normalized_rows = normalized.get("segments")
        if not isinstance(normalized_rows, list) or [
            item.get("segment_id") for item in normalized_rows if isinstance(item, dict)
        ] != segment_ids:
            raise SelectionBlocked("arm normalized output does not preserve its frozen batch")
        for item in raw_rows:
            raw_by_segment[str(item["segment_id"])] = item
        for item in normalized_rows:
            normalized_by_segment[str(item["segment_id"])] = item
        artifact_rows.append(
            {
                "batch_id": batch.get("batch_id"),
                "raw_output_path": str(raw_path),
                "raw_output_sha256": _sha256_file(raw_path),
                "normalized_output_path": str(normalized_path),
                "normalized_output_sha256": _sha256_file(normalized_path),
                "sidecar_path": str(sidecar_path),
                "sidecar_sha256": _sha256_file(sidecar_path),
            }
        )
    if len(mapped_ids) != 32 or set(mapped_ids) != expected_segment_ids or len(set(mapped_ids)) != 32:
        raise SelectionBlocked("arm private mapping does not exactly partition the manifest")
    if (
        int(report.get("requested_calls") or -1) != len(mapping["batches"])
        or int(report.get("attempted_calls") or -1) != len(mapping["batches"])
        or int(report.get("terminal_sidecars") or -1) != len(mapping["batches"])
    ):
        raise SelectionBlocked("arm report call accounting does not match its private mapping")
    if _sum_usage(sidecar_usages) != report["usage"]:
        raise SelectionBlocked("arm report usage does not equal its measured turn sidecars")
    if set(raw_by_segment) != expected_segment_ids or set(normalized_by_segment) != expected_segment_ids:
        raise SelectionBlocked("arm outputs do not exactly cover the manifest")
    return {
        "variant_id": variant_id,
        "batch_size": batch_size,
        "thread_mode": thread_mode,
        "model": report.get("model"),
        "reasoning_effort": report.get("reasoning_effort"),
        "report_path": str(report_file),
        "report_sha256": _sha256_file(report_file),
        "mapping_path": str(mapping_path),
        "mapping_sha256": _sha256_file(mapping_path),
        "usage": report["usage"],
        "raw_by_segment": raw_by_segment,
        "normalized_by_segment": normalized_by_segment,
        "artifacts": artifact_rows,
        "config": {
            "variant_id": variant_id,
            "batch_size": batch_size,
            "thread_mode": thread_mode,
            "model": report.get("model"),
            "reasoning_effort": report.get("reasoning_effort"),
            "concurrency": report.get("concurrency"),
            "retry_count": report.get("retry_count"),
            "window_count": report.get("window_count"),
            "context_chars": report.get("context_chars"),
            "max_events_per_segment": report.get("max_events_per_segment"),
            "output_schema_version": report.get("output_schema_version"),
            "transport_client_version": report.get("transport_client_version"),
            "guideline_artifact_sha256": report.get("guideline_artifact_sha256"),
            "core_instructions_sha256": report.get("core_instructions_sha256"),
        },
    }


def load_interrupted_arm_provenance(
    *,
    provenance_path: Path,
    manifest_rows: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    """Verify and preserve the terminal batch-5/same-thread ITT interruption.

    Completed partial outputs are intentionally not admitted to the shared
    selection reference: doing so would make the incomplete arm asymmetrically
    influence five complete arms.  Their hashes, measured partial usage, and the
    cancelled unknown-usage attempt remain frozen as exploratory provenance.
    """

    path = Path(provenance_path).expanduser().resolve()
    payload = _load_json(path, purpose="interrupted arm intent-to-treat provenance")
    arm = payload.get("arm") or {}
    if (
        payload.get("schema_version") != "pif_app_server_arm_intent_to_treat_interruption_v1"
        or payload.get("classification") != "terminal_interrupted_accounting_incomplete"
        or payload.get("automatic_retry_prohibited") is not True
        or payload.get("selection_eligible") is not False
        or payload.get("semantic_quality_score") != 0
        or arm.get("batch_size") != 5
        or arm.get("thread_mode") != "same_thread"
        or arm.get("concurrency") != 1
        or arm.get("retry_count") != 0
        or payload.get("usage_status") != "partial_unknown"
        or payload.get("usage") is not None
        or payload.get("planned_calls") != 8
        or payload.get("attempted_calls") != 5
        or payload.get("completed_measured_calls") != 4
        or payload.get("cancelled_unknown_usage_calls") != 1
        or payload.get("not_started_calls") != 3
        or payload.get("planned_segments") != 32
        or payload.get("validated_segments") != 16
        or not _valid_usage(payload.get("measured_partial_usage"))
    ):
        raise SelectionBlocked("interrupted arm ITT provenance is incomplete or inconsistent")
    arm_dir = path.parent
    mapping_path = arm_dir / "private-mapping.json"
    mapping = _load_json(mapping_path, purpose="interrupted arm private mapping")
    artifacts = payload.get("artifacts") or {}
    if (
        mapping.get("schema_version") != APP_SERVER_CORE_ARM_VERSION
        or mapping.get("batch_size") != 5
        or mapping.get("thread_mode") != "same_thread"
        or not isinstance(mapping.get("batches"), list)
        or len(mapping["batches"]) != 8
        or _sha256_file(mapping_path) != artifacts.get("private_mapping_sha256")
    ):
        raise SelectionBlocked("interrupted arm private mapping drift")
    expected_segments = {str(item["segment_id"]) for item in manifest_rows}
    mapped_segments = [
        str(segment_id)
        for batch in mapping["batches"]
        for segment_id in (batch.get("segment_ids") or [])
    ]
    if (
        len(mapped_segments) != 32
        or len(set(mapped_segments)) != 32
        or set(mapped_segments) != expected_segments
    ):
        raise SelectionBlocked("interrupted arm mapping does not preserve all 32 ITT cases")
    completed = []
    cancelled = []
    not_started = []
    for batch in mapping["batches"]:
        sidecar_path = _resolve_artifact(batch.get("sidecar_path"))
        if not _inside(sidecar_path, arm_dir):
            raise SelectionBlocked("interrupted arm sidecar points outside its arm directory")
        if not sidecar_path.exists():
            not_started.append(batch)
            continue
        sidecar = _load_json(sidecar_path, purpose="interrupted arm sidecar")
        if sidecar.get("state") == "completed":
            raw_path = _resolve_artifact(batch.get("raw_output_path"))
            normalized_path = _resolve_artifact(batch.get("normalized_output_path"))
            if (
                sidecar.get("usage_complete") is not True
                or not _valid_usage(sidecar.get("usage"))
                or not raw_path.is_file()
                or not normalized_path.is_file()
                or not _output_hash_matches(raw_path, sidecar.get("output_sha256"))
            ):
                raise SelectionBlocked("interrupted arm completed attempt is not recoverably measured")
            completed.append(
                {
                    "batch_id": batch.get("batch_id"),
                    "sidecar_path": str(sidecar_path),
                    "sidecar_sha256": _sha256_file(sidecar_path),
                    "raw_output_sha256": _sha256_file(raw_path),
                    "normalized_output_sha256": _sha256_file(normalized_path),
                    "usage": sidecar["usage"],
                    "segments": len(batch.get("segment_ids") or []),
                }
            )
        elif sidecar.get("state") == "cancelled":
            if (
                sidecar.get("usage_complete") is not False
                or sidecar.get("usage") is not None
                or sidecar.get("recovery_reran_model") is not False
            ):
                raise SelectionBlocked("interrupted arm cancelled attempt does not preserve unknown usage")
            cancelled.append(
                {
                    "batch_id": batch.get("batch_id"),
                    "sidecar_path": str(sidecar_path),
                    "sidecar_sha256": _sha256_file(sidecar_path),
                    "segments": len(batch.get("segment_ids") or []),
                }
            )
        else:
            raise SelectionBlocked("interrupted arm has an unexpected sidecar terminal state")
    if (
        len(completed) != 4
        or len(cancelled) != 1
        or len(not_started) != 3
        or sum(item["segments"] for item in completed) != 16
        or _sum_usage(item["usage"] for item in completed) != payload["measured_partial_usage"]
        or sorted(item["sidecar_sha256"] for item in completed)
        != sorted(artifacts.get("completed_sidecar_sha256s") or [])
    ):
        raise SelectionBlocked("interrupted arm reconstructed ITT counts or hashes differ")
    declared_cancelled = artifacts.get("cancelled_sidecar_sha256")
    declared_cancelled_valid = bool(
        isinstance(declared_cancelled, str)
        and len(declared_cancelled) == 64
        and declared_cancelled == cancelled[0]["sidecar_sha256"]
    )
    return {
        "variant_id": INTERRUPTED_ARM,
        "selection_eligible": False,
        "semantic_pool_included": False,
        "automatic_retry_prohibited": True,
        "provenance_path": str(path),
        "provenance_sha256": _sha256_file(path),
        "mapping_path": str(mapping_path),
        "mapping_sha256": _sha256_file(mapping_path),
        "completed_attempts": completed,
        "cancelled_attempt": cancelled[0],
        "not_started_batch_ids": [item.get("batch_id") for item in not_started],
        "measured_partial_usage": payload["measured_partial_usage"],
        "cancelled_usage_status": "unknown",
        "declared_cancelled_sidecar_hash_valid": declared_cancelled_valid,
        "exclusion_reason": "terminal_incomplete_arm_cannot_asymmetrically_augment_shared_selection_reference",
    }


def assemble_dev_shared_witness_pool(
    conn: sqlite3.Connection,
    *,
    manifest_path: Path,
    arm_report_paths: Sequence[Path],
    output_dir: Path,
    interrupted_arm_provenance_path: Optional[Path] = None,
) -> Dict[str, Any]:
    root = Path(output_dir).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    manifest_file = Path(manifest_path).expanduser().resolve()
    manifest = _load_json(manifest_file, purpose="development manifest")
    if manifest.get("schema_version") != APP_SERVER_DEVELOPMENT_MANIFEST_V2:
        raise SelectionBlocked("development selection requires manifest v2")
    manifest_sha = _sha256_file(manifest_file)
    manifest_rows, manifest_by_id = _manifest_segments(manifest)
    sources = _load_sources(conn, manifest_rows)
    baselines = _load_baselines(conn, manifest_rows)
    arms = {}
    for raw_path in arm_report_paths:
        arm = _load_arm(
            report_path=Path(raw_path),
            manifest_path=manifest_file,
            manifest_sha256=manifest_sha,
            manifest_rows=manifest_rows,
            manifest_by_id=manifest_by_id,
            sources=sources,
        )
        if arm["variant_id"] in arms:
            raise SelectionBlocked("duplicate development arm report")
        arms[arm["variant_id"]] = arm
    observed_arms = set(arms)
    clean_six_mode = observed_arms == set(ALL_MATRIX_ARMS)
    interrupted_mode = observed_arms == set(INTERRUPTED_SELECTABLE_ARMS)
    if not clean_six_mode and not interrupted_mode:
        raise SelectionBlocked(
            "development arm set mismatch; missing=%s extra=%s"
            % (
                sorted(set(INTERRUPTED_SELECTABLE_ARMS) - observed_arms),
                sorted(observed_arms - set(ALL_MATRIX_ARMS)),
            )
        )
    interrupted = None
    if interrupted_mode:
        if interrupted_arm_provenance_path is None:
            raise SelectionBlocked("five-clean-arm selection requires interrupted-arm ITT provenance")
        interrupted = load_interrupted_arm_provenance(
            provenance_path=Path(interrupted_arm_provenance_path),
            manifest_rows=manifest_rows,
        )
    elif interrupted_arm_provenance_path is not None:
        raise SelectionBlocked("clean six-arm selection cannot also declare an interrupted arm")

    systems = {
        BASELINE_RAW_SYSTEM: {"kind": "baseline", "representation": "raw_label_run"},
        BASELINE_REPAIRED_SYSTEM: {"kind": "baseline", "representation": "repaired_db"},
    }
    for variant_id, arm in sorted(arms.items()):
        for representation in ("raw", "normalized"):
            systems[_arm_system(variant_id, representation)] = {
                "kind": "candidate",
                "representation": representation,
                "variant_id": variant_id,
                "selectable": representation == "normalized",
                "config": arm["config"],
            }

    raw_cases = []
    system_cases = {system_id: {} for system_id in systems}
    exact_dedup_removed = 0
    evidence_nonexact = 0
    for spec in manifest_rows:
        segment_id = str(spec["segment_id"])
        source_excerpt = str(sources[segment_id]["text"])
        side_entries = {"a": {}, "b": {}}  # type: Dict[str, Dict[str, Dict[str, Any]]]

        def add_system_payload(
            *,
            system_id: str,
            side: str,
            payload: Mapping[str, Any],
            baseline: bool,
            provenance: Mapping[str, Any],
        ) -> None:
            nonlocal exact_dedup_removed, evidence_nonexact
            events = _extract_events(payload, baseline=baseline)
            event_hashes = []
            exact_flags = []
            for event_index, event in enumerate(events):
                canonical_hash = _event_hash(event)
                evidence = event.get("evidence")
                evidence_exact = isinstance(evidence, str) and bool(evidence) and evidence in source_excerpt
                evidence_nonexact += int(not evidence_exact)
                event_hashes.append(canonical_hash)
                exact_flags.append(evidence_exact)
                membership = {
                    "system_id": system_id,
                    "event_index": event_index,
                    "submitted_evidence_exact": evidence_exact,
                    **dict(provenance),
                }
                existing = side_entries[side].get(canonical_hash)
                if existing is None:
                    side_entries[side][canonical_hash] = {
                        "event": event,
                        "canonical_json": _canonical_json(event),
                        "memberships": [membership],
                    }
                else:
                    if existing["canonical_json"] != _canonical_json(event):
                        raise SelectionBlocked("canonical event hash collision")
                    existing["memberships"].append(membership)
                    exact_dedup_removed += 1
            system_cases[system_id][segment_id] = {
                "status": _status(payload, baseline=baseline),
                "event_hashes": sorted(set(event_hashes)),
                "submitted_event_count": len(events),
                "exact_evidence_event_count": sum(exact_flags),
            }

        baseline = baselines[segment_id]
        add_system_payload(
            system_id=BASELINE_RAW_SYSTEM,
            side="a",
            payload=baseline["raw"],
            baseline=True,
            provenance={
                "artifact_path": baseline["raw_path"],
                "artifact_sha256": baseline["raw_sha256"],
            },
        )
        add_system_payload(
            system_id=BASELINE_REPAIRED_SYSTEM,
            side="a",
            payload=baseline["repaired"],
            baseline=True,
            provenance={
                "db_output_json_sha256": baseline["repaired_output_json_sha256"],
            },
        )
        for variant_id, arm in sorted(arms.items()):
            for representation in ("raw", "normalized"):
                add_system_payload(
                    system_id=_arm_system(variant_id, representation),
                    side="b",
                    payload=arm["%s_by_segment" % representation][segment_id],
                    baseline=False,
                    provenance={
                        "variant_id": variant_id,
                        "representation": representation,
                        "arm_report_sha256": arm["report_sha256"],
                    },
                )
        # Collapse exact canonical identity across the baseline/candidate display
        # boundary too.  Memberships retain every originating system, so no
        # system observation is lost and the model need not re-judge byte-identical
        # events merely because they came from opposite sides.
        for canonical_hash in sorted(set(side_entries["a"]) & set(side_entries["b"])):
            left = side_entries["a"][canonical_hash]
            right = side_entries["b"].pop(canonical_hash)
            if left["canonical_json"] != right["canonical_json"]:
                raise SelectionBlocked("canonical event hash collision across judge sides")
            left["memberships"].extend(right["memberships"])
            exact_dedup_removed += 1
        rendered_sets = {}
        for side in ("a", "b"):
            rendered_sets[side] = [
                {
                    "event": entry["event"],
                    "provenance": {
                        "canonical_event_sha256": canonical_hash,
                        "memberships": sorted(
                            entry["memberships"],
                            key=lambda item: (
                                str(item["system_id"]),
                                int(item["event_index"]),
                            ),
                        ),
                        "structural_sentinel": False,
                    },
                }
                for canonical_hash, entry in sorted(side_entries[side].items())
            ]
        raw_cases.append(
            {
                "case_key": segment_id,
                "source_excerpt": source_excerpt,
                "event_set_a": rendered_sets["a"],
                "event_set_b": rendered_sets["b"],
                "provenance": {
                    "segment_id": segment_id,
                    "episode_id": spec["episode_id"],
                    "source_id": spec["source_id"],
                    "density_stratum": spec["density_stratum"],
                    "text_sha256": spec["text_sha256"],
                    "source_file_sha256": sources[segment_id]["file_sha256"],
                },
            }
        )

    pool, private_mapping = make_shared_witness_pool(
        raw_cases,
        seed="app-server-dev-shared-witness-pool-v1",
    )
    pool_path = root / "shared-witness-pool.private.json"
    mapping_path = root / "private-mapping.json"
    membership_path = root / "membership-index.private.json"
    write_immutable_json(pool_path, pool)
    write_immutable_json(mapping_path, private_mapping)
    membership_index = {
        "schema_version": DEV_MEMBERSHIP_VERSION,
        "manifest_path": str(manifest_file),
        "manifest_sha256": manifest_sha,
        "systems": systems,
        "system_cases": system_cases,
        "case_order": [case["case_id"] for case in pool["cases"]],
        "segment_order": [str(row["segment_id"]) for row in manifest_rows],
        "arm_artifacts": {
            variant_id: {
                key: value
                for key, value in arm.items()
                if key
                in {
                    "variant_id",
                    "batch_size",
                    "thread_mode",
                    "model",
                    "reasoning_effort",
                    "report_path",
                    "report_sha256",
                    "mapping_path",
                    "mapping_sha256",
                    "usage",
                    "artifacts",
                    "config",
                }
            }
            for variant_id, arm in sorted(arms.items())
        },
        "matrix_mode": "five_clean_plus_terminal_interrupted" if interrupted_mode else "six_clean",
        "interrupted_arm": interrupted,
        "privacy": "private_ids_memberships_and_local_artifact_paths_no_source_or_event_text",
    }
    write_immutable_json(membership_path, membership_index)
    assembly = {
        "schema_version": DEV_POOL_ASSEMBLY_VERSION,
        "manifest_path": str(manifest_file),
        "manifest_sha256": manifest_sha,
        "segment_count": 32,
        "source_count": len({str(row["source_id"]) for row in manifest_rows}),
        "arm_count": len(arms),
        "matrix_mode": "five_clean_plus_terminal_interrupted" if interrupted_mode else "six_clean",
        "interrupted_arm_provenance_sha256": (
            interrupted["provenance_sha256"] if interrupted is not None else None
        ),
        "system_count": len(systems),
        "witness_count": sum(
            len(case["event_set_a"]) + len(case["event_set_b"]) for case in pool["cases"]
        ),
        "exact_canonical_duplicates_removed": exact_dedup_removed,
        "submitted_nonexact_evidence_events_preserved_for_judgment": evidence_nonexact,
        "pool_path": str(pool_path),
        "pool_sha256": _sha256_file(pool_path),
        "private_mapping_path": str(mapping_path),
        "private_mapping_sha256": _sha256_file(mapping_path),
        "membership_index_path": str(membership_path),
        "membership_index_sha256": _sha256_file(membership_path),
        "deduplication": "exact_canonical_event_json_within_segment_across_all_systems",
        "semantic_pruning": False,
        "privacy": "hashes_counts_and_private_artifact_paths_no_source_or_event_text",
    }
    write_immutable_json(root / "assembly-report.json", assembly)
    return {
        "assembly": assembly,
        "assembly_path": root / "assembly-report.json",
        "pool": pool,
        "private_mapping": private_mapping,
        "membership_index": membership_index,
        "manifest": manifest,
        "manifest_rows": manifest_rows,
        "arms": arms,
        "interrupted_arm": interrupted,
    }


def load_preassembled_dev_shared_witness_pool(
    *,
    manifest_path: Path,
    witness_root: Path,
    arm_report_paths: Sequence[Path],
    interrupted_arm_provenance_path: Path,
) -> Dict[str, Any]:
    """Adopt the immutable v1 assembly without re-reading mutable label state.

    The model-facing pool, private membership mapping, arm metadata, manifest,
    and terminal interrupted-arm provenance are all revalidated by hash.  This
    deterministic path is used only by the versioned v2 recovery pipeline.
    """

    root = Path(witness_root).expanduser().resolve()
    assembly_path = root / "assembly-report.json"
    pool_path = root / "shared-witness-pool.private.json"
    mapping_path = root / "private-mapping.json"
    membership_path = root / "membership-index.private.json"
    assembly = _load_json(assembly_path, purpose="preassembled witness report")
    pool = _load_json(pool_path, purpose="preassembled shared witness pool")
    private_mapping = _load_json(mapping_path, purpose="preassembled private mapping")
    membership = _load_json(membership_path, purpose="preassembled membership index")
    manifest_file = Path(manifest_path).expanduser().resolve()
    manifest = _load_json(manifest_file, purpose="development manifest")
    if manifest.get("schema_version") != APP_SERVER_DEVELOPMENT_MANIFEST_V2:
        raise SelectionBlocked("preassembled selection requires manifest v2")
    manifest_rows, _manifest_by_id = _manifest_segments(manifest)
    if (
        assembly.get("schema_version") != DEV_POOL_ASSEMBLY_VERSION
        or assembly.get("matrix_mode") != "five_clean_plus_terminal_interrupted"
        or assembly.get("arm_count") != 5
        or assembly.get("segment_count") != 32
        or assembly.get("semantic_pruning") is not False
        or assembly.get("manifest_sha256") != _sha256_file(manifest_file)
        or assembly.get("pool_sha256") != _sha256_file(pool_path)
        or assembly.get("private_mapping_sha256") != _sha256_file(mapping_path)
        or assembly.get("membership_index_sha256") != _sha256_file(membership_path)
    ):
        raise SelectionBlocked("preassembled witness report or artifact hashes drifted")
    pool_errors = validate_shared_witness_pool(pool)
    if pool_errors:
        raise SelectionBlocked(
            "preassembled witness pool is invalid: %s" % "; ".join(pool_errors)
        )
    if (
        private_mapping.get("schema_version") != WITNESS_MAPPING_VERSION
        or membership.get("schema_version") != DEV_MEMBERSHIP_VERSION
        or membership.get("manifest_sha256") != assembly["manifest_sha256"]
        or membership.get("matrix_mode") != assembly["matrix_mode"]
        or membership.get("case_order")
        != [str(case["case_id"]) for case in pool["cases"]]
        or membership.get("segment_order")
        != [str(row["segment_id"]) for row in manifest_rows]
    ):
        raise SelectionBlocked("preassembled witness membership contract drifted")
    arm_artifacts = membership.get("arm_artifacts")
    if not isinstance(arm_artifacts, dict) or set(arm_artifacts) != set(
        INTERRUPTED_SELECTABLE_ARMS
    ):
        raise SelectionBlocked("preassembled witness arm set is not the frozen five-arm set")
    provided_reports = {
        _sha256_file(Path(path).expanduser().resolve()): Path(path).expanduser().resolve()
        for path in arm_report_paths
    }
    if len(provided_reports) != 5:
        raise SelectionBlocked("preassembled selection requires five unique arm reports")
    arms = {}
    for variant_id, value in sorted(arm_artifacts.items()):
        if not isinstance(value, dict):
            raise SelectionBlocked("preassembled arm metadata is malformed")
        report_path = Path(str(value.get("report_path") or "")).expanduser().resolve()
        report_sha = value.get("report_sha256")
        mapping_file = Path(str(value.get("mapping_path") or "")).expanduser().resolve()
        if (
            not report_path.is_file()
            or _sha256_file(report_path) != report_sha
            or report_sha not in provided_reports
            or provided_reports[report_sha] != report_path
            or not mapping_file.is_file()
            or _sha256_file(mapping_file) != value.get("mapping_sha256")
            or not _valid_usage(value.get("usage"))
            or value.get("variant_id") != variant_id
        ):
            raise SelectionBlocked("preassembled arm evidence changed: %s" % variant_id)
        arms[variant_id] = deepcopy(value)
    interrupted = membership.get("interrupted_arm")
    interrupted_path = Path(interrupted_arm_provenance_path).expanduser().resolve()
    if (
        not isinstance(interrupted, dict)
        or interrupted.get("variant_id") != INTERRUPTED_ARM
        or interrupted.get("selection_eligible") is not False
        or interrupted.get("semantic_pool_included") is not False
        or interrupted.get("automatic_retry_prohibited") is not True
        or interrupted.get("cancelled_usage_status") != "unknown"
        or Path(str(interrupted.get("provenance_path") or "")).expanduser().resolve()
        != interrupted_path
        or not interrupted_path.is_file()
        or _sha256_file(interrupted_path) != interrupted.get("provenance_sha256")
        or assembly.get("interrupted_arm_provenance_sha256")
        != interrupted.get("provenance_sha256")
    ):
        raise SelectionBlocked("preassembled interrupted-arm ITT evidence drifted")
    return {
        "assembly": assembly,
        "assembly_path": assembly_path,
        "pool": pool,
        "private_mapping": private_mapping,
        "membership_index": membership,
        "manifest": manifest,
        "manifest_rows": manifest_rows,
        "arms": arms,
        "interrupted_arm": interrupted,
        "reused_preassembled_witness_pool": True,
    }


def plan_judge_shards(
    pool: Mapping[str, Any],
    *,
    max_cases: int = 2,
    max_witnesses: int = 160,
    max_canonical_bytes: int = 400000,
) -> List[Dict[str, Any]]:
    if max_cases < 1 or max_witnesses < 1 or max_canonical_bytes < 1:
        raise ValueError("judge shard limits must be positive")
    shards = []
    current = []
    current_witnesses = 0
    current_bytes = 0
    for case in pool.get("cases") or []:
        witnesses = len(case.get("event_set_a") or []) + len(case.get("event_set_b") or [])
        case_bytes = len(_canonical_json(case).encode("utf-8"))
        if witnesses > max_witnesses or case_bytes > max_canonical_bytes:
            raise SelectionBlocked(
                "single judge case exceeds frozen shard witness or byte cap"
            )
        if current and (
            len(current) + 1 > max_cases
            or current_witnesses + witnesses > max_witnesses
            or current_bytes + case_bytes > max_canonical_bytes
        ):
            shards.append(current)
            current = []
            current_witnesses = 0
            current_bytes = 0
        current.append(deepcopy(case))
        current_witnesses += witnesses
        current_bytes += case_bytes
    if current:
        shards.append(current)
    if not shards:
        raise SelectionBlocked("shared witness pool produced no judge shards")
    return [
        {
            "schema_version": pool["schema_version"],
            "seed_sha256": pool["seed_sha256"],
            "cases": cases,
            "privacy": pool["privacy"],
        }
        for cases in shards
    ]


def merge_judge_consensuses(
    *,
    master_pool: Mapping[str, Any],
    shard_consensuses: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    by_id = {}
    for consensus in shard_consensuses:
        if consensus.get("schema_version") != JUDGE_CONSENSUS_VERSION:
            raise SelectionBlocked("full judge shard consensus version mismatch")
        for case in consensus.get("cases") or []:
            case_id = case.get("case_id") if isinstance(case, dict) else None
            if not isinstance(case_id, str) or case_id in by_id:
                raise SelectionBlocked("full judge shard cases overlap or are malformed")
            by_id[case_id] = case
    expected_ids = [case["case_id"] for case in master_pool["cases"]]
    if set(by_id) != set(expected_ids):
        raise SelectionBlocked("full judge shard cases do not partition the master pool")
    cases = [by_id[case_id] for case_id in expected_ids]
    support_denominator = sum(len(case.get("support_results") or []) for case in cases)
    support_numerator = sum(
        item.get("verdict") == "abstain"
        for case in cases
        for item in case.get("support_results") or []
        if isinstance(item, dict)
    )
    alignment_denominator = sum(len(case.get("alignment_results") or []) for case in cases)
    alignment_numerator = sum(
        item.get("relation") == "abstain"
        for case in cases
        for item in case.get("alignment_results") or []
        if isinstance(item, dict)
    )
    topology_numerator = sum(bool(case.get("alignment_abstained_witness_ids")) for case in cases)
    partition_numerator = sum(
        bool(case.get("partition_abstained_witness_ids")) for case in cases
    )
    abstained_cases = sum(
        bool(
            case.get("status") in {"abstain", "partial_abstain"}
            or any(
                item.get("verdict") == "abstain"
                for item in case.get("support_results") or []
                if isinstance(item, dict)
            )
            or any(
                item.get("relation") == "abstain"
                for item in case.get("alignment_results") or []
                if isinstance(item, dict)
            )
            or bool(case.get("partition_abstained_witness_ids"))
        )
        for case in cases
    )

    def ratio(numerator: int, denominator: int) -> float:
        return round(numerator / denominator, 6) if denominator else 0.0

    return {
        "schema_version": JUDGE_CONSENSUS_VERSION,
        "pool_sha256": sha256_text(_canonical_json(master_pool)),
        "cases": cases,
        "abstentions": {
            "support": {
                "numerator": support_numerator,
                "denominator": support_denominator,
                "rate": ratio(support_numerator, support_denominator),
            },
            "alignment_labels": {
                "numerator": alignment_numerator,
                "denominator": alignment_denominator,
                "rate": ratio(alignment_numerator, alignment_denominator),
            },
            "alignment_topology": {
                "numerator": topology_numerator,
                "denominator": len(cases),
                "rate": ratio(topology_numerator, len(cases)),
            },
            "equivalence_partition": {
                "numerator": partition_numerator,
                "denominator": len(cases),
                "rate": ratio(partition_numerator, len(cases)),
            },
            "cases": {
                "numerator": abstained_cases,
                "denominator": len(cases),
                "rate": ratio(abstained_cases, len(cases)),
            },
        },
        "selection_admissible": True,
        "abstention_gate_applied": False,
    }


async def run_full_judge_shards(
    *,
    pool: Mapping[str, Any],
    output_dir: Path,
    model: str,
    reasoning_effort: str,
    timeout_seconds: float,
    client_factory: Callable[[], Any] = CapacityGatedCodexAppServerClient,
    judge_runner: Callable[..., Awaitable[Dict[str, Any]]] = run_app_server_semantic_judge,
) -> Dict[str, Any]:
    root = Path(output_dir).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    shards = plan_judge_shards(pool)
    reports = []
    consensuses = []

    class BorrowedClientContext:
        def __init__(self, client: CodexAppServerClient):
            self.client = client

        async def __aenter__(self) -> CodexAppServerClient:
            return self.client

        async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool:
            return False

    # Keep one authenticated app-server stdio process for the whole sharded
    # judge.  The generic runner still creates fresh ephemeral AB/BA threads and
    # independent immutable checkpoints for each shard.
    async with client_factory() as persistent_client:
        borrowed_factory = lambda: BorrowedClientContext(persistent_client)
        for index, shard in enumerate(shards):
            shard_root = root / ("shard-%03d" % index)
            shard_pool_path = shard_root / "pool.private.json"
            write_immutable_json(shard_pool_path, shard)
            report = await judge_runner(
                pool_path=shard_pool_path,
                output_dir=shard_root / "judge",
                model=model,
                reasoning_effort=reasoning_effort,
                timeout_seconds=timeout_seconds,
                client_factory=borrowed_factory,
            )
            if not report.get("accounting_complete"):
                raise SelectionBlocked("full semantic judge shard has incomplete accounting")
            consensus_path = shard_root / "judge" / "consensus.private.json"
            consensus = _load_json(consensus_path, purpose="full judge shard consensus")
            reports.append(
                {
                    "shard_index": index,
                    "case_count": len(shard["cases"]),
                    "pool_sha256": _sha256_file(shard_pool_path),
                    "report_path": str(shard_root / "judge" / "report.json"),
                    "report_sha256": _sha256_file(shard_root / "judge" / "report.json"),
                    "consensus_path": str(consensus_path),
                    "consensus_sha256": _sha256_file(consensus_path),
                    "usage": report.get("usage"),
                }
            )
            consensuses.append(consensus)
    merged = merge_judge_consensuses(master_pool=pool, shard_consensuses=consensuses)
    consensus_path = root / "consensus.private.json"
    write_immutable_json(consensus_path, merged)
    usage = _sum_usage(item["usage"] for item in reports)
    report = {
        "schema_version": DEV_FULL_JUDGE_VERSION,
        "state": "completed",
        "model": model,
        "reasoning_effort": reasoning_effort,
        "shard_count": len(shards),
        "case_count": len(pool["cases"]),
        "accounting_complete": True,
        "usage": usage,
        "abstentions": merged["abstentions"],
        "consensus_path": str(consensus_path),
        "consensus_sha256": _sha256_file(consensus_path),
        "shards": reports,
        "privacy": "hashes_counts_usage_and_private_artifact_paths_no_source_or_event_text",
    }
    write_immutable_json(root / "report.json", report)
    return report


def _mapping_memberships(
    private_mapping: Mapping[str, Any],
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, str], Dict[str, str]]:
    witness = {}
    segment_by_case = {}
    case_by_witness = {}
    if private_mapping.get("schema_version") != WITNESS_MAPPING_VERSION:
        raise SelectionBlocked("private witness mapping version mismatch")
    for case in private_mapping.get("cases") or []:
        case_id = str(case.get("case_id"))
        case_key = str(case.get("case_key"))
        segment_by_case[case_id] = case_key
        for item in case.get("witnesses") or []:
            witness_id = str(item.get("witness_id"))
            provenance = item.get("provenance") or {}
            if witness_id in witness or not isinstance(provenance, dict):
                raise SelectionBlocked("private witness mapping contains duplicate or invalid witnesses")
            witness[witness_id] = provenance
            case_by_witness[witness_id] = case_id
    return witness, segment_by_case, case_by_witness


def score_dev_shared_reference(
    *,
    private_mapping: Mapping[str, Any],
    membership_index: Mapping[str, Any],
    consensus: Mapping[str, Any],
    manifest_rows: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    if membership_index.get("schema_version") != DEV_MEMBERSHIP_VERSION:
        raise SelectionBlocked("development membership index version mismatch")
    if consensus.get("schema_version") != JUDGE_CONSENSUS_VERSION:
        raise SelectionBlocked("semantic consensus version mismatch")
    witness_provenance, segment_by_case, case_by_witness = _mapping_memberships(private_mapping)
    support_by_witness = {}
    edges_by_case = defaultdict(list)
    group_by_witness = {}
    witnesses_by_group = {}
    abstained_case_ids = set()
    seen_cases = set()
    expected_witnesses_by_case = defaultdict(set)
    for witness_id, case_id in case_by_witness.items():
        expected_witnesses_by_case[case_id].add(witness_id)
    for case in consensus.get("cases") or []:
        if not isinstance(case, dict):
            raise SelectionBlocked("semantic consensus case is malformed")
        case_id = str(case.get("case_id"))
        if case_id in seen_cases or case_id not in segment_by_case:
            raise SelectionBlocked("semantic consensus case IDs are invalid")
        seen_cases.add(case_id)
        case_abstained = case.get("status") in {"abstain", "partial_abstain"}
        for item in case.get("support_results") or []:
            witness_id = str(item.get("witness_id"))
            verdict = item.get("verdict")
            if (
                witness_id in support_by_witness
                or witness_id not in witness_provenance
                or case_by_witness.get(witness_id) != case_id
                or verdict not in {"supported", "unsupported", "abstain"}
            ):
                raise SelectionBlocked("semantic consensus support IDs are invalid")
            support_by_witness[witness_id] = verdict
            case_abstained = case_abstained or verdict == "abstain"
        for item in case.get("alignment_results") or []:
            left_id = str(item.get("left_witness_id"))
            right_id = str(item.get("right_witness_id"))
            relation = item.get("relation")
            if (
                left_id not in witness_provenance
                or right_id not in witness_provenance
                or case_by_witness.get(left_id) != case_id
                or case_by_witness.get(right_id) != case_id
                or relation not in {"equivalent", "partial", "non_equivalent", "abstain"}
            ):
                raise SelectionBlocked("semantic consensus alignment IDs are invalid")
            edges_by_case[case_id].append((left_id, right_id, relation))
            case_abstained = case_abstained or relation == "abstain"
        if case.get("alignment_abstained_witness_ids"):
            case_abstained = True
        expected_witnesses = expected_witnesses_by_case.get(case_id, set())
        partition_abstained = case.get("partition_abstained_witness_ids") or []
        groups = case.get("equivalence_groups")
        if not isinstance(groups, list) or not isinstance(partition_abstained, list):
            raise SelectionBlocked("semantic equivalence partition is missing")
        if partition_abstained:
            if groups or set(partition_abstained) != expected_witnesses or len(
                partition_abstained
            ) != len(set(partition_abstained)):
                raise SelectionBlocked("semantic equivalence partition abstention is malformed")
            case_abstained = True
            normalized_groups = [{witness_id} for witness_id in sorted(expected_witnesses)]
        else:
            normalized_groups = []
            partition_seen = set()
            for group in groups:
                if (
                    not isinstance(group, list)
                    or not group
                    or any(not isinstance(item, str) for item in group)
                    or len(group) != len(set(group))
                    or set(group) & partition_seen
                    or not set(group).issubset(expected_witnesses)
                ):
                    raise SelectionBlocked("semantic equivalence groups are malformed")
                partition_seen.update(group)
                normalized_groups.append(set(group))
            if partition_seen != expected_witnesses:
                raise SelectionBlocked("semantic equivalence groups do not partition the case")
        for group in normalized_groups:
            group_key = (case_id, sha256_text("|".join(sorted(group))))
            witnesses_by_group[group_key] = set(group)
            for witness_id in group:
                if witness_id in group_by_witness:
                    raise SelectionBlocked("semantic witness appears in multiple equivalence groups")
                group_by_witness[witness_id] = group_key
        if case_abstained:
            abstained_case_ids.add(case_id)
    if seen_cases != set(segment_by_case):
        raise SelectionBlocked("semantic consensus does not exactly cover every case")
    if set(support_by_witness) != set(witness_provenance) or set(group_by_witness) != set(
        witness_provenance
    ):
        raise SelectionBlocked("semantic consensus does not exactly cover every witness")

    exact_system_witnesses = defaultdict(set)
    systems_for_witness = defaultdict(set)
    witness_has_exact_membership = set()
    for witness_id, provenance in witness_provenance.items():
        if provenance.get("structural_sentinel"):
            continue
        memberships = provenance.get("memberships")
        if not isinstance(provenance.get("canonical_event_sha256"), str) or not isinstance(
            memberships, list
        ):
            raise SelectionBlocked("private witness provenance is incomplete")
        case_id = case_by_witness.get(witness_id)
        if not isinstance(case_id, str):
            raise SelectionBlocked("private witness cannot be assigned to a case")
        for membership in memberships:
            if not isinstance(membership, dict) or not isinstance(membership.get("system_id"), str):
                raise SelectionBlocked("private event membership is malformed")
            system_id = str(membership["system_id"])
            systems_for_witness[witness_id].add(system_id)
            if membership.get("submitted_evidence_exact") is True:
                exact_system_witnesses[(system_id, case_id)].add(witness_id)
                witness_has_exact_membership.add(witness_id)

    reference_units = {
        group_key
        for group_key, witness_ids in witnesses_by_group.items()
        if any(
            support_by_witness[witness_id] == "supported"
            and witness_id in witness_has_exact_membership
            for witness_id in witness_ids
        )
    }
    units_by_case = defaultdict(set)
    for unit in reference_units:
        units_by_case[unit[0]].add(unit)

    partial_neighbors = defaultdict(set)
    for case_id, edges in edges_by_case.items():
        for left_id, right_id, relation in edges:
            if relation == "partial":
                partial_neighbors[left_id].add(right_id)
                partial_neighbors[right_id].add(left_id)

    specs_by_segment = {str(row["segment_id"]): row for row in manifest_rows}
    system_cases = membership_index.get("system_cases") or {}
    systems = membership_index.get("systems") or {}
    scores = {}
    case_id_by_segment = {segment: case_id for case_id, segment in segment_by_case.items()}
    for system_id in sorted(systems):
        per_case = []
        for segment_id in membership_index["segment_order"]:
            case_id = case_id_by_segment[segment_id]
            system_case = system_cases[system_id][segment_id]
            submitted_witnesses = {
                witness_id
                for witness_id, system_ids in systems_for_witness.items()
                if system_id in system_ids and case_by_witness[witness_id] == case_id
            }
            submitted_units = {group_by_witness[item] for item in submitted_witnesses}
            supporting_witnesses = {
                witness_id
                for witness_id in exact_system_witnesses.get((system_id, case_id), set())
                if support_by_witness[witness_id] == "supported"
            }
            supported_units = {group_by_witness[item] for item in supporting_witnesses}
            reference_case_units = units_by_case.get(case_id, set())
            covered = supported_units & reference_case_units
            partial_covered = set(covered)
            for witness_id in supporting_witnesses:
                for neighbor in partial_neighbors.get(witness_id, set()):
                    neighbor_unit = group_by_witness[neighbor]
                    if neighbor_unit in reference_case_units:
                        partial_covered.add(neighbor_unit)
            precision = (
                len(supported_units) / len(submitted_units)
                if submitted_units
                else 1.0
                if not reference_case_units
                else 0.0
            )
            recall = (
                len(covered) / len(reference_case_units)
                if reference_case_units
                else 1.0
                if not submitted_units
                else 0.0
            )
            f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
            if system_id != BASELINE_REPAIRED_SYSTEM and case_id in abstained_case_ids:
                precision = recall = f1 = 0.0
            spec = specs_by_segment[segment_id]
            per_case.append(
                {
                    "case_id": case_id,
                    "segment_id": segment_id,
                    "episode_id": spec["episode_id"],
                    "source_id": spec["source_id"],
                    "density_stratum": spec["density_stratum"],
                    "submitted_units": len(submitted_units),
                    "supported_units": len(supported_units),
                    "reference_units": len(reference_case_units),
                    "covered_reference_units": len(covered),
                    "partial_diagnostic_reference_units_covered": len(partial_covered),
                    "precision": round(precision, 6),
                    "recall": round(recall, 6),
                    "f1": round(f1, 6),
                    "case_abstained_worst_case": bool(
                        system_id != BASELINE_REPAIRED_SYSTEM and case_id in abstained_case_ids
                    ),
                    "submitted_nonexact_evidence_events": int(system_case["submitted_event_count"])
                    - int(system_case["exact_evidence_event_count"]),
                }
            )
        scores[system_id] = {
            "case_count": len(per_case),
            "macro_f1": round(sum(item["f1"] for item in per_case) / len(per_case), 6),
            "cases": per_case,
        }
    case_abstention_rate = len(abstained_case_ids) / len(segment_by_case) if segment_by_case else 1.0
    return {
        "schema_version": DEV_SCORE_VERSION,
        "baseline_system_id": BASELINE_REPAIRED_SYSTEM,
        "reference_unit_count": len(reference_units),
        "reference_policy": (
            "union_of_consensus_equivalence_groups_with_at_least_one_supported_"
            "exact_evidence_membership_from_any_system"
        ),
        "deduplication": "llm_consensus_full_semantic_equivalence_groups_within_case",
        "full_field_match": "consensus_equivalence_group_only_partial_is_diagnostic_not_strict_credit",
        "abstained_cases": sorted(abstained_case_ids),
        "abstained_case_rate": round(case_abstention_rate, 6),
        "abstention_worst_case_policy": "candidate_case_precision_recall_f1_zero",
        "systems": scores,
    }


def _quantile(sorted_values: Sequence[float], probability: float) -> float:
    position = probability * (len(sorted_values) - 1)
    lower = int(position)
    upper = min(lower + 1, len(sorted_values) - 1)
    fraction = position - lower
    return sorted_values[lower] * (1 - fraction) + sorted_values[upper] * fraction


def source_cluster_paired_bootstrap(
    rows: Sequence[Mapping[str, Any]],
    *,
    iterations: int = 10000,
    confidence: float = 0.95,
    seed: str = "windowed-paired-cluster-bootstrap-v2",
) -> Dict[str, Any]:
    if not rows or iterations != 10000 or confidence != 0.95:
        raise ValueError("development bootstrap is frozen to nonempty 10k 95% evaluation")
    by_source = defaultdict(list)
    for row in rows:
        by_source[str(row["source_id"])].append(row)
    source_ids = sorted(by_source)
    if len(source_ids) < 2:
        raise SelectionBlocked("source-cluster bootstrap requires multiple sources")

    def statistic(multiplicity: Optional[Counter]) -> Tuple[float, float]:
        baseline_values = []
        candidate_values = []
        for source_id in source_ids:
            repeats = multiplicity[source_id] if multiplicity is not None else 1
            for _index in range(repeats):
                baseline_values.extend(float(item["baseline_f1"]) for item in by_source[source_id])
                candidate_values.extend(float(item["candidate_f1"]) for item in by_source[source_id])
        return (
            sum(baseline_values) / len(baseline_values),
            sum(candidate_values) / len(candidate_values),
        )

    baseline, candidate = statistic(None)
    rng = random.Random(int(sha256_text(seed)[:16], 16))
    deltas = []
    for _iteration in range(iterations):
        multiplicity = Counter(
            source_ids[rng.randrange(len(source_ids))] for _draw in range(len(source_ids))
        )
        baseline_sample, candidate_sample = statistic(multiplicity)
        deltas.append(candidate_sample - baseline_sample)
    deltas.sort()
    alpha = (1 - confidence) / 2
    return {
        "method": "paired_source_clustered",
        "iterations": iterations,
        "confidence": confidence,
        "seed": seed,
        "source_clusters": len(source_ids),
        "segments": len(rows),
        "baseline_f1": round(baseline, 6),
        "candidate_f1": round(candidate, 6),
        "candidate_minus_baseline": round(candidate - baseline, 6),
        "ci_lower": round(_quantile(deltas, alpha), 6),
        "ci_upper": round(_quantile(deltas, 1 - alpha), 6),
    }


def evaluate_semantic_gates(
    score: Mapping[str, Any],
    *,
    candidate_system_id: str,
) -> Dict[str, Any]:
    baseline_rows = {
        item["segment_id"]: item for item in score["systems"][BASELINE_REPAIRED_SYSTEM]["cases"]
    }
    candidate_rows = {
        item["segment_id"]: item for item in score["systems"][candidate_system_id]["cases"]
    }
    rows = [
        {
            "segment_id": segment_id,
            "source_id": baseline_rows[segment_id]["source_id"],
            "density_stratum": baseline_rows[segment_id]["density_stratum"],
            "baseline_f1": baseline_rows[segment_id]["f1"],
            "candidate_f1": candidate_rows[segment_id]["f1"],
        }
        for segment_id in baseline_rows
    ]
    bootstrap = source_cluster_paired_bootstrap(
        rows,
        iterations=QUALITY_GATES["bootstrap_iterations"],
        confidence=QUALITY_GATES["bootstrap_confidence"],
        seed=QUALITY_GATES["bootstrap_seed"],
    )
    by_source = {}
    for source_id in sorted({str(row["source_id"]) for row in rows}):
        selected = [row for row in rows if str(row["source_id"]) == source_id]
        baseline = sum(float(row["baseline_f1"]) for row in selected) / len(selected)
        candidate = sum(float(row["candidate_f1"]) for row in selected) / len(selected)
        by_source[source_id] = {
            "segments": len(selected),
            "baseline_f1": round(baseline, 6),
            "candidate_f1": round(candidate, 6),
            "candidate_minus_baseline": round(candidate - baseline, 6),
        }
    macro_delta = sum(item["candidate_minus_baseline"] for item in by_source.values()) / len(
        by_source
    )
    worst_source_delta = min(item["candidate_minus_baseline"] for item in by_source.values())
    no_signal = [row for row in candidate_rows.values() if row["density_stratum"] == "no_signal"]
    false_positive_count = sum(
        bool(row["supported_units"] > 0 or row["case_abstained_worst_case"]) for row in no_signal
    )
    no_signal_rate = false_positive_count / len(no_signal) if no_signal else 1.0
    nonexact = sum(int(row["submitted_nonexact_evidence_events"]) for row in candidate_rows.values())
    checks = {
        "paired_bootstrap_noninferiority": bootstrap["ci_lower"]
        >= -QUALITY_GATES["max_paired_f1_drop"],
        "macro_source_noninferiority": macro_delta >= -QUALITY_GATES["max_macro_f1_drop"],
        "worst_source_noninferiority": worst_source_delta
        >= -QUALITY_GATES["max_source_f1_drop"],
        "no_signal_worst_case": no_signal_rate
        <= QUALITY_GATES["max_no_signal_false_positive_rate"],
        "abstention_rate": float(score["abstained_case_rate"])
        <= QUALITY_GATES["max_abstained_case_rate"],
        "normalized_exact_evidence": nonexact == 0,
    }
    return {
        "candidate_system_id": candidate_system_id,
        "passed": all(checks.values()),
        "checks": checks,
        "bootstrap": bootstrap,
        "macro_source_delta": round(macro_delta, 6),
        "worst_source_delta": round(worst_source_delta, 6),
        "by_source": by_source,
        "no_signal": {
            "cases": len(no_signal),
            "false_positive_or_abstained_worst_case": false_positive_count,
            "false_positive_rate": round(no_signal_rate, 6),
        },
        "abstained_case_rate": score["abstained_case_rate"],
        "normalized_nonexact_evidence_events": nonexact,
    }


def load_exact_cost_contract(
    context_usage_recovery_path: Path,
    *,
    verify_sidecars: bool = True,
) -> Dict[str, Any]:
    recovery_path = Path(context_usage_recovery_path).expanduser().resolve()
    recovery = _load_json(recovery_path, purpose="exact context usage recovery")
    if (
        recovery.get("schema_version") != "historical_episode_context_usage_recovery_v1"
        or recovery.get("ok") is not True
        or recovery.get("recovery_reran_model") is not False
        or int(recovery.get("requested_runs") or -1) != 57
        or int(recovery.get("recovered_runs") or -1) != 57
        or recovery.get("missing_run_ids")
        or recovery.get("duplicate_matches")
        or recovery.get("invalid_matches")
        or recovery.get("artifact_errors")
        or recovery.get("expected_usage_mismatches")
        or not _valid_usage(recovery.get("usage"))
        or any(
            (recovery.get("usage") or {}).get(field) != expected
            for field, expected in EXPECTED_CONTEXT_USAGE.items()
        )
        or any(
            (recovery.get("expected_usage") or {}).get(field) != expected
            for field, expected in EXPECTED_CONTEXT_USAGE.items()
        )
        or not _valid_usage(recovery.get("production_amortized_usage"))
        or int(recovery["production_amortized_usage"]["total_tokens"])
        != EXPECTED_PRODUCTION_CONTEXT_TOTAL
    ):
        raise SelectionBlocked("exact recovered episode-context accounting is missing or inconsistent")
    if verify_sidecars:
        sidecars = recovery.get("sidecars")
        if not isinstance(sidecars, list) or len(sidecars) != 57:
            raise SelectionBlocked("exact context recovery does not bind all 57 sidecars")
        for sidecar in sidecars:
            path = _resolve_artifact(sidecar.get("sidecar_path"))
            if not path.is_file() or _sha256_file(path) != sidecar.get("sidecar_sha256"):
                raise SelectionBlocked("exact context usage sidecar hash drift")
    context_cost_path = _resolve_artifact(recovery.get("context_cost_report_path"))
    if (
        not context_cost_path.is_file()
        or _sha256_file(context_cost_path) != recovery.get("context_cost_report_sha256")
    ):
        raise SelectionBlocked("context cost report hash drift")
    context_cost = _load_json(context_cost_path, purpose="context cost report")
    if (
        context_cost.get("exact_usage_available") is not True
        or context_cost.get("end_to_end_cost_evaluable") is not True
        or context_cost.get("fail_closed_reason") is not None
        or context_cost.get("exact_context_usage") != recovery.get("usage")
        or context_cost.get("exact_context_usage_production_amortized")
        != recovery.get("production_amortized_usage")
    ):
        raise SelectionBlocked("context cost report does not preserve exact recovered accounting")
    phase_path = _resolve_artifact(context_cost.get("phase_one_report_path"))
    phase = _load_json(phase_path, purpose="paired baseline phase-one report")
    baseline = phase.get("baseline") or {}
    if (
        baseline.get("accounting_complete") is not True
        or not _valid_usage(baseline.get("usage"))
        or isinstance(phase.get("requested_segments"), bool)
        or not isinstance(phase.get("requested_segments"), int)
        or int(phase["requested_segments"]) < 1
    ):
        raise SelectionBlocked("paired baseline exact token accounting is unavailable")
    return {
        "context_usage_recovery_path": str(recovery_path),
        "context_usage_recovery_sha256": _sha256_file(recovery_path),
        "context_cost_report_path": str(context_cost_path),
        "context_cost_report_sha256": _sha256_file(context_cost_path),
        "baseline_phase_one_path": str(phase_path),
        "baseline_phase_one_sha256": _sha256_file(phase_path),
        "baseline_segments": int(phase["requested_segments"]),
        "baseline_usage": baseline["usage"],
        "exact_context_usage": recovery["usage"],
        "production_amortized_context_usage": recovery["production_amortized_usage"],
        "formula": (
            "((arm_total_tokens / arm_segments) * baseline_segments + "
            "production_amortized_context_tokens) / "
            "(baseline_total_tokens + production_amortized_context_tokens)"
        ),
    }


def production_amortized_cost(
    *,
    arm_usage: Mapping[str, Any],
    arm_segments: int,
    contract: Mapping[str, Any],
) -> Dict[str, Any]:
    if not _valid_usage(arm_usage) or arm_segments < 1:
        raise SelectionBlocked("arm cost accounting is incomplete")
    baseline_segments = int(contract["baseline_segments"])
    baseline_total = int(contract["baseline_usage"]["total_tokens"])
    context_total = int(contract["production_amortized_context_usage"]["total_tokens"])
    scaled_product = int(arm_usage["total_tokens"]) * baseline_segments
    scaled_candidate = (scaled_product + arm_segments - 1) // arm_segments
    numerator = scaled_candidate + context_total
    denominator = baseline_total + context_total
    ratio = numerator / denominator
    divisor = math.gcd(numerator, denominator)
    return {
        "arm_segments": arm_segments,
        "baseline_segments": baseline_segments,
        "arm_measured_total_tokens": int(arm_usage["total_tokens"]),
        "candidate_tokens_scaled_to_baseline_segment_scope": scaled_candidate,
        "baseline_measured_total_tokens": baseline_total,
        "shared_production_amortized_context_tokens_each_system": context_total,
        "candidate_end_to_end_tokens": numerator,
        "baseline_end_to_end_tokens": denominator,
        "production_amortized_total_token_ratio_numerator": numerator // divisor,
        "production_amortized_total_token_ratio_denominator": denominator // divisor,
        "production_amortized_total_token_ratio": round(ratio, 6),
        "passed_lte_0_28": numerator * 25 <= denominator * 7,
        "formula": contract["formula"],
    }


def _blocked_result(
    *,
    reasons: Sequence[str],
    stage: str,
    spec_sha256: Optional[str] = None,
    details: Optional[Mapping[str, Any]] = None,
    terminal_classification: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "schema_version": DEV_SELECTION_VERSION,
        "selection_status": "blocked",
        "winner_frozen": False,
        "blocked_stage": stage,
        "blocked_reasons": sorted(set(str(item) for item in reasons)),
        "selection_spec_sha256": spec_sha256,
        "details": dict(details or {}),
        "terminal_classification": terminal_classification,
        "production_changed": False,
        "holdout_preparation_authorized": False,
        "holdout_model_calls_authorized": False,
    }


def _freeze_extraction_config(arm: Mapping[str, Any]) -> Tuple[Dict[str, Any], str]:
    raw = arm.get("config")
    if not isinstance(raw, Mapping) or set(raw) != set(FROZEN_EXTRACTION_CONFIG_FIELDS):
        raise SelectionBlocked("selected arm does not expose the complete extraction config")
    config = {field: deepcopy(raw[field]) for field in FROZEN_EXTRACTION_CONFIG_FIELDS}
    if (
        config["variant_id"] != arm.get("variant_id")
        or config["batch_size"] != arm.get("batch_size")
        or config["thread_mode"] != arm.get("thread_mode")
        or config["model"] != arm.get("model")
        or config["reasoning_effort"] != arm.get("reasoning_effort")
        or isinstance(config["batch_size"], bool)
        or not isinstance(config["batch_size"], int)
        or config["batch_size"] < 1
        or config["thread_mode"] not in {"new_thread", "same_thread"}
        or not isinstance(config["model"], str)
        or not config["model"]
        or not isinstance(config["reasoning_effort"], str)
        or not config["reasoning_effort"]
        or config["concurrency"] != 1
        or config["retry_count"] != 0
        or isinstance(config["window_count"], bool)
        or not isinstance(config["window_count"], int)
        or config["window_count"] < 1
        or isinstance(config["context_chars"], bool)
        or not isinstance(config["context_chars"], int)
        or config["context_chars"] < 0
        or isinstance(config["max_events_per_segment"], bool)
        or not isinstance(config["max_events_per_segment"], int)
        or config["max_events_per_segment"] < 1
        or not isinstance(config["output_schema_version"], str)
        or not config["output_schema_version"]
        or config["transport_client_version"] != APP_SERVER_CLIENT_VERSION
    ):
        raise SelectionBlocked("selected arm extraction config is malformed or unsafe")
    for field in ("guideline_artifact_sha256", "core_instructions_sha256"):
        value = config[field]
        if not (
            isinstance(value, str)
            and len(value) == 64
            and all(character in "0123456789abcdef" for character in value)
        ):
            raise SelectionBlocked("selected arm extraction config hash is invalid: %s" % field)
    return config, sha256_text(_canonical_json(config))


async def run_app_server_dev_selection(
    conn: sqlite3.Connection,
    *,
    manifest_path: Path,
    arm_report_paths: Sequence[Path],
    context_usage_recovery_path: Path,
    output_dir: Path,
    selection_output_path: Optional[Path] = None,
    run_spec_path: Optional[Path] = None,
    interrupted_arm_provenance_path: Optional[Path] = None,
    judge_model: str = "gpt-5.6-sol",
    judge_reasoning_effort: str = "high",
    judge_timeout_seconds: float = 1200.0,
    verify_context_sidecars: bool = True,
    preassembled_witness_root: Optional[Path] = None,
    selection_context: Optional[Mapping[str, Any]] = None,
    client_factory: Callable[[], Any] = CapacityGatedCodexAppServerClient,
    calibration_runner: Callable[..., Awaitable[Dict[str, Any]]] = run_app_server_judge_calibration,
    judge_runner: Callable[..., Awaitable[Dict[str, Any]]] = run_app_server_semantic_judge,
) -> Dict[str, Any]:
    root = Path(output_dir).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = Path(selection_output_path or (root / "selection-result.json")).expanduser().resolve()
    try:
        if preassembled_witness_root is not None:
            if interrupted_arm_provenance_path is None:
                raise SelectionBlocked(
                    "preassembled five-arm selection requires interrupted-arm provenance"
                )
            assembled = load_preassembled_dev_shared_witness_pool(
                manifest_path=Path(manifest_path),
                witness_root=Path(preassembled_witness_root),
                arm_report_paths=[Path(item) for item in arm_report_paths],
                interrupted_arm_provenance_path=Path(interrupted_arm_provenance_path),
            )
        else:
            assembled = assemble_dev_shared_witness_pool(
                conn,
                manifest_path=Path(manifest_path),
                arm_report_paths=[Path(item) for item in arm_report_paths],
                output_dir=root / "witness-pool",
                interrupted_arm_provenance_path=interrupted_arm_provenance_path,
            )
        cost_contract = load_exact_cost_contract(
            Path(context_usage_recovery_path),
            verify_sidecars=verify_context_sidecars,
        )
        spec = {
            "schema_version": DEV_SELECTION_VERSION,
            "state": "frozen_before_judge_calls",
            "manifest_sha256": assembled["assembly"]["manifest_sha256"],
            "assembly_report_sha256": _sha256_file(
                Path(
                    assembled.get(
                        "assembly_path", root / "witness-pool" / "assembly-report.json"
                    )
                )
            ),
            "assembly_mode": (
                "hash_bound_preassembled_v1"
                if assembled.get("reused_preassembled_witness_pool") is True
                else "fresh_deterministic_assembly"
            ),
            "pool_sha256": assembled["assembly"]["pool_sha256"],
            "arm_report_sha256": {
                variant_id: arm["report_sha256"]
                for variant_id, arm in sorted(assembled["arms"].items())
            },
            "matrix_mode": assembled["assembly"]["matrix_mode"],
            "interrupted_arm_provenance_sha256": assembled["assembly"].get(
                "interrupted_arm_provenance_sha256"
            ),
            "context_usage_recovery_sha256": cost_contract["context_usage_recovery_sha256"],
            "context_cost_report_sha256": cost_contract["context_cost_report_sha256"],
            "baseline_phase_one_sha256": cost_contract["baseline_phase_one_sha256"],
            "judge_model": judge_model,
            "judge_reasoning_effort": judge_reasoning_effort,
            "judge_timeout_seconds": judge_timeout_seconds,
            "judge_transport": "official_codex_app_server_managed_chatgpt_auth_only",
            "quality_gates": QUALITY_GATES,
            "baseline_system_id": BASELINE_REPAIRED_SYSTEM,
            "run_spec_sha256": (
                _sha256_file(Path(run_spec_path).expanduser().resolve()) if run_spec_path else None
            ),
            "production_changes_allowed": False,
            "selection_context": deepcopy(dict(selection_context or {})),
        }
        write_immutable_json(root / "selection-spec.json", spec)
        spec_sha = _sha256_file(root / "selection-spec.json")
        if terminal_path.exists():
            prior = _load_json(terminal_path, purpose="terminal development selection")
            if prior.get("selection_spec_sha256") not in {None, spec_sha}:
                raise SelectionBlocked("terminal development selection spec drift")
            return prior

        calibration = await calibration_runner(
            output_dir=root / "calibration",
            model=judge_model,
            reasoning_effort=judge_reasoning_effort,
            timeout_seconds=judge_timeout_seconds,
            client_factory=client_factory,
        )
        if calibration.get("calibrated") is not True:
            fail_reason = str(
                calibration.get("fail_closed_reason") or "judge_calibration_gate_not_passed"
            )
            infrastructure_failure = fail_reason == "infrastructure_or_judge_attempt_failed"
            result = _blocked_result(
                reasons=[fail_reason],
                stage=("calibration_attempt" if infrastructure_failure else "calibration"),
                spec_sha256=spec_sha,
                details={
                    "calibration_report_sha256": _sha256_file(root / "calibration" / "report.json")
                },
                terminal_classification=(
                    "infrastructure_or_judge_attempt_failed"
                    if infrastructure_failure
                    else "judge_calibration_gate_not_passed"
                ),
            )
            write_immutable_json(terminal_path, result)
            return result

        full_judge = await run_full_judge_shards(
            pool=assembled["pool"],
            output_dir=root / "full-judge",
            model=judge_model,
            reasoning_effort=judge_reasoning_effort,
            timeout_seconds=judge_timeout_seconds,
            client_factory=client_factory,
            judge_runner=judge_runner,
        )
        consensus = _load_json(
            root / "full-judge" / "consensus.private.json",
            purpose="merged full judge consensus",
        )
        score = score_dev_shared_reference(
            private_mapping=assembled["private_mapping"],
            membership_index=assembled["membership_index"],
            consensus=consensus,
            manifest_rows=assembled["manifest_rows"],
        )
        arm_results = {}
        for variant_id, arm in sorted(assembled["arms"].items()):
            system_id = _arm_system(variant_id, "normalized")
            semantic = evaluate_semantic_gates(score, candidate_system_id=system_id)
            cost = production_amortized_cost(
                arm_usage=arm["usage"],
                arm_segments=32,
                contract=cost_contract,
            )
            arm_results[variant_id] = {
                "candidate_system_id": system_id,
                "semantic": semantic,
                "cost": cost,
                "semantic_passed": semantic["passed"],
                "cost_passed": cost["passed_lte_0_28"],
                "selection_passed": bool(semantic["passed"] and cost["passed_lte_0_28"]),
            }
        score_report = {
            **score,
            "quality_gates": QUALITY_GATES,
            "judge_report_sha256": _sha256_file(root / "full-judge" / "report.json"),
            "arms": arm_results,
        }
        write_immutable_json(root / "score-report.json", score_report)
        passing = [
            (int(item["cost"]["candidate_end_to_end_tokens"]), variant_id)
            for variant_id, item in arm_results.items()
            if item["selection_passed"]
        ]
        if not passing:
            reasons = []
            if not any(item["semantic_passed"] for item in arm_results.values()):
                reasons.append("no_arm_passed_semantic_noninferiority")
            if not any(item["cost_passed"] for item in arm_results.values()):
                reasons.append("no_arm_met_production_amortized_token_ratio_lte_0_28")
            if not reasons:
                reasons.append("semantic_and_cost_gates_did_not_pass_on_the_same_arm")
            result = _blocked_result(
                reasons=reasons,
                stage="winner_selection",
                spec_sha256=spec_sha,
                details={
                    "score_report_sha256": _sha256_file(root / "score-report.json"),
                    "full_judge_report_sha256": _sha256_file(root / "full-judge" / "report.json"),
                },
                terminal_classification="development_quality_or_cost_gate_not_passed",
            )
            write_immutable_json(terminal_path, result)
            return result
        passing.sort()
        _ratio_value, winner_id = passing[0]
        winner_arm = assembled["arms"][winner_id]
        winner_result = arm_results[winner_id]
        frozen_hashes = {
            "manifest": assembled["assembly"]["manifest_sha256"],
            "witness_pool": assembled["assembly"]["pool_sha256"],
            "private_mapping": assembled["assembly"]["private_mapping_sha256"],
            "membership_index": assembled["assembly"]["membership_index_sha256"],
            "selection_spec": spec_sha,
            "context_usage_recovery": cost_contract["context_usage_recovery_sha256"],
            "context_cost_report": cost_contract["context_cost_report_sha256"],
            "baseline_phase_one": cost_contract["baseline_phase_one_sha256"],
            "calibration_report": _sha256_file(root / "calibration" / "report.json"),
            "full_judge_report": _sha256_file(root / "full-judge" / "report.json"),
            "full_judge_consensus": _sha256_file(root / "full-judge" / "consensus.private.json"),
            "score_report": _sha256_file(root / "score-report.json"),
            **{
                "arm_report_%s" % variant_id: arm["report_sha256"]
                for variant_id, arm in sorted(assembled["arms"].items())
            },
        }
        if assembled.get("interrupted_arm") is not None:
            frozen_hashes["interrupted_arm_intent_to_treat"] = assembled["interrupted_arm"][
                "provenance_sha256"
            ]
            frozen_hashes["interrupted_arm_private_mapping"] = assembled["interrupted_arm"][
                "mapping_sha256"
            ]
        if run_spec_path:
            frozen_hashes["run_spec"] = _sha256_file(Path(run_spec_path).expanduser().resolve())
        frozen_config, config_sha = _freeze_extraction_config(winner_arm)
        result = {
            "schema_version": FROZEN_WINNER_VERSION,
            "selection_status": "frozen_winner",
            "winner_frozen": True,
            "selection_spec_sha256": spec_sha,
            "gates": {
                "quality_noninferior": True,
                "production_amortized_total_token_ratio_lte_0_28": True,
                "calibrated_app_server_llm_judge": True,
                "abstained_case_rate_lte_0_05": winner_result["semantic"]["checks"][
                    "abstention_rate"
                ],
                "no_signal_false_positive_rate_lte_0_05": winner_result["semantic"]["checks"][
                    "no_signal_worst_case"
                ],
            },
            "winner": {
                "variant_id": winner_id,
                "batch_size": winner_arm["batch_size"],
                "thread_mode": winner_arm["thread_mode"],
                "model": winner_arm["model"],
                "reasoning_effort": winner_arm["reasoning_effort"],
                "config": frozen_config,
                "config_sha256": config_sha,
                "report_sha256": winner_arm["report_sha256"],
                "production_amortized_total_token_ratio": winner_result["cost"][
                    "production_amortized_total_token_ratio"
                ],
                "quality": winner_result["semantic"],
                "cost": winner_result["cost"],
            },
            "selection_policy": (
                "lowest_measured_production_amortized_total_token_ratio_among_arms_"
                "passing_one_shared_reference_semantic_gates"
            ),
            "frozen_artifact_hashes": frozen_hashes,
            "production_changed": False,
            "holdout_preparation_authorized": True,
            "holdout_model_calls_authorized": False,
        }
        write_immutable_json(terminal_path, result)
        return result
    except SelectionBlocked as exc:
        result = _blocked_result(
            reasons=[str(exc)],
            stage="preflight_or_execution",
            terminal_classification="preflight_contract_failed",
        )
        if not terminal_path.exists():
            write_immutable_json(terminal_path, result)
        return result
    except JudgeArtifactError as exc:
        result = _blocked_result(
            reasons=["infrastructure_or_judge_attempt_failed"],
            stage="calibration_or_judge_attempt",
            details={"error_class": type(exc).__name__},
            terminal_classification="infrastructure_or_judge_attempt_failed",
        )
        if not terminal_path.exists():
            write_immutable_json(terminal_path, result)
        return result
    except Exception as exc:  # noqa: BLE001 - unattended runner must leave a terminal block
        result = _blocked_result(
            reasons=["unexpected_%s" % type(exc).__name__],
            stage="preflight_or_execution",
            terminal_classification="infrastructure_or_judge_attempt_failed",
        )
        if not terminal_path.exists():
            write_immutable_json(terminal_path, result)
        return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Calibrate, judge, and select the app-server dev winner")
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--arm-report", action="append", required=True)
    parser.add_argument("--context-usage-recovery", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--selection-output")
    parser.add_argument("--run-spec")
    parser.add_argument("--interrupted-arm-provenance")
    parser.add_argument("--judge-model", default="gpt-5.6-sol")
    parser.add_argument("--judge-reasoning-effort", default="high")
    parser.add_argument("--judge-timeout-seconds", type=float, default=1200.0)
    parser.add_argument("--database", default=str(db_path()))
    return parser


def _read_only_connection(path: Path) -> sqlite3.Connection:
    resolved = Path(path).expanduser().resolve()
    connection = sqlite3.connect("file:%s?mode=ro" % resolved.as_posix(), uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only = ON")
    connection.execute("PRAGMA busy_timeout = 30000")
    return connection


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    conn = _read_only_connection(Path(args.database))
    try:
        result = asyncio.run(
            run_app_server_dev_selection(
                conn,
                manifest_path=Path(args.manifest),
                arm_report_paths=[Path(item) for item in args.arm_report],
                context_usage_recovery_path=Path(args.context_usage_recovery),
                output_dir=Path(args.output_dir),
                selection_output_path=(Path(args.selection_output) if args.selection_output else None),
                run_spec_path=(Path(args.run_spec) if args.run_spec else None),
                interrupted_arm_provenance_path=(
                    Path(args.interrupted_arm_provenance)
                    if args.interrupted_arm_provenance
                    else None
                ),
                judge_model=args.judge_model,
                judge_reasoning_effort=args.judge_reasoning_effort,
                judge_timeout_seconds=args.judge_timeout_seconds,
            )
        )
    finally:
        conn.close()
    print(json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True))
    return 0 if result.get("selection_status") == "frozen_winner" else 2


if __name__ == "__main__":
    raise SystemExit(main())
