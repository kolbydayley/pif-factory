from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any, Mapping, Sequence

from .app_server_evaluation import APP_SERVER_DEVELOPMENT_MANIFEST_V2
from .app_server_llm_judge import (
    build_judge_prompt,
    build_judge_variants,
    make_shared_witness_pool,
    semantic_judge_output_schema,
    write_immutable_json,
)
from .paths import corpus_dir
from .util import now_iso, sha256_text


BASELINE_TRANSFORM_PACKET_VERSION = "pif_app_server_baseline_transform_packets_v2"


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_object(path: Path, *, purpose: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read valid {purpose}: {path}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{purpose} is not an object: {path}")
    return value


def _resolve_local_path(value: str) -> Path:
    path = Path(value).expanduser()
    return path.resolve() if path.is_absolute() else (corpus_dir().parent / path).resolve()


def _events(payload: Mapping[str, Any], *, purpose: str) -> list[dict[str, Any]]:
    value = payload.get("discourse_events") or []
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise ValueError(f"{purpose} discourse_events must be an object array")
    return list(value)


def _exact_identity_residuals(
    left: Sequence[dict[str, Any]], right: Sequence[dict[str, Any]]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    right_counts: dict[bytes, int] = {}
    for event in right:
        encoded = _canonical_bytes(event)
        right_counts[encoded] = right_counts.get(encoded, 0) + 1
    left_residual = []
    exact_count = 0
    for event in left:
        encoded = _canonical_bytes(event)
        if right_counts.get(encoded, 0):
            right_counts[encoded] -= 1
            exact_count += 1
        else:
            left_residual.append(event)

    left_counts: dict[bytes, int] = {}
    for event in left:
        encoded = _canonical_bytes(event)
        left_counts[encoded] = left_counts.get(encoded, 0) + 1
    right_residual = []
    for event in right:
        encoded = _canonical_bytes(event)
        if left_counts.get(encoded, 0):
            left_counts[encoded] -= 1
        else:
            right_residual.append(event)
    return left_residual, right_residual, exact_count


def _packet_size(raw_cases: Sequence[Mapping[str, Any]], *, seed: str) -> dict[str, int]:
    pool, _mapping = make_shared_witness_pool(raw_cases, seed=seed)
    variants = build_judge_variants(pool)
    prompt_bytes = max(
        len(build_judge_prompt(variant).encode("utf-8"))
        for variant in variants.values()
    )
    schema_bytes = max(
        len(_canonical_bytes(semantic_judge_output_schema(variant)))
        for variant in variants.values()
    )
    return {
        "prompt_bytes_max_orientation": prompt_bytes,
        "schema_bytes_max_orientation": schema_bytes,
    }


def _shard_cases(
    raw_cases: Sequence[Mapping[str, Any]],
    *,
    seed: str,
    max_cases_per_shard: int,
    max_prompt_bytes: int,
    max_schema_bytes: int,
) -> list[list[Mapping[str, Any]]]:
    if max_cases_per_shard < 1 or max_prompt_bytes < 1 or max_schema_bytes < 1:
        raise ValueError("judge shard bounds must be positive")
    shards: list[list[Mapping[str, Any]]] = []
    current: list[Mapping[str, Any]] = []
    for case in raw_cases:
        trial = [*current, case]
        size = _packet_size(trial, seed=seed)
        exceeds = (
            len(trial) > max_cases_per_shard
            or size["prompt_bytes_max_orientation"] > max_prompt_bytes
            or size["schema_bytes_max_orientation"] > max_schema_bytes
        )
        if exceeds and current:
            shards.append(current)
            current = [case]
            size = _packet_size(current, seed=seed)
        else:
            current = trial
        if (
            size["prompt_bytes_max_orientation"] > max_prompt_bytes
            or size["schema_bytes_max_orientation"] > max_schema_bytes
        ):
            raise ValueError("one judge case exceeds the frozen shard byte bounds")
    if current:
        shards.append(current)
    return shards


def build_baseline_transform_judge_packets(
    conn: sqlite3.Connection,
    *,
    manifest_path: str | Path,
    reference_seed_path: str | Path,
    output_dir: str | Path,
    seed: str = "app-server-baseline-transform-v1",
    max_cases_per_shard: int = 4,
    max_prompt_bytes: int = 300_000,
    max_schema_bytes: int = 100_000,
) -> dict[str, Any]:
    manifest_file = Path(manifest_path).expanduser().resolve()
    reference_file = Path(reference_seed_path).expanduser().resolve()
    manifest = _read_object(manifest_file, purpose="development manifest")
    reference = _read_object(reference_file, purpose="shared reference seed")
    if manifest.get("schema_version") != APP_SERVER_DEVELOPMENT_MANIFEST_V2:
        raise ValueError("baseline transform packets require development manifest v2")
    manifest_sha = _file_sha256(manifest_file)
    if reference.get("schema_version") != "pif_shared_reference_seed_v1":
        raise ValueError("unsupported shared reference seed")

    segment_specs = [
        {**segment, "episode_id": episode["episode_id"]}
        for episode in manifest.get("episodes") or []
        for segment in episode.get("segments") or []
    ]
    if not segment_specs:
        raise ValueError("development manifest has no selected segments")
    reference_rows = reference.get("references")
    if not isinstance(reference_rows, list):
        raise ValueError("shared reference seed has no references")
    reference_by_segment = {
        row.get("segment_id"): row
        for row in reference_rows
        if isinstance(row, dict) and isinstance(row.get("segment_id"), str)
    }
    selected_ids = [item["segment_id"] for item in segment_specs]
    if len(reference_by_segment) != len(reference_rows) or set(reference_by_segment) != set(
        selected_ids
    ):
        raise ValueError("shared reference seed does not exactly partition selected segments")

    raw_cases = []
    double_empty = []
    raw_event_count = 0
    submitted_event_count = 0
    exact_identity_event_count = 0
    raw_residual_event_count = 0
    submitted_residual_event_count = 0
    for segment in segment_specs:
        row = conn.execute(
            """
            SELECT l.output_json, lr.output_path, seg.text_path, seg.text_sha256,
                   seg.episode_id, seg.transcript_id
            FROM labels l
            JOIN label_runs lr ON lr.id = ? AND lr.segment_id = l.segment_id
            JOIN segments seg ON seg.id = l.segment_id
            WHERE l.id = ? AND l.segment_id = ?
            """,
            (segment["label_run_id"], segment["label_id"], segment["segment_id"]),
        ).fetchone()
        if row is None:
            raise ValueError(f"baseline label/run provenance is missing: {segment['segment_id']}")
        if str(row["episode_id"]) != str(segment["episode_id"]):
            raise ValueError("baseline segment episode drift")
        text_path = _resolve_local_path(str(row["text_path"]))
        source_excerpt = text_path.read_text(encoding="utf-8")
        if sha256_text(source_excerpt) != segment["text_sha256"]:
            raise ValueError("baseline segment text hash drift")

        raw_path = _resolve_local_path(str(row["output_path"]))
        if _file_sha256(raw_path) != segment["label_run_output_sha256"]:
            raise ValueError("raw baseline output artifact hash drift")
        raw_payload = _read_object(raw_path, purpose="raw baseline output")
        submitted_json = str(row["output_json"])
        if sha256_text(submitted_json) != segment["golden_output_sha256"]:
            raise ValueError("submitted baseline output hash drift")
        submitted_payload = json.loads(submitted_json)
        reference_row = reference_by_segment[segment["segment_id"]]
        if submitted_payload != reference_row.get("golden_output"):
            raise ValueError("shared reference seed differs from submitted baseline output")

        raw_events = _events(raw_payload, purpose="raw baseline")
        submitted_events = _events(submitted_payload, purpose="submitted baseline")
        raw_event_count += len(raw_events)
        submitted_event_count += len(submitted_events)
        raw_residuals, submitted_residuals, exact_count = _exact_identity_residuals(
            raw_events, submitted_events
        )
        exact_identity_event_count += exact_count
        raw_residual_event_count += len(raw_residuals)
        submitted_residual_event_count += len(submitted_residuals)
        case_provenance = {
            "episode_id": segment["episode_id"],
            "segment_id": segment["segment_id"],
            "text_sha256": segment["text_sha256"],
            "density_stratum": segment.get("density_stratum"),
            "system_a_id": "baseline_raw_artifact",
            "system_b_id": "baseline_submitted_db",
            "exact_identity_event_count": exact_count,
        }
        if not raw_residuals and not submitted_residuals:
            double_empty.append(case_provenance)
            continue
        raw_cases.append(
            {
                "case_key": segment["segment_id"],
                "source_excerpt": source_excerpt,
                "event_set_a": [
                    {
                        "event": event,
                        "provenance": {
                            "system_id": "baseline_raw_artifact",
                            "canonical_index": index,
                        },
                    }
                    for index, event in enumerate(raw_residuals)
                ],
                "event_set_b": [
                    {
                        "event": event,
                        "provenance": {
                            "system_id": "baseline_submitted_db",
                            "canonical_index": index,
                        },
                    }
                    for index, event in enumerate(submitted_residuals)
                ],
                "provenance": case_provenance,
            }
        )

    shards = _shard_cases(
        raw_cases,
        seed=seed,
        max_cases_per_shard=max_cases_per_shard,
        max_prompt_bytes=max_prompt_bytes,
        max_schema_bytes=max_schema_bytes,
    )
    root = Path(output_dir).expanduser().resolve()
    try:
        root.mkdir(parents=True, exist_ok=False)
    except FileExistsError as exc:
        raise ValueError("baseline transform packet directory already exists") from exc

    shard_records = []
    for index, shard_cases in enumerate(shards):
        pool, mapping = make_shared_witness_pool(shard_cases, seed=seed)
        size = _packet_size(shard_cases, seed=seed)
        shard_root = root / f"shard-{index:03d}"
        shard_root.mkdir()
        pool_path = shard_root / "pool.private.json"
        mapping_path = shard_root / "mapping.private.json"
        write_immutable_json(pool_path, pool)
        write_immutable_json(mapping_path, mapping)
        shard_records.append(
            {
                "shard_index": index,
                "case_count": len(pool["cases"]),
                "witness_count": sum(
                    len(case["event_set_a"]) + len(case["event_set_b"])
                    for case in pool["cases"]
                ),
                "pool_path": str(pool_path),
                "pool_sha256": _file_sha256(pool_path),
                "mapping_path": str(mapping_path),
                "mapping_sha256": _file_sha256(mapping_path),
                **size,
            }
        )

    private_empty_path = root / "double-empty-cases.private.json"
    write_immutable_json(
        private_empty_path,
        {
            "schema_version": BASELINE_TRANSFORM_PACKET_VERSION,
            "cases": double_empty,
            "interpretation": "no_residual_after_exact_identity_ownership_dedup_no_judge_call_needed",
            "privacy": "private_analysis_only_contains_segment_provenance_no_transcript_text",
        },
    )
    report = {
        "schema_version": BASELINE_TRANSFORM_PACKET_VERSION,
        "created_at": now_iso(),
        "manifest_sha256": manifest_sha,
        "reference_seed_sha256": _file_sha256(reference_file),
        "seed_sha256": sha256_text(seed),
        "systems": ["baseline_raw_artifact", "baseline_submitted_db"],
        "selected_segment_count": len(segment_specs),
        "judge_case_count": len(raw_cases),
        "double_empty_case_count": len(double_empty),
        "raw_event_count": raw_event_count,
        "submitted_event_count": submitted_event_count,
        "exact_identity_event_count": exact_identity_event_count,
        "raw_residual_event_count": raw_residual_event_count,
        "submitted_residual_event_count": submitted_residual_event_count,
        "shard_count": len(shard_records),
        "bounds": {
            "max_cases_per_shard": max_cases_per_shard,
            "max_prompt_bytes": max_prompt_bytes,
            "max_schema_bytes": max_schema_bytes,
        },
        "shards": shard_records,
        "double_empty_cases_path": str(private_empty_path),
        "double_empty_cases_sha256": _file_sha256(private_empty_path),
        "deduplication": "exact_canonical_event_json_multiset_identity_only",
        "semantic_status": "residuals_pending_calibrated_support_first_then_alignment_judge",
        "model_calls_made": 0,
        "privacy": "sanitized_counts_hashes_and_private_artifact_paths_no_transcript_or_event_text",
    }
    write_immutable_json(root / "packet-report.json", report)
    return report
