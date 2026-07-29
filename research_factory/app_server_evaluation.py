from __future__ import annotations

import asyncio
import hashlib
import json
import time
from collections import Counter
from dataclasses import asdict
from pathlib import Path
from typing import Any, Callable

from .codex_app_server import APP_SERVER_CLIENT_VERSION, AppServerError, CodexAppServerClient
from .efficient_backtest import (
    DEFAULT_WINDOWED_GUIDELINES_PATH,
    _load_windowed_guideline_instructions,
    _safe_segment_text,
    _windowed_candidate_event,
    _windowed_core_events,
    _windowed_event_core_instruction,
    build_windowed_segment_packet,
    normalize_windowed_core_payload,
    windowed_event_core_schema,
)
from .labels import ValidationError, _validate_schema
from .paths import corpus_dir
from .util import now_iso, sha256_text, stable_id, write_text_atomic
from .worker import completed_episode_context_for_episode


APP_SERVER_DEVELOPMENT_MANIFEST_VERSION = "pif_app_server_development_manifest_v1"
APP_SERVER_DEVELOPMENT_MANIFEST_V2 = "pif_app_server_development_manifest_v2"
APP_SERVER_SHARED_REFERENCE_SEED_VERSION = "pif_shared_reference_seed_v1"
APP_SERVER_REFERENCE_NOISE_VERSION = "pif_reference_noise_v1"
APP_SERVER_EPISODE_BATCH_SCHEMA_VERSION = "pif_app_server_episode_batch_core_v3"
APP_SERVER_CORE_ARM_VERSION = "pif_app_server_core_arm_v3"
APP_SERVER_CORE_MATRIX_VERSION = "pif_app_server_core_matrix_v3"


def _sha256_file(path: str | Path) -> str:
    return sha256_text(Path(path).expanduser().resolve().read_text(encoding="utf-8"))


def _sha256_file_bytes(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).expanduser().resolve().open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _resolve_corpus_artifact(path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    return candidate.resolve() if candidate.is_absolute() else (corpus_dir().parent / candidate).resolve()


def _segment_file_sha256(conn, segment_id: str) -> str:
    row = conn.execute("SELECT text_path FROM segments WHERE id = ?", (segment_id,)).fetchone()
    if not row:
        raise ValueError(f"segment not found while hashing text: {segment_id}")
    return _sha256_file_bytes(_resolve_corpus_artifact(row["text_path"]))


def _collect_exclusion_provenance(value: Any) -> tuple[set[str], set[str]]:
    episode_ids: set[str] = set()
    text_hashes: set[str] = set()
    stack = [value]
    while stack:
        item = stack.pop()
        if isinstance(item, dict):
            for key, child in item.items():
                if key == "episode_id" and isinstance(child, str):
                    episode_ids.add(child)
                elif key in {"text_sha256", "segment_text_sha256"} and isinstance(child, str):
                    text_hashes.add(child)
                else:
                    stack.append(child)
        elif isinstance(item, list):
            stack.extend(item)
    return episode_ids, text_hashes


def _load_exclusion_manifests(paths: list[str | Path]) -> tuple[set[str], set[str], list[dict[str, Any]]]:
    episode_ids: set[str] = set()
    text_hashes: set[str] = set()
    provenance = []
    for raw_path in paths:
        path = Path(raw_path).expanduser().resolve()
        payload = json.loads(path.read_text(encoding="utf-8"))
        manifest_episode_ids, manifest_text_hashes = _collect_exclusion_provenance(payload)
        episode_ids.update(manifest_episode_ids)
        text_hashes.update(manifest_text_hashes)
        provenance.append(
            {
                "artifact_sha256": _sha256_file_bytes(path),
                "episode_id_count": len(manifest_episode_ids),
                "text_sha256_count": len(manifest_text_hashes),
            }
        )
    return episode_ids, text_hashes, provenance


def _canonical_context_transcript(conn, *, episode_id: str, label_pack: str, golden_model: str):
    context_run = completed_episode_context_for_episode(
        conn,
        episode_id,
        label_pack=label_pack,
        model=golden_model,
    )
    if not context_run or not context_run["context_artifact_path"]:
        raise ValueError(f"episode {episode_id} has no completed episode context")
    transcript_id = context_run["transcript_id"]
    if not transcript_id:
        raise ValueError(f"episode {episode_id} context run is not bound to a transcript")
    transcript = conn.execute(
        """
        SELECT t.id, t.episode_id, t.source_kind, t.source_url, t.raw_text_path,
               t.raw_text_sha256, t.status, t.word_count, t.updated_at,
               tp.id AS preparation_id, tp.status AS preparation_status,
               tp.cleaned_text_path, tp.cleaned_text_sha256,
               tp.substantive_word_count, tp.quality_score
        FROM transcripts t
        LEFT JOIN transcript_preparations tp ON tp.transcript_id = t.id
        WHERE t.id = ? AND t.episode_id = ?
        ORDER BY tp.updated_at DESC, tp.id DESC
        LIMIT 1
        """,
        (transcript_id, episode_id),
    ).fetchone()
    if not transcript or transcript["status"] != "ready":
        raise ValueError(f"episode {episode_id} context-bound transcript is not ready")
    if transcript["preparation_id"] and transcript["preparation_status"] != "prepared":
        raise ValueError(f"episode {episode_id} context-bound transcript preparation is not ready")
    context_path = Path(context_run["context_artifact_path"]).expanduser().resolve()
    if not context_path.exists():
        raise ValueError(f"episode {episode_id} context artifact is missing")
    return context_run, transcript, context_path


def export_app_server_development_manifest_v2(
    conn,
    *,
    output_path: str | Path,
    episode_ids: list[str],
    exclusion_manifest_paths: list[str | Path],
    segments_per_episode: int = 8,
    minimum_no_signal_per_episode: int = 1,
    maximum_no_signal_per_episode: int = 2,
    minimum_dense_per_episode: int = 7,
    dense_event_min: int = 16,
    event_cap: int = 32,
    label_pack: str = "ai_discourse_v3_1",
    golden_model: str = "gpt-5.5",
    seed: str = "app-server-development-v2",
) -> dict[str, Any]:
    if len(episode_ids) < 2 or len(set(episode_ids)) != len(episode_ids):
        raise ValueError("development manifest v2 requires at least two unique episodes")
    if segments_per_episode < 1:
        raise ValueError("segments per episode must be positive")
    if not 1 <= minimum_no_signal_per_episode <= maximum_no_signal_per_episode:
        raise ValueError("invalid no-signal selection bounds")
    if minimum_no_signal_per_episode + minimum_dense_per_episode > segments_per_episode:
        raise ValueError("required no-signal and dense counts exceed the episode allocation")
    if event_cap < dense_event_min:
        raise ValueError("event cap must be at least the dense-event threshold")

    target = Path(output_path).expanduser().resolve()
    reference_path = target.parent / "shared-reference-seed-v1.json"
    noise_path = target.parent / "reference-noise-v1.json"
    for artifact in (target, reference_path, noise_path):
        if artifact.exists():
            raise ValueError(f"development manifest v2 artifact already exists: {artifact}")
    excluded_episode_ids, excluded_text_hashes, exclusion_provenance = _load_exclusion_manifests(
        exclusion_manifest_paths
    )

    episode_records = []
    reference_records = []
    noise_records = []
    selected_text_hashes: set[str] = set()
    selected_sources: set[str] = set()
    for episode_id in episode_ids:
        if episode_id in excluded_episode_ids:
            raise ValueError(f"development episode is reserved by an exclusion manifest: {episode_id}")
        episode_row = conn.execute(
            """
            SELECT e.id, e.title, e.source_id, s.name AS source_name
            FROM episodes e
            JOIN sources s ON s.id = e.source_id
            WHERE e.id = ?
            """,
            (episode_id,),
        ).fetchone()
        if not episode_row:
            raise ValueError(f"episode not found: {episode_id}")
        if episode_row["source_id"] in selected_sources:
            raise ValueError("development manifest v2 requires unique sources for its explicit episodes")
        selected_sources.add(episode_row["source_id"])
        context_run, transcript, context_path = _canonical_context_transcript(
            conn,
            episode_id=episode_id,
            label_pack=label_pack,
            golden_model=golden_model,
        )
        canonical_rows = conn.execute(
            """
            SELECT seg.id AS segment_id, seg.segment_index, seg.text_sha256,
                   l.id AS label_id, l.output_json, l.output_path AS label_output_path,
                   l.created_at AS label_created_at,
                   (
                     SELECT lr.id
                     FROM label_runs lr
                     WHERE lr.segment_id = seg.id
                       AND lr.label_pack = l.label_pack
                       AND lr.model = l.model
                       AND lr.status = 'completed'
                       AND (lr.output_path = l.output_path OR l.output_path IS NULL)
                     ORDER BY lr.completed_at DESC, lr.updated_at DESC, lr.id DESC
                     LIMIT 1
                   ) AS label_run_id
            FROM segments seg
            JOIN labels l ON l.segment_id = seg.id
            WHERE seg.episode_id = ?
              AND seg.transcript_id = ?
              AND l.label_pack = ?
              AND l.model = ?
              AND l.status IN ('ready', 'completed')
            ORDER BY seg.segment_index, l.created_at DESC, l.id DESC
            """,
            (episode_id, transcript["id"], label_pack, golden_model),
        ).fetchall()
        by_segment = {}
        for row in canonical_rows:
            by_segment.setdefault(row["segment_id"], row)
        canonical_rows = list(by_segment.values())
        index_counts = Counter(int(row["segment_index"]) for row in canonical_rows)
        duplicate_indices = sorted(index for index, count in index_counts.items() if count > 1)
        if duplicate_indices:
            raise ValueError(f"canonical transcript has duplicate segment indices: {duplicate_indices}")
        text_counts = Counter(str(row["text_sha256"] or "") for row in canonical_rows)
        duplicate_hashes = sorted(value for value, count in text_counts.items() if value and count > 1)
        if duplicate_hashes:
            raise ValueError("canonical transcript contains duplicate segment text hashes")

        candidates = []
        canonical_output_by_hash = {}
        for row in canonical_rows:
            if not row["label_run_id"]:
                raise ValueError(f"canonical segment has no completed label run: {row['segment_id']}")
            segment_text, read_error = _safe_segment_text(conn, row["segment_id"])
            if read_error:
                raise ValueError(f"canonical segment text unavailable: {read_error}")
            text_hash = _segment_file_sha256(conn, row["segment_id"])
            if row["text_sha256"] and row["text_sha256"] != text_hash:
                raise ValueError(f"canonical segment text hash drift: {row['segment_id']}")
            if text_hash in excluded_text_hashes:
                continue
            if text_hash in selected_text_hashes:
                raise ValueError("development manifest v2 selected duplicate text across episodes")
            golden = json.loads(row["output_json"])
            event_count = len(golden.get("discourse_events") or [])
            if not row["label_output_path"]:
                raise ValueError(f"canonical label has no output artifact: {row['label_id']}")
            label_output_path = Path(row["label_output_path"]).expanduser().resolve()
            if not label_output_path.exists():
                raise ValueError(f"label run output artifact is missing: {row['label_run_id']}")
            golden_output_sha = sha256_text(row["output_json"])
            reference_id = stable_id(
                row["label_id"],
                row["label_run_id"],
                text_hash,
                golden_output_sha,
                prefix="refseed_",
            )
            candidate = {
                "segment_id": row["segment_id"],
                "segment_index": int(row["segment_index"]),
                "text_sha256": text_hash,
                "label_id": row["label_id"],
                "label_run_id": row["label_run_id"],
                "golden_output_sha256": golden_output_sha,
                "label_run_output_sha256": _sha256_file_bytes(label_output_path),
                "golden_event_count": event_count,
                "density_stratum": _density(event_count, dense_event_min=dense_event_min),
                "shared_reference_seed_id": reference_id,
            }
            candidates.append(candidate)
            canonical_output_by_hash[text_hash] = candidate

        no_signal = sorted(
            (row for row in candidates if row["density_stratum"] == "no_signal"),
            key=lambda row: row["segment_index"],
        )
        dense = sorted(
            (row for row in candidates if row["density_stratum"] == "dense"),
            key=lambda row: (-row["golden_event_count"], row["segment_index"]),
        )
        if len(no_signal) < minimum_no_signal_per_episode:
            raise ValueError(f"episode {episode_id} lacks genuine no-signal cases")
        no_signal_count = min(maximum_no_signal_per_episode, len(no_signal))
        dense_count = segments_per_episode - no_signal_count
        if dense_count < minimum_dense_per_episode or len(dense) < dense_count:
            raise ValueError(f"episode {episode_id} lacks required dense cases")
        selected = [*no_signal[:no_signal_count], *dense[:dense_count]]
        selected.sort(key=lambda row: row["segment_index"])
        observed_max = max(int(row["golden_event_count"]) for row in selected)
        if observed_max > event_cap:
            raise ValueError(
                f"event cap {event_cap} is below observed development maximum {observed_max}"
            )
        for row in selected:
            selected_text_hashes.add(row["text_sha256"])
            golden_row = next(item for item in canonical_rows if item["segment_id"] == row["segment_id"])
            reference_records.append(
                {
                    "shared_reference_seed_id": row["shared_reference_seed_id"],
                    "episode_id": episode_id,
                    "segment_id": row["segment_id"],
                    "text_sha256": row["text_sha256"],
                    "label_id": row["label_id"],
                    "label_run_id": row["label_run_id"],
                    "golden_output_sha256": row["golden_output_sha256"],
                    "golden_output": json.loads(golden_row["output_json"]),
                }
            )

        placeholders = ",".join("?" for _ in selected)
        alternate_rows = conn.execute(
            f"""
            SELECT seg.text_sha256, seg.id AS segment_id, seg.transcript_id,
                   l.id AS label_id, l.output_json,
                   (
                     SELECT lr.id FROM label_runs lr
                     WHERE lr.segment_id = seg.id
                       AND lr.label_pack = l.label_pack
                       AND lr.model = l.model
                       AND lr.status = 'completed'
                     ORDER BY lr.completed_at DESC, lr.updated_at DESC, lr.id DESC
                     LIMIT 1
                   ) AS label_run_id
            FROM segments seg
            JOIN labels l ON l.segment_id = seg.id
            WHERE seg.episode_id = ?
              AND seg.text_sha256 IN ({placeholders})
              AND l.label_pack = ?
              AND l.model = ?
              AND l.status IN ('ready', 'completed')
            ORDER BY seg.text_sha256, seg.transcript_id, seg.id
            """,
            (
                episode_id,
                *(row["text_sha256"] for row in selected),
                label_pack,
                golden_model,
            ),
        ).fetchall()
        alternatives_by_hash: dict[str, list[dict[str, Any]]] = {}
        for row in alternate_rows:
            output_sha = sha256_text(row["output_json"])
            alternatives_by_hash.setdefault(row["text_sha256"], []).append(
                {
                    "segment_id": row["segment_id"],
                    "transcript_id": row["transcript_id"],
                    "label_id": row["label_id"],
                    "label_run_id": row["label_run_id"],
                    "golden_output_sha256": output_sha,
                    "golden_event_count": len(json.loads(row["output_json"]).get("discourse_events") or []),
                }
            )
        for text_hash, alternatives in sorted(alternatives_by_hash.items()):
            if len(alternatives) < 2:
                continue
            unique_outputs = {item["golden_output_sha256"] for item in alternatives}
            canonical = canonical_output_by_hash[text_hash]
            noise_records.append(
                {
                    "episode_id": episode_id,
                    "text_sha256": text_hash,
                    "canonical_label_id": canonical["label_id"],
                    "replicate_label_count": len(alternatives),
                    "distinct_output_count": len(unique_outputs),
                    "event_counts": sorted({item["golden_event_count"] for item in alternatives}),
                    "disagreement": len(unique_outputs) > 1,
                    "reconciliation_status": (
                        "reference_noise_requires_support_alignment_reconciliation"
                        if len(unique_outputs) > 1
                        else "exact_replicate_output"
                    ),
                    "replicates": alternatives,
                }
            )

        canonical_transcript_path = _resolve_corpus_artifact(transcript["raw_text_path"])
        if not canonical_transcript_path.exists():
            raise ValueError("canonical transcript artifact is missing")
        if _sha256_file_bytes(canonical_transcript_path) != transcript["raw_text_sha256"]:
            raise ValueError("canonical transcript artifact hash drift")
        preparation_hash = None
        if transcript["preparation_id"]:
            preparation_path = _resolve_corpus_artifact(transcript["cleaned_text_path"])
            if not preparation_path.exists():
                raise ValueError("canonical prepared transcript artifact is missing")
            preparation_hash = _sha256_file_bytes(preparation_path)
            if preparation_hash != transcript["cleaned_text_sha256"]:
                raise ValueError("canonical prepared transcript artifact hash drift")
        density_counts = Counter(row["density_stratum"] for row in selected)
        episode_records.append(
            {
                "episode_id": episode_id,
                "source_id": episode_row["source_id"],
                "source_name": episode_row["source_name"],
                "episode_title": episode_row["title"] or "",
                "canonical_transcript": {
                    "selection_policy": "completed_episode_context_bound_transcript",
                    "transcript_id": transcript["id"],
                    "raw_text_sha256": transcript["raw_text_sha256"],
                    "source_kind": transcript["source_kind"],
                    "preparation_id": transcript["preparation_id"],
                    "prepared_text_sha256": preparation_hash,
                },
                "episode_context": {
                    "run_id": context_run["id"],
                    "transcript_id": context_run["transcript_id"],
                    "artifact_path": str(context_path),
                    "artifact_sha256": _sha256_file_bytes(context_path),
                },
                "segments": selected,
                "density_counts": dict(sorted(density_counts.items())),
                "observed_golden_event_max": observed_max,
            }
        )

    source_count = len({episode["source_id"] for episode in episode_records})
    if source_count != len(episode_records):
        raise ValueError("development manifest v2 source uniqueness invariant failed")
    all_selected = [segment for episode in episode_records for segment in episode["segments"]]
    if len({segment["text_sha256"] for segment in all_selected}) != len(all_selected):
        raise ValueError("development manifest v2 contains duplicate selected text")
    if len({(episode["episode_id"], segment["segment_index"]) for episode in episode_records for segment in episode["segments"]}) != len(all_selected):
        raise ValueError("development manifest v2 contains duplicate canonical segment indices")

    reference_payload = {
        "schema_version": APP_SERVER_SHARED_REFERENCE_SEED_VERSION,
        "created_at": now_iso(),
        "reference_policy": "one_immutable_canonical_baseline_seed_per_unique_text_for_later_shared_augmentation",
        "label_pack": label_pack,
        "golden_model": golden_model,
        "references": reference_records,
        "privacy": "private_analysis_only_contains_baseline_event_evidence",
    }
    noise_payload = {
        "schema_version": APP_SERVER_REFERENCE_NOISE_VERSION,
        "created_at": now_iso(),
        "policy": "duplicate_text_labels_are_reference_noise_not_additional_cases",
        "records": noise_records,
        "disagreement_count": sum(bool(record["disagreement"]) for record in noise_records),
        "privacy": "private_analysis_only_hashes_ids_and_event_counts_no_transcript_text",
    }
    write_text_atomic(
        reference_path,
        json.dumps(reference_payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
    )
    write_text_atomic(
        noise_path,
        json.dumps(noise_payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
    )
    density_counts = Counter(segment["density_stratum"] for segment in all_selected)
    payload = {
        "schema_version": APP_SERVER_DEVELOPMENT_MANIFEST_V2,
        "evaluation_role": "retrospective_balanced_development_not_acceptance",
        "created_at": now_iso(),
        "seed": seed,
        "selection_policy": "explicit_multi_source_context_bound_transcripts_llm_label_counts_only_no_transcript_semantic_rules",
        "label_pack": label_pack,
        "golden_model": golden_model,
        "event_cap": event_cap,
        "dense_event_min": dense_event_min,
        "segments_per_episode": segments_per_episode,
        "minimum_no_signal_per_episode": minimum_no_signal_per_episode,
        "maximum_no_signal_per_episode": maximum_no_signal_per_episode,
        "minimum_dense_per_episode": minimum_dense_per_episode,
        "episode_count": len(episode_records),
        "source_count": source_count,
        "segment_count": len(all_selected),
        "unique_text_sha256_count": len({segment["text_sha256"] for segment in all_selected}),
        "unique_episode_segment_index_count": len(
            {
                (episode["episode_id"], segment["segment_index"])
                for episode in episode_records
                for segment in episode["segments"]
            }
        ),
        "density_counts": dict(sorted(density_counts.items())),
        "observed_golden_event_max": max(segment["golden_event_count"] for segment in all_selected),
        "episodes": episode_records,
        "exclusion_policy": "reject_by_episode_id_or_segment_text_sha256",
        "exclusion_manifest_provenance": exclusion_provenance,
        "shared_reference_seed": {
            "schema_version": APP_SERVER_SHARED_REFERENCE_SEED_VERSION,
            "artifact_path": str(reference_path),
            "artifact_sha256": _sha256_file_bytes(reference_path),
            "reference_count": len(reference_records),
        },
        "reference_noise": {
            "schema_version": APP_SERVER_REFERENCE_NOISE_VERSION,
            "artifact_path": str(noise_path),
            "artifact_sha256": _sha256_file_bytes(noise_path),
            "record_count": len(noise_records),
            "disagreement_count": noise_payload["disagreement_count"],
        },
        "privacy": "private_analysis_only_contains_local_paths_ids_and_reference_provenance",
    }
    write_text_atomic(target, json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
    return {
        "ok": True,
        "manifest_path": str(target),
        "manifest_sha256": _sha256_file_bytes(target),
        "episode_count": len(episode_records),
        "source_count": source_count,
        "segment_count": len(all_selected),
        "unique_text_sha256_count": payload["unique_text_sha256_count"],
        "density_counts": payload["density_counts"],
        "event_cap": event_cap,
        "observed_golden_event_max": payload["observed_golden_event_max"],
        "reference_noise_disagreements": noise_payload["disagreement_count"],
        "evaluation_role": payload["evaluation_role"],
    }


def _density(event_count: int, *, dense_event_min: int) -> str:
    if event_count == 0:
        return "no_signal"
    if event_count >= dense_event_min:
        return "dense"
    return "other"


def export_app_server_development_manifest(
    conn,
    *,
    output_path: str | Path,
    episode_ids: list[str],
    segments_per_episode: int = 16,
    minimum_no_signal_per_episode: int = 1,
    minimum_dense_per_episode: int = 8,
    dense_event_min: int = 16,
    label_pack: str = "ai_discourse_v3_1",
    golden_model: str = "gpt-5.5",
    seed: str = "app-server-development-v1",
) -> dict[str, Any]:
    if not episode_ids:
        raise ValueError("at least one development episode is required")
    if len(set(episode_ids)) != len(episode_ids):
        raise ValueError("development episode IDs must be unique")
    if segments_per_episode < 1:
        raise ValueError("segments per episode must be positive")
    if minimum_no_signal_per_episode < 1 or minimum_dense_per_episode < 1:
        raise ValueError("development selection must include no-signal and dense segments")
    if minimum_no_signal_per_episode + minimum_dense_per_episode > segments_per_episode:
        raise ValueError("required density counts exceed segments per episode")

    episodes = []
    for episode_id in episode_ids:
        episode_row = conn.execute(
            """
            SELECT e.id, e.title, e.source_id, s.name AS source_name
            FROM episodes e
            JOIN sources s ON s.id = e.source_id
            WHERE e.id = ?
            """,
            (episode_id,),
        ).fetchone()
        if not episode_row:
            raise ValueError(f"episode not found: {episode_id}")
        rows = conn.execute(
            """
            SELECT seg.id AS segment_id, seg.segment_index, seg.text_sha256,
                   l.output_json, l.created_at
            FROM segments seg
            JOIN labels l ON l.segment_id = seg.id
            WHERE seg.episode_id = ?
              AND l.label_pack = ?
              AND l.model = ?
              AND l.status IN ('ready', 'completed')
            ORDER BY seg.segment_index, l.created_at DESC, l.id DESC
            """,
            (episode_id, label_pack, golden_model),
        ).fetchall()
        latest_by_segment = {}
        for row in rows:
            latest_by_segment.setdefault(row["segment_id"], row)
        candidates = []
        for row in latest_by_segment.values():
            golden = json.loads(row["output_json"])
            event_count = len(golden.get("discourse_events") or [])
            segment_text, read_error = _safe_segment_text(conn, row["segment_id"])
            if read_error:
                raise ValueError(f"development segment text unavailable: {read_error}")
            observed_text_sha = sha256_text(segment_text)
            expected_text_sha = str(row["text_sha256"] or "")
            if expected_text_sha and expected_text_sha != observed_text_sha:
                raise ValueError(f"development segment hash drift: {row['segment_id']}")
            candidates.append(
                {
                    "segment_id": row["segment_id"],
                    "segment_index": int(row["segment_index"]),
                    "text_sha256": observed_text_sha,
                    "golden_output_sha256": sha256_text(row["output_json"]),
                    "golden_event_count": event_count,
                    "density_stratum": _density(event_count, dense_event_min=dense_event_min),
                }
            )

        no_signal = sorted(
            (row for row in candidates if row["density_stratum"] == "no_signal"),
            key=lambda row: row["segment_index"],
        )
        dense = sorted(
            (row for row in candidates if row["density_stratum"] == "dense"),
            key=lambda row: (-row["golden_event_count"], row["segment_index"]),
        )
        if len(no_signal) < minimum_no_signal_per_episode:
            raise ValueError(f"episode {episode_id} lacks required no-signal development segments")
        if len(dense) < minimum_dense_per_episode:
            raise ValueError(f"episode {episode_id} lacks required dense development segments")

        selected = list(no_signal[:minimum_no_signal_per_episode])
        selected_ids = {row["segment_id"] for row in selected}
        remaining = sorted(
            (row for row in candidates if row["segment_id"] not in selected_ids),
            key=lambda row: (-row["golden_event_count"], row["segment_index"]),
        )
        selected.extend(remaining[: segments_per_episode - len(selected)])
        if len(selected) != segments_per_episode:
            raise ValueError(f"episode {episode_id} lacks {segments_per_episode} eligible development segments")
        selected.sort(key=lambda row: row["segment_index"])
        selected_counts = Counter(row["density_stratum"] for row in selected)
        if selected_counts["dense"] < minimum_dense_per_episode:
            raise ValueError(f"episode {episode_id} selection does not retain enough dense segments")

        context_run = completed_episode_context_for_episode(
            conn,
            episode_id,
            label_pack=label_pack,
            model=golden_model,
        )
        if not context_run or not context_run["context_artifact_path"]:
            raise ValueError(f"episode {episode_id} has no completed episode context")
        context_path = Path(context_run["context_artifact_path"]).expanduser().resolve()
        if not context_path.exists():
            raise ValueError(f"episode {episode_id} context artifact is missing")
        episodes.append(
            {
                "episode_id": episode_id,
                "source_id": episode_row["source_id"],
                "source_name": episode_row["source_name"],
                "episode_title": episode_row["title"] or "",
                "episode_context_run_id": context_run["id"],
                "episode_context_artifact_path": str(context_path),
                "episode_context_artifact_sha256": _sha256_file(context_path),
                "segments": selected,
                "density_counts": dict(sorted(selected_counts.items())),
            }
        )

    payload = {
        "schema_version": APP_SERVER_DEVELOPMENT_MANIFEST_VERSION,
        "evaluation_role": "retrospective_development_not_acceptance",
        "created_at": now_iso(),
        "seed": seed,
        "selection_policy": "explicit_episode_llm_golden_event_count_only_no_transcript_keyword_or_regex_selection",
        "label_pack": label_pack,
        "golden_model": golden_model,
        "segments_per_episode": segments_per_episode,
        "minimum_no_signal_per_episode": minimum_no_signal_per_episode,
        "minimum_dense_per_episode": minimum_dense_per_episode,
        "dense_event_min": dense_event_min,
        "episodes": episodes,
        "privacy": "private_analysis_only_contains_local_artifact_paths_and_segment_ids",
    }
    target = Path(output_path).expanduser().resolve()
    write_text_atomic(target, json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
    return {
        "ok": True,
        "manifest_path": str(target),
        "manifest_sha256": _sha256_file(target),
        "episode_count": len(episodes),
        "segment_count": sum(len(episode["segments"]) for episode in episodes),
        "density_counts": dict(
            sorted(
                Counter(
                    segment["density_stratum"]
                    for episode in episodes
                    for segment in episode["segments"]
                ).items()
            )
        ),
        "evaluation_role": payload["evaluation_role"],
    }


def episode_batch_core_schema(
    *,
    episode_id: str,
    segment_ids: list[str],
    max_events_per_segment: int = 25,
) -> dict[str, Any]:
    if not segment_ids or len(set(segment_ids)) != len(segment_ids):
        raise ValueError("batch segment IDs must be non-empty and unique")
    segment_schema = json.loads(
        json.dumps(windowed_event_core_schema(max_events=max_events_per_segment), ensure_ascii=True)
    )
    segment_schema.pop("$schema", None)
    segment_schema.pop("allOf", None)
    segment_schema["properties"]["segment_id"] = {
        "type": "string",
        "enum": list(segment_ids),
    }
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["episode_id", "segments"],
        "properties": {
            "episode_id": {"type": "string", "enum": [episode_id]},
            "segments": {
                "type": "array",
                "minItems": len(segment_ids),
                "maxItems": len(segment_ids),
                "items": segment_schema,
            },
        },
    }


def episode_batch_core_instructions(
    *,
    guideline_path: str | Path = DEFAULT_WINDOWED_GUIDELINES_PATH,
    max_events_per_segment: int = 25,
) -> tuple[str, str]:
    guidelines, guideline_sha = _load_windowed_guideline_instructions(guideline_path)
    base = _windowed_event_core_instruction(
        guideline_instructions=guidelines,
        max_total_events=max_events_per_segment,
    )
    contract = (
        "Episode-batch contract: these thread instructions contain one immutable shared episode context. Each user "
        "turn contains only one or more independent segment packets. Apply every rule above independently to every "
        "listed segment. Return one top-level "
        "episode_id and exactly one segment object for every listed segment_id. Each segment has its own single global "
        f"events array capped at {max_events_per_segment}; never nest events under windows and never move evidence "
        "between segments. Keep output segment order equal to input order."
    )
    return base + "\n\n" + contract, str(guideline_sha or "")


def build_episode_base_instructions(
    *,
    core_instructions: str,
    episode: dict[str, Any],
    episode_context: dict[str, Any],
) -> str:
    shared = {
        "episode_id": episode["episode_id"],
        "source_name": episode.get("source_name") or "",
        "episode_title": episode.get("episode_title") or "",
        "context_summary": episode_context.get("context_summary") or "",
        "speaker_map": episode_context.get("speaker_map") or [],
        "section_map": episode_context.get("section_map") or [],
        "entity_seed": episode_context.get("entity_seed") or {},
        "concept_seed": episode_context.get("concept_seed") or [],
        "extraction_guidance": episode_context.get("extraction_guidance") or "",
        "quality_flags": episode_context.get("quality_flags") or [],
    }
    return (
        core_instructions
        + "\n\n# Shared episode title, context, and extraction guidance\n"
        + json.dumps(shared, ensure_ascii=True, separators=(",", ":"))
        + "\n"
    )


def build_episode_batch_prompt(*, segments: list[dict[str, Any]]) -> str:
    segment_packets = [
        {
            "segment_id": segment["segment_id"],
            "segment_index": segment["segment_index"],
            "windows": segment["windows"],
        }
        for segment in segments
    ]
    return (
        "# Segment-specific IDs and fixed evidence windows\n"
        + json.dumps(segment_packets, ensure_ascii=True, separators=(",", ":"))
        + "\n"
    )


def plan_episode_batches(segments: list[dict[str, Any]], *, batch_size: int) -> list[list[dict[str, Any]]]:
    if batch_size < 1:
        raise ValueError("batch size must be positive")
    return [segments[index : index + batch_size] for index in range(0, len(segments), batch_size)]


def _metric_grounding_errors(event: dict[str, Any]) -> list[str]:
    evidence = str(event.get("evidence") or "")
    metric_raw = str(event.get("metric_raw_text") or "")
    parts = {
        key: str(event.get(key) or "")
        for key in ("metric_value", "metric_unit", "metric_comparator")
        if str(event.get(key) or "")
    }
    errors = []
    if metric_raw and metric_raw not in evidence:
        errors.append("metric_raw_text_not_in_evidence")
    if parts and not metric_raw:
        errors.append("metric_components_without_raw_text")
    for key, value in parts.items():
        if value not in evidence and value not in metric_raw:
            errors.append(f"{key}_not_in_evidence")
    return errors


def normalize_episode_batch_output(
    payload: dict[str, Any],
    *,
    episode_id: str,
    prepared_segments: list[dict[str, Any]],
    max_events_per_segment: int = 25,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    expected_ids = [segment["segment_id"] for segment in prepared_segments]
    schema = episode_batch_core_schema(
        episode_id=episode_id,
        segment_ids=expected_ids,
        max_events_per_segment=max_events_per_segment,
    )
    _validate_schema(schema, payload, path="$")
    if payload.get("episode_id") != episode_id:
        raise ValidationError("$.episode_id does not match the requested episode")
    rows = payload.get("segments") or []
    if len(rows) != len(expected_ids):
        raise ValidationError("$.segments must contain exactly the requested segment count")
    actual_ids = [row.get("segment_id") if isinstance(row, dict) else None for row in rows]
    if actual_ids != expected_ids:
        raise ValidationError("$.segments must preserve every requested segment exactly once and in order")

    prepared_by_id = {segment["segment_id"]: segment for segment in prepared_segments}
    normalized_rows = []
    diagnostics = []
    segment_schema = schema["properties"]["segments"]["items"]
    for row in rows:
        _validate_schema(segment_schema, row, path=f"$.segments[{len(normalized_rows)}]")
        prepared = prepared_by_id[row["segment_id"]]
        normalized, ownership = normalize_windowed_core_payload(
            row,
            segment_text=prepared["segment_text"],
            boundaries=prepared["boundaries"],
            max_events=max_events_per_segment,
        )
        _validate_schema(segment_schema, normalized, path=f"$.segments[{len(normalized_rows)}]")
        if (normalized.get("status") == "coded") != bool(normalized.get("events")):
            raise ValidationError("coded status must match grounded event presence")
        metric_errors = [
            {"event_index": index, "errors": _metric_grounding_errors(event)}
            for index, event in enumerate(normalized.get("events") or [])
            if _metric_grounding_errors(event)
        ]
        normalized_rows.append(normalized)
        diagnostics.append(
            {
                "segment_id": row["segment_id"],
                "input_events": ownership["input_events"],
                "output_events": ownership["output_events"],
                "exactness_pruned_events": ownership["exactness_pruned_events"],
                "event_cap_hit": ownership["event_cap_hit"],
                "metric_grounding_error_events": len(metric_errors),
                "metric_grounding_errors": metric_errors,
                "status_ok": not metric_errors,
            }
        )
    return {"episode_id": episode_id, "segments": normalized_rows}, diagnostics


def _load_prepared_episodes(
    conn,
    *,
    manifest: dict[str, Any],
    window_count: int,
    context_chars: int,
) -> list[dict[str, Any]]:
    if manifest.get("schema_version") != APP_SERVER_DEVELOPMENT_MANIFEST_V2:
        raise ValueError("app-server core arms require development manifest v2")
    prepared_episodes = []
    for episode in manifest.get("episodes") or []:
        context_record = episode.get("episode_context") or {}
        context_path = Path(context_record["artifact_path"]).expanduser().resolve()
        if _sha256_file_bytes(context_path) != context_record["artifact_sha256"]:
            raise ValueError("episode context artifact hash drift")
        episode_context = json.loads(context_path.read_text(encoding="utf-8"))
        prepared_segments = []
        for segment in episode.get("segments") or []:
            segment_text, read_error = _safe_segment_text(conn, segment["segment_id"])
            if read_error:
                raise ValueError(f"development segment text unavailable: {read_error}")
            if _segment_file_sha256(conn, segment["segment_id"]) != segment["text_sha256"]:
                raise ValueError("development segment text hash drift")
            windows, boundaries = build_windowed_segment_packet(
                segment_text,
                window_count=window_count,
                context_chars=context_chars,
            )
            prepared_segments.append(
                {
                    **segment,
                    "segment_text": segment_text,
                    "windows": windows,
                    "boundaries": boundaries,
                }
            )
        prepared_episodes.append(
            {
                **episode,
                "episode_context": episode_context,
                "segments": prepared_segments,
            }
        )
    return prepared_episodes


def _read_sidecar(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _sidecar_usage(path: Path) -> tuple[dict[str, int] | None, bool]:
    payload = _read_sidecar(path)
    if payload is None:
        return None, False
    usage = payload.get("usage")
    return (usage if isinstance(usage, dict) else None), bool(payload.get("usage_complete"))


async def run_app_server_core_arm(
    conn,
    *,
    manifest_path: str | Path,
    output_dir: str | Path,
    batch_size: int,
    thread_mode: str,
    model: str = "gpt-5.6-sol",
    reasoning_effort: str = "low",
    concurrency: int = 1,
    timeout_seconds: float = 600.0,
    window_count: int = 4,
    context_chars: int = 900,
    max_events_per_segment: int | None = None,
    guideline_path: str | Path = DEFAULT_WINDOWED_GUIDELINES_PATH,
    client_factory: Callable[[], CodexAppServerClient] = CodexAppServerClient,
) -> dict[str, Any]:
    if thread_mode not in {"new_thread", "same_thread"}:
        raise ValueError("thread mode must be new_thread or same_thread")
    if concurrency < 1:
        raise ValueError("concurrency must be positive")
    manifest_file = Path(manifest_path).expanduser().resolve()
    manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
    if manifest.get("schema_version") != APP_SERVER_DEVELOPMENT_MANIFEST_V2:
        raise ValueError("app-server core arms require development manifest v2")
    manifest_event_cap = int(manifest.get("event_cap") or 0)
    if manifest_event_cap < 1:
        raise ValueError("development manifest v2 has no valid frozen event cap")
    if max_events_per_segment is None:
        max_events_per_segment = manifest_event_cap
    elif max_events_per_segment != manifest_event_cap:
        raise ValueError("core arm event cap must equal the frozen manifest event cap")
    prepared_episodes = _load_prepared_episodes(
        conn,
        manifest=manifest,
        window_count=window_count,
        context_chars=context_chars,
    )
    root = Path(output_dir).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    report_path = root / "report.json"
    mapping_path = root / "private-mapping.json"
    if report_path.exists() or mapping_path.exists():
        raise ValueError("app-server arm output already exists; explicit recovery or a new output directory is required")

    core_instructions, guideline_sha = episode_batch_core_instructions(
        guideline_path=guideline_path,
        max_events_per_segment=max_events_per_segment,
    )
    episode_base_by_id = {
        episode["episode_id"]: build_episode_base_instructions(
            core_instructions=core_instructions,
            episode=episode,
            episode_context=episode["episode_context"],
        )
        for episode in prepared_episodes
    }
    episode_base_fingerprints = sorted(
        (
            sha256_text(base),
            len(base.encode("utf-8")),
            len(base.encode("utf-8")) - len(core_instructions.encode("utf-8")),
        )
        for base in episode_base_by_id.values()
    )
    episode_base_set_sha = sha256_text(
        json.dumps(episode_base_fingerprints, ensure_ascii=True, separators=(",", ":"))
    )
    batches = []
    for episode in prepared_episodes:
        for batch_index, segment_batch in enumerate(
            plan_episode_batches(episode["segments"], batch_size=batch_size)
        ):
            opaque_id = stable_id(
                manifest_file.as_posix(),
                thread_mode,
                str(batch_size),
                episode["episode_id"],
                str(batch_index),
                prefix="asb_",
            )
            prompt = build_episode_batch_prompt(segments=segment_batch)
            segment_ids = [segment["segment_id"] for segment in segment_batch]
            schema = episode_batch_core_schema(
                episode_id=episode["episode_id"],
                segment_ids=segment_ids,
                max_events_per_segment=max_events_per_segment,
            )
            batches.append(
                {
                    "batch_id": opaque_id,
                    "batch_index": batch_index,
                    "episode": episode,
                    "segments": segment_batch,
                    "segment_ids": segment_ids,
                    "prompt": prompt,
                    "base_instructions": episode_base_by_id[episode["episode_id"]],
                    "schema": schema,
                    "sidecar_path": root / "sidecars" / f"{opaque_id}.json",
                    "raw_output_path": root / "raw_outputs" / f"{opaque_id}.json",
                    "normalized_output_path": root / "normalized_outputs" / f"{opaque_id}.json",
                }
            )
    if not batches:
        raise ValueError("development manifest v2 produced no app-server batches")

    private_mapping = {
        "schema_version": APP_SERVER_CORE_ARM_VERSION,
        "manifest_path": str(manifest_file),
        "manifest_sha256": _sha256_file(manifest_file),
        "batch_size": batch_size,
        "thread_mode": thread_mode,
        "batches": [
            {
                "batch_id": batch["batch_id"],
                "episode_id": batch["episode"]["episode_id"],
                "segment_ids": batch["segment_ids"],
                "sidecar_path": str(batch["sidecar_path"]),
                "raw_output_path": str(batch["raw_output_path"]),
                "normalized_output_path": str(batch["normalized_output_path"]),
            }
            for batch in batches
        ],
        "privacy": "private_analysis_only_contains_segment_and_episode_ids",
    }
    write_text_atomic(mapping_path, json.dumps(private_mapping, ensure_ascii=True, indent=2, sort_keys=True) + "\n")

    async def execute(batch: dict[str, Any], *, thread=None) -> dict[str, Any]:
        try:
            if thread is None:
                result = await client.run_ephemeral_structured_turn(
                    model=model,
                    effort=reasoning_effort,
                    base_instructions=batch["base_instructions"],
                    prompt=batch["prompt"],
                    output_schema=batch["schema"],
                    cwd=Path.cwd(),
                    sidecar_path=batch["sidecar_path"],
                    output_path=batch["raw_output_path"],
                    batch_size=len(batch["segments"]),
                    thread_mode=thread_mode,
                    timeout_seconds=timeout_seconds,
                )
            else:
                result = await client.run_structured_turn(
                    thread=thread,
                    effort=reasoning_effort,
                    prompt=batch["prompt"],
                    output_schema=batch["schema"],
                    sidecar_path=batch["sidecar_path"],
                    output_path=batch["raw_output_path"],
                    batch_size=len(batch["segments"]),
                    thread_mode=thread_mode,
                    timeout_seconds=timeout_seconds,
                )
            if not result.status_ok or not isinstance(result.output, dict):
                return {
                    "batch_id": batch["batch_id"],
                    "status_ok": False,
                    "thread_reusable": False,
                    "failure_class": result.error_class or f"turn_{result.status}",
                    "requested_segments": len(batch["segments"]),
                }
            try:
                normalized, diagnostics = normalize_episode_batch_output(
                    result.output,
                    episode_id=batch["episode"]["episode_id"],
                    prepared_segments=batch["segments"],
                    max_events_per_segment=max_events_per_segment,
                )
            except (ValidationError, ValueError) as exc:
                return {
                    "batch_id": batch["batch_id"],
                    "status_ok": False,
                    "transport_status_ok": True,
                    "thread_reusable": True,
                    "failure_class": type(exc).__name__,
                    "requested_segments": len(batch["segments"]),
                }
            write_text_atomic(
                batch["normalized_output_path"],
                json.dumps(normalized, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
            )
            return {
                "batch_id": batch["batch_id"],
                "status_ok": all(item["status_ok"] for item in diagnostics),
                "thread_reusable": True,
                "transport_status_ok": True,
                "failure_class": (
                    None if all(item["status_ok"] for item in diagnostics) else "metric_grounding_error"
                ),
                "requested_segments": len(batch["segments"]),
                "validated_segments": len(diagnostics),
                "diagnostics": diagnostics,
                "output_sha256": _sha256_file(batch["normalized_output_path"]),
            }
        except AppServerError as exc:
            return {
                "batch_id": batch["batch_id"],
                "status_ok": False,
                "thread_reusable": False,
                "failure_class": type(exc).__name__,
                "requested_segments": len(batch["segments"]),
            }

    wall_started = time.monotonic()
    started_at = now_iso()
    results = []
    async with client_factory() as client:
        if thread_mode == "new_thread":
            semaphore = asyncio.Semaphore(concurrency)

            async def bounded(batch: dict[str, Any]) -> dict[str, Any]:
                async with semaphore:
                    return await execute(batch)

            results = await asyncio.gather(*(bounded(batch) for batch in batches))
        else:
            by_episode: dict[str, list[dict[str, Any]]] = {}
            for batch in batches:
                by_episode.setdefault(batch["episode"]["episode_id"], []).append(batch)
            semaphore = asyncio.Semaphore(concurrency)

            async def run_episode(episode_batches: list[dict[str, Any]]) -> list[dict[str, Any]]:
                async with semaphore:
                    first = episode_batches[0]
                    try:
                        thread = await client.start_thread(
                            model=model,
                            base_instructions=first["base_instructions"],
                            cwd=Path.cwd(),
                            ephemeral=True,
                        )
                    except AppServerError as exc:
                        return [
                            {
                                "batch_id": batch["batch_id"],
                                "status_ok": False,
                                "thread_reusable": False,
                                "failure_class": f"thread_start_{type(exc).__name__}",
                                "requested_segments": len(batch["segments"]),
                            }
                            for batch in episode_batches
                        ]
                    episode_results = []
                    failed = False
                    for batch in episode_batches:
                        if failed:
                            episode_results.append(
                                {
                                    "batch_id": batch["batch_id"],
                                    "status_ok": False,
                                    "failure_class": "not_started_after_same_thread_failure",
                                    "requested_segments": len(batch["segments"]),
                                }
                            )
                            continue
                        item = await execute(batch, thread=thread)
                        episode_results.append(item)
                        failed = not item.get("thread_reusable", False)
                    return episode_results

            grouped = await asyncio.gather(*(run_episode(items) for items in by_episode.values()))
            results = [item for group in grouped for item in group]

    batch_by_id = {batch["batch_id"]: batch for batch in batches}
    usage_fields = (
        "input_tokens",
        "cached_input_tokens",
        "output_tokens",
        "reasoning_output_tokens",
        "total_tokens",
    )
    usage = Counter()
    attempted_calls = 0
    usage_measured_attempts = 0
    usage_unknown_attempts = 0
    sidecars = []
    segment_diagnostics = []
    failure_classes = Counter()
    for result in results:
        batch = batch_by_id[result["batch_id"]]
        sidecar = _read_sidecar(batch["sidecar_path"])
        sidecar_usage, usage_complete = _sidecar_usage(batch["sidecar_path"])
        if sidecar is not None:
            sidecars.append(sidecar)
            attempted_calls += 1
            if usage_complete and sidecar_usage is not None:
                usage_measured_attempts += 1
            else:
                usage_unknown_attempts += 1
        if sidecar_usage:
            usage.update({field: int(sidecar_usage.get(field) or 0) for field in usage_fields})
        if result.get("failure_class"):
            failure_classes[result["failure_class"]] += 1
        segment_diagnostics.extend(result.get("diagnostics") or [])

    requested_segments = sum(len(episode["segments"]) for episode in prepared_episodes)
    validated_segments = sum(int(result.get("validated_segments") or 0) for result in results)
    golden_by_segment = {
        segment["segment_id"]: segment
        for episode in prepared_episodes
        for segment in episode["segments"]
    }
    candidate_count_by_segment = {
        item["segment_id"]: item["output_events"] for item in segment_diagnostics
    }
    no_signal_ids = [
        segment_id
        for segment_id, segment in golden_by_segment.items()
        if segment["density_stratum"] == "no_signal"
    ]
    unadjudicated_candidate_positive = sum(
        candidate_count_by_segment.get(segment_id, 0) > 0 for segment_id in no_signal_ids
    )
    raw_candidate_events = sum(item["input_events"] for item in segment_diagnostics)
    exactness_pruned_events = sum(item["exactness_pruned_events"] for item in segment_diagnostics)
    retained_candidate_events = sum(item["output_events"] for item in segment_diagnostics)
    golden_events = sum(int(segment["golden_event_count"]) for segment in golden_by_segment.values())
    golden_segments_above_cap = sum(
        int(segment["golden_event_count"]) > max_events_per_segment
        for segment in golden_by_segment.values()
    )
    elapsed = round(time.monotonic() - wall_started, 3)
    accounting_complete = attempted_calls == len(batches) and usage_unknown_attempts == 0
    metrics_reportable = validated_segments > 0 and accounting_complete
    measured_usage = {field: int(usage[field]) for field in usage_fields}
    usage_status = (
        "complete"
        if accounting_complete
        else "partial_unknown"
        if usage_measured_attempts
        else "unknown"
        if attempted_calls
        else "not_started"
    )
    thread_metadata = {}
    instruction_source_sets = Counter()
    terminal_sidecars = 0
    for sidecar in sidecars:
        thread_id = sidecar.get("thread_id")
        if isinstance(thread_id, str):
            thread_metadata.setdefault(
                thread_id,
                {
                    "base_instructions_bytes": int(sidecar.get("base_instructions_bytes") or 0),
                    "instruction_sources_sha256": sidecar.get("instruction_sources_sha256"),
                    "instruction_sources_count": int(sidecar.get("instruction_sources_count") or 0),
                },
            )
        if sidecar.get("state") in {"completed", "failed", "interrupted", "cancelled"}:
            terminal_sidecars += 1
    for metadata in thread_metadata.values():
        source_sha = metadata.get("instruction_sources_sha256")
        source_count = int(metadata.get("instruction_sources_count") or 0)
        if isinstance(source_sha, str):
            instruction_source_sets[(source_sha, source_count)] += 1
    observed_base_bytes = sum(
        int(metadata["base_instructions_bytes"]) for metadata in thread_metadata.values()
    )
    observed_prompt_bytes = sum(int(sidecar.get("prompt_bytes") or 0) for sidecar in sidecars)
    observed_schema_bytes = sum(int(sidecar.get("output_schema_bytes") or 0) for sidecar in sidecars)
    planned_base_bytes = (
        sum(len(batch["base_instructions"].encode("utf-8")) for batch in batches)
        if thread_mode == "new_thread"
        else sum(len(base.encode("utf-8")) for base in episode_base_by_id.values())
    )
    planned_prompt_bytes = sum(len(batch["prompt"].encode("utf-8")) for batch in batches)
    planned_schema_bytes = sum(
        len(
            json.dumps(
                batch["schema"],
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        for batch in batches
    )
    summary = {
        "schema_version": APP_SERVER_CORE_ARM_VERSION,
        "evaluation_role": "retrospective_development_not_acceptance",
        "started_at": started_at,
        "finished_at": now_iso(),
        "manifest_sha256": _sha256_file(manifest_file),
        "core_instructions_sha256": sha256_text(core_instructions),
        "episode_base_instructions_set_sha256": episode_base_set_sha,
        "guideline_artifact_sha256": guideline_sha,
        "output_schema_version": APP_SERVER_EPISODE_BATCH_SCHEMA_VERSION,
        "transport_client_version": APP_SERVER_CLIENT_VERSION,
        "model": model,
        "reasoning_effort": reasoning_effort,
        "batch_size_ceiling": batch_size,
        "effective_batch_sizes": dict(sorted(Counter(len(batch["segments"]) for batch in batches).items())),
        "thread_mode": thread_mode,
        "cache_interpretation": "observed_cached_input_tokens_only_no_cache_claim",
        "concurrency": concurrency,
        "retry_count": 0,
        "window_count": window_count,
        "context_chars": context_chars,
        "max_events_per_segment": max_events_per_segment,
        "requested_segments": requested_segments,
        "validated_segments": validated_segments,
        "schema_status_success_rate": validated_segments / requested_segments if requested_segments else 0,
        "requested_calls": len(batches),
        "attempted_calls": attempted_calls,
        "terminal_sidecars": terminal_sidecars,
        "transport_completed_calls": sum(bool(result.get("transport_status_ok")) for result in results),
        "validator_clean_calls": sum(bool(result.get("status_ok")) for result in results),
        "failed_or_not_started_calls": sum(not bool(result.get("status_ok")) for result in results),
        "failure_classes": dict(sorted(failure_classes.items())),
        "usage_status": usage_status,
        "usage": measured_usage if accounting_complete else None,
        "measured_partial_usage": measured_usage if usage_measured_attempts else None,
        "usage_measured_attempts": usage_measured_attempts,
        "usage_unknown_attempts": usage_unknown_attempts,
        "accounting_complete": accounting_complete,
        "wall_elapsed_seconds": elapsed,
        "segments_per_second": (
            requested_segments / elapsed if metrics_reportable and elapsed else None
        ),
        "raw_input_tokens_per_segment": (
            usage["input_tokens"] / requested_segments
            if metrics_reportable and requested_segments
            else None
        ),
        "cached_input_tokens_per_segment": (
            usage["cached_input_tokens"] / requested_segments
            if metrics_reportable and requested_segments
            else None
        ),
        "total_tokens_per_segment": (
            usage["total_tokens"] / requested_segments
            if metrics_reportable and requested_segments
            else None
        ),
        "request_overhead_bytes": {
            "immutable_core_instructions_per_thread": len(core_instructions.encode("utf-8")),
            "episode_bootstrap_context_bytes": [item[2] for item in episode_base_fingerprints],
            "planned_thread_start_base_instructions_total": planned_base_bytes,
            "observed_thread_start_base_instructions_total": observed_base_bytes,
            "planned_turn_segment_prompts_total": planned_prompt_bytes,
            "observed_turn_segment_prompts_total": observed_prompt_bytes,
            "planned_turn_output_schemas_total": planned_schema_bytes,
            "observed_turn_output_schemas_total": observed_schema_bytes,
            "planned_fixed_instructions_and_schemas_total": planned_base_bytes + planned_schema_bytes,
            "observed_fixed_instructions_and_schemas_total": observed_base_bytes + observed_schema_bytes,
        },
        "observed_thread_count": len(thread_metadata),
        "instruction_source_sets": [
            {
                "instruction_sources_sha256": source_sha,
                "instruction_sources_count": source_count,
                "thread_count": count,
            }
            for (source_sha, source_count), count in sorted(instruction_source_sets.items())
        ],
        "golden_events": golden_events,
        "golden_segments_above_cap": golden_segments_above_cap,
        "candidate_events": retained_candidate_events,
        "raw_candidate_events": raw_candidate_events,
        "exactness_pruned_events": exactness_pruned_events,
        "raw_exact_evidence_rate": (
            (raw_candidate_events - exactness_pruned_events) / raw_candidate_events
            if metrics_reportable and raw_candidate_events
            else None
        ),
        "normalized_exact_evidence_rate": 1.0 if metrics_reportable and retained_candidate_events else None,
        "event_cap_hits": sum(bool(item["event_cap_hit"]) for item in segment_diagnostics),
        "metric_grounding_error_events": sum(
            item["metric_grounding_error_events"] for item in segment_diagnostics
        ),
        "no_signal_golden_segments": len(no_signal_ids),
        "no_signal_candidate_positive_segments_unadjudicated": unadjudicated_candidate_positive,
        "no_signal_false_positive_rate": None,
        "semantic_quality_status": "pending_calibrated_llm_support_and_alignment_adjudication",
        "selection_eligible": False,
        "privacy": "sanitized_aggregates_hashes_counts_and_failure_classes_only",
    }
    write_text_atomic(report_path, json.dumps(summary, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
    return summary


async def run_app_server_development_matrix(
    conn,
    *,
    manifest_path: str | Path,
    output_dir: str | Path,
    batch_sizes: tuple[int, ...] = (3, 5, 8),
    thread_modes: tuple[str, ...] = ("new_thread", "same_thread"),
    **arm_kwargs: Any,
) -> dict[str, Any]:
    root = Path(output_dir).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    matrix_report_path = root / "matrix-report.json"
    if matrix_report_path.exists():
        raise ValueError("development matrix report already exists; use a new output directory")
    arms = []
    for batch_size in batch_sizes:
        for thread_mode in thread_modes:
            arm_dir = root / f"batch-{batch_size}" / thread_mode
            arm = await run_app_server_core_arm(
                conn,
                manifest_path=manifest_path,
                output_dir=arm_dir,
                batch_size=batch_size,
                thread_mode=thread_mode,
                **arm_kwargs,
            )
            arms.append(arm)
    matrix = {
        "schema_version": APP_SERVER_CORE_MATRIX_VERSION,
        "evaluation_role": "retrospective_development_not_acceptance",
        "created_at": now_iso(),
        "manifest_sha256": _sha256_file(manifest_path),
        "arm_count": len(arms),
        "batch_sizes": list(batch_sizes),
        "thread_modes": list(thread_modes),
        "arms": arms,
        "semantic_quality_status": "pending_calibrated_llm_support_and_alignment_adjudication",
        "winner": None,
        "selection_eligible": False,
        "privacy": "sanitized_aggregates_hashes_counts_and_failure_classes_only",
    }
    write_text_atomic(
        matrix_report_path,
        json.dumps(matrix, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
    )
    return matrix


def aggregate_app_server_development_matrix(
    *,
    arm_report_paths: list[str | Path],
    output_path: str | Path,
    required_batch_sizes: tuple[int, ...] = (3, 5, 8),
    required_thread_modes: tuple[str, ...] = ("new_thread", "same_thread"),
) -> dict[str, Any]:
    expected = {
        (batch_size, thread_mode)
        for batch_size in required_batch_sizes
        for thread_mode in required_thread_modes
    }
    arms = []
    seen = set()
    shared_fields = (
        "manifest_sha256",
        "core_instructions_sha256",
        "episode_base_instructions_set_sha256",
        "guideline_artifact_sha256",
        "output_schema_version",
        "transport_client_version",
        "model",
        "reasoning_effort",
        "concurrency",
        "retry_count",
        "window_count",
        "context_chars",
        "max_events_per_segment",
        "requested_segments",
    )
    shared = None
    for path in arm_report_paths:
        payload = json.loads(Path(path).expanduser().resolve().read_text(encoding="utf-8"))
        if payload.get("schema_version") != APP_SERVER_CORE_ARM_VERSION:
            raise ValueError("unsupported app-server core arm report")
        key = (int(payload["batch_size_ceiling"]), str(payload["thread_mode"]))
        if key in seen:
            raise ValueError(f"duplicate development matrix arm: {key}")
        seen.add(key)
        current_shared = {field: payload.get(field) for field in shared_fields}
        if shared is None:
            shared = current_shared
        elif current_shared != shared:
            raise ValueError("development matrix arm provenance or execution policy differs")
        arms.append(payload)
    if seen != expected:
        missing = sorted(expected - seen)
        extra = sorted(seen - expected)
        raise ValueError(f"development matrix arm set mismatch; missing={missing}, extra={extra}")
    arms.sort(key=lambda item: (int(item["batch_size_ceiling"]), str(item["thread_mode"])))
    matrix = {
        "schema_version": APP_SERVER_CORE_MATRIX_VERSION,
        "evaluation_role": "retrospective_development_not_acceptance",
        "created_at": now_iso(),
        "arm_count": len(arms),
        "batch_sizes": list(required_batch_sizes),
        "thread_modes": list(required_thread_modes),
        "shared_execution_policy": shared,
        "arms": arms,
        "semantic_quality_status": "pending_calibrated_llm_support_and_alignment_adjudication",
        "winner": None,
        "selection_eligible": False,
        "privacy": "sanitized_aggregates_hashes_counts_and_failure_classes_only",
    }
    target = Path(output_path).expanduser().resolve()
    if target.exists():
        raise ValueError("development matrix report already exists")
    write_text_atomic(target, json.dumps(matrix, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
    return matrix
