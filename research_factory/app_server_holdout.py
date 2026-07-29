from __future__ import annotations

"""Prepare an untouched, immutable app-server holdout without model calls.

This module deliberately does only preparation.  It freezes an already-selected
winner, reconstructs every known prior semantic exposure, validates local file
provenance, and selects two deterministic reservoirs.  It does not run either
the winning extractor or a judge.
"""

import argparse
import hashlib
import json
import sqlite3
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence, Tuple, Union

from .paths import db_path, root as factory_root
from .util import sha256_text, write_text_atomic


FROZEN_WINNER_VERSION = "pif_app_server_frozen_winner_v3"
LEGACY_FROZEN_WINNER_VERSION = "pif_app_server_frozen_winner_v2"
HOLDOUT_COVENANT_VERSION = "pif_app_server_holdout_covenant_v1"
HOLDOUT_EXCLUSIONS_VERSION = "pif_app_server_holdout_exclusions_v1"
PAIRED_QUALITY_RESERVOIR_VERSION = "pif_app_server_paired_quality_reservoir_v1"
TERMINAL_NO_SIGNAL_RESERVOIR_VERSION = (
    "pif_app_server_terminal_position_no_signal_candidate_reservoir_v1"
)

_HASH_FIELDS = {"text_sha256", "segment_text_sha256"}
_HASH_LIST_FIELDS = {"text_sha256s", "segment_text_sha256s"}
_EPISODE_FIELDS = {"episode_id"}
_EPISODE_LIST_FIELDS = {"episode_ids"}
_SEGMENT_FIELDS = {"segment_id"}
_SEGMENT_LIST_FIELDS = {"segment_ids"}
_HEX = frozenset("0123456789abcdef")
FROZEN_WINNER_CONFIG_FIELDS = frozenset(
    {
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
    }
)


def _canonical_json_bytes(payload: Any) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _valid_sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in _HEX for character in value)
    )


def _require_sha256(value: Any, *, field: str) -> str:
    if not _valid_sha256(value):
        raise ValueError(f"{field} must be a lowercase SHA-256 value")
    return str(value)


def _read_json_file(path: Union[str, Path], *, purpose: str) -> Tuple[Path, Any]:
    resolved = Path(path).expanduser().resolve()
    if not resolved.is_file():
        raise ValueError(f"{purpose} is missing: {resolved}")
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{purpose} is not valid UTF-8 JSON: {resolved}") from exc
    if not isinstance(payload, (dict, list)):
        raise ValueError(f"{purpose} must contain a JSON object or array: {resolved}")
    return resolved, payload


def verify_holdout_model_call_authorization(
    covenant_path: Union[str, Path],
) -> Tuple[Path, dict[str, Any]]:
    """Read only the covenant and fail before any protected holdout access."""

    resolved, payload = _read_json_file(
        covenant_path, purpose="holdout authorization covenant"
    )
    if not isinstance(payload, dict):
        raise ValueError("holdout authorization covenant must contain an object")
    if payload.get("schema_version") != HOLDOUT_COVENANT_VERSION:
        raise ValueError("unsupported holdout authorization covenant schema")
    if payload.get("holdout_model_calls_authorized") is not True:
        raise ValueError("holdout covenant does not authorize protected access")
    return resolved, payload


def load_frozen_winner(path: Union[str, Path]) -> dict[str, Any]:
    """Load and fail-closed validate the winner that authorizes holdout freezing."""

    # Lazy to preserve the pre-existing holdout/dev-selection import graph.
    from . import app_server_expanded_cap_episode_batch as expanded_cap

    resolved, payload = _read_json_file(path, purpose="frozen winner artifact")
    if not isinstance(payload, dict):
        raise ValueError("frozen winner artifact must contain a JSON object")
    if payload.get("schema_version") != FROZEN_WINNER_VERSION:
        raise ValueError("holdout preparation requires the frozen winner schema")
    if payload.get("selection_status") != "frozen_winner":
        raise ValueError("winner selection_status must be frozen_winner")
    if payload.get("winner_frozen") is not True:
        raise ValueError("winner_frozen must be true")

    gates = payload.get("gates")
    if not isinstance(gates, dict):
        raise ValueError("frozen winner gates are missing")
    if gates.get("quality_noninferior") is not True:
        raise ValueError("frozen winner did not pass the semantic quality gate")
    if gates.get("production_amortized_total_token_ratio_lte_0_28") is not True:
        raise ValueError("frozen winner did not pass the production token gate")

    winner = payload.get("winner")
    if not isinstance(winner, dict):
        raise ValueError("frozen winner record is missing")
    required_winner_fields = (
        "variant_id",
        "winner_system_id",
        "batch_size",
        "thread_mode",
        "model",
        "reasoning_effort",
        "frozen_configuration_sha256",
        "report_sha256",
    )
    for field in required_winner_fields:
        if winner.get(field) in (None, ""):
            raise ValueError(f"frozen winner field is missing: {field}")
    _require_sha256(
        winner["frozen_configuration_sha256"],
        field="winner.frozen_configuration_sha256",
    )
    _require_sha256(winner["report_sha256"], field="winner.report_sha256")
    if (
        isinstance(winner["batch_size"], bool)
        or not isinstance(winner["batch_size"], int)
        or winner["batch_size"] < 1
    ):
        raise ValueError("winner.batch_size must be a positive integer")
    required_v3_fields = {
        "variant_id",
        "winner_system_id",
        "batch_size",
        "thread_mode",
        "model",
        "reasoning_effort",
        "concurrency",
        "retry_count",
        "window_count",
        "context_chars",
        "max_events_per_segment",
        "frozen_configuration",
        "frozen_configuration_sha256",
        "report_sha256",
    }
    if not required_v3_fields.issubset(winner) or "config" in winner:
        raise ValueError("frozen winner v3 requires the exact expanded-cap configuration")
    config = winner.get("frozen_configuration")
    if not isinstance(config, dict):
        raise ValueError("winner.frozen_configuration must be an object")
    try:
        verified_config = expanded_cap.verify_frozen_configuration(config)
    except expanded_cap.ExpandedCapEpisodeBatchError as exc:
        raise ValueError("winner.frozen_configuration does not match the final adapter") from exc
    observed_config_sha256 = sha256_text(
        json.dumps(
            verified_config,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    _require_sha256(
        winner.get("frozen_configuration_sha256"),
        field="winner.frozen_configuration_sha256",
    )
    if observed_config_sha256 != winner["frozen_configuration_sha256"]:
        raise ValueError(
            "winner.frozen_configuration_sha256 does not match winner.frozen_configuration"
        )
    expected_variant = "batch_%d_%s" % (
        int(verified_config["batch_size"]),
        str(verified_config["thread_mode"]),
    )
    scalar_bindings = {
        "variant_id": expected_variant,
        "winner_system_id": verified_config["winner_system_id"],
        "batch_size": verified_config["batch_size"],
        "thread_mode": verified_config["thread_mode"],
        "model": verified_config["model"],
        "reasoning_effort": verified_config["effort"],
        "concurrency": 1,
        "retry_count": verified_config["retry_count"],
        "max_events_per_segment": verified_config["max_events_per_segment"],
    }
    if any(winner.get(field) != value for field, value in scalar_bindings.items()):
        raise ValueError("winner scalar mirrors differ from its frozen configuration")
    if (
        winner["winner_system_id"] != expanded_cap.WINNER_SYSTEM_ID
        or winner["model"] != expanded_cap.MODEL
        or winner["reasoning_effort"] != expanded_cap.EFFORT
        or winner["concurrency"] != 1
        or winner["retry_count"] != 0
        or winner["max_events_per_segment"] != expanded_cap.MAX_EVENTS_PER_SEGMENT
        or isinstance(winner.get("window_count"), bool)
        or not isinstance(winner.get("window_count"), int)
        or winner["window_count"] < 1
        or isinstance(winner.get("context_chars"), bool)
        or not isinstance(winner.get("context_chars"), int)
        or winner["context_chars"] < 0
        or verified_config.get("semantic_postprocessing") is not False
        or verified_config.get("production_mutation_allowed") is not False
    ):
        raise ValueError("winner frozen configuration is unsafe for holdout")

    frozen_hashes = payload.get("frozen_artifact_hashes")
    if not isinstance(frozen_hashes, dict) or not frozen_hashes:
        raise ValueError("frozen winner must bind at least one frozen artifact hash")
    for name, value in sorted(frozen_hashes.items()):
        _require_sha256(value, field=f"frozen_artifact_hashes.{name}")
    if (
        frozen_hashes.get(f"arm_configuration_{winner['variant_id']}")
        != winner["frozen_configuration_sha256"]
        or frozen_hashes.get(f"arm_report_{winner['variant_id']}")
        != winner["report_sha256"]
    ):
        raise ValueError("frozen winner does not close over its selected configuration and report")
    if (
        payload.get("holdout_preparation_authorized") is not True
        or payload.get("holdout_model_calls_authorized") is not False
        or payload.get("production_changed") is not False
        or payload.get("production_mutated") is not False
    ):
        raise ValueError("frozen winner does not authorize prepare-only holdout freezing")

    return {
        "artifact_path": str(resolved),
        "artifact_sha256": _sha256_file(resolved),
        "payload": payload,
    }


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _require_schema(conn: sqlite3.Connection) -> None:
    required = {
        "sources": {"id", "name"},
        "episodes": {"id", "source_id", "published_at"},
        "transcripts": {
            "id",
            "episode_id",
            "raw_text_path",
            "raw_text_sha256",
            "status",
            "fetched_at",
            "created_at",
            "updated_at",
        },
        "transcript_preparations": {
            "id",
            "transcript_id",
            "status",
            "cleaned_text_path",
            "cleaned_text_sha256",
        },
        "segments": {
            "id",
            "transcript_id",
            "episode_id",
            "source_id",
            "segment_index",
            "text_path",
            "text_sha256",
        },
        "labels": {"id", "segment_id"},
        "label_runs": {"id", "segment_id"},
        "episode_context_runs": {"id", "episode_id"},
    }
    for table, columns in required.items():
        present = _table_columns(conn, table)
        missing = sorted(columns - present)
        if missing:
            raise ValueError(f"database schema is missing {table} columns: {', '.join(missing)}")


def _resolve_local_artifact(path_value: str) -> Path:
    candidate = Path(path_value).expanduser()
    return candidate.resolve() if candidate.is_absolute() else (factory_root() / candidate).resolve()


def _verified_file_hash(path_value: Any, expected_sha256: Any, *, purpose: str) -> str:
    expected = _require_sha256(expected_sha256, field=f"{purpose} database hash")
    if not isinstance(path_value, str) or not path_value:
        raise ValueError(f"{purpose} has no artifact path")
    path = _resolve_local_artifact(path_value)
    if not path.is_file():
        raise ValueError(f"{purpose} artifact is missing: {path}")
    observed = _sha256_file(path)
    if observed != expected:
        raise ValueError(f"{purpose} DB/file hash drift: {path}")
    return observed


def _collect_manifest_values(value: Any) -> Tuple[set[str], set[str], set[str]]:
    episode_ids: set[str] = set()
    segment_ids: set[str] = set()
    text_hashes: set[str] = set()
    stack = [value]
    while stack:
        item = stack.pop()
        if isinstance(item, dict):
            for key, child in item.items():
                if key in _EPISODE_FIELDS and isinstance(child, str) and child:
                    episode_ids.add(child)
                elif key in _SEGMENT_FIELDS and isinstance(child, str) and child:
                    segment_ids.add(child)
                elif key in _HASH_FIELDS and isinstance(child, str) and child:
                    text_hashes.add(_require_sha256(child, field=key))
                elif key in _EPISODE_LIST_FIELDS and isinstance(child, list):
                    episode_ids.update(str(part) for part in child if isinstance(part, str) and part)
                elif key in _SEGMENT_LIST_FIELDS and isinstance(child, list):
                    segment_ids.update(str(part) for part in child if isinstance(part, str) and part)
                elif key in _HASH_LIST_FIELDS and isinstance(child, list):
                    for part in child:
                        if isinstance(part, str) and part:
                            text_hashes.add(_require_sha256(part, field=key))
                else:
                    stack.append(child)
        elif isinstance(item, list):
            stack.extend(item)
    return episode_ids, segment_ids, text_hashes


def _manifest_paths(roots: Sequence[Union[str, Path]]) -> list[Path]:
    paths: set[Path] = set()
    for raw_root in roots:
        root = Path(raw_root).expanduser().resolve()
        if root.is_file():
            paths.add(root)
        elif root.is_dir():
            paths.update(path.resolve() for path in root.rglob("*manifest*.json") if path.is_file())
        else:
            raise ValueError(f"exclusion root is missing: {root}")
    return sorted(paths, key=lambda path: path.as_posix())


def _rows_for_ids(
    conn: sqlite3.Connection,
    *,
    table: str,
    id_field: str,
    ids: Iterable[str],
) -> list[sqlite3.Row]:
    values = sorted(set(ids))
    rows: list[sqlite3.Row] = []
    for offset in range(0, len(values), 500):
        batch = values[offset : offset + 500]
        placeholders = ",".join("?" for _ in batch)
        rows.extend(
            conn.execute(
                f"SELECT * FROM {table} WHERE {id_field} IN ({placeholders}) ORDER BY {id_field}",
                tuple(batch),
            ).fetchall()
        )
    return rows


def _segment_rows_for_episodes(
    conn: sqlite3.Connection, episode_ids: Iterable[str]
) -> list[sqlite3.Row]:
    values = sorted(set(episode_ids))
    rows: list[sqlite3.Row] = []
    for offset in range(0, len(values), 500):
        batch = values[offset : offset + 500]
        placeholders = ",".join("?" for _ in batch)
        rows.extend(
            conn.execute(
                f"""
                SELECT id, transcript_id, episode_id, source_id, segment_index,
                       text_path, text_sha256
                FROM segments
                WHERE episode_id IN ({placeholders})
                ORDER BY episode_id, transcript_id, segment_index, id
                """,
                tuple(batch),
            ).fetchall()
        )
    return rows


def reconstruct_prior_exclusions(
    conn: sqlite3.Connection,
    *,
    exclude_roots: Sequence[Union[str, Path]],
) -> dict[str, Any]:
    """Reconstruct episode and exact-text exclusions from artifacts and DB exposure."""

    _require_schema(conn)
    orphan_labels = conn.execute(
        """
        SELECT l.id
        FROM labels l LEFT JOIN segments sg ON sg.id = l.segment_id
        WHERE sg.id IS NULL
        ORDER BY l.id
        LIMIT 5
        """
    ).fetchall()
    if orphan_labels:
        raise ValueError(
            "cannot reconstruct label episode exclusions because labels reference missing segments: "
            + ", ".join(str(row[0]) for row in orphan_labels)
        )
    orphan_label_runs = conn.execute(
        """
        SELECT lr.id
        FROM label_runs lr LEFT JOIN segments sg ON sg.id = lr.segment_id
        WHERE sg.id IS NULL
        ORDER BY lr.id
        LIMIT 5
        """
    ).fetchall()
    if orphan_label_runs:
        raise ValueError(
            "cannot reconstruct label-run episode exclusions because runs reference missing segments: "
            + ", ".join(str(row[0]) for row in orphan_label_runs)
        )
    manifest_episode_ids: set[str] = set()
    manifest_segment_ids: set[str] = set()
    text_hashes: set[str] = set()
    manifest_provenance = []
    for path in _manifest_paths(exclude_roots):
        _, payload = _read_json_file(path, purpose="prior manifest")
        episode_ids, segment_ids, hashes = _collect_manifest_values(payload)
        manifest_episode_ids.update(episode_ids)
        manifest_segment_ids.update(segment_ids)
        text_hashes.update(hashes)
        manifest_provenance.append(
            {
                "artifact_path": str(path),
                "artifact_sha256": _sha256_file(path),
                "episode_id_count": len(episode_ids),
                "segment_id_count": len(segment_ids),
                "exact_text_sha256_count": len(hashes),
            }
        )

    context_episode_ids = {
        str(row[0])
        for row in conn.execute(
            "SELECT DISTINCT episode_id FROM episode_context_runs ORDER BY episode_id"
        ).fetchall()
        if row[0]
    }
    label_episode_ids = {
        str(row[0])
        for row in conn.execute(
            """
            SELECT DISTINCT sg.episode_id
            FROM labels l JOIN segments sg ON sg.id = l.segment_id
            ORDER BY sg.episode_id
            """
        ).fetchall()
        if row[0]
    }
    label_run_episode_ids = {
        str(row[0])
        for row in conn.execute(
            """
            SELECT DISTINCT sg.episode_id
            FROM label_runs lr JOIN segments sg ON sg.id = lr.segment_id
            ORDER BY sg.episode_id
            """
        ).fetchall()
        if row[0]
    }
    all_episode_ids = set().union(
        manifest_episode_ids,
        context_episode_ids,
        label_episode_ids,
        label_run_episode_ids,
    )

    episode_rows = _rows_for_ids(
        conn, table="episodes", id_field="id", ids=all_episode_ids
    )
    found_episode_ids = {str(row["id"]) for row in episode_rows}
    missing_episodes = sorted(all_episode_ids - found_episode_ids)
    if missing_episodes:
        raise ValueError(
            "prior exposure references episodes missing from DB: " + ", ".join(missing_episodes[:5])
        )

    explicit_segment_rows = _rows_for_ids(
        conn, table="segments", id_field="id", ids=manifest_segment_ids
    )
    found_segment_ids = {str(row["id"]) for row in explicit_segment_rows}
    missing_segments = sorted(manifest_segment_ids - found_segment_ids)
    if missing_segments:
        raise ValueError(
            "prior manifest references segments missing from DB: " + ", ".join(missing_segments[:5])
        )
    all_episode_ids.update(str(row["episode_id"]) for row in explicit_segment_rows)

    # Expanding every exposed episode to every local segment prevents a later
    # transcript variant or unlisted chunk from leaking into the holdout.
    episode_segment_rows = _segment_rows_for_episodes(conn, all_episode_ids)
    all_segment_rows: dict[str, sqlite3.Row] = {
        str(row["id"]): row for row in [*episode_segment_rows, *explicit_segment_rows]
    }
    episode_segment_counts: Counter[str] = Counter(
        str(row["episode_id"]) for row in episode_segment_rows
    )
    empty_exposed_episodes = sorted(
        episode_id for episode_id in all_episode_ids if episode_segment_counts[episode_id] == 0
    )
    if empty_exposed_episodes:
        raise ValueError(
            "cannot reconstruct exact text hashes for exposed episodes with no segments: "
            + ", ".join(empty_exposed_episodes[:5])
        )
    for row in all_segment_rows.values():
        observed = _verified_file_hash(
            row["text_path"],
            row["text_sha256"],
            purpose=f"segment {row['id']}",
        )
        text_hashes.add(observed)

    return {
        "schema_version": HOLDOUT_EXCLUSIONS_VERSION,
        "selection_semantics": "exact_ids_and_exact_file_sha256_only_no_keyword_regex_or_embedding_rules",
        "manifest_artifacts": manifest_provenance,
        "exposure_sources": {
            "prior_manifest_episode_ids": sorted(manifest_episode_ids),
            "prior_manifest_segment_ids": sorted(manifest_segment_ids),
            "episode_context_episode_ids": sorted(context_episode_ids),
            "label_episode_ids": sorted(label_episode_ids),
            "label_run_episode_ids": sorted(label_run_episode_ids),
        },
        "excluded_episode_ids": sorted(all_episode_ids),
        "excluded_segment_ids": sorted(all_segment_rows),
        "excluded_text_sha256s": sorted(text_hashes),
        "counts": {
            "manifest_artifacts": len(manifest_provenance),
            "episodes": len(all_episode_ids),
            "segments": len(all_segment_rows),
            "exact_text_sha256s": len(text_hashes),
        },
        "privacy": "private_analysis_only_ids_hashes_and_local_artifact_provenance_no_transcript_text",
    }


def _parse_timestamp(value: Any, *, field: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} timestamp is missing")
    text = value.strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field} is not a valid ISO timestamp: {text}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso_timestamp(value: Any, *, field: str) -> str:
    return _parse_timestamp(value, field=field).isoformat()


def prospective_epoch_watermarks(conn: sqlite3.Connection) -> dict[str, Any]:
    """Freeze independent acquisition and publication boundaries."""

    acquisition_rows = []
    for row in conn.execute(
        "SELECT id, fetched_at, created_at FROM transcripts ORDER BY id"
    ).fetchall():
        raw_timestamp = row["fetched_at"] or row["created_at"]
        acquisition_rows.append(
            (
                _parse_timestamp(raw_timestamp, field=f"transcript {row['id']} acquisition"),
                str(row["id"]),
            )
        )
    if not acquisition_rows:
        raise ValueError("cannot freeze an acquisition watermark without transcripts")
    acquisition_time = max(item[0] for item in acquisition_rows)
    acquisition_ids = sorted(item[1] for item in acquisition_rows if item[0] == acquisition_time)

    publication_rows = []
    for row in conn.execute(
        "SELECT id, published_at FROM episodes WHERE published_at IS NOT NULL ORDER BY id"
    ).fetchall():
        publication_rows.append(
            (
                _parse_timestamp(row["published_at"], field=f"episode {row['id']} publication"),
                str(row["id"]),
            )
        )
    if not publication_rows:
        raise ValueError("cannot freeze a publication watermark without published episodes")
    publication_time = max(item[0] for item in publication_rows)
    publication_ids = sorted(item[1] for item in publication_rows if item[0] == publication_time)

    return {
        "acquisition": {
            "timestamp": acquisition_time.isoformat(),
            "basis": "transcripts.fetched_at_else_created_at",
            "ids_at_watermark": acquisition_ids,
            "ids_at_watermark_sha256": sha256_text("\n".join(acquisition_ids)),
        },
        "publication": {
            "timestamp": publication_time.isoformat(),
            "basis": "episodes.published_at",
            "ids_at_watermark": publication_ids,
            "ids_at_watermark_sha256": sha256_text("\n".join(publication_ids)),
        },
        "prospective_eligibility": (
            "require transcript acquisition timestamp strictly after the acquisition watermark "
            "and episode publication timestamp strictly after the publication watermark; "
            "timestamps equal to either watermark remain in the frozen epoch"
        ),
    }


def _canonical_transcript_inventory(
    conn: sqlite3.Connection,
    *,
    excluded_episode_ids: set[str],
    excluded_text_hashes: set[str],
) -> list[dict[str, Any]]:
    transcript_rows = conn.execute(
        """
        SELECT t.id AS transcript_id, t.episode_id, t.source_kind,
               t.raw_text_path, t.raw_text_sha256, t.fetched_at,
               t.created_at AS transcript_created_at, t.updated_at AS transcript_updated_at,
               e.source_id, e.published_at, s.name AS source_name
        FROM transcripts t
        JOIN episodes e ON e.id = t.episode_id
        JOIN sources s ON s.id = e.source_id
        WHERE t.status = 'ready'
        ORDER BY t.episode_id, t.id
        """
    ).fetchall()
    prepared_rows = conn.execute(
        """
        SELECT id, transcript_id, cleaned_text_path, cleaned_text_sha256
        FROM transcript_preparations
        WHERE status = 'prepared'
        ORDER BY transcript_id, id
        """
    ).fetchall()
    prepared_by_transcript: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for row in prepared_rows:
        prepared_by_transcript[str(row["transcript_id"])].append(row)

    by_episode: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for transcript in transcript_rows:
        episode_id = str(transcript["episode_id"])
        if episode_id in excluded_episode_ids:
            continue
        preparations = prepared_by_transcript.get(str(transcript["transcript_id"])) or []
        if not preparations:
            continue
        if len(preparations) > 1:
            hashes = {str(row["cleaned_text_sha256"]) for row in preparations}
            if len(hashes) > 1:
                raise ValueError(
                    f"prepared transcript has conflicting artifacts: {transcript['transcript_id']}"
                )
        preparation = preparations[0]
        raw_hash = _verified_file_hash(
            transcript["raw_text_path"],
            transcript["raw_text_sha256"],
            purpose=f"transcript {transcript['transcript_id']}",
        )
        prepared_hash = _verified_file_hash(
            preparation["cleaned_text_path"],
            preparation["cleaned_text_sha256"],
            purpose=f"prepared transcript {transcript['transcript_id']}",
        )
        segment_rows = conn.execute(
            """
            SELECT id, transcript_id, episode_id, source_id, segment_index,
                   text_path, text_sha256
            FROM segments
            WHERE transcript_id = ?
            ORDER BY segment_index, id
            """,
            (transcript["transcript_id"],),
        ).fetchall()
        if not segment_rows:
            continue
        indices = [int(row["segment_index"]) for row in segment_rows]
        if len(indices) != len(set(indices)):
            raise ValueError(f"canonical transcript has duplicate segment indices: {transcript['transcript_id']}")
        observed_hashes = []
        segments = []
        for row in segment_rows:
            if row["episode_id"] != transcript["episode_id"]:
                raise ValueError(f"segment episode drift: {row['id']}")
            if row["source_id"] != transcript["source_id"]:
                raise ValueError(f"segment source drift: {row['id']}")
            observed_hash = _verified_file_hash(
                row["text_path"], row["text_sha256"], purpose=f"segment {row['id']}"
            )
            observed_hashes.append(observed_hash)
            segments.append(
                {
                    "segment_id": str(row["id"]),
                    "segment_index": int(row["segment_index"]),
                    "text_sha256": observed_hash,
                }
            )
        if len(observed_hashes) != len(set(observed_hashes)):
            raise ValueError(f"canonical transcript contains duplicate exact text: {transcript['transcript_id']}")
        # Any exact overlap means the episode is not untouched.  Reject the
        # entire episode rather than selecting an apparently clean fragment.
        if set(observed_hashes) & excluded_text_hashes:
            continue
        acquisition_value = transcript["fetched_at"] or transcript["transcript_created_at"]
        by_episode[episode_id].append(
            {
                "episode_id": episode_id,
                "source_id": str(transcript["source_id"]),
                "source_name": str(transcript["source_name"]),
                "published_at": (
                    _iso_timestamp(
                        transcript["published_at"], field=f"episode {episode_id} publication"
                    )
                    if transcript["published_at"]
                    else None
                ),
                "transcript_id": str(transcript["transcript_id"]),
                "transcript_source_kind": str(transcript["source_kind"] or ""),
                "transcript_acquired_at": _iso_timestamp(
                    acquisition_value,
                    field=f"transcript {transcript['transcript_id']} acquisition",
                ),
                "raw_text_sha256": raw_hash,
                "preparation_id": str(preparation["id"]),
                "prepared_text_sha256": prepared_hash,
                "segments": segments,
            }
        )

    inventory = []
    for episode_id, transcripts in sorted(by_episode.items()):
        # A transcript with more prepared segments gives the later paired run
        # the broadest deterministic choice.  Hashes and IDs break all ties;
        # recency and title text never influence selection.
        transcripts.sort(
            key=lambda item: (
                -len(item["segments"]),
                item["prepared_text_sha256"],
                item["raw_text_sha256"],
                item["transcript_id"],
            )
        )
        inventory.append(transcripts[0])
    return inventory


def _selection_key(seed: str, reservoir: str, item: dict[str, Any]) -> str:
    return sha256_text(
        "|".join(
            (
                seed,
                reservoir,
                str(item["source_id"]),
                str(item["episode_id"]),
                str(item["segment_id"]),
                str(item["text_sha256"]),
            )
        )
    )


def _source_balanced_hash_select(
    candidates: Sequence[dict[str, Any]],
    *,
    count: int,
    max_per_source: int,
    max_per_episode: int,
    seed: str,
    reservoir: str,
) -> list[dict[str, Any]]:
    if count < 1 or max_per_source < 1 or max_per_episode < 1:
        raise ValueError("reservoir count and source/episode caps must be positive")
    by_source: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for candidate in candidates:
        row = dict(candidate)
        row["selection_hash"] = _selection_key(seed, reservoir, row)
        by_source[str(row["source_id"])].append(row)
    for rows in by_source.values():
        rows.sort(key=lambda row: (row["selection_hash"], row["segment_id"]))
    source_order = sorted(
        by_source,
        key=lambda source_id: (sha256_text(f"{seed}|{reservoir}|source|{source_id}"), source_id),
    )
    selected: list[dict[str, Any]] = []
    source_counts: Counter[str] = Counter()
    episode_counts: Counter[str] = Counter()
    seen_text_hashes: set[str] = set()
    while len(selected) < count:
        added = False
        for source_id in source_order:
            if source_counts[source_id] >= max_per_source:
                continue
            rows = by_source[source_id]
            while rows:
                row = rows.pop(0)
                episode_id = str(row["episode_id"])
                if episode_counts[episode_id] >= max_per_episode:
                    continue
                if row["text_sha256"] in seen_text_hashes:
                    continue
                selected.append(row)
                source_counts[source_id] += 1
                episode_counts[episode_id] += 1
                seen_text_hashes.add(str(row["text_sha256"]))
                added = True
                break
            if len(selected) >= count:
                break
        if not added:
            break
    for index, row in enumerate(selected):
        row["selection_rank"] = index
    return selected


def _reservoir_candidates(
    inventory: Sequence[dict[str, Any]],
) -> Tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    paired = []
    terminal = []
    for episode in inventory:
        terminal_index = max(segment["segment_index"] for segment in episode["segments"])
        common = {
            key: episode[key]
            for key in (
                "episode_id",
                "source_id",
                "source_name",
                "published_at",
                "transcript_id",
                "transcript_source_kind",
                "transcript_acquired_at",
                "raw_text_sha256",
                "preparation_id",
                "prepared_text_sha256",
            )
        }
        for segment in episode["segments"]:
            row = {**common, **segment}
            if segment["segment_index"] == terminal_index:
                terminal.append(row)
            else:
                paired.append(row)
    return paired, terminal


def _reservoir_payload(
    *,
    schema_version: str,
    role: str,
    selected: list[dict[str, Any]],
    requested_count: int,
    minimum_sources: int,
    max_per_source: int,
    max_per_episode: int,
    seed: str,
    candidate_semantics: str,
) -> dict[str, Any]:
    return {
        "schema_version": schema_version,
        "evaluation_role": role,
        "seed": seed,
        "selection_policy": (
            "canonical_prepared_transcript_source_balanced_sha256_round_robin_exact_caps_"
            "no_semantic_keyword_regex_embedding_or_model_selection"
        ),
        "candidate_semantics": candidate_semantics,
        "requested_count": requested_count,
        "selected_count": len(selected),
        "source_count": len({row["source_id"] for row in selected}),
        "minimum_sources": minimum_sources,
        "episode_count": len({row["episode_id"] for row in selected}),
        "max_per_source": max_per_source,
        "max_per_episode": max_per_episode,
        "source_counts": dict(sorted(Counter(row["source_id"] for row in selected).items())),
        "episode_counts": dict(sorted(Counter(row["episode_id"] for row in selected).items())),
        "segments": selected,
        "privacy": "private_analysis_only_ids_hashes_metadata_and_local_provenance_no_transcript_text",
    }


def _write_new_directory(output_dir: Path, files: dict[str, bytes]) -> None:
    try:
        output_dir.mkdir(parents=True, exist_ok=False)
    except FileExistsError as exc:
        raise ValueError(f"holdout output directory already exists; refusing overwrite: {output_dir}") from exc
    try:
        for name, content in sorted(files.items()):
            write_text_atomic(output_dir / name, content.decode("utf-8"))
    except BaseException:
        # Preserve the partial immutable directory as evidence.  A caller must
        # investigate it rather than silently rerun over a damaged freeze.
        raise


def prepare_frozen_holdout_reservoirs(
    conn: sqlite3.Connection,
    *,
    frozen_winner_path: Union[str, Path],
    output_dir: Union[str, Path],
    exclude_roots: Sequence[Union[str, Path]],
    paired_quality_count: int = 240,
    terminal_position_count: int = 240,
    paired_minimum_sources: int = 20,
    terminal_minimum_sources: int = 20,
    paired_max_per_source: int = 12,
    paired_max_per_episode: int = 2,
    terminal_max_per_source: int = 12,
    terminal_max_per_episode: int = 1,
    seed: str = "app-server-untouched-holdout-v1",
) -> dict[str, Any]:
    """Freeze the covenant, exact exclusions, and both untouched reservoirs."""

    target = Path(output_dir).expanduser().resolve()
    if target.exists():
        raise ValueError(f"holdout output directory already exists; refusing overwrite: {target}")
    if not seed:
        raise ValueError("holdout seed must be nonempty")
    if not exclude_roots:
        raise ValueError("at least one prior-manifest exclusion root is required")
    if paired_minimum_sources < 1 or terminal_minimum_sources < 1:
        raise ValueError("reservoir minimum source counts must be positive")
    _require_schema(conn)
    winner = load_frozen_winner(frozen_winner_path)
    exclusions = reconstruct_prior_exclusions(conn, exclude_roots=exclude_roots)
    watermarks = prospective_epoch_watermarks(conn)
    inventory = _canonical_transcript_inventory(
        conn,
        excluded_episode_ids=set(exclusions["excluded_episode_ids"]),
        excluded_text_hashes=set(exclusions["excluded_text_sha256s"]),
    )
    paired_candidates, terminal_candidates = _reservoir_candidates(inventory)
    terminal_selected = _source_balanced_hash_select(
        terminal_candidates,
        count=terminal_position_count,
        max_per_source=terminal_max_per_source,
        max_per_episode=terminal_max_per_episode,
        seed=seed,
        reservoir="terminal_position_no_signal_candidates",
    )
    terminal_hashes = {row["text_sha256"] for row in terminal_selected}
    paired_selected = _source_balanced_hash_select(
        [row for row in paired_candidates if row["text_sha256"] not in terminal_hashes],
        count=paired_quality_count,
        max_per_source=paired_max_per_source,
        max_per_episode=paired_max_per_episode,
        seed=seed,
        reservoir="paired_quality",
    )
    if {row["segment_id"] for row in paired_selected} & {
        row["segment_id"] for row in terminal_selected
    }:
        raise ValueError("holdout reservoirs overlap")

    paired_payload = _reservoir_payload(
        schema_version=PAIRED_QUALITY_RESERVOIR_VERSION,
        role="untouched_single_winner_paired_quality_acceptance",
        selected=paired_selected,
        requested_count=paired_quality_count,
        minimum_sources=paired_minimum_sources,
        max_per_source=paired_max_per_source,
        max_per_episode=paired_max_per_episode,
        seed=seed,
        candidate_semantics=(
            "unknown_until_one_frozen_winner_run_and_one_frozen_support_first_alignment_judge; "
            "selection made no claim about transcript meaning"
        ),
    )
    terminal_payload = _reservoir_payload(
        schema_version=TERMINAL_NO_SIGNAL_RESERVOIR_VERSION,
        role="terminal_position_no_signal_candidate_pool_not_golden_no_signal",
        selected=terminal_selected,
        requested_count=terminal_position_count,
        minimum_sources=terminal_minimum_sources,
        max_per_source=terminal_max_per_source,
        max_per_episode=terminal_max_per_episode,
        seed=seed,
        candidate_semantics=(
            "unknown positional candidates only; terminal transcript position is not a semantic "
            "no-signal label and every retained case requires LLM support-first adjudication"
        ),
    )

    exclusion_bytes = _canonical_json_bytes(exclusions)
    paired_bytes = _canonical_json_bytes(paired_payload)
    terminal_bytes = _canonical_json_bytes(terminal_payload)
    winner_payload = winner["payload"]
    paired_source_count = len({row["source_id"] for row in paired_selected})
    terminal_source_count = len({row["source_id"] for row in terminal_selected})
    capacity_checks = {
        "paired_count": {
            "required": paired_quality_count,
            "observed": len(paired_selected),
            "passed": len(paired_selected) == paired_quality_count,
        },
        "paired_sources": {
            "required": paired_minimum_sources,
            "observed": paired_source_count,
            "passed": paired_source_count >= paired_minimum_sources,
        },
        "terminal_count": {
            "required": terminal_position_count,
            "observed": len(terminal_selected),
            "passed": len(terminal_selected) == terminal_position_count,
        },
        "terminal_sources": {
            "required": terminal_minimum_sources,
            "observed": terminal_source_count,
            "passed": terminal_source_count >= terminal_minimum_sources,
        },
    }
    holdout_ready = all(check["passed"] for check in capacity_checks.values())
    covenant = {
        "schema_version": HOLDOUT_COVENANT_VERSION,
        "freeze_status": (
            "immutable_prepare_only_ready"
            if holdout_ready
            else "immutable_blocked_future_only_reservoir_undercapacity"
        ),
        "winner": winner_payload["winner"],
        "winner_gates": winner_payload["gates"],
        "winner_artifact_sha256": winner["artifact_sha256"],
        "winner_frozen_artifact_hashes": winner_payload["frozen_artifact_hashes"],
        "seed": seed,
        "model_calls_performed_during_preparation": 0,
        "semantic_selection_prohibited": True,
        "selection_policy": (
            "exact_prior_exposure_exclusion_then_canonical_prepared_transcript_and_"
            "source_balanced_sha256_selection_only"
        ),
        "reservoir_covenant": (
            "both reservoirs are frozen before any holdout model call; run exactly the frozen "
            "winner and frozen judge configuration once; do not tune, relabel, replace, or top up "
            "from observed holdout results"
        ),
        "holdout_model_calls_authorized": holdout_ready,
        "capacity_checks": capacity_checks,
        "undercapacity_policy": (
            None
            if holdout_ready
            else "do not weaken counts, source diversity, episode caps, exclusions, or watermarks; "
            "wait for a separately versioned future-only epoch whose transcript acquisition and "
            "episode publication are both strictly after these frozen watermarks"
        ),
        "prospective_epoch_watermarks": watermarks,
        "artifacts": {
            "exclusions": {
                "filename": "exclusions.json",
                "schema_version": HOLDOUT_EXCLUSIONS_VERSION,
                "sha256": _sha256_bytes(exclusion_bytes),
            },
            "paired_quality_reservoir": {
                "filename": "paired-quality-reservoir.json",
                "schema_version": PAIRED_QUALITY_RESERVOIR_VERSION,
                "sha256": _sha256_bytes(paired_bytes),
            },
            "terminal_position_no_signal_candidates": {
                "filename": "terminal-position-no-signal-candidates.json",
                "schema_version": TERMINAL_NO_SIGNAL_RESERVOIR_VERSION,
                "sha256": _sha256_bytes(terminal_bytes),
            },
        },
        "inventory": {
            "eligible_canonical_episodes": len(inventory),
            "eligible_sources": len({row["source_id"] for row in inventory}),
            "paired_candidate_segments": len(paired_candidates),
            "terminal_candidate_segments": len(terminal_candidates),
        },
        "privacy": "private_analysis_only_ids_hashes_metadata_and_local_provenance_no_transcript_text",
    }
    covenant_bytes = _canonical_json_bytes(covenant)
    _write_new_directory(
        target,
        {
            "covenant.json": covenant_bytes,
            "exclusions.json": exclusion_bytes,
            "paired-quality-reservoir.json": paired_bytes,
            "terminal-position-no-signal-candidates.json": terminal_bytes,
        },
    )
    return {
        "ok": holdout_ready,
        "selection_status": (
            "ready" if holdout_ready else "blocked_future_only_reservoir_undercapacity"
        ),
        "output_dir": str(target),
        "covenant_path": str(target / "covenant.json"),
        "covenant_sha256": _sha256_bytes(covenant_bytes),
        "paired_quality_segments": len(paired_selected),
        "terminal_position_candidates": len(terminal_selected),
        "capacity_checks": capacity_checks,
        "sources": len(
            {row["source_id"] for row in [*paired_selected, *terminal_selected]}
        ),
        "model_calls_performed": 0,
    }


# Short alias for callers that treat the covenant and reservoirs as one holdout.
prepare_frozen_holdout = prepare_frozen_holdout_reservoirs


def verify_frozen_holdout(
    conn: sqlite3.Connection, *, covenant_path: Union[str, Path]
) -> dict[str, Any]:
    """Verify immutable artifact hashes plus current segment DB/file provenance."""

    covenant_file, covenant = _read_json_file(covenant_path, purpose="holdout covenant")
    if not isinstance(covenant, dict):
        raise ValueError("holdout covenant must contain a JSON object")
    if covenant.get("schema_version") != HOLDOUT_COVENANT_VERSION:
        raise ValueError("unsupported holdout covenant schema")
    root = covenant_file.parent
    verified_artifacts = 0
    selected_segments = []
    for name, record in sorted((covenant.get("artifacts") or {}).items()):
        if not isinstance(record, dict) or not record.get("filename"):
            raise ValueError(f"invalid covenant artifact record: {name}")
        artifact = (root / str(record["filename"])).resolve()
        if artifact.parent != root.resolve() or not artifact.is_file():
            raise ValueError(f"covenant artifact is missing or escapes its directory: {name}")
        if _sha256_file(artifact) != _require_sha256(record.get("sha256"), field=f"{name}.sha256"):
            raise ValueError(f"frozen holdout artifact hash drift: {name}")
        payload = json.loads(artifact.read_text(encoding="utf-8"))
        if name in {"paired_quality_reservoir", "terminal_position_no_signal_candidates"}:
            selected_segments.extend(payload.get("segments") or [])
        verified_artifacts += 1

    rows = _rows_for_ids(
        conn,
        table="segments",
        id_field="id",
        ids=(str(row.get("segment_id")) for row in selected_segments),
    )
    by_id = {str(row["id"]): row for row in rows}
    if len(by_id) != len(selected_segments):
        raise ValueError("one or more frozen holdout segments are missing from DB")
    for frozen in selected_segments:
        segment_id = str(frozen["segment_id"])
        row = by_id[segment_id]
        observed = _verified_file_hash(
            row["text_path"], row["text_sha256"], purpose=f"segment {segment_id}"
        )
        if observed != frozen.get("text_sha256"):
            raise ValueError(f"frozen holdout segment hash drift: {segment_id}")
        if str(row["episode_id"]) != str(frozen.get("episode_id")):
            raise ValueError(f"frozen holdout episode drift: {segment_id}")
        if str(row["transcript_id"]) != str(frozen.get("transcript_id")):
            raise ValueError(f"frozen holdout transcript drift: {segment_id}")
    return {
        "ok": True,
        "covenant_sha256": _sha256_file(covenant_file),
        "verified_artifacts": verified_artifacts,
        "verified_segments": len(selected_segments),
    }


def _read_only_connection(path: Union[str, Path]) -> sqlite3.Connection:
    resolved = Path(path).expanduser().resolve()
    if not resolved.is_file():
        raise ValueError(f"database is missing: {resolved}")
    connection = sqlite3.connect(f"file:{resolved}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only = ON")
    return connection


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prepare-only immutable app-server holdout reservoirs; performs zero model calls."
    )
    parser.add_argument("--database", default=str(db_path()))
    parser.add_argument("--winner", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--exclude-root", action="append", required=True)
    parser.add_argument("--paired-count", type=int, default=240)
    parser.add_argument("--terminal-count", type=int, default=240)
    parser.add_argument("--paired-minimum-sources", type=int, default=20)
    parser.add_argument("--terminal-minimum-sources", type=int, default=20)
    parser.add_argument("--paired-max-per-source", type=int, default=12)
    parser.add_argument("--paired-max-per-episode", type=int, default=2)
    parser.add_argument("--terminal-max-per-source", type=int, default=12)
    parser.add_argument("--terminal-max-per-episode", type=int, default=1)
    parser.add_argument("--seed", default="app-server-untouched-holdout-v1")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    connection: Optional[sqlite3.Connection] = None
    try:
        connection = _read_only_connection(args.database)
        result = prepare_frozen_holdout_reservoirs(
            connection,
            frozen_winner_path=args.winner,
            output_dir=args.output_dir,
            exclude_roots=args.exclude_root,
            paired_quality_count=args.paired_count,
            terminal_position_count=args.terminal_count,
            paired_minimum_sources=args.paired_minimum_sources,
            terminal_minimum_sources=args.terminal_minimum_sources,
            paired_max_per_source=args.paired_max_per_source,
            paired_max_per_episode=args.paired_max_per_episode,
            terminal_max_per_source=args.terminal_max_per_source,
            terminal_max_per_episode=args.terminal_max_per_episode,
            seed=args.seed,
        )
    except (OSError, sqlite3.Error, ValueError) as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=True, sort_keys=True), file=sys.stderr)
        return 2
    finally:
        if connection is not None:
            connection.close()
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0 if result.get("ok") else 3


if __name__ == "__main__":
    raise SystemExit(main())
