from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any, Iterable

from .db import connect
from .paths import corpus_dir
from .util import write_text_atomic


PROVENANCE_SCHEMA_VERSION = "pif_app_server_eval_provenance_v1"
EXCLUSION_SCHEMA_VERSION = "pif_reconstructed_exclusion_ledger_v1"
REFERENCE_TRANSFORM_SCHEMA_VERSION = "pif_reference_transform_noise_v1"


def sha256_file(path: str | Path) -> str:
    return hashlib.sha256(Path(path).expanduser().resolve().read_bytes()).hexdigest()


def canonical_json_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _write_immutable_json(path: str | Path, payload: dict[str, Any]) -> Path:
    destination = Path(path).expanduser().resolve()
    rendered = json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    if destination.exists():
        if destination.read_text(encoding="utf-8") != rendered:
            raise ValueError(f"immutable artifact already exists with different content: {destination}")
        return destination
    write_text_atomic(destination, rendered)
    return destination


def build_instruction_provenance(
    *,
    run_spec_path: str | Path,
    arm_report_paths: Iterable[str | Path],
    output_path: str | Path,
) -> dict[str, Any]:
    run_spec_file = Path(run_spec_path).expanduser().resolve()
    spec = json.loads(run_spec_file.read_text(encoding="utf-8"))
    contract = spec.get("instruction_contract") or {}
    sources = contract.get("sources") or []
    checked_sources = []
    for source in sources:
        source_path = Path(str(source["path"])).expanduser().resolve()
        observed = sha256_file(source_path)
        observed_size = source_path.stat().st_size
        checked_sources.append(
            {
                "path_sha256": hashlib.sha256(str(source_path).encode("utf-8")).hexdigest(),
                "expected_content_sha256": source.get("content_sha256"),
                "observed_content_sha256": observed,
                "expected_size_bytes": source.get("size_bytes"),
                "observed_size_bytes": observed_size,
                "verified": bool(
                    observed == source.get("content_sha256")
                    and observed_size == int(source.get("size_bytes") or -1)
                ),
            }
        )
    reports = []
    for value in arm_report_paths:
        report_path = Path(value).expanduser().resolve()
        report = json.loads(report_path.read_text(encoding="utf-8"))
        observed_sets = report.get("instruction_source_sets") or []
        path_set_ok = bool(
            observed_sets
            and all(
                item.get("instruction_sources_sha256")
                == contract.get("expected_path_set_sha256")
                for item in observed_sets
            )
        )
        reports.append(
            {
                "report_path": str(report_path),
                "report_sha256": sha256_file(report_path),
                "batch_size": report.get("batch_size_ceiling"),
                "thread_mode": report.get("thread_mode"),
                "observed_instruction_source_sets": observed_sets,
                "path_set_verified": path_set_ok,
            }
        )
    complete = bool(
        checked_sources
        and all(item["verified"] for item in checked_sources)
        and reports
        and all(item["path_set_verified"] for item in reports)
    )
    payload = {
        "schema_version": PROVENANCE_SCHEMA_VERSION,
        "run_spec_path": str(run_spec_file),
        "run_spec_sha256": sha256_file(run_spec_file),
        "expected_instruction_path_set_sha256": contract.get("expected_path_set_sha256"),
        "instruction_sources": checked_sources,
        "arm_reports": reports,
        "complete": complete,
        "privacy": "hashed_instruction_paths_content_hashes_counts_and_arm_report_metadata_only",
    }
    destination = _write_immutable_json(output_path, payload)
    return {**payload, "artifact_path": str(destination), "artifact_sha256": sha256_file(destination)}


def _manifest_candidates(work_root: Path) -> list[Path]:
    selected = []
    for path in work_root.rglob("*.json"):
        name = path.name.lower()
        if "manifest" in name or name in {"sample.json", "sample-manifest.json"}:
            selected.append(path.resolve())
    return sorted(set(selected), key=lambda item: str(item))


def _collect_identifiers(value: Any, collected: dict[str, set[str]]) -> None:
    if isinstance(value, list):
        for item in value:
            _collect_identifiers(item, collected)
        return
    if not isinstance(value, dict):
        return
    singular = {
        "segment_id": "segment_ids",
        "episode_id": "episode_ids",
        "transcript_id": "transcript_ids",
        "text_sha256": "text_hashes",
    }
    plural = {
        "segment_ids": "segment_ids",
        "episode_ids": "episode_ids",
        "transcript_ids": "transcript_ids",
        "text_sha256s": "text_hashes",
        "text_hashes": "text_hashes",
    }
    for key, item in value.items():
        if key in singular and isinstance(item, str) and item:
            collected[singular[key]].add(item)
        elif key in plural and isinstance(item, list):
            collected[plural[key]].update(str(entry) for entry in item if isinstance(entry, str) and entry)
        _collect_identifiers(item, collected)


def _chunks(values: list[str], size: int = 500) -> Iterable[list[str]]:
    for index in range(0, len(values), size):
        yield values[index : index + size]


def _query_rows(conn: sqlite3.Connection, sql_prefix: str, values: set[str]) -> list[sqlite3.Row]:
    rows: list[sqlite3.Row] = []
    ordered = sorted(values)
    for chunk in _chunks(ordered):
        placeholders = ",".join("?" for _ in chunk)
        rows.extend(conn.execute(f"{sql_prefix} ({placeholders})", chunk).fetchall())
    return rows


def build_reconstructed_exclusion_ledger(
    conn: sqlite3.Connection,
    *,
    work_root: str | Path,
    output_path: str | Path,
    verify_files: bool = True,
) -> dict[str, Any]:
    root = Path(work_root).expanduser().resolve()
    explicit = {
        "segment_ids": set(),
        "episode_ids": set(),
        "transcript_ids": set(),
        "text_hashes": set(),
    }
    artifacts = []
    unreadable = []
    for path in _manifest_candidates(root):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            unreadable.append(
                {
                    "path_sha256": hashlib.sha256(str(path).encode("utf-8")).hexdigest(),
                    "failure_class": "manifest_unreadable",
                }
            )
            continue
        _collect_identifiers(payload, explicit)
        artifacts.append(
            {
                "path": str(path),
                "sha256": sha256_file(path),
            }
        )

    segment_rows = _query_rows(
        conn,
        "SELECT id, episode_id, transcript_id, text_sha256, text_path FROM segments WHERE id IN",
        explicit["segment_ids"],
    )
    episode_ids = set(explicit["episode_ids"])
    transcript_ids = set(explicit["transcript_ids"])
    for row in segment_rows:
        episode_ids.add(str(row["episode_id"]))
        transcript_ids.add(str(row["transcript_id"]))

    episode_segment_rows = _query_rows(
        conn,
        "SELECT id, episode_id, transcript_id, text_sha256, text_path FROM segments WHERE episode_id IN",
        episode_ids,
    )
    transcript_segment_rows = _query_rows(
        conn,
        "SELECT id, episode_id, transcript_id, text_sha256, text_path FROM segments WHERE transcript_id IN",
        transcript_ids,
    )
    all_rows_by_id = {
        str(row["id"]): row
        for row in [*segment_rows, *episode_segment_rows, *transcript_segment_rows]
    }
    missing_segment_ids = sorted(
        explicit["segment_ids"] - {str(row["id"]) for row in segment_rows}
    )
    missing_episode_ids = sorted(
        episode_ids
        - {
            str(row["episode_id"])
            for row in episode_segment_rows
        }
    )
    missing_transcript_ids = sorted(
        transcript_ids
        - {
            str(row["transcript_id"])
            for row in transcript_segment_rows
        }
    )
    text_hashes = set(explicit["text_hashes"])
    file_drift = []
    for row in all_rows_by_id.values():
        stored_hash = str(row["text_sha256"])
        text_hashes.add(stored_hash)
        if not verify_files:
            continue
        path = corpus_dir().parent / str(row["text_path"])
        if not path.exists():
            file_drift.append({"segment_id": row["id"], "failure_class": "text_file_missing"})
            continue
        observed_hash = sha256_file(path)
        if observed_hash != stored_hash:
            file_drift.append(
                {
                    "segment_id": row["id"],
                    "failure_class": "stored_file_hash_mismatch",
                    "stored_sha256": stored_hash,
                    "observed_sha256": observed_hash,
                }
            )
    complete = not (
        unreadable
        or missing_segment_ids
        or missing_episode_ids
        or missing_transcript_ids
        or file_drift
    )
    payload = {
        "schema_version": EXCLUSION_SCHEMA_VERSION,
        "work_root": str(root),
        "manifest_artifacts": artifacts,
        "manifest_artifact_count": len(artifacts),
        "unreadable_artifacts": unreadable,
        "episode_ids": sorted(episode_ids),
        "transcript_ids": sorted(transcript_ids),
        "segment_ids": sorted(set(explicit["segment_ids"]) | set(all_rows_by_id)),
        "text_sha256s": sorted(text_hashes),
        "counts": {
            "episodes": len(episode_ids),
            "transcripts": len(transcript_ids),
            "segments": len(set(explicit["segment_ids"]) | set(all_rows_by_id)),
            "unique_text_sha256s": len(text_hashes),
        },
        "missing_segment_ids": missing_segment_ids,
        "missing_episode_ids": missing_episode_ids,
        "missing_transcript_ids": missing_transcript_ids,
        "file_drift": file_drift,
        "complete": complete,
        "privacy": "local_ids_hashes_artifact_paths_and_failure_classes_no_transcript_or_model_output_text",
    }
    if not complete:
        raise ValueError("exclusion provenance is incomplete or has stored/file hash drift")
    destination = _write_immutable_json(output_path, payload)
    return {**payload, "artifact_path": str(destination), "artifact_sha256": sha256_file(destination)}


def _event_hashes(payload: dict[str, Any]) -> list[str]:
    events = payload.get("discourse_events") or []
    if not isinstance(events, list):
        raise ValueError("baseline output discourse_events is not a list")
    return [canonical_json_sha256(event) for event in events]


def build_reference_transform_noise(
    conn: sqlite3.Connection,
    *,
    manifest_path: str | Path,
    output_path: str | Path,
) -> dict[str, Any]:
    manifest_file = Path(manifest_path).expanduser().resolve()
    manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
    segment_specs = [
        item
        for episode in manifest.get("episodes") or []
        for item in episode.get("segments") or []
    ]
    rows = []
    for item in segment_specs:
        row = conn.execute(
            """
            SELECT
              labels.id AS label_id,
              labels.segment_id,
              labels.output_json,
              label_runs.id AS label_run_id,
              label_runs.output_path
            FROM labels
            JOIN label_runs ON label_runs.id = ?
            WHERE labels.id = ?
            """,
            (item.get("label_run_id"), item.get("label_id")),
        ).fetchone()
        if row is None:
            raise ValueError(f"missing baseline label/run pair for {item.get('segment_id')}")
        raw_path = Path(str(row["output_path"])).expanduser().resolve()
        raw_payload = json.loads(raw_path.read_text(encoding="utf-8"))
        db_payload = json.loads(str(row["output_json"]))
        raw_hashes = _event_hashes(raw_payload)
        db_hashes = _event_hashes(db_payload)
        raw_set = set(raw_hashes)
        db_set = set(db_hashes)
        raw_file_sha = sha256_file(raw_path)
        declared_raw_sha = item.get("label_run_output_sha256")
        rows.append(
            {
                "segment_id": str(row["segment_id"]),
                "label_id": str(row["label_id"]),
                "label_run_id": str(row["label_run_id"]),
                "raw_output_file_sha256": raw_file_sha,
                "declared_raw_output_sha256": declared_raw_sha,
                "declared_raw_output_verified": raw_file_sha == declared_raw_sha,
                "raw_canonical_json_sha256": canonical_json_sha256(raw_payload),
                "submitted_db_canonical_json_sha256": canonical_json_sha256(db_payload),
                "whole_output_changed": canonical_json_sha256(raw_payload)
                != canonical_json_sha256(db_payload),
                "raw_event_count": len(raw_hashes),
                "submitted_db_event_count": len(db_hashes),
                "exact_event_hashes_removed_by_submission": sorted(raw_set - db_set),
                "exact_event_hashes_added_by_submission": sorted(db_set - raw_set),
            }
        )
    changed = [row for row in rows if row["whole_output_changed"]]
    count_changed = [
        row for row in rows if row["raw_event_count"] != row["submitted_db_event_count"]
    ]
    removed = sum(len(row["exact_event_hashes_removed_by_submission"]) for row in rows)
    added = sum(len(row["exact_event_hashes_added_by_submission"]) for row in rows)
    payload = {
        "schema_version": REFERENCE_TRANSFORM_SCHEMA_VERSION,
        "manifest_path": str(manifest_file),
        "manifest_sha256": sha256_file(manifest_file),
        "segments": rows,
        "summary": {
            "segment_count": len(rows),
            "changed_segment_count": len(changed),
            "event_count_changed_segment_count": len(count_changed),
            "raw_event_count": sum(row["raw_event_count"] for row in rows),
            "submitted_db_event_count": sum(row["submitted_db_event_count"] for row in rows),
            "net_event_count_removed_by_submission": sum(
                row["raw_event_count"] - row["submitted_db_event_count"] for row in rows
            ),
            "exact_event_hashes_removed_by_submission": removed,
            "exact_event_hashes_added_by_submission": added,
            "all_declared_raw_artifacts_verified": all(
                row["declared_raw_output_verified"] for row in rows
            ),
        },
        "semantic_interpretation": "pending_llm_support_first_then_alignment_judgment",
        "privacy": "local_ids_counts_and_content_hashes_only_no_transcript_or_event_text",
    }
    if not payload["summary"]["all_declared_raw_artifacts_verified"]:
        raise ValueError("one or more declared raw baseline artifacts have drifted")
    destination = _write_immutable_json(output_path, payload)
    return {**payload, "artifact_path": str(destination), "artifact_sha256": sha256_file(destination)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build immutable app-server evaluation provenance artifacts")
    subparsers = parser.add_subparsers(dest="command", required=True)

    exclusions = subparsers.add_parser("exclusions")
    exclusions.add_argument("--work-root", default="work")
    exclusions.add_argument("--output", required=True)

    transforms = subparsers.add_parser("reference-transform")
    transforms.add_argument("--manifest", required=True)
    transforms.add_argument("--output", required=True)

    instructions = subparsers.add_parser("instructions")
    instructions.add_argument("--run-spec", required=True)
    instructions.add_argument("--arm-report", action="append", required=True)
    instructions.add_argument("--output", required=True)

    args = parser.parse_args(argv)
    if args.command == "instructions":
        result = build_instruction_provenance(
            run_spec_path=args.run_spec,
            arm_report_paths=args.arm_report,
            output_path=args.output,
        )
    else:
        conn = connect()
        try:
            if args.command == "exclusions":
                result = build_reconstructed_exclusion_ledger(
                    conn,
                    work_root=args.work_root,
                    output_path=args.output,
                )
            else:
                result = build_reference_transform_noise(
                    conn,
                    manifest_path=args.manifest,
                    output_path=args.output,
                )
        finally:
            conn.close()
    print(json.dumps({"ok": True, "artifact_path": result["artifact_path"], "artifact_sha256": result["artifact_sha256"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
