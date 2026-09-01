#!/usr/bin/env python3
"""Audit and freeze the currently qualifying Signal Desk benchmark shows.

This command performs no network or model calls.  It may freeze independent
per-show selection artifacts while the complete 57 + 10 / 804-window benchmark
is still blocked.  Private Gold A/B packet materialization is opt-in because it
contains transcript text and must remain under the ignored work tree.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any, Sequence


PIF_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIF_ROOT))

from research_factory.signal_desk_rebuild_gold import (  # noqa: E402
    build_gold_packets,
    build_split_manifest,
    freeze_per_show_artifacts,
)
from research_factory.signal_desk_rebuild_acquisition import (  # noqa: E402
    load_browser_acquisition_overlay,
)
from research_factory.util import write_text_atomic  # noqa: E402


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_text_atomic(path, json.dumps(value, indent=2, sort_keys=True) + "\n")


def prepare(
    *,
    database: Path,
    project_root: Path,
    output_root: Path,
    show_alias_path: Path,
    freeze: bool,
    materialize_gold_packets: bool,
    acquisition_receipts: Sequence[Path] = (),
    ood_receipts: Sequence[Path] = (),
    refreeze_current_transcript_bytes: bool = False,
) -> dict[str, Any]:
    aliases_payload = json.loads(show_alias_path.read_text(encoding="utf-8"))
    aliases = aliases_payload.get("aliases") or {}
    conn = sqlite3.connect(database)
    conn.row_factory = sqlite3.Row
    overlays = [load_browser_acquisition_overlay(path) for path in acquisition_receipts]
    ood_entries = [json.loads(path.read_text(encoding="utf-8")) for path in ood_receipts]
    try:
        manifest = build_split_manifest(
            conn,
            project_root=project_root,
            show_aliases=aliases,
            in_domain_entries=overlays,
            ood_entries=ood_entries,
            expected_in_domain_shows=57 if ood_entries else None,
            expected_ood_shows=10 if ood_entries else None,
            refreeze_current_transcript_bytes=refreeze_current_transcript_bytes,
        )
    finally:
        conn.close()

    artifacts = freeze_per_show_artifacts(manifest)
    in_domain_artifacts = [
        row for row in artifacts if row.get("show", {}).get("corpus") == "in_domain"
    ]
    blocked = [row for row in manifest["coverage_diagnostics"] if not row["covered"]]
    stale_refrozen = sorted(
        {
            str(window["show_id"])
            for window in manifest["windows"]
            if bool((window.get("transcript_revision") or {}).get("stale_hash_refrozen"))
        }
    )
    blocker_counts = {
        "qualified": len(in_domain_artifacts),
        "blocked_flattened": sum(
            "all_selected_transcripts_are_flattened" in row.get("blocking_reasons", [])
            for row in blocked
        ),
        "blocked_insufficient_episodes": sum(
            any(
                reason in {
                    "fewer_than_four_dated_qualifying_episodes",
                    "fewer_than_four_qualifying_episodes",
                    "publication_period_bins_not_distinct",
                    "insufficient_distinct_publication_months",
                    "insufficient_publication_span",
                }
                for reason in row.get("blocking_reasons", [])
            )
            for row in blocked
        ),
        "blocked_stale_hash": sum(
            any(str(reason).startswith("stale_transcript_hash:") for reason in row.get("rejection_counts", {}))
            for row in blocked
        ),
        "blocked_no_transcript": sum(
            "no_ready_local_transcripts" in row.get("blocking_reasons", []) for row in blocked
        ),
    }
    summary = {
        "schema_version": "pif_signal_desk_rebuild_partial_freeze_v2",
        "manifest_sha256": manifest["manifest_sha256"],
        "covered_current_shows": len(in_domain_artifacts),
        "blocked_current_shows": len(blocked),
        "ood_shows": len([row for row in manifest["shows"] if row.get("corpus") == "ood"]),
        "windows": manifest["counts"]["windows"],
        "authoring_allowed_per_show": True,
        "complete_benchmark": manifest["counts"]["windows"] == 804,
        "tournament_allowed": False,
        "dev_error_reading_allowed": False,
        "gold_reliability_audit_allowed": False,
        "refreeze_current_transcript_bytes": refreeze_current_transcript_bytes,
        "stale_hash_refrozen_show_ids": stale_refrozen,
        "blocker_counts": blocker_counts,
        "blocked": [
            {
                "show_id": row["show_id"],
                "blocking_reasons": row["blocking_reasons"],
                "qualifying_transcripts": row["qualifying_transcripts"],
            }
            for row in blocked
        ],
    }
    if freeze:
        _write_json(output_root / "partial-manifest.json", manifest)
        for artifact in artifacts:
            show_id = str(artifact["show"]["show_id"])
            _write_json(output_root / "shows" / f"{show_id}.json", artifact)
        _write_json(output_root / "partial-freeze-summary.json", summary)
    if materialize_gold_packets:
        if not freeze:
            raise ValueError("gold packet materialization requires --freeze-qualified")
        for gold_pass in ("A", "B", "C"):
            packets = build_gold_packets(
                manifest,
                project_root=project_root,
                gold_pass=gold_pass,
                splits=("development", "validation", "sealed_holdout"),
                allow_sealed=True,
            )
            by_split_show: dict[tuple[str, str], list[dict[str, Any]]] = {}
            for packet in packets:
                key = (str(packet["input"]["split"]), str(packet["input"]["show_id"]))
                by_split_show.setdefault(key, []).append(packet)
            for (split, show_id), show_packets in by_split_show.items():
                # Validation and holdout are item-level sealed surfaces even
                # when the same show also contributes a development episode.
                destination = (
                    "private-gold-development-inputs"
                    if split == "development"
                    else "private-gold-sealed-inputs"
                )
                _write_json(
                    output_root / destination / split / show_id / f"gold-{gold_pass}.json",
                    show_packets,
                )
        summary["private_gold_packets_materialized"] = True
        _write_json(output_root / "partial-freeze-summary.json", summary)
    return summary


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=PIF_ROOT / "data/factory.sqlite")
    parser.add_argument("--project-root", type=Path, default=PIF_ROOT)
    parser.add_argument(
        "--output-root",
        type=Path,
        default=PIF_ROOT / "work/signal-desk-rebuild/benchmark",
    )
    parser.add_argument(
        "--show-aliases",
        type=Path,
        default=PIF_ROOT / "config/signal_desk_show_aliases.json",
    )
    parser.add_argument("--freeze-qualified", action="store_true")
    parser.add_argument("--materialize-gold-packets", action="store_true")
    parser.add_argument(
        "--refreeze-current-transcript-bytes",
        action="store_true",
        help="Explicitly bind stale transcript rows to current local bytes before gold authoring",
    )
    parser.add_argument(
        "--acquisition-receipt",
        action="append",
        type=Path,
        default=[],
        help="Private browser acquisition receipt to overlay without canonical ingest",
    )
    parser.add_argument(
        "--ood-receipt",
        action="append",
        type=Path,
        default=[],
        help="Private isolated OOD show receipt; exactly ten are required for complete freeze",
    )
    args = parser.parse_args(argv)
    result = prepare(
        database=args.database.resolve(),
        project_root=args.project_root.resolve(),
        output_root=args.output_root.resolve(),
        show_alias_path=args.show_aliases.resolve(),
        freeze=args.freeze_qualified,
        materialize_gold_packets=args.materialize_gold_packets,
        acquisition_receipts=tuple(path.resolve() for path in args.acquisition_receipt),
        ood_receipts=tuple(path.resolve() for path in args.ood_receipt),
        refreeze_current_transcript_bytes=args.refreeze_current_transcript_bytes,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
