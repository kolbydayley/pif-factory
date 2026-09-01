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
) -> dict[str, Any]:
    aliases_payload = json.loads(show_alias_path.read_text(encoding="utf-8"))
    aliases = aliases_payload.get("aliases") or {}
    conn = sqlite3.connect(database)
    conn.row_factory = sqlite3.Row
    overlays = [load_browser_acquisition_overlay(path) for path in acquisition_receipts]
    try:
        manifest = build_split_manifest(
            conn,
            project_root=project_root,
            show_aliases=aliases,
            in_domain_entries=overlays,
        )
    finally:
        conn.close()

    artifacts = freeze_per_show_artifacts(manifest)
    blocked = [row for row in manifest["coverage_diagnostics"] if not row["covered"]]
    summary = {
        "schema_version": "pif_signal_desk_rebuild_partial_freeze_v1",
        "manifest_sha256": manifest["manifest_sha256"],
        "covered_current_shows": len(artifacts),
        "blocked_current_shows": len(blocked),
        "ood_shows": 0,
        "windows": manifest["counts"]["windows"],
        "authoring_allowed_per_show": True,
        "complete_benchmark": False,
        "tournament_allowed": False,
        "dev_error_reading_allowed": False,
        "gold_reliability_audit_allowed": False,
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
        for gold_pass in ("A", "B"):
            packets = build_gold_packets(
                manifest,
                project_root=project_root,
                gold_pass=gold_pass,
                splits=("development", "validation", "sealed_holdout"),
                allow_sealed=True,
            )
            by_show: dict[str, list[dict[str, Any]]] = {}
            for packet in packets:
                by_show.setdefault(str(packet["input"]["show_id"]), []).append(packet)
            for show_id, show_packets in by_show.items():
                _write_json(
                    output_root / "private-gold-inputs" / show_id / f"gold-{gold_pass}.json",
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
        "--acquisition-receipt",
        action="append",
        type=Path,
        default=[],
        help="Private browser acquisition receipt to overlay without canonical ingest",
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
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
