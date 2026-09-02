#!/usr/bin/env python3
"""Conservatively settle one verified orphaned Gold provider reservation."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import sys
from pathlib import Path


PIF_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIF_ROOT))

from research_factory.signal_desk_gold_budget import (  # noqa: E402
    GOLD_LEASE_SECONDS,
    reconcile_orphaned_started_gold_reservations,
)


_LABEL = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")


def _write_receipt(path: Path, body: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    payload = dict(body)
    payload["receipt_sha256"] = hashlib.sha256(
        json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)
    os.chmod(path, 0o600)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reservation-id", required=True)
    parser.add_argument("--task-key", required=True)
    parser.add_argument("--authorization-source", required=True)
    parser.add_argument("--database", type=Path, default=PIF_ROOT / "data/factory.sqlite")
    parser.add_argument(
        "--receipt-dir",
        type=Path,
        default=PIF_ROOT / "work/pif-ops/budget/receipts",
    )
    args = parser.parse_args(argv)
    if not _LABEL.fullmatch(args.authorization_source):
        parser.error("authorization source must be a compact non-secret audit label")
    conn = sqlite3.connect(args.database)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            """SELECT id,task_key FROM signal_desk_gold_budget_reservations
               WHERE id=? AND status='active' AND provider_started=1""",
            (args.reservation_id,),
        ).fetchone()
        if row is None or str(row["task_key"]) != args.task_key:
            raise RuntimeError("exact active started reservation binding was not found")
        reconciliation = reconcile_orphaned_started_gold_reservations(
            conn,
            minimum_age_seconds=GOLD_LEASE_SECONDS,
            task_keys={args.task_key},
        )
    finally:
        conn.close()
    if reconciliation["settled_count"] != 1:
        raise RuntimeError("orphan reconciliation did not settle exactly one reservation")
    body = {
        "schema_version": "pif_signal_desk_gold_orphan_operator_reconciliation_v1",
        "authorization_source": args.authorization_source,
        "reservation_id": args.reservation_id,
        "reconciliation": reconciliation,
    }
    stamp = str(reconciliation["reconciled_at"]).replace("-", "").replace(":", "")
    target = args.receipt_dir / f"signal-desk-gold-orphan-reconciliation-{stamp}.json"
    _write_receipt(target, body)
    print(json.dumps({"receipt": str(target), **body}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
