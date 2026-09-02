#!/usr/bin/env python3
"""Record an authorized Gold resume clearance and archive its operator stop."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
import sys
from pathlib import Path


PIF_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIF_ROOT))

from research_factory.signal_desk_gold_budget import (  # noqa: E402
    EXPECTED_MODEL,
    EXPECTED_SCOPE,
    clear_operator_requested_gold_kill,
    reconcile_orphaned_started_gold_reservations,
)


_DEV_C_KEY = re.compile(
    r"^dev:C:(?P<window_id>[^:]+):attempt:(?P<attempt_id>\d+):generation:(?P<generation>\d+)$"
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _completed_sidecar_usage(
    *, budget: sqlite3.Connection, dispatch_database: Path, result_root: Path,
) -> dict[str, int]:
    """Bind stale started reservations to completed Dev-C output receipts.

    This is deliberately narrower than a general repair: if a row cannot be
    tied to the same current attempt, accepted output hash, and completed
    sidecar, the resume fails closed and the caller must investigate it.
    """

    dispatch = sqlite3.connect(dispatch_database)
    dispatch.row_factory = sqlite3.Row
    try:
        rows = budget.execute(
            """SELECT id,task_key FROM signal_desk_gold_budget_reservations
               WHERE status='active' AND provider_started=1 AND lane=? AND model=?
               ORDER BY created_at,id""",
            (EXPECTED_SCOPE, EXPECTED_MODEL),
        ).fetchall()
        usage: dict[str, int] = {}
        for row in rows:
            reservation_id, task_key = str(row["id"]), str(row["task_key"])
            match = _DEV_C_KEY.fullmatch(task_key)
            if match is None:
                raise RuntimeError("stale started Gold reservation is outside the bounded Dev-C resume repair")
            window_id = match.group("window_id")
            task = dispatch.execute(
                """SELECT t.current_attempt_id,a.status,a.output_json
                   FROM signal_desk_rebuild_tasks t
                   JOIN signal_desk_rebuild_attempts a ON a.id=t.current_attempt_id
                   WHERE t.task_key=?""",
                (f"dev:C:{window_id}",),
            ).fetchone()
            if task is None or int(task["current_attempt_id"]) != int(match.group("attempt_id")):
                raise RuntimeError("stale Gold reservation does not bind to its current Dev-C attempt")
            if task["status"] != "succeeded" or not task["output_json"]:
                raise RuntimeError("stale Gold reservation lacks a succeeded Dev-C attempt")
            output = json.loads(str(task["output_json"]))
            tokens = output.get("tokens")
            output_sha = str(output.get("output_sha256") or "")
            if (
                output.get("window_id") != window_id
                or output.get("turn_type") != "C"
                or not isinstance(tokens, int)
                or tokens < 0
                or len(output_sha) != 64
            ):
                raise RuntimeError("succeeded Dev-C attempt lacks a valid output accounting record")
            output_path = result_root / "C" / f"{window_id}.json"
            sidecar_path = result_root / "sidecars" / "C" / f"{window_id}.json"
            if not output_path.is_file() or _sha256(output_path) != output_sha:
                raise RuntimeError("succeeded Dev-C output hash does not bind to its result file")
            sidecar = json.loads(sidecar_path.read_text(encoding="utf-8")) if sidecar_path.is_file() else None
            sidecar_tokens = ((sidecar or {}).get("usage") or {}).get("total_tokens")
            if (sidecar or {}).get("state") != "completed" or sidecar_tokens != tokens:
                raise RuntimeError("succeeded Dev-C sidecar does not bind exact provider usage")
            usage[reservation_id] = tokens
        return usage
    finally:
        dispatch.close()


def _write_reconciliation_receipt(*, budget_dir: Path, reconciliation: dict[str, object]) -> Path:
    timestamp = str(reconciliation["reconciled_at"]).replace("-", "").replace(":", "").replace("+00:00", "Z")
    path = budget_dir / "receipts" / f"signal-desk-gold-pre-resume-reconciliation-{timestamp}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    body = {
        "schema_version": "pif_signal_desk_gold_pre_resume_reconciliation_receipt_v1",
        "reconciliation": reconciliation,
    }
    body["receipt_sha256"] = hashlib.sha256(
        json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    with path.open("x", encoding="utf-8") as handle:
        handle.write(json.dumps(body, indent=2, sort_keys=True) + "\n")
    path.chmod(0o600)
    return path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--authorization-source",
        required=True,
        help="Concise current-turn owner-authorization source label; never include transcript or credential text.",
    )
    parser.add_argument(
        "--grant-path",
        type=Path,
        default=PIF_ROOT / "config/signal_desk_gold_authoring_budget_grant.json",
    )
    parser.add_argument(
        "--budget-dir",
        type=Path,
        default=PIF_ROOT / "work/pif-ops/budget",
    )
    parser.add_argument(
        "--budget-database",
        type=Path,
        default=PIF_ROOT / "data/factory.sqlite",
    )
    parser.add_argument(
        "--dev-dispatch-database",
        type=Path,
        default=PIF_ROOT / "work/signal-desk-rebuild/gold-authoring-v2/dispatch-dev.sqlite",
    )
    parser.add_argument(
        "--dev-result-root",
        type=Path,
        default=PIF_ROOT / "work/signal-desk-rebuild/gold-authoring-v2/results/development",
    )
    args = parser.parse_args()
    budget = sqlite3.connect(args.budget_database)
    budget.row_factory = sqlite3.Row
    try:
        exact_usage = _completed_sidecar_usage(
            budget=budget,
            dispatch_database=args.dev_dispatch_database,
            result_root=args.dev_result_root,
        )
        reconciliation = reconcile_orphaned_started_gold_reservations(
            budget,
            actual_tokens_by_reservation=exact_usage,
        )
        reconciliation_path = _write_reconciliation_receipt(
            budget_dir=args.budget_dir,
            reconciliation=reconciliation,
        )
    finally:
        budget.close()
    receipt = clear_operator_requested_gold_kill(
        grant_path=args.grant_path,
        budget_dir=args.budget_dir,
        authorization_source=args.authorization_source,
        pre_resume_reconciliation={
            "receipt_path": str(reconciliation_path),
            "reconciliation_sha256": reconciliation["reconciliation_sha256"],
            "settled_count": reconciliation["settled_count"],
            "settled_reserved_tokens": reconciliation["settled_reserved_tokens"],
        },
    )
    print(json.dumps({"reconciliation": reconciliation, "clearance": receipt}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
