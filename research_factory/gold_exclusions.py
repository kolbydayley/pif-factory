"""Gold claim exclusion ledger and the shared, non-destructive gold loader.

Audited-illegitimate claims are excluded from the gold data set *at load time*.
The sealed / receipted C-phase files on disk are never modified; instead every
consumer loads gold through :func:`load_gold_windows`, which drops excluded
events in memory and records what it dropped on each window.

The ledger is derived from the audit database (``gold_claim_machine_flags``
verdict ``not_legit`` and the operator's ``gold_claim_flags`` verdict
``illegitimate``).  ``unsure`` never excludes.  Entries are keyed by
``(window_id, event_id)`` because event_ids are only unique within a window.
"""
from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any, Iterable, Mapping

EXCLUSION_SCHEMA = "pif_gold_claim_exclusions_v1"
LEDGER_FILENAME = "gold-claim-exclusions.json"

ExclusionKey = tuple[str, str]  # (window_id, event_id)


def _entries_sha(entries: list[dict[str, Any]]) -> str:
    canonical = [
        (e["window_id"], e["event_id"], e["split"], tuple(sorted(e["sources"])))
        for e in entries
    ]
    return hashlib.sha256(
        json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def build_exclusion_ledger(audit: sqlite3.Connection, *, now: str) -> dict[str, Any]:
    """Union machine ``not_legit`` and manual ``illegitimate`` verdicts."""

    audit.row_factory = sqlite3.Row
    merged: dict[ExclusionKey, dict[str, Any]] = {}
    counts = {"machine_not_legit": 0, "manual_illegitimate": 0}

    def _add(row: sqlite3.Row, source: str, reason_col: str) -> None:
        key = (str(row["window_id"]), str(row["event_id"]))
        entry = merged.setdefault(key, {
            "window_id": key[0], "event_id": key[1], "split": str(row["split"]),
            "sources": [], "reason": "",
        })
        entry["sources"].append(source)
        reason = str(row[reason_col] or "")
        if reason and not entry["reason"]:
            entry["reason"] = reason
        counts[source] += 1

    for row in audit.execute(
        "SELECT window_id, event_id, split, reason FROM gold_claim_machine_flags WHERE verdict='not_legit'"
    ):
        _add(row, "machine_not_legit", "reason")
    # the operator's manual table is created by the audit UI; a fresh DB may not have it yet
    try:
        manual = audit.execute(
            "SELECT window_id, event_id, split, note FROM gold_claim_flags WHERE verdict='illegitimate'"
        ).fetchall()
    except sqlite3.OperationalError as exc:
        if "no such table" not in str(exc):
            raise
        manual = []
    for row in manual:
        _add(row, "manual_illegitimate", "note")

    entries = [merged[k] for k in sorted(merged)]
    for e in entries:
        e["sources"] = sorted(e["sources"])
    return {
        "schema_version": EXCLUSION_SCHEMA,
        "generated_at": now,
        "counts": {"total": len(entries), **counts},
        "entries": entries,
        "ledger_sha256": _entries_sha(entries),
    }


def load_exclusions(path: Path, *, required: bool = False) -> frozenset[ExclusionKey]:
    """Read a ledger into a set of (window_id, event_id).  Missing -> empty."""

    if not path.exists():
        if required:
            raise FileNotFoundError(f"exclusion ledger required but missing: {path}")
        return frozenset()
    ledger = json.loads(path.read_text(encoding="utf-8"))
    if ledger.get("schema_version") != EXCLUSION_SCHEMA:
        raise ValueError(f"unexpected exclusion ledger schema: {ledger.get('schema_version')!r}")
    entries = list(ledger.get("entries") or [])
    if _entries_sha(entries) != ledger.get("ledger_sha256"):
        raise ValueError(f"exclusion ledger hash drift: {path}")
    return frozenset((str(e["window_id"]), str(e["event_id"])) for e in entries)


def default_exclusion_path(gold_root: Path) -> Path:
    """``<campaign>/artifacts/gold-claim-exclusions.json`` for ``<campaign>/results/<split>/C``."""

    return gold_root.parents[2] / "artifacts" / LEDGER_FILENAME


def apply_exclusions(window: Mapping[str, Any], exclusions: Iterable[ExclusionKey]) -> dict[str, Any]:
    """Return a deep copy of one window with excluded events removed and recorded."""

    keys = set(exclusions)
    window_id = str(window.get("window_id"))
    out = copy.deepcopy(dict(window))
    events = list(out.get("events") or [])
    kept, dropped = [], []
    for event in events:
        if (window_id, str(event.get("event_id"))) in keys:
            dropped.append(str(event.get("event_id")))
        else:
            kept.append(event)
    out["events"] = kept
    out["authored_event_count"] = len(events)
    out["excluded_event_ids"] = dropped
    return out


def load_gold_windows(
    gold_root: Path, *, exclusions: Iterable[ExclusionKey] = (),
) -> dict[str, dict[str, Any]]:
    """window_id -> window, with audited-illegitimate events filtered out in memory."""

    keys = frozenset(exclusions)
    windows: dict[str, dict[str, Any]] = {}
    for path in sorted(gold_root.glob("*.json")):
        windows[path.stem] = apply_exclusions(json.loads(path.read_text(encoding="utf-8")), keys)
    return windows
