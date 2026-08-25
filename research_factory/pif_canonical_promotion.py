"""Promote audited shadow bulk drafts into canonical ``labels`` rows.

The cheap lanes draft into ``work/bulk-drafts/drafts.sqlite`` (shadow-only).
This module is the deliberate write path from that shadow store into
``data/factory.sqlite`` — the first and only cheap-lane writer of the
canonical DB. Design decisions:

- **Distinct pack** ``ai_discourse_bulk_v1``: the exact contract the lanes
  qualified against (claims/entities/topics). Never disguised as
  ``ai_discourse_v3_1`` (a different contract) — downstream consumers opt in
  to the bulk pack explicitly. This also makes rollback exact:
  ``DELETE FROM labels WHERE label_pack='ai_discourse_bulk_v1'`` restores the
  prior corpus byte-for-byte.
- **One label per segment**: multiple lanes may have drafted the same
  segment; the best draft wins (audited-pass first, then lane preference,
  then newest). The labels UNIQUE key alone would admit one per model.
- **Fail-closed re-verification at write time**: each draft must re-validate
  against the pack schema AND every claims[]/topics[] evidence string must be
  an exact contiguous substring of the segment text as read today. Any doubt
  skips the draft with a reason; nothing is repaired silently.
- **Dry-run by default**: mutation requires ``--execute``. Writes cooperate
  with the daily cycle via the shared pipeline lock and abort politely when
  it is busy.
- **Receipts**: every run writes an immutable JSON receipt (counts, skip
  taxonomy, inserted label ids) under ``work/pif-ops/canonical-promotion/``.

Usage:
    python3 -m research_factory.pif_canonical_promotion [--execute]
        [--limit N] [--lanes glm,glm-zai,grok,codex]
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

from .cheap_lane_adapters import LABEL_SCHEMA
from .lane_profiles import LANE_PROFILES
from .orchestrator import pipeline_lock
from .util import now_iso, stable_id

PIF_ROOT = Path.home() / "pif-factory"
CANONICAL_DB = PIF_ROOT / "data" / "factory.sqlite"
SHADOW_DB = PIF_ROOT / "work" / "bulk-drafts" / "drafts.sqlite"
RECEIPT_ROOT = PIF_ROOT / "work" / "pif-ops" / "canonical-promotion"

PACK = "ai_discourse_bulk_v1"
# Preference when several lanes drafted one segment: reference model first,
# then the two qualified GLM routes, then grok (candidate-qualified last).
LANE_PREFERENCE = ["codex", "glm-zai", "glm", "grok"]
# grok's profile has no "model" key (the adapter pins GROK_MODEL itself).
LANE_MODEL = {lane: prof.get("model") for lane, prof in LANE_PROFILES.items()
              if prof.get("model")}
LANE_MODEL.setdefault("grok", "grok-4.6")
BATCH_SIZE = 500


def _schema_ok(value: Any, schema: Dict[str, Any] = LABEL_SCHEMA) -> bool:
    """Minimal structural validation against the qualified contract."""
    def check(sch: Dict[str, Any], val: Any) -> bool:
        t = sch.get("type")
        if t == "object":
            if not isinstance(val, dict):
                return False
            props = sch.get("properties", {})
            if sch.get("additionalProperties") is False and \
                    any(k not in props for k in val):
                return False
            if any(k not in val for k in sch.get("required", [])):
                return False
            return all(check(props[k], v) for k, v in val.items() if k in props)
        if t == "array":
            return isinstance(val, list) and \
                all(check(sch["items"], item) for item in val)
        if t == "string":
            return isinstance(val, str) and \
                ("enum" not in sch or val in sch["enum"])
        if t == "number":
            return isinstance(val, (int, float)) and not isinstance(val, bool)
        if t == "boolean":
            return isinstance(val, bool)
        return True
    return check(schema, value)


def _evidence_grounded(label: Dict[str, Any], segment_text: str) -> bool:
    for claim in label.get("claims", []):
        if claim["evidence"] not in segment_text:
            return False
    for topic in label.get("topics", []):
        if topic["evidence"] not in segment_text:
            return False
    return True


def _audited_pass(audit_json: Optional[str]) -> bool:
    if not audit_json:
        return False
    try:
        return json.loads(audit_json).get("verdict") == "pass"
    except json.JSONDecodeError:
        return False


def _ensure_promoted_column(shadow: sqlite3.Connection) -> None:
    cols = {r[1] for r in shadow.execute("PRAGMA table_info(draft_labels)")}
    if "promoted_at" not in cols:
        shadow.execute("ALTER TABLE draft_labels ADD COLUMN promoted_at TEXT")


def _rank(row: sqlite3.Row) -> tuple:
    return (0 if _audited_pass(row["audit_json"]) else 1,
            LANE_PREFERENCE.index(row["lane"])
            if row["lane"] in LANE_PREFERENCE else 99,
            row["created_at"])


def select_candidates(shadow: sqlite3.Connection, lanes: List[str],
                      limit: Optional[int]) -> List[sqlite3.Row]:
    """Unpromoted drafts — best draft per segment.

    The runner only stores drafts that already passed schema validation
    (validation_json holds drop-counts, not a schema flag), and promote()
    re-validates every label in-loop anyway, so no schema filter here.
    """
    shadow.row_factory = sqlite3.Row
    rows = shadow.execute(
        "SELECT segment_id, lane, label_json, validation_json, audit_json,"
        "       created_at FROM draft_labels"
        " WHERE promoted_at IS NULL"
        f"  AND lane IN ({','.join('?' * len(lanes))})",
        lanes).fetchall()
    best: Dict[str, sqlite3.Row] = {}
    for row in rows:
        cur = best.get(row["segment_id"])
        if cur is None or _rank(row) < _rank(cur):
            best[row["segment_id"]] = row
    ordered = sorted(best.values(), key=lambda r: r["created_at"])
    return ordered[:limit] if limit else ordered


def promote(*, execute: bool, lanes: List[str], limit: Optional[int],
            canonical_db: Path = CANONICAL_DB,
            shadow_db: Path = SHADOW_DB,
            pif_root: Optional[Path] = None,
            receipt_root: Optional[Path] = None) -> Dict[str, Any]:
    root = pif_root or PIF_ROOT
    receipts_dir = receipt_root or (RECEIPT_ROOT if pif_root is None
                                    else root / "promotion-receipts")
    started = now_iso()
    shadow = sqlite3.connect(shadow_db)
    _ensure_promoted_column(shadow)
    shadow.commit()
    candidates = select_candidates(shadow, lanes, limit)

    mode = "rw" if execute else "ro"
    canon = sqlite3.connect(f"file:{canonical_db}?mode={mode}", uri=True)
    canon.row_factory = sqlite3.Row

    already = {r["segment_id"] for r in canon.execute(
        "SELECT segment_id FROM labels WHERE label_pack = ?", (PACK,))}

    inserted: List[str] = []
    skips: Dict[str, int] = {}
    pending: List[tuple] = []
    promoted_marks: List[tuple] = []

    def skip(reason: str) -> None:
        skips[reason] = skips.get(reason, 0) + 1

    def flush() -> None:
        if not pending:
            return
        canon.executemany(
            "INSERT INTO labels (id, segment_id, label_pack,"
            " label_pack_version, model, status, output_json, confidence,"
            " needs_review, worker_id, created_at)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?)", pending)
        canon.commit()
        shadow.executemany(
            "UPDATE draft_labels SET promoted_at = ?"
            " WHERE segment_id = ? AND lane = ?", promoted_marks)
        shadow.commit()
        pending.clear()
        promoted_marks.clear()

    for row in candidates:
        if row["segment_id"] in already:
            skip("segment_already_promoted")
            continue
        seg = canon.execute(
            "SELECT text_path FROM segments WHERE id = ?",
            (row["segment_id"],)).fetchone()
        if seg is None:
            skip("segment_missing")
            continue
        try:
            label = json.loads(row["label_json"])
        except json.JSONDecodeError:
            skip("label_json_unparseable")
            continue
        if not _schema_ok(label):
            skip("schema_invalid")
            continue
        try:
            text = (root / seg["text_path"]).read_text()
        except OSError:
            skip("segment_text_unreadable")
            continue
        if not _evidence_grounded(label, text):
            skip("evidence_not_grounded")
            continue

        label_id = stable_id(PACK, row["segment_id"], prefix="lbl_")
        if execute:
            pending.append((
                label_id, row["segment_id"], PACK, PACK,
                LANE_MODEL.get(row["lane"], row["lane"]), "ready",
                json.dumps(label, sort_keys=True),
                label.get("overall_confidence"),
                1 if label.get("needs_review") else 0,
                f"bulk:{row['lane']}", now_iso()))
            promoted_marks.append((started, row["segment_id"], row["lane"]))
            if len(pending) >= BATCH_SIZE:
                flush()
        inserted.append(label_id)
        already.add(row["segment_id"])

    if execute:
        flush()
    canon.close()
    shadow.close()

    receipt = {
        "started_at": started,
        "finished_at": now_iso(),
        "mode": "execute" if execute else "dry_run",
        "pack": PACK,
        "lanes": lanes,
        "candidates": len(candidates),
        "promoted": len(inserted),
        "skips": skips,
        "rollback": f"DELETE FROM labels WHERE label_pack='{PACK}' AND id IN"
                    " (this receipt's label_ids); the whole pack by"
                    " label_pack restores the pre-promotion corpus exactly.",
        "label_ids": inserted,
    }
    receipts_dir.mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.now().strftime("%Y%m%dT%H%M%S")
    path = receipts_dir / f"promotion-{ts}-{receipt['mode']}.json"
    path.write_text(json.dumps(receipt, indent=1))
    receipt["receipt_path"] = str(path)
    return receipt


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true",
                        help="Mutate the canonical DB (default: dry run).")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--lanes", default=",".join(LANE_PREFERENCE))
    args = parser.parse_args()
    lanes = [l.strip() for l in args.lanes.split(",") if l.strip()]

    with pipeline_lock(wait=False) as acquired:
        if not acquired:
            print(json.dumps({"aborted": "pipeline_lock_busy"}))
            raise SystemExit(75)
        receipt = promote(execute=args.execute, lanes=lanes, limit=args.limit)

    summary = {k: v for k, v in receipt.items() if k != "label_ids"}
    print(json.dumps(summary, indent=1))


if __name__ == "__main__":
    main()
