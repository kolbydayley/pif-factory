"""Read-only gold-claim reader and an isolated flag store for manual auditing.

The gold campaign is live: this module never writes to a campaign database or
result file.  It reads the final adjudicated C-phase outputs, reconstructs the
transcript window each claim came from (so a claim's evidence can be shown in
context), and records verdicts in a separate SQLite file under work/pif-ops.

The sealed holdout split is deliberately excluded everywhere here - auditing it
would break the seal the campaign depends on.
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

AUDITABLE_SPLITS = ("development", "validation")
VERDICTS = ("legitimate", "illegitimate", "unsure")


# --- flag store --------------------------------------------------------------

def connect_audit_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS gold_claim_flags (
            event_id TEXT PRIMARY KEY,
            window_id TEXT NOT NULL,
            split TEXT NOT NULL,
            verdict TEXT NOT NULL CHECK (verdict IN ('legitimate','illegitimate','unsure')),
            note TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.commit()
    return conn


def set_flag(
    conn: sqlite3.Connection, *, event_id: str, window_id: str, split: str,
    verdict: str, note: str = "", now: str,
) -> dict[str, Any]:
    """Upsert one verdict.  Re-flagging the same event replaces it."""

    if verdict not in VERDICTS:
        raise ValueError(f"unknown verdict: {verdict!r}")
    if not event_id or not window_id:
        raise ValueError("event_id and window_id are required")
    conn.execute(
        """
        INSERT INTO gold_claim_flags (event_id, window_id, split, verdict, note, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(event_id) DO UPDATE SET
            verdict=excluded.verdict, note=excluded.note,
            window_id=excluded.window_id, split=excluded.split, updated_at=excluded.updated_at
        """,
        (event_id, window_id, split, verdict, note or "", now),
    )
    conn.commit()
    return dict(conn.execute("SELECT * FROM gold_claim_flags WHERE event_id=?", (event_id,)).fetchone())


def clear_flag(conn: sqlite3.Connection, *, event_id: str) -> None:
    conn.execute("DELETE FROM gold_claim_flags WHERE event_id=?", (event_id,))
    conn.commit()


def all_flags(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    return {row["event_id"]: dict(row) for row in conn.execute("SELECT * FROM gold_claim_flags")}


def flag_summary(conn: sqlite3.Connection) -> dict[str, int]:
    rows = conn.execute("SELECT verdict, COUNT(*) c FROM gold_claim_flags GROUP BY verdict").fetchall()
    summary = {v: 0 for v in VERDICTS}
    for row in rows:
        summary[row["verdict"]] = row["c"]
    return summary


def illegitimate_flags(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return [
        dict(row) for row in conn.execute(
            "SELECT * FROM gold_claim_flags WHERE verdict='illegitimate' ORDER BY updated_at DESC"
        )
    ]


# --- claim reader (read-only over the live campaign) -------------------------

@dataclass(frozen=True)
class ClaimContext:
    """A claim with the surrounding transcript window and evidence span."""

    context: str
    evidence_start: int
    evidence_end: int

    def segments(self) -> tuple[str, str, str]:
        """(before, evidence, after) for highlighting; robust to bad offsets."""

        n = len(self.context)
        s, e = self.evidence_start, self.evidence_end
        if not (isinstance(s, int) and isinstance(e, int) and 0 <= s < e <= n):
            return (self.context, "", "")
        return (self.context[:s], self.context[s:e], self.context[e:])


def build_context(window_text: str, evidence_start: Any, evidence_end: Any, *, pad: int = 320) -> ClaimContext:
    """Trim the window around the evidence so a card shows local context.

    Offsets are re-based onto the trimmed slice.  If the offsets do not point
    at valid text, the whole window is returned with an empty evidence span.
    """

    n = len(window_text)
    if not (isinstance(evidence_start, int) and isinstance(evidence_end, int)
            and 0 <= evidence_start < evidence_end <= n):
        return ClaimContext(window_text.strip(), 0, 0)
    start = max(0, evidence_start - pad)
    end = min(n, evidence_end + pad)
    prefix = "" if start == 0 else "…"
    suffix = "" if end == n else "…"
    context = prefix + window_text[start:end] + suffix
    offset = len(prefix) - start
    return ClaimContext(context, evidence_start + offset, evidence_end + offset)


def load_window_shows(manifest_path: Path) -> dict[str, str]:
    """window_id -> human show name, from the frozen manifest."""

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    return {
        str(w["window_id"]): str(w.get("show_name") or w.get("show_id") or "unknown")
        for w in manifest.get("windows") or []
    }


def load_window_texts(
    manifest_path: Path, *, project_root: Path, splits: Iterable[str] = AUDITABLE_SPLITS,
) -> dict[str, str]:
    """window_id -> exact transcript text the model saw, via the runner's builder."""

    from .signal_desk_rebuild_gold import build_gold_packets

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    texts: dict[str, str] = {}
    for split in splits:
        for packet in build_gold_packets(
            manifest, project_root=project_root, gold_pass="A", splits=(split,), allow_sealed=False
        ):
            window = packet["input"]
            texts[str(window["window_id"])] = str(window["window_text"])
    return texts


def _result_dirs(gold_root: Path, split: str, turn: str) -> list[Path]:
    return [gold_root / "sealed-gold-results" / split / turn, gold_root / "results" / split / turn]


def iter_window_outputs(gold_root: Path, split: str, *, turn: str = "C") -> Iterable[dict[str, Any]]:
    seen: set[str] = set()
    for directory in _result_dirs(gold_root, split, turn):
        if not directory.exists():
            continue
        for path in sorted(directory.glob("*.json")):
            if path.stem in seen:
                continue
            seen.add(path.stem)
            try:
                yield json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue


def load_claims(
    gold_root: Path, *, window_texts: Mapping[str, str],
    flags: Mapping[str, Mapping[str, Any]] | None = None,
    window_shows: Mapping[str, str] | None = None,
    splits: Iterable[str] = AUDITABLE_SPLITS,
) -> list[dict[str, Any]]:
    """Every claim across the auditable splits, with context and current flag."""

    flags = flags or {}
    window_shows = window_shows or {}
    claims: list[dict[str, Any]] = []
    for split in splits:
        for window in iter_window_outputs(gold_root, split):
            window_id = str(window.get("window_id"))
            window_text = window_texts.get(window_id, "")
            for event in window.get("events") or []:
                ctx = build_context(window_text, event.get("evidence_start"), event.get("evidence_end"))
                before, evidence, after = ctx.segments()
                event_id = str(event.get("event_id") or f"{window_id}_{len(claims)}")
                flag = flags.get(event_id)
                claims.append({
                    "event_id": event_id,
                    "window_id": window_id,
                    "split": split,
                    "show": window_shows.get(window_id, "unknown"),
                    "disposition": window.get("window_disposition"),
                    "claim_text": event.get("claim_text"),
                    "speech_act": event.get("speech_act"),
                    "stance": event.get("stance"),
                    "issue_label": event.get("issue_label"),
                    "issue_aliases": event.get("issue_aliases") or [],
                    "speaker_id": event.get("speaker_id"),
                    "attribution_type": event.get("attribution_type"),
                    "attribution_confidence": event.get("attribution_confidence"),
                    "publishability_state": event.get("publishability_state"),
                    "evidence_text": event.get("evidence_text"),
                    "context_before": before,
                    "context_evidence": evidence,
                    "context_after": after,
                    "verdict": (flag or {}).get("verdict"),
                    "note": (flag or {}).get("note", ""),
                })
    return claims
