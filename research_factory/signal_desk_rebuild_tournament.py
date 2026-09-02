"""Hash-bound tournament registry for Signal Desk prompt optimization."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Mapping

from .signal_desk_rebuild_gates import (
    evaluate_powered_show_promotion,
    show_macro_composite_lcb,
)
from .util import dumps_json, now_iso, sha256_text


SCHEMA_VERSION = "pif_signal_desk_rebuild_tournament_v2"
FAMILY_TYPES = ("prompt", "representation")
SPLITS = ("development", "validation", "sealed_holdout")


class TournamentError(RuntimeError):
    pass


def hash_prompt(text: str) -> str:
    if not text.strip():
        raise TournamentError("prompt must not be empty")
    return sha256_text(text)


def representation_hash(config: Mapping[str, Any]) -> str:
    required = {"window_chars", "turn_aligned_overlap", "speaker_map_header"}
    if set(config) != required:
        raise TournamentError("representation config has an unexpected shape")
    if int(config["window_chars"]) <= 0:
        raise TournamentError("window_chars must be positive")
    return sha256_text(dumps_json(dict(config)))


def ensure_tournament_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS signal_desk_rebuild_experiments (
          variant_id TEXT PRIMARY KEY,
          campaign_id TEXT NOT NULL,
          family_id TEXT NOT NULL,
          family_type TEXT NOT NULL CHECK(family_type IN ('prompt','representation')),
          parent_variant_id TEXT REFERENCES signal_desk_rebuild_experiments(variant_id),
          round_number INTEGER NOT NULL,
          hypothesis TEXT NOT NULL,
          changed_dimension TEXT,
          model TEXT NOT NULL,
          provider TEXT NOT NULL,
          prompt_sha256 TEXT NOT NULL,
          representation_sha256 TEXT NOT NULL,
          scorer_version TEXT NOT NULL,
          seed TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS signal_desk_rebuild_scores (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          variant_id TEXT NOT NULL REFERENCES signal_desk_rebuild_experiments(variant_id),
          split TEXT NOT NULL CHECK(split IN ('development','validation','sealed_holdout')),
          scorer_version TEXT NOT NULL,
          metrics_json TEXT NOT NULL,
          metrics_sha256 TEXT NOT NULL,
          created_at TEXT NOT NULL,
          UNIQUE(variant_id, split, scorer_version)
        );
        CREATE TABLE IF NOT EXISTS signal_desk_rebuild_per_show_scores (
          variant_id TEXT NOT NULL REFERENCES signal_desk_rebuild_experiments(variant_id),
          split TEXT NOT NULL CHECK(split IN ('development','validation')),
          scorer_version TEXT NOT NULL,
          show_id TEXT NOT NULL,
          counts_json TEXT NOT NULL,
          metrics_json TEXT NOT NULL,
          row_sha256 TEXT NOT NULL,
          created_at TEXT NOT NULL,
          PRIMARY KEY(variant_id, split, scorer_version, show_id)
        );
        CREATE TABLE IF NOT EXISTS signal_desk_rebuild_promotions (
          campaign_id TEXT NOT NULL,
          candidate_variant_id TEXT NOT NULL REFERENCES signal_desk_rebuild_experiments(variant_id),
          parent_variant_id TEXT REFERENCES signal_desk_rebuild_experiments(variant_id),
          split TEXT NOT NULL,
          scorer_version TEXT NOT NULL,
          decision_json TEXT NOT NULL,
          decision_sha256 TEXT NOT NULL,
          created_at TEXT NOT NULL,
          PRIMARY KEY(campaign_id, candidate_variant_id, split, scorer_version)
        );
        CREATE TABLE IF NOT EXISTS signal_desk_rebuild_seal (
          campaign_id TEXT PRIMARY KEY,
          sealed_holdout_opened_at TEXT NOT NULL,
          winner_variant_id TEXT NOT NULL,
          configuration_sha256 TEXT NOT NULL
        );
        """
    )


def register_variant(
    conn: sqlite3.Connection,
    *,
    variant_id: str,
    campaign_id: str,
    family_id: str,
    family_type: str,
    parent_variant_id: str | None,
    round_number: int,
    hypothesis: str,
    changed_dimension: str | None,
    model: str,
    provider: str,
    prompt: str,
    representation: Mapping[str, Any],
    scorer_version: str,
    seed: str,
) -> None:
    """Register one root or one-change child; fail closed on lineage drift."""

    ensure_tournament_schema(conn)
    if family_type not in FAMILY_TYPES:
        raise TournamentError("unknown experiment family type")
    if not all(value.strip() for value in (variant_id, campaign_id, family_id, hypothesis, model, provider, scorer_version, seed)):
        raise TournamentError("experiment identity fields must not be empty")
    prompt_sha = hash_prompt(prompt)
    rep_sha = representation_hash(representation)
    if parent_variant_id is None:
        if changed_dimension is not None:
            raise TournamentError("root variant cannot declare a changed dimension")
    else:
        parent = conn.execute(
            "SELECT * FROM signal_desk_rebuild_experiments WHERE variant_id = ?",
            (parent_variant_id,),
        ).fetchone()
        if parent is None:
            raise TournamentError("parent variant does not exist")
        if parent["campaign_id"] != campaign_id or parent["family_id"] != family_id or parent["family_type"] != family_type:
            raise TournamentError("child cannot cross campaign or family lineage")
        if int(round_number) < int(parent["round_number"]):
            raise TournamentError("child round precedes parent")
        if not changed_dimension or not changed_dimension.strip():
            raise TournamentError("child must name exactly one changed dimension")
        prompt_changed = prompt_sha != parent["prompt_sha256"]
        representation_changed = rep_sha != parent["representation_sha256"]
        if family_type == "prompt" and (not prompt_changed or representation_changed):
            raise TournamentError("prompt child must change only its prompt")
        if family_type == "representation" and (prompt_changed or not representation_changed):
            raise TournamentError("representation child must change only its input representation")
        if family_type == "representation" and not 3 <= int(round_number) <= 9:
            raise TournamentError("representation family is restricted to rounds 3-9")
    conn.execute(
        """
        INSERT INTO signal_desk_rebuild_experiments
          (variant_id,campaign_id,family_id,family_type,parent_variant_id,
           round_number,hypothesis,changed_dimension,model,provider,
           prompt_sha256,representation_sha256,scorer_version,seed,created_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            variant_id, campaign_id, family_id, family_type, parent_variant_id,
            int(round_number), hypothesis, changed_dimension, model, provider,
            prompt_sha, rep_sha, scorer_version, seed, now_iso(),
        ),
    )


def record_score(
    conn: sqlite3.Connection,
    *,
    variant_id: str,
    split: str,
    scorer_version: str,
    metrics: Mapping[str, Any],
) -> None:
    ensure_tournament_schema(conn)
    if split not in SPLITS:
        raise TournamentError("unknown split")
    row = conn.execute(
        "SELECT scorer_version FROM signal_desk_rebuild_experiments WHERE variant_id = ?",
        (variant_id,),
    ).fetchone()
    if row is None:
        raise TournamentError("variant is not registered")
    if row["scorer_version"] != scorer_version:
        raise TournamentError("scorer change invalidates cross-round comparison")
    metrics_payload = dict(metrics)
    per_show = metrics_payload.pop("per_show", None)
    if split == "sealed_holdout" and per_show is not None:
        raise TournamentError("sealed holdout is aggregate-only and cannot persist per-show rows")
    if per_show is not None and not isinstance(per_show, Mapping):
        raise TournamentError("per_show score data must be a mapping")
    aggregation = metrics_payload.get("aggregation")
    if per_show is not None and isinstance(aggregation, Mapping):
        if aggregation.get("selection_metrics") != "unweighted_show_macro":
            raise TournamentError("per_show scores require show-macro selection metrics")
    metrics_json = dumps_json(metrics_payload)
    conn.execute(
        """
        INSERT INTO signal_desk_rebuild_scores
          (variant_id,split,scorer_version,metrics_json,metrics_sha256,created_at)
        VALUES (?,?,?,?,?,?)
        """,
        (variant_id, split, scorer_version, metrics_json, sha256_text(metrics_json), now_iso()),
    )
    if per_show is None:
        return
    if split not in {"development", "validation"}:
        raise TournamentError("per-show rows are only permitted for development or validation")
    for show_id, payload in sorted(per_show.items()):
        if not isinstance(show_id, str) or not show_id.strip() or not isinstance(payload, Mapping):
            raise TournamentError("per_show rows require stable show ids and mappings")
        counts = payload.get("counts")
        show_metrics = payload.get("metrics")
        if not isinstance(counts, Mapping) or not isinstance(show_metrics, Mapping):
            raise TournamentError("per_show rows require counts and metrics")
        counts_json = dumps_json(dict(counts))
        show_metrics_json = dumps_json(dict(show_metrics))
        row_hash = sha256_text(dumps_json({"counts": dict(counts), "metrics": dict(show_metrics)}))
        conn.execute(
            """INSERT INTO signal_desk_rebuild_per_show_scores
               (variant_id,split,scorer_version,show_id,counts_json,metrics_json,row_sha256,created_at)
               VALUES (?,?,?,?,?,?,?,?)""",
            (
                variant_id,
                split,
                scorer_version,
                show_id,
                counts_json,
                show_metrics_json,
                row_hash,
                now_iso(),
            ),
        )


def _load_per_show_score(
    conn: sqlite3.Connection, *, variant_id: str, split: str, scorer_version: str
) -> dict[str, dict[str, Any]]:
    rows = conn.execute(
        """SELECT show_id,counts_json,metrics_json
           FROM signal_desk_rebuild_per_show_scores
           WHERE variant_id=? AND split=? AND scorer_version=? ORDER BY show_id""",
        (variant_id, split, scorer_version),
    ).fetchall()
    if not rows:
        raise TournamentError("variant has no persisted per-show score rows")
    return {
        str(row["show_id"]): {
            "counts": json.loads(row["counts_json"]),
            "metrics": json.loads(row["metrics_json"]),
        }
        for row in rows
    }


def evaluate_variant_promotion(
    conn: sqlite3.Connection,
    *,
    candidate_variant_id: str,
    parent_variant_id: str,
    split: str = "validation",
    scorer_version: str,
) -> dict[str, Any]:
    """Evaluate a candidate on show-level LCB and 80%-of-powered-show gates."""

    if split not in {"development", "validation"}:
        raise TournamentError("promotion selection is only allowed on development or validation")
    candidate = _load_per_show_score(
        conn,
        variant_id=candidate_variant_id,
        split=split,
        scorer_version=scorer_version,
    )
    parent = _load_per_show_score(
        conn,
        variant_id=parent_variant_id,
        split=split,
        scorer_version=scorer_version,
    )
    candidate_lcb = show_macro_composite_lcb(candidate)
    parent_lcb = show_macro_composite_lcb(parent)
    promotion = evaluate_powered_show_promotion(candidate, parent)
    return {
        "split": split,
        "resampling_unit": "show",
        "candidate_variant_id": candidate_variant_id,
        "parent_variant_id": parent_variant_id,
        "candidate_show_macro_composite": candidate_lcb,
        "parent_show_macro_composite": parent_lcb,
        "powered_show_promotion": promotion,
        "passed": promotion["passed"] and candidate_lcb["lcb"] > parent_lcb["lcb"],
    }


def open_sealed_holdout_once(
    conn: sqlite3.Connection,
    *,
    campaign_id: str,
    winner_variant_id: str,
    configuration: Mapping[str, Any],
) -> None:
    ensure_tournament_schema(conn)
    winner = conn.execute(
        "SELECT * FROM signal_desk_rebuild_experiments WHERE variant_id = ?",
        (winner_variant_id,),
    ).fetchone()
    if winner is None or winner["campaign_id"] != campaign_id:
        raise TournamentError("winner is not registered to this campaign")
    parent_variant_id = winner["parent_variant_id"]
    promotion: Mapping[str, Any] | None = None
    if parent_variant_id is not None:
        promotion = evaluate_variant_promotion(
            conn,
            candidate_variant_id=winner_variant_id,
            parent_variant_id=str(parent_variant_id),
            split="validation",
            scorer_version=str(winner["scorer_version"]),
        )
        if not promotion["passed"]:
            raise TournamentError(
                "winner cannot open the sealed holdout without show-macro LCB and 80%-powered-show promotion"
            )
    try:
        conn.execute(
            "INSERT INTO signal_desk_rebuild_seal VALUES (?,?,?,?)",
            (
                campaign_id,
                now_iso(),
                winner_variant_id,
                sha256_text(dumps_json(dict(configuration))),
            ),
        )
    except sqlite3.IntegrityError as exc:
        raise TournamentError("sealed holdout has already been opened") from exc
    if promotion is not None:
        decision_json = dumps_json(dict(promotion))
        conn.execute(
            """INSERT INTO signal_desk_rebuild_promotions
               (campaign_id,candidate_variant_id,parent_variant_id,split,scorer_version,
                decision_json,decision_sha256,created_at)
               VALUES (?,?,?,?,?,?,?,?)""",
            (
                campaign_id,
                winner_variant_id,
                str(parent_variant_id),
                "validation",
                str(winner["scorer_version"]),
                decision_json,
                sha256_text(decision_json),
                now_iso(),
            ),
        )


def load_prompt(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError as exc:
        raise TournamentError("prompt file is unreadable") from exc
