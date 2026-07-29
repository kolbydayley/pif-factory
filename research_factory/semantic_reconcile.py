"""Bounded, auditable LLM reconciliation for the production intelligence graph.

The routines in this module deliberately split semantic work into three
separate acts:

1. deterministically prepare a release-bound evidence packet;
2. run one ephemeral, managed-auth Codex app-server turn; and
3. transactionally import a schema-validated decision packet.

No function here uses embeddings, regular expressions, token similarity, or a
legacy deterministic graph as semantic authority.  A packet can be prepared or
reviewed without starting a model, and model output is never imported merely
because a turn completed successfully.
"""

from __future__ import annotations

import asyncio
import calendar
import datetime as dt
import json
import os
import re
import sqlite3
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .codex_app_server import CodexAppServerClient, AppServerTurnResult
from .paths import root
from .util import dumps_json, now_iso, sha256_text, stable_id


PACKET_SCHEMA_VERSION = "pif_semantic_reconciliation_packet_v2"
OUTPUT_SCHEMA_VERSION = "pif_semantic_reconciliation_output_v2"
RECEIPT_SCHEMA_VERSION = "pif_semantic_reconciliation_receipt_v1"
JUDGE_SCHEMA_VERSION = "pif_semantic_judge_v1"
DEFAULT_MODEL = "gpt-5.5"
DEFAULT_EFFORT = "high"
DEFAULT_LIMIT = 25
MAX_LIMIT = 200
MAX_PACKET_BYTES = 8 * 1024 * 1024
TARGETS = frozenset({"identities", "claims", "relations", "networks", "outcomes"})
SCOPES = frozenset({"all", "last_18_months"})


class SemanticReconciliationError(ValueError):
    """A reconciliation packet or decision violated the production contract."""


def prepare_reconciliation_packet(
    conn: sqlite3.Connection,
    *,
    target: str,
    release_id: str | None = None,
    limit: int = DEFAULT_LIMIT,
    output_dir: str | Path | None = None,
    as_of: str | None = None,
    scope: str = "all",
    scope_as_of: str | None = None,
) -> dict[str, Any]:
    """Write one immutable, release-bound semantic evidence packet.

    Candidate selection is deliberately structural.  For example, relation
    packets enumerate pairs only after an accepted LLM subject/variant decision;
    the code does not decide that two claims are semantically similar.
    """

    resolved_target = _target(target)
    bounded_limit = _limit(limit)
    resolved_release = _release_id(conn, release_id)
    generated_at = now_iso()
    resolved_scope, resolved_scope_as_of, scope_start = _scope_window(
        scope,
        scope_as_of or generated_at,
    )
    items = _packet_items(
        conn,
        target=resolved_target,
        release_id=resolved_release,
        limit=bounded_limit,
        as_of=as_of,
        scope_start=scope_start,
        scope_as_of=resolved_scope_as_of,
    )
    body: dict[str, Any] = {
        "schema_version": PACKET_SCHEMA_VERSION,
        "target": resolved_target,
        "corpus_release_id": resolved_release,
        "generated_at": generated_at,
        "as_of": as_of,
        "scope": resolved_scope,
        "scope_as_of": resolved_scope_as_of,
        "scope_start": scope_start,
        "limit": bounded_limit,
        "item_count": len(items),
        "semantic_authority": "managed_llm_only",
        "selection_authority": "deterministic_structure_only",
        "items": items,
    }
    canonical_without_hash = dumps_json(body)
    body["packet_sha256"] = sha256_text(canonical_without_hash)
    canonical = dumps_json(body)
    if len(canonical.encode("utf-8")) > MAX_PACKET_BYTES:
        raise SemanticReconciliationError("reconciliation packet exceeds the 8 MiB bound")

    packet_id = stable_id(
        resolved_release,
        resolved_target,
        body["packet_sha256"],
        prefix="srp_",
    )
    artifact_dir = _artifact_dir(output_dir, resolved_release, resolved_target, packet_id)
    packet_path = artifact_dir / "packet.json"
    _write_immutable_json(packet_path, body)
    receipt = {
        "schema_version": RECEIPT_SCHEMA_VERSION,
        "state": "prepared",
        "packet_id": packet_id,
        "packet_path": str(packet_path),
        "packet_sha256": body["packet_sha256"],
        "target": resolved_target,
        "corpus_release_id": resolved_release,
        "item_count": len(items),
        "generated_at": generated_at,
        "scope": resolved_scope,
        "scope_as_of": resolved_scope_as_of,
        "scope_start": scope_start,
        "model_execution_attempted": False,
        "semantic_inference_performed": False,
    }
    receipt_path = artifact_dir / "prepare-receipt.json"
    _write_immutable_json(receipt_path, receipt)
    return {**receipt, "receipt_path": str(receipt_path)}


def count_reconciliation_candidates(
    conn: sqlite3.Connection,
    *,
    target: str,
    release_id: str | None = None,
    limit: int = DEFAULT_LIMIT,
    scope: str = "all",
    scope_as_of: str | None = None,
) -> int:
    """Return the bounded selector count without writing or starting a model."""

    resolved_target = _target(target)
    bounded_limit = _limit(limit)
    resolved_release = _release_id(conn, release_id)
    _, resolved_scope_as_of, scope_start = _scope_window(
        scope,
        scope_as_of or now_iso(),
    )
    return len(
        _packet_items(
            conn,
            target=resolved_target,
            release_id=resolved_release,
            limit=bounded_limit,
            as_of=None,
            scope_start=scope_start,
            scope_as_of=resolved_scope_as_of,
        )
    )


async def run_managed_reconciliation_packet(
    packet_path: str | Path,
    *,
    output_dir: str | Path | None = None,
    model: str = DEFAULT_MODEL,
    effort: str = DEFAULT_EFFORT,
    timeout_seconds: float = 900.0,
    codex_binary: str | Path | None = None,
    client: CodexAppServerClient | None = None,
) -> dict[str, Any]:
    """Run exactly one ephemeral structured turn for a prepared packet.

    The app-server sandbox is read-only with network disabled.  The returned
    output remains a candidate artifact until :func:`import_reconciliation_output`
    is called explicitly.
    """

    if model != DEFAULT_MODEL:
        raise SemanticReconciliationError("production reconciliation requires GPT-5.5")
    packet_file = Path(packet_path).expanduser().resolve()
    packet = _read_packet(packet_file)
    artifact_dir = (
        Path(output_dir).expanduser().resolve()
        if output_dir is not None
        else packet_file.parent
    )
    artifact_dir.mkdir(parents=True, exist_ok=True)
    output_path = artifact_dir / "candidate-output.json"
    sidecar_path = artifact_dir / "managed-turn.json"
    if output_path.exists() or sidecar_path.exists():
        raise FileExistsError("managed reconciliation artifacts already exist")

    binary = Path(codex_binary).expanduser().resolve() if codex_binary else _pinned_codex_binary()
    command = [str(binary), "app-server", "--stdio", "--strict-config"]
    owns_client = client is None
    active_client = client or CodexAppServerClient(command=command, verify_cli=True)
    prompt = _judge_prompt(packet)
    schema = output_schema(packet["target"])
    try:
        if owns_client:
            await active_client.start()
        result: AppServerTurnResult = await active_client.run_ephemeral_structured_turn(
            model=model,
            effort=effort,
            base_instructions=_base_instructions(packet["target"]),
            prompt=prompt,
            output_schema=schema,
            cwd=root(),
            sidecar_path=sidecar_path,
            output_path=output_path,
            batch_size=packet["item_count"] or 1,
            thread_mode="new_thread",
            timeout_seconds=float(timeout_seconds),
        )
    finally:
        if owns_client:
            await active_client.close()

    candidate = json.loads(output_path.read_text(encoding="utf-8"))
    _validate_output_envelope(candidate, packet)
    receipt = {
        "schema_version": RECEIPT_SCHEMA_VERSION,
        "state": "candidate_completed",
        "packet_path": str(packet_file),
        "packet_sha256": packet["packet_sha256"],
        "output_path": str(output_path),
        "output_sha256": _sha256_file(output_path),
        "sidecar_path": str(sidecar_path),
        "target": packet["target"],
        "corpus_release_id": packet["corpus_release_id"],
        "model": model,
        "effort": effort,
        "thread_id": result.thread_id,
        "turn_id": result.turn_id,
        "turn_status": result.status,
        "status_ok": result.status_ok,
        "usage": _usage_dict(result),
        "completed_at": now_iso(),
        "model_execution_attempted": True,
        "canonical_mutation": False,
    }
    receipt_path = artifact_dir / "candidate-receipt.json"
    _write_immutable_json(receipt_path, receipt)
    return {**receipt, "receipt_path": str(receipt_path)}


def run_managed_reconciliation_packet_sync(*args: Any, **kwargs: Any) -> dict[str, Any]:
    """Synchronous CLI-friendly wrapper around the one-turn async runner."""

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(run_managed_reconciliation_packet(*args, **kwargs))
    raise RuntimeError("sync reconciliation runner cannot be used inside an active event loop")


def import_reconciliation_output(
    conn: sqlite3.Connection,
    *,
    packet_path: str | Path,
    output_path: str | Path,
    accept: bool = False,
    reviewer: str | None = None,
    producer_model: str = DEFAULT_MODEL,
    producer_model_version: str | None = None,
) -> dict[str, Any]:
    """Transactionally import a structured LLM decision packet.

    By default decisions are retained as ``needs_review``.  Accepted canonical
    rows require an explicit caller decision and a non-empty reviewer label.
    The import is idempotent by packet/output hash and never starts a model.
    """

    packet_file = Path(packet_path).expanduser().resolve()
    output_file = Path(output_path).expanduser().resolve()
    packet = _read_packet(packet_file)
    output = _read_json_object(output_file, "output")
    if accept and not str(reviewer or "").strip():
        raise SemanticReconciliationError("accepted import requires a reviewer label")
    resolved_producer_model = str(producer_model or "").strip()
    if not resolved_producer_model:
        raise SemanticReconciliationError("producer_model must be non-empty")
    resolved_producer_version = str(
        producer_model_version or resolved_producer_model
    ).strip()
    if not resolved_producer_version:
        raise SemanticReconciliationError("producer_model_version must be non-empty")
    _validate_output_envelope(output, packet)

    from . import intelligence  # imported late so migrations can be replayed independently

    packet_digest = packet["packet_sha256"]
    output_digest = _sha256_file(output_file)
    run_id = stable_id(
        packet["corpus_release_id"],
        packet["target"],
        packet_digest,
        output_digest,
        "accepted" if accept else "needs_review",
        prefix="pir_sem_",
    )
    existing = conn.execute("SELECT * FROM pipeline_runs WHERE id = ?", (run_id,)).fetchone()
    if existing is not None:
        if existing["status"] != "succeeded":
            raise SemanticReconciliationError("existing semantic import is not succeeded")
        if (
            str(existing["model"] or "") != resolved_producer_model
            or str(existing["model_version"] or "") != resolved_producer_version
        ):
            raise SemanticReconciliationError(
                "existing semantic import producer provenance does not match"
            )
        authority = None
        if accept:
            from . import intelligence

            authority = intelligence.accept_pipeline_run(
                conn,
                run_id,
                stage=str(packet["target"]),
                reviewed_by=str(reviewer),
                rationale="Accepted schema-validated managed semantic reconciliation output.",
            )
            conn.commit()
        return {
            "ok": True,
            "idempotent_replay": True,
            "pipeline_run_id": run_id,
            "target": packet["target"],
            "review_status": "accepted" if accept else "needs_review",
            "output_count": int(existing["output_count"]),
            "run_authority_decision_id": authority["id"] if authority else None,
        }

    review_status = "accepted" if accept else "needs_review"
    conn.execute("SAVEPOINT semantic_reconciliation_import")
    try:
        run = intelligence.create_pipeline_run(
            conn,
            run_id=run_id,
            run_type=f"semantic_reconcile_{packet['target']}",
            run_schema=OUTPUT_SCHEMA_VERSION,
            run_schema_version="1",
            corpus_release_id=packet["corpus_release_id"],
            model=resolved_producer_model,
            model_version=resolved_producer_version,
            prompt_version=JUDGE_SCHEMA_VERSION,
            status="running",
            parameters={
                "packet_sha256": packet_digest,
                "output_sha256": output_digest,
                "review_status": review_status,
                "reviewer": reviewer,
                "producer_model": resolved_producer_model,
                "producer_model_version": resolved_producer_version,
            },
            receipt={
                "packet_path": str(packet_file),
                "output_path": str(output_file),
                "privacy": "local_private_semantic_evidence",
            },
            input_count=int(packet["item_count"]),
            input_sha256=packet_digest,
        )
        stored = _import_decisions(
            conn,
            intelligence=intelligence,
            packet=packet,
            output=output,
            pipeline_run_id=run_id,
            review_status=review_status,
            reviewer=reviewer,
            producer_model=resolved_producer_model,
        )
        intelligence.transition_pipeline_run(
            conn,
            run_id,
            status="succeeded",
            expected_status="running",
            input_count=max(int(packet["item_count"]), len(stored)),
            output_count=len(stored),
            failure_count=0,
            output_sha256=output_digest,
            metrics={
                "stored_count": len(stored),
                "decision_counts": _decision_counts(stored),
                "review_status": review_status,
            },
            receipt={
                "packet_sha256": packet_digest,
                "output_sha256": output_digest,
                "reviewer": reviewer,
                "canonical_mutation": bool(accept),
            },
        )
        authority = None
        if accept:
            authority = intelligence.accept_pipeline_run(
                conn,
                run_id,
                stage=str(packet["target"]),
                reviewed_by=str(reviewer),
                rationale="Accepted schema-validated managed semantic reconciliation output.",
            )
        conn.execute("RELEASE SAVEPOINT semantic_reconciliation_import")
        conn.commit()
    except Exception:
        conn.execute("ROLLBACK TO SAVEPOINT semantic_reconciliation_import")
        conn.execute("RELEASE SAVEPOINT semantic_reconciliation_import")
        conn.rollback()
        raise

    return {
        "ok": True,
        "idempotent_replay": False,
        "pipeline_run_id": run["id"],
        "target": packet["target"],
        "review_status": review_status,
        "output_count": len(stored),
        "stored_ids": [row["id"] for row in stored],
        "canonical_mutation": bool(accept),
        "run_authority_decision_id": authority["id"] if authority else None,
    }


def output_schema(target: str) -> dict[str, Any]:
    """Return the strict structured-output schema for one semantic target."""

    resolved = _target(target)
    decision_schema = {
        "identities": _identity_decisions_schema(),
        "claims": _claim_decisions_schema(),
        "relations": _relation_decisions_schema(),
        "networks": _network_decisions_schema(),
        "outcomes": _outcome_decisions_schema(),
    }[resolved]
    return {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "schema_version",
            "target",
            "corpus_release_id",
            "packet_sha256",
            "decisions",
            "abstentions",
        ],
        "properties": {
            "schema_version": {"type": "string", "const": OUTPUT_SCHEMA_VERSION},
            "target": {"type": "string", "const": resolved},
            "corpus_release_id": {"type": "string"},
            "packet_sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
            "decisions": decision_schema,
            "abstentions": {
                "type": "array",
                "maxItems": MAX_LIMIT,
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["item_id", "reason"],
                    "properties": {
                        "item_id": {"type": "string"},
                        "reason": {"type": "string"},
                    },
                },
            },
        },
    }


def _packet_items(
    conn: sqlite3.Connection,
    *,
    target: str,
    release_id: str,
    limit: int,
    as_of: str | None,
    scope_start: str | None,
    scope_as_of: str,
) -> list[dict[str, Any]]:
    if target == "identities":
        return _identity_items(conn, release_id, limit, scope_start, scope_as_of)
    if target == "claims":
        return _claim_items(conn, release_id, limit, scope_start, scope_as_of)
    if target == "relations":
        return _relation_items(conn, release_id, limit, scope_start, scope_as_of)
    if target == "networks":
        return _network_items(conn, release_id, limit, scope_start, scope_as_of)
    return _outcome_items(
        conn,
        release_id,
        limit,
        as_of or scope_as_of,
        scope_start,
        scope_as_of,
    )


def _identity_items(
    conn: sqlite3.Connection,
    release_id: str,
    limit: int,
    scope_start: str | None,
    scope_as_of: str,
) -> list[dict[str, Any]]:
    """Group exact mention occurrences without making an identity assertion."""

    rows = conn.execute(
        """
        WITH ranked_judgments AS (
          SELECT judgments.*,
                 ROW_NUMBER() OVER (
                   PARTITION BY judgments.identity_lineage_id
                   ORDER BY judgments.revision DESC, judgments.decided_at DESC,
                            judgments.created_at DESC, judgments.id DESC
                 ) AS current_rank
          FROM identity_resolution_judgments AS judgments
          JOIN pipeline_runs AS producing_run
            ON producing_run.id = judgments.pipeline_run_id
           AND producing_run.status = 'succeeded'
           AND producing_run.corpus_release_id = judgments.corpus_release_id
          WHERE judgments.corpus_release_id = ?
        ),
        current_blocking_judgments AS (
          SELECT raw_mention_type, raw_mention_id
          FROM ranked_judgments
          WHERE current_rank = 1
            AND review_status IN ('accepted', 'needs_review')
        ),
        eligible_mentions AS (
          SELECT mentions.*,
                 episodes.title AS episode_title, episodes.published_at,
                 sources.id AS source_id, sources.name AS source_title,
                 sources.homepage_url AS source_homepage_url,
                 events.claim_text, events.evidence_text,
                 events.evidence_start, events.evidence_end
          FROM raw_speaker_mentions AS mentions
          JOIN corpus_release_episodes AS members
            ON members.episode_id = mentions.episode_id
           AND members.corpus_release_id = ?
          JOIN episodes ON episodes.id = mentions.episode_id
          JOIN sources ON sources.id = episodes.source_id
          LEFT JOIN discourse_events AS events
            ON events.id = mentions.discourse_event_id
          WHERE (
                  ? IS NULL
                  OR (
                    julianday(episodes.published_at) >= julianday(?)
                    AND julianday(episodes.published_at) <= julianday(?)
                  )
                )
            AND NOT EXISTS (
              SELECT 1
              FROM raw_speaker_mentions AS siblings
              JOIN current_blocking_judgments AS blocking
                ON blocking.raw_mention_type = 'speaker'
               AND blocking.raw_mention_id = siblings.id
              WHERE siblings.episode_id = mentions.episode_id
                AND siblings.surface_name = mentions.surface_name
                AND siblings.role IS mentions.role
                AND siblings.affiliation_surface IS mentions.affiliation_surface
            )
        ),
        group_keys AS (
          SELECT episode_id, surface_name, role, affiliation_surface,
                 ROW_NUMBER() OVER (
                   ORDER BY surface_name COLLATE BINARY, episode_id,
                            COALESCE(role, '') COLLATE BINARY,
                            COALESCE(affiliation_surface, '') COLLATE BINARY
                 ) AS group_rank
          FROM eligible_mentions
          GROUP BY episode_id, surface_name, role, affiliation_surface
        )
        SELECT eligible.*
        FROM eligible_mentions AS eligible
        JOIN group_keys AS groups
          ON groups.episode_id = eligible.episode_id
         AND groups.surface_name = eligible.surface_name
         AND groups.role IS eligible.role
         AND groups.affiliation_surface IS eligible.affiliation_surface
        WHERE groups.group_rank <= ?
        ORDER BY groups.group_rank, eligible.id
        """,
        (
            release_id,
            release_id,
            scope_start,
            scope_start,
            scope_as_of,
            limit,
        ),
    ).fetchall()

    grouped: dict[tuple[str, str, str | None, str | None], dict[str, Any]] = {}
    for row in rows:
        key = (
            str(row["episode_id"]),
            str(row["surface_name"]),
            row["role"],
            row["affiliation_surface"],
        )
        item = grouped.get(key)
        if item is None:
            item_id = stable_id(
                release_id,
                key[0],
                key[1],
                key[2] if key[2] is not None else "<null-role>",
                key[3] if key[3] is not None else "<null-affiliation>",
                prefix="idgrp_",
            )
            item = {
                "item_id": item_id,
                "raw_mention_type": "speaker",
                "raw_mention_ids": [],
                "surface_name": row["surface_name"],
                "role_surface": row["role"],
                "affiliation_surface": row["affiliation_surface"],
                "grouping_basis": "release_episode_exact_surface_role_affiliation",
                "episode": {
                    "id": row["episode_id"],
                    "title": row["episode_title"],
                    "published_at": row["published_at"],
                    "source_id": row["source_id"],
                    "source_title": row["source_title"],
                    "source_homepage_url": row["source_homepage_url"],
                },
                "representative_evidence": {
                    "raw_mention_id": row["id"],
                    "segment_id": row["segment_id"],
                    "discourse_event_id": row["discourse_event_id"],
                    "claim_text": row["claim_text"],
                    "exact_evidence_text": row["evidence_text"],
                    "evidence_start": row["evidence_start"],
                    "evidence_end": row["evidence_end"],
                    "raw_mention_evidence": _safe_json(row["evidence_json"]),
                },
            }
            grouped[key] = item
        item["raw_mention_ids"].append(str(row["id"]))
    return list(grouped.values())


def _claim_items(
    conn: sqlite3.Connection,
    release_id: str,
    limit: int,
    scope_start: str | None,
    scope_as_of: str,
) -> list[dict[str, Any]]:
    """Select claims only through current accepted raw-mention resolutions."""

    rows = conn.execute(
        """
        WITH ranked_claims AS (
          SELECT claims.*,
                 ROW_NUMBER() OVER (
                   PARTITION BY claims.claim_lineage_id
                   ORDER BY claims.revision DESC, claims.created_at DESC, claims.id DESC
                 ) AS current_rank
          FROM atomic_claims AS claims
          JOIN current_accepted_corpus_releases AS release
            ON release.id = claims.corpus_release_id
          JOIN pipeline_runs AS producing_run
            ON producing_run.id = claims.pipeline_run_id
           AND producing_run.status = 'succeeded'
           AND producing_run.corpus_release_id = release.id
          WHERE claims.corpus_release_id = ?
        ),
        accepted_claims AS (
          SELECT *
          FROM ranked_claims
          WHERE current_rank = 1 AND review_status = 'accepted'
        ),
        claim_identity AS (
          SELECT claims.id AS atomic_claim_id,
                 MIN(resolutions.canonical_person_id) AS canonical_person_id,
                 MIN(mentions.id) AS raw_mention_id,
                 COUNT(DISTINCT resolutions.canonical_person_id) AS person_count
          FROM accepted_claims AS claims
          JOIN raw_speaker_mentions AS mentions
            ON mentions.discourse_event_id = claims.discourse_event_id
           AND mentions.episode_id = claims.episode_id
          JOIN current_accepted_identity_resolutions AS resolutions
            ON resolutions.raw_mention_type = 'speaker'
           AND resolutions.raw_mention_id = mentions.id
           AND resolutions.corpus_release_id = claims.corpus_release_id
          WHERE claims.corpus_release_id = ?
          GROUP BY claims.id
          HAVING COUNT(DISTINCT resolutions.canonical_person_id) = 1
        )
        SELECT claims.id, claims.claim_text, claims.claim_type, claims.raw_speaker,
               identity.canonical_person_id, people.display_name AS canonical_person_display_name,
               identity.raw_mention_id, claims.stance, claims.certainty,
               claims.time_horizon, claims.observed_at, claims.episode_id,
               claims.segment_id, claims.discourse_event_id, claims.evidence_text,
               claims.evidence_start, claims.evidence_end,
               episodes.title AS episode_title, episodes.published_at,
               sources.id AS source_id, sources.name AS source_title
        FROM accepted_claims AS claims
        JOIN claim_identity AS identity ON identity.atomic_claim_id = claims.id
        JOIN current_accepted_people AS people
          ON people.id = identity.canonical_person_id
        JOIN episodes ON episodes.id = claims.episode_id
        JOIN sources ON sources.id = claims.source_id
        WHERE claims.corpus_release_id = ?
          AND NOT EXISTS (
            SELECT 1
            FROM accepted_position_observations AS positions
            WHERE positions.atomic_claim_id = claims.id
              AND positions.corpus_release_id = claims.corpus_release_id
              AND positions.review_status IN ('accepted', 'needs_review')
          )
          AND (
            ? IS NULL
            OR (
              julianday(claims.observed_at) >= julianday(?)
              AND julianday(claims.observed_at) <= julianday(?)
            )
          )
        ORDER BY claims.observed_at, claims.id
        """,
        (
            release_id,
            release_id,
            release_id,
            scope_start,
            scope_start,
            scope_as_of,
        ),
    ).fetchall()
    selected = _balanced_claim_rows(rows, limit)
    return [
        {
            "item_id": row["id"],
            "atomic_claim_id": row["id"],
            "claim_text": row["claim_text"],
            "claim_type": row["claim_type"],
            "raw_speaker": row["raw_speaker"],
            "canonical_person_id": row["canonical_person_id"],
            "canonical_person_display_name": row["canonical_person_display_name"],
            "stance": row["stance"],
            "certainty": row["certainty"],
            "time_horizon": row["time_horizon"],
            "observed_at": row["observed_at"],
            "episode": {
                "id": row["episode_id"],
                "title": row["episode_title"],
                "published_at": row["published_at"],
                "source_id": row["source_id"],
                "source_title": row["source_title"],
            },
            "identity_evidence": {
                "raw_speaker_mention_id": row["raw_mention_id"],
                "resolution_source": "current_accepted_identity_resolutions",
            },
            "evidence": {
                "segment_id": row["segment_id"],
                "discourse_event_id": row["discourse_event_id"],
                "exact_evidence_text": row["evidence_text"],
                "evidence_start": row["evidence_start"],
                "evidence_end": row["evidence_end"],
            },
        }
        for row in selected
    ]


def _balanced_claim_rows(
    rows: Sequence[sqlite3.Row],
    limit: int,
) -> list[sqlite3.Row]:
    """Cover distinct accepted people and shows before balanced repeats.

    The scoring is purely structural.  It does not inspect claim text or make a
    semantic similarity decision.
    """

    remaining = list(rows)
    selected: list[sqlite3.Row] = []
    seen_people: set[str] = set()
    seen_sources: set[str] = set()
    person_counts: dict[str, int] = {}
    source_counts: dict[str, int] = {}
    pair_counts: dict[tuple[str, str], int] = {}

    def add(row: sqlite3.Row) -> None:
        person_id = str(row["canonical_person_id"])
        source_id = str(row["source_id"])
        selected.append(row)
        seen_people.add(person_id)
        seen_sources.add(source_id)
        person_counts[person_id] = person_counts.get(person_id, 0) + 1
        source_counts[source_id] = source_counts.get(source_id, 0) + 1
        pair = (person_id, source_id)
        pair_counts[pair] = pair_counts.get(pair, 0) + 1

    while remaining and len(selected) < limit:
        scored = [
            (
                int(str(row["canonical_person_id"]) not in seen_people)
                + int(str(row["source_id"]) not in seen_sources),
                int(str(row["canonical_person_id"]) not in seen_people),
                int(str(row["source_id"]) not in seen_sources),
                str(row["observed_at"]),
                str(row["id"]),
                index,
            )
            for index, row in enumerate(remaining)
        ]
        best_coverage = max(score[0] for score in scored)
        if best_coverage == 0:
            break
        # Higher unseen coverage wins; stable timestamp/id ordering breaks ties.
        best = min(
            (score for score in scored if score[0] == best_coverage),
            key=lambda score: (-score[1], -score[2], score[3], score[4]),
        )
        add(remaining.pop(best[5]))

    while remaining and len(selected) < limit:
        best_index = min(
            range(len(remaining)),
            key=lambda index: (
                person_counts.get(str(remaining[index]["canonical_person_id"]), 0),
                source_counts.get(str(remaining[index]["source_id"]), 0),
                pair_counts.get(
                    (
                        str(remaining[index]["canonical_person_id"]),
                        str(remaining[index]["source_id"]),
                    ),
                    0,
                ),
                str(remaining[index]["observed_at"]),
                str(remaining[index]["id"]),
            ),
        )
        add(remaining.pop(best_index))
    return selected


def _relation_items(
    conn: sqlite3.Connection,
    release_id: str,
    limit: int,
    scope_start: str | None,
    scope_as_of: str,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        WITH candidates AS (
          SELECT left_pos.subject_id, left_pos.atomic_claim_id AS source_claim_id,
                 right_pos.atomic_claim_id AS target_claim_id,
                 left_claim.claim_text AS source_claim_text,
                 right_claim.claim_text AS target_claim_text,
                 left_claim.evidence_text AS source_evidence_text,
                 right_claim.evidence_text AS target_evidence_text,
                 left_claim.observed_at AS source_observed_at,
                 right_claim.observed_at AS target_observed_at,
                 left_pos.canonical_person_id AS source_person_id,
                 right_pos.canonical_person_id AS target_person_id
          FROM current_accepted_position_observations AS left_pos
          JOIN current_accepted_position_observations AS right_pos
            ON right_pos.subject_id = left_pos.subject_id
           AND right_pos.atomic_claim_id > left_pos.atomic_claim_id
          JOIN current_accepted_atomic_claims AS left_claim
            ON left_claim.id = left_pos.atomic_claim_id
          JOIN current_accepted_atomic_claims AS right_claim
            ON right_claim.id = right_pos.atomic_claim_id
          LEFT JOIN claim_relation_judgments AS judged
            ON judged.corpus_release_id = left_pos.corpus_release_id
           AND ((judged.source_claim_id = left_pos.atomic_claim_id
                 AND judged.target_claim_id = right_pos.atomic_claim_id)
             OR (judged.source_claim_id = right_pos.atomic_claim_id
                 AND judged.target_claim_id = left_pos.atomic_claim_id))
           AND judged.review_status IN ('accepted', 'needs_review')
          WHERE left_pos.corpus_release_id = ?
            AND judged.id IS NULL
            AND (
              ? IS NULL
              OR (
                julianday(left_claim.observed_at) >= julianday(?)
                AND julianday(left_claim.observed_at) <= julianday(?)
                AND julianday(right_claim.observed_at) >= julianday(?)
                AND julianday(right_claim.observed_at) <= julianday(?)
              )
            )
        )
        SELECT * FROM candidates
        ORDER BY subject_id, source_claim_id, target_claim_id
        LIMIT ?
        """,
        (
            release_id,
            scope_start,
            scope_start,
            scope_as_of,
            scope_start,
            scope_as_of,
            limit,
        ),
    ).fetchall()
    return [
        {
            "item_id": stable_id(row["source_claim_id"], row["target_claim_id"], prefix="pair_"),
            "subject_id": row["subject_id"],
            "source": {
                "claim_id": row["source_claim_id"],
                "claim_text": row["source_claim_text"],
                "exact_evidence_text": row["source_evidence_text"],
                "observed_at": row["source_observed_at"],
                "canonical_person_id": row["source_person_id"],
            },
            "target": {
                "claim_id": row["target_claim_id"],
                "claim_text": row["target_claim_text"],
                "exact_evidence_text": row["target_evidence_text"],
                "observed_at": row["target_observed_at"],
                "canonical_person_id": row["target_person_id"],
            },
        }
        for row in rows
    ]


def _network_items(
    conn: sqlite3.Connection,
    release_id: str,
    limit: int,
    scope_start: str | None,
    scope_as_of: str,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT DISTINCT sources.id AS source_id, sources.name AS source_title,
               COALESCE(sources.homepage_url, sources.rss_url) AS source_url,
               episodes.id AS episode_id,
               episodes.title AS episode_title, episodes.published_at,
               episodes.description AS episode_description
        FROM corpus_release_episodes AS members
        JOIN episodes ON episodes.id = members.episode_id
        JOIN sources ON sources.id = episodes.source_id
        WHERE members.corpus_release_id = ?
          AND (
            ? IS NULL
            OR (
              julianday(episodes.published_at) >= julianday(?)
              AND julianday(episodes.published_at) <= julianday(?)
            )
          )
        ORDER BY sources.id, episodes.published_at
        LIMIT ?
        """,
        (release_id, scope_start, scope_start, scope_as_of, limit),
    ).fetchall()
    items = [
        {
            "item_id": row["episode_id"],
            "source_id": row["source_id"],
            "source_title": row["source_title"],
            "source_url": row["source_url"],
            "episode_id": row["episode_id"],
            "episode_title": row["episode_title"],
            "published_at": row["published_at"],
            "episode_description": row["episode_description"],
            "accepted_people": [],
        }
        for row in rows
    ]
    if not items:
        return items

    by_episode = {str(item["episode_id"]): item for item in items}
    placeholders = ", ".join("?" for _ in by_episode)
    identity_rows = conn.execute(
        f"""
        SELECT mentions.episode_id, mentions.id AS raw_mention_id,
               mentions.surface_name, mentions.role, mentions.affiliation_surface,
               mentions.segment_id, mentions.discourse_event_id, mentions.evidence_json,
               resolutions.canonical_person_id, people.display_name
        FROM raw_speaker_mentions AS mentions
        JOIN current_accepted_identity_resolutions AS resolutions
          ON resolutions.raw_mention_type = 'speaker'
         AND resolutions.raw_mention_id = mentions.id
         AND resolutions.corpus_release_id = ?
        JOIN current_accepted_people AS people
          ON people.id = resolutions.canonical_person_id
        WHERE mentions.episode_id IN ({placeholders})
        ORDER BY mentions.episode_id, resolutions.canonical_person_id, mentions.id
        """,
        (release_id, *by_episode.keys()),
    ).fetchall()
    people_by_episode: dict[str, dict[str, dict[str, Any]]] = {
        episode_id: {} for episode_id in by_episode
    }
    for row in identity_rows:
        episode_id = str(row["episode_id"])
        person_id = str(row["canonical_person_id"])
        person = people_by_episode[episode_id].get(person_id)
        if person is None:
            person = {
                "canonical_person_id": person_id,
                "display_name": row["display_name"],
                "accepted_identity_raw_mention_ids": [],
                "mention_evidence": [],
            }
            people_by_episode[episode_id][person_id] = person
        person["accepted_identity_raw_mention_ids"].append(str(row["raw_mention_id"]))
        person["mention_evidence"].append(
            {
                "raw_mention_id": row["raw_mention_id"],
                "surface_name": row["surface_name"],
                "role_surface": row["role"],
                "affiliation_surface": row["affiliation_surface"],
                "segment_id": row["segment_id"],
                "discourse_event_id": row["discourse_event_id"],
                "raw_mention_evidence": _safe_json(row["evidence_json"]),
            }
        )
    for episode_id, item in by_episode.items():
        item["accepted_people"] = list(people_by_episode[episode_id].values())
    return items


def _outcome_items(
    conn: sqlite3.Connection,
    release_id: str,
    limit: int,
    as_of: str,
    scope_start: str | None,
    scope_as_of: str,
) -> list[dict[str, Any]]:
    effective_as_of = _canonical_iso(as_of, "as_of")
    rows = conn.execute(
        """
        SELECT claims.id, claims.claim_text, claims.claim_type, claims.raw_speaker,
               claims.canonical_person_id, claims.time_horizon, claims.observed_at,
               claims.forecast_probability, claims.evidence_text,
               episodes.title AS episode_title, episodes.published_at,
               sources.name AS source_title
        FROM current_accepted_atomic_claims AS claims
        LEFT JOIN current_accepted_outcome_resolutions AS outcomes
          ON outcomes.claim_id = claims.id
        LEFT JOIN episodes ON episodes.id = claims.episode_id
        LEFT JOIN sources ON sources.id = claims.source_id
        WHERE claims.corpus_release_id = ?
          AND outcomes.id IS NULL
          AND julianday(claims.observed_at) <= julianday(?)
          AND (
            ? IS NULL
            OR (
              julianday(claims.observed_at) >= julianday(?)
              AND julianday(claims.observed_at) <= julianday(?)
            )
          )
        ORDER BY claims.observed_at, claims.id
        LIMIT ?
        """,
        (
            release_id,
            effective_as_of,
            scope_start,
            scope_start,
            scope_as_of,
            limit,
        ),
    ).fetchall()
    return [
        {
            "item_id": row["id"],
            "atomic_claim_id": row["id"],
            "claim_text": row["claim_text"],
            "claim_type": row["claim_type"],
            "raw_speaker": row["raw_speaker"],
            "canonical_person_id": row["canonical_person_id"],
            "time_horizon": row["time_horizon"],
            "observed_at": row["observed_at"],
            "forecast_probability": row["forecast_probability"],
            "episode_title": row["episode_title"],
            "episode_published_at": row["published_at"],
            "source_title": row["source_title"],
            "exact_evidence_text": row["evidence_text"],
            "as_of": effective_as_of,
        }
        for row in rows
    ]


def _import_decisions(
    conn: sqlite3.Connection,
    *,
    intelligence: Any,
    packet: Mapping[str, Any],
    output: Mapping[str, Any],
    pipeline_run_id: str,
    review_status: str,
    reviewer: str | None,
    producer_model: str,
) -> list[dict[str, Any]]:
    common = {
        "corpus_release_id": packet["corpus_release_id"],
        "pipeline_run_id": pipeline_run_id,
        "judge_model": producer_model,
        "judge_schema_version": JUDGE_SCHEMA_VERSION,
        "review_status": review_status,
    }
    decisions = output["decisions"]
    if packet["target"] == "identities":
        return _import_identities(
            conn,
            intelligence,
            decisions,
            common,
            reviewer,
            packet,
        )
    if packet["target"] == "claims":
        return _import_claim_grouping(conn, intelligence, decisions, common)
    if packet["target"] == "relations":
        return [
            intelligence.record_claim_relation_judgment(conn, {**row, **common})
            for row in decisions
        ]
    if packet["target"] == "networks":
        stored = [
            intelligence.record_source_affiliation(conn, {**row, **common})
            for row in decisions.get("affiliations", [])
        ]
        stored.extend(
            intelligence.record_person_appearance(conn, {**row, **common})
            for row in decisions.get("appearances", [])
        )
        return stored
    return [
        intelligence.record_outcome_resolution(
            conn,
            {
                **row,
                **common,
                "resolver_model": DEFAULT_MODEL,
                "resolver_version": JUDGE_SCHEMA_VERSION,
                "reviewer_version": reviewer or "unreviewed",
            },
        )
        for row in decisions
    ]


def _import_identities(
    conn: sqlite3.Connection,
    intelligence: Any,
    decisions: Mapping[str, Any],
    common: Mapping[str, Any],
    reviewer: str | None,
    packet: Mapping[str, Any],
) -> list[dict[str, Any]]:
    record_person = getattr(intelligence, "record_canonical_person_decision", None)
    if record_person is None:
        raise SemanticReconciliationError("canonical person decision helper is unavailable")
    people: dict[str, str] = {}
    stored: list[dict[str, Any]] = []
    for row in decisions.get("people", []):
        effective_decision = (
            row["decision"] if common["review_status"] == "accepted" else "candidate"
        )
        value = record_person(
            conn,
            {
                **row,
                **common,
                "person_id": stable_id(row["normalized_name"], prefix="cp_"),
                "decision": effective_decision,
                "decision_source": "managed_llm",
                "reviewer": reviewer,
                "evidence": row.get("evidence", {}),
            },
        )
        people[row["person_key"]] = value["id"]
        stored.append(value)
    groups = {str(item["item_id"]): item for item in packet["items"]}
    for original in decisions.get("judgments", []):
        row = dict(original)
        group_id = str(row.pop("item_id"))
        group = groups[group_id]
        person_key = row.pop("person_key", None)
        decision = row.get("decision")
        person_id = people.get(person_key) if person_key else None
        if decision in {"accepted", "merged", "candidate"} and person_id is None:
            raise SemanticReconciliationError("identity decision references an unknown person_key")
        if common["review_status"] == "accepted" and decision == "candidate":
            raise SemanticReconciliationError(
                "accepted identity import cannot retain a candidate group judgment"
            )
        effective_decision = decision
        if common["review_status"] != "accepted" and person_id is not None:
            effective_decision = "candidate"
        evidence = dict(row.get("evidence") or {})
        evidence["identity_group_item_id"] = group_id
        evidence["group_raw_mention_ids"] = list(group["raw_mention_ids"])
        for mention_id in group["raw_mention_ids"]:
            stored.append(
                intelligence.record_identity_resolution_judgment(
                    conn,
                    {
                        **row,
                        **common,
                        "raw_mention_type": "speaker",
                        "raw_mention_id": mention_id,
                        "canonical_person_id": person_id,
                        "decision": effective_decision,
                        "evidence": evidence,
                    },
                )
            )
    return stored


def _import_claim_grouping(
    conn: sqlite3.Connection,
    intelligence: Any,
    decisions: Mapping[str, Any],
    common: Mapping[str, Any],
) -> list[dict[str, Any]]:
    subjects: dict[str, str] = {}
    variants: dict[str, str] = {}
    stored: list[dict[str, Any]] = []
    for row in decisions.get("subjects", []):
        value = intelligence.record_claim_subject(conn, {**row, **common})
        subjects[row["subject_key"]] = value["id"]
        stored.append(value)
    for original in decisions.get("variants", []):
        row = dict(original)
        subject_key = row.pop("subject_key")
        subject_id = subjects.get(subject_key)
        if subject_id is None:
            raise SemanticReconciliationError("variant references an unknown subject_key")
        value = intelligence.record_proposition_variant(
            conn, {**row, **common, "subject_id": subject_id}
        )
        variants[row["variant_key"]] = value["id"]
        stored.append(value)
    for original in decisions.get("positions", []):
        row = dict(original)
        subject_key = row.pop("subject_key")
        variant_key = row.pop("variant_key")
        subject_id = subjects.get(subject_key)
        variant_id = variants.get(variant_key)
        if subject_id is None or variant_id is None:
            raise SemanticReconciliationError("position references an unknown subject or variant key")
        stored.append(
            intelligence.record_position_observation(
                conn,
                {
                    **row,
                    **common,
                    "subject_id": subject_id,
                    "variant_id": variant_id,
                },
            )
        )
    return stored


def _validate_output_envelope(output: Mapping[str, Any], packet: Mapping[str, Any]) -> None:
    _validate_json_schema(output_schema(str(packet["target"])), output, path="$")
    required = {
        "schema_version",
        "target",
        "corpus_release_id",
        "packet_sha256",
        "decisions",
        "abstentions",
    }
    if set(output) != required:
        raise SemanticReconciliationError("semantic output envelope fields are invalid")
    if output["schema_version"] != OUTPUT_SCHEMA_VERSION:
        raise SemanticReconciliationError("semantic output schema version is invalid")
    for field in ("target", "corpus_release_id", "packet_sha256"):
        if output[field] != packet[field]:
            raise SemanticReconciliationError(f"semantic output {field} does not match packet")
    if not isinstance(output["abstentions"], list):
        raise SemanticReconciliationError("semantic output abstentions must be a list")
    allowed_ids = {row["item_id"] for row in packet["items"]}
    for abstention in output["abstentions"]:
        if not isinstance(abstention, dict) or abstention.get("item_id") not in allowed_ids:
            raise SemanticReconciliationError("semantic output abstention is outside packet scope")
    _validate_decision_scope(output["target"], output["decisions"], packet)


def _validate_decision_scope(target: str, decisions: Any, packet: Mapping[str, Any]) -> None:
    item_ids = {row["item_id"] for row in packet["items"]}
    claim_ids = {
        row.get("atomic_claim_id")
        for row in packet["items"]
        if row.get("atomic_claim_id")
    }
    if target == "identities":
        if not isinstance(decisions, dict):
            raise SemanticReconciliationError("identity decisions must be an object")
        people = decisions.get("people", [])
        people_keys = [row.get("person_key") for row in people]
        if len(set(people_keys)) != len(people_keys):
            raise SemanticReconciliationError("identity person_key values must be unique")
        seen_group_ids: set[str] = set()
        for row in decisions.get("judgments", []):
            group_id = row.get("item_id")
            if group_id not in item_ids:
                raise SemanticReconciliationError("identity judgment is outside packet scope")
            if group_id in seen_group_ids:
                raise SemanticReconciliationError("identity group has more than one judgment")
            seen_group_ids.add(str(group_id))
            decision = row.get("decision")
            person_key = row.get("person_key")
            if decision in {"accepted", "merged", "candidate"}:
                if person_key not in people_keys:
                    raise SemanticReconciliationError(
                        "identity decision references an unknown person_key"
                    )
            elif person_key is not None:
                raise SemanticReconciliationError(
                    "rejected or unknown identity judgment cannot reference a person_key"
                )
    elif target == "claims":
        if not isinstance(decisions, dict):
            raise SemanticReconciliationError("claim decisions must be an object")
        allowed_people = {
            str(row["atomic_claim_id"]): str(row["canonical_person_id"])
            for row in packet["items"]
        }
        seen_positions: set[str] = set()
        for row in decisions.get("positions", []):
            claim_id = row.get("atomic_claim_id")
            if claim_id not in claim_ids:
                raise SemanticReconciliationError("position decision is outside packet scope")
            if str(row.get("canonical_person_id")) != allowed_people[str(claim_id)]:
                raise SemanticReconciliationError(
                    "position canonical person is outside the claim identity scope"
                )
            if str(claim_id) in seen_positions:
                raise SemanticReconciliationError("claim has more than one position decision")
            seen_positions.add(str(claim_id))
    elif target == "relations":
        if not isinstance(decisions, list):
            raise SemanticReconciliationError("relation decisions must be a list")
        packet_pairs = {
            frozenset((row["source"]["claim_id"], row["target"]["claim_id"]))
            for row in packet["items"]
        }
        for row in decisions:
            if frozenset((row.get("source_claim_id"), row.get("target_claim_id"))) not in packet_pairs:
                raise SemanticReconciliationError("relation decision is outside packet scope")
    elif target == "networks":
        if not isinstance(decisions, dict):
            raise SemanticReconciliationError("network decisions must be an object")
        source_ids = {row["source_id"] for row in packet["items"]}
        episode_scope = {
            str(row["episode_id"]): {
                "source_id": str(row["source_id"]),
                "person_ids": {
                    str(person["canonical_person_id"])
                    for person in row.get("accepted_people", [])
                },
            }
            for row in packet["items"]
        }
        if any(row.get("source_id") not in source_ids for row in decisions.get("affiliations", [])):
            raise SemanticReconciliationError("affiliation is outside packet scope")
        for row in decisions.get("appearances", []):
            episode = episode_scope.get(str(row.get("episode_id")))
            if episode is None:
                raise SemanticReconciliationError("appearance is outside packet scope")
            if str(row.get("source_id")) != episode["source_id"]:
                raise SemanticReconciliationError("appearance source is outside episode scope")
            if str(row.get("canonical_person_id")) not in episode["person_ids"]:
                raise SemanticReconciliationError(
                    "appearance canonical person is outside packet identity scope"
                )
    else:
        if not isinstance(decisions, list):
            raise SemanticReconciliationError("outcome decisions must be a list")
        if any(row.get("claim_id") not in item_ids for row in decisions):
            raise SemanticReconciliationError("outcome decision is outside packet scope")


def _read_packet(path: Path) -> dict[str, Any]:
    packet = _read_json_object(path, "packet")
    required = {
        "schema_version",
        "target",
        "corpus_release_id",
        "generated_at",
        "as_of",
        "scope",
        "scope_as_of",
        "scope_start",
        "limit",
        "item_count",
        "semantic_authority",
        "selection_authority",
        "items",
        "packet_sha256",
    }
    if set(packet) != required or packet.get("schema_version") != PACKET_SCHEMA_VERSION:
        raise SemanticReconciliationError("reconciliation packet envelope is invalid")
    _target(packet["target"])
    bounded_limit = _limit(packet["limit"])
    resolved_scope, resolved_scope_as_of, scope_start = _scope_window(
        packet["scope"], packet["scope_as_of"]
    )
    if (
        packet["scope"] != resolved_scope
        or packet["scope_as_of"] != resolved_scope_as_of
        or packet["scope_start"] != scope_start
    ):
        raise SemanticReconciliationError("reconciliation packet scope does not verify")
    if packet["as_of"] is not None:
        _canonical_iso(packet["as_of"], "as_of")
    if not isinstance(packet["items"], list) or packet["item_count"] != len(packet["items"]):
        raise SemanticReconciliationError("reconciliation packet item count is invalid")
    if len(packet["items"]) > bounded_limit:
        raise SemanticReconciliationError("reconciliation packet exceeds its item limit")
    without_hash = dict(packet)
    stored_hash = without_hash.pop("packet_sha256")
    if stored_hash != sha256_text(dumps_json(without_hash)):
        raise SemanticReconciliationError("reconciliation packet hash does not verify")
    return packet


def _base_instructions(target: str) -> str:
    return (
        "You are the semantic judge for a private podcast intelligence corpus. "
        "Use only the supplied packet. Do not use tools, the network, unstated world knowledge, "
        "regex rules, token overlap, embeddings, or deterministic similarity as semantic authority. "
        "Return only schema-valid JSON. Preserve exact IDs. Abstain whenever evidence is insufficient. "
        "Corpus agreement is not objective truth. Identity, claim grouping, relation classification, "
        f"and outcome interpretation are LLM-owned. This turn judges only {target}."
    )


def _judge_prompt(packet: Mapping[str, Any]) -> str:
    target_guidance = {
        "identities": (
            "Each item is a deterministic group of exact raw-mention occurrences from one episode, "
            "with no identity assertion. Return exactly one group judgment keyed by item_id; it will "
            "be applied to every listed raw_mention_id. Create canonical people explicitly and reuse "
            "one person_key across groups only when the evidence supports equivalence. This is a final "
            "release adjudication: do not return candidate people or candidate judgments. A person "
            "referenced by an accepted or merged judgment must itself be accepted or merged. Use "
            "unknown rather than guessing whenever a final identity cannot be supported."
        ),
        "claims": (
            "Group atomic claims into precise subjects and proposition variants, then record the "
            "speaker position. Do not add semantics absent from the exact claim/evidence."
        ),
        "relations": (
            "Classify each supplied pair as equivalent, supports, contradicts, qualifies, orthogonal, "
            "or incomparable, respecting temporal scope and qualification."
        ),
        "networks": (
            "Judge show affiliations and person appearances only from supplied metadata/evidence. "
            "Use independent explicitly when supported and unknown roles when unresolved."
        ),
        "outcomes": (
            "First decide whether the claim has explicit checkable resolution criteria. Without supplied "
            "authoritative evidence, mark unresolved or unverifiable; never infer truth from consensus."
        ),
    }[packet["target"]]
    return (
        f"{target_guidance}\n\n"
        "Every rationale must identify the packet evidence used. Confidence is 0 through 1. "
        "Return an abstention for every packet item not represented by a decision.\n\n"
        f"PACKET_JSON\n{dumps_json(packet)}"
    )


def _identity_decisions_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["people", "judgments"],
        "properties": {
            "people": {
                "type": "array",
                "maxItems": MAX_LIMIT,
                "items": _object_schema(
                    ["person_key", "display_name", "normalized_name", "decision", "confidence", "rationale", "evidence"],
                    {
                        "person_key": {"type": "string"},
                        "display_name": {"type": "string"},
                        "normalized_name": {"type": "string"},
                        "decision": {"type": "string", "enum": ["candidate", "accepted", "rejected", "merged"]},
                        "confidence": _confidence_schema(),
                        "rationale": {"type": "string"},
                        "evidence": _evidence_schema(),
                    },
                ),
            },
            "judgments": {
                "type": "array",
                "maxItems": MAX_LIMIT,
                "items": _object_schema(
                    ["item_id", "person_key", "decision", "confidence", "rationale", "evidence"],
                    {
                        "item_id": {"type": "string"},
                        "person_key": {"type": ["string", "null"]},
                        "decision": {"type": "string", "enum": ["candidate", "accepted", "rejected", "merged", "unknown"]},
                        "confidence": _confidence_schema(),
                        "rationale": {"type": "string"},
                        "evidence": _evidence_schema(),
                    },
                ),
            },
        },
    }


def _claim_decisions_schema() -> dict[str, Any]:
    subject = _object_schema(
        ["subject_key", "subject_text", "subject_type", "domain", "scope_note", "confidence", "rationale", "evidence"],
        {
            "subject_key": {"type": "string"},
            "subject_text": {"type": "string"},
            "subject_type": {"type": "string"},
            "domain": {"type": ["string", "null"]},
            "scope_note": {"type": ["string", "null"]},
            "confidence": _confidence_schema(),
            "rationale": {"type": "string"},
            "evidence": _evidence_schema(),
        },
    )
    variant = _object_schema(
        ["variant_key", "subject_key", "proposition_text", "predicate_text", "object_text", "polarity", "time_horizon", "conditions", "confidence", "rationale", "evidence"],
        {
            "variant_key": {"type": "string"},
            "subject_key": {"type": "string"},
            "proposition_text": {"type": "string"},
            "predicate_text": {"type": ["string", "null"]},
            "object_text": {"type": ["string", "null"]},
            "polarity": {"type": ["string", "null"]},
            "time_horizon": {"type": ["string", "null"]},
            "conditions": _conditions_schema(),
            "confidence": _confidence_schema(),
            "rationale": {"type": "string"},
            "evidence": _evidence_schema(),
        },
    )
    position = _object_schema(
        ["atomic_claim_id", "canonical_person_id", "subject_key", "variant_key", "position", "certainty", "observed_at", "confidence", "rationale", "evidence"],
        {
            "atomic_claim_id": {"type": "string"},
            "canonical_person_id": {"type": "string"},
            "subject_key": {"type": "string"},
            "variant_key": {"type": "string"},
            "position": {"type": "string"},
            "certainty": {"type": "string"},
            "observed_at": {"type": "string"},
            "confidence": _confidence_schema(),
            "rationale": {"type": "string"},
            "evidence": _evidence_schema(),
        },
    )
    return _object_schema(
        ["subjects", "variants", "positions"],
        {
            "subjects": {"type": "array", "maxItems": MAX_LIMIT, "items": subject},
            "variants": {"type": "array", "maxItems": MAX_LIMIT, "items": variant},
            "positions": {"type": "array", "maxItems": MAX_LIMIT, "items": position},
        },
    )


def _relation_decisions_schema() -> dict[str, Any]:
    return {
        "type": "array",
        "maxItems": MAX_LIMIT,
        "items": _object_schema(
            ["source_claim_id", "target_claim_id", "relation", "temporal_scope", "confidence", "rationale", "evidence"],
            {
                "source_claim_id": {"type": "string"},
                "target_claim_id": {"type": "string"},
                "relation": {"type": "string", "enum": ["equivalent", "supports", "contradicts", "qualifies", "orthogonal", "incomparable"]},
                "temporal_scope": {"type": "string"},
                "confidence": _confidence_schema(),
                "rationale": {"type": "string"},
                "evidence": _evidence_schema(),
            },
        ),
    }


def _network_decisions_schema() -> dict[str, Any]:
    affiliation = _object_schema(
        ["source_id", "affiliation_kind", "affiliation_key", "affiliation_name", "canonical_org_id", "valid_from", "valid_to", "confidence", "evidence"],
        {
            "source_id": {"type": "string"},
            "affiliation_kind": {"type": "string", "enum": ["network", "publisher", "owner", "independent"]},
            "affiliation_key": {"type": "string"},
            "affiliation_name": {"type": "string"},
            "canonical_org_id": {"type": ["string", "null"]},
            "valid_from": {"type": ["string", "null"]},
            "valid_to": {"type": ["string", "null"]},
            "confidence": _confidence_schema(),
            "evidence": _evidence_schema(),
        },
    )
    appearance = _object_schema(
        ["canonical_person_id", "episode_id", "source_id", "source_affiliation_id", "role", "appeared_at", "confidence", "evidence"],
        {
            "canonical_person_id": {"type": "string"},
            "episode_id": {"type": "string"},
            "source_id": {"type": "string"},
            "source_affiliation_id": {"type": ["string", "null"]},
            "role": {"type": "string", "enum": ["host", "cohost", "guest", "panelist", "unknown"]},
            "appeared_at": {"type": "string"},
            "confidence": _confidence_schema(),
            "evidence": _evidence_schema(),
        },
    )
    return _object_schema(
        ["affiliations", "appearances"],
        {
            "affiliations": {"type": "array", "maxItems": MAX_LIMIT, "items": affiliation},
            "appearances": {"type": "array", "maxItems": MAX_LIMIT, "items": appearance},
        },
    )


def _outcome_decisions_schema() -> dict[str, Any]:
    return {
        "type": "array",
        "maxItems": MAX_LIMIT,
        "items": _object_schema(
            ["claim_id", "resolution_question", "due_at", "resolution_window_start", "resolution_window_end", "resolution_criteria", "outcome", "resolved_at", "confidence", "rationale", "evidence", "authoritative_evidence", "as_of"],
            {
                "claim_id": {"type": "string"},
                "resolution_question": {"type": "string"},
                "due_at": {"type": ["string", "null"]},
                "resolution_window_start": {"type": ["string", "null"]},
                "resolution_window_end": {"type": ["string", "null"]},
                "resolution_criteria": {"type": "string"},
                "outcome": {"type": "string", "enum": ["true", "false", "mixed", "unresolved", "unverifiable"]},
                "resolved_at": {"type": "string"},
                "confidence": _confidence_schema(),
                "rationale": {"type": "string"},
                "evidence": _evidence_schema(),
                "authoritative_evidence": _evidence_schema(),
                "as_of": {"type": "string"},
            },
        ),
    }


def _object_schema(required: Sequence[str], properties: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": list(required),
        "properties": dict(properties),
    }


def _confidence_schema() -> dict[str, Any]:
    return {"type": "number", "minimum": 0, "maximum": 1}


def _evidence_schema() -> dict[str, Any]:
    """Strict evidence pointer shape accepted by structured output."""

    return _object_schema(
        ["item_ids", "note", "source_urls"],
        {
            "item_ids": {
                "type": "array",
                "maxItems": MAX_LIMIT,
                "items": {"type": "string"},
            },
            "note": {"type": "string"},
            "source_urls": {
                "type": "array",
                "maxItems": MAX_LIMIT,
                "items": {"type": "string"},
            },
        },
    )


def _conditions_schema() -> dict[str, Any]:
    return _object_schema(
        ["condition_texts"],
        {
            "condition_texts": {
                "type": "array",
                "maxItems": MAX_LIMIT,
                "items": {"type": "string"},
            }
        },
    )


def _validate_json_schema(schema: Mapping[str, Any], value: Any, *, path: str) -> None:
    """Validate the strict JSON-Schema subset used by this module.

    Keeping the validator local avoids making canonical imports depend on an
    optional package.  Unsupported schema keywords fail closed so future
    changes cannot silently weaken import validation.
    """

    supported = {
        "type",
        "const",
        "enum",
        "properties",
        "required",
        "additionalProperties",
        "items",
        "maxItems",
        "minimum",
        "maximum",
        "pattern",
    }
    unknown = set(schema) - supported
    if unknown:
        raise SemanticReconciliationError(
            f"unsupported output-schema keywords at {path}: {', '.join(sorted(unknown))}"
        )
    expected = schema.get("type")
    allowed_types = expected if isinstance(expected, list) else [expected]
    if expected is not None and not any(_json_type_matches(name, value) for name in allowed_types):
        raise SemanticReconciliationError(f"semantic output type is invalid at {path}")
    if "const" in schema and value != schema["const"]:
        raise SemanticReconciliationError(f"semantic output const is invalid at {path}")
    if "enum" in schema and value not in schema["enum"]:
        raise SemanticReconciliationError(f"semantic output enum is invalid at {path}")
    if isinstance(value, dict):
        properties = schema.get("properties", {})
        if not isinstance(properties, dict):
            raise SemanticReconciliationError(f"output schema properties are invalid at {path}")
        required = schema.get("required", [])
        missing = [name for name in required if name not in value]
        if missing:
            raise SemanticReconciliationError(
                f"semantic output is missing {', '.join(missing)} at {path}"
            )
        if schema.get("additionalProperties") is False:
            extras = set(value) - set(properties)
            if extras:
                raise SemanticReconciliationError(
                    f"semantic output has extra fields at {path}: {', '.join(sorted(extras))}"
                )
        for name, child in value.items():
            child_schema = properties.get(name)
            if child_schema is not None:
                _validate_json_schema(child_schema, child, path=f"{path}.{name}")
    if isinstance(value, list):
        maximum = schema.get("maxItems")
        if maximum is not None and len(value) > int(maximum):
            raise SemanticReconciliationError(f"semantic output list is too long at {path}")
        item_schema = schema.get("items")
        if item_schema is not None:
            for index, child in enumerate(value):
                _validate_json_schema(item_schema, child, path=f"{path}[{index}]")
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if "minimum" in schema and value < schema["minimum"]:
            raise SemanticReconciliationError(f"semantic output number is too small at {path}")
        if "maximum" in schema and value > schema["maximum"]:
            raise SemanticReconciliationError(f"semantic output number is too large at {path}")
    if isinstance(value, str) and "pattern" in schema:
        if re.fullmatch(str(schema["pattern"]), value) is None:
            raise SemanticReconciliationError(f"semantic output string pattern is invalid at {path}")


def _json_type_matches(name: Any, value: Any) -> bool:
    return {
        "null": value is None,
        "object": isinstance(value, dict),
        "array": isinstance(value, list),
        "string": isinstance(value, str),
        "number": isinstance(value, (int, float)) and not isinstance(value, bool),
        "integer": isinstance(value, int) and not isinstance(value, bool),
        "boolean": isinstance(value, bool),
    }.get(str(name), False)


def _release_id(conn: sqlite3.Connection, release_id: str | None) -> str:
    if release_id:
        row = conn.execute(
            "SELECT id FROM current_accepted_corpus_releases WHERE id = ?", (release_id,)
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT id FROM current_accepted_corpus_releases ORDER BY promotion_revision DESC LIMIT 1"
        ).fetchone()
    if row is None:
        raise SemanticReconciliationError("an accepted promoted corpus release is required")
    return str(row["id"])


def _target(value: str) -> str:
    target = str(value or "").strip().lower()
    if target not in TARGETS:
        raise SemanticReconciliationError(f"target must be one of: {', '.join(sorted(TARGETS))}")
    return target


def _scope_window(scope: str, scope_as_of: str) -> tuple[str, str, str | None]:
    resolved_scope = str(scope or "").strip().lower()
    if resolved_scope not in SCOPES:
        raise SemanticReconciliationError(
            f"scope must be one of: {', '.join(sorted(SCOPES))}"
        )
    resolved_as_of = _canonical_iso(scope_as_of, "scope_as_of")
    if resolved_scope == "all":
        return resolved_scope, resolved_as_of, None
    parsed = dt.datetime.fromisoformat(resolved_as_of.replace("Z", "+00:00"))
    month_index = parsed.year * 12 + (parsed.month - 1) - 18
    year, zero_based_month = divmod(month_index, 12)
    month = zero_based_month + 1
    day = min(parsed.day, calendar.monthrange(year, month)[1])
    scope_start = parsed.replace(year=year, month=month, day=day).isoformat()
    return resolved_scope, resolved_as_of, scope_start


def _canonical_iso(value: Any, label: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise SemanticReconciliationError(f"{label} must be a non-empty ISO timestamp")
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SemanticReconciliationError(f"{label} must be an ISO timestamp") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc).isoformat()


def _limit(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= MAX_LIMIT:
        raise SemanticReconciliationError(f"limit must be between 1 and {MAX_LIMIT}")
    return value


def _artifact_dir(
    output_dir: str | Path | None,
    release_id: str,
    target: str,
    packet_id: str,
) -> Path:
    base = (
        Path(output_dir).expanduser().resolve()
        if output_dir is not None
        else root() / "work" / "pif-intelligence" / "reconciliation"
    )
    return base / release_id / target / packet_id


def _write_immutable_json(path: Path, value: Mapping[str, Any]) -> None:
    payload = dumps_json(dict(value)) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise FileExistsError(f"immutable artifact already exists with different content: {path}")
        return
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    descriptor = os.open(path, flags, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        raise


def _read_json_object(path: Path, label: str) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(path)
    if path.stat().st_size > MAX_PACKET_BYTES:
        raise SemanticReconciliationError(f"{label} exceeds the 8 MiB bound")
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise SemanticReconciliationError(f"{label} must be a JSON object")
    return value


def _safe_json(value: Any) -> Any:
    if value is None:
        return {}
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, ValueError):
        return {}


def _pinned_codex_binary() -> Path:
    configured = os.environ.get("PIF_CODEX_BINARY")
    candidates = [
        Path(configured).expanduser() if configured else None,
        root() / "work" / "app-server-development-v2" / "pinned-runtime" / "codex-0.144.1" / "bin" / "codex",
    ]
    for candidate in candidates:
        if candidate is not None and candidate.is_file() and os.access(candidate, os.X_OK):
            return candidate.resolve()
    raise FileNotFoundError(
        "the pinned Codex 0.144.1 app-server binary is unavailable; set PIF_CODEX_BINARY"
    )


def _sha256_file(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _usage_dict(result: AppServerTurnResult) -> dict[str, Any] | None:
    if result.usage is None:
        return None
    return {
        "input_tokens": result.usage.input_tokens,
        "cached_input_tokens": result.usage.cached_input_tokens,
        "output_tokens": result.usage.output_tokens,
        "reasoning_output_tokens": result.usage.reasoning_output_tokens,
        "total_tokens": result.usage.total_tokens,
    }


def _decision_counts(rows: Iterable[Mapping[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        schema = str(row.get("schema_version") or "canonical_person")
        counts[schema] = counts.get(schema, 0) + 1
    return dict(sorted(counts.items()))
