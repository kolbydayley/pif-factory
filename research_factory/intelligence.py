"""Versioned intelligence records and deterministic accepted-decision scoring.

This module deliberately does not infer claim meaning, identity, stance, claim
relations, or outcomes.  Those decisions arrive from an accepted LLM/judge
record.  The code below only validates exact provenance, stores immutable
revisions, selects current accepted rows, and performs explicit arithmetic.
"""

from __future__ import annotations

import datetime as dt
import json
import math
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .cohort import CohortValidationError, load_production_cohort
from .util import UTC, dumps_json, now_iso, parse_datetime, sha256_text, stable_id


ATOMIC_CLAIM_SCHEMA = "atomic_claim_v1"
RELATIONS = frozenset(
    {"equivalent", "supports", "contradicts", "qualifies", "orthogonal", "incomparable"}
)
CONTRARIAN_CLASSIFICATIONS = frozenset(
    {"contrarian", "not_contrarian", "insufficient_coverage"}
)
OUTCOMES = frozenset({"true", "false", "mixed", "unresolved", "unverifiable"})
REVIEW_STATUSES = frozenset({"pending", "accepted", "rejected", "needs_review"})
PIPELINE_STATUSES = frozenset({"pending", "running", "succeeded", "failed", "canceled"})
PIPELINE_AUTHORITY_STAGES = frozenset(
    {
        "release",
        "atomic_claims",
        "identities",
        "claims",
        "networks",
        "relations",
        "consensus",
        "contrarian",
        "outcomes",
    }
)
PIPELINE_AUTHORITY_DECISIONS = frozenset({"accepted", "rejected", "superseded"})

DEFAULT_WINDOW_DAYS = 90
DEFAULT_DOMINANT_THRESHOLD = 0.50
DEFAULT_TARGET_SHARE_THRESHOLD = 0.25
DEFAULT_MINIMUM_PEOPLE = 5
DEFAULT_MINIMUM_SHOWS = 3
DEFAULT_MINIMUM_NETWORKS = 2
DEFAULT_AUDITED_PILOT_ID = "scale-gate-v31-2026-07-02"
DEFAULT_PRODUCTION_COHORT_ID = "pif-gold-25-v1"
DEFAULT_PRODUCTION_COHORT_PATH = (
    Path(__file__).resolve().parents[1] / "config" / "production_cohort_v1.json"
)

_PIPELINE_TRANSITIONS = {
    "pending": frozenset({"running", "canceled"}),
    "running": frozenset({"succeeded", "failed", "canceled"}),
    "succeeded": frozenset(),
    "failed": frozenset(),
    "canceled": frozenset(),
}

# Stage authority is deliberately allowlisted.  A run cannot become canonical
# merely by succeeding or by choosing a production-looking schema string.  The
# legacy monolithic contract remains available only for already-audited v1
# fixtures/releases; lab, evaluator, diagnostic, candidate, and shadow run
# families are absent by construction.
_STAGE_RUN_CONTRACTS: dict[str, frozenset[tuple[str, str, str]]] = {
    "release": frozenset(
        {
            ("release_build", "corpus_release_build", "corpus_release_build_v1"),
            ("atomic_claim_import", "atomic_claim_v1", "atomic_claim_import_v1"),
            ("intelligence", "versioned_intelligence", "1"),
        }
    ),
    "atomic_claims": frozenset(
        {
            ("atomic_claim_import", "atomic_claim_v1", "atomic_claim_import_v1"),
            ("intelligence", "versioned_intelligence", "1"),
        }
    ),
    "identities": frozenset(
        {
            (
                "semantic_reconcile_identities",
                "pif_semantic_reconciliation_output_v2",
                "1",
            ),
            ("intelligence", "versioned_intelligence", "1"),
        }
    ),
    "claims": frozenset(
        {
            (
                "semantic_reconcile_claims",
                "pif_semantic_reconciliation_output_v2",
                "1",
            ),
            ("intelligence", "versioned_intelligence", "1"),
        }
    ),
    "networks": frozenset(
        {
            (
                "semantic_reconcile_networks",
                "pif_semantic_reconciliation_output_v2",
                "1",
            ),
            ("intelligence", "versioned_intelligence", "1"),
        }
    ),
    "relations": frozenset(
        {
            (
                "semantic_reconcile_relations",
                "pif_semantic_reconciliation_output_v2",
                "1",
            ),
            ("intelligence", "versioned_intelligence", "1"),
        }
    ),
    "consensus": frozenset(
        {
            ("consensus_refresh", "consensus_snapshot_v1", "1"),
            ("intelligence", "versioned_intelligence", "1"),
        }
    ),
    "contrarian": frozenset(
        {
            ("contrarian_refresh", "contrarian_snapshot_v1", "1"),
            ("intelligence", "versioned_intelligence", "1"),
        }
    ),
    "outcomes": frozenset(
        {
            (
                "semantic_reconcile_outcomes",
                "pif_semantic_reconciliation_output_v2",
                "1",
            ),
            ("manual_outcome_resolution", "outcome_resolution_v1", "1"),
            ("outcome_resolution", "outcome_resolution_v1", "1"),
            ("outcomes_resolve", "outcome_resolution_v1", "1"),
            ("intelligence", "versioned_intelligence", "1"),
        }
    ),
}


class IntelligenceValidationError(ValueError):
    """A deterministic intelligence contract validation failed."""


def categorical_outcome_score(outcome: str) -> float | None:
    """Return the approved 1/.5/0 score; unresolved outcomes are excluded."""

    value = _enum(outcome, OUTCOMES, "outcome")
    return {"true": 1.0, "mixed": 0.5, "false": 0.0}.get(value)


def brier_score(probability: float | None, outcome: str) -> float | None:
    """Score a probabilistic binary forecast separately from categorical accuracy.

    ``mixed``, ``unresolved``, and ``unverifiable`` do not have a binary target,
    so they are intentionally excluded rather than coerced to an outcome.
    """

    value = _enum(outcome, OUTCOMES, "outcome")
    if probability is None or value not in {"true", "false"}:
        return None
    p = _probability(probability, "probability")
    observed = 1.0 if value == "true" else 0.0
    return (p - observed) ** 2


def relation_alignment_score(relation: str) -> float | None:
    """Map an accepted semantic relation to explicit consensus arithmetic."""

    value = _enum(relation, RELATIONS, "relation")
    if value in {"equivalent", "supports"}:
        return 1.0
    if value == "qualifies":
        return 0.5
    if value == "contradicts":
        return 0.0
    return None


def relation_bucket(relation: str) -> str | None:
    value = _enum(relation, RELATIONS, "relation")
    if value in {"equivalent", "supports"}:
        return "aligned"
    if value == "qualifies":
        return "qualified"
    if value == "contradicts":
        return "opposed"
    return None


def validate_exact_evidence(
    evidence_unit_text: str,
    evidence_text: str,
    evidence_start: int,
    evidence_end: int,
) -> None:
    """Require an exact literal range inside the caller-supplied evidence unit."""

    if not isinstance(evidence_unit_text, str):
        raise IntelligenceValidationError("evidence_unit_text must be a string")
    if not isinstance(evidence_text, str) or not evidence_text:
        raise IntelligenceValidationError("evidence_text must be non-empty")
    start = _integer(evidence_start, "evidence_start", minimum=0)
    end = _integer(evidence_end, "evidence_end", minimum=1)
    if end <= start:
        raise IntelligenceValidationError("evidence_end must be greater than evidence_start")
    if end > len(evidence_unit_text):
        raise IntelligenceValidationError("evidence range exceeds the evidence unit")
    if evidence_unit_text[start:end] != evidence_text:
        raise IntelligenceValidationError("evidence text does not exactly match the supplied offsets")


def create_corpus_release(
    conn: sqlite3.Connection,
    *,
    release_version: int | None = None,
    cutoff_at: str | None = None,
    pilot_id: str | None = None,
    cohort_manifest_path: str | Path | None = None,
    episode_ids: Sequence[str] | None = None,
    transcript_ids: Sequence[str] | None = None,
    segment_ids: Sequence[str] | None = None,
    accepted_label_ids: Sequence[str] | None = None,
    manifest_metadata: Mapping[str, Any] | None = None,
    manifest_sha256: str | None = None,
    release_id: str | None = None,
    parent_release_id: str | None = None,
    status: str = "accepted",
    created_at: str | None = None,
) -> dict[str, Any]:
    """Freeze episode/transcript/segment/accepted-label membership and hashes.

    With no explicit IDs, membership is derived from the audited v3.1 pilot.
    Promotion is a separate append-only action; this function never mutates an
    older release or a current pointer.
    """

    explicit_membership = any(
        value is not None
        for value in (episode_ids, transcript_ids, segment_ids, accepted_label_ids)
    )
    cohort_spec: dict[str, Any] | None = None
    if not explicit_membership:
        cohort_spec = _load_cohort_manifest(cohort_manifest_path or DEFAULT_PRODUCTION_COHORT_PATH)
        episode_ids = [str(row["id"]) for row in cohort_spec["episodes"]]
        if pilot_id is None:
            pilot_id = str(cohort_spec["cohort_id"])
    pilot_id = pilot_id or DEFAULT_PRODUCTION_COHORT_ID

    if release_version is None:
        version_row = conn.execute(
            "SELECT COALESCE(MAX(release_version), 0) + 1 AS next_version FROM corpus_releases"
        ).fetchone()
        version = int(version_row["next_version"])
    else:
        version = _integer(release_version, "release_version", minimum=1)
    created = _iso(created_at or now_iso(), "created_at")
    cutoff = _iso(cutoff_at or created, "cutoff_at")
    members = _collect_release_members(
        conn,
        pilot_id=_text(pilot_id, "pilot_id"),
        episode_ids=episode_ids,
        transcript_ids=transcript_ids,
        segment_ids=segment_ids,
        accepted_label_ids=accepted_label_ids,
        expected_episodes=(cohort_spec or {}).get("episodes"),
        require_production_shape=(cohort_spec is not None or pilot_id == DEFAULT_PRODUCTION_COHORT_ID),
    )
    manifest = _release_manifest(
        pilot_id=pilot_id,
        metadata={
            **_json_object(manifest_metadata or {}, "manifest_metadata"),
            **(
                {
                    "cohort_schema_version": cohort_spec.get("schema_version"),
                    "cohort_id": cohort_spec.get("cohort_id"),
                    "cohort_sha256": cohort_spec.get("sha256"),
                    "audited_pilot_episode_ids": cohort_spec.get(
                        "constraint_evidence", {}
                    ).get("audited_pilot_episode_ids", []),
                }
                if cohort_spec
                else {}
            ),
        },
        members=members,
    )
    manifest_json = dumps_json(manifest)
    computed_digest = sha256_text(manifest_json)
    if manifest_sha256 is not None and _sha256(manifest_sha256, "manifest_sha256") != computed_digest:
        raise IntelligenceValidationError("manifest_sha256 does not match frozen release membership")
    digest = computed_digest
    release_status = _enum(status, {"draft", "accepted", "retired"}, "status")
    sources = {
        row["source_id"]
        for row in members["episodes"]
        if row.get("source_id") is not None
    }
    label_ids = [row["id"] for row in members["labels"]]
    legacy_claim_count = 0
    if label_ids:
        placeholders = ",".join("?" for _ in label_ids)
        legacy_claim_count = int(
            conn.execute(
                f"SELECT COUNT(*) AS count FROM claims WHERE label_id IN ({placeholders})",
                label_ids,
            ).fetchone()["count"]
        )
    row = {
        "id": release_id or stable_id(str(version), digest, cutoff, prefix="crel_"),
        "schema_version": "corpus_release_v1",
        "release_version": version,
        "parent_release_id": parent_release_id,
        "cutoff_at": cutoff,
        "manifest_sha256": digest,
        "manifest_json": manifest_json,
        "source_count": len(sources),
        "item_count": len(members["episodes"]),
        "claim_count": legacy_claim_count,
        "status": release_status,
        "created_at": created,
    }
    savepoint = "create_corpus_release"
    conn.execute(f"SAVEPOINT {savepoint}")
    try:
        stored = _insert_immutable(conn, "corpus_releases", row)
        _insert_release_members(conn, stored["id"], members, created)
        conn.execute(f"RELEASE SAVEPOINT {savepoint}")
    except Exception:
        conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
        conn.execute(f"RELEASE SAVEPOINT {savepoint}")
        raise
    stored["pilot_id"] = pilot_id
    stored["membership_counts"] = {
        kind: len(members[kind]) for kind in ("episodes", "transcripts", "segments", "labels")
    }
    return stored


def verify_corpus_release(conn: sqlite3.Connection, release_id: str) -> dict[str, Any]:
    """Verify immutable membership closure and every current content hash."""

    release = _fetch_required(conn, "corpus_releases", _text(release_id, "release_id"))
    stored_manifest = json.loads(release["manifest_json"])
    stored_members = _load_release_members(conn, release_id)
    errors: list[str] = []
    for kind in ("episodes", "transcripts", "segments", "labels"):
        if not stored_members[kind]:
            errors.append(f"release has no {kind}")

    current_members: dict[str, list[dict[str, Any]]] = {
        "episodes": [],
        "transcripts": [],
        "segments": [],
        "labels": [],
    }
    for kind, rows in stored_members.items():
        for member in rows:
            try:
                current = _release_member_record(conn, kind, member["id"])
            except IntelligenceValidationError:
                errors.append(f"missing {kind[:-1]} {member['id']}")
                continue
            current_members[kind].append(current)
            if current["content_sha256"] != member["content_sha256"]:
                errors.append(f"content hash changed for {kind[:-1]} {member['id']}")

    episode_set = {row["id"] for row in stored_members["episodes"]}
    transcript_set = {row["id"] for row in stored_members["transcripts"]}
    segment_set = {row["id"] for row in stored_members["segments"]}
    for transcript in stored_members["transcripts"]:
        if transcript["episode_id"] not in episode_set:
            errors.append(f"transcript {transcript['id']} has no release episode")
    for segment in stored_members["segments"]:
        if segment["transcript_id"] not in transcript_set or segment["episode_id"] not in episode_set:
            errors.append(f"segment {segment['id']} has incomplete release ancestry")
    for label in stored_members["labels"]:
        if label["segment_id"] not in segment_set:
            errors.append(f"label {label['id']} has no release segment")
        if label["accepted_status"] != "ready":
            errors.append(f"label {label['id']} was not accepted at release creation")
    for label in current_members["labels"]:
        try:
            _validate_release_label(conn, label)
        except IntelligenceValidationError as exc:
            errors.append(str(exc))

    rebuilt_manifest = _release_manifest(
        pilot_id=str(stored_manifest.get("pilot_id") or ""),
        metadata=_json_object(stored_manifest.get("metadata", {}), "manifest.metadata"),
        members=current_members,
    )
    rebuilt_json = dumps_json(rebuilt_manifest)
    rebuilt_sha256 = sha256_text(rebuilt_json)
    if rebuilt_json != release["manifest_json"]:
        errors.append("release manifest no longer matches member records")
    if rebuilt_sha256 != release["manifest_sha256"]:
        errors.append("release manifest hash mismatch")
    return {
        "ok": not errors,
        "release_id": release_id,
        "release_version": int(release["release_version"]),
        "status": release["status"],
        "manifest_sha256": release["manifest_sha256"],
        "membership_counts": {
            kind: len(stored_members[kind])
            for kind in ("episodes", "transcripts", "segments", "labels")
        },
        "errors": errors,
    }


def promote_corpus_release(
    conn: sqlite3.Connection,
    release_id: str,
    *,
    pipeline_run_id: str,
    promoted_by: str,
    rationale: str,
    created_at: str | None = None,
) -> dict[str, Any]:
    """Append the only current-release pointer after verification and run success."""

    verification = verify_corpus_release(conn, release_id)
    if not verification["ok"]:
        raise IntelligenceValidationError(
            "corpus release verification failed: " + "; ".join(verification["errors"])
        )
    release = _fetch_required(conn, "corpus_releases", release_id)
    if release["status"] != "accepted":
        raise IntelligenceValidationError("only an immutable accepted release can be promoted")
    run = _fetch_required(conn, "pipeline_runs", pipeline_run_id)
    if run["corpus_release_id"] != release_id or run["status"] != "succeeded":
        raise IntelligenceValidationError(
            "release promotion requires a succeeded pipeline run bound to the same release"
        )
    created = _iso(created_at or now_iso(), "created_at")
    reviewer = _text(promoted_by, "promoted_by")
    promotion_rationale = _text(rationale, "rationale")
    authority = accept_pipeline_run(
        conn,
        pipeline_run_id,
        stage="release",
        reviewed_by=reviewer,
        rationale="Verified immutable release promotion: " + promotion_rationale,
        decided_at=created,
    )
    # Compatibility for the original deliberately-monolithic production
    # contract.  Its explicit promotion was historically the single reviewer
    # boundary for every intelligence table.  New production runs use the
    # stage-specific contracts above and receive only release authority here.
    if (
        run["run_type"],
        run["run_schema"],
        run["run_schema_version"],
    ) == ("intelligence", "versioned_intelligence", "1"):
        for stage in sorted(PIPELINE_AUTHORITY_STAGES - {"release"}):
            accept_pipeline_run(
                conn,
                pipeline_run_id,
                stage=stage,
                reviewed_by=reviewer,
                rationale=(
                    "Legacy monolithic intelligence run explicitly accepted at "
                    "verified release promotion: " + promotion_rationale
                ),
                decided_at=created,
            )
    previous = conn.execute(
        """
        SELECT id, promotion_revision FROM corpus_release_promotions
        ORDER BY promotion_revision DESC LIMIT 1
        """
    ).fetchone()
    revision = int(previous["promotion_revision"]) + 1 if previous else 1
    row = {
        "id": stable_id(release_id, str(revision), pipeline_run_id, prefix="crp_"),
        "schema_version": "corpus_release_promotion_v1",
        "promotion_revision": revision,
        "corpus_release_id": release_id,
        "pipeline_run_id": pipeline_run_id,
        "previous_promotion_id": previous["id"] if previous else None,
        "action": "promote",
        "promoted_by": reviewer,
        "rationale": promotion_rationale,
        "created_at": created,
    }
    stored = _insert_immutable(conn, "corpus_release_promotions", row)
    stored["verification"] = verification
    stored["run_authority_decision_id"] = authority["id"]
    return stored


def accept_corpus_release(
    conn: sqlite3.Connection,
    release_id: str,
    *,
    pipeline_run_id: str,
    accepted_by: str,
    rationale: str,
    created_at: str | None = None,
) -> dict[str, Any]:
    """Stable acceptance alias; acceptance is the append-only promotion event."""

    return promote_corpus_release(
        conn,
        release_id,
        pipeline_run_id=pipeline_run_id,
        promoted_by=accepted_by,
        rationale=rationale,
        created_at=created_at,
    )


def create_pipeline_run(
    conn: sqlite3.Connection,
    *,
    run_type: str,
    run_schema: str,
    run_schema_version: str,
    run_id: str | None = None,
    corpus_release_id: str | None = None,
    parent_run_id: str | None = None,
    model: str | None = None,
    model_version: str | None = None,
    prompt_version: str | None = None,
    configuration_sha256: str | None = None,
    status: str = "pending",
    parameters: Mapping[str, Any] | None = None,
    receipt: Mapping[str, Any] | None = None,
    input_count: int = 0,
    output_count: int = 0,
    failure_count: int = 0,
    input_sha256: str | None = None,
    started_at: str | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    """Create a traceable pipeline lifecycle row without performing semantics."""

    created = _iso(created_at or now_iso(), "created_at")
    state = _enum(status, PIPELINE_STATUSES, "status")
    if state not in {"pending", "running"}:
        raise IntelligenceValidationError("new pipeline runs must be pending or running")
    started = _iso(started_at, "started_at") if started_at else (created if state == "running" else None)
    if input_sha256 is not None:
        _sha256(input_sha256, "input_sha256")
    inputs = _integer(input_count, "input_count", minimum=0)
    outputs = _integer(output_count, "output_count", minimum=0)
    failures = _integer(failure_count, "failure_count", minimum=0)
    if outputs + failures > inputs:
        raise IntelligenceValidationError(
            "output_count + failure_count cannot exceed input_count"
        )
    params_json = dumps_json(parameters or {})
    receipt_json = dumps_json(receipt or {})
    config_digest = configuration_sha256 or sha256_text(
        dumps_json(
            {
                "run_type": run_type,
                "run_schema": run_schema,
                "run_schema_version": run_schema_version,
                "model": model,
                "model_version": model_version,
                "prompt_version": prompt_version,
                "parameters": parameters or {},
            }
        )
    )
    _sha256(config_digest, "configuration_sha256")
    row = {
        "id": run_id
        or stable_id(
            _text(run_type, "run_type"),
            _text(run_schema, "run_schema"),
            _text(run_schema_version, "run_schema_version"),
            corpus_release_id or "unreleased",
            created,
            prefix="pir_",
        ),
        "schema_version": "pipeline_run_v1",
        "run_type": _text(run_type, "run_type"),
        "run_schema": _text(run_schema, "run_schema"),
        "run_schema_version": _text(run_schema_version, "run_schema_version"),
        "corpus_release_id": corpus_release_id,
        "parent_run_id": parent_run_id,
        "model": model,
        "model_version": model_version,
        "prompt_version": prompt_version,
        "configuration_sha256": config_digest,
        "status": state,
        "parameters_json": params_json,
        "metrics_json": "{}",
        "receipt_json": receipt_json,
        "input_count": inputs,
        "output_count": outputs,
        "failure_count": failures,
        "input_sha256": input_sha256,
        "output_sha256": None,
        "started_at": started,
        "completed_at": None,
        "error": None,
        "created_at": created,
        "updated_at": created,
    }
    return _insert_immutable(conn, "pipeline_runs", row)


def transition_pipeline_run(
    conn: sqlite3.Connection,
    run_id: str,
    *,
    status: str,
    expected_status: str | None = None,
    metrics: Mapping[str, Any] | None = None,
    receipt: Mapping[str, Any] | None = None,
    input_count: int | None = None,
    output_count: int | None = None,
    failure_count: int | None = None,
    output_sha256: str | None = None,
    error: str | None = None,
    at: str | None = None,
) -> dict[str, Any]:
    """Apply one deterministic, optimistic pipeline state transition."""

    current = _fetch_required(conn, "pipeline_runs", run_id)
    current_state = str(current["status"])
    if expected_status is not None and current_state != expected_status:
        raise IntelligenceValidationError(
            f"pipeline run state conflict: expected {expected_status}, found {current_state}"
        )
    target = _enum(status, PIPELINE_STATUSES, "status")
    if target not in _PIPELINE_TRANSITIONS[current_state]:
        raise IntelligenceValidationError(f"invalid pipeline transition {current_state} -> {target}")
    when = _iso(at or now_iso(), "at")
    if output_sha256 is not None:
        _sha256(output_sha256, "output_sha256")
    metrics_json = dumps_json(metrics or json.loads(current["metrics_json"] or "{}"))
    receipt_json = dumps_json(receipt or json.loads(current["receipt_json"] or "{}"))
    inputs = int(current["input_count"]) if input_count is None else _integer(input_count, "input_count", minimum=0)
    outputs = int(current["output_count"]) if output_count is None else _integer(output_count, "output_count", minimum=0)
    failures = int(current["failure_count"]) if failure_count is None else _integer(failure_count, "failure_count", minimum=0)
    if outputs + failures > inputs:
        raise IntelligenceValidationError("output_count + failure_count cannot exceed input_count")
    started_at = current["started_at"] or (when if target == "running" else None)
    completed_at = when if target in {"succeeded", "failed", "canceled"} else None
    updated = conn.execute(
        """
        UPDATE pipeline_runs
        SET status = ?, metrics_json = ?, receipt_json = ?, input_count = ?, output_count = ?, failure_count = ?,
            output_sha256 = ?, started_at = ?,
            completed_at = ?, error = ?, updated_at = ?
        WHERE id = ? AND status = ?
        """,
        (
            target,
            metrics_json,
            receipt_json,
            inputs,
            outputs,
            failures,
            output_sha256 if output_sha256 is not None else current["output_sha256"],
            started_at,
            completed_at,
            error,
            when,
            run_id,
            current_state,
        ),
    )
    if updated.rowcount != 1:
        raise IntelligenceValidationError("pipeline run changed concurrently")
    return _row_dict(_fetch_required(conn, "pipeline_runs", run_id))


def accept_pipeline_run(
    conn: sqlite3.Connection,
    run_id: str,
    *,
    stage: str,
    reviewed_by: str,
    rationale: str,
    decided_at: str | None = None,
) -> dict[str, Any]:
    """Append explicit authority for one completed production stage run.

    Pipeline success is necessary but deliberately insufficient.  This helper
    snapshots the immutable run contract and reviewer decision; current views
    expose the run only while its latest authority revision remains accepted.
    """

    return _record_pipeline_run_authority_decision(
        conn,
        run_id,
        stage=stage,
        decision="accepted",
        reviewed_by=reviewed_by,
        rationale=rationale,
        decided_at=decided_at,
        allowed_accept_statuses={"succeeded"},
    )


def reject_pipeline_run(
    conn: sqlite3.Connection,
    run_id: str,
    *,
    stage: str,
    reviewed_by: str,
    rationale: str,
    decided_at: str | None = None,
) -> dict[str, Any]:
    """Append a rejection; a later explicit acceptance may supersede it."""

    return _record_pipeline_run_authority_decision(
        conn,
        run_id,
        stage=stage,
        decision="rejected",
        reviewed_by=reviewed_by,
        rationale=rationale,
        decided_at=decided_at,
    )


def supersede_pipeline_run(
    conn: sqlite3.Connection,
    run_id: str,
    *,
    replacement_run_id: str,
    stage: str,
    reviewed_by: str,
    rationale: str,
    decided_at: str | None = None,
) -> dict[str, Any]:
    """Atomically accept a replacement and supersede the prior stage run."""

    old_run = _authorized_stage_run(conn, run_id, stage)
    replacement = _authorized_stage_run(conn, replacement_run_id, stage)
    if old_run["corpus_release_id"] != replacement["corpus_release_id"]:
        raise IntelligenceValidationError(
            "a superseding pipeline run must belong to the same corpus release"
        )
    if replacement["status"] != "succeeded":
        raise IntelligenceValidationError("replacement pipeline run must be succeeded")
    savepoint = "pipeline_run_authority_supersede"
    conn.execute(f"SAVEPOINT {savepoint}")
    try:
        accepted = accept_pipeline_run(
            conn,
            replacement_run_id,
            stage=stage,
            reviewed_by=reviewed_by,
            rationale=f"Replacement accepted: {rationale}",
            decided_at=decided_at,
        )
        superseded = _record_pipeline_run_authority_decision(
            conn,
            run_id,
            stage=stage,
            decision="superseded",
            replacement_pipeline_run_id=replacement_run_id,
            reviewed_by=reviewed_by,
            rationale=rationale,
            decided_at=decided_at,
        )
        conn.execute(f"RELEASE SAVEPOINT {savepoint}")
    except Exception:
        conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
        conn.execute(f"RELEASE SAVEPOINT {savepoint}")
        raise
    return {"superseded": superseded, "replacement": accepted}


def _accept_pipeline_run_at_verified_import_boundary(
    conn: sqlite3.Connection,
    run_id: str,
    *,
    stage: str,
    reviewed_by: str,
    rationale: str,
    decided_at: str | None = None,
) -> dict[str, Any]:
    """Accept a validated import while it is running; views still require success."""

    return _record_pipeline_run_authority_decision(
        conn,
        run_id,
        stage=stage,
        decision="accepted",
        reviewed_by=reviewed_by,
        rationale=rationale,
        decided_at=decided_at,
        allowed_accept_statuses={"running", "succeeded"},
    )


def _record_pipeline_run_authority_decision(
    conn: sqlite3.Connection,
    run_id: str,
    *,
    stage: str,
    decision: str,
    reviewed_by: str,
    rationale: str,
    replacement_pipeline_run_id: str | None = None,
    decided_at: str | None = None,
    allowed_accept_statuses: set[str] | None = None,
) -> dict[str, Any]:
    resolved_stage = _enum(stage, PIPELINE_AUTHORITY_STAGES, "stage")
    resolved_decision = _enum(
        decision, PIPELINE_AUTHORITY_DECISIONS, "decision"
    )
    run = _authorized_stage_run(conn, run_id, resolved_stage)
    if resolved_decision == "accepted" and run["status"] not in (
        allowed_accept_statuses or {"succeeded"}
    ):
        raise IntelligenceValidationError(
            "accepted pipeline authority requires a verified running import or succeeded run"
        )
    if resolved_decision == "superseded":
        replacement_pipeline_run_id = _text(
            replacement_pipeline_run_id, "replacement_pipeline_run_id"
        )
        replacement = _authorized_stage_run(
            conn, replacement_pipeline_run_id, resolved_stage
        )
        if replacement["corpus_release_id"] != run["corpus_release_id"]:
            raise IntelligenceValidationError(
                "replacement pipeline run belongs to a different corpus release"
            )
    elif replacement_pipeline_run_id is not None:
        raise IntelligenceValidationError(
            "replacement_pipeline_run_id is only valid for a superseded decision"
        )

    reviewer = _text(reviewed_by, "reviewed_by")
    reason = _text(rationale, "rationale")
    when = _iso(decided_at or now_iso(), "decided_at")
    lineage = f"prauth_{resolved_stage}_{run_id}"
    latest = conn.execute(
        """
        SELECT * FROM pipeline_run_authority_decisions
        WHERE authority_lineage_id = ?
        ORDER BY revision DESC, decided_at DESC, created_at DESC, id DESC
        LIMIT 1
        """,
        (lineage,),
    ).fetchone()
    if (
        latest is not None
        and latest["decision"] == resolved_decision
        and latest["replacement_pipeline_run_id"] == replacement_pipeline_run_id
        and _authority_snapshot_matches(latest, run)
    ):
        return _row_dict(latest)
    revision = int(latest["revision"]) + 1 if latest is not None else 1
    values = {
        "id": stable_id(
            lineage,
            str(revision),
            resolved_decision,
            replacement_pipeline_run_id or "none",
            prefix="pra_",
        ),
        "schema_version": "pipeline_run_authority_v1",
        "authority_lineage_id": lineage,
        "revision": revision,
        "supersedes_decision_id": latest["id"] if latest is not None else None,
        "stage": resolved_stage,
        "pipeline_run_id": run_id,
        "replacement_pipeline_run_id": replacement_pipeline_run_id,
        "run_type": run["run_type"],
        "run_schema": run["run_schema"],
        "run_schema_version": run["run_schema_version"],
        "configuration_sha256": run["configuration_sha256"],
        "model": run["model"],
        "model_version": run["model_version"],
        "prompt_version": run["prompt_version"],
        "corpus_release_id": run["corpus_release_id"],
        "run_status": run["status"],
        "decision": resolved_decision,
        "reviewed_by": reviewer,
        "rationale": reason,
        "decided_at": when,
        "created_at": when,
    }
    return _insert_immutable(conn, "pipeline_run_authority_decisions", values)


def _authorized_stage_run(
    conn: sqlite3.Connection, run_id: str, stage: str
) -> sqlite3.Row:
    run = _fetch_required(conn, "pipeline_runs", _text(run_id, "run_id"))
    release_id = run["corpus_release_id"]
    if not release_id:
        raise IntelligenceValidationError(
            "canonical pipeline authority requires a release-bound run"
        )
    release = _fetch_required(conn, "corpus_releases", str(release_id))
    if release["status"] != "accepted":
        raise IntelligenceValidationError(
            "canonical pipeline authority requires an accepted immutable release"
        )
    contract = (
        str(run["run_type"]),
        str(run["run_schema"]),
        str(run["run_schema_version"]),
    )
    if contract not in _STAGE_RUN_CONTRACTS[stage]:
        raise IntelligenceValidationError(
            f"pipeline run contract {contract!r} is not authorized for stage {stage}"
        )
    return run


def _authority_snapshot_matches(decision: sqlite3.Row, run: sqlite3.Row) -> bool:
    fields = (
        "run_type",
        "run_schema",
        "run_schema_version",
        "configuration_sha256",
        "model",
        "model_version",
        "prompt_version",
        "corpus_release_id",
    )
    return all(decision[field] == run[field] for field in fields)


def _pipeline_run_reviewer(run: sqlite3.Row, *, fallback: str) -> str:
    try:
        parameters = json.loads(run["parameters_json"] or "{}")
    except (TypeError, json.JSONDecodeError):
        parameters = {}
    if isinstance(parameters, dict):
        reviewer = parameters.get("reviewer")
        if isinstance(reviewer, str) and reviewer.strip():
            return reviewer.strip()
    return fallback


def store_atomic_claim_v1(
    conn: sqlite3.Connection,
    claim: Mapping[str, Any] | None = None,
    *,
    evidence_unit_text: str | None = None,
    **fields: Any,
) -> dict[str, Any]:
    """Validate and insert one immutable atomic-claim revision.

    Exact evidence is checked against the referenced discourse event.  When the
    full evidence unit is available it is also checked literally at the stored
    offsets.  No claim fields are inferred from text.
    """

    data = _payload(claim, fields)
    schema_version = data.get("schema_version", ATOMIC_CLAIM_SCHEMA)
    if schema_version != ATOMIC_CLAIM_SCHEMA:
        raise IntelligenceValidationError("atomic claim schema_version must be atomic_claim_v1")
    corpus_release_id = _text(data.get("corpus_release_id"), "corpus_release_id")
    pipeline_run_id = _text(data.get("pipeline_run_id"), "pipeline_run_id")
    _validate_release_run_binding(
        conn, corpus_release_id, pipeline_run_id, allowed_run_statuses={"running", "succeeded"}
    )

    discourse_event_id = _text(data.get("discourse_event_id"), "discourse_event_id")
    segment_id = _text(data.get("segment_id"), "segment_id")
    event = conn.execute(
        """
        SELECT label_id, segment_id, evidence_text, evidence_start, evidence_end
        FROM discourse_events WHERE id = ?
        """,
        (discourse_event_id,),
    ).fetchone()
    if event is None:
        raise IntelligenceValidationError("referenced discourse_event_id does not exist")
    if event["segment_id"] != segment_id:
        raise IntelligenceValidationError("discourse event does not belong to segment_id")

    evidence_text = _nonempty_evidence(data.get("evidence_text"))
    evidence_start = _integer(data.get("evidence_start"), "evidence_start", minimum=0)
    evidence_end = _integer(data.get("evidence_end"), "evidence_end", minimum=1)
    if evidence_end <= evidence_start:
        raise IntelligenceValidationError("evidence_end must be greater than evidence_start")
    if (
        event["evidence_text"] != evidence_text
        or int(event["evidence_start"]) != evidence_start
        or int(event["evidence_end"]) != evidence_end
    ):
        raise IntelligenceValidationError(
            "atomic claim evidence must exactly match its referenced discourse event"
        )
    if evidence_unit_text is not None:
        validate_exact_evidence(evidence_unit_text, evidence_text, evidence_start, evidence_end)
    else:
        _validate_evidence_from_segment_path(conn, segment_id, evidence_text, evidence_start, evidence_end)

    segment = conn.execute(
        "SELECT source_id, episode_id FROM segments WHERE id = ?", (segment_id,)
    ).fetchone()
    if segment is None:
        raise IntelligenceValidationError("referenced segment_id does not exist")
    source_id = data.get("source_id") or segment["source_id"]
    episode_id = data.get("episode_id") or segment["episode_id"]
    if source_id != segment["source_id"] or episode_id != segment["episode_id"]:
        raise IntelligenceValidationError("claim source/episode provenance does not match segment_id")
    _require_release_member(conn, "corpus_release_episodes", corpus_release_id, "episode_id", episode_id)
    _require_release_member(conn, "corpus_release_segments", corpus_release_id, "segment_id", segment_id)
    _require_release_member(conn, "corpus_release_labels", corpus_release_id, "label_id", event["label_id"])

    claim_text = _text(data.get("claim_text"), "claim_text")
    raw_speaker = _text(data.get("raw_speaker"), "raw_speaker")
    revision = _integer(data.get("revision", 1), "revision", minimum=1)
    lineage_id = data.get("claim_lineage_id") or stable_id(
        discourse_event_id, raw_speaker, prefix="aclin_"
    )
    created = _iso(data.get("created_at") or now_iso(), "created_at")
    observed = data.get("observed_at")
    if not observed:
        episode = conn.execute("SELECT published_at FROM episodes WHERE id = ?", (episode_id,)).fetchone()
        observed = episode["published_at"] if episode and episode["published_at"] else created
    observed = _iso(observed, "observed_at")
    review_status = _enum(data.get("review_status"), REVIEW_STATUSES, "review_status")
    reviewed_at = data.get("reviewed_at")
    if reviewed_at:
        reviewed_at = _iso(reviewed_at, "reviewed_at")
    forecast_probability = data.get("forecast_probability")
    if forecast_probability is not None:
        forecast_probability = _probability(forecast_probability, "forecast_probability")
    for sha_field in ("extractor_prompt_sha256", "source_artifact_sha256"):
        if data.get(sha_field) is not None:
            _sha256(data[sha_field], sha_field)

    supersedes = data.get("supersedes_claim_id")
    claim_record_id = data.get("id") or stable_id(
        lineage_id, str(revision), claim_text, evidence_text, prefix="ac_"
    )
    existing_claim = conn.execute(
        "SELECT id FROM atomic_claims WHERE id = ?", (claim_record_id,)
    ).fetchone()
    if existing_claim is None:
        _validate_revision_link(
            conn,
            table="atomic_claims",
            lineage_column="claim_lineage_id",
            lineage_id=lineage_id,
            revision=revision,
            supersedes_id=supersedes,
        )
    row = {
        "id": claim_record_id,
        "schema_version": ATOMIC_CLAIM_SCHEMA,
        "claim_lineage_id": lineage_id,
        "revision": revision,
        "supersedes_claim_id": supersedes,
        "corpus_release_id": corpus_release_id,
        "pipeline_run_id": pipeline_run_id,
        "claim_text": claim_text,
        "claim_type": _text(data.get("claim_type"), "claim_type"),
        "raw_speaker": raw_speaker,
        "canonical_person_id": data.get("canonical_person_id"),
        "stance": _text(data.get("stance"), "stance"),
        "certainty": _text(data.get("certainty"), "certainty"),
        "time_horizon": _text(data.get("time_horizon"), "time_horizon"),
        "discourse_event_id": discourse_event_id,
        "segment_id": segment_id,
        "source_id": source_id,
        "episode_id": episode_id,
        "evidence_unit_type": _text(data.get("evidence_unit_type", "segment"), "evidence_unit_type"),
        "evidence_unit_id": _text(data.get("evidence_unit_id", segment_id), "evidence_unit_id"),
        "evidence_text": evidence_text,
        "evidence_start": evidence_start,
        "evidence_end": evidence_end,
        "extractor_model": _text(data.get("extractor_model"), "extractor_model"),
        "extractor_schema": _text(data.get("extractor_schema"), "extractor_schema"),
        "extractor_schema_version": _text(
            data.get("extractor_schema_version"), "extractor_schema_version"
        ),
        "extractor_prompt_sha256": data.get("extractor_prompt_sha256"),
        "source_artifact_sha256": data.get("source_artifact_sha256"),
        "provenance_json": dumps_json(_json_object(data.get("provenance", data.get("provenance_json", {})), "provenance")),
        "confidence": _probability(data.get("confidence"), "confidence"),
        "review_status": review_status,
        "reviewed_by_model": data.get("reviewed_by_model"),
        "reviewed_at": reviewed_at,
        "forecast_probability": forecast_probability,
        "observed_at": observed,
        "created_at": created,
    }
    return _insert_immutable(conn, "atomic_claims", row)


def build_atomic_claims_from_release(
    conn: sqlite3.Connection,
    release_id: str,
    *,
    pipeline_run_id: str,
    claims: Iterable[Mapping[str, Any]] | None = None,
    evidence_units: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Store supplied accepted atomic decisions against one frozen release.

    This is intentionally an import/validation service, not an extractor.  It
    will never manufacture atomic claims from labels or transcript text.
    """

    verification = verify_corpus_release(conn, release_id)
    if not verification["ok"]:
        raise IntelligenceValidationError(
            "cannot build atomic claims from an invalid release: "
            + "; ".join(verification["errors"])
        )
    _validate_release_run_binding(
        conn, release_id, pipeline_run_id, allowed_run_statuses={"running"}
    )
    projected = claims is None
    quarantined: list[dict[str, Any]] = []
    if projected:
        payloads, quarantined = _project_atomic_claims_from_release(
            conn, release_id=release_id, pipeline_run_id=pipeline_run_id
        )
    else:
        payloads = [dict(item) for item in claims or []]
    units = evidence_units or {}
    claim_ids: list[str] = []
    status_counts: Counter[str] = Counter()
    for payload in payloads:
        if payload.get("review_status") not in {"accepted", "pending", "needs_review"}:
            raise IntelligenceValidationError(
                "atomic claim imports require accepted, pending, or needs_review status"
            )
        payload["corpus_release_id"] = release_id
        payload["pipeline_run_id"] = pipeline_run_id
        unit_id = str(payload.get("evidence_unit_id") or payload.get("segment_id") or "")
        stored = store_atomic_claim_v1(
            conn,
            payload,
            evidence_unit_text=units.get(unit_id),
        )
        claim_ids.append(stored["id"])
        status_counts[str(stored["review_status"])] += 1
    run = _fetch_required(conn, "pipeline_runs", pipeline_run_id)
    authority = _accept_pipeline_run_at_verified_import_boundary(
        conn,
        pipeline_run_id,
        stage="atomic_claims",
        reviewed_by=_pipeline_run_reviewer(
            run, fallback="atomic_claim_exact_evidence_validator_v1"
        ),
        rationale=(
            "The bounded atomic_claim_v1 import passed release membership, exact "
            "evidence-offset, schema, and provenance validation."
        ),
    )
    return {
        "ok": True,
        "release_id": release_id,
        "pipeline_run_id": pipeline_run_id,
        "inserted_or_existing": len(claim_ids),
        "accepted": status_counts["accepted"],
        "needs_review": status_counts["needs_review"],
        "pending": status_counts["pending"],
        "quarantined": len(quarantined),
        "quarantine": quarantined,
        "claim_ids": claim_ids,
        "mechanical_projection": projected,
        "semantic_inference_performed": False,
        "run_authority_decision_id": authority["id"],
    }


def record_claim_subject(
    conn: sqlite3.Connection,
    subject: Mapping[str, Any] | None = None,
    **fields: Any,
) -> dict[str, Any]:
    """Append one LLM-owned, release-scoped claim-subject judgment."""

    data = _payload(subject, fields)
    release_id, run_id = _decision_context(conn, data)
    subject_text = _text(data.get("subject_text"), "subject_text")
    lineage = data.get("subject_lineage_id") or stable_id(subject_text, prefix="subjlin_")
    revision, supersedes = _next_revision(
        conn,
        "accepted_claim_subjects",
        "subject_lineage_id",
        lineage,
        data.get("revision"),
        data.get("supersedes_subject_id"),
    )
    decided = _iso(data.get("decided_at") or now_iso(), "decided_at")
    row = {
        "id": data.get("id") or stable_id(lineage, str(revision), subject_text, prefix="subj_"),
        "schema_version": "claim_subject_judgment_v1",
        "subject_lineage_id": lineage,
        "revision": revision,
        "supersedes_subject_id": supersedes,
        "corpus_release_id": release_id,
        "pipeline_run_id": run_id,
        "subject_text": subject_text,
        "subject_type": _text(data.get("subject_type"), "subject_type"),
        "domain": data.get("domain"),
        "scope_note": data.get("scope_note"),
        "judge_model": _text(data.get("judge_model"), "judge_model"),
        "judge_schema_version": _text(data.get("judge_schema_version"), "judge_schema_version"),
        "confidence": _probability(data.get("confidence"), "confidence"),
        "rationale": _text(data.get("rationale"), "rationale"),
        "evidence_json": dumps_json(_json_object(data.get("evidence", data.get("evidence_json", {})), "evidence")),
        "review_status": _enum(data.get("review_status", "accepted"), REVIEW_STATUSES, "review_status"),
        "decided_at": decided,
        "created_at": _iso(data.get("created_at") or decided, "created_at"),
    }
    return _insert_immutable(conn, "accepted_claim_subjects", row)


def record_canonical_person_decision(
    conn: sqlite3.Connection,
    decision_record: Mapping[str, Any] | None = None,
    **fields: Any,
) -> dict[str, Any]:
    """Create or advance a canonical person only from an explicit LLM decision.

    The legacy ``canonical_people`` table remains the identity registry, but an
    accepted row is deliberately insufficient for public/current use.  The
    ``current_accepted_people`` view also requires a separate accepted identity
    resolution judgment from the promoted release and a succeeded pipeline run.
    """

    data = _payload(decision_record, fields)
    release_id, run_id = _decision_context(conn, data)
    person_id = _text(data.get("person_id", data.get("id")), "person_id")
    display_name = _text(data.get("display_name"), "display_name")
    normalized_name = _text(data.get("normalized_name"), "normalized_name")
    decision = _enum(
        data.get("decision"), {"candidate", "accepted", "rejected", "merged"}, "decision"
    )
    confidence = _probability(data.get("confidence"), "confidence")
    judge_model = _text(data.get("judge_model"), "judge_model")
    judge_schema_version = _text(
        data.get("judge_schema_version"), "judge_schema_version"
    )
    rationale = _text(data.get("rationale"), "rationale")
    decided_at = _iso(data.get("decided_at") or now_iso(), "decided_at")
    primary_org_id = data.get("primary_org_id")
    if primary_org_id is not None:
        primary_org_id = _text(primary_org_id, "primary_org_id")

    merged_into_person_id = data.get("merged_into_person_id")
    if decision == "merged":
        merged_into_person_id = _text(merged_into_person_id, "merged_into_person_id")
        if merged_into_person_id == person_id:
            raise IntelligenceValidationError("a canonical person cannot be merged into itself")
        _require_canonical_person_record(conn, merged_into_person_id)
    elif merged_into_person_id is not None:
        raise IntelligenceValidationError(
            "merged_into_person_id is only valid for a merged decision"
        )

    explicit_evidence = _json_object(
        data.get("evidence", data.get("evidence_json", {})), "evidence"
    )
    provenance = {
        "schema_version": "canonical_person_decision_v1",
        "corpus_release_id": release_id,
        "pipeline_run_id": run_id,
        "decision": decision,
        "judge_model": judge_model,
        "judge_schema_version": judge_schema_version,
        "confidence": confidence,
        "rationale": rationale,
        "evidence": explicit_evidence,
        "merged_into_person_id": merged_into_person_id,
        "decided_at": decided_at,
    }

    existing = conn.execute(
        "SELECT * FROM canonical_people WHERE id = ?", (person_id,)
    ).fetchone()
    conflicting_name = conn.execute(
        "SELECT id FROM canonical_people WHERE normalized_name = ? AND id <> ?",
        (normalized_name, person_id),
    ).fetchone()
    if conflicting_name is not None:
        raise IntelligenceValidationError(
            "normalized_name already belongs to a different canonical person"
        )

    if existing is None:
        conn.execute(
            """
            INSERT INTO canonical_people
              (id, display_name, normalized_name, primary_org_id, confidence, status,
               canonical_version, evidence_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
            """,
            (
                person_id,
                display_name,
                normalized_name,
                primary_org_id,
                confidence,
                decision,
                dumps_json({"llm_identity_decision": provenance}),
                decided_at,
                decided_at,
            ),
        )
        return _row_dict(_fetch_required(conn, "canonical_people", person_id))

    previous_status = str(existing["status"])
    allowed_transitions = {
        "candidate": {"candidate", "accepted", "rejected", "merged"},
        "accepted": {"accepted"},
        "rejected": {"rejected"},
        "merged": {"merged"},
    }
    if decision not in allowed_transitions.get(previous_status, set()):
        raise IntelligenceValidationError(
            f"invalid canonical person transition {previous_status} -> {decision}"
        )
    try:
        stored_evidence = json.loads(existing["evidence_json"] or "{}")
    except (TypeError, json.JSONDecodeError):
        stored_evidence = {"legacy_evidence_json": existing["evidence_json"]}
    if not isinstance(stored_evidence, dict):
        stored_evidence = {"legacy_evidence": stored_evidence}
    if (
        existing["display_name"] == display_name
        and existing["normalized_name"] == normalized_name
        and existing["primary_org_id"] == primary_org_id
        and math.isclose(float(existing["confidence"]), confidence, rel_tol=0, abs_tol=1e-12)
        and previous_status == decision
        and stored_evidence.get("llm_identity_decision") == provenance
    ):
        return _row_dict(existing)

    stored_evidence["llm_identity_decision"] = provenance
    conn.execute(
        """
        UPDATE canonical_people
        SET display_name = ?, normalized_name = ?, primary_org_id = ?, confidence = ?,
            status = ?, canonical_version = canonical_version + 1,
            evidence_json = ?, updated_at = ?
        WHERE id = ?
        """,
        (
            display_name,
            normalized_name,
            primary_org_id,
            confidence,
            decision,
            dumps_json(stored_evidence),
            decided_at,
            person_id,
        ),
    )
    return _row_dict(_fetch_required(conn, "canonical_people", person_id))


def record_identity_resolution_judgment(
    conn: sqlite3.Connection,
    judgment: Mapping[str, Any] | None = None,
    **fields: Any,
) -> dict[str, Any]:
    """Append one LLM-owned mapping from raw evidence to a canonical person."""

    data = _payload(judgment, fields)
    release_id, run_id = _decision_context(conn, data)
    mention_type = _enum(
        data.get("raw_mention_type"),
        {"speaker", "actor", "guest_metadata", "episode_metadata"},
        "raw_mention_type",
    )
    mention_id = _text(data.get("raw_mention_id"), "raw_mention_id")
    decision = _enum(
        data.get("decision"),
        {"candidate", "accepted", "rejected", "merged", "unknown"},
        "decision",
    )
    person_id = data.get("canonical_person_id")
    if decision in {"accepted", "merged"}:
        person_id = _text(person_id, "canonical_person_id")
        _require_canonical_person_record(conn, person_id)
    elif decision == "candidate":
        person_id = _text(person_id, "canonical_person_id")
        candidate = conn.execute(
            "SELECT 1 FROM canonical_people WHERE id = ? AND status IN ('candidate', 'accepted')",
            (person_id,),
        ).fetchone()
        if candidate is None:
            raise IntelligenceValidationError(
                "candidate identity judgment requires a candidate canonical person"
            )
    if mention_type == "speaker":
        exists = conn.execute(
            "SELECT 1 FROM raw_speaker_mentions WHERE id = ?", (mention_id,)
        ).fetchone()
    elif mention_type == "actor":
        exists = conn.execute(
            "SELECT 1 FROM raw_actor_mentions WHERE id = ?", (mention_id,)
        ).fetchone()
    else:
        exists = conn.execute("SELECT 1 FROM episodes WHERE id = ?", (mention_id,)).fetchone()
        if exists is not None:
            _require_release_member(
                conn, "corpus_release_episodes", release_id, "episode_id", mention_id
            )
    if exists is None:
        raise IntelligenceValidationError("raw identity evidence does not exist")
    lineage = data.get("identity_lineage_id") or stable_id(
        mention_type, mention_id, prefix="idjlin_"
    )
    revision, supersedes = _next_revision(
        conn,
        "identity_resolution_judgments",
        "identity_lineage_id",
        lineage,
        data.get("revision"),
        data.get("supersedes_judgment_id"),
    )
    decided = _iso(data.get("decided_at") or now_iso(), "decided_at")
    row = {
        "id": data.get("id") or stable_id(lineage, str(revision), decision, prefix="idj_"),
        "schema_version": "identity_resolution_judgment_v1",
        "identity_lineage_id": lineage,
        "revision": revision,
        "supersedes_judgment_id": supersedes,
        "corpus_release_id": release_id,
        "pipeline_run_id": run_id,
        "raw_mention_type": mention_type,
        "raw_mention_id": mention_id,
        "canonical_person_id": person_id,
        "decision": decision,
        "rationale": _text(data.get("rationale"), "rationale"),
        "evidence_json": dumps_json(_json_object(data.get("evidence", data.get("evidence_json", {})), "evidence")),
        "judge_model": _text(data.get("judge_model"), "judge_model"),
        "judge_schema_version": _text(data.get("judge_schema_version"), "judge_schema_version"),
        "confidence": _probability(data.get("confidence"), "confidence"),
        "review_status": _enum(data.get("review_status", "accepted"), REVIEW_STATUSES, "review_status"),
        "decided_at": decided,
        "created_at": _iso(data.get("created_at") or decided, "created_at"),
    }
    return _insert_immutable(conn, "identity_resolution_judgments", row)


def record_proposition_variant(
    conn: sqlite3.Connection,
    variant: Mapping[str, Any] | None = None,
    **fields: Any,
) -> dict[str, Any]:
    """Append one accepted proposition interpretation under a judged subject."""

    data = _payload(variant, fields)
    release_id, run_id = _decision_context(conn, data)
    subject_id = _text(data.get("subject_id"), "subject_id")
    _require_accepted_parent(
        conn, "accepted_claim_subjects", subject_id, release_id, "claim subject"
    )
    text = _text(data.get("proposition_text"), "proposition_text")
    lineage = data.get("variant_lineage_id") or stable_id(subject_id, text, prefix="varlin_")
    revision, supersedes = _next_revision(
        conn,
        "accepted_proposition_variants",
        "variant_lineage_id",
        lineage,
        data.get("revision"),
        data.get("supersedes_variant_id"),
    )
    decided = _iso(data.get("decided_at") or now_iso(), "decided_at")
    row = {
        "id": data.get("id") or stable_id(lineage, str(revision), text, prefix="var_"),
        "schema_version": "proposition_variant_judgment_v1",
        "variant_lineage_id": lineage,
        "revision": revision,
        "supersedes_variant_id": supersedes,
        "corpus_release_id": release_id,
        "pipeline_run_id": run_id,
        "subject_id": subject_id,
        "proposition_text": text,
        "predicate_text": data.get("predicate_text"),
        "object_text": data.get("object_text"),
        "polarity": data.get("polarity"),
        "time_horizon": data.get("time_horizon"),
        "conditions_json": dumps_json(_json_object(data.get("conditions", data.get("conditions_json", {})), "conditions")),
        "judge_model": _text(data.get("judge_model"), "judge_model"),
        "judge_schema_version": _text(data.get("judge_schema_version"), "judge_schema_version"),
        "confidence": _probability(data.get("confidence"), "confidence"),
        "rationale": _text(data.get("rationale"), "rationale"),
        "evidence_json": dumps_json(_json_object(data.get("evidence", data.get("evidence_json", {})), "evidence")),
        "review_status": _enum(data.get("review_status", "accepted"), REVIEW_STATUSES, "review_status"),
        "decided_at": decided,
        "created_at": _iso(data.get("created_at") or decided, "created_at"),
    }
    return _insert_immutable(conn, "accepted_proposition_variants", row)


def record_position_observation(
    conn: sqlite3.Connection,
    observation: Mapping[str, Any] | None = None,
    **fields: Any,
) -> dict[str, Any]:
    """Append one person/variant position accepted by an LLM judge."""

    data = _payload(observation, fields)
    release_id, run_id = _decision_context(conn, data)
    subject_id = _text(data.get("subject_id"), "subject_id")
    variant_id = _text(data.get("variant_id"), "variant_id")
    claim_id = _text(data.get("atomic_claim_id"), "atomic_claim_id")
    person_id = _text(data.get("canonical_person_id"), "canonical_person_id")
    _require_accepted_parent(conn, "accepted_claim_subjects", subject_id, release_id, "claim subject")
    variant_row = _require_accepted_parent(
        conn, "accepted_proposition_variants", variant_id, release_id, "proposition variant"
    )
    if variant_row["subject_id"] != subject_id:
        raise IntelligenceValidationError("variant does not belong to subject_id")
    claim_row = _require_accepted_parent(conn, "atomic_claims", claim_id, release_id, "atomic claim")
    _require_accepted_person(conn, person_id, release_id=release_id)
    lineage = data.get("position_lineage_id") or stable_id(
        variant_id, claim_row["claim_lineage_id"], person_id, prefix="poslin_"
    )
    revision, supersedes = _next_revision(
        conn,
        "accepted_position_observations",
        "position_lineage_id",
        lineage,
        data.get("revision"),
        data.get("supersedes_position_id"),
    )
    decided = _iso(data.get("decided_at") or now_iso(), "decided_at")
    row = {
        "id": data.get("id") or stable_id(lineage, str(revision), claim_id, prefix="pos_"),
        "schema_version": "position_observation_judgment_v1",
        "position_lineage_id": lineage,
        "revision": revision,
        "supersedes_position_id": supersedes,
        "corpus_release_id": release_id,
        "pipeline_run_id": run_id,
        "subject_id": subject_id,
        "variant_id": variant_id,
        "atomic_claim_id": claim_id,
        "canonical_person_id": person_id,
        "position": _text(data.get("position"), "position"),
        "certainty": _text(data.get("certainty"), "certainty"),
        "observed_at": _iso(data.get("observed_at") or claim_row["observed_at"], "observed_at"),
        "judge_model": _text(data.get("judge_model"), "judge_model"),
        "judge_schema_version": _text(data.get("judge_schema_version"), "judge_schema_version"),
        "confidence": _probability(data.get("confidence"), "confidence"),
        "rationale": _text(data.get("rationale"), "rationale"),
        "evidence_json": dumps_json(_json_object(data.get("evidence", data.get("evidence_json", {})), "evidence")),
        "review_status": _enum(data.get("review_status", "accepted"), REVIEW_STATUSES, "review_status"),
        "decided_at": decided,
        "created_at": _iso(data.get("created_at") or decided, "created_at"),
    }
    return _insert_immutable(conn, "accepted_position_observations", row)


def record_source_affiliation(
    conn: sqlite3.Connection,
    affiliation: Mapping[str, Any] | None = None,
    **fields: Any,
) -> dict[str, Any]:
    data = _payload(affiliation, fields)
    release_id, run_id = _decision_context(conn, data)
    source_id = _text(data.get("source_id"), "source_id")
    kind = _enum(
        data.get("affiliation_kind"),
        {"network", "publisher", "owner", "independent"},
        "affiliation_kind",
    )
    source_member = conn.execute(
        """
        SELECT 1
        FROM corpus_release_episodes AS members
        JOIN episodes ON episodes.id = members.episode_id
        WHERE members.corpus_release_id = ? AND episodes.source_id = ?
        LIMIT 1
        """,
        (release_id, source_id),
    ).fetchone()
    if source_member is None:
        raise IntelligenceValidationError("source_id is not represented in the corpus release")
    key = _text(data.get("affiliation_key"), "affiliation_key")
    lineage = data.get("affiliation_lineage_id") or stable_id(source_id, kind, key, prefix="saflin_")
    revision, supersedes = _next_revision(
        conn,
        "source_affiliations",
        "affiliation_lineage_id",
        lineage,
        data.get("revision"),
        data.get("supersedes_affiliation_id"),
    )
    created = _iso(data.get("created_at") or now_iso(), "created_at")
    valid_from = _iso(data["valid_from"], "valid_from") if data.get("valid_from") else None
    valid_to = _iso(data["valid_to"], "valid_to") if data.get("valid_to") else None
    if valid_from and valid_to and valid_to < valid_from:
        raise IntelligenceValidationError("valid_to cannot precede valid_from")
    row = {
        "id": data.get("id") or stable_id(lineage, str(revision), prefix="saf_"),
        "schema_version": "source_affiliation_v1",
        "affiliation_lineage_id": lineage,
        "revision": revision,
        "supersedes_affiliation_id": supersedes,
        "corpus_release_id": release_id,
        "source_id": source_id,
        "affiliation_kind": kind,
        "affiliation_key": key,
        "affiliation_name": _text(data.get("affiliation_name", key), "affiliation_name"),
        "canonical_org_id": data.get("canonical_org_id"),
        "valid_from": valid_from,
        "valid_to": valid_to,
        "confidence": _probability(data.get("confidence"), "confidence"),
        "review_status": _enum(data.get("review_status", "accepted"), REVIEW_STATUSES, "review_status"),
        "judge_model": _text(data.get("judge_model"), "judge_model"),
        "pipeline_run_id": run_id,
        "evidence_json": dumps_json(_json_object(data.get("evidence", data.get("evidence_json", {})), "evidence")),
        "created_at": created,
    }
    return _insert_immutable(conn, "source_affiliations", row)


def record_person_appearance(
    conn: sqlite3.Connection,
    appearance: Mapping[str, Any] | None = None,
    **fields: Any,
) -> dict[str, Any]:
    data = _payload(appearance, fields)
    release_id, run_id = _decision_context(conn, data)
    person_id = _text(data.get("canonical_person_id"), "canonical_person_id")
    episode_id = _text(data.get("episode_id"), "episode_id")
    role = _enum(data.get("role"), {"host", "cohost", "guest", "panelist", "unknown"}, "role")
    lineage = data.get("appearance_lineage_id") or stable_id(
        person_id, episode_id, role, prefix="applin_"
    )
    revision, supersedes = _next_revision(
        conn,
        "person_appearances",
        "appearance_lineage_id",
        lineage,
        data.get("revision"),
        data.get("supersedes_appearance_id"),
    )
    episode = conn.execute("SELECT source_id, published_at FROM episodes WHERE id = ?", (episode_id,)).fetchone()
    if episode is None:
        raise IntelligenceValidationError("referenced episode_id does not exist")
    source_id = data.get("source_id") or episode["source_id"]
    if source_id != episode["source_id"]:
        raise IntelligenceValidationError("appearance source_id does not match episode_id")
    _require_release_member(conn, "corpus_release_episodes", release_id, "episode_id", episode_id)
    review_status = _enum(data.get("review_status", "accepted"), REVIEW_STATUSES, "review_status")
    if review_status == "accepted":
        _require_accepted_person(conn, person_id, release_id=release_id)
    source_affiliation_id = data.get("source_affiliation_id")
    if source_affiliation_id is not None:
        affiliation_row = _require_accepted_parent(
            conn, "source_affiliations", source_affiliation_id, release_id, "source affiliation"
        )
        if affiliation_row["source_id"] != source_id:
            raise IntelligenceValidationError("source affiliation does not match appearance source")
    created = _iso(data.get("created_at") or now_iso(), "created_at")
    appeared_at = _iso(data.get("appeared_at") or episode["published_at"] or created, "appeared_at")
    row = {
        "id": data.get("id") or stable_id(lineage, str(revision), prefix="app_"),
        "schema_version": "person_appearance_v1",
        "appearance_lineage_id": lineage,
        "revision": revision,
        "supersedes_appearance_id": supersedes,
        "corpus_release_id": release_id,
        "canonical_person_id": person_id,
        "source_id": source_id,
        "episode_id": episode_id,
        "source_affiliation_id": source_affiliation_id,
        "role": role,
        "appeared_at": appeared_at,
        "confidence": _probability(data.get("confidence"), "confidence"),
        "review_status": review_status,
        "judge_model": _text(data.get("judge_model"), "judge_model"),
        "pipeline_run_id": run_id,
        "evidence_json": dumps_json(_json_object(data.get("evidence", data.get("evidence_json", {})), "evidence")),
        "created_at": created,
    }
    return _insert_immutable(conn, "person_appearances", row)


def record_claim_relation_judgment(
    conn: sqlite3.Connection,
    judgment: Mapping[str, Any] | None = None,
    **fields: Any,
) -> dict[str, Any]:
    """Store an LLM-owned claim relation without deriving it from claim text."""

    data = _payload(judgment, fields)
    release_id, run_id = _decision_context(conn, data)
    source_claim_id = _text(data.get("source_claim_id"), "source_claim_id")
    target_claim_id = _text(data.get("target_claim_id"), "target_claim_id")
    if source_claim_id == target_claim_id:
        raise IntelligenceValidationError("a claim cannot be related to itself")
    _require_accepted_parent(conn, "atomic_claims", source_claim_id, release_id, "source claim")
    _require_accepted_parent(conn, "atomic_claims", target_claim_id, release_id, "target claim")
    relation = _enum(data.get("relation"), RELATIONS, "relation")
    # Temporal applicability is a semantic judgment supplied by the LLM.  V1
    # deliberately stores the judge's non-empty wording verbatim; deterministic
    # code must not invent a scope or interpret it as an applicability rule.
    temporal_scope = _text(data.get("temporal_scope"), "temporal_scope")
    lineage = data.get("relation_lineage_id") or stable_id(
        source_claim_id, target_claim_id, prefix="crjlin_"
    )
    revision, supersedes = _next_revision(
        conn,
        "claim_relation_judgments",
        "relation_lineage_id",
        lineage,
        data.get("revision"),
        data.get("supersedes_judgment_id"),
    )
    decided = _iso(data.get("decided_at") or now_iso(), "decided_at")
    created = _iso(data.get("created_at") or decided, "created_at")
    row = {
        "id": data.get("id") or stable_id(lineage, str(revision), relation, prefix="crj_"),
        "schema_version": "claim_relation_judgment_v1",
        "relation_lineage_id": lineage,
        "revision": revision,
        "supersedes_judgment_id": supersedes,
        "corpus_release_id": release_id,
        "source_claim_id": source_claim_id,
        "target_claim_id": target_claim_id,
        "relation": relation,
        "temporal_scope": temporal_scope,
        "confidence": _probability(data.get("confidence"), "confidence"),
        "rationale": _text(data.get("rationale"), "rationale"),
        "evidence_json": dumps_json(_json_object(data.get("evidence", data.get("evidence_json", {})), "evidence")),
        "judge_model": _text(data.get("judge_model"), "judge_model"),
        "pipeline_run_id": run_id,
        "review_status": _enum(data.get("review_status", "accepted"), REVIEW_STATUSES, "review_status"),
        "decided_at": decided,
        "created_at": created,
    }
    return _insert_immutable(conn, "claim_relation_judgments", row)


def _accepted_affiliation_keys(
    conn: sqlite3.Connection,
    *,
    source_id: str,
    observed_at: str,
) -> list[str]:
    """Return accepted organization coverage keys active at an observation.

    The four approved affiliation kinds all contribute to product-level
    network coverage.  Kind remains part of the key so an owner and publisher
    with the same external identifier are two explicit accepted affiliations,
    not a deterministic equivalence judgment.
    """

    rows = conn.execute(
        """
        SELECT DISTINCT affiliation_kind || ':' || affiliation_key AS coverage_key
        FROM current_accepted_source_affiliations
        WHERE source_id = ?
          AND affiliation_kind IN ('network', 'publisher', 'owner', 'independent')
          AND (valid_from IS NULL OR datetime(valid_from) <= datetime(?))
          AND (valid_to IS NULL OR datetime(valid_to) >= datetime(?))
        ORDER BY coverage_key
        """,
        (
            _text(source_id, "source_id"),
            _iso(observed_at, "observed_at"),
            _iso(observed_at, "observed_at"),
        ),
    ).fetchall()
    return [str(row["coverage_key"]) for row in rows]


def latest_accepted_positions(
    conn: sqlite3.Connection,
    *,
    focal_claim_id: str,
    as_of: str | None = None,
    window_days: int = DEFAULT_WINDOW_DAYS,
    exclude_claim_id: str | None = None,
) -> list[dict[str, Any]]:
    """Return one latest accepted, judged position per exact person key.

    The relation must be an accepted judgment directed from the observation to
    ``focal_claim_id``.  Unresolved speakers are excluded: every vote must use
    an accepted canonical person, never a raw-speaker fallback.
    """

    _require_current_accepted_claim(conn, focal_claim_id)
    end, start = _window(as_of, window_days)
    rows = conn.execute(
        """
        WITH target_variant AS (
          SELECT variant_id
          FROM current_accepted_position_observations
          WHERE atomic_claim_id = ?
          ORDER BY datetime(observed_at) DESC, decided_at DESC, id DESC
          LIMIT 1
        ), ranked_positions AS (
          SELECT
            c.id AS claim_id,
            c.claim_lineage_id,
            positions.canonical_person_id,
            'person:' || positions.canonical_person_id AS person_key,
            c.raw_speaker,
            c.source_id,
            c.episode_id,
            positions.observed_at,
            c.created_at AS claim_created_at,
            ROW_NUMBER() OVER (
              PARTITION BY positions.canonical_person_id
              ORDER BY datetime(positions.observed_at) DESC, positions.decided_at DESC,
                       c.created_at DESC, c.id DESC
            ) AS person_position_rank
          FROM current_accepted_atomic_claims AS c
          JOIN current_accepted_position_observations AS positions
            ON positions.atomic_claim_id = c.id
          JOIN target_variant
            ON target_variant.variant_id = positions.variant_id
          JOIN current_accepted_people AS person
            ON person.id = positions.canonical_person_id
          WHERE datetime(c.observed_at) >= datetime(?)
            AND datetime(c.observed_at) <= datetime(?)
            AND (? IS NULL OR c.id <> ?)
        ), latest_positions AS (
          SELECT * FROM ranked_positions WHERE person_position_rank = 1
        )
        SELECT latest_positions.*,
               r.id AS relation_judgment_id,
               r.relation,
               r.temporal_scope,
               r.confidence AS relation_confidence,
               r.decided_at
        FROM latest_positions
        JOIN current_accepted_claim_relations AS r
          ON r.source_claim_id = latest_positions.claim_id
         AND r.target_claim_id = ?
        WHERE datetime(r.decided_at) <= datetime(?)
        ORDER BY datetime(latest_positions.observed_at) DESC, latest_positions.claim_id
        """,
        (
            focal_claim_id,
            start,
            end,
            exclude_claim_id,
            exclude_claim_id,
            focal_claim_id,
            end,
        ),
    ).fetchall()
    result: list[dict[str, Any]] = []
    for row in rows:
        position = _row_dict(row)
        position["network_keys"] = _accepted_affiliation_keys(
            conn,
            source_id=position["source_id"],
            observed_at=position["observed_at"],
        )
        result.append(position)
    return result


def compute_consensus_snapshot(
    conn: sqlite3.Connection,
    *,
    focal_claim_id: str,
    as_of: str | None = None,
    window_days: int = DEFAULT_WINDOW_DAYS,
    exclude_claim_id: str | None = None,
) -> dict[str, Any]:
    """Calculate consensus from current accepted claims and relation judgments."""

    end, start = _window(as_of, window_days)
    positions = latest_accepted_positions(
        conn,
        focal_claim_id=focal_claim_id,
        as_of=end,
        window_days=window_days,
        exclude_claim_id=exclude_claim_id,
    )
    included: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    buckets: Counter[str] = Counter()
    scores: list[float] = []
    for position in positions:
        score = relation_alignment_score(position["relation"])
        bucket = relation_bucket(position["relation"])
        if score is None or bucket is None:
            excluded.append(position)
            continue
        enriched = dict(position)
        enriched["alignment_score"] = score
        enriched["bucket"] = bucket
        included.append(enriched)
        scores.append(score)
        buckets[bucket] += 1

    dominant_bucket, dominant_share = _dominant_bucket(buckets, len(included))
    input_descriptor = {
        "focal_claim_id": focal_claim_id,
        "as_of": end,
        "window_days": window_days,
        "exclude_claim_id": exclude_claim_id,
        "accepted_inputs": [
            [
                row["claim_id"],
                row["relation_judgment_id"],
                row["relation"],
                row["temporal_scope"],
            ]
            for row in included
        ],
        "excluded_inputs": [
            [
                row["claim_id"],
                row["relation_judgment_id"],
                row["relation"],
                row["temporal_scope"],
            ]
            for row in excluded
        ],
    }
    return {
        "schema_version": "consensus_snapshot_v1",
        "calculation_version": "accepted_relations_v1",
        "focal_claim_id": focal_claim_id,
        "as_of": end,
        "window_start": start,
        "window_end": end,
        "window_days": _integer(window_days, "window_days", minimum=1),
        "people_count": len({row["person_key"] for row in included}),
        "show_count": len({row["source_id"] for row in included if row["source_id"]}),
        "network_count": len(
            {
                affiliation_key
                for row in included
                for affiliation_key in row["network_keys"]
            }
        ),
        "aligned_count": buckets["aligned"],
        "qualified_count": buckets["qualified"],
        "opposed_count": buckets["opposed"],
        "excluded_count": len(excluded),
        "consensus_score": (sum(scores) / len(scores)) if scores else None,
        "dominant_bucket": dominant_bucket,
        "dominant_share": dominant_share,
        "inputs_sha256": sha256_text(dumps_json(input_descriptor)),
        "included_claim_ids": [row["claim_id"] for row in included],
        "positions": included,
        "excluded_positions": excluded,
    }


def store_consensus_snapshot(
    conn: sqlite3.Connection,
    snapshot: Mapping[str, Any],
    *,
    snapshot_id: str | None = None,
    corpus_release_id: str,
    pipeline_run_id: str,
    review_status: str = "accepted",
    created_at: str | None = None,
) -> dict[str, Any]:
    data = dict(snapshot)
    _validate_release_run_binding(
        conn, corpus_release_id, pipeline_run_id, allowed_run_statuses={"running", "succeeded"}
    )
    focal_claim = _require_current_accepted_claim(conn, _text(data.get("focal_claim_id"), "focal_claim_id"))
    if focal_claim["corpus_release_id"] != corpus_release_id:
        raise IntelligenceValidationError("consensus focal claim does not belong to release")
    created = _iso(created_at or now_iso(), "created_at")
    inputs_sha256 = _sha256(_text(data.get("inputs_sha256"), "inputs_sha256"), "inputs_sha256")
    status = _enum(review_status, {"accepted", "rejected"}, "review_status")
    row = {
        "id": snapshot_id
        or stable_id(
            _text(data.get("focal_claim_id"), "focal_claim_id"),
            _text(data.get("calculation_version"), "calculation_version"),
            _text(data.get("as_of"), "as_of"),
            inputs_sha256,
            prefix="con_",
        ),
        "schema_version": "consensus_snapshot_v1",
        "corpus_release_id": corpus_release_id,
        "pipeline_run_id": pipeline_run_id,
        "focal_claim_id": data["focal_claim_id"],
        "calculation_version": data["calculation_version"],
        "as_of": _iso(data["as_of"], "as_of"),
        "window_start": _iso(data["window_start"], "window_start"),
        "window_end": _iso(data["window_end"], "window_end"),
        "window_days": _integer(data["window_days"], "window_days", minimum=1),
        "people_count": _integer(data["people_count"], "people_count", minimum=0),
        "show_count": _integer(data["show_count"], "show_count", minimum=0),
        "network_count": _integer(data["network_count"], "network_count", minimum=0),
        "aligned_count": _integer(data["aligned_count"], "aligned_count", minimum=0),
        "qualified_count": _integer(data["qualified_count"], "qualified_count", minimum=0),
        "opposed_count": _integer(data["opposed_count"], "opposed_count", minimum=0),
        "excluded_count": _integer(data["excluded_count"], "excluded_count", minimum=0),
        "consensus_score": _optional_probability(data.get("consensus_score"), "consensus_score"),
        "dominant_bucket": data.get("dominant_bucket"),
        "dominant_share": _optional_probability(data.get("dominant_share"), "dominant_share"),
        "inputs_sha256": inputs_sha256,
        "included_claim_ids_json": dumps_json(data.get("included_claim_ids", [])),
        "metrics_json": dumps_json(
            {
                "positions": data.get("positions", []),
                "excluded_positions": data.get("excluded_positions", []),
            }
        ),
        "review_status": status,
        "created_at": created,
    }
    return _insert_immutable(conn, "consensus_snapshots", row)


def compute_contrarian_snapshot(
    conn: sqlite3.Connection,
    *,
    target_claim_id: str,
    as_of: str | None = None,
    window_days: int = DEFAULT_WINDOW_DAYS,
    dominant_threshold: float = DEFAULT_DOMINANT_THRESHOLD,
    target_share_threshold: float = DEFAULT_TARGET_SHARE_THRESHOLD,
    minimum_people: int = DEFAULT_MINIMUM_PEOPLE,
    minimum_shows: int = DEFAULT_MINIMUM_SHOWS,
    minimum_networks: int = DEFAULT_MINIMUM_NETWORKS,
) -> dict[str, Any]:
    """Apply the approved conservative contrarian gate.

    The target claim is explicitly excluded from its comparison population.
    A claim is contrarian only when an opposing position uniquely dominates,
    the target-aligned share is below 25%, and all diversity floors pass.
    """

    dominant_cutoff = _probability(dominant_threshold, "dominant_threshold")
    target_cutoff = _probability(target_share_threshold, "target_share_threshold")
    min_people = _integer(minimum_people, "minimum_people", minimum=1)
    min_shows = _integer(minimum_shows, "minimum_shows", minimum=1)
    min_networks = _integer(minimum_networks, "minimum_networks", minimum=1)
    consensus = compute_consensus_snapshot(
        conn,
        focal_claim_id=target_claim_id,
        as_of=as_of,
        window_days=window_days,
        exclude_claim_id=target_claim_id,
    )
    scored_count = (
        consensus["aligned_count"]
        + consensus["qualified_count"]
        + consensus["opposed_count"]
    )
    target_share = (consensus["aligned_count"] / scored_count) if scored_count else None
    coverage_reasons: list[str] = []
    if consensus["people_count"] < min_people:
        coverage_reasons.append("insufficient_people")
    if consensus["show_count"] < min_shows:
        coverage_reasons.append("insufficient_shows")
    if consensus["network_count"] < min_networks:
        coverage_reasons.append("insufficient_networks")
    decision_reasons: list[str] = []
    if consensus["dominant_bucket"] != "opposed":
        decision_reasons.append("dominant_position_not_opposed_to_target")
    if consensus["dominant_share"] is None or consensus["dominant_share"] < dominant_cutoff:
        decision_reasons.append("dominant_share_below_threshold")
    if target_share is None or target_share >= target_cutoff:
        decision_reasons.append("target_share_not_below_threshold")

    if coverage_reasons:
        classification = "insufficient_coverage"
    elif decision_reasons:
        classification = "not_contrarian"
    else:
        classification = "contrarian"
    reasons = coverage_reasons + decision_reasons

    descriptor = {
        "consensus_inputs_sha256": consensus["inputs_sha256"],
        "target_claim_id": target_claim_id,
        "dominant_threshold": dominant_cutoff,
        "target_share_threshold": target_cutoff,
        "minimum_people": min_people,
        "minimum_shows": min_shows,
        "minimum_networks": min_networks,
        "classification": classification,
    }
    return {
        "schema_version": "contrarian_snapshot_v1",
        "calculation_version": "contrarian_v1",
        "target_claim_id": target_claim_id,
        "as_of": consensus["as_of"],
        "window_start": consensus["window_start"],
        "window_end": consensus["window_end"],
        "window_days": consensus["window_days"],
        "people_count": consensus["people_count"],
        "show_count": consensus["show_count"],
        "network_count": consensus["network_count"],
        "dominant_bucket": consensus["dominant_bucket"],
        "dominant_share": consensus["dominant_share"],
        "target_share": target_share,
        "dominant_threshold": dominant_cutoff,
        "target_share_threshold": target_cutoff,
        "minimum_people": min_people,
        "minimum_shows": min_shows,
        "minimum_networks": min_networks,
        "classification": classification,
        "is_contrarian": classification == "contrarian",
        "exclusion_reason": ",".join(reasons) if reasons else None,
        "inputs_sha256": sha256_text(dumps_json(descriptor)),
        "included_claim_ids": consensus["included_claim_ids"],
        "consensus": consensus,
    }


def store_contrarian_snapshot(
    conn: sqlite3.Connection,
    snapshot: Mapping[str, Any],
    *,
    snapshot_id: str | None = None,
    consensus_snapshot_id: str | None = None,
    corpus_release_id: str,
    pipeline_run_id: str,
    review_status: str = "accepted",
    created_at: str | None = None,
) -> dict[str, Any]:
    data = dict(snapshot)
    _validate_release_run_binding(
        conn, corpus_release_id, pipeline_run_id, allowed_run_statuses={"running", "succeeded"}
    )
    target_claim = _require_current_accepted_claim(conn, _text(data.get("target_claim_id"), "target_claim_id"))
    if target_claim["corpus_release_id"] != corpus_release_id:
        raise IntelligenceValidationError("contrarian target claim does not belong to release")
    created = _iso(created_at or now_iso(), "created_at")
    inputs_sha256 = _sha256(_text(data.get("inputs_sha256"), "inputs_sha256"), "inputs_sha256")
    classification = _enum(
        data.get("classification"), CONTRARIAN_CLASSIFICATIONS, "classification"
    )
    is_contrarian = bool(data.get("is_contrarian"))
    if is_contrarian != (classification == "contrarian"):
        raise IntelligenceValidationError(
            "is_contrarian must agree with the explicit classification"
        )
    row = {
        "id": snapshot_id
        or stable_id(
            _text(data.get("target_claim_id"), "target_claim_id"),
            _text(data.get("calculation_version"), "calculation_version"),
            _text(data.get("as_of"), "as_of"),
            inputs_sha256,
            prefix="ctr_",
        ),
        "schema_version": "contrarian_snapshot_v1",
        "corpus_release_id": corpus_release_id,
        "pipeline_run_id": pipeline_run_id,
        "target_claim_id": data["target_claim_id"],
        "consensus_snapshot_id": consensus_snapshot_id,
        "calculation_version": data["calculation_version"],
        "as_of": _iso(data["as_of"], "as_of"),
        "window_start": _iso(data["window_start"], "window_start"),
        "window_end": _iso(data["window_end"], "window_end"),
        "window_days": _integer(data["window_days"], "window_days", minimum=1),
        "people_count": _integer(data["people_count"], "people_count", minimum=0),
        "show_count": _integer(data["show_count"], "show_count", minimum=0),
        "network_count": _integer(data["network_count"], "network_count", minimum=0),
        "dominant_bucket": data.get("dominant_bucket"),
        "dominant_share": _optional_probability(data.get("dominant_share"), "dominant_share"),
        "target_share": _optional_probability(data.get("target_share"), "target_share"),
        "dominant_threshold": _probability(data["dominant_threshold"], "dominant_threshold"),
        "target_share_threshold": _probability(data["target_share_threshold"], "target_share_threshold"),
        "minimum_people": _integer(data["minimum_people"], "minimum_people", minimum=1),
        "minimum_shows": _integer(data["minimum_shows"], "minimum_shows", minimum=1),
        "minimum_networks": _integer(data["minimum_networks"], "minimum_networks", minimum=1),
        "classification": classification,
        "is_contrarian": 1 if is_contrarian else 0,
        "exclusion_reason": data.get("exclusion_reason"),
        "inputs_sha256": inputs_sha256,
        "included_claim_ids_json": dumps_json(data.get("included_claim_ids", [])),
        "metrics_json": dumps_json({"consensus": data.get("consensus", {})}),
        "review_status": _enum(review_status, {"accepted", "rejected"}, "review_status"),
        "created_at": created,
    }
    return _insert_immutable(conn, "contrarian_snapshots", row)


def record_outcome_resolution(
    conn: sqlite3.Connection,
    resolution: Mapping[str, Any] | None = None,
    **fields: Any,
) -> dict[str, Any]:
    """Append an outcome revision and compute only its explicit score fields."""

    data = _payload(resolution, fields)
    release_id, run_id = _decision_context(conn, data)
    claim_id = _text(data.get("claim_id"), "claim_id")
    claim = _require_current_accepted_claim(conn, claim_id)
    if claim["corpus_release_id"] != release_id:
        raise IntelligenceValidationError("outcome claim does not belong to corpus_release_id")
    outcome = _enum(data.get("outcome"), OUTCOMES, "outcome")
    revision, supersedes = _next_revision(
        conn,
        "outcome_resolution_revisions",
        "claim_id",
        claim_id,
        data.get("revision"),
        data.get("supersedes_resolution_id"),
    )
    categorical = categorical_outcome_score(outcome)
    brier = brier_score(claim["forecast_probability"], outcome)
    if "categorical_score" in data and data["categorical_score"] != categorical:
        raise IntelligenceValidationError("categorical_score does not match the outcome enum")
    if "brier_score" in data and not _same_optional_float(data["brier_score"], brier):
        raise IntelligenceValidationError("brier_score does not match forecast probability and outcome")
    resolved_at = _iso(data.get("resolved_at") or now_iso(), "resolved_at")
    created = _iso(data.get("created_at") or resolved_at, "created_at")
    due_at = _iso(data["due_at"], "due_at") if data.get("due_at") else None
    window_start = (
        _iso(data["resolution_window_start"], "resolution_window_start")
        if data.get("resolution_window_start")
        else None
    )
    window_end = (
        _iso(data["resolution_window_end"], "resolution_window_end")
        if data.get("resolution_window_end")
        else None
    )
    if window_start and window_end and window_end < window_start:
        raise IntelligenceValidationError("resolution_window_end cannot precede start")
    row = {
        "id": data.get("id") or stable_id(claim_id, str(revision), outcome, prefix="orv_"),
        "schema_version": "outcome_resolution_v1",
        "claim_id": claim_id,
        "corpus_release_id": release_id,
        "revision": revision,
        "supersedes_resolution_id": supersedes,
        "resolution_question": _text(data.get("resolution_question"), "resolution_question"),
        "due_at": due_at,
        "resolution_window_start": window_start,
        "resolution_window_end": window_end,
        "resolution_criteria": _text(data.get("resolution_criteria"), "resolution_criteria"),
        "outcome": outcome,
        "categorical_score": categorical,
        "brier_score": brier,
        "resolved_at": resolved_at,
        "confidence": _probability(data.get("confidence"), "confidence"),
        "rationale": _text(data.get("rationale"), "rationale"),
        "evidence_json": dumps_json(_json_object(data.get("evidence", data.get("evidence_json", {})), "evidence")),
        "authoritative_evidence_json": dumps_json(
            _json_value(
                data.get("authoritative_evidence", data.get("authoritative_evidence_json", {})),
                "authoritative_evidence",
            )
        ),
        "as_of": _iso(data.get("as_of") or resolved_at, "as_of"),
        "resolver_model": _text(data.get("resolver_model"), "resolver_model"),
        "resolver_version": _text(data.get("resolver_version"), "resolver_version"),
        "reviewer_version": _text(data.get("reviewer_version"), "reviewer_version"),
        "pipeline_run_id": run_id,
        "review_status": _enum(data.get("review_status", "accepted"), REVIEW_STATUSES, "review_status"),
        "created_at": created,
    }
    return _insert_immutable(conn, "outcome_resolution_revisions", row)


def outcome_score_summary(
    conn: sqlite3.Connection,
    *,
    claim_ids: Iterable[str] | None = None,
) -> dict[str, Any]:
    """Aggregate only current accepted revisions; keep Brier separate."""

    ids = list(dict.fromkeys(claim_ids or []))
    params: list[Any] = []
    where = ""
    if ids:
        where = f"WHERE claim_id IN ({','.join('?' for _ in ids)})"
        params.extend(ids)
    rows = conn.execute(
        f"""
        SELECT claim_id, outcome, categorical_score, brier_score
        FROM current_accepted_outcome_resolutions
        {where}
        ORDER BY claim_id
        """,
        params,
    ).fetchall()
    categorical = [float(row["categorical_score"]) for row in rows if row["categorical_score"] is not None]
    brier_values = [float(row["brier_score"]) for row in rows if row["brier_score"] is not None]
    return {
        "accepted_resolution_count": len(rows),
        "categorical_scored_count": len(categorical),
        "categorical_excluded_count": len(rows) - len(categorical),
        "categorical_mean": (sum(categorical) / len(categorical)) if categorical else None,
        "brier_scored_count": len(brier_values),
        "brier_mean": (sum(brier_values) / len(brier_values)) if brier_values else None,
        "claim_ids": [row["claim_id"] for row in rows],
    }


def get_current_accepted_claim(conn: sqlite3.Connection, claim_id: str) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM current_accepted_atomic_claims WHERE id = ?", (claim_id,)
    ).fetchone()
    return _row_dict(row) if row is not None else None


def list_current_accepted_claims(
    conn: sqlite3.Connection,
    *,
    person_id: str | None = None,
    source_id: str | None = None,
    observed_after: str | None = None,
    observed_before: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if person_id is not None:
        clauses.append("canonical_person_id = ?")
        params.append(person_id)
    if source_id is not None:
        clauses.append("source_id = ?")
        params.append(source_id)
    if observed_after is not None:
        clauses.append("datetime(observed_at) >= datetime(?)")
        params.append(_iso(observed_after, "observed_after"))
    if observed_before is not None:
        clauses.append("datetime(observed_at) <= datetime(?)")
        params.append(_iso(observed_before, "observed_before"))
    where = "WHERE " + " AND ".join(clauses) if clauses else ""
    params.append(_integer(limit, "limit", minimum=1))
    rows = conn.execute(
        f"""
        SELECT * FROM current_accepted_atomic_claims
        {where}
        ORDER BY datetime(observed_at) DESC, id
        LIMIT ?
        """,
        params,
    ).fetchall()
    return [_row_dict(row) for row in rows]


def _load_cohort_manifest(path: str | Path) -> dict[str, Any]:
    try:
        return load_production_cohort(path)
    except CohortValidationError as exc:
        raise IntelligenceValidationError(str(exc)) from exc


def _project_atomic_claims_from_release(
    conn: sqlite3.Connection,
    *,
    release_id: str,
    pipeline_run_id: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Mechanically project already-LLM-owned event fields; never infer meaning."""

    release = _fetch_required(conn, "corpus_releases", release_id)
    manifest = json.loads(release["manifest_json"])
    audited_episode_ids = {
        str(value)
        for value in manifest.get("metadata", {}).get("audited_pilot_episode_ids", [])
    }
    rows = conn.execute(
        """
        SELECT
          events.id AS discourse_event_id,
          events.segment_id,
          events.event_type AS claim_type,
          events.claim_text,
          COALESCE(contexts.speaker_name, events.actor_name) AS raw_speaker,
          events.stance,
          events.certainty,
          events.temporal_horizon AS time_horizon,
          events.evidence_text,
          events.evidence_start,
          events.evidence_end,
          events.confidence,
          labels.id AS label_id,
          labels.model AS extractor_model,
          labels.label_pack AS extractor_schema,
          labels.label_pack_version AS extractor_schema_version,
          labels.created_at AS label_created_at,
          segments.source_id,
          segments.episode_id,
          segments.text_path,
          release_segments.content_sha256 AS source_artifact_sha256,
          episodes.published_at AS observed_at,
          (
            SELECT judgments.canonical_person_id
            FROM raw_speaker_mentions AS mentions
            JOIN identity_resolution_judgments AS judgments
              ON judgments.raw_mention_type = 'speaker'
             AND judgments.raw_mention_id = mentions.id
             AND judgments.corpus_release_id = release_labels.corpus_release_id
             AND judgments.review_status = 'accepted'
             AND judgments.decision IN ('accepted', 'merged')
            JOIN pipeline_runs AS identity_run
              ON identity_run.id = judgments.pipeline_run_id
             AND identity_run.status IN ('running', 'succeeded')
            JOIN canonical_people AS people
              ON people.id = judgments.canonical_person_id
             AND people.status = 'accepted'
            WHERE mentions.discourse_event_id = events.id
              AND NOT EXISTS (
                SELECT 1 FROM identity_resolution_judgments AS later
                WHERE later.identity_lineage_id = judgments.identity_lineage_id
                  AND later.revision > judgments.revision
              )
            ORDER BY judgments.revision DESC, judgments.decided_at DESC, judgments.id DESC
            LIMIT 1
          ) AS canonical_person_id
        FROM corpus_release_labels AS release_labels
        JOIN labels ON labels.id = release_labels.label_id
        JOIN discourse_events AS events ON events.label_id = labels.id
        LEFT JOIN discourse_event_contexts AS contexts
          ON contexts.discourse_event_id = events.id
        JOIN segments ON segments.id = events.segment_id
        JOIN corpus_release_segments AS release_segments
          ON release_segments.corpus_release_id = release_labels.corpus_release_id
         AND release_segments.segment_id = segments.id
        JOIN episodes ON episodes.id = segments.episode_id
        WHERE release_labels.corpus_release_id = ?
          AND trim(COALESCE(events.claim_text, '')) <> ''
        ORDER BY segments.episode_id, segments.segment_index, events.event_index, events.id
        """,
        (release_id,),
    ).fetchall()
    payloads: list[dict[str, Any]] = []
    quarantine: list[dict[str, Any]] = []
    for row in rows:
        required = (
            "claim_type",
            "claim_text",
            "raw_speaker",
            "stance",
            "certainty",
            "time_horizon",
            "evidence_text",
        )
        missing = [field for field in required if not str(row[field] or "").strip()]
        if missing:
            quarantine.append(
                {
                    "discourse_event_id": row["discourse_event_id"],
                    "reason": "missing_required_fields",
                    "fields": missing,
                }
            )
            continue
        try:
            confidence = _probability(row["confidence"], "confidence")
            start = _integer(row["evidence_start"], "evidence_start", minimum=0)
            end = _integer(row["evidence_end"], "evidence_end", minimum=1)
            path = Path(str(row["text_path"] or ""))
            if not path.is_file():
                raise IntelligenceValidationError("evidence unit file is unavailable")
            validate_exact_evidence(
                path.read_text(encoding="utf-8"),
                str(row["evidence_text"]),
                start,
                end,
            )
        except (OSError, IntelligenceValidationError) as exc:
            quarantine.append(
                {
                    "discourse_event_id": row["discourse_event_id"],
                    "reason": "invalid_exact_evidence",
                    "detail": str(exc),
                }
            )
            continue
        accepted = row["episode_id"] in audited_episode_ids
        lineage = stable_id(str(row["discourse_event_id"]), str(row["raw_speaker"]), prefix="aclin_")
        payloads.append(
            {
                "id": stable_id(lineage, "1", str(row["claim_text"]), str(row["evidence_text"]), prefix="ac_"),
                "claim_lineage_id": lineage,
                "revision": 1,
                "claim_text": row["claim_text"],
                "claim_type": row["claim_type"],
                "raw_speaker": row["raw_speaker"],
                "canonical_person_id": row["canonical_person_id"],
                "stance": row["stance"],
                "certainty": row["certainty"],
                "time_horizon": row["time_horizon"],
                "discourse_event_id": row["discourse_event_id"],
                "segment_id": row["segment_id"],
                "source_id": row["source_id"],
                "episode_id": row["episode_id"],
                "evidence_unit_type": "segment",
                "evidence_unit_id": row["segment_id"],
                "evidence_text": row["evidence_text"],
                "evidence_start": start,
                "evidence_end": end,
                "extractor_model": row["extractor_model"],
                "extractor_schema": row["extractor_schema"],
                "extractor_schema_version": row["extractor_schema_version"],
                "source_artifact_sha256": row["source_artifact_sha256"],
                "provenance": {
                    "label_id": row["label_id"],
                    "projection": "literal_discourse_event_fields_v1",
                    "audited_pilot_acceptance": accepted,
                },
                "confidence": confidence,
                "review_status": "accepted" if accepted else "needs_review",
                "reviewed_by_model": "audited_pilot_gate" if accepted else None,
                "reviewed_at": row["label_created_at"] if accepted else None,
                "observed_at": row["observed_at"] or row["label_created_at"],
                "created_at": now_iso(),
                "corpus_release_id": release_id,
                "pipeline_run_id": pipeline_run_id,
            }
        )
    return payloads, quarantine


def _collect_release_members(
    conn: sqlite3.Connection,
    *,
    pilot_id: str,
    episode_ids: Sequence[str] | None,
    transcript_ids: Sequence[str] | None,
    segment_ids: Sequence[str] | None,
    accepted_label_ids: Sequence[str] | None,
    expected_episodes: Sequence[Mapping[str, Any]] | None,
    require_production_shape: bool,
) -> dict[str, list[dict[str, Any]]]:
    episode_set = _unique_ids(episode_ids, "episode_ids")
    if not episode_set:
        episode_set = [
            row["episode_id"]
            for row in conn.execute(
                """
                SELECT DISTINCT segments.episode_id
                FROM jobs
                JOIN segments ON segments.id = jobs.target_id
                WHERE json_extract(jobs.payload_json, '$.pilot_id') = ?
                  AND jobs.job_type = 'label_segment'
                ORDER BY segments.episode_id
                """,
                (pilot_id,),
            ).fetchall()
        ]
    if not episode_set:
        raise IntelligenceValidationError("release has no episode membership")

    episode_rows = [_release_member_record(conn, "episodes", item_id) for item_id in episode_set]
    if expected_episodes is not None:
        expected_by_id = {str(row["id"]): row for row in expected_episodes}
        if set(expected_by_id) != set(episode_set):
            raise IntelligenceValidationError("release episode IDs differ from the cohort manifest")
        for row in episode_rows:
            expected = expected_by_id[row["id"]]
            if row["source_id"] != expected.get("source_id"):
                raise IntelligenceValidationError(
                    f"episode {row['id']} source_id differs from the cohort manifest"
                )
            if _iso(row["published_at"], "published_at") != _iso(
                expected.get("published_at"), "manifest published_at"
            ):
                raise IntelligenceValidationError(
                    f"episode {row['id']} published_at differs from the cohort manifest"
                )
    if require_production_shape:
        from .cohort import COHORT_SHAPES
        if len(episode_rows) not in COHORT_SHAPES:
            allowed = ", ".join(str(n) for n in sorted(COHORT_SHAPES))
            raise IntelligenceValidationError(
                f"production release must contain exactly {allowed} episodes")
        expected_shows, _per_show = COHORT_SHAPES[len(episode_rows)]
        if len({row["source_id"] for row in episode_rows}) != expected_shows:
            raise IntelligenceValidationError(
                f"production release must contain exactly {expected_shows} shows")

    for episode in episode_rows:
        completed_context = conn.execute(
            """
            SELECT 1 FROM episode_context_runs
            WHERE episode_id = ?
              AND label_pack = 'ai_discourse_v3_1'
              AND model = 'gpt-5.5'
              AND status = 'completed'
            LIMIT 1
            """,
            (episode["id"],),
        ).fetchone()
        if completed_context is None:
            raise IntelligenceValidationError(
                f"episode {episode['id']} lacks a completed GPT-5.5 ai_discourse_v3_1 context run"
            )

    transcript_set = _unique_ids(transcript_ids, "transcript_ids")
    if not transcript_set:
        placeholders = ",".join("?" for _ in episode_set)
        transcript_set = [
            row["id"]
            for row in conn.execute(
                f"""
                WITH candidates AS (
                  SELECT transcripts.id, transcripts.episode_id,
                         COUNT(DISTINCT labels.id) AS ready_label_count,
                         MAX(labels.created_at) AS latest_label_at
                  FROM transcripts
                  JOIN segments ON segments.transcript_id = transcripts.id
                  JOIN labels ON labels.segment_id = segments.id
                  WHERE transcripts.episode_id IN ({placeholders})
                    AND transcripts.status = 'ready'
                    AND labels.status = 'ready'
                    AND labels.model = 'gpt-5.5'
                    AND labels.label_pack = 'ai_discourse_v3_1'
                  GROUP BY transcripts.id, transcripts.episode_id
                ), ranked AS (
                  SELECT candidates.*,
                         ROW_NUMBER() OVER (
                           PARTITION BY candidates.episode_id
                           ORDER BY datetime(candidates.latest_label_at) DESC,
                                    candidates.ready_label_count DESC,
                                    candidates.id DESC
                         ) AS transcript_rank
                  FROM candidates
                )
                SELECT id
                FROM ranked
                WHERE transcript_rank = 1
                ORDER BY episode_id, id
                """,
                episode_set,
            ).fetchall()
        ]
    if not transcript_set:
        raise IntelligenceValidationError("release has no ready transcript membership")
    transcript_rows = [
        _release_member_record(conn, "transcripts", item_id) for item_id in transcript_set
    ]
    if any(row["episode_id"] not in set(episode_set) for row in transcript_rows):
        raise IntelligenceValidationError("release transcript belongs to an episode outside the cohort")
    if any(row["status"] != "ready" for row in transcript_rows):
        raise IntelligenceValidationError("every release transcript must have status=ready")
    episodes_with_transcripts = {row["episode_id"] for row in transcript_rows}
    missing_transcript_episodes = sorted(set(episode_set) - episodes_with_transcripts)
    if missing_transcript_episodes:
        raise IntelligenceValidationError(
            "release episodes lack ready transcripts: " + ", ".join(missing_transcript_episodes)
        )

    segment_set = _unique_ids(segment_ids, "segment_ids")
    if not segment_set:
        placeholders = ",".join("?" for _ in transcript_set)
        segment_set = [
            row["id"]
            for row in conn.execute(
                f"""
                SELECT DISTINCT segments.id
                FROM segments
                JOIN labels ON labels.segment_id = segments.id
                WHERE segments.transcript_id IN ({placeholders})
                  AND labels.status = 'ready'
                  AND labels.model = 'gpt-5.5'
                  AND labels.label_pack = 'ai_discourse_v3_1'
                ORDER BY segments.transcript_id, segments.segment_index, segments.id
                """,
                transcript_set,
            ).fetchall()
        ]
    if not segment_set:
        raise IntelligenceValidationError("release has no segment membership")
    segment_rows = [_release_member_record(conn, "segments", item_id) for item_id in segment_set]
    if any(row["transcript_id"] not in set(transcript_set) for row in segment_rows):
        raise IntelligenceValidationError("release segment belongs to a transcript outside the release")
    transcripts_with_segments = {row["transcript_id"] for row in segment_rows}
    unreferenced_transcripts = sorted(set(transcript_set) - transcripts_with_segments)
    if unreferenced_transcripts:
        raise IntelligenceValidationError(
            "release transcripts must directly back accepted segment evidence: "
            + ", ".join(unreferenced_transcripts)
        )
    label_set = _unique_ids(accepted_label_ids, "accepted_label_ids")
    if not label_set:
        placeholders = ",".join("?" for _ in segment_set)
        label_set = [
            row["id"]
            for row in conn.execute(
                f"""
                SELECT labels.id
                FROM labels
                WHERE labels.segment_id IN ({placeholders})
                  AND labels.status = 'ready'
                  AND labels.model = 'gpt-5.5'
                  AND labels.label_pack = 'ai_discourse_v3_1'
                ORDER BY labels.segment_id, labels.id
                """,
                segment_set,
            ).fetchall()
        ]
    if not label_set:
        raise IntelligenceValidationError("release has no ready accepted label membership")
    label_rows = [_release_member_record(conn, "labels", item_id) for item_id in label_set]
    segment_label_counts: Counter[str] = Counter()
    for label in label_rows:
        if label["segment_id"] not in set(segment_set):
            raise IntelligenceValidationError("release label belongs to a segment outside the release")
        _validate_release_label(conn, label)
        segment_label_counts[label["segment_id"]] += 1
    missing_or_ambiguous = [
        segment_id for segment_id in segment_set if segment_label_counts[segment_id] != 1
    ]
    if missing_or_ambiguous:
        raise IntelligenceValidationError(
            "every release segment must have exactly one ready GPT-5.5 v3.1 label: "
            + ", ".join(sorted(missing_or_ambiguous))
        )

    return {
        "episodes": episode_rows,
        "transcripts": transcript_rows,
        "segments": segment_rows,
        "labels": label_rows,
    }


def _validate_release_label(conn: sqlite3.Connection, label: Mapping[str, Any]) -> None:
    if (
        label.get("status") != "ready"
        or label.get("model") != "gpt-5.5"
        or label.get("label_pack") != "ai_discourse_v3_1"
    ):
        raise IntelligenceValidationError(
            f"label {label['id']} is not a ready GPT-5.5 ai_discourse_v3_1 label"
        )
    try:
        output = json.loads(str(label.get("output_json") or ""))
    except json.JSONDecodeError as exc:
        raise IntelligenceValidationError(f"label {label['id']} output_json is invalid") from exc
    if output.get("schema_version") != "ai_discourse_v3_1":
        raise IntelligenceValidationError(
            f"label {label['id']} output schema is not ai_discourse_v3_1"
        )
    completed = conn.execute(
        """
        SELECT 1 FROM label_runs
        WHERE segment_id = ?
          AND label_pack = 'ai_discourse_v3_1'
          AND model = 'gpt-5.5'
          AND status = 'completed'
        LIMIT 1
        """,
        (label["segment_id"],),
    ).fetchone()
    if completed is None:
        raise IntelligenceValidationError(
            f"label {label['id']} lacks a completed GPT-5.5 v3.1 label run"
        )


def _release_member_record(
    conn: sqlite3.Connection, kind: str, record_id: str
) -> dict[str, Any]:
    queries = {
        "episodes": "SELECT * FROM episodes WHERE id = ?",
        "transcripts": "SELECT * FROM transcripts WHERE id = ?",
        "segments": "SELECT * FROM segments WHERE id = ?",
        "labels": "SELECT * FROM labels WHERE id = ?",
    }
    if kind not in queries:
        raise IntelligenceValidationError(f"unsupported release member kind: {kind}")
    row = conn.execute(queries[kind], (record_id,)).fetchone()
    if row is None:
        raise IntelligenceValidationError(f"{kind[:-1]} does not exist: {record_id}")
    data = _row_dict(row)
    content = {
        key: value
        for key, value in data.items()
        if key not in {"created_at", "updated_at", "fetched_at"}
    }
    member = {
        "id": record_id,
        "content_sha256": sha256_text(dumps_json(content)),
    }
    if kind == "episodes":
        member.update(
            source_id=data["source_id"],
            published_at=data["published_at"],
        )
    elif kind == "transcripts":
        member.update(episode_id=data["episode_id"], status=data["status"])
    elif kind == "segments":
        member.update(
            transcript_id=data["transcript_id"],
            episode_id=data["episode_id"],
        )
    elif kind == "labels":
        member.update(
            segment_id=data["segment_id"],
            accepted_status=data["status"],
            status=data["status"],
            model=data["model"],
            label_pack=data["label_pack"],
            label_pack_version=data["label_pack_version"],
            needs_review=data["needs_review"],
            output_json=data["output_json"],
        )
    return member


def _release_manifest(
    *,
    pilot_id: str,
    metadata: Mapping[str, Any],
    members: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, Any]:
    return {
        "schema_version": "corpus_release_manifest_v1",
        "pilot_id": pilot_id,
        "metadata": dict(metadata),
        **{
            kind: [
                {"id": row["id"], "content_sha256": row["content_sha256"]}
                for row in sorted(members[kind], key=lambda item: str(item["id"]))
            ]
            for kind in ("episodes", "transcripts", "segments", "labels")
        },
    }


def _insert_release_members(
    conn: sqlite3.Connection,
    release_id: str,
    members: Mapping[str, Sequence[Mapping[str, Any]]],
    created_at: str,
) -> None:
    definitions = {
        "episodes": ("corpus_release_episodes", ("episode_id",)),
        "transcripts": ("corpus_release_transcripts", ("transcript_id", "episode_id")),
        "segments": (
            "corpus_release_segments",
            ("segment_id", "transcript_id", "episode_id"),
        ),
        "labels": ("corpus_release_labels", ("label_id", "segment_id", "accepted_status")),
    }
    for kind, (table, identity_columns) in definitions.items():
        for index, member in enumerate(sorted(members[kind], key=lambda item: str(item["id"]))):
            values: dict[str, Any] = {
                "corpus_release_id": release_id,
                "content_sha256": member["content_sha256"],
                "member_index": index,
                "created_at": created_at,
            }
            for column in identity_columns:
                values[column] = member["id"] if column.endswith("_id") and column.startswith(kind[:-1]) else member[column]
            columns = list(values)
            sql = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})"
            try:
                conn.execute(sql, [values[column] for column in columns])
            except sqlite3.IntegrityError:
                id_column = identity_columns[0]
                existing = conn.execute(
                    f"SELECT * FROM {table} WHERE corpus_release_id = ? AND {id_column} = ?",
                    (release_id, values[id_column]),
                ).fetchone()
                if existing is None or any(existing[column] != values[column] for column in columns):
                    raise


def _load_release_members(
    conn: sqlite3.Connection, release_id: str
) -> dict[str, list[dict[str, Any]]]:
    rows = {
        "episodes": conn.execute(
            """
            SELECT episode_id AS id, content_sha256, member_index
            FROM corpus_release_episodes WHERE corpus_release_id = ? ORDER BY member_index
            """,
            (release_id,),
        ).fetchall(),
        "transcripts": conn.execute(
            """
            SELECT transcript_id AS id, episode_id, content_sha256, member_index
            FROM corpus_release_transcripts WHERE corpus_release_id = ? ORDER BY member_index
            """,
            (release_id,),
        ).fetchall(),
        "segments": conn.execute(
            """
            SELECT segment_id AS id, transcript_id, episode_id, content_sha256, member_index
            FROM corpus_release_segments WHERE corpus_release_id = ? ORDER BY member_index
            """,
            (release_id,),
        ).fetchall(),
        "labels": conn.execute(
            """
            SELECT label_id AS id, segment_id, accepted_status, content_sha256, member_index
            FROM corpus_release_labels WHERE corpus_release_id = ? ORDER BY member_index
            """,
            (release_id,),
        ).fetchall(),
    }
    return {kind: [_row_dict(row) for row in values] for kind, values in rows.items()}


def _unique_ids(values: Sequence[str] | None, field: str) -> list[str]:
    if values is None:
        return []
    result = [_text(value, field) for value in values]
    if len(result) != len(set(result)):
        raise IntelligenceValidationError(f"{field} must not contain duplicates")
    return result


def _validate_release_run_binding(
    conn: sqlite3.Connection,
    release_id: str,
    run_id: str,
    *,
    allowed_run_statuses: set[str],
) -> tuple[sqlite3.Row, sqlite3.Row]:
    release = _fetch_required(conn, "corpus_releases", release_id)
    if release["status"] != "accepted":
        raise IntelligenceValidationError("semantic decisions require an accepted immutable release")
    run = _fetch_required(conn, "pipeline_runs", run_id)
    if run["corpus_release_id"] != release_id:
        raise IntelligenceValidationError("pipeline run is not bound to corpus_release_id")
    if run["status"] not in allowed_run_statuses:
        raise IntelligenceValidationError(
            f"pipeline run status must be one of {sorted(allowed_run_statuses)}"
        )
    return release, run


def _decision_context(
    conn: sqlite3.Connection, data: Mapping[str, Any]
) -> tuple[str, str]:
    release_id = _text(data.get("corpus_release_id"), "corpus_release_id")
    run_id = _text(data.get("pipeline_run_id"), "pipeline_run_id")
    _validate_release_run_binding(
        conn, release_id, run_id, allowed_run_statuses={"running", "succeeded"}
    )
    return release_id, run_id


def _require_release_member(
    conn: sqlite3.Connection,
    table: str,
    release_id: str,
    id_column: str,
    record_id: str,
) -> None:
    allowed = {
        ("corpus_release_episodes", "episode_id"),
        ("corpus_release_transcripts", "transcript_id"),
        ("corpus_release_segments", "segment_id"),
        ("corpus_release_labels", "label_id"),
    }
    if (table, id_column) not in allowed:
        raise IntelligenceValidationError("unsupported release membership lookup")
    row = conn.execute(
        f"SELECT 1 FROM {table} WHERE corpus_release_id = ? AND {id_column} = ?",
        (release_id, record_id),
    ).fetchone()
    if row is None:
        raise IntelligenceValidationError(
            f"{record_id} is not an immutable member of corpus release {release_id}"
        )


def _require_accepted_parent(
    conn: sqlite3.Connection,
    table: str,
    record_id: str,
    release_id: str,
    label: str,
) -> sqlite3.Row:
    allowed = {
        "atomic_claims",
        "accepted_claim_subjects",
        "accepted_proposition_variants",
        "source_affiliations",
    }
    if table not in allowed:
        raise IntelligenceValidationError("unsupported semantic parent table")
    row = conn.execute(
        f"""
        SELECT parent.*
        FROM {table} AS parent
        JOIN pipeline_runs AS producing_run ON producing_run.id = parent.pipeline_run_id
        WHERE parent.id = ?
          AND parent.corpus_release_id = ?
          AND parent.review_status = 'accepted'
          AND producing_run.status IN ('running', 'succeeded')
        """,
        (record_id, release_id),
    ).fetchone()
    if row is None:
        raise IntelligenceValidationError(f"{label} is not an accepted decision in this release")
    return row


def _require_canonical_person_record(conn: sqlite3.Connection, person_id: str) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM canonical_people WHERE id = ? AND status = 'accepted'", (person_id,)
    ).fetchone()
    if row is None:
        raise IntelligenceValidationError("canonical person record is not marked accepted")
    return row


def _require_accepted_person(
    conn: sqlite3.Connection, person_id: str, *, release_id: str
) -> sqlite3.Row:
    person = _require_canonical_person_record(conn, person_id)
    judgment = conn.execute(
        """
        WITH ranked AS (
          SELECT judgments.*,
                 ROW_NUMBER() OVER (
                   PARTITION BY judgments.identity_lineage_id
                   ORDER BY judgments.revision DESC, judgments.decided_at DESC,
                            judgments.created_at DESC, judgments.id DESC
                 ) AS current_rank
          FROM identity_resolution_judgments AS judgments
          JOIN pipeline_runs AS producing_run
            ON producing_run.id = judgments.pipeline_run_id
           AND producing_run.status IN ('running', 'succeeded')
          WHERE judgments.corpus_release_id = ?
        )
        SELECT * FROM ranked
        WHERE current_rank = 1
          AND canonical_person_id = ?
          AND decision IN ('accepted', 'merged')
          AND review_status = 'accepted'
        LIMIT 1
        """,
        (release_id, person_id),
    ).fetchone()
    if judgment is None:
        raise IntelligenceValidationError(
            "canonical person lacks a current accepted LLM identity judgment in this release"
        )
    return person


def _insert_immutable(
    conn: sqlite3.Connection,
    table: str,
    values: Mapping[str, Any],
) -> dict[str, Any]:
    allowed = {
        "corpus_releases",
        "corpus_release_promotions",
        "pipeline_runs",
        "pipeline_run_authority_decisions",
        "atomic_claims",
        "identity_resolution_judgments",
        "accepted_claim_subjects",
        "accepted_proposition_variants",
        "accepted_position_observations",
        "source_affiliations",
        "person_appearances",
        "claim_relation_judgments",
        "consensus_snapshots",
        "contrarian_snapshots",
        "outcome_resolution_revisions",
    }
    if table not in allowed:
        raise IntelligenceValidationError(f"unsupported intelligence table: {table}")
    columns = list(values)
    sql = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})"
    try:
        conn.execute(sql, [values[column] for column in columns])
    except sqlite3.IntegrityError:
        existing = conn.execute(f"SELECT * FROM {table} WHERE id = ?", (values["id"],)).fetchone()
        if existing is None or any(existing[column] != values[column] for column in columns):
            raise
    return _row_dict(_fetch_required(conn, table, str(values["id"])))


def _next_revision(
    conn: sqlite3.Connection,
    table: str,
    lineage_column: str,
    lineage_id: str,
    requested_revision: Any,
    requested_supersedes: Any,
) -> tuple[int, str | None]:
    if table not in {
        "source_affiliations",
        "person_appearances",
        "claim_relation_judgments",
        "outcome_resolution_revisions",
        "identity_resolution_judgments",
        "accepted_claim_subjects",
        "accepted_proposition_variants",
        "accepted_position_observations",
    }:
        raise IntelligenceValidationError("unsupported revision table")
    latest = conn.execute(
        f"""
        SELECT id, revision FROM {table}
        WHERE {lineage_column} = ?
        ORDER BY revision DESC, created_at DESC, id DESC
        LIMIT 1
        """,
        (lineage_id,),
    ).fetchone()
    expected = (int(latest["revision"]) + 1) if latest else 1
    revision = (
        _integer(requested_revision, "revision", minimum=1)
        if requested_revision is not None
        else expected
    )
    if revision != expected:
        raise IntelligenceValidationError(
            f"revision must move forward exactly once; expected {expected}, got {revision}"
        )
    supersedes = requested_supersedes if requested_supersedes is not None else (latest["id"] if latest else None)
    if latest is None and supersedes is not None:
        raise IntelligenceValidationError("first revision cannot supersede another row")
    if latest is not None and supersedes != latest["id"]:
        raise IntelligenceValidationError("revision must supersede the latest row in its lineage")
    return revision, supersedes


def _validate_revision_link(
    conn: sqlite3.Connection,
    *,
    table: str,
    lineage_column: str,
    lineage_id: str,
    revision: int,
    supersedes_id: str | None,
) -> None:
    latest = conn.execute(
        f"""
        SELECT id, revision FROM {table}
        WHERE {lineage_column} = ?
        ORDER BY revision DESC, created_at DESC, id DESC
        LIMIT 1
        """,
        (lineage_id,),
    ).fetchone()
    if latest is None:
        if revision != 1 or supersedes_id is not None:
            raise IntelligenceValidationError("first claim lineage revision must be 1 without supersedes")
        return
    expected = int(latest["revision"]) + 1
    if revision != expected or supersedes_id != latest["id"]:
        raise IntelligenceValidationError(
            f"claim revision must be {expected} and supersede {latest['id']}"
        )


def _require_current_accepted_claim(conn: sqlite3.Connection, claim_id: str) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM current_accepted_atomic_claims WHERE id = ?", (claim_id,)
    ).fetchone()
    if row is None:
        raise IntelligenceValidationError("claim is not a current accepted atomic_claim_v1 row")
    return row


def _validate_evidence_from_segment_path(
    conn: sqlite3.Connection,
    segment_id: str,
    evidence_text: str,
    evidence_start: int,
    evidence_end: int,
) -> None:
    row = conn.execute("SELECT text_path FROM segments WHERE id = ?", (segment_id,)).fetchone()
    if row is None or not row["text_path"]:
        return
    path = Path(row["text_path"])
    if not path.is_file():
        return
    validate_exact_evidence(path.read_text(encoding="utf-8"), evidence_text, evidence_start, evidence_end)


def _window(as_of: str | None, window_days: int) -> tuple[str, str]:
    days = _integer(window_days, "window_days", minimum=1)
    end_text = _iso(as_of or now_iso(), "as_of")
    end_dt = parse_datetime(end_text)
    if end_dt is None:
        raise IntelligenceValidationError("as_of must be an ISO-8601 datetime")
    start_dt = end_dt - dt.timedelta(days=days)
    return end_dt.astimezone(UTC).replace(microsecond=0).isoformat(), start_dt.astimezone(UTC).replace(microsecond=0).isoformat()


def _dominant_bucket(counts: Counter[str], total: int) -> tuple[str | None, float | None]:
    if total <= 0:
        return None, None
    maximum = max((counts[name] for name in ("aligned", "qualified", "opposed")), default=0)
    winners = [name for name in ("aligned", "qualified", "opposed") if counts[name] == maximum]
    if maximum == 0 or len(winners) != 1:
        return None, maximum / total
    return winners[0], maximum / total


def _fetch_required(conn: sqlite3.Connection, table: str, record_id: str) -> sqlite3.Row:
    row = conn.execute(f"SELECT * FROM {table} WHERE id = ?", (record_id,)).fetchone()
    if row is None:
        raise IntelligenceValidationError(f"{table} row does not exist: {record_id}")
    return row


def _row_dict(row: sqlite3.Row | Mapping[str, Any]) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def _payload(primary: Mapping[str, Any] | None, fields: Mapping[str, Any]) -> dict[str, Any]:
    if primary is None:
        return dict(fields)
    if not isinstance(primary, Mapping):
        raise IntelligenceValidationError("record payload must be a mapping")
    data = dict(primary)
    overlap = set(data).intersection(fields)
    if overlap:
        raise IntelligenceValidationError(f"duplicate payload fields: {sorted(overlap)}")
    data.update(fields)
    return data


def _json_object(value: Any, field: str) -> dict[str, Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise IntelligenceValidationError(f"{field} must be valid JSON") from exc
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise IntelligenceValidationError(f"{field} must be a JSON object")
    return dict(value)


def _json_value(value: Any, field: str) -> Any:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise IntelligenceValidationError(f"{field} must be valid JSON") from exc
    if value is None:
        return {}
    if not isinstance(value, (Mapping, list)):
        raise IntelligenceValidationError(f"{field} must be a JSON object or array")
    return dict(value) if isinstance(value, Mapping) else list(value)


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise IntelligenceValidationError(f"{field} must be a non-empty string")
    return value.strip()


def _nonempty_evidence(value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise IntelligenceValidationError("evidence_text must be a non-empty string")
    return value


def _enum(value: Any, values: Iterable[str], field: str) -> str:
    text = _text(value, field)
    allowed = set(values)
    if text not in allowed:
        raise IntelligenceValidationError(
            f"{field} must be one of {', '.join(sorted(allowed))}"
        )
    return text


def _integer(value: Any, field: str, *, minimum: int) -> int:
    if isinstance(value, bool):
        raise IntelligenceValidationError(f"{field} must be an integer")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise IntelligenceValidationError(f"{field} must be an integer") from exc
    if result != value and not (isinstance(value, str) and str(result) == value.strip()):
        raise IntelligenceValidationError(f"{field} must be an integer")
    if result < minimum:
        raise IntelligenceValidationError(f"{field} must be at least {minimum}")
    return result


def _probability(value: Any, field: str) -> float:
    if isinstance(value, bool):
        raise IntelligenceValidationError(f"{field} must be a number from 0 to 1")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise IntelligenceValidationError(f"{field} must be a number from 0 to 1") from exc
    if not math.isfinite(result) or result < 0 or result > 1:
        raise IntelligenceValidationError(f"{field} must be a finite number from 0 to 1")
    return result


def _optional_probability(value: Any, field: str) -> float | None:
    return None if value is None else _probability(value, field)


def _iso(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise IntelligenceValidationError(f"{field} must be an ISO-8601 datetime")
    parsed = parse_datetime(value)
    if parsed is None:
        raise IntelligenceValidationError(f"{field} must be an ISO-8601 datetime")
    return parsed.astimezone(UTC).replace(microsecond=0).isoformat()


def _sha256(value: Any, field: str) -> str:
    if not isinstance(value, str) or len(value) != 64:
        raise IntelligenceValidationError(f"{field} must be a 64-character SHA-256 hex digest")
    try:
        int(value, 16)
    except ValueError as exc:
        raise IntelligenceValidationError(f"{field} must be a SHA-256 hex digest") from exc
    return value.lower()


def _same_optional_float(left: Any, right: float | None) -> bool:
    if left is None or right is None:
        return left is None and right is None
    try:
        return math.isclose(float(left), right, rel_tol=0, abs_tol=1e-12)
    except (TypeError, ValueError):
        return False


__all__ = [
    "ATOMIC_CLAIM_SCHEMA",
    "DEFAULT_DOMINANT_THRESHOLD",
    "DEFAULT_AUDITED_PILOT_ID",
    "DEFAULT_MINIMUM_NETWORKS",
    "DEFAULT_MINIMUM_PEOPLE",
    "DEFAULT_MINIMUM_SHOWS",
    "DEFAULT_TARGET_SHARE_THRESHOLD",
    "DEFAULT_WINDOW_DAYS",
    "DEFAULT_PRODUCTION_COHORT_ID",
    "DEFAULT_PRODUCTION_COHORT_PATH",
    "IntelligenceValidationError",
    "OUTCOMES",
    "PIPELINE_AUTHORITY_DECISIONS",
    "PIPELINE_AUTHORITY_STAGES",
    "RELATIONS",
    "accept_pipeline_run",
    "brier_score",
    "categorical_outcome_score",
    "compute_consensus_snapshot",
    "compute_contrarian_snapshot",
    "accept_corpus_release",
    "build_atomic_claims_from_release",
    "create_corpus_release",
    "create_pipeline_run",
    "get_current_accepted_claim",
    "latest_accepted_positions",
    "list_current_accepted_claims",
    "outcome_score_summary",
    "promote_corpus_release",
    "record_canonical_person_decision",
    "record_claim_subject",
    "record_claim_relation_judgment",
    "record_identity_resolution_judgment",
    "record_outcome_resolution",
    "record_person_appearance",
    "record_position_observation",
    "record_proposition_variant",
    "record_source_affiliation",
    "reject_pipeline_run",
    "relation_alignment_score",
    "relation_bucket",
    "store_atomic_claim_v1",
    "store_consensus_snapshot",
    "store_contrarian_snapshot",
    "supersede_pipeline_run",
    "transition_pipeline_run",
    "validate_exact_evidence",
    "verify_corpus_release",
]
