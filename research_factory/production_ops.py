from __future__ import annotations

import datetime as dt
import hashlib
import importlib
import inspect
import json
import os
import re
import sqlite3
import tempfile
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from . import db
from .cohort import load_production_cohort, verify_production_cohort
from .daily_cycle import (
    MAX_EXCEPTION_RUNTIME_MS,
    build_headless_exception_contract,
    ensure_daily_schema,
    record_exception_contract,
)
from .observer import publish_snapshot, write_snapshot
from .paths import exports_dir, root
from .util import dumps_json, now_iso, sha256_text, stable_id


ACCEPTED_PILOT_ID = "pif-gold-25-v1"
LEGACY_AUDITED_PILOT_ID = "scale-gate-v31-2026-07-02"
ACCEPTED_PILOT_EPISODES = 25
ACCEPTED_PILOT_SOURCES = 5
ACCEPTED_RELEASE_SCHEMA = "corpus_release_v1"
ACCEPTED_LABEL_PACK = "ai_discourse_v3_1"
ACCEPTED_LABEL_SCHEMA = "ai_discourse_v3_1"
ACCEPTED_MODEL = "gpt-5.5"
MAX_RELEASE_ERRORS = 100
MAX_ATOMIC_CLAIMS = 10_000


class _ServiceContractUnavailable(RuntimeError):
    pass


def pif_status(conn: sqlite3.Connection) -> dict[str, Any]:
    """Return compact operational status while preserving legacy fields."""

    counts = db.counts(conn)
    jobs = [
        dict(row)
        for row in conn.execute(
            """
            SELECT lane, job_type, status, COUNT(*) AS count
            FROM jobs
            GROUP BY lane, job_type, status
            ORDER BY lane, job_type, status
            """
        ).fetchall()
    ]
    schema_version = _schema_migration_version(conn)
    latest_release = None
    current_release = None
    if _table_exists(conn, "corpus_releases"):
        row = conn.execute(
            """
            SELECT id, schema_version, release_version, parent_release_id, cutoff_at,
                   manifest_sha256, source_count, item_count, claim_count, status, created_at
            FROM corpus_releases
            ORDER BY release_version DESC
            LIMIT 1
            """
        ).fetchone()
        latest_release = dict(row) if row else None
    if _table_exists(conn, "current_accepted_corpus_releases"):
        row = conn.execute(
            """
            SELECT id, schema_version, release_version, parent_release_id, cutoff_at,
                   manifest_sha256, source_count, item_count, claim_count, status,
                   created_at, promotion_id, promotion_revision, promoted_at
            FROM current_accepted_corpus_releases
            LIMIT 1
            """
        ).fetchone()
        current_release = dict(row) if row else None
    latest_daily = None
    if _table_exists(conn, "pif_daily_runs"):
        row = conn.execute(
            """
            SELECT id, run_date, status, started_at, completed_at, receipt_path, receipt_sha256
            FROM pif_daily_runs
            ORDER BY run_date DESC, started_at DESC
            LIMIT 1
            """
        ).fetchone()
        latest_daily = dict(row) if row else None
    pending_outcomes = _pending_outcome_count(conn)
    exception_contracts = 0
    if _table_exists(conn, "pif_exception_dispatches"):
        exception_contracts = int(conn.execute("SELECT COUNT(*) FROM pif_exception_dispatches").fetchone()[0])
    return {
        "ok": True,
        "counts": counts,
        "jobs": jobs,
        "operations": {
            "local_source_of_truth": True,
            "managed_app_server_boundary": True,
            "automatic_model_execution": False,
            "self_resuming_chats": False,
            "schema_migration_version": schema_version,
            "accepted_release_schema": ACCEPTED_RELEASE_SCHEMA,
            "current_release": current_release,
            "latest_release_candidate": latest_release,
            "latest_daily_cycle": latest_daily,
            "pending_outcomes": pending_outcomes,
            "recorded_exception_contracts": exception_contracts,
        },
    }


def build_release(
    conn: sqlite3.Connection,
    *,
    pilot_id: str = ACCEPTED_PILOT_ID,
    label_pack: str = "ai_discourse_v3_1",
    model: str = "gpt-5.5",
    output_dir: str | Path | None = None,
    release_id: str | None = None,
) -> dict[str, Any]:
    """Freeze the one approved pilot and emit immutable local artifacts.

    Release membership is owned by :mod:`research_factory.intelligence`; this
    wrapper only enforces the production gate, records a pipeline lifecycle,
    and materializes the canonical manifest and verification receipt.
    """

    contract_error = _approved_release_contract_error(
        pilot_id=pilot_id,
        label_pack=label_pack,
        model=model,
    )
    if contract_error:
        return contract_error
    functions = _required_intelligence_functions(
        "create_corpus_release",
        "verify_corpus_release",
        "create_pipeline_run",
        "transition_pipeline_run",
    )
    if isinstance(functions, dict):
        return functions
    create_release, verify_corpus, create_run, transition_run = functions
    try:
        cohort = verify_production_cohort(
            conn,
            label_pack=label_pack,
            model=model,
        )
        if not cohort["ok"]:
            return _release_boundary(
                {
                    "ok": False,
                    "error": "production_cohort_not_ready",
                    "cohort": _bounded_value(cohort),
                    "canonical_mutation": False,
                    "model_execution_attempted": False,
                },
                operation="release_build",
                pilot_id=pilot_id,
            )
        cohort_config = load_production_cohort()
        release = _find_reusable_release(
            conn,
            verify_corpus=verify_corpus,
            release_id=release_id,
            pilot_id=pilot_id,
            label_pack=label_pack,
            model=model,
            cohort_sha256=cohort["cohort_sha256"],
        )
        reused = release is not None
        if release is None:
            release = _call_service(
                create_release,
                conn,
                pilot_id=pilot_id,
                release_id=release_id,
                episode_ids=cohort["episode_ids"],
                manifest_metadata={
                    "membership_source": "config/production_cohort_v1.json",
                    "cohort_id": cohort["cohort_id"],
                    "cohort_sha256": cohort["cohort_sha256"],
                    "production_cohort_id": cohort["cohort_id"],
                    "production_cohort_sha256": cohort["cohort_sha256"],
                    "legacy_audited_pilot_id": LEGACY_AUDITED_PILOT_ID,
                    "audited_pilot_episode_ids": list(
                        cohort_config["constraint_evidence"]["audited_pilot_episode_ids"]
                    ),
                    "label_readiness_source": "jobs.payload_json_and_canonical_labels",
                    "label_pack": label_pack,
                    "model": model,
                    "expected_episode_count": ACCEPTED_PILOT_EPISODES,
                    "expected_source_count": ACCEPTED_PILOT_SOURCES,
                },
                status="accepted",
            )
        release = dict(release)
        canonical_manifest = _canonical_manifest(release)
        verification = _verify_release_contract(
            conn,
            release=release,
            service_result=_call_service(
                verify_corpus,
                conn,
                release_id=release["id"],
            ),
        )
        artifact_dir = _release_artifact_dir(output_dir, release["id"])
        manifest_path = _write_immutable_json(artifact_dir / "manifest.json", canonical_manifest)
        verification_path = _write_immutable_json(
            artifact_dir / "verification.json", verification
        )
        run = _finish_release_build_run(
            conn,
            create_run=create_run,
            transition_run=transition_run,
            release=release,
            verification=verification,
        )
        conn.commit()
    except _ServiceContractUnavailable as exc:
        conn.rollback()
        return {
            **_service_unavailable("release service contract", operation="release_build"),
            "detail": str(exc),
        }
    except Exception as exc:
        conn.rollback()
        return _service_failure("release_build", exc)
    return _release_boundary(
        {
            "ok": bool(verification["ok"] and run["status"] == "succeeded"),
            "release_id": release["id"],
            "release_version": release["release_version"],
            "manifest_sha256": release["manifest_sha256"],
            "manifest_path": str(manifest_path),
            "verification_path": str(verification_path),
            "verification": verification,
            "pipeline_run_id": run["id"],
            "pipeline_status": run["status"],
            "idempotent_replay": reused or run.get("_idempotent_replay", False),
            "canonical_mutation": not reused,
            "model_execution_attempted": False,
        },
        operation="release_build",
        pilot_id=pilot_id,
    )


def verify_release(
    conn: sqlite3.Connection,
    *,
    release_id: str | None = None,
    manifest_path: str | Path | None = None,
    output_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Verify one stored release by ID, optionally cross-checking an artifact."""

    function = _intelligence_function("verify_corpus_release")
    if not function:
        return _service_unavailable("verify_corpus_release", operation="release_verify")
    try:
        resolved_id = _resolve_release_id(
            conn,
            release_id=release_id,
            manifest_path=manifest_path,
        )
        release = _release_row(conn, resolved_id)
        canonical_manifest = _canonical_manifest(release)
        if manifest_path is not None:
            _assert_manifest_matches(canonical_manifest, manifest_path)
        verification = _verify_release_contract(
            conn,
            release=release,
            service_result=_call_service(function, conn, release_id=resolved_id),
        )
        artifact_dir = _release_artifact_dir(
            output_dir or (Path(manifest_path).expanduser().resolve().parent if manifest_path else None),
            resolved_id,
            already_scoped=bool(manifest_path and output_dir is None),
        )
        receipt_path = _write_immutable_json(
            artifact_dir / "verification.json", verification
        )
    except _ServiceContractUnavailable as exc:
        return {
            **_service_unavailable("verify_corpus_release", operation="release_verify"),
            "detail": str(exc),
        }
    except Exception as exc:
        return _service_failure("release_verify", exc)
    return _release_boundary(
        {
            **verification,
            "verification_path": str(receipt_path),
            "canonical_mutation": False,
            "model_execution_attempted": False,
        },
        operation="release_verify",
        pilot_id=ACCEPTED_PILOT_ID,
    )


def promote_release(
    conn: sqlite3.Connection,
    *,
    release_id: str | None = None,
    manifest_path: str | Path | None = None,
    verification_path: str | Path | None = None,
    claims_path: str | Path | None = None,
    pipeline_run_id: str | None = None,
    promoted_by: str = "pif-operator",
    rationale: str = "promote verified approved pilot release",
    model: str = "gpt-5.5",
    output_dir: str | Path | None = None,
    max_claims: int = MAX_ATOMIC_CLAIMS,
) -> dict[str, Any]:
    """Import accepted atomic decisions, finish their run, then promote.

    With no packet, the intelligence service mechanically projects the frozen
    LLM-owned discourse fields and preserves its audited/needs-review gate. An
    explicit packet may instead come from the managed app server. This command
    never starts a model or performs semantic inference.
    """

    if model != "gpt-5.5":
        return _release_contract_failure("release promotion requires model gpt-5.5")
    if not 1 <= int(max_claims) <= MAX_ATOMIC_CLAIMS:
        raise ValueError(f"max_claims must be between 1 and {MAX_ATOMIC_CLAIMS}")
    functions = _required_intelligence_functions(
        "verify_corpus_release",
        "create_pipeline_run",
        "transition_pipeline_run",
        "build_atomic_claims_from_release",
        "promote_corpus_release",
    )
    if isinstance(functions, dict):
        return functions
    verify_corpus, create_run, transition_run, atomic_builder, promote = functions
    try:
        resolved_id = _resolve_release_id(
            conn,
            release_id=release_id,
            manifest_path=manifest_path,
        )
        release = _release_row(conn, resolved_id)
        canonical_manifest = _canonical_manifest(release)
        if manifest_path is not None:
            _assert_manifest_matches(canonical_manifest, manifest_path)
        live_verification = _verify_release_contract(
            conn,
            release=release,
            service_result=_call_service(verify_corpus, conn, release_id=resolved_id),
        )
        if not live_verification["ok"]:
            return _release_boundary(
                {
                    "ok": False,
                    "error": "release_verification_failed",
                    "release_id": resolved_id,
                    "verification": live_verification,
                    "canonical_mutation": False,
                    "model_execution_attempted": False,
                },
                operation="release_promote",
                pilot_id=ACCEPTED_PILOT_ID,
            )
        if verification_path is not None:
            _assert_verification_matches(
                verification_path,
                release_id=resolved_id,
                manifest_sha256=release["manifest_sha256"],
            )
        existing_promotion = _existing_promotion(conn, resolved_id)
        if existing_promotion is not None:
            return _release_boundary(
                {
                    "ok": True,
                    "release_id": resolved_id,
                    "promotion": existing_promotion,
                    "pipeline_run_id": existing_promotion["pipeline_run_id"],
                    "atomic_claim_count": _atomic_claim_count(
                        conn, resolved_id, existing_promotion["pipeline_run_id"]
                    ),
                    "accepted_atomic_claim_count": _accepted_atomic_claim_count(
                        conn, resolved_id, existing_promotion["pipeline_run_id"]
                    ),
                    "idempotent_replay": True,
                    "canonical_mutation": False,
                    "model_execution_attempted": False,
                },
                operation="release_promote",
                pilot_id=ACCEPTED_PILOT_ID,
            )

        claims, claims_sha256 = _load_atomic_claim_packet(
            claims_path,
            max_claims=max_claims,
        ) if claims_path is not None else (
            None,
            sha256_text(
                dumps_json(
                    {
                        "release_manifest_sha256": release["manifest_sha256"],
                        "projection": "literal_discourse_event_fields_v1",
                    }
                )
            ),
        )
        projected_input_count = _release_claim_event_count(conn, resolved_id)
        if claims is not None and len(claims) != projected_input_count:
            return _release_boundary(
                {
                    "ok": False,
                    "error": "explicit_claim_packet_incomplete",
                    "release_id": resolved_id,
                    "packet_claim_count": len(claims),
                    "release_claim_event_count": projected_input_count,
                    "canonical_mutation": False,
                    "model_execution_attempted": False,
                },
                operation="release_promote",
                pilot_id=ACCEPTED_PILOT_ID,
            )
        if claims is None and not 1 <= projected_input_count <= int(max_claims):
            return _release_boundary(
                {
                    "ok": False,
                    "error": "mechanical_projection_item_bound_failed",
                    "release_id": resolved_id,
                    "projectable_claim_events": projected_input_count,
                    "max_claims": int(max_claims),
                    "canonical_mutation": False,
                    "model_execution_attempted": False,
                },
                operation="release_promote",
                pilot_id=ACCEPTED_PILOT_ID,
            )
        run = _prepare_atomic_import_run(
            conn,
            create_run=create_run,
            release=release,
            pipeline_run_id=pipeline_run_id,
            claims=claims,
            claims_sha256=claims_sha256,
            model=model,
            projected_input_count=projected_input_count,
        )
        if run["status"] == "running":
            atomic_result = _build_atomic_claims_transactionally(
                conn,
                builder=atomic_builder,
                transition_run=transition_run,
                release_id=resolved_id,
                run=run,
                claims=claims,
                claims_sha256=claims_sha256,
                expected_input_count=(len(claims) if claims is not None else projected_input_count),
            )
            if not atomic_result["ok"]:
                conn.commit()
                return _release_boundary(
                    {
                        **atomic_result,
                        "release_id": resolved_id,
                        "pipeline_run_id": run["id"],
                        "model_execution_attempted": False,
                    },
                    operation="release_promote",
                    pilot_id=ACCEPTED_PILOT_ID,
                )
            run = atomic_result["run"]
        elif run["status"] != "succeeded":
            return _release_boundary(
                {
                    "ok": False,
                    "error": "atomic_claim_pipeline_not_succeeded",
                    "release_id": resolved_id,
                    "pipeline_run_id": run["id"],
                    "pipeline_status": run["status"],
                    "canonical_mutation": False,
                    "model_execution_attempted": False,
                },
                operation="release_promote",
                pilot_id=ACCEPTED_PILOT_ID,
            )

        atomic_count = _atomic_claim_count(conn, resolved_id, run["id"])
        accepted_atomic_count = _accepted_atomic_claim_count(conn, resolved_id, run["id"])
        if atomic_count < 1 or accepted_atomic_count < 1:
            return _release_boundary(
                {
                    "ok": False,
                    "error": "accepted_atomic_claims_required",
                    "release_id": resolved_id,
                    "pipeline_run_id": run["id"],
                    "atomic_claim_count": atomic_count,
                    "accepted_atomic_claim_count": accepted_atomic_count,
                    "canonical_mutation": False,
                    "model_execution_attempted": False,
                },
                operation="release_promote",
                pilot_id=ACCEPTED_PILOT_ID,
            )
        promotion = _call_service(
            promote,
            conn,
            release_id=resolved_id,
            pipeline_run_id=run["id"],
            promoted_by=_sanitized_operator_text(promoted_by, "promoted_by"),
            rationale=_sanitized_operator_text(rationale, "rationale"),
        )
        receipt = {
            "schema_version": "corpus_release_promotion_receipt_v1",
            "ok": True,
            "release_id": resolved_id,
            "manifest_sha256": release["manifest_sha256"],
            "pipeline_run_id": run["id"],
            "atomic_claim_count": atomic_count,
            "accepted_atomic_claim_count": accepted_atomic_count,
            "promotion_id": promotion["id"],
            "promotion_revision": promotion["promotion_revision"],
            "promoted_by": promotion["promoted_by"],
            "rationale": promotion["rationale"],
            "created_at": promotion["created_at"],
            "model_execution_attempted": False,
        }
        artifact_dir = _release_artifact_dir(output_dir, resolved_id)
        receipt_path = _write_immutable_json(artifact_dir / "promotion.json", receipt)
        conn.commit()
    except _ServiceContractUnavailable as exc:
        conn.rollback()
        return {
            **_service_unavailable("release promotion service contract", operation="release_promote"),
            "detail": str(exc),
        }
    except Exception as exc:
        conn.rollback()
        return _service_failure("release_promote", exc)
    return _release_boundary(
        {
            **receipt,
            "promotion_path": str(receipt_path),
            "promotion": _bounded_value(promotion),
            "canonical_mutation": True,
            "idempotent_replay": False,
        },
        operation="release_promote",
        pilot_id=ACCEPTED_PILOT_ID,
    )


def reconcile_target(
    conn: sqlite3.Connection,
    *,
    target: str,
    apply: bool = False,
    release_id: str | None = None,
    model: str = "gpt-5.5",
    limit: int = 25,
    scope: str = "last_18_months",
    output_dir: str | Path | None = None,
) -> dict[str, Any]:
    if target not in {"identities", "claims", "relations", "networks"}:
        raise ValueError("target must be identities, claims, relations, or networks")
    if not 1 <= int(limit) <= 200:
        raise ValueError("limit must be between 1 and 200")
    if scope not in {"last_18_months", "all"}:
        raise ValueError("scope must be last_18_months or all")
    if model != ACCEPTED_MODEL:
        return {
            "ok": False,
            "error": "production_reconciliation_requires_gpt_5_5",
            "target": target,
            "apply": False,
            "canonical_mutation": False,
            "model_execution_attempted": False,
        }
    try:
        module = importlib.import_module("research_factory.semantic_reconcile")
    except (ImportError, ModuleNotFoundError):
        return {
            "ok": False,
            "error": "semantic_runtime_unavailable",
            "target": target,
            "apply": False,
            "canonical_mutation": False,
            "model_execution_attempted": False,
            "planned_items": _semantic_pending_count(conn, target, limit=int(limit)),
            "required_module": "research_factory.semantic_reconcile",
        }
    function = getattr(module, "prepare_reconciliation_packet", None)
    if not function:
        return {
            "ok": False,
            "error": "semantic_runtime_unavailable",
            "target": target,
            "apply": False,
            "canonical_mutation": False,
            "model_execution_attempted": False,
            "planned_items": _semantic_pending_count(conn, target, limit=int(limit)),
            "required_function": "prepare_reconciliation_packet",
        }
    resolved_release_id = release_id or _current_accepted_release_id(conn)
    if not resolved_release_id:
        return {
            "ok": False,
            "error": "accepted_promoted_release_required",
            "target": target,
            "apply": False,
            "canonical_mutation": False,
            "model_execution_attempted": False,
            "planned_items": 0,
        }
    counter = getattr(module, "count_reconciliation_candidates", None)
    planned_items = (
        int(
            _call_service(
                counter,
                conn,
                target=target,
                release_id=resolved_release_id,
                limit=int(limit),
                scope=scope,
            )
        )
        if counter
        else _semantic_pending_count(conn, target, limit=int(limit))
    )
    if not apply:
        return {
            "ok": True,
            "target": target,
            "apply": False,
            "release_id": resolved_release_id,
            "planned_items": planned_items,
            "packet_prepared": False,
            "packet_only": True,
            "scope": scope,
            "canonical_mutation": False,
            "model_execution_attempted": False,
        }
    try:
        result = _call_service(
            function,
            conn,
            target=target,
            release_id=resolved_release_id,
            limit=int(limit),
            scope=scope,
            output_dir=output_dir,
        )
    except Exception as exc:
        return {
            **_service_failure("semantic_packet_preparation", exc),
            "target": target,
            "apply": False,
            "release_id": resolved_release_id,
            "model_execution_attempted": False,
        }
    bounded = _bounded_value(result)
    if not isinstance(bounded, dict):
        bounded = {"result": bounded}
    return {
        "ok": bool(bounded.get("ok", True)),
        "target": target,
        "apply": True,
        "release_id": resolved_release_id,
        "packet_prepared": bounded.get("state") == "prepared",
        "packet_only": True,
        "scope": scope,
        "canonical_mutation": False,
        "model_execution_attempted": False,
        "processed": int(bounded.get("item_count", 0) or 0),
        "result": bounded,
    }


def reconcile_all(
    conn: sqlite3.Connection,
    *,
    apply: bool,
    release_id: str | None,
    model: str,
    limit: int,
    scope: str = "last_18_months",
) -> dict[str, Any]:
    total_limit = int(limit)
    if not 1 <= total_limit <= 200:
        raise ValueError("limit must be between 1 and 200")
    if scope not in {"last_18_months", "all"}:
        raise ValueError("scope must be last_18_months or all")
    targets = ("identities", "claims", "relations", "networks")
    base, remainder = divmod(total_limit, len(targets))
    budgets = [base + (1 if index < remainder else 0) for index in range(len(targets))]
    results: list[dict[str, Any]] = []
    for target, budget in zip(targets, budgets):
        if budget == 0:
            results.append(
                {
                    "ok": True,
                    "target": target,
                    "apply": False,
                    "processed": 0,
                    "planned_items": 0,
                    "skipped": "global_item_budget_exhausted",
                    "canonical_mutation": False,
                    "model_execution_attempted": False,
                }
            )
            continue
        results.append(
            reconcile_target(
                conn,
                target=target,
                apply=apply,
                release_id=release_id,
                model=model,
                limit=budget,
                scope=scope,
            )
        )
    processed = sum(int(item.get("processed", 0) or 0) for item in results)
    if processed > total_limit:
        raise RuntimeError("reconciliation exceeded the global item budget")
    return {
        "ok": all(bool(item.get("ok")) for item in results),
        "apply": bool(apply),
        "processed": processed,
        "limit": total_limit,
        "scope": scope,
        "canonical_mutation": False,
        "model_execution_attempted": False,
        "targets": results,
    }


def plan_or_resolve_outcomes(
    conn: sqlite3.Connection,
    *,
    claim_id: str | None,
    outcome: str | None,
    confidence: float | None,
    rationale: str | None,
    evidence: Mapping[str, Any] | None,
    authoritative_evidence: Any | None,
    resolution_question: str | None,
    due_at: str | None,
    resolution_window_start: str | None,
    resolution_window_end: str | None,
    resolution_criteria: str | None,
    as_of: str | None,
    resolver_model: str,
    resolver_version: str,
    reviewer_version: str,
    review_status: str,
    resolved_at: str | None,
    categorical_score: float | None,
    brier_score: float | None,
    pipeline_run_id: str | None,
    limit: int,
    max_runtime_ms: int,
    record_exceptions: bool,
) -> dict[str, Any]:
    supplied = [claim_id is not None, outcome is not None, confidence is not None, rationale is not None]
    if any(supplied):
        if not all(supplied):
            raise ValueError("--claim-id, --outcome, --confidence, and --rationale are required together")
        if not resolution_question or not resolution_criteria:
            raise ValueError("--resolution-question and --resolution-criteria are required for an explicit resolution")
        if not 0 <= float(confidence) <= 1:
            raise ValueError("confidence must be between 0 and 1")
        if review_status == "accepted":
            if not _has_evidence(evidence):
                raise ValueError("accepted resolutions require non-empty evidence metadata")
            if not _has_evidence(authoritative_evidence):
                raise ValueError("accepted resolutions require authoritative evidence metadata")
            if outcome in {"true", "false", "mixed"} and not (due_at or resolution_window_end):
                raise ValueError("resolved outcomes require a due date or resolution-window end")
        function = _first_intelligence_function(
            "resolve_outcome",
            "append_outcome_resolution",
            "record_outcome_resolution",
        )
        if not function:
            return _service_unavailable("resolve_outcome", operation="outcomes_resolve")
        try:
            claim = conn.execute(
                "SELECT corpus_release_id FROM current_accepted_atomic_claims WHERE id = ?",
                (claim_id,),
            ).fetchone()
            if claim is None:
                raise ValueError("claim is not a current accepted atomic claim")
            release_id = str(claim["corpus_release_id"])
            resolution: dict[str, Any] = {
                "claim_id": claim_id,
                "corpus_release_id": release_id,
                "outcome": outcome,
                "confidence": float(confidence),
                "rationale": rationale,
                "evidence": dict(evidence or {}),
                "authoritative_evidence": authoritative_evidence or {},
                "resolution_question": resolution_question,
                "resolution_criteria": resolution_criteria,
                "resolver_model": resolver_model,
                "resolver_version": resolver_version,
                "reviewer_version": reviewer_version,
                "review_status": review_status,
            }
            for key, value in {
                "due_at": due_at,
                "resolution_window_start": resolution_window_start,
                "resolution_window_end": resolution_window_end,
                "as_of": as_of,
                "resolved_at": resolved_at,
                "categorical_score": categorical_score,
                "brier_score": brier_score,
            }.items():
                if value is not None:
                    resolution[key] = value
            prior = _matching_outcome_resolution(conn, resolution)
            if prior is not None:
                return {
                    "ok": True,
                    "mode": "explicit_resolution_revision",
                    "canonical_mutation": False,
                    "idempotent_replay": True,
                    "pipeline_run_id": prior["pipeline_run_id"],
                    "result": _bounded_value(dict(prior)),
                }

            run_was_created = False
            transition_run = None
            if pipeline_run_id:
                _validate_outcome_pipeline_run(
                    conn,
                    pipeline_run_id=pipeline_run_id,
                    corpus_release_id=release_id,
                )
            else:
                create_run = _intelligence_function("create_pipeline_run")
                transition_run = _intelligence_function("transition_pipeline_run")
                if not create_run or not transition_run:
                    return _service_unavailable(
                        "create_pipeline_run, transition_pipeline_run",
                        operation="outcomes_resolve",
                    )
                resolution_digest = sha256_text(dumps_json(resolution))
                pipeline_run_id = stable_id(
                    release_id,
                    claim_id,
                    resolution_digest,
                    prefix="pir_outcome_",
                )
                existing_run = conn.execute(
                    "SELECT * FROM pipeline_runs WHERE id = ?", (pipeline_run_id,)
                ).fetchone()
                if existing_run is None:
                    _call_service(
                        create_run,
                        conn,
                        run_id=pipeline_run_id,
                        run_type="manual_outcome_resolution",
                        run_schema="outcome_resolution_v1",
                        run_schema_version="1",
                        corpus_release_id=release_id,
                        model=resolver_model,
                        model_version=resolver_version,
                        prompt_version=reviewer_version,
                        status="running",
                        parameters={
                            "claim_id": claim_id,
                            "resolution_sha256": resolution_digest,
                            "review_status": review_status,
                        },
                        input_count=1,
                        input_sha256=resolution_digest,
                    )
                    run_was_created = True
                elif existing_run["status"] != "running":
                    raise ValueError("deterministic outcome pipeline run is not resumable")
            resolution["pipeline_run_id"] = pipeline_run_id
            result = _call_service(
                function,
                conn,
                resolution=resolution,
            )
            if run_was_created and transition_run is not None:
                output_digest = sha256_text(dumps_json(_bounded_value(result)))
                _call_service(
                    transition_run,
                    conn,
                    run_id=pipeline_run_id,
                    status="succeeded",
                    expected_status="running",
                    input_count=1,
                    output_count=1,
                    failure_count=0,
                    output_sha256=output_digest,
                    metrics={"resolution_count": 1, "claim_id": claim_id},
                    receipt={
                        "resolution_sha256": sha256_text(dumps_json(resolution)),
                        "review_status": review_status,
                    },
                )
            conn.commit()
        except Exception as exc:
            conn.rollback()
            return _service_failure("outcomes_resolve", exc)
        bounded = _bounded_value(result)
        return {
            "ok": bool(bounded.get("ok", True)) if isinstance(bounded, Mapping) else True,
            "mode": "explicit_resolution_revision",
            "canonical_mutation": True,
            "idempotent_replay": False,
            "pipeline_run_id": pipeline_run_id,
            "result": bounded,
        }
    return plan_due_outcome_dispatches(
        conn,
        limit=limit,
        max_runtime_ms=max_runtime_ms,
        record=record_exceptions,
    )


def plan_due_outcome_dispatches(
    conn: sqlite3.Connection,
    *,
    limit: int = 10,
    max_runtime_ms: int = MAX_EXCEPTION_RUNTIME_MS,
    record: bool = False,
    since: str | None = None,
) -> dict[str, Any]:
    if not 1 <= int(limit) <= 100:
        raise ValueError("limit must be between 1 and 100")
    runtime = min(MAX_EXCEPTION_RUNTIME_MS, int(max_runtime_ms))
    if runtime < 1:
        raise ValueError("max_runtime_ms must be positive")
    rows = _due_outcome_rows(conn, limit=int(limit), since=since)
    contracts: list[dict[str, Any]] = []
    recorded = 0
    already_recorded = 0
    for row in rows:
        contract = build_headless_exception_contract(
            source_task_id=f"outcome:{row['claim_id']}",
            title=f"Resolve PIF forecast outcome {row['claim_id']}",
            prompt=(
                f"Resolve atomic forecast claim {row['claim_id']} using public, source-cited evidence only. "
                "Return an outcome_resolution_v1 packet for local validation. Do not include transcript text, "
                "do not mutate canonical SQLite, do not launch another task, and do not resume a chat."
            ),
            policy_profile="pif-public-outcome-evidence-only",
            max_runtime_ms=runtime,
        )
        if record:
            recording = record_exception_contract(conn, contract)
            if recording.get("recorded"):
                recorded += 1
            else:
                already_recorded += 1
        contracts.append(contract)
    return {
        "ok": True,
        "mode": "bounded_exception_dispatch_contract",
        "due": len(rows),
        "due_total": _due_outcome_count(conn, since=since),
        "backlog_total": _due_outcome_count(conn),
        "since": since,
        "recorded": recorded,
        "already_recorded": already_recorded,
        "dispatch_state": "recorded_only" if record else "contract_only",
        "dispatch_contracts": contracts,
        "external_launch_attempted": False,
        "self_resuming_chats": False,
        "canonical_mutation": False,
    }


def publish_ops(
    conn: sqlite3.Connection,
    *,
    output: str | Path | None = None,
    publish: bool = False,
    observer_url: str | None = None,
    token: str | None = None,
) -> dict[str, Any]:
    path = write_snapshot(conn, output or (exports_dir() / "observer-snapshot.json"))
    snapshot = json.loads(path.read_text(encoding="utf-8"))
    privacy = _validate_observer_snapshot(snapshot)
    if not privacy["ok"]:
        return {
            "ok": False,
            "error": "observer_privacy_validation_failed",
            "snapshot_path": str(path),
            "privacy": privacy,
            "published": False,
            "network_attempted": False,
        }
    published = False
    publish_status = None
    if publish:
        response = publish_snapshot(path, url=observer_url, token=token)
        publish_status = int(response.get("status", 0))
        published = publish_status == 200
    return {
        "ok": not publish or published,
        "snapshot_path": str(path),
        "snapshot_sha256": _sha256_file(path),
        "privacy": privacy,
        "published": published,
        "publish_status": publish_status,
        "network_attempted": bool(publish),
        "local_source_of_truth": True,
    }


def supersede_legacy_label_jobs(
    conn: sqlite3.Connection,
    *,
    manifest_path: str | Path,
    verification_path: str | Path,
    limit: int = 500,
    backup_output: str | Path | None = None,
    reason: str = "superseded after verified corpus release",
) -> dict[str, Any]:
    """Back up authority, then supersede a bounded obsolete label-job batch.

    ``pilot_id`` is deliberately not part of the selector.  The historical
    backlog contains missing and obsolete pilot identifiers, so the only safe
    authority boundary is membership in the *currently promoted* release.  A
    pending label job is protected when its target is a release segment, its
    target segment belongs to a release episode, or its target is itself a
    release episode.  Every other pending ``label_segment`` job is eligible.

    The backup is created and integrity-checked before the transaction that
    changes job state.  Jobs are never deleted, and a completed batch is a
    clean no-op on replay.
    """

    item_limit = int(limit)
    if not 1 <= item_limit <= 5_000:
        raise ValueError("limit must be between 1 and 5,000")
    clean_reason = _sanitized_reason(reason)
    manifest, verification = _load_verified_manifest(manifest_path, verification_path)
    release_id = _require_canonical_verified_release(conn, manifest, verification)
    membership = _protected_release_membership(conn, release_id, manifest)
    if not membership["segment_ids"] or not membership["episode_ids"]:
        return {
            "ok": False,
            "error": "verified_release_has_no_protected_membership",
            "updated": 0,
            "backup_created": False,
        }
    before = _legacy_label_job_counts(conn, release_id)
    job_ids = _eligible_legacy_label_job_ids(conn, release_id, item_limit)
    if not job_ids:
        return {
            "ok": True,
            "release_id": release_id,
            "manifest_sha256": verification.get("manifest_sha256"),
            "before": before,
            "after": before,
            "updated": 0,
            "remaining_eligible": 0,
            "backup_created": False,
            "idempotent": True,
            "idempotent_replay": True,
            "batch_complete": True,
            "selected": 0,
        }

    backup_path = _backup_before_supersede(
        conn,
        output=backup_output,
        release_id=release_id,
        job_ids=job_ids,
    )
    backup_verification = _verify_supersede_backup(
        backup_path,
        release_id=release_id,
        job_ids=job_ids,
    )
    if not backup_verification["ok"]:
        raise ValueError("pre-supersede SQLite backup verification failed")
    placeholders = ",".join("?" for _ in job_ids)
    timestamp = now_iso()
    try:
        conn.execute("BEGIN IMMEDIATE")
        _require_current_promoted_release(conn, release_id)
        updated = conn.execute(
            f"""
            UPDATE jobs
            SET status = 'superseded', completed_at = ?, updated_at = ?, error = ?
            WHERE id IN ({placeholders})
              AND status = 'pending'
              AND job_type = 'label_segment'
              AND NOT EXISTS (
                SELECT 1
                FROM corpus_release_segments AS protected_segment
                WHERE protected_segment.corpus_release_id = ?
                  AND protected_segment.segment_id = jobs.target_id
              )
              AND NOT EXISTS (
                SELECT 1
                FROM segments AS target_segment
                JOIN corpus_release_episodes AS protected_episode
                  ON protected_episode.corpus_release_id = ?
                 AND protected_episode.episode_id = target_segment.episode_id
                WHERE target_segment.id = jobs.target_id
              )
              AND NOT EXISTS (
                SELECT 1
                FROM corpus_release_episodes AS protected_target_episode
                WHERE protected_target_episode.corpus_release_id = ?
                  AND protected_target_episode.episode_id = jobs.target_id
              )
            """,
            (
                timestamp,
                timestamp,
                clean_reason,
                *job_ids,
                release_id,
                release_id,
                release_id,
            ),
        ).rowcount
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    after = _legacy_label_job_counts(conn, release_id)
    remaining = after["eligible_outside_release"]
    mutated = int(updated) > 0
    return {
        "ok": True,
        "release_id": release_id,
        "manifest_sha256": verification.get("manifest_sha256"),
        "before": before,
        "after": after,
        "selected": len(job_ids),
        "updated": int(updated),
        "skipped_after_backup": len(job_ids) - int(updated),
        "remaining_eligible": remaining,
        "backup_created": True,
        "backup_path": str(backup_path),
        "backup_sha256": backup_verification["sha256"],
        "backup_verification": backup_verification,
        "idempotent": not mutated,
        "idempotent_replay": not mutated,
        "batch_complete": remaining == 0,
    }


def _intelligence_function(name: str) -> Callable[..., Any] | None:
    try:
        module = importlib.import_module("research_factory.intelligence")
    except (ImportError, ModuleNotFoundError):
        return None
    value = getattr(module, name, None)
    return value if callable(value) else None


def _first_intelligence_function(*names: str) -> Callable[..., Any] | None:
    return next((function for name in names if (function := _intelligence_function(name))), None)


def _call_service(function: Callable[..., Any], conn: sqlite3.Connection, **kwargs: Any) -> Any:
    signature = inspect.signature(function)
    parameters = signature.parameters
    accepts_kwargs = any(item.kind == inspect.Parameter.VAR_KEYWORD for item in parameters.values())
    call_kwargs = kwargs if accepts_kwargs else {key: value for key, value in kwargs.items() if key in parameters}
    positional = []
    if parameters:
        first = next(iter(parameters.values()))
        if first.name in {"conn", "connection", "db"} and first.kind in {
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        }:
            positional.append(conn)
    supplied = set(call_kwargs)
    if positional:
        supplied.add(next(iter(parameters)))
    missing = [
        name
        for name, parameter in parameters.items()
        if parameter.default is inspect.Parameter.empty
        and parameter.kind not in {inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD}
        and name not in supplied
    ]
    if missing:
        raise _ServiceContractUnavailable(
            f"service function {function.__name__} requires unsupported arguments: {', '.join(missing)}"
        )
    return function(*positional, **call_kwargs)


def _approved_release_contract_error(
    *,
    pilot_id: str,
    label_pack: str,
    model: str,
) -> dict[str, Any] | None:
    if pilot_id != ACCEPTED_PILOT_ID:
        return _release_contract_failure(
            f"release build is limited to approved pilot {ACCEPTED_PILOT_ID}"
        )
    if label_pack != ACCEPTED_LABEL_PACK:
        return _release_contract_failure(
            f"release build requires label pack {ACCEPTED_LABEL_PACK}"
        )
    if model != ACCEPTED_MODEL:
        return _release_contract_failure(
            f"release build requires model {ACCEPTED_MODEL}"
        )
    return None


def _release_contract_failure(message: str) -> dict[str, Any]:
    return {
        "ok": False,
        "error": "approved_release_contract_failed",
        "message": message,
        "pilot_id": ACCEPTED_PILOT_ID,
        "expected_episode_count": ACCEPTED_PILOT_EPISODES,
        "expected_source_count": ACCEPTED_PILOT_SOURCES,
        "accepted_label_pack": ACCEPTED_LABEL_PACK,
        "accepted_model": ACCEPTED_MODEL,
        "canonical_mutation": False,
        "model_execution_attempted": False,
    }


def _required_intelligence_functions(*names: str) -> tuple[Callable[..., Any], ...] | dict[str, Any]:
    functions = tuple(_intelligence_function(name) for name in names)
    missing = [name for name, function in zip(names, functions) if function is None]
    if missing:
        return _service_unavailable(
            ", ".join(missing),
            operation="release_service_boundary",
        )
    return tuple(function for function in functions if function is not None)


def _find_reusable_release(
    conn: sqlite3.Connection,
    *,
    verify_corpus: Callable[..., Any],
    release_id: str | None,
    pilot_id: str,
    label_pack: str,
    model: str,
    cohort_sha256: str,
) -> dict[str, Any] | None:
    if release_id is not None:
        row = conn.execute("SELECT * FROM corpus_releases WHERE id = ?", (release_id,)).fetchone()
        if row is None:
            return None
        release = dict(row)
        if not _manifest_matches_build_contract(
            _canonical_manifest(release),
            pilot_id=pilot_id,
            label_pack=label_pack,
            model=model,
            cohort_sha256=cohort_sha256,
        ):
            raise ValueError("existing release_id does not match the approved build contract")
        return release

    rows = conn.execute(
        "SELECT * FROM corpus_releases ORDER BY release_version DESC, created_at DESC"
    ).fetchall()
    for row in rows:
        release = dict(row)
        try:
            manifest = _canonical_manifest(release)
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if not _manifest_matches_build_contract(
            manifest,
            pilot_id=pilot_id,
            label_pack=label_pack,
            model=model,
            cohort_sha256=cohort_sha256,
        ):
            continue
        verification = _call_service(
            verify_corpus,
            conn,
            release_id=release["id"],
        )
        membership = _release_membership_counts(conn, release["id"])
        if (
            isinstance(verification, Mapping)
            and verification.get("ok") is True
            and membership.get("episodes") == ACCEPTED_PILOT_EPISODES
            and membership.get("transcripts") == ACCEPTED_PILOT_EPISODES
            and membership.get("segments", 0) > 0
            and membership.get("labels", 0) > 0
        ):
            return release
    return None


def _manifest_matches_build_contract(
    manifest: Mapping[str, Any],
    *,
    pilot_id: str,
    label_pack: str,
    model: str,
    cohort_sha256: str,
) -> bool:
    metadata = manifest.get("metadata")
    return bool(
        manifest.get("pilot_id") == pilot_id
        and isinstance(metadata, Mapping)
        and metadata.get("label_pack") == label_pack
        and metadata.get("model") == model
        and metadata.get("production_cohort_sha256") == cohort_sha256
    )


def _release_row(conn: sqlite3.Connection, release_id: str) -> dict[str, Any]:
    row = conn.execute("SELECT * FROM corpus_releases WHERE id = ?", (release_id,)).fetchone()
    if row is None:
        raise ValueError(f"corpus release does not exist: {release_id}")
    return dict(row)


def _resolve_release_id(
    conn: sqlite3.Connection,
    *,
    release_id: str | None,
    manifest_path: str | Path | None,
) -> str:
    if release_id:
        _release_row(conn, release_id)
        if manifest_path is not None:
            _assert_manifest_matches(
                _canonical_manifest(_release_row(conn, release_id)),
                manifest_path,
            )
        return release_id
    if manifest_path is None:
        raise ValueError("release_id or manifest_path is required")
    manifest = _read_json_object(manifest_path, "manifest")
    digest = sha256_text(dumps_json(manifest))
    row = conn.execute(
        "SELECT id FROM corpus_releases WHERE manifest_sha256 = ?",
        (digest,),
    ).fetchone()
    if row is None:
        raise ValueError("manifest is not registered in canonical SQLite")
    return str(row["id"])


def _canonical_manifest(release: Mapping[str, Any]) -> dict[str, Any]:
    if release.get("schema_version") != ACCEPTED_RELEASE_SCHEMA:
        raise ValueError(f"release must use {ACCEPTED_RELEASE_SCHEMA}")
    manifest = json.loads(str(release.get("manifest_json") or ""))
    if not isinstance(manifest, dict):
        raise ValueError("stored release manifest must be a JSON object")
    digest = sha256_text(dumps_json(manifest))
    if digest != release.get("manifest_sha256"):
        raise ValueError("stored release manifest hash does not match")
    return manifest


def _assert_manifest_matches(
    canonical_manifest: Mapping[str, Any],
    manifest_path: str | Path,
) -> None:
    supplied = _read_json_object(manifest_path, "manifest")
    if dumps_json(supplied) != dumps_json(canonical_manifest):
        raise ValueError("manifest artifact does not match canonical SQLite")


def _verify_release_contract(
    conn: sqlite3.Connection,
    *,
    release: Mapping[str, Any],
    service_result: Any,
) -> dict[str, Any]:
    service = dict(service_result) if isinstance(service_result, Mapping) else {}
    errors = [str(item)[:300] for item in service.get("errors", []) if str(item).strip()]
    if service.get("ok") is not True:
        errors.append("canonical release verification did not succeed")
    manifest = _canonical_manifest(release)
    metadata = manifest.get("metadata") if isinstance(manifest.get("metadata"), Mapping) else {}
    cohort = load_production_cohort()
    if manifest.get("pilot_id") != ACCEPTED_PILOT_ID:
        errors.append(f"manifest pilot_id must be {ACCEPTED_PILOT_ID}")
    if metadata.get("label_pack") != ACCEPTED_LABEL_PACK:
        errors.append(f"manifest label_pack must be {ACCEPTED_LABEL_PACK}")
    if metadata.get("model") != ACCEPTED_MODEL:
        errors.append(f"manifest model must be {ACCEPTED_MODEL}")
    if metadata.get("production_cohort_id") != cohort["cohort_id"]:
        errors.append(f"manifest cohort_id must be {cohort['cohort_id']}")
    if metadata.get("production_cohort_sha256") != cohort["sha256"]:
        errors.append("manifest production cohort hash does not match the frozen config")
    if metadata.get("cohort_id") != cohort["cohort_id"]:
        errors.append(f"manifest canonical cohort_id must be {cohort['cohort_id']}")
    if metadata.get("cohort_sha256") != cohort["sha256"]:
        errors.append("manifest canonical cohort hash does not match the frozen config")
    audited_ids = cohort["constraint_evidence"]["audited_pilot_episode_ids"]
    if metadata.get("audited_pilot_episode_ids") != audited_ids:
        errors.append("manifest audited_pilot_episode_ids do not match the frozen config")
    if release.get("schema_version") != ACCEPTED_RELEASE_SCHEMA:
        errors.append(f"release schema must be {ACCEPTED_RELEASE_SCHEMA}")
    if release.get("status") != "accepted":
        errors.append("release row must be immutable accepted status before promotion")
    if int(release.get("item_count") or 0) != ACCEPTED_PILOT_EPISODES:
        errors.append(f"release must contain exactly {ACCEPTED_PILOT_EPISODES} episodes")
    if int(release.get("source_count") or 0) != ACCEPTED_PILOT_SOURCES:
        errors.append(f"release must contain exactly {ACCEPTED_PILOT_SOURCES} sources")

    counts = _release_membership_counts(conn, str(release["id"]))
    if counts["episodes"] != ACCEPTED_PILOT_EPISODES:
        errors.append(f"episode membership={counts['episodes']} expected={ACCEPTED_PILOT_EPISODES}")
    if counts["transcripts"] != ACCEPTED_PILOT_EPISODES:
        errors.append(f"transcript membership={counts['transcripts']} expected={ACCEPTED_PILOT_EPISODES}")
    if counts["segments"] < ACCEPTED_PILOT_EPISODES:
        errors.append("release must contain at least one segment per episode")
    if counts["labels"] != counts["segments"]:
        errors.append("release must freeze exactly one accepted label per segment")
    source_count = int(
        conn.execute(
            """
            SELECT COUNT(DISTINCT episodes.source_id)
            FROM corpus_release_episodes AS members
            JOIN episodes ON episodes.id = members.episode_id
            WHERE members.corpus_release_id = ?
            """,
            (release["id"],),
        ).fetchone()[0]
    )
    if source_count != ACCEPTED_PILOT_SOURCES:
        errors.append(f"live release sources={source_count} expected={ACCEPTED_PILOT_SOURCES}")

    label_result = _verify_release_labels(conn, str(release["id"]))
    errors.extend(label_result["errors"])
    evidence_result = _verify_release_evidence(conn, str(release["id"]))
    errors.extend(evidence_result["errors"])
    errors.extend(_manifest_privacy_errors(manifest))
    errors = list(dict.fromkeys(errors))[:MAX_RELEASE_ERRORS]
    return {
        "schema_version": "corpus_release_verification_v1",
        "ok": not errors,
        "status": "verified" if not errors else "rejected",
        "release_id": release["id"],
        "release_version": int(release["release_version"]),
        "release_created_at": release["created_at"],
        "manifest_sha256": release["manifest_sha256"],
        "accepted_release_schema": ACCEPTED_RELEASE_SCHEMA,
        "pilot_id": ACCEPTED_PILOT_ID,
        "expected_episode_count": ACCEPTED_PILOT_EPISODES,
        "expected_source_count": ACCEPTED_PILOT_SOURCES,
        "membership_counts": counts,
        "label_records_checked": label_result["checked"],
        "evidence_records_checked": evidence_result["checked"],
        "exact_evidence_verified": evidence_result["ok"],
        "privacy_verified": not _manifest_privacy_errors(manifest),
        "errors": errors,
        "model_execution_attempted": False,
    }


def _release_membership_counts(conn: sqlite3.Connection, release_id: str) -> dict[str, int]:
    tables = {
        "episodes": "corpus_release_episodes",
        "transcripts": "corpus_release_transcripts",
        "segments": "corpus_release_segments",
        "labels": "corpus_release_labels",
    }
    return {
        name: int(
            conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE corpus_release_id = ?",
                (release_id,),
            ).fetchone()[0]
        )
        for name, table in tables.items()
    }


def _verify_release_labels(conn: sqlite3.Connection, release_id: str) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT labels.id, labels.label_pack, labels.label_pack_version, labels.model,
               labels.status, labels.output_json
        FROM corpus_release_labels AS members
        JOIN labels ON labels.id = members.label_id
        WHERE members.corpus_release_id = ?
        ORDER BY members.member_index
        """,
        (release_id,),
    ).fetchall()
    errors: list[str] = []
    for row in rows:
        label_id = str(row["id"])
        if row["label_pack"] != ACCEPTED_LABEL_PACK:
            errors.append(f"label {label_id} has unaccepted label_pack")
        if row["label_pack_version"] != ACCEPTED_LABEL_SCHEMA:
            errors.append(f"label {label_id} has unaccepted label schema version")
        if row["model"] != ACCEPTED_MODEL:
            errors.append(f"label {label_id} was not produced by {ACCEPTED_MODEL}")
        if row["status"] != "ready":
            errors.append(f"label {label_id} is not ready")
        try:
            output = json.loads(row["output_json"])
        except (TypeError, json.JSONDecodeError):
            errors.append(f"label {label_id} output is not valid JSON")
            continue
        if not isinstance(output, Mapping) or output.get("schema_version") != ACCEPTED_LABEL_SCHEMA:
            errors.append(f"label {label_id} output does not use {ACCEPTED_LABEL_SCHEMA}")
        if len(errors) >= MAX_RELEASE_ERRORS:
            break
    return {"ok": not errors, "checked": len(rows), "errors": errors[:MAX_RELEASE_ERRORS]}


def _verify_release_evidence(conn: sqlite3.Connection, release_id: str) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT events.id, events.segment_id, events.evidence_text,
               events.evidence_start, events.evidence_end, segments.text_path
        FROM corpus_release_labels AS members
        JOIN discourse_events AS events ON events.label_id = members.label_id
        JOIN segments ON segments.id = events.segment_id
        WHERE members.corpus_release_id = ?
        ORDER BY members.member_index, events.event_index, events.id
        """,
        (release_id,),
    ).fetchall()
    errors: list[str] = []
    segment_texts: dict[str, str | None] = {}
    for row in rows:
        segment_id = str(row["segment_id"])
        if segment_id not in segment_texts:
            path = Path(str(row["text_path"])).expanduser()
            try:
                segment_texts[segment_id] = path.read_text(encoding="utf-8")
            except OSError:
                segment_texts[segment_id] = None
        text = segment_texts[segment_id]
        if text is None:
            errors.append(f"evidence unit unavailable for segment {segment_id}")
        else:
            start = int(row["evidence_start"])
            end = int(row["evidence_end"])
            if start < 0 or end <= start or end > len(text) or text[start:end] != row["evidence_text"]:
                errors.append(f"discourse event {row['id']} does not have exact evidence")
        if len(errors) >= MAX_RELEASE_ERRORS:
            break
    if not rows:
        errors.append("release has no discourse-event evidence")
    return {"ok": not errors, "checked": len(rows), "errors": errors[:MAX_RELEASE_ERRORS]}


def _manifest_privacy_errors(manifest: Mapping[str, Any]) -> list[str]:
    forbidden = {"raw_text", "transcript_text", "output_json", "claim_text", "evidence_text"}
    found: set[str] = set()

    def walk(value: Any) -> None:
        if isinstance(value, Mapping):
            for key, item in value.items():
                if str(key).lower() in forbidden:
                    found.add(str(key))
                walk(item)
        elif isinstance(value, list):
            for item in value:
                walk(item)

    walk(manifest)
    return [f"manifest contains forbidden raw field {key}" for key in sorted(found)]


def _release_artifact_dir(
    output_dir: str | Path | None,
    release_id: str,
    *,
    already_scoped: bool = False,
) -> Path:
    if output_dir is not None:
        path = Path(output_dir).expanduser().resolve()
        return path if already_scoped else path / release_id
    return (exports_dir() / "releases" / release_id).resolve()


def _write_immutable_json(path: Path, value: Any) -> Path:
    path = path.expanduser().resolve()
    serialized = dumps_json(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_text(encoding="utf-8") != serialized:
            raise ValueError(f"immutable artifact already exists with different content: {path}")
        return path
    with tempfile.NamedTemporaryFile(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
        mode="w",
        encoding="utf-8",
        delete=False,
    ) as handle:
        handle.write(serialized)
        temp_path = Path(handle.name)
    try:
        os.replace(temp_path, path)
    finally:
        if temp_path.exists():
            temp_path.unlink()
    return path


def _finish_release_build_run(
    conn: sqlite3.Connection,
    *,
    create_run: Callable[..., Any],
    transition_run: Callable[..., Any],
    release: Mapping[str, Any],
    verification: Mapping[str, Any],
) -> dict[str, Any]:
    run_id = stable_id(str(release["id"]), "release_build", str(release["manifest_sha256"]), prefix="pir_")
    existing = conn.execute("SELECT * FROM pipeline_runs WHERE id = ?", (run_id,)).fetchone()
    if existing is None:
        run = _call_service(
            create_run,
            conn,
            run_id=run_id,
            run_type="release_build",
            run_schema="corpus_release_build",
            run_schema_version="corpus_release_build_v1",
            corpus_release_id=release["id"],
            status="running",
            parameters={
                "pilot_id": ACCEPTED_PILOT_ID,
                "manifest_sha256": release["manifest_sha256"],
                "automatic_model_execution": False,
            },
            receipt={"verification_schema": "corpus_release_verification_v1"},
            input_count=int(release["item_count"]),
            input_sha256=release["manifest_sha256"],
            created_at=release["created_at"],
            started_at=release["created_at"],
        )
    else:
        run = dict(existing)
        if run["status"] in {"succeeded", "failed", "canceled"}:
            run["_idempotent_replay"] = True
            return run
    successful = bool(verification.get("ok"))
    run = _call_service(
        transition_run,
        conn,
        run_id=run_id,
        status="succeeded" if successful else "failed",
        expected_status="running",
        metrics={
            "episode_count": int(release["item_count"]),
            "source_count": int(release["source_count"]),
            "verification_ok": successful,
        },
        receipt={
            "manifest_sha256": release["manifest_sha256"],
            "verification_sha256": sha256_text(dumps_json(verification)),
        },
        input_count=int(release["item_count"]),
        output_count=int(release["item_count"]) if successful else 0,
        failure_count=0 if successful else 1,
        output_sha256=release["manifest_sha256"] if successful else None,
        error=None if successful else "approved release verification failed",
    )
    return dict(run)


def _read_json_object(path: str | Path, label: str) -> dict[str, Any]:
    source = Path(path).expanduser().resolve()
    value = json.loads(source.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be a JSON object")
    return value


def _assert_verification_matches(
    verification_path: str | Path,
    *,
    release_id: str,
    manifest_sha256: str,
) -> None:
    verification = _read_json_object(verification_path, "verification")
    if verification.get("ok") is not True or verification.get("status") != "verified":
        raise ValueError("verification artifact is not successful")
    if verification.get("release_id") != release_id:
        raise ValueError("verification artifact belongs to another release")
    if verification.get("manifest_sha256") != manifest_sha256:
        raise ValueError("verification artifact does not match the release manifest")
    if verification.get("accepted_release_schema") != ACCEPTED_RELEASE_SCHEMA:
        raise ValueError("verification artifact does not accept corpus_release_v1")


def _load_atomic_claim_packet(
    claims_path: str | Path,
    *,
    max_claims: int,
) -> tuple[list[dict[str, Any]], str]:
    bound = int(max_claims)
    if not 1 <= bound <= MAX_ATOMIC_CLAIMS:
        raise ValueError(f"max_claims must be between 1 and {MAX_ATOMIC_CLAIMS}")
    source = Path(claims_path).expanduser().resolve()
    value = json.loads(source.read_text(encoding="utf-8"))
    raw_claims = value.get("claims") if isinstance(value, Mapping) else value
    if not isinstance(raw_claims, list) or not raw_claims:
        raise ValueError("atomic claim artifact must contain a non-empty claims array")
    if len(raw_claims) > bound:
        raise ValueError(f"atomic claim artifact exceeds max_claims={bound}")
    claims: list[dict[str, Any]] = []
    for index, item in enumerate(raw_claims):
        if not isinstance(item, Mapping):
            raise ValueError(f"atomic claim {index} must be a JSON object")
        claim = dict(item)
        if claim.get("review_status") != "accepted":
            raise ValueError(f"atomic claim {index} is not an accepted decision")
        if claim.get("extractor_model") != ACCEPTED_MODEL:
            raise ValueError(f"atomic claim {index} extractor_model must be {ACCEPTED_MODEL}")
        if claim.get("extractor_schema_version") != ACCEPTED_LABEL_SCHEMA:
            raise ValueError(
                f"atomic claim {index} extractor_schema_version must be {ACCEPTED_LABEL_SCHEMA}"
            )
        claims.append(claim)
    return claims, sha256_text(dumps_json(value))


def _prepare_atomic_import_run(
    conn: sqlite3.Connection,
    *,
    create_run: Callable[..., Any],
    release: Mapping[str, Any],
    pipeline_run_id: str | None,
    claims: Sequence[Mapping[str, Any]] | None,
    claims_sha256: str | None,
    model: str,
    projected_input_count: int,
) -> dict[str, Any]:
    if pipeline_run_id is None and claims is None:
        existing = conn.execute(
            """
            SELECT runs.*
            FROM pipeline_runs AS runs
            WHERE runs.corpus_release_id = ?
              AND runs.run_type = 'atomic_claim_import'
              AND runs.status = 'succeeded'
              AND runs.input_sha256 = ?
              AND EXISTS (
                SELECT 1 FROM atomic_claims
                WHERE atomic_claims.pipeline_run_id = runs.id
              )
            ORDER BY runs.completed_at DESC, runs.id
            LIMIT 1
            """,
            (release["id"], claims_sha256),
        ).fetchone()
        if existing is not None:
            return dict(existing)

    run_id = pipeline_run_id or stable_id(
        str(release["id"]),
        "atomic_claim_import",
        str(claims_sha256),
        prefix="pir_",
    )
    existing = conn.execute("SELECT * FROM pipeline_runs WHERE id = ?", (run_id,)).fetchone()
    if existing is not None:
        run = dict(existing)
        if run["corpus_release_id"] != release["id"] or run["run_type"] != "atomic_claim_import":
            raise ValueError("pipeline run is not an atomic import bound to this release")
        if claims_sha256 is not None and run["input_sha256"] != claims_sha256:
            raise ValueError("pipeline run input hash does not match the supplied atomic packet")
        return run
    if claims_sha256 is None:
        raise ValueError("atomic-claim import requires a deterministic input hash")
    input_count = len(claims) if claims is not None else int(projected_input_count)
    if input_count < 1:
        raise ValueError("atomic-claim import requires at least one input")
    return dict(
        _call_service(
            create_run,
            conn,
            run_id=run_id,
            run_type="atomic_claim_import",
            run_schema="atomic_claim_v1",
            run_schema_version="atomic_claim_import_v1",
            corpus_release_id=release["id"],
            model=model,
            model_version=model,
            status="running",
            parameters={
                "semantic_authority": "managed_app_server_accepted_packet",
                "automatic_model_execution": False,
                "claim_packet_sha256": claims_sha256,
            },
            receipt={"claim_packet_sha256": claims_sha256},
            input_count=input_count,
            input_sha256=claims_sha256,
            created_at=release["created_at"],
            started_at=release["created_at"],
        )
    )


def _build_atomic_claims_transactionally(
    conn: sqlite3.Connection,
    *,
    builder: Callable[..., Any],
    transition_run: Callable[..., Any],
    release_id: str,
    run: Mapping[str, Any],
    claims: Sequence[Mapping[str, Any]] | None,
    claims_sha256: str | None,
    expected_input_count: int,
) -> dict[str, Any]:
    savepoint = "production_atomic_claim_import"
    conn.execute(f"SAVEPOINT {savepoint}")
    try:
        result = _call_service(
            builder,
            conn,
            release_id=release_id,
            pipeline_run_id=run["id"],
            claims=claims,
        )
        if not isinstance(result, Mapping) or result.get("ok") is not True:
            raise ValueError("atomic claim service did not return success")
        inserted = int(result.get("inserted_or_existing") or len(result.get("claim_ids", [])))
        quarantined = int(result.get("quarantined") or 0)
        if inserted < 1 or inserted + quarantined != expected_input_count:
            raise ValueError(
                "atomic claim import did not account for every bounded input: "
                f"inserted={inserted} quarantined={quarantined} expected={expected_input_count}"
            )
        conn.execute(f"RELEASE SAVEPOINT {savepoint}")
    except Exception as exc:
        conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
        conn.execute(f"RELEASE SAVEPOINT {savepoint}")
        failed = _call_service(
            transition_run,
            conn,
            run_id=run["id"],
            status="failed",
            expected_status="running",
            input_count=expected_input_count,
            output_count=0,
            failure_count=1,
            metrics={"accepted": 0, "supplied": expected_input_count},
            error=_sanitized_operator_text(
                f"atomic claim validation failed: {type(exc).__name__}: {str(exc)[:200]}",
                "pipeline error",
            ),
        )
        return {
            "ok": False,
            "error": "atomic_claim_import_failed",
            "message": str(exc)[:300],
            "run": dict(failed),
            "atomic_claims_built": False,
            "canonical_mutation": False,
        }
    claim_ids = [str(item) for item in result.get("claim_ids", [])]
    quarantined = int(result.get("quarantined") or 0)
    output_sha256 = sha256_text(dumps_json({"claim_ids": claim_ids}))
    succeeded = _call_service(
        transition_run,
        conn,
        run_id=run["id"],
        status="succeeded",
        expected_status="running",
        input_count=expected_input_count,
        output_count=len(claim_ids),
        failure_count=quarantined,
        metrics={
            "inserted_or_existing": len(claim_ids),
            "accepted": int(result.get("accepted") or 0),
            "needs_review": int(result.get("needs_review") or 0),
            "pending": int(result.get("pending") or 0),
            "quarantined": quarantined,
            "supplied": expected_input_count,
            "mechanical_projection": bool(result.get("mechanical_projection")),
        },
        receipt={
            "claim_packet_sha256": claims_sha256,
            "claim_ids_sha256": output_sha256,
        },
        output_sha256=output_sha256,
    )
    return {
        "ok": True,
        "run": dict(succeeded),
        "atomic_claims_built": True,
        "atomic_claim_count": len(claim_ids),
        "accepted_atomic_claim_count": int(result.get("accepted") or 0),
        "needs_review_atomic_claim_count": int(result.get("needs_review") or 0),
        "quarantined_atomic_claim_count": quarantined,
    }


def _atomic_claim_count(conn: sqlite3.Connection, release_id: str, run_id: str) -> int:
    return int(
        conn.execute(
            "SELECT COUNT(*) FROM atomic_claims WHERE corpus_release_id = ? AND pipeline_run_id = ?",
            (release_id, run_id),
        ).fetchone()[0]
    )


def _accepted_atomic_claim_count(conn: sqlite3.Connection, release_id: str, run_id: str) -> int:
    return int(
        conn.execute(
            """
            SELECT COUNT(*) FROM atomic_claims
            WHERE corpus_release_id = ? AND pipeline_run_id = ? AND review_status = 'accepted'
            """,
            (release_id, run_id),
        ).fetchone()[0]
    )


def _release_claim_event_count(conn: sqlite3.Connection, release_id: str) -> int:
    return int(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM corpus_release_labels AS members
            JOIN discourse_events AS events ON events.label_id = members.label_id
            WHERE members.corpus_release_id = ?
              AND trim(COALESCE(events.claim_text, '')) <> ''
            """,
            (release_id,),
        ).fetchone()[0]
    )


def _existing_promotion(conn: sqlite3.Connection, release_id: str) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT * FROM corpus_release_promotions
        WHERE corpus_release_id = ? AND action = 'promote'
        ORDER BY promotion_revision DESC
        LIMIT 1
        """,
        (release_id,),
    ).fetchone()
    return dict(row) if row is not None else None


def _sanitized_operator_text(value: str, field: str) -> str:
    text = " ".join(str(value).split())
    if not text or len(text) > 300:
        raise ValueError(f"{field} must be between 1 and 300 characters")
    if "BEGIN RAW TRANSCRIPT" in text.upper():
        raise ValueError(f"raw transcript text is not allowed in {field}")
    if re.search(r"\b(?:sk-[A-Za-z0-9_-]{12,}|Bearer\s+[A-Za-z0-9._~+/-]{12,})", text, re.I):
        raise ValueError(f"credentials are not allowed in {field}")
    return text


def _service_unavailable(required: str, *, operation: str) -> dict[str, Any]:
    return {
        "ok": False,
        "error": "intelligence_service_unavailable",
        "operation": operation,
        "required_module": "research_factory.intelligence",
        "required_function": required,
        "canonical_mutation": False,
        "fallback_used": False,
    }


def _service_failure(operation: str, exc: Exception) -> dict[str, Any]:
    return {
        "ok": False,
        "error": f"{operation}_failed",
        "operation": operation,
        "error_class": type(exc).__name__,
        "message": str(exc)[:500],
        "canonical_mutation": False,
        "fallback_used": False,
    }


def _release_boundary(value: Any, *, operation: str, pilot_id: str) -> dict[str, Any]:
    result = _bounded_value(value)
    if not isinstance(result, dict):
        result = {"result": result}
    ok = bool(result.get("ok", True))
    return {
        **result,
        "ok": ok,
        "operation": operation,
        "pilot_id": pilot_id,
        "expected_episode_count": ACCEPTED_PILOT_EPISODES,
        "expected_source_count": ACCEPTED_PILOT_SOURCES,
        "accepted_release_schema": ACCEPTED_RELEASE_SCHEMA,
        "fallback_used": False,
        "local_source_of_truth": True,
    }


def _schema_migration_version(conn: sqlite3.Connection) -> int:
    try:
        return int(db.schema_migration_version(conn))
    except AttributeError:
        return 0


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return bool(
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name = ?",
            (name,),
        ).fetchone()
    )


def _current_accepted_release_id(conn: sqlite3.Connection) -> str | None:
    if not _table_exists(conn, "current_accepted_corpus_releases"):
        return None
    row = conn.execute(
        """
        SELECT id
        FROM current_accepted_corpus_releases
        ORDER BY promotion_revision DESC, promoted_at DESC
        LIMIT 1
        """
    ).fetchone()
    return str(row["id"]) if row else None


def _pending_outcome_count(conn: sqlite3.Connection) -> int:
    if not _table_exists(conn, "current_accepted_atomic_claims"):
        return 0
    try:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM current_accepted_atomic_claims AS claims
            WHERE claims.forecast_probability IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM current_accepted_outcome_resolutions AS outcomes
                WHERE outcomes.claim_id = claims.id
              )
            """
        ).fetchone()
    except sqlite3.OperationalError:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM atomic_claims AS claims
            WHERE claims.review_status = 'accepted'
              AND claims.forecast_probability IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM outcome_resolution_revisions AS outcomes
                WHERE outcomes.claim_id = claims.id
                  AND outcomes.review_status = 'accepted'
              )
            """
        ).fetchone()
    return int(row[0]) if row else 0


def _has_evidence(value: Any) -> bool:
    if isinstance(value, Mapping):
        return bool(value)
    if isinstance(value, (list, tuple)):
        return bool(value)
    return bool(str(value or "").strip())


def _validate_outcome_pipeline_run(
    conn: sqlite3.Connection,
    *,
    pipeline_run_id: str,
    corpus_release_id: str,
) -> None:
    row = conn.execute(
        """
        SELECT id, run_type, run_schema, corpus_release_id, status
        FROM pipeline_runs
        WHERE id = ?
        """,
        (pipeline_run_id,),
    ).fetchone()
    if row is None:
        raise ValueError("outcome pipeline run does not exist")
    if row["corpus_release_id"] != corpus_release_id:
        raise ValueError("outcome pipeline run belongs to a different corpus release")
    if row["status"] not in {"running", "succeeded"}:
        raise ValueError("outcome pipeline run must be running or succeeded")
    allowed_types = {
        "semantic_reconcile_outcomes",
        "manual_outcome_resolution",
        "outcome_resolution",
        "outcomes_resolve",
    }
    if row["run_type"] not in allowed_types and row["run_schema"] != "outcome_resolution_v1":
        raise ValueError("pipeline run is not authorized for outcome resolution provenance")


def _matching_outcome_resolution(
    conn: sqlite3.Connection,
    resolution: Mapping[str, Any],
) -> sqlite3.Row | None:
    """Return an identical append-only revision, ignoring generated timestamps."""

    rows = conn.execute(
        """
        SELECT *
        FROM outcome_resolution_revisions
        WHERE claim_id = ? AND corpus_release_id = ?
        ORDER BY revision DESC, created_at DESC, id DESC
        """,
        (resolution["claim_id"], resolution["corpus_release_id"]),
    ).fetchall()
    scalar_fields = (
        "resolution_question",
        "due_at",
        "resolution_window_start",
        "resolution_window_end",
        "resolution_criteria",
        "outcome",
        "confidence",
        "rationale",
        "resolver_model",
        "resolver_version",
        "reviewer_version",
        "review_status",
    )
    for row in rows:
        if any(row[field] != resolution.get(field) for field in scalar_fields):
            continue
        if "as_of" in resolution and row["as_of"] != resolution["as_of"]:
            continue
        if "resolved_at" in resolution and row["resolved_at"] != resolution["resolved_at"]:
            continue
        try:
            stored_evidence = json.loads(row["evidence_json"] or "{}")
            stored_authoritative = json.loads(row["authoritative_evidence_json"] or "{}")
        except (TypeError, json.JSONDecodeError):
            continue
        if dumps_json(stored_evidence) != dumps_json(resolution.get("evidence") or {}):
            continue
        if dumps_json(stored_authoritative) != dumps_json(
            resolution.get("authoritative_evidence") or {}
        ):
            continue
        return row
    return None


def _due_outcome_rows(
    conn: sqlite3.Connection,
    *,
    limit: int,
    since: str | None = None,
) -> list[sqlite3.Row]:
    if not _table_exists(conn, "current_accepted_atomic_claims"):
        return []
    try:
        since_clause = " AND claims.observed_at >= ?" if since else ""
        params: tuple[Any, ...] = (since, limit) if since else (limit,)
        return conn.execute(
            f"""
            SELECT claims.id AS claim_id, claims.observed_at, claims.time_horizon
            FROM current_accepted_atomic_claims AS claims
            WHERE claims.forecast_probability IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM current_accepted_outcome_resolutions AS outcomes
                WHERE outcomes.claim_id = claims.id
              )
              {since_clause}
            ORDER BY claims.observed_at, claims.id
            LIMIT ?
            """,
            params,
        ).fetchall()
    except sqlite3.OperationalError:
        since_clause = " AND claims.observed_at >= ?" if since else ""
        params = (since, limit) if since else (limit,)
        return conn.execute(
            f"""
            SELECT claims.id AS claim_id, claims.observed_at, claims.time_horizon
            FROM atomic_claims AS claims
            WHERE claims.review_status = 'accepted'
              AND claims.forecast_probability IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM outcome_resolution_revisions AS outcomes
                WHERE outcomes.claim_id = claims.id
                  AND outcomes.review_status = 'accepted'
              )
              {since_clause}
            ORDER BY claims.observed_at, claims.id
            LIMIT ?
            """,
            params,
        ).fetchall()


def _due_outcome_count(
    conn: sqlite3.Connection,
    *,
    since: str | None = None,
) -> int:
    if not _table_exists(conn, "current_accepted_atomic_claims"):
        return 0
    try:
        since_clause = " AND claims.observed_at >= ?" if since else ""
        params: tuple[Any, ...] = (since,) if since else ()
        row = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM current_accepted_atomic_claims AS claims
            WHERE claims.forecast_probability IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM current_accepted_outcome_resolutions AS outcomes
                WHERE outcomes.claim_id = claims.id
              )
              {since_clause}
            """,
            params,
        ).fetchone()
    except sqlite3.OperationalError:
        since_clause = " AND claims.observed_at >= ?" if since else ""
        params = (since,) if since else ()
        row = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM atomic_claims AS claims
            WHERE claims.review_status = 'accepted'
              AND claims.forecast_probability IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM outcome_resolution_revisions AS outcomes
                WHERE outcomes.claim_id = claims.id
                  AND outcomes.review_status = 'accepted'
              )
              {since_clause}
            """,
            params,
        ).fetchone()
    return int(row[0]) if row else 0


def _semantic_pending_count(conn: sqlite3.Connection, target: str, *, limit: int) -> int:
    queries = {
        "identities": """
            SELECT (
              (SELECT COUNT(*) FROM raw_speaker_mentions WHERE resolution_status IN ('unresolved', 'candidate_match')) +
              (SELECT COUNT(*) FROM raw_actor_mentions WHERE resolution_status IN ('unresolved', 'candidate_match'))
            )
        """,
        "claims": "SELECT COUNT(*) FROM discourse_events",
        "relations": "SELECT COUNT(*) FROM current_accepted_atomic_claims",
        "networks": "SELECT COUNT(*) FROM sources",
    }
    try:
        row = conn.execute(queries[target]).fetchone()
    except sqlite3.OperationalError:
        return 0
    return min(limit, int(row[0]) if row else 0)


def _validate_observer_snapshot(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    forbidden_keys = {"raw_text", "transcript_text", "claim_text", "evidence_text", "output_json", "prompt"}
    found: set[str] = set()

    def walk(value: Any) -> None:
        if isinstance(value, Mapping):
            for key, item in value.items():
                if str(key).lower() in forbidden_keys:
                    found.add(str(key))
                walk(item)
        elif isinstance(value, list):
            for item in value:
                walk(item)

    walk(snapshot)
    privacy = str(snapshot.get("privacy") or "")
    return {
        "ok": privacy == "sanitized_operational_snapshot_no_raw_transcripts" and not found,
        "contract": snapshot.get("contract_version"),
        "privacy": privacy,
        "forbidden_keys": sorted(found),
    }


def _load_verified_manifest(
    manifest_path: str | Path, verification_path: str | Path
) -> tuple[dict[str, Any], dict[str, Any]]:
    manifest_file = Path(manifest_path).expanduser().resolve()
    verification_file = Path(verification_path).expanduser().resolve()
    manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
    verification = json.loads(verification_file.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict) or not isinstance(verification, dict):
        raise ValueError("manifest and verification must be JSON objects")
    manifest_sha = sha256_text(dumps_json(manifest))
    verified_sha = verification.get("manifest_sha256") or verification.get("manifestSha256")
    verified = verification.get("ok") is True or verification.get("status") in {"verified", "accepted"}
    accepted_schema = (
        verification.get("accepted_schema")
        or verification.get("accepted_release_schema")
        or verification.get("schema_version")
    )
    if not verified:
        raise ValueError("release verification is not successful")
    if verified_sha != manifest_sha:
        raise ValueError("verification does not match the manifest hash")
    if accepted_schema not in {None, ACCEPTED_RELEASE_SCHEMA}:
        raise ValueError("verification does not accept corpus_release_v1")
    return manifest, {**verification, "manifest_sha256": manifest_sha}


def _require_canonical_verified_release(
    conn: sqlite3.Connection,
    manifest: Mapping[str, Any],
    verification: Mapping[str, Any],
) -> str:
    release_id = str(
        verification.get("release_id")
        or manifest.get("release_id")
        or manifest.get("id")
        or ""
    )
    if not release_id:
        raise ValueError("verified release artifacts must identify release_id")
    row = conn.execute(
        """
        SELECT id, schema_version, manifest_sha256, status
        FROM corpus_releases WHERE id = ?
        """,
        (release_id,),
    ).fetchone()
    if row is None:
        raise ValueError("verified release does not exist in canonical SQLite")
    if row["schema_version"] != ACCEPTED_RELEASE_SCHEMA or row["status"] != "accepted":
        raise ValueError("canonical release is not an accepted corpus_release_v1 row")
    if row["manifest_sha256"] != verification.get("manifest_sha256"):
        raise ValueError("verified artifact hash does not match canonical SQLite")
    verifier = _intelligence_function("verify_corpus_release")
    if verifier is None:
        raise ValueError("canonical release verifier is unavailable")
    live = _call_service(verifier, conn, release_id=release_id)
    if not isinstance(live, Mapping) or live.get("ok") is not True:
        raise ValueError("canonical release no longer verifies")
    _require_current_promoted_release(conn, release_id)
    return release_id


def _require_current_promoted_release(conn: sqlite3.Connection, release_id: str) -> None:
    """Fail closed unless ``release_id`` is the single current promotion."""

    row = conn.execute(
        "SELECT id FROM current_accepted_corpus_releases LIMIT 1"
    ).fetchone()
    if row is None:
        raise ValueError("no currently promoted accepted corpus release exists")
    if str(row["id"]) != release_id:
        raise ValueError("verified artifact is not the currently promoted corpus release")


def _manifest_member_ids(manifest: Mapping[str, Any], member_kind: str) -> set[str]:
    rows = manifest.get(member_kind)
    if not isinstance(rows, list):
        return set()
    found: set[str] = set()
    for row in rows:
        if isinstance(row, Mapping) and isinstance(row.get("id"), str) and row["id"].strip():
            found.add(str(row["id"]))
        elif isinstance(row, str) and row.strip():
            # Retain compatibility with early release receipts while keeping
            # membership scoped to the explicit top-level collection.
            found.add(row)
    return found


def _manifest_segment_ids(manifest: Mapping[str, Any]) -> set[str]:
    """Compatibility helper for callers that consume release segment IDs."""

    legacy_rows = manifest.get("segment_ids")
    return _manifest_member_ids(manifest, "segments") or {
        str(value)
        for value in (legacy_rows if isinstance(legacy_rows, list) else [])
        if isinstance(value, str) and value.strip()
    }


def _protected_release_membership(
    conn: sqlite3.Connection,
    release_id: str,
    manifest: Mapping[str, Any],
) -> dict[str, set[str]]:
    """Load exact canonical membership and cross-check the immutable artifact."""

    segment_ids = {
        str(row["segment_id"])
        for row in conn.execute(
            """
            SELECT segment_id FROM corpus_release_segments
            WHERE corpus_release_id = ?
            """,
            (release_id,),
        ).fetchall()
    }
    episode_ids = {
        str(row["episode_id"])
        for row in conn.execute(
            """
            SELECT episode_id FROM corpus_release_episodes
            WHERE corpus_release_id = ?
            """,
            (release_id,),
        ).fetchall()
    }
    manifest_segments = _manifest_segment_ids(manifest)
    legacy_episode_rows = manifest.get("episode_ids")
    manifest_episodes = _manifest_member_ids(manifest, "episodes") or {
        str(value)
        for value in (legacy_episode_rows if isinstance(legacy_episode_rows, list) else [])
        if isinstance(value, str) and value.strip()
    }
    if manifest_segments != segment_ids:
        raise ValueError("manifest segment membership differs from canonical SQLite")
    if manifest_episodes != episode_ids:
        raise ValueError("manifest episode membership differs from canonical SQLite")
    return {"segment_ids": segment_ids, "episode_ids": episode_ids}


def _eligible_legacy_label_job_ids(
    conn: sqlite3.Connection,
    release_id: str,
    limit: int,
) -> list[int]:
    rows = conn.execute(
        """
        SELECT jobs.id
        FROM jobs
        LEFT JOIN corpus_release_segments AS protected_segment
          ON protected_segment.corpus_release_id = ?
         AND protected_segment.segment_id = jobs.target_id
        LEFT JOIN segments AS target_segment
          ON target_segment.id = jobs.target_id
        LEFT JOIN corpus_release_episodes AS protected_episode
          ON protected_episode.corpus_release_id = ?
         AND protected_episode.episode_id = target_segment.episode_id
        LEFT JOIN corpus_release_episodes AS protected_target_episode
          ON protected_target_episode.corpus_release_id = ?
         AND protected_target_episode.episode_id = jobs.target_id
        WHERE jobs.status = 'pending'
          AND jobs.job_type = 'label_segment'
          AND protected_segment.segment_id IS NULL
          AND protected_episode.episode_id IS NULL
          AND protected_target_episode.episode_id IS NULL
        ORDER BY jobs.priority, jobs.id
        LIMIT ?
        """,
        (release_id, release_id, release_id, int(limit)),
    ).fetchall()
    return [int(row["id"]) for row in rows]


def _legacy_label_job_counts(conn: sqlite3.Connection, release_id: str) -> dict[str, int]:
    row = conn.execute(
        """
        SELECT
          COUNT(*) AS pending_label_jobs,
          SUM(CASE WHEN protected_segment.segment_id IS NOT NULL
                        OR protected_episode.episode_id IS NOT NULL
                        OR protected_target_episode.episode_id IS NOT NULL
                   THEN 1 ELSE 0 END) AS protected_by_release,
          SUM(CASE WHEN protected_segment.segment_id IS NULL
                        AND protected_episode.episode_id IS NULL
                        AND protected_target_episode.episode_id IS NULL
                   THEN 1 ELSE 0 END) AS eligible_outside_release
        FROM jobs
        LEFT JOIN corpus_release_segments AS protected_segment
          ON protected_segment.corpus_release_id = ?
         AND protected_segment.segment_id = jobs.target_id
        LEFT JOIN segments AS target_segment
          ON target_segment.id = jobs.target_id
        LEFT JOIN corpus_release_episodes AS protected_episode
          ON protected_episode.corpus_release_id = ?
         AND protected_episode.episode_id = target_segment.episode_id
        LEFT JOIN corpus_release_episodes AS protected_target_episode
          ON protected_target_episode.corpus_release_id = ?
         AND protected_target_episode.episode_id = jobs.target_id
        WHERE jobs.status = 'pending'
          AND jobs.job_type = 'label_segment'
        """,
        (release_id, release_id, release_id),
    ).fetchone()
    return {
        "pending_label_jobs": int(row["pending_label_jobs"] or 0),
        "protected_by_release": int(row["protected_by_release"] or 0),
        "eligible_outside_release": int(row["eligible_outside_release"] or 0),
    }


def _backup_before_supersede(
    conn: sqlite3.Connection,
    *,
    output: str | Path | None,
    release_id: str,
    job_ids: Sequence[int],
) -> Path:
    suffix = stable_id(release_id, str(job_ids[0]), str(job_ids[-1]))[:12]
    if output:
        path = Path(output).expanduser().resolve()
    else:
        path = root() / "work" / "pif-ops" / "supersede-backups" / f"{release_id}-{suffix}.sqlite"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise ValueError(f"backup output already exists: {path}")
    with tempfile.NamedTemporaryFile(prefix="supersede-", suffix=".sqlite", dir=path.parent, delete=False) as handle:
        temp_path = Path(handle.name)
    try:
        target = sqlite3.connect(temp_path)
        try:
            conn.commit()
            conn.backup(target)
        finally:
            target.close()
        os.replace(temp_path, path)
    finally:
        if temp_path.exists():
            temp_path.unlink()
    return path


def _verify_supersede_backup(
    path: Path,
    *,
    release_id: str,
    job_ids: Sequence[int],
) -> dict[str, Any]:
    """Verify the immutable pre-mutation backup contains the intended batch."""

    backup = sqlite3.connect(str(path.resolve()))
    backup.row_factory = sqlite3.Row
    backup.execute("PRAGMA query_only = ON")
    try:
        integrity_rows = [str(row[0]) for row in backup.execute("PRAGMA integrity_check").fetchall()]
        integrity_ok = integrity_rows == ["ok"]
        current = backup.execute(
            "SELECT id FROM current_accepted_corpus_releases LIMIT 1"
        ).fetchone()
        release_ok = current is not None and str(current["id"]) == release_id
        if job_ids:
            placeholders = ",".join("?" for _ in job_ids)
            backed_up_rows = backup.execute(
                f"""
                SELECT id FROM jobs
                WHERE id IN ({placeholders})
                  AND status = 'pending'
                  AND job_type = 'label_segment'
                ORDER BY id
                """,
                tuple(int(value) for value in job_ids),
            ).fetchall()
        else:
            backed_up_rows = []
        backed_up_ids = [int(row["id"]) for row in backed_up_rows]
        expected_ids = sorted(int(value) for value in job_ids)
        jobs_ok = backed_up_ids == expected_ids
    finally:
        backup.close()
    digest = _sha256_file(path)
    return {
        "ok": integrity_ok and release_ok and jobs_ok,
        "integrity_check": integrity_rows,
        "current_release_id": str(current["id"]) if current is not None else None,
        "candidate_job_count": len(backed_up_ids),
        "candidate_jobs_verified": jobs_ok,
        "sha256": digest,
    }


def _sanitized_reason(value: str) -> str:
    reason = " ".join(str(value).split())
    if not reason or len(reason) > 300:
        raise ValueError("reason must be between 1 and 300 characters")
    if "BEGIN RAW TRANSCRIPT" in reason.upper():
        raise ValueError("raw transcript text is not allowed in the operational reason")
    if re.search(r"\b(?:sk-[A-Za-z0-9_-]{12,}|Bearer\s+[A-Za-z0-9._~+/-]{12,})", reason, re.I):
        raise ValueError("credentials are not allowed in the operational reason")
    return reason


def _bounded_value(value: Any, *, depth: int = 0) -> Any:
    if depth > 8:
        return "[depth limit]"
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, str):
        return value if len(value) <= 2_000 else value[:2_000] + "...[truncated]"
    if isinstance(value, Mapping):
        return {str(key): _bounded_value(item, depth=depth + 1) for key, item in list(value.items())[:200]}
    if isinstance(value, (list, tuple)):
        return [_bounded_value(item, depth=depth + 1) for item in value[:500]]
    return str(value)[:2_000]


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
