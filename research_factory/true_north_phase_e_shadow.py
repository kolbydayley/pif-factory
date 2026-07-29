"""Fail-closed preflight for the Phase-E production shadow trial."""

from __future__ import annotations

import contextlib
import os
import sqlite3
from pathlib import Path
from typing import Any, Iterator

from . import true_north


SCHEMA_VERSION = "pif_true_north_phase_e_shadow_v1"
EXPERIMENT_ID = "phase-e-three-episode-production-shadow-20260729-v1"
MAX_CALLS = 60
MAX_TOKENS = 1_500_000
EPISODE_COUNT = 3

# Darwin's dataless file flag is not exposed by Python's stat module.
SF_DATALESS = 0x40000000


class PhaseEShadowError(RuntimeError):
    """Raised before provider dispatch when isolation cannot be proved."""


def database_identity(path: str | Path) -> dict[str, Any]:
    """Return metadata without opening or hydrating database contents."""
    resolved = Path(path).expanduser().resolve()
    info = os.stat(resolved)
    flags = int(getattr(info, "st_flags", 0))
    return {
        "path": str(resolved),
        "size_bytes": int(info.st_size),
        "inode": int(info.st_ino),
        "mtime_ns": int(info.st_mtime_ns),
        "flags": flags,
        "dataless": bool(flags & SF_DATALESS),
    }


def strict_isolation_preflight(
    *,
    production_database: str | Path,
    shadow_root: str | Path,
) -> dict[str, Any]:
    """Prove the minimum prerequisites before any production shadow call."""
    database = database_identity(production_database)
    shadow = Path(shadow_root).expanduser().resolve()
    production = Path(database["path"])
    reasons: list[str] = []
    if database["dataless"]:
        reasons.append("production_database_is_dataless_and_unreadable")
    if (
        shadow == production
        or production in shadow.parents
        or shadow in production.parents
    ):
        reasons.append("shadow_store_overlaps_production_database")
    result = {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": EXPERIMENT_ID,
        "budget": {
            "max_calls": MAX_CALLS,
            "max_tokens": MAX_TOKENS,
        },
        "production_database": database,
        "requested_access": "sqlite_uri_mode_ro_plus_query_only",
        "shadow_root": str(shadow),
        "production_writes_allowed": False,
        "queue_mutation_allowed": False,
        "release_or_label_publication_allowed": False,
        "provider_calls_made": 0,
        "provider_tokens": 0,
        "eligible_to_dispatch": not reasons,
        "stop_reasons": reasons,
    }
    result["preflight_sha256"] = true_north.sha256_text(
        true_north.dumps_json(result)
    )
    return result


def require_dispatch_eligibility(preflight: dict[str, Any]) -> None:
    if not preflight.get("eligible_to_dispatch"):
        raise PhaseEShadowError(
            "Phase E stopped before provider dispatch: "
            + ", ".join(preflight.get("stop_reasons", []))
        )


@contextlib.contextmanager
def open_read_only_database(
    production_database: str | Path,
) -> Iterator[sqlite3.Connection]:
    """Open the hydrated authority through two independent read-only guards."""
    path = Path(production_database).expanduser().resolve()
    identity = database_identity(path)
    if identity["dataless"]:
        raise PhaseEShadowError(
            "production database is still an APFS dataless placeholder"
        )
    connection = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)
    try:
        connection.execute("PRAGMA query_only = ON")
        if int(connection.execute("PRAGMA query_only").fetchone()[0]) != 1:
            raise PhaseEShadowError(
                "SQLite query_only enforcement did not activate"
            )
        yield connection
    finally:
        connection.close()


def _assert_write_blocked(connection: sqlite3.Connection) -> None:
    """Prove a schema write is rejected; success is a fatal isolation defect."""
    try:
        connection.execute(
            "CREATE TABLE pif_phase_e_write_probe (id INTEGER)"
        )
    except sqlite3.OperationalError as exc:
        message = str(exc).lower()
        if "readonly" in message or "query only" in message:
            return
        raise PhaseEShadowError(
            f"production write probe failed inconclusively: {exc}"
        ) from exc
    connection.rollback()
    raise PhaseEShadowError(
        "production connection unexpectedly accepted a write"
    )


def executable_isolation_preflight(
    *,
    production_database: str | Path,
    shadow_root: str | Path,
) -> dict[str, Any]:
    """Executable post-hydration gate; never call before production access is authorized."""
    preflight = strict_isolation_preflight(
        production_database=production_database,
        shadow_root=shadow_root,
    )
    require_dispatch_eligibility(preflight)
    production_before = database_identity(production_database)
    shadow = Path(shadow_root).expanduser().resolve()
    shadow.mkdir(parents=True, exist_ok=True)
    if shadow.stat().st_dev == Path(production_database).resolve().stat().st_dev:
        # Same device is safe; identity, not device, is the invariant. Record it
        # so the run receipt cannot imply stronger physical isolation.
        shadow_device_relation = "same_device_distinct_path"
    else:
        shadow_device_relation = "different_device"
    probe = shadow / ".phase-e-shadow-write-probe"
    if probe.exists():
        raise PhaseEShadowError("shadow write probe path already exists")
    probe.write_text("isolated-shadow-ok\n", encoding="utf-8")
    probe.unlink()
    with open_read_only_database(production_database) as connection:
        _assert_write_blocked(connection)
        schema_version = connection.execute(
            "PRAGMA schema_version"
        ).fetchone()[0]
        query_only = connection.execute("PRAGMA query_only").fetchone()[0]
    production_after = database_identity(production_database)
    stable_keys = ("size_bytes", "inode", "mtime_ns", "flags")
    if any(
        production_before[key] != production_after[key]
        for key in stable_keys
    ):
        raise PhaseEShadowError(
            "production database identity changed during read-only preflight"
        )
    result = {
        **preflight,
        "eligible_to_dispatch": True,
        "executable_checks": {
            "sqlite_uri_mode": "ro",
            "query_only": int(query_only),
            "write_probe_rejected": True,
            "schema_version_observed": int(schema_version),
            "production_identity_stable": True,
            "shadow_write_probe_passed": True,
            "shadow_device_relation": shadow_device_relation,
        },
    }
    result["preflight_sha256"] = true_north.sha256_text(
        true_north.dumps_json(result)
    )
    return result


def select_recent_episodes(
    connection: sqlite3.Connection,
    *,
    limit: int = EPISODE_COUNT,
) -> list[dict[str, Any]]:
    """Select recent transcript-ready episodes with an all-Codex baseline."""
    if int(connection.execute("PRAGMA query_only").fetchone()[0]) != 1:
        raise PhaseEShadowError("episode selection requires query_only")
    required = {"episodes", "transcripts", "segments", "labels"}
    present = {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_schema WHERE type='table'"
        )
    }
    if not required.issubset(present):
        raise PhaseEShadowError(
            "production schema lacks episode-selection dependencies"
        )
    atomic_branch = ""
    if "atomic_claims" in present:
        atomic_branch = """
          SELECT DISTINCT episode_id
          FROM atomic_claims
          WHERE review_status = 'accepted'
            AND episode_id IS NOT NULL
            AND (
              lower(extractor_model) LIKE '%codex%'
              OR lower(extractor_model) LIKE '%gpt%'
            )
          UNION
        """
    rows = connection.execute(
        f"""
        WITH codex_baseline AS (
          {atomic_branch}
          SELECT DISTINCT s.episode_id
          FROM labels AS l
          JOIN segments AS s ON s.id = l.segment_id
          WHERE l.status IN ('accepted', 'completed', 'ready')
            AND (
              lower(l.model) LIKE '%codex%'
              OR lower(l.model) LIKE '%gpt%'
            )
        )
        SELECT
          e.id AS episode_id,
          e.source_id,
          e.title,
          e.published_at,
          MAX(t.updated_at) AS transcript_updated_at
        FROM episodes AS e
        JOIN codex_baseline AS b ON b.episode_id = e.id
        JOIN transcripts AS t ON t.episode_id = e.id
        WHERE t.status = 'ready'
        GROUP BY e.id, e.source_id, e.title, e.published_at
        ORDER BY
          COALESCE(e.published_at, MAX(t.updated_at), e.updated_at) DESC,
          e.id ASC
        LIMIT ?
        """,
        (int(limit),),
    ).fetchall()
    selected = [
        {
            "episode_id": str(row[0]),
            "source_id": str(row[1]),
            "title": str(row[2]),
            "published_at": row[3],
            "transcript_updated_at": row[4],
        }
        for row in rows
    ]
    if len(selected) != limit:
        raise PhaseEShadowError(
            f"only {len(selected)} eligible episodes; {limit} required"
        )
    return selected


def measurement_plan() -> dict[str, Any]:
    """Return the frozen drift and cost contract without reading production."""
    return {
        "schema_version": "pif_true_north_phase_e_measurement_plan_v1",
        "episode_count": EPISODE_COUNT,
        "selection": (
            "three most recent transcript-ready episodes with accepted or "
            "completed Codex/GPT baseline outputs"
        ),
        "stage_drift": [
            {
                "stage": "candidate_disposition",
                "metrics": [
                    "three_class_macro_f1",
                    "retained_value_recall",
                    "junk_escape_rate",
                ],
            },
            {
                "stage": "atomic_decomposition",
                "metrics": [
                    "acceptable_atomic_count_rate",
                    "claim_alignment_rate",
                    "claim_text_faithfulness",
                ],
            },
            {
                "stage": "speaker_and_actor",
                "metrics": [
                    "speaker_exactness",
                    "reported_actor_exactness",
                    "hallucination_rate",
                ],
            },
            {
                "stage": "canonicalization",
                "metrics": [
                    "subject_cluster_pairwise_f1",
                    "relation_macro_f1",
                    "singleton_preservation_rate",
                ],
            },
        ],
        "retained_valid_atomic_definition": (
            "atomic reaches the isolated shadow corpus, passes evidence and "
            "schema validation, and is not rejected or quarantined"
        ),
        "cost": {
            "hybrid": [
                "provider_calls",
                "input_tokens",
                "output_tokens",
                "total_tokens",
                "calls_per_retained_valid_atomic",
                "tokens_per_retained_valid_atomic",
            ],
            "all_codex_baseline": [
                "receipt_calls",
                "receipt_input_tokens",
                "receipt_output_tokens",
                "receipt_total_tokens",
                "calls_per_retained_valid_atomic",
                "tokens_per_retained_valid_atomic",
            ],
            "ratios": [
                "hybrid_calls_per_atomic_over_all_codex",
                "hybrid_tokens_per_atomic_over_all_codex",
            ],
            "missing_receipt_policy": (
                "fail measurement; never estimate missing baseline usage"
            ),
        },
        "isolation": {
            "production_database": "mode=ro plus PRAGMA query_only=ON",
            "all_outputs": "distinct isolated shadow root",
            "queue_mutation": False,
            "release_or_label_publication": False,
            "before_after_identity_check": True,
        },
        "declared_budget": {
            "max_calls": MAX_CALLS,
            "max_tokens": MAX_TOKENS,
            "status": "recorded_not_opened",
        },
    }


def certified_hybrid_budget_preflight(
    connection: sqlite3.Connection,
    *,
    episode_ids: list[str],
    max_calls: int = MAX_CALLS,
) -> dict[str, Any]:
    """Fail closed when the exact certified packet topology cannot fit.

    The certified disposition ensemble consists of two independent GLM
    segment packets. The frozen Task-5 decomposition contributes one further
    GLM segment packet before the compound-only Sol and adjudication lanes.
    Combining segments would change the evaluated input contract, so this
    computes an exact-contract lower bound.
    """
    if int(connection.execute("PRAGMA query_only").fetchone()[0]) != 1:
        raise PhaseEShadowError("budget preflight requires query_only")
    if not episode_ids:
        raise PhaseEShadowError("budget preflight requires episodes")
    placeholders = ",".join("?" for _ in episode_ids)
    rows = connection.execute(
        f"""
        SELECT episode_id, COUNT(*) AS segment_count
        FROM segments
        WHERE episode_id IN ({placeholders})
        GROUP BY episode_id
        """,
        tuple(episode_ids),
    ).fetchall()
    counts = {str(row[0]): int(row[1]) for row in rows}
    if set(counts) != set(episode_ids):
        missing = sorted(set(episode_ids) - set(counts))
        raise PhaseEShadowError(
            "selected episode has no segment scope: " + ", ".join(missing)
        )
    segment_count = sum(counts.values())
    fixed_stages = {
        "disposition_glm_pass_a": segment_count,
        "disposition_glm_pass_b": segment_count,
        "task5_glm_decomposition": segment_count,
    }
    minimum_calls = sum(fixed_stages.values())
    result = {
        "schema_version": "pif_true_north_phase_e_budget_preflight_v1",
        "experiment_id": EXPERIMENT_ID,
        "episode_ids": list(episode_ids),
        "segment_counts": counts,
        "segment_count": segment_count,
        "exact_contract_fixed_stage_calls": fixed_stages,
        "minimum_provider_calls_before_compound_sol_lanes": minimum_calls,
        "compound_sol_decomposition_calls": (
            "not_needed_to_prove_ineligibility"
        ),
        "compound_sol_adjudication_calls": (
            "not_needed_to_prove_ineligibility"
        ),
        "max_calls": int(max_calls),
        "eligible_to_dispatch": minimum_calls <= int(max_calls),
        "stop_reason": (
            None
            if minimum_calls <= int(max_calls)
            else "certified_exact_contract_exceeds_declared_call_ceiling"
        ),
        "semantic_contract_change_required_to_fit": (
            "cross_segment_batching"
            if minimum_calls > int(max_calls)
            else None
        ),
        "provider_calls_made": 0,
        "provider_tokens": 0,
    }
    result["preflight_sha256"] = true_north.sha256_text(
        true_north.dumps_json(result)
    )
    return result


def require_hybrid_budget_eligibility(preflight: dict[str, Any]) -> None:
    if not preflight.get("eligible_to_dispatch"):
        raise PhaseEShadowError(
            "Phase E stopped before provider dispatch: "
            + str(preflight.get("stop_reason"))
        )
