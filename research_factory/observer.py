from __future__ import annotations

import datetime as dt
import json
import os
import re
import shutil
import sqlite3
import subprocess
import urllib.request
from pathlib import Path
from typing import Any

from . import db
from .fast_quality import quality_velocity
from .paths import exports_dir, root
from .research_queue import sync_queue_envelopes
from .scale_gate import build_scale_gate_report
from .util import now_iso, write_text_atomic


DEFAULT_OBSERVER_SCALE_PILOT_ID = "scale-gate-v31-2026-07-02"
CODEX_CRON_TIMEOUT_SECONDS = 2


def build_snapshot(conn) -> dict[str, Any]:
    generated_at = now_iso()
    queue_sync_error: str | None = None
    try:
        sync_queue_envelopes(conn)
    except sqlite3.OperationalError as exc:
        if "database is locked" not in str(exc).lower():
            raise
        queue_sync_error = "queue envelope refresh skipped because the database was locked"
    # Build the public observer from operational primitives only.  Analytical
    # views (claim text, subjects, people, networks, samples, and transcript-
    # derived findings) belong exclusively to the local query service.
    counts = db.counts(conn)
    queues = _operational_queues(conn)
    runs = _operational_runs(conn, generated_at=generated_at)
    failures = _operational_failures(conn, generated_at=generated_at, queue_sync_error=queue_sync_error)
    intervention_flags = _operational_intervention_flags(counts, queues, failures)
    return {
        "contract_version": "railway-operational-v2",
        "generated_at": generated_at,
        "privacy": "sanitized_operational_snapshot_no_raw_transcripts",
        "counts": counts,
        "queues": queues,
        "runs": runs,
        "failures": failures,
        "artifacts": _recent_artifacts(exports_dir()),
        "intervention_flags": intervention_flags,
    }


def _operational_queues(conn) -> dict[str, Any]:
    by_status = {
        str(row["status"]): int(row["count"])
        for row in conn.execute("SELECT status, COUNT(*) AS count FROM jobs GROUP BY status").fetchall()
    }
    by_lane_type_status = [
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
    by_role_content_status = [
        dict(row)
        for row in conn.execute(
            """
            SELECT queue_envelopes.worker_role, queue_envelopes.content_type,
                   jobs.status, COUNT(*) AS count
            FROM jobs
            JOIN queue_envelopes ON queue_envelopes.job_id = jobs.id
            GROUP BY queue_envelopes.worker_role, queue_envelopes.content_type, jobs.status
            ORDER BY queue_envelopes.worker_role, queue_envelopes.content_type, jobs.status
            """
        ).fetchall()
    ]
    privacy_tiers = {
        str(row["privacy_tier"]): int(row["count"])
        for row in conn.execute(
            "SELECT privacy_tier, COUNT(*) AS count FROM queue_envelopes GROUP BY privacy_tier"
        ).fetchall()
    }
    remote_claimable = {
        str(row["privacy_tier"]): int(row["count"])
        for row in conn.execute(
            """
            SELECT queue_envelopes.privacy_tier, COUNT(*) AS count
            FROM queue_envelopes
            JOIN jobs ON jobs.id = queue_envelopes.job_id
            WHERE queue_envelopes.remote_claimable = 1
              AND jobs.status = 'pending'
              AND jobs.attempts < jobs.max_attempts
            GROUP BY queue_envelopes.privacy_tier
            """
        ).fetchall()
    }
    return {
        "by_status": by_status,
        "by_lane_type_status": by_lane_type_status,
        "by_role_content_status": by_role_content_status,
        "privacy_tiers": privacy_tiers,
        "remote_claimable_by_privacy_tier": remote_claimable,
    }


def _operational_runs(conn, *, generated_at: str) -> dict[str, Any]:
    label_runs = [
        _safe_label_run(row)
        for row in conn.execute(
            """
            SELECT id, job_id, segment_id, label_pack, model, status, prompt_path,
                   output_path, claimed_at, completed_at
            FROM label_runs
            ORDER BY updated_at DESC
            LIMIT 100
            """
        ).fetchall()
    ]
    worker_runs = [
        dict(row)
        for row in conn.execute(
            """
            SELECT worker_role, status, COUNT(*) AS count,
                   COALESCE(SUM(claimed_jobs), 0) AS claimed_jobs,
                   COALESCE(SUM(completed_jobs), 0) AS completed_jobs,
                   COALESCE(SUM(failed_jobs), 0) AS failed_jobs,
                   MAX(updated_at) AS last_observed_at
            FROM worker_runs
            GROUP BY worker_role, status
            ORDER BY worker_role, status
            """
        ).fetchall()
    ]
    local_active_claims = int(
        conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM jobs
            WHERE status = 'claimed'
              AND (leased_until IS NULL OR leased_until >= ?)
              AND COALESCE(lease_owner, '') NOT LIKE 'mcp-%'
            """,
            (generated_at,),
        ).fetchone()["count"]
    )
    remote_active_claims = int(
        conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM jobs
            WHERE status = 'claimed'
              AND (leased_until IS NULL OR leased_until >= ?)
              AND COALESCE(lease_owner, '') LIKE 'mcp-%'
            """,
            (generated_at,),
        ).fetchone()["count"]
    )
    local_worker_observed = bool(
        conn.execute(
            """
            SELECT 1 FROM worker_runs
            WHERE COALESCE(worker_id, '') NOT LIKE 'mcp-%'
            LIMIT 1
            """
        ).fetchone()
    )
    scheduler = _codex_scheduler_status()
    scheduled_compute_enabled = any(
        bool(job.get("enabled"))
        and any(
            marker in str(job.get("name") or "")
            for marker in ("local-extractor", "local-reviewer", "babysitter", "watchdog", "supervisor", "prime-control")
        )
        for job in scheduler.get("jobs") or []
    )
    phase2_configured = os.environ.get("PIF_MCP_PHASE2_ENABLED", "0") == "1"
    railway_module = os.environ.get("PIF_RAILWAY_MODULE", "").strip()
    observer_configured = bool(os.environ.get("RAILWAY_UI_URL")) or (
        railway_module in {"", "research_factory.ui_server"}
        and bool(os.environ.get("RAILWAY_ENVIRONMENT"))
    )
    broker_configured = bool(os.environ.get("PIF_MCP_PUBLIC_URL")) or railway_module in {
        "research_factory.mcp_server",
        "research_factory.mcp_broker",
    }
    if local_active_claims:
        local_worker_state = "active"
    elif scheduler.get("state") == "available":
        local_worker_state = "scheduled_idle" if scheduled_compute_enabled else "disabled"
    else:
        local_worker_state = "idle" if local_worker_observed else "not_observed"
    return {
        "label_runs": label_runs,
        "worker_runs": worker_runs,
        "scheduler": scheduler,
        "worker_status": {
            "local_headless": {
                "state": local_worker_state,
                "active_claims": local_active_claims,
                "evidence": "database_leases_codex_cron_and_worker_runs",
            },
            "remote": {
                "state": "active" if remote_active_claims else ("configured_idle" if phase2_configured else "disabled"),
                "active_claims": remote_active_claims,
                "evidence": "database_leases_and_PIF_MCP_PHASE2_ENABLED",
            },
        },
        "service_status": {
            "observer_ui": {
                "state": "configured" if observer_configured else "not_observed",
                "evidence": "environment_configuration_only",
            },
            "mcp_broker": {
                "state": "configured" if broker_configured else "not_configured",
                "evidence": "environment_configuration_only",
            },
            "railway_compute_workers": {
                "state": "not_configured",
                "evidence": "observer_contract_prohibits_remote_compute",
            },
        },
    }


def _codex_scheduler_status() -> dict[str, Any]:
    if os.environ.get("RAILWAY_ENVIRONMENT"):
        return {
            "state": "unavailable",
            "jobs": [],
            "enabled_count": 0,
            "disabled_count": 0,
            "evidence": "railway_has_no_local_codex_scheduler",
        }
    binary = _find_codex_cron()
    if binary is None:
        return {
            "state": "unavailable",
            "jobs": [],
            "enabled_count": 0,
            "disabled_count": 0,
            "evidence": "codex_cron_binary_not_found",
        }
    try:
        listed = subprocess.run(
            [str(binary), "list"],
            capture_output=True,
            text=True,
            timeout=CODEX_CRON_TIMEOUT_SECONDS,
            check=False,
        )
        status = subprocess.run(
            [str(binary), "status"],
            capture_output=True,
            text=True,
            timeout=CODEX_CRON_TIMEOUT_SECONDS,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return {
            "state": "unavailable",
            "jobs": [],
            "enabled_count": 0,
            "disabled_count": 0,
            "evidence": "codex_cron_probe_failed",
        }
    if listed.returncode != 0 or status.returncode != 0:
        return {
            "state": "unavailable",
            "jobs": [],
            "enabled_count": 0,
            "disabled_count": 0,
            "evidence": "codex_cron_probe_failed",
        }
    jobs = _parse_codex_cron_output(listed.stdout, status.stdout)
    return {
        "state": "available",
        "jobs": jobs,
        "enabled_count": sum(1 for job in jobs if job["enabled"]),
        "disabled_count": sum(1 for job in jobs if not job["enabled"]),
        "evidence": "codex_cron_list_and_status",
    }


def _find_codex_cron() -> Path | None:
    discovered = shutil.which("codex-cron")
    if discovered:
        return Path(discovered)
    compatibility_path = Path.home() / ".codex" / "bin" / "codex-cron"
    return compatibility_path if compatibility_path.is_file() else None


def _parse_codex_cron_output(list_output: str, status_output: str) -> list[dict[str, Any]]:
    live_status: dict[str, str] = {}
    for line in status_output.splitlines():
        name, separator, value = line.partition(":")
        sanitized_name = _safe_cron_name(name)
        if separator and sanitized_name:
            live_status[sanitized_name] = value.strip().lower()
    jobs: list[dict[str, Any]] = []
    for line in list_output.splitlines():
        fields = line.split("\t")
        if len(fields) < 2:
            continue
        name = _safe_cron_name(fields[0])
        if not name or not _is_project_operational_cron(name):
            continue
        configured_enabled = fields[1].strip().lower() == "enabled"
        observed = live_status.get(name, "")
        enabled = configured_enabled and observed not in {"disabled", "not loaded", "not_loaded"}
        jobs.append({"name": name, "enabled": enabled})
    return sorted(jobs, key=lambda job: str(job["name"]))


def _safe_cron_name(value: object) -> str | None:
    text = str(value).strip()
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,79}", text):
        return None
    return text


def _is_project_operational_cron(name: str) -> bool:
    if name == "pif-discovery-thread-compactor":
        return False
    return name.startswith("pif-") or name.startswith("podcast-intelligence-")


def _operational_failures(conn, *, generated_at: str, queue_sync_error: str | None) -> dict[str, Any]:
    failed_jobs = [
        dict(row)
        for row in conn.execute(
            """
            SELECT lane, job_type, COUNT(*) AS count
            FROM jobs
            WHERE status = 'failed'
            GROUP BY lane, job_type
            ORDER BY count DESC, lane, job_type
            """
        ).fetchall()
    ]
    expired_claims = [
        dict(row)
        for row in conn.execute(
            """
            SELECT lane, job_type, COUNT(*) AS count
            FROM jobs
            WHERE status = 'claimed'
              AND leased_until IS NOT NULL
              AND leased_until < ?
            GROUP BY lane, job_type
            ORDER BY count DESC, lane, job_type
            """,
            (generated_at,),
        ).fetchall()
    ]
    failed_label_runs = [
        dict(row)
        for row in conn.execute(
            """
            SELECT label_pack, model, COUNT(*) AS count
            FROM label_runs
            WHERE status = 'failed'
            GROUP BY label_pack, model
            ORDER BY count DESC, label_pack, model
            """
        ).fetchall()
    ]
    missing_outputs = sum(
        1
        for row in conn.execute(
            "SELECT output_path FROM label_runs WHERE status = 'claimed' AND output_path IS NOT NULL"
        ).fetchall()
        if not Path(row["output_path"]).exists()
    )
    return {
        "jobs": failed_jobs,
        "expired_claims": expired_claims,
        "label_runs": failed_label_runs,
        "missing_claimed_run_outputs": missing_outputs,
        "queue_sync": "locked" if queue_sync_error else "ok",
    }


def _operational_intervention_flags(
    counts: dict[str, int], queues: dict[str, Any], failures: dict[str, Any]
) -> list[dict[str, str]]:
    flags: list[dict[str, str]] = []
    failed_count = sum(int(item["count"]) for item in failures["jobs"])
    expired_count = sum(int(item["count"]) for item in failures["expired_claims"])
    if failed_count:
        flags.append({"severity": "warning", "message": f"{failed_count} failed job(s) need review."})
    if expired_count:
        flags.append({"severity": "critical", "message": f"{expired_count} claimed job lease(s) have expired."})
    if int(failures["missing_claimed_run_outputs"]):
        flags.append({
            "severity": "critical",
            "message": f"{failures['missing_claimed_run_outputs']} claimed run output artifact(s) are missing.",
        })
    if failures["queue_sync"] != "ok":
        flags.append({"severity": "warning", "message": "Queue envelope refresh was skipped because the database was locked."})
    claimed_count = int((queues.get("by_status") or {}).get("claimed", 0))
    if claimed_count > 25:
        flags.append({"severity": "warning", "message": "Large claimed-job backlog; check worker health."})
    if counts.get("segments", 0) and not counts.get("labels", 0):
        flags.append({"severity": "critical", "message": "Segments exist but no completed labels are recorded."})
    return flags


def _scale_gate_snapshot(conn, *, pilot_id: str) -> dict[str, Any]:
    try:
        report = build_scale_gate_report(conn, pilot_id=pilot_id)
    except Exception as exc:
        return {
            "ok": False,
            "pilot_id": pilot_id,
            "gate_state": "unknown",
            "error": str(exc)[:300],
            "privacy": "sanitized_operational_report_no_raw_transcripts",
        }
    return {
        "ok": bool(report.get("ok")),
        "pilot_id": report.get("pilot_id"),
        "generated_at": report.get("generated_at"),
        "gate_state": report.get("gate_state"),
        "failed_checks": report.get("failed_checks") or [],
        "selected_episodes": (report.get("episodes") or {}).get("selected"),
        "selected_segments": (report.get("segments") or {}).get("selected_segments"),
        "completed_labels": (report.get("segments") or {}).get("completed_labels"),
        "failed_label_jobs": (report.get("segments") or {}).get("failed_label_jobs"),
        "pending_label_jobs": (report.get("segments") or {}).get("pending_label_jobs"),
        "audits": report.get("audits"),
        "semantic_reviewer_audit": report.get("semantic_reviewer_audit"),
        "identity_and_graph": report.get("identity_and_graph"),
        "quality_and_graph_blockers": report.get("quality_and_graph_blockers"),
        "privacy": "sanitized_operational_report_no_raw_transcripts",
    }


def counts_by_status(conn, table: str) -> dict[str, int]:
    return {
        row["status"]: int(row["count"])
        for row in conn.execute(f"SELECT status, COUNT(*) AS count FROM {table} GROUP BY status").fetchall()
    }


def _derived_table_counts(conn) -> dict[str, int]:
    tables = [
        "content_sources",
        "content_items",
        "content_artifacts",
        "content_spans",
        "context_packages",
        "extraction_runs",
        "queue_envelopes",
        "worker_runs",
        "job_claims",
        "output_submissions",
        "queue_events",
        "coded_observations",
        "topic_mentions",
        "term_mentions",
        "entity_mentions",
        "speaker_positions",
        "relationship_edges",
        "product_signals",
        "release_signal_links",
        "concepts",
        "concept_aliases",
        "concept_versions",
        "discourse_events",
        "discourse_event_contexts",
        "term_usages",
        "frame_usages",
        "actor_positions",
        "concept_candidates",
        "shift_signals",
        "signal_runs",
        "episode_context_runs",
        "reviewer_audits",
        "raw_speaker_mentions",
        "raw_actor_mentions",
        "identity_resolution_candidates",
        "claim_clusters",
        "agreement_edges",
        "disagreement_edges",
        "canonical_people",
        "canonical_orgs",
        "canonical_products",
        "canonical_models",
        "podcast_guest_edges",
        "person_concept_edges",
        "expert_authority_scores",
    ]
    return {table: int(conn.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()["count"]) for table in tables}


def _research_queue_metrics(conn) -> dict[str, Any]:
    depth_by_role = [
        dict(row)
        for row in conn.execute(
            """
            SELECT queue_envelopes.worker_role, queue_envelopes.content_type, jobs.status, COUNT(*) AS count
            FROM jobs
            JOIN queue_envelopes ON queue_envelopes.job_id = jobs.id
            GROUP BY queue_envelopes.worker_role, queue_envelopes.content_type, jobs.status
            ORDER BY queue_envelopes.worker_role, queue_envelopes.content_type, jobs.status
            """
        ).fetchall()
    ]
    privacy = {
        row["privacy_tier"]: int(row["count"])
        for row in conn.execute(
            """
            SELECT privacy_tier, COUNT(*) AS count
            FROM queue_envelopes
            GROUP BY privacy_tier
            """
        ).fetchall()
    }
    worker_runs = [
        dict(row)
        for row in conn.execute(
            """
            SELECT worker_role, status, COUNT(*) AS count,
                   COALESCE(SUM(claimed_jobs), 0) AS claimed_jobs,
                   COALESCE(SUM(completed_jobs), 0) AS completed_jobs,
                   COALESCE(SUM(failed_jobs), 0) AS failed_jobs
            FROM worker_runs
            GROUP BY worker_role, status
            ORDER BY worker_role, status
            """
        ).fetchall()
    ]
    remote_claimable = {
        row["privacy_tier"]: int(row["count"])
        for row in conn.execute(
            """
            SELECT queue_envelopes.privacy_tier, COUNT(*) AS count
            FROM queue_envelopes
            JOIN jobs ON jobs.id = queue_envelopes.job_id
            WHERE queue_envelopes.remote_claimable = 1
              AND jobs.status = 'pending'
              AND jobs.attempts < jobs.max_attempts
            GROUP BY queue_envelopes.privacy_tier
            """
        ).fetchall()
    }
    remote_leases = [
        dict(row)
        for row in conn.execute(
            """
            SELECT queue_envelopes.worker_role, queue_envelopes.privacy_tier, COUNT(*) AS count
            FROM jobs
            JOIN queue_envelopes ON queue_envelopes.job_id = jobs.id
            WHERE jobs.status = 'claimed'
              AND jobs.lease_owner LIKE 'mcp-%'
            GROUP BY queue_envelopes.worker_role, queue_envelopes.privacy_tier
            ORDER BY queue_envelopes.worker_role, queue_envelopes.privacy_tier
            """
        ).fetchall()
    ]
    remote_submissions = [
        dict(row)
        for row in conn.execute(
            """
            SELECT status, COUNT(*) AS count
            FROM output_submissions
            GROUP BY status
            ORDER BY status
            """
        ).fetchall()
    ]
    import_failures = int(
        conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM output_submissions
            WHERE status IN ('rejected', 'validation_failed', 'import_failed')
            """
        ).fetchone()["count"]
    )
    return {
        "project_mode": "research_intelligence_factory",
        "local_source_of_truth": True,
        "headless_codex_workers_enabled": True,
        "mcp_remote_worker_enabled": False,
        "mcp_broker_enabled": False,
        "remote_worker_contract_ready": True,
        "depth_by_role": depth_by_role,
        "privacy_tiers": privacy,
        "remote_claimable_by_privacy_tier": remote_claimable,
        "active_remote_leases": remote_leases,
        "remote_output_submissions": remote_submissions,
        "remote_import_failures": import_failures,
        "worker_runs": worker_runs,
    }


def _content_metrics(conn) -> dict[str, Any]:
    content_types = [
        dict(row)
        for row in conn.execute(
            """
            SELECT content_type, acquisition_status, privacy_tier, COUNT(*) AS count
            FROM content_items
            GROUP BY content_type, acquisition_status, privacy_tier
            ORDER BY content_type, acquisition_status, privacy_tier
            """
        ).fetchall()
    ]
    artifact_types = [
        dict(row)
        for row in conn.execute(
            """
            SELECT artifact_type, source_kind, status, COUNT(*) AS count
            FROM content_artifacts
            GROUP BY artifact_type, source_kind, status
            ORDER BY artifact_type, source_kind, status
            """
        ).fetchall()
    ]
    source_types = [
        dict(row)
        for row in conn.execute(
            """
            SELECT source_type, category, COUNT(*) AS count
            FROM content_sources
            GROUP BY source_type, category
            ORDER BY source_type, category
            """
        ).fetchall()
    ]
    return {
        "source_types": source_types,
        "content_types": content_types,
        "artifact_types": artifact_types,
        "supported_content_types": [
            "podcast_episode",
            "blog_post",
            "substack_post",
            "youtube_video",
            "paper",
            "release_note",
            "docs_page",
            "newsletter_issue",
        ],
    }


def _coding_metrics(conn) -> dict[str, Any]:
    labels_row = conn.execute(
        """
        SELECT
          COUNT(DISTINCT labels.id) AS v2_labels,
          COUNT(coded_observations.id) AS observations
        FROM labels
        JOIN segments ON segments.id = labels.segment_id
        LEFT JOIN coded_observations ON coded_observations.label_id = labels.id
        WHERE labels.label_pack = 'ai_discourse_v2'
        """
    ).fetchone()
    words = _distinct_labeled_segment_words(conn, "ai_discourse_v2")
    observations = int(labels_row["observations"] or 0)
    labels_by_pack = [dict(item) for item in conn.execute("SELECT label_pack, COUNT(*) AS count FROM labels GROUP BY label_pack ORDER BY label_pack").fetchall()]
    audits = [
        dict(item)
        for item in conn.execute(
            "SELECT label_pack, status, COUNT(*) AS count, AVG(score) AS avg_score FROM quality_audits GROUP BY label_pack, status ORDER BY label_pack, status"
        ).fetchall()
    ]
    v3 = _pack_discourse_metrics(conn, "ai_discourse_v3")
    v31 = _pack_discourse_metrics(conn, "ai_discourse_v3_1")
    return {
        "labels_by_pack": labels_by_pack,
        "quality_audits": audits,
        "v2_labels": int(labels_row["v2_labels"] or 0),
        "v2_observations": observations,
        "v2_observations_per_1000_segment_words": round((observations / words) * 1000, 2) if words else 0,
        "v3_labels": v3["labels"],
        "v3_discourse_events": v3["events"],
        "v3_events_per_1000_segment_words": v3["events_per_1000_words"],
        "v3_1_labels": v31["labels"],
        "v3_1_discourse_events": v31["events"],
        "v3_1_events_per_1000_segment_words": v31["events_per_1000_words"],
        "v3_1_full_episode_context_required": True,
        "v3_1_local_draft_disabled": True,
    }


def _pack_discourse_metrics(conn, label_pack: str) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT
          COUNT(DISTINCT labels.id) AS labels,
          COUNT(DISTINCT discourse_events.id) AS events,
          COUNT(DISTINCT discourse_event_contexts.discourse_event_id) AS context_rows
        FROM labels
        JOIN segments ON segments.id = labels.segment_id
        LEFT JOIN discourse_events ON discourse_events.label_id = labels.id
        LEFT JOIN discourse_event_contexts ON discourse_event_contexts.discourse_event_id = discourse_events.id
        WHERE labels.label_pack = ?
        """,
        (label_pack,),
    ).fetchone()
    words = _distinct_labeled_segment_words(conn, label_pack)
    events = int(row["events"] or 0)
    return {
        "labels": int(row["labels"] or 0),
        "events": events,
        "context_rows": int(row["context_rows"] or 0),
        "events_per_1000_words": round((events / words) * 1000, 2) if words else 0,
    }


def _distinct_labeled_segment_words(conn, label_pack: str) -> int:
    row = conn.execute(
        """
        SELECT COALESCE(SUM(word_count), 0) AS segment_words
        FROM (
          SELECT DISTINCT segments.id, segments.word_count
          FROM labels
          JOIN segments ON segments.id = labels.segment_id
          WHERE labels.label_pack = ?
        )
        """,
        (label_pack,),
    ).fetchone()
    return int(row["segment_words"] or 0)


def _useful_signal_metrics(conn) -> dict[str, Any]:
    candidate_status = {
        row["status"]: int(row["count"])
        for row in conn.execute("SELECT status, COUNT(*) AS count FROM concept_candidates GROUP BY status").fetchall()
    }
    alias_status = {
        row["status"]: int(row["count"])
        for row in conn.execute("SELECT status, COUNT(*) AS count FROM concept_aliases GROUP BY status").fetchall()
    }
    signal_type_counts = [
        dict(row)
        for row in conn.execute(
            "SELECT signal_type, COUNT(*) AS count, AVG(score) AS avg_score FROM shift_signals GROUP BY signal_type ORDER BY count DESC, signal_type"
        ).fetchall()
    ]
    burst_terms = [
        dict(row)
        for row in conn.execute(
            """
            SELECT term_a AS term, COUNT(*) AS count, MAX(score) AS max_score
            FROM shift_signals
            WHERE signal_type = 'term_burst'
            GROUP BY term_a
            ORDER BY max_score DESC, count DESC
            LIMIT 20
            """
        ).fetchall()
    ]
    actor_stance_changes = int(
        conn.execute("SELECT COUNT(*) AS count FROM shift_signals WHERE signal_type = 'stance_shift'").fetchone()["count"]
    )
    audit = conn.execute(
        "SELECT COUNT(*) AS count, AVG(score) AS avg_score FROM quality_audits WHERE label_pack = 'ai_discourse_v3'"
    ).fetchone()
    v31_audit = conn.execute(
        "SELECT COUNT(*) AS count, AVG(score) AS avg_score FROM quality_audits WHERE label_pack = 'ai_discourse_v3_1'"
    ).fetchone()
    episode_context_runs = {
        row["status"]: int(row["count"])
        for row in conn.execute("SELECT status, COUNT(*) AS count FROM episode_context_runs GROUP BY status").fetchall()
    }
    raw_speaker_status = {
        row["resolution_status"]: int(row["count"])
        for row in conn.execute("SELECT resolution_status, COUNT(*) AS count FROM raw_speaker_mentions GROUP BY resolution_status").fetchall()
    }
    raw_actor_status = {
        row["resolution_status"]: int(row["count"])
        for row in conn.execute("SELECT resolution_status, COUNT(*) AS count FROM raw_actor_mentions GROUP BY resolution_status").fetchall()
    }
    reviewer_status = {
        row["status"]: int(row["count"])
        for row in conn.execute(
            """
            WITH ranked AS (
              SELECT
                status,
                ROW_NUMBER() OVER (
                  PARTITION BY pilot_id, episode_id, review_mode
                  ORDER BY COALESCE(completed_at, updated_at, created_at) DESC, id DESC
                ) AS rn
              FROM reviewer_audits
            )
            SELECT status, COUNT(*) AS count
            FROM ranked
            WHERE rn = 1
            GROUP BY status
            """
        ).fetchall()
    }
    reviewer_scores = conn.execute(
        """
        WITH latest_completed AS (
          SELECT *
          FROM (
            SELECT
              reviewer_audits.*,
              ROW_NUMBER() OVER (
                PARTITION BY pilot_id, episode_id, review_mode
                ORDER BY COALESCE(completed_at, updated_at, created_at) DESC, id DESC
              ) AS rn
            FROM reviewer_audits
            WHERE status = 'completed'
          )
          WHERE rn = 1
        )
        SELECT
          COUNT(*) AS count,
          AVG(overall_score) AS overall,
          AVG(coverage_score) AS coverage,
          AVG(precision_score) AS precision,
          AVG(grounding_score) AS grounding,
          AVG(identity_score) AS identity,
          SUM(p0_issue_count) AS p0_issues
        FROM latest_completed
        """
    ).fetchone()
    claim_network = {
        "claim_clusters": int(conn.execute("SELECT COUNT(*) AS count FROM claim_clusters").fetchone()["count"] or 0),
        "agreement_edges": int(conn.execute("SELECT COUNT(*) AS count FROM agreement_edges").fetchone()["count"] or 0),
        "disagreement_edges": int(conn.execute("SELECT COUNT(*) AS count FROM disagreement_edges").fetchone()["count"] or 0),
    }
    waiting_context = int(
        conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM jobs
            JOIN segments ON segments.id = jobs.target_id
            WHERE jobs.job_type = 'label_segment'
              AND jobs.status = 'pending'
              AND json_extract(jobs.payload_json, '$.label_pack') = 'ai_discourse_v3_1'
              AND NOT EXISTS (
                SELECT 1
                FROM episode_context_runs
                WHERE episode_context_runs.episode_id = segments.episode_id
                  AND episode_context_runs.label_pack = 'ai_discourse_v3_1'
                  AND episode_context_runs.model = 'gpt-5.5'
                  AND episode_context_runs.status = 'completed'
              )
            """
        ).fetchone()["count"]
    )
    return {
        "candidate_concepts": candidate_status,
        "concept_aliases": alias_status,
        "shift_signals_by_type": signal_type_counts,
        "burst_terms": burst_terms,
        "actor_stance_changes": actor_stance_changes,
        "v3_audit_count": int(audit["count"] or 0),
        "v3_avg_audit_score": round(float(audit["avg_score"] or 0), 3) if audit["avg_score"] is not None else 0,
        "v3_1_audit_count": int(v31_audit["count"] or 0),
        "v3_1_avg_audit_score": round(float(v31_audit["avg_score"] or 0), 3) if v31_audit["avg_score"] is not None else 0,
        "v3_1_scale_state": "blocked_until_gpt55_full_episode_pilot_passes",
        "episode_context_runs": episode_context_runs,
        "reviewer_audits": reviewer_status,
        "reviewer_audit_scores": {
            "completed": int(reviewer_scores["count"] or 0),
            "overall": round(float(reviewer_scores["overall"] or 0), 2) if reviewer_scores["overall"] is not None else 0,
            "coverage": round(float(reviewer_scores["coverage"] or 0), 2) if reviewer_scores["coverage"] is not None else 0,
            "precision": round(float(reviewer_scores["precision"] or 0), 2) if reviewer_scores["precision"] is not None else 0,
            "grounding": round(float(reviewer_scores["grounding"] or 0), 2) if reviewer_scores["grounding"] is not None else 0,
            "identity_graph_usefulness": round(float(reviewer_scores["identity"] or 0), 2) if reviewer_scores["identity"] is not None else 0,
            "p0_issues": int(reviewer_scores["p0_issues"] or 0),
        },
        "claim_network": claim_network,
        "v3_1_labels_waiting_on_episode_context": waiting_context,
        "raw_speaker_mentions_by_resolution": raw_speaker_status,
        "raw_actor_mentions_by_resolution": raw_actor_status,
        "unresolved_identity_mentions": int(raw_speaker_status.get("unresolved", 0)) + int(raw_actor_status.get("unresolved", 0)),
    }


def _transcript_preparation_metrics(conn) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in conn.execute(
            """
            SELECT artifact_type, status, COUNT(*) AS count, AVG(boilerplate_ratio) AS avg_boilerplate_ratio, AVG(quality_score) AS avg_quality_score
            FROM transcript_preparations
            GROUP BY artifact_type, status
            ORDER BY artifact_type, status
            """
        ).fetchall()
    ]


def _transcript_acquisition_metrics(conn) -> dict[str, Any]:
    status_counts = {
        row["status"]: int(row["count"])
        for row in conn.execute("SELECT status, COUNT(*) AS count FROM transcript_acquisition_status GROUP BY status").fetchall()
    }
    attempt_counts = [
        dict(row)
        for row in conn.execute(
            """
            SELECT method, status, COUNT(*) AS count
            FROM transcript_acquisition_attempts
            GROUP BY method, status
            ORDER BY count DESC, method, status
            """
        ).fetchall()
    ]
    transcription_runs = [
        dict(row)
        for row in conn.execute(
            """
            SELECT provider, status, COUNT(*) AS count
            FROM transcription_runs
            GROUP BY provider, status
            ORDER BY provider, status
            """
        ).fetchall()
    ]
    transcript_source_kinds = [
        dict(row)
        for row in conn.execute(
            """
            SELECT source_kind, status, COUNT(*) AS count
            FROM transcripts
            GROUP BY source_kind, status
            ORDER BY source_kind, status
            """
        ).fetchall()
    ]
    return {
        "status_counts": status_counts,
        "attempt_counts": attempt_counts,
        "transcription_runs": transcription_runs,
        "transcript_source_kinds": transcript_source_kinds,
    }


def _source_yield(conn) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in conn.execute(
            """
            WITH source_transcripts AS (
              SELECT episodes.source_id, COUNT(DISTINCT transcripts.id) AS transcripts
              FROM episodes
              JOIN transcripts ON transcripts.episode_id = episodes.id
              GROUP BY episodes.source_id
            ),
            source_segments AS (
              SELECT source_id, COUNT(*) AS segments, SUM(word_count) AS segment_words
              FROM segments
              GROUP BY source_id
            ),
            source_labels AS (
              SELECT segments.source_id, COUNT(labels.id) AS labels
              FROM segments
              JOIN labels ON labels.segment_id = segments.id
              GROUP BY segments.source_id
            ),
            source_observations AS (
              SELECT segments.source_id, COUNT(coded_observations.id) AS observations
              FROM segments
              JOIN labels ON labels.segment_id = segments.id
              JOIN coded_observations ON coded_observations.label_id = labels.id
              GROUP BY segments.source_id
            ),
            source_events AS (
              SELECT segments.source_id, COUNT(discourse_events.id) AS discourse_events
              FROM segments
              JOIN labels ON labels.segment_id = segments.id
              JOIN discourse_events ON discourse_events.label_id = labels.id
              GROUP BY segments.source_id
            )
            SELECT
              sources.name AS source_name,
              sources.category,
              COALESCE(source_transcripts.transcripts, 0) AS transcripts,
              COALESCE(source_segments.segments, 0) AS segments,
              COALESCE(source_labels.labels, 0) AS labels,
              COALESCE(source_observations.observations, 0) AS observations,
              COALESCE(source_events.discourse_events, 0) AS discourse_events,
              ROUND(CASE WHEN COALESCE(source_segments.segment_words, 0) > 0 THEN
                COALESCE(source_observations.observations, 0) * 1000.0 / source_segments.segment_words
              ELSE 0 END, 2) AS observations_per_1000_words
            FROM sources
            LEFT JOIN source_transcripts ON source_transcripts.source_id = sources.id
            LEFT JOIN source_segments ON source_segments.source_id = sources.id
            LEFT JOIN source_labels ON source_labels.source_id = sources.id
            LEFT JOIN source_observations ON source_observations.source_id = sources.id
            LEFT JOIN source_events ON source_events.source_id = sources.id
            WHERE COALESCE(source_transcripts.transcripts, 0) > 0
               OR COALESCE(source_labels.labels, 0) > 0
               OR COALESCE(source_observations.observations, 0) > 0
            ORDER BY discourse_events DESC, observations DESC, labels DESC, segments DESC
            LIMIT 25
            """
        ).fetchall()
    ]


def _episode_inventory(conn) -> dict[str, Any]:
    coverage = dict(
        conn.execute(
            """
            WITH transcript_counts AS (
              SELECT episode_id, COUNT(*) AS transcript_count
              FROM transcripts
              GROUP BY episode_id
            )
            SELECT
              COUNT(*) AS episodes,
              COUNT(DISTINCT source_id) AS sources,
              COALESCE(SUM(CASE WHEN published_at < '2025-01-01' THEN 1 ELSE 0 END), 0) AS pre_2025_episodes,
              COALESCE(SUM(CASE WHEN feed_transcript_url IS NOT NULL THEN 1 ELSE 0 END), 0) AS feed_transcript_episodes,
              COALESCE(SUM(CASE WHEN COALESCE(transcript_counts.transcript_count, 0) > 0 THEN 1 ELSE 0 END), 0) AS episodes_with_transcripts,
              MIN(substr(published_at, 1, 10)) AS first_published_at,
              MAX(substr(published_at, 1, 10)) AS last_published_at
            FROM episodes
            LEFT JOIN transcript_counts ON transcript_counts.episode_id = episodes.id
            """
        ).fetchone()
    )
    coverage["transcript_coverage_ratio"] = (
        round(coverage["episodes_with_transcripts"] / coverage["episodes"], 3)
        if coverage.get("episodes")
        else 0
    )
    month_timeline = [
        dict(row)
        for row in conn.execute(
            """
            WITH transcript_counts AS (
              SELECT episode_id, COUNT(*) AS transcript_count
              FROM transcripts
              GROUP BY episode_id
            )
            SELECT
              substr(episodes.published_at, 1, 7) AS day,
              COUNT(*) AS episodes,
              COUNT(DISTINCT episodes.source_id) AS sources,
              COALESCE(SUM(CASE WHEN episodes.feed_transcript_url IS NOT NULL THEN 1 ELSE 0 END), 0) AS feed_transcript_episodes,
              COALESCE(SUM(CASE WHEN COALESCE(transcript_counts.transcript_count, 0) > 0 THEN 1 ELSE 0 END), 0) AS episodes_with_transcripts
            FROM episodes
            LEFT JOIN transcript_counts ON transcript_counts.episode_id = episodes.id
            WHERE episodes.published_at IS NOT NULL
            GROUP BY day
            ORDER BY day
            """
        ).fetchall()
    ]
    year_timeline = [
        dict(row)
        for row in conn.execute(
            """
            SELECT
              substr(published_at, 1, 4) AS year,
              COUNT(*) AS episodes,
              COUNT(DISTINCT source_id) AS sources
            FROM episodes
            WHERE published_at IS NOT NULL
            GROUP BY year
            ORDER BY year
            """
        ).fetchall()
    ]
    source_coverage = [
        dict(row)
        for row in conn.execute(
            """
            WITH transcript_counts AS (
              SELECT episode_id, COUNT(*) AS transcript_count
              FROM transcripts
              GROUP BY episode_id
            )
            SELECT
              sources.name AS source_name,
              sources.category,
              COUNT(episodes.id) AS episodes,
              COALESCE(SUM(CASE WHEN episodes.published_at < '2025-01-01' THEN 1 ELSE 0 END), 0) AS pre_2025_episodes,
              COALESCE(SUM(CASE WHEN episodes.feed_transcript_url IS NOT NULL THEN 1 ELSE 0 END), 0) AS feed_transcript_episodes,
              COALESCE(SUM(CASE WHEN COALESCE(transcript_counts.transcript_count, 0) > 0 THEN 1 ELSE 0 END), 0) AS episodes_with_transcripts,
              MIN(substr(episodes.published_at, 1, 10)) AS first_published_at,
              MAX(substr(episodes.published_at, 1, 10)) AS last_published_at
            FROM sources
            JOIN episodes ON episodes.source_id = sources.id
            LEFT JOIN transcript_counts ON transcript_counts.episode_id = episodes.id
            GROUP BY sources.id
            ORDER BY episodes DESC, source_name
            LIMIT 30
            """
        ).fetchall()
    ]
    return {
        "coverage": coverage,
        "month_timeline": month_timeline,
        "year_timeline": year_timeline,
        "source_coverage": source_coverage,
    }


def _trend_metrics(conn) -> dict[str, Any]:
    episode_inventory = _episode_inventory(conn)
    max_episode_published_day = episode_inventory["coverage"].get("last_published_at")
    max_published_day = conn.execute(
        """
        SELECT MAX(substr(episodes.published_at, 1, 10)) AS day
        FROM labels
        JOIN segments ON segments.id = labels.segment_id
        JOIN episodes ON episodes.id = segments.episode_id
        WHERE episodes.published_at IS NOT NULL
        """
    ).fetchone()["day"]
    if not max_published_day:
        return {
            "episode_inventory": episode_inventory["coverage"],
            "episode_month_timeline": episode_inventory["month_timeline"],
            "episode_year_timeline": episode_inventory["year_timeline"],
            "source_episode_coverage": episode_inventory["source_coverage"],
            "label_velocity": [],
            "processing_velocity": [],
            "publication_coverage": [],
            "concept_momentum": [],
            "concept_heatmap": {"days": [], "concepts": []},
            "event_type_mix": [],
            "event_type_timeline": [],
            "concept_timeline": [],
            "claim_subject_timeline": [],
            "claim_subject_top": [],
            "claim_subject_stance": [],
            "claim_subject_variants": [],
            "claim_subject_frame_mix": [],
            "claim_subject_samples": [],
            "source_timeline": [],
            "claim_subject_coverage": {},
            "compatibility_metrics": {},
            "source_productivity": [],
            "trend_windows": {
                "current_days": 90,
                "previous_days": 90,
                "anchor_day": None,
                "episode_inventory_anchor_day": max_episode_published_day,
                "bucket": "month",
                "axis": "episode_published_at",
            },
        }
    label_velocity = [
        dict(row)
        for row in conn.execute(
            """
            SELECT substr(episodes.published_at, 1, 7) AS day, labels.label_pack, COUNT(*) AS count
            FROM labels
            JOIN segments ON segments.id = labels.segment_id
            JOIN episodes ON episodes.id = segments.episode_id
            WHERE episodes.published_at IS NOT NULL
              AND date(substr(episodes.published_at, 1, 10)) >= date(?, '-18 month')
            GROUP BY day, labels.label_pack
            ORDER BY day, labels.label_pack
            """,
            (max_published_day,),
        ).fetchall()
    ]
    processing_velocity = [
        dict(row)
        for row in conn.execute(
            """
            SELECT substr(created_at, 1, 10) AS day, label_pack, COUNT(*) AS count
            FROM labels
            WHERE substr(created_at, 1, 10) >= date((SELECT MAX(substr(created_at, 1, 10)) FROM labels), '-20 day')
            GROUP BY day, label_pack
            ORDER BY day, label_pack
            """
        ).fetchall()
    ]
    publication_coverage = [
        dict(row)
        for row in conn.execute(
            """
            SELECT
              substr(episodes.published_at, 1, 7) AS day,
              COUNT(DISTINCT episodes.id) AS episodes,
              COUNT(DISTINCT labels.segment_id) AS labeled_segments,
              COUNT(labels.id) AS labels,
              COUNT(discourse_events.id) AS discourse_events
            FROM labels
            JOIN segments ON segments.id = labels.segment_id
            JOIN episodes ON episodes.id = segments.episode_id
            LEFT JOIN discourse_events ON discourse_events.label_id = labels.id
            WHERE episodes.published_at IS NOT NULL
              AND date(substr(episodes.published_at, 1, 10)) >= date(?, '-18 month')
            GROUP BY day
            ORDER BY day
            """,
            (max_published_day,),
        ).fetchall()
    ]
    concept_momentum = [
        dict(row)
        for row in conn.execute(
            """
            WITH events AS (
              SELECT
                COALESCE(NULLIF(discourse_events.canonical_concept_name, ''), NULLIF(discourse_events.candidate_concept, '')) AS concept,
                substr(episodes.published_at, 1, 10) AS day
              FROM discourse_events
              JOIN labels ON labels.id = discourse_events.label_id
              JOIN segments ON segments.id = labels.segment_id
              JOIN episodes ON episodes.id = segments.episode_id
              WHERE concept IS NOT NULL
                AND episodes.published_at IS NOT NULL
            ),
            scored AS (
              SELECT
                concept,
                SUM(CASE WHEN date(day) > date(?, '-90 day') THEN 1 ELSE 0 END) AS current_count,
                SUM(CASE WHEN date(day) <= date(?, '-90 day') AND date(day) > date(?, '-180 day') THEN 1 ELSE 0 END) AS previous_count
              FROM events
              WHERE date(day) > date(?, '-180 day')
              GROUP BY concept
            )
            SELECT
              concept,
              current_count,
              previous_count,
              current_count - previous_count AS delta,
              ROUND(CASE WHEN previous_count > 0 THEN (current_count - previous_count) * 100.0 / previous_count ELSE current_count * 100.0 END, 1) AS pct_change
            FROM scored
            WHERE current_count > 0 OR previous_count > 0
            ORDER BY ABS(delta) DESC, current_count DESC, concept
            LIMIT 18
            """,
            (max_published_day, max_published_day, max_published_day, max_published_day),
        ).fetchall()
    ]
    top_heatmap_concepts = [
        str(row["concept"])
        for row in conn.execute(
            """
            SELECT
              COALESCE(NULLIF(discourse_events.canonical_concept_name, ''), NULLIF(discourse_events.candidate_concept, '')) AS concept,
              COUNT(*) AS count
            FROM discourse_events
            JOIN labels ON labels.id = discourse_events.label_id
            JOIN segments ON segments.id = labels.segment_id
            JOIN episodes ON episodes.id = segments.episode_id
            WHERE concept IS NOT NULL
              AND episodes.published_at IS NOT NULL
              AND date(substr(episodes.published_at, 1, 10)) > date(?, '-18 month')
            GROUP BY concept
            ORDER BY count DESC, concept
            LIMIT 14
            """,
            (max_published_day,),
        ).fetchall()
    ]
    heatmap_rows: list[dict[str, Any]] = []
    if top_heatmap_concepts:
        placeholders = ",".join("?" for _ in top_heatmap_concepts)
        heatmap_rows = [
            dict(row)
            for row in conn.execute(
                f"""
                SELECT
                  substr(episodes.published_at, 1, 7) AS day,
                  COALESCE(NULLIF(discourse_events.canonical_concept_name, ''), NULLIF(discourse_events.candidate_concept, '')) AS concept,
                  COUNT(*) AS count
                FROM discourse_events
                JOIN labels ON labels.id = discourse_events.label_id
                JOIN segments ON segments.id = labels.segment_id
                JOIN episodes ON episodes.id = segments.episode_id
                WHERE concept IN ({placeholders})
                  AND episodes.published_at IS NOT NULL
                  AND date(substr(episodes.published_at, 1, 10)) > date(?, '-18 month')
                GROUP BY day, concept
                ORDER BY day, concept
                """,
                (*top_heatmap_concepts, max_published_day),
            ).fetchall()
        ]
    heatmap_days = sorted({row["day"] for row in heatmap_rows})
    heatmap_concepts = []
    for concept in sorted({row["concept"] for row in heatmap_rows}):
        values = {row["day"]: int(row["count"] or 0) for row in heatmap_rows if row["concept"] == concept}
        heatmap_concepts.append(
            {
                "concept": concept,
                "total": sum(values.values()),
                "values": [values.get(day, 0) for day in heatmap_days],
            }
        )
    heatmap_concepts.sort(key=lambda row: (-int(row["total"]), str(row["concept"])))
    event_type_mix = [
        dict(row)
        for row in conn.execute(
            """
            SELECT discourse_events.event_type, COUNT(*) AS count
            FROM discourse_events
            JOIN labels ON labels.id = discourse_events.label_id
            JOIN segments ON segments.id = labels.segment_id
            JOIN episodes ON episodes.id = segments.episode_id
            WHERE episodes.published_at IS NOT NULL
              AND date(substr(episodes.published_at, 1, 10)) > date(?, '-12 month')
            GROUP BY discourse_events.event_type
            ORDER BY count DESC, discourse_events.event_type
            LIMIT 12
            """,
            (max_published_day,),
        ).fetchall()
    ]
    event_type_timeline = [
        dict(row)
        for row in conn.execute(
            """
            SELECT
              substr(episodes.published_at, 1, 7) AS day,
              discourse_events.event_type,
              COUNT(*) AS count
            FROM discourse_events
            JOIN labels ON labels.id = discourse_events.label_id
            JOIN segments ON segments.id = labels.segment_id
            JOIN episodes ON episodes.id = segments.episode_id
            WHERE episodes.published_at IS NOT NULL
              AND date(substr(episodes.published_at, 1, 10)) > date(?, '-18 month')
            GROUP BY day, discourse_events.event_type
            ORDER BY day, discourse_events.event_type
            """,
            (max_published_day,),
        ).fetchall()
    ]
    top_timeline_concepts = [
        row["concept"]
        for row in conn.execute(
            """
            SELECT
              COALESCE(NULLIF(discourse_events.canonical_concept_name, ''), NULLIF(discourse_events.candidate_concept, '')) AS concept,
              COUNT(*) AS count
            FROM discourse_events
            JOIN labels ON labels.id = discourse_events.label_id
            JOIN segments ON segments.id = labels.segment_id
            JOIN episodes ON episodes.id = segments.episode_id
            WHERE concept IS NOT NULL
              AND episodes.published_at IS NOT NULL
              AND date(substr(episodes.published_at, 1, 10)) > date(?, '-18 month')
            GROUP BY concept
            ORDER BY count DESC, concept
            LIMIT 8
            """,
            (max_published_day,),
        ).fetchall()
    ]
    concept_timeline: list[dict[str, Any]] = []
    if top_timeline_concepts:
        placeholders = ",".join("?" for _ in top_timeline_concepts)
        concept_timeline = [
            dict(row)
            for row in conn.execute(
                f"""
                SELECT
                  substr(episodes.published_at, 1, 7) AS day,
                  COALESCE(NULLIF(discourse_events.canonical_concept_name, ''), NULLIF(discourse_events.candidate_concept, '')) AS concept,
                  COUNT(*) AS count
                FROM discourse_events
                JOIN labels ON labels.id = discourse_events.label_id
                JOIN segments ON segments.id = labels.segment_id
                JOIN episodes ON episodes.id = segments.episode_id
                WHERE concept IN ({placeholders})
                  AND episodes.published_at IS NOT NULL
                  AND date(substr(episodes.published_at, 1, 10)) > date(?, '-18 month')
                GROUP BY day, concept
                ORDER BY day, concept
                """,
                (*top_timeline_concepts, max_published_day),
            ).fetchall()
        ]
    top_timeline_claims = [
        row["canonical_claim"]
        for row in conn.execute(
            """
            WITH clustered_claims AS (
              SELECT claim_clusters.canonical_claim_text AS canonical_claim, claim_cluster_members.claim_id
              FROM claim_cluster_members
              JOIN claim_clusters ON claim_clusters.id = claim_cluster_members.cluster_id
              WHERE claim_cluster_members.relation = 'supports_canonical_claim'
                AND claim_clusters.status NOT LIKE 'quarantined%'
                AND claim_clusters.status != 'needs_review'
              UNION ALL
              SELECT claim_clusters.canonical_claim_text AS canonical_claim, claims.id AS claim_id
              FROM claim_clusters, json_each(claim_clusters.evidence_json, '$.claim_ids') AS claim_ref
              JOIN claims ON claims.id = claim_ref.value
              WHERE (SELECT COUNT(*) FROM claim_cluster_members) = 0
            )
            SELECT clustered_claims.canonical_claim, COUNT(*) AS count
            FROM clustered_claims
            JOIN claims ON claims.id = clustered_claims.claim_id
            JOIN segments ON segments.id = claims.segment_id
            JOIN episodes ON episodes.id = segments.episode_id
            WHERE episodes.published_at IS NOT NULL
              AND date(substr(episodes.published_at, 1, 10)) > date(?, '-18 month')
            GROUP BY clustered_claims.canonical_claim
            ORDER BY count DESC, clustered_claims.canonical_claim
            LIMIT 8
            """,
            (max_published_day,),
        ).fetchall()
    ]
    claim_timeline: list[dict[str, Any]] = []
    if top_timeline_claims:
        placeholders = ",".join("?" for _ in top_timeline_claims)
        claim_timeline = [
            dict(row)
            for row in conn.execute(
                f"""
                WITH clustered_claims AS (
                  SELECT claim_clusters.canonical_claim_text AS canonical_claim, claim_cluster_members.claim_id
                  FROM claim_cluster_members
                  JOIN claim_clusters ON claim_clusters.id = claim_cluster_members.cluster_id
                  WHERE claim_cluster_members.relation = 'supports_canonical_claim'
                    AND claim_clusters.status NOT LIKE 'quarantined%'
                    AND claim_clusters.status != 'needs_review'
                  UNION ALL
                  SELECT claim_clusters.canonical_claim_text AS canonical_claim, claims.id AS claim_id
                  FROM claim_clusters, json_each(claim_clusters.evidence_json, '$.claim_ids') AS claim_ref
                  JOIN claims ON claims.id = claim_ref.value
                  WHERE (SELECT COUNT(*) FROM claim_cluster_members) = 0
                )
                SELECT
                  substr(episodes.published_at, 1, 7) AS day,
                  clustered_claims.canonical_claim,
                  COUNT(*) AS count
                FROM clustered_claims
                JOIN claims ON claims.id = clustered_claims.claim_id
                JOIN segments ON segments.id = claims.segment_id
                JOIN episodes ON episodes.id = segments.episode_id
                WHERE clustered_claims.canonical_claim IN ({placeholders})
                  AND episodes.published_at IS NOT NULL
                  AND date(substr(episodes.published_at, 1, 10)) > date(?, '-18 month')
                GROUP BY day, clustered_claims.canonical_claim
                ORDER BY day, clustered_claims.canonical_claim
                """,
                (*top_timeline_claims, max_published_day),
            ).fetchall()
        ]
    source_timeline = [
        dict(row)
        for row in conn.execute(
            """
            WITH top_sources AS (
              SELECT sources.id
              FROM discourse_events
              JOIN labels ON labels.id = discourse_events.label_id
              JOIN segments ON segments.id = labels.segment_id
              JOIN episodes ON episodes.id = segments.episode_id
              JOIN sources ON sources.id = episodes.source_id
              WHERE episodes.published_at IS NOT NULL
                AND date(substr(episodes.published_at, 1, 10)) > date(?, '-18 month')
              GROUP BY sources.id
              ORDER BY COUNT(*) DESC, sources.name
              LIMIT 8
            )
            SELECT
              substr(episodes.published_at, 1, 7) AS day,
              sources.name AS source_name,
              COUNT(discourse_events.id) AS count
            FROM discourse_events
            JOIN labels ON labels.id = discourse_events.label_id
            JOIN segments ON segments.id = labels.segment_id
            JOIN episodes ON episodes.id = segments.episode_id
            JOIN sources ON sources.id = episodes.source_id
            WHERE sources.id IN (SELECT id FROM top_sources)
              AND episodes.published_at IS NOT NULL
              AND date(substr(episodes.published_at, 1, 10)) > date(?, '-18 month')
            GROUP BY day, sources.name
            ORDER BY day, sources.name
            """,
            (max_published_day, max_published_day),
        ).fetchall()
    ]
    stance_mix = [
        dict(row)
        for row in conn.execute(
            """
            WITH clustered_claims AS (
              SELECT claim_clusters.canonical_claim_text AS canonical_claim, claim_cluster_members.claim_id
              FROM claim_cluster_members
              JOIN claim_clusters ON claim_clusters.id = claim_cluster_members.cluster_id
              WHERE claim_cluster_members.relation = 'supports_canonical_claim'
                AND claim_clusters.status NOT LIKE 'quarantined%'
                AND claim_clusters.status != 'needs_review'
              UNION ALL
              SELECT claim_clusters.canonical_claim_text AS canonical_claim, claims.id AS claim_id
              FROM claim_clusters, json_each(claim_clusters.evidence_json, '$.claim_ids') AS claim_ref
              JOIN claims ON claims.id = claim_ref.value
              WHERE (SELECT COUNT(*) FROM claim_cluster_members) = 0
            )
            SELECT
              clustered_claims.canonical_claim,
              COALESCE(NULLIF(claims.stance, ''), 'unspecified') AS stance,
              COUNT(*) AS count,
              COUNT(DISTINCT sources.name) AS source_count,
              MIN(substr(episodes.published_at, 1, 10)) AS first_published_at,
              MAX(substr(episodes.published_at, 1, 10)) AS last_published_at
            FROM clustered_claims
            JOIN claims ON claims.id = clustered_claims.claim_id
            JOIN segments ON segments.id = claims.segment_id
            JOIN episodes ON episodes.id = segments.episode_id
            JOIN sources ON sources.id = episodes.source_id
            WHERE episodes.published_at IS NOT NULL
              AND date(substr(episodes.published_at, 1, 10)) > date(?, '-12 month')
            GROUP BY clustered_claims.canonical_claim, stance
            ORDER BY count DESC, source_count DESC, clustered_claims.canonical_claim, stance
            LIMIT 30
            """,
            (max_published_day,),
        ).fetchall()
    ]
    canonical_claim_coverage = dict(
        conn.execute(
            """
            WITH all_member_claims AS (
              SELECT claim_cluster_members.cluster_id, claim_cluster_members.claim_id
              FROM claim_cluster_members
              JOIN claim_clusters ON claim_clusters.id = claim_cluster_members.cluster_id
              WHERE claim_cluster_members.relation = 'supports_canonical_claim'
                AND claim_clusters.status NOT LIKE 'quarantined%'
            ),
            member_claims AS (
              SELECT all_member_claims.cluster_id, all_member_claims.claim_id
              FROM all_member_claims
              JOIN claim_clusters ON claim_clusters.id = all_member_claims.cluster_id
              WHERE claim_clusters.status != 'needs_review'
            ),
            json_claims AS (
              SELECT claim_clusters.id AS cluster_id, claims.id AS claim_id
              FROM claim_clusters, json_each(claim_clusters.evidence_json, '$.claim_ids') AS claim_ref
              JOIN claims ON claims.id = claim_ref.value
              WHERE (SELECT COUNT(*) FROM member_claims) = 0
            ),
            canonical_claims AS (
              SELECT * FROM member_claims
              UNION ALL
              SELECT * FROM json_claims
            ),
            cluster_sizes AS (
              SELECT cluster_id, COUNT(DISTINCT claim_id) AS claim_count
              FROM canonical_claims
              GROUP BY cluster_id
            ),
            all_cluster_sizes AS (
              SELECT cluster_id, COUNT(DISTINCT claim_id) AS claim_count
              FROM all_member_claims
              GROUP BY cluster_id
            ),
            latest_run AS (
              SELECT metrics_json, completed_at
              FROM claim_canonicalization_runs
              WHERE status = 'completed'
              ORDER BY completed_at DESC
              LIMIT 1
            )
            SELECT
              (SELECT COUNT(*) FROM claims) AS claims,
              (SELECT COUNT(*) FROM cluster_sizes) AS claim_clusters,
              (SELECT COUNT(DISTINCT claim_id) FROM canonical_claims) AS assigned_claims,
              COALESCE(SUM(CASE WHEN claim_count > 1 THEN 1 ELSE 0 END), 0) AS multi_claim_clusters,
              COALESCE((SELECT COUNT(*) FROM all_cluster_sizes JOIN claim_clusters ON claim_clusters.id = all_cluster_sizes.cluster_id WHERE all_cluster_sizes.claim_count > 1 AND claim_clusters.status = 'semantic_candidate'), 0) AS semantic_multi_claim_clusters,
              COALESCE((SELECT COUNT(*) FROM all_cluster_sizes JOIN claim_clusters ON claim_clusters.id = all_cluster_sizes.cluster_id WHERE claim_clusters.status = 'needs_review'), 0) AS needs_review_clusters,
              COALESCE((SELECT COUNT(*) FROM all_cluster_sizes JOIN claim_clusters ON claim_clusters.id = all_cluster_sizes.cluster_id WHERE claim_clusters.confidence < 0.7 AND claim_clusters.status NOT LIKE 'quarantined%'), 0) AS low_confidence_clusters,
              COALESCE(MAX(claim_count), 0) AS largest_cluster_size,
              ROUND(CASE WHEN (SELECT COUNT(DISTINCT claim_id) FROM canonical_claims) > 0 THEN (SELECT COUNT(*) FROM cluster_sizes) * 1.0 / (SELECT COUNT(DISTINCT claim_id) FROM canonical_claims) ELSE 0 END, 3) AS cluster_to_claim_ratio,
              ROUND(CASE WHEN (SELECT COUNT(*) FROM claims) > 0 THEN (SELECT COUNT(DISTINCT claim_id) FROM canonical_claims) * 1.0 / (SELECT COUNT(*) FROM claims) ELSE 0 END, 3) AS assigned_claim_coverage_ratio,
              (SELECT COUNT(*) FROM claim_canonicalization_runs WHERE status = 'completed') AS completed_runs,
              (SELECT completed_at FROM latest_run) AS latest_run_completed_at,
              CAST(COALESCE(json_extract((SELECT metrics_json FROM latest_run), '$.template_claims_quarantined'), 0) AS INTEGER) AS latest_template_claims_quarantined,
              CAST(COALESCE(json_extract((SELECT metrics_json FROM latest_run), '$.semantic_merge_count'), 0) AS INTEGER) AS latest_semantic_merge_count,
              CAST(COALESCE(json_extract((SELECT metrics_json FROM latest_run), '$.needs_review_clusters'), 0) AS INTEGER) AS latest_needs_review_clusters
            FROM cluster_sizes
            """
        ).fetchone()
    )
    canonical_claim_samples = [
        dict(row)
        for row in conn.execute(
            """
            WITH ranked_clusters AS (
              SELECT
                claim_clusters.id AS cluster_id,
                claim_clusters.canonical_claim_text AS canonical_claim,
                COUNT(DISTINCT claim_cluster_members.claim_id) AS claim_count
              FROM claim_cluster_members
              JOIN claim_clusters ON claim_clusters.id = claim_cluster_members.cluster_id
              WHERE claim_cluster_members.relation = 'supports_canonical_claim'
                AND claim_clusters.status = 'semantic_candidate'
              GROUP BY claim_clusters.id
              HAVING claim_count > 1
              ORDER BY claim_count DESC, claim_clusters.confidence DESC, claim_clusters.canonical_claim_text
              LIMIT 12
            ),
            sample_claims AS (
              SELECT
                ranked_clusters.cluster_id,
                GROUP_CONCAT(substr(claims.text, 1, 220), ' || ') AS sampled_member_claims
              FROM ranked_clusters
              JOIN claim_cluster_members ON claim_cluster_members.cluster_id = ranked_clusters.cluster_id
              JOIN claims ON claims.id = claim_cluster_members.claim_id
              WHERE claims.id IN (
                SELECT claim_cluster_members.claim_id
                FROM claim_cluster_members
                WHERE claim_cluster_members.cluster_id = ranked_clusters.cluster_id
                ORDER BY claim_cluster_members.claim_id
                LIMIT 3
              )
              GROUP BY ranked_clusters.cluster_id
            )
            SELECT
              ranked_clusters.canonical_claim,
              ranked_clusters.claim_count,
              COUNT(DISTINCT sources.name) AS source_count,
              COUNT(DISTINCT COALESCE(NULLIF(claims.stance, ''), 'unspecified')) AS stance_count,
              MIN(substr(episodes.published_at, 1, 10)) AS first_published_at,
              MAX(substr(episodes.published_at, 1, 10)) AS last_published_at,
              sample_claims.sampled_member_claims
            FROM ranked_clusters
            JOIN claim_cluster_members ON claim_cluster_members.cluster_id = ranked_clusters.cluster_id
            JOIN claims ON claims.id = claim_cluster_members.claim_id
            JOIN segments ON segments.id = claims.segment_id
            JOIN episodes ON episodes.id = segments.episode_id
            JOIN sources ON sources.id = episodes.source_id
            LEFT JOIN sample_claims ON sample_claims.cluster_id = ranked_clusters.cluster_id
            GROUP BY ranked_clusters.cluster_id
            ORDER BY ranked_clusters.claim_count DESC, source_count DESC, ranked_clusters.canonical_claim
            """
        ).fetchall()
    ]
    claim_subject_views = _claim_subject_views(conn, max_published_day=max_published_day)
    source_productivity = [
        dict(row)
        for row in conn.execute(
            """
            WITH source_words AS (
              SELECT source_id, SUM(word_count) AS segment_words
              FROM segments
              GROUP BY source_id
            ),
            source_labels AS (
              SELECT segments.source_id, COUNT(labels.id) AS labels
              FROM segments
              JOIN labels ON labels.segment_id = segments.id
              GROUP BY segments.source_id
            ),
            source_events AS (
              SELECT segments.source_id, COUNT(discourse_events.id) AS discourse_events
              FROM segments
              JOIN labels ON labels.segment_id = segments.id
              JOIN discourse_events ON discourse_events.label_id = labels.id
              GROUP BY segments.source_id
            ),
            source_observations AS (
              SELECT segments.source_id, COUNT(coded_observations.id) AS observations
              FROM segments
              JOIN labels ON labels.segment_id = segments.id
              JOIN coded_observations ON coded_observations.label_id = labels.id
              GROUP BY segments.source_id
            )
            SELECT
              sources.name AS source_name,
              COALESCE(source_labels.labels, 0) AS labels,
              COALESCE(source_events.discourse_events, 0) AS discourse_events,
              COALESCE(source_observations.observations, 0) AS observations,
              ROUND(CASE WHEN COALESCE(source_words.segment_words, 0) > 0 THEN
                COALESCE(source_events.discourse_events, 0) * 1000.0 / source_words.segment_words
              ELSE 0 END, 2) AS events_per_1000_words
            FROM sources
            LEFT JOIN source_words ON source_words.source_id = sources.id
            LEFT JOIN source_labels ON source_labels.source_id = sources.id
            LEFT JOIN source_events ON source_events.source_id = sources.id
            LEFT JOIN source_observations ON source_observations.source_id = sources.id
            WHERE COALESCE(source_labels.labels, 0) > 0
            ORDER BY discourse_events DESC, labels DESC, source_name
            LIMIT 12
            """
        ).fetchall()
    ]
    return {
        "episode_inventory": episode_inventory["coverage"],
        "episode_month_timeline": episode_inventory["month_timeline"],
        "episode_year_timeline": episode_inventory["year_timeline"],
        "source_episode_coverage": episode_inventory["source_coverage"],
        "label_velocity": label_velocity,
        "processing_velocity": processing_velocity,
        "publication_coverage": publication_coverage,
        "concept_momentum": concept_momentum,
        "concept_heatmap": {"days": heatmap_days, "concepts": heatmap_concepts},
        "event_type_mix": event_type_mix,
        "event_type_timeline": event_type_timeline,
        "concept_timeline": concept_timeline,
        "claim_subject_timeline": claim_subject_views["timeline"],
        "claim_subject_top": claim_subject_views["top"],
        "claim_subject_stance": claim_subject_views["stance"],
        "claim_subject_variants": claim_subject_views["variants"],
        "claim_subject_frame_mix": claim_subject_views["frame_mix"],
        "claim_subject_samples": claim_subject_views["samples"],
        "source_timeline": source_timeline,
        "claim_subject_coverage": claim_subject_views["coverage"],
        "compatibility_metrics": {
            "canonical_claim_coverage": canonical_claim_coverage,
            "canonical_claim_samples": canonical_claim_samples,
            "claim_timeline": claim_timeline,
            "stance_mix": stance_mix,
        },
        "source_productivity": source_productivity,
        "trend_windows": {
            "current_days": 90,
            "previous_days": 90,
            "anchor_day": max_published_day,
            "episode_inventory_anchor_day": max_episode_published_day,
            "bucket": "month",
            "axis": "episode_published_at",
        },
    }


def _claim_subject_views(conn, *, max_published_day: str) -> dict[str, Any]:
    coverage = dict(
        conn.execute(
            """
            WITH subject_sizes AS (
              SELECT subject_id, COUNT(DISTINCT claim_id) AS claim_count
              FROM claim_subject_members
              WHERE relation = 'about_subject'
              GROUP BY subject_id
            ),
            latest_run AS (
              SELECT metrics_json, completed_at
              FROM claim_subject_runs
              WHERE status = 'completed'
              ORDER BY completed_at DESC
              LIMIT 1
            )
            SELECT
              (SELECT COUNT(*) FROM claims) AS claims,
              (SELECT COUNT(*) FROM claim_subjects WHERE status NOT LIKE 'quarantined%') AS claim_subjects,
              (SELECT COUNT(*) FROM claim_proposition_variants) AS proposition_variants,
              (SELECT COUNT(*) FROM claim_position_observations) AS position_observations,
              (SELECT COUNT(*) FROM claim_subject_event_observations) AS event_observations,
              (SELECT COUNT(*) FROM claim_subject_expert_positions) AS expert_positions,
              (SELECT COUNT(*) FROM claim_subject_expert_positions WHERE canonical_person_id IS NOT NULL) AS expert_positions_with_canonical_person,
              (SELECT COUNT(*) FROM claim_subject_expert_positions WHERE canonical_person_id IS NULL) AS expert_positions_with_speaker_fallback,
              (SELECT COUNT(DISTINCT claim_id) FROM claim_subject_members WHERE relation = 'about_subject') AS assigned_claims,
              COALESCE(SUM(CASE WHEN claim_count > 1 THEN 1 ELSE 0 END), 0) AS multi_claim_subjects,
              COALESCE(MAX(claim_count), 0) AS largest_subject_size,
              ROUND(CASE WHEN (SELECT COUNT(DISTINCT claim_id) FROM claim_subject_members WHERE relation = 'about_subject') > 0
                THEN (SELECT COUNT(*) FROM subject_sizes) * 1.0 / (SELECT COUNT(DISTINCT claim_id) FROM claim_subject_members WHERE relation = 'about_subject')
                ELSE 0 END, 3) AS subject_to_claim_ratio,
              ROUND(CASE WHEN (SELECT COUNT(*) FROM claims) > 0
                THEN (SELECT COUNT(DISTINCT claim_id) FROM claim_subject_members WHERE relation = 'about_subject') * 1.0 / (SELECT COUNT(*) FROM claims)
                ELSE 0 END, 3) AS assigned_claim_coverage_ratio,
              (SELECT COUNT(*) FROM claim_subject_runs WHERE status = 'completed') AS completed_runs,
              (SELECT completed_at FROM latest_run) AS latest_run_completed_at,
              CAST(COALESCE(json_extract((SELECT metrics_json FROM latest_run), '$.template_claims_quarantined'), 0) AS INTEGER) AS latest_template_claims_quarantined,
              CAST(COALESCE(json_extract((SELECT metrics_json FROM latest_run), '$.event_observations_seen'), 0) AS INTEGER) AS latest_event_observations_seen,
              CAST(COALESCE(json_extract((SELECT metrics_json FROM latest_run), '$.event_observations_framed'), 0) AS INTEGER) AS latest_event_observations_framed,
              CAST(COALESCE(json_extract((SELECT metrics_json FROM latest_run), '$.event_observations_quarantined'), 0) AS INTEGER) AS latest_event_observations_quarantined,
              CAST(COALESCE(json_extract((SELECT metrics_json FROM latest_run), '$.legacy_claims_framed'), 0) AS INTEGER) AS latest_legacy_claims_framed,
              CAST(COALESCE(json_extract((SELECT metrics_json FROM latest_run), '$.subjects_considered'), 0) AS INTEGER) AS latest_subjects_considered,
              CAST(COALESCE(json_extract((SELECT metrics_json FROM latest_run), '$.variants_considered'), 0) AS INTEGER) AS latest_variants_considered
            FROM subject_sizes
            """
        ).fetchone()
    )
    top_subjects = [
        dict(row)
        for row in conn.execute(
            """
            WITH subject_observations AS (
              SELECT subject_id, source_id, episode_id, speaker_name, stance, frame, published_at, 'claim' AS observation_kind
              FROM claim_position_observations
              UNION ALL
              SELECT subject_id, source_id, episode_id, speaker_name, stance, frame, published_at, event_role AS observation_kind
              FROM claim_subject_event_observations
            ),
            variant_counts AS (
              SELECT subject_id, COUNT(DISTINCT id) AS variant_count
              FROM claim_proposition_variants
              GROUP BY subject_id
            )
            SELECT
              claim_subjects.subject_text AS claim_subject,
              claim_subjects.domain,
              COUNT(*) AS observation_count,
              SUM(CASE WHEN subject_observations.observation_kind = 'claim' THEN 1 ELSE 0 END) AS claim_observation_count,
              SUM(CASE WHEN subject_observations.observation_kind != 'claim' THEN 1 ELSE 0 END) AS event_observation_count,
              COALESCE(variant_counts.variant_count, 0) AS variant_count,
              COUNT(DISTINCT subject_observations.source_id) AS source_count,
              COUNT(DISTINCT COALESCE(NULLIF(subject_observations.speaker_name, ''), 'unknown')) AS speaker_count,
              COUNT(DISTINCT COALESCE(NULLIF(subject_observations.stance, ''), 'unspecified')) AS stance_count,
              MIN(substr(subject_observations.published_at, 1, 10)) AS first_published_at,
              MAX(substr(subject_observations.published_at, 1, 10)) AS last_published_at,
              CASE
                WHEN COUNT(DISTINCT subject_observations.source_id) <= 1 THEN 'single_source'
                WHEN COUNT(DISTINCT COALESCE(NULLIF(subject_observations.stance, ''), 'unspecified')) <= 1 THEN 'single_stance'
                WHEN COUNT(DISTINCT COALESCE(NULLIF(subject_observations.speaker_name, ''), 'unknown')) <= 1 THEN 'single_speaker'
                ELSE ''
              END AS blindspot_warning
            FROM claim_subjects
            JOIN subject_observations ON subject_observations.subject_id = claim_subjects.id
            LEFT JOIN variant_counts ON variant_counts.subject_id = claim_subjects.id
            WHERE subject_observations.published_at IS NOT NULL
              AND date(substr(subject_observations.published_at, 1, 10)) > date(?, '-18 month')
            GROUP BY claim_subjects.id
            ORDER BY observation_count DESC, source_count DESC, claim_subjects.subject_text
            LIMIT 20
            """,
            (max_published_day,),
        ).fetchall()
    ]
    top_names = [row["claim_subject"] for row in top_subjects[:8]]
    timeline: list[dict[str, Any]] = []
    if top_names:
        placeholders = ",".join("?" for _ in top_names)
        timeline = [
            dict(row)
            for row in conn.execute(
                f"""
                WITH subject_observations AS (
                  SELECT subject_id, published_at
                  FROM claim_position_observations
                  UNION ALL
                  SELECT subject_id, published_at
                  FROM claim_subject_event_observations
                )
                SELECT
                  substr(subject_observations.published_at, 1, 7) AS day,
                  claim_subjects.subject_text AS claim_subject,
                  COUNT(*) AS count
                FROM subject_observations
                JOIN claim_subjects ON claim_subjects.id = subject_observations.subject_id
                WHERE claim_subjects.subject_text IN ({placeholders})
                  AND subject_observations.published_at IS NOT NULL
                  AND date(substr(subject_observations.published_at, 1, 10)) > date(?, '-18 month')
                GROUP BY day, claim_subjects.subject_text
                ORDER BY day, claim_subjects.subject_text
                """,
                (*top_names, max_published_day),
            ).fetchall()
        ]
    stance = [
        dict(row)
        for row in conn.execute(
            """
            SELECT
              claim_subjects.subject_text AS claim_subject,
              claim_subject_expert_positions.speaker_name AS speaker,
              CASE WHEN claim_subject_expert_positions.canonical_person_id IS NULL THEN 'speaker_fallback' ELSE 'canonical_person' END AS speaker_resolution,
              claim_subject_expert_positions.stance,
              claim_subject_expert_positions.observation_count AS count,
              claim_subject_expert_positions.claim_observation_count,
              claim_subject_expert_positions.event_observation_count,
              claim_subject_expert_positions.source_count,
              claim_subject_expert_positions.first_published_at,
              claim_subject_expert_positions.last_published_at
            FROM claim_subject_expert_positions
            JOIN claim_subjects ON claim_subjects.id = claim_subject_expert_positions.subject_id
            WHERE claim_subject_expert_positions.first_published_at IS NOT NULL
              AND date(claim_subject_expert_positions.first_published_at) > date(?, '-18 month')
            ORDER BY claim_subject_expert_positions.observation_count DESC, claim_subject_expert_positions.source_count DESC, claim_subjects.subject_text, speaker, stance
            LIMIT 40
            """,
            (max_published_day,),
        ).fetchall()
    ]
    variants = [
        dict(row)
        for row in conn.execute(
            """
            SELECT
              claim_subjects.subject_text AS claim_subject,
              claim_proposition_variants.variant_text,
              claim_proposition_variants.horizon,
              claim_proposition_variants.market_context,
              claim_proposition_variants.geography,
              claim_proposition_variants.polarity,
              COUNT(DISTINCT claim_position_observations.claim_id) AS observation_count,
              COUNT(DISTINCT COALESCE(NULLIF(claim_position_observations.stance, ''), 'unspecified')) AS stance_count
            FROM claim_proposition_variants
            JOIN claim_subjects ON claim_subjects.id = claim_proposition_variants.subject_id
            LEFT JOIN claim_position_observations ON claim_position_observations.variant_id = claim_proposition_variants.id
            GROUP BY claim_proposition_variants.id
            ORDER BY observation_count DESC, claim_subjects.subject_text, claim_proposition_variants.variant_text
            LIMIT 40
            """
        ).fetchall()
    ]
    frame_mix = [
        dict(row)
        for row in conn.execute(
            """
            WITH subject_observations AS (
              SELECT subject_id, COALESCE(NULLIF(frame, ''), 'unspecified') AS frame
              FROM claim_position_observations
              UNION ALL
              SELECT subject_id, COALESCE(NULLIF(frame, ''), event_role, 'unspecified') AS frame
              FROM claim_subject_event_observations
            )
            SELECT
              claim_subjects.subject_text AS claim_subject,
              subject_observations.frame,
              COUNT(*) AS count
            FROM subject_observations
            JOIN claim_subjects ON claim_subjects.id = subject_observations.subject_id
            GROUP BY claim_subjects.subject_text, frame
            ORDER BY count DESC, claim_subjects.subject_text, frame
            LIMIT 40
            """
        ).fetchall()
    ]
    samples = [
        dict(row)
        for row in conn.execute(
            """
            WITH ranked_subjects AS (
              SELECT
                claim_position_observations.subject_id,
                COUNT(DISTINCT claim_position_observations.claim_id) AS observation_count,
                (SELECT COUNT(*) FROM claim_subject_event_observations WHERE claim_subject_event_observations.subject_id = claim_position_observations.subject_id) AS event_observation_count,
                COUNT(DISTINCT claim_proposition_variants.id) AS variant_count
              FROM claim_position_observations
              JOIN claim_proposition_variants ON claim_proposition_variants.id = claim_position_observations.variant_id
              GROUP BY claim_position_observations.subject_id
              HAVING observation_count > 1
              ORDER BY observation_count DESC, variant_count DESC
              LIMIT 12
            ),
            ranked_variants AS (
              SELECT
                claim_proposition_variants.subject_id,
                substr(claim_proposition_variants.variant_text, 1, 120) AS variant_text,
                ROW_NUMBER() OVER (
                  PARTITION BY claim_proposition_variants.subject_id
                  ORDER BY COUNT(DISTINCT claim_position_observations.claim_id) DESC, claim_proposition_variants.variant_text
                ) AS variant_rank
              FROM claim_proposition_variants
              LEFT JOIN claim_position_observations ON claim_position_observations.variant_id = claim_proposition_variants.id
              WHERE claim_proposition_variants.subject_id IN (SELECT subject_id FROM ranked_subjects)
              GROUP BY claim_proposition_variants.id
            ),
            sampled_variants AS (
              SELECT subject_id, GROUP_CONCAT(variant_text, ' || ') AS sampled_variants
              FROM (
                SELECT subject_id, variant_text, variant_rank
                FROM ranked_variants
                WHERE variant_rank <= 3
                ORDER BY subject_id, variant_rank
              )
              GROUP BY subject_id
            )
            SELECT
              claim_subjects.subject_text AS claim_subject,
              ranked_subjects.observation_count,
              ranked_subjects.event_observation_count,
              ranked_subjects.variant_count,
              sampled_variants.sampled_variants
            FROM ranked_subjects
            JOIN claim_subjects ON claim_subjects.id = ranked_subjects.subject_id
            LEFT JOIN sampled_variants ON sampled_variants.subject_id = ranked_subjects.subject_id
            ORDER BY ranked_subjects.observation_count DESC, ranked_subjects.variant_count DESC, claim_subjects.subject_text
            LIMIT 12
            """
        ).fetchall()
    ]
    return {
        "coverage": coverage,
        "top": top_subjects,
        "timeline": timeline,
        "stance": stance,
        "variants": variants,
        "frame_mix": frame_mix,
        "samples": samples,
    }


def write_snapshot(conn, output: str | Path | None = None) -> Path:
    path = Path(output).expanduser().resolve() if output else exports_dir() / "observer-snapshot.json"
    write_text_atomic(path, json.dumps(build_snapshot(conn), ensure_ascii=True, indent=2, sort_keys=True) + "\n")
    return path


def publish_snapshot(snapshot_path: str | Path, *, url: str | None = None, token: str | None = None) -> dict[str, Any]:
    target_url = url or os.environ.get("RAILWAY_UI_URL")
    ingest_token = token or os.environ.get("RAILWAY_UI_INGEST_TOKEN")
    if not target_url:
        raise ValueError("Missing Railway UI URL. Set RAILWAY_UI_URL or pass --url.")
    if not ingest_token:
        raise ValueError("Missing ingest token. Set RAILWAY_UI_INGEST_TOKEN or pass --token.")
    endpoint = target_url.rstrip("/") + "/ingest-snapshot"
    body = Path(snapshot_path).read_bytes()
    request = urllib.request.Request(
        endpoint,
        data=body,
        method="POST",
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {ingest_token}"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        text = response.read().decode("utf-8", errors="replace")
        return {"status": response.status, "body": text}


def _safe_job(row) -> dict[str, Any]:
    return {
        "lane": row["lane"],
        "job_type": row["job_type"],
        "status": row["status"],
        "priority": row["priority"],
        "attempts": row["attempts"],
        "lease_owner": _public_worker(row["lease_owner"]),
        "leased_until": row["leased_until"],
        "updated_at": row["updated_at"],
        "error": _public_text(row["error"]),
    }


def _recent_artifacts(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    artifacts: dict[str, dict[str, Any]] = {}
    for item in sorted(path.glob("*"), key=lambda p: p.stat().st_mtime, reverse=True)[:50]:
        if item.is_file():
            stat = item.stat()
            kind = item.suffix.lower().lstrip(".") or "artifact"
            aggregate = artifacts.setdefault(
                kind,
                {"artifact_type": kind, "count": 0, "bytes": 0, "most_recent_at": None},
            )
            aggregate["count"] += 1
            aggregate["bytes"] += int(stat.st_size)
            observed_at = dt.datetime.fromtimestamp(stat.st_mtime, tz=dt.timezone.utc).isoformat()
            if aggregate["most_recent_at"] is None or observed_at > aggregate["most_recent_at"]:
                aggregate["most_recent_at"] = observed_at
    return sorted(artifacts.values(), key=lambda item: (-int(item["count"]), str(item["artifact_type"])))


def _safe_label_run(row) -> dict[str, Any]:
    prompt_path = Path(row["prompt_path"]) if row["prompt_path"] else None
    output_path = Path(row["output_path"]) if row["output_path"] else None
    return {
        "label_pack": row["label_pack"],
        "model": row["model"],
        "status": row["status"],
        "prompt_artifact": _artifact_kind(prompt_path),
        "output_artifact": _artifact_kind(output_path),
        "output_exists": output_path.exists() if output_path else False,
        "claimed_at": row["claimed_at"],
        "completed_at": row["completed_at"],
    }


def _public_text(value: Any, *, limit: int = 160) -> str | None:
    if value is None:
        return None
    text = str(value)
    text = text.replace(str(root()), "<project>")
    text = re.sub(r"/Users/[^\s\"']+", "<local-path>", text)
    text = re.sub(r"https?://[^\s\"')]+", "<url>", text)
    text = re.sub(r"\b(ep|seg|tr|lbl|run|aud)_[a-f0-9]{12,}\b", r"\1_<id>", text)
    return text[:limit]


def _public_worker(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return re.sub(r"[^A-Za-z0-9_.:-]+", "", text)[:80]


def _artifact_kind(path: Path | None) -> str | None:
    if not path:
        return None
    return path.suffix.lstrip(".") or "artifact"
