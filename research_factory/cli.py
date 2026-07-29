from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import re
import signal
import subprocess
import sys
from pathlib import Path

from . import db
from .app_server_evaluation import (
    aggregate_app_server_development_matrix,
    export_app_server_development_manifest,
    export_app_server_development_manifest_v2,
    run_app_server_core_arm,
)
from .claim_canonicalizer import canonicalize_claims
from .claim_subjects import build_claim_subjects
from .daily_cycle import DEFAULT_DAILY_RUNTIME_SECONDS, run_daily_cycle
from .exports import (
    export_actor_stance_report,
    export_graph,
    export_narrative_map,
    export_product_correlation_memo,
    export_signal_report,
    export_term_drift_report,
    export_terminology_drift,
    export_trend_report,
)
from .efficient_backtest import (
    DEFAULT_WINDOWED_GUIDELINES_PATH,
    candidate_prompt_status,
    efficiency_readiness,
    run_candidate_prompt_smoke,
    run_candidate_event_verifier_smoke,
    run_proposition_extractor_smoke,
    run_event_core_smoke,
    run_windowed_event_core_smoke,
    run_windowed_event_enrichment_smoke,
    run_windowed_semantic_judge_smoke,
    export_windowed_source_holdout_manifest,
    run_proposition_classifier_smoke,
    run_proposition_decision_smoke,
    run_chunk_size_sweep,
    run_efficiency_backtest,
    export_distillation_dataset,
)
from .windowed_evaluation import (
    DEFAULT_ACCEPTANCE_SPEC_PATH,
    DEFAULT_EVALUATOR_SPEC_PATH,
    DEFAULT_JUDGE_FIXTURE_PATH,
    build_fail_closed_acceptance_report,
    build_no_signal_power_report,
    build_paired_provenance_and_context_cost,
    export_paired_evaluation_manifest,
    export_blinded_paired_adjudication,
    export_consolidated_human_adjudication,
    export_no_signal_human_audit,
    export_no_signal_power_manifest,
    export_prospective_shadow_manifest,
    run_judge_calibration,
    run_expanded_judge_calibration,
    run_paired_baseline,
    run_paired_core_repairs,
    run_paired_enrichment_path,
    run_paired_phase_one,
    materialize_consolidated_human_outputs,
    recover_historical_episode_context_usage,
    recover_interrupted_windowed_core_run,
    run_checkpointed_windowed_core,
    score_blinded_paired_adjudication,
    score_no_signal_human_audit,
)
from .fast_quality import build_failure_bank, queue_delta_audits
from .headless_codex import execute_claimed_label_runs, execute_pending_reviewer_audits
from .ingest import (
    attach_transcript,
    enqueue_transcript_backlog,
    enqueue_sources,
    enqueue_transcription_jobs,
    mark_transcript_exhausted,
    quarantine_contaminated_transcripts,
    record_transcript_acquisition_attempt,
    release_transcript_candidate_claims,
    sync_gcp_official_transcripts,
    sync_verge_official_transcripts,
    transcript_candidates,
    verify_sources,
)
from .identity_graph import groom_identity_graph
from .mcp_bridge import (
    ensure_source_card_buffer,
    fetch_broker_audit_events,
    import_remote_submissions,
    publish_broker_snapshot,
    publish_broker_status,
    publish_remote_work_packages,
    read_token,
)
from .observer import publish_snapshot, write_snapshot
from .orchestrator import run_cycle
from .paths import db_path, exports_dir, root
from .prep import prepare_transcript
from .production import DEFAULT_OBSERVER_URL, DEFAULT_PILOT_ID, production_cycle, railway_cost_guard
from .production_ops import (
    ACCEPTED_PILOT_ID,
    build_release,
    pif_status,
    plan_or_resolve_outcomes,
    promote_release,
    publish_ops,
    reconcile_target,
    supersede_legacy_label_jobs,
    verify_release,
)
from .research_queue import (
    claim_remote_job,
    queue_status,
    release_or_fail_remote_job,
    run_worker,
    submit_remote_output,
    sync_queue_envelopes,
)
from .scale_gate import build_scale_gate_report, enqueue_controlled_100_batch, enqueue_scale_gate
from .scale_ops import (
    acquisition_funnel,
    cluster_claims,
    create_reviewer_audits,
    judge_claim_edges,
    requeue_reviewed_segments,
    reviewer_findings,
    retry_failed_labels,
    submit_reviewer_audit,
)
from .signals import detect_shifts, discover_concepts
from .transcript_strategies import transcript_strategy_report
from .util import dumps_json, loads_json, now_iso, read_text, sha256_text
from .worker import (
    EPISODE_CONTEXT_SCHEMA_VERSION,
    create_episode_context_prompt,
    create_label_prompt,
    fail_or_retry_job,
    fail_job,
    gate_v31_label_on_episode_context,
    preflight,
    recover_label_handoffs,
    run_jobs,
    submit_episode_context_output,
    submit_label_output,
)


LAB_COMMANDS = (
    "efficiency-backtest",
    "efficiency-smoke-candidates",
    "efficiency-verify-candidates",
    "efficiency-proposition-smoke",
    "efficiency-event-core-smoke",
    "efficiency-windowed-core-smoke",
    "efficiency-windowed-enrich",
    "efficiency-windowed-judge",
    "efficiency-windowed-holdout",
    "efficiency-judge-calibrate",
    "efficiency-judge-calibrate-expanded",
    "efficiency-paired-manifest",
    "efficiency-paired-baseline",
    "efficiency-paired-phase-one",
    "efficiency-paired-enrich",
    "efficiency-paired-repair",
    "efficiency-paired-adjudication-export",
    "efficiency-paired-adjudication-score",
    "efficiency-paired-adjudication-consolidate",
    "efficiency-paired-adjudication-materialize",
    "efficiency-paired-provenance",
    "efficiency-context-usage-recover",
    "efficiency-no-signal-manifest",
    "efficiency-no-signal-power-report",
    "efficiency-no-signal-audit-export",
    "efficiency-no-signal-audit-score",
    "efficiency-paired-gate",
    "efficiency-prospective-shadow-manifest",
    "efficiency-windowed-core-recover",
    "efficiency-windowed-core-checkpointed",
    "efficiency-app-server-dev-manifest",
    "efficiency-app-server-dev-manifest-v2",
    "efficiency-app-server-core-arm",
    "efficiency-app-server-matrix-aggregate",
    "efficiency-proposition-classify",
    "efficiency-proposition-decide",
    "efficiency-candidate-status",
    "efficiency-chunk-sweep",
    "efficiency-readiness",
    "efficiency-distillation-export",
)


def _run_cancellable(coroutine):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    task = loop.create_task(coroutine)
    cancellation_requested = False
    cancellation_signal = signal.SIGINT

    def cancel_once(signum: int) -> None:
        nonlocal cancellation_requested, cancellation_signal
        if cancellation_requested:
            return
        cancellation_requested = True
        cancellation_signal = signum
        task.cancel()

    installed = []
    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, cancel_once, signum)
            installed.append(signum)
        except (NotImplementedError, RuntimeError):
            pass
    try:
        return loop.run_until_complete(task)
    except asyncio.CancelledError as exc:
        raise SystemExit(128 + int(cancellation_signal)) from exc
    finally:
        for signum in installed:
            loop.remove_signal_handler(signum)
        if not task.done():
            task.cancel()
            loop.run_until_complete(asyncio.gather(task, return_exceptions=True))
        loop.run_until_complete(loop.shutdown_asyncgens())
        asyncio.set_event_loop(None)
        loop.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="research-factory")
    parser.add_argument("--db", default=str(db_path()), help="SQLite database path.")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init", help="Initialize the local database.")
    sub.add_parser("status", help="Print compact production, queue, release, and corpus status.")
    private_ui = sub.add_parser("ui", help="Serve the private accepted-only intelligence UI on loopback.")
    private_ui.add_argument("--host", default="127.0.0.1")
    private_ui.add_argument("--port", type=int, default=8766)

    release = sub.add_parser("release", help="Build, verify, or promote an immutable corpus release.")
    release_sub = release.add_subparsers(dest="release_command", required=True)
    release_build = release_sub.add_parser("build", help="Freeze the accepted audited pilot into a release candidate manifest.")
    release_build.add_argument("--pilot-id", default=ACCEPTED_PILOT_ID)
    release_build.add_argument("--label-pack", default="ai_discourse_v3_1")
    release_build.add_argument("--model", default="gpt-5.5")
    release_build.add_argument("--output-dir")
    release_build.add_argument("--release-id")
    release_verify = release_sub.add_parser("verify", help="Fail closed unless the frozen candidate satisfies the accepted release contract.")
    release_verify.add_argument("--release-id", help="Canonical corpus release ID. Required unless --manifest resolves it by hash.")
    release_verify.add_argument("--manifest", help="Optional immutable manifest artifact to cross-check against SQLite.")
    release_verify.add_argument("--output-dir")
    release_promote = release_sub.add_parser("promote", help="Accept one verified manifest into the immutable release ledger.")
    release_promote.add_argument("--release-id", help="Canonical corpus release ID. Required unless --manifest resolves it by hash.")
    release_promote.add_argument("--manifest", help="Optional immutable manifest artifact to cross-check against SQLite.")
    release_promote.add_argument("--verification")
    release_promote.add_argument("--claims", help="Managed-app-server accepted atomic_claim_v1 packet. Required for a new import run.")
    release_promote.add_argument("--pipeline-run-id", help="Existing release-bound atomic import run to finish or reuse.")
    release_promote.add_argument("--promoted-by", default="pif-operator")
    release_promote.add_argument("--rationale", default="promote verified approved pilot release")
    release_promote.add_argument("--model", default="gpt-5.5")
    release_promote.add_argument("--output-dir")
    release_promote.add_argument("--max-claims", type=int, default=10_000)
    release_supersede = release_sub.add_parser(
        "supersede-legacy",
        help="Back up SQLite, then supersede a bounded set of pending legacy label jobs outside a verified release.",
    )
    release_supersede.add_argument("--manifest", required=True)
    release_supersede.add_argument("--verification", required=True)
    release_supersede.add_argument("--limit", type=int, default=500)
    release_supersede.add_argument("--backup-output")
    release_supersede.add_argument(
        "--reason",
        default="superseded after verified corpus release",
        help="Short sanitized operational reason stored on affected jobs.",
    )

    reconcile = sub.add_parser("reconcile", help="Plan or apply bounded production reconciliation.")
    reconcile_sub = reconcile.add_subparsers(dest="reconcile_command", required=True)
    for target in ("identities", "claims", "relations"):
        target_parser = reconcile_sub.add_parser(target, help=f"Plan or apply bounded {target} reconciliation.")
        target_parser.add_argument(
            "--apply",
            action="store_true",
            help="Prepare an immutable release-bound candidate packet. Never imports semantic decisions.",
        )
        target_parser.add_argument("--release-id", help="Accepted promoted release; defaults to the current release.")
        target_parser.add_argument("--pilot-id", dest="release_id", help=argparse.SUPPRESS)
        target_parser.add_argument("--model", default="gpt-5.5")
        target_parser.add_argument("--limit", type=int, default=25)
        target_parser.add_argument("--scope", choices=["last_18_months", "all"], default="last_18_months")
        target_parser.add_argument("--output-dir")

    outcomes = sub.add_parser("outcomes", help="Inspect due forecast outcomes or append an explicit resolution revision.")
    outcomes_sub = outcomes.add_subparsers(dest="outcomes_command", required=True)
    outcomes_resolve = outcomes_sub.add_parser("resolve", help="Plan bounded managed-app resolution work or record one supplied resolution.")
    outcomes_resolve.add_argument("--claim-id")
    outcomes_resolve.add_argument("--outcome", choices=["true", "false", "mixed", "unresolved", "unverifiable"])
    outcomes_resolve.add_argument("--confidence", type=float)
    outcomes_resolve.add_argument("--rationale")
    outcomes_resolve.add_argument("--evidence-json", help="JSON object or @path with public evidence metadata; never transcript text.")
    outcomes_resolve.add_argument("--authoritative-evidence-json", help="JSON object/array or @path describing authoritative public evidence.")
    outcomes_resolve.add_argument("--resolution-question")
    outcomes_resolve.add_argument("--due-at")
    outcomes_resolve.add_argument("--resolution-window-start")
    outcomes_resolve.add_argument("--resolution-window-end")
    outcomes_resolve.add_argument("--resolution-criteria")
    outcomes_resolve.add_argument("--as-of")
    outcomes_resolve.add_argument("--resolver-model", default="operator-supplied")
    outcomes_resolve.add_argument("--resolver-version", default="outcome_resolver_v1")
    outcomes_resolve.add_argument("--reviewer-version", default="outcome_reviewer_v1")
    outcomes_resolve.add_argument("--review-status", choices=["pending", "accepted", "rejected", "needs_review"], default="accepted")
    outcomes_resolve.add_argument("--resolved-at")
    outcomes_resolve.add_argument("--categorical-score", type=float, choices=[0.0, 0.5, 1.0])
    outcomes_resolve.add_argument("--brier-score", type=float)
    outcomes_resolve.add_argument("--pipeline-run-id")
    outcomes_resolve.add_argument("--limit", type=int, default=10)
    outcomes_resolve.add_argument("--max-runtime-ms", type=int, default=1_200_000)
    outcomes_resolve.add_argument("--record-exceptions", action="store_true", help="Record bounded dispatch contracts locally; never launch them.")

    publish_ops_parser = sub.add_parser("publish-ops", help="Write a sanitized observer snapshot and optionally publish it.")
    publish_ops_parser.add_argument("--output", default=str(exports_dir() / "observer-snapshot.json"))
    publish_ops_parser.add_argument("--publish", action="store_true", help="Post the snapshot after local privacy validation.")
    publish_ops_parser.add_argument("--url", default=DEFAULT_OBSERVER_URL)
    publish_ops_parser.add_argument("--token")
    publish_ops_parser.add_argument("--token-file")

    lab = sub.add_parser("lab", help="Explicit evaluator compatibility surface; dry-run plan unless writes are enabled.")
    lab.add_argument("--allow-write", action="store_true", help="Run the legacy evaluator command. Must be explicit.")
    lab.add_argument("lab_command", choices=["list", *LAB_COMMANDS])
    lab.add_argument("lab_args", nargs=argparse.REMAINDER)

    production = sub.add_parser("production-cycle", help="Run the local-first production control loop.")
    production.add_argument("--pilot-id", default=DEFAULT_PILOT_ID)
    production.add_argument("--model", default="gpt-5.5")
    production.add_argument("--label-pack", default="ai_discourse_v3_1")
    production.add_argument("--worker-mode", choices=["none", "bounded"], default="none")
    production.add_argument("--acquisition-limit", type=int, default=4)
    production.add_argument("--extractor-limit", type=int, default=4)
    production.add_argument("--reviewer-limit", type=int, default=2)
    production.add_argument("--run-graph-jobs", action="store_true")
    production.add_argument("--graph-limit", type=int, default=100)
    production.add_argument("--snapshot-output", default=str(exports_dir() / "observer-snapshot.json"))
    production.add_argument("--publish", action="store_true")
    production.add_argument("--observer-url", default=DEFAULT_OBSERVER_URL)
    production.add_argument("--token-file", default=".railway-ingest-token.local")

    railway_guard = sub.add_parser("railway-cost-guard", help="Verify Railway stays a minimal observer/storage edge, not a compute tier.")
    railway_guard.add_argument("--status-json", help="Read Railway status JSON from a file instead of calling railway status.")
    railway_guard.add_argument("--project-name", default="podcast-intelligence-observer")
    railway_guard.add_argument("--service-name", default="observer-ui")
    railway_guard.add_argument("--allowed-service", action="append", default=[], help="Additional Railway service name allowed by policy.")
    railway_guard.add_argument(
        "--allowed-managed-storage-service",
        action="append",
        default=["Postgres"],
        help="Managed storage service name allowed by policy without treating it as Railway compute.",
    )
    railway_guard.add_argument("--max-replicas", type=int, default=1)

    queue = sub.add_parser("queue", help="Inspect or synchronize the portable research queue contract.")
    queue_sub = queue.add_subparsers(dest="queue_command", required=True)
    queue_status_parser = queue_sub.add_parser("status", help="Show queue depth by lane/content type/worker role.")
    queue_status_parser.add_argument(
        "--by",
        default="lane,content_type,role,status",
        help="Comma-separated grouping: lane,job_type,status,role,content_type,privacy_tier.",
    )
    queue_sub.add_parser("sync-envelopes", help="Backfill portable queue envelopes from existing jobs.")

    worker_parser = sub.add_parser("worker", help="Run bounded local headless-style worker roles.")
    worker_sub = worker_parser.add_subparsers(dest="worker_command", required=True)
    worker_run = worker_sub.add_parser("run", help="Claim and process a bounded role-specific job batch locally.")
    worker_run.add_argument("--role", required=True, choices=["acquisition", "extractor", "reviewer", "identity_judge", "claim_judge", "snapshot_publisher"])
    worker_run.add_argument("--lane", default="podcast")
    worker_run.add_argument("--limit", type=int, default=4)
    worker_run.add_argument("--model", default="gpt-5.5")
    worker_run.add_argument("--label-pack", default="ai_discourse_v3_1")
    worker_run.add_argument("--worker-id", default="headless-codex-worker")
    worker_run.add_argument("--local-draft", action="store_true", help="Tests/bootstrap only; v3.1 still rejects local draft extraction.")
    worker_run.add_argument("--no-claim-prompts", action="store_true", help="Release prompt handoff jobs instead of creating prompts.")
    worker_run.add_argument("--burst", action="store_true", help="Allow up to 6 jobs for small clean batches.")

    headless_exec = sub.add_parser("headless-exec", help="Execute already-claimed label prompt handoffs through local codex exec.")
    headless_exec.add_argument("--lease-owner", required=True)
    headless_exec.add_argument("--limit", type=int, default=1)
    headless_exec.add_argument("--model", default="gpt-5.5")
    headless_exec.add_argument("--timeout-seconds", type=int, default=900)
    headless_exec.add_argument("--no-audit", action="store_true")

    headless_review = sub.add_parser("headless-review", help="Execute pending reviewer-audit handoffs through local codex exec.")
    headless_review.add_argument("--patch-tag")
    headless_review.add_argument("--limit", type=int, default=1)
    headless_review.add_argument("--model", default="gpt-5.5")
    headless_review.add_argument("--timeout-seconds", type=int, default=1200)
    headless_review.add_argument("--concurrency", type=int, default=1)

    remote_queue = sub.add_parser("remote-queue", help="Disabled future MCP/ChatGPT queue contract smoke surface.")
    remote_sub = remote_queue.add_subparsers(dest="remote_command", required=True)
    remote_claim = remote_sub.add_parser("claim-job", help="Claim a remote-eligible job through the future MCP contract shape.")
    remote_claim.add_argument("--worker-id", required=True)
    remote_claim.add_argument("--capability", action="append", default=[], help="Worker role capability, e.g. extractor or reviewer.")
    remote_claim.add_argument("--max-items", type=int, default=1)
    remote_submit = remote_sub.add_parser("submit-output", help="Submit remote-style output metadata for local validation/import.")
    remote_submit.add_argument("--job-id", type=int, required=True)
    remote_submit.add_argument("--worker-id", required=True)
    remote_submit.add_argument("--output-ref", required=True)
    remote_submit.add_argument("--status", default="submitted", choices=["submitted", "rejected", "needs_review"])
    remote_release = remote_sub.add_parser("release-or-fail", help="Release or fail a remote-claimed job.")
    remote_release.add_argument("--job-id", type=int, required=True)
    remote_release.add_argument("--worker-id", required=True)
    remote_release.add_argument("--reason", required=True)
    remote_release.add_argument("--fail", action="store_true")

    mcp_broker = sub.add_parser("mcp-broker", help="Bridge local sanitized state to the remote MCP broker.")
    mcp_broker_sub = mcp_broker.add_subparsers(dest="mcp_broker_command", required=True)
    publish_broker = mcp_broker_sub.add_parser("publish-snapshot", help="Publish sanitized local observer state to the MCP broker.")
    publish_broker.add_argument("--url", required=True, help="Base URL for the MCP broker service.")
    publish_broker.add_argument("--token")
    publish_broker.add_argument("--token-file")
    publish_broker.add_argument("--timeout-seconds", type=int, default=30)
    publish_status = mcp_broker_sub.add_parser("publish-status", help="Publish fast sanitized queue/status metrics to the MCP broker.")
    publish_status.add_argument("--url", required=True, help="Base URL for the MCP broker service.")
    publish_status.add_argument("--token")
    publish_status.add_argument("--token-file")
    publish_status.add_argument("--timeout-seconds", type=int, default=30)
    publish_work = mcp_broker_sub.add_parser("publish-work-packages", help="Publish eligible full_text_allowed remote extraction packages to the MCP broker.")
    publish_work.add_argument("--url", required=True, help="Base URL for the MCP broker service.")
    publish_work.add_argument("--token")
    publish_work.add_argument("--token-file")
    publish_work.add_argument("--limit", type=int, default=1)
    publish_work.add_argument("--worker-id", default="mcp-broker-bridge")
    publish_work.add_argument("--replace", action="store_true")
    publish_work.add_argument("--timeout-seconds", type=int, default=30)
    refill_source_cards = mcp_broker_sub.add_parser("ensure-source-card-buffer", help="Keep a small local-to-broker source-card work buffer full.")
    refill_source_cards.add_argument("--url", required=True, help="Base URL for the MCP broker service.")
    refill_source_cards.add_argument("--token")
    refill_source_cards.add_argument("--token-file")
    refill_source_cards.add_argument("--target-buffer", type=int, default=12)
    refill_source_cards.add_argument("--max-new", type=int, default=2)
    refill_source_cards.add_argument("--worker-id", default="pif-source-card-buffer-bridge")
    refill_source_cards.add_argument("--timeout-seconds", type=int, default=30)
    import_work = mcp_broker_sub.add_parser("import-submissions", help="Import broker-submitted remote extraction outputs through local validators.")
    import_work.add_argument("--url", required=True, help="Base URL for the MCP broker service.")
    import_work.add_argument("--token")
    import_work.add_argument("--token-file")
    import_work.add_argument("--limit", type=int, default=10)
    import_work.add_argument("--timeout-seconds", type=int, default=30)
    audit_events = mcp_broker_sub.add_parser("audit-events", help="Fetch sanitized broker audit events.")
    audit_events.add_argument("--url", required=True, help="Base URL for the MCP broker service.")
    audit_events.add_argument("--token")
    audit_events.add_argument("--token-file")
    audit_events.add_argument("--limit", type=int, default=100)
    audit_events.add_argument("--timeout-seconds", type=int, default=30)

    orchestrator = sub.add_parser("orchestrator", help="Run lock-aware local Codex control-plane cycles.")
    orchestrator_sub = orchestrator.add_subparsers(dest="orchestrator_command", required=True)
    orchestrator_run = orchestrator_sub.add_parser("run", help="Run one lock-aware local cycle.")
    orchestrator_run.add_argument("--cycle", required=True, choices=["bridge_fast", "bridge_sync", "observer_publish", "local_extractor", "local_reviewer", "claim_judge"])
    orchestrator_run.add_argument("--model", default="gpt-5.5")
    orchestrator_run.add_argument("--label-pack", default="ai_discourse_v3_1")
    orchestrator_run.add_argument("--limit", type=int, default=4)
    orchestrator_run.add_argument("--broker-url")
    orchestrator_run.add_argument("--broker-token")
    orchestrator_run.add_argument("--broker-token-file")
    orchestrator_run.add_argument("--observer-url", default=DEFAULT_OBSERVER_URL)
    orchestrator_run.add_argument("--observer-token")
    orchestrator_run.add_argument("--observer-token-file")
    orchestrator_run.add_argument("--wait-for-lock", action="store_true")
    orchestrator_run.add_argument("--skip-init", action="store_true", help="Skip schema initialization for hot scheduled cycles against an existing DB.")

    preflight_parser = sub.add_parser("preflight", help="Check subscription-backed Codex CLI availability.")
    preflight_parser.add_argument("--model", default="gpt-5.4")

    enqueue = sub.add_parser("enqueue", help="Fetch source feeds and enqueue transcript jobs.")
    enqueue.add_argument("--lane", default="podcast")
    enqueue.add_argument("--since", required=True)
    enqueue.add_argument("--source-list", required=True)
    enqueue.add_argument("--label-pack", default="ai_discourse_v1")

    backfill_episodes = sub.add_parser("backfill-episodes", help="Fetch configured feeds without the production cutoff and backfill episode metadata.")
    backfill_episodes.add_argument("--lane", default="podcast")
    backfill_episodes.add_argument("--source-list", required=True)
    backfill_episodes.add_argument("--label-pack", default="ai_discourse_v3_1")
    backfill_episodes.add_argument("--since", help="Optional lower published-date bound. Omit for full available feed history.")
    backfill_episodes.add_argument("--until", help="Optional upper published-date bound, inclusive for YYYY-MM-DD values.")
    backfill_episodes.add_argument("--source", action="append", default=[], help="Limit to a source name or source id. Can be repeated.")
    backfill_episodes.add_argument("--max-items", type=int, help="Global cap on selected feed items after date/source filtering.")
    backfill_episodes.add_argument("--per-source-limit", type=int, help="Cap selected feed items per source after date filtering.")
    backfill_episodes.add_argument("--oldest-first", action="store_true", help="Process each feed oldest-first instead of feed order.")
    backfill_episodes.add_argument("--fetch-concurrency", type=int, default=8, help="Concurrent RSS fetches before sequential SQLite writes.")
    backfill_episodes.add_argument("--enqueue-transcript-jobs", action="store_true", help="Also enqueue transcript/manual-discovery jobs. Default is metadata-only.")
    backfill_episodes.add_argument("--dry-run", action="store_true")

    strategy_report = sub.add_parser("transcript-strategy-report", help="Summarize source-level transcript acquisition strategies and coverage.")
    strategy_report.add_argument("--strategy-file", default="config/transcript_strategies.json")

    transcript_backlog = sub.add_parser("enqueue-transcript-backlog", help="Enqueue fetch jobs for episodes with known transcript links but no ready transcript.")
    transcript_backlog.add_argument("--lane", default="podcast")
    transcript_backlog.add_argument("--label-pack", default="ai_discourse_v3_1")
    transcript_backlog.add_argument("--limit", type=int, default=100)
    transcript_backlog.add_argument("--source", action="append", default=[], help="Limit to a source name or source id. Can be repeated.")
    transcript_backlog.add_argument("--include-youtube-captions", action="store_true", help="Include YouTube caption URLs; default keeps them in the blocked/monitored caption lane.")
    transcript_backlog.add_argument("--include-document-transcripts", action="store_true", help="Include PDF/DOC/DOCX transcript URLs only after document parsing is ready.")
    transcript_backlog.add_argument("--include-quarantined-retry", action="store_true", help="Requeue previously quarantined transcript links after parser fixes. Default skips quarantined rows to avoid churn.")
    transcript_backlog.add_argument("--priority", type=int, default=35)
    transcript_backlog.add_argument("--dry-run", action="store_true")

    verge_sync = sub.add_parser("sync-verge-official-transcripts", help="Attach verified official Verge article transcript pages to existing Decoder/Vergecast episodes.")
    verge_sync.add_argument("--lane", default="podcast")
    verge_sync.add_argument("--label-pack", default="ai_discourse_v3_1")
    verge_sync.add_argument("--limit", type=int, default=50)
    verge_sync.add_argument("--source", action="append", default=[], help="Limit to decoder-with-nilay-patel or the-vergecast.")
    verge_sync.add_argument("--dry-run", action="store_true")

    gcp_sync = sub.add_parser("sync-gcp-official-transcripts", help="Attach official GCP podcast archive transcript pages to existing Google Cloud Platform Podcast episodes.")
    gcp_sync.add_argument("--lane", default="podcast")
    gcp_sync.add_argument("--label-pack", default="ai_discourse_v3_1")
    gcp_sync.add_argument("--limit", type=int, default=100)
    gcp_sync.add_argument("--max-archive-pages", type=int, default=40)
    gcp_sync.add_argument("--dry-run", action="store_true")

    candidates = sub.add_parser("transcript-candidates", help="List episodes that need official transcript discovery.")
    candidates.add_argument("--lane", default="podcast")
    candidates.add_argument("--limit", type=int, default=10)
    candidates.add_argument("--claim", action="store_true", help="Lease candidates so parallel discovery workers avoid duplicates.")
    candidates.add_argument("--worker-id", default="transcript-discovery")
    candidates.add_argument("--lease-minutes", type=int, default=60)
    candidates.add_argument("--max-claimed", type=int, help="Do not claim more candidates when this many unexpired manual-discovery claims already exist.")
    candidates.add_argument("--per-source-limit", type=int, help="Limit unexpired manual-discovery claims per source.")

    release_candidates = sub.add_parser(
        "release-transcript-candidates",
        help="Release unresolved manual transcript discovery claims back to pending.",
    )
    release_candidates.add_argument("--lane", default="podcast")
    release_candidates.add_argument("--worker-id", help="Only release candidates leased by this worker.")
    release_candidates.add_argument("--expired-only", action="store_true", help="Only release claims whose lease has expired.")
    release_candidates.add_argument("--limit", type=int, default=200)

    attach = sub.add_parser("attach-transcript", help="Attach a verified official transcript URL and enqueue fetching.")
    attach.add_argument("--episode-id", required=True)
    attach.add_argument("--transcript-url", required=True)
    attach.add_argument("--transcript-type", default="text/html")
    attach.add_argument(
        "--source-kind",
        default="official_show_transcript",
        choices=["creator_provided_rss_transcript", "official_show_transcript", "youtube_captions", "voyager_transcription"],
    )
    attach.add_argument("--lane", default="podcast")
    attach.add_argument("--label-pack", default="ai_discourse_v1")

    attempt = sub.add_parser("record-transcript-attempt", help="Record a transcript search attempt or outcome for an episode.")
    attempt.add_argument("--episode-id", required=True)
    attempt.add_argument("--method", required=True)
    attempt.add_argument("--status", required=True)
    attempt.add_argument("--result-url")
    attempt.add_argument("--source-kind")
    attempt.add_argument("--official-public", action="store_true")
    attempt.add_argument("--policy-blocked", action="store_true")
    attempt.add_argument("--error-class")
    attempt.add_argument("--notes")
    attempt.add_argument("--worker-id")

    exhausted = sub.add_parser("mark-transcript-exhausted", help="Mark public online transcript discovery exhausted for an episode.")
    exhausted.add_argument("--episode-id", required=True)
    exhausted.add_argument("--worker-id")
    exhausted.add_argument("--notes")

    transcribe = sub.add_parser("enqueue-transcription", help="Enqueue configured audio transcription fallback for eligible episodes.")
    transcribe.add_argument("--lane", default="podcast")
    transcribe.add_argument("--provider", default="voyager", choices=["voyager"])
    transcribe.add_argument("--label-pack", default="ai_discourse_v3_1")
    transcribe.add_argument("--limit", type=int, default=10)
    transcribe.add_argument("--priority", type=int, default=40)
    transcribe.add_argument("--dry-run", action="store_true")

    identity = sub.add_parser("groom-identities", help="Build a conservative candidate identity graph from v3.1 raw mentions.")
    identity.add_argument("--model", default="gpt-5.5")
    identity.add_argument("--pilot-id")
    identity.add_argument("--limit", type=int)

    scale_gate = sub.add_parser("scale-gate-report", help="Write a sanitized v3.1 scale-gate readiness report.")
    scale_gate.add_argument("--pilot-id", required=True)
    scale_gate.add_argument("--output")

    scale_batch = sub.add_parser("scale-batch-report", help="Alias for the stricter complete-episode v3.1 scale readiness report.")
    scale_batch.add_argument("--pilot-id", required=True)
    scale_batch.add_argument("--output")

    scale_gate_enqueue = sub.add_parser("scale-gate-enqueue", help="Select ready unlabeled transcripts and enqueue a bounded v3.1 scale gate.")
    scale_gate_enqueue.add_argument("--pilot-id", required=True)
    scale_gate_enqueue.add_argument("--lane", default="podcast")
    scale_gate_enqueue.add_argument("--label-pack", default="ai_discourse_v3_1")
    scale_gate_enqueue.add_argument("--model", default="gpt-5.5")
    scale_gate_enqueue.add_argument("--source", action="append", default=[], help="Exact source name to include. Defaults to the v3.1 gate source set.")
    scale_gate_enqueue.add_argument("--limit", type=int, default=25)
    scale_gate_enqueue.add_argument("--per-source-limit", type=int, default=5)
    scale_gate_enqueue.add_argument("--priority", type=int, default=10)
    scale_gate_enqueue.add_argument("--force-prepare", action="store_true")
    scale_gate_enqueue.add_argument("--include-low-signal", action="store_true")

    controlled_batch = sub.add_parser("scale-batch-enqueue", help="Prepare the controlled 100-episode v3.1 scale batch without running extraction.")
    controlled_batch.add_argument("--pilot-id", required=True)
    controlled_batch.add_argument("--lane", default="podcast")
    controlled_batch.add_argument("--label-pack", default="ai_discourse_v3_1")
    controlled_batch.add_argument("--model", default="gpt-5.5")
    controlled_batch.add_argument("--priority", type=int, default=8)
    controlled_batch.add_argument("--acquired-since", default="2026-07-03T00:00:00+00:00")
    controlled_batch.add_argument("--force-prepare", action="store_true")
    controlled_batch.add_argument("--include-low-signal", action="store_true")
    controlled_batch.add_argument("--dry-run", action="store_true")

    retry_failed = sub.add_parser("retry-failed-labels", help="Repair or requeue failed label jobs for a label pack.")
    retry_failed.add_argument("--label-pack", required=True)
    retry_failed.add_argument("--mode", required=True, choices=["repair-or-requeue", "requeue-only", "repair-only"])
    retry_failed.add_argument("--pilot-id")
    retry_failed.add_argument("--limit", type=int)
    retry_failed.add_argument("--worker-id", default="retry-failed-labels")

    pilot_labels = sub.add_parser("run-pilot-labels", help="Run bounded GPT-5.5 label workers for one v3.1 pilot.")
    pilot_labels.add_argument("--pilot-id")
    pilot_labels.add_argument("--label-pack", default="ai_discourse_v3_1")
    pilot_labels.add_argument("--model", default="gpt-5.5")
    pilot_labels.add_argument("--concurrency", type=int, default=4)
    pilot_labels.add_argument("--max-jobs", type=int, default=25)
    pilot_labels.add_argument("--max-failures", type=int, default=8)
    pilot_labels.add_argument("--timeout-seconds", type=int, default=1800)
    pilot_labels.add_argument("--worker-prefix", default="scale-v31-pilot")

    cleanup_pilot = sub.add_parser(
        "cleanup-pilot-label-claims",
        help="Release empty stranded label claims for a pilot without touching label handoffs that have outputs.",
    )
    cleanup_pilot.add_argument("--pilot-id", required=True)
    cleanup_pilot.add_argument("--dry-run", action="store_true")

    reviewer = sub.add_parser("reviewer-audit", help="Create GPT-5.5 reviewer-audit handoffs for pilot episodes.")
    reviewer.add_argument("--pilot-id", required=True)
    reviewer.add_argument("--episodes", type=int, default=10)
    reviewer.add_argument("--model", default="gpt-5.5")
    reviewer.add_argument("--fresh", action="store_true", help="Refresh existing reviewer audit handoffs and mark them pending again.")
    reviewer.add_argument("--mode", choices=["full", "targeted"], default="full", help="Use targeted for 3-5 episode remediation micro-reviews.")
    reviewer.add_argument("--patch-tag", help="Patch/remediation tag for comparable targeted reviewer runs.")

    failure_bank = sub.add_parser("failure-bank", help="Build/check a sanitized fast-QA failure bank for a pilot.")
    failure_bank.add_argument("--pilot-id", required=True)
    failure_bank.add_argument("--patch-tag", required=True)
    failure_bank.add_argument("--output")
    failure_bank.add_argument("--check-only", action="store_true")

    audit_delta = sub.add_parser("audit-delta", help="Queue fast delta audits for failed/reviewer-impacted labels plus sentinel labels.")
    audit_delta.add_argument("--pilot-id", required=True)
    audit_delta.add_argument("--label-pack", default="ai_discourse_v3_1")
    audit_delta.add_argument("--model", default="gpt-5.5")
    audit_delta.add_argument("--patch-tag", required=True)
    audit_delta.add_argument("--sentinel", type=int, default=10)
    audit_delta.add_argument("--fresh", action="store_true")

    efficiency = sub.add_parser("efficiency-backtest", help="Backtest compact episode-batch extraction against golden labels without mutating SQLite.")
    efficiency.add_argument("--label-pack", default="ai_discourse_v3_1")
    efficiency.add_argument("--model", default="gpt-5.5")
    efficiency.add_argument("--episode-limit", type=int, help="Deterministic broad episode sample. Omit to use all golden episodes.")
    efficiency.add_argument("--per-source-limit", type=int, help="Cap selected episodes per source before round-robin sampling.")
    efficiency.add_argument("--seed", default="efficiency-backtest-v1")
    efficiency.add_argument("--output")
    efficiency.add_argument("--candidate-output-dir", help="Optional directory of model-produced batch outputs to compare with golden labels.")
    efficiency.add_argument("--export-candidate-prompts", help="Optional directory for sparse compact episode-batch prompt handoffs and output placeholders.")
    efficiency.add_argument("--candidate-chunk-size", type=int, default=10, help="Segments per sparse compact candidate prompt. Default keeps output windows bounded while preserving quarter-target savings.")
    efficiency.add_argument("--candidate-representation", choices=["sparse_compact", "offset_sparse_compact", "evidence_sparse_compact", "flat_ledger", "flat_ledger_full", "full_schema", "compact"], default="sparse_compact", help="Candidate batch output representation to estimate/export.")

    efficiency_smoke = sub.add_parser("efficiency-smoke-candidates", help="Run bounded Codex smoke extraction for exported sparse compact candidate prompts.")
    efficiency_smoke.add_argument("--manifest", required=True, help="Manifest from efficiency-backtest --export-candidate-prompts.")
    efficiency_smoke.add_argument("--model", default="gpt-5.5")
    efficiency_smoke.add_argument("--limit", type=int, default=1)
    efficiency_smoke.add_argument("--rerun", action="store_true", help="Overwrite non-empty candidate outputs instead of skipping them.")
    efficiency_smoke.add_argument("--max-active-gpt55", type=int, default=0, help="Refuse to start when more external GPT-5.5 Codex exec processes are active.")
    efficiency_smoke.add_argument("--timeout-seconds", type=int, default=900)
    efficiency_smoke.add_argument("--wait-for-clear-seconds", type=int, default=0, help="Poll for the GPT-5.5 lane to fall under --max-active-gpt55 before starting.")
    efficiency_smoke.add_argument("--wait-poll-seconds", type=int, default=30, help="Polling interval while waiting for the GPT-5.5 lane to clear.")
    efficiency_smoke.add_argument("--ignore-user-config", action="store_true")
    efficiency_smoke.add_argument("--dry-run", action="store_true", help="Select candidate chunks and return sanitized metadata without starting Codex.")
    efficiency_smoke.add_argument("--min-expected-events", type=int, help="Only select chunks with at least this many golden discourse events.")
    efficiency_smoke.add_argument("--max-expected-events", type=int, help="Only select chunks with at most this many golden discourse events.")
    efficiency_smoke.add_argument("--reasoning-effort", choices=["minimal", "low", "medium", "high"], default="low")
    efficiency_smoke.add_argument("--concurrency", type=int, default=1)

    efficiency_verify = sub.add_parser("efficiency-verify-candidates", help="Run a bounded LLM precision verifier over completed one-segment candidate outputs.")
    efficiency_verify.add_argument("--manifest", required=True)
    efficiency_verify.add_argument("--limit", type=int, default=1)
    efficiency_verify.add_argument("--model", default="gpt-5.5")
    efficiency_verify.add_argument("--reasoning-effort", choices=["minimal", "low", "medium", "high"], default="low")
    efficiency_verify.add_argument("--timeout-seconds", type=int, default=600)

    proposition_smoke = sub.add_parser("efficiency-proposition-smoke", help="Run proposition-first low-reasoning extraction on one-segment holdouts.")
    proposition_smoke.add_argument("--manifest", required=True)
    proposition_smoke.add_argument("--limit", type=int, default=1)
    proposition_smoke.add_argument("--concurrency", type=int, default=1)
    proposition_smoke.add_argument("--model", default="gpt-5.5")
    proposition_smoke.add_argument("--reasoning-effort", choices=["minimal", "low", "medium", "high"], default="low")
    proposition_smoke.add_argument("--timeout-seconds", type=int, default=600)

    event_core_smoke = sub.add_parser("efficiency-event-core-smoke", help="Run compact typed-event extraction while deferring full-schema enrichment.")
    event_core_smoke.add_argument("--manifest", required=True)
    event_core_smoke.add_argument("--limit", type=int, default=1)
    event_core_smoke.add_argument("--concurrency", type=int, default=1)
    event_core_smoke.add_argument("--model", default="gpt-5.5")
    event_core_smoke.add_argument("--reasoning-effort", choices=["minimal", "low", "medium", "high"], default="medium")
    event_core_smoke.add_argument("--timeout-seconds", type=int, default=600)

    windowed_core_smoke = sub.add_parser(
        "efficiency-windowed-core-smoke",
        help="Run context-aware four-window Sol event discovery with compact semantic outputs.",
    )
    windowed_core_smoke.add_argument("--manifest", required=True)
    windowed_core_smoke.add_argument("--limit", type=int, default=7)
    windowed_core_smoke.add_argument("--concurrency", type=int, default=4)
    windowed_core_smoke.add_argument("--model", default="gpt-5.6-sol")
    windowed_core_smoke.add_argument("--reasoning-effort", choices=["minimal", "low", "medium", "high"], default="low")
    windowed_core_smoke.add_argument("--timeout-seconds", type=int, default=600)
    windowed_core_smoke.add_argument("--window-count", type=int, default=4)
    windowed_core_smoke.add_argument("--context-chars", type=int, default=900)
    windowed_core_smoke.add_argument(
        "--guidelines",
        default=str(DEFAULT_WINDOWED_GUIDELINES_PATH),
        help="Structured LLM-authored calibration artifact.",
    )
    windowed_core_smoke.add_argument(
        "--max-total-events",
        type=int,
        default=25,
        help="Total event ceiling across all windows.",
    )
    windowed_core_smoke.add_argument("--min-expected-events", type=int, default=10)
    windowed_core_smoke.add_argument("--max-expected-events", type=int)
    windowed_core_smoke.add_argument("--retry-count", type=int, default=0)
    windowed_core_smoke.add_argument("--rerun", action="store_true")

    windowed_enrich = sub.add_parser(
        "efficiency-windowed-enrich",
        help="Batch Spark semantic enrichment for immutable windowed event cores and hydrate full v3.1 labels.",
    )
    windowed_enrich.add_argument("--core-manifest", required=True)
    windowed_enrich.add_argument("--limit", type=int, default=7)
    windowed_enrich.add_argument("--model", default="gpt-5.4-mini")
    windowed_enrich.add_argument("--reasoning-effort", choices=["minimal", "low", "medium", "high"], default="low")
    windowed_enrich.add_argument("--timeout-seconds", type=int, default=600)
    windowed_enrich.add_argument("--output-namespace")
    windowed_enrich.add_argument("--retry-count", type=int, default=0)
    windowed_enrich.add_argument("--rerun", action="store_true")

    windowed_judge = sub.add_parser(
        "efficiency-windowed-judge",
        help="Run a strict one-to-one LLM semantic judge over windowed event cores.",
    )
    windowed_judge.add_argument("--core-manifest", required=True)
    windowed_judge.add_argument("--output-dir", required=True)
    windowed_judge.add_argument("--limit", type=int, default=10)
    windowed_judge.add_argument("--concurrency", type=int, default=4)
    windowed_judge.add_argument("--model", default="gpt-5.3-codex-spark")
    windowed_judge.add_argument("--reasoning-effort", choices=["minimal", "low", "medium", "high"], default="low")
    windowed_judge.add_argument("--timeout-seconds", type=int, default=300)
    windowed_judge.add_argument("--candidate-output-dir", help="Optional hydrated v3.1 label directory.")
    windowed_judge.add_argument("--full-fields", action="store_true")
    windowed_judge.add_argument("--rerun", action="store_true")

    windowed_holdout = sub.add_parser(
        "efficiency-windowed-holdout",
        help="Export a deterministic source-stratified holdout excluding prior manifests.",
    )
    windowed_holdout.add_argument("--output", required=True)
    windowed_holdout.add_argument("--exclude-root", action="append", default=[])
    windowed_holdout.add_argument("--source-limit", type=int, default=0)
    windowed_holdout.add_argument("--min-events", type=int, default=14)
    windowed_holdout.add_argument("--max-events", type=int, default=25)
    windowed_holdout.add_argument("--seed", default="windowed-final-holdout-v1")

    judge_calibrate = sub.add_parser(
        "efficiency-judge-calibrate",
        help="Calibrate a blinded full-field semantic judge before paired acceptance scoring.",
    )
    judge_calibrate.add_argument("--output-dir", required=True)
    judge_calibrate.add_argument("--model", default="gpt-5.4-mini")
    judge_calibrate.add_argument(
        "--reasoning-effort",
        choices=["minimal", "low", "medium", "high"],
        default="low",
    )
    judge_calibrate.add_argument("--timeout-seconds", type=int, default=300)
    judge_calibrate.add_argument("--fixture", default=str(DEFAULT_JUDGE_FIXTURE_PATH))
    judge_calibrate.add_argument("--acceptance-spec", default=str(DEFAULT_ACCEPTANCE_SPEC_PATH))
    judge_calibrate.add_argument("--rerun", action="store_true")

    expanded_judge_calibrate = sub.add_parser(
        "efficiency-judge-calibrate-expanded",
        help="Run the frozen 60-case blinded set-level judge calibration once.",
    )
    expanded_judge_calibrate.add_argument("--output-dir", required=True)
    expanded_judge_calibrate.add_argument("--model", default="gpt-5.5")
    expanded_judge_calibrate.add_argument(
        "--reasoning-effort",
        choices=["minimal", "low", "medium", "high"],
        default="high",
    )
    expanded_judge_calibrate.add_argument("--timeout-seconds", type=int, default=900)
    expanded_judge_calibrate.add_argument("--evaluator-spec", default=str(DEFAULT_EVALUATOR_SPEC_PATH))
    expanded_judge_calibrate.add_argument("--rerun", action="store_true")

    paired_manifest = sub.add_parser(
        "efficiency-paired-manifest",
        help="Freeze a source-balanced no/low/medium/dense paired evaluation manifest.",
    )
    paired_manifest.add_argument("--output", required=True)
    paired_manifest.add_argument("--exclude-root", action="append", default=[])
    paired_manifest.add_argument("--acceptance-spec", default=str(DEFAULT_ACCEPTANCE_SPEC_PATH))
    paired_manifest.add_argument("--seed", default="windowed-paired-holdout-v1")
    paired_manifest.add_argument("--exclude-prior-episodes", action="store_true")

    paired_baseline = sub.add_parser(
        "efficiency-paired-baseline",
        help="Run the frozen fresh GPT-5.5 baseline without mutating SQLite.",
    )
    paired_baseline.add_argument("--manifest", required=True)
    paired_baseline.add_argument("--limit", type=int, default=0, help="Pilot limit; zero runs the full manifest.")
    paired_baseline.add_argument("--concurrency", type=int, default=4)
    paired_baseline.add_argument("--model", default="gpt-5.5")
    paired_baseline.add_argument(
        "--reasoning-effort",
        choices=["minimal", "low", "medium", "high"],
        default="high",
    )
    paired_baseline.add_argument("--timeout-seconds", type=int, default=900)
    paired_baseline.add_argument("--retry-count", type=int, default=1)
    paired_baseline.add_argument("--acceptance-spec", default=str(DEFAULT_ACCEPTANCE_SPEC_PATH))
    paired_baseline.add_argument("--rerun", action="store_true")

    paired_phase_one = sub.add_parser(
        "efficiency-paired-phase-one",
        help="Run alternating equal-concurrency baseline and frozen candidate-core blocks.",
    )
    paired_phase_one.add_argument("--manifest", required=True)
    paired_phase_one.add_argument("--output-dir", required=True)
    paired_phase_one.add_argument("--limit", type=int, default=0, help="Pilot limit; zero runs the full manifest.")
    paired_phase_one.add_argument("--timeout-seconds", type=int, default=900)
    paired_phase_one.add_argument("--acceptance-spec", default=str(DEFAULT_ACCEPTANCE_SPEC_PATH))
    paired_phase_one.add_argument("--rerun", action="store_true")

    paired_enrich = sub.add_parser(
        "efficiency-paired-enrich",
        help="Run one frozen Spark-only or Mini-only enrichment path over paired cores.",
    )
    paired_enrich.add_argument("--phase-one-report", required=True)
    paired_enrich.add_argument("--core-repair-report")
    paired_enrich.add_argument("--path", choices=["spark_only", "mini_only"], required=True)
    paired_enrich.add_argument("--batch-size", type=int, default=7)
    paired_enrich.add_argument("--timeout-seconds", type=int, default=600)
    paired_enrich.add_argument("--acceptance-spec", default=str(DEFAULT_EVALUATOR_SPEC_PATH))
    paired_enrich.add_argument("--rerun", action="store_true")

    paired_repair = sub.add_parser(
        "efficiency-paired-repair",
        help="Run one stronger Sol repair only for observable candidate-core failures.",
    )
    paired_repair.add_argument("--phase-one-report", required=True)
    paired_repair.add_argument("--timeout-seconds", type=int, default=600)
    paired_repair.add_argument("--acceptance-spec", default=str(DEFAULT_EVALUATOR_SPEC_PATH))
    paired_repair.add_argument("--rerun", action="store_true")

    paired_adjudication = sub.add_parser(
        "efficiency-paired-adjudication-export",
        help="Export anonymous A/B human packets only for non-identical paired residuals.",
    )
    paired_adjudication.add_argument("--phase-one-report", required=True)
    paired_adjudication.add_argument("--enrichment-report", required=True)
    paired_adjudication.add_argument("--output-dir", required=True)
    paired_adjudication.add_argument("--shared-human-output-dir")
    paired_adjudication.add_argument("--seed", default="windowed-paired-adjudication-v1")

    paired_adjudication_score = sub.add_parser(
        "efficiency-paired-adjudication-score",
        help="Score completed blinded human packets and compute paired bootstrap intervals.",
    )
    paired_adjudication_score.add_argument("--mapping", required=True)
    paired_adjudication_score.add_argument("--output")
    paired_adjudication_score.add_argument("--acceptance-spec", default=str(DEFAULT_EVALUATOR_SPEC_PATH))

    consolidated_adjudication = sub.add_parser(
        "efficiency-paired-adjudication-consolidate",
        help="Collapse multiple blinded path mappings into one human source read per segment.",
    )
    consolidated_adjudication.add_argument("--mapping", action="append", required=True)
    consolidated_adjudication.add_argument("--output-dir", required=True)
    consolidated_adjudication.add_argument("--seed", default="windowed-consolidated-human-v1")

    consolidated_materialize = sub.add_parser(
        "efficiency-paired-adjudication-materialize",
        help="Validate consolidated human reviews and fan them into path-specific score inputs.",
    )
    consolidated_materialize.add_argument("--mapping", required=True)

    paired_provenance = sub.add_parser(
        "efficiency-paired-provenance",
        help="Hash paired evaluator inputs and report shared episode-context accounting coverage.",
    )
    paired_provenance.add_argument("--phase-one-report", required=True)
    paired_provenance.add_argument("--output-dir", required=True)
    paired_provenance.add_argument("--evaluator-spec", default=str(DEFAULT_EVALUATOR_SPEC_PATH))

    context_usage_recovery = sub.add_parser(
        "efficiency-context-usage-recover",
        help="Recover exact historical episode-context usage from matching local Codex rollouts.",
    )
    context_usage_recovery.add_argument("--context-cost-report", required=True)
    context_usage_recovery.add_argument("--rollout-root", action="append", default=[])
    context_usage_recovery.add_argument("--output", required=True)
    context_usage_recovery.add_argument("--expected-input-tokens", type=int)
    context_usage_recovery.add_argument("--expected-output-tokens", type=int)
    context_usage_recovery.add_argument("--expected-total-tokens", type=int)

    no_signal_manifest = sub.add_parser(
        "efficiency-no-signal-manifest",
        help="Export 60 source-balanced zero-event candidates for explicit human cleanliness audit.",
    )
    no_signal_manifest.add_argument("--output", required=True)
    no_signal_manifest.add_argument("--exclude-root", action="append", default=[])
    no_signal_manifest.add_argument("--count", type=int, default=60)
    no_signal_manifest.add_argument("--seed", default="windowed-no-signal-power-v1")

    no_signal_report = sub.add_parser(
        "efficiency-no-signal-power-report",
        help="Compute a diagnostic binomial bound from counts; it cannot prove human provenance.",
    )
    no_signal_report.add_argument("--trials", type=int, required=True)
    no_signal_report.add_argument("--false-positives", type=int, required=True)
    no_signal_report.add_argument("--human-confirmed-clean", type=int, required=True)
    no_signal_report.add_argument("--output", required=True)
    no_signal_report.add_argument("--evaluator-spec", default=str(DEFAULT_EVALUATOR_SPEC_PATH))

    no_signal_audit = sub.add_parser(
        "efficiency-no-signal-audit-export",
        help="Export private source packets for explicit human no-signal cleanliness review.",
    )
    no_signal_audit.add_argument("--manifest", required=True)
    no_signal_audit.add_argument("--output-dir", required=True)

    no_signal_score = sub.add_parser(
        "efficiency-no-signal-audit-score",
        help="Score human-confirmed clean cases against a complete candidate result report.",
    )
    no_signal_score.add_argument("--mapping", required=True)
    no_signal_score.add_argument("--candidate-results", required=True)
    no_signal_score.add_argument("--output", required=True)
    no_signal_score.add_argument("--evaluator-spec", default=str(DEFAULT_EVALUATOR_SPEC_PATH))

    paired_gate = sub.add_parser(
        "efficiency-paired-gate",
        help="Build the single fail-closed acceptance report from frozen evidence artifacts.",
    )
    paired_gate.add_argument("--evidence-bundle", required=True)
    paired_gate.add_argument("--output")
    paired_gate.add_argument("--evaluator-spec", default=str(DEFAULT_EVALUATOR_SPEC_PATH))

    prospective_shadow = sub.add_parser(
        "efficiency-prospective-shadow-manifest",
        help="Select post-freeze episodes and preferably unseen shows for sustained shadow validation.",
    )
    prospective_shadow.add_argument("--output", required=True)
    prospective_shadow.add_argument("--frozen-after", required=True)
    prospective_shadow.add_argument("--exclude-root", action="append", default=[])
    prospective_shadow.add_argument("--minimum-segments", type=int, default=60)
    prospective_shadow.add_argument("--minimum-unseen-sources", type=int, default=5)
    prospective_shadow.add_argument("--max-segments-per-episode", type=int, default=2)
    prospective_shadow.add_argument("--seed", default="windowed-prospective-shadow-v1")

    recover_core = sub.add_parser(
        "efficiency-windowed-core-recover",
        help="Recover an interrupted core run without rerunning missing or failed calls.",
    )
    recover_core.add_argument("--source-manifest", required=True)
    recover_core.add_argument("--core-manifest", required=True)
    recover_core.add_argument("--output", required=True)
    recover_core.add_argument("--parent-exit-code", type=int, required=True)

    checkpointed_core = sub.add_parser(
        "efficiency-windowed-core-checkpointed",
        help="Run the frozen Sol core in durable four-call blocks without rerunning interrupted blocks.",
    )
    checkpointed_core.add_argument("--manifest", required=True)
    checkpointed_core.add_argument("--output-dir", required=True)
    checkpointed_core.add_argument("--timeout-seconds", type=int, default=600)
    checkpointed_core.add_argument("--block-size", type=int, default=4)
    checkpointed_core.add_argument("--evaluator-spec", default=str(DEFAULT_EVALUATOR_SPEC_PATH))
    checkpointed_core.add_argument("--guidelines", default=str(DEFAULT_WINDOWED_GUIDELINES_PATH))

    app_server_dev = sub.add_parser(
        "efficiency-app-server-dev-manifest",
        help="Freeze a retrospective no-signal plus dense app-server development set.",
    )
    app_server_dev.add_argument("--output", required=True)
    app_server_dev.add_argument("--episode-id", action="append", required=True)
    app_server_dev.add_argument("--segments-per-episode", type=int, default=16)
    app_server_dev.add_argument("--minimum-no-signal", type=int, default=1)
    app_server_dev.add_argument("--minimum-dense", type=int, default=8)
    app_server_dev.add_argument("--dense-event-min", type=int, default=16)
    app_server_dev.add_argument("--seed", default="app-server-development-v1")

    app_server_dev_v2 = sub.add_parser(
        "efficiency-app-server-dev-manifest-v2",
        help="Freeze a canonical-transcript, unique-text, multi-source app-server development set.",
    )
    app_server_dev_v2.add_argument("--output", required=True)
    app_server_dev_v2.add_argument("--episode-id", action="append", required=True)
    app_server_dev_v2.add_argument("--exclude-manifest", action="append", required=True)
    app_server_dev_v2.add_argument("--segments-per-episode", type=int, default=8)
    app_server_dev_v2.add_argument("--minimum-no-signal", type=int, default=1)
    app_server_dev_v2.add_argument("--maximum-no-signal", type=int, default=2)
    app_server_dev_v2.add_argument("--minimum-dense", type=int, default=7)
    app_server_dev_v2.add_argument("--dense-event-min", type=int, default=16)
    app_server_dev_v2.add_argument("--event-cap", type=int, default=32)
    app_server_dev_v2.add_argument("--seed", default="app-server-development-v2")

    app_server_arm = sub.add_parser(
        "efficiency-app-server-core-arm",
        help="Run one no-retry app-server episode-batch development arm.",
    )
    app_server_arm.add_argument("--manifest", required=True)
    app_server_arm.add_argument("--output-dir", required=True)
    app_server_arm.add_argument("--batch-size", type=int, required=True, choices=[3, 5, 8])
    app_server_arm.add_argument(
        "--thread-mode",
        required=True,
        choices=["new_thread", "same_thread"],
    )
    app_server_arm.add_argument("--model", default="gpt-5.6-sol")
    app_server_arm.add_argument(
        "--reasoning-effort",
        choices=["minimal", "low", "medium", "high"],
        default="low",
    )
    app_server_arm.add_argument("--concurrency", type=int, default=1)
    app_server_arm.add_argument("--timeout-seconds", type=float, default=600)
    app_server_arm.add_argument("--window-count", type=int, default=4)
    app_server_arm.add_argument("--context-chars", type=int, default=900)
    app_server_arm.add_argument(
        "--max-events",
        type=int,
        help="Must equal the event cap frozen in development manifest v2; defaults to that cap.",
    )
    app_server_arm.add_argument("--guidelines", default=str(DEFAULT_WINDOWED_GUIDELINES_PATH))

    app_server_matrix = sub.add_parser(
        "efficiency-app-server-matrix-aggregate",
        help="Fail closed unless all six symmetric app-server development arms are present.",
    )
    app_server_matrix.add_argument("--arm-report", action="append", required=True)
    app_server_matrix.add_argument("--output", required=True)

    proposition_classify = sub.add_parser("efficiency-proposition-classify", help="Classify proposition-first outputs into strict v3.1 candidate labels.")
    proposition_classify.add_argument("--manifest", required=True)
    proposition_classify.add_argument("--limit", type=int, default=1)
    proposition_classify.add_argument("--concurrency", type=int, default=1)
    proposition_classify.add_argument("--model", default="gpt-5.5")
    proposition_classify.add_argument("--reasoning-effort", choices=["minimal", "low", "medium", "high"], default="low")
    proposition_classify.add_argument("--timeout-seconds", type=int, default=600)

    proposition_decide = sub.add_parser("efficiency-proposition-decide", help="Classify proposition candidates with a minimal indexed decision schema.")
    proposition_decide.add_argument("--manifest", required=True)
    proposition_decide.add_argument("--limit", type=int, default=1)
    proposition_decide.add_argument("--model", default="gpt-5.5")
    proposition_decide.add_argument("--reasoning-effort", choices=["minimal", "low", "medium", "high"], default="low")
    proposition_decide.add_argument("--timeout-seconds", type=int, default=600)

    efficiency_status = sub.add_parser("efficiency-candidate-status", help="Summarize exported efficiency candidate prompt output coverage without reading prompt text.")
    efficiency_status.add_argument("--manifest", required=True, help="Manifest from efficiency-backtest --export-candidate-prompts.")
    efficiency_status.add_argument("--min-expected-events", type=int, help="Only rank next chunks with at least this many golden discourse events.")
    efficiency_status.add_argument("--max-expected-events", type=int, help="Only rank next chunks with at most this many golden discourse events.")
    efficiency_status.add_argument("--next-limit", type=int, default=10, help="Number of pending chunks to include in sanitized next-chunk guidance.")

    efficiency_sweep = sub.add_parser("efficiency-chunk-sweep", help="Compare sparse compact chunk sizes against the golden-label cost target.")
    efficiency_sweep.add_argument("--label-pack", default="ai_discourse_v3_1")
    efficiency_sweep.add_argument("--model", default="gpt-5.5")
    efficiency_sweep.add_argument("--chunk-sizes", default="6,8,10,12,15,20", help="Comma-separated sparse compact chunk sizes to compare.")
    efficiency_sweep.add_argument("--candidate-representation", choices=["sparse_compact", "offset_sparse_compact", "evidence_sparse_compact", "flat_ledger", "flat_ledger_full", "full_schema", "compact"], default="sparse_compact", help="Candidate batch output representation to sweep.")
    efficiency_sweep.add_argument("--episode-limit", type=int, help="Deterministic broad episode sample. Omit to use all golden episodes.")
    efficiency_sweep.add_argument("--per-source-limit", type=int, help="Cap selected episodes per source before round-robin sampling.")
    efficiency_sweep.add_argument("--seed", default="efficiency-backtest-v1")
    efficiency_sweep.add_argument("--report-dir", help="Directory for per-size backtest reports.")
    efficiency_sweep.add_argument("--output", help="Optional aggregate sweep JSON path.")

    efficiency_ready = sub.add_parser("efficiency-readiness", help="Summarize remaining proof gap for the efficient extraction candidate.")
    efficiency_ready.add_argument("--manifest", required=True, help="Manifest from efficiency-backtest --export-candidate-prompts.")
    efficiency_ready.add_argument("--sweep", required=True, help="Aggregate JSON from efficiency-chunk-sweep.")
    efficiency_ready.add_argument("--min-expected-events", type=int, default=25)
    efficiency_ready.add_argument("--max-expected-events", type=int, default=80)
    efficiency_ready.add_argument("--next-limit", type=int, default=5)

    distillation_export = sub.add_parser("efficiency-distillation-export", help="Export private source-held-out examples for supervised LLM distillation.")
    distillation_export.add_argument("--output-dir", required=True)
    distillation_export.add_argument("--label-pack", default="ai_discourse_v3_1")
    distillation_export.add_argument("--model", default="gpt-5.5")
    distillation_export.add_argument("--seed", default="distillation-source-split-v1")
    distillation_export.add_argument("--full-context", action="store_true", help="Retain verbose context and adjacent transcript excerpts instead of the compact training input.")
    distillation_export.add_argument("--task", choices=["full_label", "event_detection"], default="full_label")

    submit_reviewer = sub.add_parser("submit-reviewer-audit", help="Submit one GPT-5.5 reviewer audit JSON output.")
    submit_reviewer.add_argument("--audit-id", required=True)
    submit_reviewer.add_argument("--output-json", required=True)

    reviewer_findings_parser = sub.add_parser("reviewer-findings", help="List sanitized reviewer findings for remediation.")
    reviewer_findings_parser.add_argument("--pilot-id", required=True)
    reviewer_findings_parser.add_argument("--severity", default="P0,P1", help="Comma-separated severities to include, e.g. P0,P1.")
    reviewer_findings_parser.add_argument("--patch-tag")

    requeue_reviewed = sub.add_parser("requeue-reviewed-segments", help="Requeue v3.1 pilot segments selected by reviewer findings.")
    requeue_reviewed.add_argument("--pilot-id", required=True)
    requeue_reviewed.add_argument("--mode", required=True, choices=["failed-review-only", "reviewed-episodes"])
    requeue_reviewed.add_argument("--label-pack", default="ai_discourse_v3_1")
    requeue_reviewed.add_argument("--model", default="gpt-5.5")
    requeue_reviewed.add_argument("--priority", type=int, default=5)
    requeue_reviewed.add_argument("--worker-id", default="reviewer-remediation")
    requeue_reviewed.add_argument("--patch-tag")
    requeue_reviewed.add_argument("--dry-run", action="store_true")

    judge_identities = sub.add_parser("judge-identities", help="Run bounded v3.1 identity grooming for a pilot.")
    judge_identities.add_argument("--pilot-id")
    judge_identities.add_argument("--model", default="gpt-5.5")
    judge_identities.add_argument("--limit", type=int)

    cluster_claims_parser = sub.add_parser("cluster-claims", help="Build first-pass claim clusters for GPT-5.5 semantic review.")
    cluster_claims_parser.add_argument("--pilot-id")
    cluster_claims_parser.add_argument("--model", default="gpt-5.5")
    cluster_claims_parser.add_argument("--limit", type=int)

    canonicalize_claims_parser = sub.add_parser("canonicalize-claims", help="Build precise canonical claim memberships for trend and stance views.")
    canonicalize_claims_parser.add_argument("--pilot-id")
    canonicalize_claims_parser.add_argument("--scope", choices=["last_18_months", "all"], default="last_18_months")
    canonicalize_claims_parser.add_argument("--model", default="deterministic-local")
    canonicalize_claims_parser.add_argument("--limit", type=int)
    canonicalize_claims_parser.add_argument("--mode", choices=["deterministic", "semantic"], default="deterministic")
    canonicalize_claims_parser.add_argument("--dry-run", action="store_true")

    claim_edges = sub.add_parser("judge-claim-edges", help="Create claim-edge candidates that require GPT-5.5 semantic confirmation.")
    claim_edges.add_argument("--pilot-id")
    claim_edges.add_argument("--model", default="gpt-5.5")
    claim_edges.add_argument("--limit", type=int, default=100)

    acquisition = sub.add_parser("acquisition-funnel", help="Summarize transcript acquisition coverage by source.")
    acquisition.add_argument("--source")
    acquisition.add_argument("--limit", type=int, default=25)

    verify_sources_parser = sub.add_parser("verify-sources", help="Check source feeds before a large enqueue.")
    verify_sources_parser.add_argument("--source-list", required=True)

    run = sub.add_parser(
        "run",
        help="Run legacy deterministic queue work, or use `run daily` for bounded production orchestration.",
    )
    run.add_argument("run_mode", nargs="?", choices=["daily"], help="Compact production mode. Omit for the legacy queue runner.")
    run.add_argument("--lane", default="podcast")
    run.add_argument("--limit", type=int, default=20)
    run.add_argument("--model", default="gpt-5.4")
    run.add_argument("--label-pack", default="ai_discourse_v1")
    run.add_argument("--worker-id", default="local-controller")
    run.add_argument("--local-draft", action="store_true", help="Use deterministic local draft labels for tests/bootstrap.")
    run.add_argument("--no-claim-prompts", action="store_true", help="Leave label jobs pending instead of writing Codex prompts.")
    run.add_argument(
        "--job-types",
        help="Comma-separated queue job types this run may claim. Example: fetch_transcript,audit_label",
    )
    run.add_argument(
        "--max-label-prompts",
        type=int,
        default=25,
        help="Maximum label prompt handoffs this run may claim. Use 0 to prevent prompt creation.",
    )
    run.add_argument("--date", dest="run_date", help="Daily cycle date in YYYY-MM-DD; defaults to current UTC date.")
    run.add_argument("--max-runtime-seconds", type=int, default=DEFAULT_DAILY_RUNTIME_SECONDS)
    run.add_argument("--max-items", type=int, default=25, help="Per-stage daily item bound.")
    run.add_argument("--idempotency-key", help="Explicit retry key; identical keys replay the immutable receipt.")
    run.add_argument("--receipt-dir")
    run.add_argument("--source-list", help="Required only with --execute-ingestion.")
    run.add_argument("--since", help="Optional lower publication-date bound for daily RSS ingestion.")
    run.add_argument("--execute-ingestion", action="store_true")
    run.add_argument("--execute-normalize", action="store_true")
    run.add_argument(
        "--execute-extraction",
        action="store_true",
        help="Explicitly execute the bounded subscription-auth extraction stage.",
    )
    run.add_argument("--apply-reconcile", action="store_true")
    run.add_argument("--record-exception-contracts", action="store_true")
    run.add_argument("--publish-ops", dest="daily_publish_ops", action="store_true")
    run.add_argument("--snapshot-output")
    run.add_argument("--observer-url", default=DEFAULT_OBSERVER_URL)
    run.add_argument("--observer-token")
    run.add_argument("--observer-token-file")
    run.add_argument("--baseline-model", default="gpt-5.5")
    run.add_argument("--daily-label-pack", default="ai_discourse_v3_1")
    run.add_argument("--daily-pilot-id", default=ACCEPTED_PILOT_ID)

    prioritize = sub.add_parser("prioritize-labels", help="Promote a bounded subset of pending label jobs for a pilot run.")
    prioritize.add_argument("--lane", default="podcast")
    prioritize.add_argument("--label-pack", default="ai_discourse_v1")
    prioritize.add_argument("--limit", type=int, default=25)
    prioritize.add_argument("--priority", type=int, default=20)
    prioritize.add_argument("--pilot-id", help="Stable id to write into selected job payloads.")
    prioritize.add_argument("--source", action="append", default=[], help="Exact source name to include. May be repeated.")
    prioritize.add_argument("--category", action="append", default=[], help="Exact source category to include. May be repeated.")
    prioritize.add_argument("--keyword", action="append", default=[], help="Keyword to require in the episode title or segment text. May be repeated.")
    prioritize.add_argument("--title-regex", help="Regular expression that must match the episode title.")
    prioritize.add_argument("--per-source-limit", type=int, default=5)
    prioritize.add_argument("--dry-run", action="store_true")

    prepare = sub.add_parser("prepare-transcripts", help="Classify/clean ready transcripts before dense coding.")
    prepare.add_argument("--lane", default="podcast")
    prepare.add_argument("--limit", type=int, default=25)
    prepare.add_argument("--label-pack", default="ai_discourse_v2")
    prepare.add_argument("--source", action="append", default=[], help="Exact source name to include. May be repeated.")
    prepare.add_argument("--category", action="append", default=[], help="Exact source category to include. May be repeated.")
    prepare.add_argument("--force", action="store_true", help="Recompute existing transcript preparation rows.")
    prepare.add_argument("--enqueue-labels", action="store_true", help="Enqueue dense label jobs for prepared transcript segments.")
    prepare.add_argument("--include-low-signal", action="store_true", help="Also enqueue labels for low-signal prepared transcripts.")
    prepare.add_argument("--priority", type=int, default=25)
    prepare.add_argument("--pilot-id")

    discover = sub.add_parser("discover-concepts", help="Aggregate v3 discourse events into dynamic concept candidates.")
    discover.add_argument("--window", default="month", choices=["day", "week", "month", "quarter"])
    discover.add_argument("--min-evidence", type=int, default=3)
    discover.add_argument("--min-source-diversity", type=int, default=2)
    discover.add_argument("--min-usefulness", type=float, default=0.6)

    shifts = sub.add_parser("detect-shifts", help="Detect term bursts, term substitutions, and stance shifts from v3 discourse events.")
    shifts.add_argument("--slice", dest="slice_key", help="Optional slice such as org:Google, actor:name, source:Latent Space, or concept:agents.")
    shifts.add_argument("--window", default="month", choices=["day", "week", "month", "quarter"])
    shifts.add_argument("--min-support", type=int, default=2)

    claim = sub.add_parser("claim", help="Claim one label job and emit a prompt/output handoff.")
    claim.add_argument("--lane", default="podcast")
    claim.add_argument("--label-pack", default="ai_discourse_v1")
    claim.add_argument("--model", default="gpt-5.4")
    claim.add_argument("--worker-id", default="codex-app-worker")
    claim.add_argument("--pilot-id", help="Only claim a label job from this pilot.")

    claim_context = sub.add_parser("claim-context", help="Claim one full-episode context job and emit a GPT-5.5 prompt/output handoff.")
    claim_context.add_argument("--lane", default="podcast")
    claim_context.add_argument("--label-pack", default="ai_discourse_v3_1")
    claim_context.add_argument("--model", default="gpt-5.5")
    claim_context.add_argument("--worker-id", default="codex-context-worker")
    claim_context.add_argument("--pilot-id", help="Only claim an episode_context job from this pilot.")

    submit = sub.add_parser("submit", help="Submit a Codex-produced JSON label output.")
    submit.add_argument("--job-id", required=True, type=int)
    submit.add_argument("--output-json", required=True)
    submit.add_argument("--worker-id", default="codex-app-worker")
    submit.add_argument("--allow-expired", action="store_true", help="Explicit recovery mode for submitting after lease expiry.")

    submit_context = sub.add_parser("submit-context", help="Submit a GPT-5.5 full-episode context JSON output.")
    submit_context.add_argument("--job-id", required=True, type=int)
    submit_context.add_argument("--output-json", required=True)
    submit_context.add_argument("--worker-id", default="codex-context-worker")
    submit_context.add_argument("--allow-expired", action="store_true", help="Explicit recovery mode for submitting after lease expiry.")

    fail = sub.add_parser("fail", help="Mark a job failed.")
    fail.add_argument("--job-id", required=True, type=int)
    fail.add_argument("--reason", required=True)

    recover = sub.add_parser("recover-label-runs", help="Inspect or recover stranded label prompt handoffs.")
    recover.add_argument(
        "--release-missing-outputs",
        action="store_true",
        help="Mark claimed label runs with missing outputs failed and release their jobs for a fresh claim.",
    )

    audit = sub.add_parser("audit", help="Queue and summarize label quality audits.")
    audit.add_argument("--sample", type=float, default=0.1)
    audit.add_argument("--label-pack", default="ai_discourse_v1")
    audit.add_argument("--model", default="gpt-5.4")
    audit.add_argument("--fresh", action="store_true", help="Requeue audits even when prior audit jobs exist for the same labels.")

    export = sub.add_parser("export", help="Export reports and graph artifacts.")
    export_sub = export.add_subparsers(dest="export_command", required=True)
    trend = export_sub.add_parser("trend-report")
    trend.add_argument("--topic", required=True)
    trend.add_argument("--window", default="month", choices=["day", "week", "month"])
    trend.add_argument("--output")
    graph = export_sub.add_parser("graph")
    graph.add_argument("--type", required=True, choices=["guest_network", "org_network", "concept_network"])
    graph.add_argument("--output")
    terminology = export_sub.add_parser("terminology-drift")
    terminology.add_argument("--output")
    product_memo = export_sub.add_parser("product-correlation-memo")
    product_memo.add_argument("--output")
    signal_report = export_sub.add_parser("signal-report")
    signal_report.add_argument("--window", default="month", choices=["day", "week", "month", "quarter"])
    signal_report.add_argument("--limit", type=int, default=50)
    signal_report.add_argument("--output")
    actor_stance = export_sub.add_parser("actor-stance-report", help="Compatibility alias for the Claim Subject expert stance report.")
    actor_stance.add_argument("--window", default="month", choices=["day", "week", "month", "quarter"])
    actor_stance.add_argument("--output")
    term_drift = export_sub.add_parser("term-drift-report")
    term_drift.add_argument("--window", default="month", choices=["day", "week", "month", "quarter"])
    term_drift.add_argument("--output")
    narrative_map = export_sub.add_parser("narrative-map")
    narrative_map.add_argument("--window", default="month", choices=["day", "week", "month", "quarter"])
    narrative_map.add_argument("--output")

    snapshot = sub.add_parser("snapshot", help="Write sanitized observer snapshot JSON.")
    snapshot.add_argument("--output")

    publish = sub.add_parser("publish-snapshot", help="Post sanitized observer snapshot to the Railway UI.")
    publish.add_argument("--snapshot", default=str(exports_dir() / "observer-snapshot.json"))
    publish.add_argument("--url")
    publish.add_argument("--token")
    publish.add_argument("--token-file", help="Read the ingest token from a local file instead of argv.")

    dashboard = sub.add_parser("dashboard", help="Write a local static dashboard HTML shell.")
    dashboard.add_argument("--output", default=str(exports_dir() / "dashboard.html"))

    privacy = sub.add_parser("privacy-scan", help="Check exported artifacts for obvious private raw-text leaks.")
    privacy.add_argument("--path", default=str(exports_dir()))

    corpus = sub.add_parser("corpus-verify", help="Verify transcript and segment files exist and match stored hashes.")
    corpus.add_argument("--repair-hashes", action="store_true", help="Update stored hashes/word counts to match existing local files.")

    quarantine = sub.add_parser("quarantine-contaminated", help="Scan transcripts for raw JSON/caption residue and quarantine contaminated rows.")
    quarantine.add_argument("--apply", action="store_true", help="Apply quarantine cleanup. Default is dry-run.")
    quarantine.add_argument("--limit", type=int, help="Limit transcripts scanned, newest first.")
    quarantine.add_argument("--include-quarantined", action="store_true", help="Include already-quarantined transcripts in the scan report.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "run-pilot-labels":
        return _run_pilot_label_runner(args)
    if args.command == "lab":
        return _run_lab_surface(args)
    if args.command == "ui":
        from .private_ui import main as private_ui_main

        return private_ui_main(
            ["--db", str(args.db), "--host", str(args.host), "--port", str(args.port)]
        )
    conn = db.connect(Path(args.db))
    try:
        if args.command == "init":
            db.init_db(conn)
            print_json({"ok": True, "db": str(Path(args.db).resolve()), "root": str(root())})
        elif args.command == "status":
            db.init_db(conn)
            print_json(pif_status(conn))
        elif args.command == "release":
            db.init_db(conn)
            if args.release_command == "build":
                result = build_release(
                        conn,
                        pilot_id=args.pilot_id,
                        label_pack=args.label_pack,
                        model=args.model,
                        output_dir=args.output_dir,
                        release_id=args.release_id,
                    )
            elif args.release_command == "verify":
                result = verify_release(
                        conn,
                        release_id=args.release_id,
                        manifest_path=args.manifest,
                        output_dir=args.output_dir,
                    )
            elif args.release_command == "promote":
                result = promote_release(
                        conn,
                        release_id=args.release_id,
                        manifest_path=args.manifest,
                        verification_path=args.verification,
                        claims_path=args.claims,
                        pipeline_run_id=args.pipeline_run_id,
                        promoted_by=args.promoted_by,
                        rationale=args.rationale,
                        model=args.model,
                        output_dir=args.output_dir,
                        max_claims=args.max_claims,
                    )
            elif args.release_command == "supersede-legacy":
                result = supersede_legacy_label_jobs(
                        conn,
                        manifest_path=args.manifest,
                        verification_path=args.verification,
                        limit=args.limit,
                        backup_output=args.backup_output,
                        reason=args.reason,
                    )
            else:
                raise ValueError(f"Unknown release command: {args.release_command}")
            print_json(result)
            if _operation_failed(result):
                return 1
        elif args.command == "reconcile":
            db.init_db(conn)
            result = reconcile_target(
                    conn,
                    target=args.reconcile_command,
                    apply=args.apply,
                    release_id=args.release_id,
                    model=args.model,
                    limit=args.limit,
                    scope=args.scope,
                    output_dir=args.output_dir,
                )
            print_json(result)
            if _operation_failed(result):
                return 1
        elif args.command == "outcomes":
            db.init_db(conn)
            evidence = _json_object_argument(args.evidence_json) if args.evidence_json else None
            authoritative_evidence = (
                _json_value_argument(args.authoritative_evidence_json)
                if args.authoritative_evidence_json
                else None
            )
            result = plan_or_resolve_outcomes(
                    conn,
                    claim_id=args.claim_id,
                    outcome=args.outcome,
                    confidence=args.confidence,
                    rationale=args.rationale,
                    evidence=evidence,
                    authoritative_evidence=authoritative_evidence,
                    resolution_question=args.resolution_question,
                    due_at=args.due_at,
                    resolution_window_start=args.resolution_window_start,
                    resolution_window_end=args.resolution_window_end,
                    resolution_criteria=args.resolution_criteria,
                    as_of=args.as_of,
                    resolver_model=args.resolver_model,
                    resolver_version=args.resolver_version,
                    reviewer_version=args.reviewer_version,
                    review_status=args.review_status,
                    resolved_at=args.resolved_at,
                    categorical_score=args.categorical_score,
                    brier_score=args.brier_score,
                    pipeline_run_id=args.pipeline_run_id,
                    limit=args.limit,
                    max_runtime_ms=args.max_runtime_ms,
                    record_exceptions=args.record_exceptions,
                )
            print_json(result)
            if _operation_failed(result):
                return 1
        elif args.command == "publish-ops":
            db.init_db(conn)
            token = args.token
            if args.token_file:
                token = Path(args.token_file).expanduser().read_text(encoding="utf-8").strip()
            result = publish_ops(
                    conn,
                    output=args.output,
                    publish=args.publish,
                    observer_url=args.url,
                    token=token,
                )
            print_json(result)
            if _operation_failed(result):
                return 1
        elif args.command == "production-cycle":
            db.init_db(conn)
            print_json(
                production_cycle(
                    conn,
                    pilot_id=args.pilot_id,
                    model=args.model,
                    label_pack=args.label_pack,
                    worker_mode=args.worker_mode,
                    acquisition_limit=args.acquisition_limit,
                    extractor_limit=args.extractor_limit,
                    reviewer_limit=args.reviewer_limit,
                    run_graph_jobs=args.run_graph_jobs,
                    graph_limit=args.graph_limit,
                    snapshot_output=args.snapshot_output,
                    publish=args.publish,
                    observer_url=args.observer_url,
                    token_file=args.token_file,
                )
            )
        elif args.command == "railway-cost-guard":
            print_json(
                railway_cost_guard(
                    status_json_path=args.status_json,
                    project_name=args.project_name,
                    service_name=args.service_name,
                    allowed_services=[args.service_name, *args.allowed_service],
                    allowed_managed_storage_services=args.allowed_managed_storage_service,
                    max_replicas=args.max_replicas,
                )
            )
        elif args.command == "queue":
            db.init_db(conn)
            if args.queue_command == "status":
                print_json(queue_status(conn, group_by=_parse_csv(args.by)))
            elif args.queue_command == "sync-envelopes":
                print_json(sync_queue_envelopes(conn))
            else:
                raise ValueError(f"Unknown queue command: {args.queue_command}")
        elif args.command == "worker":
            db.init_db(conn)
            if args.worker_command == "run":
                print_json(
                    run_worker(
                        conn,
                        role=args.role,
                        lane=args.lane,
                        limit=args.limit,
                        model=args.model,
                        label_pack=args.label_pack,
                        worker_id=args.worker_id,
                        local_draft=args.local_draft,
                        claim_prompts=not args.no_claim_prompts,
                        burst=args.burst,
                    )
                )
            else:
                raise ValueError(f"Unknown worker command: {args.worker_command}")
        elif args.command == "headless-exec":
            db.init_db(conn)
            print_json(
                execute_claimed_label_runs(
                    conn,
                    lease_owner=args.lease_owner,
                    limit=args.limit,
                    model=args.model,
                    timeout_seconds=args.timeout_seconds,
                    audit=not args.no_audit,
                )
            )
        elif args.command == "headless-review":
            db.init_db(conn)
            print_json(
                execute_pending_reviewer_audits(
                    conn,
                    patch_tag=args.patch_tag,
                    limit=args.limit,
                    model=args.model,
                    timeout_seconds=args.timeout_seconds,
                    concurrency=args.concurrency,
                )
            )
        elif args.command == "remote-queue":
            db.init_db(conn)
            if args.remote_command == "claim-job":
                print_json(claim_remote_job(conn, worker_id=args.worker_id, capabilities=args.capability, max_items=args.max_items))
            elif args.remote_command == "submit-output":
                print_json(
                    submit_remote_output(
                        conn,
                        job_id=args.job_id,
                        worker_id=args.worker_id,
                        output_ref=args.output_ref,
                        status=args.status,
                    )
                )
            elif args.remote_command == "release-or-fail":
                print_json(
                    release_or_fail_remote_job(
                        conn,
                        job_id=args.job_id,
                        worker_id=args.worker_id,
                        reason=args.reason,
                        fail=args.fail,
                    )
                )
            else:
                raise ValueError(f"Unknown remote-queue command: {args.remote_command}")
        elif args.command == "mcp-broker":
            db.init_db(conn)
            if args.mcp_broker_command == "publish-snapshot":
                print_json(
                    publish_broker_snapshot(
                        conn,
                        broker_url=args.url,
                        token=read_token(args.token, args.token_file),
                        timeout_seconds=args.timeout_seconds,
                    )
                )
            elif args.mcp_broker_command == "publish-status":
                print_json(
                    publish_broker_status(
                        conn,
                        broker_url=args.url,
                        token=read_token(args.token, args.token_file),
                        timeout_seconds=args.timeout_seconds,
                    )
                )
            elif args.mcp_broker_command == "publish-work-packages":
                print_json(
                    publish_remote_work_packages(
                        conn,
                        broker_url=args.url,
                        token=read_token(args.token, args.token_file),
                        limit=args.limit,
                        worker_id=args.worker_id,
                        replace=args.replace,
                        timeout_seconds=args.timeout_seconds,
                    )
                )
            elif args.mcp_broker_command == "ensure-source-card-buffer":
                print_json(
                    ensure_source_card_buffer(
                        conn,
                        broker_url=args.url,
                        token=read_token(args.token, args.token_file),
                        target_buffer=args.target_buffer,
                        max_new=args.max_new,
                        worker_id=args.worker_id,
                        timeout_seconds=args.timeout_seconds,
                    )
                )
            elif args.mcp_broker_command == "import-submissions":
                print_json(
                    import_remote_submissions(
                        conn,
                        broker_url=args.url,
                        token=read_token(args.token, args.token_file),
                        limit=args.limit,
                        timeout_seconds=args.timeout_seconds,
                    )
                )
            elif args.mcp_broker_command == "audit-events":
                print_json(
                    fetch_broker_audit_events(
                        broker_url=args.url,
                        token=read_token(args.token, args.token_file),
                        limit=args.limit,
                        timeout_seconds=args.timeout_seconds,
                    )
                )
            else:
                raise ValueError(f"Unknown mcp-broker command: {args.mcp_broker_command}")
        elif args.command == "orchestrator":
            if args.orchestrator_command == "run":
                if not args.skip_init:
                    db.init_db(conn)
                broker_token = read_token(args.broker_token, args.broker_token_file) if (args.broker_token or args.broker_token_file) else None
                observer_token = read_token(args.observer_token, args.observer_token_file) if (args.observer_token or args.observer_token_file) else None
                print_json(
                    run_cycle(
                        conn,
                        cycle=args.cycle,
                        model=args.model,
                        label_pack=args.label_pack,
                        limit=args.limit,
                        broker_url=args.broker_url,
                        broker_token=broker_token,
                        observer_url=args.observer_url,
                        observer_token=observer_token,
                        wait_for_lock=args.wait_for_lock,
                        initialize_db=False,
                    )
                )
            else:
                raise ValueError(f"Unknown orchestrator command: {args.orchestrator_command}")
        elif args.command == "preflight":
            print_json(preflight(args.model))
        elif args.command == "enqueue":
            db.init_db(conn)
            print_json(enqueue_sources(conn, args.source_list, lane=args.lane, since=args.since, label_pack=args.label_pack))
        elif args.command == "backfill-episodes":
            db.init_db(conn)
            print_json(
                enqueue_sources(
                    conn,
                    args.source_list,
                    lane=args.lane,
                    since=args.since,
                    until=args.until,
                    source_filter=args.source,
                    max_items=args.max_items,
                    per_source_limit=args.per_source_limit,
                    oldest_first=args.oldest_first,
                    dry_run=args.dry_run,
                    enqueue_transcripts=args.enqueue_transcript_jobs,
                    fetch_concurrency=args.fetch_concurrency,
                    label_pack=args.label_pack,
                )
            )
        elif args.command == "transcript-strategy-report":
            db.init_db(conn)
            print_json(transcript_strategy_report(conn, strategy_path=args.strategy_file))
        elif args.command == "enqueue-transcript-backlog":
            db.init_db(conn)
            print_json(
                enqueue_transcript_backlog(
                    conn,
                    lane=args.lane,
                    label_pack=args.label_pack,
                    limit=args.limit,
                    source_filter=args.source,
                    include_youtube_captions=args.include_youtube_captions,
                    include_document_transcripts=args.include_document_transcripts,
                    include_quarantined_retry=args.include_quarantined_retry,
                    priority=args.priority,
                    dry_run=args.dry_run,
                )
            )
        elif args.command == "sync-verge-official-transcripts":
            db.init_db(conn)
            print_json(
                sync_verge_official_transcripts(
                    conn,
                    lane=args.lane,
                    label_pack=args.label_pack,
                    limit=args.limit,
                    source_filter=args.source,
                    dry_run=args.dry_run,
                )
            )
        elif args.command == "sync-gcp-official-transcripts":
            db.init_db(conn)
            print_json(
                sync_gcp_official_transcripts(
                    conn,
                    lane=args.lane,
                    label_pack=args.label_pack,
                    limit=args.limit,
                    max_archive_pages=args.max_archive_pages,
                    dry_run=args.dry_run,
                )
            )
        elif args.command == "transcript-candidates":
            db.init_db(conn)
            print_json(
                {
                    "ok": True,
                    "candidates": transcript_candidates(
                        conn,
                        lane=args.lane,
                        limit=args.limit,
                        claim=args.claim,
                        worker_id=args.worker_id,
                        lease_minutes=args.lease_minutes,
                        max_claimed=args.max_claimed,
                        per_source_limit=args.per_source_limit,
                    ),
                }
            )
        elif args.command == "release-transcript-candidates":
            db.init_db(conn)
            print_json(
                release_transcript_candidate_claims(
                    conn,
                    lane=args.lane,
                    worker_id=args.worker_id,
                    expired_only=args.expired_only,
                    limit=args.limit,
                )
            )
        elif args.command == "attach-transcript":
            db.init_db(conn)
            print_json(
                attach_transcript(
                    conn,
                    episode_id=args.episode_id,
                    transcript_url=args.transcript_url,
                    transcript_type=args.transcript_type,
                    source_kind=args.source_kind,
                    lane=args.lane,
                    label_pack=args.label_pack,
                )
            )
        elif args.command == "record-transcript-attempt":
            db.init_db(conn)
            print_json(
                record_transcript_acquisition_attempt(
                    conn,
                    episode_id=args.episode_id,
                    method=args.method,
                    status=args.status,
                    result_url=args.result_url,
                    result_source_kind=args.source_kind,
                    official_public=args.official_public,
                    policy_allowed=not args.policy_blocked,
                    error_class=args.error_class,
                    notes=args.notes,
                    worker_id=args.worker_id,
                )
            )
            conn.commit()
        elif args.command == "mark-transcript-exhausted":
            db.init_db(conn)
            print_json(mark_transcript_exhausted(conn, episode_id=args.episode_id, worker_id=args.worker_id, notes=args.notes))
            conn.commit()
        elif args.command == "enqueue-transcription":
            db.init_db(conn)
            print_json(
                enqueue_transcription_jobs(
                    conn,
                    lane=args.lane,
                    provider=args.provider,
                    label_pack=args.label_pack,
                    limit=args.limit,
                    priority=args.priority,
                    dry_run=args.dry_run,
                )
            )
        elif args.command == "groom-identities":
            db.init_db(conn)
            print_json(groom_identity_graph(conn, model=args.model, pilot_id=args.pilot_id, limit=args.limit))
        elif args.command == "scale-gate-report":
            db.init_db(conn)
            print_json(build_scale_gate_report(conn, pilot_id=args.pilot_id, output=args.output))
        elif args.command == "scale-batch-report":
            db.init_db(conn)
            print_json(build_scale_gate_report(conn, pilot_id=args.pilot_id, output=args.output))
        elif args.command == "scale-gate-enqueue":
            db.init_db(conn)
            print_json(
                enqueue_scale_gate(
                    conn,
                    pilot_id=args.pilot_id,
                    lane=args.lane,
                    label_pack=args.label_pack,
                    model=args.model,
                    sources=args.source,
                    limit=args.limit,
                    per_source_limit=args.per_source_limit,
                    priority=args.priority,
                    force_prepare=args.force_prepare,
                    include_low_signal=args.include_low_signal,
                )
            )
        elif args.command == "scale-batch-enqueue":
            db.init_db(conn)
            print_json(
                enqueue_controlled_100_batch(
                    conn,
                    pilot_id=args.pilot_id,
                    lane=args.lane,
                    label_pack=args.label_pack,
                    model=args.model,
                    priority=args.priority,
                    dry_run=args.dry_run,
                    force_prepare=args.force_prepare,
                    include_low_signal=args.include_low_signal,
                    acquired_since=args.acquired_since,
                )
            )
        elif args.command == "retry-failed-labels":
            db.init_db(conn)
            print_json(
                retry_failed_labels(
                    conn,
                    label_pack=args.label_pack,
                    mode=args.mode,
                    pilot_id=args.pilot_id,
                    limit=args.limit,
                    worker_id=args.worker_id,
                )
            )
        elif args.command == "cleanup-pilot-label-claims":
            db.init_db(conn)
            print_json(_cleanup_pilot_label_claims(conn, pilot_id=args.pilot_id, dry_run=args.dry_run))
        elif args.command == "reviewer-audit":
            db.init_db(conn)
            print_json(
                create_reviewer_audits(
                    conn,
                    pilot_id=args.pilot_id,
                    episodes=args.episodes,
                    model=args.model,
                    fresh=args.fresh,
                    mode=args.mode,
                    patch_tag=args.patch_tag,
                )
            )
        elif args.command == "failure-bank":
            db.init_db(conn)
            print_json(build_failure_bank(conn, pilot_id=args.pilot_id, patch_tag=args.patch_tag, output=args.output, check_only=args.check_only))
        elif args.command == "audit-delta":
            db.init_db(conn)
            print_json(
                queue_delta_audits(
                    conn,
                    pilot_id=args.pilot_id,
                    label_pack=args.label_pack,
                    model=args.model,
                    patch_tag=args.patch_tag,
                    sentinel=args.sentinel,
                    fresh=args.fresh,
                )
            )
        elif args.command == "efficiency-backtest":
            db.init_db(conn)
            print_json(
                run_efficiency_backtest(
                    conn,
                    label_pack=args.label_pack,
                    model=args.model,
                    episode_limit=args.episode_limit,
                    per_source_limit=args.per_source_limit,
                    seed=args.seed,
                    output=args.output,
                    candidate_output_dir=args.candidate_output_dir,
                    export_candidate_prompts=args.export_candidate_prompts,
                    candidate_chunk_size=args.candidate_chunk_size,
                    candidate_representation=args.candidate_representation,
                )
            )
        elif args.command == "efficiency-smoke-candidates":
            print_json(
                run_candidate_prompt_smoke(
                    manifest_path=args.manifest,
                    model=args.model,
                    limit=args.limit,
                    rerun=args.rerun,
                    max_active_gpt55=args.max_active_gpt55,
                    timeout_seconds=args.timeout_seconds,
                    ignore_user_config=args.ignore_user_config,
                    dry_run=args.dry_run,
                    min_expected_events=args.min_expected_events,
                    max_expected_events=args.max_expected_events,
                    wait_for_clear_seconds=args.wait_for_clear_seconds,
                    wait_poll_seconds=args.wait_poll_seconds,
                    reasoning_effort=args.reasoning_effort,
                    concurrency=args.concurrency,
                )
            )
        elif args.command == "efficiency-candidate-status":
            print_json(
                candidate_prompt_status(
                    manifest_path=args.manifest,
                    min_expected_events=args.min_expected_events,
                    max_expected_events=args.max_expected_events,
                    next_limit=args.next_limit,
                )
            )
        elif args.command == "efficiency-verify-candidates":
            db.init_db(conn)
            print_json(
                run_candidate_event_verifier_smoke(
                    conn,
                    manifest_path=args.manifest,
                    limit=args.limit,
                    model=args.model,
                    reasoning_effort=args.reasoning_effort,
                    timeout_seconds=args.timeout_seconds,
                )
            )
        elif args.command == "efficiency-proposition-smoke":
            db.init_db(conn)
            print_json(
                run_proposition_extractor_smoke(
                    conn,
                    manifest_path=args.manifest,
                    limit=args.limit,
                    concurrency=args.concurrency,
                    model=args.model,
                    reasoning_effort=args.reasoning_effort,
                    timeout_seconds=args.timeout_seconds,
                )
            )
        elif args.command == "efficiency-proposition-classify":
            db.init_db(conn)
            print_json(
                run_proposition_classifier_smoke(
                    conn,
                    manifest_path=args.manifest,
                    limit=args.limit,
                    concurrency=args.concurrency,
                    model=args.model,
                    reasoning_effort=args.reasoning_effort,
                    timeout_seconds=args.timeout_seconds,
                )
            )
        elif args.command == "efficiency-event-core-smoke":
            db.init_db(conn)
            print_json(run_event_core_smoke(conn, manifest_path=args.manifest, limit=args.limit, concurrency=args.concurrency, model=args.model, reasoning_effort=args.reasoning_effort, timeout_seconds=args.timeout_seconds))
        elif args.command == "efficiency-windowed-core-smoke":
            db.init_db(conn)
            print_json(
                run_windowed_event_core_smoke(
                    conn,
                    manifest_path=args.manifest,
                    limit=args.limit,
                    concurrency=args.concurrency,
                    model=args.model,
                    reasoning_effort=args.reasoning_effort,
                    timeout_seconds=args.timeout_seconds,
                    window_count=args.window_count,
                    context_chars=args.context_chars,
                    guideline_path=args.guidelines,
                    max_total_events=args.max_total_events,
                    min_expected_events=args.min_expected_events,
                    max_expected_events=args.max_expected_events,
                    retry_count=args.retry_count,
                    rerun=args.rerun,
                )
            )
        elif args.command == "efficiency-windowed-enrich":
            db.init_db(conn)
            print_json(
                run_windowed_event_enrichment_smoke(
                    conn,
                    core_manifest_path=args.core_manifest,
                    limit=args.limit,
                    model=args.model,
                    reasoning_effort=args.reasoning_effort,
                    timeout_seconds=args.timeout_seconds,
                    output_namespace=args.output_namespace,
                    retry_count=args.retry_count,
                    rerun=args.rerun,
                )
            )
        elif args.command == "efficiency-windowed-judge":
            db.init_db(conn)
            print_json(
                run_windowed_semantic_judge_smoke(
                    conn,
                    core_manifest_path=args.core_manifest,
                    output_dir=args.output_dir,
                    limit=args.limit,
                    concurrency=args.concurrency,
                    model=args.model,
                    reasoning_effort=args.reasoning_effort,
                    timeout_seconds=args.timeout_seconds,
                    candidate_output_dir=args.candidate_output_dir,
                    full_fields=args.full_fields,
                    rerun=args.rerun,
                )
            )
        elif args.command == "efficiency-windowed-holdout":
            db.init_db(conn)
            print_json(
                export_windowed_source_holdout_manifest(
                    conn,
                    output_path=args.output,
                    exclude_roots=args.exclude_root,
                    source_limit=args.source_limit,
                    min_events=args.min_events,
                    max_events=args.max_events,
                    seed=args.seed,
                )
            )
        elif args.command == "efficiency-judge-calibrate":
            print_json(
                run_judge_calibration(
                    output_dir=args.output_dir,
                    model=args.model,
                    reasoning_effort=args.reasoning_effort,
                    timeout_seconds=args.timeout_seconds,
                    fixture_path=args.fixture,
                    acceptance_spec_path=args.acceptance_spec,
                    rerun=args.rerun,
                )
            )
        elif args.command == "efficiency-judge-calibrate-expanded":
            print_json(
                run_expanded_judge_calibration(
                    output_dir=args.output_dir,
                    model=args.model,
                    reasoning_effort=args.reasoning_effort,
                    timeout_seconds=args.timeout_seconds,
                    evaluator_spec_path=args.evaluator_spec,
                    rerun=args.rerun,
                )
            )
        elif args.command == "efficiency-paired-manifest":
            db.init_db(conn)
            print_json(
                export_paired_evaluation_manifest(
                    conn,
                    output_path=args.output,
                    exclude_roots=args.exclude_root,
                    acceptance_spec_path=args.acceptance_spec,
                    seed=args.seed,
                    exclude_prior_episodes=args.exclude_prior_episodes,
                )
            )
        elif args.command == "efficiency-paired-baseline":
            db.init_db(conn)
            print_json(
                run_paired_baseline(
                    conn,
                    manifest_path=args.manifest,
                    limit=args.limit,
                    concurrency=args.concurrency,
                    model=args.model,
                    reasoning_effort=args.reasoning_effort,
                    timeout_seconds=args.timeout_seconds,
                    retry_count=args.retry_count,
                    acceptance_spec_path=args.acceptance_spec,
                    rerun=args.rerun,
                )
            )
        elif args.command == "efficiency-paired-phase-one":
            db.init_db(conn)
            print_json(
                run_paired_phase_one(
                    conn,
                    manifest_path=args.manifest,
                    output_dir=args.output_dir,
                    limit=args.limit,
                    timeout_seconds=args.timeout_seconds,
                    acceptance_spec_path=args.acceptance_spec,
                    rerun=args.rerun,
                )
            )
        elif args.command == "efficiency-paired-enrich":
            db.init_db(conn)
            print_json(
                run_paired_enrichment_path(
                    conn,
                    phase_one_report_path=args.phase_one_report,
                    path_name=args.path,
                    core_repair_report_path=args.core_repair_report,
                    batch_size=args.batch_size,
                    timeout_seconds=args.timeout_seconds,
                    acceptance_spec_path=args.acceptance_spec,
                    rerun=args.rerun,
                )
            )
        elif args.command == "efficiency-paired-repair":
            db.init_db(conn)
            print_json(
                run_paired_core_repairs(
                    conn,
                    phase_one_report_path=args.phase_one_report,
                    timeout_seconds=args.timeout_seconds,
                    acceptance_spec_path=args.acceptance_spec,
                    rerun=args.rerun,
                )
            )
        elif args.command == "efficiency-paired-adjudication-export":
            db.init_db(conn)
            print_json(
                export_blinded_paired_adjudication(
                    conn,
                    phase_one_report_path=args.phase_one_report,
                    enrichment_report_path=args.enrichment_report,
                    output_dir=args.output_dir,
                    shared_human_output_dir=args.shared_human_output_dir,
                    seed=args.seed,
                )
            )
        elif args.command == "efficiency-paired-adjudication-score":
            print_json(
                score_blinded_paired_adjudication(
                    mapping_path=args.mapping,
                    output_path=args.output,
                    acceptance_spec_path=args.acceptance_spec,
                )
            )
        elif args.command == "efficiency-paired-adjudication-consolidate":
            print_json(
                export_consolidated_human_adjudication(
                    mapping_paths=args.mapping,
                    output_dir=args.output_dir,
                    seed=args.seed,
                )
            )
        elif args.command == "efficiency-paired-adjudication-materialize":
            print_json(
                materialize_consolidated_human_outputs(
                    mapping_path=args.mapping,
                )
            )
        elif args.command == "efficiency-paired-provenance":
            db.init_db(conn)
            print_json(
                build_paired_provenance_and_context_cost(
                    conn,
                    phase_one_report_path=args.phase_one_report,
                    output_dir=args.output_dir,
                    evaluator_spec_path=args.evaluator_spec,
                )
            )
        elif args.command == "efficiency-context-usage-recover":
            db.init_db(conn)
            expected_usage = {
                field: value
                for field, value in {
                    "input_tokens": args.expected_input_tokens,
                    "output_tokens": args.expected_output_tokens,
                    "total_tokens": args.expected_total_tokens,
                }.items()
                if value is not None
            }
            print_json(
                recover_historical_episode_context_usage(
                    conn,
                    context_cost_report_path=args.context_cost_report,
                    rollout_roots=(
                        args.rollout_root
                        or [str(Path.home() / ".codex" / "sessions"), str(Path.home() / ".codex" / "archived_sessions")]
                    ),
                    output_path=args.output,
                    expected_usage=expected_usage,
                )
            )
        elif args.command == "efficiency-no-signal-manifest":
            db.init_db(conn)
            print_json(
                export_no_signal_power_manifest(
                    conn,
                    output_path=args.output,
                    exclude_roots=args.exclude_root,
                    count=args.count,
                    seed=args.seed,
                )
            )
        elif args.command == "efficiency-no-signal-power-report":
            print_json(
                build_no_signal_power_report(
                    trials=args.trials,
                    false_positives=args.false_positives,
                    human_confirmed_clean=args.human_confirmed_clean,
                    output_path=args.output,
                    evaluator_spec_path=args.evaluator_spec,
                )
            )
        elif args.command == "efficiency-no-signal-audit-export":
            db.init_db(conn)
            print_json(
                export_no_signal_human_audit(
                    conn,
                    manifest_path=args.manifest,
                    output_dir=args.output_dir,
                )
            )
        elif args.command == "efficiency-no-signal-audit-score":
            print_json(
                score_no_signal_human_audit(
                    mapping_path=args.mapping,
                    candidate_results_path=args.candidate_results,
                    output_path=args.output,
                    evaluator_spec_path=args.evaluator_spec,
                )
            )
        elif args.command == "efficiency-paired-gate":
            print_json(
                build_fail_closed_acceptance_report(
                    evidence_bundle_path=args.evidence_bundle,
                    output_path=args.output,
                    evaluator_spec_path=args.evaluator_spec,
                )
            )
        elif args.command == "efficiency-prospective-shadow-manifest":
            db.init_db(conn)
            print_json(
                export_prospective_shadow_manifest(
                    conn,
                    output_path=args.output,
                    frozen_after=args.frozen_after,
                    exclude_roots=args.exclude_root,
                    minimum_segments=args.minimum_segments,
                    minimum_unseen_sources=args.minimum_unseen_sources,
                    max_segments_per_episode=args.max_segments_per_episode,
                    seed=args.seed,
                )
            )
        elif args.command == "efficiency-windowed-core-recover":
            db.init_db(conn)
            print_json(
                recover_interrupted_windowed_core_run(
                    conn,
                    source_manifest_path=args.source_manifest,
                    core_manifest_path=args.core_manifest,
                    output_path=args.output,
                    parent_exit_code=args.parent_exit_code,
                )
            )
        elif args.command == "efficiency-windowed-core-checkpointed":
            db.init_db(conn)
            print_json(
                run_checkpointed_windowed_core(
                    conn,
                    manifest_path=args.manifest,
                    output_dir=args.output_dir,
                    timeout_seconds=args.timeout_seconds,
                    block_size=args.block_size,
                    evaluator_spec_path=args.evaluator_spec,
                    guideline_path=args.guidelines,
                )
            )
        elif args.command == "efficiency-app-server-dev-manifest":
            db.init_db(conn)
            print_json(
                export_app_server_development_manifest(
                    conn,
                    output_path=args.output,
                    episode_ids=args.episode_id,
                    segments_per_episode=args.segments_per_episode,
                    minimum_no_signal_per_episode=args.minimum_no_signal,
                    minimum_dense_per_episode=args.minimum_dense,
                    dense_event_min=args.dense_event_min,
                    seed=args.seed,
                )
            )
        elif args.command == "efficiency-app-server-dev-manifest-v2":
            db.init_db(conn)
            print_json(
                export_app_server_development_manifest_v2(
                    conn,
                    output_path=args.output,
                    episode_ids=args.episode_id,
                    exclusion_manifest_paths=args.exclude_manifest,
                    segments_per_episode=args.segments_per_episode,
                    minimum_no_signal_per_episode=args.minimum_no_signal,
                    maximum_no_signal_per_episode=args.maximum_no_signal,
                    minimum_dense_per_episode=args.minimum_dense,
                    dense_event_min=args.dense_event_min,
                    event_cap=args.event_cap,
                    seed=args.seed,
                )
            )
        elif args.command == "efficiency-app-server-core-arm":
            db.init_db(conn)
            print_json(
                _run_cancellable(
                    run_app_server_core_arm(
                        conn,
                        manifest_path=args.manifest,
                        output_dir=args.output_dir,
                        batch_size=args.batch_size,
                        thread_mode=args.thread_mode,
                        model=args.model,
                        reasoning_effort=args.reasoning_effort,
                        concurrency=args.concurrency,
                        timeout_seconds=args.timeout_seconds,
                        window_count=args.window_count,
                        context_chars=args.context_chars,
                        max_events_per_segment=args.max_events,
                        guideline_path=args.guidelines,
                    )
                )
            )
        elif args.command == "efficiency-app-server-matrix-aggregate":
            print_json(
                aggregate_app_server_development_matrix(
                    arm_report_paths=args.arm_report,
                    output_path=args.output,
                )
            )
        elif args.command == "efficiency-proposition-decide":
            db.init_db(conn)
            print_json(run_proposition_decision_smoke(conn, manifest_path=args.manifest, limit=args.limit, model=args.model, reasoning_effort=args.reasoning_effort, timeout_seconds=args.timeout_seconds))
        elif args.command == "efficiency-chunk-sweep":
            db.init_db(conn)
            chunk_sizes = [int(part.strip()) for part in args.chunk_sizes.split(",") if part.strip()]
            print_json(
                run_chunk_size_sweep(
                    conn,
                    label_pack=args.label_pack,
                    model=args.model,
                    chunk_sizes=chunk_sizes,
                    episode_limit=args.episode_limit,
                    per_source_limit=args.per_source_limit,
                    seed=args.seed,
                    report_dir=args.report_dir,
                    output=args.output,
                    candidate_representation=args.candidate_representation,
                )
            )
        elif args.command == "efficiency-readiness":
            print_json(
                efficiency_readiness(
                    manifest_path=args.manifest,
                    sweep_path=args.sweep,
                    min_expected_events=args.min_expected_events,
                    max_expected_events=args.max_expected_events,
                    next_limit=args.next_limit,
                )
            )
        elif args.command == "efficiency-distillation-export":
            db.init_db(conn)
            print_json(export_distillation_dataset(conn, output_dir=args.output_dir, label_pack=args.label_pack, model=args.model, seed=args.seed, compact_context=not args.full_context, task=args.task))
        elif args.command == "submit-reviewer-audit":
            db.init_db(conn)
            print_json(submit_reviewer_audit(conn, audit_id=args.audit_id, output_json_path=args.output_json))
        elif args.command == "reviewer-findings":
            db.init_db(conn)
            print_json(reviewer_findings(conn, pilot_id=args.pilot_id, severities=_parse_csv(args.severity), patch_tag=args.patch_tag))
        elif args.command == "requeue-reviewed-segments":
            db.init_db(conn)
            print_json(
                requeue_reviewed_segments(
                    conn,
                    pilot_id=args.pilot_id,
                    mode=args.mode,
                    label_pack=args.label_pack,
                    model=args.model,
                    priority=args.priority,
                    worker_id=args.worker_id,
                    patch_tag=args.patch_tag,
                    dry_run=args.dry_run,
                )
            )
        elif args.command == "judge-identities":
            db.init_db(conn)
            print_json(groom_identity_graph(conn, model=args.model, pilot_id=args.pilot_id, limit=args.limit))
        elif args.command == "cluster-claims":
            db.init_db(conn)
            print_json(cluster_claims(conn, pilot_id=args.pilot_id, model=args.model, limit=args.limit))
        elif args.command == "canonicalize-claims":
            db.init_db(conn)
            result = canonicalize_claims(
                conn,
                pilot_id=args.pilot_id,
                scope=args.scope,
                model=args.model,
                limit=args.limit,
                dry_run=args.dry_run,
                mode=args.mode,
            )
            result["claim_subjects"] = build_claim_subjects(
                conn,
                pilot_id=args.pilot_id,
                scope=args.scope,
                model=args.model,
                limit=args.limit,
                dry_run=args.dry_run,
            )
            print_json(result)
        elif args.command == "judge-claim-edges":
            db.init_db(conn)
            print_json(judge_claim_edges(conn, pilot_id=args.pilot_id, model=args.model, limit=args.limit))
        elif args.command == "acquisition-funnel":
            db.init_db(conn)
            print_json(acquisition_funnel(conn, source=args.source, limit=args.limit))
        elif args.command == "verify-sources":
            print_json(verify_sources(args.source_list))
        elif args.command == "run":
            db.init_db(conn)
            if args.run_mode == "daily":
                observer_token = args.observer_token
                if args.observer_token_file:
                    observer_token = Path(args.observer_token_file).expanduser().read_text(encoding="utf-8").strip()
                result = run_daily_cycle(
                        conn,
                        run_date=args.run_date,
                        receipt_dir=args.receipt_dir,
                        max_runtime_seconds=args.max_runtime_seconds,
                        max_items=args.max_items,
                        idempotency_key=args.idempotency_key,
                        source_list=args.source_list,
                        since=args.since,
                        execute_ingestion=args.execute_ingestion,
                        execute_normalize=args.execute_normalize,
                        execute_extraction=args.execute_extraction,
                        apply_reconcile=args.apply_reconcile,
                        record_exception_contracts=args.record_exception_contracts,
                        publish_observer=args.daily_publish_ops,
                        snapshot_output=args.snapshot_output,
                        observer_url=args.observer_url,
                        observer_token=observer_token,
                        lane=args.lane,
                        label_pack=args.daily_label_pack,
                        model=args.baseline_model,
                        pilot_id=args.daily_pilot_id,
                    )
                print_json(result)
                if _operation_failed(result):
                    return 1
            else:
                print_json(
                    run_jobs(
                        conn,
                        lane=args.lane,
                        limit=args.limit,
                        model=args.model,
                        label_pack=args.label_pack,
                        worker_id=args.worker_id,
                        local_draft=args.local_draft,
                        claim_prompts=not args.no_claim_prompts,
                        job_types=_parse_job_types(args.job_types),
                        max_label_prompts=args.max_label_prompts,
                    )
                )
        elif args.command == "prioritize-labels":
            db.init_db(conn)
            print_json(
                prioritize_label_jobs(
                    conn,
                    lane=args.lane,
                    label_pack=args.label_pack,
                    limit=args.limit,
                    priority=args.priority,
                    pilot_id=args.pilot_id,
                    sources=args.source,
                    categories=args.category,
                    keywords=args.keyword,
                    title_regex=args.title_regex,
                    per_source_limit=args.per_source_limit,
                    dry_run=args.dry_run,
                )
            )
        elif args.command == "prepare-transcripts":
            db.init_db(conn)
            print_json(
                prepare_transcripts_for_dense_coding(
                    conn,
                    lane=args.lane,
                    limit=args.limit,
                    label_pack=args.label_pack,
                    sources=args.source,
                    categories=args.category,
                    force=args.force,
                    enqueue_labels=args.enqueue_labels,
                    include_low_signal=args.include_low_signal,
                    priority=args.priority,
                    pilot_id=args.pilot_id,
                )
            )
        elif args.command == "discover-concepts":
            db.init_db(conn)
            print_json(
                discover_concepts(
                    conn,
                    window=args.window,
                    min_evidence=args.min_evidence,
                    min_source_diversity=args.min_source_diversity,
                    min_usefulness=args.min_usefulness,
                )
            )
        elif args.command == "detect-shifts":
            db.init_db(conn)
            print_json(detect_shifts(conn, slice_key=args.slice_key, window=args.window, min_support=args.min_support))
        elif args.command == "claim":
            db.init_db(conn)
            job = _claim_label_job(conn, args)
            print_json(job)
        elif args.command == "claim-context":
            db.init_db(conn)
            job = _claim_episode_context_job(conn, args)
            print_json(job)
        elif args.command == "submit":
            db.init_db(conn)
            print_json(
                submit_label_output(
                    conn,
                    job_id=args.job_id,
                    output_json_path=args.output_json,
                    worker_id=args.worker_id,
                    allow_expired=args.allow_expired,
                )
            )
        elif args.command == "submit-context":
            db.init_db(conn)
            print_json(
                submit_episode_context_output(
                    conn,
                    job_id=args.job_id,
                    output_json_path=args.output_json,
                    worker_id=args.worker_id,
                    allow_expired=args.allow_expired,
                )
            )
        elif args.command == "fail":
            db.init_db(conn)
            fail_job(conn, args.job_id, args.reason)
            conn.commit()
            print_json({"ok": True, "job_id": args.job_id})
        elif args.command == "recover-label-runs":
            db.init_db(conn)
            print_json(recover_label_handoffs(conn, release_missing_outputs=args.release_missing_outputs))
        elif args.command == "audit":
            db.init_db(conn)
            print_json(queue_audits(conn, sample=args.sample, label_pack=args.label_pack, model=args.model, fresh=args.fresh))
        elif args.command == "export":
            db.init_db(conn)
            if args.export_command == "trend-report":
                path = export_trend_report(conn, topic=args.topic, window=args.window, output=args.output)
            elif args.export_command == "graph":
                path = export_graph(conn, graph_type=args.type, output=args.output)
            elif args.export_command == "terminology-drift":
                path = export_terminology_drift(conn, output=args.output)
            elif args.export_command == "product-correlation-memo":
                path = export_product_correlation_memo(conn, output=args.output)
            elif args.export_command == "signal-report":
                path = export_signal_report(conn, window=args.window, limit=args.limit, output=args.output)
            elif args.export_command == "actor-stance-report":
                path = export_actor_stance_report(conn, window=args.window, output=args.output)
            elif args.export_command == "term-drift-report":
                path = export_term_drift_report(conn, window=args.window, output=args.output)
            elif args.export_command == "narrative-map":
                path = export_narrative_map(conn, window=args.window, output=args.output)
            else:
                raise ValueError(f"Unknown export command: {args.export_command}")
            print_json({"ok": True, "path": str(path)})
        elif args.command == "snapshot":
            db.init_db(conn)
            path = write_snapshot(conn, args.output)
            print_json({"ok": True, "path": str(path)})
        elif args.command == "publish-snapshot":
            token = args.token
            if args.token_file:
                token = Path(args.token_file).expanduser().read_text(encoding="utf-8").strip()
            print_json(publish_snapshot(args.snapshot, url=args.url, token=token))
        elif args.command == "dashboard":
            path = write_local_dashboard(args.output)
            print_json({"ok": True, "path": str(path)})
        elif args.command == "privacy-scan":
            print_json(privacy_scan(Path(args.path)))
        elif args.command == "corpus-verify":
            db.init_db(conn)
            print_json(corpus_verify(conn, repair_hashes=args.repair_hashes))
        elif args.command == "quarantine-contaminated":
            db.init_db(conn)
            print_json(
                quarantine_contaminated_transcripts(
                    conn,
                    apply=args.apply,
                    limit=args.limit,
                    include_quarantined=args.include_quarantined,
                )
            )
        else:
            raise AssertionError(args.command)
    except RuntimeError as exc:
        if "another research_factory process is initializing the SQLite schema" in str(exc):
            print_json(
                {
                    "ok": False,
                    "error": "initialization_in_progress",
                    "message": str(exc),
                },
                file=sys.stderr,
            )
            return 2
        raise
    finally:
        conn.close()
    return 0


def _run_lab_surface(args) -> int:
    if args.lab_command == "list":
        print_json(
            {
                "ok": True,
                "commands": list(LAB_COMMANDS),
                "writes_enabled": False,
                "policy": "explicit_allow_write_required",
            }
        )
        return 0
    lab_args = list(args.lab_args)
    allow_write = bool(args.allow_write)
    if "--allow-write" in lab_args:
        allow_write = True
        lab_args = [item for item in lab_args if item != "--allow-write"]
    if not allow_write:
        print_json(
            {
                "ok": True,
                "command": args.lab_command,
                "arguments": lab_args,
                "dry_run": True,
                "writes_enabled": False,
                "external_execution_enabled": False,
                "legacy_command_available": True,
                "next": "repeat with `pif lab --allow-write ...` only after reviewing the legacy evaluator command",
            }
        )
        return 0
    return main(["--db", args.db, args.lab_command, *lab_args])


def _json_object_argument(value: str) -> dict[str, object]:
    if value.startswith("@"):
        payload = json.loads(Path(value[1:]).expanduser().read_text(encoding="utf-8"))
    else:
        payload = json.loads(value)
    if not isinstance(payload, dict):
        raise ValueError("--evidence-json must contain a JSON object")
    serialized = dumps_json(payload)
    if len(serialized) > 16_000:
        raise ValueError("--evidence-json exceeds the 16,000 byte bound")
    if "BEGIN RAW TRANSCRIPT" in serialized.upper():
        raise ValueError("raw transcript text is not allowed in outcome evidence metadata")
    return payload


def _json_value_argument(value: str):
    if value.startswith("@"):
        payload = json.loads(Path(value[1:]).expanduser().read_text(encoding="utf-8"))
    else:
        payload = json.loads(value)
    if not isinstance(payload, (dict, list)):
        raise ValueError("--authoritative-evidence-json must contain a JSON object or array")
    serialized = dumps_json(payload)
    if len(serialized) > 32_000:
        raise ValueError("--authoritative-evidence-json exceeds the 32,000 byte bound")
    if "BEGIN RAW TRANSCRIPT" in serialized.upper():
        raise ValueError("raw transcript text is not allowed in outcome evidence metadata")
    return payload


def _run_pilot_label_runner(args) -> int:
    script = root() / "work" / "complete_v31_pilot_labels.py"
    command = [
        sys.executable,
        str(script),
        "--model",
        args.model,
        "--label-pack",
        args.label_pack,
        "--concurrency",
        str(args.concurrency),
        "--max-jobs",
        str(args.max_jobs),
        "--max-failures",
        str(args.max_failures),
        "--timeout-seconds",
        str(args.timeout_seconds),
        "--worker-prefix",
        args.worker_prefix,
    ]
    if args.pilot_id:
        command.extend(["--pilot-id", args.pilot_id])
    completed = subprocess.run(command, cwd=str(root()))
    return completed.returncode


def _cleanup_pilot_label_claims(conn, *, pilot_id: str, dry_run: bool = False) -> dict[str, object]:
    from .worker import release_job

    rows = conn.execute(
        """
        SELECT id
        FROM jobs
        WHERE job_type = 'label_segment'
          AND status = 'claimed'
          AND json_extract(payload_json, '$.pilot_id') = ?
          AND json_extract(payload_json, '$.label_run_id') IS NULL
          AND json_extract(payload_json, '$.output_path') IS NULL
        ORDER BY id
        """,
        (pilot_id,),
    ).fetchall()
    job_ids = [int(row["id"]) for row in rows]
    if not dry_run:
        for job_id in job_ids:
            release_job(conn, job_id)
        conn.commit()
    return {
        "ok": True,
        "pilot_id": pilot_id,
        "dry_run": dry_run,
        "released": 0 if dry_run else len(job_ids),
        "would_release": len(job_ids),
        "job_ids": job_ids,
    }


def _claim_label_job(conn, args) -> dict[str, str]:
    job = _claim_next_label_job(
        conn,
        lane=args.lane,
        worker_id=args.worker_id,
        pilot_id=args.pilot_id,
        label_pack=args.label_pack,
        model=args.model,
    )
    if not job:
        return {"ok": "false", "message": "no pending jobs"}
    if job["job_type"] != "label_segment":
        from .worker import release_job

        release_job(conn, job["id"])
        conn.commit()
        return {"ok": "false", "message": f"next job was {job['job_type']}; run deterministic jobs first"}
    payload = loads_json(job["payload_json"], {})
    effective_pack = payload.get("label_pack", args.label_pack)
    effective_model = payload.get("model", args.model)
    context_gate = gate_v31_label_on_episode_context(conn, job, label_pack=effective_pack, model=effective_model)
    if context_gate:
        return {"ok": "false", **context_gate}
    return create_label_prompt(conn, job, label_pack=effective_pack, model=effective_model, worker_id=args.worker_id)


def _claim_next_label_job(
    conn,
    *,
    lane: str,
    worker_id: str,
    pilot_id: str | None,
    label_pack: str,
    model: str,
    lease_minutes: int = 45,
):
    now = now_iso()
    leased_until = (dt.datetime.fromisoformat(now) + dt.timedelta(minutes=lease_minutes)).isoformat()
    pilot_filter = ""
    params: list[object] = [worker_id, leased_until, now, lane, label_pack, label_pack]
    if pilot_id:
        pilot_filter = "AND json_extract(candidate.payload_json, '$.pilot_id') = ?"
        params.append(pilot_id)
    params.extend(
        [
            now,
            label_pack,
            model,
            EPISODE_CONTEXT_SCHEMA_VERSION,
            label_pack,
            model,
            label_pack,
            model,
            EPISODE_CONTEXT_SCHEMA_VERSION,
        ]
    )
    claimed = conn.execute(
        f"""
        UPDATE jobs
        SET status = 'claimed',
            lease_owner = ?,
            leased_until = ?,
            attempts = attempts + 1,
            updated_at = ?
        WHERE id = (
          SELECT candidate.id
          FROM jobs AS candidate
          WHERE candidate.lane = ?
            AND candidate.job_type = 'label_segment'
            AND COALESCE(json_extract(candidate.payload_json, '$.label_pack'), ?) = ?
            {pilot_filter}
            AND candidate.attempts < candidate.max_attempts
            AND (candidate.status = 'pending' OR (candidate.status = 'claimed' AND candidate.leased_until < ?))
            AND NOT EXISTS (
              SELECT 1
              FROM segments AS pending_segments
              JOIN jobs AS pending_context
                ON pending_context.target_id = pending_segments.episode_id
              WHERE pending_segments.id = candidate.target_id
                AND pending_context.job_type = 'episode_context'
                AND pending_context.status IN ('pending', 'claimed')
                AND json_extract(pending_context.payload_json, '$.label_pack') = COALESCE(json_extract(candidate.payload_json, '$.label_pack'), ?)
                AND json_extract(pending_context.payload_json, '$.model') = COALESCE(json_extract(candidate.payload_json, '$.model'), ?)
                AND json_extract(pending_context.payload_json, '$.episode_context_version') = ?
            )
          ORDER BY
            CASE
              WHEN EXISTS (
                SELECT 1
                FROM segments
                JOIN episode_context_runs
                  ON episode_context_runs.episode_id = segments.episode_id
                WHERE segments.id = candidate.target_id
                  AND episode_context_runs.label_pack = COALESCE(json_extract(candidate.payload_json, '$.label_pack'), ?)
                  AND episode_context_runs.model = COALESCE(json_extract(candidate.payload_json, '$.model'), ?)
                  AND episode_context_runs.status = 'completed'
              )
              THEN 0 ELSE 1
            END,
            CASE
              WHEN EXISTS (
                SELECT 1
                FROM segments AS failed_segments
                JOIN jobs AS failed_context
                  ON failed_context.target_id = failed_segments.episode_id
                WHERE failed_segments.id = candidate.target_id
                  AND failed_context.job_type = 'episode_context'
                  AND failed_context.status = 'failed'
                  AND json_extract(failed_context.payload_json, '$.label_pack') = COALESCE(json_extract(candidate.payload_json, '$.label_pack'), ?)
                  AND json_extract(failed_context.payload_json, '$.model') = COALESCE(json_extract(candidate.payload_json, '$.model'), ?)
                  AND json_extract(failed_context.payload_json, '$.episode_context_version') = ?
              )
              THEN 1 ELSE 0
            END,
            candidate.priority ASC,
            candidate.id ASC
          LIMIT 1
        )
        RETURNING *
        """,
        params,
    ).fetchone()
    conn.commit()
    return claimed


def _claim_episode_context_job(conn, args) -> dict[str, str]:
    from .worker import claim_next_job, release_job

    if args.pilot_id:
        job = _claim_episode_context_job_for_pilot(conn, args)
    else:
        job = claim_next_job(conn, lane=args.lane, worker_id=args.worker_id, job_types=("episode_context",))
    if not job:
        return {"ok": "false", "message": "no pending episode_context jobs"}
    if job["job_type"] != "episode_context":
        release_job(conn, job["id"])
        conn.commit()
        return {"ok": "false", "message": f"next job was {job['job_type']}; run deterministic jobs first"}
    try:
        return create_episode_context_prompt(conn, job, label_pack=args.label_pack, model=args.model, worker_id=args.worker_id)
    except Exception as exc:
        reason = str(exc)
        fail_or_retry_job(conn, job, reason)
        conn.commit()
        return {
            "ok": "false",
            "status": "episode_context_prompt_failed",
            "job_id": str(job["id"]),
            "message": reason,
        }


def _claim_episode_context_job_for_pilot(conn, args):
    now = now_iso()
    leased_until = (dt.datetime.fromisoformat(now) + dt.timedelta(minutes=45)).isoformat()
    return conn.execute(
        """
        UPDATE jobs
        SET status = 'claimed',
            lease_owner = ?,
            leased_until = ?,
            attempts = attempts + 1,
            updated_at = ?
        WHERE id = (
          SELECT id FROM jobs
          WHERE lane = ?
            AND job_type = 'episode_context'
            AND attempts < max_attempts
            AND (status = 'pending' OR (status = 'claimed' AND leased_until < ?))
            AND payload_json LIKE ?
          ORDER BY priority ASC, id ASC
          LIMIT 1
        )
        RETURNING *
        """,
        (args.worker_id, leased_until, now, args.lane, now, f'%"pilot_id":"{args.pilot_id}"%'),
    ).fetchone()


def _parse_job_types(value: str | None) -> tuple[str, ...] | None:
    if not value:
        return None
    allowed = {"fetch_transcript", "transcribe_audio", "prepare_transcript", "episode_context", "label_segment", "audit_label", "manual_transcript_required"}
    parsed = tuple(item.strip() for item in value.split(",") if item.strip())
    unknown = sorted(set(parsed) - allowed)
    if unknown:
        raise ValueError(f"Unknown job type(s): {', '.join(unknown)}")
    return parsed


def _parse_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def prioritize_label_jobs(
    conn,
    *,
    lane: str,
    label_pack: str,
    limit: int,
    priority: int,
    pilot_id: str | None,
    sources: list[str],
    categories: list[str],
    keywords: list[str],
    title_regex: str | None,
    per_source_limit: int,
    dry_run: bool,
) -> dict[str, object]:
    if limit < 1:
        raise ValueError("--limit must be at least 1")
    if per_source_limit < 1:
        raise ValueError("--per-source-limit must be at least 1")
    source_filters = [item.strip().lower() for item in sources if item.strip()]
    category_filters = [item.strip().lower() for item in categories if item.strip()]
    keyword_filters = [item.strip().lower() for item in keywords if item.strip()]
    source_rank = {name: index for index, name in enumerate(source_filters)}
    title_pattern = re.compile(title_regex, re.I) if title_regex else None
    assigned_pilot_id = pilot_id or f"pilot-{now_iso().replace(':', '').replace('+', 'z')}"
    rows = conn.execute(
        """
        SELECT
          jobs.id AS job_id,
          jobs.priority,
          jobs.payload_json,
          segments.id AS segment_id,
          segments.text_path,
          episodes.id AS episode_id,
          episodes.title,
          episodes.published_at,
          sources.name AS source_name,
          sources.category
        FROM jobs
        JOIN segments ON segments.id = jobs.target_id
        JOIN episodes ON episodes.id = segments.episode_id
        JOIN sources ON sources.id = segments.source_id
        WHERE jobs.lane = ?
          AND jobs.job_type = 'label_segment'
          AND jobs.status = 'pending'
          AND jobs.attempts < jobs.max_attempts
        ORDER BY jobs.priority ASC, jobs.id ASC
        """,
        (lane,),
    ).fetchall()
    eligible = []
    base = root()
    for row in rows:
        payload = json.loads(row["payload_json"] or "{}")
        if payload.get("label_pack") and payload.get("label_pack") != label_pack:
            continue
        source_name = str(row["source_name"] or "")
        source_key = source_name.lower()
        category = str(row["category"] or "")
        category_key = category.lower()
        title = str(row["title"] or "")
        if source_filters and source_key not in source_filters:
            continue
        if category_filters and category_key not in category_filters:
            continue
        if title_pattern and not title_pattern.search(title):
            continue
        haystack = " ".join([source_name, category, title]).lower()
        segment_text = ""
        keyword_hits = 0
        if keyword_filters:
            missing_keyword_context = True
            text_path = base / row["text_path"]
            if text_path.exists():
                segment_text = read_text(text_path).lower()
            for keyword in keyword_filters:
                if keyword in haystack or keyword in segment_text:
                    keyword_hits += 1
                    missing_keyword_context = False
            if missing_keyword_context:
                continue
        eligible.append(
            {
                "job_id": row["job_id"],
                "segment_id": row["segment_id"],
                "episode_id": row["episode_id"],
                "source_name": source_name,
                "category": category,
                "title": title,
                "published_at": row["published_at"],
                "old_priority": row["priority"],
                "new_priority": priority,
                "keyword_hits": keyword_hits,
                "payload": payload,
                "_source_rank": source_rank.get(source_key, len(source_rank)),
            }
        )
    eligible.sort(key=lambda item: (item["_source_rank"], -int(item["keyword_hits"]), str(item["published_at"] or ""), int(item["job_id"])))
    selected = []
    source_counts: dict[str, int] = {}
    for item in eligible:
        source_key = item["source_name"].lower()
        if source_counts.get(source_key, 0) >= per_source_limit:
            continue
        source_counts[source_key] = source_counts.get(source_key, 0) + 1
        selected.append(item)
        if len(selected) >= limit:
            break
    if not dry_run:
        ts = now_iso()
        for item in selected:
            payload = {
                **item["payload"],
                "label_pack": label_pack,
                "pilot_id": assigned_pilot_id,
                "priority_reason": "bounded_source_aware_label_pilot",
            }
            conn.execute(
                """
                UPDATE jobs
                SET priority = ?,
                    payload_json = ?,
                    updated_at = ?
                WHERE id = ?
                  AND status = 'pending'
                """,
                (priority, dumps_json(payload), ts, item["job_id"]),
            )
        conn.commit()
    public_selected = [
        {key: value for key, value in item.items() if not key.startswith("_") and key != "payload"}
        for item in selected
    ]
    return {
        "ok": True,
        "dry_run": dry_run,
        "pilot_id": assigned_pilot_id,
        "matched": len(eligible),
        "selected": len(selected),
        "updated": 0 if dry_run else len(selected),
        "selected_jobs": public_selected,
    }


def prepare_transcripts_for_dense_coding(
    conn,
    *,
    lane: str,
    limit: int,
    label_pack: str,
    sources: list[str],
    categories: list[str],
    force: bool,
    enqueue_labels: bool,
    include_low_signal: bool,
    priority: int,
    pilot_id: str | None,
) -> dict[str, object]:
    if limit < 1:
        raise ValueError("--limit must be at least 1")
    source_filters = {item.strip().lower() for item in sources if item.strip()}
    category_filters = {item.strip().lower() for item in categories if item.strip()}
    assigned_pilot_id = pilot_id or f"pilot-{label_pack}-{now_iso().replace(':', '').replace('+', 'z')}"
    rows = conn.execute(
        """
        SELECT
          transcripts.id AS transcript_id,
          transcripts.status AS transcript_status,
          episodes.title AS episode_title,
          episodes.published_at,
          sources.name AS source_name,
          sources.category,
          transcript_preparations.id AS existing_preparation_id
        FROM transcripts
        JOIN episodes ON episodes.id = transcripts.episode_id
        JOIN sources ON sources.id = episodes.source_id
        LEFT JOIN transcript_preparations ON transcript_preparations.transcript_id = transcripts.id
        WHERE transcripts.status = 'ready'
        ORDER BY episodes.published_at DESC, transcripts.updated_at DESC
        """
    ).fetchall()
    selected = []
    for row in rows:
        if not force and row["existing_preparation_id"]:
            continue
        if source_filters and str(row["source_name"] or "").lower() not in source_filters:
            continue
        if category_filters and str(row["category"] or "").lower() not in category_filters:
            continue
        selected.append(row)
        if len(selected) >= limit:
            break
    prepared = []
    label_jobs = 0
    skipped_low_signal = 0
    artifact_counts: dict[str, int] = {}
    for row in selected:
        prep = prepare_transcript(conn, row["transcript_id"], force=force)
        artifact_counts[prep["artifact_type"]] = artifact_counts.get(prep["artifact_type"], 0) + 1
        enqueued_for_transcript = 0
        if enqueue_labels and (include_low_signal or prep["status"] == "prepared"):
            for segment in conn.execute(
                """
                SELECT id
                FROM segments
                WHERE transcript_id = ?
                  AND NOT EXISTS (
                    SELECT 1 FROM labels
                    WHERE labels.segment_id = segments.id
                      AND labels.label_pack = ?
                  )
                ORDER BY segment_index
                """,
                (row["transcript_id"], label_pack),
            ).fetchall():
                job_id = db.enqueue_job(
                    conn,
                    lane=lane,
                    job_type="label_segment",
                    target_id=segment["id"],
                    payload={
                        "label_pack": label_pack,
                        "pilot_id": assigned_pilot_id,
                        "transcript_preparation_id": prep["id"],
                        "transcript_artifact_type": prep["artifact_type"],
                        "source_quality_score": prep["quality_score"],
                        "priority_reason": "prepared_dense_coding_pilot",
                    },
                    priority=priority,
                )
                if job_id:
                    enqueued_for_transcript += 1
                    label_jobs += 1
        elif enqueue_labels and prep["status"] != "prepared":
            skipped_low_signal += 1
        prepared.append(
            {
                "transcript_id": row["transcript_id"],
                "source_name": row["source_name"],
                "title": row["episode_title"],
                "artifact_type": prep["artifact_type"],
                "status": prep["status"],
                "quality_score": prep["quality_score"],
                "boilerplate_ratio": prep["boilerplate_ratio"],
                "label_jobs": enqueued_for_transcript,
            }
        )
    conn.commit()
    return {
        "ok": True,
        "pilot_id": assigned_pilot_id,
        "selected": len(selected),
        "prepared": len(prepared),
        "label_jobs": label_jobs,
        "skipped_low_signal": skipped_low_signal,
        "artifact_counts": artifact_counts,
        "transcripts": prepared,
    }


def queue_audits(conn, *, sample: float, label_pack: str, model: str, fresh: bool = False) -> dict[str, int | float | str | bool]:
    rows = conn.execute(
        "SELECT id, segment_id FROM labels WHERE label_pack = ? ORDER BY created_at DESC",
        (label_pack,),
    ).fetchall()
    if not rows:
        return {"queued": 0, "sample": sample, "label_pack": label_pack}
    every = max(int(1 / sample), 1) if sample > 0 else len(rows) + 1
    queued = 0
    refreshed = 0
    for index, row in enumerate(rows):
        if index % every != 0:
            continue
        payload = {"segment_id": row["segment_id"], "label_pack": label_pack, "model": model}
        if fresh:
            refreshed += conn.execute(
                """
                DELETE FROM jobs
                WHERE lane = 'quality'
                  AND job_type = 'audit_label'
                  AND target_id = ?
                  AND payload_json = ?
                  AND status IN ('completed', 'failed')
                """,
                (row["id"], dumps_json(payload)),
            ).rowcount
        existing = conn.execute(
            """
            SELECT id
            FROM jobs
            WHERE lane = 'quality'
              AND job_type = 'audit_label'
              AND target_id = ?
              AND payload_json = ?
            """,
            (row["id"], dumps_json(payload)),
        ).fetchone()
        if existing:
            continue
        job_id = db.enqueue_job(
            conn,
            lane="quality",
            job_type="audit_label",
            target_id=row["id"],
            payload=payload,
            priority=80,
        )
        if job_id:
            queued += 1
    conn.commit()
    return {"queued": queued, "sample": sample, "label_pack": label_pack, "fresh": fresh, "refreshed_jobs": refreshed}


def job_counts(conn) -> list[dict[str, object]]:
    return [
        dict(row)
        for row in conn.execute(
            "SELECT lane, job_type, status, COUNT(*) AS count FROM jobs GROUP BY lane, job_type, status ORDER BY lane, job_type, status"
        ).fetchall()
    ]


def write_local_dashboard(output: str) -> Path:
    path = Path(output).expanduser().resolve()
    html = """<!doctype html><meta charset="utf-8"><title>Podcast Intelligence Factory</title>
<h1>Podcast Intelligence Factory</h1>
<p>Open <code>observer-snapshot.json</code> next to this file for the sanitized operational snapshot, or use the Railway UI server.</p>
"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(html, encoding="utf-8")
    return path


def privacy_scan(path: Path) -> dict[str, object]:
    suspicious = []
    patterns = [
        ("absolute_local_path", re.compile(r"/Users/[^\\s\"']+")),
        ("bearer_token", re.compile(r"Bearer\\s+[A-Za-z0-9._~+/-]{12,}", re.I)),
        ("api_key_like", re.compile(r"(?i)\b(?:api[_-]?key|token|secret)\b\s*[\"':=]+\s*[\"']?[A-Za-z0-9._~+/-]{16,}")),
        ("openai_key_like", re.compile(r"\bsk-[A-Za-z0-9_-]{20,}\b")),
        ("raw_transcript_marker", re.compile(r"BEGIN RAW TRANSCRIPT")),
    ]
    for item in path.expanduser().resolve().rglob("*"):
        if not item.is_file() or item.suffix.lower() not in {".md", ".json", ".html", ".txt"}:
            continue
        text = item.read_text(encoding="utf-8", errors="ignore")
        reasons = []
        if len(text.split()) > 5000 and ("raw_text" in item.name or "transcript" in item.name):
            reasons.append("long_raw_text_like_file")
        for name, pattern in patterns:
            if pattern.search(text):
                reasons.append(name)
        if _has_long_copied_excerpt(text):
            reasons.append("long_copied_excerpt_like_text")
        if reasons:
            suspicious.append({"path": str(item), "reasons": sorted(set(reasons))})
    return {"ok": not suspicious, "suspicious": suspicious}


def _has_long_copied_excerpt(text: str) -> bool:
    compact_lines = [line.strip() for line in text.splitlines() if line.strip()]
    long_natural_lines = 0
    for line in compact_lines:
        words = line.split()
        if len(words) >= 120 and not line.lstrip().startswith(("{", "[", "#", "-", "|")):
            long_natural_lines += 1
    return long_natural_lines > 0


def corpus_verify(conn, *, repair_hashes: bool = False) -> dict[str, object]:
    issues = []
    repaired = 0
    base = root()
    for table, path_column, hash_column in [
        ("transcripts", "raw_text_path", "raw_text_sha256"),
        ("segments", "text_path", "text_sha256"),
    ]:
        for row in conn.execute(f"SELECT id, {path_column} AS path, {hash_column} AS expected_hash FROM {table}").fetchall():
            path = base / row["path"]
            if not path.exists():
                issues.append({"table": table, "id": row["id"], "issue": "missing_file"})
                continue
            actual = sha256_text(read_text(path))
            if actual != row["expected_hash"]:
                if repair_hashes:
                    text = read_text(path)
                    conn.execute(
                        f"UPDATE {table} SET {hash_column} = ?, word_count = ? WHERE id = ?",
                        (sha256_text(text), len(text.split()), row["id"]),
                    )
                    repaired += 1
                else:
                    issues.append({"table": table, "id": row["id"], "issue": "hash_mismatch"})
    if repair_hashes:
        conn.commit()
    return {
        "ok": not issues,
        "issues": issues,
        "repaired": repaired,
        "checked": {
            "transcripts": conn.execute("SELECT COUNT(*) FROM transcripts").fetchone()[0],
            "segments": conn.execute("SELECT COUNT(*) FROM segments").fetchone()[0],
        },
    }


def print_json(value, *, file=None) -> None:
    print(json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True), file=file)


def _operation_failed(value) -> bool:
    """Make production commands usable as fail-closed shell gates."""

    return isinstance(value, dict) and value.get("ok") is False


if __name__ == "__main__":
    raise SystemExit(main())
