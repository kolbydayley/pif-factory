from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import sys
from pathlib import Path

from . import db
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
from .fast_quality import build_failure_bank, queue_delta_audits
from .headless_codex import execute_claimed_label_runs, execute_pending_reviewer_audits
from .ingest import (
    attach_transcript,
    enqueue_sources,
    enqueue_transcription_jobs,
    mark_transcript_exhausted,
    quarantine_contaminated_transcripts,
    record_transcript_acquisition_attempt,
    release_transcript_candidate_claims,
    transcript_candidates,
    verify_sources,
)
from .identity_graph import groom_identity_graph
from .observer import publish_snapshot, write_snapshot
from .paths import db_path, exports_dir, root
from .prep import prepare_transcript
from .production import DEFAULT_OBSERVER_URL, DEFAULT_PILOT_ID, production_cycle, railway_cost_guard
from .research_queue import (
    claim_remote_job,
    queue_status,
    release_or_fail_remote_job,
    run_worker,
    submit_remote_output,
    sync_queue_envelopes,
)
from .scale_gate import build_scale_gate_report, enqueue_scale_gate
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
from .util import dumps_json, loads_json, now_iso, read_text, sha256_text
from .worker import (
    create_episode_context_prompt,
    create_label_prompt,
    fail_job,
    gate_v31_label_on_episode_context,
    preflight,
    recover_label_handoffs,
    run_jobs,
    submit_episode_context_output,
    submit_label_output,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="research-factory")
    parser.add_argument("--db", default=str(db_path()), help="SQLite database path.")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init", help="Initialize the local database.")
    sub.add_parser("status", help="Print queue and corpus counts.")

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

    preflight_parser = sub.add_parser("preflight", help="Check subscription-backed Codex CLI availability.")
    preflight_parser.add_argument("--model", default="gpt-5.4")

    enqueue = sub.add_parser("enqueue", help="Fetch source feeds and enqueue transcript jobs.")
    enqueue.add_argument("--lane", default="podcast")
    enqueue.add_argument("--since", required=True)
    enqueue.add_argument("--source-list", required=True)
    enqueue.add_argument("--label-pack", default="ai_discourse_v1")

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

    retry_failed = sub.add_parser("retry-failed-labels", help="Repair or requeue failed label jobs for a label pack.")
    retry_failed.add_argument("--label-pack", required=True)
    retry_failed.add_argument("--mode", required=True, choices=["repair-or-requeue", "requeue-only", "repair-only"])
    retry_failed.add_argument("--pilot-id")
    retry_failed.add_argument("--limit", type=int)
    retry_failed.add_argument("--worker-id", default="retry-failed-labels")

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

    claim_edges = sub.add_parser("judge-claim-edges", help="Create claim-edge candidates that require GPT-5.5 semantic confirmation.")
    claim_edges.add_argument("--pilot-id")
    claim_edges.add_argument("--model", default="gpt-5.5")
    claim_edges.add_argument("--limit", type=int, default=100)

    acquisition = sub.add_parser("acquisition-funnel", help="Summarize transcript acquisition coverage by source.")
    acquisition.add_argument("--source")
    acquisition.add_argument("--limit", type=int, default=25)

    verify_sources_parser = sub.add_parser("verify-sources", help="Check source feeds before a large enqueue.")
    verify_sources_parser.add_argument("--source-list", required=True)

    run = sub.add_parser("run", help="Run deterministic queue work and optionally claim label prompts.")
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
    actor_stance = export_sub.add_parser("actor-stance-report")
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
    conn = db.connect(Path(args.db))
    try:
        if args.command == "init":
            db.init_db(conn)
            print_json({"ok": True, "db": str(Path(args.db).resolve()), "root": str(root())})
        elif args.command == "status":
            db.init_db(conn)
            print_json({"ok": True, "counts": db.counts(conn), "jobs": job_counts(conn)})
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
        elif args.command == "preflight":
            print_json(preflight(args.model))
        elif args.command == "enqueue":
            db.init_db(conn)
            print_json(enqueue_sources(conn, args.source_list, lane=args.lane, since=args.since, label_pack=args.label_pack))
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


def _claim_label_job(conn, args) -> dict[str, str]:
    from .worker import claim_next_job

    if args.pilot_id:
        job = _claim_next_label_job_for_pilot(conn, lane=args.lane, worker_id=args.worker_id, pilot_id=args.pilot_id)
    else:
        job = claim_next_job(conn, lane=args.lane, worker_id=args.worker_id, job_types=("label_segment",))
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


def _claim_next_label_job_for_pilot(conn, *, lane: str, worker_id: str, pilot_id: str, lease_minutes: int = 45):
    now = now_iso()
    leased_until = (dt.datetime.fromisoformat(now) + dt.timedelta(minutes=lease_minutes)).isoformat()
    claimed = conn.execute(
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
            AND job_type = 'label_segment'
            AND json_extract(payload_json, '$.pilot_id') = ?
            AND attempts < max_attempts
            AND (status = 'pending' OR (status = 'claimed' AND leased_until < ?))
          ORDER BY priority ASC, id ASC
          LIMIT 1
        )
        RETURNING *
        """,
        (worker_id, leased_until, now, lane, pilot_id, now),
    ).fetchone()
    conn.commit()
    return claimed


def _claim_episode_context_job(conn, args) -> dict[str, str]:
    from .worker import claim_next_job, release_job

    job = claim_next_job(conn, lane=args.lane, worker_id=args.worker_id, job_types=("episode_context",))
    if not job:
        return {"ok": "false", "message": "no pending episode_context jobs"}
    if job["job_type"] != "episode_context":
        release_job(conn, job["id"])
        conn.commit()
        return {"ok": "false", "message": f"next job was {job['job_type']}; run deterministic jobs first"}
    return create_episode_context_prompt(conn, job, label_pack=args.label_pack, model=args.model, worker_id=args.worker_id)


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


if __name__ == "__main__":
    raise SystemExit(main())
