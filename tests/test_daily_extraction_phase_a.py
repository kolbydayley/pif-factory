from __future__ import annotations

import subprocess
import threading
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from research_factory import db
from research_factory.cli import build_parser
from research_factory.daily_cycle import (
    DEFAULT_DAILY_RUNTIME_SECONDS,
    DailyStageContext,
    _default_stage_handlers,
    _job_count_recent,
    _record_daily_extraction_pipeline_run,
    ensure_daily_schema,
)
from research_factory.headless_codex import (
    execute_claimed_label_runs,
    execute_pending_reviewer_audits,
)
from research_factory.worker import claim_next_job


NOW = "2026-07-29T12:00:00+00:00"


def _baseline_handler(conn, artifact_dir: Path, *, execute_extraction: bool):
    handlers = _default_stage_handlers(
        conn,
        source_list=None,
        since=None,
        execute_ingestion=False,
        execute_normalize=False,
        execute_extraction=execute_extraction,
        apply_reconcile=False,
        record_exception_contracts=False,
        publish_observer=False,
        snapshot_output=None,
        observer_url=None,
        observer_token=None,
        lane="podcast",
        label_pack="ai_discourse_v3_1",
        model="gpt-5.5",
        pilot_id=None,
        now=lambda: NOW,
    )
    return handlers["bounded_baseline_extraction"], DailyStageContext(
        conn=conn,
        run_id="current-run",
        run_date="2026-07-29",
        stage_name="bounded_baseline_extraction",
        stage_index=4,
        max_items=5,
        remaining_seconds=300.0,
        deadline_monotonic=300.0,
        artifact_dir=artifact_dir,
    )


def _ingestion_handler(conn, artifact_dir: Path, *, execute_ingestion: bool):
    handlers = _default_stage_handlers(
        conn,
        source_list=artifact_dir / "sources.yaml",
        since=None,
        execute_ingestion=execute_ingestion,
        execute_normalize=False,
        execute_extraction=False,
        apply_reconcile=False,
        record_exception_contracts=False,
        publish_observer=False,
        snapshot_output=None,
        observer_url=None,
        observer_token=None,
        lane="podcast",
        label_pack="ai_discourse_v3_1",
        model="gpt-5.5",
        pilot_id=None,
        now=lambda: NOW,
    )
    return handlers["rss_ingestion_and_due_transcript_strategies"], DailyStageContext(
        conn=conn,
        run_id="current-run",
        run_date="2026-07-29",
        stage_name="rss_ingestion_and_due_transcript_strategies",
        stage_index=2,
        max_items=5,
        remaining_seconds=120.0,
        deadline_monotonic=120.0,
        artifact_dir=artifact_dir,
    )


def _outcomes_handler(conn, artifact_dir: Path, *, execute_outcomes: bool):
    handlers = _default_stage_handlers(
        conn,
        source_list=None,
        since=None,
        execute_ingestion=False,
        execute_normalize=False,
        execute_extraction=False,
        apply_reconcile=False,
        record_exception_contracts=False,
        publish_observer=False,
        snapshot_output=None,
        observer_url=None,
        observer_token=None,
        lane="podcast",
        label_pack="ai_discourse_v3_1",
        model="gpt-5.5",
        pilot_id=None,
        execute_outcomes=execute_outcomes,
        now=lambda: NOW,
    )
    return handlers["due_outcomes"], DailyStageContext(
        conn=conn,
        run_id="current-run",
        run_date="2026-07-29",
        stage_name="due_outcomes",
        stage_index=7,
        max_items=5,
        remaining_seconds=120.0,
        deadline_monotonic=120.0,
        artifact_dir=artifact_dir,
    )


def test_validation_stage_uses_injected_clock(tmp_path: Path) -> None:
    conn = db.connect(tmp_path / "factory.sqlite")
    try:
        db.init_db(conn)
        ensure_daily_schema(conn)
        handlers = _default_stage_handlers(
            conn,
            source_list=None,
            since=None,
            execute_ingestion=False,
            execute_normalize=False,
            execute_extraction=False,
            apply_reconcile=False,
            record_exception_contracts=False,
            publish_observer=False,
            snapshot_output=None,
            observer_url=None,
            observer_token=None,
            lane="podcast",
            label_pack="ai_discourse_v3_1",
            model="gpt-5.5",
            pilot_id=None,
            now=lambda: NOW,
        )
        context = DailyStageContext(
            conn=conn,
            run_id="current-run",
            run_date="2026-07-29",
            stage_name="evidence_schema_privacy_validation",
            stage_index=5,
            max_items=5,
            remaining_seconds=300.0,
            deadline_monotonic=300.0,
            artifact_dir=tmp_path / "receipts",
        )
        validation = {
            "ok": True,
            "checked_claims": 0,
            "issue_count": 0,
            "issues": [],
        }
        with patch(
            "research_factory.daily_cycle._current_accepted_release_count",
            return_value=1,
        ), patch(
            "research_factory.daily_cycle._validate_current_accepted_release",
            return_value=validation,
        ) as validate:
            result = handlers["evidence_schema_privacy_validation"](context)

        validate.assert_called_once_with(conn, at=NOW)
        assert result["status"] == "completed"
    finally:
        conn.close()


def test_validation_stage_is_healthy_no_work_before_first_release(
    tmp_path: Path,
) -> None:
    conn = db.connect(tmp_path / "factory.sqlite")
    try:
        db.init_db(conn)
        ensure_daily_schema(conn)
        handlers = _default_stage_handlers(
            conn,
            source_list=None,
            since=None,
            execute_ingestion=False,
            execute_normalize=False,
            execute_extraction=False,
            apply_reconcile=False,
            record_exception_contracts=False,
            publish_observer=False,
            snapshot_output=None,
            observer_url=None,
            observer_token=None,
            lane="podcast",
            label_pack="ai_discourse_v3_1",
            model="gpt-5.5",
            pilot_id=None,
            now=lambda: NOW,
        )
        context = DailyStageContext(
            conn=conn,
            run_id="current-run",
            run_date="2026-07-29",
            stage_name="evidence_schema_privacy_validation",
            stage_index=5,
            max_items=5,
            remaining_seconds=300.0,
            deadline_monotonic=300.0,
            artifact_dir=tmp_path / "receipts",
        )

        result = handlers["evidence_schema_privacy_validation"](context)

        assert result["status"] == "completed"
        assert result["reason"] == "no_current_accepted_release"
        assert result["work_due"] is False
        assert result["healthy_no_work"] is True
        assert result["work_satisfied"] is True
    finally:
        conn.close()


def test_reconciliation_backlog_is_not_daily_blocker_after_quality_floor(
    tmp_path: Path,
) -> None:
    conn = db.connect(tmp_path / "factory.sqlite")
    try:
        db.init_db(conn)
        ensure_daily_schema(conn)
        handlers = _default_stage_handlers(
            conn,
            source_list=None,
            since=None,
            execute_ingestion=False,
            execute_normalize=False,
            execute_extraction=False,
            apply_reconcile=True,
            record_exception_contracts=False,
            publish_observer=False,
            snapshot_output=None,
            observer_url=None,
            observer_token=None,
            lane="podcast",
            label_pack="ai_discourse_v3_1",
            model="gpt-5.5",
            pilot_id=None,
            now=lambda: NOW,
        )
        context = DailyStageContext(
            conn=conn,
            run_id="current-run",
            run_date="2026-07-29",
            stage_name="identity_and_semantic_reconciliation",
            stage_index=6,
            max_items=5,
            remaining_seconds=300.0,
            deadline_monotonic=300.0,
            artifact_dir=tmp_path / "receipts",
        )
        reconciliation = {
            "ok": True,
            "processed": 5,
            "targets": [{"planned_items": 5}],
        }
        with patch(
            "research_factory.daily_cycle._current_release_for_scale_gate",
            return_value={"id": "crel_test", "item_count": 25},
        ), patch(
            "research_factory.daily_cycle._quality_gate",
            return_value={"passed": True, "counts": {}, "thresholds_met": {}},
        ), patch(
            "research_factory.production_ops.reconcile_all",
            return_value=reconciliation,
        ) as reconcile_all:
            result = handlers["identity_and_semantic_reconciliation"](context)

        assert result["status"] == "completed"
        assert result["work_due"] is True
        assert result["work_satisfied"] is True
        assert result["healthy_no_work"] is False
        assert result["reconciliation_backlog_due"] is True
        assert result["reason"] == "quality_minimums_satisfied_reconciliation_backlog_deferred"
        assert reconcile_all.call_args.kwargs["apply"] is False
    finally:
        conn.close()


def test_execute_extraction_enables_real_bounded_baseline_and_honest_counts(tmp_path: Path) -> None:
    conn = db.connect(tmp_path / "factory.sqlite")
    try:
        db.init_db(conn)
        ensure_daily_schema(conn)
        handler, context = _baseline_handler(conn, tmp_path / "receipts", execute_extraction=True)
        worker_result = {
            "processed": 2,
            "completed": 0,
            "claimed_prompts": 2,
            "failed": 0,
            "details": [],
        }
        headless_result = {
            "ok": True,
            "selected": 2,
            "processed": 2,
            "submitted": 2,
            "failed": 0,
            "results": [],
        }
        with patch("research_factory.daily_cycle._job_count_recent", side_effect=[2, 0]), patch(
            "research_factory.worker.run_jobs",
            return_value=worker_result,
        ) as run_jobs, patch(
            "research_factory.headless_codex.execute_claimed_label_runs",
            return_value=headless_result,
        ) as execute:
            result = handler(context)

        assert result["status"] == "completed"
        assert result["processed"] == 2
        assert result["required_work_enabled"] is True
        assert result["work_due"] is True
        assert result["work_satisfied"] is True
        assert result["recent_pending"] == 2
        assert result["recent_pending_after"] == 0
        assert run_jobs.call_args.kwargs["job_types"] == ("episode_context", "label_segment")
        assert run_jobs.call_args.kwargs["limit"] == 5
        assert execute.call_args.kwargs["concurrency"] == 3
        assert execute.call_args.kwargs["limit"] == 2
    finally:
        conn.close()


def test_daily_extraction_tolerates_one_of_twenty_five_provider_failures(
    tmp_path: Path,
) -> None:
    conn = db.connect(tmp_path / "factory.sqlite")
    try:
        db.init_db(conn)
        ensure_daily_schema(conn)
        handler, prior_context = _baseline_handler(
            conn, tmp_path / "receipts", execute_extraction=True
        )
        context = DailyStageContext(
            **{**prior_context.__dict__, "max_items": 25}
        )
        worker_result = {
            "processed": 25,
            "completed": 0,
            "claimed_prompts": 25,
            "failed": 0,
            "details": [],
        }
        headless_result = {
            "ok": False,
            "selected": 25,
            "processed": 25,
            "submitted": 24,
            "failed": 1,
            "results": [],
        }
        with patch(
            "research_factory.daily_cycle._job_count_recent", side_effect=[25, 1]
        ), patch(
            "research_factory.worker.run_jobs", return_value=worker_result
        ), patch(
            "research_factory.headless_codex.execute_claimed_label_runs",
            return_value=headless_result,
        ), patch(
            "research_factory.daily_cycle._record_daily_extraction_pipeline_run",
            return_value={"recorded": True, "status": "succeeded"},
        ):
            result = handler(context)

        assert result["status"] == "completed"
        assert result["work_satisfied"] is True
        assert result["headless_failure_rate"] == 0.04
        assert result["failure_rate_tolerance"] == 0.10
        assert result["failure_tolerance_passed"] is True
        assert result["reason"] == "bounded_extraction_within_failure_tolerance"
    finally:
        conn.close()


def test_daily_extraction_fails_above_ten_percent_provider_failures(
    tmp_path: Path,
) -> None:
    conn = db.connect(tmp_path / "factory.sqlite")
    try:
        db.init_db(conn)
        ensure_daily_schema(conn)
        handler, prior_context = _baseline_handler(
            conn, tmp_path / "receipts", execute_extraction=True
        )
        context = DailyStageContext(
            **{**prior_context.__dict__, "max_items": 25}
        )
        worker_result = {
            "processed": 25,
            "completed": 0,
            "claimed_prompts": 25,
            "failed": 0,
            "details": [],
        }
        headless_result = {
            "ok": False,
            "selected": 25,
            "processed": 25,
            "submitted": 22,
            "failed": 3,
            "results": [],
        }
        with patch(
            "research_factory.daily_cycle._job_count_recent", side_effect=[25, 3]
        ), patch(
            "research_factory.worker.run_jobs", return_value=worker_result
        ), patch(
            "research_factory.headless_codex.execute_claimed_label_runs",
            return_value=headless_result,
        ), patch(
            "research_factory.daily_cycle._record_daily_extraction_pipeline_run",
            return_value={"recorded": True, "status": "failed"},
        ):
            result = handler(context)

        assert result["status"] == "failed"
        assert result["work_satisfied"] is False
        assert result["headless_failure_rate"] == 0.12
        assert result["failure_tolerance_passed"] is False
    finally:
        conn.close()


def test_execute_ingestion_enables_recent_bounded_work_and_honest_counts(
    tmp_path: Path,
) -> None:
    conn = db.connect(tmp_path / "factory.sqlite")
    try:
        db.init_db(conn)
        ensure_daily_schema(conn)
        handler, context = _ingestion_handler(
            conn,
            tmp_path / "receipts",
            execute_ingestion=True,
        )
        ingestion = {
            "sources": 10,
            "episodes": 2,
            "episodes_inserted": 1,
            "source_errors": 0,
            "skipped_after_max_items": 0,
            "sources_deferred_due_to_runtime": 0,
            "runtime_exhausted": False,
        }
        strategy = {
            "strategy_count": 3,
            "totals": {
                "manual_transcript_required": 1_227,
                "terminal_failures": 1_891,
            },
        }
        with patch(
            "research_factory.transcript_strategies.transcript_strategy_report",
            return_value=strategy,
        ), patch(
            "research_factory.daily_cycle._recent_job_window_start",
            return_value="2026-07-28T12:00:00+00:00",
        ), patch(
            "research_factory.ingest.enqueue_sources",
            return_value=ingestion,
        ) as enqueue_sources, patch(
            "research_factory.ingest.route_terminal_fetch_failures",
            return_value={"routed": 1, "by_reason": {}, "by_source": {}},
        ), patch(
            "research_factory.ingest.enqueue_transcript_backlog",
            return_value={"selected": 2, "enqueued": 2},
        ):
            result = handler(context)

        assert result["status"] == "completed"
        assert result["processed"] == 5
        assert result["work_due"] is True
        assert result["work_satisfied"] is True
        assert result["required_work_enabled"] is True
        assert result["recent_window_start"] == "2026-07-28T12:00:00+00:00"
        assert result["backlog_total"] == strategy["totals"]
        assert enqueue_sources.call_args.kwargs["since"] == "2026-07-28T12:00:00+00:00"
        assert enqueue_sources.call_args.kwargs["max_items"] == 5
        assert enqueue_sources.call_args.kwargs["max_runtime_seconds"] == 120.0
    finally:
        conn.close()


def test_ingestion_item_bound_with_progress_is_satisfied_not_noop(
    tmp_path: Path,
) -> None:
    conn = db.connect(tmp_path / "factory.sqlite")
    try:
        db.init_db(conn)
        ensure_daily_schema(conn)
        handler, context = _ingestion_handler(
            conn,
            tmp_path / "receipts",
            execute_ingestion=True,
        )
        with patch(
            "research_factory.transcript_strategies.transcript_strategy_report",
            return_value={"strategy_count": 1, "totals": {}},
        ), patch(
            "research_factory.daily_cycle._recent_job_window_start",
            return_value="2026-07-28T12:00:00+00:00",
        ), patch(
            "research_factory.ingest.enqueue_sources",
            return_value={
                "sources": 57,
                "episodes": 5,
                "episodes_inserted": 4,
                "source_errors": 0,
                "skipped_after_max_items": 3,
                "sources_deferred_due_to_runtime": 0,
                "runtime_exhausted": False,
            },
        ):
            result = handler(context)

        assert result["processed"] == 5
        assert result["bounded_remainder"] == 3
        assert result["work_due"] is True
        assert result["work_satisfied"] is True
        assert result["status"] == "completed"
    finally:
        conn.close()


def test_ingestion_historic_terminal_and_manual_counts_are_telemetry_not_pass_condition(
    tmp_path: Path,
) -> None:
    conn = db.connect(tmp_path / "factory.sqlite")
    try:
        db.init_db(conn)
        ensure_daily_schema(conn)
        handler, context = _ingestion_handler(
            conn,
            tmp_path / "receipts",
            execute_ingestion=True,
        )
        with patch(
            "research_factory.transcript_strategies.transcript_strategy_report",
            return_value={
                "strategy_count": 3,
                "totals": {
                    "manual_transcript_required": 1_227,
                    "terminal_failures": 1_891,
                },
            },
        ), patch(
            "research_factory.daily_cycle._recent_job_window_start",
            return_value="2026-07-28T12:00:00+00:00",
        ), patch(
            "research_factory.ingest.enqueue_sources",
            return_value={
                "sources": 10,
                "episodes": 0,
                "episodes_inserted": 0,
                "source_errors": 0,
                "skipped_after_max_items": 0,
                "sources_deferred_due_to_runtime": 0,
                "runtime_exhausted": False,
            },
        ), patch(
            "research_factory.ingest.route_terminal_fetch_failures",
            return_value={"routed": 0, "by_reason": {}, "by_source": {}},
        ), patch(
            "research_factory.ingest.enqueue_transcript_backlog",
            return_value={"selected": 0, "enqueued": 0},
        ):
            result = handler(context)

        assert result["processed"] == 0
        assert result["work_due"] is False
        assert result["healthy_no_work"] is True
        assert result["work_satisfied"] is True
        assert result["backlog_total"]["manual_transcript_required"] == 1_227
        assert result["backlog_total"]["terminal_failures"] == 1_891
    finally:
        conn.close()


def test_execute_outcomes_records_recent_bounded_work_and_satisfies_stage(
    tmp_path: Path,
) -> None:
    conn = db.connect(tmp_path / "factory.sqlite")
    try:
        db.init_db(conn)
        ensure_daily_schema(conn)
        handler, context = _outcomes_handler(
            conn,
            tmp_path / "receipts",
            execute_outcomes=True,
        )
        with patch(
            "research_factory.daily_cycle._recent_job_window_start",
            return_value="2026-07-28T12:00:00+00:00",
        ), patch(
            "research_factory.production_ops.plan_due_outcome_dispatches",
            return_value={
                "ok": True,
                "due": 2,
                "due_total": 2,
                "backlog_total": 25,
                "recorded": 2,
                "already_recorded": 0,
                "dispatch_contracts": [{}, {}],
                "dispatch_state": "recorded_only",
            },
        ) as plan:
            result = handler(context)

        assert result["status"] == "completed"
        assert result["processed"] == 2
        assert result["work_due"] is True
        assert result["required_work_enabled"] is True
        assert result["work_satisfied"] is True
        assert result["recent_due_total"] == 2
        assert result["backlog_total"] == 25
        assert plan.call_args.kwargs["record"] is True
        assert plan.call_args.kwargs["since"] == "2026-07-28T12:00:00+00:00"
    finally:
        conn.close()


def test_outcome_historic_backlog_is_telemetry_not_recent_pass_condition(
    tmp_path: Path,
) -> None:
    conn = db.connect(tmp_path / "factory.sqlite")
    try:
        db.init_db(conn)
        ensure_daily_schema(conn)
        handler, context = _outcomes_handler(
            conn,
            tmp_path / "receipts",
            execute_outcomes=False,
        )
        with patch(
            "research_factory.daily_cycle._recent_job_window_start",
            return_value="2026-07-28T12:00:00+00:00",
        ), patch(
            "research_factory.production_ops.plan_due_outcome_dispatches",
            return_value={
                "ok": True,
                "due": 0,
                "due_total": 0,
                "backlog_total": 25,
                "recorded": 0,
                "already_recorded": 0,
                "dispatch_contracts": [],
                "dispatch_state": "contract_only",
            },
        ):
            result = handler(context)

        assert result["status"] == "completed"
        assert result["work_due"] is False
        assert result["healthy_no_work"] is True
        assert result["work_satisfied"] is True
        assert result["backlog_total"] == 25
    finally:
        conn.close()


def test_daily_extraction_is_attributed_to_current_release_without_paid_api(
    tmp_path: Path,
) -> None:
    conn = db.connect(tmp_path / "factory.sqlite")
    try:
        db.init_db(conn)
        with patch(
            "research_factory.daily_cycle._current_release_for_scale_gate",
            return_value={"id": "crel_test", "item_count": 25},
        ), patch(
            "research_factory.intelligence.create_pipeline_run",
        ) as create_run, patch(
            "research_factory.intelligence.transition_pipeline_run",
        ) as transition_run:
            result = _record_daily_extraction_pipeline_run(
                conn,
                daily_run_id="pdr_test",
                run_date="2026-07-29",
                lane="podcast",
                model="gpt-5.5",
                lease_owner="daily-test",
                worker_result={"failed": 0},
                headless_result={
                    "ok": True,
                    "selected": 2,
                    "submitted": 2,
                    "failed": 0,
                },
            )

        assert result["recorded"] is True
        assert result["corpus_release_id"] == "crel_test"
        assert result["status"] == "succeeded"
        assert result["provider_lane"] == "codex_subscription"
        assert result["paid_api"] is False
        assert create_run.call_args.kwargs["corpus_release_id"] == "crel_test"
        assert create_run.call_args.kwargs["parameters"]["paid_api"] is False
        assert transition_run.call_args.kwargs["status"] == "succeeded"
        assert transition_run.call_args.kwargs["metrics"]["paid_api"] is False
    finally:
        conn.close()


def test_recent_window_not_absolute_backlog_controls_satisfaction(tmp_path: Path) -> None:
    conn = db.connect(tmp_path / "factory.sqlite")
    try:
        db.init_db(conn)
        ensure_daily_schema(conn)
        old_job_id = db.enqueue_job(
            conn,
            lane="podcast",
            job_type="label_segment",
            target_id="old-backlog",
            payload={"label_pack": "ai_discourse_v3_1", "model": "gpt-5.5"},
        )
        conn.execute(
            "UPDATE jobs SET created_at = ?, updated_at = ? WHERE id = ?",
            ("2026-07-27T11:59:59+00:00", "2026-07-27T11:59:59+00:00", old_job_id),
        )
        conn.commit()
        handler, context = _baseline_handler(conn, tmp_path / "receipts", execute_extraction=True)
        empty_worker = {
            "processed": 0,
            "completed": 0,
            "claimed_prompts": 0,
            "failed": 0,
            "details": [],
        }
        empty_headless = {
            "ok": True,
            "selected": 0,
            "processed": 0,
            "submitted": 0,
            "failed": 0,
            "results": [],
        }
        with patch("research_factory.worker.run_jobs", return_value=empty_worker), patch(
            "research_factory.headless_codex.execute_claimed_label_runs",
            return_value=empty_headless,
        ):
            result = handler(context)

        assert _job_count_recent(
            conn,
            ("episode_context", "label_segment"),
            "2026-07-28T12:00:00+00:00",
        ) == 0
        assert result["backlog_total"] == 1
        assert result["recent_pending"] == 0
        assert result["work_due"] is False
        assert result["work_satisfied"] is True
        assert result["healthy_no_work"] is True
    finally:
        conn.close()


def test_target_scoped_claim_does_not_take_unrelated_backfill_work(
    tmp_path: Path,
) -> None:
    conn = db.connect(tmp_path / "factory.sqlite")
    try:
        db.init_db(conn)
        wanted = db.enqueue_job(
            conn,
            lane="podcast",
            job_type="label_segment",
            target_id="segment-wanted",
            payload={"label_pack": "ai_discourse_v3_1"},
            priority=100,
        )
        unrelated = db.enqueue_job(
            conn,
            lane="podcast",
            job_type="label_segment",
            target_id="segment-unrelated",
            payload={"label_pack": "ai_discourse_v3_1"},
            priority=1,
        )

        claimed = claim_next_job(
            conn,
            lane="podcast",
            worker_id="targeted-backfill",
            job_types=("label_segment",),
            target_ids=("segment-wanted",),
        )

        assert claimed["id"] == wanted
        assert conn.execute(
            "SELECT status FROM jobs WHERE id = ?",
            (unrelated,),
        ).fetchone()["status"] == "pending"
    finally:
        conn.close()


def test_job_scoped_claim_selects_exact_label_pack_job_on_shared_segment(
    tmp_path: Path,
) -> None:
    conn = db.connect(tmp_path / "factory.sqlite")
    try:
        db.init_db(conn)
        legacy = db.enqueue_job(
            conn,
            lane="podcast",
            job_type="label_segment",
            target_id="shared-segment",
            payload={"label_pack": "ai_discourse_v1"},
            priority=1,
        )
        current = db.enqueue_job(
            conn,
            lane="podcast",
            job_type="label_segment",
            target_id="shared-segment",
            payload={"label_pack": "ai_discourse_v3_1"},
            priority=100,
        )

        claimed = claim_next_job(
            conn,
            lane="podcast",
            worker_id="exact-backfill",
            job_types=("label_segment",),
            job_ids=(current,),
        )

        assert claimed["id"] == current
        assert conn.execute(
            "SELECT status FROM jobs WHERE id = ?",
            (legacy,),
        ).fetchone()["status"] == "pending"
    finally:
        conn.close()


class _Cursor:
    def __init__(self, *, rows=None, row=None):
        self._rows = list(rows or [])
        self._row = row

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._row


class _FakeClaimedConnection:
    def __init__(self, rows):
        self.rows = rows
        self.execute_thread_ids: list[int] = []
        self.submitted_labels: dict[int, str] = {}

    def execute(self, sql, params=()):
        self.execute_thread_ids.append(threading.get_ident())
        if "ORDER BY jobs.id" in sql:
            return _Cursor(rows=self.rows[: int(params[-1])])
        if "SELECT jobs.status AS job_status" in sql:
            return _Cursor(
                row={
                    "job_status": "claimed",
                    "run_status": "claimed",
                    "lease_owner": "daily-owner",
                }
            )
        # Subscription budget ledger (Phase 1): schema, gate read, and the
        # end-of-wave usage insert all run through the same connection.
        if "pif_subscription_budget_ledger" in sql:
            if sql.lstrip().upper().startswith("SELECT"):
                return _Cursor(row={"total": 0})
            return _Cursor()
        raise AssertionError(sql)

    def commit(self):
        return None


def _fake_claimed_rows(tmp_path: Path):
    rows = []
    for index in range(1, 5):
        prompt_path = tmp_path / f"prompt-{index}.json"
        prompt_path.write_text(f"single-shot prompt {index}", encoding="utf-8")
        rows.append({
            "job_id": index,
            "lease_owner": "daily-owner",
            "label_run_id": f"run-{index}",
            "prompt_path": str(prompt_path),
            "output_path": str(tmp_path / f"output-{index}.json"),
        })
    return rows


def test_concurrent_claimed_execution_matches_serial_with_fake_codex(tmp_path: Path) -> None:
    rows = _fake_claimed_rows(tmp_path)
    invocations = []
    submissions: list[int] = []

    def fake_run(*args, **kwargs):
        invocations.append((args, kwargs))
        return SimpleNamespace(returncode=0)

    def fake_submit(conn, *, job_id, output_json_path, worker_id, allow_expired):
        assert job_id not in conn.submitted_labels
        conn.submitted_labels[job_id] = f"label-{job_id}"
        submissions.append(job_id)
        return {"label_id": f"label-{job_id}"}

    summaries = []
    connections = []
    with patch("research_factory.headless_codex.runs_dir", return_value=tmp_path), patch(
        "research_factory.headless_codex.root",
        return_value=tmp_path,
    ), patch("research_factory.headless_codex.subprocess.run", side_effect=fake_run), patch(
        "research_factory.headless_codex.submit_label_output",
        side_effect=fake_submit,
    ):
        for concurrency in (1, 3):
            connection = _FakeClaimedConnection(rows)
            connections.append(connection)
            summaries.append(
                execute_claimed_label_runs(
                    connection,
                    lease_owner="daily-owner",
                    limit=4,
                    model="gpt-5.5",
                    timeout_seconds=30,
                    audit=False,
                    concurrency=concurrency,
                )
            )

    serial, concurrent = summaries
    for result in summaries:
        assert result["selected"] == 4
        assert result["processed"] == 4
        assert result["submitted"] == 4
        assert result["failed"] == 0
    assert [item["job_id"] for item in serial["results"]] == [
        item["job_id"] for item in concurrent["results"]
    ]
    assert [item["status"] for item in serial["results"]] == [
        item["status"] for item in concurrent["results"]
    ]
    assert submissions[:4] == [1, 2, 3, 4]
    assert sorted(submissions[4:]) == [1, 2, 3, 4]
    assert len(set(submissions[4:])) == 4
    assert connections[0].submitted_labels == connections[1].submitted_labels
    assert len(invocations) == 8
    assert all(call_args[0][-1] == "-" for call_args, _ in invocations)
    for _, call_kwargs in invocations:
        assert "Write the final JSON object" in call_kwargs["input"]
        assert any(
            f"single-shot prompt {index}" in call_kwargs["input"]
            and str(tmp_path / f"output-{index}.json") in call_kwargs["input"]
            for index in range(1, 5)
        )
    main_thread = threading.get_ident()
    assert all(set(connection.execute_thread_ids) == {main_thread} for connection in connections)


def test_claimed_execution_finalizes_submission_failure(tmp_path: Path) -> None:
    rows = _fake_claimed_rows(tmp_path)[:1]
    connection = _FakeClaimedConnection(rows)
    with patch(
        "research_factory.headless_codex.runs_dir", return_value=tmp_path
    ), patch(
        "research_factory.headless_codex.root", return_value=tmp_path
    ), patch(
        "research_factory.headless_codex.subprocess.run",
        return_value=SimpleNamespace(returncode=0),
    ), patch(
        "research_factory.headless_codex.submit_label_output",
        side_effect=ValueError("invalid exact-evidence metric"),
    ), patch(
        "research_factory.headless_codex._finalize_submission_failure",
        return_value={
            "finalized": True,
            "job_status": "pending",
            "label_run_status": "failed",
        },
    ) as finalize:
        result = execute_claimed_label_runs(
            connection,
            lease_owner="daily-owner",
            limit=1,
            model="gpt-5.5",
            timeout_seconds=30,
            audit=False,
            concurrency=1,
        )

    assert result["failed"] == 1
    assert result["results"][0]["status"] == "submission_failed"
    assert result["results"][0]["failure_finalization"]["finalized"] is True
    finalize.assert_called_once_with(
        connection,
        job_id=1,
        label_run_id="run-1",
        lease_owner="daily-owner",
        error="invalid exact-evidence metric",
    )


def test_claimed_execution_timeout_returns_job_pending_without_spending_attempt(
    tmp_path: Path,
) -> None:
    rows = _fake_claimed_rows(tmp_path)[:1]
    connection = _FakeClaimedConnection(rows)
    with patch(
        "research_factory.headless_codex.runs_dir", return_value=tmp_path
    ), patch(
        "research_factory.headless_codex.root", return_value=tmp_path
    ), patch(
        "research_factory.headless_codex.resolve_codex_binary",
        return_value="codex",
    ), patch(
        "research_factory.headless_codex.subprocess.run",
        side_effect=subprocess.TimeoutExpired(cmd="codex", timeout=30),
    ), patch(
        "research_factory.headless_codex._finalize_submission_failure",
        return_value={
            "finalized": True,
            "job_status": "pending",
            "label_run_status": "failed",
            "attempt_consumed": False,
        },
    ) as finalize:
        result = execute_claimed_label_runs(
            connection,
            lease_owner="daily-owner",
            limit=1,
            model="gpt-5.5",
            timeout_seconds=30,
            audit=False,
            concurrency=1,
        )

    assert result["failed"] == 1
    assert result["results"][0]["status"] == "codex_exec_timeout"
    assert result["results"][0]["timed_out"] is True
    finalize.assert_called_once_with(
        connection,
        job_id=1,
        label_run_id="run-1",
        lease_owner="daily-owner",
        error="codex_exec_timeout",
        consume_attempt=False,
    )


def test_reviewer_audit_uses_single_shot_stdin(tmp_path: Path) -> None:
    prompt_path = tmp_path / "reviewer-prompt.md"
    output_path = tmp_path / "reviewer-output.json"
    prompt_path.write_text("review this label exactly once", encoding="utf-8")

    class MainConnection:
        def execute(self, sql, params=()):
            if "FROM reviewer_audits" in sql:
                return _Cursor(
                    rows=[
                        {
                            "id": "audit-1",
                            "prompt_path": str(prompt_path),
                            "output_path": str(output_path),
                            "patch_tag": "phase1",
                        }
                    ]
                )
            if "UPDATE reviewer_audits" in sql:
                return SimpleNamespace(rowcount=1)
            # Subscription budget ledger (Phase 1): reviewer audits meter
            # their usage into the same daily ledger as every other lane.
            if "pif_subscription_budget_ledger" in sql:
                if sql.lstrip().upper().startswith("SELECT"):
                    return _Cursor(row={"total": 0})
                return _Cursor()
            raise AssertionError(sql)

        def commit(self):
            return None

    class WorkerConnection:
        def execute(self, sql, params=()):
            if "SELECT status FROM reviewer_audits" in sql:
                return _Cursor(row={"status": "claimed"})
            if "UPDATE reviewer_audits" in sql:
                return SimpleNamespace(rowcount=1)
            # Subscription budget ledger (Phase 1): reviewer audits meter
            # their usage into the same daily ledger as every other lane.
            if "pif_subscription_budget_ledger" in sql:
                if sql.lstrip().upper().startswith("SELECT"):
                    return _Cursor(row={"total": 0})
                return _Cursor()
            raise AssertionError(sql)

        def commit(self):
            return None

        def close(self):
            return None

    calls = []

    def fake_run(*args, **kwargs):
        calls.append((args, kwargs))
        return SimpleNamespace(returncode=0)

    with patch(
        "research_factory.headless_codex.runs_dir", return_value=tmp_path
    ), patch(
        "research_factory.headless_codex.root", return_value=tmp_path
    ), patch(
        "research_factory.headless_codex.db.connect",
        return_value=WorkerConnection(),
    ), patch(
        "research_factory.headless_codex.subprocess.run",
        side_effect=fake_run,
    ), patch(
        "research_factory.headless_codex.submit_reviewer_audit",
        return_value={"audit_id": "audit-1"},
    ):
        result = execute_pending_reviewer_audits(
            MainConnection(),
            patch_tag="phase1",
            limit=1,
            model="gpt-5.5",
        )

    assert result["submitted"] == 1
    assert calls[0][0][0][-1] == "-"
    assert "Write the final JSON object" in calls[0][1]["input"]
    assert str(output_path) in calls[0][1]["input"]
    assert calls[0][1]["input"].endswith("review this label exactly once")


def test_daily_cli_requires_explicit_extraction_flag() -> None:
    parser = build_parser()
    default = parser.parse_args(["run", "daily"])
    enabled = parser.parse_args(
        [
            "run",
            "daily",
            "--execute-extraction",
            "--execute-ingestion",
            "--execute-outcomes",
        ]
    )
    assert default.execute_extraction is False
    assert default.execute_ingestion is False
    assert enabled.execute_extraction is True
    assert enabled.execute_ingestion is True
    assert default.execute_outcomes is False
    assert enabled.execute_outcomes is True
    assert default.max_runtime_seconds == DEFAULT_DAILY_RUNTIME_SECONDS == 5_400
