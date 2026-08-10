from __future__ import annotations

import argparse
import datetime as dt
import fcntl
import json
import os
import signal
import time
from pathlib import Path
from typing import Any, Callable

from . import db
from .paths import root
from .production_backfill_series import (
    LABEL_FIELD_QUARANTINE_ROLLING_LABELS,
    evaluate_label_campaign_gate,
    request_active_label_campaign_stop,
    run_label_campaign_series,
)
from .util import now_iso, write_text_atomic


STATUS_PATH = root() / "work/pif-ops/label-backfill-daemon/status.json"
LOCK_PATH = root() / "work/pif-ops/label-backfill-daemon/daemon.lock"
CAMPAIGN_PAUSE_SECONDS = 30


def _default_state() -> dict[str, Any]:
    return {
        "schema_version": "pif_label_backfill_daemon_state_v1",
        "generation": 0,
        "status": "idle",
        "halted": False,
        "halt_reason": None,
        "current_campaign": None,
        "campaigns_completed": 0,
        "campaign_history": [],
        "rolling_windows": [],
        "updated_at": now_iso(),
        "configuration": {
            "job_type": "label_segment",
            "model": "gpt-5.5",
            "provider_lane": "codex_subscription",
            "concurrency": 3,
            "claim_wave_size": 25,
            "runtime_ceiling_seconds": 3600,
            "max_daily_campaigns": 1,
            "max_daily_campaigns_reason": (
                "Kolby ruling 2026-08-10: the uncapped daemon drove the "
                "~1.03B-token August wave. One campaign per calendar day, "
                "wave 25 at concurrency 3, behind the 5M/day subscription "
                "budget gate. The daemon stays disabled until the GLM bulk "
                "lane exists (durability plan Phase 2)."
            ),
        },
    }


def _load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return _default_state()
    state = json.loads(path.read_text(encoding="utf-8"))
    if state.get("schema_version") != "pif_label_backfill_daemon_state_v1":
        raise RuntimeError("unsupported_daemon_state")
    return state


def _save_state(path: Path, state: dict[str, Any]) -> None:
    state["generation"] = int(state.get("generation") or 0) + 1
    state["updated_at"] = now_iso()
    write_text_atomic(path, json.dumps(state, indent=2, sort_keys=True) + "\n")


def _unexpired_label_claims() -> int:
    conn = db.connect()
    try:
        return int(
            conn.execute(
                """
                SELECT COUNT(*) FROM jobs
                WHERE lane = 'podcast' AND job_type = 'label_segment'
                  AND status = 'claimed' AND leased_until >= ?
                """,
                (now_iso(),),
            ).fetchone()[0]
        )
    finally:
        conn.close()


def _dispatchable_pending() -> int:
    conn = db.connect()
    try:
        return int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM jobs
                JOIN segments ON segments.id = jobs.target_id
                WHERE jobs.lane = 'podcast'
                  AND jobs.job_type = 'label_segment'
                  AND jobs.status = 'pending'
                  AND EXISTS (
                    SELECT 1 FROM episode_context_runs
                    WHERE episode_context_runs.episode_id = segments.episode_id
                      AND episode_context_runs.label_pack = 'ai_discourse_v3_1'
                      AND episode_context_runs.model = 'gpt-5.5'
                      AND episode_context_runs.status = 'completed'
                  )
                """
            ).fetchone()[0]
        )
    finally:
        conn.close()


def _recover_expired_daemon_claims(started_at: str) -> dict[str, int]:
    """Return an interrupted daemon wave to pending after its leases expire."""

    conn = db.connect()
    released = 0
    failed_runs = 0
    try:
        rows = conn.execute(
            """
            SELECT jobs.id, jobs.payload_json
            FROM jobs
            WHERE jobs.lane = 'podcast'
              AND jobs.job_type = 'label_segment'
              AND jobs.status = 'claimed'
              AND jobs.leased_until < ?
              AND jobs.updated_at >= ?
              AND NOT EXISTS (
                SELECT 1 FROM labels
                WHERE labels.segment_id = jobs.target_id
                  AND labels.label_pack = 'ai_discourse_v3_1'
                  AND labels.model = 'gpt-5.5'
              )
            """,
            (now_iso(), started_at),
        ).fetchall()
        ts = now_iso()
        conn.execute("BEGIN IMMEDIATE")
        for row in rows:
            payload = json.loads(row["payload_json"] or "{}")
            label_run_id = payload.get("label_run_id")
            if label_run_id:
                updated = conn.execute(
                    """
                    UPDATE label_runs
                    SET status = 'failed', error = ?, updated_at = ?
                    WHERE id = ? AND status = 'claimed'
                    """,
                    ("Daemon restart recovered an expired interrupted campaign claim.", ts, label_run_id),
                )
                failed_runs += int(updated.rowcount == 1)
            for key in ("prompt_path", "output_path", "label_run_id"):
                payload.pop(key, None)
            payload["daemon_restart_recovered_at"] = ts
            updated = conn.execute(
                """
                UPDATE jobs
                SET status = 'pending', lease_owner = NULL, leased_until = NULL,
                    attempts = CASE
                      WHEN max_attempts > 0 AND attempts >= max_attempts
                      THEN max_attempts - 1 ELSE attempts END,
                    payload_json = ?, error = ?, updated_at = ?
                WHERE id = ? AND status = 'claimed' AND leased_until < ?
                """,
                (json.dumps(payload, sort_keys=True, separators=(",", ":")), "Recovered after interrupted daemon campaign; ready for a fresh claim.", ts, row["id"], ts),
            )
            released += int(updated.rowcount == 1)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return {"released_jobs": released, "failed_label_runs": failed_runs}


def _rolling_totals(windows: list[dict[str, int]]) -> tuple[int, int]:
    labels = 0
    quarantines = 0
    for item in reversed(windows):
        if labels >= LABEL_FIELD_QUARANTINE_ROLLING_LABELS:
            break
        labels += int(item.get("labels") or 0)
        quarantines += int(item.get("field_bearing_quarantines") or 0)
    return labels, quarantines


def _historical_windows() -> list[dict[str, int]]:
    """Seed the rolling gate from immutable campaign reports on first start."""

    campaigns: dict[str, dict[str, Any]] = {}
    report_root = root() / "work/pif-ops/label-campaign-series"
    for path in report_root.glob("*/report.json"):
        try:
            report = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        for campaign in report.get("campaigns") or []:
            run_id = str(campaign.get("run_id") or "")
            repairs = campaign.get("deterministic_repair") or {}
            labels = int(campaign.get("segments_completed") or 0)
            if run_id and labels:
                campaigns[run_id] = {
                    "run_id": run_id,
                    "completed_at": str(campaign.get("completed_at") or ""),
                    "labels": labels,
                    "field_bearing_quarantines": int(
                        repairs.get("field_bearing_metric_quarantine_count") or 0
                    ),
                }
    ordered = sorted(campaigns.values(), key=lambda item: (item["completed_at"], item["run_id"]))
    return ordered[-8:]


def _field_flags(label_ids: list[str]) -> list[int]:
    if not label_ids:
        return []
    conn = db.connect()
    rows: list[Any] = []
    try:
        for offset in range(0, len(label_ids), 300):
            chunk = label_ids[offset : offset + 300]
            marks = ",".join("?" for _ in chunk)
            rows.extend(
                conn.execute(
                    f"""
                    SELECT l.id, l.created_at, q.original_metric_json
                    FROM labels AS l
                    LEFT JOIN label_metric_quarantines AS q ON q.label_id = l.id
                    WHERE l.id IN ({marks})
                    ORDER BY l.created_at, l.id
                    """,
                    chunk,
                ).fetchall()
            )
    finally:
        conn.close()
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        item = grouped.setdefault(
            str(row["id"]),
            {"created_at": str(row["created_at"]), "field_bearing": False},
        )
        if row["original_metric_json"]:
            metric = json.loads(row["original_metric_json"])
            item["field_bearing"] |= any(
                metric.get(field) not in (None, "")
                for field in ("raw_text", "value", "unit", "comparator")
            )
    ordered = sorted(grouped.items(), key=lambda pair: (pair[1]["created_at"], pair[0]))
    return [int(item["field_bearing"]) for _, item in ordered]


def _historical_label_flags() -> list[int]:
    label_ids: list[str] = []
    for window in _historical_windows():
        run_id = window["run_id"]
        for path in (root() / "work/pif-ops/backfills").glob(f"{run_id}/manifest.json"):
            try:
                label_ids.extend(json.loads(path.read_text(encoding="utf-8"))["label_ids"])
            except (OSError, json.JSONDecodeError, KeyError):
                pass
    return _field_flags(label_ids)[-LABEL_FIELD_QUARANTINE_ROLLING_LABELS:]


class LabelBackfillDaemon:
    def __init__(
        self,
        *,
        status_path: Path = STATUS_PATH,
        runner: Callable[..., dict[str, Any]] = run_label_campaign_series,
        claim_probe: Callable[[], int] = _unexpired_label_claims,
        pending_probe: Callable[[], int] = _dispatchable_pending,
        window_seed: Callable[[], list[dict[str, int]]] = _historical_windows,
        flag_seed: Callable[[], list[int]] = _historical_label_flags,
        expired_recover: Callable[[str], dict[str, int]] = _recover_expired_daemon_claims,
    ) -> None:
        self.status_path = status_path
        self.runner = runner
        self.claim_probe = claim_probe
        self.pending_probe = pending_probe
        self.window_seed = window_seed
        self.flag_seed = flag_seed
        self.expired_recover = expired_recover

    def run_iteration(self) -> dict[str, Any]:
        new_state = not self.status_path.exists()
        state = _load_state(self.status_path)
        if new_state:
            state["rolling_windows"] = self.window_seed()
            flags = self.flag_seed()
            state["rolling_label_field_flags"] = flags
            state["rolling_labels"] = len(flags)
            state["rolling_field_bearing_quarantines"] = sum(flags)
        if state.get("halted"):
            return state
        if state.get("status") in {"running", "waiting_for_claim_reconciliation"}:
            active = self.claim_probe()
            if active:
                state["status"] = "waiting_for_claim_reconciliation"
                state["unexpired_claims"] = active
                _save_state(self.status_path, state)
                return state
            started_at = str((state.get("current_campaign") or {}).get("started_at") or "")
            if started_at:
                state["last_restart_recovery"] = self.expired_recover(started_at)
            state["current_campaign"] = None
            state.pop("unexpired_claims", None)
        if self.pending_probe() == 0:
            state["status"] = "queue_empty"
            state["current_campaign"] = None
            _save_state(self.status_path, state)
            return state

        configuration = dict(state.get("configuration") or {})
        max_daily = configuration.get("max_daily_campaigns")
        if max_daily is not None:
            today = now_iso()[:10]
            launched_today = sum(
                1
                for entry in (state.get("campaign_history") or [])
                if str(entry.get("completed_at") or "").startswith(today)
            )
            if launched_today >= int(max_daily):
                state["status"] = "daily_campaign_cap_reached"
                state["current_campaign"] = None
                state["daily_campaign_cap"] = {
                    "day": today,
                    "launched": launched_today,
                    "max_daily_campaigns": int(max_daily),
                }
                _save_state(self.status_path, state)
                return state
        wave_size = int(configuration.get("claim_wave_size") or 25)
        concurrency = int(configuration.get("concurrency") or 3)
        runtime_ceiling = int(
            configuration.get("runtime_ceiling_seconds") or 3600
        )
        state["status"] = "running"
        state["current_campaign"] = {
            "started_at": now_iso(),
            "pid": os.getpid(),
            "bounds": {
                "campaign_limit": 1,
                "claim_wave_size": wave_size,
                "concurrency": concurrency,
                "runtime_ceiling_seconds": runtime_ceiling,
                "model": str(configuration.get("model") or "gpt-5.5"),
            },
        }
        _save_state(self.status_path, state)
        result = self.runner(
            campaign_limit=1,
            wave_size=wave_size,
            concurrency=concurrency,
            max_runtime_seconds=runtime_ceiling,
        )
        state = _load_state(self.status_path)
        if not result.get("campaigns"):
            state["status"] = "yielded"
            state["current_campaign"] = None
            state["last_yield"] = {
                "at": now_iso(),
                "reason": result.get("stop_reason") or "pipeline_lock_or_daily_cycle_priority",
                "skips": result.get("skips") or [],
                "daily_priority_waits": result.get("daily_priority_waits") or [],
            }
            _save_state(self.status_path, state)
            return state

        campaign = result["campaigns"][0]
        repairs = campaign.get("deterministic_repair") or {}
        windows = list(state.get("rolling_windows") or [])
        windows.append(
            {
                "labels": int(campaign.get("segments_completed") or 0),
                "field_bearing_quarantines": int(repairs.get("field_bearing_metric_quarantine_count") or 0),
            }
        )
        windows = windows[-8:]
        flags = list(state.get("rolling_label_field_flags") or [])
        manifest_path = campaign.get("manifest_path")
        if manifest_path:
            label_ids = json.loads(Path(manifest_path).read_text(encoding="utf-8"))["label_ids"]
            flags.extend(_field_flags(label_ids))
        else:
            labels = int(campaign.get("segments_completed") or 0)
            field_count = int(repairs.get("field_bearing_metric_quarantine_count") or 0)
            flags.extend([1] * min(field_count, labels) + [0] * max(0, labels - field_count))
        flags = flags[-LABEL_FIELD_QUARANTINE_ROLLING_LABELS:]
        rolling_labels, rolling_quarantines = len(flags), sum(flags)
        gate = evaluate_label_campaign_gate(
            campaign,
            rolling_labels=rolling_labels,
            rolling_field_bearing_quarantines=rolling_quarantines,
        )
        summary = {
            "series_id": result.get("series_id"),
            "run_id": campaign.get("run_id"),
            "completed_at": campaign.get("completed_at"),
            "segments_completed": campaign.get("segments_completed"),
            "provider_calls": campaign.get("provider_calls"),
            "tokens": campaign.get("tokens"),
            "report_path": campaign.get("report_path"),
            "gate": gate,
        }
        history = list(state.get("campaign_history") or [])
        history.append(summary)
        state["campaign_history"] = history[-100:]
        state["rolling_windows"] = windows
        state["rolling_label_field_flags"] = flags
        state["rolling_labels"] = rolling_labels
        state["rolling_field_bearing_quarantines"] = rolling_quarantines
        state["campaigns_completed"] = int(state.get("campaigns_completed") or 0) + 1
        state["current_campaign"] = None
        if not gate["passed"]:
            state["status"] = "halted_on_gate"
            state["halted"] = True
            state["halt_reason"] = {
                "at": now_iso(),
                "reasons": gate["reasons"],
                "run_id": campaign.get("run_id"),
                "report_path": campaign.get("report_path"),
            }
        else:
            state["status"] = "idle"
        _save_state(self.status_path, state)
        return state

    def serve(self, *, max_iterations: int | None = None, pause_seconds: float = CAMPAIGN_PAUSE_SECONDS) -> dict[str, Any]:
        iterations = 0
        while max_iterations is None or iterations < max_iterations:
            state = self.run_iteration()
            iterations += 1
            if state.get("halted") or state.get("status") == "queue_empty":
                return state
            time.sleep(pause_seconds)
        return _load_state(self.status_path)


def _serve_with_singleton(daemon: LabelBackfillDaemon) -> dict[str, Any]:
    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOCK_PATH.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            state = _load_state(daemon.status_path)
            state["status"] = "duplicate_daemon_refused"
            _save_state(daemon.status_path, state)
            return state
        return daemon.serve()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("serve", "once", "status", "stop"))
    args = parser.parse_args()
    daemon = LabelBackfillDaemon()
    if args.command == "serve":
        result = _serve_with_singleton(daemon)
    elif args.command == "once":
        result = daemon.run_iteration()
    elif args.command == "stop":
        result = request_active_label_campaign_stop(
            reason="operator_requested_daemon_stop"
        )
    else:
        result = _load_state(STATUS_PATH)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
