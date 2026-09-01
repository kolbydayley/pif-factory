"""Resumable A1 single-pass calibration under the ordinary subscription cap."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import subprocess
import time
from pathlib import Path
from typing import Any, Mapping

from .codex_app_server import CodexAppServerClient
from .signal_desk_frontier import measure_frontier
from .signal_desk_gold_runner import repair_unique_evidence_offsets
from .signal_desk_rebuild_contracts import EvidenceContractError, event_schema, validate_output
from .signal_desk_rebuild_dispatch import (
    acquire_lease,
    complete_attempt,
    enqueue_task,
    fail_attempt_semantically,
    initialize_dispatch_schema,
    release_attempt_for_retry,
)
from .signal_desk_rebuild_gold import build_gold_packets, verify_frozen_manifest
from .signal_desk_rebuild_gold_canary import _prompt
from .subscription_budget import (
    budget_gate,
    ensure_budget_schema,
    record_usage,
    subscription_budget_window,
)
from .util import now_iso, stable_id


LANE = "gpt_5_6_sol_frontier_calibration"
RESERVE_TOKENS = 48_000


def ensure_frontier_budget_schema(conn: sqlite3.Connection) -> None:
    ensure_budget_schema(conn)
    conn.execute(
        """CREATE TABLE IF NOT EXISTS signal_desk_frontier_reservations (
             id TEXT PRIMARY KEY, day TEXT NOT NULL, task_key TEXT NOT NULL,
             reserved_tokens INTEGER NOT NULL, actual_tokens INTEGER,
             status TEXT NOT NULL CHECK(status IN ('active','settled')),
             created_at TEXT NOT NULL, settled_at TEXT,
             UNIQUE(day,task_key)
           )"""
    )


def reserve_frontier_call(
    conn: sqlite3.Connection, *, task_key: str, budget_dir: Path
) -> dict[str, Any]:
    ensure_frontier_budget_schema(conn)
    day, window_start = subscription_budget_window()
    gate = budget_gate(
        conn, day=day, budget_dir=budget_dir, window_start_iso=window_start
    )
    active = int(
        conn.execute(
            "SELECT COALESCE(SUM(reserved_tokens),0) FROM signal_desk_frontier_reservations "
            "WHERE day=? AND status='active'",
            (day,),
        ).fetchone()[0]
    )
    if not gate["allowed"] or gate["tokens_used"] + active + RESERVE_TOKENS > gate["cap_tokens"]:
        return {
            **gate,
            "allowed": False,
            "reason": gate["reason"] or "daily_reservations_exhaust_cap",
        }
    reservation_id = stable_id(day, task_key, prefix="sda1r_")
    conn.execute(
        "INSERT INTO signal_desk_frontier_reservations VALUES (?,?,?,?,NULL,'active',?,NULL)",
        (reservation_id, day, task_key, RESERVE_TOKENS, now_iso()),
    )
    conn.commit()
    return {"allowed": True, "reservation_id": reservation_id, "day": day}


def settle_frontier_call(
    conn: sqlite3.Connection, *, reservation_id: str, actual_tokens: int
) -> None:
    row = conn.execute(
        "SELECT * FROM signal_desk_frontier_reservations WHERE id=? AND status='active'",
        (reservation_id,),
    ).fetchone()
    if row is None:
        raise RuntimeError("active A1 reservation not found")
    record_usage(
        conn,
        day=str(row["day"]),
        provider_lane="codex_subscription",
        lane=LANE,
        run_id=f"a1:{reservation_id}",
        tokens=actual_tokens,
        provider_calls=1,
    )
    conn.execute(
        "UPDATE signal_desk_frontier_reservations SET status='settled',actual_tokens=?,settled_at=? WHERE id=?",
        (actual_tokens, now_iso(), reservation_id),
    )
    conn.commit()


def _notify(detail: str) -> None:
    subprocess.run(
        [
            "codex-ops", "notify", "--source", "signal-desk-a1",
            "--summary", "Signal Desk A1 calibration stalled", "--severity", "high",
            "--details", detail,
            "--next-step", "Resume after the ordinary subscription budget resets or repair the failed semantic lease.",
            "--dedupe-key", "signal-desk-a1-stall", "--telegram-mode", "prefer", "--json",
        ],
        capture_output=True, text=True, timeout=30, check=False,
    )


async def run_frontier_calibration(
    *, manifest_path: Path, project_root: Path, gold_c_root: Path,
    prediction_root: Path, artifact_root: Path, dispatch_database: Path,
    budget_database: Path, budget_dir: Path, system_prompt_path: Path,
    dev_audit_path: Path, concurrency: int = 4,
) -> dict[str, Any]:
    if not 2 <= concurrency <= 8:
        raise ValueError("A1 concurrency must be 2-8")
    audit = json.loads(dev_audit_path.read_text(encoding="utf-8"))
    if audit.get("passed") is not True:
        raise RuntimeError("A1 is blocked until the development gold audit passes")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    verify_frozen_manifest(manifest)
    packets = build_gold_packets(
        manifest, project_root=project_root, gold_pass="A", splits=("development",)
    )
    by_window = {str(packet["input"]["window_id"]): packet for packet in packets}
    prompt_text = system_prompt_path.read_text(encoding="utf-8")
    prediction_root.mkdir(parents=True, exist_ok=True)
    dispatch = sqlite3.connect(dispatch_database); dispatch.row_factory = sqlite3.Row
    budget = sqlite3.connect(budget_database); budget.row_factory = sqlite3.Row
    initialize_dispatch_schema(dispatch); ensure_frontier_budget_schema(budget)
    stop = asyncio.Event(); completed = []
    try:
        for window_id, packet in sorted(by_window.items()):
            path = prediction_root / f"{window_id}.json"
            if path.exists():
                value, repairs = repair_unique_evidence_offsets(
                    json.loads(path.read_text()),
                    transcript_window=str(packet["input"]["window_text"]),
                )
                validate_output(
                    value, transcript_window=str(packet["input"]["window_text"]),
                    expected_window_id=window_id,
                )
                if repairs:
                    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
                continue
            enqueue_task(
                dispatch, task_key=f"a1:{window_id}", task_type="frontier_window",
                payload={"window_id": window_id},
            )

        lock = asyncio.Lock()

        async def worker(index: int) -> None:
            async with CodexAppServerClient(
                command=["codex", "app-server", "--stdio", "--strict-config"],
                expected_cli_version="0.147.0",
            ) as client:
                while not stop.is_set():
                    async with lock:
                        lease = acquire_lease(
                            dispatch, lease_owner=f"a1-{index}", lease_seconds=1200
                        )
                    if lease is None:
                        return
                    window_id = str(lease["payload"]["window_id"])
                    packet = by_window[window_id]
                    reservation = reserve_frontier_call(
                        budget,
                        task_key=(
                            f"a1:{window_id}:attempt:{lease['current_attempt_id']}:"
                            f"generation:{lease['lease_generation']}"
                        ),
                        budget_dir=budget_dir,
                    )
                    if not reservation["allowed"]:
                        release_attempt_for_retry(
                            dispatch, attempt_id=int(lease["current_attempt_id"]),
                            lease_owner=str(lease["lease_owner"]),
                            lease_generation=int(lease["lease_generation"]),
                            failure_code="a1_budget_stall", failure_detail=str(reservation["reason"]),
                        )
                        stop.set(); _notify(str(reservation["reason"])); return
                    output_path = prediction_root / f"{window_id}.json"
                    settled = False
                    try:
                        result = await client.run_ephemeral_structured_turn(
                            model="gpt-5.6-sol", effort="medium", base_instructions=prompt_text,
                            prompt=_prompt(packet), output_schema=event_schema(), cwd=project_root,
                            sidecar_path=artifact_root / "a1-sidecars" / f"{window_id}.json",
                            output_path=output_path, timeout_seconds=900,
                        )
                        usage = int(result.usage.total_tokens) if result.usage else RESERVE_TOKENS
                        settle_frontier_call(
                            budget, reservation_id=str(reservation["reservation_id"]),
                            actual_tokens=usage,
                        )
                        settled = True
                        if not result.status_ok or result.output is None:
                            raise RuntimeError(result.error_class or result.status)
                        value, repairs = repair_unique_evidence_offsets(
                            result.output,
                            transcript_window=str(packet["input"]["window_text"]),
                        )
                        validated = validate_output(
                            value, transcript_window=str(packet["input"]["window_text"]),
                            expected_window_id=window_id,
                        )
                        if repairs:
                            output_path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
                    except Exception as exc:  # noqa: BLE001
                        if not settled:
                            settle_frontier_call(
                                budget,
                                reservation_id=str(reservation["reservation_id"]),
                                actual_tokens=RESERVE_TOKENS,
                            )
                        detail = f"{type(exc).__name__}: {exc}"
                        if isinstance(exc, EvidenceContractError):
                            if output_path.exists():
                                rejected = artifact_root / "a1-rejected" / output_path.name
                                rejected.parent.mkdir(parents=True, exist_ok=True)
                                output_path.replace(rejected)
                            fail_attempt_semantically(
                                dispatch, attempt_id=int(lease["current_attempt_id"]),
                                lease_owner=str(lease["lease_owner"]),
                                lease_generation=int(lease["lease_generation"]),
                                failure_code="a1_contract_failure", failure_detail=detail,
                            )
                        else:
                            release_attempt_for_retry(
                                dispatch, attempt_id=int(lease["current_attempt_id"]),
                                lease_owner=str(lease["lease_owner"]),
                                lease_generation=int(lease["lease_generation"]),
                                failure_code="a1_infrastructure_failure", failure_detail=detail,
                            )
                        stop.set(); _notify(str(exc)); return
                    complete_attempt(
                        dispatch, attempt_id=int(lease["current_attempt_id"]),
                        lease_owner=str(lease["lease_owner"]),
                        lease_generation=int(lease["lease_generation"]),
                        output={"window_id": window_id, "events": len(validated["events"]),
                                "tokens": usage, "offset_repairs": repairs},
                    )
                    completed.append({"window_id": window_id, "tokens": usage})

        started = time.monotonic()
        await asyncio.gather(*(worker(index) for index in range(concurrency)))
        if stop.is_set():
            raise RuntimeError("A1 stopped with a clean checkpoint")
        if len(list(prediction_root.glob("*.json"))) != len(by_window):
            raise RuntimeError("A1 output set is incomplete")
        ceiling, gates = measure_frontier(
            manifest_path=manifest_path, gold_c_root=gold_c_root,
            prediction_root=prediction_root,
        )
        artifact_root.mkdir(parents=True, exist_ok=True)
        (artifact_root / "frontier-ceiling.json").write_text(
            json.dumps(ceiling, indent=2, sort_keys=True) + "\n"
        )
        (artifact_root / "frozen-gates.json").write_text(
            json.dumps(gates, indent=2, sort_keys=True) + "\n"
        )
        return {"complete": True, "windows": len(by_window), "new_calls": len(completed),
                "new_tokens": sum(row["tokens"] for row in completed),
                "wall_seconds": time.monotonic() - started,
                "frontier_receipt_sha256": ceiling["receipt_sha256"]}
    finally:
        dispatch.close(); budget.close()
