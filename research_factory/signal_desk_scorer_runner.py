"""Run GPT-5.6-sol adjudication for the frozen A2 scorer decisions."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import subprocess
import time
from pathlib import Path
from typing import Any, Mapping

from .codex_app_server import CodexAppServerClient
from .signal_desk_rebuild_dispatch import (
    acquire_lease,
    complete_attempt,
    enqueue_task,
    initialize_dispatch_schema,
    release_attempt_for_retry,
)
from .signal_desk_rebuild_gold import build_gold_packets, verify_frozen_manifest
from .signal_desk_scorer_qualification import (
    qualification_receipt,
    select_qualification_cases,
)
from .subscription_budget import (
    budget_gate,
    ensure_budget_schema,
    record_usage,
    subscription_budget_window,
)
from .util import now_iso, stable_id


LANE = "gpt_5_6_sol_scorer_qualification"
RESERVE_TOKENS = 32_000
OUTPUT_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "additionalProperties": False,
    "required": ["case_id", "expected_match", "error_family", "rationale"],
    "properties": {
        "case_id": {"type": "string"},
        "expected_match": {"type": "boolean"},
        "error_family": {"type": "string"},
        "rationale": {"type": "string"},
    },
}
SYSTEM_PROMPT = """You are the independent qualification judge for a deterministic claim matcher.
Use only the complete transcript window, the gold event, the predicted event, and the frozen scorer
specification supplied in the request. Decide whether the prediction is eligible to represent that
gold event under the specification. Do not defer to the scorer's own decision. Check exact evidence
location/overlap, atomic claim equivalence, attribution role and identities, stance, quoted/mentioned
person rules, transcript structure, and ASR surface aliases. Issue-label wording is diagnostic and does
not by itself make an otherwise equivalent event ineligible. Return one JSON decision."""


def ensure_scorer_budget_schema(conn: sqlite3.Connection) -> None:
    ensure_budget_schema(conn)
    conn.execute(
        """CREATE TABLE IF NOT EXISTS signal_desk_scorer_reservations (
             id TEXT PRIMARY KEY, day TEXT NOT NULL, task_key TEXT NOT NULL,
             reserved_tokens INTEGER NOT NULL, actual_tokens INTEGER,
             status TEXT NOT NULL CHECK(status IN ('active','settled')),
             created_at TEXT NOT NULL, settled_at TEXT,
             UNIQUE(day,task_key)
           )"""
    )


def reserve_scorer_call(
    conn: sqlite3.Connection, *, task_key: str, budget_dir: Path
) -> dict[str, Any]:
    ensure_scorer_budget_schema(conn)
    day, window_start = subscription_budget_window()
    gate = budget_gate(
        conn, day=day, budget_dir=budget_dir, window_start_iso=window_start
    )
    active = int(
        conn.execute(
            "SELECT COALESCE(SUM(reserved_tokens),0) FROM signal_desk_scorer_reservations "
            "WHERE day=? AND status='active'", (day,),
        ).fetchone()[0]
    )
    if not gate["allowed"] or gate["tokens_used"] + active + RESERVE_TOKENS > gate["cap_tokens"]:
        return {**gate, "allowed": False,
                "reason": gate["reason"] or "daily_reservations_exhaust_cap"}
    reservation_id = stable_id(day, task_key, prefix="sda2r_")
    conn.execute(
        "INSERT INTO signal_desk_scorer_reservations VALUES (?,?,?,?,NULL,'active',?,NULL)",
        (reservation_id, day, task_key, RESERVE_TOKENS, now_iso()),
    )
    conn.commit()
    return {"allowed": True, "reservation_id": reservation_id, "day": day}


def settle_scorer_call(
    conn: sqlite3.Connection, *, reservation_id: str, actual_tokens: int
) -> None:
    row = conn.execute(
        "SELECT * FROM signal_desk_scorer_reservations WHERE id=? AND status='active'",
        (reservation_id,),
    ).fetchone()
    if row is None:
        raise RuntimeError("active A2 reservation not found")
    record_usage(
        conn, day=str(row["day"]), provider_lane="codex_subscription", lane=LANE,
        run_id=f"a2:{reservation_id}", tokens=actual_tokens, provider_calls=1,
    )
    conn.execute(
        "UPDATE signal_desk_scorer_reservations SET status='settled',actual_tokens=?,settled_at=? WHERE id=?",
        (actual_tokens, now_iso(), reservation_id),
    )
    conn.commit()


def build_selection(
    *, manifest: Mapping[str, Any], project_root: Path,
    gold_root: Path, prediction_root: Path, total: int,
) -> tuple[dict[str, Any], dict[str, Mapping[str, Any]], dict[str, Mapping[str, Any]]]:
    packets = build_gold_packets(
        manifest, project_root=project_root, gold_pass="A", splits=("development",)
    )
    by_window = {str(packet["input"]["window_id"]): packet for packet in packets}
    gold = {path.stem: json.loads(path.read_text()) for path in gold_root.glob("*.json")}
    predictions = {
        path.stem: json.loads(path.read_text()) for path in prediction_root.glob("*.json")
    }
    if set(by_window) != set(gold) or set(by_window) != set(predictions):
        raise RuntimeError("A2 requires the complete shared A1 prediction-vs-gold pairs")
    rows = [
        {
            "window_id": window_id,
            "transcript_structure": packet["input"]["transcript_structure"],
            "gold": gold[window_id],
            "predicted": predictions[window_id],
        }
        for window_id, packet in sorted(by_window.items())
    ]
    return select_qualification_cases(rows, total=total), by_window, {
        "gold": gold, "predicted": predictions
    }


def _notify(detail: str) -> None:
    subprocess.run(
        ["codex-ops", "notify", "--source", "signal-desk-a2",
         "--summary", "Signal Desk A2 scorer qualification stalled", "--severity", "high",
         "--details", detail,
         "--next-step", "Resume from the leased checkpoint after the ordinary budget or provider recovers.",
         "--dedupe-key", "signal-desk-a2-stall", "--telegram-mode", "prefer", "--json"],
        capture_output=True, text=True, timeout=30, check=False,
    )


async def run_scorer_qualification(
    *, manifest_path: Path, project_root: Path, gold_root: Path,
    prediction_root: Path, artifact_root: Path, dispatch_database: Path,
    budget_database: Path, budget_dir: Path, total: int = 100,
    concurrency: int = 4,
) -> dict[str, Any]:
    if not 2 <= concurrency <= 8:
        raise ValueError("A2 concurrency must be 2-8")
    manifest = json.loads(manifest_path.read_text())
    verify_frozen_manifest(manifest)
    selection, packets, outputs = build_selection(
        manifest=manifest, project_root=project_root, gold_root=gold_root,
        prediction_root=prediction_root, total=total,
    )
    artifact_root.mkdir(parents=True, exist_ok=True)
    selection_path = artifact_root / f"scorer-selection-{total}.private.json"
    if selection_path.exists():
        prior = json.loads(selection_path.read_text())
        if prior["selection_sha256"] != selection["selection_sha256"]:
            raise RuntimeError("frozen A2 selection drifted")
        selection = prior
    else:
        selection_path.write_text(json.dumps(selection, indent=2, sort_keys=True) + "\n")
        selection_path.chmod(0o600)
    result_root = artifact_root / f"scorer-adjudications-{total}.private"
    result_root.mkdir(parents=True, exist_ok=True)
    dispatch = sqlite3.connect(dispatch_database); dispatch.row_factory = sqlite3.Row
    budget = sqlite3.connect(budget_database); budget.row_factory = sqlite3.Row
    initialize_dispatch_schema(dispatch); ensure_scorer_budget_schema(budget)
    stop = asyncio.Event(); completed = []
    case_map = {case["case_id"]: case for case in selection["cases"]}
    try:
        for case_id in sorted(case_map):
            if (result_root / f"{case_id}.json").exists():
                continue
            enqueue_task(
                dispatch, task_key=f"a2:{selection['selection_sha256']}:{case_id}",
                task_type="scorer_case", payload={"case_id": case_id},
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
                            dispatch, lease_owner=f"a2-{index}", lease_seconds=1200
                        )
                    if lease is None:
                        return
                    case_id = str(lease["payload"]["case_id"])
                    case = case_map[case_id]
                    reservation = reserve_scorer_call(
                        budget,
                        task_key=(f"a2:{case_id}:attempt:{lease['current_attempt_id']}:"
                                  f"generation:{lease['lease_generation']}"),
                        budget_dir=budget_dir,
                    )
                    if not reservation["allowed"]:
                        release_attempt_for_retry(
                            dispatch, attempt_id=int(lease["current_attempt_id"]),
                            lease_owner=str(lease["lease_owner"]),
                            lease_generation=int(lease["lease_generation"]),
                            failure_code="a2_budget_stall", failure_detail=str(reservation["reason"]),
                        )
                        stop.set(); _notify(str(reservation["reason"])); return
                    packet = packets[case["window_id"]]
                    gold = outputs["gold"][case["window_id"]]["events"][case["gold_index"]]
                    predicted = outputs["predicted"][case["window_id"]]["events"][case["predicted_index"]]
                    prompt = (
                        f"case_id: {case_id}\ntranscript_structure: {case['transcript_structure']}\n\n"
                        "FROZEN SCORER SPECIFICATION\n" + json.dumps(selection["scorer_specification"], sort_keys=True)
                        + "\n\nTRANSCRIPT WINDOW START\n" + str(packet["input"]["window_text"])
                        + "\nTRANSCRIPT WINDOW END\n\nGOLD EVENT\n" + json.dumps(gold, sort_keys=True)
                        + "\n\nPREDICTED EVENT\n" + json.dumps(predicted, sort_keys=True)
                    )
                    settled = False
                    try:
                        result = await client.run_ephemeral_structured_turn(
                            model="gpt-5.6-sol", effort="medium", base_instructions=SYSTEM_PROMPT,
                            prompt=prompt, output_schema=OUTPUT_SCHEMA, cwd=project_root,
                            sidecar_path=result_root / f"{case_id}.sidecar.json",
                            output_path=result_root / f"{case_id}.json", timeout_seconds=900,
                        )
                        usage = int(result.usage.total_tokens) if result.usage else RESERVE_TOKENS
                        settle_scorer_call(
                            budget, reservation_id=str(reservation["reservation_id"]),
                            actual_tokens=usage,
                        )
                        settled = True
                        if not result.status_ok or result.output is None:
                            raise RuntimeError(result.error_class or result.status)
                        if result.output.get("case_id") != case_id:
                            raise RuntimeError("A2 case identity mismatch")
                    except Exception as exc:  # noqa: BLE001
                        if not settled:
                            settle_scorer_call(
                                budget, reservation_id=str(reservation["reservation_id"]),
                                actual_tokens=RESERVE_TOKENS,
                            )
                        release_attempt_for_retry(
                            dispatch, attempt_id=int(lease["current_attempt_id"]),
                            lease_owner=str(lease["lease_owner"]),
                            lease_generation=int(lease["lease_generation"]),
                            failure_code="a2_failure", failure_detail=f"{type(exc).__name__}: {exc}",
                        )
                        stop.set(); _notify(str(exc)); return
                    complete_attempt(
                        dispatch, attempt_id=int(lease["current_attempt_id"]),
                        lease_owner=str(lease["lease_owner"]),
                        lease_generation=int(lease["lease_generation"]),
                        output={"case_id": case_id, "tokens": usage},
                    )
                    completed.append(usage)

        started = time.monotonic()
        await asyncio.gather(*(worker(index) for index in range(concurrency)))
        if stop.is_set():
            raise RuntimeError("A2 stopped with a clean checkpoint")
        adjudications = [
            json.loads((result_root / f"{case['case_id']}.json").read_text())
            for case in selection["cases"]
        ]
        receipt = qualification_receipt(selection, adjudications)
        (artifact_root / "scorer-qualified.json").write_text(
            json.dumps(receipt, indent=2, sort_keys=True) + "\n"
        )
        return {"complete": True, "new_calls": len(completed),
                "new_tokens": sum(completed), "wall_seconds": time.monotonic() - started,
                **receipt}
    finally:
        dispatch.close(); budget.close()
