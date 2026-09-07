#!/usr/bin/env python3
"""Serial, metered experimental SOL labeling with fenced leases and receipts."""
import argparse
import asyncio
import fcntl
import json
import os
from pathlib import Path
import sqlite3
import sys
import time
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.pif_signal_desk_attribution_contrasts import main as freeze
from scripts.pif_signal_desk_rubric_reference_review import QUAL
from scripts.pif_signal_desk_gold_merge_provenance import immutable_json
from research_factory.signal_desk_attribution_contrasts import SYSTEM, response_schema, validate_response
from research_factory.signal_desk_rubric_reference_packets import digest
from research_factory.codex_app_server import CodexAppServerClient
from research_factory.signal_desk_rebuild_dispatch import initialize_dispatch_schema, enqueue_task, acquire_lease, complete_attempt, fail_attempt_semantically, release_attempt_for_retry
from research_factory.signal_desk_gold_budget import ensure_gold_budget_schema, reserve_gold_call, settle_gold_call, mark_provider_started, release_unstarted_reservation
from research_factory.signal_desk_gold_capacity import admit_gold_call, release_gold_admission, record_gold_admission_success, capacity_error_from_sidecar, capacity_backend_message_from_sidecar, record_capacity_failure, is_model_capacity_error
from research_factory.signal_desk_adaptive_concurrency import initialize_lane, GOLD_BOUNDS, admission_limit, record_outcome

BASE = QUAL / "attribution-schema-v3-contrasts-v1"
OUT = BASE / "sol-independent-v1"


async def execute(packets, *, output_root=None, task_prefix="attribution-probe-sol-v1",
                  system_for_packet=None, schema_for_packet=None, validator=None,
                  turn_for_packet=None):
    out = output_root if output_root is not None else OUT
    system_for_packet = system_for_packet or (lambda p: SYSTEM)
    schema_for_packet = schema_for_packet or response_schema
    validator = validator or validate_response
    turn_for_packet = turn_for_packet or (lambda p: "A")
    out.mkdir(parents=True, exist_ok=True)
    dispatch = sqlite3.connect(out / "dispatch.sqlite"); dispatch.row_factory = sqlite3.Row
    budget = sqlite3.connect(ROOT / "data/factory.sqlite"); budget.row_factory = sqlite3.Row
    initialize_dispatch_schema(dispatch); ensure_gold_budget_schema(budget)
    initialize_lane(budget, lane="gold", bounds=GOLD_BOUNDS, initial_limit=2)
    by_sha = {p["packet_sha256"]: p for p in packets}
    for sha in by_sha:
        enqueue_task(dispatch, task_key=task_prefix + ":" + sha, task_type="gold_attribution_probe", payload={"packet_sha256": sha})
    try:
        async with CodexAppServerClient(command=["codex", "app-server", "--stdio", "--strict-config"], expected_cli_version="0.147.0") as client:
            while True:
                lease = acquire_lease(dispatch, lease_owner=f"attribution-probe:{os.getpid()}", lease_seconds=1800)
                if lease is None: break
                fence = {"attempt_id": lease["current_attempt_id"], "lease_owner": lease["lease_owner"], "lease_generation": lease["lease_generation"]}
                sha = lease["payload"]["packet_sha256"]; packet = by_sha[sha]
                result_path = out / f"{sha}.result.json"
                if result_path.exists():
                    result = validator(json.loads(result_path.read_text()), packet)
                    complete_attempt(dispatch, **fence, output=result); continue
                # Unknown previous paid work is never silently sent a second time.
                previous = budget.execute("SELECT id FROM signal_desk_gold_budget_reservations WHERE task_key LIKE ? AND provider_started=1", (f"{task_prefix}:{sha}:%",)).fetchone()
                if previous:
                    release_attempt_for_retry(dispatch, **fence, failure_code="previous_provider_recovery_required",
                        failure_detail="previous paid attempt must be recovered; no duplicate dispatch")
                    raise RuntimeError("previous provider attempt requires sidecar recovery before redispatch")
                key = f"{task_prefix}:{sha}:{fence['attempt_id']}:{fence['lease_generation']}"
                limit = admission_limit(budget, lane="gold")["effective_limit"]
                admitted = admit_gold_call(budget, task_key=key, lease_owner=fence["lease_owner"], configured_concurrency=min(2, int(limit)), lane="gpt_5_6_sol_gold_authoring")
                if not admitted["allowed"]:
                    release_attempt_for_retry(dispatch, **fence, failure_code=admitted["reason"], failure_detail="capacity admission deferred; no provider call")
                    raise RuntimeError("capacity admission deferred; resume from checkpoint after circuit recovery")
                reservation = None; started = False; began = time.monotonic()
                try:
                    snapshot = await client.read_weekly_rate_limit()
                    reservation = reserve_gold_call(budget, grant_path=ROOT / "config/signal_desk_gold_authoring_budget_grant.json",
                        session_root=Path.home() / ".codex/sessions", budget_dir=ROOT / "work/pif-ops/budget",
                        task_key=key, turn_type=turn_for_packet(packet), reserve_tokens=75000 if turn_for_packet(packet) == "C" else 48000, live_snapshot=snapshot)
                    if not reservation.get("allowed"):
                        raise RuntimeError("weekly gold budget denied: " + str(reservation.get("reason")))
                    mark_provider_started(budget, reservation["reservation_id"]); started = True
                    result = await client.run_ephemeral_structured_turn(model="gpt-5.6-sol", effort="medium", base_instructions=system_for_packet(packet),
                        prompt=json.dumps(packet, ensure_ascii=False), output_schema=schema_for_packet(packet), cwd=ROOT,
                        sidecar_path=out / f"{sha}.sidecar.json", output_path=out / f"{sha}.output.json", timeout_seconds=900)
                    if result.usage is not None:
                        settle_gold_call(budget, reservation_id=reservation["reservation_id"], actual_tokens=result.usage.total_tokens)
                    else:
                        raise RuntimeError("provider usage missing; reservation retained for recovery")
                    if not result.status_ok or result.output is None:
                        raise RuntimeError("provider result requires recovery: " + str(result.error_class or result.status))
                    try:
                        value = validator(result.output, packet)
                    except ValueError as exc:
                        fail_attempt_semantically(dispatch, **fence, failure_code="attribution_probe_contract_failure", failure_detail=str(exc))
                        record_outcome(budget, lane="gold", outcome="parse_schema", latency_seconds=time.monotonic()-began)
                        continue
                    immutable_json(result_path, value)
                    complete_attempt(dispatch, **fence, output=value)
                    record_outcome(budget, lane="gold", outcome="success", latency_seconds=time.monotonic()-began)
                    record_gold_admission_success(budget, admission_id=admitted["admission_id"])
                except Exception as exc:
                    sidecar = out / f"{sha}.sidecar.json"
                    capacity_code = capacity_error_from_sidecar(sidecar) if sidecar.exists() else None
                    if not is_model_capacity_error(error_code=capacity_code):
                        capacity_code = None
                    if capacity_code:
                        record_capacity_failure(budget, admission_id=admitted["admission_id"], error_code=capacity_code,
                            backend_message=capacity_backend_message_from_sidecar(sidecar))
                    record_outcome(budget, lane="gold", outcome="rate_limit" if capacity_code else ("timeout" if "timeout" in type(exc).__name__.lower() else "failure"), latency_seconds=time.monotonic()-began)
                    release_attempt_for_retry(dispatch, **fence, failure_code="attribution_probe_recovery_required",
                        failure_detail=str(exc)[:200])
                    raise
                finally:
                    release_gold_admission(budget, admission_id=admitted["admission_id"])
                    if reservation and reservation.get("allowed") and not started:
                        release_unstarted_reservation(budget, reservation["reservation_id"])
        counts = dict(dispatch.execute("SELECT status,COUNT(*) FROM signal_desk_rebuild_tasks GROUP BY status").fetchall())
        complete = counts.get("succeeded", 0) == len(packets)
        immutable_json(out / "receipt.json", {"complete": complete, "tasks": counts, "qualified": False, "gold_accepted": False})
        return 0 if complete else 2
    finally:
        dispatch.close(); budget.close()


def main():
    p = argparse.ArgumentParser(); p.add_argument("--execute", action="store_true"); args = p.parse_args()
    freeze()  # Rechecks source bytes, frozen selection, system/schema and plan.
    plan = json.loads((BASE / "plan.json").read_text())
    packets = [json.loads((BASE / f"{sha}.packet.json").read_text()) for sha in plan["packet_digests"]]
    for packet in packets:
        if digest({k:v for k,v in packet.items() if k != "packet_sha256"}) != packet["packet_sha256"]:
            raise ValueError("packet digest mismatch")
    with (BASE / "sol-independent.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if args.execute: raise SystemExit(asyncio.run(execute(packets)))


if __name__ == "__main__":
    main()
