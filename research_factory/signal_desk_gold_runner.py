"""Resumable dev-first Gold A/B/C/audit runner for Signal Desk."""

from __future__ import annotations

import asyncio
import hashlib
import json
import shutil
import sqlite3
import subprocess
import time
from pathlib import Path
from typing import Any, Mapping

from .codex_app_server import CodexAppServerClient
from .signal_desk_gold_budget import mark_provider_started, reserve_gold_call, settle_gold_call
from .signal_desk_gold_measurement import (
    A_SYSTEM_PROMPT, AUDIT_SYSTEM_PROMPT, B_SYSTEM_PROMPT, C_SYSTEM_PROMPT,
)
from .signal_desk_rebuild_contracts import validate_output
from .signal_desk_rebuild_dispatch import (
    acquire_lease, complete_attempt, enqueue_task, fail_attempt_semantically,
    initialize_dispatch_schema, release_attempt_for_retry,
)
from .signal_desk_rebuild_gold import (
    build_gold_packets, select_blind_gold_audit_windows, verify_frozen_manifest,
)
from .signal_desk_rebuild_gold_canary import _prompt
from .util import now_iso


RESERVE_TOKENS = {"A": 48_000, "B": 49_000, "C": 57_000, "AUDIT": 48_000}
SYSTEM_PROMPTS = {"A": A_SYSTEM_PROMPT, "B": B_SYSTEM_PROMPT,
                  "C": C_SYSTEM_PROMPT, "AUDIT": AUDIT_SYSTEM_PROMPT}


def repair_unique_evidence_offsets(
    output: Mapping[str, Any], *, transcript_window: str
) -> tuple[dict[str, Any], int]:
    """Rebind offset-only slips when a verbatim excerpt occurs exactly once."""

    repaired = json.loads(json.dumps(output))
    count = 0
    for event in repaired.get("events") or []:
        evidence = str(event.get("evidence_text") or "")
        declared_start = int(event.get("evidence_start") or 0)
        declared_end = int(event.get("evidence_end") or 0)
        if (
            evidence
            and declared_start >= 0
            and declared_end == declared_start + len(evidence)
            and transcript_window[declared_start:declared_end] == evidence
        ):
            continue
        first = transcript_window.find(evidence) if evidence else -1
        if first >= 0 and transcript_window.find(evidence, first + 1) < 0:
            event["evidence_start"] = first
            event["evidence_end"] = first + len(evidence)
            count += 1
            continue
        # Flattened captions occasionally omit or add a space while copying.
        # Restore only source whitespace at the declared span; any non-space
        # character disagreement remains a semantic contract failure.
        if 0 <= declared_start < declared_end <= len(transcript_window):
            declared_source = transcript_window[declared_start:declared_end]
            if evidence and "".join(evidence.split()) == "".join(declared_source.split()):
                event["evidence_text"] = declared_source
                count += 1
    return repaired, count


def _notify_stall(kind: str, detail: str, next_step: str) -> None:
    subprocess.run(
        ["codex-ops", "notify", "--source", "signal-desk-gold-authoring",
         "--summary", f"Signal Desk gold authoring stalled: {kind}",
         "--severity", "high", "--details", detail, "--next-step", next_step,
         "--dedupe-key", f"signal-desk-gold-stall:{kind}", "--telegram-mode", "prefer", "--json"],
        capture_output=True, text=True, timeout=30, check=False,
    )


def _load_outputs(root: Path, turn_type: str) -> dict[str, Mapping[str, Any]]:
    output_dir = root / turn_type
    result = {}
    if output_dir.exists():
        for path in output_dir.glob("*.json"):
            value = json.loads(path.read_text(encoding="utf-8"))
            result[str(value["window_id"])] = value
    return result


def _import_seed_outputs(
    seed_roots: Mapping[str, Path], result_root: Path,
    allowed_ids: Mapping[str, set[str]],
) -> dict[str, int]:
    counts = {}
    for turn_type, source_root in seed_roots.items():
        destination = result_root / turn_type
        destination.mkdir(parents=True, exist_ok=True)
        count = 0
        for source in sorted(source_root.glob("*.output.json")):
            value = json.loads(source.read_text(encoding="utf-8"))
            if str(value["window_id"]) not in allowed_ids[turn_type]:
                continue
            target = destination / f"{value['window_id']}.json"
            if not target.exists():
                shutil.copyfile(source, target)
            count += 1
        counts[turn_type] = count
    return counts


async def run_dev_gold(
    *, manifest_path: Path, project_root: Path, result_root: Path,
    dispatch_database: Path, budget_database: Path, grant_path: Path,
    session_root: Path, budget_dir: Path, seed_roots: Mapping[str, Path],
    concurrency: int = 4, binary: str = "codex",
) -> dict[str, Any]:
    if not 2 <= concurrency <= 8:
        raise ValueError("gold concurrency must be 2-8")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    verify_frozen_manifest(manifest)
    packets = build_gold_packets(
        manifest, project_root=project_root, gold_pass="A", splits=("development",)
    )
    by_window = {str(packet["input"]["window_id"]): packet for packet in packets}
    audit_ids = set(select_blind_gold_audit_windows(manifest)) & set(by_window)
    imported = _import_seed_outputs(
        seed_roots, result_root,
        {"A": set(by_window), "B": set(by_window), "C": set(by_window),
         "AUDIT": set(audit_ids)},
    )
    dispatch_database.parent.mkdir(parents=True, exist_ok=True)
    dispatch = sqlite3.connect(dispatch_database)
    dispatch.row_factory = sqlite3.Row
    initialize_dispatch_schema(dispatch)
    budget = sqlite3.connect(budget_database)
    budget.row_factory = sqlite3.Row
    phase_receipts = []
    run_started = time.monotonic()
    stop = asyncio.Event()

    try:
        for turn_type in ("A", "B", "C", "AUDIT"):
            target_ids = sorted(audit_ids if turn_type == "AUDIT" else by_window)
            existing = _load_outputs(result_root, turn_type)
            for window_id in set(existing) & set(target_ids):
                transcript_window = str(by_window[window_id]["input"]["window_text"])
                repaired, repair_count = repair_unique_evidence_offsets(
                    existing[window_id], transcript_window=transcript_window
                )
                validate_output(
                    repaired,
                    transcript_window=transcript_window,
                    expected_window_id=window_id,
                )
                if repair_count:
                    (result_root / turn_type / f"{window_id}.json").write_text(
                        json.dumps(repaired, indent=2, sort_keys=True) + "\n", encoding="utf-8"
                    )
                    existing[window_id] = repaired
            missing = [window_id for window_id in target_ids if window_id not in existing]
            for window_id in missing:
                packet = by_window[window_id]
                enqueue_task(
                    dispatch, task_key=f"dev:{turn_type}:{window_id}", task_type="gold_window",
                    payload={"window_id": window_id, "turn_type": turn_type,
                             "text_sha256": hashlib.sha256(packet["input"]["window_text"].encode()).hexdigest()},
                )
            phase_start = time.monotonic()
            completed_rows: list[dict[str, Any]] = []

            async def process_one(client: CodexAppServerClient, lease: Mapping[str, Any], worker_id: int) -> None:
                payload = lease["payload"]
                window_id = str(payload["window_id"])
                packet = by_window[window_id]
                reservation = reserve_gold_call(
                    budget, grant_path=grant_path, session_root=session_root,
                    budget_dir=budget_dir,
                    task_key=(
                        f"dev:{turn_type}:{window_id}:attempt:{lease['current_attempt_id']}:"
                        f"generation:{lease['lease_generation']}"
                    ),
                    turn_type=turn_type, reserve_tokens=RESERVE_TOKENS[turn_type],
                )
                if not reservation.get("allowed"):
                    release_attempt_for_retry(
                        dispatch, attempt_id=int(lease["current_attempt_id"]),
                        lease_owner=str(lease["lease_owner"]), lease_generation=int(lease["lease_generation"]),
                        failure_code=str(reservation.get("reason")), failure_detail="weekly health gate stalled",
                    )
                    stop.set()
                    _notify_stall(str(reservation.get("reason")), "Gold checkpoint is clean; no new call started.",
                                  "Restore/reset the weekly subscription window; the runner can resume from leases.")
                    return
                reservation_id = str(reservation["reservation_id"])
                mark_provider_started(budget, reservation_id)
                prompt = _prompt(packet)
                if turn_type == "C":
                    a = json.loads((result_root / "A" / f"{window_id}.json").read_text())
                    b = json.loads((result_root / "B" / f"{window_id}.json").read_text())
                    prompt += "\n\nGOLD A OUTPUT\n" + json.dumps(a, sort_keys=True)
                    prompt += "\n\nGOLD B OUTPUT\n" + json.dumps(b, sort_keys=True)
                output_path = result_root / turn_type / f"{window_id}.json"
                sidecar_path = result_root / "sidecars" / turn_type / f"{window_id}.json"
                started = time.monotonic()
                reservation_settled = False
                try:
                    result = await client.run_ephemeral_structured_turn(
                        model="gpt-5.6-sol", effort="medium",
                        base_instructions=SYSTEM_PROMPTS[turn_type], prompt=prompt,
                        output_schema=packet["output_schema"], cwd=project_root,
                        sidecar_path=sidecar_path, output_path=output_path,
                        timeout_seconds=900,
                    )
                    usage = int(result.usage.total_tokens) if result.usage else 0
                    settle_gold_call(budget, reservation_id=reservation_id,
                                     actual_tokens=usage, provider_calls=1)
                    reservation_settled = True
                    if not result.status_ok or result.output is None:
                        raise RuntimeError(result.error_class or result.status)
                    repaired, repair_count = repair_unique_evidence_offsets(
                        result.output,
                        transcript_window=str(packet["input"]["window_text"]),
                    )
                    validated = validate_output(
                        repaired, transcript_window=str(packet["input"]["window_text"]),
                        expected_window_id=window_id,
                    )
                    if repair_count:
                        output_path.write_text(
                            json.dumps(repaired, indent=2, sort_keys=True) + "\n", encoding="utf-8"
                        )
                except Exception as exc:  # noqa: BLE001
                    if not reservation_settled:
                        # The provider call started, but an exception denied us
                        # authoritative usage. Charge the full reservation so
                        # an infrastructure failure can never become unmetered.
                        settle_gold_call(
                            budget,
                            reservation_id=reservation_id,
                            actual_tokens=RESERVE_TOKENS[turn_type],
                            provider_calls=1,
                        )
                    if output_path.exists():
                        rejected = result_root / "rejected" / turn_type / output_path.name
                        rejected.parent.mkdir(parents=True, exist_ok=True)
                        output_path.replace(rejected)
                    detail = f"{type(exc).__name__}: {str(exc)[:300]}"
                    if "EvidenceContract" in type(exc).__name__:
                        fail_attempt_semantically(
                            dispatch, attempt_id=int(lease["current_attempt_id"]),
                            lease_owner=str(lease["lease_owner"]), lease_generation=int(lease["lease_generation"]),
                            failure_code="gold_contract_failure", failure_detail=detail,
                        )
                    else:
                        release_attempt_for_retry(
                            dispatch, attempt_id=int(lease["current_attempt_id"]),
                            lease_owner=str(lease["lease_owner"]), lease_generation=int(lease["lease_generation"]),
                            failure_code="gold_infrastructure_failure", failure_detail=detail,
                        )
                    stop.set()
                    _notify_stall(type(exc).__name__, detail,
                                  "Inspect the failed lease; resume uses the same semantic task lineage.")
                    return
                complete_attempt(
                    dispatch, attempt_id=int(lease["current_attempt_id"]),
                    lease_owner=str(lease["lease_owner"]), lease_generation=int(lease["lease_generation"]),
                    output={"window_id": window_id, "turn_type": turn_type,
                            "events": len(validated["events"]), "tokens": usage,
                            "deterministic_offset_repairs": repair_count,
                            "output_sha256": hashlib.sha256(output_path.read_bytes()).hexdigest()},
                )
                completed_rows.append({"window_id": window_id, "tokens": usage,
                                       "wall_seconds": time.monotonic() - started,
                                       "events": len(validated["events"]), "worker": worker_id})

            lock = asyncio.Lock()

            async def worker(worker_id: int) -> None:
                async with CodexAppServerClient(
                    command=[binary, "app-server", "--stdio", "--strict-config"],
                    expected_cli_version="0.147.0",
                ) as client:
                    while not stop.is_set():
                        async with lock:
                            lease = acquire_lease(
                                dispatch, lease_owner=f"dev-gold-{turn_type}-{worker_id}",
                                lease_seconds=1200,
                            )
                        if lease is None:
                            return
                        await process_one(client, lease, worker_id)

            await asyncio.gather(*(worker(i) for i in range(concurrency)))
            if stop.is_set():
                raise RuntimeError(f"dev gold stopped during {turn_type}; checkpoint preserved")
            final_outputs = _load_outputs(result_root, turn_type)
            if any(window_id not in final_outputs for window_id in target_ids):
                raise RuntimeError(f"dev gold phase {turn_type} is incomplete")
            phase_receipts.append({
                "turn_type": turn_type, "target_windows": len(target_ids),
                "reused_outputs": len(target_ids) - len(missing),
                "new_outputs": len(completed_rows),
                "new_tokens": sum(row["tokens"] for row in completed_rows),
                "wall_seconds": time.monotonic() - phase_start,
                "mean_call_wall_seconds": (
                    sum(row["wall_seconds"] for row in completed_rows) / len(completed_rows)
                    if completed_rows else 0.0
                ),
            })

        receipt = {
            "schema_version": "pif_signal_desk_dev_gold_run_v1", "created_at": now_iso(),
            "complete": True, "manifest_sha256": manifest["manifest_sha256"],
            "development_windows": len(by_window), "development_audit_windows": len(audit_ids),
            "concurrency": concurrency, "imported_seed_outputs": imported,
            "phases": phase_receipts, "wall_seconds": time.monotonic() - run_started,
        }
        receipt["receipt_sha256"] = hashlib.sha256(
            json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        (result_root / "dev-gold-receipt.json").write_text(
            json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        return receipt
    finally:
        dispatch.close(); budget.close()
