"""J2/J3 turn-type cost measurement for Signal Desk gold authoring."""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import sqlite3
from pathlib import Path
from typing import Any, Mapping

from .codex_app_server import CodexAppServerClient
from .signal_desk_gold_budget import (
    mark_provider_started,
    reserve_gold_call,
    settle_gold_call,
)
from .signal_desk_rebuild_contracts import event_schema, validate_output
from .signal_desk_rebuild_gold import build_gold_packets, verify_frozen_manifest
from .signal_desk_rebuild_gold_canary import SYSTEM_PROMPT as A_SYSTEM_PROMPT, _prompt, _select
from .util import now_iso


SCHEMA_VERSION = "pif_signal_desk_gold_j2_j3_measurement_v1"
TURN_TYPES = ("A", "B", "C", "AUDIT")
RESERVE_TOKENS = {"B": 45_000, "C": 75_000, "AUDIT": 45_000}

B_SYSTEM_PROMPT = A_SYSTEM_PROMPT.replace(
    "independent Gold A author", "independent Gold B author"
) + "\nYou are independent of Gold A and must not assume or imitate another author."

AUDIT_SYSTEM_PROMPT = A_SYSTEM_PROMPT.replace(
    "independent Gold A author", "blind gold-reliability auditor"
) + "\nAuthor the window independently. Do not defer to Gold A, B, or C."

C_SYSTEM_PROMPT = A_SYSTEM_PROMPT.replace(
    "independent Gold A author", "Gold C adjudicator"
) + """
You will receive independent Gold A and Gold B outputs after the transcript.
Resolve their disagreements against transcript evidence. Preserve supported events one-to-one,
repair contract errors, split merged propositions, and omit unsupported claims. Do not vote,
average, or invent a compromise. The transcript remains the only semantic authority."""


def _p90(values: list[int]) -> float:
    ordered = sorted(values)
    position = 0.90 * (len(ordered) - 1)
    left, right = math.floor(position), math.ceil(position)
    if left == right:
        return float(ordered[left])
    return ordered[left] + (ordered[right] - ordered[left]) * (position - left)


def _turn_metrics(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    total = [int(row["total_tokens"]) for row in rows]
    input_values = [int(row["input_tokens"]) for row in rows]
    output_values = [int(row["output_tokens"]) for row in rows]
    reasoning = [int(row["reasoning_output_tokens"]) for row in rows]
    maximum = max(rows, key=lambda row: int(row["total_tokens"]))
    return {
        "calls": len(rows),
        "total_tokens": sum(total),
        "mean_tokens": sum(total) / len(total),
        "p90_tokens": _p90(total),
        "maximum_tokens": max(total),
        "input_mean": sum(input_values) / len(input_values),
        "input_p90": _p90(input_values),
        "output_mean": sum(output_values) / len(output_values),
        "output_p90": _p90(output_values),
        "reasoning_mean": sum(reasoning) / len(reasoning),
        "reasoning_p90": _p90(reasoning),
        "outlier": {
            "window_id_sha256": maximum["window_id_sha256"],
            "transcript_structure": maximum["transcript_structure"],
            "disposition": maximum["disposition"],
            "events": maximum["events"],
            "input_tokens": maximum["input_tokens"],
            "output_tokens": maximum["output_tokens"],
            "reasoning_output_tokens": maximum["reasoning_output_tokens"],
            "input_share": int(maximum["input_tokens"]) / int(maximum["total_tokens"]),
            "output_share": int(maximum["output_tokens"]) / int(maximum["total_tokens"]),
            "reasoning_share": int(maximum["reasoning_output_tokens"]) / int(maximum["total_tokens"]),
        },
    }


def _load_existing_a(root: Path) -> tuple[list[dict[str, Any]], dict[str, Mapping[str, Any]]]:
    receipt = json.loads((root / "receipt.json").read_text(encoding="utf-8"))
    outputs: dict[str, Mapping[str, Any]] = {}
    for path in sorted((root / "private-development").glob("*.output.json")):
        output = json.loads(path.read_text(encoding="utf-8"))
        outputs[str(output["window_id"])] = output
    if len(outputs) != 10 or len(receipt["rows"]) != 10:
        raise RuntimeError("J2/J3 requires the complete ten-window Gold A canary")
    return [dict(row) for row in receipt["rows"]], outputs


async def run_measurement(
    *, manifest_path: Path, project_root: Path, output_root: Path,
    existing_a_root: Path, grant_path: Path, budget_database: Path,
    budget_dir: Path, session_root: Path, concurrency: int = 4,
    binary: str = "codex",
) -> dict[str, Any]:
    if not 2 <= concurrency <= 8:
        raise ValueError("measurement concurrency must be 2-8")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    verify_frozen_manifest(manifest)
    packets = _select(build_gold_packets(
        manifest, project_root=project_root, gold_pass="A", splits=("development",)
    ))
    a_rows, a_outputs = _load_existing_a(existing_a_root)
    output_root.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(budget_database)
    conn.row_factory = sqlite3.Row
    all_rows: dict[str, list[dict[str, Any]]] = {"A": a_rows}
    prior_outputs = a_outputs

    try:
        for turn_type, system_prompt in (
            ("B", B_SYSTEM_PROMPT), ("C", C_SYSTEM_PROMPT), ("AUDIT", AUDIT_SYSTEM_PROMPT)
        ):
            queue: asyncio.Queue[tuple[int, Mapping[str, Any]]] = asyncio.Queue()
            for index, packet in enumerate(packets):
                queue.put_nowait((index, packet))
            rows: list[dict[str, Any]] = []
            outputs: dict[str, Mapping[str, Any]] = {}

            async def process_one(client: CodexAppServerClient, index: int, packet: Mapping[str, Any]) -> None:
                value = packet["input"]
                window_id = str(value["window_id"])
                task_key = f"j2j3:{turn_type}:{window_id}"
                reservation = reserve_gold_call(
                    conn, grant_path=grant_path, session_root=session_root,
                    budget_dir=budget_dir, task_key=task_key, turn_type=turn_type,
                    reserve_tokens=RESERVE_TOKENS[turn_type],
                )
                if not reservation.get("allowed"):
                    raise RuntimeError(f"gold measurement stalled: {reservation.get('reason')}")
                reservation_id = str(reservation["reservation_id"])
                mark_provider_started(conn, reservation_id)
                prompt = _prompt(packet)
                if turn_type == "C":
                    prompt += (
                        "\n\nGOLD A OUTPUT\n" + json.dumps(a_outputs[window_id], sort_keys=True)
                        + "\n\nGOLD B OUTPUT\n" + json.dumps(prior_outputs[window_id], sort_keys=True)
                    )
                result = await client.run_ephemeral_structured_turn(
                    model="gpt-5.6-sol", effort="medium", base_instructions=system_prompt,
                    prompt=prompt, output_schema=event_schema(), cwd=project_root,
                    sidecar_path=output_root / f"private-{turn_type.lower()}" / f"{index:02d}.sidecar.json",
                    output_path=output_root / f"private-{turn_type.lower()}" / f"{index:02d}.output.json",
                    timeout_seconds=900,
                )
                if not result.status_ok or result.output is None or result.usage is None:
                    raise RuntimeError(f"{turn_type} measurement failed: {result.error_class or result.status}")
                validated = validate_output(
                    result.output, transcript_window=str(value["window_text"]), expected_window_id=window_id
                )
                settle_gold_call(conn, reservation_id=reservation_id, actual_tokens=result.usage.total_tokens)
                outputs[window_id] = validated
                rows.append({
                    "window_id_sha256": hashlib.sha256(window_id.encode()).hexdigest(),
                    "transcript_structure": value["transcript_structure"],
                    "disposition": validated["window_disposition"], "events": len(validated["events"]),
                    "input_tokens": result.usage.input_tokens,
                    "output_tokens": result.usage.output_tokens,
                    "reasoning_output_tokens": result.usage.reasoning_output_tokens,
                    "total_tokens": result.usage.total_tokens,
                })

            async def worker() -> None:
                async with CodexAppServerClient(
                    command=[binary, "app-server", "--stdio", "--strict-config"],
                    expected_cli_version="0.147.0",
                ) as client:
                    while not queue.empty():
                        index, packet = await queue.get()
                        try:
                            await process_one(client, index, packet)
                        finally:
                            queue.task_done()

            await asyncio.gather(*(worker() for _ in range(concurrency)))
            rows.sort(key=lambda row: str(row["window_id_sha256"]))
            all_rows[turn_type] = rows
            if turn_type == "B":
                prior_outputs = outputs

        metrics = {turn: _turn_metrics(all_rows[turn]) for turn in TURN_TYPES}
        counts = {"A": 804, "B": 804, "C": 804, "AUDIT": 81}
        projected = sum(metrics[turn]["mean_tokens"] * counts[turn] for turn in TURN_TYPES)
        receipt: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION, "created_at": now_iso(), "passed": True,
            "manifest_sha256": manifest["manifest_sha256"], "sample_windows": 10,
            "concurrency": concurrency, "turn_metrics": metrics,
            "program_turn_counts": counts, "revised_total_program_tokens": round(projected),
            "normal_5m_full_days_at_mean": projected / 5_000_000,
            "under_seven_normal_5m_days": projected <= 35_000_000,
            "grant_activated_despite_normal_estimate": True,
            "privacy": "hashed_window_ids_metrics_and_outlier_shapes_no_transcript_or_event_text",
        }
        receipt["receipt_sha256"] = hashlib.sha256(
            json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        (output_root / "receipt.json").write_text(
            json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        return receipt
    finally:
        conn.close()
