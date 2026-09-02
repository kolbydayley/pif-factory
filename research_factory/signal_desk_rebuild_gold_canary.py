"""Bounded semantic GPT-5.6-sol gold-authoring budget and validity canary."""

from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any, Mapping

from .codex_app_server import CodexAppServerClient
from .signal_desk_rebuild_contracts import event_schema, validate_output
from .signal_desk_rebuild_gold import build_gold_packets, verify_frozen_manifest
from .subscription_budget import record_usage, subscription_budget_window
from .util import now_iso


SYSTEM_PROMPT = """You are the independent Gold A author for a private podcast intelligence benchmark.
Extract every consequential factual claim, forecast, explanation, recommendation, commitment,
disagreement, or explicitly reported position that is grounded in the supplied transcript window.
Do not extract greetings, questions, jokes, ads, navigation, boilerplate, or vague conversational filler.
Chrome, subscription appeals, sponsor copy, navigation, and episode-listing language may appear inside
the source window; never use any of it as evidence. If that is all the window contains, return
no_consequential_claims rather than manufacturing an event from it.
Each event must be one atomic proposition. evidence_text must be an exact contiguous substring of
the window and evidence_start/evidence_end must be exact zero-based character offsets. Separate the
actual speaker, quoted person, and mentioned people. Never infer identity from show metadata. If the
speaker is not textually supported, use unresolved_speaker and speaker_id null. Preserve
transcript-surface proper-name spellings, especially for ASR. Use candidate, uncertain, or quarantined only;
never self-approve accepted evidence.

Field coupling is mandatory:
- direct_speech and reported_paraphrase require the actual speaker in speaker_id.
- quoted_speech requires both the person uttering the quote in speaker_id and the person being quoted
  in quoted_person_id. If the quoted person is not textually identifiable, do not use quoted_speech.
- third_party_mention requires at least one textually named person in mentioned_person_ids.
- unresolved_speaker requires speaker_id null and must never guess a name.
- A person merely discussed is not the speaker and must not be put in speaker_id.
Return only the required JSON schema."""

GOLD_BUDGET_LANE = "gpt_5_6_sol_gold_authoring"


def record_canary_usage(receipt: Mapping[str, Any], database: Path) -> dict[str, Any]:
    """Idempotently attribute a completed canary to the ordinary governor."""

    if not receipt.get("passed") or int(receipt.get("calls") or 0) < 1:
        raise RuntimeError("only a passed, nonempty canary receipt may be metered")
    run_id = f"semantic-gold-canary:{receipt['receipt_sha256']}"
    day, window_start = subscription_budget_window(str(receipt["created_at"]))
    conn = sqlite3.connect(database)
    try:
        # Schema may not exist yet on a new test or campaign database.
        from .subscription_budget import ensure_budget_schema
        ensure_budget_schema(conn)
        existing = conn.execute(
            """SELECT id, tokens, provider_calls FROM pif_subscription_budget_ledger
               WHERE run_id=? AND lane=?""",
            (run_id, GOLD_BUDGET_LANE),
        ).fetchone()
        if existing:
            row_id, tokens, calls = existing
            status = "already_recorded"
        else:
            row_id = record_usage(
                conn,
                day=day,
                provider_lane="codex_subscription",
                lane=GOLD_BUDGET_LANE,
                run_id=run_id,
                tokens=int(receipt["total_tokens"]),
                provider_calls=int(receipt["calls"]),
            )
            conn.commit()
            tokens, calls, status = int(receipt["total_tokens"]), int(receipt["calls"]), "recorded"
        return {
            "status": status,
            "ledger_row_id": row_id,
            "day": day,
            "window_start": window_start,
            "lane": GOLD_BUDGET_LANE,
            "model": receipt["model"],
            "tokens": int(tokens),
            "calls": int(calls),
            "gpt_5_5_rebuild_grant_used": False,
        }
    finally:
        conn.close()


def _select(packets: tuple[Mapping[str, Any], ...], count: int = 10) -> list[Mapping[str, Any]]:
    by_structure: dict[str, list[Mapping[str, Any]]] = {}
    for packet in packets:
        by_structure.setdefault(str(packet["input"]["transcript_structure"]), []).append(packet)
    selected: list[Mapping[str, Any]] = []
    while len(selected) < count:
        progressed = False
        for structure in sorted(by_structure):
            if by_structure[structure] and len(selected) < count:
                selected.append(by_structure[structure].pop(0))
                progressed = True
        if not progressed:
            break
    if len(selected) != count:
        raise RuntimeError("could not select ten development gold canary windows")
    return selected


def _prompt(packet: Mapping[str, Any]) -> str:
    value = packet["input"]
    return (
        f"window_id: {value['window_id']}\n"
        f"transcript_structure: {value['transcript_structure']}\n"
        f"attribution_rule: {value['attribution_instruction']}\n"
        f"source_quality_rule: {value['source_quality_instruction']}\n\n"
        "TRANSCRIPT WINDOW START\n"
        f"{value['window_text']}\n"
        "TRANSCRIPT WINDOW END"
    )


async def run_gold_canary(
    *,
    manifest_path: Path,
    project_root: Path,
    output_root: Path,
    binary: str = "codex",
) -> dict[str, Any]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    verify_frozen_manifest(manifest)
    packets = build_gold_packets(
        manifest,
        project_root=project_root,
        gold_pass="A",
        splits=("development",),
    )
    selected = _select(packets)
    output_root.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    queue: asyncio.Queue[tuple[int, Mapping[str, Any]]] = asyncio.Queue()
    for index, packet in enumerate(selected):
        queue.put_nowait((index, packet))

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

    async def process_one(client: CodexAppServerClient, index: int, packet: Mapping[str, Any]) -> None:
            value = packet["input"]
            window_id = str(value["window_id"])
            result = await client.run_ephemeral_structured_turn(
                model="gpt-5.6-sol",
                effort="medium",
                base_instructions=SYSTEM_PROMPT,
                prompt=_prompt(packet),
                output_schema=event_schema(),
                cwd=project_root,
                sidecar_path=output_root / "private-development" / f"{index:02d}.sidecar.json",
                output_path=output_root / "private-development" / f"{index:02d}.output.json",
                timeout_seconds=600,
            )
            if not result.status_ok or result.output is None:
                raise RuntimeError(f"gold canary failed: {result.error_class or result.status}")
            validated = validate_output(
                result.output,
                transcript_window=str(value["window_text"]),
                expected_window_id=window_id,
            )
            if result.usage is None:
                raise RuntimeError("gold canary did not return token accounting")
            rows.append({
                "window_id_sha256": hashlib.sha256(window_id.encode()).hexdigest(),
                "transcript_structure": value["transcript_structure"],
                "events": len(validated["events"]),
                "disposition": validated["window_disposition"],
                "input_tokens": result.usage.input_tokens,
                "output_tokens": result.usage.output_tokens,
                "reasoning_output_tokens": result.usage.reasoning_output_tokens,
                "total_tokens": result.usage.total_tokens,
                "schema_valid": True,
            })
    await asyncio.gather(*(worker() for _ in range(4)))
    rows.sort(key=lambda row: str(row["window_id_sha256"]))
    total = sum(int(row["total_tokens"]) for row in rows)
    receipt: dict[str, Any] = {
        "schema_version": "pif_signal_desk_rebuild_semantic_gold_canary_v1",
        "created_at": now_iso(),
        "passed": len(rows) == 10 and all(row["schema_valid"] for row in rows),
        "model": "gpt-5.6-sol",
        "reasoning_effort": "medium",
        "manifest_sha256": manifest["manifest_sha256"],
        "system_prompt_sha256": hashlib.sha256(SYSTEM_PROMPT.encode()).hexdigest(),
        "calls": len(rows),
        "total_tokens": total,
        "mean_tokens_per_call": total / len(rows),
        "maximum_tokens_per_call": max(int(row["total_tokens"]) for row in rows),
        "rows": rows,
        "privacy": "hashed_window_ids_counts_usage_and_dispositions_no_transcript_or_event_text",
    }
    body = json.dumps(receipt, sort_keys=True, separators=(",", ":"))
    receipt["receipt_sha256"] = hashlib.sha256(body.encode()).hexdigest()
    (output_root / "receipt.json").write_text(
        json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return receipt
