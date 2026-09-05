"""Bounded GPT-5.5 wider-context lane for unresolved Gold-C speakers.

Planning is deterministic and provider-free. Execution is explicit, leased,
metered, and accepts only speaker-surface decisions; it cannot rewrite claims.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from .signal_desk_rebuild_approval import (
    ApprovalBudgetConfig, MAX_APPROVAL_PACKET_CANDIDATES,
    MAX_APPROVAL_PACKET_INPUT_TOKENS, conservative_input_token_upper_bound,
    plan_approval_packets,
)
from .signal_desk_rebuild_budget import rebuild_budget_gate
from .signal_desk_rebuild_dispatch import enqueue_task, initialize_dispatch_schema
from .subscription_budget import record_usage, subscription_budget_window
from .util import now_iso, sha256_text


SCHEMA_VERSION = "pif_signal_desk_gold_wider_speaker_v1"
MODEL = "gpt-5.5"
EFFORT = "high"
CAMPAIGN_ID = "signal-desk-clean-corpus-2026-08-31"
TASK_PREFIX = "gold-speaker-wide-v1:"
RESERVE_TOKENS_PER_CALL = 50_000
EXPECTED_CALLS = 9
MAX_CONCURRENCY = 2
DEFAULT_CONCURRENCY = 1
CALL_DEADLINE_SECONDS = 900
LEASE_SECONDS = 1_800
GENERIC_SPEAKERS = frozenset({
    "army film clip", "archival tape", "clip", "speaker 1", "speaker 2",
    "unknown speaker", "narrator", "advertisement", "sponsor",
})

SYSTEM_PROMPT = """You are GPT-5.5, the final speaker-attribution authority for a private Gold benchmark. The packet already contains bounded wider context. For every event ID, return either an actual speaker whose exact surface surface occurs in context with exact provenance offsets, or indeterminable. Never identify a mentioned, quoted, reported, hypothetical, archival, or generic clip source as the actual speaker. Never use outside knowledge, show-title inference, turn alternation, or writing style. Do not rewrite claims, issues, stance, evidence, or entities. If more context would be needed, return request_wider_context; because this is already the single allowed wide attempt, the harness will fail closed. Attest model exactly gpt-5.5."""

OUTPUT_SCHEMA = {
    "type": "object", "additionalProperties": False,
    "required": ["model", "window_id", "packet_sha256", "action", "decisions", "rationale"],
    "properties": {
        "model": {"type": "string", "const": MODEL},
        "window_id": {"type": "string"},
        "packet_sha256": {"type": "string"},
        "action": {"type": "string", "enum": ["accept", "request_wider_context", "fail_closed"]},
        "decisions": {"type": "array", "items": {
            "type": "object", "additionalProperties": False,
            "required": ["event_id", "decision", "speaker_surface", "provenance_start", "provenance_end"],
            "properties": {
                "event_id": {"type": "string"},
                "decision": {"type": "string", "enum": ["supported_speaker", "indeterminable"]},
                "speaker_surface": {"type": ["string", "null"]},
                "provenance_start": {"type": ["integer", "null"]},
                "provenance_end": {"type": ["integer", "null"]},
            },
        }},
        "rationale": {"type": "string", "minLength": 1},
    },
}


class WiderSpeakerError(RuntimeError):
    pass


def _canonical(value: Any) -> str:
    return " ".join(str(value or "").casefold().replace("’", "'").split())


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha(value: Any) -> str:
    return hashlib.sha256(_json(value).encode()).hexdigest()


def initialize_reservation_schema(connection: sqlite3.Connection) -> None:
    connection.execute(
        """CREATE TABLE IF NOT EXISTS signal_desk_wider_speaker_reservations (
        reservation_id TEXT PRIMARY KEY, task_key TEXT NOT NULL UNIQUE,
        reserved_tokens INTEGER NOT NULL, actual_tokens INTEGER,
        status TEXT NOT NULL CHECK(status IN ('reserved','settled','unknown_charged','released')),
        ledger_id TEXT, created_at TEXT NOT NULL, settled_at TEXT)"""
    )
    connection.commit()


def reserve_call(
    connection: sqlite3.Connection, *, task_key: str,
    budget: ApprovalBudgetConfig, at: datetime | None = None,
) -> dict[str, Any]:
    """Reserve one call under the campaign grant before provider dispatch."""

    initialize_reservation_schema(connection)
    existing = connection.execute(
        "SELECT reservation_id, reserved_tokens, status FROM signal_desk_wider_speaker_reservations WHERE task_key=?",
        (task_key,),
    ).fetchone()
    if existing:
        return {"reservation_id": existing[0], "reserved_tokens": existing[1], "status": existing[2], "existing": True}
    instant = at or datetime.now(timezone.utc)
    day, start = subscription_budget_window(instant)
    gate = dict(rebuild_budget_gate(
        connection, day=day, campaign_id=budget.campaign_id,
        grant_path=budget.grant_path, budget_dir=budget.budget_dir, at=instant,
        clean_release_exists=budget.clean_release_exists,
        additional_budget_db_paths=budget.additional_budget_db_paths,
        window_start_iso=budget.window_start_iso or start,
    ))
    pending = int(connection.execute(
        "SELECT COALESCE(SUM(reserved_tokens),0) FROM signal_desk_wider_speaker_reservations WHERE status='reserved'"
    ).fetchone()[0])
    if not gate.get("allowed") or pending + RESERVE_TOKENS_PER_CALL > int(gate.get("remaining_tokens") or 0):
        raise WiderSpeakerError(f"approval budget reservation denied: {gate.get('reason') or 'insufficient headroom'}")
    reservation_id = sha256_text(f"{task_key}\0{now_iso()}")
    connection.execute(
        "INSERT INTO signal_desk_wider_speaker_reservations VALUES (?,?,?,NULL,'reserved',NULL,?,NULL)",
        (reservation_id, task_key, RESERVE_TOKENS_PER_CALL, now_iso()),
    )
    connection.commit()
    return {"reservation_id": reservation_id, "reserved_tokens": RESERVE_TOKENS_PER_CALL,
            "status": "reserved", "existing": False, "budget_receipt_sha256": _sha(gate)}


def settle_call(
    connection: sqlite3.Connection, *, reservation_id: str, task_key: str,
    actual_tokens: int | None, at: datetime | None = None,
) -> dict[str, Any]:
    initialize_reservation_schema(connection)
    row = connection.execute(
        "SELECT reserved_tokens,status FROM signal_desk_wider_speaker_reservations WHERE reservation_id=? AND task_key=?",
        (reservation_id, task_key),
    ).fetchone()
    if not row or row[1] != "reserved":
        raise WiderSpeakerError("reservation is missing or not active")
    charged = int(actual_tokens) if actual_tokens and int(actual_tokens) > 0 else int(row[0])
    instant = at or datetime.now(timezone.utc); day, _ = subscription_budget_window(instant)
    ledger_id = record_usage(
        connection, day=day, provider_lane="openai-codex/gpt-5.5",
        lane="signal_desk_rebuild_approval", run_id=task_key,
        tokens=charged, provider_calls=1,
    )
    status = "settled" if actual_tokens and int(actual_tokens) > 0 else "unknown_charged"
    connection.execute(
        "UPDATE signal_desk_wider_speaker_reservations SET actual_tokens=?,status=?,ledger_id=?,settled_at=? WHERE reservation_id=?",
        (charged, status, ledger_id, now_iso(), reservation_id),
    )
    connection.commit()
    return {"charged_tokens": charged, "status": status, "ledger_id": ledger_id}


def _event_descriptor(event: Mapping[str, Any], *, context_start: int, window_start: int) -> dict[str, Any]:
    return {
        # Compact keys are part of the frozen lane contract: 31-event windows
        # must fit beside the complete 6k source window under the conservative
        # 12k-byte upper bound.
        "id": str(event["event_id"]),
        "s": window_start - context_start + int(event["evidence_start"]),
        "e": window_start - context_start + int(event["evidence_end"]),
        "quoted": event.get("quoted_person_id"),
        "mentioned": list(event.get("mentioned_person_ids") or []),
    }


def build_private_packets(
    *, v5_root: Path, manifest_path: Path, baseline_root: Path,
    project_root: Path, private_root: Path,
) -> dict[str, Any]:
    """Freeze nine private packets and return a content-free plan receipt."""

    wider = json.loads((v5_root / "wider-context-plan.json").read_text())
    if wider.get("expected_calls") != EXPECTED_CALLS or wider.get("provider_calls_started") is not False:
        raise WiderSpeakerError("V5 wider-context plan is not the frozen nine-call input")
    manifest = json.loads(manifest_path.read_text()); metadata = {str(row["window_id"]): row for row in manifest["windows"]}
    private_root.mkdir(parents=True, exist_ok=True); private_root.chmod(0o700)
    packet_rows = []
    for planned in wider["windows"]:
        window_id = str(planned["window_id"]); row = metadata[window_id]
        transcript_path = Path(str(row["transcript_path"])); transcript_path = transcript_path if transcript_path.is_absolute() else project_root / transcript_path
        transcript = transcript_path.read_text(encoding="utf-8")
        unresolved = json.loads((v5_root / "provenance" / f"{window_id}.json").read_text())["unresolved"]
        baseline = json.loads((baseline_root / "C" / f"{window_id}.json").read_text())
        by_id = {str(event["event_id"]): event for event in baseline["events"]}
        # Start with the complete frozen window. If packet overhead breaches the
        # byte-conservative limit, trim symmetric outer context, never the window.
        window_start, window_end = int(row["start_char"]), int(row["end_char"])
        padding = 1800
        while True:
            context_start=max(0,window_start-padding); context_end=min(len(transcript),window_end+padding)
            context_text=transcript[context_start:context_end]
            candidate={
                "window_id":window_id,
                "events":[_event_descriptor(by_id[str(item["event_id"])],context_start=context_start,window_start=window_start) for item in unresolved],
            }
            item={"semantic_sample_id":f"speaker-wide:{window_id}","candidate":candidate,
                  "context":{"context_scope":"wide","context_text":context_text}}
            try:
                approval_packet=plan_approval_packets([item])[0]
                transport_upper_bound = conservative_input_token_upper_bound({
                    "system_prompt": SYSTEM_PROMPT,
                    "candidate": candidate,
                    "context": item["context"],
                })
                if transport_upper_bound > MAX_APPROVAL_PACKET_INPUT_TOKENS:
                    raise WiderSpeakerError("transport input exceeds 12,000-token upper bound")
                break
            except Exception:
                if padding == 0: raise
                padding=max(0,padding-300)
        packet={"schema_version":SCHEMA_VERSION,"model":MODEL,"system_prompt":SYSTEM_PROMPT,
                "candidate":candidate,"context":{"context_scope":"wide","context_text":context_text},
                "approval_packet_sha256":approval_packet["packet_sha256"],
                "input_token_upper_bound":transport_upper_bound}
        packet["packet_sha256"]=_sha(packet)
        path=private_root/f"{window_id}.packet.json"
        if path.exists() and _sha(json.loads(path.read_text())) != _sha(packet):
            raise WiderSpeakerError(f"immutable packet conflict: {window_id}")
        if not path.exists(): path.write_text(json.dumps(packet,indent=2,sort_keys=True)+"\n"); path.chmod(0o600)
        packet_rows.append({"window_id":window_id,"packet_sha256":packet["packet_sha256"],
                            "input_token_upper_bound":packet["input_token_upper_bound"],
                            "event_count":len(candidate["events"])})
    receipt={"schema_version":"pif_signal_desk_gold_wider_speaker_plan_v1","model":MODEL,
             "provider_calls_started":False,"expected_calls":len(packet_rows),
             "maximum_concurrency":MAX_CONCURRENCY,"default_concurrency":DEFAULT_CONCURRENCY,
             "reserve_tokens_per_call":RESERVE_TOKENS_PER_CALL,
             "reserved_token_ceiling":len(packet_rows)*RESERVE_TOKENS_PER_CALL,
             "deadline_seconds":CALL_DEADLINE_SECONDS,"lease_seconds":LEASE_SECONDS,
             "health_guards":{"model_fallback_forbidden":True,"max_consecutive_infrastructure_failures":3,
                              "exponential_backoff_seconds":[30,60,120],"maximum_backoff_seconds":300,
                              "semantic_contract_failure":"terminal_until_explicit_resurrection"},
             "packet_limits":{"candidate_max":MAX_APPROVAL_PACKET_CANDIDATES,
                              "input_token_upper_bound":MAX_APPROVAL_PACKET_INPUT_TOKENS},
             "packets":packet_rows,"packets_digest":_sha(packet_rows),
             "receipt_contains_transcript_or_claim_text":False}
    receipt["receipt_sha256"]=_sha(receipt)
    return receipt


def validate_decision(output: Mapping[str, Any], packet: Mapping[str, Any]) -> dict[str, Any]:
    """Validate exact provenance and role separation; fail closed on any drift."""
    if (
        output.get("model") != MODEL
        or output.get("window_id") != packet["candidate"]["window_id"]
        or output.get("packet_sha256") != packet["packet_sha256"]
    ):
        raise WiderSpeakerError("model, window, or packet attestation mismatch")
    action=str(output.get("action") or "")
    if action == "request_wider_context":
        raise WiderSpeakerError("second wider-context request denied; fail closed")
    if action == "fail_closed":
        return {"window_id":output["window_id"],"packet_sha256":packet["packet_sha256"],"action":"fail_closed","decisions":[],"rationale":str(output.get("rationale") or "")}
    if action != "accept": raise WiderSpeakerError("unsupported approval action")
    expected={str(row["id"]):row for row in packet["candidate"]["events"]}
    decisions=output.get("decisions")
    if not isinstance(decisions,list) or {str(row.get("event_id")) for row in decisions} != set(expected) or len(decisions)!=len(expected):
        raise WiderSpeakerError("decision IDs must exactly partition unresolved events")
    context=str(packet["context"]["context_text"]); validated=[]
    for row in decisions:
        event_id=str(row["event_id"]); decision=str(row.get("decision") or "")
        if decision == "indeterminable":
            if any(row.get(key) is not None for key in ("speaker_surface","provenance_start","provenance_end")):
                raise WiderSpeakerError("indeterminable decision carries speaker provenance")
            validated.append(dict(row)); continue
        if decision != "supported_speaker": raise WiderSpeakerError("unknown speaker decision")
        speaker=" ".join(str(row.get("speaker_surface") or "").split()); start=row.get("provenance_start"); end=row.get("provenance_end")
        if not speaker or _canonical(speaker) in GENERIC_SPEAKERS or not isinstance(start,int) or not isinstance(end,int) or start<0 or end<=start or context[start:end]!=speaker:
            raise WiderSpeakerError("speaker provenance is not an exact supported surface")
        references={_canonical(expected[event_id].get("quoted")),*(_canonical(v) for v in expected[event_id].get("mentioned") or [])}
        if _canonical(speaker) in references: raise WiderSpeakerError("quoted or mentioned person cannot become actual speaker")
        validated.append(dict(row))
    return {"window_id":output["window_id"],"packet_sha256":packet["packet_sha256"],"action":"accept","decisions":validated,
            "rationale":str(output.get("rationale") or ""),"packet_sha256":packet["packet_sha256"]}


def enqueue_packets(*, dispatch_path: Path, plan: Mapping[str, Any], private_root: Path) -> dict[str, int]:
    connection=sqlite3.connect(dispatch_path);connection.row_factory=sqlite3.Row
    try:
        initialize_dispatch_schema(connection);counts=Counter()
        for row in plan["packets"]:
            window_id=str(row["window_id"]); packet_path=private_root/f"{window_id}.packet.json"
            result=enqueue_task(connection,task_key=TASK_PREFIX+window_id,task_type="gpt55_wider_speaker",
                                payload={"window_id":window_id,"packet_path":str(packet_path),"packet_sha256":row["packet_sha256"]})
            counts[result["enqueue_outcome"]]+=1
        return dict(counts)
    finally: connection.close()
