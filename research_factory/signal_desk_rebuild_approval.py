"""Persisted GPT-5.5 approval runner for the Signal Desk clean-corpus rebuild.

The runner has no provider implementation of its own.  A caller must inject the
single GPT-5.5 transport, which makes provider choice explicit and keeps tests
offline.  Every provider call is checked against the campaign-scoped rebuild
budget immediately before dispatch and appended to the subscription ledger
afterwards.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from .signal_desk_rebuild_budget import rebuild_budget_gate
from .signal_desk_rebuild_quality import (
    APPROVED_ACTIONS,
    ApprovalAction,
    ApprovalState,
    ContextScope,
    QualityContractError,
    record_approval_attempt,
    wider_context_packet,
)
from .subscription_budget import (
    record_usage,
    subscription_budget_window,
    usage_tokens_from_item,
)
from .util import dumps_json, now_iso, sha256_text


SCHEMA_VERSION = "pif_signal_desk_rebuild_approval_v1"
EXPECTED_MODEL = "gpt-5.5"
PROVIDER_LANE = "openai-codex/gpt-5.5"
BUDGET_LANE = "signal_desk_rebuild_approval"


class ApprovalExecutionError(RuntimeError):
    """Base class for fail-closed approval execution failures."""


class ApprovalConflictError(ApprovalExecutionError):
    """A semantic sample id was reused for different immutable input."""


class ApprovalBudgetDenied(ApprovalExecutionError):
    """The rebuild governor refused a model dispatch."""


class ApprovalResponseError(ApprovalExecutionError):
    """GPT-5.5 returned a response outside the frozen approval contract."""


@dataclass(frozen=True)
class ApprovalBudgetConfig:
    campaign_id: str
    grant_path: Path
    budget_dir: Path
    additional_budget_db_paths: tuple[Path, ...] = ()
    window_start_iso: str | None = None
    clean_release_exists: bool = False


@dataclass(frozen=True)
class ApprovalResult:
    semantic_sample_id: str
    final_action: ApprovalAction
    rationale: str
    publishable: bool
    disposition: str
    approval_attempt_count: int
    provider_call_count: int
    wider_context_retries: int
    output: object | None


ModelCall = Callable[[Mapping[str, Any]], Mapping[str, Any]]
BudgetGate = Callable[..., Mapping[str, Any]]


def _canonical(value: Any) -> str:
    return dumps_json(value)


def _digest(value: Any) -> str:
    return sha256_text(_canonical(value))


def initialize_approval_schema(conn: sqlite3.Connection) -> None:
    """Install the private append-oriented approval ledger idempotently."""

    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS signal_desk_rebuild_approval_runs (
          semantic_sample_id TEXT PRIMARY KEY,
          schema_version TEXT NOT NULL,
          candidate_json TEXT NOT NULL,
          candidate_sha256 TEXT NOT NULL,
          bounded_packet_json TEXT NOT NULL,
          bounded_packet_sha256 TEXT NOT NULL,
          final_action TEXT,
          final_rationale TEXT,
          final_output_json TEXT,
          publishable INTEGER NOT NULL DEFAULT 0 CHECK(publishable IN (0, 1)),
          disposition TEXT NOT NULL DEFAULT 'pending',
          created_at TEXT NOT NULL,
          completed_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS signal_desk_rebuild_approval_calls (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          semantic_sample_id TEXT NOT NULL
            REFERENCES signal_desk_rebuild_approval_runs(semantic_sample_id)
            ON DELETE RESTRICT,
          provider_call_number INTEGER NOT NULL CHECK(provider_call_number >= 1),
          approval_attempt_number INTEGER,
          context_scope TEXT NOT NULL CHECK(context_scope IN ('bounded', 'wide')),
          packet_json TEXT NOT NULL,
          packet_sha256 TEXT NOT NULL,
          request_json TEXT NOT NULL,
          request_sha256 TEXT NOT NULL,
          budget_receipt_json TEXT NOT NULL,
          budget_receipt_sha256 TEXT NOT NULL,
          status TEXT NOT NULL CHECK(
            status IN ('started', 'completed', 'model_error', 'invalid_response')
          ),
          reported_action TEXT,
          effective_action TEXT,
          rationale TEXT,
          decision_json TEXT,
          decision_sha256 TEXT,
          usage_tokens INTEGER,
          usage_ledger_id TEXT,
          error_sha256 TEXT,
          created_at TEXT NOT NULL,
          completed_at TEXT,
          UNIQUE(semantic_sample_id, provider_call_number),
          UNIQUE(semantic_sample_id, approval_attempt_number)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS signal_desk_rebuild_approval_calls_sample_idx
        ON signal_desk_rebuild_approval_calls(semantic_sample_id, provider_call_number)
        """
    )
    conn.commit()


def _ensure_run(
    conn: sqlite3.Connection,
    *,
    semantic_sample_id: str,
    candidate: Mapping[str, Any],
    bounded_packet: Mapping[str, Any],
) -> None:
    if not semantic_sample_id.strip():
        raise ValueError("semantic_sample_id must not be empty")
    candidate_json = _canonical(dict(candidate))
    bounded_json = _canonical(dict(bounded_packet))
    row = conn.execute(
        """
        SELECT candidate_sha256, bounded_packet_sha256
        FROM signal_desk_rebuild_approval_runs WHERE semantic_sample_id = ?
        """,
        (semantic_sample_id,),
    ).fetchone()
    candidate_hash = sha256_text(candidate_json)
    bounded_hash = sha256_text(bounded_json)
    if row is not None:
        if row[0] != candidate_hash or row[1] != bounded_hash:
            raise ApprovalConflictError(
                "semantic_sample_id already names different candidate or context"
            )
        return
    conn.execute(
        """
        INSERT INTO signal_desk_rebuild_approval_runs
          (semantic_sample_id, schema_version, candidate_json, candidate_sha256,
           bounded_packet_json, bounded_packet_sha256, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            semantic_sample_id,
            SCHEMA_VERSION,
            candidate_json,
            candidate_hash,
            bounded_json,
            bounded_hash,
            now_iso(),
        ),
    )
    conn.commit()


def _load_state(conn: sqlite3.Connection, semantic_sample_id: str) -> ApprovalState:
    state = ApprovalState(semantic_sample_id)
    rows = conn.execute(
        """
        SELECT effective_action, context_scope
        FROM signal_desk_rebuild_approval_calls
        WHERE semantic_sample_id = ? AND approval_attempt_number IS NOT NULL
        ORDER BY approval_attempt_number
        """,
        (semantic_sample_id,),
    ).fetchall()
    for action, scope in rows:
        state = record_approval_attempt(state, action=action, context_scope=scope)
    return state


def _result_from_row(conn: sqlite3.Connection, semantic_sample_id: str) -> ApprovalResult:
    row = conn.execute(
        """
        SELECT final_action, final_rationale, final_output_json, publishable,
               disposition
        FROM signal_desk_rebuild_approval_runs WHERE semantic_sample_id = ?
        """,
        (semantic_sample_id,),
    ).fetchone()
    if row is None or row[0] is None:
        raise ApprovalExecutionError("approval run is not terminal")
    state = _load_state(conn, semantic_sample_id)
    provider_calls = conn.execute(
        """
        SELECT COUNT(*) FROM signal_desk_rebuild_approval_calls
        WHERE semantic_sample_id = ?
        """,
        (semantic_sample_id,),
    ).fetchone()[0]
    return ApprovalResult(
        semantic_sample_id=semantic_sample_id,
        final_action=ApprovalAction(row[0]),
        rationale=str(row[1]),
        publishable=bool(row[3]),
        disposition=str(row[4]),
        approval_attempt_count=state.approval_attempt_count,
        provider_call_count=int(provider_calls),
        wider_context_retries=state.wider_context_retry_count,
        output=json.loads(row[2]) if row[2] is not None else None,
    )


def _decision_output(action: ApprovalAction, response: Mapping[str, Any], candidate: Mapping[str, Any]) -> object | None:
    if action == ApprovalAction.ACCEPT:
        return dict(candidate)
    if action == ApprovalAction.CORRECT:
        corrected = response.get("corrected_candidate")
        if not isinstance(corrected, Mapping):
            raise ApprovalResponseError("correct requires corrected_candidate")
        return dict(corrected)
    if action == ApprovalAction.SPLIT:
        split = response.get("split_candidates")
        if (
            not isinstance(split, Sequence)
            or isinstance(split, (str, bytes))
            or not split
            or any(not isinstance(item, Mapping) for item in split)
        ):
            raise ApprovalResponseError("split requires nonempty split_candidates")
        return [dict(item) for item in split]
    return None


def _validate_response(response: Mapping[str, Any]) -> tuple[ApprovalAction, str]:
    if not isinstance(response, Mapping):
        raise ApprovalResponseError("model response must be an object")
    if response.get("model") != EXPECTED_MODEL:
        raise ApprovalResponseError(
            "approval response must attest the exact gpt-5.5 model; fallback is forbidden"
        )
    try:
        action = ApprovalAction(str(response.get("action") or ""))
    except ValueError as exc:
        raise ApprovalResponseError("unknown approval action") from exc
    rationale = response.get("rationale")
    if not isinstance(rationale, str) or not rationale.strip():
        raise ApprovalResponseError("approval rationale must not be empty")
    return action, rationale.strip()


def _finalize(
    conn: sqlite3.Connection,
    *,
    semantic_sample_id: str,
    action: ApprovalAction,
    rationale: str,
    output: object | None,
) -> ApprovalResult:
    publishable = action in APPROVED_ACTIONS
    disposition = "publishable" if publishable else "quarantined"
    conn.execute(
        """
        UPDATE signal_desk_rebuild_approval_runs
        SET final_action = ?, final_rationale = ?, final_output_json = ?,
            publishable = ?, disposition = ?, completed_at = ?
        WHERE semantic_sample_id = ?
        """,
        (
            action.value,
            rationale,
            _canonical(output) if output is not None else None,
            int(publishable),
            disposition,
            now_iso(),
            semantic_sample_id,
        ),
    )
    conn.commit()
    return _result_from_row(conn, semantic_sample_id)


def run_approval(
    conn: sqlite3.Connection,
    *,
    semantic_sample_id: str,
    candidate: Mapping[str, Any],
    bounded_packet: Mapping[str, Any],
    full_segment: str,
    model_call: ModelCall,
    budget: ApprovalBudgetConfig,
    turns: Sequence[Mapping[str, Any]] | None = None,
    candidate_turn_index: int | None = None,
    at: datetime | None = None,
    budget_gate_fn: BudgetGate = rebuild_budget_gate,
) -> ApprovalResult:
    """Run or resume one semantic candidate through fail-closed GPT-5.5 approval."""

    initialize_approval_schema(conn)
    _ensure_run(
        conn,
        semantic_sample_id=semantic_sample_id,
        candidate=candidate,
        bounded_packet=bounded_packet,
    )
    terminal = conn.execute(
        "SELECT final_action FROM signal_desk_rebuild_approval_runs WHERE semantic_sample_id = ?",
        (semantic_sample_id,),
    ).fetchone()[0]
    if terminal is not None:
        return _result_from_row(conn, semantic_sample_id)

    state = _load_state(conn, semantic_sample_id)
    instant = at or datetime.now(timezone.utc)
    if instant.tzinfo is None:
        raise ValueError("at must be timezone-aware")

    while state.terminal_action is None:
        scope = ContextScope.WIDE if state.awaiting_wider_context else ContextScope.BOUNDED
        if scope == ContextScope.WIDE:
            packet = wider_context_packet(
                turns=turns,
                candidate_turn_index=candidate_turn_index,
                full_segment=full_segment,
            )
        else:
            packet = {"context_scope": ContextScope.BOUNDED.value, **dict(bounded_packet)}

        day, derived_window_start = subscription_budget_window(instant)
        gate = dict(
            budget_gate_fn(
                conn,
                day=day,
                campaign_id=budget.campaign_id,
                grant_path=budget.grant_path,
                budget_dir=budget.budget_dir,
                at=instant,
                clean_release_exists=budget.clean_release_exists,
                additional_budget_db_paths=budget.additional_budget_db_paths,
                window_start_iso=budget.window_start_iso or derived_window_start,
            )
        )
        conn.commit()
        if not gate.get("allowed"):
            raise ApprovalBudgetDenied(
                f"GPT-5.5 approval blocked by budget: {gate.get('reason') or 'denied'}"
            )

        request = {
            "schema_version": SCHEMA_VERSION,
            "model": EXPECTED_MODEL,
            "semantic_sample_id": semantic_sample_id,
            "candidate": dict(candidate),
            "context": packet,
            "allowed_actions": [action.value for action in ApprovalAction],
        }
        provider_call_number = int(
            conn.execute(
                "SELECT COUNT(*) FROM signal_desk_rebuild_approval_calls WHERE semantic_sample_id = ?",
                (semantic_sample_id,),
            ).fetchone()[0]
        ) + 1
        created_at = now_iso()
        cursor = conn.execute(
            """
            INSERT INTO signal_desk_rebuild_approval_calls
              (semantic_sample_id, provider_call_number, context_scope,
               packet_json, packet_sha256, request_json, request_sha256,
               budget_receipt_json, budget_receipt_sha256, status, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'started', ?)
            """,
            (
                semantic_sample_id,
                provider_call_number,
                scope.value,
                _canonical(packet),
                _digest(packet),
                _canonical(request),
                _digest(request),
                _canonical(gate),
                _digest(gate),
                created_at,
            ),
        )
        call_id = int(cursor.lastrowid)
        conn.commit()

        try:
            response = model_call(request)
        except Exception as exc:
            # Unknown provider usage consumes the remaining authorized budget so
            # a broken adapter cannot permit further unaccounted calls.
            reserved = max(0, int(gate.get("remaining_tokens") or 0))
            ledger_id = record_usage(
                conn,
                day=day,
                provider_lane=PROVIDER_LANE,
                lane=BUDGET_LANE,
                run_id=f"{semantic_sample_id}:call:{provider_call_number}:unknown",
                tokens=reserved,
                provider_calls=1,
            )
            conn.execute(
                """
                UPDATE signal_desk_rebuild_approval_calls
                SET status = 'model_error', usage_tokens = ?, usage_ledger_id = ?,
                    error_sha256 = ?, completed_at = ? WHERE id = ?
                """,
                (reserved, ledger_id, sha256_text(repr(exc)), now_iso(), call_id),
            )
            conn.commit()
            if scope == ContextScope.WIDE:
                state = record_approval_attempt(
                    state, action=ApprovalAction.FAIL_CLOSED, context_scope=scope
                )
                conn.execute(
                    """
                    UPDATE signal_desk_rebuild_approval_calls
                    SET approval_attempt_number = ?, effective_action = 'fail_closed',
                        rationale = ? WHERE id = ?
                    """,
                    (
                        state.approval_attempt_count,
                        "wide-context GPT-5.5 call failed; candidate failed closed",
                        call_id,
                    ),
                )
                conn.commit()
                return _finalize(
                    conn,
                    semantic_sample_id=semantic_sample_id,
                    action=ApprovalAction.FAIL_CLOSED,
                    rationale="wide-context GPT-5.5 call failed; candidate failed closed",
                    output=None,
                )
            raise ApprovalExecutionError("bounded GPT-5.5 call failed without fallback") from exc

        decision_json = _canonical(dict(response)) if isinstance(response, Mapping) else _canonical(response)
        tokens = usage_tokens_from_item(response) if isinstance(response, Mapping) else 0
        if tokens <= 0:
            tokens = max(0, int(gate.get("remaining_tokens") or 0))
        ledger_id = record_usage(
            conn,
            day=day,
            provider_lane=PROVIDER_LANE,
            lane=BUDGET_LANE,
            run_id=f"{semantic_sample_id}:call:{provider_call_number}",
            tokens=tokens,
            provider_calls=1,
        )
        try:
            reported_action, rationale = _validate_response(response)
            output = _decision_output(reported_action, response, candidate)
        except (ApprovalResponseError, TypeError) as exc:
            conn.execute(
                """
                UPDATE signal_desk_rebuild_approval_calls
                SET status = 'invalid_response', decision_json = ?, decision_sha256 = ?,
                    usage_tokens = ?, usage_ledger_id = ?, error_sha256 = ?, completed_at = ?
                WHERE id = ?
                """,
                (
                    decision_json,
                    sha256_text(decision_json),
                    tokens,
                    ledger_id,
                    sha256_text(str(exc)),
                    now_iso(),
                    call_id,
                ),
            )
            conn.commit()
            if scope == ContextScope.WIDE:
                state = record_approval_attempt(
                    state, action=ApprovalAction.FAIL_CLOSED, context_scope=scope
                )
                conn.execute(
                    """
                    UPDATE signal_desk_rebuild_approval_calls
                    SET approval_attempt_number = ?, effective_action = 'fail_closed',
                        rationale = ? WHERE id = ?
                    """,
                    (
                        state.approval_attempt_count,
                        "wide-context GPT-5.5 response was invalid; candidate failed closed",
                        call_id,
                    ),
                )
                conn.commit()
                return _finalize(
                    conn,
                    semantic_sample_id=semantic_sample_id,
                    action=ApprovalAction.FAIL_CLOSED,
                    rationale="wide-context GPT-5.5 response was invalid; candidate failed closed",
                    output=None,
                )
            raise ApprovalResponseError("bounded GPT-5.5 response was invalid") from exc

        # A premature fail_closed is widened rather than published as terminal.
        # A second context request after the one permitted retry fails closed.
        effective_action = reported_action
        if scope == ContextScope.BOUNDED and reported_action == ApprovalAction.FAIL_CLOSED:
            effective_action = ApprovalAction.REQUEST_WIDER_CONTEXT
            rationale = f"{rationale} [harness required wider context before fail_closed]"
        elif scope == ContextScope.WIDE and reported_action == ApprovalAction.REQUEST_WIDER_CONTEXT:
            effective_action = ApprovalAction.FAIL_CLOSED
            rationale = f"{rationale} [second wider-context request denied; failed closed]"
            output = None

        try:
            next_state = record_approval_attempt(
                state, action=effective_action, context_scope=scope
            )
        except QualityContractError as exc:
            raise ApprovalResponseError(str(exc)) from exc
        conn.execute(
            """
            UPDATE signal_desk_rebuild_approval_calls
            SET status = 'completed', approval_attempt_number = ?, reported_action = ?,
                effective_action = ?, rationale = ?, decision_json = ?,
                decision_sha256 = ?, usage_tokens = ?, usage_ledger_id = ?, completed_at = ?
            WHERE id = ?
            """,
            (
                next_state.approval_attempt_count,
                reported_action.value,
                effective_action.value,
                rationale,
                decision_json,
                sha256_text(decision_json),
                tokens,
                ledger_id,
                now_iso(),
                call_id,
            ),
        )
        conn.commit()
        state = next_state
        if state.terminal_action is not None:
            return _finalize(
                conn,
                semantic_sample_id=semantic_sample_id,
                action=state.terminal_action,
                rationale=rationale,
                output=output,
            )

    raise AssertionError("unreachable approval state")
