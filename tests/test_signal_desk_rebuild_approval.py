from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from research_factory.signal_desk_rebuild_approval import (
    ApprovalBudgetConfig,
    ApprovalBudgetDenied,
    ApprovalConflictError,
    ApprovalExecutionError,
    ApprovalResponseError,
    initialize_approval_schema,
    run_approval,
)


CAMPAIGN = "signal-desk-clean-corpus-2026-08-31"
NOW = datetime(2026, 9, 2, 12, tzinfo=timezone.utc)


@pytest.fixture
def conn():
    value = sqlite3.connect(":memory:")
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def budget(tmp_path):
    return ApprovalBudgetConfig(
        campaign_id=CAMPAIGN,
        grant_path=tmp_path / "grant.json",
        budget_dir=tmp_path / "budget",
    )


def allowed_budget(*args, **kwargs):
    return {
        "allowed": True,
        "remaining_tokens": 20_000_000,
        "campaign_id": kwargs["campaign_id"],
        "reason": None,
    }


def candidate():
    return {"claim": "Frontier training costs are rising", "speaker": "A"}


def bounded():
    return {"turns": [{"speaker": "A", "text": "Costs keep rising."}]}


def response(action, *, tokens=100, **extra):
    return {
        "model": "gpt-5.5",
        "action": action,
        "rationale": f"reason for {action}",
        "usage": {"total_tokens": tokens},
        **extra,
    }


def test_accept_persists_candidate_packet_decision_hashes_and_usage(conn, budget):
    calls = []

    def model(request):
        calls.append(request)
        return response("accept", tokens=321)

    result = run_approval(
        conn,
        semantic_sample_id="sample-1",
        candidate=candidate(),
        bounded_packet=bounded(),
        full_segment="complete segment",
        model_call=model,
        budget=budget,
        at=NOW,
        budget_gate_fn=allowed_budget,
    )
    assert result.publishable is True
    assert result.disposition == "publishable"
    assert result.final_action.value == "accept"
    assert result.approval_attempt_count == result.provider_call_count == 1
    assert calls[0]["model"] == "gpt-5.5"
    row = conn.execute(
        """
        SELECT candidate_sha256, bounded_packet_sha256, final_action, disposition
        FROM signal_desk_rebuild_approval_runs WHERE semantic_sample_id = 'sample-1'
        """
    ).fetchone()
    assert all(row[:2])
    assert row[2:] == ("accept", "publishable")
    call = conn.execute(
        """
        SELECT packet_sha256, request_sha256, decision_sha256, usage_tokens,
               reported_action, effective_action
        FROM signal_desk_rebuild_approval_calls
        """
    ).fetchone()
    assert all(call[:3])
    assert call[3:] == (321, "accept", "accept")
    assert conn.execute(
        "SELECT SUM(tokens) FROM pif_subscription_budget_ledger"
    ).fetchone()[0] == 321


def test_wider_context_is_second_attempt_on_same_semantic_sample(conn, budget):
    seen = []
    replies = iter((response("request_wider_context"), response("correct", corrected_candidate={"claim": "corrected"})))

    def model(request):
        seen.append(request)
        return next(replies)

    turns = [{"speaker": str(i), "text": f"turn {i}"} for i in range(10)]
    result = run_approval(
        conn,
        semantic_sample_id="sample-wide",
        candidate=candidate(),
        bounded_packet=bounded(),
        full_segment="fallback",
        turns=turns,
        candidate_turn_index=5,
        model_call=model,
        budget=budget,
        at=NOW,
        budget_gate_fn=allowed_budget,
    )
    assert result.final_action.value == "correct"
    assert result.publishable is True
    assert result.output == {"claim": "corrected"}
    assert result.approval_attempt_count == 2
    assert result.provider_call_count == 2
    assert result.wider_context_retries == 1
    assert seen[1]["context"]["context_source"] == "speaker_turns_plus_minus_three"
    assert seen[1]["context"]["turn_start"] == 2
    assert seen[1]["context"]["turn_end_exclusive"] == 9
    assert conn.execute(
        "SELECT COUNT(DISTINCT semantic_sample_id) FROM signal_desk_rebuild_approval_runs"
    ).fetchone()[0] == 1


def test_wider_context_falls_back_to_full_segment_and_second_request_fails_closed(conn, budget):
    replies = iter((response("request_wider_context"), response("request_wider_context")))
    contexts = []

    def model(request):
        contexts.append(request["context"])
        return next(replies)

    result = run_approval(
        conn,
        semantic_sample_id="sample-full",
        candidate=candidate(),
        bounded_packet=bounded(),
        full_segment="the entire flattened segment",
        model_call=model,
        budget=budget,
        at=NOW,
        budget_gate_fn=allowed_budget,
    )
    assert contexts[1]["context_source"] == "full_segment"
    assert result.final_action.value == "fail_closed"
    assert result.publishable is False
    assert result.disposition == "quarantined"
    assert result.approval_attempt_count == 2


def test_early_fail_closed_is_forced_through_wide_retry(conn, budget):
    replies = iter((response("fail_closed"), response("reject")))
    result = run_approval(
        conn,
        semantic_sample_id="sample-early-fail",
        candidate=candidate(),
        bounded_packet=bounded(),
        full_segment="whole segment",
        model_call=lambda request: next(replies),
        budget=budget,
        at=NOW,
        budget_gate_fn=allowed_budget,
    )
    assert result.final_action.value == "reject"
    assert result.approval_attempt_count == 2
    actions = conn.execute(
        """
        SELECT reported_action, effective_action
        FROM signal_desk_rebuild_approval_calls ORDER BY provider_call_number
        """
    ).fetchall()
    assert actions == [("fail_closed", "request_wider_context"), ("reject", "reject")]


@pytest.mark.parametrize("action", ["reject", "require_audio"])
def test_nonapproved_terminal_actions_are_quarantined(conn, budget, action):
    result = run_approval(
        conn,
        semantic_sample_id=f"sample-{action}",
        candidate=candidate(),
        bounded_packet=bounded(),
        full_segment="whole segment",
        model_call=lambda request: response(action),
        budget=budget,
        at=NOW,
        budget_gate_fn=allowed_budget,
    )
    assert result.publishable is False
    assert result.disposition == "quarantined"


def test_budget_checked_before_every_call_and_denial_prevents_second_model_call(conn, budget):
    gates = iter(
        (
            {"allowed": True, "remaining_tokens": 1_000, "reason": None},
            {"allowed": False, "remaining_tokens": 0, "reason": "daily_cap_reached"},
        )
    )
    model_calls = []

    def gate(*args, **kwargs):
        return next(gates)

    def model(request):
        model_calls.append(request)
        return response("request_wider_context")

    with pytest.raises(ApprovalBudgetDenied, match="daily_cap_reached"):
        run_approval(
            conn,
            semantic_sample_id="sample-budget",
            candidate=candidate(),
            bounded_packet=bounded(),
            full_segment="whole segment",
            model_call=model,
            budget=budget,
            at=NOW,
            budget_gate_fn=gate,
        )
    assert len(model_calls) == 1
    assert conn.execute(
        "SELECT COUNT(*) FROM signal_desk_rebuild_approval_calls"
    ).fetchone()[0] == 1


def test_wrong_model_attestation_is_rejected_without_fallback(conn, budget):
    with pytest.raises(ApprovalResponseError, match="bounded GPT-5.5 response was invalid"):
        run_approval(
            conn,
            semantic_sample_id="sample-wrong-model",
            candidate=candidate(),
            bounded_packet=bounded(),
            full_segment="whole segment",
            model_call=lambda request: {
                **response("accept"),
                "model": "some-fallback-model",
            },
            budget=budget,
            at=NOW,
            budget_gate_fn=allowed_budget,
        )
    assert conn.execute(
        "SELECT status FROM signal_desk_rebuild_approval_calls"
    ).fetchone()[0] == "invalid_response"


def test_wide_transport_failure_becomes_terminal_quarantine(conn, budget):
    call = 0

    def model(request):
        nonlocal call
        call += 1
        if call == 1:
            return response("request_wider_context", tokens=10)
        raise RuntimeError("provider unavailable")

    result = run_approval(
        conn,
        semantic_sample_id="sample-wide-error",
        candidate=candidate(),
        bounded_packet=bounded(),
        full_segment="whole segment",
        model_call=model,
        budget=budget,
        at=NOW,
        budget_gate_fn=allowed_budget,
    )
    assert result.final_action.value == "fail_closed"
    assert result.publishable is False
    assert result.provider_call_count == 2
    assert result.approval_attempt_count == 2
    assert conn.execute(
        "SELECT status FROM signal_desk_rebuild_approval_calls ORDER BY id DESC LIMIT 1"
    ).fetchone()[0] == "model_error"


def test_bounded_transport_failure_has_no_fallback_and_reserves_budget(conn, budget):
    with pytest.raises(ApprovalExecutionError, match="without fallback"):
        run_approval(
            conn,
            semantic_sample_id="sample-model-error",
            candidate=candidate(),
            bounded_packet=bounded(),
            full_segment="whole segment",
            model_call=lambda request: (_ for _ in ()).throw(RuntimeError("down")),
            budget=budget,
            at=NOW,
            budget_gate_fn=allowed_budget,
        )
    assert conn.execute(
        "SELECT SUM(tokens) FROM pif_subscription_budget_ledger"
    ).fetchone()[0] == 20_000_000


def test_terminal_run_is_idempotent_and_input_hash_conflicts_fail(conn, budget):
    calls = 0

    def model(request):
        nonlocal calls
        calls += 1
        return response("accept")

    kwargs = dict(
        conn=conn,
        semantic_sample_id="sample-idempotent",
        candidate=candidate(),
        bounded_packet=bounded(),
        full_segment="whole segment",
        model_call=model,
        budget=budget,
        at=NOW,
        budget_gate_fn=allowed_budget,
    )
    first = run_approval(**kwargs)
    second = run_approval(**kwargs)
    assert first == second
    assert calls == 1
    with pytest.raises(ApprovalConflictError):
        run_approval(**{**kwargs, "candidate": {"claim": "different"}})


def test_schema_install_is_idempotent(conn):
    initialize_approval_schema(conn)
    initialize_approval_schema(conn)
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }
    assert "signal_desk_rebuild_approval_runs" in tables
    assert "signal_desk_rebuild_approval_calls" in tables
