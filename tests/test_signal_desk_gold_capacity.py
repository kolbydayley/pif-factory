from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone

from research_factory.signal_desk_gold_capacity import (
    INITIAL_BACKOFF_SECONDS,
    SUCCESSFUL_PROBES_TO_CLOSE,
    admit_gold_call,
    capacity_backend_message_from_sidecar,
    capacity_error_from_sidecar,
    capacity_status,
    is_model_capacity_error,
    record_capacity_failure,
    record_gold_admission_success,
)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return conn


def _at(seconds: int = 0) -> datetime:
    return datetime(2026, 9, 2, tzinfo=timezone.utc) + timedelta(seconds=seconds)


def test_capacity_error_is_recognized_from_provider_code_and_sidecar(tmp_path):
    assert is_model_capacity_error(error_code="serverOverloaded")
    assert is_model_capacity_error(detail="Selected model is at capacity. Please try a different model.")
    assert not is_model_capacity_error(error_code="invalidOutput", detail="schema mismatch")
    sidecar = tmp_path / "sidecar.json"
    sidecar.write_text(
        '{"turn_error":{"codex_error_info":"serverOverloaded"}}', encoding="utf-8"
    )
    assert capacity_error_from_sidecar(str(sidecar)) == "serverOverloaded"


def test_capacity_backend_message_is_retained_only_as_bounded_diagnostic(tmp_path):
    sidecar = tmp_path / "sidecar.json"
    message = "Selected model is at capacity. Please try a different model."
    sidecar.write_text(
        json.dumps({"turn_error": {
            "codex_error_info": "serverOverloaded",
            "backend_message": message,
        }}),
        encoding="utf-8",
    )
    assert capacity_backend_message_from_sidecar(str(sidecar)) == message

    sidecar.write_text(
        json.dumps({"turn_error": {
            "codex_error_info": "invalidOutput",
            "backend_message": "private arbitrary provider payload",
        }}),
        encoding="utf-8",
    )
    assert capacity_backend_message_from_sidecar(str(sidecar)) is None


def test_capacity_failure_opens_persistent_backoff_and_prevents_stampede():
    conn = _conn()
    first = admit_gold_call(
        conn, task_key="t1", lease_owner="w1", configured_concurrency=2, at=_at()
    )
    assert first["allowed"] is True and first["state"] == "closed"
    failure = record_capacity_failure(
        conn, admission_id=first["admission_id"], error_code="serverOverloaded",
        backend_message="Selected model is at capacity. Please try a different model.",
        at=_at(1),
    )
    assert failure["backoff_seconds"] == INITIAL_BACKOFF_SECONDS
    blocked = admit_gold_call(
        conn, task_key="t2", lease_owner="w2", configured_concurrency=8, at=_at(2)
    )
    assert blocked == {
        "allowed": False,
        "reason": "gold_model_capacity_backoff",
        "retry_after_seconds": INITIAL_BACKOFF_SECONDS - 1,
        "state": "open",
    }
    state = capacity_status(conn, at=_at(2))
    assert state["state"] == "open"
    assert state["active_admissions"] == 0
    event = conn.execute(
        "SELECT backend_message FROM signal_desk_model_capacity_events "
        "WHERE event_type='capacity_failure'"
    ).fetchone()
    assert event["backend_message"] == (
        "Selected model is at capacity. Please try a different model."
    )


def test_half_open_uses_one_probe_then_restores_only_after_three_successes():
    conn = _conn()
    first = admit_gold_call(
        conn, task_key="t1", lease_owner="w1", configured_concurrency=2, at=_at()
    )
    record_capacity_failure(
        conn, admission_id=first["admission_id"], error_code="serverOverloaded", at=_at(1)
    )
    now = _at(1 + INITIAL_BACKOFF_SECONDS)
    for index in range(SUCCESSFUL_PROBES_TO_CLOSE):
        probe = admit_gold_call(
            conn,
            task_key=f"probe-{index}",
            lease_owner=f"w{index}",
            configured_concurrency=8,
            at=now + timedelta(seconds=index),
        )
        assert probe["allowed"] is True
        assert probe["state"] == "half_open"
        assert probe["provider_concurrency_limit"] == 1
        outcome = record_gold_admission_success(
            conn, admission_id=probe["admission_id"], at=now + timedelta(seconds=index)
        )
    assert outcome["state"] == "closed"
    normal_one = admit_gold_call(
        conn, task_key="normal-1", lease_owner="n1", configured_concurrency=2, at=_at(400)
    )
    normal_two = admit_gold_call(
        conn, task_key="normal-2", lease_owner="n2", configured_concurrency=2, at=_at(400)
    )
    blocked = admit_gold_call(
        conn, task_key="normal-3", lease_owner="n3", configured_concurrency=2, at=_at(400)
    )
    assert normal_one["allowed"] and normal_two["allowed"]
    assert blocked["reason"] == "gold_model_capacity_slots_full"


def test_gold_a1_and_a2_share_one_provider_admission_budget():
    """Distinct rebuild lanes may not independently fill Sol capacity."""

    conn = _conn()
    gold = admit_gold_call(
        conn,
        task_key="gold:C:window-1",
        lease_owner="gold-worker",
        configured_concurrency=1,
        at=_at(),
    )
    a1 = admit_gold_call(
        conn,
        task_key="a1:frontier:window-2",
        lease_owner="frontier-worker",
        configured_concurrency=1,
        lane="gpt_5_6_sol_frontier_calibration",
        at=_at(),
    )
    a2 = admit_gold_call(
        conn,
        task_key="a2:scorer:case-3",
        lease_owner="scorer-worker",
        configured_concurrency=1,
        lane="gpt_5_6_sol_scorer_qualification",
        at=_at(),
    )

    assert gold["allowed"]
    assert a1["reason"] == "gold_model_capacity_slots_full"
    assert a2["reason"] == "gold_model_capacity_slots_full"
    events = conn.execute(
        "SELECT lane,event_type FROM signal_desk_model_capacity_events ORDER BY id"
    ).fetchall()
    assert [(row["lane"], row["event_type"]) for row in events] == [
        ("gpt_5_6_sol_gold_authoring", "admitted"),
        ("gpt_5_6_sol_frontier_calibration", "denied"),
        ("gpt_5_6_sol_scorer_qualification", "denied"),
    ]
