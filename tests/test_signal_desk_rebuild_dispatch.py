from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from research_factory.signal_desk_rebuild_dispatch import (
    DispatchError,
    InvalidTransition,
    LostLease,
    TaskDefinitionConflict,
    acquire_lease,
    complete_attempt,
    complete_resurrected_attempt_from_artifact,
    enqueue_task,
    fail_attempt_semantically,
    get_task,
    initialize_dispatch_schema,
    list_attempts,
    release_attempt_for_retry,
    resurrect_task,
)


T0 = datetime(2026, 8, 31, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
def conn() -> sqlite3.Connection:
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    initialize_dispatch_schema(db)
    yield db
    db.close()


def enqueue(conn: sqlite3.Connection, key: str = "window:1") -> dict:
    return enqueue_task(
        conn,
        task_key=key,
        task_type="extract_window",
        payload={"window_id": key, "prompt_hash": "abc"},
        now=T0,
    )


def test_initializer_is_idempotent_and_uses_own_migration_ledger(conn) -> None:
    initialize_dispatch_schema(conn)
    version = conn.execute(
        "SELECT version FROM signal_desk_rebuild_dispatch_migrations"
    ).fetchall()
    assert [row[0] for row in version] == [1]
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 0


def test_enqueue_creates_lineage_and_attempt_once(conn) -> None:
    first = enqueue(conn)
    second = enqueue(conn)

    assert first["enqueue_outcome"] == "created"
    assert first["status"] == "pending"
    assert first["attempt_number"] == 1
    assert second["enqueue_outcome"] == "existing"
    assert second["id"] == first["id"]
    assert len(list_attempts(conn, "window:1")) == 1


def test_same_key_with_different_work_is_rejected(conn) -> None:
    enqueue(conn)
    with pytest.raises(TaskDefinitionConflict):
        enqueue_task(
            conn,
            task_key="window:1",
            task_type="extract_window",
            payload={"window_id": "different"},
            now=T0,
        )


def test_active_lease_is_exclusive_and_completion_is_fenced(conn) -> None:
    enqueue(conn)
    lease = acquire_lease(
        conn, lease_owner="worker-a", lease_seconds=60, now=T0
    )
    assert lease is not None
    assert lease["lease_kind"] == "initial"
    assert acquire_lease(
        conn, lease_owner="worker-b", lease_seconds=60, now=T0
    ) is None

    with pytest.raises(LostLease):
        complete_attempt(
            conn,
            attempt_id=lease["current_attempt_id"],
            lease_owner="worker-b",
            lease_generation=lease["lease_generation"],
            output={"ok": True},
            now=T0 + timedelta(seconds=1),
        )

    completed = complete_attempt(
        conn,
        attempt_id=lease["current_attempt_id"],
        lease_owner="worker-a",
        lease_generation=lease["lease_generation"],
        output={"ok": True},
        now=T0 + timedelta(seconds=1),
    )
    assert completed["status"] == "succeeded"
    assert completed["attempt_status"] == "succeeded"
    assert acquire_lease(
        conn, lease_owner="worker-c", lease_seconds=60, now=T0 + timedelta(seconds=2)
    ) is None


def test_worker_crash_releases_same_attempt_after_expiry(conn) -> None:
    enqueue(conn)
    crashed = acquire_lease(
        conn, lease_owner="crashed-worker", lease_seconds=10, now=T0
    )
    assert crashed is not None

    recovered = acquire_lease(
        conn,
        lease_owner="recovery-worker",
        lease_seconds=30,
        now=T0 + timedelta(seconds=11),
    )
    assert recovered is not None
    assert recovered["lease_kind"] == "expired_recovery"
    assert recovered["current_attempt_id"] == crashed["current_attempt_id"]
    assert recovered["attempt_number"] == 1
    assert recovered["lease_generation"] == crashed["lease_generation"] + 1
    assert len(list_attempts(conn, "window:1")) == 1

    with pytest.raises(LostLease):
        complete_attempt(
            conn,
            attempt_id=crashed["current_attempt_id"],
            lease_owner="crashed-worker",
            lease_generation=crashed["lease_generation"],
            output={"stale": True},
            now=T0 + timedelta(seconds=12),
        )


def test_exact_expiry_is_recoverable(conn) -> None:
    enqueue(conn)
    first = acquire_lease(conn, lease_owner="a", lease_seconds=10, now=T0)
    recovered = acquire_lease(
        conn, lease_owner="b", lease_seconds=10, now=T0 + timedelta(seconds=10)
    )
    assert recovered["lease_kind"] == "expired_recovery"
    assert recovered["current_attempt_id"] == first["current_attempt_id"]


def test_semantic_failure_is_terminal_and_enqueue_does_not_revive(conn) -> None:
    enqueue(conn)
    lease = acquire_lease(conn, lease_owner="judge", lease_seconds=60, now=T0)
    failed = fail_attempt_semantically(
        conn,
        attempt_id=lease["current_attempt_id"],
        lease_owner="judge",
        lease_generation=lease["lease_generation"],
        failure_code="speaker_attribution_unresolvable",
        failure_detail="Evidence does not identify the speaker.",
        now=T0 + timedelta(seconds=1),
    )
    assert failed["status"] == "terminal_failed"
    assert failed["attempt_status"] == "terminal_failed"

    duplicate = enqueue(conn)
    assert duplicate["enqueue_outcome"] == "existing_terminal_failed"
    assert duplicate["current_attempt_id"] == failed["current_attempt_id"]
    assert len(list_attempts(conn, "window:1")) == 1
    assert acquire_lease(
        conn, lease_owner="other", lease_seconds=60, now=T0 + timedelta(days=1)
    ) is None


def test_infrastructure_failure_requeues_same_attempt_without_resurrection(conn) -> None:
    enqueue(conn)
    lease = acquire_lease(conn, lease_owner="worker", lease_seconds=60, now=T0)
    pending = release_attempt_for_retry(
        conn,
        attempt_id=lease["current_attempt_id"],
        lease_owner="worker",
        lease_generation=lease["lease_generation"],
        failure_code="provider_429",
        failure_detail="temporary capacity",
        now=T0 + timedelta(seconds=1),
    )
    assert pending["status"] == "pending"
    assert pending["current_attempt_id"] == lease["current_attempt_id"]
    assert len(list_attempts(conn, "window:1")) == 1
    retry = acquire_lease(
        conn, lease_owner="worker-2", lease_seconds=60, now=T0 + timedelta(seconds=2)
    )
    assert retry["current_attempt_id"] == lease["current_attempt_id"]
    assert retry["attempt_number"] == 1


def test_explicit_resurrection_appends_new_attempt_same_lineage(conn) -> None:
    original = enqueue(conn)
    lease = acquire_lease(conn, lease_owner="judge", lease_seconds=60, now=T0)
    fail_attempt_semantically(
        conn,
        attempt_id=lease["current_attempt_id"],
        lease_owner="judge",
        lease_generation=lease["lease_generation"],
        failure_code="bad_context",
        failure_detail="Context was malformed.",
        now=T0 + timedelta(seconds=1),
    )

    revived = resurrect_task(
        conn,
        task_key="window:1",
        resurrected_by="operator",
        reason="Corrected the source context and authorized another attempt.",
        now=T0 + timedelta(seconds=2),
    )
    attempts = list_attempts(conn, "window:1")
    assert revived["id"] == original["id"]
    assert revived["status"] == "pending"
    assert revived["attempt_number"] == 2
    assert revived["resurrects_attempt_id"] == attempts[0]["id"]
    assert [row["status"] for row in attempts] == ["terminal_failed", "pending"]
    assert attempts[1]["resurrection_reason"].startswith("Corrected")
    assert attempts[1]["resurrected_by"] == "operator"

    next_lease = acquire_lease(
        conn, lease_owner="worker-new", lease_seconds=60, now=T0 + timedelta(seconds=3)
    )
    assert next_lease["current_attempt_id"] == attempts[1]["id"]
    assert next_lease["lease_kind"] == "initial"


def test_explicit_resurrection_can_complete_from_validated_artifact(conn) -> None:
    enqueue(conn)
    lease = acquire_lease(conn, lease_owner="judge", lease_seconds=60, now=T0)
    fail_attempt_semantically(
        conn,
        attempt_id=lease["current_attempt_id"],
        lease_owner="judge",
        lease_generation=lease["lease_generation"],
        failure_code="offset_only",
        failure_detail="verbatim excerpt had an incorrect offset",
        now=T0 + timedelta(seconds=1),
    )
    resurrect_task(
        conn,
        task_key="window:1",
        resurrected_by="operator",
        reason="The preserved artifact was repaired and independently validated.",
        now=T0 + timedelta(seconds=2),
    )
    recovered = complete_resurrected_attempt_from_artifact(
        conn,
        task_key="window:1",
        recovered_by="contract-validator-v2",
        output={"artifact_sha256": "a" * 64, "semantic_fields_changed": False},
        now=T0 + timedelta(seconds=3),
    )
    assert recovered["status"] == "succeeded"
    output = conn.execute(
        "SELECT output_json FROM signal_desk_rebuild_attempts WHERE id=?",
        (recovered["current_attempt_id"],),
    ).fetchone()[0]
    assert '"artifact_recovery":true' in output
    assert '"semantic_fields_changed":false' in output


def test_artifact_recovery_rejects_ordinary_pending_work(conn) -> None:
    enqueue(conn)
    with pytest.raises(InvalidTransition):
        complete_resurrected_attempt_from_artifact(
            conn,
            task_key="window:1",
            recovered_by="validator",
            output={"artifact_sha256": "a" * 64},
            now=T0 + timedelta(seconds=1),
        )


@pytest.mark.parametrize("state", ["pending", "running", "succeeded"])
def test_resurrection_rejects_nonterminal_tasks(conn, state) -> None:
    enqueue(conn)
    if state in {"running", "succeeded"}:
        lease = acquire_lease(conn, lease_owner="worker", lease_seconds=60, now=T0)
        if state == "succeeded":
            complete_attempt(
                conn,
                attempt_id=lease["current_attempt_id"],
                lease_owner="worker",
                lease_generation=lease["lease_generation"],
                output={},
                now=T0 + timedelta(seconds=1),
            )
    with pytest.raises(InvalidTransition):
        resurrect_task(
            conn,
            task_key="window:1",
            resurrected_by="operator",
            reason="not allowed",
            now=T0 + timedelta(seconds=2),
        )


def test_expired_lease_cannot_complete_without_reacquisition(conn) -> None:
    enqueue(conn)
    lease = acquire_lease(conn, lease_owner="worker", lease_seconds=5, now=T0)
    with pytest.raises(LostLease):
        complete_attempt(
            conn,
            attempt_id=lease["current_attempt_id"],
            lease_owner="worker",
            lease_generation=lease["lease_generation"],
            output={},
            now=T0 + timedelta(seconds=5),
        )


def test_mutation_rejects_an_ambient_transaction(conn) -> None:
    conn.execute("BEGIN")
    with pytest.raises(DispatchError):
        enqueue(conn)
    conn.rollback()


def test_dispatch_order_recovers_expired_work_before_pending(conn) -> None:
    enqueue(conn, "window:1")
    acquire_lease(conn, lease_owner="crashed", lease_seconds=5, now=T0)
    enqueue(conn, "window:2")

    recovered = acquire_lease(
        conn, lease_owner="recovery", lease_seconds=10, now=T0 + timedelta(seconds=6)
    )
    assert recovered["task_key"] == "window:1"
    assert recovered["lease_kind"] == "expired_recovery"
    pending = acquire_lease(
        conn, lease_owner="next", lease_seconds=10, now=T0 + timedelta(seconds=6)
    )
    assert pending["task_key"] == "window:2"


def test_lease_prefix_keeps_staged_workers_in_their_partition(conn) -> None:
    enqueue(conn, "dev:C:window-1")
    enqueue(conn, "dev:A:window-1")

    a_lease = acquire_lease(
        conn,
        lease_owner="gold-a",
        lease_seconds=60,
        task_key_prefix="dev:A:",
        now=T0,
    )
    assert a_lease is not None
    assert a_lease["task_key"] == "dev:A:window-1"
    assert acquire_lease(
        conn,
        lease_owner="gold-b",
        lease_seconds=60,
        task_key_prefix="dev:B:",
        now=T0,
    ) is None

    c_lease = acquire_lease(
        conn,
        lease_owner="gold-c",
        lease_seconds=60,
        task_key_prefix="dev:C:",
        now=T0,
    )
    assert c_lease is not None
    assert c_lease["task_key"] == "dev:C:window-1"


def test_snapshot_round_trips_canonical_payload(conn) -> None:
    enqueue_task(
        conn,
        task_key="unicode",
        task_type="extract",
        payload={"b": [2, 1], "a": "Café"},
        now=T0,
    )
    assert get_task(conn, "unicode")["payload"] == {"a": "Café", "b": [2, 1]}
