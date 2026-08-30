from pathlib import Path

from research_factory import pif_bulk_draft_runner as runner


def test_only_repeated_deterministic_failures_quarantine(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, "SHADOW_DB", Path(tmp_path) / "drafts.sqlite")
    monkeypatch.setattr(runner, "SHADOW_ROOT", Path(tmp_path))
    runner.init_shadow_db()

    for _ in range(5):
        assert runner.record_draft_failure(
            segment_id="transient",
            lane="glm-zai",
            failure_class="provider",
            reason="timeout",
            deterministic=False,
        ) is False

    assert runner.record_draft_failure(
        segment_id="poison",
        lane="glm-zai",
        failure_class="schema",
        reason="bad_topic",
        deterministic=True,
    ) is False
    assert runner.record_draft_failure(
        segment_id="poison",
        lane="glm-zai",
        failure_class="schema",
        reason="bad_claim",
        deterministic=True,
    ) is False
    assert runner.record_draft_failure(
        segment_id="poison",
        lane="glm-zai",
        failure_class="schema",
        reason="bad_topic",
        deterministic=True,
    ) is True

    with runner._shadow_conn() as conn:
        rows = conn.execute(
            "SELECT segment_id, failure_class, attempts, quarantined_at"
            " FROM draft_failures ORDER BY segment_id"
        ).fetchall()
    assert rows[0][0:3] == ("poison", "schema", 3)
    assert rows[0][3]
    assert rows[1][0:3] == ("transient", "provider", 5)
    assert rows[1][3] is None
