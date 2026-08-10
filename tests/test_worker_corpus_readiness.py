from __future__ import annotations

import sqlite3
from pathlib import Path

from research_factory import worker


def _jobs_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE jobs (
          id INTEGER PRIMARY KEY,
          lane TEXT NOT NULL,
          job_type TEXT NOT NULL,
          target_id TEXT NOT NULL,
          payload_json TEXT NOT NULL,
          status TEXT NOT NULL,
          priority INTEGER NOT NULL,
          attempts INTEGER NOT NULL,
          max_attempts INTEGER NOT NULL,
          lease_owner TEXT,
          leased_until TEXT,
          error TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          completed_at TEXT
        )
        """
    )
    return conn


def test_dataless_prompt_input_is_deferred_without_consuming_attempt(
    monkeypatch,
) -> None:
    conn = _jobs_connection()
    try:
        conn.execute(
            """
            INSERT INTO jobs (
              id, lane, job_type, target_id, payload_json, status, priority,
              attempts, max_attempts, created_at, updated_at
            )
            VALUES (
              1, 'podcast', 'label_segment', 'seg_dataless',
              '{"label_pack":"ai_discourse_v3_1","model":"gpt-5.5"}',
              'pending', 10, 0, 2,
              '2026-07-29T00:00:00+00:00',
              '2026-07-29T00:00:00+00:00'
            )
            """
        )
        conn.commit()
        monkeypatch.setattr(
            worker,
            "prompt_input_readiness",
            lambda _conn, _job: {
                "ready": False,
                "checked": 1,
                "unready": [
                    {
                        "kind": "segment",
                        "id": "seg_dataless",
                        "reason": "dataless",
                    }
                ],
            },
        )

        result = worker.run_jobs(
            conn,
            lane="podcast",
            limit=1,
            model="gpt-5.5",
            label_pack="ai_discourse_v3_1",
            worker_id="readiness-test",
            claim_prompts=True,
            job_types=("label_segment",),
        )

        job = conn.execute("SELECT * FROM jobs WHERE id = 1").fetchone()
        assert result["processed"] == 0
        assert result["failed"] == 0
        assert result["deferred"] == 1
        assert job["status"] == "pending"
        assert job["attempts"] == 0
        assert job["lease_owner"] is None
        assert job["leased_until"] is None
        assert job["error"].startswith("corpus_input_not_hydrated:dataless")
    finally:
        conn.close()


def test_deferred_job_is_not_reclaimed_within_same_run(monkeypatch) -> None:
    conn = _jobs_connection()
    try:
        for job_id in (1, 2):
            conn.execute(
                """
                INSERT INTO jobs (
                  id, lane, job_type, target_id, payload_json, status, priority,
                  attempts, max_attempts, created_at, updated_at
                )
                VALUES (?, 'podcast', 'label_segment', ?, '{}', 'pending', 10,
                        0, 2, '2026-07-29T00:00:00+00:00',
                        '2026-07-29T00:00:00+00:00')
                """,
                (job_id, f"seg_{job_id}"),
            )
        conn.commit()
        seen: list[int] = []

        def readiness(_conn, job):
            seen.append(int(job["id"]))
            return {
                "ready": False,
                "checked": 1,
                "unready": [
                    {
                        "kind": "segment",
                        "id": job["target_id"],
                        "reason": "dataless",
                    }
                ],
            }

        monkeypatch.setattr(worker, "prompt_input_readiness", readiness)

        result = worker.run_jobs(
            conn,
            lane="podcast",
            limit=2,
            model="gpt-5.5",
            label_pack="ai_discourse_v3_1",
            worker_id="readiness-test",
            claim_prompts=True,
            job_types=("label_segment",),
        )

        assert seen == [1, 2]
        assert result["deferred"] == 2
        assert [
            row["attempts"]
            for row in conn.execute("SELECT attempts FROM jobs ORDER BY id")
        ] == [0, 0]
    finally:
        conn.close()


def test_v31_label_readiness_includes_adjacent_segments(
    monkeypatch,
    tmp_path: Path,
) -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    try:
        conn.execute(
            """
            CREATE TABLE segments (
              id TEXT PRIMARY KEY,
              episode_id TEXT NOT NULL,
              segment_index INTEGER NOT NULL,
              text_path TEXT NOT NULL
            )
            """
        )
        for index in range(3):
            conn.execute(
                "INSERT INTO segments VALUES (?, 'ep_1', ?, ?)",
                (
                    f"seg_{index}",
                    index,
                    f"corpus/segments/seg_{index}.txt",
                ),
            )
        conn.commit()
        monkeypatch.setattr(worker, "corpus_dir", lambda: tmp_path / "corpus")
        checked: list[str] = []

        def readiness(path):
            checked.append(Path(path).name)
            return {"ready": True, "reason": None}

        monkeypatch.setattr(worker, "corpus_path_readiness", readiness)
        result = worker.prompt_input_readiness(
            conn,
            {
                "job_type": "label_segment",
                "target_id": "seg_1",
                "payload_json": '{"label_pack":"ai_discourse_v3_1"}',
            },
        )

        assert result == {
            "ready": True,
            "checked": 3,
            "already_hydrated": 3,
            "materialized_on_demand": 0,
            "materialization_wall_seconds": 0.0,
            "unready": [],
        }
        assert checked == ["seg_0.txt", "seg_1.txt", "seg_2.txt"]
    finally:
        conn.close()


def _segments_connection(count: int = 1) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE segments (
          id TEXT PRIMARY KEY,
          episode_id TEXT NOT NULL,
          transcript_id TEXT NOT NULL,
          segment_index INTEGER NOT NULL,
          text_path TEXT NOT NULL
        )
        """
    )
    for index in range(count):
        conn.execute(
            "INSERT INTO segments VALUES (?, 'ep_1', 'tr_1', ?, ?)",
            (
                f"seg_{index}",
                index,
                f"corpus/segments/seg_{index}.txt",
            ),
        )
    conn.commit()
    return conn


def _episode_context_job() -> dict:
    return {
        "job_type": "episode_context",
        "target_id": "ep_1",
        "payload_json": '{"label_pack":"ai_discourse_v3_1"}',
    }


def test_already_hydrated_input_is_ready_without_read_attempt(
    monkeypatch,
    tmp_path: Path,
) -> None:
    conn = _segments_connection()
    try:
        monkeypatch.setattr(worker, "corpus_dir", lambda: tmp_path / "corpus")
        monkeypatch.setattr(
            worker,
            "corpus_path_readiness",
            lambda _path: {"ready": True, "reason": None},
        )

        def unexpected_read(*_args, **_kwargs):
            raise AssertionError("hydrated input must not be read during readiness")

        monkeypatch.setattr(worker, "_materialize_dataless_path", unexpected_read)
        result = worker.prompt_input_readiness(conn, _episode_context_job())

        assert result["ready"] is True
        assert result["already_hydrated"] == 1
        assert result["materialized_on_demand"] == 0
        assert result["materialization_wall_seconds"] == 0.0
    finally:
        conn.close()


def test_dataless_input_materializes_within_timeout(
    monkeypatch,
    tmp_path: Path,
) -> None:
    conn = _segments_connection()
    try:
        monkeypatch.setattr(worker, "corpus_dir", lambda: tmp_path / "corpus")
        monkeypatch.setattr(
            worker,
            "corpus_path_readiness",
            lambda _path: {"ready": False, "reason": "dataless"},
        )
        calls: list[float] = []

        def materialize(_path, *, timeout_seconds):
            calls.append(timeout_seconds)
            return {"ready": True, "reason": None, "elapsed_seconds": 6.656}

        monkeypatch.setattr(worker, "_materialize_dataless_path", materialize)
        result = worker.prompt_input_readiness(conn, _episode_context_job())

        assert result["ready"] is True
        assert result["already_hydrated"] == 0
        assert result["materialized_on_demand"] == 1
        assert result["materialization_wall_seconds"] == 6.656
        assert calls == [30.0]
    finally:
        conn.close()


def test_dataless_read_timeout_defers_with_existing_reason(
    monkeypatch,
    tmp_path: Path,
) -> None:
    conn = _segments_connection()
    try:
        monkeypatch.setattr(worker, "corpus_dir", lambda: tmp_path / "corpus")
        monkeypatch.setattr(
            worker,
            "corpus_path_readiness",
            lambda _path: {"ready": False, "reason": "dataless"},
        )
        monkeypatch.setattr(
            worker,
            "_materialize_dataless_path",
            lambda _path, *, timeout_seconds: {
                "ready": False,
                "reason": "read_timeout",
                "elapsed_seconds": timeout_seconds,
            },
        )
        result = worker.prompt_input_readiness(conn, _episode_context_job())

        assert result["ready"] is False
        assert result["materialized_on_demand"] == 0
        assert result["unready"][0]["reason"] == "dataless:read_timeout"
        assert (
            "corpus_input_not_hydrated:" + result["unready"][0]["reason"]
        ).startswith("corpus_input_not_hydrated:dataless")
    finally:
        conn.close()


def test_per_job_materialization_cap_defers(
    monkeypatch,
    tmp_path: Path,
) -> None:
    conn = _segments_connection(count=5)
    try:
        monkeypatch.setattr(worker, "corpus_dir", lambda: tmp_path / "corpus")
        monkeypatch.setattr(
            worker,
            "corpus_path_readiness",
            lambda _path: {"ready": False, "reason": "dataless"},
        )
        calls = 0

        def materialize(_path, *, timeout_seconds):
            nonlocal calls
            calls += 1
            return {
                "ready": True,
                "reason": None,
                "elapsed_seconds": timeout_seconds + 0.001,
            }

        monkeypatch.setattr(worker, "_materialize_dataless_path", materialize)
        result = worker.prompt_input_readiness(conn, _episode_context_job())

        assert result["ready"] is False
        assert calls == 4
        assert result["materialized_on_demand"] == 3
        assert result["materialization_wall_seconds"] > 120.0
        assert any(
            item["reason"] == "dataless:job_materialization_cap_exceeded:120s"
            for item in result["unready"]
        )
    finally:
        conn.close()
