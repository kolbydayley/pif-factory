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

        assert result == {"ready": True, "checked": 3, "unready": []}
        assert checked == ["seg_0.txt", "seg_1.txt", "seg_2.txt"]
    finally:
        conn.close()
