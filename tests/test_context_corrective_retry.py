from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from unittest.mock import patch

from research_factory.windowed_evaluation import (
    execute_instrumented_episode_context_job,
    release_instrumented_context_job_for_corrective_retry,
)


def _connection(
    tmp_path: Path,
    *,
    status: str = "claimed",
) -> tuple[sqlite3.Connection, Path, Path, Path]:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE jobs (
          id INTEGER PRIMARY KEY,
          job_type TEXT NOT NULL,
          target_id TEXT NOT NULL,
          payload_json TEXT NOT NULL,
          status TEXT NOT NULL,
          attempts INTEGER NOT NULL,
          lease_owner TEXT,
          leased_until TEXT,
          error TEXT,
          updated_at TEXT
        );
        CREATE TABLE episode_context_runs (
          id TEXT PRIMARY KEY,
          job_id INTEGER NOT NULL,
          episode_id TEXT NOT NULL,
          status TEXT NOT NULL,
          prompt_path TEXT NOT NULL,
          output_path TEXT NOT NULL,
          error TEXT,
          updated_at TEXT
        );
        """
    )
    prompt_path = tmp_path / "prompt.md"
    output_path = tmp_path / "output.json"
    sidecar_path = tmp_path / "sidecar.json"
    prompt_path.write_text("ORIGINAL PROMPT", encoding="utf-8")
    output_path.write_text("{", encoding="utf-8")
    payload = {
        "episode_context_run_id": "ectx-test",
        "prompt_path": str(prompt_path),
        "output_path": str(output_path),
    }
    conn.execute(
        """
        INSERT INTO jobs
          (id, job_type, target_id, payload_json, status, attempts,
           lease_owner, leased_until, error, updated_at)
        VALUES (1, 'episode_context', 'ep-test', ?, ?, 1,
                'worker-test', '2099-01-01T00:00:00+00:00', NULL, 'now')
        """,
        (json.dumps(payload), status),
    )
    conn.execute(
        """
        INSERT INTO episode_context_runs
          (id, job_id, episode_id, status, prompt_path, output_path, error, updated_at)
        VALUES ('ectx-test', 1, 'ep-test', 'claimed', ?, ?, NULL, 'now')
        """,
        (str(prompt_path), str(output_path)),
    )
    conn.commit()
    return conn, prompt_path, output_path, sidecar_path


def test_release_stuck_invalid_output_for_one_corrective_retry(
    tmp_path: Path,
) -> None:
    conn, _, _, sidecar_path = _connection(tmp_path)
    sidecar_path.write_text(
        json.dumps(
            {
                "state": "failed",
                "exit_code": 0,
                "timed_out": False,
                "json_ok": False,
                "usage": {"total_tokens": 10},
                "usage_profile": {
                    "cumulative_billed_total_tokens": 10,
                    "last_turn_unique_total_tokens": 8,
                },
            }
        ),
        encoding="utf-8",
    )

    with patch(
        "research_factory.windowed_evaluation._context_usage_sidecar_path",
        return_value=sidecar_path,
    ):
        result = release_instrumented_context_job_for_corrective_retry(
            conn,
            job_id=1,
        )

    job = conn.execute("SELECT * FROM jobs WHERE id = 1").fetchone()
    run = conn.execute(
        "SELECT * FROM episode_context_runs WHERE id = 'ectx-test'"
    ).fetchone()
    payload = json.loads(job["payload_json"])
    assert result["job_status"] == "pending"
    assert result["validation_error"].startswith("JSONDecodeError:")
    assert job["status"] == "pending"
    assert job["lease_owner"] is None
    assert job["leased_until"] is None
    assert payload["corrective_retry_pending"] is True
    assert payload["corrective_validation_error"] == result["validation_error"]
    assert run["status"] == "failed"


def test_invalid_output_gets_one_retry_then_terminal_failure(
    tmp_path: Path,
) -> None:
    conn, _, output_path, sidecar_path = _connection(tmp_path)
    calls = 0

    def run_command(*args, **kwargs):
        nonlocal calls
        calls += 1
        output_path.write_text("{}", encoding="utf-8")
        Path(kwargs["log_path"]).write_text("{}\n", encoding="utf-8")
        return 0, False, 1.0

    usage = {
        "input_tokens": 10,
        "cached_input_tokens": 0,
        "output_tokens": 2,
        "reasoning_output_tokens": 0,
        "total_tokens": 12,
    }
    profile = {
        "usage_event_count": 1,
        "cumulative_billed_total_tokens": 12,
        "cumulative_input_tokens": 10,
        "cumulative_cached_input_tokens": 0,
        "cumulative_output_tokens": 2,
        "last_turn_unique_input_tokens": 10,
        "last_turn_unique_total_tokens": 12,
    }
    with patch(
        "research_factory.windowed_evaluation._context_usage_sidecar_path",
        return_value=sidecar_path,
    ), patch(
        "research_factory.windowed_evaluation._run_codex_smoke_command",
        side_effect=run_command,
    ), patch(
        "research_factory.windowed_evaluation._codex_usage_from_jsonl",
        return_value=usage,
    ), patch(
        "research_factory.headless_codex._usage_profile_from_jsonl",
        return_value=profile,
    ), patch(
        "research_factory.headless_codex._provider_pressure_signals",
        return_value=[],
    ), patch(
        "research_factory.windowed_evaluation._context_output_validation_error",
        side_effect=[
            (False, "JSONDecodeError: exact first error"),
            (False, "JSONDecodeError: exact second error"),
        ],
    ), patch(
        "research_factory.windowed_evaluation._sha256_file",
        return_value="spec-sha",
    ):
        result = execute_instrumented_episode_context_job(
            conn,
            job_id=1,
            worker_id="worker-test",
            timeout_seconds=10,
        )

    job = conn.execute("SELECT * FROM jobs WHERE id = 1").fetchone()
    run = conn.execute(
        "SELECT * FROM episode_context_runs WHERE id = 'ectx-test'"
    ).fetchone()
    corrective_prompt = tmp_path / "ectx-test.corrective-2.md"
    assert calls == 2
    assert result["provider_calls_this_invocation"] == 2
    assert result["provider_retry_count"] == 1
    assert result["status_ok"] is False
    assert "exact first error" in corrective_prompt.read_text(encoding="utf-8")
    assert job["status"] == "failed"
    assert job["lease_owner"] is None
    assert job["leased_until"] is None
    assert run["status"] == "failed"
