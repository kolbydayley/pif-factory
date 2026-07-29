from __future__ import annotations

import asyncio
import json
import sqlite3
from pathlib import Path

import pytest

from research_factory.app_server_judge_v5_selection_v204_fresh_exhaustive_diagnostic import (
    _validate_v203_authorization,
)
from research_factory.app_server_judge_v5_selection_v205_fresh_exhaustive_recovery import (
    EXPECTED_V204_ERROR_MESSAGE_SHA256,
    JudgeV5SelectionV205Error,
    _validate_v204_presemantic_failure,
    freeze_v205,
    run_v205,
    verify_runtime_lock,
)


def _write_normalized_outputs(root: Path, manifest: dict) -> None:
    rows = []
    for episode in manifest["episodes"]:
        for segment in episode["segments"]:
            golden = int(segment["golden_event_count"])
            count = 0 if segment["density_stratum"] == "no_signal" else round(golden * 0.8)
            rows.append(
                {
                    "segment_id": segment["segment_id"],
                    "events": [{"opaque": index} for index in range(count)],
                }
            )
    output = root / "normalized_outputs" / "all.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps({"episode_id": "synthetic", "segments": rows}) + "\n",
        encoding="utf-8",
    )


def _passing_report() -> dict:
    return {
        "validated_segments": 16,
        "schema_status_success_rate": 1.0,
        "attempted_calls": 4,
        "accounting_complete": True,
        "usage_status": "complete",
        "usage_measured_attempts": 4,
        "usage_unknown_attempts": 0,
        "normalized_exact_evidence_rate": 1.0,
        "no_signal_candidate_positive_segments_unadjudicated": 0,
        "metric_grounding_error_events": 0,
        "usage": {
            "input_tokens": 380_000,
            "cached_input_tokens": 10_000,
            "output_tokens": 20_000,
            "reasoning_output_tokens": 10_000,
            "total_tokens": 400_000,
        },
    }


def test_v205_binds_exact_v204_failure_as_presemantic_infrastructure_evidence():
    predecessor = _validate_v204_presemantic_failure()
    assert predecessor["failure"]["error_class"] == "TypeError"
    assert (
        predecessor["failure"]["error_message_sha256"]
        == EXPECTED_V204_ERROR_MESSAGE_SHA256
    )
    assert predecessor["failure"]["attempted_turn_count"] == 0
    assert predecessor["failure"]["unknown_usage_turn_count"] == 0
    assert predecessor["terminal"]["usage"]["total_tokens"] == 0
    assert not list(predecessor["root"].rglob("capacity.json"))


def test_v205_freezes_one_recovery_without_semantic_artifacts(tmp_path: Path):
    root = tmp_path / "v205"
    first = freeze_v205(output_dir=root)
    second = freeze_v205(output_dir=root)
    assert first["turn_names"] == second["turn_names"]
    assert len(first["turn_names"]) == 4
    lock = verify_runtime_lock(first["runtime_lock"])
    assert len(lock["v204_attempt"]) == 7
    spec = json.loads((root / "attempt-spec.json").read_text())
    assert spec["recovery_scope"] == "sqlite_row_factory_only"
    assert spec["v204_root_cause"]["semantic_turns_started"] == 0
    assert spec["retry_count_per_turn"] == 0
    assert spec["holdout_authorized"] is False
    assert not (root / "launch-receipt.json").exists()
    assert not list(root.rglob("capacity.json"))
    assert not list(root.rglob("sidecar.json"))


def test_v205_runtime_lock_rejects_missing_runtime_file(tmp_path: Path):
    frozen = freeze_v205(output_dir=tmp_path / "v205")
    lock_path = frozen["runtime_lock"]
    value = json.loads(lock_path.read_text())
    value["runtime_files"] = value["runtime_files"][1:]
    lock_path.write_text(json.dumps(value) + "\n", encoding="utf-8")
    with pytest.raises(JudgeV5SelectionV205Error, match="runtime lock drifted"):
        verify_runtime_lock(lock_path)


def test_v205_real_runner_boundary_sets_sqlite_row_factory_and_never_replays(
    tmp_path: Path,
):
    root = tmp_path / "v205"
    calls = 0

    async def arm_runner(conn, **kwargs):
        nonlocal calls
        calls += 1
        assert conn.row_factory is sqlite3.Row
        assert (root / "launch-receipt.json").is_file()
        assert kwargs["model"] == "gpt-5.6-sol"
        assert kwargs["reasoning_effort"] == "high"
        assert kwargs["concurrency"] == 1
        manifest = _validate_v203_authorization()["manifest"]
        _write_normalized_outputs(root / "arm", manifest)
        report = _passing_report()
        (root / "arm" / "report.json").write_text(
            json.dumps(report) + "\n", encoding="utf-8"
        )
        return report

    first = asyncio.run(
        run_v205(
            output_dir=root,
            database_path=Path("data/factory.sqlite"),
            arm_runner=arm_runner,
        )
    )
    second = asyncio.run(
        run_v205(
            output_dir=root,
            database_path=Path("data/factory.sqlite"),
            arm_runner=arm_runner,
        )
    )
    assert first == second
    assert calls == 1
    assert first["terminal_reason"].startswith("v205_fresh_exhaustive")
    assert first["fresh_judge_authorized"] is True
    assert first["development_winner_frozen"] is False
    assert first["holdout_authorized"] is False
    assert first["production_mutated"] is False
    assert first["semantic_retry_allowed"] is False
