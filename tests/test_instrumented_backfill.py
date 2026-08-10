from __future__ import annotations

import contextlib
import json
from unittest.mock import patch

import pytest

from research_factory.instrumented_backfill import (
    _historic_baseline,
    _run_context_batch,
    run_instrumented_backfill,
)


class _Rows:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _BaselineConnection:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, sql, params=()):
        assert "FROM labels" in sql
        return _Rows(self.rows)


def _label(label_id: str, segment_id: str) -> dict:
    return {
        "id": label_id,
        "segment_id": segment_id,
        "label_pack": "ai_discourse_v3_1",
        "output_json": json.dumps(
            {
                "discourse_events": [
                    {
                        "evidence": "Exact evidence.",
                        "evidence_start": 0,
                        "evidence_end": 15,
                    }
                ]
            }
        ),
    }


def test_historical_baseline_skips_unreadable_segment_instead_of_aborting() -> None:
    conn = _BaselineConnection(
        [_label("label-bad", "segment-bad"), _label("label-good", "segment-good")]
    )

    def segment(_, job):
        if job["target_id"] == "segment-bad":
            raise ValueError("segment_read_timeout")
        return {
            "segment_text": "Exact evidence.",
            "context": {},
        }

    with patch(
        "research_factory.instrumented_backfill.segment_for_job",
        side_effect=segment,
    ), patch(
        "research_factory.instrumented_backfill.audit_label_grounding",
        return_value={"status": "passed"},
    ):
        result = _historic_baseline(
            conn,
            before="2026-07-29T00:00:00+00:00",
            limit=400,
            max_seconds=10,
        )

    assert result["available"] is True
    assert result["labels_measured"] == 1
    assert result["labels_skipped"] == 1
    assert result["skip_reasons"] == {"ValueError": 1}
    assert result["baseline_unavailable"] is None


def test_run_context_batch_no_ops_when_no_jobs_are_eligible() -> None:
    assert _run_context_batch([], "empty-batch-worker", 900) == []


@pytest.mark.parametrize("concurrency", [3, 6, 10, 16, 24, 32])
def test_label_only_authorized_concurrency_steps_reach_lock(
    concurrency: int,
) -> None:
    connection = type("_Connection", (), {"close": lambda self: None})()
    with patch(
        "research_factory.instrumented_backfill.db.connect",
        return_value=connection,
    ) as connect, patch(
        "research_factory.instrumented_backfill.pipeline_lock",
        return_value=contextlib.nullcontext(False),
    ):
        result = run_instrumented_backfill(
            max_contexts=0,
            max_labels=0,
            concurrency=concurrency,
            label_only_existing_context=True,
        )
    assert result == {"ok": False, "stopped": True, "reason": "pipeline_lock_busy"}
    connect.assert_called_once()


def test_label_only_rejects_unapproved_concurrency() -> None:
    with pytest.raises(ValueError, match="authorized steps"):
        run_instrumented_backfill(
            max_contexts=0,
            max_labels=0,
            concurrency=12,
            label_only_existing_context=True,
        )
