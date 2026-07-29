from __future__ import annotations

import json
from unittest.mock import patch

from research_factory.instrumented_backfill import _historic_baseline


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
