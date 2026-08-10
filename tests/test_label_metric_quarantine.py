from __future__ import annotations

import json
import sqlite3

import pytest

from research_factory import db
from research_factory.labels import (
    ValidationError,
    _validate_v31_metric_grounding,
    repair_label_output_for_submission,
)
from research_factory.worker import _insert_label_metric_quarantines


def _event(metric: dict) -> dict:
    return {
        "claim_text": "Revenue increased.",
        "evidence": "revenue increased by twenty percent",
        "metric": metric,
        "quality_flags": [],
    }


def test_ungrounded_metric_is_quarantined_before_repair() -> None:
    original = {
        "value": "20",
        "unit": "percent",
        "comparator": None,
        "direction": "increase",
        "raw_text": "increased by 20 percent",
    }
    payload = {"discourse_events": [_event(dict(original))]}
    quarantines: list[dict] = []

    repairs = repair_label_output_for_submission(
        "ai_discourse_v3_1",
        payload,
        segment_text="revenue increased by twenty percent",
        metric_quarantines=quarantines,
    )

    assert repairs == 1
    assert quarantines[0]["original_metric"] == original
    assert quarantines[0]["failed_rules"] == [
        "raw_text_not_evidence_substring",
    ]
    assert payload["discourse_events"][0]["metric"]["raw_text"] is None
    assert quarantines[0]["original_metric"]["raw_text"] == "increased by 20 percent"


def test_grounded_metric_is_untouched_and_not_quarantined() -> None:
    original = {
        "value": "twenty",
        "unit": "percent",
        "comparator": None,
        "direction": "increase",
        "raw_text": "increased by twenty percent",
    }
    payload = {"discourse_events": [_event(dict(original))]}
    quarantines: list[dict] = []

    repairs = repair_label_output_for_submission(
        "ai_discourse_v3_1",
        payload,
        segment_text="revenue increased by twenty percent",
        metric_quarantines=quarantines,
    )

    assert repairs == 0
    assert quarantines == []
    assert payload["discourse_events"][0]["metric"] == original


def test_direction_only_metric_without_evidence_is_quarantined() -> None:
    # Review-loop Ruling 2 (2026-08-01) revoked the former ungrounded-direction
    # contract: every direction-only metric now requires direction_evidence.
    original = {
        "value": None,
        "unit": None,
        "comparator": None,
        "direction": "increase",
        "raw_text": None,
    }
    payload = {"discourse_events": [_event(dict(original))]}
    quarantines: list[dict] = []

    repairs = repair_label_output_for_submission(
        "ai_discourse_v3_1",
        payload,
        segment_text="revenue increased by twenty percent",
        metric_quarantines=quarantines,
    )

    assert repairs == 1
    assert quarantines[0]["original_metric"] == original
    assert quarantines[0]["failed_rules"] == [
        "direction_evidence_not_evidence_substring"
    ]
    assert payload["discourse_events"][0]["metric"] == {
        "value": None,
        "unit": None,
        "comparator": None,
        "direction": "not_applicable",
        "direction_evidence": None,
        "raw_text": None,
    }


def test_literal_metric_fields_without_raw_text_are_quarantined() -> None:
    original = {
        "value": "twenty",
        "unit": "percent",
        "comparator": None,
        "direction": "increase",
        "raw_text": None,
    }
    payload = {"discourse_events": [_event(dict(original))]}
    quarantines: list[dict] = []

    repairs = repair_label_output_for_submission(
        "ai_discourse_v3_1",
        payload,
        segment_text="revenue increased by twenty percent",
        metric_quarantines=quarantines,
    )

    assert repairs == 1
    assert quarantines[0]["failed_rules"] == [
        "raw_text_not_evidence_substring"
    ]


def test_validator_requires_direction_evidence_and_literal_raw_text() -> None:
    direction_only = _event(
        {
            "value": None,
            "unit": None,
            "comparator": None,
            "direction": "increase",
            "raw_text": None,
        }
    )
    with pytest.raises(
        ValidationError, match="direction_evidence_not_evidence_substring"
    ):
        _validate_v31_metric_grounding(0, direction_only)

    grounded_direction = _event(
        {
            "value": None,
            "unit": None,
            "comparator": None,
            "direction": "increase",
            "direction_evidence": "increased",
            "raw_text": None,
        }
    )
    _validate_v31_metric_grounding(0, grounded_direction)

    missing_raw = _event(
        {
            "value": "twenty",
            "unit": "percent",
            "comparator": None,
            "direction": "increase",
            "raw_text": None,
        }
    )
    with pytest.raises(ValidationError, match="raw_text_not_evidence_substring"):
        _validate_v31_metric_grounding(0, missing_raw)


def test_dropped_prior_event_does_not_create_phantom_metric_quarantine() -> None:
    grounded = {
        "value": "twenty",
        "unit": "percent",
        "comparator": None,
        "direction": "increase",
        "raw_text": "increased by twenty percent",
    }
    dropped = _event(
        {
            "value": None,
            "unit": None,
            "comparator": None,
            "direction": "not_applicable",
            "raw_text": None,
        }
    )
    dropped["evidence"] = "not present in the segment"
    payload = {"discourse_events": [dropped, _event(dict(grounded))]}
    quarantines: list[dict] = []

    repair_label_output_for_submission(
        "ai_discourse_v3_1",
        payload,
        segment_text="revenue increased by twenty percent",
        metric_quarantines=quarantines,
    )

    assert len(payload["discourse_events"]) == 1
    assert payload["discourse_events"][0]["metric"] == grounded
    assert quarantines == []


def test_quarantine_row_retains_original_payload_after_repair() -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE segments (id TEXT PRIMARY KEY);
        CREATE TABLE label_runs (id TEXT PRIMARY KEY);
        CREATE TABLE labels (id TEXT PRIMARY KEY);
        INSERT INTO segments VALUES ('seg_1');
        INSERT INTO label_runs VALUES ('run_1');
        INSERT INTO labels VALUES ('lbl_1');
        """
    )
    for statement in db.LABEL_METRIC_QUARANTINE_V5:
        conn.execute(statement)
    original = {
        "value": "20",
        "unit": "percent",
        "comparator": None,
        "direction": "increase",
        "raw_text": "increased by 20 percent",
    }
    payload = {"discourse_events": [_event(dict(original))]}
    quarantines: list[dict] = []
    repair_label_output_for_submission(
        "ai_discourse_v3_1",
        payload,
        segment_text="revenue increased by twenty percent",
        metric_quarantines=quarantines,
    )

    assert _insert_label_metric_quarantines(
        conn,
        label_id="lbl_1",
        label_run_id="run_1",
        segment_id="seg_1",
        quarantines=quarantines,
    ) == 1
    row = conn.execute("SELECT * FROM label_metric_quarantines").fetchone()
    assert json.loads(row["original_metric_json"]) == original
    assert json.loads(row["failed_rules_json"]) == [
        "raw_text_not_evidence_substring",
    ]
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        conn.execute(
            "UPDATE label_metric_quarantines SET evidence = 'changed' WHERE id = ?",
            (row["id"],),
        )
    conn.close()
