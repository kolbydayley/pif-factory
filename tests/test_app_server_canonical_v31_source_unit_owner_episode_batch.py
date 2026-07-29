from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from research_factory import app_server_canonical_v31_source_unit_owner_episode_batch as owner
from test_app_server_canonical_v31_bounded_span_episode_batch import (
    _coded_output,
    _episode,
)


def _request_and_output() -> tuple[dict, dict]:
    request = owner.prepare_episode_batches(
        _episode("Host: New Atlas cuts inference latency by 40 percent.\n"),
        batch_size=3,
        thread_mode="new_thread",
    )[0]
    bounded = owner._bounded_request(request)
    output = _coded_output(bounded)
    source = request["private_input"]["segments"][0]
    event = output["segments"][0]["discourse_events"][0]
    metric = event["metric"]
    metric.update(
        {
            "value": "40",
            "unit": "percent",
            "comparator": None,
            "direction": "decrease",
            "raw_text": "40 percent",
            "value_start_source_unit_id": source["units"][0]["unit_id"],
            "value_end_source_unit_id": source["units"][0]["unit_id"],
            "unit_start_source_unit_id": source["units"][0]["unit_id"],
            "unit_end_source_unit_id": source["units"][0]["unit_id"],
            "comparator_start_source_unit_id": None,
            "comparator_end_source_unit_id": None,
            "raw_text_start_source_unit_id": source["units"][0]["unit_id"],
            "raw_text_end_source_unit_id": source["units"][0]["unit_id"],
        }
    )
    output["segments"][0]["unit_receipts"][0]["grounded_event_count"] = 0
    return request, output


def test_request_adds_source_unit_owner_contract_without_token_catalog() -> None:
    request, _output = _request_and_output()
    assert request == owner.validate_prepared_request(request)
    assert "literal_token_texts" not in request["prompt"]
    metric = request["output_schema"]["properties"]["segments"]["items"][
        "properties"
    ]["discourse_events"]["items"]["properties"]["metric"]
    assert "raw_text_start_source_unit_id" in metric["required"]
    assert "raw_text_end_source_unit_id" in metric["required"]
    assert (
        request["semantic_integrity"][
            "model_selects_source_unit_span_for_every_nonnull_metric_literal"
        ]
        is True
    )


def test_exact_source_unit_metric_projection_and_coverage_reconciliation() -> None:
    request, output = _request_and_output()
    untouched = copy.deepcopy(output)
    projected = owner.validate_and_project_output(request, output)
    assert output == untouched
    metric = projected["labels"][0]["discourse_events"][0]["metric"]
    assert metric == {
        "value": "40",
        "unit": "percent",
        "comparator": None,
        "direction": "decrease",
        "raw_text": "40 percent",
    }
    assert projected["provenance"]["segments"][0]["discourse_events"][0][
        "metric_source_unit_records"
    ][0]["start_source_unit_id"] == request["private_input"]["segments"][0][
        "units"
    ][0]["unit_id"]
    assert projected["provenance"][
        "deterministic_coverage_owner_count_records"
    ][0]["event_count"] == 1


def test_nonexact_metric_literal_fails_closed() -> None:
    request, output = _request_and_output()
    output["segments"][0]["discourse_events"][0]["metric"][
        "raw_text"
    ] = "a 40% improvement"
    with pytest.raises(owner.CanonicalV31OutputError, match="not an exact substring"):
        owner.validate_and_project_output(request, output)


def test_null_literal_requires_null_source_unit_owner() -> None:
    request, output = _request_and_output()
    output["segments"][0]["discourse_events"][0]["metric"][
        "comparator_start_source_unit_id"
    ] = request["private_input"]["segments"][0]["units"][0]["unit_id"]
    with pytest.raises(owner.CanonicalV31OutputError, match="all be null or non-null"):
        owner.validate_and_project_output(request, output)


def test_epoch15_exact_metrics_are_losslessly_representable_in_window_zero() -> None:
    base_root = Path(
        "work/app-server-development-v2/unattended-pipeline-v5/"
        "canonical-v31-epoch13-bounded-span-canary-v1"
    )
    source_request = json.loads(
        (base_root / "prepared-turn/request.private.json").read_text(encoding="utf-8")
    )
    source_output = json.loads(
        (base_root / "turn/output.private.json").read_text(encoding="utf-8")
    )
    exact_labels = json.loads(
        Path(
            "work/app-server-development-v2/unattended-pipeline-v5/"
            "canonical-v31-epoch15-metric-compiler-canary-v1/turn/"
            "canonical-labels.private.json"
        ).read_text(encoding="utf-8")
    )
    episode = copy.deepcopy(source_request["episode_context"])
    episode["segments"] = [
        {
            key: copy.deepcopy(segment[key])
            for key in (
                "segment_id",
                "segment_text",
                "segment_quality",
                "density_stratum",
                "boundaries",
            )
        }
        for segment in source_request["private_input"]["segments"][:3]
    ]
    request = owner.prepare_episode_batches(
        episode, batch_size=3, thread_mode="new_thread"
    )[0]
    output = {
        "episode_id": source_output["episode_id"],
        "segments": copy.deepcopy(source_output["segments"][:3]),
    }
    for raw_segment, label, source in zip(
        output["segments"], exact_labels[:3], request["private_input"]["segments"]
    ):
        spans = {row["evidence_span_id"]: row for row in source["evidence_spans"]}
        for event, exact_event in zip(
            raw_segment["discourse_events"], label["discourse_events"]
        ):
            event["metric"] = copy.deepcopy(exact_event["metric"])
            span = spans[event["evidence_span_id"]]
            for field in owner.METRIC_LITERAL_FIELDS:
                literal = event["metric"][field]
                start_owner = end_owner = None
                if literal is not None:
                    start = source["segment_text"].find(
                        literal, span["start_char"], span["end_char"]
                    )
                    assert start >= 0
                    end = start + len(literal) - 1
                    start_owner = next(
                        row["unit_id"]
                        for row in source["units"]
                        if row["start_char"] <= start < row["end_char"]
                    )
                    end_owner = next(
                        row["unit_id"]
                        for row in source["units"]
                        if row["start_char"] <= end < row["end_char"]
                    )
                event["metric"][f"{field}_start_source_unit_id"] = start_owner
                event["metric"][f"{field}_end_source_unit_id"] = end_owner
    projected = owner.validate_and_project_output(request, output)
    assert len(projected["labels"]) == 3
    assert sum(len(row["discourse_events"]) for row in projected["labels"]) == 28
