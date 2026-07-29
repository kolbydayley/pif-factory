from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import pytest

from research_factory import app_server_canonical_v31_unit_local_pointer_episode_batch as local
from test_app_server_canonical_v31_bounded_span_episode_batch import (
    _coded_output,
    _episode,
)


EPOCH13_ROOT = Path(
    "work/app-server-development-v2/unattended-pipeline-v5/"
    "canonical-v31-epoch13-bounded-span-canary-v1"
)
EPOCH15_LABELS = Path(
    "work/app-server-development-v2/unattended-pipeline-v5/"
    "canonical-v31-epoch15-metric-compiler-canary-v1/turn/"
    "canonical-labels.private.json"
)
FROZEN_ARTIFACT_SHA256 = {
    EPOCH13_ROOT / "prepared-turn/request.private.json": (
        "7f2e23b294164e0a530435f4ef00719f0e9f63754d3fb054460dc412cca53f6f"
    ),
    EPOCH13_ROOT / "turn/output.private.json": (
        "202c0cd1d9de66b7b999e49f3ac5d094886dcb32bf8845a22deda0f8f9a8f418"
    ),
    EPOCH15_LABELS: (
        "fa0f9c0c8a9edb569f98c3a1f19f817465632a645a60a3f2ae195f0784fb282c"
    ),
}


def _token_index(unit: dict, text: str) -> int:
    rows = [
        row["literal_token_index"]
        for row in unit["literal_tokens"]
        if unit["text"][
            row["start_char"] - unit["start_char"] : row["end_char"] - unit["start_char"]
        ]
        == text
    ]
    assert len(rows) == 1
    return rows[0]


def _request_and_output() -> tuple[dict, dict]:
    request = local.prepare_episode_batches(
        _episode("Host: New Atlas cuts inference latency by 40 percent.\n"),
        batch_size=3,
        thread_mode="new_thread",
    )[0]
    output = _coded_output(local._bounded_request(request))
    unit = request["private_input"]["segments"][0]["units"][0]
    value = _token_index(unit, "40")
    percent = _token_index(unit, "percent")
    event = output["segments"][0]["discourse_events"][0]
    event["metric"] = {
        "direction": "decrease",
        "value_start_source_unit_id": unit["unit_id"],
        "value_start_token_index": value,
        "value_end_source_unit_id": unit["unit_id"],
        "value_end_token_index": value,
        "unit_start_source_unit_id": unit["unit_id"],
        "unit_start_token_index": percent,
        "unit_end_source_unit_id": unit["unit_id"],
        "unit_end_token_index": percent,
        "comparator_start_source_unit_id": None,
        "comparator_start_token_index": None,
        "comparator_end_source_unit_id": None,
        "comparator_end_token_index": None,
        "raw_text_start_source_unit_id": unit["unit_id"],
        "raw_text_start_token_index": value,
        "raw_text_end_source_unit_id": unit["unit_id"],
        "raw_text_end_token_index": percent,
    }
    output["segments"][0]["unit_receipts"][0]["grounded_event_count"] = 0
    return request, output


def test_request_exposes_only_local_token_counts_not_duplicate_catalogs() -> None:
    request, _output = _request_and_output()
    assert request == local.validate_prepared_request(request)
    assert "literal_token_count" in request["prompt"]
    assert "literal_token_texts" not in request["prompt"]
    assert request["semantic_integrity"][
        "model_selects_unit_local_exact_metric_token_ranges"
    ] is True


def test_unit_local_metric_projection_is_exact_and_reconciles_coverage() -> None:
    request, output = _request_and_output()
    untouched = copy.deepcopy(output)
    projected = local.validate_and_project_output(request, output)
    assert output == untouched
    assert projected["labels"][0]["discourse_events"][0]["metric"] == {
        "value": "40",
        "unit": "percent",
        "comparator": None,
        "direction": "decrease",
        "raw_text": "40 percent",
    }
    assert projected["provenance"][
        "deterministic_coverage_owner_count_records"
    ][0]["event_count"] == 1


def test_partial_or_invalid_pointer_fails_closed() -> None:
    request, output = _request_and_output()
    output["segments"][0]["discourse_events"][0]["metric"][
        "raw_text_end_token_index"
    ] = 9999
    with pytest.raises(local.CanonicalV31OutputError, match="token index is invalid"):
        local.validate_and_project_output(request, output)


def _exact_pointer_for_literal(
    source: dict, span: dict, literal: str
) -> tuple[tuple[str, int], tuple[str, int]]:
    starts: dict[int, list[tuple[str, int]]] = {}
    ends: dict[int, list[tuple[str, int]]] = {}
    for unit in source["units"]:
        for token in unit["literal_tokens"]:
            coordinate = (unit["unit_id"], token["literal_token_index"])
            starts.setdefault(token["start_char"], []).append(coordinate)
            ends.setdefault(token["end_char"], []).append(coordinate)
    offset = source["segment_text"].find(
        literal, span["start_char"], span["end_char"]
    )
    while offset >= 0 and offset + len(literal) <= span["end_char"]:
        if offset in starts and offset + len(literal) in ends:
            return starts[offset][0], ends[offset + len(literal)][0]
        offset = source["segment_text"].find(
            literal, offset + 1, span["end_char"]
        )
    raise AssertionError(f"exact metric literal is not token-range representable: {literal!r}")


def test_all_epoch15_exact_metrics_are_losslessly_unit_local_representable() -> None:
    for path, expected_sha256 in FROZEN_ARTIFACT_SHA256.items():
        assert hashlib.sha256(path.read_bytes()).hexdigest() == expected_sha256
    source_request = json.loads(
        (EPOCH13_ROOT / "prepared-turn/request.private.json").read_text(
            encoding="utf-8"
        )
    )
    output = json.loads(
        (EPOCH13_ROOT / "turn/output.private.json").read_text(encoding="utf-8")
    )
    exact_labels = json.loads(EPOCH15_LABELS.read_text(encoding="utf-8"))
    request = local._pointer_request(source_request)
    pointer_count = 0
    for raw_segment, exact_label, source in zip(
        output["segments"], exact_labels, request["private_input"]["segments"]
    ):
        assert raw_segment["segment_id"] == exact_label["segment_id"]
        spans = {row["evidence_span_id"]: row for row in source["evidence_spans"]}
        for event, exact_event in zip(
            raw_segment["discourse_events"], exact_label["discourse_events"]
        ):
            metric = {"direction": exact_event["metric"]["direction"]}
            span = spans[event["evidence_span_id"]]
            for field in local.METRIC_LITERAL_FIELDS:
                literal = exact_event["metric"][field]
                start = end = None
                if literal is not None:
                    start, end = _exact_pointer_for_literal(source, span, literal)
                    pointer_count += 1
                metric[f"{field}_start_source_unit_id"] = (
                    None if start is None else start[0]
                )
                metric[f"{field}_start_token_index"] = (
                    None if start is None else start[1]
                )
                metric[f"{field}_end_source_unit_id"] = None if end is None else end[0]
                metric[f"{field}_end_token_index"] = None if end is None else end[1]
            event["metric"] = metric
    projected = local.validate_and_project_output(request, output)
    assert projected["labels"] == exact_labels
    assert len(projected["labels"]) == 6
    assert sum(len(row["discourse_events"]) for row in projected["labels"]) == 56
    assert pointer_count == 77
