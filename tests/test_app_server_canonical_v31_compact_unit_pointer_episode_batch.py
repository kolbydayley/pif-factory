from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import pytest

from research_factory import app_server_canonical_v31_compact_unit_pointer_episode_batch as compact
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
    matches = [
        row["literal_token_index"]
        for row in unit["literal_tokens"]
        if unit["text"][
            row["start_char"] - unit["start_char"] : row["end_char"]
            - unit["start_char"]
        ]
        == text
    ]
    assert len(matches) == 1
    return matches[0]


def _request_and_output() -> tuple[dict, dict]:
    request = compact.prepare_episode_batches(
        _episode("Host: New Atlas cuts inference latency by 40 percent.\n"),
        batch_size=3,
        thread_mode="new_thread",
    )[0]
    local_request = compact._local_request(request)
    output = _coded_output(compact.local._bounded_request(local_request))
    unit = request["private_input"]["segments"][0]["units"][0]
    value = _token_index(unit, "40")
    percent = _token_index(unit, "percent")
    event = output["segments"][0]["discourse_events"][0]
    event["metric"] = {
        "direction": "decrease",
        "value_range": [0, value, 0, value],
        "unit_range": [0, percent, 0, percent],
        "comparator_range": None,
        "raw_text_range": [0, value, 0, percent],
    }
    output["segments"][0]["unit_receipts"][0]["grounded_event_count"] = 0
    return request, output


def test_request_uses_compact_ranges_and_smaller_schema() -> None:
    request, _output = _request_and_output()
    assert request == compact.validate_prepared_request(request)
    assert "source_unit_index" in request["prompt"]
    assert "literal_token_count" in request["prompt"]
    assert "literal_token_texts" not in request["prompt"]
    metric = request["output_schema"]["properties"]["segments"]["items"][
        "properties"
    ]["discourse_events"]["items"]["properties"]["metric"]
    assert set(metric["properties"]) == {
        "direction",
        "value_range",
        "unit_range",
        "comparator_range",
        "raw_text_range",
    }
    assert all(
        metric["properties"][f"{field}_range"]["maxItems"] == 4
        for field in compact.METRIC_LITERAL_FIELDS
    )
    local_request = compact._local_request(request)
    assert len(compact._canonical_json(request["output_schema"]).encode("utf-8")) < len(
        compact._canonical_json(local_request["output_schema"]).encode("utf-8")
    )


def test_compact_metric_ranges_project_exactly_and_reconcile_coverage() -> None:
    request, output = _request_and_output()
    untouched = copy.deepcopy(output)
    projected = compact.validate_and_project_output(request, output)
    assert output == untouched
    assert projected["labels"][0]["discourse_events"][0]["metric"] == {
        "value": "40",
        "unit": "percent",
        "comparator": None,
        "direction": "decrease",
        "raw_text": "40 percent",
    }
    assert projected["provenance"]["segments"][0]["discourse_events"][0][
        "metric_compact_range_records"
    ][0]["compact_range"] == [0, _token_index(
        request["private_input"]["segments"][0]["units"][0], "40"
    ), 0, _token_index(request["private_input"]["segments"][0]["units"][0], "40")]
    assert projected["fidelity"][
        "model_selected_compact_metric_ranges_projected_exactly"
    ] is True


def test_invalid_compact_range_fails_closed() -> None:
    request, output = _request_and_output()
    output["segments"][0]["discourse_events"][0]["metric"]["raw_text_range"][
        3
    ] = 9999
    with pytest.raises(compact.CanonicalV31OutputError, match="token index is invalid"):
        compact.validate_and_project_output(request, output)


def _exact_range_for_literal(
    source: dict, span: dict, literal: str
) -> list[int]:
    starts: dict[int, tuple[int, int]] = {}
    ends: dict[int, tuple[int, int]] = {}
    for unit_index, unit in enumerate(source["units"]):
        for token in unit["literal_tokens"]:
            starts[token["start_char"]] = (unit_index, token["literal_token_index"])
            ends[token["end_char"]] = (unit_index, token["literal_token_index"])
    offset = source["segment_text"].find(
        literal, span["start_char"], span["end_char"]
    )
    while offset >= 0 and offset + len(literal) <= span["end_char"]:
        if offset in starts and offset + len(literal) in ends:
            return [*starts[offset], *ends[offset + len(literal)]]
        offset = source["segment_text"].find(
            literal, offset + 1, span["end_char"]
        )
    raise AssertionError(f"exact metric literal is not compact-range representable: {literal!r}")


def test_all_epoch15_exact_metrics_are_losslessly_compact_range_representable() -> None:
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
        for segment in source_request["private_input"]["segments"]
    ]
    request = compact.prepare_episode_batches(
        episode, batch_size=8, thread_mode="new_thread"
    )[0]
    pointer_count = 0
    for raw_segment, exact_label, source in zip(
        output["segments"], exact_labels, request["private_input"]["segments"]
    ):
        spans = {row["evidence_span_id"]: row for row in source["evidence_spans"]}
        for event, exact_event in zip(
            raw_segment["discourse_events"], exact_label["discourse_events"]
        ):
            metric = {"direction": exact_event["metric"]["direction"]}
            span = spans[event["evidence_span_id"]]
            for field in compact.METRIC_LITERAL_FIELDS:
                literal = exact_event["metric"][field]
                metric[f"{field}_range"] = (
                    None
                    if literal is None
                    else _exact_range_for_literal(source, span, literal)
                )
                pointer_count += int(literal is not None)
            event["metric"] = metric
    projected = compact.validate_and_project_output(request, output)
    assert projected["labels"] == exact_labels
    assert sum(len(row["discourse_events"]) for row in projected["labels"]) == 56
    assert pointer_count == 77


def test_request_or_source_index_tamper_is_rejected() -> None:
    request, _output = _request_and_output()
    packet = json.loads(request["prompt"].split("\n", 1)[1])
    packet["segments"][0]["source_units"][0]["source_unit_index"] = 7
    request["prompt"] = (
        "# Canonical v3.1 compact unit-pointer packet\n"
        + json.dumps(packet, ensure_ascii=True, separators=(",", ":"))
        + "\n"
    )
    request["prompt_sha256"] = compact.base.sha256_text(request["prompt"])
    with pytest.raises(compact.CanonicalV31EpisodeBatchError, match="index drifted"):
        compact.validate_prepared_request(request)
