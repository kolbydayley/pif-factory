from __future__ import annotations

import copy
import json

import pytest

from research_factory import app_server_canonical_v31_single_message_compact_pointer_episode_batch as single
from test_app_server_canonical_v31_bounded_span_episode_batch import _coded_output, _episode
from test_app_server_canonical_v31_compact_unit_pointer_episode_batch import (
    EPOCH13_ROOT,
    EPOCH15_LABELS,
    _exact_range_for_literal,
    _token_index,
)


def _request_and_output() -> tuple[dict, dict]:
    request = single.prepare_episode_batches(
        _episode("Host: New Atlas cuts inference latency by 40 percent.\n"),
        batch_size=3,
        thread_mode="new_thread",
    )[0]
    compact_request = single._compact_request(request)
    local_request = single.compact._local_request(compact_request)
    output = _coded_output(single.compact.local._bounded_request(local_request))
    unit = request["private_input"]["segments"][0]["units"][0]
    value = _token_index(unit, "40")
    percent = _token_index(unit, "percent")
    output["segments"][0]["discourse_events"][0]["metric"] = {
        "direction": "decrease",
        "value_range": [0, value, 0, value],
        "unit_range": [0, percent, 0, percent],
        "comparator_range": None,
        "raw_text_range": [0, value, 0, percent],
    }
    return request, output


def test_request_adds_exact_single_message_contract_without_semantic_schema_change() -> None:
    request, _output = _request_and_output()
    compact_request = single._compact_request(request)
    assert request == single.validate_prepared_request(request)
    assert request["base_instructions"].startswith(single.SINGLE_MESSAGE_INSTRUCTIONS)
    assert request["base_instructions"].count(single.SINGLE_MESSAGE_INSTRUCTIONS) == 1
    assert request["output_schema"] == compact_request["output_schema"]
    assert request["prompt"] == compact_request["prompt"]
    assert request["semantic_integrity"][
        "exactly_one_final_structured_agent_message_requested"
    ] is True


def test_single_message_compact_projection_preserves_exact_metric() -> None:
    request, output = _request_and_output()
    untouched = copy.deepcopy(output)
    projected = single.validate_and_project_output(request, output)
    assert output == untouched
    assert projected["labels"][0]["discourse_events"][0]["metric"] == {
        "value": "40",
        "unit": "percent",
        "comparator": None,
        "direction": "decrease",
        "raw_text": "40 percent",
    }
    assert projected["fidelity"]["single_final_structured_message_requested"] is True


def test_all_epoch15_metrics_remain_exactly_representable() -> None:
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
    request = single.prepare_episode_batches(
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
            for field in single.METRIC_LITERAL_FIELDS:
                literal = exact_event["metric"][field]
                metric[f"{field}_range"] = (
                    None
                    if literal is None
                    else _exact_range_for_literal(source, span, literal)
                )
                pointer_count += int(literal is not None)
            event["metric"] = metric
    projected = single.validate_and_project_output(request, output)
    assert projected["labels"] == exact_labels
    assert pointer_count == 77


def test_single_message_instruction_tamper_fails_closed() -> None:
    request, _output = _request_and_output()
    request["base_instructions"] = request["base_instructions"].replace(
        "exactly one final", "one or more final", 1
    )
    request["base_instructions_sha256"] = single.base.sha256_text(
        request["base_instructions"]
    )
    with pytest.raises(single.CanonicalV31EpisodeBatchError, match="instructions drifted"):
        single.validate_prepared_request(request)
