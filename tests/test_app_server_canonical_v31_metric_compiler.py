from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from research_factory import app_server_canonical_v31_bounded_span_episode_batch as bounded
from research_factory import app_server_canonical_v31_metric_compiler as compiler
from test_app_server_canonical_v31_bounded_span_episode_batch import (
    _coded_output,
    _episode,
)


EPOCH13_ROOT = Path(
    "work/app-server-development-v2/unattended-pipeline-v5/"
    "canonical-v31-epoch13-bounded-span-canary-v1"
)


def _fixture_request_and_output() -> tuple[dict, dict]:
    request = bounded.prepare_episode_batches(
        _episode("Host: New Atlas cuts inference latency by 40 percent.\n"),
        batch_size=3,
        thread_mode="new_thread",
    )[0]
    output = _coded_output(request)
    output["segments"][0]["discourse_events"][0]["metric"][
        "raw_text"
    ] = "a 40% improvement"
    output["segments"][0]["unit_receipts"][0]["grounded_event_count"] = 0
    return request, output


def _index(case: dict, text: str) -> int:
    matches = [
        token["literal_token_index"]
        for token in case["literal_tokens"]
        if token["text"] == text
    ]
    assert len(matches) == 1
    return matches[0]


def _valid_compiler_output(request: dict) -> dict:
    case = request["private_input"]["metric_cases"][0]
    value = _index(case, "40")
    unit = _index(case, "percent")
    return {
        "metric_repairs": [
            {
                "metric_case_id": case["metric_case_id"],
                "direction": "decrease",
                "value_start_token_index": value,
                "value_end_token_index": value,
                "unit_start_token_index": unit,
                "unit_end_token_index": unit,
                "comparator_start_token_index": None,
                "comparator_end_token_index": None,
                "raw_text_start_token_index": value,
                "raw_text_end_token_index": unit,
            }
        ]
    }


def test_prepare_selects_every_and_only_exactness_disputed_metric_case() -> None:
    source_request, source_output = _fixture_request_and_output()
    request = compiler.prepare_request(source_request, source_output)
    assert request == compiler.validate_prepared_request(request)
    assert request["effective_batch_size"] == 1
    assert request["semantic_integrity"]["applicable_metric_case_count"] == 1
    assert request["semantic_integrity"]["exact_metric_case_count_preserved"] == 0
    case = request["private_input"]["metric_cases"][0]
    assert case["disputed_fields"] == ["raw_text"]
    assert "evidence" not in json.loads(request["prompt"].split("\n", 1)[1])[
        "metric_cases"
    ][0]


def test_llm_metric_projection_and_structural_coverage_reconciliation_pass() -> None:
    source_request, source_output = _fixture_request_and_output()
    request = compiler.prepare_request(source_request, source_output)
    output = _valid_compiler_output(request)
    untouched = copy.deepcopy(output)
    projected = compiler.validate_and_project_output(request, output)
    assert output == untouched
    metric = projected["labels"][0]["discourse_events"][0]["metric"]
    assert metric == {
        "value": "40",
        "unit": "percent",
        "comparator": None,
        "direction": "decrease",
        "raw_text": "40 percent",
    }
    coverage = projected["provenance"][
        "deterministic_coverage_owner_count_records"
    ][0]
    assert coverage["event_count"] == 1
    assert projected["fidelity"]["deterministic_coverage_owner_counts_only"] is True
    assert projected["fidelity"]["deterministic_semantic_defaults"] == {}


def test_invalid_or_reordered_metric_pointer_fails_closed() -> None:
    source_request, source_output = _fixture_request_and_output()
    request = compiler.prepare_request(source_request, source_output)
    output = _valid_compiler_output(request)
    output["metric_repairs"][0]["raw_text_start_token_index"] = 9999
    with pytest.raises(compiler.CanonicalV31OutputError, match="token range is invalid"):
        compiler.validate_and_project_output(request, output)


def test_source_lineage_tamper_is_rejected() -> None:
    source_request, source_output = _fixture_request_and_output()
    request = compiler.prepare_request(source_request, source_output)
    request["private_input"]["source_output"]["segments"][0]["discourse_events"][0][
        "claim_text"
    ] = "A different but schema-valid tampered technical claim."
    with pytest.raises(compiler.CanonicalV31EpisodeBatchError, match="request drifted"):
        compiler.validate_prepared_request(request)


def test_epoch13_compiler_canary_is_small_complete_and_predeclared() -> None:
    source_request = json.loads(
        (EPOCH13_ROOT / "prepared-turn/request.private.json").read_text(
            encoding="utf-8"
        )
    )
    source_output = json.loads(
        (EPOCH13_ROOT / "turn/output.private.json").read_text(encoding="utf-8")
    )
    request = compiler.prepare_request(source_request, source_output)
    assert request["effective_batch_size"] == 16
    assert request["semantic_integrity"]["applicable_metric_case_count"] == 28
    assert request["semantic_integrity"]["exact_metric_case_count_preserved"] == 12
    assert len(request["prompt"].encode("utf-8")) == 22_389
    assert sum(
        len(case["literal_tokens"])
        for case in request["private_input"]["metric_cases"]
    ) == 2_423
    assert request["semantic_integrity"]["deterministic_semantic_pruning"] is False
