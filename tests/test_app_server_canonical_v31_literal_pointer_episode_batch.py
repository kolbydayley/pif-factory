from __future__ import annotations

import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research_factory import app_server_canonical_v31_literal_pointer_episode_batch as adapter
from test_app_server_canonical_v31_bounded_span_episode_batch import (
    _coded_output,
    _episode,
)


EPOCH13_ROOT = Path(
    "work/app-server-development-v2/unattended-pipeline-v5/"
    "canonical-v31-epoch13-bounded-span-canary-v1"
)


def _token_index(source: dict, text: str, *, occurrence: int = 0) -> int:
    matches = [
        token["literal_token_index"]
        for token in source["literal_tokens"]
        if token["text"] == text
    ]
    return matches[occurrence]


def _pointerize_fixture_metric(request: dict, output: dict) -> None:
    source = request["private_input"]["segments"][0]
    event = output["segments"][0]["discourse_events"][0]
    value = _token_index(source, "40")
    unit = _token_index(source, "percent")
    event["metric"] = {
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


def test_literal_tokens_are_exact_stable_and_cover_nonwhitespace() -> None:
    text = "Value 40.5%, don't rewrite it.\n"
    first = adapter._literal_tokens(text, segment_position=3)
    second = adapter._literal_tokens(text, segment_position=3)
    assert first == second
    assert [token["text"] for token in first] == [
        "Value", "40", ".", "5", "%", ",", "don", "'", "t", "rewrite", "it", "."
    ]
    represented = set()
    for token in first:
        assert text[token["start_char"] : token["end_char"]] == token["text"]
        represented.update(range(token["start_char"], token["end_char"]))
    assert represented == {index for index, char in enumerate(text) if not char.isspace()}


def test_prepared_schema_replaces_all_free_form_metric_literals_with_pointers() -> None:
    text = "Host: New Atlas cuts inference latency by 40 percent.\n"
    request = adapter.prepare_episode_batches(
        _episode(text), batch_size=3, thread_mode="new_thread"
    )[0]
    assert request == adapter.validate_prepared_request(request)
    metric = request["output_schema"]["properties"]["segments"]["items"][
        "properties"
    ]["discourse_events"]["items"]["properties"]["metric"]
    assert set(metric["properties"]) == {
        "direction",
        *{
            f"{field}_{endpoint}_token_index"
            for field in adapter.METRIC_LITERAL_FIELDS
            for endpoint in ("start", "end")
        },
    }
    assert not ({"value", "unit", "comparator", "raw_text"} & set(metric["properties"]))
    assert "literal_tokens" in request["private_input"]["segments"][0]
    assert "zero-based start/end indices" in request["base_instructions"]
    prompt_packet = json.loads(request["prompt"].split("\n", 1)[1])
    assert "literal_token_texts" in prompt_packet["segments"][0]
    assert len(request["prompt"].encode("utf-8")) < 100_000


def test_model_selected_metric_pointers_project_exact_literals_without_semantic_repair() -> None:
    text = (
        "Host: New Atlas cuts inference latency by 40 percent.\n"
        "Guest: That makes on-device adoption feasible.\n"
    )
    request = adapter.prepare_episode_batches(
        _episode(text), batch_size=3, thread_mode="new_thread"
    )[0]
    output = _coded_output(request)
    _pointerize_fixture_metric(request, output)
    untouched = copy.deepcopy(output)
    projected = adapter.validate_and_project_output(request, output)
    metric = projected["labels"][0]["discourse_events"][0]["metric"]
    assert output == untouched
    assert metric == {
        "value": "40",
        "unit": "percent",
        "comparator": None,
        "direction": "decrease",
        "raw_text": "40 percent",
    }
    pointer_records = projected["provenance"]["segments"][0][
        "discourse_events"
    ][0]["metric_literal_pointers"]
    assert [row["field"] for row in pointer_records] == list(
        adapter.METRIC_LITERAL_FIELDS
    )
    assert projected["fidelity"][
        "model_selected_metric_literal_pointers_projected_exactly"
    ] is True


def test_applicable_metric_requires_raw_text_pointer_pair() -> None:
    text = "Host: New Atlas cuts inference latency by 40 percent.\n"
    request = adapter.prepare_episode_batches(
        _episode(text), batch_size=3, thread_mode="new_thread"
    )[0]
    output = _coded_output(request)
    _pointerize_fixture_metric(request, output)
    output["segments"][0]["discourse_events"][0]["metric"][
        "raw_text_start_token_index"
    ] = None
    output["segments"][0]["discourse_events"][0]["metric"][
        "raw_text_end_token_index"
    ] = None
    with pytest.raises(adapter.CanonicalV31OutputError, match="requires raw_text"):
        adapter.validate_and_project_output(request, output)


def test_metric_pointer_outside_selected_event_evidence_is_rejected() -> None:
    text = (
        "Host: New Atlas cuts inference latency by 40 percent. "
        + ("filler " * 100)
        + "Guest: comparator later.\n"
    )
    request = adapter.prepare_episode_batches(
        _episode(text), batch_size=3, thread_mode="new_thread"
    )[0]
    output = _coded_output(request)
    _pointerize_fixture_metric(request, output)
    later = _token_index(request["private_input"]["segments"][0], "later")
    metric = output["segments"][0]["discourse_events"][0]["metric"]
    metric["comparator_start_token_index"] = later
    metric["comparator_end_token_index"] = later
    with pytest.raises(adapter.CanonicalV31OutputError, match="outside event evidence"):
        adapter.validate_and_project_output(request, output)


def test_epoch13_failure_is_systematic_free_form_metric_literal_drift() -> None:
    request = json.loads(
        (EPOCH13_ROOT / "prepared-turn/request.private.json").read_text(encoding="utf-8")
    )
    output = json.loads(
        (EPOCH13_ROOT / "turn/output.private.json").read_text(encoding="utf-8")
    )
    failures = {"raw_text": 0, "value": 0, "unit": 0, "comparator": 0}
    for raw_segment, source in zip(output["segments"], request["private_input"]["segments"]):
        spans = {span["evidence_span_id"]: span for span in source["evidence_spans"]}
        for event in raw_segment["discourse_events"]:
            metric = event["metric"]
            span = spans[event["evidence_span_id"]]
            evidence = source["segment_text"][span["start_char"] : span["end_char"]]
            for field in failures:
                value = metric.get(field)
                if value not in (None, "") and value not in evidence:
                    failures[field] += 1
    assert failures == {"raw_text": 16, "value": 3, "unit": 0, "comparator": 1}


def test_literal_token_or_request_tamper_is_rejected() -> None:
    request = adapter.prepare_episode_batches(
        _episode("Host: New Atlas cuts inference latency by 40 percent.\n"),
        batch_size=3,
        thread_mode="new_thread",
    )[0]
    request["private_input"]["segments"][0]["literal_tokens"][0]["text"] = "drift"
    with pytest.raises(adapter.CanonicalV31EpisodeBatchError):
        adapter.validate_prepared_request(request)


def test_turn_sidecar_binds_pointer_prompt_schema_thread_and_output(
    tmp_path: Path,
) -> None:
    request = adapter.prepare_episode_batches(
        _episode("Host: New Atlas cuts inference latency by 40 percent.\n"),
        batch_size=3,
        thread_mode="new_thread",
    )[0]
    output_path = tmp_path / "output.json"
    output_path.write_text('{"segments":[]}\n', encoding="utf-8")
    sources = adapter.expected_instruction_source_contract()
    thread = SimpleNamespace(
        thread_id="thread-literal-pointer",
        model=adapter.MODEL,
        ephemeral=True,
        base_instructions_sha256=request["base_instructions_sha256"],
        base_instructions_bytes=len(request["base_instructions"].encode("utf-8")),
        instruction_sources_sha256=sources["effective_instruction_sources_sha256"],
        instruction_sources_count=sources["effective_instruction_sources_count"],
    )
    usage = {
        "input_tokens": 10,
        "cached_input_tokens": 0,
        "output_tokens": 5,
        "reasoning_output_tokens": 1,
        "total_tokens": 15,
    }
    sidecar = {
        "schema_version": adapter.base.codex_app_server.TURN_SIDECAR_SCHEMA_VERSION,
        "state": "completed",
        "status": "completed",
        "started_at": "2026-07-19T20:00:00+00:00",
        "finished_at": "2026-07-19T20:00:01+00:00",
        "client_version": adapter.base.codex_app_server.APP_SERVER_CLIENT_VERSION,
        "cli_version": adapter.base.codex_app_server.PINNED_CODEX_CLI_VERSION,
        "protocol_schema_sha256": adapter.base._sha256_file(  # noqa: SLF001
            adapter.base.codex_app_server.PROTOCOL_SCHEMA_PATH
        ),
        "transport": "stdio",
        "app_server_user_agent": "test",
        "max_message_bytes": 64 * 1024,
        "synthetic_debug_errors": False,
        "auth_type": "chatgpt",
        "plan_type": "pro",
        "thread_id": thread.thread_id,
        "turn_id": "turn-literal-pointer",
        "model": adapter.MODEL,
        "effort": adapter.EFFORT,
        "thread_mode": "new_thread",
        "batch_size": request["effective_batch_size"],
        "prompt_sha256": request["prompt_sha256"],
        "prompt_bytes": len(request["prompt"].encode("utf-8")),
        "base_instructions_sha256": request["base_instructions_sha256"],
        "base_instructions_bytes": len(request["base_instructions"].encode("utf-8")),
        "instruction_sources_sha256": sources["effective_instruction_sources_sha256"],
        "instruction_sources_count": sources["effective_instruction_sources_count"],
        "output_schema_sha256": request["output_schema_sha256"],
        "output_schema_bytes": len(
            adapter._canonical_json(request["output_schema"]).encode("utf-8")
        ),
        "error_class": None,
        "usage_status": "measured",
        "usage_complete": True,
        "usage": usage,
        "thread_total_usage": usage,
        "wall_elapsed_seconds": 1.0,
        "recovery_reran_model": False,
        "stderr_sha256": "0" * 64,
        "stderr_bytes": 0,
        "output_sha256": adapter.base._output_message_hash(output_path),  # noqa: SLF001
        "output_path": str(output_path.resolve()),
    }
    sidecar_path = tmp_path / "sidecar.json"
    sidecar_path.write_text(json.dumps(sidecar), encoding="utf-8")
    telemetry = adapter.validate_turn_sidecar(
        request,
        sidecar_path,
        output_path=output_path,
        expected_thread=thread,
    )
    assert telemetry["usage"] == usage
    sidecar["prompt_sha256"] = "0" * 64
    sidecar_path.write_text(json.dumps(sidecar), encoding="utf-8")
    with pytest.raises(adapter.CanonicalV31TelemetryError):
        adapter.validate_turn_sidecar(
            request,
            sidecar_path,
            output_path=output_path,
            expected_thread=thread,
        )
