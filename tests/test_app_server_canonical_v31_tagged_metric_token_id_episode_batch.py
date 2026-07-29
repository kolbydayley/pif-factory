from __future__ import annotations

import copy
import json
from functools import lru_cache
from pathlib import Path

import pytest

from research_factory import app_server_canonical_v31_single_message_compact_pointer_episode_batch as canonical_parent
from research_factory import app_server_canonical_v31_tagged_metric_token_id_episode_batch as adapter
from research_factory import app_server_canonical_v31_unit_owned_token_id_canary_runtime as provider


ROOT = Path(
    "work/app-server-development-v2/unattended-pipeline-v5/"
    "canonical-v31-epoch19-single-message-compact-pointer-canary-v1"
)


@lru_cache(maxsize=1)
def _canonical_request() -> dict:
    return json.loads((ROOT / "prepared-turn/request.private.json").read_text())


@lru_cache(maxsize=1)
def _canonical_output() -> dict:
    return json.loads((ROOT / "turn/output.private.json").read_text())


@lru_cache(maxsize=1)
def _request() -> dict:
    source = _canonical_request()
    episode = copy.deepcopy(source["episode_context"])
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
        for segment in source["private_input"]["segments"]
    ]
    values = adapter.prepare_episode_batches(
        episode, batch_size=8, thread_mode="new_thread"
    )
    assert len(values) == 1
    return values[0]


@lru_cache(maxsize=1)
def _parent_wire() -> dict:
    parent_request = adapter._parent_request(_request())  # noqa: SLF001
    return adapter.parent.encode_parent_output_for_test(
        parent_request, _canonical_output()
    )


@lru_cache(maxsize=1)
def _tagged_wire() -> dict:
    return adapter.encode_parent_output_for_test(_request(), _parent_wire())


def _first_event(request: dict, output: dict) -> tuple[dict, dict, dict]:
    parent_request = adapter._parent_request(request)  # noqa: SLF001
    layout = adapter._metric_layout(parent_request)  # noqa: SLF001
    for segment_index, segment in enumerate(output["s"]):
        source = request["private_input"]["segments"][segment_index]
        for row in segment["2"]:
            if row["0"]:
                return row["0"][0], source, layout
    raise AssertionError("fixture contains no event")


def test_tagged_metric_schema_is_provider_compatible_and_structurally_coupled() -> None:
    request = _request()
    assert request["effort"] == "medium"
    assert request["retry_count"] == 0
    assert request["semantic_integrity"][
        "model_authors_metric_applicability_by_bundle_cardinality"
    ] is True
    provider.epoch24._validate_provider_schema(request["output_schema"])  # noqa: SLF001
    parent_request = adapter._parent_request(request)  # noqa: SLF001
    layout = adapter._metric_layout(parent_request)  # noqa: SLF001
    metric = (
        request["output_schema"]["properties"]["s"]["items"]["properties"]["2"]
        ["items"]["properties"]["0"]["items"]["properties"]
        [str(layout["event_metric"])]
    )
    assert metric["properties"]["0"]["maxItems"] == 1
    entry = metric["properties"]["0"]["items"]
    assert "not_applicable" not in entry["properties"]["0"]["enum"]
    assert entry["properties"]["4"]["type"] == "object"


def test_parent_output_round_trips_without_semantic_defaults_or_pruning() -> None:
    request = _request()
    projected = adapter.validate_and_project_output(
        request, copy.deepcopy(_tagged_wire())
    )
    expected = canonical_parent.validate_and_project_output(
        _canonical_request(), _canonical_output()
    )
    assert projected["labels"] == expected["labels"]
    assert projected["fidelity"]["emitted_event_count"] == 31
    assert projected["fidelity"]["deterministic_semantic_defaults"] == {}
    assert projected["fidelity"]["deterministic_semantic_pruning"] is False


def test_empty_bundle_is_an_explicit_not_applicable_choice() -> None:
    request = _request()
    output = copy.deepcopy(_tagged_wire())
    event, _, layout = _first_event(request, output)
    event[str(layout["event_metric"])] = {"0": []}
    parent_wire, diagnostics = adapter._parent_wire_output(request, output)  # noqa: SLF001
    parent_event, _, _ = _first_event(request, parent_wire)
    metric = parent_event[str(layout["event_metric"])]
    assert metric[str(layout["metric_direction"])] == "not_applicable"
    assert all(
        metric[str(layout["metric_fields"][field])] is None
        for field in adapter.parent.METRIC_RANGE_FIELDS
    )
    assert diagnostics["explicit_not_applicable_metric_bundle_count"] >= 1


def test_applicable_bundle_requires_exact_raw_text_pair() -> None:
    request = _request()
    output = copy.deepcopy(_tagged_wire())
    event, _, layout = _first_event(request, output)
    event[str(layout["event_metric"])] = {
        "0": [{"0": "increase", "1": None, "2": None, "3": None, "4": None}]
    }
    with pytest.raises(adapter.CanonicalV31OutputError, match="applicable metric"):
        adapter.validate_and_project_output(request, output)


def test_tagged_binding_is_medium_effort_managed_and_nonpruning() -> None:
    binding = adapter.build_six_arm_matrix_binding()
    assert binding["model"] == "gpt-5.6-sol"
    assert binding["effort"] == "medium"
    assert binding["tagged_metric_protocol_version"] == adapter.TAGGED_METRIC_PROTOCOL_VERSION
    assert binding["deterministic_semantic_defaults"] == {}
    assert binding["deterministic_semantic_pruning"] is False
