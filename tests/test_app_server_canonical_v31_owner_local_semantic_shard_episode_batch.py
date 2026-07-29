from __future__ import annotations

import copy
import json
from functools import lru_cache
from pathlib import Path

import pytest

from research_factory import (
    app_server_canonical_v31_owner_local_semantic_shard_episode_batch as adapter,
)


EPOCH27_ROOT = Path(
    "work/app-server-development-v2/unattended-pipeline-v5/"
    "canonical-v31-epoch27-tagged-metric-token-id-canary-v1"
)


@lru_cache(maxsize=1)
def _tagged_request() -> dict:
    return json.loads(
        (EPOCH27_ROOT / "prepared-turn/request.private.json").read_text(encoding="utf-8")
    )


@lru_cache(maxsize=1)
def _tagged_output() -> dict:
    return json.loads(
        (EPOCH27_ROOT / "turn/output.private.json").read_text(encoding="utf-8")
    )


@lru_cache(maxsize=1)
def _request() -> dict:
    values = adapter.prepare_episode_batches(
        adapter._episode_from_request(_tagged_request()),  # noqa: SLF001
        batch_size=3,
        thread_mode="new_thread",
    )
    assert len(values) == 1
    return values[0]


@lru_cache(maxsize=1)
def _compact_output() -> dict:
    request = _request()
    compact_request = adapter._parent_request(request)  # noqa: SLF001
    return adapter.parent.encode_parent_output_for_test(compact_request, _tagged_output())


@lru_cache(maxsize=1)
def _output() -> dict:
    return adapter.encode_parent_output_for_test(_request(), _compact_output())


def _first_populated_row(output: dict) -> dict:
    for segment in output["1"]:
        for row in segment["2"]:
            if any(row["0"].values()):
                return row
    raise AssertionError("fixture has no populated semantic-shard row")


def _first_parent_populated_row(output: dict) -> dict:
    for segment in output["1"]:
        for row in segment["2"]:
            if row["0"]:
                return row
    raise AssertionError("fixture has no populated compact-parent row")


def _event_count(labels: list[dict]) -> int:
    return sum(len(label["discourse_events"]) for label in labels)


def test_schema_has_exactly_four_closed_event_lanes_without_prefix_items() -> None:
    request = _request()
    schema = request["output_schema"]
    row = schema["properties"]["1"]["items"]["properties"]["2"]["items"]
    lanes = row["properties"]["0"]
    assert lanes["type"] == "object"
    assert lanes["additionalProperties"] is False
    assert lanes["required"] == ["0", "1", "2", "3"]
    assert list(lanes["properties"]) == ["0", "1", "2", "3"]
    assert all(value["type"] == "array" for value in lanes["properties"].values())
    assert "prefixItems" not in json.dumps(schema, sort_keys=True)
    assert request["effort"] == "medium"
    assert request["retry_count"] == 0
    assert "four independent owner-local semantic passes" in request["base_instructions"]
    assert "do not aim for a target count" in request["base_instructions"]


def test_semantic_shards_project_compact_parent_labels_byte_exact() -> None:
    request = _request()
    projected = adapter.validate_and_project_output(request, copy.deepcopy(_output()))
    expected = adapter.parent.validate_and_project_output(
        adapter._parent_request(request),  # noqa: SLF001
        copy.deepcopy(_compact_output()),
    )
    assert projected["labels"] == expected["labels"]
    fidelity = projected["fidelity"]
    assert fidelity["semantic_shard_event_count"] == 25
    assert sum(fidelity["semantic_shard_event_counts"]) == 25
    assert fidelity["deterministic_shard_concatenation_only"] is True
    assert fidelity["deterministic_semantic_pruning"] is False
    assert fidelity["deterministic_support_filtering"] is False
    assert fidelity["deterministic_deduplication"] is False
    assert fidelity["deterministic_relabeling"] is False


def test_missing_semantic_lane_fails_closed() -> None:
    output = copy.deepcopy(_output())
    del _first_populated_row(output)["0"]["3"]
    with pytest.raises(adapter.CanonicalV31OutputError, match="lane object"):
        adapter.validate_and_project_output(_request(), output)


def test_fixed_lane_concatenation_preserves_exact_event_order_and_count() -> None:
    output = copy.deepcopy(_output())
    row = _first_populated_row(output)
    expected = [
        copy.deepcopy(event)
        for lane_index in range(len(adapter.SHARD_NAMES))
        for event in row["0"][str(lane_index)]
    ]
    expected_lane_counts = [
        sum(
            len(unit_row["0"][str(lane_index)])
            for segment in output["1"]
            for unit_row in segment["2"]
        )
        for lane_index in range(len(adapter.SHARD_NAMES))
    ]
    converted, diagnostics = adapter._parent_wire_output(_request(), output)  # noqa: SLF001
    converted_row = _first_parent_populated_row(converted)
    assert converted_row["0"] == expected
    assert diagnostics["semantic_shard_event_count"] == 25
    assert diagnostics["semantic_shard_event_counts"] == expected_lane_counts


def test_cross_lane_duplicate_is_preserved_and_diagnostic_only() -> None:
    output = copy.deepcopy(_output())
    row = _first_populated_row(output)
    source_lane = next(index for index in range(4) if row["0"][str(index)])
    target_lane = (source_lane + 1) % 4
    row["0"][str(target_lane)].append(copy.deepcopy(row["0"][str(source_lane)][0]))
    projected = adapter.validate_and_project_output(_request(), output)
    assert projected["fidelity"]["semantic_shard_event_count"] == 26
    assert projected["fidelity"]["cross_shard_duplicate_count"] == 1
    assert projected["fidelity"]["cross_shard_duplicate_count_is_diagnostic_only"] is True
    assert _event_count(projected["labels"]) == 26


def test_binding_is_medium_effort_managed_and_nonsemantic_projection() -> None:
    binding = adapter.build_six_arm_matrix_binding()
    assert binding["candidate_system_id"] == adapter.CANDIDATE_SYSTEM_ID
    assert binding["shard_protocol_version"] == adapter.SHARD_PROTOCOL_VERSION
    assert binding["semantic_shard_names"] == list(adapter.SHARD_NAMES)
    assert binding["model"] == "gpt-5.6-sol"
    assert binding["effort"] == "medium"
    assert binding["deterministic_shard_concatenation_only"] is True
    assert binding["deterministic_semantic_defaults"] == {}
    assert binding["deterministic_semantic_pruning"] is False
    assert binding["deterministic_support_filtering"] is False
    assert binding["deterministic_deduplication"] is False
    assert binding["deterministic_relabeling"] is False
