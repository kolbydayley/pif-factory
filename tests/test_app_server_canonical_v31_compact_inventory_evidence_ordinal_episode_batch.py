from __future__ import annotations

import copy
import json
from functools import lru_cache
from pathlib import Path

import pytest

from research_factory import (
    app_server_canonical_v31_compact_inventory_evidence_ordinal_episode_batch as adapter,
)


EPOCH27_ROOT = Path(
    "work/app-server-development-v2/unattended-pipeline-v5/"
    "canonical-v31-epoch27-tagged-metric-token-id-canary-v1"
)


@lru_cache(maxsize=1)
def _parent_request() -> dict:
    return json.loads(
        (EPOCH27_ROOT / "prepared-turn/request.private.json").read_text(encoding="utf-8")
    )


@lru_cache(maxsize=1)
def _parent_output() -> dict:
    return json.loads(
        (EPOCH27_ROOT / "turn/output.private.json").read_text(encoding="utf-8")
    )


@lru_cache(maxsize=1)
def _request() -> dict:
    values = adapter.prepare_episode_batches(
        adapter._episode_from_request(_parent_request()),  # noqa: SLF001
        batch_size=3,
        thread_mode="new_thread",
    )
    assert len(values) == 1
    return values[0]


@lru_cache(maxsize=1)
def _output() -> dict:
    return adapter.encode_parent_output_for_test(_request(), _parent_output())


def _first_populated_row(output: dict) -> tuple[int, int, dict]:
    for segment_index, segment in enumerate(output["1"]):
        for unit_index, row in enumerate(segment["2"]):
            if row["1"]:
                return segment_index, unit_index, row
    raise AssertionError("fixture has no compact inventory event")


def test_schema_is_inventory_first_with_evidence_relative_metric_ordinals() -> None:
    request = _request()
    schema = request["output_schema"]
    row = schema["properties"]["1"]["items"]["properties"]["2"]["items"]
    assert row["required"] == ["0", "1", "2"]
    assert row["properties"]["0"]["items"]["required"] == ["0", "1"]
    event = row["properties"]["1"]["items"]
    assert event["required"] == ["0", "1", "2", "3", "4", "5"]
    pair = event["properties"]["3"]["properties"]["0"]["items"]["properties"]["4"]
    assert pair["properties"]["0"] == {"type": "integer", "minimum": 0}
    assert pair["properties"]["1"] == {"type": "integer", "minimum": 0}
    encoded = json.dumps(schema, sort_keys=True)
    assert "prefixItems" not in encoded
    assert request["effort"] == "medium"
    assert request["retry_count"] == 0
    assert "complete ordered proposition inventory" in request["base_instructions"]
    assert "or aim for a target count" in request["base_instructions"].casefold()


def test_inventory_compact_projection_matches_parent_labels_byte_exact() -> None:
    projected = adapter.validate_and_project_output(_request(), copy.deepcopy(_output()))
    expected = adapter.parent.validate_and_project_output(
        adapter._parent_request(_request()),  # noqa: SLF001
        _parent_output(),
    )
    assert projected["labels"] == expected["labels"]
    fidelity = projected["fidelity"]
    assert fidelity["inventory_item_count"] == 25
    assert fidelity["inventory_event_count"] == 25
    assert fidelity["inventory_evidence_identity_count"] == 25
    assert fidelity["deterministic_inventory_removal_only"] is True
    assert fidelity["deterministic_evidence_relative_metric_ordinal_projection_only"] is True
    assert fidelity["deterministic_semantic_pruning"] is False
    assert fidelity["deterministic_support_filtering"] is False
    assert fidelity["deterministic_deduplication"] is False
    assert fidelity["deterministic_relabeling"] is False


def test_inventory_event_cardinality_and_evidence_identity_fail_closed() -> None:
    output = copy.deepcopy(_output())
    _segment_index, _unit_index, row = _first_populated_row(output)
    row["0"].pop()
    with pytest.raises(adapter.CanonicalV31OutputError, match="cardinality"):
        adapter.validate_and_project_output(_request(), output)

    output = copy.deepcopy(_output())
    _segment_index, _unit_index, row = _first_populated_row(output)
    row["0"][0]["1"] = (row["1"][0]["5"] + 1) % len(
        _request()["private_input"]["segments"][0]["evidence_spans"]
    )
    with pytest.raises(adapter.CanonicalV31OutputError, match="evidence identity"):
        adapter.validate_and_project_output(_request(), output)


def test_metric_ordinals_project_only_to_exact_selected_evidence_tokens() -> None:
    output = copy.deepcopy(_output())
    segment_index, _unit_index, row = _first_populated_row(output)
    event = row["1"][0]
    event["3"] = {
        "0": [
            {
                "0": "increase",
                "1": {"0": 0, "1": 0},
                "2": None,
                "3": None,
                "4": {"0": 0, "1": 0},
            }
        ]
    }
    converted, diagnostics = adapter._parent_wire_output(_request(), output)  # noqa: SLF001
    converted_event = next(
        event
        for unit in converted["s"][segment_index]["2"]
        for event in unit["0"]
        if event["20"]["0"]
    )
    source = _request()["private_input"]["segments"][segment_index]
    expected_id = adapter._evidence_tokens(source, event["5"])[0]["literal_token_id"]  # noqa: SLF001
    assert converted_event["20"]["0"][0]["1"] == {"0": expected_id, "1": expected_id}
    assert converted_event["20"]["0"][0]["4"] == {"0": expected_id, "1": expected_id}
    assert diagnostics["inventory_metric_ordinal_pair_count"] == 2


def test_out_of_range_metric_ordinal_fails_without_repair() -> None:
    output = copy.deepcopy(_output())
    segment_index, _unit_index, row = _first_populated_row(output)
    event = row["1"][0]
    source = _request()["private_input"]["segments"][segment_index]
    invalid = len(adapter._evidence_tokens(source, event["5"]))  # noqa: SLF001
    event["3"] = {
        "0": [
            {
                "0": "increase",
                "1": {"0": invalid, "1": invalid},
                "2": None,
                "3": None,
                "4": {"0": 0, "1": 0},
            }
        ]
    }
    with pytest.raises(adapter.CanonicalV31OutputError, match="ordinal range"):
        adapter.validate_and_project_output(_request(), output)


def test_duplicate_table_value_remains_diagnostic_only() -> None:
    output = copy.deepcopy(_output())
    output["0"]["0"].append(copy.deepcopy(output["0"]["0"][0]))
    projected = adapter.validate_and_project_output(_request(), output)
    assert projected["fidelity"]["compact_table_duplicate_count"] == 1
    assert projected["fidelity"]["compact_event_count"] == 25
    assert projected["fidelity"]["table_duplicate_count_is_diagnostic_only"] is True


def test_binding_is_medium_effort_managed_and_nonsemantic_projection() -> None:
    binding = adapter.build_six_arm_matrix_binding()
    assert binding["candidate_system_id"] == adapter.CANDIDATE_SYSTEM_ID
    assert binding["inventory_protocol_version"] == adapter.INVENTORY_PROTOCOL_VERSION
    assert binding["model"] == "gpt-5.6-sol"
    assert binding["effort"] == "medium"
    assert binding["deterministic_inventory_removal_only"] is True
    assert binding["deterministic_evidence_relative_metric_ordinal_projection_only"] is True
    assert binding["deterministic_semantic_defaults"] == {}
    assert binding["deterministic_semantic_pruning"] is False
    assert binding["deterministic_support_filtering"] is False
    assert binding["deterministic_deduplication"] is False
    assert binding["deterministic_relabeling"] is False
