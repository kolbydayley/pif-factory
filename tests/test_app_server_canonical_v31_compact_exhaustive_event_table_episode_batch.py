from __future__ import annotations

import copy
import json
from functools import lru_cache
from pathlib import Path

import pytest

from research_factory import (
    app_server_canonical_v31_compact_exhaustive_event_table_episode_batch as adapter,
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


def _first_event(output: dict) -> dict:
    for segment in output["1"]:
        for row in segment["2"]:
            if row["0"]:
                return row["0"][0]
    raise AssertionError("fixture has no compact event")


def test_schema_is_provider_compatible_compact_normalized_table() -> None:
    request = _request()
    schema = request["output_schema"]
    assert schema["required"] == ["0", "1"]
    assert list(schema["properties"]["0"]["properties"]) == [
        str(index) for index in range(13)
    ]
    event = (
        schema["properties"]["1"]["items"]["properties"]["2"]["items"]
        ["properties"]["0"]["items"]
    )
    assert event["required"] == ["0", "1", "2", "3", "4", "5"]
    assert event["properties"]["0"]["minItems"] == 5
    assert event["properties"]["1"]["minItems"] == 6
    assert event["properties"]["2"]["minItems"] == 13
    encoded = json.dumps(schema, sort_keys=True)
    assert "prefixItems" not in encoded
    assert '"items": false' not in encoded
    assert request["effort"] == "high"
    assert request["retry_count"] == 0
    assert "three independent semantic sweeps" in request["base_instructions"]
    assert "The key s contains one segment object" not in request["base_instructions"]
    assert "do not aim for a target count" in request["base_instructions"]


def test_compact_table_projects_parent_labels_byte_exact() -> None:
    projected = adapter.validate_and_project_output(_request(), copy.deepcopy(_output()))
    expected = adapter.parent.validate_and_project_output(
        adapter._parent_request(_request()),  # noqa: SLF001
        _parent_output(),
    )
    assert projected["labels"] == expected["labels"]
    fidelity = projected["fidelity"]
    assert fidelity["compact_event_count"] == 25
    assert fidelity["compact_concept_count"] == expected["fidelity"]["emitted_concept_count"]
    assert fidelity["compact_table_reference_count"] > 25 * 13
    assert fidelity["deterministic_reference_expansion_only"] is True
    assert fidelity["deterministic_semantic_defaults"] == {}
    assert fidelity["deterministic_semantic_pruning"] is False
    assert fidelity["deterministic_support_filtering"] is False
    assert fidelity["deterministic_deduplication"] is False
    assert fidelity["deterministic_relabeling"] is False


def test_out_of_range_reference_fails_closed_without_semantic_repair() -> None:
    output = copy.deepcopy(_output())
    event = _first_event(output)
    event["2"][0] = len(output["0"]["0"])
    with pytest.raises(adapter.CanonicalV31OutputError, match="out of range"):
        adapter.validate_and_project_output(_request(), output)


def test_compact_vector_cardinality_drift_fails_closed() -> None:
    output = copy.deepcopy(_output())
    _first_event(output)["1"].pop()
    with pytest.raises(adapter.CanonicalV31OutputError, match="vector cardinality"):
        adapter.validate_and_project_output(_request(), output)


def test_duplicate_table_value_is_diagnostic_only_and_changes_no_event() -> None:
    output = copy.deepcopy(_output())
    duplicate = copy.deepcopy(output["0"]["0"][0])
    output["0"]["0"].append(duplicate)
    projected = adapter.validate_and_project_output(_request(), output)
    assert projected["fidelity"]["compact_table_duplicate_count"] == 1
    assert projected["fidelity"]["compact_event_count"] == 25
    assert projected["fidelity"]["event_count_is_diagnostic_only"] is True
    assert projected["fidelity"]["table_duplicate_count_is_diagnostic_only"] is True


def test_binding_is_high_effort_managed_and_nonsemantic_projection() -> None:
    binding = adapter.build_six_arm_matrix_binding()
    assert binding["candidate_system_id"] == adapter.CANDIDATE_SYSTEM_ID
    assert binding["compact_table_protocol_version"] == adapter.COMPACT_TABLE_PROTOCOL_VERSION
    assert binding["model"] == "gpt-5.6-sol"
    assert binding["effort"] == "high"
    assert binding["deterministic_semantic_defaults"] == {}
    assert binding["deterministic_semantic_pruning"] is False
    assert binding["deterministic_support_filtering"] is False
    assert binding["deterministic_deduplication"] is False
    assert binding["deterministic_relabeling"] is False
