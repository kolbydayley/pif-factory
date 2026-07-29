from __future__ import annotations

import copy
import json
from functools import lru_cache
from pathlib import Path

import pytest

from research_factory import app_server_canonical_v31_nested_proposition_event_ledger_episode_batch as adapter


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


def _first_populated_row(output: dict) -> dict:
    for segment in output["s"]:
        for row in segment["2"]:
            if row["0"]:
                return row
    raise AssertionError("fixture has no populated ledger row")


def test_schema_nests_exactly_one_full_event_in_each_proposition_item() -> None:
    request = _request()
    row = request["output_schema"]["properties"]["s"]["items"]["properties"]["2"]["items"]
    assert row["required"] == ["0", "1"]
    assert list(row["properties"]) == ["0", "1"]
    item = row["properties"]["0"]["items"]
    assert item["required"] == ["0", "1"]
    assert item["properties"]["0"]["maxLength"] == 96
    assert item["properties"]["1"]["required"] == [str(index) for index in range(27)]
    assert request["effort"] == "medium"
    assert request["retry_count"] == 0
    assert "Do not aim for a target count" in request["base_instructions"]


def test_nested_ledger_projects_parent_labels_byte_exact() -> None:
    request = _request()
    projected = adapter.validate_and_project_output(request, copy.deepcopy(_output()))
    expected = adapter.parent.validate_and_project_output(
        adapter._parent_request(request),  # noqa: SLF001
        _parent_output(),
    )
    assert projected["labels"] == expected["labels"]
    assert projected["fidelity"]["ledger_item_count"] == 25
    assert projected["fidelity"]["ledger_event_count"] == 25
    assert projected["fidelity"]["deterministic_ledger_unwrap_only"] is True
    assert projected["fidelity"]["deterministic_semantic_pruning"] is False
    assert projected["fidelity"]["deterministic_support_filtering"] is False
    assert projected["fidelity"]["deterministic_deduplication"] is False
    assert projected["fidelity"]["deterministic_relabeling"] is False


def test_ledger_item_without_full_event_fails_closed() -> None:
    output = copy.deepcopy(_output())
    item = _first_populated_row(output)["0"][0]
    del item["1"]
    with pytest.raises(adapter.CanonicalV31OutputError, match="ledger item"):
        adapter.validate_and_project_output(_request(), output)


def test_ledger_summary_cannot_create_or_remove_an_event() -> None:
    output = copy.deepcopy(_output())
    row = _first_populated_row(output)
    original_count = len(row["0"])
    row["0"][0]["0"] = "Different private planning label"
    converted, diagnostics = adapter._parent_wire_output(_request(), output)  # noqa: SLF001
    converted_count = sum(
        len(unit["0"])
        for segment in converted["s"]
        for unit in segment["2"]
    )
    assert converted_count == 25
    assert len(row["0"]) == original_count
    assert diagnostics["ledger_item_count"] == 25


def test_binding_is_medium_effort_managed_and_nonsemantic_projection() -> None:
    binding = adapter.build_six_arm_matrix_binding()
    assert binding["candidate_system_id"] == adapter.CANDIDATE_SYSTEM_ID
    assert binding["ledger_protocol_version"] == adapter.LEDGER_PROTOCOL_VERSION
    assert binding["model"] == "gpt-5.6-sol"
    assert binding["effort"] == "medium"
    assert binding["deterministic_semantic_defaults"] == {}
    assert binding["deterministic_semantic_pruning"] is False
    assert binding["deterministic_support_filtering"] is False
    assert binding["deterministic_deduplication"] is False
    assert binding["deterministic_relabeling"] is False
