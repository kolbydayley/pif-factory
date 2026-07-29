from __future__ import annotations

import copy
import json
from functools import lru_cache
from pathlib import Path

import pytest

from research_factory import (
    app_server_canonical_v31_inventory_first_tagged_metric_token_id_episode_batch as adapter,
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


def _first_populated_row(output: dict) -> dict:
    for segment in output["s"]:
        for row in segment["2"]:
            if row["1"]:
                return row
    raise AssertionError("fixture has no populated unit row")


def test_schema_orders_inventory_before_events_and_preserves_high_effort() -> None:
    request = _request()
    row = request["output_schema"]["properties"]["s"]["items"]["properties"]["2"]["items"]
    assert row["required"] == ["0", "1", "2"]
    assert list(row["properties"]) == ["0", "1", "2"]
    assert row["properties"]["0"]["items"]["required"] == ["0", "1"]
    assert request["model"] == "gpt-5.6-sol"
    assert request["effort"] == "high"
    assert request["retry_count"] == 0
    assert "Do not aim for a target count" in request["base_instructions"]


def test_one_to_one_inventory_projects_parent_labels_byte_exact() -> None:
    request = _request()
    output = copy.deepcopy(_output())
    projected = adapter.validate_and_project_output(request, output)
    expected = adapter.parent.validate_and_project_output(
        adapter._parent_request(request),  # noqa: SLF001
        _parent_output(),
    )
    assert projected["labels"] == expected["labels"]
    assert projected["fidelity"]["inventory_item_count"] == 25
    assert projected["fidelity"]["inventory_event_count"] == 25
    assert projected["fidelity"]["inventory_evidence_identity_count"] == 25
    assert projected["fidelity"]["deterministic_semantic_pruning"] is False
    assert projected["fidelity"]["deterministic_support_filtering"] is False
    assert projected["fidelity"]["deterministic_deduplication"] is False
    assert projected["fidelity"]["deterministic_relabeling"] is False


def test_inventory_event_cardinality_mismatch_fails_closed() -> None:
    output = copy.deepcopy(_output())
    row = _first_populated_row(output)
    row["0"].pop()
    with pytest.raises(adapter.CanonicalV31OutputError, match="cardinality"):
        adapter.validate_and_project_output(_request(), output)


def test_inventory_event_evidence_pointer_mismatch_fails_closed() -> None:
    output = copy.deepcopy(_output())
    row = _first_populated_row(output)
    row["0"][0]["1"] = row["1"][0]["26"] + 1
    with pytest.raises(adapter.CanonicalV31OutputError, match="evidence pointer"):
        adapter.validate_and_project_output(_request(), output)


def test_binding_declares_only_structural_inventory_removal() -> None:
    binding = adapter.build_six_arm_matrix_binding()
    assert binding["candidate_system_id"] == adapter.CANDIDATE_SYSTEM_ID
    assert binding["inventory_protocol_version"] == adapter.INVENTORY_PROTOCOL_VERSION
    assert binding["model"] == "gpt-5.6-sol"
    assert binding["effort"] == "high"
    assert binding["deterministic_semantic_defaults"] == {}
    assert binding["deterministic_semantic_pruning"] is False
    assert binding["deterministic_support_filtering"] is False
    assert binding["deterministic_deduplication"] is False
    assert binding["deterministic_relabeling"] is False
