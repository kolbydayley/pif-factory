from __future__ import annotations

import copy
import json
from functools import lru_cache
from pathlib import Path

from research_factory import (
    app_server_canonical_v31_low_effort_keyed_enum_compact_inventory_episode_batch as adapter,
)


EPOCH27_ROOT = Path(
    "work/app-server-development-v2/unattended-pipeline-v5/"
    "canonical-v31-epoch27-tagged-metric-token-id-canary-v1"
)


@lru_cache(maxsize=1)
def _source_request() -> dict:
    return json.loads(
        (EPOCH27_ROOT / "prepared-turn/request.private.json").read_text(encoding="utf-8")
    )


@lru_cache(maxsize=1)
def _source_output() -> dict:
    return json.loads(
        (EPOCH27_ROOT / "turn/output.private.json").read_text(encoding="utf-8")
    )


@lru_cache(maxsize=1)
def _request() -> dict:
    values = adapter.prepare_episode_batches(
        adapter._episode_from_request(_source_request()),  # noqa: SLF001
        batch_size=3,
        thread_mode="new_thread",
    )
    assert len(values) == 1
    return values[0]


def test_request_changes_only_recovery_identity_and_effort_surface() -> None:
    request = _request()
    parent = adapter._parent_request(request)  # noqa: SLF001
    assert request["effort"] == "low"
    assert parent["effort"] == "medium"
    assert request["prompt"] == parent["prompt"]
    assert request["base_instructions"] == parent["base_instructions"]
    assert request["output_schema"] == parent["output_schema"]
    assert request["semantic_integrity"]["low_effort_timeout_recovery_only"] is True
    assert request["semantic_integrity"]["reasoning_effort"] == "low"


def test_projection_preserves_keyed_parent_labels_exactly() -> None:
    request = _request()
    parent_request = adapter._parent_request(request)  # noqa: SLF001
    output = adapter.encode_parent_output_for_test(request, _source_output())
    projected = adapter.validate_and_project_output(request, copy.deepcopy(output))
    expected = adapter.parent.validate_and_project_output(parent_request, output)
    assert projected["labels"] == expected["labels"]
    assert projected["fidelity"]["reasoning_effort"] == "low"
    assert projected["fidelity"]["low_effort_timeout_recovery_only"] is True


def test_closed_keyed_enum_contract_is_unchanged() -> None:
    request = _request()
    event = request["output_schema"]["properties"]["1"]["items"]["properties"][
        "2"
    ]["items"]["properties"]["1"]["items"]
    keyed = event["properties"]["0"]
    assert keyed["type"] == "object"
    assert keyed["required"] == ["0", "1", "2", "3", "4"]
    assert keyed["additionalProperties"] is False


def test_binding_is_low_effort_zero_retry_and_nonsemantic() -> None:
    binding = adapter.build_six_arm_matrix_binding()
    assert binding["model"] == "gpt-5.6-sol"
    assert binding["effort"] == "low"
    assert adapter.RETRY_COUNT == 0
    assert binding["low_effort_timeout_recovery_only"] is True
    assert binding["deterministic_semantic_defaults"] == {}
    assert binding["deterministic_semantic_pruning"] is False
    assert binding["deterministic_deduplication"] is False
    assert binding["deterministic_relabeling"] is False
