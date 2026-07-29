from __future__ import annotations

import copy
import json
from functools import lru_cache
from pathlib import Path

import pytest

from research_factory import (
    app_server_canonical_v31_keyed_enum_compact_inventory_evidence_ordinal_episode_batch as adapter,
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


@lru_cache(maxsize=1)
def _output() -> dict:
    return adapter.encode_parent_output_for_test(_request(), _source_output())


def _first_event(output: dict) -> dict:
    for segment in output["1"]:
        for unit in segment["2"]:
            if unit["1"]:
                return unit["1"][0]
    raise AssertionError("fixture has no event")


def test_all_enum_positions_are_closed_keyed_fields() -> None:
    request = _request()
    event = request["output_schema"]["properties"]["1"]["items"]["properties"][
        "2"
    ]["items"]["properties"]["1"]["items"]
    keyed = event["properties"]["0"]
    assert keyed["type"] == "object"
    assert keyed["additionalProperties"] is False
    assert keyed["required"] == ["0", "1", "2", "3", "4"]
    assert keyed["properties"]["0"]["enum"][0] == "term_usage"
    assert "supportive" not in keyed["properties"]["0"]["enum"]
    assert "supportive" in keyed["properties"]["1"]["enum"]
    assert "five-value enum vector" not in request["base_instructions"]
    assert "closed keyed enum object" in request["base_instructions"]


def test_keyed_enum_projection_preserves_parent_labels_exactly() -> None:
    projected = adapter.validate_and_project_output(_request(), copy.deepcopy(_output()))
    expected = adapter.parent.validate_and_project_output(
        adapter._parent_request(_request()),  # noqa: SLF001
        adapter.parent.encode_parent_output_for_test(
            adapter._parent_request(_request()), _source_output()  # noqa: SLF001
        ),
    )
    assert projected["labels"] == expected["labels"]
    fidelity = projected["fidelity"]
    assert fidelity["model_authors_closed_keyed_canonical_enum_object"] is True
    assert fidelity["all_canonical_enum_fields_are_position_specific"] is True
    assert fidelity["deterministic_keyed_enum_order_projection_only"] is True
    assert fidelity["deterministic_semantic_pruning"] is False
    assert fidelity["deterministic_deduplication"] is False


def test_cross_field_enum_value_fails_closed_without_relabeling() -> None:
    output = copy.deepcopy(_output())
    event = _first_event(output)
    event["0"]["0"] = event["0"]["2"]
    with pytest.raises(adapter.CanonicalV31OutputError, match="event_type"):
        adapter.validate_and_project_output(_request(), output)


def test_missing_or_extra_enum_key_fails_closed() -> None:
    output = copy.deepcopy(_output())
    del _first_event(output)["0"]["4"]
    with pytest.raises(adapter.CanonicalV31OutputError, match="keyed-enum"):
        adapter.validate_and_project_output(_request(), output)

    output = copy.deepcopy(_output())
    _first_event(output)["0"]["5"] = "unspecified"
    with pytest.raises(adapter.CanonicalV31OutputError, match="keyed-enum"):
        adapter.validate_and_project_output(_request(), output)


def test_inventory_evidence_and_metric_ordinal_contract_is_retained() -> None:
    integrity = _request()["semantic_integrity"]
    assert integrity["inventory_protocol_version"] == adapter.INVENTORY_PROTOCOL_VERSION
    assert integrity["one_inventory_item_per_canonical_event"] is True
    assert integrity["inventory_and_event_evidence_pointer_identity_required"] is True
    assert integrity["model_authors_evidence_relative_metric_token_ordinals"] is True
    assert integrity["keyed_enum_protocol_version"] == adapter.KEYED_ENUM_PROTOCOL_VERSION


def test_binding_is_medium_effort_managed_and_nonsemantic_projection() -> None:
    binding = adapter.build_six_arm_matrix_binding()
    assert binding["candidate_system_id"] == adapter.CANDIDATE_SYSTEM_ID
    assert binding["keyed_enum_protocol_version"] == adapter.KEYED_ENUM_PROTOCOL_VERSION
    assert binding["model"] == "gpt-5.6-sol"
    assert binding["effort"] == "medium"
    assert binding["deterministic_keyed_enum_order_projection_only"] is True
    assert binding["deterministic_semantic_defaults"] == {}
    assert binding["deterministic_semantic_pruning"] is False
    assert binding["deterministic_support_filtering"] is False
    assert binding["deterministic_deduplication"] is False
    assert binding["deterministic_relabeling"] is False
