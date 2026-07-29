from __future__ import annotations

import copy
import json
from functools import lru_cache
from pathlib import Path

import pytest

from research_factory import app_server_canonical_v31_dual_pass_omission_audit_ledger_episode_batch as adapter


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
    raise AssertionError("fixture has no populated primary ledger row")


def test_schema_has_two_full_canonical_event_ledgers() -> None:
    request = _request()
    row = request["output_schema"]["properties"]["s"]["items"]["properties"]["2"]["items"]
    assert row["required"] == ["0", "1", "2"]
    assert list(row["properties"]) == ["0", "1", "2"]
    assert row["properties"]["0"] == row["properties"]["2"]
    for lane in ("0", "2"):
        event = row["properties"][lane]["items"]["properties"]["1"]
        assert event["required"] == [str(index) for index in range(27)]
    assert "Do not aim for a target count" in request["base_instructions"]


def test_empty_audit_lane_projects_parent_labels_byte_exact() -> None:
    request = _request()
    projected = adapter.validate_and_project_output(request, copy.deepcopy(_output()))
    expected = adapter.parent.validate_and_project_output(
        adapter._parent_request(request),  # noqa: SLF001
        adapter.parent.encode_parent_output_for_test(
            adapter._parent_request(request),  # noqa: SLF001
            _parent_output(),
        ),
    )
    assert projected["labels"] == expected["labels"]
    assert projected["fidelity"]["primary_ledger_item_count"] == 25
    assert projected["fidelity"]["omission_audit_ledger_item_count"] == 0
    assert projected["fidelity"]["dual_ledger_item_count"] == 25


def test_audit_event_is_structurally_appended_without_deduplication() -> None:
    output = copy.deepcopy(_output())
    row = _first_populated_row(output)
    row["2"].append(copy.deepcopy(row["0"][0]))

    converted, diagnostics = adapter._parent_wire_output(_request(), output)  # noqa: SLF001
    converted_row = _first_populated_row(converted)

    assert converted_row["0"][-1] == converted_row["0"][0]
    assert diagnostics["primary_ledger_item_count"] == 25
    assert diagnostics["omission_audit_ledger_item_count"] == 1
    assert diagnostics["dual_ledger_item_count"] == 26


def test_malformed_audit_item_fails_closed() -> None:
    output = copy.deepcopy(_output())
    row = _first_populated_row(output)
    row["2"].append({"0": "Missing canonical event"})
    with pytest.raises(adapter.CanonicalV31OutputError, match="ledger item"):
        adapter.validate_and_project_output(_request(), output)


def test_binding_is_managed_full_semantic_and_nonpruning() -> None:
    binding = adapter.build_six_arm_matrix_binding()
    assert binding["candidate_system_id"] == adapter.CANDIDATE_SYSTEM_ID
    assert binding["dual_pass_protocol_version"] == adapter.DUAL_PASS_PROTOCOL_VERSION
    assert binding["model"] == "gpt-5.6-sol"
    assert binding["effort"] == "medium"
    assert binding["deterministic_dual_ledger_concatenation_only"] is True
    assert binding["audit_duplicates_preserved"] is True
    assert binding["deterministic_semantic_defaults"] == {}
    assert binding["deterministic_semantic_pruning"] is False
    assert binding["deterministic_support_filtering"] is False
    assert binding["deterministic_deduplication"] is False
    assert binding["deterministic_relabeling"] is False
