from __future__ import annotations

import copy

import pytest

from research_factory import app_server_canonical_v31_unit_owned_positional_canary_runtime as source
from research_factory import app_server_canonical_v31_unit_owned_token_id_low_episode_batch as adapter


def _request() -> dict:
    values = adapter.prepare_episode_batches(
        source._source_episode(),  # noqa: SLF001
        batch_size=3,
        thread_mode="new_thread",
    )
    assert len(values) == 1
    return values[0]


def test_low_variant_changes_only_explicit_system_lineage() -> None:
    request = _request()
    parent = adapter._parent_request(request)  # noqa: SLF001
    assert request["effort"] == "low"
    assert parent["effort"] == "high"
    assert request["prompt"] == parent["prompt"]
    assert request["base_instructions"] == parent["base_instructions"]
    assert request["output_schema"] == parent["output_schema"]
    assert request["prompt_sha256"] == parent["prompt_sha256"]
    assert request["base_instructions_sha256"] == parent["base_instructions_sha256"]
    assert request["output_schema_sha256"] == parent["output_schema_sha256"]


def test_request_validation_rejects_effort_drift() -> None:
    request = _request()
    request["effort"] = "high"
    with pytest.raises(adapter.CanonicalV31EpisodeBatchError, match="low-effort request drifted"):
        adapter.validate_prepared_request(request)


def test_projection_delegates_exact_parent_request_without_semantic_change(monkeypatch) -> None:
    request = _request()
    sentinel = {
        "labels": [{"segment_id": "fixture"}],
        "provenance": {"schema_version": "parent"},
        "fidelity": {"schema_version": "parent", "deterministic_semantic_pruning": False},
    }
    observed = {}

    def fake(parent_request, output):
        observed["request"] = copy.deepcopy(parent_request)
        observed["output"] = copy.deepcopy(output)
        return copy.deepcopy(sentinel)

    monkeypatch.setattr(adapter.parent, "validate_and_project_output", fake)
    output = {"s": []}
    projected = adapter.validate_and_project_output(request, output)
    assert observed["request"] == adapter._parent_request(request)  # noqa: SLF001
    assert observed["output"] == output
    assert projected["labels"] == sentinel["labels"]
    assert projected["fidelity"]["reasoning_effort"] == "low"


def test_binding_is_managed_low_effort_and_nonpruning() -> None:
    binding = adapter.build_six_arm_matrix_binding()
    assert binding["model"] == "gpt-5.6-sol"
    assert binding["effort"] == "low"
    assert binding["prompt_unchanged_from_parent"] is True
    assert binding["output_schema_unchanged_from_parent"] is True
    assert binding["deterministic_semantic_pruning"] is False
